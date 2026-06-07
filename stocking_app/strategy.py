from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd


CMO_PERIOD = 11
CMO_EMA_PERIOD = 5
CMO_SMA_PERIOD = 11
FRACTAL_LEFT_WINDOW = 2
FRACTAL_RIGHT_WINDOW = 2


def _empty(symbol: str, reason: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "asof_ts": None,
        "last_price": None,
        "signal": None,
        "signal_price": None,
        "signal_reason": reason,
    }


def _to_weekly(daily: pd.DataFrame, exchange_tz: str) -> pd.DataFrame:
    # OOM-FIX-v3: Avoid daily.copy() — only convert the index for resampling.
    # The index is a lightweight DatetimeTZDtype object (~8 bytes/element) vs
    # copying all OHLCV columns (~500 rows × 6 cols × 4 bytes = ~12 KB per symbol).
    # With 500 symbols this saves ~6 MB of unnecessary allocation per cycle.
    idx = daily.index
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    idx = idx.tz_convert(exchange_tz)

    # Assign converted index to a view (no column data copied)
    daily_local = daily.copy(deep=False)  # shallow copy: shares column arrays
    daily_local.index = idx

    weekly = (
        daily_local.resample("W-MON", label="left", closed="left")
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        .dropna(subset=["open", "high", "low", "close"])
    )

    # Drop the last (current, incomplete) weekly bar so fractal and CMO calculations
    # are never based on a partial week.  A week that started on Monday but whose
    # data only covers Mon–Wed will have a misleadingly low high/close that changes
    # by Friday, causing unstable fractal confirmations and false CMO crossovers.
    # The most-recent COMPLETE week is always at index -2 before this drop, -1 after.
    if len(weekly) > 1:
        weekly = weekly.iloc[:-1]

    del daily_local, idx
    return weekly


def compute_all_indicators(daily: pd.DataFrame, exchange_tz: str) -> pd.DataFrame:
    from .indicators import calculate_cmo, ema, fractal_chaos_bands, sma

    weekly = _to_weekly(daily, exchange_tz=exchange_tz)
    
    if weekly.empty:
        return pd.DataFrame()

    weekly = fractal_chaos_bands(weekly, FRACTAL_LEFT_WINDOW, FRACTAL_RIGHT_WINDOW)

    daily["cmo"] = calculate_cmo(daily, "close", CMO_PERIOD)
    weekly["cmo"] = calculate_cmo(weekly, "close", CMO_PERIOD)

    daily["ema_cmo"] = ema(daily["cmo"], CMO_EMA_PERIOD)
    daily["sma_cmo"] = sma(daily["cmo"], CMO_SMA_PERIOD)

    weekly["ema_cmo"] = ema(weekly["cmo"], CMO_EMA_PERIOD)
    weekly["sma_cmo"] = sma(weekly["cmo"], CMO_SMA_PERIOD)

    weekly_aliased = weekly[["upper_band_line", "lower_band_line", "ema_cmo", "sma_cmo"]].rename(
        columns={
            "upper_band_line": "weekly_upper_band",
            "lower_band_line": "weekly_lower_band",
            "ema_cmo": "weekly_ema_cmo",
            "sma_cmo": "weekly_sma_cmo",
        }
    )

    # BUG-WEEKLY-BAND-02 fix: daily index is UTC midnight, weekly index is
    # exchange_tz midnight (e.g. IST +05:30).  A plain .join() matches on exact
    # timestamp equality — but 2026-02-10T00:00+05:30 ≠ 2026-02-10T00:00+00:00,
    # so ZERO rows match and every weekly column is NaN.
    #
    # merge_asof(direction="backward") finds, for each daily timestamp, the most
    # recent weekly timestamp ≤ it (using absolute time comparison).  This
    # correctly maps each trading day to its enclosing weekly bar regardless of
    # timezone, and inherently forward-fills the weekly values.
    weekly_aliased.index = weekly_aliased.index.tz_convert("UTC")

    aligned = pd.merge_asof(
        daily.sort_index(),
        weekly_aliased.sort_index(),
        left_index=True,
        right_index=True,
        direction="backward",
    )

    # OOM-FIX-v2: Release intermediate DataFrames immediately
    del weekly, weekly_aliased

    return aligned

STOP_LOSS_PCT = 0.12  # exit if position is down 12% from buy price


def _compute_latest_signal(
    aligned: pd.DataFrame,
    prev_price_override: float | None = None,
    has_position: bool = False,
    entry_price: float | None = None,
) -> tuple[str | None, float | None, str]:

    critical = [
        "close",
        "weekly_upper_band",
        # weekly_lower_band is NOT in critical: it's not used in any signal logic,
        # and including it would drop all rows for stocks with no confirmed lower fractal.
        "ema_cmo",
        "sma_cmo",
        "weekly_ema_cmo",
        "weekly_sma_cmo",
    ]
    aligned = aligned.dropna(subset=critical)
    if len(aligned) < 2:
        return None, None, "insufficient_rows_after_indicator_warmup"

    prev = aligned.iloc[-2]
    curr = aligned.iloc[-1]

    # BUG-CROSS-01: Use prev_price_override (from last cycle) if provided,
    # otherwise fall back to the previous day's close. This allows 5-minute
    # sensitivity on daily data polling.
    prev_close = prev_price_override if prev_price_override is not None else prev["close"]

    # BUG-BAND-DROP: Only buy when the upper band has NOT dropped since the previous
    # bar.  A new lower weekly fractal shrinks the band, making price appear to have
    # crossed resistance when it never actually broke the prior high.  Requiring
    # curr_band >= prev_band ensures we only enter on genuine breakouts.
    buy_cross = (
        prev_close <= prev["weekly_upper_band"]
        and curr["close"] > curr["weekly_upper_band"]
        and curr["weekly_upper_band"] >= prev["weekly_upper_band"]
    )
    daily_sell_cross = curr["ema_cmo"] < curr["sma_cmo"] and prev["ema_cmo"] >= prev["sma_cmo"]
    weekly_sell_cross = (
        curr["weekly_ema_cmo"] < curr["weekly_sma_cmo"]
        and prev["weekly_ema_cmo"] >= prev["weekly_sma_cmo"]
    )

    # Stop-loss: exit immediately if the position is down beyond STOP_LOSS_PCT from
    # the entry price.  Checked before CMO signals so it always takes priority.
    if has_position and entry_price is not None and entry_price > 0:
        drawdown = (float(curr["close"]) - entry_price) / entry_price
        if drawdown <= -STOP_LOSS_PCT:
            return "SELL", float(curr["close"]), f"stop_loss_{abs(drawdown)*100:.1f}pct"

    if daily_sell_cross or weekly_sell_cross:
        trigger = "daily_cmo_crossdown" if daily_sell_cross else "weekly_cmo_crossdown"
        return "SELL", float(curr["close"]), trigger

    # BUG-MISSED-CROSSOVER: The crossover conditions above only fire on the exact bar
    # of the cross.  If the engine was down that day the signal is lost forever and a
    # held position bleeds indefinitely.  For positions we already hold, also sell
    # when the CMO has been persistently bearish across two consecutive bars — this
    # catches missed crossover exits without generating spurious SELLs on stocks we
    # don't own (the engine filters to open_positions before acting).
    if has_position:
        daily_bearish = curr["ema_cmo"] < curr["sma_cmo"] and prev["ema_cmo"] < prev["sma_cmo"]
        weekly_bearish = (
            curr["weekly_ema_cmo"] < curr["weekly_sma_cmo"]
            and prev["weekly_ema_cmo"] < prev["weekly_sma_cmo"]
        )
        if daily_bearish or weekly_bearish:
            trigger = (
                "daily_cmo_bearish_missed_crossover_exit"
                if daily_bearish
                else "weekly_cmo_bearish_missed_crossover_exit"
            )
            return "SELL", float(curr["close"]), trigger

    if buy_cross:
        return "BUY", float(curr["weekly_upper_band"]), "daily_close_crossed_weekly_upper_band"

    return None, None, "no_signal"


def compute_symbol_signal(symbol: str, daily: pd.DataFrame, exchange_tz: str, prev_price: float | None = None, has_position: bool = False, entry_price: float | None = None) -> dict[str, Any]:
    """
    Compute the latest signal for a symbol given its 1d candle DataFrame.
    """

    if daily.empty:
        return _empty(symbol, "no_daily_candles")

    last_ts = daily.index[-1]
    last_price = float(daily["close"].iloc[-1])

    aligned = compute_all_indicators(daily, exchange_tz)
    
    if aligned.empty:
        return {
            "symbol": symbol,
            "asof_ts": last_ts.isoformat(),
            "last_price": last_price,
            "signal": None,
            "signal_price": None,
            "signal_reason": "insufficient_weekly_bars",
        }

    signal, signal_price, reason = _compute_latest_signal(aligned, prev_price_override=prev_price, has_position=has_position, entry_price=entry_price)
    # OOM-FIX-v3: Release merged aligned DF immediately — it's ~180 rows × ~12 cols
    # per symbol, so freeing it before returning saves ~1-2 MB per symbol in the
    # compute loop (500 symbols × ~4 KB = ~2 MB held if not freed proactively).
    del aligned
    return {
        "symbol": symbol,
        "asof_ts": last_ts.isoformat(),
        "last_price": last_price,
        "signal": signal,
        "signal_price": signal_price,
        "signal_reason": reason,
    }
