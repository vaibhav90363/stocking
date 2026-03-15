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
    local = daily.copy()
    if local.index.tz is None:
        local.index = local.index.tz_localize("UTC")
    local.index = local.index.tz_convert(exchange_tz)

    weekly = (
        local.resample("W-MON", label="left", closed="left")
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

    return aligned

def _compute_latest_signal(aligned: pd.DataFrame, prev_price_override: float | None = None) -> tuple[str | None, float | None, str]:

    critical = [
        "close",
        "weekly_upper_band",
        "weekly_lower_band",
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

    buy_cross = prev_close <= prev["weekly_upper_band"] and curr["close"] > curr["weekly_upper_band"]
    daily_sell_cross = curr["ema_cmo"] < curr["sma_cmo"] and prev["ema_cmo"] >= prev["sma_cmo"]
    weekly_sell_cross = (
        curr["weekly_ema_cmo"] < curr["weekly_sma_cmo"]
        and prev["weekly_ema_cmo"] >= prev["weekly_sma_cmo"]
    )

    if daily_sell_cross or weekly_sell_cross:
        trigger = "daily_cmo_crossdown" if daily_sell_cross else "weekly_cmo_crossdown"
        return "SELL", float(curr["close"]), trigger

    if buy_cross:
        return "BUY", float(curr["weekly_upper_band"]), "daily_close_crossed_weekly_upper_band"

    return None, None, "no_signal"


def compute_symbol_signal(symbol: str, daily: pd.DataFrame, exchange_tz: str, prev_price: float | None = None) -> dict[str, Any]:
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

    signal, signal_price, reason = _compute_latest_signal(aligned, prev_price_override=prev_price)
    return {
        "symbol": symbol,
        "asof_ts": last_ts.isoformat(),
        "last_price": last_price,
        "signal": signal,
        "signal_price": signal_price,
        "signal_reason": reason,
    }
