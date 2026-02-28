from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from .indicators import calculate_cmo, ema, fractal_chaos_bands, sma


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


def _load_candles(conn: sqlite3.Connection, symbol: str, lookback_days: int) -> pd.DataFrame:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).replace(microsecond=0).isoformat()
    q = """
    SELECT ts, open, high, low, close, volume
    FROM candles_5m
    WHERE symbol = ? AND ts >= ?
    ORDER BY ts
    """
    df = pd.read_sql_query(q, conn, params=(symbol, cutoff))
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.set_index("ts")
    return df


def _to_daily_weekly(df_5m: pd.DataFrame, exchange_tz: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    local = df_5m.copy()
    local.index = local.index.tz_convert(exchange_tz)

    daily = (
        local.resample("1D")
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

    weekly = (
        daily.resample("W-MON", label="left", closed="left")
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

    return daily, weekly


def _compute_latest_signal(daily: pd.DataFrame, weekly: pd.DataFrame) -> tuple[str | None, float | None, str]:
    weekly = fractal_chaos_bands(weekly, FRACTAL_LEFT_WINDOW, FRACTAL_RIGHT_WINDOW)

    daily["cmo"] = calculate_cmo(daily, "close", CMO_PERIOD)
    weekly["cmo"] = calculate_cmo(weekly, "close", CMO_PERIOD)

    daily["ema_cmo"] = ema(daily["cmo"], CMO_EMA_PERIOD)
    daily["sma_cmo"] = sma(daily["cmo"], CMO_SMA_PERIOD)

    weekly["ema_cmo"] = ema(weekly["cmo"], CMO_EMA_PERIOD)
    weekly["sma_cmo"] = sma(weekly["cmo"], CMO_SMA_PERIOD)

    weekly_aliased = weekly[["upper_band_line", "ema_cmo", "sma_cmo"]].rename(
        columns={
            "upper_band_line": "weekly_upper_band",
            "ema_cmo": "weekly_ema_cmo",
            "sma_cmo": "weekly_sma_cmo",
        }
    )

    aligned = daily.join(weekly_aliased, how="left")
    aligned[["weekly_upper_band", "weekly_ema_cmo", "weekly_sma_cmo"]] = aligned[
        ["weekly_upper_band", "weekly_ema_cmo", "weekly_sma_cmo"]
    ].ffill()

    critical = [
        "close",
        "weekly_upper_band",
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

    buy_cross = prev["close"] <= prev["weekly_upper_band"] and curr["close"] > curr["weekly_upper_band"]
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


def compute_symbol_signal(db_path: str, symbol: str, lookback_days: int, exchange_tz: str) -> dict[str, Any]:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        df_5m = _load_candles(conn, symbol, lookback_days)
    finally:
        conn.close()

    if df_5m.empty:
        return _empty(symbol, "no_5m_candles")

    last_ts = df_5m.index[-1]
    last_price = float(df_5m["close"].iloc[-1])

    daily, weekly = _to_daily_weekly(df_5m, exchange_tz=exchange_tz)
    if daily.empty or weekly.empty:
        return {
            "symbol": symbol,
            "asof_ts": last_ts.isoformat(),
            "last_price": last_price,
            "signal": None,
            "signal_price": None,
            "signal_reason": "insufficient_daily_or_weekly_bars",
        }

    signal, signal_price, reason = _compute_latest_signal(daily, weekly)
    return {
        "symbol": symbol,
        "asof_ts": last_ts.isoformat(),
        "last_price": last_price,
        "signal": signal,
        "signal_price": signal_price,
        "signal_reason": reason,
    }
