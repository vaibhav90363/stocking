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


def _get_db_url(db_path: str) -> str:
    """
    Return the Postgres DATABASE_URL from the environment.
    The `db_path` argument is kept for backward compatibility but is ignored —
    the actual data lives in Supabase, not in a local SQLite file.
    """
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set. "
            "Strategy compute requires a live Postgres/Supabase connection."
        )
    return url


def _load_candles(db_url: str, symbol: str, lookback_days: int) -> pd.DataFrame:
    """Load 5m candles for a symbol from the Postgres/Supabase database."""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=lookback_days)
    ).replace(microsecond=0).isoformat()

    sql = """
    SELECT ts, open, high, low, close, volume
    FROM candles_5m
    WHERE symbol = %s AND ts >= %s
    ORDER BY ts
    """
    try:
        conn = psycopg2.connect(
            db_url,
            cursor_factory=RealDictCursor,
            connect_timeout=10,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (symbol, cutoff))
                rows = cur.fetchall()
                if not rows:
                    return pd.DataFrame()
                cols = [desc[0] for desc in cur.description]
        finally:
            conn.close()
    except Exception as exc:
        raise RuntimeError(f"Failed to load candles for {symbol}: {exc}") from exc

    df = pd.DataFrame([dict(r) for r in rows], columns=cols)
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
    from .indicators import calculate_cmo, ema, fractal_chaos_bands, sma

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
    """
    Compute the latest signal for a symbol.

    `db_path` is accepted for interface compatibility but ignored — data is
    always loaded from the Postgres/Supabase DATABASE_URL environment variable.
    """
    try:
        db_url = _get_db_url(db_path)
        df_5m = _load_candles(db_url, symbol, lookback_days)
    except Exception as exc:
        return _empty(symbol, f"db_error:{exc}")

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
