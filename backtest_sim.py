#!/usr/bin/env python3
"""
backtest_sim.py  —  Nifty 500 Fractal Momentum Backtest

Strategy:
  - Download DAILY bars for 2 years (Yahoo Finance allows unlimited daily history)
    → used for indicator warmup (fractal bands, CMO on daily/weekly timeframes)
  - Download 5-MINUTE bars for 60 days (Yahoo Finance hard cap)
    → merged on top of daily for recent candle resolution
  - Signals are computed across full history; only events in the last 30 days
    (backtest window) are acted upon in the ledger.

Usage:
    python backtest_sim.py --csv '/path/to/ind_nifty500list (1).csv'
    python backtest_sim.py --csv '...' --skip-download   # reuse cached DB
"""

from __future__ import annotations

import argparse
import asyncio
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np
import yfinance as yf

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from stocking_app.indicators import calculate_cmo, ema, fractal_chaos_bands, sma

# ── Defaults (overridden by CLI) ───────────────────────────────────────────
DAILY_LOOKBACK     = "2y"
INTRADAY_DAYS      = 60
BACKTEST_DAYS      = 30
FETCH_CONCURRENCY  = 16
EXCHANGE_TZ        = "Asia/Kolkata"
TICKER_SUFFIX      = ".NS"
DB_PATH            = ROOT / "data" / "backtest_nifty500.db"
REPORT_PATH        = ROOT / "data" / "backtest_report.txt"
TRADES_CSV_PATH    = ROOT / "data" / "backtest_trades.csv"
CAPITAL_PER_TRADE  = 100_000

# Strategy constants
CMO_PERIOD     = 11
CMO_EMA_PERIOD = 5
CMO_SMA_PERIOD = 11
FRACTAL_LEFT   = 2
FRACTAL_RIGHT  = 2


# ═══════════════════════════════════════════════════════════════════════════
# Stage 1 — Download & Store
# ═══════════════════════════════════════════════════════════════════════════

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS daily_bars (
    symbol   TEXT NOT NULL,
    ts       TEXT NOT NULL,        -- date ISO, e.g. 2024-01-15
    open     REAL NOT NULL,
    high     REAL NOT NULL,
    low      REAL NOT NULL,
    close    REAL NOT NULL,
    volume   REAL,
    PRIMARY KEY (symbol, ts)
);

CREATE TABLE IF NOT EXISTS candles_5m (
    symbol   TEXT NOT NULL,
    ts       TEXT NOT NULL,
    open     REAL NOT NULL,
    high     REAL NOT NULL,
    low      REAL NOT NULL,
    close    REAL NOT NULL,
    volume   REAL,
    PRIMARY KEY (symbol, ts)
);

CREATE INDEX IF NOT EXISTS idx_daily_sym_ts ON daily_bars(symbol, ts);
CREATE INDEX IF NOT EXISTS idx_5m_sym_ts    ON candles_5m(symbol, ts);
"""


def _init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=60, check_same_thread=False)
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


def _normalize_ohlcv(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return data
    cols = {c: str(c).strip().lower() for c in data.columns}
    data = data.rename(columns=cols)
    required = ["open", "high", "low", "close"]
    if not all(c in data.columns for c in required):
        return pd.DataFrame()
    keep = [c for c in ["open", "high", "low", "close", "volume"] if c in data.columns]
    data = data[keep].copy()
    if "volume" not in data.columns:
        data["volume"] = 0.0
    for col in ["open", "high", "low", "close", "volume"]:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")
    data = data.dropna(subset=["open", "high", "low", "close"])
    if not data.index.tzinfo:
        data.index = pd.to_datetime(data.index, utc=True)
    else:
        data.index = data.index.tz_convert("UTC")
    data = data[~data.index.duplicated(keep="last")]
    data.sort_index(inplace=True)
    return data


def _fetch_one_symbol(symbol: str) -> tuple[pd.DataFrame, pd.DataFrame, str | None]:
    """Return (daily_df, intraday_5m_df, error)."""
    try:
        ticker = yf.Ticker(symbol)

        # Daily — 2 years
        daily_raw = ticker.history(period=DAILY_LOOKBACK, interval="1d",
                                   auto_adjust=True, prepost=False)
        daily = _normalize_ohlcv(daily_raw)

        # 5m — 60 days
        intra_raw = ticker.history(period=f"{INTRADAY_DAYS}d", interval="5m",
                                   auto_adjust=True, prepost=False)
        intra = _normalize_ohlcv(intra_raw)

        return daily, intra, None
    except Exception as exc:
        return pd.DataFrame(), pd.DataFrame(), str(exc)


async def _fetch_all_async(
    symbols: list[str],
) -> list[tuple[str, pd.DataFrame, pd.DataFrame, str | None]]:
    sem = asyncio.Semaphore(FETCH_CONCURRENCY)

    async def _wrapped(sym: str):
        async with sem:
            daily, intra, err = await asyncio.to_thread(_fetch_one_symbol, sym)
            return sym, daily, intra, err

    return await asyncio.gather(*[_wrapped(s) for s in symbols])


def _store_daily(conn: sqlite3.Connection, symbol: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = []
    for ts, row in df.iterrows():
        ts_iso = pd.Timestamp(ts).tz_convert("UTC").date().isoformat()
        rows.append((symbol, ts_iso,
                     float(row["open"]), float(row["high"]),
                     float(row["low"]),  float(row["close"]),
                     float(row.get("volume", 0.0))))
    conn.executemany(
        """INSERT INTO daily_bars(symbol,ts,open,high,low,close,volume) VALUES(?,?,?,?,?,?,?)
           ON CONFLICT(symbol,ts) DO UPDATE SET
             open=excluded.open,high=excluded.high,low=excluded.low,
             close=excluded.close,volume=excluded.volume""",
        rows,
    )
    conn.commit()
    return len(rows)


def _store_5m(conn: sqlite3.Connection, symbol: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = []
    for ts, row in df.iterrows():
        ts_iso = pd.Timestamp(ts).tz_convert("UTC").isoformat()
        rows.append((symbol, ts_iso,
                     float(row["open"]), float(row["high"]),
                     float(row["low"]),  float(row["close"]),
                     float(row.get("volume", 0.0))))
    conn.executemany(
        """INSERT INTO candles_5m(symbol,ts,open,high,low,close,volume) VALUES(?,?,?,?,?,?,?)
           ON CONFLICT(symbol,ts) DO UPDATE SET
             open=excluded.open,high=excluded.high,low=excluded.low,
             close=excluded.close,volume=excluded.volume""",
        rows,
    )
    conn.commit()
    return len(rows)


def _read_symbols(csv_path: str) -> list[str]:
    df = pd.read_csv(csv_path)
    col = next((c for c in df.columns if c.strip().lower() == "symbol"), None)
    if col is None:
        raise ValueError(f"No 'Symbol' column found. Columns: {list(df.columns)}")
    return sorted({
        s.strip().upper() + ("" if s.strip().upper().endswith(TICKER_SUFFIX) else TICKER_SUFFIX)
        for s in df[col].dropna() if str(s).strip()
    })


def stage1_download(
    symbols: list[str], conn: sqlite3.Connection
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """
    Returns:
        daily_all  : symbol → daily OHLCV DataFrame (2 years)
        intra_all  : symbol → 5m   OHLCV DataFrame (60 days)
    """
    print(f"\n{'─'*60}")
    print(f"  Stage 1 — Downloading {len(symbols)} symbols")
    print(f"  Daily bars  : {DAILY_LOOKBACK} history  (for indicator warmup)")
    print(f"  5m bars     : last {INTRADAY_DAYS} days (Yahoo Finance cap)")
    print(f"  Backtest    : last {BACKTEST_DAYS} days  (signal window)")
    print(f"  Concurrency : {FETCH_CONCURRENCY}")
    print(f"{'─'*60}\n")

    t0 = time.monotonic()
    results = asyncio.run(_fetch_all_async(symbols))
    elapsed = time.monotonic() - t0

    daily_all:  dict[str, pd.DataFrame] = {}
    intra_all:  dict[str, pd.DataFrame] = {}
    failed = []

    for sym, daily, intra, err in results:
        if err or daily.empty:
            failed.append(sym)
            continue
        daily_all[sym] = daily
        intra_all[sym] = intra
        _store_daily(conn, sym, daily)
        if not intra.empty:
            _store_5m(conn, sym, intra)

    d_bars = sum(len(v) for v in daily_all.values())
    i_bars = sum(len(v) for v in intra_all.values())
    print(f"  ✓ Fetched {len(daily_all)}/{len(symbols)} symbols in {elapsed:.1f}s")
    print(f"  ✓ Daily bars stored  : {d_bars:,}")
    print(f"  ✓ 5m bars stored     : {i_bars:,}")
    if failed:
        prefix = ", ".join(failed[:15])
        print(f"  ✗ Failed ({len(failed)}): {prefix}" + (" …" if len(failed) > 15 else ""))
    print()
    return daily_all, intra_all


def stage1_load_from_db(
    conn: sqlite3.Connection,
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """Load previously cached bars from the backtest DB."""
    print("\n  --skip-download: loading bars from existing DB …")

    daily_all: dict[str, pd.DataFrame] = {}
    for (sym,) in conn.execute("SELECT DISTINCT symbol FROM daily_bars").fetchall():
        df = pd.read_sql_query(
            "SELECT ts,open,high,low,close,volume FROM daily_bars WHERE symbol=? ORDER BY ts",
            conn, params=(sym,),
        )
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        daily_all[sym] = df.set_index("ts")

    intra_all: dict[str, pd.DataFrame] = {}
    for (sym,) in conn.execute("SELECT DISTINCT symbol FROM candles_5m").fetchall():
        df = pd.read_sql_query(
            "SELECT ts,open,high,low,close,volume FROM candles_5m WHERE symbol=? ORDER BY ts",
            conn, params=(sym,),
        )
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        intra_all[sym] = df.set_index("ts")

    print(f"  Loaded {len(daily_all)} symbols (daily) + {len(intra_all)} (5m) from DB\n")
    return daily_all, intra_all


# ═══════════════════════════════════════════════════════════════════════════
# Stage 2 — Signal Computation & Replay
# ═══════════════════════════════════════════════════════════════════════════

def _build_daily_series(
    daily_df: pd.DataFrame,
    intra_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge daily (2y) + 5m (60d) into a single daily OHLCV series.
    The 5m bars are resampled to daily and appended/overwrite the tail
    of the daily history so recent days are filled from higher-resolution data.
    """
    agg = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}

    # Daily baseline (already 1D resolution, UTC index)
    daily = daily_df.copy()
    daily.index = daily.index.tz_convert(EXCHANGE_TZ)
    daily = daily.resample("1D").agg(agg).dropna(subset=["open", "high", "low", "close"])

    if not intra_df.empty:
        local_5m = intra_df.copy()
        local_5m.index = local_5m.index.tz_convert(EXCHANGE_TZ)
        intra_daily = (
            local_5m.resample("1D").agg(agg)
            .dropna(subset=["open", "high", "low", "close"])
        )
        # Overwrite/append with higher-res 5m-resampled daily rows
        daily = daily.combine_first(intra_daily)
        daily.update(intra_daily)          # prefer 5m-derived values for overlap days
        daily.sort_index(inplace=True)

    return daily


def _compute_signals_full(
    daily: pd.DataFrame,
) -> list[tuple[pd.Timestamp, str, float, str]]:
    """
    Run fractal+CMO indicators on the full daily series and return every crossover
    event as (date_IST, signal, price, reason).
    """
    if daily.empty:
        return []

    weekly = (
        daily.resample("W-MON", label="left", closed="left")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna(subset=["open", "high", "low", "close"])
    )
    if weekly.empty:
        return []

    # Weekly indicators
    weekly = fractal_chaos_bands(weekly, FRACTAL_LEFT, FRACTAL_RIGHT)
    weekly["cmo"]     = calculate_cmo(weekly, "close", CMO_PERIOD)
    weekly["ema_cmo"] = ema(weekly["cmo"], CMO_EMA_PERIOD)
    weekly["sma_cmo"] = sma(weekly["cmo"], CMO_SMA_PERIOD)

    # Daily indicators
    daily = daily.copy()
    daily["cmo"]     = calculate_cmo(daily, "close", CMO_PERIOD)
    daily["ema_cmo"] = ema(daily["cmo"], CMO_EMA_PERIOD)
    daily["sma_cmo"] = sma(daily["cmo"], CMO_SMA_PERIOD)

    # Align weekly → daily (forward-fill)
    w_cols = weekly[["upper_band_line", "ema_cmo", "sma_cmo"]].rename(
        columns={"upper_band_line": "w_upper", "ema_cmo": "w_ema_cmo", "sma_cmo": "w_sma_cmo"}
    )
    aligned = daily.join(w_cols, how="left")
    aligned[["w_upper", "w_ema_cmo", "w_sma_cmo"]] = (
        aligned[["w_upper", "w_ema_cmo", "w_sma_cmo"]].ffill()
    )

    critical = ["close", "w_upper", "ema_cmo", "sma_cmo", "w_ema_cmo", "w_sma_cmo"]
    aligned = aligned.dropna(subset=critical)
    if len(aligned) < 2:
        return []

    events: list[tuple[pd.Timestamp, str, float, str]] = []

    for i in range(1, len(aligned)):
        prev = aligned.iloc[i - 1]
        curr = aligned.iloc[i]
        day  = aligned.index[i]

        # SELL — priority
        d_sell = curr["ema_cmo"] < curr["sma_cmo"] and prev["ema_cmo"] >= prev["sma_cmo"]
        w_sell = curr["w_ema_cmo"] < curr["w_sma_cmo"] and prev["w_ema_cmo"] >= prev["w_sma_cmo"]
        if d_sell or w_sell:
            events.append((day, "SELL", float(curr["close"]),
                           "daily_cmo_crossdown" if d_sell else "weekly_cmo_crossdown"))
            continue

        # BUY
        if prev["close"] <= prev["w_upper"] and curr["close"] > curr["w_upper"]:
            events.append((day, "BUY", float(curr["w_upper"]),
                           "daily_close_crossed_weekly_upper_band"))

    return events


@dataclass
class Position:
    symbol:    str
    qty:       int
    avg_price: float
    opened_at: pd.Timestamp


@dataclass
class Trade:
    symbol:  str
    side:    str
    qty:     int
    price:   float
    capital: float
    ts:      pd.Timestamp
    reason:  str
    pnl:     float | None = None


def stage2_replay(
    daily_all: dict[str, pd.DataFrame],
    intra_all: dict[str, pd.DataFrame],
) -> list[Trade]:
    print(f"{'─'*60}")
    print(f"  Stage 2 — Signal computation & ledger replay")
    print(f"{'─'*60}\n")

    if not daily_all:
        print("  No daily bars available.")
        return []

    print(f"  Symbols with daily data : {len(daily_all)}")
    print(f"  Symbols with 5m data    : {len(intra_all)}")
    print(f"  Computing indicators …\n")

    # Backtest window filter
    now_ist       = pd.Timestamp.now(tz=EXCHANGE_TZ).normalize()
    bt_cutoff_ist = now_ist - pd.Timedelta(days=BACKTEST_DAYS)

    all_events: list[tuple[pd.Timestamp, str, str, float, str]] = []
    sym_with_signals = 0

    for i, symbol in enumerate(daily_all, 1):
        daily_series = _build_daily_series(
            daily_all[symbol],
            intra_all.get(symbol, pd.DataFrame()),
        )
        events = _compute_signals_full(daily_series)
        if events:
            sym_with_signals += 1
        for (day, sig, price, reason) in events:
            if day >= bt_cutoff_ist:
                all_events.append((day, symbol, sig, price, reason))
        if i % 50 == 0 or i == len(daily_all):
            print(f"  [{i:3d}/{len(daily_all)}] computed … {sym_with_signals} with signals so far")

    all_events.sort(key=lambda x: x[0])

    print(f"\n  Total events in backtest window : {len(all_events)}")
    print(f"  Symbols with ≥1 signal          : {sym_with_signals}")
    print(f"\n  Simulating ledger …\n")

    positions: dict[str, Position] = {}
    trades:    list[Trade]         = []
    buys = sells = 0

    for (day, symbol, sig, price, reason) in all_events:
        if sig == "SELL" and symbol in positions:
            pos = positions.pop(symbol)
            pnl = (price - pos.avg_price) * pos.qty
            trades.append(Trade(symbol=symbol, side="SELL", qty=pos.qty,
                                price=price, capital=pos.qty * pos.avg_price,
                                ts=day, reason=reason, pnl=pnl))
            sells += 1

        elif sig == "BUY" and symbol not in positions:
            qty = max(1, int(CAPITAL_PER_TRADE / price)) if price > 0 else 1
            positions[symbol] = Position(symbol=symbol, qty=qty,
                                         avg_price=price, opened_at=day)
            trades.append(Trade(symbol=symbol, side="BUY", qty=qty,
                                price=price, capital=qty * price,
                                ts=day, reason=reason))
            buys += 1

    print(f"  BUY legs  : {buys}")
    print(f"  SELL legs : {sells}")

    # Force-close open positions at last known price
    print(f"\n  Force-closing {len(positions)} open position(s) at last price …")
    for symbol, pos in positions.items():
        src = intra_all.get(symbol) if symbol in intra_all else daily_all.get(symbol)
        last_price = float(src["close"].iloc[-1]) if src is not None and not src.empty else pos.avg_price
        last_ts    = now_ist
        pnl        = (last_price - pos.avg_price) * pos.qty
        trades.append(Trade(symbol=symbol, side="SELL_EOB", qty=pos.qty,
                            price=last_price, capital=pos.qty * pos.avg_price,
                            ts=last_ts, reason="end_of_backtest", pnl=pnl))

    print(f"  ✓ Replay done — {len(trades)} trade legs\n")
    return trades


# ═══════════════════════════════════════════════════════════════════════════
# Stage 3 — Report
# ═══════════════════════════════════════════════════════════════════════════

def stage3_report(trades: list[Trade], total_symbols: int) -> None:
    print(f"{'─'*60}")
    print(f"  Stage 3 — Generating Report")
    print(f"{'─'*60}\n")

    if not trades:
        print("  No trades to report.\n")
        return

    rows = [{"symbol": t.symbol, "side": t.side, "qty": t.qty, "price": t.price,
             "capital": t.capital, "ts": t.ts, "reason": t.reason, "pnl": t.pnl}
            for t in trades]
    df = pd.DataFrame(rows)
    df.to_csv(TRADES_CSV_PATH, index=False)
    print(f"  Trade log → {TRADES_CSV_PATH}")

    closed = df[df["pnl"].notna()].copy()
    buys   = df[df["side"] == "BUY"]

    realized    = closed["pnl"].sum()
    n_closed    = len(closed)
    n_wins      = int((closed["pnl"] > 0).sum())
    n_loss      = int((closed["pnl"] <= 0).sum())
    win_rate    = n_wins / n_closed * 100 if n_closed else 0
    avg_win     = closed.loc[closed["pnl"] > 0, "pnl"].mean() if n_wins else 0.0
    avg_loss    = closed.loc[closed["pnl"] <= 0, "pnl"].mean() if n_loss else 0.0

    cum     = closed["pnl"].cumsum()
    max_dd  = (cum - cum.cummax()).min() if not cum.empty else 0.0

    no_sig  = total_symbols - df["symbol"].nunique()

    top_win  = closed.nlargest(10,  "pnl")[["symbol", "ts", "price", "pnl", "reason"]]
    top_loss = closed.nsmallest(10, "pnl")[["symbol", "ts", "price", "pnl", "reason"]]

    lines = [
        "=" * 60,
        "  Nifty 500 Fractal Momentum — Backtest Report",
        f"  Data          : {DAILY_LOOKBACK} daily + {INTRADAY_DAYS}d 5m (warmup)",
        f"  Backtest      : last {BACKTEST_DAYS} days",
        f"  Universe      : {total_symbols} symbols",
        f"  Capital/trade : ₹{CAPITAL_PER_TRADE:,.0f}",
        "=" * 60,
        "",
        "  ── Summary ──────────────────────────────────────────",
        f"  BUY entries             : {len(buys)}",
        f"  Closed trade legs       : {n_closed}",
        f"  Winning trades          : {n_wins}",
        f"  Losing  trades          : {n_loss}",
        f"  Win rate                : {win_rate:.1f}%",
        f"  Realized P&L            : ₹{realized:>12,.2f}",
        f"  Avg winning trade       : ₹{avg_win:>12,.2f}",
        f"  Avg losing  trade       : ₹{avg_loss:>12,.2f}",
        f"  Max drawdown            : ₹{max_dd:>12,.2f}",
        f"  Symbols with no signal  : {no_sig}",
        "",
        "  ── Top 10 Winners ───────────────────────────────────",
    ]
    for _, r in top_win.iterrows():
        ts_str = r["ts"].strftime("%Y-%m-%d") if hasattr(r["ts"], "strftime") else str(r["ts"])[:10]
        lines.append(f"  {r['symbol']:<18}  P&L ₹{r['pnl']:>10,.2f}  @ ₹{r['price']:>8,.2f}  [{ts_str}]")

    lines += ["", "  ── Top 10 Losers ────────────────────────────────────"]
    for _, r in top_loss.iterrows():
        ts_str = r["ts"].strftime("%Y-%m-%d") if hasattr(r["ts"], "strftime") else str(r["ts"])[:10]
        lines.append(f"  {r['symbol']:<18}  P&L ₹{r['pnl']:>10,.2f}  @ ₹{r['price']:>8,.2f}  [{ts_str}]")

    lines += ["", "=" * 60]
    report = "\n".join(lines)
    print(report)

    REPORT_PATH.write_text(report + "\n")
    print(f"\n  Report → {REPORT_PATH}\n")


# ═══════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    global EXCHANGE_TZ, DB_PATH, REPORT_PATH, TRADES_CSV_PATH, CAPITAL_PER_TRADE
    global BACKTEST_DAYS, TICKER_SUFFIX

    parser = argparse.ArgumentParser(description="Fractal momentum backtest — any exchange")
    parser.add_argument("--csv",          required=True,  help="Universe CSV with 'Symbol' column")
    parser.add_argument("--suffix",       default=".NS",  help="Yahoo Finance ticker suffix (default: .NS)")
    parser.add_argument("--exchange-tz",  default="Asia/Kolkata", help="Exchange timezone")
    parser.add_argument("--db",           default=None,   help="SQLite DB path (default: data/backtest_<suffix>.db)")
    parser.add_argument("--report",       default=None,   help="Report .txt output path")
    parser.add_argument("--trades",       default=None,   help="Trades .csv output path")
    parser.add_argument("--capital",      type=float, default=100_000, help="Capital per trade in local currency")
    parser.add_argument("--backtest-days",type=int,   default=30,      help="Signal window in days")
    parser.add_argument("--skip-download", action="store_true",
                        help="Skip download and reuse bars from existing DB")
    args = parser.parse_args()

    # Apply CLI overrides to globals
    TICKER_SUFFIX     = args.suffix
    EXCHANGE_TZ       = args.exchange_tz
    CAPITAL_PER_TRADE = args.capital
    BACKTEST_DAYS     = args.backtest_days
    safe_suffix       = args.suffix.replace(".", "")
    DB_PATH           = Path(args.db)     if args.db     else ROOT / "data" / f"backtest_{safe_suffix}.db"
    REPORT_PATH       = Path(args.report) if args.report else ROOT / "data" / f"backtest_{safe_suffix}_report.txt"
    TRADES_CSV_PATH   = Path(args.trades) if args.trades else ROOT / "data" / f"backtest_{safe_suffix}_trades.csv"

    print("\n" + "═" * 60)
    print(f"  Stocking — Fractal Momentum Backtest")
    print(f"  Exchange : {EXCHANGE_TZ}  |  Suffix: {TICKER_SUFFIX}")
    print(f"  Data: {DAILY_LOOKBACK} daily warmup + {INTRADAY_DAYS}d 5m | Backtest: last {BACKTEST_DAYS}d")
    print("═" * 60)

    symbols = _read_symbols(args.csv)
    print(f"\n  Loaded {len(symbols)} symbols from CSV")

    conn = _init_db(DB_PATH)
    print(f"  Backtest DB : {DB_PATH}")

    if args.skip_download:
        daily_all, intra_all = stage1_load_from_db(conn)
    else:
        daily_all, intra_all = stage1_download(symbols, conn)

    conn.close()

    trades = stage2_replay(daily_all, intra_all)
    stage3_report(trades, total_symbols=len(daily_all))


if __name__ == "__main__":
    main()


# ═══════════════════════════════════════════════════════════════════════════
# Streamlit-callable API  (no globals mutated; progress via callbacks)
# ═══════════════════════════════════════════════════════════════════════════

def run_backtest_for_strategy(
    *,
    universe_csv: str,
    suffix: str = ".NS",
    exchange_tz: str = "Asia/Kolkata",
    daily_lookback: str = "2y",
    intraday_days: int = 60,
    backtest_days: int = 30,
    capital_per_trade: float = 100_000,
    fetch_concurrency: int = 16,
    on_progress=None,   # callable(done: int, total: int, symbol: str, stage: str)
    on_status=None,     # callable(msg: str)
) -> tuple[str, "pd.DataFrame"]:
    """
    Run a full backtest in-process and return (report_text, trades_df).

    Parameters
    ----------
    on_progress : optional callable(done, total, symbol, stage)
        Called after every symbol is processed.
    on_status   : optional callable(msg)
        Called to emit a brief status line.
    """
    import asyncio, time as _time
    from pathlib import Path as _Path

    def _emit_status(msg: str):
        if on_status:
            on_status(msg)

    def _emit_progress(done: int, total: int, symbol: str, stage: str):
        if on_progress:
            on_progress(done, total, symbol, stage)

    # ── 1. Read symbols ──────────────────────────────────────────────────────
    _emit_status(f"📂 Reading universe CSV …")
    sym_df = pd.read_csv(universe_csv)
    col = next((c for c in sym_df.columns if c.strip().lower() == "symbol"), None)
    if col is None:
        raise ValueError(f"No 'Symbol' column in {universe_csv}. Found: {list(sym_df.columns)}")
    raw_symbols = sorted({
        s.strip().upper() + ("" if s.strip().upper().endswith(suffix) else suffix)
        for s in sym_df[col].dropna() if str(s).strip()
    })
    total = len(raw_symbols)
    _emit_status(f"🔢 {total} symbols to download ({suffix}, {exchange_tz})")

    # ── 2. Download with per-symbol progress ──────────────────────────────────
    _emit_status(f"📡 Stage 1 / 3 — Downloading {total} symbols from Yahoo Finance …")

    daily_all: dict[str, pd.DataFrame] = {}
    intra_all: dict[str, pd.DataFrame] = {}
    failed: list[str] = []
    t0 = _time.monotonic()

    # We run one symbol at a time so we can emit progress after each.
    # For speed we still use async batching in chunks of `fetch_concurrency`.
    CHUNK = fetch_concurrency

    async def _fetch_chunk(chunk: list[str]):
        sem = asyncio.Semaphore(CHUNK)
        async def _one(sym):
            async with sem:
                return sym, *await asyncio.to_thread(_fetch_one_symbol, sym)
        return await asyncio.gather(*[_one(s) for s in chunk])

    done_count = 0
    for chunk_start in range(0, total, CHUNK):
        chunk = raw_symbols[chunk_start: chunk_start + CHUNK]
        results = asyncio.run(_fetch_chunk(chunk))
        for sym, daily, intra, err in results:
            done_count += 1
            if err or daily.empty:
                failed.append(sym)
            else:
                daily_all[sym] = daily
                if not intra.empty:
                    intra_all[sym] = intra
            elapsed = _time.monotonic() - t0
            rate = done_count / elapsed if elapsed > 0 else 0
            remaining = int((total - done_count) / rate) if rate > 0 else 0
            _emit_progress(done_count, total, sym,
                           f"⬇ Download — {done_count}/{total} • "
                           f"{elapsed:.0f}s elapsed • "
                           f"~{remaining}s left")

    elapsed_dl = _time.monotonic() - t0
    _emit_status(
        f"✅ Download done in {elapsed_dl:.1f}s — "
        f"{len(daily_all)} ok, {len(failed)} failed"
    )

    # ── 3. Signal computation & replay ────────────────────────────────────────
    _emit_status("⚙️  Stage 2 / 3 — Computing signals & replaying ledger …")

    # pd is already imported at module level
    now_ist = pd.Timestamp.now(tz=exchange_tz).normalize()
    bt_cutoff = now_ist - pd.Timedelta(days=backtest_days)

    all_events: list[tuple] = []
    sym_list = list(daily_all.keys())
    t1 = _time.monotonic()

    for i, symbol in enumerate(sym_list, 1):
        # Build daily series with the correct exchange timezone (do NOT use _build_daily_series
        # which hard-codes the module-level EXCHANGE_TZ global = "Asia/Kolkata")
        daily_loc = daily_all[symbol].copy()
        daily_loc.index = daily_loc.index.tz_convert(exchange_tz)
        agg = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
        daily_loc = daily_loc.resample("1D").agg(agg).dropna(subset=["open","high","low","close"])
        if symbol in intra_all:
            intra_loc = intra_all[symbol].copy()
            intra_loc.index = intra_loc.index.tz_convert(exchange_tz)
            intra_d = intra_loc.resample("1D").agg(agg).dropna(subset=["open","high","low","close"])
            daily_loc = daily_loc.combine_first(intra_d)
            daily_loc.update(intra_d)
            daily_loc.sort_index(inplace=True)

        all_sym_events = _compute_signals_full(daily_loc)   # full history
        n_in_window = 0
        for (day, sig, price, reason) in all_sym_events:
            if day >= bt_cutoff:
                all_events.append((day, symbol, sig, price, reason))
                n_in_window += 1

        elapsed2 = _time.monotonic() - t1
        rate2 = i / elapsed2 if elapsed2 > 0 else 0
        rem2 = int((len(sym_list) - i) / rate2) if rate2 > 0 else 0
        _emit_progress(i, len(sym_list), symbol,
                       f"⚙️  Compute — {i}/{len(sym_list)} • "
                       f"{elapsed2:.0f}s elapsed • ~{rem2}s left • "
                       f"signals (all-time): {len(all_sym_events)}  (in window): {n_in_window}")

    all_events.sort(key=lambda x: x[0])

    # Diagnostic summary
    syms_with_events = len({e[1] for e in all_events})
    _emit_status(
        f"✅ Signals computed — {len(all_events)} events in last {backtest_days}d window "
        f"across {syms_with_events}/{len(sym_list)} symbols"
    )
    if all_events:
        sample = all_events[:3]
        _emit_status(
            f"📋 Sample events: " +
            " | ".join(f"{e[2]} {e[1]} @ {e[3]:.2f} [{str(e[0])[:10]}]" for e in sample)
        )
    else:
        _emit_status(
            f"⚠ Zero signals in the {backtest_days}-day window. "
            f"Downloaded {len(daily_all)} symbols ({len(failed)} failed). "
            f"Try increasing backtest_days or check that the suffix '{suffix}' is correct for this universe."
        )

    # ── 4. Ledger replay ──────────────────────────────────────────────────────
    _emit_status("📒 Stage 3 / 3 — Replaying ledger & generating report …")
    positions: dict[str, Position] = {}
    trades: list[Trade] = []
    for (day, symbol, sig, price, reason) in all_events:
        if sig == "SELL" and symbol in positions:
            pos = positions.pop(symbol)
            pnl = (price - pos.avg_price) * pos.qty
            trades.append(Trade(symbol=symbol, side="SELL", qty=pos.qty,
                                price=price, capital=pos.qty * pos.avg_price,
                                ts=day, reason=reason, pnl=pnl))
        elif sig == "BUY" and symbol not in positions:
            qty = max(1, int(capital_per_trade / price)) if price > 0 else 1
            positions[symbol] = Position(symbol=symbol, qty=qty,
                                         avg_price=price, opened_at=day)
            trades.append(Trade(symbol=symbol, side="BUY", qty=qty,
                                price=price, capital=qty * price,
                                ts=day, reason=reason))

    # Force-close open positions at last known price
    # FIX: use explicit 'in' check instead of 'or' to avoid DataFrame truth-value error
    for symbol, pos in positions.items():
        src = intra_all[symbol] if symbol in intra_all else daily_all.get(symbol)
        last_price = float(src["close"].iloc[-1]) if src is not None and not src.empty else pos.avg_price
        pnl = (last_price - pos.avg_price) * pos.qty
        trades.append(Trade(symbol=symbol, side="SELL_EOB", qty=pos.qty,
                            price=last_price, capital=pos.qty * pos.avg_price,
                            ts=now_ist, reason="end_of_backtest", pnl=pnl))

    # ── 5. Build report ───────────────────────────────────────────────────────
    rows = [{"symbol": t.symbol, "side": t.side, "qty": t.qty,
             "price": t.price, "capital": t.capital,
             "ts": str(t.ts)[:19], "reason": t.reason, "pnl": t.pnl}
            for t in trades]
    trades_df = pd.DataFrame(rows)

    if trades_df.empty:
        report_text = "No trades generated in the backtest window."
    else:
        closed = trades_df[trades_df["pnl"].notna()].copy()
        buys_df = trades_df[trades_df["side"] == "BUY"]
        realized = closed["pnl"].sum()
        n_closed = len(closed)
        n_wins = int((closed["pnl"] > 0).sum())
        n_loss = int((closed["pnl"] <= 0).sum())
        win_rate = n_wins / n_closed * 100 if n_closed else 0.0
        avg_win  = closed.loc[closed["pnl"] > 0, "pnl"].mean() if n_wins else 0.0
        avg_loss = closed.loc[closed["pnl"] <= 0, "pnl"].mean() if n_loss else 0.0
        cum = closed["pnl"].cumsum()
        max_dd = (cum - cum.cummax()).min() if not cum.empty else 0.0

        top_win  = closed.nlargest(10,  "pnl")[["symbol","ts","price","pnl","reason"]]
        top_loss = closed.nsmallest(10, "pnl")[["symbol","ts","price","pnl","reason"]]

        lines = [
            "=" * 60,
            f"  Fractal Momentum Backtest — {suffix} / {exchange_tz}",
            f"  Data   : {daily_lookback} daily + {intraday_days}d 5m (warmup)",
            f"  Window : last {backtest_days} days",
            f"  Universe: {total} symbols  |  Capital/trade: {capital_per_trade:,.0f}",
            "=" * 60, "",
            "  ── Summary ──────────────────────────────────────────",
            f"  BUY entries           : {len(buys_df)}",
            f"  Closed trade legs     : {n_closed}",
            f"  Winning trades        : {n_wins}",
            f"  Losing  trades        : {n_loss}",
            f"  Win rate              : {win_rate:.1f}%",
            f"  Realized P&L          : {realized:>12,.2f}",
            f"  Avg winning trade     : {avg_win:>12,.2f}",
            f"  Avg losing  trade     : {avg_loss:>12,.2f}",
            f"  Max drawdown          : {max_dd:>12,.2f}",
            f"  Failed downloads      : {len(failed)}",
            "", "  ── Top 10 Winners ───────────────────────────────────",
        ]
        for _, r in top_win.iterrows():
            lines.append(f"  {r['symbol']:<18}  P&L {r['pnl']:>10,.2f}  @ {r['price']:>8,.2f}  [{str(r['ts'])[:10]}]")
        lines += ["", "  ── Top 10 Losers ────────────────────────────────────"]
        for _, r in top_loss.iterrows():
            lines.append(f"  {r['symbol']:<18}  P&L {r['pnl']:>10,.2f}  @ {r['price']:>8,.2f}  [{str(r['ts'])[:10]}]")
        lines += ["", "=" * 60]
        report_text = "\n".join(lines)

    total_elapsed = _time.monotonic() - t0
    _emit_status(f"🏁 Backtest complete in {total_elapsed:.1f}s — {len(trades)} trade legs")
    return report_text, trades_df

