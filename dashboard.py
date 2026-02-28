from __future__ import annotations

"""
dashboard.py — Stocking Strategy Dashboard
Full tabbed UX with system visibility, portfolio analytics, universe sync status,
live log tail, and real-time engine monitoring.
"""

import os
import signal
import subprocess
import time
from pathlib import Path

import pandas as pd
import streamlit as st

from stocking_app.config import load_config
from stocking_app.db import TradingRepository

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Stocking Dashboard",
    layout="wide",
    page_icon="📈",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
[data-testid="stMetricValue"] { font-size: 1.4rem; font-weight: 700; }
[data-testid="stMetricLabel"] { font-size: 0.75rem; color: #888; }
.status-running { color: #4ade80; font-weight: 700; }
.status-paused  { color: #facc15; font-weight: 700; }
.status-error   { color: #f87171; font-weight: 700; }
div[data-testid="stDataFrameContainer"] { border-radius: 8px; }
.section-header { font-size: 1.1rem; font-weight: 600; color: #e2e8f0;
                   border-left: 3px solid #6366f1; padding-left: 10px;
                   margin: 1rem 0 0.5rem 0; }
</style>
""", unsafe_allow_html=True)

# ── Load config & repo ─────────────────────────────────────────────────────────
cfg  = load_config()

repo = TradingRepository(cfg.database_url or cfg.db_path)
repo.init_db()
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--strategy-dir", type=str, required=False)
args, unknown = parser.parse_known_args()

if args.strategy_dir:
    strategy_dir = Path(args.strategy_dir)
else:
    # fallback if not provided
    strategy_dir = Path.cwd()

db_dir   = strategy_dir / "data"
log_file = db_dir / "logs" / "engine.log"

# ── Sidebar — System Parameters & Controls ─────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ System")

    enabled   = repo.get_engine_enabled()
    hb        = repo.get_engine_heartbeat() or {}
    state     = hb.get("state", "unknown")
    last_run  = (hb.get("last_run") or "—")[:19]
    last_err  = hb.get("error")

    dot = "🟢" if state == "running" else ("🟡" if state == "paused" else "🔴")
    st.markdown(f"**Engine State:** {dot} `{state.upper()}`")
    st.caption(f"Last run: `{last_run}`")
    if last_err:
        st.error(f"⚠ {last_err}")

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("▶ Start Engine in Cloud", use_container_width=True, type="primary", disabled=(state == "running")):
            repo.set_engine_enabled(True)
            st.toast("Start signal sent to database. Engine will resume shortly.")
            time.sleep(1.5)
            st.rerun()

    with col_b:
        if st.button("⏹ Stop Engine in Cloud", use_container_width=True, disabled=(state != "running")):
            repo.set_engine_enabled(False)
            st.toast("Stop signal sent to database. Engine will pause shortly.")
            time.sleep(1.5)
            st.rerun()

    st.divider()
    st.markdown("## 📐 Parameters")
    params = {
        "DB Path":       str(cfg.db_path),
        "Ticker Suffix": cfg.ticker_suffix,
        "Cycle (s)":     cfg.cycle_seconds,
        "Fetch Lookback":f"{cfg.fetch_lookback_days}d",
        "Compute Window":f"{cfg.compute_lookback_days}d",
        "Fetch Workers": cfg.max_fetch_concurrency,
        "Compute Workers": cfg.compute_workers,
        "Order Qty":     cfg.order_qty,
    }
    for k, v in params.items():
        st.markdown(f"**{k}:** `{v}`")

    st.divider()
    auto = st.checkbox("⏱ Auto-refresh (30s)", value=False)
    if st.button("🔄 Refresh now", use_container_width=True):
        st.rerun()

# ── Header KPI strip ───────────────────────────────────────────────────────────
st.markdown("# 📈 Stocking Strategy Dashboard")
st.caption(
    f"`{cfg.db_path}`  |  suffix `{cfg.ticker_suffix}`  |  "
    f"cycle `{cfg.cycle_seconds}s`  |  "
    f"log `{log_file}`"
)

# Pull top-level numbers
uni_summary  = repo.get_universe_summary()
open_pos_map = repo.get_open_positions()
n_open       = len(open_pos_map)

pnl_snap = repo.read_df(
    "SELECT realized_pnl, unrealized_pnl, total_pnl FROM pnl_snapshots ORDER BY ts DESC LIMIT 1"
)
realized   = float(pnl_snap["realized_pnl"].iloc[0])   if not pnl_snap.empty else 0.0
unrealized = float(pnl_snap["unrealized_pnl"].iloc[0]) if not pnl_snap.empty else 0.0
total_pnl  = float(pnl_snap["total_pnl"].iloc[0])      if not pnl_snap.empty else 0.0

latest_run = repo.read_df("SELECT * FROM run_metrics ORDER BY id DESC LIMIT 1")
last_cycle_status = str(latest_run["status"].iloc[0]) if not latest_run.empty else "—"
last_duration     = float(latest_run["duration_seconds"].iloc[0]) if not latest_run.empty else 0.0
fetched_last      = int(latest_run["symbols_fetched"].iloc[0]) if not latest_run.empty else 0

k1, k2, k3, k4, k5, k6, k7 = st.columns(7)
k1.metric("Engine",         f"{dot} {state.upper()}")
k2.metric("Universe",       uni_summary["active"])
k3.metric("Open Positions", n_open)
k4.metric("Realized P&L",   f"{realized:,.2f}")
k5.metric("Unrealized P&L", f"{unrealized:,.2f}", delta=f"{unrealized:+,.0f}")
k6.metric("Total P&L",      f"{total_pnl:,.2f}",  delta=f"{total_pnl:+,.0f}")
k7.metric("Last Cycle",     last_cycle_status, delta=f"{last_duration:.1f}s")

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab_overview, tab_portfolio, tab_universe, tab_signals, tab_ledger, tab_system, tab_log = st.tabs([
    "📊 Overview",
    "💰 Portfolio",
    "🌐 Universe",
    "🚦 Signals",
    "📒 Ledger",
    "⚙️ System",
    "📋 Live Log",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
with tab_overview:
    st.markdown('<p class="section-header">Last Cycle Performance</p>', unsafe_allow_html=True)

    if not latest_run.empty:
        row = latest_run.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Status",           str(row["status"]))
        c2.metric("Total Duration",   f"{float(row['duration_seconds'] or 0):.1f}s")
        c3.metric("Fetch Time",       f"{float(row['fetch_seconds'] or 0):.1f}s")
        c4.metric("Compute Time",     f"{float(row['compute_seconds'] or 0):.1f}s")
        c5.metric("Persist Time",     f"{float(row['persist_seconds'] or 0):.2f}s")
        d1, d2, d3 = st.columns(3)
        d1.metric("Symbols Total",    int(row.get("symbols_total", 0) or 0))
        d2.metric("Symbols Fetched",  int(row.get("symbols_fetched", 0) or 0))
        d3.metric("Symbols Computed", int(row.get("symbols_computed", 0) or 0))
        if row.get("error"):
            st.error(f"⚠️ Last cycle error: {row['error']}")
    else:
        st.info("No cycle data yet — engine hasn't run a full cycle.")

    st.markdown('<p class="section-header">Cycle History (last 50)</p>', unsafe_allow_html=True)
    history = repo.read_df(
        """
        SELECT id AS cycle,
               SUBSTR(run_started_at,1,19) AS started,
               status,
               symbols_total AS total,
               symbols_fetched AS fetched,
               symbols_computed AS computed,
               ROUND(fetch_seconds,1)   AS fetch_s,
               ROUND(compute_seconds,1) AS compute_s,
               ROUND(duration_seconds,1) AS total_s,
               error
        FROM run_metrics
        ORDER BY id DESC
        LIMIT 50
        """
    )
    if history.empty:
        st.info("No cycle history yet.")
    else:
        def _color_status(val):
            if val == "OK":     return "background-color:#14532d; color:#4ade80"
            if val == "FAILED": return "background-color:#450a0a; color:#f87171"
            return ""
        st.dataframe(
            history.style.applymap(_color_status, subset=["status"]),
            use_container_width=True, hide_index=True,
        )

    st.markdown('<p class="section-header">P&L Over Time</p>', unsafe_allow_html=True)
    pnl_hist = repo.read_df(
        """
        SELECT ts, realized_pnl, unrealized_pnl, total_pnl
        FROM pnl_snapshots ORDER BY ts ASC LIMIT 500
        """
    )
    if not pnl_hist.empty:
        pnl_hist["ts"] = pd.to_datetime(pnl_hist["ts"], utc=True)
        st.line_chart(pnl_hist.set_index("ts")[["realized_pnl", "unrealized_pnl", "total_pnl"]])
    else:
        st.info("No P&L history yet.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — PORTFOLIO
# ══════════════════════════════════════════════════════════════════════════════
with tab_portfolio:
    st.markdown('<p class="section-header">Open Positions</p>', unsafe_allow_html=True)

    positions = repo.read_df(
        """
        SELECT symbol,
               qty,
               ROUND(avg_price,4)   AS avg_price,
               ROUND(last_price,4)  AS last_price,
               ROUND((last_price - avg_price) * qty, 2) AS unrealized_pnl,
               ROUND((last_price - avg_price) / avg_price * 100, 2) AS pct_chg,
               opened_at,
               last_updated_at
        FROM positions_ledger
        ORDER BY unrealized_pnl DESC
        """
    )

    if positions.empty:
        st.info("No open positions. Engine is watching for BUY signals.")
    else:
        total_cost   = (positions["avg_price"] * positions["qty"]).sum()
        total_mkt    = (positions["last_price"] * positions["qty"]).sum()
        total_unreal = positions["unrealized_pnl"].sum()
        p1, p2, p3, p4 = st.columns(4)
        p1.metric("Positions",     len(positions))
        p2.metric("Cost Basis",    f"{total_cost:,.2f}")
        p3.metric("Market Value",  f"{total_mkt:,.2f}")
        p4.metric("Unrealized P&L",f"{total_unreal:,.2f}", delta=f"{total_unreal:+,.0f}")

        def _color_pnl(val):
            try:
                v = float(val)
                return "color:#4ade80" if v > 0 else ("color:#f87171" if v < 0 else "")
            except Exception:
                return ""

        st.dataframe(
            positions.style
                .applymap(_color_pnl, subset=["unrealized_pnl", "pct_chg"])
                .format({"avg_price": "{:.4f}", "last_price": "{:.4f}",
                         "unrealized_pnl": "{:+,.2f}", "pct_chg": "{:+.2f}%"}),
            use_container_width=True, hide_index=True,
        )

    st.markdown('<p class="section-header">P&L Summary</p>', unsafe_allow_html=True)
    p_a, p_b, p_c = st.columns(3)
    p_a.metric("Realized P&L",   f"{realized:,.2f}",   delta=f"{realized:+,.0f}")
    p_b.metric("Unrealized P&L", f"{unrealized:,.2f}",  delta=f"{unrealized:+,.0f}")
    p_c.metric("Total P&L",      f"{total_pnl:,.2f}",   delta=f"{total_pnl:+,.0f}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — UNIVERSE & SYNC STATUS
# ══════════════════════════════════════════════════════════════════════════════
with tab_universe:
    st.markdown('<p class="section-header">Universe Overview</p>', unsafe_allow_html=True)
    u1, u2, u3 = st.columns(3)
    u1.metric("Total Symbols",    uni_summary["total"])
    u2.metric("Active (monitored)", uni_summary["active"])
    u3.metric("Selected",          uni_summary["selected"])

    st.markdown('<p class="section-header">Symbol Sync Status</p>', unsafe_allow_html=True)
    st.caption("Shows when data was last fetched (candles) and last computed (signals) for each symbol.")

    sync_df = repo.read_df(
        """
        SELECT
            u.symbol,
            u.is_active,
            SUBSTR(ss.last_candle_ts, 1, 19)  AS last_fetched,
            SUBSTR(ss.last_compute_ts, 1, 19) AS last_computed,
            CASE
              WHEN ss.last_candle_ts IS NULL THEN 'Never fetched'
              WHEN ss.last_compute_ts IS NULL THEN 'Never computed'
              WHEN ss.last_candle_ts > ss.last_compute_ts THEN 'Compute pending'
              ELSE 'Up to date'
            END AS sync_status,
            SUBSTR(ss.updated_at, 1, 19) AS state_updated
        FROM universe u
        LEFT JOIN symbol_state ss ON ss.symbol = u.symbol
        ORDER BY u.is_active DESC, sync_status, u.symbol
        """
    )

    if sync_df.empty:
        st.info("No symbols in universe yet.")
    else:
        # Filter controls
        search = st.text_input("🔍 Filter by symbol", "")
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            status_filter = st.multiselect(
                "Sync status",
                options=sync_df["sync_status"].unique().tolist(),
                default=sync_df["sync_status"].unique().tolist(),
            )
        with col_f2:
            active_only = st.checkbox("Active only", value=True)

        filtered = sync_df.copy()
        if search:
            filtered = filtered[filtered["symbol"].str.contains(search.upper())]
        if active_only:
            filtered = filtered[filtered["is_active"] == 1]
        if status_filter:
            filtered = filtered[filtered["sync_status"].isin(status_filter)]

        def _color_sync(val):
            m = {"Up to date": "color:#4ade80",
                 "Compute pending": "color:#facc15",
                 "Never computed": "color:#fb923c",
                 "Never fetched": "color:#f87171"}
            return m.get(val, "")

        st.dataframe(
            filtered.style.applymap(_color_sync, subset=["sync_status"]),
            use_container_width=True, hide_index=True,
        )
        st.caption(f"Showing {len(filtered)} of {len(sync_df)} symbols")

        # Sync health summary
        counts = sync_df["sync_status"].value_counts()
        st.markdown('<p class="section-header">Sync Health</p>', unsafe_allow_html=True)
        health_cols = st.columns(len(counts))
        for col, (status, count) in zip(health_cols, counts.items()):
            col.metric(status, count)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — SIGNALS
# ══════════════════════════════════════════════════════════════════════════════
with tab_signals:
    st.markdown('<p class="section-header">Signal Summary</p>', unsafe_allow_html=True)

    sig_summary = repo.read_df(
        """
        SELECT
            signal_type,
            COUNT(*) AS total,
            SUM(acted) AS acted,
            COUNT(*) - SUM(acted) AS not_acted,
            ROUND(AVG(price),4) AS avg_price,
            SUBSTR(MAX(ts),1,19) AS latest
        FROM signals
        GROUP BY signal_type
        """
    )
    if not sig_summary.empty:
        st.dataframe(sig_summary, use_container_width=True, hide_index=True)

    st.markdown('<p class="section-header">Recent Signals</p>', unsafe_allow_html=True)

    sig_filter = st.multiselect("Filter by type", ["BUY", "SELL"], default=["BUY", "SELL"])
    acted_filter = st.checkbox("Acted only", value=False)

    signals = repo.read_df(
        """
        SELECT id, symbol, SUBSTR(ts,1,19) AS ts, signal_type, ROUND(price,4) AS price, reason, acted
        FROM signals
        ORDER BY id DESC
        LIMIT 500
        """
    )

    if not signals.empty:
        if sig_filter:
            signals = signals[signals["signal_type"].isin(sig_filter)]
        if acted_filter:
            signals = signals[signals["acted"] == 1]

        def _sig_type_color(val):
            if val == "BUY":  return "background-color:#14532d; color:#4ade80; font-weight:700"
            if val == "SELL": return "background-color:#450a0a; color:#f87171; font-weight:700"
            return ""
        def _acted_color(val):
            return "color:#4ade80" if val == 1 else "color:#64748b"

        st.dataframe(
            signals.style
                .applymap(_sig_type_color, subset=["signal_type"])
                .applymap(_acted_color, subset=["acted"]),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("No signals yet — waiting for first compute cycle.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — LEDGER
# ══════════════════════════════════════════════════════════════════════════════
with tab_ledger:
    st.markdown('<p class="section-header">Trade Activity Log</p>', unsafe_allow_html=True)

    trades = repo.read_df(
        """
        SELECT id,
               symbol,
               side,
               qty,
               ROUND(price,4) AS price,
               ROUND(pnl,2)   AS pnl,
               SUBSTR(ts,1,19) AS ts,
               reason
        FROM trade_activity_log
        ORDER BY id DESC
        LIMIT 500
        """
    )

    if trades.empty:
        st.info("No trades executed yet.")
    else:
        # Summary stats
        buys  = trades[trades["side"] == "BUY"]
        sells = trades[trades["side"].isin(["SELL", "SELL_EOB"])]
        pnl_trades = trades[trades["pnl"].notna()]
        wins  = pnl_trades[pnl_trades["pnl"] > 0]
        loss  = pnl_trades[pnl_trades["pnl"] <= 0]

        t1, t2, t3, t4, t5 = st.columns(5)
        t1.metric("Total Trades",   len(trades))
        t2.metric("Buys",           len(buys))
        t3.metric("Sells/Closed",   len(sells))
        t4.metric("Win Rate",       f"{len(wins)/len(pnl_trades)*100:.1f}%" if len(pnl_trades) else "—")
        t5.metric("Realized P&L",   f"{pnl_trades['pnl'].sum():,.2f}" if not pnl_trades.empty else "—")

        def _side_color(val):
            if val == "BUY":      return "background-color:#14532d; color:#4ade80; font-weight:700"
            if val == "SELL":     return "background-color:#450a0a; color:#f87171; font-weight:700"
            if val == "SELL_EOB": return "background-color:#312e81; color:#a5b4fc; font-weight:700"
            return ""
        def _pnl_color(val):
            try:
                v = float(val)
                return "color:#4ade80" if v > 0 else ("color:#f87171" if v < 0 else "")
            except Exception:
                return ""

        st.dataframe(
            trades.style
                .applymap(_side_color, subset=["side"])
                .applymap(_pnl_color, subset=["pnl"]),
            use_container_width=True, hide_index=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — SYSTEM
# ══════════════════════════════════════════════════════════════════════════════
with tab_system:
    st.markdown('<p class="section-header">Engine Heartbeat</p>', unsafe_allow_html=True)
    hb_display = {k: v for k, v in hb.items() if v is not None}
    if hb_display:
        hb_df = pd.DataFrame(list(hb_display.items()), columns=["Parameter", "Value"])
        st.dataframe(hb_df, use_container_width=True, hide_index=True)
    else:
        st.info("No heartbeat data yet.")

    st.markdown('<p class="section-header">Full Strategy Configuration</p>', unsafe_allow_html=True)

    strategy_params = {
        "Exchange Timezone": "Asia/Kolkata (default) / per .env",
        "Ticker Suffix": cfg.ticker_suffix,
        "Fetch Lookback": f"{cfg.fetch_lookback_days} days (5m bars)",
        "Compute Lookback": f"{cfg.compute_lookback_days} days (daily warmup)",
        "Fractal Left Window": "2 bars",
        "Fractal Right Window": "2 bars",
        "CMO Period": "11",
        "CMO EMA Period": "5",
        "CMO SMA Period": "11",
        "BUY Signal": "Daily close > Weekly Fractal Upper Band",
        "SELL Signal": "Daily CMO EMA crosses below SMA  OR  Weekly CMO EMA crosses below SMA",
        "Order Qty": cfg.order_qty,
        "Cycle Interval": f"{cfg.cycle_seconds}s",
        "Fetch Concurrency": cfg.max_fetch_concurrency,
        "Compute Workers": cfg.compute_workers,
    }
    cfg_df = pd.DataFrame(list(strategy_params.items()), columns=["Parameter", "Value"])
    st.dataframe(cfg_df, use_container_width=True, hide_index=True)

    st.markdown('<p class="section-header">Data Files</p>', unsafe_allow_html=True)
    files = {
        "Live DB": str(cfg.db_path),
        "Engine Log": str(log_file),
        "Backtest DB (.L)": str(db_dir / "backtest_L.db"),
        "Backtest Report": str(db_dir / "backtest_L_report.txt"),
        "Backtest Trades": str(db_dir / "backtest_L_trades.csv"),
    }
    files_df = pd.DataFrame(
        [{"File": k, "Path": v, "Exists": "✅" if Path(v).exists() else "❌"}
         for k, v in files.items()]
    )
    st.dataframe(files_df, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 7 — LIVE LOG
# ══════════════════════════════════════════════════════════════════════════════
with tab_log:
    st.markdown('<p class="section-header">Live Engine Log</p>', unsafe_allow_html=True)

    n_lines = st.slider("Lines to show", min_value=20, max_value=200, value=80, step=20)

    if log_file.exists():
        lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
        recent = "\n".join(reversed(lines[-n_lines:]))
        st.code(recent, language=None)
        st.caption(
            f"📄 `{log_file}`  |  "
            f"{len(lines):,} total lines  |  "
            f"Size: {log_file.stat().st_size / 1024:.1f} KB"
        )
        st.info("💡 Showing most-recent lines first. Tail live in terminal: "
                f"`tail -f {log_file}`")
    else:
        st.warning(f"Log not created yet at `{log_file}`. Engine needs to run at least one cycle.")

# ── Footer / auto-refresh ──────────────────────────────────────────────────────
st.divider()
st.caption(
    f"DB: `{cfg.db_path}`  |  Suffix: `{cfg.ticker_suffix}`  |  "
    f"Cycle: `{cfg.cycle_seconds}s`  |  "
    f"Open positions: `{n_open}`  |  Universe: `{uni_summary['active']} active`"
)

if auto:
    st.caption("⏱ Auto-refreshing every 30 seconds…")
    time.sleep(30)
    st.rerun()

repo.close()
