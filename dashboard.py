from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from stocking_app.config import load_config
from stocking_app.db import TradingRepository

# ── Page Config ───────────────────────────────────────────────────────────────
if not st.session_state.get("_hub_hosted", False):
    st.set_page_config(
        page_title="Stocking Dashboard",
        layout="wide",
        page_icon="📈",
        initial_sidebar_state="expanded",
    )

# ── Load config & repo ─────────────────────────────────────────────────────────
import sys
import argparse
from stocking_app.strategy_loader import load_strategy
def run_dashboard(strategy_name_or_path=None):
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
    global_cfg = load_config()
    database_url = global_cfg.database_url
    if strategy_name_or_path:
        p = Path(strategy_name_or_path)
        if not p.is_absolute() and not p.exists():
            root = Path(__file__).resolve().parent
            strategy_dir = root / 'strategies' / p.name
        else:
            strategy_dir = p
    else:
        strategy_dir = Path.cwd()

    if not strategy_dir.exists() or not (strategy_dir / "strategy.yaml").exists():
        st.error(f"Strategy config not found in {strategy_dir}. Please access via the Hub or pass a valid `--strategy-dir`.")
        st.stop()

    strat_config = load_strategy(strategy_dir)
    cfg = strat_config.to_app_config(database_url=database_url)

    repo = TradingRepository(cfg.database_url or cfg.db_path, suffix=cfg.ticker_suffix)
    repo.init_db()

    db_dir   = strat_config.strategy_dir / "data"
    log_file = strat_config.log_dir / "engine.log"

    # ── Sidebar — System Parameters & Controls ─────────────────────────────────────
    with st.sidebar:
        st.markdown("## ⚙️ System")

        enabled   = repo.get_engine_enabled()
        hb        = repo.get_engine_heartbeat() or {}
        state     = hb.get("state", "unknown")
        last_run  = (hb.get("last_run") or "—")[:19]
        last_err  = hb.get("error")

        # ── Market status from heartbeat ─────────────────────────────────────
        mkt_open       = hb.get("market_open", None)
        mkt_hours      = hb.get("market_hours", f"{cfg.market_open}–{cfg.market_close}")
        mkt_tz         = hb.get("market_tz", cfg.exchange_tz)
        next_evt       = hb.get("next_event", "")
        next_evt_in    = hb.get("next_event_in", "")
        auto_sched     = cfg.auto_schedule

        # Compute live market status directly (so it's always fresh even if heartbeat is stale)
        from stocking_app.market_schedule import market_status as _mkt_status, fmt_duration
        _live = _mkt_status(cfg.exchange_tz, cfg.market_open, cfg.market_close)
        live_open      = _live["market_open"]
        live_next_evt  = _live["next_event"]
        live_next_in   = _live["next_event_in"]

        # ── Rich status banner ───────────────────────────────────────────────
        if state in ("running", "starting", "fetching", "computing"):
            banner_color = "#14532d"  # dark green
            if state in ("starting", "fetching", "computing"):
                banner_icon = "🟡"
                act_str = hb.get('fetch_progress') if state == 'fetching' else hb.get('compute_progress') if state == 'computing' else ''
                banner_title = f"{state.upper()} — {act_str}"
            else:
                banner_icon = "🟢"
                banner_title = f"RUNNING — market closes in {live_next_in}" if live_open else "RUNNING"
        elif state == "paused_market_closed":
            banner_color = "#1e1b4b"  # dark indigo
            banner_icon = "⏰"
            banner_title = f"WAITING — market {live_next_evt} in {live_next_in}"
        elif state == "paused":
            banner_color = "#450a0a"  # dark red
            banner_icon = "🔴"
            banner_title = "MANUALLY PAUSED"
        else:
            banner_color = "#1c1917"  # dark stone
            banner_icon = "⚫"
            banner_title = f"OFFLINE — {live_next_evt} in {live_next_in}"

        st.markdown(f"""
    <div style="background:{banner_color};border-radius:8px;padding:10px 14px;margin-bottom:10px">
      <div style="font-size:1.1rem;font-weight:700;color:#f1f5f9;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{banner_icon} {banner_title}</div>
      <div style="font-size:0.78rem;color:#94a3b8;margin-top:4px">
        Market: <b>{mkt_hours}</b> {mkt_tz}<br>
        Last run: <code>{last_run}</code><br>
        Auto-schedule: {"✅ ON" if auto_sched else "❌ OFF"}
      </div>
    </div>
    """, unsafe_allow_html=True)

        if last_err:
            st.error(f"⚠ {last_err}")

        # ── Safety guard: detect if a cycle is actively running ──────────────
        cycle_started_at = hb.get("cycle_started_at")
        cycle_secs       = int(hb.get("cycle_seconds", cfg.cycle_seconds))
        cycle_in_progress = False
        secs_left_in_cycle = 0
        if cycle_started_at and state in ("running", "starting", "fetching", "computing"):
            import datetime as _dt
            try:
                started = _dt.datetime.fromisoformat(cycle_started_at.replace("Z", "+00:00"))
                age = (_dt.datetime.now(_dt.timezone.utc) - started).total_seconds()
                secs_left_in_cycle = max(0, int(cycle_secs - age))
                cycle_in_progress = age < cycle_secs
            except Exception:
                pass

        # ── Engine controls ───────────────────────────────────────────────────
        if auto_sched:
            st.caption("🤖 Auto-schedule is ON — engine manages itself during market hours.")
            st.caption("Use override buttons below only for testing or emergencies.")

        col_a, col_b = st.columns(2)
        with col_a:
            manual_start_disabled = (state in ("running", "starting", "fetching", "computing")) or (auto_sched and live_open)
            if st.button("▶ Start", use_container_width=True, type="primary",
                         disabled=manual_start_disabled):
                repo.set_engine_enabled(True)
                st.toast("▶ Start signal sent. Engine will resume shortly.", icon="🟢")
                time.sleep(1.5)
                st.rerun()

        with col_b:
            stop_disabled = cycle_in_progress
            stop_label = "⏹ Stop"
            if st.button(stop_label, use_container_width=True, disabled=stop_disabled):
                repo.set_engine_enabled(False)
                st.toast("⏹ Stop signal sent. Engine will pause after current cycle.", icon="🟡")
                time.sleep(1.5)
                st.rerun()

        if cycle_in_progress:
            st.warning(
                f"⚠ Cycle in progress — safe to stop in ~{fmt_duration(secs_left_in_cycle)}. "
                f"Stop button will re-enable once cycle ends.",
                icon="⚠️"
            )

        st.markdown("## 📐 Parameters")
        params = {
            "DB Path":       str(cfg.db_path),
            "Ticker Suffix": cfg.ticker_suffix,
            "Cycle (s)":     cfg.cycle_seconds,
            "Fetch Lookback":f"{cfg.fetch_lookback_days}d (1D Polling)",
            "Compute Window":f"{cfg.compute_lookback_days}d",
            "Fetch Workers": cfg.max_fetch_concurrency,
            "Compute Workers": cfg.compute_workers,
            "Order Qty":     cfg.order_qty,
        }
        for k, v in params.items():
            st.markdown(f"**{k}:** `{v}`")

        st.divider()
        auto = st.checkbox("⏱ Auto-refresh (10s)", value=False)
        if auto:
            st_autorefresh(interval=10 * 1000, key="dash_refresh")
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
        "SELECT realized_pnl, unrealized_pnl, total_pnl FROM pnl_snapshots WHERE suffix=%s ORDER BY ts DESC LIMIT 1",
        (cfg.ticker_suffix,)
    )
    realized   = float(pnl_snap["realized_pnl"].iloc[0])   if not pnl_snap.empty else 0.0
    unrealized = float(pnl_snap["unrealized_pnl"].iloc[0]) if not pnl_snap.empty else 0.0
    total_pnl  = float(pnl_snap["total_pnl"].iloc[0])      if not pnl_snap.empty else 0.0

    latest_run = repo.read_df(
        "SELECT * FROM run_metrics WHERE suffix=%s ORDER BY id DESC LIMIT 1",
        (cfg.ticker_suffix,)
    )
    last_cycle_status = str(latest_run["status"].iloc[0]) if not latest_run.empty else "—"
    last_duration     = float(latest_run["duration_seconds"].iloc[0]) if not latest_run.empty else 0.0
    fetched_last      = int(latest_run["symbols_fetched"].iloc[0]) if not latest_run.empty else 0

    k1, k2, k3, k4, k5, k6, k7 = st.columns(7)

    # Build a dynamic engine state string using the live progress payloads
    engine_status_str = f"{state.upper()}"
    if state == "fetching" and hb.get("fetch_progress"):
        engine_status_str = f"fetching: {hb['fetch_progress']}"
    elif state == "computing" and hb.get("compute_progress"):
        engine_status_str = f"computing: {hb['compute_progress']}"
    elif state == "starting":
        engine_status_str = "STARTING"

    _dot = "🟢" if state in ("running", "starting", "fetching", "computing") else ("🟡" if state in ("paused", "paused_market_closed") else "⚫")

    k1.metric("Engine",         f"{_dot} {engine_status_str}")
    k2.metric("Universe",       uni_summary["active"])
    k3.metric("Open Positions", n_open)
    k4.metric("Realized P&L",   f"{realized:,.2f}")
    k5.metric("Unrealized P&L", f"{unrealized:,.2f}", delta=f"{unrealized:+,.0f}")
    k6.metric("Total P&L",      f"{total_pnl:,.2f}",  delta=f"{total_pnl:+,.0f}")
    k7.metric("Last Cycle",     last_cycle_status, delta=f"{last_duration:.1f}s")

    st.divider()

    # ── Tabs ───────────────────────────────────────────────────────────────────────
    tabs = st.tabs([
        "📊 Overview",
        "💰 Portfolio",
        "🌐 Universe",
        "🕵️ Alerts",
        "🔬 Deep Dive",
        "🚦 Signals",
        "📒 Ledger",
        "⚙️ System",
        "📋 Live Log"
    ])
    tab_overview, tab_portfolio, tab_universe, tab_alerts, tab_deep_dive, tab_signals, tab_ledger, tab_system, tab_logs = tabs


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
                   SUBSTR(CAST(run_started_at AS TEXT),1,19) AS started,
                   status,
                   symbols_total AS total,
                   symbols_fetched AS fetched,
                   symbols_computed AS computed,
                   fetch_seconds    AS fetch_s,
                   compute_seconds  AS compute_s,
                   duration_seconds AS total_s,
                   error
            FROM run_metrics
            WHERE suffix = %s
            ORDER BY id DESC LIMIT 50
            """,
            (cfg.ticker_suffix,)
        )
        if not history.empty:
            history['fetch_s'] = history['fetch_s'].round(1)
            history['compute_s'] = history['compute_s'].round(1)
            history['total_s'] = history['total_s'].round(1)
        
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
            FROM pnl_snapshots WHERE suffix = %s ORDER BY ts ASC LIMIT 500
            """,
            (cfg.ticker_suffix,)
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
                   ROUND(avg_price::NUMERIC,4)   AS avg_price,
                   ROUND(last_price::NUMERIC,4)  AS last_price,
                   ROUND(((last_price - avg_price) * qty)::NUMERIC, 2) AS unrealized_pnl,
                   ROUND(((last_price - avg_price) / avg_price * 100)::NUMERIC, 2) AS pct_chg,
                   opened_at,
                   last_updated_at
            FROM positions_ledger
            WHERE symbol LIKE %s
            ORDER BY unrealized_pnl DESC
            """,
            (f"%{cfg.ticker_suffix}",)
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
            WHERE u.symbol LIKE %s
            ORDER BY u.is_active DESC, sync_status, u.symbol
            """,
            (f"%{cfg.ticker_suffix}",)
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
    # TAB 4 — ALERTS (PROXIMITY)
    # ══════════════════════════════════════════════════════════════════════════════
    with tab_alerts:
        st.markdown('<p class="section-header">Fractal Proximity Alerts (±0.5%)</p>', unsafe_allow_html=True)
        st.caption("Scans the active universe for stocks approaching or just crossing their weekly fractal bands.")

        col1, col2 = st.columns([1, 4])
        with col1:
            run_btn = st.button("🚀 Run Universe Scan", use_container_width=True)
        
        if run_btn:
            with st.status("Scanning universe...", expanded=True) as status:
                st.write("Fetching active symbols...")
                uni_df = repo.read_df("SELECT symbol FROM universe WHERE is_active=1")
                all_syms = uni_df['symbol'].tolist()
                
                results = {
                    "above_upper": [],
                    "below_upper": [],
                    "above_lower": [],
                    "below_lower": []
                }
                
                progress_bar = st.progress(0)
                batch_size = 30
                from stocking_app.strategy import compute_all_indicators
                
                for idx in range(0, len(all_syms), batch_size):
                    batch = all_syms[idx : idx + batch_size]
                    status.update(label=f"Scanning {idx + len(batch)} / {len(all_syms)} symbols...")
                    
                    daily_dict = repo.get_combined_bars_for_symbols(batch, daily_lookback_days=365)
                    
                    for sym in batch:
                        df = daily_dict.get(sym)
                        if df is None or df.empty:
                            continue
                        
                        try:
                            # Use Asia/Kolkata for NSE, or NYSE for US. 
                            # We'll use cfg.exchange_tz to be safe.
                            aligned = compute_all_indicators(df, cfg.exchange_tz)
                            if aligned.empty:
                                continue
                                
                            last = aligned.iloc[-1]
                            close = float(last["close"])
                            ub = float(last["weekly_upper_band"])
                            lb = float(last["weekly_lower_band"])
                            
                            if pd.isna(ub) or pd.isna(lb):
                                continue
                                
                            diff_u = (close - ub) / ub
                            diff_l = (close - lb) / lb
                            
                            # Categorize (0.5% threshold)
                            if 0 < diff_u <= 0.005:
                                results["above_upper"].append({"Symbol": sym, "Price": close, "Band": ub, "Dist%": round(diff_u * 100, 3)})
                            elif -0.005 <= diff_u < 0:
                                results["below_upper"].append({"Symbol": sym, "Price": close, "Band": ub, "Dist%": round(diff_u * 100, 3)})
                                
                            if 0 < diff_l <= 0.005:
                                results["above_lower"].append({"Symbol": sym, "Price": close, "Band": lb, "Dist%": round(diff_l * 100, 3)})
                            elif -0.005 <= diff_l < 0:
                                results["below_lower"].append({"Symbol": sym, "Price": close, "Band": lb, "Dist%": round(diff_l * 100, 3)})
                        except:
                            continue
                            
                    progress_bar.progress(min((idx + batch_size) / len(all_syms), 1.0))
                
                st.session_state["proximity_alerts"] = results
                status.update(label="Scan complete!", state="complete", expanded=False)

        # Display results from session state
        if "proximity_alerts" in st.session_state:
            res = st.session_state["proximity_alerts"]
            
            # Helper to display a section
            def show_alert_table(label, data, color):
                st.markdown(f"#### {label}")
                if not data:
                    st.info("No stocks found in this range.")
                else:
                    df = pd.DataFrame(data).sort_values("Dist%", ascending=True)
                    st.dataframe(df, use_container_width=True, hide_index=True)

            # Four Sections as requested
            st.write("---")
            c1, c2 = st.columns(2)
            with c1:
                show_alert_table("📈 0.5% ABOVE Upper Fractal Band", res["above_upper"], "green")
                show_alert_table("📉 0.5% BELOW Upper Fractal Band", res["below_upper"], "orange")
            with c2:
                show_alert_table("📈 0.5% ABOVE Lower Fractal Band", res["above_lower"], "blue")
                show_alert_table("📉 0.5% BELOW Lower Fractal Band", res["below_lower"], "red")

    # ══════════════════════════════════════════════════════════════════════════════
    # TAB 5 — DEEP DIVE
    # ══════════════════════════════════════════════════════════════════════════════
    with tab_deep_dive:
        st.markdown('<p class="section-header">Symbol Deep Dive</p>', unsafe_allow_html=True)
        st.caption("Select a symbol to view its complete indicator calculations and charts.")

        active_symbols = uni_summary.get("active", 0)
        
        if active_symbols == 0:
            st.info("No active symbols in universe.")
        else:
            # Get list of all active symbols
            active_df = repo.read_df(
                "SELECT symbol FROM universe WHERE is_active=1 AND symbol LIKE %s ORDER BY symbol",
                (f"%{cfg.ticker_suffix}",)
            )
            
            if not active_df.empty:
                symbols_list = active_df["symbol"].tolist()
                
                selected_symbol = st.selectbox("Select Symbol", symbols_list)
                
                if selected_symbol:
                    with st.spinner(f"Loading data for {selected_symbol}..."):
                        # Fetch the raw daily bars
                        daily_bars_dict = repo.get_combined_bars_for_symbols(
                            [selected_symbol],
                            daily_lookback_days=cfg.daily_lookback_days
                        )
                        
                        daily = daily_bars_dict.get(selected_symbol)
                        
                        if daily is None or daily.empty:
                            st.warning(f"No daily bar data found for {selected_symbol}.")
                        else:
                            from stocking_app.strategy import compute_all_indicators, _to_weekly
                            from stocking_app.indicators import fractal_chaos_bands
                            import plotly.graph_objects as go
                            from plotly.subplots import make_subplots

                            # Run the shared indicator logic
                            try:
                                aligned = compute_all_indicators(daily, cfg.exchange_tz)

                                if aligned.empty:
                                    st.warning(f"Insufficient data to compute indicators for {selected_symbol}.")
                                else:
                                    # ── Compute weekly OHLCV + fractals for the weekly subplot ──
                                    weekly_df = _to_weekly(daily, cfg.exchange_tz)
                                    weekly_df = fractal_chaos_bands(weekly_df, 2, 2)

                                    # ── 3-panel chart ─────────────────────────────────────────
                                    st.markdown(f"### 📈 {selected_symbol} — Deep Dive")

                                    fig = make_subplots(
                                        rows=3, cols=1,
                                        shared_xaxes=False,   # independent x-axes so weekly doesn't stretch daily
                                        vertical_spacing=0.07,
                                        subplot_titles=(
                                            "📅 Daily Price · Fractal Chaos Bands",
                                            "📆 Weekly Candles · Fractal Pivots",
                                            "〰️ CMO Oscillator · Daily & Weekly",
                                        ),
                                        row_heights=[0.38, 0.32, 0.30],
                                    )

                                    # ── Row 1: Daily candlestick + weekly band lines ───────────
                                    fig.add_trace(go.Candlestick(
                                        x=aligned.index, name="Daily",
                                        open=aligned["open"], high=aligned["high"],
                                        low=aligned["low"],   close=aligned["close"],
                                        increasing_line_color="#4ade80",
                                        decreasing_line_color="#f87171",
                                        showlegend=True,
                                    ), row=1, col=1)

                                    if "weekly_upper_band" in aligned.columns:
                                        fig.add_trace(go.Scatter(
                                            x=aligned.index, y=aligned["weekly_upper_band"],
                                            mode="lines", name="Weekly Upper Band",
                                            line=dict(color="#22d3ee", width=1.5, dash="dash"),
                                        ), row=1, col=1)
                                    if "weekly_lower_band" in aligned.columns:
                                        fig.add_trace(go.Scatter(
                                            x=aligned.index, y=aligned["weekly_lower_band"],
                                            mode="lines", name="Weekly Lower Band",
                                            line=dict(color="#f9a8d4", width=1.5, dash="dash"),
                                        ), row=1, col=1)

                                    # ── Row 2: Weekly candlestick + fractal pivot markers ──────
                                    if not weekly_df.empty:
                                        fig.add_trace(go.Candlestick(
                                            x=weekly_df.index, name="Weekly",
                                            open=weekly_df["open"], high=weekly_df["high"],
                                            low=weekly_df["low"],   close=weekly_df["close"],
                                            increasing_line_color="#34d399",
                                            decreasing_line_color="#fb923c",
                                            showlegend=True,
                                        ), row=2, col=1)

                                        # Upper fractal pivots — triangle-down above bar
                                        uf = weekly_df[weekly_df["upper_fractal_point"].notna()]
                                        if not uf.empty:
                                            fig.add_trace(go.Scatter(
                                                x=uf.index, y=uf["upper_fractal_point"] * 1.002,
                                                mode="markers", name="Upper Fractal",
                                                marker=dict(symbol="triangle-down", color="#22d3ee", size=10),
                                            ), row=2, col=1)

                                        # Lower fractal pivots — triangle-up below bar
                                        lf = weekly_df[weekly_df["lower_fractal_point"].notna()]
                                        if not lf.empty:
                                            fig.add_trace(go.Scatter(
                                                x=lf.index, y=lf["lower_fractal_point"] * 0.998,
                                                mode="markers", name="Lower Fractal",
                                                marker=dict(symbol="triangle-up", color="#f9a8d4", size=10),
                                            ), row=2, col=1)

                                        # Forward-filled band lines on weekly chart too
                                        if "upper_band_line" in weekly_df.columns:
                                            fig.add_trace(go.Scatter(
                                                x=weekly_df.index, y=weekly_df["upper_band_line"],
                                                mode="lines", name="Upper Band (W)",
                                                line=dict(color="#22d3ee", width=1, dash="dot"),
                                                showlegend=False,
                                            ), row=2, col=1)
                                        if "lower_band_line" in weekly_df.columns:
                                            fig.add_trace(go.Scatter(
                                                x=weekly_df.index, y=weekly_df["lower_band_line"],
                                                mode="lines", name="Lower Band (W)",
                                                line=dict(color="#f9a8d4", width=1, dash="dot"),
                                                showlegend=False,
                                            ), row=2, col=1)

                                    # ── Row 3: CMO oscillator — daily + weekly ─────────────────
                                    # Daily CMO raw (faint background)
                                    if "cmo" in aligned.columns:
                                        fig.add_trace(go.Scatter(
                                            x=aligned.index, y=aligned["cmo"],
                                            mode="lines", name="CMO (D)",
                                            line=dict(color="#64748b", width=1),
                                        ), row=3, col=1)
                                    # Daily EMA / SMA
                                    if "ema_cmo" in aligned.columns:
                                        fig.add_trace(go.Scatter(
                                            x=aligned.index, y=aligned["ema_cmo"],
                                            mode="lines", name="EMA CMO (D)",
                                            line=dict(color="#facc15", width=1.5),
                                        ), row=3, col=1)
                                    if "sma_cmo" in aligned.columns:
                                        fig.add_trace(go.Scatter(
                                            x=aligned.index, y=aligned["sma_cmo"],
                                            mode="lines", name="SMA CMO (D)",
                                            line=dict(color="#fb923c", width=1.5),
                                        ), row=3, col=1)
                                    # Weekly EMA / SMA
                                    if "weekly_ema_cmo" in aligned.columns:
                                        fig.add_trace(go.Scatter(
                                            x=aligned.index, y=aligned["weekly_ema_cmo"],
                                            mode="lines", name="EMA CMO (W)",
                                            line=dict(color="#22d3ee", width=1.5, dash="dash"),
                                        ), row=3, col=1)
                                    if "weekly_sma_cmo" in aligned.columns:
                                        fig.add_trace(go.Scatter(
                                            x=aligned.index, y=aligned["weekly_sma_cmo"],
                                            mode="lines", name="SMA CMO (W)",
                                            line=dict(color="#f9a8d4", width=1.5, dash="dash"),
                                        ), row=3, col=1)
                                    # Zero line
                                    fig.add_hline(y=0, line=dict(color="#475569", width=1, dash="dot"), row=3, col=1)

                                    # ── Layout ────────────────────────────────────────────────
                                    fig.update_layout(
                                        height=900,
                                        showlegend=True,
                                        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0),
                                        margin=dict(l=20, r=20, t=60, b=20),
                                        template="plotly_dark",
                                        paper_bgcolor="#0f172a",
                                        plot_bgcolor="#0f172a",
                                    )
                                    fig.update_xaxes(rangeslider_visible=False)
                                    fig.update_xaxes(showgrid=True, gridcolor="#1e293b")
                                    fig.update_yaxes(showgrid=True, gridcolor="#1e293b")

                                    st.plotly_chart(fig, use_container_width=True)

                                    # ── Raw data table ────────────────────────────────────────
                                    st.markdown("### 🧮 Raw Indicator Data (Full Year)")
                                    display_df = aligned.copy()
                                    display_df.index = pd.to_datetime(display_df.index)
                                    display_df = display_df.sort_index(ascending=False)
                                    display_df.index = display_df.index.strftime("%Y-%m-%d")
                                    cols_to_show = [
                                        "close",
                                        "weekly_upper_band", "weekly_lower_band",
                                        "cmo", "ema_cmo", "sma_cmo",
                                        "weekly_ema_cmo", "weekly_sma_cmo",
                                    ]
                                    available_cols = [c for c in cols_to_show if c in display_df.columns]
                                    st.dataframe(
                                        display_df[available_cols].style.format("{:.4f}"),
                                        use_container_width=True,
                                    )

                                    # ── Weekly data table ─────────────────────────────────────
                                    if not weekly_df.empty:
                                        st.markdown("### 📆 Weekly Bar Data (Full Year)")
                                        wk_display = weekly_df[["open", "high", "low", "close",
                                                                  "upper_fractal_point", "lower_fractal_point",
                                                                  "upper_band_line", "lower_band_line"]].copy()
                                        wk_display.index = pd.to_datetime(wk_display.index)
                                        wk_display = wk_display.sort_index(ascending=False)
                                        wk_display.index = wk_display.index.strftime("%Y-%m-%d")
                                        st.dataframe(
                                            wk_display.style.format("{:.4f}"),
                                            use_container_width=True,
                                        )

                            except Exception as e:
                                import traceback
                                st.error(f"Error computing indicators: {e}")
                                st.code(traceback.format_exc())


    # ══════════════════════════════════════════════════════════════════════════════
    # TAB 5 — SIGNALS
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
                ROUND(AVG(price)::NUMERIC,4) AS avg_price,
                MAX(ts) AS latest
            FROM signals
            WHERE symbol LIKE %s
            GROUP BY signal_type
            """,
            (f"%{cfg.ticker_suffix}",)
        )
        if not sig_summary.empty:
            st.dataframe(sig_summary, use_container_width=True, hide_index=True)

        st.markdown('<p class="section-header">Recent Signals</p>', unsafe_allow_html=True)

        sig_filter = st.multiselect("Filter by type", ["BUY", "SELL"], default=["BUY", "SELL"])
        acted_filter = st.checkbox("Acted only", value=False)

        signals = repo.read_df(
            """
            SELECT id, symbol, SUBSTR(ts,1,19) AS ts, signal_type, ROUND(price::NUMERIC,4) AS price, reason, acted
            FROM signals
            WHERE symbol LIKE %s
            ORDER BY id DESC
            LIMIT 500
            """,
            (f"%{cfg.ticker_suffix}",)
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
                   ROUND(price::NUMERIC,4) AS price,
                   ROUND(pnl::NUMERIC,2)   AS pnl,
                   SUBSTR(ts,1,19) AS ts,
                   reason
            FROM trade_activity_log
            WHERE symbol LIKE %s
            ORDER BY id DESC
            LIMIT 500
            """,
            (f"%{cfg.ticker_suffix}",)
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
            "Exchange Timezone": cfg.exchange_tz,
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
        files = {}
        if cfg.database_url:
            files["Live DB"] = ("Supabase (PostgreSQL)", True)
            files["Engine Log"] = ("Supabase (system_logs)", True)
        else:
            files["Live DB"] = (str(cfg.db_path), Path(cfg.db_path).exists())
            files["Engine Log"] = (str(log_file), Path(log_file).exists())
            
        files["Backtest DB"] = (str(strat_config.backtest_dir / "backtest.db"), (strat_config.backtest_dir / "backtest.db").exists())
        files["Backtest Report"] = (str(strat_config.backtest_dir / "report.txt"), (strat_config.backtest_dir / "report.txt").exists())
        files["Backtest Trades"] = (str(strat_config.backtest_dir / "trades.csv"), (strat_config.backtest_dir / "trades.csv").exists())

        files_df = pd.DataFrame(
            [{"File": k, "Path": v[0], "Exists": "✅" if v[1] else "❌"}
             for k, v in files.items()]
        )
        st.dataframe(files_df, use_container_width=True, hide_index=True)


    # ══════════════════════════════════════════════════════════════════════════════
    # TAB 7 — LIVE LOG
    # ══════════════════════════════════════════════════════════════════════════════
    with tab_logs:
        st.markdown('<p class="section-header">Live Engine Log</p>', unsafe_allow_html=True)

        n_lines = st.slider("Lines to show", min_value=20, max_value=500, value=100, step=20)

        # Read from Supabase instead of local file system
        logs_df = repo.get_recent_logs(limit=n_lines)
        
        if not logs_df.empty:
            # Construct a code block to simulate a terminal view
            # The logs come back sorted DESC, so we reverse them to chronologically display them top-to-bottom
            log_lines = []
            for _, row in logs_df.iloc[::-1].iterrows():
                # Format: 2026-03-02T12:00:00Z  INFO     Message
                ts = row["ts"][:19].replace("T", " ")
                level = str(row["level"]).ljust(7)
                log_lines.append(f"{ts}  {level}  {row['message']}")
                
            recent = "\n".join(log_lines)
            st.code(recent, language=None)
            
            counts = logs_df["level"].value_counts().to_dict()
            count_str = " | ".join(f"{k}: {v}" for k,v in counts.items())
            
            st.caption(
                f"📄 Reading live from Supabase `system_logs` | "
                f"Showing last {len(logs_df)} lines | {count_str}"
            )
        else:
            st.warning("No logs found in the database. Engine needs to run at least one cycle with the new database handler.")

    # ── Footer / auto-refresh ──────────────────────────────────────────────────────
    st.divider()
    st.caption(
        f"VER: `2026-03-14-v1` | LOOKBACK: `{cfg.daily_lookback_days}d` | "
        f"DB: `{cfg.db_path}`  |  Suffix: `{cfg.ticker_suffix}`  |  "
        f"Cycle: `{cfg.cycle_seconds}s`  |  "
        f"Open positions: `{n_open}`  |  Universe: `{uni_summary['active']} active`"
    )
    if st.sidebar.button("🗑️ Clear Dashboard Cache"):
        st.cache_data.clear()
        st.rerun()

    # Auto-refresh handled by st_autorefresh in sidebar

    repo.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--strategy-dir', type=str, required=False)
    args, unknown = parser.parse_known_args()
    strat = args.strategy_dir
    if 'strategy' in st.query_params:
        strat = st.query_params['strategy']
    run_dashboard(strat)
