#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from stocking_app.strategy_loader import discover_strategies, StrategyConfig

# ── Page Config — MUST be first Streamlit call ───────────────────────────────
st.set_page_config(
    page_title="Stocking Hub — All Strategies",
    layout="wide",
    page_icon="🏦",
    initial_sidebar_state="expanded",
)

# ── Cloud URL (used for dashboard deep-links) ─────────────────────────────────
# Set STREAMLIT_CLOUD_APP_URL in Streamlit Cloud secrets/env if your app URL differs.
_CLOUD_BASE_URL = os.environ.get(
    "STREAMLIT_CLOUD_APP_URL",
    "https://stocking-vaibhav2.streamlit.app",
).rstrip("/")

# ── Cloud Routing (Dashboard view) ──────────────────────────────────────────────
if "strategy" in st.query_params:
    strat_id = st.query_params["strategy"]
    strategy_path = ROOT / "strategies" / strat_id
    if strategy_path.exists():
        sys.argv = ["streamlit", "run", "dashboard.py", "--strategy-dir", str(strategy_path)]
        dash_path = str(ROOT / "dashboard.py")
        import runpy
        runpy.run_path(dash_path, run_name="__main__")
        st.stop()
    else:
        st.error(f"Strategy '{strat_id}' not found. Available: fractal_momentum_lse, fractal_momentum_nse")
        if st.button("← Back to Hub"):
            st.query_params.clear()
            st.rerun()


# ── Helper — read live state from a strategy's DB ─────────────────────────────
def _read_strategy_state(sc: StrategyConfig) -> dict:
    from stocking_app.config import load_config
    import psycopg2
    from psycopg2.extras import RealDictCursor
    cfg = load_config()
    db_url = cfg.database_url
    
    result = {
        "engine_state":      "offline",
        "last_run":          None,
        "last_cycle_status": "—",
        "symbols_fetched":   0,
        "symbols_total":     0,
        "realized_pnl":      0.0,
        "unrealized_pnl":    0.0,
        "open_positions":    0,
    }
    if not db_url:
        return result
    try:
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT value FROM engine_state WHERE key='engine_heartbeat'")
                row = cur.fetchone()
                if row:
                    hb = json.loads(row["value"])
                    result["engine_state"] = hb.get("state", "offline")
                    result["last_run"]     = hb.get("last_run")
            except Exception:
                pass
            try:
                cur.execute("SELECT status, symbols_fetched, symbols_total FROM run_metrics ORDER BY id DESC LIMIT 1")
                row = cur.fetchone()
                if row:
                    result["last_cycle_status"] = row["status"] or "—"
                    result["symbols_fetched"]   = int(row["symbols_fetched"] or 0)
                    result["symbols_total"]     = int(row["symbols_total"] or 0)
            except Exception:
                pass
            try:
                cur.execute("SELECT realized_pnl, unrealized_pnl, open_positions FROM pnl_snapshots ORDER BY ts DESC LIMIT 1")
                row = cur.fetchone()
                if row:
                    result["realized_pnl"]   = float(row["realized_pnl"] or 0)
                    result["unrealized_pnl"] = float(row["unrealized_pnl"] or 0)
                    result["open_positions"] = int(row["open_positions"] or 0)
            except Exception:
                pass
        conn.close()
    except Exception:
        pass
    return result

st.markdown("""
<style>
[data-testid="stMetricValue"] { font-size: 1.3rem; font-weight: 700; }
.strategy-card { border: 1px solid #334155; border-radius: 12px;
                  padding: 1rem 1.2rem; margin-bottom: 0.8rem;
                  background: #0f172a; }
.card-name  { font-size: 1.15rem; font-weight: 700; color: #e2e8f0; }
.card-type  { font-size: 0.8rem; color: #94a3b8; }
.state-running { color: #4ade80; font-weight: bold; }
.state-paused  { color: #facc15; font-weight: bold; }
.state-offline { color: #f87171; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏦 Stocking Hub")
    st.caption("Manages all strategy instances")
    if st.button("🔄 Refresh", use_container_width=True):
        st.rerun()
    auto = st.checkbox("⏱ Auto-refresh (30s)", value=False)
    st.divider()
    st.markdown("### 📊 Strategy Dashboards")
    strat_dirs = [
        d.name for d in (ROOT / "strategies").iterdir()
        if d.is_dir() and (d / "strategy.yaml").exists()
    ] if (ROOT / "strategies").exists() else []
    for sdir in strat_dirs:
        url = f"{_CLOUD_BASE_URL}/?strategy={sdir}"
        st.link_button(f"Open {sdir}", url, use_container_width=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🏦 Strategy Hub")
st.caption(f"Strategies folder: `{ROOT / 'strategies'}`")

strategies = discover_strategies(ROOT)

if not strategies:
    st.warning(
        "No strategy folders found under `strategies/`. "
        "Create a folder with a `strategy.yaml` to get started."
    )
    st.stop()

# ── Aggregate metrics strip ───────────────────────────────────────────────────
total_realized = 0.0
total_unreal   = 0.0
total_open     = 0
n_running      = 0

strategy_states: list[dict] = []
for sc in strategies:
    state_row = _read_strategy_state(sc)
    strategy_states.append(state_row)
    total_realized += state_row.get("realized_pnl", 0.0)
    total_unreal   += state_row.get("unrealized_pnl", 0.0)
    total_open     += state_row.get("open_positions", 0)
    if state_row.get("engine_state") == "running":
        n_running += 1

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Strategies",      len(strategies))
m2.metric("Engines Running", n_running)
m3.metric("Total Open Pos.", total_open)
m4.metric("Total Realized",  f"{total_realized:,.2f}")
m5.metric("Total P&L",       f"{total_realized + total_unreal:,.2f}",
          delta=f"{total_realized + total_unreal:+,.0f}")

st.divider()

# ── Strategy cards ─────────────────────────────────────────────────────────────
st.markdown("## Strategy Instances")

for sc, row in zip(strategies, strategy_states):
    with st.container():
        state    = row.get("engine_state", "offline")
        dot      = "🟢" if state == "running" else ("🟡" if state == "paused" else "⚫")
        realized = row.get("realized_pnl", 0.0)
        unreal   = row.get("unrealized_pnl", 0.0)
        n_open   = row.get("open_positions", 0)
        last_run = (row.get("last_run") or "—")[:16]
        last_cycle_ok = row.get("last_cycle_status", "—")
        fetched  = row.get("symbols_fetched", "—")
        total_sym= row.get("symbols_total", "—")

        # Card header
        c_name, c_state, c_pnl, c_pos, c_cycle, c_btlink = st.columns([3, 1.5, 2, 1.5, 2, 1.5])
        with c_name:
            st.markdown(f"**{sc.name}**")
            st.caption(f"`{sc.strategy_dir.name}`  ·  {sc.suffix}  ·  {sc.timezone}")
        with c_state:
            st.markdown(f"{dot} `{state.upper()}`")
            st.caption(f"Last: {last_run}")
        with c_pnl:
            st.metric("Realized P&L", f"{realized:,.2f}", delta=f"{realized:+,.0f}")
        with c_pos:
            st.metric("Open", n_open)
        with c_cycle:
            st.metric("Last Cycle", last_cycle_ok)
            st.caption(f"Fetched {fetched}/{total_sym}")
        with c_btlink:
            bt_report = sc.backtest_dir / "report.txt"
            if bt_report.exists():
                st.markdown("📄 Backtest done")
                with st.expander("View summary"):
                    txt = bt_report.read_text()[:2000]
                    st.code(txt, language=None)
            else:
                st.caption("No backtest yet")

        # Action buttons
        b2, b3, b4 = st.columns(3)
        with b2:
            # Engine control — write to DB so Render engine picks it up
            is_running = (state == "running")
            if is_running:
                if st.button("⏹ Stop Engine", key=f"stop_{sc.strategy_dir.name}",
                             use_container_width=True):
                    try:
                        from stocking_app.db import TradingRepository
                        from stocking_app.config import load_config
                        _cfg = load_config()
                        _repo = TradingRepository(_cfg.database_url or _cfg.db_path)
                        _repo.set_engine_enabled(False)
                        _repo.close()
                        st.toast(f"⏹ Stop signal sent for {sc.name}.", icon="🟡")
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"DB error: {e}")
            else:
                if st.button("▶ Start Engine", key=f"start_{sc.strategy_dir.name}",
                             type="primary", use_container_width=True):
                    try:
                        from stocking_app.db import TradingRepository
                        from stocking_app.config import load_config
                        _cfg = load_config()
                        _repo = TradingRepository(_cfg.database_url or _cfg.db_path)
                        _repo.set_engine_enabled(True)
                        _repo.close()
                        st.toast(f"▶ Start signal sent for {sc.name}. Engine will resume shortly.", icon="🟢")
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"DB error: {e}")
        with b3:
            dash_url = f"{_CLOUD_BASE_URL}/?strategy={sc.strategy_dir.name}"
            st.link_button("📊 View Dashboard", dash_url, use_container_width=True)
        with b4:
            log_f = sc.log_dir / "engine.log"
            if log_f.exists():
                lines = log_f.read_text(errors="replace").splitlines()[-10:]
                with st.expander("📋 Last log lines"):
                    st.code("\n".join(reversed(lines)), language=None)
            else:
                st.caption("No log yet")


        st.divider()

# ── Comparison table ──────────────────────────────────────────────────────────
st.markdown("## 📊 Side-by-Side Comparison")
rows = []
for sc, row in zip(strategies, strategy_states):
    rows.append({
        "Strategy":      sc.name,
        "Folder":        sc.strategy_dir.name,
        "Exchange":      sc.suffix,
        "State":         row.get("engine_state", "offline"),
        "Open Pos.":     row.get("open_positions", 0),
        "Realized P&L":  round(row.get("realized_pnl", 0.0), 2),
        "Unrealized":    round(row.get("unrealized_pnl", 0.0), 2),
        "Total P&L":     round(row.get("realized_pnl", 0.0) + row.get("unrealized_pnl", 0.0), 2),
        "Last Run":      (row.get("last_run") or "—")[:16],
        "Last Status":   row.get("last_cycle_status", "—"),
        "Backtest Done": "✅" if (sc.backtest_dir / "report.txt").exists() else "❌",
    })
cmp_df = pd.DataFrame(rows)

def _state_color(val):
    if val == "running": return "color:#4ade80"
    if val == "paused":  return "color:#facc15"
    return "color:#f87171"
def _pnl_color(val):
    try:
        v = float(val)
        return "color:#4ade80" if v > 0 else ("color:#f87171" if v < 0 else "")
    except Exception:
        return ""

st.dataframe(
    cmp_df.style
        .applymap(_state_color, subset=["State"])
        .applymap(_pnl_color,   subset=["Realized P&L", "Unrealized", "Total P&L"]),
    use_container_width=True, hide_index=True,
)

# ── Auto-refresh ──────────────────────────────────────────────────────────────
if auto:
    time.sleep(30)
    st.rerun()

