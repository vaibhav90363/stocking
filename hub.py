#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

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
        
        # Tell the child dashboard NOT to run set_page_config() since the hub already did
        st.session_state["_hub_hosted"] = True
        
        import runpy
        runpy.run_path(dash_path, run_name="__main__")
        st.stop()
    else:
        avail = [d.name for d in (ROOT / "strategies").iterdir() if d.is_dir() and (d / "strategy.yaml").exists()]
        st.error(f"Strategy '{strat_id}' not found. Available: {', '.join(avail)}")
        if st.button("← Back to Hub"):
            st.query_params.clear()
            st.rerun()



def _get_db_url() -> str:
    """Centralised DB URL resolution — avoids creating a load_config() per card."""
    from stocking_app.config import load_config
    cfg = load_config()
    return cfg.database_url


# ── Helper — read live state from a strategy's DB ─────────────────────────────
def _read_strategy_state(sc: StrategyConfig) -> dict:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    db_url = _get_db_url()

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
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor, connect_timeout=8)
        with conn.cursor() as cur:
            # Heartbeat key is suffix-namespaced: engine_heartbeat_.NS, engine_heartbeat_.L etc.
            try:
                hb_key = f"engine_heartbeat_{sc.suffix}"
                cur.execute("SELECT value FROM engine_state WHERE key=%s", (hb_key,))
                row = cur.fetchone()
                if row:
                    hb = json.loads(row["value"])
                    result["engine_state"] = hb.get("state", "offline")
                    result["last_run"]     = hb.get("last_run")
            except Exception:
                pass
            # Cycle metrics are suffix-scoped
            try:
                cur.execute(
                    "SELECT status, symbols_fetched, symbols_total FROM run_metrics "
                    "WHERE suffix=%s ORDER BY id DESC LIMIT 1",
                    (sc.suffix,)
                )
                row = cur.fetchone()
                if row:
                    result["last_cycle_status"] = row["status"] or "—"
                    result["symbols_fetched"]   = int(row["symbols_fetched"] or 0)
                    result["symbols_total"]     = int(row["symbols_total"] or 0)
            except Exception:
                pass
            try:
                cur.execute(
                    "SELECT realized_pnl, unrealized_pnl, open_positions FROM pnl_snapshots "
                    "WHERE suffix=%s ORDER BY ts DESC LIMIT 1",
                    (sc.suffix,)
                )
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
span:has(> b:contains("PAUSED")) { white-space: nowrap !important; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏦 Stocking Hub")
    st.caption("Manages all strategy instances")
    if st.button("🔄 Refresh", use_container_width=True):
        st.rerun()
    auto = st.checkbox("⏱ Auto-refresh (10s)", value=False)
    if auto:
        st_autorefresh(interval=10 * 1000, key="hub_refresh")
    st.divider()
    st.markdown("### 📊 Strategy Dashboards")
    strat_dirs = [
        d.name for d in (ROOT / "strategies").iterdir()
        if d.is_dir() and (d / "strategy.yaml").exists()
    ] if (ROOT / "strategies").exists() else []
    for sdir in strat_dirs:
        url = f"/?strategy={sdir}"
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

# ── Main tabs ─────────────────────────────────────────────────────────────────
tab_strategies, tab_health = st.tabs(["📊 Strategies", "🩺 System Health"])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — STRATEGIES
# ═══════════════════════════════════════════════════════════════════════════════
with tab_strategies:
    st.markdown("## Strategy Instances")

    for sc, row in zip(strategies, strategy_states):
        with st.container():
            state = row.get("engine_state", "offline")
            from stocking_app.market_schedule import (
                market_status as _ms, fmt_duration as _fmt
            )
            _mkt = _ms(sc.timezone, sc.market_open, sc.market_close)
            _mkt_open     = _mkt["market_open"]
            _next_evt     = _mkt["next_event"]
            _next_in      = _mkt["next_event_in"]
            _mkt_badge    = "🟢 OPEN" if _mkt_open else "⚫ CLOSED"

            _hb           = row
            _cycle_start  = _hb.get("cycle_started_at")
            _cycle_secs   = int(_hb.get("cycle_seconds", sc.cycle_seconds))
            _cycle_active = False
            _secs_left    = 0
            if _cycle_start and state in ("running", "starting"):
                import datetime as _dt
                try:
                    _started = _dt.datetime.fromisoformat(_cycle_start.replace("Z", "+00:00"))
                    _age = (_dt.datetime.now(_dt.timezone.utc) - _started).total_seconds()
                    _secs_left = max(0, int(_cycle_secs - _age))
                    _cycle_active = _age < _cycle_secs
                except Exception:
                    pass

            if state in ("running", "starting"):
                dot = "🟢" if state == "running" else "🟡"
            elif state == "paused_market_closed":
                dot = "⏰"
            else:
                dot = "⚫"

            realized      = row.get("realized_pnl", 0.0)
            unreal        = row.get("unrealized_pnl", 0.0)
            n_open        = row.get("open_positions", 0)
            last_run      = (row.get("last_run") or "—")[:16]
            last_cycle_ok = row.get("last_cycle_status", "—")
            fetched       = row.get("symbols_fetched", "—")
            total_sym     = row.get("symbols_total", "—")
            c_name, c_state, c_pnl, c_pos, c_cycle, c_btlink = st.columns([3, 2.5, 2, 1.5, 2, 1.5])
            with c_name:
                st.markdown(f"**{sc.name}**")
                st.caption(f"`{sc.strategy_dir.name}`  ·  {sc.suffix}  ·  {sc.timezone}")
            with c_state:
                st.markdown(f"<div style='white-space:nowrap;'>{dot} `{state.upper()}`</div>", unsafe_allow_html=True)
                st.caption(f"Last: {last_run}")
                mkt_color = "#14532d" if _mkt_open else "#1e293b"
                st.markdown(
                    f"<div style='white-space:nowrap;'><span style='background:{mkt_color};padding:2px 7px;border-radius:4px;"
                    f"font-size:0.72rem;color:#f1f5f9'>{_mkt_badge}</span>&nbsp;"
                    f"<span style='font-size:0.72rem;color:#64748b'>{_next_evt} {_next_in}</span></div>",
                    unsafe_allow_html=True,
                )
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

            b2, b3, b4, b5 = st.columns(4)
            with b2:
                is_running = state in ("running", "starting")
                if is_running:
                    stop_disabled = _cycle_active
                    if st.button("⏹ Stop Engine", key=f"stop_{sc.strategy_dir.name}",
                                 use_container_width=True, disabled=stop_disabled):
                        try:
                            from stocking_app.db import TradingRepository
                            from stocking_app.config import load_config
                            _cfg = load_config()
                            _repo = TradingRepository(_cfg.database_url or _cfg.db_path, suffix=sc.suffix)
                            _repo.set_engine_enabled(False)
                            _repo.close()
                            st.toast(f"⏹ Stop signal sent for {sc.name}.", icon="🟡")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"DB error: {e}")
                    if _cycle_active:
                        st.warning(f"Cycle active — safe in ~{_fmt(_secs_left)}", icon="⚠️")
                else:
                    start_disabled = (
                        os.environ.get("STOCKING_AUTO_SCHEDULE", "1") not in ("0", "false", "False")
                        and not _mkt_open
                    )
                    if st.button("▶ Start Engine", key=f"start_{sc.strategy_dir.name}",
                                 type="primary", use_container_width=True,
                                 disabled=start_disabled):
                        try:
                            from stocking_app.db import TradingRepository
                            from stocking_app.config import load_config
                            _cfg = load_config()
                            _repo = TradingRepository(_cfg.database_url or _cfg.db_path, suffix=sc.suffix)
                            _repo.set_engine_enabled(True)
                            _repo.close()
                            st.toast(f"▶ Start signal sent for {sc.name}. Engine will resume shortly.", icon="🟢")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"DB error: {e}")
                    if start_disabled:
                        st.caption("🤖 Auto-schedule will start this when market opens.")
            with b3:
                dash_url = f"/?strategy={sc.strategy_dir.name}"
                st.link_button("📊 View Dashboard", dash_url, use_container_width=True)
            with b4:
                log_f = sc.log_dir / "engine.log"
                if log_f.exists():
                    lines = log_f.read_text(errors="replace").splitlines()[-10:]
                    with st.expander("📋 Last log lines"):
                        st.code("\n".join(reversed(lines)), language=None)
                else:
                    st.caption("No log yet")
            with b5:
                strat_key = sc.strategy_dir.name
                if st.button("🔬 Run Backtest", key=f"bt_run_{strat_key}",
                             use_container_width=True):
                    st.session_state[f"bt_trigger_{strat_key}"] = True
                    st.session_state.pop(f"bt_result_{strat_key}", None)

            strat_key = sc.strategy_dir.name
            if st.session_state.get(f"bt_trigger_{strat_key}"):
                st.session_state[f"bt_trigger_{strat_key}"] = False

                universe_csv = str(sc.universe_csv)
                if not sc.universe_csv.exists():
                    st.error(f"universe.csv not found at {universe_csv}")
                else:
                    from backtest_sim import run_backtest_for_strategy

                    status_box = st.empty()
                    prog_bar   = st.progress(0.0, text="Initialising …")
                    eta_box    = st.empty()
                    log_lines: list[str] = []
                    log_box    = st.expander("📋 Backtest log", expanded=True)

                    def _on_status(msg: str):
                        status_box.info(msg)
                        log_lines.append(msg)
                        with log_box:
                            st.text("\n".join(log_lines[-20:]))

                    def _on_progress(done: int, total: int, sym: str, stage_msg: str):
                        pct = done / total if total else 0
                        prog_bar.progress(pct, text=f"{stage_msg}  |  last: `{sym}`")
                        eta_box.caption(stage_msg)

                    try:
                        report_text, trades_df = run_backtest_for_strategy(
                            universe_csv      = universe_csv,
                            suffix            = sc.suffix,
                            exchange_tz       = sc.timezone,
                            daily_lookback    = sc.daily_lookback,
                            intraday_days     = sc.intraday_days,
                            backtest_days     = sc.backtest_days,
                            capital_per_trade = float(sc.parameters.get("capital_per_trade", 100_000)),
                            fetch_concurrency = sc.fetch_concurrency,
                            on_progress       = _on_progress,
                            on_status         = _on_status,
                        )
                        prog_bar.progress(1.0, text="✅ Complete")
                        st.session_state[f"bt_result_{strat_key}"] = (report_text, trades_df)
                    except Exception as exc:
                        st.error(f"Backtest failed: {exc}")

            bt_result = st.session_state.get(f"bt_result_{strat_key}")
            if bt_result:
                report_text, trades_df = bt_result
                st.markdown(f"### 🔬 Backtest Results — {sc.name}")

                if not trades_df.empty:
                    closed    = trades_df[trades_df["pnl"].notna()].copy()
                    n_wins    = int((closed["pnl"] > 0).sum()) if not closed.empty else 0
                    n_loss    = int((closed["pnl"] <= 0).sum()) if not closed.empty else 0
                    total_pnl = closed["pnl"].sum() if not closed.empty else 0.0
                    win_rate  = n_wins / len(closed) * 100 if len(closed) else 0.0
                    r1, r2, r3, r4, r5 = st.columns(5)
                    r1.metric("BUY entries", len(trades_df[trades_df["side"] == "BUY"]))
                    r2.metric("Closed legs", len(closed))
                    r3.metric("Win rate",    f"{win_rate:.1f}%")
                    r4.metric("Realized P&L", f"{total_pnl:,.2f}", delta=f"{total_pnl:+,.0f}")
                    r5.metric("Wins / Losses", f"{n_wins} / {n_loss}")

                    if not closed.empty:
                        cum = closed[["ts", "pnl"]].copy()
                        cum["cum_pnl"] = cum["pnl"].cumsum()
                        cum["ts"] = pd.to_datetime(cum["ts"])
                        st.markdown("**Cumulative P&L over backtest window**")
                        st.line_chart(cum.set_index("ts")["cum_pnl"])

                    with st.expander("📋 All trades"):
                        def _side_color(val):
                            if val == "BUY":      return "background-color:#14532d;color:#4ade80;font-weight:700"
                            if val == "SELL":     return "background-color:#450a0a;color:#f87171;font-weight:700"
                            if val == "SELL_EOB": return "background-color:#312e81;color:#a5b4fc;font-weight:700"
                            return ""
                        def _pnl_clr(val):
                            try:
                                return "color:#4ade80" if float(val) > 0 else ("color:#f87171" if float(val) < 0 else "")
                            except Exception:
                                return ""
                        st.dataframe(
                            trades_df.style.applymap(_side_color, subset=["side"])
                                           .applymap(_pnl_clr,    subset=["pnl"]),
                            use_container_width=True, hide_index=True,
                        )

                with st.expander("📄 Full report"):
                    st.code(report_text, language=None)

                if not trades_df.empty:
                    st.download_button(
                        "⬇ Download trades CSV",
                        data=trades_df.to_csv(index=False).encode(),
                        file_name=f"backtest_{strat_key}.csv",
                        mime="text/csv",
                        key=f"dl_{strat_key}",
                    )
                if st.button("🗑 Clear results", key=f"bt_clear_{strat_key}"):
                    st.session_state.pop(f"bt_result_{strat_key}", None)
                    st.rerun()

            st.divider()

    # ── Comparison table ──────────────────────────────────────────────────────
    st.markdown("## 📊 Side-by-Side Comparison")
    cmp_rows = []
    for sc, row in zip(strategies, strategy_states):
        cmp_rows.append({
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
    cmp_df = pd.DataFrame(cmp_rows)

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


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — SYSTEM HEALTH
# ═══════════════════════════════════════════════════════════════════════════════
with tab_health:
    import requests as _requests

    st.markdown("## 🩺 System Health Dashboard")
    st.caption(f"Checked at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if st.button("🔄 Re-check now", key="health_refresh"):
        st.rerun()

    st.divider()

    # ── Helper: ping a URL ────────────────────────────────────────────────────
    def _ping(url: str, timeout: int = 6) -> tuple[bool, int | None, str]:
        try:
            r = _requests.get(url, timeout=timeout)
            return r.status_code < 400, r.status_code, ""
        except Exception as exc:
            return False, None, str(exc)[:80]

    def _badge(ok: bool, ok_label: str = "✅ Online", fail_label: str = "❌ Unreachable") -> str:
        color    = "#14532d" if ok else "#450a0a"
        txt_clr  = "#4ade80" if ok else "#f87171"
        label    = ok_label  if ok else fail_label
        return (
            f"<span style='background:{color};color:{txt_clr};"
            f"padding:3px 10px;border-radius:6px;font-weight:700;"
            f"font-size:0.85rem'>{label}</span>"
        )

    # ── Section 1: Service Status Pings ──────────────────────────────────────
    st.markdown("### 🌐 Service Status")
    _render_url    = os.environ.get("RENDER_HEALTH_URL", "").strip()
    _streamlit_url = _CLOUD_BASE_URL.strip()

    col_sb, col_st, col_rd = st.columns(3)

    with col_sb:
        st.markdown("**☁️ Supabase (Database)**")
        try:
            from stocking_app.config import load_config as _lc
            from stocking_app.db import TradingRepository as _TR
            _cfg_h = _lc()
            _repo_h = _TR(_cfg_h.database_url or _cfg_h.db_path)
            _uni = _repo_h.get_universe_summary()
            _repo_h.close()
            st.markdown(_badge(True, "✅ Connected"), unsafe_allow_html=True)
            st.caption(f"Universe: **{_uni['total']}** symbols · **{_uni['active']}** active")
        except Exception as _e:
            st.markdown(_badge(False, fail_label=f"❌ Error"), unsafe_allow_html=True)
            st.caption(str(_e)[:120])

    with col_st:
        st.markdown("**🖥️ Streamlit Cloud**")
        _st_ok, _st_code, _st_err = _ping(_streamlit_url)
        st.markdown(
            _badge(_st_ok, f"✅ Online ({_st_code})", f"❌ {_st_code or _st_err}"),
            unsafe_allow_html=True,
        )
        st.caption(_streamlit_url)

    with col_rd:
        st.markdown("**⚙️ Render Engine**")
        if _render_url:
            _rd_ok, _rd_code, _rd_err = _ping(_render_url)
            st.markdown(
                _badge(_rd_ok, f"✅ Online ({_rd_code})", f"❌ {_rd_code or _rd_err}"),
                unsafe_allow_html=True,
            )
            st.caption(_render_url)
        else:
            st.markdown(
                "<span style='background:#1e293b;color:#94a3b8;padding:3px 10px;"
                "border-radius:6px;font-size:0.85rem'>⚠️ URL not set</span>",
                unsafe_allow_html=True,
            )
            st.caption("Add `RENDER_HEALTH_URL` to Streamlit secrets to enable.")

    st.divider()

    # ── Section 2: Per-strategy Engine Heartbeats ─────────────────────────────
    st.markdown("### 💓 Engine Heartbeats (per strategy)")
    try:
        import psycopg2 as _pg2
        from psycopg2.extras import RealDictCursor as _RDC
        _db_url = _get_db_url()
        _conn_hb = _pg2.connect(_db_url, cursor_factory=_RDC, connect_timeout=8)

        _hb_cols = st.columns(max(1, len(strategies)))
        for _hb_idx, _hb_sc in enumerate(strategies):
            with _hb_cols[_hb_idx]:
                st.markdown(f"**{_hb_sc.suffix}** — {_hb_sc.name.split('—')[-1].strip()}")
                try:
                    _hb_key = f"engine_heartbeat_{_hb_sc.suffix}"
                    with _conn_hb.cursor() as _hb_cur:
                        _hb_cur.execute(
                            "SELECT value, updated_at FROM engine_state WHERE key=%s",
                            (_hb_key,)
                        )
                        _hb_row = _hb_cur.fetchone()

                    if _hb_row:
                        _hb_data  = json.loads(_hb_row["value"])
                        _hb_upd   = str(_hb_row["updated_at"])
                        _hb_state = _hb_data.get("state", "unknown")
                        _hb_ts    = _hb_data.get("last_run") or _hb_data.get("ts", "—")

                        try:
                            _hb_dt  = datetime.fromisoformat(_hb_upd.replace("Z", "+00:00"))
                            _hb_age = (datetime.now(timezone.utc) - _hb_dt).total_seconds()
                            _age_str = f"{int(_hb_age // 60)}m {int(_hb_age % 60)}s ago"
                            _stale   = _hb_age > 600
                        except Exception:
                            _age_str = "unknown"
                            _stale   = False

                        _dot = {"running": "🟢", "starting": "🟡", "paused_market_closed": "⏰"}.get(_hb_state, "⚫")
                        st.metric("State", f"{_dot} {_hb_state.upper()}")
                        _f = _hb_data.get("fetch_seconds")
                        _c = _hb_data.get("compute_seconds")
                        st.metric("Last Cycle", f"{float(_f)+float(_c):.1f}s" if (_f and _c) else "—")
                        st.caption(f"Age: {_age_str}")
                        if _stale:
                            st.warning("⚠️ Stale >10m", icon="⚠️")
                    else:
                        st.caption("No heartbeat yet")
                except Exception as _hbe:
                    st.caption(f"Error: {str(_hbe)[:60]}")
        _conn_hb.close()
    except Exception as _he:
        st.error(f"Could not read heartbeats: {_he}")

    st.divider()

    # ── Section 3: Last 10 Engine Cycles (per strategy) ─────────────────────
    st.markdown("### 🔄 Recent Engine Cycles")
    try:
        import psycopg2 as _pg3
        from psycopg2.extras import RealDictCursor as _RDC3
        _db_url3 = _get_db_url()
        _conn3 = _pg3.connect(_db_url3, cursor_factory=_RDC3, connect_timeout=8)

        for _cyc_sc in strategies:
            st.markdown(f"**{_cyc_sc.name}** (`{_cyc_sc.suffix}`):")
            try:
                with _conn3.cursor() as _cur3:
                    _cur3.execute("""
                        SELECT
                            run_started_at,
                            status,
                            symbols_total,
                            symbols_fetched,
                            ROUND(fetch_seconds::numeric,    1) AS fetch_s,
                            ROUND(compute_seconds::numeric,  1) AS compute_s,
                            ROUND(duration_seconds::numeric, 1) AS total_s,
                            error
                        FROM run_metrics
                        WHERE suffix = %s
                        ORDER BY id DESC
                        LIMIT 10
                    """, (_cyc_sc.suffix,))
                    _cyc_rows = _cur3.fetchall()

                if _cyc_rows:
                    _cyc_df = pd.DataFrame([dict(r) for r in _cyc_rows])
                    def _cyc_color(val):
                        if val == "OK":     return "color:#4ade80;font-weight:700"
                        if val == "FAILED": return "color:#f87171;font-weight:700"
                        return ""
                    st.dataframe(
                        _cyc_df.style.applymap(_cyc_color, subset=["status"]),
                        use_container_width=True, hide_index=True,
                    )
                    _n_ok   = sum(1 for r in _cyc_rows if r["status"] == "OK")
                    _n_fail = sum(1 for r in _cyc_rows if r["status"] == "FAILED")
                    sc1, sc2 = st.columns(2)
                    sc1.metric("✅ OK cycles",     _n_ok)
                    sc2.metric("❌ Failed cycles", _n_fail)
                else:
                    st.caption("No cycles yet for this strategy.")
            except Exception as _cye:
                st.caption(f"Error reading cycles: {str(_cye)[:80]}")

        _conn3.close()
    except Exception as _ce:
        st.error(f"Could not read cycle history: {_ce}")

    st.divider()

    # ── Section 4: Supabase Table Row Counts ──────────────────────────────────
    st.markdown("### 🗄️ Supabase Table Sizes")
    try:
        from stocking_app.config import load_config as _lc5
        from stocking_app.db import TradingRepository as _TR5
        _cfg5   = _lc5()
        _repo5  = _TR5(_cfg5.database_url or _cfg5.db_path)
        _tables = [
            "universe", "candles_5m", "signals",
            "positions_ledger", "trade_activity_log",
            "pnl_snapshots", "run_metrics",
        ]
        _counts = {}
        for _t in _tables:
            try:
                _df_t = _repo5.read_df(f"SELECT COUNT(*) AS n FROM {_t}")
                _counts[_t] = int(_df_t["n"].iloc[0]) if not _df_t.empty else 0
            except Exception:
                _counts[_t] = "—"
        _repo5.close()
        _tbl_cols = st.columns(len(_tables))
        for _col, (_tname, _cnt) in zip(_tbl_cols, _counts.items()):
            _col.metric(_tname, f"{_cnt:,}" if isinstance(_cnt, int) else _cnt)
    except Exception as _te:
        st.error(f"Could not read table counts: {_te}")

    st.divider()

    # ── Section 5: UptimeRobot Setup Guide ───────────────────────────────────
    st.markdown("### ⚙️ Uptime Monitoring Setup")
    with st.expander("How to set up free UptimeRobot alerts (5 min)"):
        _render_svc = os.environ.get("RENDER_HEALTH_URL", "https://YOUR-SERVICE.onrender.com")
        st.markdown(f"""
1. Go to [uptimerobot.com](https://uptimerobot.com) → create a free account
2. Click **+ Add New Monitor**
3. **Monitor Type** → `HTTP(s)`
4. **URL** → `{_render_svc}`
5. **Monitoring Interval** → `5 minutes`
6. Add your email under **Alert Contacts**
7. Save → done ✅

You'll receive an email if the Render engine process goes down.

**To enable the Render ping check on this page**, add to your **Streamlit Cloud secrets**:
```toml
RENDER_HEALTH_URL = "{_render_svc}"
```
""")


# ── Auto-refresh ──────────────────────────────────────────────────────────────
# Handled by st_autorefresh in sidebar
