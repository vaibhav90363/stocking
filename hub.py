#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from stocking_app.config import load_config
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
        from dashboard import run_dashboard
        st.session_state["_hub_hosted"] = True
        run_dashboard(str(strategy_path))
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
    """Read live state using TradingRepository (pooled connections).

    BUG-TIMEOUT-11 fix: previously used raw psycopg2.connect() per card,
    creating un-pooled connections that could exhaust Supabase limits.
    """
    from stocking_app.db import TradingRepository
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
        repo = TradingRepository(db_url, suffix=sc.suffix)
        # Read heartbeat
        try:
            hb = repo.get_engine_heartbeat()
            if hb:
                result["engine_state"] = hb.get("state", "offline")
                result["last_run"]     = hb.get("last_run")
        except Exception:
            pass
        # Read last cycle metrics
        try:
            metrics_df = repo.read_df(
                "SELECT status, symbols_fetched, symbols_total FROM run_metrics "
                "WHERE suffix=%s ORDER BY id DESC LIMIT 1",
                (sc.suffix,)
            )
            if not metrics_df.empty:
                row = metrics_df.iloc[0]
                result["last_cycle_status"] = str(row.get("status", "—"))
                result["symbols_fetched"]   = int(row.get("symbols_fetched", 0))
                result["symbols_total"]     = int(row.get("symbols_total", 0))
        except Exception:
            pass
        # Read PnL
        try:
            pnl_df = repo.read_df(
                "SELECT realized_pnl, unrealized_pnl, open_positions FROM pnl_snapshots "
                "WHERE suffix=%s ORDER BY ts DESC LIMIT 1",
                (sc.suffix,)
            )
            if not pnl_df.empty:
                row = pnl_df.iloc[0]
                result["realized_pnl"]   = float(row.get("realized_pnl", 0))
                result["unrealized_pnl"] = float(row.get("unrealized_pnl", 0))
                result["open_positions"] = int(row.get("open_positions", 0))
        except Exception:
            pass
        repo.close()
    except Exception:
        pass
    return result


_BENCHMARKS = {
    ".US": ("Nasdaq 100", "^NDX", ["QQQ", "^IXIC"]),
    ".L":  ("FTSE All-Share", "^FTAS", ["^FTSE", "ISF.L"]),
    ".NS": ("Nifty 500", "^CRSLDX", ["^NSEI", "INDY"]),
}


def _fmt_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):,.2f}%"


def _fmt_num(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{float(value):,.2f}"


def _compound_returns(returns: pd.Series) -> tuple[float | None, int]:
    factor = 1.0
    count = 0
    for value in returns.dropna():
        try:
            r = float(value)
        except Exception:
            continue
        if not math.isfinite(r):
            continue
        factor *= (1.0 + r)
        count += 1
    if count == 0:
        return None, 0
    return factor - 1.0, count


def _fetch_benchmark_close(symbol: str, start_date: str, end_date: str, fallbacks: tuple[str, ...]) -> tuple[str | None, pd.Series]:
    """Fetch daily benchmark closes from Yahoo Finance."""
    try:
        import yfinance as yf
    except Exception:
        return None, pd.Series(dtype=float)

    start = pd.Timestamp(start_date).date()
    end = pd.Timestamp(end_date).date() + timedelta(days=4)
    for ticker in (symbol, *fallbacks):
        try:
            df = yf.download(
                ticker,
                start=str(start),
                end=str(end),
                interval="1d",
                auto_adjust=True,
                progress=False,
                threads=False,
            )
            if df is None or df.empty:
                continue
            close = df["Close"]
            if isinstance(close, pd.DataFrame):
                close = close.iloc[:, 0]
            close = close.dropna()
            if close.empty:
                continue
            close.index = pd.to_datetime(close.index).date
            close = close[close.index <= pd.Timestamp(end_date).date()]
            if len(close) >= 2:
                return ticker, close
        except Exception:
            continue
    return None, pd.Series(dtype=float)


def _deployed_capital_series(trades: pd.DataFrame, dates: list) -> tuple[pd.Series, float, float, float]:
    """Reconstruct deployed cost basis through time from BUY/SELL rows."""
    if trades.empty:
        return pd.Series(dtype=float), 0.0, 0.0, 0.0

    rows_by_day: dict[object, list[dict]] = defaultdict(list)
    for row in trades.to_dict("records"):
        rows_by_day[pd.Timestamp(row["ts"]).date()].append(row)

    all_dates = sorted(set(dates) | set(rows_by_day.keys()))
    pos_qty: dict[str, float] = defaultdict(float)
    pos_cost: dict[str, float] = defaultdict(float)
    values: dict[object, float] = {}
    peak = 0.0
    gross_buys = 0.0

    for day in all_dates:
        for row in rows_by_day.get(day, []):
            symbol = str(row["symbol"])
            side = str(row["side"])
            qty = float(row["qty"])
            price = float(row["price"])
            notional = qty * price
            if side == "BUY":
                pos_qty[symbol] += qty
                pos_cost[symbol] += notional
                gross_buys += notional
            elif side.startswith("SELL") and pos_qty[symbol] > 0:
                avg_cost = pos_cost[symbol] / pos_qty[symbol]
                close_qty = min(qty, pos_qty[symbol])
                pos_qty[symbol] -= close_qty
                pos_cost[symbol] -= close_qty * avg_cost
                if pos_qty[symbol] <= 1e-9:
                    pos_qty[symbol] = 0.0
                    pos_cost[symbol] = 0.0

        deployed = float(sum(pos_cost.values()))
        peak = max(peak, deployed)
        values[day] = deployed

    raw = pd.Series(values).sort_index()
    current = float(raw.iloc[-1]) if not raw.empty else 0.0
    if not dates:
        return raw, peak, gross_buys, current

    aligned_index = sorted(set(raw.index) | set(dates))
    aligned = raw.reindex(aligned_index).sort_index().ffill().reindex(dates).fillna(0.0)
    return aligned, peak, gross_buys, current


def _read_strategy_performance(sc: StrategyConfig) -> dict:
    """Compute live strategy-vs-index performance from Supabase/Postgres."""
    from stocking_app.db import TradingRepository

    db_url = _get_db_url()
    benchmark_name, benchmark_symbol, fallbacks = _BENCHMARKS.get(
        sc.suffix,
        (f"{sc.suffix} benchmark", "", []),
    )
    result = {
        "strategy": sc.name,
        "folder": sc.strategy_dir.name,
        "suffix": sc.suffix,
        "benchmark": benchmark_name,
        "benchmark_symbol": benchmark_symbol,
        "error": None,
    }
    if not db_url:
        result["error"] = "DATABASE_URL is not configured."
        return result

    try:
        repo = TradingRepository(db_url, suffix=sc.suffix)
        trades = repo.read_df(
            """
            SELECT id, symbol, side, qty, price, ts, pnl, reason
            FROM trade_activity_log
            WHERE symbol LIKE %s
            ORDER BY ts ASC, id ASC
            """,
            (f"%{sc.suffix}",),
        )
        snapshots = repo.read_df(
            """
            SELECT ts, realized_pnl, unrealized_pnl, total_pnl, open_positions
            FROM pnl_snapshots
            WHERE suffix = %s
            ORDER BY ts ASC
            """,
            (sc.suffix,),
        )
        repo.close()
    except Exception as exc:
        result["error"] = f"DB read failed: {exc}"
        return result

    if trades.empty or snapshots.empty:
        result["error"] = "No trades or P&L snapshots yet."
        return result

    trades["ts"] = pd.to_datetime(trades["ts"], errors="coerce")
    snapshots["ts"] = pd.to_datetime(snapshots["ts"], errors="coerce")
    trades = trades.dropna(subset=["ts"])
    snapshots = snapshots.dropna(subset=["ts"])
    if trades.empty or snapshots.empty:
        result["error"] = "Trade or P&L timestamps are invalid."
        return result

    first_trade = trades["ts"].min().date()
    latest_ts = snapshots["ts"].max()
    latest_date = latest_ts.date()
    latest = snapshots.sort_values("ts").iloc[-1]

    bench_used = None
    close = pd.Series(dtype=float)
    if benchmark_symbol:
        bench_used, close = _fetch_benchmark_close(
            benchmark_symbol,
            str(first_trade),
            str(latest_date),
            tuple(fallbacks),
        )

    dates = list(close.index) if not close.empty else list(pd.date_range(first_trade, latest_date, freq="D").date)
    deployed, peak_deployed, gross_buys, current_deployed = _deployed_capital_series(trades, dates)
    avg_deployed = float(deployed.mean()) if not deployed.empty else 0.0
    total_pnl = float(latest.get("total_pnl", 0.0))
    realized = float(latest.get("realized_pnl", 0.0))
    unrealized = float(latest.get("unrealized_pnl", 0.0))

    snap_daily = snapshots.sort_values("ts").groupby(snapshots["ts"].dt.date).tail(1)
    daily_pnl = snap_daily.set_index(snap_daily["ts"].dt.date)["total_pnl"].astype(float)
    if dates:
        pnl_aligned = (
            daily_pnl.reindex(sorted(set(daily_pnl.index) | set(dates)))
            .sort_index()
            .ffill()
            .reindex(dates)
            .fillna(0.0)
        )
    else:
        pnl_aligned = daily_pnl
    pnl_delta = pnl_aligned.diff().fillna(pnl_aligned)
    prev_deployed = deployed.shift(1).fillna(0.0) if not deployed.empty else pd.Series(dtype=float)
    avg_day_deployed = ((prev_deployed + deployed) / 2.0).replace(0.0, pd.NA) if not deployed.empty else pd.Series(dtype=float)
    exposure_returns = (pnl_delta / avg_day_deployed).replace([float("inf"), -float("inf")], pd.NA).dropna()
    exposure_compounded, exposure_days = _compound_returns(exposure_returns)

    index_return = None
    exposure_matched_index = None
    benchmark_first = None
    benchmark_last = None
    benchmark_first_date = None
    benchmark_last_date = None
    if not close.empty and peak_deployed > 0:
        benchmark_first = float(close.iloc[0])
        benchmark_last = float(close.iloc[-1])
        benchmark_first_date = str(close.index[0])
        benchmark_last_date = str(close.index[-1])
        index_return = (benchmark_last / benchmark_first) - 1.0
        index_daily = close.pct_change().reindex(dates).fillna(0.0)
        exposure_fraction = (prev_deployed / peak_deployed).clip(0, 1)
        exposure_matched_index, _ = _compound_returns((index_daily * exposure_fraction).iloc[1:])

    closed = trades[trades["side"].astype(str).str.startswith("SELL")].copy()
    buys = trades[trades["side"].astype(str) == "BUY"].copy()
    wins = int((closed["pnl"].astype(float) > 0).sum()) if not closed.empty else 0
    losses = int((closed["pnl"].astype(float) < 0).sum()) if not closed.empty else 0
    result.update({
        "first_trade": str(first_trade),
        "latest_ts": latest_ts.isoformat(),
        "latest_date": str(latest_date),
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "total_pnl": total_pnl,
        "open_positions": int(latest.get("open_positions", 0)),
        "buys": int(len(buys)),
        "sells": int(len(closed)),
        "trade_rows": int(len(trades)),
        "wins": wins,
        "losses": losses,
        "win_rate_pct": (wins / len(closed) * 100.0) if len(closed) else None,
        "avg_closed_pnl": float(closed["pnl"].astype(float).mean()) if not closed.empty else None,
        "median_closed_pnl": float(closed["pnl"].astype(float).median()) if not closed.empty else None,
        "peak_deployed": peak_deployed,
        "avg_deployed": avg_deployed,
        "current_deployed": current_deployed,
        "gross_buy_notional": gross_buys,
        "yield_on_peak_pct": (total_pnl / peak_deployed * 100.0) if peak_deployed else None,
        "return_on_avg_deployed_pct": (total_pnl / avg_deployed * 100.0) if avg_deployed else None,
        "exposure_adjusted_compounded_pct": (exposure_compounded * 100.0) if exposure_compounded is not None else None,
        "exposure_adjusted_days": exposure_days,
        "benchmark_symbol": bench_used or benchmark_symbol,
        "benchmark_first": benchmark_first,
        "benchmark_last": benchmark_last,
        "benchmark_first_date": benchmark_first_date,
        "benchmark_last_date": benchmark_last_date,
        "index_fully_invested_pct": (index_return * 100.0) if index_return is not None else None,
        "index_exposure_matched_pct": (exposure_matched_index * 100.0) if exposure_matched_index is not None else None,
        "excess_exposure_strategy_vs_index_pp": (
            (exposure_compounded - exposure_matched_index) * 100.0
            if exposure_compounded is not None and exposure_matched_index is not None else None
        ),
        "excess_vs_full_index_pp": (
            (total_pnl / peak_deployed - index_return) * 100.0
            if peak_deployed and index_return is not None else None
        ),
        "excess_vs_exposure_matched_pp": (
            (total_pnl / peak_deployed - exposure_matched_index) * 100.0
            if peak_deployed and exposure_matched_index is not None else None
        ),
        "top_closed_winners": closed.sort_values("pnl", ascending=False).head(10),
        "top_closed_losers": closed.sort_values("pnl", ascending=True).head(10),
    })
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
tab_strategies, tab_perf, tab_health, tab_reset = st.tabs([
    "📊 Strategies",
    "📈 Performance",
    "🩺 System Health",
    "🧹 Reset Data",
])


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
                # BUG-LOGS-10 fix: read logs from Supabase system_logs table instead of local file.
                # Local engine.log doesn't exist on Streamlit Cloud or Render.
                try:
                    from stocking_app.db import TradingRepository as _TRLog
                    _cfg_log = load_config()
                    _repo_log = _TRLog(_cfg_log.database_url or _cfg_log.db_path, suffix=sc.suffix)
                    _log_df = _repo_log.get_recent_logs(limit=10)
                    _repo_log.close()
                    if not _log_df.empty:
                        lines = _log_df["message"].tolist()
                        with st.expander("📋 Last log lines"):
                            st.code("\n".join(lines), language=None)
                    else:
                        # Fallback: try local file if no DB logs
                        log_f = sc.log_dir / "engine.log"
                        if log_f.exists():
                            lines = log_f.read_text(errors="replace").splitlines()[-10:]
                            with st.expander("📋 Last log lines (local)"):
                                st.code("\n".join(reversed(lines)), language=None)
                        else:
                            st.caption("No logs yet")
                except Exception:
                    # Final fallback: local file
                    log_f = sc.log_dir / "engine.log"
                    if log_f.exists():
                        lines = log_f.read_text(errors="replace").splitlines()[-10:]
                        with st.expander("📋 Last log lines (local)"):
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
                            trades_df.style.map(_side_color, subset=["side"])
                                           .map(_pnl_clr,    subset=["pnl"]),
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
            .map(_state_color, subset=["State"])
            .map(_pnl_color,   subset=["Realized P&L", "Unrealized", "Total P&L"]),
        use_container_width=True, hide_index=True,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — PERFORMANCE
# ═══════════════════════════════════════════════════════════════════════════════
with tab_perf:
    st.markdown("## Strategy Performance vs Index")
    st.caption(
        "Live Supabase data and benchmark prices are recomputed on every dashboard rerun."
    )
    if st.button("🔄 Refresh performance", key="perf_refresh"):
        st.rerun()

    with st.spinner("Reading Supabase trades and benchmark prices …"):
        perf_results = [_read_strategy_performance(sc) for sc in strategies]

    summary_rows = []
    for perf in perf_results:
        summary_rows.append({
            "Strategy": perf.get("strategy"),
            "Exchange": perf.get("suffix"),
            "Period": (
                f"{perf.get('first_trade')} → {perf.get('latest_date')}"
                if not perf.get("error") else "—"
            ),
            "Total P&L": round(perf.get("total_pnl", 0.0), 2) if not perf.get("error") else None,
            "Peak Capital": round(perf.get("peak_deployed", 0.0), 2) if not perf.get("error") else None,
            "Strict Peak Yield": round(perf.get("yield_on_peak_pct", 0.0), 2) if perf.get("yield_on_peak_pct") is not None else None,
            "Avg Deployed Return": round(perf.get("return_on_avg_deployed_pct", 0.0), 2) if perf.get("return_on_avg_deployed_pct") is not None else None,
            "Exposure Strategy": round(perf.get("exposure_adjusted_compounded_pct", 0.0), 2) if perf.get("exposure_adjusted_compounded_pct") is not None else None,
            "Full Index": round(perf.get("index_fully_invested_pct", 0.0), 2) if perf.get("index_fully_invested_pct") is not None else None,
            "Exposure Index": round(perf.get("index_exposure_matched_pct", 0.0), 2) if perf.get("index_exposure_matched_pct") is not None else None,
            "Strict Alpha vs Exposure": round(perf.get("excess_vs_exposure_matched_pp", 0.0), 2) if perf.get("excess_vs_exposure_matched_pp") is not None else None,
            "Exposure Alpha": round(perf.get("excess_exposure_strategy_vs_index_pp", 0.0), 2) if perf.get("excess_exposure_strategy_vs_index_pp") is not None else None,
            "Win Rate": round(perf.get("win_rate_pct", 0.0), 1) if perf.get("win_rate_pct") is not None else None,
            "Error": perf.get("error") or "",
        })

    summary_df = pd.DataFrame(summary_rows)
    st.markdown("### Summary")

    def _perf_color(val):
        try:
            v = float(val)
            return "color:#4ade80" if v > 0 else ("color:#f87171" if v < 0 else "")
        except Exception:
            return ""

    pct_cols = [
        "Strict Peak Yield",
        "Avg Deployed Return",
        "Exposure Strategy",
        "Full Index",
        "Exposure Index",
        "Strict Alpha vs Exposure",
        "Exposure Alpha",
        "Win Rate",
    ]
    num_cols = ["Total P&L", "Peak Capital"]
    if not summary_df.empty:
        st.dataframe(
            summary_df.style
                .map(_perf_color, subset=[
                    "Total P&L",
                    "Strict Peak Yield",
                    "Avg Deployed Return",
                    "Exposure Strategy",
                    "Full Index",
                    "Exposure Index",
                    "Strict Alpha vs Exposure",
                    "Exposure Alpha",
                ])
                .format({c: "{:,.2f}" for c in num_cols if c in summary_df.columns})
                .format({c: "{:,.2f}%" for c in pct_cols if c in summary_df.columns}),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()
    market_tabs = st.tabs([f"{p.get('suffix', '')} {p.get('strategy', 'Strategy').split('—')[-1].strip()}" for p in perf_results])

    for subtab, perf in zip(market_tabs, perf_results):
        with subtab:
            st.markdown(f"### {perf.get('strategy')}")
            st.caption(f"Folder: `{perf.get('folder')}`  ·  Exchange: `{perf.get('suffix')}`")
            if perf.get("error"):
                st.warning(perf["error"])
                continue

            st.caption(
                f"Period: `{perf.get('first_trade')}` to `{perf.get('latest_date')}`  ·  "
                f"Benchmark: `{perf.get('benchmark')}` / `{perf.get('benchmark_symbol')}`"
            )
            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric("Total P&L", _fmt_num(perf.get("total_pnl")), delta=_fmt_num(perf.get("total_pnl")))
            k2.metric("Strict Peak Yield", _fmt_pct(perf.get("yield_on_peak_pct")))
            k3.metric("Avg Deployed Return", _fmt_pct(perf.get("return_on_avg_deployed_pct")))
            k4.metric("Full Index Return", _fmt_pct(perf.get("index_fully_invested_pct")))
            k5.metric("Exposure Index", _fmt_pct(perf.get("index_exposure_matched_pct")))

            a1, a2, a3, a4 = st.columns(4)
            a1.metric("Alpha vs Full Index", _fmt_pct(perf.get("excess_vs_full_index_pp")))
            a2.metric("Strict Alpha vs Exposure", _fmt_pct(perf.get("excess_vs_exposure_matched_pp")))
            a3.metric("Exposure Strategy", _fmt_pct(perf.get("exposure_adjusted_compounded_pct")))
            a4.metric("Exposure Alpha", _fmt_pct(perf.get("excess_exposure_strategy_vs_index_pp")))

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Peak Capital", _fmt_num(perf.get("peak_deployed")))
            c2.metric("Average Deployed", _fmt_num(perf.get("avg_deployed")))
            c3.metric("Current Deployed", _fmt_num(perf.get("current_deployed")))
            c4.metric("Gross Buy Notional", _fmt_num(perf.get("gross_buy_notional")))
            c5.metric("Open Positions", perf.get("open_positions", 0))

            chart_values = pd.DataFrame({
                "Return %": {
                    "Strict peak yield": perf.get("yield_on_peak_pct"),
                    "Avg deployed return": perf.get("return_on_avg_deployed_pct"),
                    "Exposure strategy": perf.get("exposure_adjusted_compounded_pct"),
                    "Fully invested index": perf.get("index_fully_invested_pct"),
                    "Exposure-matched index": perf.get("index_exposure_matched_pct"),
                }
            }).dropna()
            if not chart_values.empty:
                st.bar_chart(chart_values)

            st.markdown("#### Trade Distribution")
            d1, d2, d3, d4, d5 = st.columns(5)
            d1.metric("Buys", perf.get("buys", 0))
            d2.metric("Closed Sells", perf.get("sells", 0))
            d3.metric("Win Rate", _fmt_pct(perf.get("win_rate_pct")))
            d4.metric("Wins / Losses", f"{perf.get('wins', 0)} / {perf.get('losses', 0)}")
            d5.metric("Median Closed P&L", _fmt_num(perf.get("median_closed_pnl")))

            top_winners = perf.get("top_closed_winners")
            top_losers = perf.get("top_closed_losers")
            w_col, l_col = st.columns(2)
            with w_col:
                with st.expander("Top closed winners", expanded=True):
                    if isinstance(top_winners, pd.DataFrame) and not top_winners.empty:
                        st.dataframe(
                            top_winners[["symbol", "qty", "price", "ts", "pnl", "reason"]]
                                .assign(ts=lambda df: pd.to_datetime(df["ts"]).dt.strftime("%Y-%m-%d")),
                            use_container_width=True,
                            hide_index=True,
                        )
                    else:
                        st.caption("No closed winners yet.")
            with l_col:
                with st.expander("Top closed losers", expanded=True):
                    if isinstance(top_losers, pd.DataFrame) and not top_losers.empty:
                        st.dataframe(
                            top_losers[["symbol", "qty", "price", "ts", "pnl", "reason"]]
                                .assign(ts=lambda df: pd.to_datetime(df["ts"]).dt.strftime("%Y-%m-%d")),
                            use_container_width=True,
                            hide_index=True,
                        )
                    else:
                        st.caption("No closed losers yet.")

            with st.expander("Definitions"):
                st.markdown(
                    """
                    - **Strict Peak Yield** = latest total P&L / maximum deployed cost basis at any time.
                    - **Average Deployed Return** = latest total P&L / average deployed cost basis across benchmark trading days.
                    - **Exposure Strategy** compounds daily P&L change over average deployed capital for that day.
                    - **Exposure Index** compounds the benchmark only at the strategy's deployed-capital fraction.
                    - **Strict Alpha vs Exposure** compares strict peak yield to the exposure-matched benchmark return.
                    - **Exposure Alpha** compares exposure-adjusted strategy compounding to the exposure-matched benchmark return.
                    """
                )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — SYSTEM HEALTH
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
        from stocking_app.db import TradingRepository as _TRHB
        from stocking_app.config import load_config as _LCHB
        _cfg_hb = _LCHB()
        
        _hb_cols = st.columns(max(1, len(strategies)))
        for _hb_idx, _hb_sc in enumerate(strategies):
            with _hb_cols[_hb_idx]:
                st.markdown(f"**{_hb_sc.suffix}** — {_hb_sc.name.split('—')[-1].strip()}")
                try:
                    _repo_hb = _TRHB(_cfg_hb.database_url or _cfg_hb.db_path, suffix=_hb_sc.suffix)
                    _hb_row = None
                    # Use repo method instead of raw SQL
                    _hb_data = _repo_hb.get_engine_heartbeat()
                    
                    # We still need the updated_at from the table for age calculation
                    _hb_raw = _repo_hb.read_df(
                        "SELECT updated_at FROM engine_state WHERE key=%s",
                        (f"engine_heartbeat_{_hb_sc.suffix}",)
                    )
                    _repo_hb.close()

                    if _hb_data and not _hb_raw.empty:
                        _hb_upd   = str(_hb_raw.iloc[0]["updated_at"])
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
    except Exception as _he:
        st.error(f"Could not read heartbeats: {_he}")

    st.divider()

    # ── Section 3: Last 10 Engine Cycles (per strategy) ─────────────────────
    st.markdown("### 🔄 Recent Engine Cycles")
    try:
        from stocking_app.db import TradingRepository as _TRC
        _cfg_c = _LCHB()
        
        for _cyc_sc in strategies:
            st.markdown(f"**{_cyc_sc.name}** (`{_cyc_sc.suffix}`):")
            try:
                _repo_c = _TRC(_cfg_c.database_url or _cfg_c.db_path, suffix=_cyc_sc.suffix)
                _cyc_df = _repo_c.read_df("""
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
                _repo_c.close()

                if not _cyc_df.empty:
                    def _cyc_color(val):
                        if val == "OK":     return "color:#4ade80;font-weight:700"
                        if val == "FAILED": return "color:#f87171;font-weight:700"
                        return ""
                    st.dataframe(
                        _cyc_df.style.map(_cyc_color, subset=["status"]),
                        use_container_width=True, hide_index=True,
                    )
                    _n_ok   = (_cyc_df["status"] == "OK").sum()
                    _n_fail = (_cyc_df["status"] == "FAILED").sum()
                    sc1, sc2 = st.columns(2)
                    sc1.metric("✅ OK cycles",     int(_n_ok))
                    sc2.metric("❌ Failed cycles", int(_n_fail))
                else:
                    st.caption("No cycles yet for this strategy.")
            except Exception as _cye:
                st.caption(f"Error reading cycles: {str(_cye)[:80]}")
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
            "universe", "signals",
            "positions_ledger", "trade_activity_log",
            "pnl_snapshots", "run_metrics", "candles_1d",
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


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — RESET DATA
# ═══════════════════════════════════════════════════════════════════════════════
with tab_reset:
    st.markdown("## 🧹 Reset Trading Data")
    st.caption(
        "Wipe positions, trades, signals, PnL snapshots, metrics, and logs. "
        "**Universe** (symbol list), **engine state**, and **candle data** are preserved."
    )

    st.divider()

    # ── Row counts before reset ────────────────────────────────────────────────
    st.markdown("### 📊 Current Data Volumes")
    try:
        from stocking_app.db import TradingRepository as _TRR
        from stocking_app.config import load_config as _LCR
        _cfg_r = _LCR()
        _repo_r = _TRR(_cfg_r.database_url or _cfg_r.db_path)

        _reset_tables = [
            "positions_ledger", "trade_activity_log", "signals",
            "pnl_snapshots", "symbol_state", "run_metrics", "system_logs",
        ]
        _row_counts: dict[str, int] = {}
        for _rt in _reset_tables:
            try:
                _rc_df = _repo_r.read_df(f"SELECT COUNT(*) AS n FROM {_rt}")
                _row_counts[_rt] = int(_rc_df["n"].iloc[0]) if not _rc_df.empty else 0
            except Exception:
                _row_counts[_rt] = 0
        _repo_r.close()

        _rc_cols = st.columns(len(_reset_tables))
        for _col, (_tbl, _cnt) in zip(_rc_cols, _row_counts.items()):
            _tbl_short = _tbl.replace("_", " ").title()
            _col.metric(_tbl_short, f"{_cnt:,}")

        _total_rows = sum(v for v in _row_counts.values() if isinstance(v, int))
    except Exception as _re:
        st.error(f"Could not read table counts: {_re}")
        _total_rows = 0

    st.divider()

    # ── Per-strategy reset ─────────────────────────────────────────────────────
    st.markdown("### 🎯 Reset Single Strategy")
    st.caption("Only deletes data for the selected strategy's suffix.")

    _strat_options = {f"{sc.name} ({sc.suffix})": sc.suffix for sc in strategies}
    _selected_label = st.selectbox(
        "Select strategy to reset",
        options=list(_strat_options.keys()),
        key="reset_single_strategy_select",
    )
    _selected_suffix = _strat_options[_selected_label] if _selected_label else None

    _confirm_single = st.checkbox(
        f"I understand this will **permanently delete** all trading data for `{_selected_suffix}`",
        key="reset_single_confirm",
    )

    if st.button(
        f"🗑 Reset {_selected_label}",
        key="reset_single_btn",
        type="primary",
        disabled=not _confirm_single,
        use_container_width=True,
    ):
        try:
            _cfg_rs = _LCR()
            _repo_rs = _TRR(_cfg_rs.database_url or _cfg_rs.db_path, suffix=_selected_suffix)
            deleted = _repo_rs.reset_trading_data(suffix_filter=_selected_suffix)
            _repo_rs.close()

            total_del = sum(deleted.values())
            st.success(f"✅ Reset complete for `{_selected_suffix}` — **{total_del:,}** rows deleted.")
            with st.expander("Details"):
                for tbl, cnt in deleted.items():
                    st.write(f"  `{tbl}`: {cnt:,} rows deleted")
            time.sleep(1)
            st.rerun()
        except Exception as _rse:
            st.error(f"Reset failed: {_rse}")

    st.divider()

    # ── Full reset (all strategies) ────────────────────────────────────────────
    st.markdown("### ☢️ Reset ALL Strategies")
    st.warning(
        "**This will permanently delete ALL trading data across ALL strategies.** "
        "Positions, trades, signals, PnL, metrics, and logs will be wiped. "
        "Make sure all engines are **stopped** before proceeding.",
        icon="⚠️",
    )

    _confirm_all_1 = st.checkbox(
        "I confirm I want to delete **all** trading data for **every** strategy",
        key="reset_all_confirm_1",
    )
    _confirm_all_2 = st.checkbox(
        f"I understand this will remove **{_total_rows:,}** rows and cannot be undone",
        key="reset_all_confirm_2",
    )

    if st.button(
        "☢️ RESET EVERYTHING",
        key="reset_all_btn",
        type="primary",
        disabled=not (_confirm_all_1 and _confirm_all_2),
        use_container_width=True,
    ):
        try:
            _cfg_ra = _LCR()
            _repo_ra = _TRR(_cfg_ra.database_url or _cfg_ra.db_path)
            deleted = _repo_ra.reset_trading_data(suffix_filter=None)
            _repo_ra.close()

            total_del = sum(deleted.values())
            st.success(f"✅ Full reset complete — **{total_del:,}** rows deleted across all strategies.")
            with st.expander("Details"):
                for tbl, cnt in deleted.items():
                    st.write(f"  `{tbl}`: {cnt:,} rows deleted")
            time.sleep(1)
            st.rerun()
        except Exception as _rae:
            st.error(f"Reset failed: {_rae}")


# ── Auto-refresh ──────────────────────────────────────────────────────────────
# Handled by st_autorefresh in sidebar
