#!/usr/bin/env python3
"""
run_strategy.py — Universal strategy runner.

Usage:
  python run_strategy.py <strategy_dir> --mode backtest
  python run_strategy.py <strategy_dir> --mode live
  python run_strategy.py <strategy_dir> --mode dashboard [--port 8501]
  python run_strategy.py <strategy_dir> --mode all        # backtest then live

Examples:
  python run_strategy.py strategies/fractal_momentum_nse --mode backtest
  python run_strategy.py strategies/fractal_momentum_lse --mode live
  python run_strategy.py strategies/fractal_momentum_nse --mode dashboard --port 8501
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from stocking_app.strategy_loader import load_strategy


def cmd_backtest(sc, args):
    print(f"\n{'═'*60}")
    print(f"  BACKTEST — {sc.name}")
    print(f"  Universe : {sc.universe_csv}  ({_count_csv(sc.universe_csv)} symbols)")
    print(f"  Exchange : {sc.timezone}  |  Suffix: {sc.suffix}")
    print(f"  Output   : {sc.backtest_dir}")
    print(f"{'═'*60}\n")

    if not sc.universe_csv.exists():
        print(f"ERROR: universe.csv not found at {sc.universe_csv}")
        sys.exit(1)

    cmd = [
        sys.executable, str(ROOT / "backtest_sim.py"),
        "--csv",          str(sc.universe_csv),
        "--suffix",       sc.suffix,
        "--exchange-tz",  sc.timezone,
        "--capital",      str(sc.parameters.get("capital_per_trade", 100_000)),
        "--backtest-days",str(sc.backtest_days),
        "--db",           str(sc.backtest_dir / "backtest.db"),
        "--report",       str(sc.backtest_dir / "report.txt"),
        "--trades",       str(sc.backtest_dir / "trades.csv"),
    ]
    if args.skip_download:
        cmd.append("--skip-download")

    print(f"Running: {' '.join(cmd)}\n")
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


def cmd_live(sc, args):
    print(f"\n{'═'*60}")
    print(f"  LIVE ENGINE — {sc.name}")
    print(f"  DB       : {sc.db_path}")
    print(f"  Logs     : {sc.log_dir / 'engine.log'}")
    print(f"  Suffix   : {sc.suffix}  |  TZ: {sc.timezone}")
    print(f"  Cycle    : {sc.cycle_seconds}s")
    print(f"{'═'*60}\n")

    if not sc.universe_csv.exists():
        print(f"ERROR: universe.csv not found at {sc.universe_csv}")
        sys.exit(1)

    # Set env vars for the stocking_app to pick up via load_config()
    env = os.environ.copy()
    env.update({
        "STOCKING_DB_PATH":              str(sc.db_path),
        "STOCKING_TICKER_SUFFIX":        sc.suffix,
        "STOCKING_CYCLE_SECONDS":        str(sc.cycle_seconds),
        "STOCKING_FETCH_LOOKBACK_DAYS":  str(sc.fetch_lookback_days),
        "STOCKING_COMPUTE_LOOKBACK_DAYS":str(sc.compute_lookback_days),
        "STOCKING_FETCH_CONCURRENCY":    str(sc.fetch_concurrency),
        "STOCKING_COMPUTE_WORKERS":      str(sc.compute_workers),
        "STOCKING_ORDER_QTY":            str(sc.order_qty),
        "STOCKING_LOG_DIR":              str(sc.log_dir),
        "STOCKING_EXCHANGE_TZ":          str(sc.timezone),
        "STOCKING_MARKET_OPEN":          str(sc.market_open),
        "STOCKING_MARKET_CLOSE":         str(sc.market_close),
    })

    # Let Supabase db_url pass through to child processes if it exists
    if "DATABASE_URL" in os.environ:
        env["DATABASE_URL"] = os.environ["DATABASE_URL"]

    # ── (Health endpoint moved to parent watchdog in main) ──

    print(f"\n  Starting engine … (Ctrl-C to stop)\n")

    result = subprocess.run(
        [sys.executable, "-m", "stocking_app.cli", "run-engine"],
        env=env, cwd=str(ROOT),
    )
    sys.exit(result.returncode)


def cmd_dashboard(sc, args):
    port = args.port or 8501
    print(f"\n{'═'*60}")
    print(f"  DASHBOARD — {sc.name}")
    print(f"  URL  : http://localhost:{port}")
    print(f"  DB   : {sc.db_path}")
    print(f"  Logs : {sc.log_dir / 'engine.log'}")
    print(f"{'═'*60}\n")

    env = os.environ.copy()
    env.update({
        "STOCKING_DB_PATH":       str(sc.db_path),
        "STOCKING_TICKER_SUFFIX": sc.suffix,
        "STOCKING_CYCLE_SECONDS": str(sc.cycle_seconds),
        "STOCKING_ORDER_QTY":     str(sc.order_qty),
        "STOCKING_LOG_DIR":       str(sc.log_dir),
    })

    result = subprocess.run([
        sys.executable, "-m", "streamlit", "run",
        str(ROOT / "dashboard.py"),
        "--server.port", str(port),
        "--server.headless", "false",
        "--",
        "--strategy-dir", str(sc.strategy_dir),
    ], env=env, cwd=str(ROOT))
    sys.exit(result.returncode)
# ── Helpers ──────────────────────────────────────────────────────────────────

def _run_cli(cli_args: list[str], env: dict) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "stocking_app.cli"] + cli_args,
        env=env, cwd=str(ROOT), capture_output=False,
    )
    if result.returncode != 0:
        print(f"Warning: CLI command {cli_args[0]} returned code {result.returncode}")


def _count_csv(path: Path) -> int:
    try:
        return max(0, sum(1 for _ in path.open()) - 1)  # minus header
    except Exception:
        return 0


def _detect_symbol_column(csv_path: Path) -> str:
    try:
        import pandas as pd
        df = pd.read_csv(csv_path, nrows=0)
        for col in df.columns:
            if str(col).strip().lower() in ("symbol", "ticker"):
                return str(col)
    except Exception:
        pass
    return "Symbol"


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Run a strategy in backtest, live, or dashboard mode.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("strategy_dir", nargs='?', help="Path to strategy folder (contains strategy.yaml). Mutually exclusive with --all-strategies.")
    parser.add_argument(
        "--mode", required=True,
        choices=["backtest", "live", "dashboard", "all"],
        help="backtest=historical sim | live=paper trading engine | dashboard=open UI",
    )
    parser.add_argument("--all-strategies", action="store_true", help="Run the mode on all discovered strategies concurrently (only useful for 'live' mode in Render).")
    parser.add_argument("--port",          type=int, default=None, help="Dashboard port (default: 8501)")
    parser.add_argument("--skip-download", action="store_true",    help="[backtest] reuse cached DB data")
    args = parser.parse_args()

    if args.all_strategies:
        import time as _time

        print("\n  [Runner] Launching all strategies concurrently …")
        strategies_dir = ROOT / "strategies"

        # Collect all strategy directories (sorted for determinism)
        strategy_dirs = sorted(
            d for d in strategies_dir.iterdir()
            if d.is_dir() and (d / "strategy.yaml").exists()
        )

        if not strategy_dirs:
            print("  No strategies found.")
            sys.exit(1)

        # ── Per-engine fetch-start stagger ─────────────────────────────────
        # Assign each engine a different delay so they don't all burst Yahoo
        # Finance simultaneously from the same IP on each cycle's first fetch.
        # Order: 0s → 90s → 180s based on sorted directory index.
        FETCH_DELAYS = [0, 90, 180]

        # ── Render Web Service: Health endpoint + Keep-Alive self-ping ─────────────
        # Start this in the parent watchdog process BEFORE spawning children so
        # Render sees the port open immediately and doesn't timeout during slow child DB inits.
        if os.environ.get("RENDER") or os.environ.get("PORT"):
            import threading
            import json as _json
            from http.server import HTTPServer, BaseHTTPRequestHandler
            from datetime import datetime as _dt, timezone as _tz

            _engine_start_time = _dt.now(_tz.utc).isoformat()

            class HealthCheckHandler(BaseHTTPRequestHandler):
                def do_GET(self):
                    payload = _json.dumps({
                        "status":  "running",
                        "service": "stocking-engine-watchdog",
                        "started": _engine_start_time,
                        "ts":      _dt.now(_tz.utc).isoformat(),
                    }).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)

                def do_HEAD(self):
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()

                def log_message(self, fmt, *args):
                    pass

            port = int(os.environ.get("PORT", 8080))
            httpd = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
            print(f"  [Render] Health endpoint listening on port {port}")
            threading.Thread(target=httpd.serve_forever, daemon=True).start()

            _ping_interval = 8 * 60
            _ping_urls = []
            _ext_url = os.environ.get("RENDER_EXTERNAL_URL", "").strip()
            if _ext_url:
                _ping_urls.append(_ext_url)
            _svc_name = os.environ.get("RENDER_SERVICE_NAME", "").strip()
            if _svc_name and not _ext_url:
                _ping_urls.append(f"https://{_svc_name}.onrender.com")
            _ping_urls.append(f"http://localhost:{port}")

            import urllib.request as _urllib

            def _keep_alive_loop():
                import time as _time
                while True:
                    _time.sleep(_ping_interval)
                    for _url in _ping_urls:
                        try:
                            _urllib.urlopen(_url, timeout=10)
                            print(f"  [keep-alive] Pinged {_url} ✓")
                            break
                        except Exception as _e:
                            print(f"  [keep-alive] {_url} failed: {_e} — trying next …")

            _ka = threading.Thread(target=_keep_alive_loop, daemon=True, name="keep-alive")
            _ka.start()

            _primary_url = _ping_urls[0] if _ping_urls else f"http://localhost:{port}"
            if _ext_url:
                print(f"  [Render] Keep-alive → {_primary_url}  (every 8 min, +{len(_ping_urls)-1} fallbacks)")
            elif _svc_name:
                print(f"  [Render] Keep-alive → {_primary_url}  (constructed from RENDER_SERVICE_NAME)")
            else:
                print(f"  [Render] Keep-alive → localhost:{port}  (RENDER_EXTERNAL_URL not set — using local fallback)")

        # ── Build process specs ─────────────────────────────────────────────
        # Each spec holds everything needed to (re)spawn the process.
        specs = []
        for idx, d in enumerate(strategy_dirs):
            print(f"  --> Launching {d.name} in mode {args.mode}")
            child_env = os.environ.copy()
            child_env["STOCKING_IS_PRIMARY"] = "1" if idx == 0 else "0"
            child_env["STOCKING_FETCH_START_DELAY"] = str(FETCH_DELAYS[min(idx, len(FETCH_DELAYS) - 1)])
            specs.append({
                "name": d.name,
                "cmd": [sys.executable, __file__, str(d), "--mode", args.mode],
                "env": child_env,
            })

        def _spawn(spec: dict) -> subprocess.Popen:
            return subprocess.Popen(spec["cmd"], env=spec["env"])

        # ── Start all processes ─────────────────────────────────────────────
        procs = {spec["name"]: {"proc": _spawn(spec), "spec": spec, "restarts": 0, "backoff": 1}
                 for spec in specs}

        print(f"  Started {len(procs)} engines in the background. Watching for crashes …")

        # ── Watchdog loop ───────────────────────────────────────────────────
        # Polls every 5 seconds. If a child exits unexpectedly (non-zero or
        # even zero when it shouldn't), it is restarted with exponential
        # backoff (1s → 2s → 4s … capped at 60s) so a persistent crash
        # doesn't spin-restart the whole Render service.
        try:
            while True:
                _time.sleep(5)
                for name, state in procs.items():
                    p = state["proc"]
                    rc = p.poll()
                    if rc is None:
                        continue  # still running — all good

                    state["restarts"] += 1
                    backoff = min(state["backoff"], 60)
                    print(
                        f"  [watchdog] {name} exited (code={rc}). "
                        f"Restart #{state['restarts']} in {backoff}s …"
                    )
                    _time.sleep(backoff)
                    state["proc"] = _spawn(state["spec"])
                    state["backoff"] = min(backoff * 2, 60)

        except KeyboardInterrupt:
            print("\n  Shutting down all engines …")
            for state in procs.values():
                state["proc"].terminate()

        sys.exit(0)

    if not args.strategy_dir:
        parser.error("strategy_dir is required unless --all-strategies is used")

    sc = load_strategy(args.strategy_dir)
    print(f"\n  Strategy  : {sc.name}")
    print(f"  Type      : {sc.strategy_type}")
    print(f"  Directory : {sc.strategy_dir}")

    if args.mode == "backtest":
        cmd_backtest(sc, args)
    elif args.mode == "live":
        cmd_live(sc, args)
    elif args.mode == "dashboard":
        cmd_dashboard(sc, args)
    elif args.mode == "all":
        # Run backtest then live sequentially
        print("\n  [1/2] Running backtest …")
        bt_proc = subprocess.run([
            sys.executable, __file__, args.strategy_dir,
            "--mode", "backtest",
        ])
        if bt_proc.returncode != 0:
            print("Backtest failed — not starting live engine.")
            sys.exit(bt_proc.returncode)
        print("\n  [2/2] Starting live engine …")
        cmd_live(sc, args)


if __name__ == "__main__":
    main()
