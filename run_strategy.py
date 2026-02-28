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
    })
    
    # Let Supabase db_url pass through to child processes if it exists
    if "DATABASE_URL" in os.environ:
        env["DATABASE_URL"] = os.environ["DATABASE_URL"]

    # Init DB + import universe
    _run_cli(["init-db"], env)
    sym_col = _detect_symbol_column(sc.universe_csv)
    _run_cli([
        "import-universe",
        "--csv", str(sc.universe_csv),
        "--symbol-column", sym_col,
        "--append-suffix", sc.suffix,
    ], env)
    _run_cli(["set-engine", "--enabled", "true"], env)

    print(f"\n  Starting engine … (Ctrl-C to stop)\n")
    
    # ── Render Web Service: Health endpoint + Keep-Alive self-ping ─────────────
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
                    "service": "stocking-engine",
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
                pass  # suppress access logs

        port = int(os.environ.get("PORT", 8080))
        httpd = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
        print(f"  [Render] Health endpoint listening on port {port}")
        threading.Thread(target=httpd.serve_forever, daemon=True).start()

        # ── Self-ping keep-alive ────────────────────────────────────────────
        # Render free instances spin down after ~15 min of inactivity.
        # Ping our own URL every 8 minutes to stay alive during market-closed
        # periods (nights, weekends) when the engine is paused but must persist.
        _self_url = os.environ.get("RENDER_EXTERNAL_URL", "").strip()
        if _self_url:
            import urllib.request as _urllib

            _PING_INTERVAL = 8 * 60  # 8 minutes

            def _keep_alive_loop():
                import time as _time
                while True:
                    _time.sleep(_PING_INTERVAL)
                    try:
                        _urllib.urlopen(_self_url, timeout=10)
                        print(f"  [keep-alive] Pinged {_self_url} ✓")
                    except Exception as _e:
                        print(f"  [keep-alive] Ping failed: {_e}")

            _ka = threading.Thread(target=_keep_alive_loop, daemon=True, name="keep-alive")
            _ka.start()
            print(f"  [Render] Keep-alive thread started — will self-ping every 8 min")
        else:
            print("  [Render] RENDER_EXTERNAL_URL not set — keep-alive disabled")

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


def cmd_all(sc, args):
    """Run backtest then start live engine."""
    print(f"  Running backtest first, then starting live engine …\n")
    cmd_backtest(sc, args)   # this sys.exit()s — so we use Popen approach
    # Note: backtest runs as subprocess above; if successful, fall through to live
    # (In practice user will be prompted after backtest completes)


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
        header = csv_path.open().readline().strip()
        for col in header.split(","):
            if col.strip().lower() == "symbol":
                return col.strip()
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
    parser.add_argument("strategy_dir", help="Path to strategy folder (contains strategy.yaml)")
    parser.add_argument(
        "--mode", required=True,
        choices=["backtest", "live", "dashboard", "all"],
        help="backtest=historical sim | live=paper trading engine | dashboard=open UI",
    )
    parser.add_argument("--port",          type=int, default=None, help="Dashboard port (default: 8501)")
    parser.add_argument("--skip-download", action="store_true",    help="[backtest] reuse cached DB data")
    args = parser.parse_args()

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
