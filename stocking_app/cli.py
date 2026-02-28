from __future__ import annotations

import argparse
from dataclasses import asdict

import pandas as pd

from .config import load_config
from .db import TradingRepository, utc_now_iso


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stocking scalable strategy engine")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Create SQLite schema and default state")

    p_import = sub.add_parser("import-universe", help="Import symbols from CSV")
    p_import.add_argument("--csv", required=True, help="Path to CSV with symbol column")
    p_import.add_argument("--symbol-column", default="symbol", help="Column name for symbols")
    p_import.add_argument(
        "--append-suffix",
        default=None,
        help="Append suffix to symbols when missing (example: .NS)",
    )

    p_enable = sub.add_parser("set-engine", help="Toggle engine enabled/disabled flag")
    p_enable.add_argument("--enabled", choices=["true", "false"], required=True)

    sub.add_parser("status", help="Show current engine and universe status")
    sub.add_parser("run-once", help="Run exactly one cycle")
    sub.add_parser("run-engine", help="Run engine loop forever")

    return parser


def _apply_suffix(symbol: str, suffix: str | None) -> str:
    s = symbol.strip().upper()
    if not s:
        return s
    if suffix is None:
        return s
    if s.endswith(suffix.upper()):
        return s
    return f"{s}{suffix.upper()}"


def cmd_init_db(repo: TradingRepository) -> None:
    repo.init_db()
    print(f"Initialized database at {repo.db_url}")


def cmd_import_universe(repo: TradingRepository, csv_path: str, symbol_column: str, suffix: str | None) -> None:
    df = pd.read_csv(csv_path)
    if symbol_column not in df.columns:
        raise ValueError(f"CSV missing column: {symbol_column}")

    symbols = [_apply_suffix(str(x), suffix) for x in df[symbol_column].dropna().tolist()]
    inserted = repo.upsert_universe(symbols)
    print(f"Imported {inserted} symbols into universe")


def cmd_set_engine(repo: TradingRepository, enabled: bool) -> None:
    repo.set_engine_enabled(enabled)
    print(f"Engine enabled={enabled}")


def cmd_status(repo: TradingRepository) -> None:
    state = repo.get_engine_enabled()
    heartbeat = repo.get_engine_heartbeat()
    universe = repo.get_universe_summary()
    open_positions = len(repo.get_open_positions())

    print(f"DB: {repo.db_url}")
    print(f"engine_enabled: {state}")
    print(f"universe: {universe}")
    print(f"open_positions: {open_positions}")
    print(f"heartbeat: {heartbeat}")


def cmd_run_once(cfg_pathless: None = None) -> None:
    from .engine import ScalableEngine

    cfg = load_config()
    engine = ScalableEngine(cfg)
    try:
        start = utc_now_iso()
        stats = engine.run_once(start)
        engine.repo.record_run_metrics(asdict(stats))
        engine.repo.set_engine_heartbeat(
            {
                "state": "running",
                "last_run": stats.run_ended_at,
                "status": stats.status,
                "duration_seconds": stats.duration_seconds,
                "symbols_total": stats.symbols_total,
                "symbols_fetched": stats.symbols_fetched,
                "symbols_computed": stats.symbols_computed,
            }
        )
        print(stats)
    finally:
        engine.close()


def cmd_run_engine() -> None:
    from .engine import ScalableEngine

    cfg = load_config()
    engine = ScalableEngine(cfg)
    try:
        engine.run_forever()
    finally:
        engine.close()


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    cfg = load_config()
    repo = TradingRepository(cfg.database_url or cfg.db_path)

    try:
        if args.command == "init-db":
            cmd_init_db(repo)
        elif args.command == "import-universe":
            cmd_import_universe(repo, args.csv, args.symbol_column, args.append_suffix)
        elif args.command == "set-engine":
            cmd_set_engine(repo, enabled=args.enabled == "true")
        elif args.command == "status":
            cmd_status(repo)
        elif args.command == "run-once":
            repo.close()
            cmd_run_once()
            return
        elif args.command == "run-engine":
            repo.close()
            cmd_run_engine()
            return
    finally:
        try:
            repo.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
