from __future__ import annotations

import logging
import logging.handlers
import os
import signal
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig, load_config
from .data_fetcher import fetch_5m_bars_sync
from .db import SignalRecord, TradingRepository, utc_now_iso
from .strategy import compute_symbol_signal


# ── Logging setup ─────────────────────────────────────────────────────────────
def _setup_logger(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "engine.log"

    logger = logging.getLogger("stocking.engine")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Rotating file handler — 5 MB per file, keep 3 backups
    fh = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Console handler — INFO and above
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ── Stats dataclass ──────────────────────────────────────────────────────────
@dataclass
class CycleStats:
    run_started_at: str
    run_ended_at: str
    status: str
    symbols_total: int
    symbols_fetched: int
    symbols_computed: int
    fetch_seconds: float
    compute_seconds: float
    persist_seconds: float
    duration_seconds: float
    error: str | None = None


# ── Engine ───────────────────────────────────────────────────────────────────
class ScalableEngine:
    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.repo = TradingRepository(cfg.database_url or cfg.db_path)
        self.repo.init_db()
        self._running = True
        self.executor = ProcessPoolExecutor(max_workers=max(1, cfg.compute_workers))

        # Logger writes to <db_path_dir>/logs/engine.log
        log_dir = Path(cfg.db_path).parent / "logs"
        self.log = _setup_logger(log_dir)
        self.log.info("═" * 60)
        self.log.info("  Stocking Engine starting up")
        self.log.info(f"  DB          : {cfg.db_path}")
        self.log.info(f"  Suffix      : {cfg.ticker_suffix}")
        self.log.info(f"  Cycle       : {cfg.cycle_seconds}s")
        self.log.info(f"  Concurrency : fetch={cfg.max_fetch_concurrency}  compute={cfg.compute_workers}")
        self.log.info("═" * 60)

        self.pid_file = Path(cfg.db_path).parent / "engine.pid"
        self.pid_file.write_text(str(os.getpid()))

    def stop(self) -> None:
        self._running = False

    def close(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=True)
        self.repo.close()
        self.pid_file.unlink(missing_ok=True)
        self.log.info("Engine shut down cleanly.")

    def run_forever(self) -> None:
        def _handle_signal(_sig: int, _frame: Any) -> None:
            self.log.info("Shutdown signal received — stopping after current cycle.")
            self.stop()

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        cycle_num = 0
        while self._running:
            if not self.repo.get_engine_enabled():
                self.log.debug("Engine paused — waiting for enable signal.")
                self.repo.set_engine_heartbeat(
                    {"state": "paused", "ts": utc_now_iso(), "cycle_seconds": self.cfg.cycle_seconds}
                )
                time.sleep(self.cfg.disabled_poll_seconds)
                continue

            cycle_num += 1
            loop_start = time.monotonic()
            run_started_at = utc_now_iso()

            self.log.info(f"┌─ Cycle #{cycle_num} started  [{run_started_at}]")

            try:
                stats = self.run_once(run_started_at, cycle_num)
                self.repo.record_run_metrics(asdict(stats))
                self.repo.set_engine_heartbeat(
                    {
                        "state": "running",
                        "last_run": stats.run_ended_at,
                        "status": stats.status,
                        "duration_seconds": stats.duration_seconds,
                        "symbols_total": stats.symbols_total,
                        "symbols_fetched": stats.symbols_fetched,
                        "symbols_computed": stats.symbols_computed,
                        "fetch_seconds": stats.fetch_seconds,
                        "compute_seconds": stats.compute_seconds,
                        "persist_seconds": stats.persist_seconds,
                        "error": stats.error,
                    }
                )
                self.log.info(
                    f"└─ Cycle #{cycle_num} OK  "
                    f"| total={stats.symbols_total}  "
                    f"fetched={stats.symbols_fetched}  "
                    f"computed={stats.symbols_computed}  "
                    f"| fetch={stats.fetch_seconds:.1f}s  "
                    f"compute={stats.compute_seconds:.1f}s  "
                    f"persist={stats.persist_seconds:.1f}s  "
                    f"| total={stats.duration_seconds:.1f}s"
                )

            except Exception as exc:  # noqa: BLE001
                run_ended_at = utc_now_iso()
                duration = time.monotonic() - loop_start
                failed = CycleStats(
                    run_started_at=run_started_at,
                    run_ended_at=run_ended_at,
                    status="FAILED",
                    symbols_total=0,
                    symbols_fetched=0,
                    symbols_computed=0,
                    fetch_seconds=0.0,
                    compute_seconds=0.0,
                    persist_seconds=0.0,
                    duration_seconds=duration,
                    error=str(exc),
                )
                self.repo.record_run_metrics(asdict(failed))
                self.repo.set_engine_heartbeat(
                    {"state": "running", "last_run": failed.run_ended_at, "status": "FAILED", "error": failed.error}
                )
                self.log.error(f"└─ Cycle #{cycle_num} FAILED: {exc}", exc_info=True)

            elapsed = time.monotonic() - loop_start
            wait = max(0.0, self.cfg.cycle_seconds - elapsed)
            if wait > 0:
                self.log.info(f"   Next cycle in {wait:.0f}s  (at {datetime.now().strftime('%H:%M:%S')})")
            time.sleep(wait)

    def run_once(self, run_started_at: str, cycle_num: int = 0) -> CycleStats:
        symbols = self.repo.get_monitor_symbols()
        total = len(symbols)

        self.log.info(f"  [1/3] FETCH  — {total} symbols  (lookback={self.cfg.fetch_lookback_days}d)")

        if total == 0:
            self.log.warning("  No symbols in universe — skipping cycle.")
            run_ended = utc_now_iso()
            return CycleStats(
                run_started_at=run_started_at,
                run_ended_at=run_ended,
                status="OK",
                symbols_total=0,
                symbols_fetched=0,
                symbols_computed=0,
                fetch_seconds=0.0,
                compute_seconds=0.0,
                persist_seconds=0.0,
                duration_seconds=0.0,
            )

        fetch_start = time.monotonic()
        fetch_results = fetch_5m_bars_sync(
            symbols,
            lookback_days=self.cfg.fetch_lookback_days,
            max_concurrency=self.cfg.max_fetch_concurrency,
        )
        fetch_seconds = time.monotonic() - fetch_start

        fetched_symbols: list[str] = []
        failed_symbols: list[str] = []
        total_bars = 0
        for result in fetch_results:
            if result.error or result.bars.empty:
                failed_symbols.append(result.symbol)
                continue
            self.repo.upsert_candles(result.symbol, result.bars)
            fetched_symbols.append(result.symbol)
            total_bars += len(result.bars)

        self.log.info(
            f"       ✓ {len(fetched_symbols)}/{total} fetched  "
            f"({total_bars:,} bars)  "
            f"| {fetch_seconds:.1f}s"
            + (f"  | {len(failed_symbols)} failed: {', '.join(failed_symbols[:5])}" if failed_symbols else "")
        )

        symbols_to_compute = self.repo.get_symbols_pending_compute(fetched_symbols)
        self.log.info(f"  [2/3] COMPUTE — {len(symbols_to_compute)} symbols pending")

        compute_start = time.monotonic()
        compute_payloads: list[dict[str, Any]] = []
        futures = {
            self.executor.submit(
                compute_symbol_signal,
                str(self.cfg.db_path),
                symbol,
                self.cfg.compute_lookback_days,
                "Asia/Kolkata",
            ): symbol
            for symbol in symbols_to_compute
        }

        compute_errors = 0
        for fut in as_completed(futures):
            symbol = futures[fut]
            try:
                compute_payloads.append(fut.result())
            except Exception as exc:  # noqa: BLE001
                compute_errors += 1
                compute_payloads.append(
                    {
                        "symbol": symbol,
                        "asof_ts": None,
                        "last_price": None,
                        "signal": None,
                        "signal_price": None,
                        "signal_reason": f"compute_error:{exc}",
                    }
                )

        compute_seconds = time.monotonic() - compute_start

        # Count actual signals
        buys  = [p for p in compute_payloads if p.get("signal") == "BUY"]
        sells = [p for p in compute_payloads if p.get("signal") == "SELL"]

        self.log.info(
            f"       ✓ {len(compute_payloads)} computed  "
            f"| {compute_seconds:.1f}s  "
            f"| BUY signals: {len(buys)}  SELL signals: {len(sells)}"
            + (f"  | {compute_errors} errors" if compute_errors else "")
        )

        # Log each signal found
        for p in buys:
            self.log.info(f"       🟢 BUY   {p['symbol']:<16}  @ {p.get('signal_price') or p.get('last_price'):.4f}  [{p.get('signal_reason','')}]")
        for p in sells:
            self.log.info(f"       🔴 SELL  {p['symbol']:<16}  @ {p.get('signal_price') or p.get('last_price'):.4f}  [{p.get('signal_reason','')}]")

        self.log.info(f"  [3/3] PERSIST")
        persist_start = time.monotonic()
        open_positions = self.repo.get_open_positions()
        position_prices: dict[str, tuple[float, str]] = {}

        actions_taken = []
        for payload in compute_payloads:
            symbol = payload["symbol"]
            asof_ts = payload.get("asof_ts")
            if asof_ts:
                self.repo.mark_symbol_computed(symbol, asof_ts)

            last_price = payload.get("last_price")
            if symbol in open_positions and last_price is not None and asof_ts:
                position_prices[symbol] = (float(last_price), asof_ts)

            signal = payload.get("signal")
            if signal not in {"BUY", "SELL"}:
                continue

            signal_price = payload.get("signal_price")
            price = float(signal_price if signal_price is not None else last_price)
            reason = str(payload.get("signal_reason") or "strategy_signal")

            acted = False
            if signal == "BUY" and symbol not in open_positions:
                self.repo.execute_buy(symbol, qty=self.cfg.order_qty, price=price, ts=asof_ts or utc_now_iso(), reason=reason)
                acted = True
                actions_taken.append(f"BOUGHT {symbol} × {self.cfg.order_qty} @ {price:.4f}")
                open_positions[symbol] = {"symbol": symbol, "qty": self.cfg.order_qty, "avg_price": price}

            elif signal == "SELL" and symbol in open_positions:
                self.repo.execute_sell(symbol=symbol, price=price, ts=asof_ts or utc_now_iso(), reason=reason)
                acted = True
                actions_taken.append(f"SOLD   {symbol} @ {price:.4f}")
                open_positions.pop(symbol, None)

            self.repo.upsert_signal(
                SignalRecord(
                    symbol=symbol,
                    ts=asof_ts or utc_now_iso(),
                    signal_type=signal,
                    price=price,
                    reason=reason,
                ),
                acted=acted,
            )

        if position_prices:
            self.repo.update_position_prices(position_prices)

        snap_ts = utc_now_iso()
        self.repo.snapshot_pnl(ts=snap_ts)
        self.repo.commit()
        persist_seconds = time.monotonic() - persist_start

        if actions_taken:
            for act in actions_taken:
                self.log.info(f"       ✅  {act}")
        else:
            self.log.info(f"       — No trades executed this cycle  (open positions: {len(open_positions)})")

        self.log.info(
            f"       ✓ Persisted in {persist_seconds:.2f}s  "
            f"| Open positions: {len(open_positions)}"
        )

        run_ended_at = utc_now_iso()
        duration = (
            datetime.fromisoformat(run_ended_at).timestamp()
            - datetime.fromisoformat(run_started_at).timestamp()
        )

        return CycleStats(
            run_started_at=run_started_at,
            run_ended_at=run_ended_at,
            status="OK",
            symbols_total=total,
            symbols_fetched=len(fetched_symbols),
            symbols_computed=len(compute_payloads),
            fetch_seconds=fetch_seconds,
            compute_seconds=compute_seconds,
            persist_seconds=persist_seconds,
            duration_seconds=duration,
            error=None,
        )


def run_engine() -> None:
    cfg = load_config()
    engine = ScalableEngine(cfg)
    try:
        engine.run_forever()
    finally:
        engine.close()
