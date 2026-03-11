from __future__ import annotations

import asyncio
import logging
import logging.handlers
import os
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .config import AppConfig, load_config
from .data_fetcher import FetchResult, fetch_daily_bars_gen
from .db import SignalRecord, TradingRepository, utc_now_iso
from .market_schedule import market_status as get_market_status
from .strategy import compute_symbol_signal


# ── Logging setup ─────────────────────────────────────────────────────────────
class DatabaseLogHandler(logging.Handler):
    def __init__(self, repo: 'TradingRepository'):
        super().__init__()
        self.repo = repo

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self.repo.insert_log(record.levelname, msg)
        except Exception:
            self.handleError(record)


def _setup_logger(log_dir: Path, repo: 'TradingRepository') -> logging.Logger:
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

    # Database handler — INFO and above
    db_h = DatabaseLogHandler(repo)
    db_h.setLevel(logging.INFO)
    db_h.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.addHandler(db_h)
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
        self.repo = TradingRepository(cfg.database_url or cfg.db_path, suffix=cfg.ticker_suffix)
        self.repo.init_db()
        self._running = True
        
        # ── Compute thread pool ─────────────────────────────────────────────
        # Use threads NOT processes — avoids spawning extra Python interpreters
        # (~150 MB each). Compute is numpy/pandas heavy which releases the GIL,
        # so threads get real parallelism without the memory overhead of forks.
        # On Render's 512 MB free tier this saves ~450 MB across 3 engines.
        system_cores = os.cpu_count() or 1
        target_workers = max(1, cfg.compute_workers)
        self.executor = ThreadPoolExecutor(max_workers=min(target_workers, system_cores))

        # Logger writes to <db_path_dir>/logs/engine.log and the Supabase db
        log_dir = Path(cfg.db_path).parent / "logs"
        self.log = _setup_logger(log_dir, self.repo)
        self.log.info("═" * 60)
        self.log.info("  Stocking Engine starting up")
        self.log.info(f"  DB          : {cfg.db_path}")
        self.log.info(f"  Suffix      : {cfg.ticker_suffix}")
        self.log.info(f"  Cycle       : {cfg.cycle_seconds}s")
        self.log.info(f"  Concurrency : fetch={cfg.max_fetch_concurrency}  compute={cfg.compute_workers}")
        self.log.info("═" * 60)

        # ── Startup guard: fail-fast if DATABASE_URL is missing ─────────────
        if not cfg.database_url:
            self.log.error(
                "ENGINE STARTUP FAILED: DATABASE_URL environment variable is not set.\n"
                "Set it before starting the engine:\n"
                "  export DATABASE_URL=postgresql://user:pass@host:5432/db\n"
                "Or add it to your .env file."
            )
            raise RuntimeError("DATABASE_URL is required but not set. Engine cannot start.")

        # BUG-DUALENGINE-06 fix: check if another engine is already running
        # before writing our PID. Prevents two engines from processing the same
        # universe simultaneously, which wastes resources and causes race conditions.
        self.pid_file = Path(cfg.db_path).parent / "engine.pid"
        if self.pid_file.exists():
            try:
                old_pid = int(self.pid_file.read_text().strip())
                # Check if old PID is still alive
                os.kill(old_pid, 0)  # signal 0 = just check existence
                self.log.error(
                    f"ENGINE STARTUP BLOCKED: Another engine (PID {old_pid}) is already running "
                    f"for suffix {cfg.ticker_suffix}. Kill it first or remove {self.pid_file}"
                )
                raise RuntimeError(f"Duplicate engine detected (PID {old_pid})")
            except (ProcessLookupError, ValueError):
                # Old process is dead or PID file is corrupt — safe to proceed
                self.log.info(f"  Stale PID file found (PID gone). Overwriting.")
            except PermissionError:
                # Process exists but we can't signal it — it's running
                self.log.error(
                    f"ENGINE STARTUP BLOCKED: Another engine process is running. "
                    f"Remove {self.pid_file} if this is a false positive."
                )
                raise RuntimeError("Duplicate engine detected (permission denied on PID check)")
        self.pid_file.write_text(str(os.getpid()))

        # State tracking for rate-limiting and dead symbols
        self.consecutive_zero_fetches = 0
        self.ZERO_FETCH_WARN_THRESHOLD = 3
        self.ZERO_FETCH_SLEEP_EXTRA = 60
        self._empty_fetch_counts: dict[str, int] = {}
        self._dead_symbols: set[str] = set()
        self.DEAD_SYMBOL_THRESHOLD = 5

    def stop(self) -> None:
        self._running = False

    def close(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=True)
        self.log.info("Engine shut down cleanly.")
        self.repo.close()
        self.pid_file.unlink(missing_ok=True)


    def run_forever(self) -> None:
        def _handle_signal(_sig: int, _frame: Any) -> None:
            self.log.info("Shutdown signal received — stopping after current cycle.")
            self.stop()

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        # ── Per-engine fetch-start stagger ──────────────────────────────────
        # When multiple engines start together (--all-strategies), each gets a
        # different delay so their first yfinance bursts don't overlap and hit
        # Yahoo's rate limiter from the same IP at the same moment.
        if self.cfg.fetch_start_delay_seconds > 0:
            self.log.info(
                f"  [fetch-delay] Staggering fetch start by "
                f"{self.cfg.fetch_start_delay_seconds}s to avoid rate-limit burst …"
            )
            time.sleep(self.cfg.fetch_start_delay_seconds)

        cycle_num = 0
        while self._running:
            # ── Market-hours auto-scheduler ─────────────────────────────────
            mkt = get_market_status(
                self.cfg.exchange_tz,
                self.cfg.market_open,
                self.cfg.market_close,
            )
            if self.cfg.auto_schedule:
                currently_enabled = self.repo.get_engine_enabled()
                if mkt["market_open"] and not currently_enabled:
                    self.log.info(
                        f"🟢 Market OPEN ({self.cfg.market_open} {self.cfg.exchange_tz}) "
                        f"— auto-enabling engine."
                    )
                    self.repo.set_engine_enabled(True)
                elif not mkt["market_open"] and currently_enabled:
                    self.log.info(
                        f"⚫ Market CLOSED — auto-disabling engine. "
                        f"{mkt['next_event'].capitalize()} in {mkt['next_event_in']}."
                    )
                    self.repo.set_engine_enabled(False)

            if not self.repo.get_engine_enabled():
                status_str = "paused_market_closed" if not mkt["market_open"] else "paused"
                self.log.debug(
                    f"Engine paused — {mkt['next_event']} in {mkt['next_event_in']} "
                    f"({mkt['market_hours']} {mkt['market_tz']})"
                )
                self.repo.set_engine_heartbeat({
                    "state": status_str,
                    "ts": utc_now_iso(),
                    "cycle_seconds": self.cfg.cycle_seconds,
                    **mkt,
                })
                # ── Smart sleep when market is closed ────────────────────────
                # Rather than polling every 60s for 15+ hours (generating ~900
                # no-op DB pings per overnight closure), sleep for up to half
                # the time until next open — but never more than 10 minutes so
                # the heartbeat stays fresh and a manual enable is noticed quickly.
                secs_to_next = mkt.get("next_event_secs", 0)
                if secs_to_next is not None and secs_to_next > 0 and not mkt["market_open"]:
                    smart_sleep = max(
                        self.cfg.disabled_poll_seconds,  # floor: always ≥ poll interval
                        min(int(secs_to_next) // 2, 600),     # cap at 10 min so heartbeat stays alive
                    )
                else:
                    smart_sleep = self.cfg.disabled_poll_seconds
                self.log.debug(f"   sleeping {smart_sleep}s until next check")
                time.sleep(smart_sleep)
                continue

            cycle_num += 1
            loop_start = time.monotonic()
            run_started_at = utc_now_iso()

            # Announce cycle start (STARTING state) so UI can show safety guard
            self.repo.set_engine_heartbeat({
                "state": "starting",
                "ts": utc_now_iso(),
                "cycle_num": cycle_num,
                "cycle_started_at": run_started_at,
                "cycle_seconds": self.cfg.cycle_seconds,
                **mkt,
            })

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
                        "cycle_num": cycle_num,
                        "cycle_started_at": run_started_at,
                        "cycle_seconds": self.cfg.cycle_seconds,
                        **mkt,
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
                self.log.error(f"└─ Cycle #{cycle_num} FAILED: {exc}", exc_info=True)
                
                # If the DB connection was what failed, we need to ensure it's alive before we write the error state!
                try:
                    if hasattr(self.repo, "_ensure_connection"):
                        self.repo._ensure_connection()
                        
                    self.repo.record_run_metrics(asdict(failed))
                    self.repo.set_engine_heartbeat(
                        {"state": "running", "last_run": failed.run_ended_at, "status": "FAILED", "error": failed.error, **mkt}
                    )
                except Exception as write_exc:
                    self.log.error(f"   FATAL: Could not record failure state to DB: {write_exc}")
                    
                # We intentionally don't `raise` here. We wait for the next cycle and retry.

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

        # BUG-07 fix: skip symbols that have consistently returned empty bars.
        # _dead_symbols / _empty_fetch_counts are maintained across cycles.
        live_symbols = [s for s in symbols if s not in self._dead_symbols]
        if len(live_symbols) < len(symbols):
            self.log.debug(f"  Skipping {len(symbols)-len(live_symbols)} dead/de-listed symbols.")

        fetched_symbols: list[str] = []
        failed_symbols: list[str] = []
        state = {"total_bars": 0}

        # ── Background DB keepalive during fetch ──────────────────────────────
        # Supabase/PgBouncer kills idle connections after ~5 minutes.
        # While yfinance fetches are running (can take 60-90s for 500 symbols),
        # the main DB connection sits idle. Ping it every 30s to keep it alive.
        _keepalive_stop = threading.Event()

        def _keepalive_worker():
            while not _keepalive_stop.wait(timeout=30):
                self.repo.keepalive_ping()

        _ka_thread = threading.Thread(target=_keepalive_worker, daemon=True, name="db-keepalive")
        _ka_thread.start()

        try:
            # BUG-FIX: We no longer use asyncio. yfinance's internal threading
            # combined with asyncio.to_thread caused deadlocks on Render.
            # Use a plain synchronous generator instead.
            gen = fetch_daily_bars_gen(
                live_symbols,
                lookback_days=self.cfg.daily_lookback_days,
                max_concurrency=self.cfg.max_fetch_concurrency,
            )
            count = 0
            good_results: list[FetchResult] = []
            for result in gen:
                count += 1
                if result.error or result.bars.empty:
                    failed_symbols.append(result.symbol)
                else:
                    good_results.append(result)

                # Heartbeat every 50 symbols so the UI stays responsive
                if count % 50 == 0 or count == total:
                    self.repo.set_engine_heartbeat(
                        (self.repo.get_engine_heartbeat() or {}) | {
                            "state": "fetching",
                            "fetch_progress": f"{count} / {total}",
                            "fetch_count": count,
                            "fetch_total": total,
                        }
                    )

            # ── Bulk upsert after all batches have landed ────────────────────
            for result in good_results:
                self.repo.upsert_candles_1d(result.symbol, result.bars)
                fetched_symbols.append(result.symbol)
                state["total_bars"] += len(result.bars)
                # BUG-07: successful fetch resets the dead-symbol counter
                self._empty_fetch_counts.pop(result.symbol, None)

            # BUG-07: increment counters for symbols that returned nothing
            newly_dead: list[str] = []
            for sym in failed_symbols:
                self._empty_fetch_counts[sym] = self._empty_fetch_counts.get(sym, 0) + 1
                if self._empty_fetch_counts[sym] >= self.DEAD_SYMBOL_THRESHOLD and sym not in self._dead_symbols:
                    self._dead_symbols.add(sym)
                    newly_dead.append(sym)
                    self.log.warning(
                        f"  ⚠ {sym}: empty fetch for {self.DEAD_SYMBOL_THRESHOLD} consecutive cycles. "
                        f"Treating as de-listed — permanently suspending."
                    )

            # Persist dead symbols to DB so they stay dead across restarts
            if newly_dead:
                self.repo.mark_symbols_inactive(newly_dead)
        finally:
            _keepalive_stop.set()
            _ka_thread.join(timeout=2)

        fetch_seconds = time.monotonic() - fetch_start

        # BUG-09: circuit-breaker — if Yahoo Finance returns nothing at all
        # for several cycles in a row, log a warning and sleep extra to avoid
        # hammering Supabase with useless heartbeat writes during an outage.
        if not fetched_symbols:
            self.consecutive_zero_fetches += 1
            if self.consecutive_zero_fetches >= self.ZERO_FETCH_WARN_THRESHOLD:
                self.log.warning(
                    f"  ⚠ Zero symbols fetched for {self.consecutive_zero_fetches} consecutive cycles. "
                    f"Yahoo Finance may be down or rate-limiting. "
                    f"Sleeping extra {self.ZERO_FETCH_SLEEP_EXTRA}s before retry."
                )
                time.sleep(self.ZERO_FETCH_SLEEP_EXTRA)
        else:
            self.consecutive_zero_fetches = 0  # reset on any success
            
        failed_sample = [failed_symbols[i] for i in range(min(5, len(failed_symbols)))]
        self.log.info(
            f"       ✓ {len(fetched_symbols)}/{total} fetched  "
            f"({state['total_bars']:,} bars)  "
            f"| {fetch_seconds:.1f}s"
            + (f"  | {len(failed_symbols)} failed: {', '.join(failed_sample)}" if failed_symbols else "")
        )

        symbols_to_compute = self.repo.get_symbols_pending_compute(fetched_symbols)
        self.log.info(f"  [2/3] COMPUTE — {len(symbols_to_compute)} symbols pending")
        
        self.repo.set_engine_heartbeat(
            (self.repo.get_engine_heartbeat() or {}) | {
                "state": "computing",
                "compute_progress": f"0 / {len(symbols_to_compute)}",
                "compute_count": 0,
                "compute_total": len(symbols_to_compute)
            }
        )

        compute_start = time.monotonic()
        compute_payloads: list[dict[str, Any]] = []

        # BUG-10 fix: bulk-load all symbol candle data in 1 DB round-trip
        # instead of N individual calls (one per symbol). At 100 symbols this
        # saves ~1.5s; at 500 symbols ~7.5s of serial DB idle time before any
        # compute thread can start.
        self.log.debug(f"Bulk-loading candles for {len(symbols_to_compute)} symbols in 1 query ...")
        all_bars = self.repo.get_combined_bars_for_symbols(
            symbols_to_compute,
            daily_lookback_days=self.cfg.daily_lookback_days,
        )

        futures = {}
        for symbol in symbols_to_compute:
            # Pop to free memory from the dictionary as we submit to the thread pool
            df_symbol = all_bars.pop(symbol, None)
            if df_symbol is None:
                df_symbol = pd.DataFrame()  # BUG-IMPORT-04 fix: use module-level pd import

            fut = self.executor.submit(
                compute_symbol_signal,
                symbol,
                df_symbol,
                self.cfg.exchange_tz,
            )
            futures[fut] = symbol



        compute_errors = 0
        computed_count = 0
        total_to_compute = len(symbols_to_compute)
        
        for fut in as_completed(futures):
            computed_count += 1
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
                
            # Broadcast live compute progress every 25 symbols (was 10)
            # Fewer DB writes per cycle: 500 symbols → 20 writes instead of 50
            if computed_count % 25 == 0 or computed_count == total_to_compute:
                self.repo.set_engine_heartbeat(
                    (self.repo.get_engine_heartbeat() or {}) | {
                        "state": "computing",
                        "compute_progress": f"{computed_count} / {total_to_compute}",
                        "compute_count": computed_count,
                        "compute_total": total_to_compute
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

        # Log each signal found — BUG-21 fix: guard against None price before :.4f format
        for p in buys:
            _price = p.get("signal_price") or p.get("last_price")
            _price_str = f"{_price:.4f}" if _price is not None else "N/A"
            self.log.info(f"       🟢 BUY   {p['symbol']:<16}  @ {_price_str}  [{p.get('signal_reason','')}]")
        for p in sells:
            _price = p.get("signal_price") or p.get("last_price")
            _price_str = f"{_price:.4f}" if _price is not None else "N/A"
            self.log.info(f"       🔴 SELL  {p['symbol']:<16}  @ {_price_str}  [{p.get('signal_reason','')}]")
        self.log.info(f"  [3/3] PERSIST")
        persist_start = time.monotonic()
        open_positions = self.repo.get_open_positions()
        position_prices: dict[str, tuple[float, str]] = {}

        # Collect (symbol, asof_ts) pairs for batch upsert at end — avoids N DB round-trips
        computed_pairs: list[tuple[str, str | None]] = []
        actions_taken = []
        for payload in compute_payloads:
            symbol = payload["symbol"]
            asof_ts = payload.get("asof_ts")
            if asof_ts:
                computed_pairs.append((symbol, asof_ts))

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
                # RE-ENTRY GUARD: Check if we already bought this symbol today.
                # 'asof_ts' for 1D polling is the start of the daily candle (YYYY-MM-DDT00:00:00+00:00)
                if asof_ts and self.repo.has_acted_buy_today(symbol, asof_ts):
                    self.log.info(f"       ⚠️  RE-ENTRY GUARD: Already bought {symbol} today. Skipping.")
                    continue
                
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

        # Single batch upsert for all symbol compute states — replaces N individual commits
        if computed_pairs:
            self.repo.batch_mark_symbols_computed(computed_pairs)

        snap_ts = utc_now_iso()
        self.repo.snapshot_pnl(ts=snap_ts)
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
