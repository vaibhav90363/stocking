from __future__ import annotations

import json
import threading
import psycopg2
from psycopg2.extras import RealDictCursor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from psycopg2.pool import ThreadedConnectionPool


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def retry_on_disconnect(max_retries=3):
    """Decorator to catch 'server closed the connection unexpectedly' errors and reconnect.

    BUG-01 fix: guard against max_retries=0 (last_err stays None) by raising a
    RuntimeError if we exit the retry loop without ever capturing an exception.
    BUG-02 fix: acquire the instance-level _lock before calling the wrapped method
    so that the main thread and worker threads never share the DB connection concurrently.
    """
    def decorator(func):
        from functools import wraps
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            import psycopg2
            import time
            # BUG-02: serialise all DB access through an instance lock.
            # On 512 MB / 0.15 CPU (compute_workers=1) this is rarely contended
            # but protects the heartbeat writes that fire from the main thread
            # while the single worker thread may still be reading cursor data.
            with self._lock:
                last_err = None
                for attempt in range(max_retries):
                    try:
                        self._ensure_connection()
                        return func(self, *args, **kwargs)
                    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                        last_err = e
                        # Connection dropped — wipe so _ensure_connection reopens
                        if self.conn:
                            try:
                                self.conn.close()
                            except Exception:
                                pass
                        self.conn = None
                        # Exponential backoff: 0.5s, 1s, 2s
                        if attempt < max_retries - 1:
                            time.sleep(0.5 * (2 ** attempt))
                # BUG-01: last_err is None only when max_retries=0; raise clearly.
                if last_err is None:
                    raise RuntimeError(
                        f"{func.__name__}: retry_on_disconnect called with max_retries=0"
                    )
                raise last_err
        return wrapper
    return decorator


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS universe (
    symbol TEXT PRIMARY KEY,
    is_selected INTEGER NOT NULL DEFAULT 1,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS signals (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    ts TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    price REAL,
    reason TEXT,
    acted INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    UNIQUE(symbol, ts, signal_type)
);

CREATE TABLE IF NOT EXISTS positions_ledger (
    symbol TEXT PRIMARY KEY,
    qty INTEGER NOT NULL,
    avg_price REAL NOT NULL,
    opened_at TEXT NOT NULL,
    last_price REAL,
    last_updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trade_activity_log (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    qty INTEGER NOT NULL,
    price REAL NOT NULL,
    ts TEXT NOT NULL,
    pnl REAL,
    reason TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pnl_snapshots (
    ts TEXT,
    suffix TEXT NOT NULL,
    realized_pnl REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    total_pnl REAL NOT NULL,
    open_positions INTEGER NOT NULL,
    PRIMARY KEY(ts, suffix)
);

CREATE TABLE IF NOT EXISTS engine_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS symbol_state (
    symbol TEXT PRIMARY KEY,
    last_candle_ts TEXT,
    last_compute_ts TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_metrics (
    id SERIAL PRIMARY KEY,
    suffix TEXT NOT NULL,
    run_started_at TEXT NOT NULL,
    run_ended_at TEXT,
    status TEXT NOT NULL,
    symbols_total INTEGER NOT NULL,
    symbols_fetched INTEGER NOT NULL,
    symbols_computed INTEGER NOT NULL,
    fetch_seconds REAL NOT NULL,
    compute_seconds REAL NOT NULL,
    persist_seconds REAL NOT NULL,
    duration_seconds REAL,
    error TEXT
);

CREATE TABLE IF NOT EXISTS system_logs (
    id SERIAL PRIMARY KEY,
    ts TEXT NOT NULL,
    level TEXT NOT NULL,
    suffix TEXT NOT NULL,
    message TEXT NOT NULL
);

-- Performance indexes: added to speed up vectorized candle preload and pending-compute queries.
-- CREATE INDEX IF NOT EXISTS is idempotent — safe to run on an existing database.
CREATE INDEX IF NOT EXISTS idx_symbol_state_pending ON symbol_state (symbol, last_candle_ts, last_compute_ts);
CREATE INDEX IF NOT EXISTS idx_system_logs_suffix   ON system_logs  (suffix, id DESC);

-- Daily candles table — one row per symbol per trading day.
-- Stores 90 days of history without the memory cost of 5-minute bars
-- (90 rows/symbol vs ~6750 rows/symbol at 5m resolution).
CREATE TABLE IF NOT EXISTS candles_1d (
    symbol TEXT NOT NULL,
    ts     TEXT NOT NULL,   -- date string: YYYY-MM-DD
    open   REAL NOT NULL,
    high   REAL NOT NULL,
    low    REAL NOT NULL,
    close  REAL NOT NULL,
    volume REAL,
    PRIMARY KEY (symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_candles1d_symbol_ts ON candles_1d (symbol, ts DESC);
"""


@dataclass
class SignalRecord:
    symbol: str
    ts: str
    signal_type: str
    price: float | None
    reason: str


class TradingRepository:
    # BUG-03 fix: class-level lock guards pool creation so two threads cannot
    # race to build the same ThreadedConnectionPool simultaneously.
    _pool_lock: threading.Lock = threading.Lock()
    _pools: dict[str, ThreadedConnectionPool] = {}

    @classmethod
    def get_pool(cls, db_url: str) -> ThreadedConnectionPool:
        from psycopg2.extras import RealDictCursor
        # Fast path — pool already exists (no lock needed, dict reads are safe in CPython)
        if db_url in cls._pools:
            return cls._pools[db_url]
        # Slow path — first call for this URL, acquire lock to prevent double-creation
        with cls._pool_lock:
            if db_url not in cls._pools:  # double-checked locking
                cls._pools[db_url] = ThreadedConnectionPool(
                    minconn=1,
                    maxconn=20,  # within Supabase free-tier limits
                    dsn=db_url,
                    cursor_factory=RealDictCursor,
                    keepalives=1,
                    keepalives_idle=60,
                    keepalives_interval=10,
                    keepalives_count=5,
                )
        return cls._pools[db_url]

    def __init__(self, db_path_or_url: str | Path, suffix: str = ".NS"):
        # Allow passing either a database URL directly (for Postgres/Supabase)
        # or a local generic fallback path that gets ignored if using URL string.
        if isinstance(db_path_or_url, Path):
            self.db_url = str(db_path_or_url)
        else:
            self.db_url = db_path_or_url

        self.suffix = suffix
        self.conn = None
        # BUG-02 fix: per-instance lock serialises all DB calls. A threading.Lock
        # is ~56 bytes — negligible on the 512 MB budget.
        self._lock: threading.Lock = threading.RLock()
        self._ensure_connection()

    def _ensure_connection(self) -> None:
        """Ensure connection is alive; reconnect if closed/dropped."""
        try:
            if self.conn and not self.conn.closed:
                # Test the connection quickly
                with self.conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return
        except Exception:
            pass  # Connection is dead, we need a new one

        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass

        # Establish connection with RealDictCursor for dictionaries
        # Add TCP keepalives to prevent Supabase/firewalls from dropping idle connections
        try:
            pool = self.get_pool(self.db_url)
            self.conn = pool.getconn()
            self.conn.autocommit = False # keep explicit commits
        except Exception as e:
            if "sqlite" not in str(self.db_url).lower():
                raise e
            # Fallback if someone mistakenly uses psycopg2 URL but points to sqlite logic 
            # (though this repo implies all uses are now Postgres)
            self.conn = psycopg2.connect(self.db_url, cursor_factory=RealDictCursor)
            self.conn.autocommit = False

    def close(self) -> None:
        if self.conn and not self.conn.closed:
            try:
                pool = self.get_pool(self.db_url)
                pool.putconn(self.conn)
            except Exception:
                self.conn.close()
            finally:
                self.conn = None

    def keepalive_ping(self) -> None:
        """Send a lightweight SELECT 1 to keep the Postgres connection alive.
        Call this during long idle phases (e.g. between fetch batches) to prevent
        Supabase's PgBouncer from closing the connection due to inactivity.
        """
        try:
            self._ensure_connection()
        except Exception:
            pass  # _ensure_connection already reconnects; silent failure is OK here

    @retry_on_disconnect()
    def init_db(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        self.conn.commit()
        # ── Idempotent migrations for live databases ────────────────────────
        # These ALTER statements are no-ops if the constraint/index already exists.
        # They fix databases created before the constraint was in SCHEMA_SQL.
        _migrations = [
            # pnl_snapshots PRIMARY KEY — needed for ON CONFLICT(ts, suffix)
            """
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'pnl_snapshots_pkey'
                    AND conrelid = 'pnl_snapshots'::regclass
                ) THEN
                    ALTER TABLE pnl_snapshots ADD PRIMARY KEY (ts, suffix);
                END IF;
            END $$;
            """,
            # symbol_state PRIMARY KEY — needed for ON CONFLICT(symbol)
            # Databases created before this constraint existed will get it now.
            """
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'symbol_state_pkey'
                    AND conrelid = 'symbol_state'::regclass
                ) THEN
                    ALTER TABLE symbol_state ADD PRIMARY KEY (symbol);
                END IF;
            END $$;
            """,
            # signals UNIQUE constraint — needed for ON CONFLICT(symbol, ts, signal_type)
            """
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'signals_symbol_ts_signal_type_key'
                    AND conrelid = 'signals'::regclass
                ) THEN
                    ALTER TABLE signals ADD CONSTRAINT signals_symbol_ts_signal_type_key UNIQUE (symbol, ts, signal_type);
                END IF;
            END $$;
            """,
        ]
        for _sql in _migrations:
            try:
                with self.conn.cursor() as cur:
                    cur.execute(_sql)
                self.conn.commit()
            except Exception as _me:
                self.conn.rollback()
                # Log but don't crash — the engine can still run without the constraint,
                # it just won't upsert PnL snapshots (it will insert fresh rows instead)
                import logging as _log
                _log.getLogger("stocking.engine").warning(
                    f"Migration skipped (non-fatal): {_me}"
                )
        if self.get_engine_enabled() is None:
            self.set_engine_enabled(False)

    @retry_on_disconnect()
    def set_engine_enabled(self, enabled: bool) -> None:
        now = utc_now_iso()
        key = f"engine_enabled_{self.suffix}"
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO engine_state(key, value, updated_at)
                VALUES(%s, %s, %s)
                ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at
                """,
                (key, "1" if enabled else "0", now),
            )
        self.conn.commit()

    @retry_on_disconnect()
    def get_engine_enabled(self) -> bool | None:
        key = f"engine_enabled_{self.suffix}"
        with self.conn.cursor() as cur:
            cur.execute("SELECT value FROM engine_state WHERE key=%s", (key,))
            row = cur.fetchone()
        if row is None:
            return None
        return row["value"] == "1"

    @retry_on_disconnect()
    def set_engine_heartbeat(self, payload: dict[str, Any]) -> None:
        now = utc_now_iso()
        key = f"engine_heartbeat_{self.suffix}"
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO engine_state(key, value, updated_at)
                VALUES(%s, %s, %s)
                ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at
                """,
                (key, json.dumps(payload), now),
            )
        self.conn.commit()

    @retry_on_disconnect()
    def get_engine_heartbeat(self) -> dict[str, Any] | None:
        key = f"engine_heartbeat_{self.suffix}"
        with self.conn.cursor() as cur:
            cur.execute("SELECT value FROM engine_state WHERE key=%s", (key,))
            row = cur.fetchone()
        if row is None:
            return None
        try:
            return json.loads(row["value"])
        except json.JSONDecodeError:
            return {"raw": row["value"]}

    @retry_on_disconnect()
    def upsert_universe(self, symbols: list[str], selected: bool = True, active: bool = True) -> int:
        now = utc_now_iso()
        cleaned = sorted({s.strip().upper() for s in symbols if s and s.strip()})
        rows = [(s, 1 if selected else 0, 1 if active else 0, now, now) for s in cleaned]
        from psycopg2.extras import execute_values
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO universe(symbol, is_selected, is_active, created_at, updated_at)
                VALUES %s
                ON CONFLICT(symbol) DO UPDATE SET
                    is_selected=EXCLUDED.is_selected,
                    is_active=EXCLUDED.is_active,
                    updated_at=EXCLUDED.updated_at
                """,
                rows,
            )
        self.conn.commit()
        return len(rows)

    @retry_on_disconnect()
    def get_monitor_symbols(self) -> list[str]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol FROM universe WHERE is_active=1 AND symbol LIKE %s
                UNION
                SELECT symbol FROM positions_ledger WHERE symbol LIKE %s
                ORDER BY symbol
                """,
                (f"%{self.suffix}", f"%{self.suffix}")
            )
            rows = cur.fetchall()
        return [r["symbol"] for r in rows]

    @retry_on_disconnect()
    def get_universe_summary(self) -> dict[str, int]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*) AS total,
                  SUM(CASE WHEN is_selected=1 THEN 1 ELSE 0 END) AS selected,
                  SUM(CASE WHEN is_active=1 THEN 1 ELSE 0 END) AS active
                FROM universe
                WHERE symbol LIKE %s
                """,
                (f"%{self.suffix}",)
            )
            row = cur.fetchone()
        return {
            "total": int(row["total"] or 0),
            "selected": int(row["selected"] or 0),
            "active": int(row["active"] or 0),
        }


    @retry_on_disconnect()
    def upsert_candles_1d(self, symbol: str, bars: pd.DataFrame) -> int:
        """Upsert daily (1D) OHLCV bars into candles_1d for long-horizon compute."""
        if bars.empty:
            return 0
        df = bars.reset_index()
        df["symbol"] = symbol
        ts_col = pd.to_datetime(df["ts" if "ts" in df.columns else df.columns[0]], utc=True)
        # Store as date string YYYY-MM-DD for readability and small size
        df["_ts_str"] = ts_col.dt.strftime("%Y-%m-%dT00:00:00+00:00")
        if "volume" not in df.columns:
            df["volume"] = 0.0
        rows = list(
            zip(
                df["symbol"],
                df["_ts_str"],
                df["open"].astype(float),
                df["high"].astype(float),
                df["low"].astype(float),
                df["close"].astype(float),
                df["volume"].astype(float),
            )
        )
        from psycopg2.extras import execute_values
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO candles_1d(symbol, ts, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT(symbol, ts) DO UPDATE SET
                    open=EXCLUDED.open,
                    high=EXCLUDED.high,
                    low=EXCLUDED.low,
                    close=EXCLUDED.close,
                    volume=EXCLUDED.volume
                """,
                rows,
            )
            max_ts = df["_ts_str"].max()
            now = utc_now_iso()
            cur.execute(
                """
                INSERT INTO symbol_state(symbol, last_candle_ts, last_compute_ts, updated_at)
                VALUES(%s, %s, NULL, %s)
                ON CONFLICT(symbol) DO UPDATE SET
                    last_candle_ts=EXCLUDED.last_candle_ts,
                    updated_at=EXCLUDED.updated_at
                """,
                (symbol, max_ts, now),
            )
        self.conn.commit()
        return len(rows)

    @retry_on_disconnect()
    def batch_mark_symbols_computed(self, pairs: list[tuple[str, str | None]]) -> None:
        """Batch-upsert last_compute_ts for many symbols in a single round-trip.
        Replaces N individual mark_symbol_computed() calls in the persist loop."""
        if not pairs:
            return
        now = utc_now_iso()
        rows = [(sym, None, ts, now) for sym, ts in pairs]
        from psycopg2.extras import execute_values
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO symbol_state(symbol, last_candle_ts, last_compute_ts, updated_at)
                VALUES %s
                ON CONFLICT(symbol) DO UPDATE SET
                    last_compute_ts=EXCLUDED.last_compute_ts,
                    updated_at=EXCLUDED.updated_at
                """,
                rows,
            )
        self.conn.commit()

    @retry_on_disconnect()
    def get_open_positions(self) -> dict[str, dict]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM positions_ledger WHERE symbol LIKE %s ORDER BY symbol", (f"%{self.suffix}",))
            rows = cur.fetchall()
        return {r["symbol"]: dict(r) for r in rows}

    @retry_on_disconnect()
    def get_symbols_needing_daily_fetch(self, symbols: list[str]) -> list[str]:
        """Return symbols that do not have daily data through yesterday (UTC).

        Used to skip the full daily fetch phase when we already have fresh
        candles_1d (e.g. after the first cycle). Fetch daily at most once per
        day per symbol instead of every cycle.
        """
        if not symbols:
            return []
        from datetime import timedelta
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).strftime("%Y-%m-%d") + "T00:00:00+00:00"
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT symbol
                FROM candles_1d
                WHERE symbol = ANY(%s) AND ts >= %s
                """,
                (symbols, yesterday),
            )
            rows = cur.fetchall()
        have_fresh = {r["symbol"] for r in rows}
        return [s for s in symbols if s not in have_fresh]

    @retry_on_disconnect()
    def get_symbols_pending_compute(self, symbols: list[str]) -> list[str]:
        if not symbols:
            return []
        
        # In psycopg2, we use %s or = ANY(%s) for IN clauses with arrays easily
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol
                FROM symbol_state
                WHERE symbol = ANY(%s)
                  AND last_candle_ts IS NOT NULL
                  AND (last_compute_ts IS NULL OR last_candle_ts > last_compute_ts)
                ORDER BY symbol
                """,
                (symbols,),
            )
            rows = cur.fetchall()
        return [r["symbol"] for r in rows]

    @retry_on_disconnect()
    def get_combined_bars_for_symbols(
        self,
        symbols: list[str],
        daily_lookback_days: int = 90,
    ) -> "dict[str, pd.DataFrame]":
        """
        Loads daily bars for ALL symbols. This replaces the complex hybrid daily + 5m
        logic with a pure 1D fetch, since Yahoo Finance 1D data natively updates
        the current day's candle dynamically every 5 minutes.
        """
        from collections import defaultdict
        from datetime import datetime, timedelta, timezone

        if not symbols:
            return {}

        now = datetime.now(timezone.utc)
        daily_cutoff = (now - timedelta(days=daily_lookback_days)).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).isoformat()

        result: dict = {}
        CHUNK_SIZE = 50

        for i in range(0, len(symbols), CHUNK_SIZE):
            chunk_symbols = symbols[i : i + CHUNK_SIZE]

            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol, ts, open, high, low, close, volume
                    FROM candles_1d
                    WHERE symbol = ANY(%s) AND ts >= %s
                    ORDER BY symbol, ts
                    """,
                    (chunk_symbols, daily_cutoff),
                )
                daily_rows = cur.fetchall()

            # Group rows by symbol in Python
            daily_by_sym: dict = defaultdict(list)
            for r in daily_rows:
                daily_by_sym[r["symbol"]].append(dict(r))

            for sym in set(daily_by_sym):
                df_d = pd.DataFrame(daily_by_sym[sym]).drop(columns=["symbol"], errors="ignore")
                df_d["ts"] = pd.to_datetime(df_d["ts"], utc=True)
                df_d = df_d.set_index("ts")
                
                # Sort and drop any exact duplicates just in case
                combined = df_d.sort_index()
                combined = combined[~combined.index.duplicated(keep="last")]
                result[sym] = combined

        # Symbols with no data at all
        for sym in symbols:
            if sym not in result:
                result[sym] = pd.DataFrame()

        return result


    @retry_on_disconnect()
    def get_all_candles_for_symbols(self, symbols: list[str], lookback_days: int) -> pd.DataFrame:
        """
        Pulls candles for multiple symbols, capped to the last MAX_BARS_PER_SYMBOL
        rows per symbol. Loads in chunks of CHUNK_SIZE to avoid materializing
        millions of rows in a single fetchall() — the primary OOM risk on Render's
        512 MB free tier.
        """
        import pandas as pd
        from datetime import datetime, timedelta, timezone

        if not symbols:
            return pd.DataFrame()

        # Cap per symbol: 30 days × ~75 bars/day = ~2250 bars max.
        # We keep only the most recent 500 — enough for any weekly/daily signal window

    @retry_on_disconnect()
    def upsert_signal(self, signal: SignalRecord, acted: bool = False) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO signals(symbol, ts, signal_type, price, reason, acted, created_at)
                VALUES(%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(symbol, ts, signal_type) DO UPDATE SET
                    price=EXCLUDED.price,
                    reason=EXCLUDED.reason,
                    acted=GREATEST(signals.acted, EXCLUDED.acted),
                    created_at=EXCLUDED.created_at
                """,
                (
                    signal.symbol,
                    signal.ts,
                    signal.signal_type,
                    signal.price,
                    signal.reason,
                    1 if acted else 0,
                    utc_now_iso(),
                ),
            )

    @retry_on_disconnect()
    def execute_buy(self, symbol: str, qty: int, price: float, ts: str, reason: str) -> None:
        now = utc_now_iso()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO positions_ledger(symbol, qty, avg_price, opened_at, last_price, last_updated_at)
                VALUES(%s, %s, %s, %s, %s, %s)
                ON CONFLICT(symbol) DO UPDATE SET
                    qty=positions_ledger.qty + EXCLUDED.qty,
                    avg_price=((positions_ledger.avg_price * positions_ledger.qty) + (EXCLUDED.avg_price * EXCLUDED.qty))
                        / (positions_ledger.qty + EXCLUDED.qty),
                    last_price=EXCLUDED.last_price,
                    last_updated_at=EXCLUDED.last_updated_at
                """,
                (symbol, qty, price, ts, price, now),
            )
            cur.execute(
                """
                INSERT INTO trade_activity_log(symbol, side, qty, price, ts, pnl, reason, created_at)
                VALUES(%s, 'BUY', %s, %s, %s, NULL, %s, %s)
                """,
                (symbol, qty, price, ts, reason, now),
            )
        # BUG-05 fix: commit immediately so the position is durable before
        # returning. Previously committed by the end-of-loop commit() — a crash
        # between execute_buy and that commit would silently lose the position.
        self.conn.commit()

    @retry_on_disconnect()
    def execute_sell(self, symbol: str, price: float, ts: str, reason: str) -> "float | None":
        with self.conn.cursor() as cur:
            cur.execute("SELECT symbol, qty, avg_price FROM positions_ledger WHERE symbol=%s", (symbol,))
            row = cur.fetchone()
            if row is None:
                return None

            qty = int(row["qty"])
            avg_price = float(row["avg_price"])
            pnl = (price - avg_price) * qty
            now = utc_now_iso()

            cur.execute("DELETE FROM positions_ledger WHERE symbol=%s", (symbol,))
            cur.execute(
                """
                INSERT INTO trade_activity_log(symbol, side, qty, price, ts, pnl, reason, created_at)
                VALUES(%s, 'SELL', %s, %s, %s, %s, %s, %s)
                """,
                (symbol, qty, price, ts, pnl, reason, now),
            )
        # BUG-05 fix: commit immediately so the closed position is durable.
        self.conn.commit()
        return pnl

    @retry_on_disconnect()
    def update_position_prices(self, prices: dict[str, tuple[float, str]]) -> None:
        rows = [(price, ts, sym) for sym, (price, ts) in prices.items()]
        from psycopg2.extras import execute_batch
        with self.conn.cursor() as cur:
            execute_batch(
                cur,
                """
                UPDATE positions_ledger
                SET last_price=%s, last_updated_at=%s
                WHERE symbol=%s
                """,
                rows,
            )

    @retry_on_disconnect()
    def snapshot_pnl(self, ts: str) -> dict[str, float]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT COALESCE(SUM(pnl), 0.0) AS realized FROM trade_activity_log WHERE side='SELL' AND symbol LIKE %s", (f"%{self.suffix}",))
            realized_row = cur.fetchone()
            cur.execute(
                """
                SELECT COALESCE(SUM((last_price - avg_price) * qty), 0.0) AS unrealized,
                       COUNT(*) AS open_positions
                FROM positions_ledger
                WHERE symbol LIKE %s
                """,
                (f"%{self.suffix}",)
            )
            unrealized_row = cur.fetchone()
            
            realized = float(realized_row["realized"] or 0.0)
            unrealized = float(unrealized_row["unrealized"] or 0.0)
            open_positions = int(unrealized_row["open_positions"] or 0)
            total = realized + unrealized

            try:
                cur.execute(
                    """
                    INSERT INTO pnl_snapshots(ts, suffix, realized_pnl, unrealized_pnl, total_pnl, open_positions)
                    VALUES(%s, %s, %s, %s, %s, %s)
                    ON CONFLICT(ts, suffix) DO UPDATE SET
                        realized_pnl=EXCLUDED.realized_pnl,
                        unrealized_pnl=EXCLUDED.unrealized_pnl,
                        total_pnl=EXCLUDED.total_pnl,
                        open_positions=EXCLUDED.open_positions
                    """,
                    (ts, self.suffix, realized, unrealized, total, open_positions),
                )
            except Exception:
                # Fallback: the PRIMARY KEY constraint may not exist on old databases yet.
                # Do a plain INSERT (migration in init_db will fix this on next startup).
                self.conn.rollback()
                with self.conn.cursor() as _cur:
                    _cur.execute(
                        """
                        INSERT INTO pnl_snapshots(ts, suffix, realized_pnl, unrealized_pnl, total_pnl, open_positions)
                        VALUES(%s, %s, %s, %s, %s, %s)
                        """,
                        (ts, self.suffix, realized, unrealized, total, open_positions),
                    )
        return {
            "realized": realized,
            "unrealized": unrealized,
            "total": total,
            "open_positions": float(open_positions),
        }

    def commit(self) -> None:
        self.conn.commit()

    @retry_on_disconnect()
    def record_run_metrics(self, payload: dict[str, Any]) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO run_metrics(
                    run_started_at, suffix, run_ended_at, status,
                    symbols_total, symbols_fetched, symbols_computed,
                    fetch_seconds, compute_seconds, persist_seconds,
                    duration_seconds, error
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    payload["run_started_at"],
                    self.suffix,
                    payload.get("run_ended_at"),
                    payload["status"],
                    payload["symbols_total"],
                    payload["symbols_fetched"],
                    payload["symbols_computed"],
                    payload["fetch_seconds"],
                    payload["compute_seconds"],
                    payload["persist_seconds"],
                    payload.get("duration_seconds"),
                    payload.get("error"),
                ),
            )
        self.conn.commit()

    @retry_on_disconnect()
    def read_df(self, sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
        # pd.read_sql_query does not support raw psycopg2 connections in pandas 2.x+.
        # Use the cursor directly to execute and build a DataFrame from column names + rows.
        with self.conn.cursor() as cur:
            cur.execute(sql, params or None)
            rows = cur.fetchall()
            if not rows:
                # Return empty DataFrame with correct column names when possible
                cols = [desc[0] for desc in cur.description] if cur.description else []
                return pd.DataFrame(columns=cols)
            cols = [desc[0] for desc in cur.description]
        return pd.DataFrame([dict(r) for r in rows], columns=cols)

    @retry_on_disconnect()
    def insert_log(self, level: str, message: str) -> None:
        # BUG-19 fix: commit immediately so log entries survive a crash between
        # heartbeat commits. The overhead is one extra round-trip per log line;
        # since logs are infrequent (only ERROR/WARNING in production) this is
        # acceptable on the 0.15 CPU budget. INFO-level logs can be buffered
        # by the caller if needed, but the DB handler always commits.
        now = utc_now_iso()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO system_logs(ts, level, suffix, message)
                VALUES(%s, %s, %s, %s)
                """,
                (now, level, self.suffix, message),
            )
        self.conn.commit()

    @retry_on_disconnect()
    def get_recent_logs(self, limit: int = 200) -> pd.DataFrame:
        return self.read_df(
            "SELECT ts, level, message FROM system_logs WHERE suffix = %s ORDER BY id DESC LIMIT %s",
            (self.suffix, limit)
        )
