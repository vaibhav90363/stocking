from __future__ import annotations

import json
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
    """Decorator to catch 'server closed the connection unexpectedly' errors and reconnect."""
    def decorator(func):
        from functools import wraps
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            import psycopg2
            import time
            last_err = None
            for attempt in range(max_retries):
                try:
                    self._ensure_connection()
                    return func(self, *args, **kwargs)
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    last_err = e
                    # Connection dropped - wipe it so _ensure_connection opens a new one
                    if self.conn:
                        try:
                            self.conn.close()
                        except Exception:
                            pass
                    self.conn = None
                    # Exponential backoff: 0.5s, 1s, 2s between retries
                    if attempt < max_retries - 1:
                        time.sleep(0.5 * (2 ** attempt))
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

CREATE TABLE IF NOT EXISTS candles_5m (
    symbol TEXT NOT NULL,
    ts TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL,
    PRIMARY KEY (symbol, ts)
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
"""


@dataclass
class SignalRecord:
    symbol: str
    ts: str
    signal_type: str
    price: float | None
    reason: str


class TradingRepository:
    # Class-level cache of connection pools by database URL
    _pools: dict[str, ThreadedConnectionPool] = {}

    @classmethod
    def get_pool(cls, db_url: str) -> ThreadedConnectionPool:
        from psycopg2.extras import RealDictCursor
        if db_url not in cls._pools:
            cls._pools[db_url] = ThreadedConnectionPool(
                minconn=1,
                maxconn=20,     # Safely within Supabase free-tier limits, spread across workers
                dsn=db_url,
                cursor_factory=RealDictCursor,
                keepalives=1,
                keepalives_idle=60,
                keepalives_interval=10,
                keepalives_count=5
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
    def upsert_candles(self, symbol: str, bars: pd.DataFrame) -> int:
        if bars.empty:
            return 0
        rows: list[tuple[Any, ...]] = []
        for ts, row in bars.iterrows():
            rows.append(
                (
                    symbol,
                    pd.Timestamp(ts).to_pydatetime().replace(tzinfo=timezone.utc).isoformat(),
                    float(row["open"]),
                    float(row["high"]),
                    float(row["low"]),
                    float(row["close"]),
                    float(row.get("volume", 0.0)),
                )
            )
        from psycopg2.extras import execute_values
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO candles_5m(symbol, ts, open, high, low, close, volume)
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

            max_ts = max(r[1] for r in rows)
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
    def mark_symbol_computed(self, symbol: str, asof_ts: str | None) -> None:
        now = utc_now_iso()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO symbol_state(symbol, last_candle_ts, last_compute_ts, updated_at)
                VALUES(%s, NULL, %s, %s)
                ON CONFLICT(symbol) DO UPDATE SET
                    last_compute_ts=EXCLUDED.last_compute_ts,
                    updated_at=EXCLUDED.updated_at
                """,
                (symbol, asof_ts, now),
            )
        self.conn.commit()

    @retry_on_disconnect()
    def get_open_positions(self) -> dict[str, dict]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM positions_ledger WHERE symbol LIKE %s ORDER BY symbol", (f"%{self.suffix}",))
            rows = cur.fetchall()
        return {r["symbol"]: dict(r) for r in rows}

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
    def get_all_candles_for_symbols(self, symbols: list[str], lookback_days: int) -> pd.DataFrame:
        """
        Pulls candles for multiple symbols at once into a single giant DataFrame.
        This minimizes separate DB connections and scalar queries.
        """
        import pandas as pd
        from datetime import datetime, timedelta, timezone
        
        if not symbols:
            return pd.DataFrame()

        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=lookback_days)
        ).replace(microsecond=0).isoformat()
        
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, ts, open, high, low, close, volume
                FROM candles_5m
                WHERE symbol = ANY(%s) AND ts >= %s
                ORDER BY ts
                """,
                (symbols, cutoff),
            )
            rows = cur.fetchall()
            
            if not rows:
                return pd.DataFrame()
            cols = [desc[0] for desc in cur.description]

        df = pd.DataFrame([dict(r) for r in rows], columns=cols)
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        return df

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

    @retry_on_disconnect()
    def execute_sell(self, symbol: str, price: float, ts: str, reason: str) -> float | None:
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
