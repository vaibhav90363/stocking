import sys, psycopg2, os
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv(".env")
conn = psycopg2.connect(dsn=os.getenv("DATABASE_URL"))

try:
    with conn.cursor() as cur:
        query = """
                INSERT INTO candles_1d(symbol, ts, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT(symbol, ts) DO UPDATE SET
                    open=EXCLUDED.open,
                    high=EXCLUDED.high,
                    low=EXCLUDED.low,
                    close=EXCLUDED.close,
                    volume=EXCLUDED.volume
                """
        # run with empty to see if it syntax errors
        # execute_values takes a list of sequences
        rows = [("TEST.US", "2026-03-11T00:00:00+00:00", 1.0, 2.0, 0.5, 1.5, 100)]
        execute_values(cur, query, rows)
    print("upsert_candles_1d OK")
    conn.rollback()
except Exception as e:
    print(f"upsert_candles_1d FAILED: {type(e).__name__}: {e}")
    conn.rollback()

try:
    with conn.cursor() as cur:
        query = """
                INSERT INTO symbol_state(symbol, last_candle_ts, last_compute_ts, updated_at)
                VALUES %s
                ON CONFLICT(symbol) DO UPDATE SET
                    last_compute_ts=EXCLUDED.last_compute_ts,
                    updated_at=EXCLUDED.updated_at
                """
        rows = [("TEST.US", None, "2026-03-11T00:00:00+00:00", "2026-03-11T00:00:00+00:00")]
        execute_values(cur, query, rows)
    print("batch_mark_symbols_computed OK")
    conn.rollback()
except Exception as e:
    print(f"batch_mark_symbols_computed FAILED: {type(e).__name__}: {e}")
    conn.rollback()


