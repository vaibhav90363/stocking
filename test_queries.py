import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os

load_dotenv(".env")
conn = psycopg2.connect(dsn=os.getenv("DATABASE_URL"))
print("Connected.")

suffix = ".US"

queries = {
    "q1": ("""
            SELECT u.symbol, u.is_active,
                SUBSTR(ss.last_candle_ts, 1, 19)  AS last_fetched,
                SUBSTR(ss.last_compute_ts, 1, 19) AS last_computed,
                CASE
                  WHEN ss.last_candle_ts IS NULL THEN 'Never fetched'
                  WHEN ss.last_compute_ts IS NULL THEN 'Never computed'
                  WHEN ss.last_candle_ts > ss.last_compute_ts THEN 'Compute pending'
                  ELSE 'Up to date'
                END AS sync_status,
                SUBSTR(ss.updated_at, 1, 19) AS state_updated
            FROM universe u
            LEFT JOIN symbol_state ss ON ss.symbol = u.symbol
            WHERE u.symbol LIKE %s
            ORDER BY u.is_active DESC, sync_status, u.symbol
            """,(f"%{suffix}",)),
    "q2": ("""
            SELECT signal_type, COUNT(*) AS total, SUM(acted) AS acted,
                COUNT(*) - SUM(acted) AS not_acted,
                ROUND(AVG(price)::NUMERIC,4) AS avg_price, MAX(ts) AS latest
            FROM signals WHERE symbol LIKE %s GROUP BY signal_type
            """,(f"%{suffix}",)),
    "q3": ("""
            SELECT id, symbol, SUBSTR(ts,1,19) AS ts, signal_type, ROUND(price::NUMERIC,4) AS price, reason, acted
            FROM signals
            WHERE symbol LIKE %s
             ORDER BY id DESC LIMIT 50 
            """,(f"%{suffix}",)),
    "q4": ("""
            SELECT id, symbol, side, qty, ROUND(price::NUMERIC,4) AS price, 
                   SUBSTR(ts,1,19) AS ts, ROUND(pnl::NUMERIC,2) AS pnl, reason
            FROM trade_activity_log
            WHERE symbol LIKE %s
             ORDER BY id DESC LIMIT 100
            """,(f"%{suffix}",)),
}

for name, (sql, params) in queries.items():
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        print(f"{name} OK")
    except Exception as e:
        print(f"{name} FAILED: {e}")
        conn.rollback()

