import sys, psycopg2, os
from dotenv import load_dotenv

load_dotenv(".env")
conn = psycopg2.connect(dsn=os.getenv("DATABASE_URL"))
suffix = ".US"

queries = {
    "history": ("""
            SELECT id AS cycle,
                   SUBSTR(ts, 1, 19) AS time,
                   status,
                   symbols_fetched AS fetched,
                   symbols_total AS total,
                   ROUND(elapsed_sec::NUMERIC,1) AS elapsed_s
            FROM run_metrics
            WHERE suffix=%s
            ORDER BY id DESC LIMIT 20
            """, (suffix,)),
    "pnl_hist": ("""
            SELECT ts, realized_pnl, unrealized_pnl, total_pnl
            FROM pnl_snapshots
            WHERE suffix=%s
            ORDER BY ts
            """, (suffix,)),
    "positions": ("""
            SELECT symbol, 
                   qty, 
                   ROUND(avg_price::NUMERIC,2) AS avg_px, 
                   ROUND(last_price::NUMERIC,2) AS current_px, 
                   ROUND(((last_price - avg_price) * qty)::NUMERIC,2) AS unrealized_pnl,
                   SUBSTR(opened_at,1,19) AS opened_at,
                   SUBSTR(last_updated_at,1,19) AS last_upd
            FROM positions_ledger
            WHERE symbol LIKE %s
            ORDER BY unrealized_pnl DESC
            """, (f"%{suffix}",)),
}

for name, (sql, params) in queries.items():
    try:
        with conn.cursor() as cur:
            cur.execute("EXPLAIN " + sql, params)
        print(f"{name} OK")
    except Exception as e:
        print(f"{name} FAILED: {type(e).__name__}: {e}")
        conn.rollback()

