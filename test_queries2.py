import sys, psycopg2, os
from dotenv import load_dotenv

load_dotenv(".env")
conn = psycopg2.connect(dsn=os.getenv("DATABASE_URL"))
suffix = ".US"

q = """
            SELECT id AS cycle,
                   SUBSTR(ts, 1, 19) AS time,
                   status,
                   symbols_fetched AS fetched,
                   symbols_total AS total,
                   ROUND(elapsed_sec::NUMERIC,1) AS elapsed_s
            FROM run_metrics
            WHERE suffix=%s
            ORDER BY id DESC LIMIT 20
            """
try:
    with conn.cursor() as cur:
        cur.execute(q, (suffix,))
    print("q OK")
except Exception as e:
    print(f"FAILED: {e}")
