import sys, psycopg2, os
from dotenv import load_dotenv

load_dotenv(".env")
conn = psycopg2.connect(dsn=os.getenv("DATABASE_URL"))
suffix = ".US"

q = """
                        SELECT
                            symbol, qty,
                            ROUND(avg_price::NUMERIC, 2) AS avg_price,
                            opened_at
                        FROM positions_ledger
                        WHERE symbol LIKE %s
                        """
try:
    with conn.cursor() as cur:
        cur.execute(q, (f"%{suffix}",))
    print("hub_query OK")
except Exception as e:
    print(f"FAILED: {type(e).__name__}: {e}")

