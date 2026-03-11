import psycopg2

db_url = "postgresql://postgres:Iusestocking%40123@db.pycgaxrekawtsmrwkyoe.supabase.co:5432/postgres"

def kill_idle_transactions():
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    cur = conn.cursor()
    # Check for long-running queries
    cur.execute("""
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE state = 'idle in transaction' AND pid != pg_backend_pid();
    """)
    print(f"Terminated {cur.rowcount} idle in transaction processes.")
    cur.close()
    conn.close()

if __name__ == '__main__':
    kill_idle_transactions()
