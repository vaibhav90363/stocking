import os
import psycopg2
from pprint import pprint

db_url = "postgresql://postgres:Iusestocking%40123@db.pycgaxrekawtsmrwkyoe.supabase.co:5432/postgres"

def check_stuck_queries():
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    # Check for long-running queries
    cur.execute("""
        SELECT pid, usename, state, wait_event_type, wait_event, query_start, query
        FROM pg_stat_activity
        WHERE state != 'idle' AND pid != pg_backend_pid();
    """)
    print("=== ACTIVE QUERIES ===")
    for row in cur.fetchall():
        print(row)
        
    cur.execute("""
        SELECT blocked_locks.pid     AS blocked_pid,
               blocked_activity.usename  AS blocked_user,
               blocking_locks.pid     AS blocking_pid,
               blocking_activity.usename AS blocking_user,
               blocked_activity.query    AS blocked_statement,
               blocking_activity.query   AS current_statement_in_blocking_process
        FROM  pg_catalog.pg_locks         blocked_locks
        JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
        JOIN pg_catalog.pg_locks         blocking_locks 
            ON blocking_locks.locktype = blocked_locks.locktype
            AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
            AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
            AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
            AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
            AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
            AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
            AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
            AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
            AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
            AND blocking_locks.pid != blocked_locks.pid
        JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
        WHERE NOT blocked_locks.granted;
    """)
    print("\n=== BLOCKED QUERIES ===")
    for row in cur.fetchall():
        print(row)
        
    # Check locks on system_logs
    cur.execute("""
        SELECT l.relation::regclass, l.mode, l.granted, a.query, a.state
        FROM pg_locks l
        JOIN pg_stat_activity a ON l.pid = a.pid
        WHERE l.relation IN ('system_logs'::regclass, 'universe'::regclass, 'symbol_state'::regclass, 'candles_1d'::regclass);
    """)
    print("\n=== LOCKS ON RELEVANT TABLES ===")
    for row in cur.fetchall():
        print(row)

    cur.close()
    conn.close()

if __name__ == '__main__':
    check_stuck_queries()
