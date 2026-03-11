import ast
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv(".env")
conn = psycopg2.connect(dsn=os.getenv("DATABASE_URL"))

def check_file(filename):
    with open(filename, "r") as f:
        # Find all read_df calls
        content = f.read()

    tree = ast.parse(content)
    # Just grab all string constants and try to prepare them
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            q = node.value.strip()
            if "SELECT" in q.upper():
                # Try to prepare
                if "%s" in q:
                    q = q.replace("%s", "NULL")
                try:
                    with conn.cursor() as cur:
                        cur.execute("EXPLAIN " + q)
                except psycopg2.errors.SyntaxError as e:
                    print(f"SyntaxError in {filename}:\n{q}\n---\n{e}")
                except Exception as e:
                    pass
                conn.rollback()

check_file("dashboard.py")
check_file("hub.py")
check_file("stocking_app/db.py")
