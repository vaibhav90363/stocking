import os
from pathlib import Path
from stocking_app.config import load_config
from stocking_app.db import TradingRepository
from stocking_app.strategy_loader import load_strategy

def run():
    cfg = load_config()
    db_url = cfg.database_url
    if not db_url:
        print("No DATABASE_URL set.")
        return

    repo = TradingRepository(db_url)
    
    print("Clearing universe and run_metrics to fix namespace pollution...")
    with repo.conn.cursor() as cur:
        cur.execute("DELETE FROM universe;")
        cur.execute("DELETE FROM run_metrics;")
    repo.conn.commit()
    
    import pandas as pd
    from stocking_app.cli import _apply_suffix
    from run_strategy import _detect_symbol_column
    
    for s_dir in ["fractal_momentum_nse", "fractal_momentum_lse", "fractal_momentum_nasdaq"]:
        path = Path("strategies") / s_dir
        if not path.exists():
            continue
            
        sc = load_strategy(path)
        csv_path = sc.universe_csv
        if not csv_path.exists():
            print(f"Skipping {s_dir}, no universe.csv")
            continue
            
        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
        except Exception as e:
            try:
                df = pd.read_csv(csv_path, encoding='latin-1')
            except Exception as e2:
                print(f"Failed to read {csv_path}: {e2}")
                continue
        sym_col = _detect_symbol_column(csv_path)
        
        symbols = []
        for val in df[sym_col].dropna():
            # For LSE, some might already have .L inside the CSV. We clean them up.
            s = str(val).strip()
            if sc.suffix == ".L" and s.endswith(".L"):
                s = s[:-2]
            if sc.suffix == ".NS" and s.endswith(".NS"):
                s = s[:-3]
            if sc.suffix == ".US" and s.endswith(".US"):
                s = s[:-3]
                
            symbols.append(_apply_suffix(s, sc.suffix))
            
        repo.suffix = sc.suffix
        inserted = repo.upsert_universe(symbols)
        print(f"[{s_dir}] Inserted {inserted} items with suffix {sc.suffix} via column '{sym_col}'.")

    repo.close()
    
if __name__ == "__main__":
    run()
