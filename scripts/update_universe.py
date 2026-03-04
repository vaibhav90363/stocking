#!/usr/bin/env python3
import io
import os
import sys
from pathlib import Path

# Add project root to sys.path so we can import internal modules
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd
import requests
from stocking_app.config import load_config
from stocking_app.db import TradingRepository
from stocking_app.cli import _apply_suffix
from stocking_app.strategy_loader import load_strategy

def fetch_nifty500():
    print("Fetching NIFTY 500...")
    try:
        url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            df = pd.read_csv(io.StringIO(r.text))
            if 'Symbol' in df.columns:
                return df['Symbol'].dropna().tolist()
    except Exception as e:
        print(f"Failed to fetch NIFTY 500 from NSE: {e}")
    
    # Fallback to local
    print("Falling back to local NIFTY 500 file...")
    try:
        df = pd.read_csv(ROOT / "strategies/fractal_momentum_nse/universe.csv")
        return df['Symbol' if 'Symbol' in df.columns else df.columns[0]].dropna().tolist()
    except Exception:
        return []

def fetch_ftse350():
    print("Fetching FTSE 350 from Wikipedia...")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    tickers = []
    
    try:
        html100 = requests.get('https://en.wikipedia.org/wiki/FTSE_100_Index', headers=headers).text
        for t in pd.read_html(io.StringIO(html100)):
            if 'Ticker' in t.columns:
                tickers.extend(t['Ticker'].dropna().tolist())
                break
                
        html250 = requests.get('https://en.wikipedia.org/wiki/FTSE_250_Index', headers=headers).text
        for t in pd.read_html(io.StringIO(html250)):
            if 'Ticker' in t.columns:
                tickers.extend(t['Ticker'].dropna().tolist())
                break
    except Exception as e:
        print(f"Failed to fetch FTSE 350: {e}")
        try:
            df = pd.read_csv(ROOT / "strategies/fractal_momentum_lse/universe.csv")
            return [t for t in df['Symbol' if 'Symbol' in df.columns else df.columns[0]].dropna().tolist() if not str(t).startswith('$')]
        except Exception:
            pass
            
    # Clean up (remove delisted/invalid tickers that start with $)
    cleaned = []
    for t in tickers:
        ts = str(t).replace('.', '-').strip()  # Yahoo Finance usually converts dot to dash (e.g. BT.A -> BT-A.L)
        if ts and not ts.startswith('$'):
            cleaned.append(ts)
    return cleaned

def fetch_nasdaq100():
    print("Fetching NASDAQ 100 from Wikipedia...")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    tickers = []
    try:
        html = requests.get('https://en.wikipedia.org/wiki/Nasdaq-100', headers=headers).text
        for t in pd.read_html(io.StringIO(html)):
            if 'Ticker' in t.columns:
                tickers.extend(t['Ticker'].dropna().tolist())
                break
    except Exception as e:
        print(f"Failed to fetch NASDAQ: {e}")
        try:
            df = pd.read_csv(ROOT / "strategies/fractal_momentum_nasdaq/universe.csv")
            return df['Symbol' if 'Symbol' in df.columns else df.columns[0]].dropna().tolist()
        except Exception:
            pass
            
    return [str(t).strip() for t in tickers if str(t).strip()]

def run():
    cfg = load_config()
    db_url = cfg.database_url
    if not db_url:
        print("No DATABASE_URL set. Exiting.")
        return

    repo = TradingRepository(db_url)
    
    datasets = {
        "fractal_momentum_nse": fetch_nifty500,
        "fractal_momentum_lse": fetch_ftse350,
        "fractal_momentum_nasdaq": fetch_nasdaq100
    }
    
    for s_dir, fetch_func in datasets.items():
        path = ROOT / "strategies" / s_dir
        if not path.exists():
            continue
            
        sc = load_strategy(path)
        raw_symbols = fetch_func()
        
        if not raw_symbols:
            print(f"[{s_dir}] Warning: No symbols found.")
            continue
            
        # Apply correct suffix
        symbols = []
        for val in raw_symbols:
            s = str(val).strip()
            # Strip existing suffix if accidentally included
            if sc.suffix == ".L" and s.endswith(".L"):    s = s[:-2]
            if sc.suffix == ".NS" and s.endswith(".NS"):  s = s[:-3]
            if sc.suffix == ".US" and s.endswith(".US"):  s = s[:-3]
            
            symbols.append(_apply_suffix(s, sc.suffix))
            
        repo.suffix = sc.suffix
        
        # We disable old symbols and insert/update new ones as active
        # 1. First, set is_active=0 for ALL symbols of this suffix
        with repo.conn.cursor() as cur:
            cur.execute("UPDATE universe SET is_active=0 WHERE symbol LIKE %s", (f"%{sc.suffix}",))
        repo.conn.commit()
            
        # 2. Upsert the fresh ones as active=1
        inserted = repo.upsert_universe(symbols, active=True)
        print(f"[{s_dir}] Upserted {inserted} active symbols with suffix {sc.suffix}.")
        
    print("Performing 30-day data retention cleanup on candles_5m...")
    with repo.conn.cursor() as cur:
        cur.execute("DELETE FROM candles_5m WHERE CAST(ts AS timestamptz) < NOW() - INTERVAL '30 days'")
        deleted_rows = cur.rowcount
    repo.conn.commit()
    print(f"Cleaned up {deleted_rows} old records from candles_5m.")

    repo.close()
    print("Universe update and system cleanup complete.")

if __name__ == "__main__":
    run()
