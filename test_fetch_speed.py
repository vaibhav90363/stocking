import asyncio
import time
import sys
import pandas as pd
from stocking_app.data_fetcher import fetch_daily_bars_async_gen

async def main():
    try:
        df = pd.read_csv("strategies/fractal_momentum_nse/universe.csv")
        symbols = (df["Symbol"] + ".NS").tolist()[:500]
    except Exception as e:
        print(f"Error reading universe: {e}")
        return
    
    print(f"Testing fetch for {len(symbols)} symbols...")
    start = time.time()
    gen = fetch_daily_bars_async_gen(symbols, lookback_days=2, max_concurrency=1)
    
    success = 0
    failed = 0
    async for result in gen:
        if result.error or result.bars.empty:
            failed += 1
            if failed == 1:
                print(f"Sample error: {result.error}")
        else:
            success += 1
            
    elapsed = time.time() - start
    print(f"Fetched {success} symbols successfully, {failed} failed in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    asyncio.run(main())    
