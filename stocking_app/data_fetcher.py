from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, AsyncGenerator

import pandas as pd
import yfinance as yf


@dataclass
class FetchResult:
    symbol: str
    bars: pd.DataFrame
    error: str | None


def _normalize_ohlcv(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return data

    cols = {c: str(c).strip().lower() for c in data.columns}
    data = data.rename(columns=cols)

    required = ["open", "high", "low", "close"]
    if not all(c in data.columns for c in required):
        return pd.DataFrame()

    keep = ["open", "high", "low", "close", "volume"]
    present = [c for c in keep if c in data.columns]
    data = data[present].copy()

    if "volume" not in data.columns:
        data["volume"] = 0.0

    idx = pd.to_datetime(data.index, utc=True)
    data.index = idx
    data = data[~data.index.duplicated(keep="last")]
    data.sort_index(inplace=True)

    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    data = data.dropna(subset=["open", "high", "low", "close"])
    return data


def _fetch_batch_blocking(batch: list[str], lookback_days: int) -> list[FetchResult]:
    if not batch:
        return []

    # Map engine symbols back to Yahoo-compatible symbols (e.g., remove .US suffix)
    yahoo_to_engine = {}
    yahoo_symbols = []
    
    for sym in batch:
        y_sym = sym[:-3] if sym.endswith('.US') else sym
        yahoo_symbols.append(y_sym)
        yahoo_to_engine[y_sym] = sym

    results = []
    try:
        data = yf.download(
            tickers=" ".join(yahoo_symbols),
            period=f"{lookback_days}d",
            interval="5m",
            auto_adjust=True,
            prepost=False,
            group_by="ticker",
            threads=False,   # Disable yfinance internal threading — we control concurrency via asyncio
            progress=False,  # Suppress tqdm stdout I/O
            timeout=30,
        )
        
        # If only 1 symbol was successfully fetched, yfinance returns a flat structure
        # If multiple, it returns a MultiIndex column structure
        is_multi = isinstance(data.columns, pd.MultiIndex)
        
        for y_sym in yahoo_symbols:
            engine_sym = yahoo_to_engine[y_sym]
            
            try:
                if is_multi:
                    if y_sym in data.columns.get_level_values('Ticker'):
                        df = data[y_sym].copy()
                    else:
                        results.append(FetchResult(symbol=engine_sym, bars=pd.DataFrame(), error="Not found in Yahoo batch"))
                        continue
                else:
                    # Single flat lookup
                    if len(yahoo_symbols) == 1:
                        df = data.copy()
                    else:
                        # Should not happen typically, but fallback
                        df = pd.DataFrame()

                df = df.dropna(how='all')
                bars = _normalize_ohlcv(df)
                
                if bars.empty:
                    results.append(FetchResult(symbol=engine_sym, bars=bars, error="No 5m candles returned"))
                else:
                    results.append(FetchResult(symbol=engine_sym, bars=bars, error=None))
                    
            except Exception as e:
                results.append(FetchResult(symbol=engine_sym, bars=pd.DataFrame(), error=f"Parse scalar error: {e}"))
                
    except Exception as exc:  # noqa: BLE001
        for sym in batch:
            results.append(FetchResult(symbol=sym, bars=pd.DataFrame(), error=f"Batch API error: {exc}"))

    return results

async def fetch_5m_bars_async_gen(
    symbols: list[str], lookback_days: int, max_concurrency: int = 5
) -> AsyncGenerator[FetchResult, None]:
    # We yield results asynchronously as batch loops complete
    # Chunk the symbols into blocks of 100 to avoid long URL lengths
    BATCH_SIZE = 50   # Smaller batches = less Yahoo throttling and more reliable results
    batches = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    
    semaphore = asyncio.Semaphore(max(1, max_concurrency))

    async def _wrapped(batch: list[str]) -> list[FetchResult]:
        async with semaphore:
            try:
                # Add strict 45-second timeout for a 100-symbol batch
                return await asyncio.wait_for(
                    asyncio.to_thread(_fetch_batch_blocking, batch, lookback_days),
                    timeout=45.0
                )
            except asyncio.TimeoutError:
                return [FetchResult(symbol=s, bars=pd.DataFrame(), error="Batch fetch timed out after 45s") for s in batch]
            except Exception as exc:
                return [FetchResult(symbol=s, bars=pd.DataFrame(), error=f"Batch fetch failed: {exc}") for s in batch]

    tasks = [_wrapped(b) for b in batches]
    
    # As each 100-symbol batch finishes, yield its individual FetchResults
    for completed_task in asyncio.as_completed(tasks):
        batch_results = await completed_task
        for r in batch_results:
            yield r
