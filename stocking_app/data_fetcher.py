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


def _fetch_symbol_blocking(symbol: str, lookback_days: int) -> FetchResult:
    try:
        hist = yf.Ticker(symbol).history(
            period=f"{lookback_days}d",
            interval="5m",
            auto_adjust=True,
            prepost=False,
        )
        bars = _normalize_ohlcv(hist)
        return FetchResult(symbol=symbol, bars=bars, error=None)
    except Exception as exc:  # noqa: BLE001
        return FetchResult(symbol=symbol, bars=pd.DataFrame(), error=str(exc))


async def fetch_5m_bars_async_gen(
    symbols: list[str], lookback_days: int, max_concurrency: int
) -> AsyncGenerator[FetchResult, None]:
    # We yield results asynchronously as they complete
    semaphore = asyncio.Semaphore(max(1, max_concurrency))

    async def _wrapped(symbol: str) -> FetchResult:
        async with semaphore:
            try:
                # Add strict 15-second timeout to prevent indefinite hangs from Yahoo Finance
                return await asyncio.wait_for(
                    asyncio.to_thread(_fetch_symbol_blocking, symbol, lookback_days),
                    timeout=15.0
                )
            except asyncio.TimeoutError:
                return FetchResult(symbol=symbol, bars=pd.DataFrame(), error="Fetch timed out after 15s")
            except Exception as exc:
                return FetchResult(symbol=symbol, bars=pd.DataFrame(), error=f"Fetch failed: {exc}")

    tasks = [_wrapped(symbol) for symbol in symbols]
    for completed_task in asyncio.as_completed(tasks):
        yield await completed_task
