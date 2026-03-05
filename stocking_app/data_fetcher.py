from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import AsyncGenerator

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


def _fetch_batch_blocking(
    batch: list[str], lookback_days: int, batch_index: int = 0
) -> list[FetchResult]:
    """Fetch a batch of symbols from Yahoo Finance with staggered jitter + retry/backoff.

    Args:
        batch:        list of engine symbols (may have .US / .L suffixes)
        lookback_days: yfinance period in calendar days
        batch_index:  position of this batch in the overall queue — used to stagger
                      start times so batches don't all hit Yahoo simultaneously.
    """
    if not batch:
        return []

    # ── Staggered jitter ──────────────────────────────────────────────────────
    # To prevent all concurrent workers from hitting Yahoo at exactly the same time,
    # we stagger the initial batches. For later batches, we just use a small jitter.
    if batch_index < 2:
        stagger_sleep = batch_index * 1.5 + random.uniform(0.0, 1.0)
    else:
        stagger_sleep = random.uniform(1.0, 2.0)
    time.sleep(stagger_sleep)

    # Map engine symbols → Yahoo-compatible symbols (strip .US suffix)
    yahoo_to_engine: dict[str, str] = {}
    yahoo_symbols: list[str] = []
    for sym in batch:
        y_sym = sym[:-3] if sym.endswith(".US") else sym
        yahoo_symbols.append(y_sym)
        yahoo_to_engine[y_sym] = sym

    MAX_RETRIES = 3
    BACKOFF_BASE = 10  # seconds; doubles each retry: 10 → 20 → 40

    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            data = yf.download(
                tickers=" ".join(yahoo_symbols),
                period=f"{lookback_days}d",
                interval="5m",
                auto_adjust=True,
                prepost=False,
                group_by="ticker",
                threads=False,   # we control concurrency via asyncio semaphore
                progress=False,  # suppress tqdm stdout noise
                timeout=30,
            )

            is_multi = isinstance(data.columns, pd.MultiIndex)

            results: list[FetchResult] = []
            for y_sym in yahoo_symbols:
                engine_sym = yahoo_to_engine[y_sym]
                try:
                    if is_multi:
                        if y_sym in data.columns.get_level_values("Ticker"):
                            df = data[y_sym].copy()
                        else:
                            results.append(FetchResult(symbol=engine_sym, bars=pd.DataFrame(), error="Not found in Yahoo batch"))
                            continue
                    else:
                        # Single-ticker fetch — yfinance returns a flat DataFrame
                        if len(yahoo_symbols) == 1:
                            df = data.copy()
                        else:
                            df = pd.DataFrame()

                    df = df.dropna(how="all")
                    bars = _normalize_ohlcv(df)

                    if bars.empty:
                        results.append(FetchResult(symbol=engine_sym, bars=bars, error="No 5m candles returned"))
                    else:
                        results.append(FetchResult(symbol=engine_sym, bars=bars, error=None))

                except Exception as e:
                    results.append(FetchResult(symbol=engine_sym, bars=pd.DataFrame(), error=f"Parse error: {e}"))

            return results

        except Exception as exc:
            last_exc = exc
            err_str = str(exc).lower()
            is_rate_limit = "rate limit" in err_str or "too many requests" in err_str or "429" in err_str
            if attempt < MAX_RETRIES - 1:
                # Rate limit: use much longer backoff so Yahoo can cool down (LSE/NSE
                # share the same IP and hit 429 often when all engines run together).
                if is_rate_limit:
                    wait = 30 + (attempt * 25) + random.uniform(0, 15)  # 30–35s, 55–70s, 80–95s
                else:
                    wait = BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 3)
                time.sleep(wait)
            else:
                break

    return [
        FetchResult(symbol=sym, bars=pd.DataFrame(), error=f"Batch API error after {MAX_RETRIES} retries: {last_exc}")
        for sym in batch
    ]


async def fetch_5m_bars_async_gen(
    symbols: list[str], lookback_days: int, max_concurrency: int = 5
) -> AsyncGenerator[FetchResult, None]:
    """Async generator that fetches 5-minute bars for all symbols in small batches.

    Concurrency is capped at 2 for the 5m interval to avoid Yahoo Finance rate limits.
    Batches are intentionally small (10 symbols) so each HTTP request stays lightweight
    and is less likely to be throttled than large multi-ticker payloads.
    """
    BATCH_SIZE = 10  # small batches → lighter requests, less likely to be 429'd

    batches = [symbols[i : i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]

    # Hard cap at 2 for 5m interval — Yahoo is aggressive about throttling concurrent
    # multi-ticker 5m requests.  The stagger jitter inside each batch spreads the load
    # further so we effectively drip-feed requests into Yahoo at ~1 per 1.5s per slot.
    effective_concurrency = min(max_concurrency, 2)
    semaphore = asyncio.Semaphore(max(1, effective_concurrency))

    async def _wrapped(batch: list[str], idx: int) -> list[FetchResult]:
        async with semaphore:
            try:
                # Timeout budget: 30s download × 3 retries + backoff (10+20+40s) = ~130s
                return await asyncio.wait_for(
                    asyncio.to_thread(_fetch_batch_blocking, batch, lookback_days, idx),
                    timeout=140.0,
                )
            except asyncio.TimeoutError:
                return [
                    FetchResult(symbol=s, bars=pd.DataFrame(), error="Batch fetch timed out after 140s")
                    for s in batch
                ]
            except Exception as exc:
                return [
                    FetchResult(symbol=s, bars=pd.DataFrame(), error=f"Batch fetch failed: {exc}")
                    for s in batch
                ]

    tasks = [_wrapped(b, i) for i, b in enumerate(batches)]

    for completed_task in asyncio.as_completed(tasks):
        batch_results = await completed_task
        for r in batch_results:
            yield r


def _fetch_daily_batch_blocking(
    batch: list[str], lookback_days: int, batch_index: int = 0
) -> list[FetchResult]:
    """Fetch daily (1d interval) bars for a batch of symbols.

    Daily bars are tiny (~90 rows/symbol), so we use larger batches and higher
    concurrency than 5m bars. Same retry/backoff pattern.
    """
    if not batch:
        return []

    if batch_index < 5:
        stagger_sleep = batch_index * 0.5 + random.uniform(0.0, 0.3)
    else:
        stagger_sleep = random.uniform(0.5, 1.0)
    time.sleep(stagger_sleep)

    yahoo_to_engine: dict[str, str] = {}
    yahoo_symbols: list[str] = []
    for sym in batch:
        y_sym = sym[:-3] if sym.endswith(".US") else sym
        yahoo_symbols.append(y_sym)
        yahoo_to_engine[y_sym] = sym

    MAX_RETRIES = 3
    BACKOFF_BASE = 5  # shorter backoff for daily — less rate-limit risk

    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            data = yf.download(
                tickers=" ".join(yahoo_symbols),
                period=f"{lookback_days}d",
                interval="1d",
                auto_adjust=True,
                prepost=False,
                group_by="ticker",
                threads=False,
                progress=False,
                timeout=30,
            )

            is_multi = isinstance(data.columns, pd.MultiIndex)
            results: list[FetchResult] = []
            for y_sym in yahoo_symbols:
                engine_sym = yahoo_to_engine[y_sym]
                try:
                    if is_multi:
                        if y_sym in data.columns.get_level_values("Ticker"):
                            df = data[y_sym].copy()
                        else:
                            results.append(FetchResult(symbol=engine_sym, bars=pd.DataFrame(), error="Not found in Yahoo batch"))
                            continue
                    else:
                        df = data.copy() if len(yahoo_symbols) == 1 else pd.DataFrame()

                    df = df.dropna(how="all")
                    bars = _normalize_ohlcv(df)
                    if bars.empty:
                        results.append(FetchResult(symbol=engine_sym, bars=bars, error="No daily candles returned"))
                    else:
                        results.append(FetchResult(symbol=engine_sym, bars=bars, error=None))
                except Exception as e:
                    results.append(FetchResult(symbol=engine_sym, bars=pd.DataFrame(), error=f"Parse error: {e}"))
            return results

        except Exception as exc:
            last_exc = exc
            err_str = str(exc).lower()
            is_rate_limit = "rate limit" in err_str or "too many requests" in err_str or "429" in err_str
            if attempt < MAX_RETRIES - 1:
                if is_rate_limit:
                    wait = 25 + (attempt * 20) + random.uniform(0, 10)  # 25–35s, 45–55s, 65–75s
                else:
                    wait = BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 2)
                time.sleep(wait)
            else:
                break

    return [
        FetchResult(symbol=sym, bars=pd.DataFrame(), error=f"Daily batch API error after {MAX_RETRIES} retries: {last_exc}")
        for sym in batch
    ]


async def fetch_daily_bars_async_gen(
    symbols: list[str], lookback_days: int, max_concurrency: int = 5
) -> AsyncGenerator[FetchResult, None]:
    """Async generator that fetches daily (1d) bars for all symbols in larger batches.

    Daily bars are ~75× smaller than 5m bars so we use bigger batches (50 symbols)
    and higher concurrency — Yahoo is far more lenient with daily interval requests.
    """
    BATCH_SIZE = 50  # 50 symbols per request is fine for daily interval

    batches = [symbols[i : i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    effective_concurrency = min(max_concurrency, 5)
    semaphore = asyncio.Semaphore(max(1, effective_concurrency))

    async def _wrapped(batch: list[str], idx: int) -> list[FetchResult]:
        async with semaphore:
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(_fetch_daily_batch_blocking, batch, lookback_days, idx),
                    timeout=60.0,
                )
            except asyncio.TimeoutError:
                return [FetchResult(symbol=s, bars=pd.DataFrame(), error="Daily batch timed out") for s in batch]
            except Exception as exc:
                return [FetchResult(symbol=s, bars=pd.DataFrame(), error=f"Daily batch failed: {exc}") for s in batch]

    tasks = [_wrapped(b, i) for i, b in enumerate(batches)]
    for completed_task in asyncio.as_completed(tasks):
        batch_results = await completed_task
        for r in batch_results:
            yield r

