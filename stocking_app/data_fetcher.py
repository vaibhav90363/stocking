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

    # OOM-FIX: downcast to float32 — halves memory vs float64 default
    for col in ["open", "high", "low", "close", "volume"]:
        if col in data.columns:
            data[col] = data[col].astype("float32")

    return data





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
                threads=True,
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
                            df = data.xs(y_sym, level="Ticker", axis=1).copy()
                        else:
                            results.append(FetchResult(symbol=engine_sym, bars=pd.DataFrame(), error="Not found in Yahoo batch"))
                            continue
                    else:
                        df = data.copy() if len(yahoo_symbols) == 1 else pd.DataFrame()

                    df = df.dropna(how="all")
                    bars = _normalize_ohlcv(df)
                    del df  # OOM-FIX-v2: free per-symbol copy immediately
                    if bars.empty:
                        results.append(FetchResult(symbol=engine_sym, bars=bars, error="No daily candles returned"))
                    else:
                        results.append(FetchResult(symbol=engine_sym, bars=bars, error=None))
                except Exception as e:
                    results.append(FetchResult(symbol=engine_sym, bars=pd.DataFrame(), error=f"Parse error: {e}"))

            # OOM-FIX-v2: Release bulk download DataFrame before returning
            del data
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


def fetch_daily_bars_gen(
    symbols: list[str], lookback_days: int, max_concurrency: int = 5
) -> list[FetchResult]:
    """Generator that fetches daily (1d) bars for all symbols in larger batches.

    Daily bars are ~75× smaller than 5m bars so we use bigger batches (50 symbols)
    and higher concurrency — Yahoo is far more lenient with daily interval requests.
    """
    BATCH_SIZE = 50  # 50 symbols per request is fine for daily interval

    batches = [symbols[i : i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    # BUG-FIX: yfinance internal threading (threads=True) is NOT thread-safe across
    # multiple python concurrent threads. It will scramble responses. We MUST
    # feed batches linearly (concurrency=1) and let yfinance parallelise internally.
    
    for i, batch in enumerate(batches):
        results = _fetch_daily_batch_blocking(batch, lookback_days, i)
        for r in results:
            yield r

