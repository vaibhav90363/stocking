from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    db_path: Path
    database_url: str
    cycle_seconds: int
    disabled_poll_seconds: int
    fetch_lookback_days: int
    compute_lookback_days: int
    max_fetch_concurrency: int
    compute_workers: int
    order_qty: int
    ticker_suffix: str
    # Market-hours auto-scheduler
    exchange_tz:  str = "Asia/Kolkata"
    market_open:  str = "09:15"
    market_close: str = "15:30"
    auto_schedule: bool = True   # False = manual Start/Stop only
    # Per-engine fetch stagger: delay this engine's first fetch by N seconds so
    # all 3 engines don't burst Yahoo Finance simultaneously from the same IP.
    fetch_start_delay_seconds: int = 0



def load_config() -> AppConfig:
    from dotenv import load_dotenv
    load_dotenv()
    root = Path(__file__).resolve().parents[1]
    default_db = root / "data" / "stocking.db"

    database_url = os.getenv("DATABASE_URL", "")
    try:
        import streamlit as st
        if "DATABASE_URL" in st.secrets:
            database_url = st.secrets["DATABASE_URL"]
    except Exception:
        pass

    suffix = os.getenv("STOCKING_TICKER_SUFFIX", ".NS")

    # Derive sensible market-hour defaults from the suffix
    from .market_schedule import defaults_for_suffix
    def_tz, def_open, def_close = defaults_for_suffix(suffix)

    url_str = database_url.strip() if database_url else ""
    return AppConfig(
        db_path=Path(os.getenv("STOCKING_DB_PATH", default_db)),
        database_url=url_str,
        cycle_seconds=int(os.getenv("STOCKING_CYCLE_SECONDS", "300")),
        disabled_poll_seconds=int(os.getenv("STOCKING_DISABLED_POLL_SECONDS", "60")),
        fetch_lookback_days=int(os.getenv("STOCKING_FETCH_LOOKBACK_DAYS", "3")),
        # 90 days gives enough bar history for weekly signals (~7k rows/symbol)
        # vs 365 days (28k rows/symbol) which OOMs a 512 MB Render instance with 3 engines
        compute_lookback_days=int(os.getenv("STOCKING_COMPUTE_LOOKBACK_DAYS", "90")),
        # 5 concurrent batches is safe for Yahoo Finance free tier
        max_fetch_concurrency=int(os.getenv("STOCKING_FETCH_CONCURRENCY", "5")),
        # 1 worker avoids fork/spawn overhead & CPU thrashing on 0.15 CPU free tier
        compute_workers=int(os.getenv("STOCKING_COMPUTE_WORKERS", "1")),
        order_qty=int(os.getenv("STOCKING_ORDER_QTY", "1")),
        ticker_suffix=suffix,
        exchange_tz=os.getenv("STOCKING_EXCHANGE_TZ", def_tz),
        market_open=os.getenv("STOCKING_MARKET_OPEN", def_open),
        market_close=os.getenv("STOCKING_MARKET_CLOSE", def_close),
        auto_schedule=os.getenv("STOCKING_AUTO_SCHEDULE", "1") not in ("0", "false", "False"),
        fetch_start_delay_seconds=int(os.getenv("STOCKING_FETCH_START_DELAY", "0")),
    )
