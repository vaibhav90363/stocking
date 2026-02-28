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
        disabled_poll_seconds=int(os.getenv("STOCKING_DISABLED_POLL_SECONDS", "5")),
        fetch_lookback_days=int(os.getenv("STOCKING_FETCH_LOOKBACK_DAYS", "10")),
        compute_lookback_days=int(os.getenv("STOCKING_COMPUTE_LOOKBACK_DAYS", "365")),
        max_fetch_concurrency=int(os.getenv("STOCKING_FETCH_CONCURRENCY", "12")),
        compute_workers=int(os.getenv("STOCKING_COMPUTE_WORKERS", "4")),
        order_qty=int(os.getenv("STOCKING_ORDER_QTY", "1")),
        ticker_suffix=suffix,
        exchange_tz=os.getenv("STOCKING_EXCHANGE_TZ", def_tz),
        market_open=os.getenv("STOCKING_MARKET_OPEN", def_open),
        market_close=os.getenv("STOCKING_MARKET_CLOSE", def_close),
        auto_schedule=os.getenv("STOCKING_AUTO_SCHEDULE", "1") not in ("0", "false", "False"),
    )
