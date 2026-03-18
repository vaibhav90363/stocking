from __future__ import annotations

import os
from dataclasses import dataclass
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
    # Daily-bar lookback: how many calendar days of 1d bars to fetch & store.
    # 180d ≈ 36 weekly bars — sufficient for fractal bands (5 bars) + CMO (period=11).
    # Reduced from 365 to cut peak memory by ~50% on Render's 512 MB tier.
    daily_lookback_days: int = 180



def load_config() -> AppConfig:
    from dotenv import load_dotenv
    load_dotenv()
    root = Path(__file__).resolve().parents[1]
    default_db = root / "data" / "stocking.db"

    database_url = os.getenv("DATABASE_URL", "")
    # If running inside Streamlit, try to pull configuration from st.secrets
    secrets_lookback = None
    try:
        import streamlit as st
        if hasattr(st, "secrets"):
            if "DATABASE_URL" in st.secrets:
                database_url = st.secrets["DATABASE_URL"]
            if "STOCKING_DAILY_LOOKBACK_DAYS" in st.secrets:
                secrets_lookback = int(st.secrets["STOCKING_DAILY_LOOKBACK_DAYS"])
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
        fetch_lookback_days=int(os.getenv("STOCKING_FETCH_LOOKBACK_DAYS", "2")),
        compute_lookback_days=int(os.getenv("STOCKING_COMPUTE_LOOKBACK_DAYS", "30")),  # kept for backward compat, not used for band compute
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
        daily_lookback_days=int(os.getenv("STOCKING_DAILY_LOOKBACK_DAYS", str(secrets_lookback or "365"))),
    )
