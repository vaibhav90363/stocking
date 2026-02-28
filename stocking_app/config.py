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



def load_config() -> AppConfig:
    from dotenv import load_dotenv
    load_dotenv()
    root = Path(__file__).resolve().parents[1]
    default_db = root / "data" / "stocking.db"

    database_url = os.getenv("DATABASE_URL", "")
    try:
        import streamlit as st
        # Directly attempt to read from st.secrets
        # If not running in Streamlit, this will raise a RuntimeError or FileNotFoundError
        if "DATABASE_URL" in st.secrets:
            database_url = st.secrets["DATABASE_URL"]
    except Exception:
        pass

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
        ticker_suffix=os.getenv("STOCKING_TICKER_SUFFIX", ".NS"),
    )
