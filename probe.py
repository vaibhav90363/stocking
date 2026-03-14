
import streamlit as st
import os
import pandas as pd
from stocking_app.config import load_config
from stocking_app.db import TradingRepository
from stocking_app.strategy import compute_all_indicators

st.title("System Probe")

cfg = load_config()
st.write("### Configuration")
st.write(f"Daily Lookback Days: {cfg.daily_lookback_days}")
st.write(f"Exchange TZ: {cfg.exchange_tz}")
st.write(f"Database URL (scheme): {cfg.database_url.split(':')[0] if cfg.database_url else 'None'}")

st.write("### Environment Variables")
st.write(f"STOCKING_DAILY_LOOKBACK_DAYS: {os.getenv('STOCKING_DAILY_LOOKBACK_DAYS')}")

if st.button("Test AAPL"):
    repo = TradingRepository(cfg.database_url)
    daily_bars_dict = repo.get_combined_bars_for_symbols(["AAPL.US"], daily_lookback_days=cfg.daily_lookback_days)
    daily = daily_bars_dict.get("AAPL.US")
    if daily is not None:
        st.write(f"Daily rows: {len(daily)}")
        aligned = compute_all_indicators(daily, cfg.exchange_tz)
        st.write("Latest Aligned Indicators:")
        st.dataframe(aligned.tail(10))
    else:
        st.write("No data found for AAPL.US")
