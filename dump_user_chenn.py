import pandas as pd
import yfinance as yf
import numpy as np
from user_script import get_eligible_symbols, run_backtest, SIMULATION_START_DATE, SIMULATION_END_DATE, DATA_FETCH_START_DATE, TICKER_SUFFIX

all_trades, ticker_sim_data = run_backtest(["CHENNPETRO"], SIMULATION_START_DATE, SIMULATION_END_DATE, DATA_FETCH_START_DATE, TICKER_SUFFIX)

if "CHENNPETRO" in ticker_sim_data:
    df = ticker_sim_data["CHENNPETRO"]
    print(df.loc["2026-02-15":"2026-02-20", ["Close", "Weekly_Upper_Band_Line"]])

for t in all_trades:
    if t["stock_name"] == "CHENNPETRO":
        print(t)
