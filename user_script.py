# -*- coding: utf-8 -*-
"""
Stock Backtesting Script: Fractal Breakout (Buy at Band Price) and CMO MA Crossover Exit Strategy (v6.2)
"""

import pandas as pd
import numpy as np
import yfinance as yf
import datetime
import os
import time
import random
import warnings

try:
    import scipy.optimize
    print("scipy found, XIRR calculation using scipy available.")
    SCIPY_AVAILABLE = True
except ImportError:
    print("scipy not found. Please install it ('pip install scipy') to calculate XIRR.")
    SCIPY_AVAILABLE = False

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

SIMULATION_START_DATE = "2026-01-04"
SIMULATION_END_DATE = "2026-03-05"
DATA_FETCH_START_DATE = "2023-01-01"

CSV_FILE_PATH = "full_universe.csv" 
OUTPUT_FOLDER = './StockStrategyResults_FractalCMO_Filtered_CMOMACrossover_v6_2' 
TICKER_SUFFIX = ".NS" 

MARKET_CAP_THRESHOLD_INR = 50000 * 1_00_00_000 # 5000 Crore INR
FRACTAL_LEFT_WINDOW = 2
FRACTAL_RIGHT_WINDOW = 2
CMO_PERIOD = 11
CMO_EMA_PERIOD = 5
CMO_SMA_PERIOD = 11

try:
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
except Exception as e:
    OUTPUT_FOLDER = '.'

def safe_download(ticker, start_date, end_date, max_retries=3, initial_wait=5):
    retry = 0
    wait_time = initial_wait
    while retry < max_retries:
        try:
            stock = yf.Ticker(ticker)
            data = stock.history(start=start_date, end=end_date, auto_adjust=True)
            if not data.empty:
                data.columns = [col.capitalize() for col in data.columns]
                if data.index.tz is not None:
                    data.index = data.index.tz_localize(None)
                return data
        except Exception as e:
            pass
        retry += 1
        time.sleep(1)
    return pd.DataFrame()

def safe_get_ticker_info(ticker, max_retries=3, initial_wait=2):
    return {"marketCap": 999999999999} # Mock to avoid rate limit for 4 symbols

def fractal_chaos_bands(data, left_window=2, right_window=2):
    data = data.copy()
    data['Upper_Fractal_Point'] = np.nan
    data['Lower_Fractal_Point'] = np.nan
    n = len(data)
    for i in range(left_window, n - right_window):
        if i - left_window >= 0 and i + 1 + right_window <= n:
             is_upper_fractal = data['High'].iloc[i] > data['High'].iloc[i-left_window : i].max() and \
                                data['High'].iloc[i] > data['High'].iloc[i+1 : i+1+right_window].max()
             if is_upper_fractal:
                 data.loc[data.index[i], 'Upper_Fractal_Point'] = data['High'].iloc[i]
             is_lower_fractal = data['Low'].iloc[i] < data['Low'].iloc[i-left_window : i].min() and \
                                data['Low'].iloc[i] < data['Low'].iloc[i+1 : i+1+right_window].min()
             if is_lower_fractal:
                  data.loc[data.index[i], 'Lower_Fractal_Point'] = data['Low'].iloc[i]
    data['Upper_Band_Line'] = data['Upper_Fractal_Point'].replace([np.inf, -np.inf], np.nan).fillna(method='ffill')
    data['Lower_Band_Line'] = data['Lower_Fractal_Point'].replace([np.inf, -np.inf], np.nan).fillna(method='ffill')
    return data

def calculate_cmo(data, period=11):
    data = data.copy()
    cmo_col_name = f'CMO_{period}'
    if 'Close' not in data.columns:
        data[cmo_col_name] = np.nan
        return data
    delta = data['Close'].diff(1)
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    sum_up = up.rolling(window=period, min_periods=period).sum()
    sum_down = down.rolling(window=period, min_periods=period).sum()
    sum_total = sum_up + sum_down
    cmo = pd.Series(np.nan, index=data.index)
    non_zero_sum = sum_total != 0
    cmo[non_zero_sum] = 100 * (sum_up[non_zero_sum] - sum_down[non_zero_sum]) / sum_total[non_zero_sum]
    data[cmo_col_name] = cmo
    return data

def calculate_sma(data, source_col, period=11, result_col_suffix='SMA'):
    result_col_name = f'{result_col_suffix}_{period}_{source_col}'
    data[result_col_name] = data[source_col].rolling(window=period, min_periods=period).mean()
    return data

def calculate_ema(data, source_col, period=5, result_col_suffix='EMA'):
    result_col_name = f'{result_col_suffix}_{period}_{source_col}'
    data[result_col_name] = data[source_col].ewm(span=period, adjust=False).mean()
    return data

def get_eligible_symbols(symbols, market_cap_threshold, ticker_suffix):
    return symbols

def run_backtest(symbols, start_date, end_date, data_fetch_start, ticker_suffix):
    all_trades = []
    ticker_sim_data = {} 
    simulation_start_dt = pd.to_datetime(start_date)
    simulation_end_dt = pd.to_datetime(end_date)
    for symbol in symbols:
        ticker = f"{symbol}{ticker_suffix}"
        daily_data = safe_download(ticker, data_fetch_start, end_date)
        if daily_data.empty or 'Close' not in daily_data.columns or daily_data.index.duplicated().any(): continue
        daily_data.sort_index(inplace=True)
        agg_dict = {col: 'last' for col in daily_data.columns if col not in ['High', 'Low', 'Open', 'Volume']}
        agg_dict.update({'Open': 'first','High': 'max','Low': 'min','Close': 'last','Volume': 'sum'})
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in daily_data.columns for col in required_cols): continue
        weekly_data = daily_data.resample('W-MON', label='left', closed='left').agg(agg_dict).dropna(subset=required_cols)
        if weekly_data.empty: continue
        weekly_data = fractal_chaos_bands(weekly_data, FRACTAL_LEFT_WINDOW, FRACTAL_RIGHT_WINDOW)
        daily_data = calculate_cmo(daily_data, CMO_PERIOD) 
        weekly_data = calculate_cmo(weekly_data, CMO_PERIOD) 
        cmo_col_name = f'CMO_{CMO_PERIOD}'
        daily_data = calculate_ema(daily_data, cmo_col_name, CMO_EMA_PERIOD) 
        daily_data = calculate_sma(daily_data, cmo_col_name, CMO_SMA_PERIOD) 
        weekly_data = calculate_ema(weekly_data, cmo_col_name, CMO_EMA_PERIOD) 
        weekly_data = calculate_sma(weekly_data, cmo_col_name, CMO_SMA_PERIOD) 
        if daily_data.index.tz is not None: daily_data.index = pd.to_datetime(daily_data.index).tz_localize(None)
        if weekly_data.index.tz is not None: weekly_data.index = pd.to_datetime(weekly_data.index).tz_localize(None)
        weekly_data.rename(columns={
            'Upper_Band_Line': 'Weekly_Upper_Band_Line',
            cmo_col_name: f'Weekly_{cmo_col_name}',
            f'EMA_{CMO_EMA_PERIOD}_{cmo_col_name}': f'Weekly_EMA_{CMO_EMA_PERIOD}_{cmo_col_name}',
            f'SMA_{CMO_SMA_PERIOD}_{cmo_col_name}': f'Weekly_SMA_{CMO_SMA_PERIOD}_{cmo_col_name}',
            }, inplace=True)
        weekly_cols_to_join = [
            'Weekly_Upper_Band_Line',
            f'Weekly_{cmo_col_name}',
            f'Weekly_EMA_{CMO_EMA_PERIOD}_{cmo_col_name}',
            f'Weekly_SMA_{CMO_SMA_PERIOD}_{cmo_col_name}'
        ]
        daily_data = daily_data.join(weekly_data[weekly_cols_to_join], how='left')
        daily_data[weekly_cols_to_join] = daily_data[weekly_cols_to_join].fillna(method='ffill')
        sim_data = daily_data[(daily_data.index >= simulation_start_dt) & (daily_data.index <= simulation_end_dt)].copy()
        critical_sim_cols = ['Close', 'Weekly_Upper_Band_Line',
                             f'EMA_{CMO_EMA_PERIOD}_CMO_{CMO_PERIOD}', f'SMA_{CMO_SMA_PERIOD}_CMO_{CMO_PERIOD}',
                             f'Weekly_EMA_{CMO_EMA_PERIOD}_CMO_{CMO_PERIOD}', f'Weekly_SMA_{CMO_SMA_PERIOD}_CMO_{CMO_PERIOD}']
        sim_data.dropna(subset=critical_sim_cols, inplace=True)
        if sim_data.empty: continue
        ticker_sim_data[symbol] = sim_data.copy() 
        in_position = False
        buy_price = 0.0
        buy_date = None
        daily_ema_cmo_col = f'EMA_{CMO_EMA_PERIOD}_CMO_{CMO_PERIOD}'
        daily_sma_cmo_col = f'SMA_{CMO_SMA_PERIOD}_CMO_{CMO_PERIOD}'
        weekly_ema_cmo_col = f'Weekly_EMA_{CMO_EMA_PERIOD}_CMO_{CMO_PERIOD}'
        weekly_sma_cmo_col = f'Weekly_SMA_{CMO_SMA_PERIOD}_CMO_{CMO_PERIOD}'
        for i in range(len(sim_data)):
            current_date = sim_data.index[i]
            current_row = sim_data.iloc[i]
            current_close = current_row['Close']
            weekly_upper_band = current_row['Weekly_Upper_Band_Line']
            current_daily_ema_cmo = current_row[daily_ema_cmo_col]
            current_daily_sma_cmo = current_row[daily_sma_cmo_col]
            current_weekly_ema_cmo = current_row[weekly_ema_cmo_col]
            current_weekly_sma_cmo = current_row[weekly_sma_cmo_col]
            if not in_position and pd.notna(current_close) and pd.notna(weekly_upper_band):
                 crossed_above = False
                 if i > 0:
                      prev_row = sim_data.iloc[i-1]
                      prev_close = prev_row['Close']
                      prev_weekly_upper_band = prev_row['Weekly_Upper_Band_Line']
                      if pd.notna(prev_close) and pd.notna(prev_weekly_upper_band) and \
                         prev_close <= prev_weekly_upper_band and current_close > weekly_upper_band:
                           crossed_above = True
                 elif i == 0 and current_close > weekly_upper_band:
                       crossed_above = True 
                 if crossed_above:
                      if pd.notna(current_daily_ema_cmo) and pd.notna(current_daily_sma_cmo) and \
                         pd.notna(current_weekly_ema_cmo) and pd.notna(current_weekly_sma_cmo) and \
                         pd.notna(weekly_upper_band): 
                           in_position = True
                           buy_price = weekly_upper_band 
                           buy_date = current_date
            elif in_position and i > 0:
                 prev_row = sim_data.iloc[i-1]
                 prev_daily_ema_cmo = prev_row[daily_ema_cmo_col]
                 prev_daily_sma_cmo = prev_row[daily_sma_cmo_col]
                 prev_weekly_ema_cmo = prev_row[weekly_ema_cmo_col]
                 prev_weekly_sma_cmo = prev_row[weekly_sma_cmo_col]
                 sell_triggered = False
                 if pd.notna(current_daily_ema_cmo) and pd.notna(current_daily_sma_cmo) and \
                    pd.notna(prev_daily_ema_cmo) and pd.notna(prev_daily_sma_cmo):
                      if current_daily_ema_cmo < current_daily_sma_cmo and prev_daily_ema_cmo >= prev_daily_sma_cmo:
                           sell_triggered = True
                 if not sell_triggered and pd.notna(current_weekly_ema_cmo) and pd.notna(current_weekly_sma_cmo) and \
                    pd.notna(prev_weekly_ema_cmo) and pd.notna(prev_weekly_sma_cmo):
                     if current_weekly_ema_cmo < current_weekly_sma_cmo and prev_weekly_ema_cmo >= prev_weekly_sma_cmo:
                         sell_triggered = True
                 if sell_triggered:
                    sell_price = current_close
                    sell_date = current_date
                    if buy_date is None: continue 
                    profit_loss = sell_price - buy_price
                    holding_period_days = (sell_date - buy_date).days
                    all_trades.append({
                        "stock_name": symbol,
                        "buy_date": buy_date.strftime('%Y-%m-%d'),
                        "buy_price": buy_price,
                        "sell_date": sell_date.strftime('%Y-%m-%d'),
                        "sell_price": sell_price,
                        "profit_loss": profit_loss,
                        "holding_period_days": holding_period_days
                    })
                    print(f"[{symbol}] User SELL on {sell_date.date()} sell_price={sell_price:.2f} P/L={profit_loss:.2f}")
                    in_position = False
                    buy_price = 0.0
                    buy_date = None
        if in_position:
             last_row = sim_data.iloc[-1]
             last_close_price = last_row['Close']
             last_date = sim_data.index[-1]
             if pd.notna(last_close_price) and buy_date is not None:
                  profit_loss = last_close_price - buy_price
                  holding_period_days = (last_date - buy_date).days
                  all_trades.append({
                      "stock_name": symbol, 
                      "buy_date": buy_date.strftime('%Y-%m-%d'),
                      "buy_price": buy_price,
                      "sell_date": last_date.strftime('%Y-%m-%d'), 
                      "sell_price": last_close_price, 
                      "profit_loss": profit_loss,
                      "holding_period_days": holding_period_days
                  })
    return all_trades, ticker_sim_data

if __name__ == "__main__":
    stock_list_df = pd.read_csv(CSV_FILE_PATH)
    all_symbols_original = stock_list_df['Symbol'].tolist()
    eligible_symbols_original = get_eligible_symbols(all_symbols_original, MARKET_CAP_THRESHOLD_INR, TICKER_SUFFIX)
    all_trades, ticker_sim_data = run_backtest(eligible_symbols_original, SIMULATION_START_DATE, SIMULATION_END_DATE, DATA_FETCH_START_DATE, TICKER_SUFFIX)
    for t in all_trades:
        print(f"Trade: {t['stock_name']} BUY {t['buy_date']} @ {t['buy_price']:.2f} | SELL {t['sell_date']} @ {t['sell_price']:.2f}")
    
    trades_df = pd.DataFrame(all_trades)
    if not trades_df.empty:
        trades_df.to_csv("user_trades_60.csv", index=False)
        print("Exported user trades to user_trades_60.csv")

