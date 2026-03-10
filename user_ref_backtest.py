import pandas as pd
import numpy as np
import yfinance as yf

# Copy pasting only the essential indicators from user's script
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

df = pd.read_csv("test_universe.csv")
symbols = df['Symbol'].tolist()
print("Run reference script on:", symbols)

for symbol in symbols:
    ticker = symbol + ".NS"
    data = yf.download(ticker, start="2023-01-01", end="2025-03-05", progress=False)
    if data.empty:
        continue
    # Flatten MultiIndex columns
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)
        
    data.columns = [c.capitalize() for c in data.columns]
    
    agg_dict = {
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum'
    }
    
    weekly_data = data.resample('W-MON', label='left', closed='left').agg(agg_dict).dropna()
    weekly_data = fractal_chaos_bands(weekly_data)
    
    weekly_data.rename(columns={'Upper_Band_Line': 'Weekly_Upper_Band_Line'}, inplace=True)
    daily_data = data.join(weekly_data[['Weekly_Upper_Band_Line']], how='left')
    daily_data[['Weekly_Upper_Band_Line']] = daily_data[['Weekly_Upper_Band_Line']].fillna(method='ffill')
    
    daily_data = daily_data.loc["2025-03-05":"2025-03-05"]
    
    buys = 0
    # buy logic
    for i in range(1, len(daily_data)):
        curr_close = daily_data['Close'].iloc[i]
        curr_w_ub = daily_data['Weekly_Upper_Band_Line'].iloc[i]
        prev_close = daily_data['Close'].iloc[i-1]
        prev_w_ub = daily_data['Weekly_Upper_Band_Line'].iloc[i-1]
        
        if pd.notna(prev_close) and pd.notna(prev_w_ub) and \
           prev_close <= prev_w_ub and curr_close > curr_w_ub:
            buys += 1
            print(f"[{symbol}] User BUY on {daily_data.index[i].date()} curr_close={curr_close:.2f} w_band={curr_w_ub:.2f}")
    print(f"[{symbol}] Total user buys: {buys}")
