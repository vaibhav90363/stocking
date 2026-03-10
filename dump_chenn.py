import sqlite3
import pandas as pd

def fractal_chaos_bands(data, left_window=2, right_window=2):
    import numpy as np
    data = data.copy()
    data['Upper_Fractal_Point'] = np.nan
    data['Lower_Fractal_Point'] = np.nan
    n = len(data)
    for i in range(left_window, n - right_window):
        if i - left_window >= 0 and i + 1 + right_window <= n:
             is_upper_fractal = data['high'].iloc[i] > data['high'].iloc[i-left_window : i].max() and \
                                data['high'].iloc[i] > data['high'].iloc[i+1 : i+1+right_window].max()
             if is_upper_fractal:
                 data.loc[data.index[i], 'Upper_Fractal_Point'] = data['high'].iloc[i]
             is_lower_fractal = data['low'].iloc[i] < data['low'].iloc[i-left_window : i].min()  and \
                                data['low'].iloc[i] < data['low'].iloc[i+1 : i+1+right_window].min()
             if is_lower_fractal:
                  data.loc[data.index[i], 'Lower_Fractal_Point'] = data['low'].iloc[i]
    data['w_upper'] = data['Upper_Fractal_Point'].replace([np.inf, -np.inf], np.nan).ffill()
    return data

conn = sqlite3.connect("data/backtest_NS.db")
df = pd.read_sql("SELECT ts as date, high, low, close FROM daily_bars WHERE symbol='CHENNPETRO.NS' ORDER BY date ASC", conn)
df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)

weekly = df.resample("W-MON", label="left", closed="left").agg({"high":"max", "low":"min", "close":"last"}).dropna()
weekly = fractal_chaos_bands(weekly)
df = df.join(weekly[['w_upper']], how='left').ffill()

print("CHENNPETRO Data for Feb 2026 (User Buy date: 2026-02-18?):")
print(df.loc["2026-02-10":"2026-02-25"])
