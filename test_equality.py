import pandas as pd
import numpy as np

def their_fractal(data, left_window=2, right_window=2):
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

             is_lower_fractal = data['low'].iloc[i] < data['low'].iloc[i-left_window : i].min() and \
                                data['low'].iloc[i] < data['low'].iloc[i+1 : i+1+right_window].min()

             if is_lower_fractal:
                  data.loc[data.index[i], 'Lower_Fractal_Point'] = data['low'].iloc[i]

    data['Upper_Band_Line'] = data['Upper_Fractal_Point'].replace([np.inf, -np.inf], np.nan).ffill()
    data['Lower_Band_Line'] = data['Lower_Fractal_Point'].replace([np.inf, -np.inf], np.nan).ffill()
    return data

def our_fractal(data, left_window=2, right_window=2):
    df = data.copy()
    high = df["high"]
    low  = df["low"]

    left_max_high = high.shift(1).rolling(window=left_window, min_periods=left_window).max()
    left_min_low  = low.shift(1).rolling(window=left_window, min_periods=left_window).min()

    right_max_high = high.rolling(window=right_window, min_periods=right_window).max().shift(-right_window)
    right_min_low  = low.rolling(window=right_window, min_periods=right_window).min().shift(-right_window)

    is_upper_fractal = (high > left_max_high) & (high > right_max_high)
    is_lower_fractal = (low  < left_min_low)  & (low  < right_min_low)

    df["upper"] = np.where(is_upper_fractal, high, np.nan)
    df["lower"] = np.where(is_lower_fractal, low, np.nan)

    df["upper_band"] = pd.Series(df["upper"]).replace([np.inf, -np.inf], np.nan).ffill()
    df["lower_band"] = pd.Series(df["lower"]).replace([np.inf, -np.inf], np.nan).ffill()
    return df

df = pd.DataFrame({
    'high': np.random.randint(10, 20, size=50),
    'low': np.random.randint(1, 10, size=50)
})

t = their_fractal(df)
o = our_fractal(df)

eq_u = t['Upper_Band_Line'].equals(o['upper_band'])
eq_l = t['Lower_Band_Line'].equals(o['lower_band'])
print(f"Equality Check -> Upper: {eq_u}, Lower: {eq_l}")
if not eq_u:
    print(pd.concat([t['Upper_Band_Line'], o['upper_band']], axis=1))
    print(t['Upper_Fractal_Point'])
    print(o['upper'])
