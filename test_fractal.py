import pandas as pd
import numpy as np

def original_fractal(data, left_window=2, right_window=2):
    df = data.copy()
    high = df["high"]
    left_max_high = high.shift(1).rolling(window=left_window, min_periods=left_window).max()
    right_max_high = high.shift(-1).rolling(window=right_window, min_periods=right_window).max()
    is_upper_fractal = (high > left_max_high) & (high > right_max_high)
    df["upper"] = np.where(is_upper_fractal, high, np.nan)
    return df["upper"]

def fixed_fractal(data, left_window=2, right_window=2):
    df = data.copy()
    high = df["high"]
    left_max_high = high.shift(1).rolling(window=left_window, min_periods=left_window).max()
    # Fixed shift
    right_max_high = high.shift(-left_window).rolling(window=right_window, min_periods=right_window).max() # wait!
    # Let me re-think the fixed shift
    right_max_high2 = high.shift(-1).rolling(window=right_window).max().shift(-(right_window-1))
    
    # testing another way:
    right_max_high3 = high.rolling(window=right_window).max().shift(-right_window)

    is_upper_fractal = (high > left_max_high) & (high > right_max_high3)
    df["upper"] = np.where(is_upper_fractal, high, np.nan)
    return df["upper"], right_max_high3

df = pd.DataFrame({'high': [10, 11, 15, 12, 11, 16, 10, 8]})
print("Original:\n", original_fractal(df).values)
print("Fixed:\n", fixed_fractal(df)[0].values)
print("Right_max:", fixed_fractal(df)[1].values)
