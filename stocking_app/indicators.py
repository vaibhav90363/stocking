from __future__ import annotations

import numpy as np
import pandas as pd


def fractal_chaos_bands(data: pd.DataFrame, left_window: int = 2, right_window: int = 2) -> pd.DataFrame:
    """Identify fractal highs/lows and forward-fill them into band lines.

    Loop-based implementation matching the reference backtest script exactly.
    A bar at index i is an upper fractal if its high is strictly greater than
    ALL bars in the left_window before it AND ALL bars in the right_window after it.
    A bar at index i is a lower fractal if its low is strictly less than
    ALL bars in the left_window before it AND ALL bars in the right_window after it.

    Only positions in range [left_window, n - right_window) are candidates,
    ensuring both the left and right comparison windows are fully available.
    """
    df = data.copy()
    n = len(df)

    df["upper_fractal_point"] = np.nan
    df["lower_fractal_point"] = np.nan

    for i in range(left_window, n - right_window):
        # Upper fractal: high[i] > all highs in [i-left_window, i) AND all highs in (i, i+right_window]
        high_i = df["high"].iloc[i]
        left_high_max  = df["high"].iloc[i - left_window : i].max()
        right_high_max = df["high"].iloc[i + 1 : i + 1 + right_window].max()

        if high_i > left_high_max and high_i > right_high_max:
            df.iloc[i, df.columns.get_loc("upper_fractal_point")] = high_i

        # Lower fractal: low[i] < all lows in [i-left_window, i) AND all lows in (i, i+right_window]
        low_i = df["low"].iloc[i]
        left_low_min  = df["low"].iloc[i - left_window : i].min()
        right_low_min = df["low"].iloc[i + 1 : i + 1 + right_window].min()

        if low_i < left_low_min and low_i < right_low_min:
            df.iloc[i, df.columns.get_loc("lower_fractal_point")] = low_i

    # Forward-fill fractal points to create continuous band lines
    df["upper_band_line"] = df["upper_fractal_point"].replace([np.inf, -np.inf], np.nan).ffill()
    df["lower_band_line"] = df["lower_fractal_point"].replace([np.inf, -np.inf], np.nan).ffill()
    return df


def calculate_cmo(data: pd.DataFrame, source_col: str = "close", period: int = 11) -> pd.Series:
    delta = data[source_col].diff(1)
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)

    sum_up = up.rolling(window=period, min_periods=period).sum()
    sum_down = down.rolling(window=period, min_periods=period).sum()
    total = sum_up + sum_down

    cmo = pd.Series(np.nan, index=data.index)
    non_zero = total != 0
    cmo[non_zero] = 100 * (sum_up[non_zero] - sum_down[non_zero]) / total[non_zero]
    return cmo


def sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period, min_periods=period).mean()


def ema(series: pd.Series, period: int) -> pd.Series:
    # Match reference backtest: ewm without min_periods produces values from bar 1,
    # consistent with the reference script's `ewm(span=period, adjust=False).mean()`.
    # EMA naturally weights recent bars heavily so early values are reliable enough.
    return series.ewm(span=period, adjust=False).mean()
