from __future__ import annotations

import numpy as np
import pandas as pd


def fractal_chaos_bands(data: pd.DataFrame, left_window: int = 2, right_window: int = 2) -> pd.DataFrame:
    """Identify fractal highs/lows and forward-fill them into band lines.

    Vectorized implementation — replaces the old O(n) Python loop.
    A bar at index i is an upper fractal if its high is strictly greater than
    the `left_window` bars before it AND the `right_window` bars after it.
    """
    df = data.copy()
    high = df["high"]
    low = df["low"]

    # Rolling max/min over the LEFT window (past bars, inclusive of current)
    # shift(1) excludes the current bar from the left comparison
    left_max_high = high.shift(1).rolling(window=left_window, min_periods=left_window).max()
    left_min_low  = low.shift(1).rolling(window=left_window, min_periods=left_window).min()

    # Rolling max/min over the RIGHT window (future bars)
    # Shift AFTER rolling puts the window exactly ahead of the current bar
    right_max_high = high.rolling(window=right_window, min_periods=right_window).max().shift(-right_window)
    right_min_low  = low.rolling(window=right_window, min_periods=right_window).min().shift(-right_window)

    is_upper_fractal = (high > left_max_high) & (high > right_max_high)
    is_lower_fractal = (low  < left_min_low)  & (low  < right_min_low)

    df["upper_fractal_point"] = np.where(is_upper_fractal, high, np.nan)
    df["lower_fractal_point"] = np.where(is_lower_fractal, low,  np.nan)

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
    # min_periods=period ensures we return NaN until we have enough data,
    # preventing warm-up period from producing unreliable crossover signals.
    return series.ewm(span=period, adjust=False, min_periods=period).mean()
