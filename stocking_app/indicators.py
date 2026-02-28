from __future__ import annotations

import numpy as np
import pandas as pd


def fractal_chaos_bands(data: pd.DataFrame, left_window: int = 2, right_window: int = 2) -> pd.DataFrame:
    df = data.copy()
    df["upper_fractal_point"] = np.nan
    df["lower_fractal_point"] = np.nan

    n = len(df)
    for i in range(left_window, n - right_window):
        is_upper = (
            df["high"].iloc[i] > df["high"].iloc[i - left_window : i].max()
            and df["high"].iloc[i] > df["high"].iloc[i + 1 : i + 1 + right_window].max()
        )
        if is_upper:
            df.loc[df.index[i], "upper_fractal_point"] = df["high"].iloc[i]

        is_lower = (
            df["low"].iloc[i] < df["low"].iloc[i - left_window : i].min()
            and df["low"].iloc[i] < df["low"].iloc[i + 1 : i + 1 + right_window].min()
        )
        if is_lower:
            df.loc[df.index[i], "lower_fractal_point"] = df["low"].iloc[i]

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
    return series.ewm(span=period, adjust=False).mean()
