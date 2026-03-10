import pandas as pd
import sqlite3
import numpy as np
from stocking_app.indicators import fractal_chaos_bands, calculate_cmo

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()

conn = sqlite3.connect("data/backtest_NS.db")
df = pd.read_sql("SELECT ts, open, high, low, close, volume FROM daily_bars WHERE symbol='CHENNPETRO.NS' ORDER BY ts ASC", conn)
df['ts'] = pd.to_datetime(df['ts'])
df.set_index('ts', inplace=True)

# System logic
weekly = df.resample("W-MON", label="left", closed="left").agg({
    "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"
}).dropna()

weekly = fractal_chaos_bands(weekly, 2, 2)
weekly["cmo"] = calculate_cmo(weekly, "close", 11)
weekly["ema_cmo"] = ema(weekly["cmo"], 5)
weekly["sma_cmo"] = sma(weekly["cmo"], 11)

daily = df.copy()
daily["cmo"] = calculate_cmo(daily, "close", 11)
daily["ema_cmo"] = ema(daily["cmo"], 5)
daily["sma_cmo"] = sma(daily["cmo"], 11)

w_cols = weekly[["upper_band_line", "ema_cmo", "sma_cmo"]].rename(
    columns={"upper_band_line": "w_upper", "ema_cmo": "w_ema_cmo", "sma_cmo": "w_sma_cmo"}
)
aligned = daily.join(w_cols, how="left").ffill()

for i in range(1, len(aligned)):
    prev = aligned.iloc[i - 1]
    curr = aligned.iloc[i]
    day  = aligned.index[i]

    d_sell = curr["ema_cmo"] < curr["sma_cmo"] and prev["ema_cmo"] >= prev["sma_cmo"]
    w_sell = curr["w_ema_cmo"] < curr["w_sma_cmo"] and prev["w_ema_cmo"] >= prev["w_sma_cmo"]
    
    is_buy = prev["close"] <= prev["w_upper"] and curr["close"] > curr["w_upper"]
    
    if is_buy or d_sell or w_sell:
        print(f"{day.date()}: BUY={is_buy}, D_SELL={d_sell}, W_SELL={w_sell} | close={curr['close']:.2f} w_upper={curr['w_upper']:.2f} d_ema={curr['ema_cmo']:.2f} d_sma={curr['sma_cmo']:.2f}")
