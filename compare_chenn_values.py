import pandas as pd
import numpy as np
import sqlite3
from user_script import get_eligible_symbols, run_backtest, SIMULATION_START_DATE, SIMULATION_END_DATE, DATA_FETCH_START_DATE, TICKER_SUFFIX
from stocking_app.indicators import fractal_chaos_bands, calculate_cmo

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()

all_trades, ticker_sim_data = run_backtest(["CHENNPETRO"], SIMULATION_START_DATE, SIMULATION_END_DATE, DATA_FETCH_START_DATE, TICKER_SUFFIX)
user_df = ticker_sim_data.get("CHENNPETRO")

conn = sqlite3.connect("data/backtest_NS.db")
sys_df = pd.read_sql("SELECT ts, open, high, low, close, volume FROM daily_bars WHERE symbol='CHENNPETRO.NS' ORDER BY ts ASC", conn)
sys_df['ts'] = pd.to_datetime(sys_df['ts'])
sys_df.set_index('ts', inplace=True)

weekly = sys_df.resample("W-MON", label="left", closed="left").agg({
    "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"
}).dropna()

weekly = fractal_chaos_bands(weekly, 2, 2)
weekly["cmo"] = calculate_cmo(weekly, "close", 11)
weekly["ema_cmo"] = ema(weekly["cmo"], 5)
weekly["sma_cmo"] = sma(weekly["cmo"], 11)

sys_daily = sys_df.copy()
sys_daily["cmo"] = calculate_cmo(sys_daily, "close", 11)
sys_daily["ema_cmo"] = ema(sys_daily["cmo"], 5)
sys_daily["sma_cmo"] = sma(sys_daily["cmo"], 11)

w_cols = weekly[["upper_band_line", "ema_cmo", "sma_cmo"]].rename(
    columns={"upper_band_line": "w_upper", "ema_cmo": "w_ema_cmo", "sma_cmo": "w_sma_cmo"}
)
sys_aligned = sys_daily.join(w_cols, how="left").ffill()

if user_df is not None:
    print("\n--- COMPARING FEB 3 TO FEB 20 ---")
    
    # user_df index is string or datetime?
    print(f"User index type: {type(user_df.index[0])}, System index type: {type(sys_aligned.index[0])}")
    
    # Let's align on common dates around Feb
    sys_sub = sys_aligned.loc["2026-02-03":"2026-02-20"]
    user_sub = user_df.loc["2026-02-03":"2026-02-20"] if type(user_df.index[0]) == str else user_df[(user_df.index >= pd.Timestamp("2026-02-03")) & (user_df.index <= pd.Timestamp("2026-02-20"))]
    
    for idx in sys_sub.index:
        idx_str = idx.strftime('%Y-%m-%d')
        s_row = sys_sub.loc[idx]
        try:
            u_row = user_df.loc[pd.Timestamp(idx_str)] if type(user_df.index[0]) != str else user_df.loc[idx_str]
        except KeyError:
            continue
            
        print(f"\nDate: {idx_str}")
        print(f"Close:   User={u_row['Close']:.2f} Sys={s_row['close']:.2f} Diff={abs(u_row['Close']-s_row['close']):.2f}")
        print(f"Upper:   User={u_row['Weekly_Upper_Band_Line']:.2f} Sys={s_row['w_upper']:.2f} Diff={abs(u_row['Weekly_Upper_Band_Line']-s_row['w_upper']):.2f}")
        u_d_ema = u_row['EMA_5_CMO_11']
        u_d_sma = u_row['SMA_11_CMO_11']
        u_w_ema = u_row['Weekly_EMA_5_CMO_11']
        u_w_sma = u_row['Weekly_SMA_11_CMO_11']
        print(f"D_EMA:   User={u_d_ema:.4f} Sys={s_row['ema_cmo']:.4f} Diff={abs(u_d_ema-s_row['ema_cmo']):.4f}")
        print(f"D_SMA:   User={u_d_sma:.4f} Sys={s_row['sma_cmo']:.4f} Diff={abs(u_d_sma-s_row['sma_cmo']):.4f}")
        print(f"W_EMA:   User={u_w_ema:.4f} Sys={s_row['w_ema_cmo']:.4f} Diff={abs(u_w_ema-s_row['w_ema_cmo']):.4f}")
        print(f"W_SMA:   User={u_w_sma:.4f} Sys={s_row['w_sma_cmo']:.4f} Diff={abs(u_w_sma-s_row['w_sma_cmo']):.4f}")
