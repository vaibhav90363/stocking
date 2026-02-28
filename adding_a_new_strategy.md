# Adding a New Strategy — End-to-End Guide

This document explains every step required to add a new strategy to the Stocking system — from creating the folder through writing signal logic, backtesting, running live, and monitoring it on the hub.

---

## Architecture Overview

```
strategies/
  my_new_strategy_nse/
    strategy.yaml        ← config: exchange, engine, parameters, backtest settings
    universe.csv         ← your custom stock list (subset of all NSE stocks)
    data/                ← auto-created at first run
      live.db            ← NOT used (cloud uses Supabase/Postgres via DATABASE_URL)
      logs/
        engine.log       ← rotating log, 5 MB × 3 files
      backtest/
        backtest.db      ← local SQLite used only during backtest
        report.txt       ← human-readable P&L summary
        trades.csv       ← every trade record

stocking_app/
  strategy.py            ← signal computation logic (BUY / SELL functions)
  indicators.py          ← reusable technical indicators
  strategy_loader.py     ← reads strategy.yaml into a StrategyConfig object
  engine.py              ← the perpetual cycle engine (fetch → compute → persist)
  db.py                  ← all database reads/writes (Postgres via psycopg2)
  config.py              ← AppConfig, reads env vars + .env
  market_schedule.py     ← NSE/LSE market hours + auto-scheduler logic

hub.py                   ← Streamlit multi-strategy hub (auto-discovers all folders)
dashboard.py             ← Streamlit single-strategy dashboard
run_strategy.py          ← CLI launcher (backtest / live / dashboard modes)
backtest_sim.py          ← standalone historical simulator
```

---

## Step 1 — Create the Strategy Folder

```bash
mkdir -p /Users/vaibhavchaudhary/Desktop/stocking/strategies/my_new_strategy_nse
```

The folder name becomes the strategy's identifier inside the hub URL (`?strategy=my_new_strategy_nse`).

---

## Step 2 — Create `strategy.yaml`

Create `strategies/my_new_strategy_nse/strategy.yaml`:

```yaml
name: "My Strategy — NSE"
description: >
  Short description of your buy and sell logic.

strategy_type: my_strategy   # used as a tag; must match dispatcher in strategy.py

exchange:
  suffix: ".NS"              # appended to every symbol when fetching (e.g. RELIANCE.NS)
  timezone: "Asia/Kolkata"
  market_open: "09:15"
  market_close: "15:30"

engine:
  cycle_seconds: 300         # how often the engine runs a full cycle (seconds)
  fetch_lookback_days: 10    # days of 5m bars to fetch per cycle
  compute_lookback_days: 365 # total lookback window for indicator calculations
  fetch_concurrency: 16      # parallel download threads
  compute_workers: 4         # parallel signal-computation processes
  order_qty: 1               # quantity bought/sold per signal

parameters:
  # Forwarded verbatim to your signal function as a dict
  rsi_period: 14
  rsi_buy_threshold: 30
  rsi_sell_threshold: 70
  capital_per_trade: 100000  # used by backtest for P&L calculation

backtest:
  daily_lookback: "2y"       # how much historical daily data to download
  intraday_days: 60          # days of 5m bars to download for backtest
  backtest_days: 30          # rolling window used to evaluate signals
```

> [!IMPORTANT]
> `capital_per_trade` under `parameters` is the only value used by the backtest engine for P&L. All other `parameters` keys are custom — add as many as your signal logic needs.

---

## Step 3 — Build Your `universe.csv`

The file `strategies/fractal_momentum_nse/universe.csv` already contains all 501 Nifty 500 stocks. Your `universe.csv` is a filtered subset of it.

**Required columns** (the `Symbol` column is the only one the engine uses):

```
Company Name,Industry,Symbol,Series,ISIN Code
Reliance Industries Ltd.,Oil Gas & Consumable Fuels,RELIANCE,EQ,INE002A01018
Infosys Ltd.,Information Technology,INFY,EQ,INE009A01021
HDFC Bank Ltd.,Financial Services,HDFCBANK,EQ,INE040A01034
```

**Filtering strategies:**
- Copy the full Nifty 500 CSV and delete rows manually
- Filter by `Industry` column (e.g. keep only IT and Pharma)
- Keep only Nifty 50 or Nifty 100 constituents

The engine reads only the `Symbol` column. It appends `suffix` (`.NS`) at load time, so your CSV must contain **base symbols without suffix** (e.g. `RELIANCE`, not `RELIANCE.NS`).

---

## Step 4 — Write Your Signal Logic

Open `stocking_app/strategy.py`. This is where BUY and SELL conditions are computed.

### Understanding the Signal Function Contract

The engine calls `compute_symbol_signal(db_path, symbol, lookback_days, exchange_tz)` for every symbol each cycle. The function:
1. Loads 5-minute OHLCV bars from the Postgres DB
2. Resamples them into daily and weekly DataFrames
3. Runs your indicator calculations
4. Returns exactly one of: `"BUY"`, `"SELL"`, or `None`

### Return Format (must not be changed)

```python
return {
    "symbol": symbol,
    "asof_ts": last_ts.isoformat(),   # timestamp of the last candle used
    "last_price": last_price,          # latest close price
    "signal": "BUY" | "SELL" | None,
    "signal_price": float | None,      # price at which to record the trade
    "signal_reason": "your_reason_string",
}
```

### Adding a New Strategy Type

**Option A — Simplest: Replace the existing signal logic**

Edit `_compute_latest_signal(daily, weekly)` in `strategy.py` directly. The function receives two pandas DataFrames with columns `open, high, low, close, volume` and a `DatetimeIndex`.

```python
def _compute_latest_signal(
    daily: pd.DataFrame,
    weekly: pd.DataFrame
) -> tuple[str | None, float | None, str]:
    
    # ── Your indicators ────────────────────────────────────────────
    from .indicators import ema, sma  # or add your own
    
    daily["rsi"] = _compute_rsi(daily["close"], period=14)
    
    if len(daily) < 2:
        return None, None, "insufficient_data"
    
    prev = daily.iloc[-2]
    curr = daily.iloc[-1]
    
    # ── BUY condition ──────────────────────────────────────────────
    if prev["rsi"] < 30 and curr["rsi"] >= 30:   # RSI crossing up from oversold
        return "BUY", float(curr["close"]), "rsi_oversold_crossup"
    
    # ── SELL condition ─────────────────────────────────────────────
    if prev["rsi"] > 70 and curr["rsi"] <= 70:   # RSI crossing down from overbought
        return "SELL", float(curr["close"]), "rsi_overbought_crossdown"
    
    return None, None, "no_signal"
```

**Option B — Multi-strategy dispatch (recommended for multiple active strategies)**

Add a new file `stocking_app/my_strategy.py` with your `compute_signal` function, then add a dispatcher in `strategy.py`:

```python
# In strategy.py — add at the top
STRATEGY_DISPATCH = {
    "fractal_momentum": _compute_latest_signal_fractal,
    "my_strategy":      _compute_latest_signal_my_strategy,
}

def compute_symbol_signal(db_path, symbol, lookback_days, exchange_tz, strategy_type="fractal_momentum"):
    ...
    fn = STRATEGY_DISPATCH.get(strategy_type, _compute_latest_signal_fractal)
    signal, signal_price, reason = fn(daily, weekly)
```

> [!NOTE]
> Currently `strategy_type` is not passed through from `strategy_loader.py` to the engine signal call. If you need truly independent signal logic per strategy without modifying the same file, Option B requires also threading `strategy_type` through `engine.py`'s `run_once()` method and the `compute_symbol_signal` call.

---

## Step 5 — Adding New Indicators

Add your indicator functions to `stocking_app/indicators.py`. The file already provides:

| Function | Description |
|---|---|
| `fractal_chaos_bands(df, left, right)` | Fractal upper/lower band lines |
| `calculate_cmo(df, col, period)` | Chande Momentum Oscillator |
| `sma(series, period)` | Simple Moving Average |
| `ema(series, period)` | Exponential Moving Average |

**Adding RSI as an example:**

```python
# In stocking_app/indicators.py

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff(1)
    gain  = delta.clip(lower=0)
    loss  = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))
```

Then import and use in `strategy.py`:
```python
from .indicators import rsi
daily["rsi"] = rsi(daily["close"], period=14)
```

---

## Step 6 — Run a Backtest

```bash
cd /Users/vaibhavchaudhary/Desktop/stocking

# Full backtest (downloads historical data from yfinance)
python run_strategy.py strategies/my_new_strategy_nse --mode backtest

# Re-run using already-downloaded data (faster iteration)
python run_strategy.py strategies/my_new_strategy_nse --mode backtest --skip-download
```

**What happens internally:**
1. `backtest_sim.py` downloads daily + 5m OHLCV from Yahoo Finance for every symbol in your `universe.csv`
2. It simulates the strategy signal day-by-day over the `backtest_days` window
3. Trades are recorded: BUY → SELL pairs with calculated P&L
4. Results written to `data/backtest/report.txt` and `trades.csv`

**Output files:**

| File | Content |
|---|---|
| `data/backtest/report.txt` | Win rate, total P&L, trade count, per-symbol breakdown |
| `data/backtest/trades.csv` | Every trade: symbol, side, price, timestamp, P&L |
| `data/backtest/backtest.db` | Local SQLite storing downloaded candles (reused with `--skip-download`) |

**In-browser backtest** — You can also click **🔬 Run Backtest** on the Hub card. This runs the same simulation inside the Streamlit UI with a live progress bar.

---

## Step 7 — Start the Live Engine

```bash
# Start the live engine
python run_strategy.py strategies/my_new_strategy_nse --mode live

# Run backtest first, then automatically start live
python run_strategy.py strategies/my_new_strategy_nse --mode all
```

**What the engine does on startup:**
1. Reads `strategy.yaml` into a `StrategyConfig`
2. Sets environment variables that `AppConfig`/`load_config()` picks up
3. Runs `init-db` — creates all Postgres tables (idempotent)
4. Runs `import-universe` — upserts every symbol from `universe.csv` into the `universe` table
5. Sets `engine_enabled = true` in the DB
6. Starts the `ScalableEngine.run_forever()` loop

---

## Step 8 — Engine Execution Cycle (What Happens Every 5 Minutes)

Each cycle runs three phases:

### Phase 1: FETCH
- Reads all active symbols from the `universe` table + any open position symbols
- Downloads 5-minute OHLCV bars from Yahoo Finance (parallel, up to `fetch_concurrency` threads)
- Upserts every bar into `candles_5m` table (keyed on `symbol + ts`)
- Updates `symbol_state.last_candle_ts` for each symbol
- **Log format:** `[1/3] FETCH — 120 symbols  (lookback=10d)`

### Phase 2: COMPUTE
- Selects only symbols where `last_candle_ts > last_compute_ts` (new data since last computation)
- Submits each symbol to a `ProcessPoolExecutor` (parallel, `compute_workers` processes)
- Each worker runs `compute_symbol_signal()` → your signal logic
- **Log format:** `🟢 BUY  RELIANCE          @ 1234.5000  [rsi_oversold_crossup]`
- **Log format:** `🔴 SELL INFY              @ 1890.2500  [rsi_overbought_crossdown]`

### Phase 3: PERSIST
- For each BUY signal: checks `positions_ledger` — if no open position exists, calls `execute_buy()` → inserts into `positions_ledger` and `trade_activity_log`
- For each SELL signal: checks `positions_ledger` — if an open position exists, calls `execute_sell()` → deletes from `positions_ledger`, records P&L in `trade_activity_log`
- All signals (acted or not) are upserted to the `signals` table
- Updates `last_price` for all open positions (for unrealized P&L)
- Takes a P&L snapshot → `pnl_snapshots` table
- **Log format:** `✅  BOUGHT RELIANCE × 1 @ 1234.5000`
- **Log format:** `— No trades executed this cycle  (open positions: 3)`

### Market-Hours Auto-Scheduler
Before each cycle, the engine checks if the market is open:
- If market opens → sets `engine_enabled = true` automatically
- If market closes → sets `engine_enabled = false` automatically
- While disabled → engine heartbeat shows `paused_market_closed` state, checks every 5 seconds

This is controlled by `STOCKING_AUTO_SCHEDULE` env var (default: `1` = on).

---

## Step 9 — Database Tables Reference

| Table | Purpose |
|---|---|
| `universe` | All symbols to monitor (`is_active=1`) |
| `candles_5m` | Raw 5-minute OHLCV bars, keyed on `(symbol, ts)` |
| `symbol_state` | Tracks `last_candle_ts` and `last_compute_ts` per symbol |
| `signals` | Every BUY/SELL signal ever generated, with `acted` flag |
| `positions_ledger` | Currently open positions with `avg_price`, `qty`, `last_price` |
| `trade_activity_log` | Full history of all executed BUY and SELL trades with P&L |
| `pnl_snapshots` | Time-series of realized + unrealized P&L |
| `engine_state` | Key-value store: `engine_enabled`, `engine_heartbeat` JSON |
| `run_metrics` | Every cycle's performance stats (fetch/compute/persist seconds, counts) |

---

## Step 10 — Logging Details

Log file location: `strategies/my_new_strategy_nse/data/logs/engine.log`

**Log rotation:** 5 MB per file, 3 backup files kept (`engine.log`, `engine.log.1`, `engine.log.2`)

**Log levels:**
- `DEBUG` — written to file only: engine-disabled heartbeat, cycle skips
- `INFO` — written to file + console: cycle start/end, fetch/compute/persist stats, every BUY/SELL signal found, every trade executed
- `ERROR` — written to file + console: cycle failures with full stack trace

**Sample log output for a healthy cycle:**

```
2026-02-28 09:20:01  INFO     ┌─ Cycle #1 started  [2026-02-28T03:50:01+00:00]
2026-02-28 09:20:01  INFO       [1/3] FETCH  — 50 symbols  (lookback=10d)
2026-02-28 09:20:14  INFO       ✓ 48/50 fetched  (23,040 bars)  | 13.2s  | 2 failed: SYMBOL1, SYMBOL2
2026-02-28 09:20:14  INFO       [2/3] COMPUTE — 48 symbols pending
2026-02-28 09:20:18  INFO       ✓ 48 computed  | 4.1s  | BUY signals: 2  SELL signals: 1
2026-02-28 09:20:18  INFO       🟢 BUY   RELIANCE         @ 1234.5000  [rsi_oversold_crossup]
2026-02-28 09:20:18  INFO       🟢 BUY   HDFCBANK         @ 1678.2000  [rsi_oversold_crossup]
2026-02-28 09:20:18  INFO       🔴 SELL  INFY             @ 1890.2500  [rsi_overbought_crossdown]
2026-02-28 09:20:18  INFO       [3/3] PERSIST
2026-02-28 09:20:19  INFO       ✅  BOUGHT RELIANCE × 1 @ 1234.5000
2026-02-28 09:20:19  INFO       ✅  BOUGHT HDFCBANK × 1 @ 1678.2000
2026-02-28 09:20:19  INFO       ✅  SOLD   INFY @ 1890.2500
2026-02-28 09:20:19  INFO       ✓ Persisted in 0.82s  | Open positions: 4
2026-02-28 09:20:19  INFO     └─ Cycle #1 OK  | total=50  fetched=48  computed=48  | fetch=13.2s  compute=4.1s  persist=0.8s  | total=18.2s
2026-02-28 09:20:19  INFO        Next cycle in 281s  (at 09:20:19)
```

---

## Step 11 — Hub Monitoring (Auto-Discovery)

Your new strategy **appears automatically** in the Hub (`hub.py`) — no code changes needed. The hub calls `discover_strategies(ROOT)` which scans every subfolder of `strategies/` for a `strategy.yaml`.

**Per-strategy card shows:**
- Engine state: 🟢 RUNNING / ⏰ PAUSED (market closed) / ⚫ OFFLINE
- Market status badge: 🟢 OPEN / ⚫ CLOSED with countdown to next event
- Realized P&L, open positions, last cycle status
- Symbols fetched / total per last cycle
- Backtest report summary (if run)

**Action buttons on each card:**
- **▶ Start Engine** — sets `engine_enabled=true` in DB; disabled during market hours when auto-schedule is on
- **⏹ Stop Engine** — sets `engine_enabled=false`; disabled while a cycle is actively running (safety guard)
- **📊 View Dashboard** — opens the single-strategy Streamlit dashboard
- **🔬 Run Backtest** — runs the backtest inside the hub UI with live progress
- **📋 Last log lines** — shows the last 10 lines of `engine.log`

---

## Step 12 — Open a Dedicated Dashboard

```bash
# Open dashboard for your strategy on port 8502
python run_strategy.py strategies/my_new_strategy_nse --mode dashboard --port 8502
```

The dashboard (`dashboard.py`) shows:
- Real-time engine state, market status, cycle stats
- Open positions table with unrealized P&L per position
- Signal history (all BUY/SELL signals with acted/not-acted)
- Trade activity log with P&L per trade
- Cumulative P&L chart over time
- Raw log viewer (live tail of `engine.log`)

---

## Full Checklist

```
[ ] 1. mkdir strategies/my_new_strategy_nse
[ ] 2. Create strategy.yaml (copy template above, adjust parameters)
[ ] 3. Create universe.csv (filter Nifty 500 universe to your stock selection)
[ ] 4. Add indicator functions to stocking_app/indicators.py (if needed)
[ ] 5. Write BUY + SELL signal logic in stocking_app/strategy.py
[ ] 6. Run backtest: python run_strategy.py strategies/my_new_strategy_nse --mode backtest
[ ] 7. Review report.txt and trades.csv — adjust parameters and re-run as needed
[ ] 8. Start live engine: python run_strategy.py strategies/my_new_strategy_nse --mode live
[ ] 9. Open hub to monitor: streamlit run hub.py
[ ] 10. Optionally open dedicated dashboard --mode dashboard --port 8502
```

---

## Signal Logic Pitfalls to Avoid

> [!CAUTION]
> **Look-ahead bias** — Never use `iloc[-1]` on its own to decide a signal. Always compare `iloc[-2]` (previous bar) with `iloc[-1]` (current bar) for crossover conditions. If you use only the current value, the signal fires on every bar the condition is true, not just at the crossing point.

> [!WARNING]
> **Indicator warmup** — Most indicators (RSI, EMA, SMA) produce `NaN` for their first `period` bars. Always call `.dropna()` on critical columns before accessing `iloc[-2]` and `iloc[-1]`, or check `len(df) < threshold` before computing.

> [!WARNING]
> **Double entries** — The engine already guards against this: it will only execute a BUY if the symbol has no open position, and only a SELL if an open position exists. You do not need to track this in your signal code.

> [!NOTE]
> **Signal price vs last price** — `signal_price` is recorded as the trade entry/exit price. Return the indicator value that triggered the signal (e.g. the band level for a breakout), or `curr["close"]` if no specific price level applies. The engine falls back to `last_price` if `signal_price` is `None`.
