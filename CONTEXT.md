# PROJECT CONTEXT

## Vision
The **Stocking Phase-1** project is a robust, paper-trading execution engine designed to test fractal momentum strategies. The system evolved from a simple SQLite-backed engine into a scalable, cloud-connected Supabase environment that manages multiple distinct exchanges concurrently (e.g., NSE, LSE, Nasdaq).

Our top priority is stable, autonomous, scheduled execution with explicit traceability via Streamlit dashboards.

## Architecture & Bootstrapping

The repository uses a single unified engine (`stocking_app/`) that is parameterized by independent strategies (`strategies/*`). 

### Request / Boot Flow
1. **Strategy Loader**: `stocking_app/strategy_loader.py` reads `strategy.yaml` from a specific strategy directory to determine cycles, lookbacks, market hours, and database URIs.
2. **Universal Runner**: `run_strategy.py` is the main entry point. It accepts `--mode live|backtest|dashboard`. When run in `live` mode, it launches the `ScalableEngine` and also a lightweight HTTP server natively for Render platform health checks.
3. **The Cycle**: The `ScalableEngine` (`engine.py`) operates in an infinite loop (while market is open natively via `market_schedule.py`). It orchestrates:
   - **Fetching** (`data_fetcher.py`): Async bounded fetch of 5-minute OHLCV using `yfinance`.
   - **Computing** (`strategy.py`): Submitting fetched data to a `ProcessPoolExecutor` to calculate daily/weekly fractal chaos bands and CMO crossovers.
   - **Persisting**: Pushing signals, executions, and P&L snapshots through the `TradingRepository` (`db.py`).

### The Database (Supabase PostgreSQL)
All strategies share the same Supabase database but isolate their universes using suffixes (e.g., `.NS` vs `.US`).
- Connections are via `psycopg2`.
- `TradingRepository` implements a strict `@retry_on_disconnect` decorator because long-running cloud connections frequently drop.

### The Observability Layer
1. **The Hub** (`hub.py`): A global Streamlit app that aggregates data across all strategies. It discovers strategies using `discover_strategies()`, reads isolated heartbeats in the DB, and checks Render/Streamlit deployment status via HTTP pings.
2. **Strategy Dashboard** (`dashboard.py`): A scoped Streamlit app passing explicit `--strategy-dir` flags. Allows manual Start/Stop overrides and granular live log viewing.

## Technical Philosophy
1.  **Resilience over Speed:** The engine must handle Supabase connection drops, Yahoo Finance rate limits (with strict 15.0s async timeouts), and expected networking errors without crashing.
2.  **Autonomous Management:** The engine auto-starts and auto-stops based on `market_schedule.py`. Manual intervention should only be needed for emergencies.
3.  **Strict File Segregation:** A strategy specific to `.US` tickers must store its logs and state in its own folder. Data must never leak between strategies.
4.  **Database as Source of Truth:** Open positions, signals, and performance metrics live in Supabase. The local filesystem holds temporary logs (`*_output.log`) and configs.
