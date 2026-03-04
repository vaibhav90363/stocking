# CORE ENGINE (`stocking_app/`)

This folder owns the core engine logic and execution lifecycle. It does NOT own the configuration for specific strategies.

## File Dictionary

### Execution Pipeline
- `engine.py`: Defines `ScalableEngine`. Manages the infinite `run_forever()` loop, listens to `SIGINT/SIGTERM` for graceful shutdown, and executes the three-stage `run_once()` cycle: fetch, compute, persist. Uses `ProcessPoolExecutor` for computation.
- `data_fetcher.py`: Connects to `yfinance`. Implements bounded `asyncio.Semaphore` fetching to prevent rate limits, coupled with a strict 15-second `asyncio.wait_for` timeout. Normalizes OHLCV into standard pandas dataframes.
- `strategy.py`: Transforms 5m candles into daily/weekly aggregation. Calculates fractal chaos bands and CMO. Defines the `BUY`/`SELL` logic (`_compute_latest_signal`).
- `indicators.py`: Pure math functions (SMA, EMA, CMO, Fractal Bands) devoid of business logic.

### Infrastructure & Configuration
- `db.py`: The single source of truth for Supabase Postgres operations (`TradingRepository`). Features a critical `@retry_on_disconnect` decorator to wrap all interactions to survive cloud DB drops. Includes ledger logic, log insertion, and system heartbeat tracking.
- `strategy_loader.py`: Exposes `load_strategy()` and `discover_strategies()`. Parses `strategy.yaml` (falling back to a minimal parser if `pyyaml` isn't installed) and returns a hydrated `StrategyConfig` object.
- `market_schedule.py`: Resolves IANA timezones and string HH:MM ranges (e.g. `09:15-15:30`) to determine `next_event` ("opens" or "closes") and current market status natively. Protects the engine from fetching data when the exchange is closed.
- `config.py` / `cli.py`: Legacy support tools and global environment fallbacks. Always prefer `strategy_loader.py` routing in modern phase 1 code.

## Rules
- **Agnosticism**: Do not hardcode specific tickers (e.g. `AAPL.US`) in here. The engine must read from the universe defined elsewhere.
- **Connection Handlers**: Any modifications to `db.py` must use robust retry and backoff, predicting that the Supabase connection WILL drop at some point.
- **Telemetry**: Core engine logging must use standard Python `logging` to stdout or designated file handlers, structured so that Dashboards can parse it correctly. No silent failures. `DatabaseLogHandler` routes critical info back to Supabase.
