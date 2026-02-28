# Stocking Phase-1 Scalable Engine

This is a runnable Phase-1 (paper-trading) execution stack for your fractal momentum strategy.

It includes:
- 5-minute scanner using Yahoo Finance.
- Incremental candle storage in SQLite.
- Parallel compute workers for strategy calculations.
- Ledger for open positions.
- Activity log for buys/sells.
- Signals table (including acted/not-acted).
- P&L snapshots and run metrics.
- Streamlit dashboard with Start/Stop engine control.

## Architecture

- `stocking_app/data_fetcher.py`
  - Async fetch fan-out with bounded concurrency for 5m bars.
- `stocking_app/strategy.py`
  - Strategy signal worker (fractal weekly band buy + daily/weekly CMO crossdown sell).
- `stocking_app/engine.py`
  - 5-minute loop with:
    - fetch stage (I/O concurrent)
    - compute stage (process pool parallel)
    - persist stage (ledger, signals, logs, pnl, heartbeat)
- `stocking_app/db.py`
  - SQLite schema + repository methods.
- `dashboard.py`
  - Live dashboard and engine start/stop control.

## Quickstart

1. Create venv and install dependencies:

```bash
cd /Users/vaibhavchaudhary/Desktop/stocking
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Initialize DB:

```bash
python -m stocking_app.cli init-db
```

3. Import your universe from CSV (`symbol` column):

```bash
python -m stocking_app.cli import-universe --csv data/universe_sample.csv --symbol-column symbol --append-suffix .NS
```

4. Start background services:

```bash
./scripts/start_services.sh
```

5. Open dashboard:

- [http://localhost:8501](http://localhost:8501)

6. In dashboard click **Start Engine**.

7. Stop background services when needed:

```bash
./scripts/stop_services.sh
```

## CLI Commands

```bash
python -m stocking_app.cli init-db
python -m stocking_app.cli import-universe --csv <path> --symbol-column symbol --append-suffix .NS
python -m stocking_app.cli set-engine --enabled true
python -m stocking_app.cli set-engine --enabled false
python -m stocking_app.cli status
python -m stocking_app.cli run-once
python -m stocking_app.cli run-engine
```

## Scalability controls

Environment variables:

- `STOCKING_CYCLE_SECONDS` (default `300`)
- `STOCKING_FETCH_CONCURRENCY` (default `12`)
- `STOCKING_COMPUTE_WORKERS` (default `4`)
- `STOCKING_FETCH_LOOKBACK_DAYS` (default `10`)
- `STOCKING_COMPUTE_LOOKBACK_DAYS` (default `365`)
- `STOCKING_ORDER_QTY` (default `1`)
- `STOCKING_DB_PATH` (default `data/stocking.db`)

Example:

```bash
export STOCKING_FETCH_CONCURRENCY=20
export STOCKING_COMPUTE_WORKERS=8
python -m stocking_app.cli run-engine
```

## Notes

- This is paper trading only (no broker execution in Phase-1).
- Yahoo Finance can rate limit and may deliver delayed/incomplete intraday data.
- For always-on laptop startup, next step is adding a `launchd` service on macOS.
