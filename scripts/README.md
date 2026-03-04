# SCRIPTS DIRECTORY (`scripts/`)

This folder contains legacy and utility bash scripts for managing local daemonizations of the engine and dashboard.

## Scope
- `start_services.sh`: A local launch script that creates a detached `nohup` process for the engine (using `stocking_app.cli run-engine`) and the dashboard (`dashboard.py --server.headless true`). It writes PIDs to `.runtime/` and logs to `logs/`.
- `stop_services.sh`: Reads `.runtime/engine.pid` and `.runtime/dashboard.pid` and issues `kill` commands to shut them down gracefully.

## Rules
- **Phase 1 Legacy**: These scripts were primarily used before the migration to Render.com cloud deployments. They are still useful for local `localhost` testing but are not the source of truth for Production deployment.
- **Do Not Use for Multi-Strategy**: These scripts use the legacy `stocking_app.cli` module which assumes a single `.env` global strategy. Modern execution relies on `python run_strategy.py strategies/<strategy_name> --mode <live|dashboard>`. If you need to write new automation scripts, prefer wrapping `run_strategy.py`.
