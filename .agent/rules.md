# PROJECT CONSTRAINTS

## Database
- We use Supabase (PostgreSQL). Schema changes require migrations — NEVER alter tables directly without documenting it here or in `.agent/decisions`.
- All database queries must go through `stocking_app/db.py` (TradingRepository and HubRepository). Do not query DB inline elsewhere.
- Connection resilience is required; the engine should gracefully retry any dropped database connection using tenacity or similar retry patterns.

## Engine & Multiprocessing
- No synchronous, blocking operations in `market_schedule.py` or the `cycle_manager`.
- Do not bypass `run_strategy.py` when testing specific logic.
- Avoid memory leaks in long-running processes (e.g., ensure `Hub` dashboard processes don’t orphan subprocesses).

## Code Structure
- DO NOT use the same universe `.US` for NSE; verify the suffixes in `import-universe` scripts.
- Hub Dashboard (`hub.py`) strictly handles orchestration, while `dashboard.py` strictly manages single strategy display.
- Telemetry outputs (`*_output.log` and `*_dash.log`) must be stored locally per-strategy rather than globally to prevent data overwrites.

## Deployment Environment
- Background engine and scripts typically run on Render or a local cron-like abstraction (Launchd).
- Streamlit dashboards typically run on Streamlit Community Cloud. Take into account read-only filesystems when writing app output where deployed dashboards can't natively see them, or proxy them via Supabase logs.
