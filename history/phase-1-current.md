# Phase 1: Paper Trading Scalable Engine (Active)

## Goal
Build a scalable paper-trading engine that evaluates a fractal momentum strategy and can run autonomously in the cloud, monitoring multiple global exchanges (NSE, LSE, Nasdaq).

## Completed
- [x] Basic SQLite engine loop (fetch, compute, persist).
- [x] Hub dashboard (`hub.py`) for global monitoring.
- [x] Strategy-specific dashboard (`dashboard.py`) for granular control.
- [x] Migration to Supabase (PostgreSQL) for cloud persistence.
- [x] Multi-strategy folder split (`strategies/`).
- [x] Auto-scheduler implementation (`market_schedule.py`) for autonomous running.
- [x] Fix Nasdaq ticker suffixes (`.US`) loading in `ScalableEngine`.

## In Progress
- [ ] Stabilizing DB connection drops using robust retry logic.
- [ ] Enhancing agent-first repository readability (Active Task).

## Decisions Made This Phase
- Streamlit over custom frontend for speed of iteration.
- Shared `stocking_app/` core and isolated `strategies/` configuration.
- Abandoned local SQLite to allow concurrent dashboards.

## What the Next Agent Should Know
- The `TradingRepository` handles all DB transactions. It was heavily modified during the transition to Supabase. Older references to `sqlite3` driver in docs or tests may be stale.
- `hub.py` parses local `*_output.log` files to show state. If you change the log format, the dashboard regex parsers might break. Maintain log structure backwards compatibility.
- Ensure `.env` is loaded appropriately. The system expects `STOCKING_DB_PATH` (legacy) or Supabase `SUPABASE_URL` depending on the environment.
