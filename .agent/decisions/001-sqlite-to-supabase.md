# 001. Migration: Local SQLite to Supabase (PostgreSQL)

## Date
2026-02-28

## Context
Initial iterations of the **Phase-1 Scalable Engine** were entirely local. The engine synced tick data directly with Yahoo Finance and stored state locally into `sqlite3` instances.
As we decided to scale the system for continuous, headless cloud operation (e.g. Render deployments) combined with an externally hosted observability cluster (Streamlit), a persistent local filesystem database became a bottleneck, causing lock conflicts and preventing multi-instance reads.

## Decision
We chose Supabase (Postgres) as the remote transactional layer.
- `stocking_app/db.py` was refactored significantly to implement `TradingRepository` pointing to Supabase.
- Table structures (`stock_data`, `system_config`, `run_metrics`) map 1:1, but benefit from unified scaling.
- Connection pooling and auto-retries are deemed critical because external networks drop requests.

## Consequences
**Positive:**
- Decoupled engine compute from dashboard rendering.
- Robust, cloud-native storage avoiding SQLite lock loops.
- Permitted multiple strategies to run isolated connections to the single database schema.

**Negative (Trade-offs):**
- Network latency per transaction is significantly higher.
- Unexpected disconnections from DB drop the engine natively without strict retry handling.
- Increased deployment complexity for local dev (requires `.env` mapping appropriately).
