# KNOWN ISSUES AND TECH DEBT

## Active Bugs
- There was a recent issue where the engine showed as offline while the market was open (fixed by updating `ScalableEngine` to enforce ticker suffix loading correctly). Ensure suffix handling is airtight going forward.
- Occasional "server closed the connection unexpectedly" errors from Supabase during high concurrency database reads/writes. Retry wrappers must heavily buffer against this.
- Chrome Profile context drift. Avoid heavy browser-profile dependencies unless absolutely required for Render tasks or Auth flows.

## Tech Debt
- Database Migration from SQLite left behind some orphaned `sqlite3` driver nuances in the codebase; be cautious that logic translates purely to PostgreSQL bounds via `psycopg2` or Supabase REST queries.
- Scaling telemetry to Streamlit Community Cloud requires unified log streaming, currently relying a bit on shared filesystems (logs mapped manually), which could break if fully containerized without bound volumes. Needs a proper logging DB stream in the future.
- Hub and Dashboard duplication: `hub.py` and `dashboard.py` share some visual components but do fundamentally different things. A common UI components folder might naturally evolve.
