# PREFERRED CODING PATTERNS

## Robust Resource Management
Because the `ScalableEngine` queries external APIs (Yahoo Finance bounds) and a cloud database (Supabase), every external call should be wrapped in robust retry logic, error swallowing where appropriate, and detailed logging.
Use `logging.getLogger` structured properly so the Dashboard can parse `[INFO]`, `[ERROR]`, `[WARNING]` messages into real-time visual streams.

## Isolation
A strategy is a separate entity entirely defined under `strategies/`. When adding a new strategy (e.g. `LSE`):
1. Create a `strategies/fractal_momentum_lse` directory.
2. Ensure you have independent `.env.lse` and proper run scripts.
3. Don't bleed files into the base directory.

## System Monitoring & Auto-Scheduling
The engine is intended to run autonomously:
- It uses `market_schedule.py` to deduce Open/Close states based on timezone.
- Use `Engine Config` to force state when troubleshooting, but always respect schedule hooks natively.
- Hub (`hub.py`) creates an ecosystem view.

## Logging Pattern
The `TradingRepository` handles recording metric snapshots (P&L and cycle timing).
Logs should be output to file descriptors captured by Streamlit components so that human observers get complete Traceability.
