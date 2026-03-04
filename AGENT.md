# AGENT INSTRUCTIONS

## Your Role
You are an AI Agent assigned to maintain, debug, and expand the **Stocking Phase-1 Scalable Engine** repository.
This is a paper-trading execution stack for a fractal momentum strategy, built for scalability across multiple exchanges (NSE, LSE, Nasdaq).

## Before Every Task
1. Read `CONTEXT.md` to understand the overarching system architecture and technical philosophy.
2. Check `.tasks/INDEX.md` and `.tasks/active/` to understand your current specific scope and what you are allowed to modify.
3. Check `history/INDEX.md` and the current phase document under `history/` to understand recent changes and the active trajectory.
4. Review `.agent/known-issues.md` to avoid rabbit-holes related to active bugs.
5. Check `.agent/rules.md` and `.agent/patterns.md` to ensure your code aligns with our strict conventions.
6. Read the `README.md` in any specific directory (e.g. `stocking_app/` or `strategies/`) that you are about to touch.

## Priority Constraints
*   **Locality of Context:** Never guess the purpose of a directory. Read its local `README.md`.
*   **Database Constraints:** We migrated from local SQLite to Supabase (PostgreSQL). Handle connection logic robustly due to potential disconnects.
*   **Multi-Strategy Scaling:** Strategies are strictly decoupled inside `strategies/`. Changes to the core engine in `stocking_app/` must not break individual strategy configurations.
*   **Do not break Telemetry:** The system relies on granular logs and real-time dashboard updates (Hub Dashboard and individual strategy Dashboards).

## Next Steps
Determine your goal by checking your assigned task or verifying `.tasks/active/`.
Always leave the repository in a better state by updating `history/`, `.tasks/`, and `.agent/known-issues.md` before concluding your session.
