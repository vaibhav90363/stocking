# AI AGENT PROMPT TEMPLATE

*Copy and paste the text below into your prompt when talking to an AI agent to instantly ground them in the repository's rules, context, and tech stack.*

---

You are the AI Agent for the Stocking Phase-1 Scalable Engine project. 
Before writing any code or proposing solutions, you MUST read the following to understand the architecture, strict bounds, and tech stack:

1. `AGENT.md` and `CONTEXT.md` (For system overview, DB connection logic, and multi-strategy split).
2. `.agent/rules.md` and `.agent/known-issues.md` (For strict rules on database handling, UI tracking, and known bugs).
3. The focused `README.md` for the domain you are about to touch:
   - Core engine logic -> `stocking_app/README.md`
   - Strategy config -> `strategies/README.md`
   - UI elements -> See `hub.py` and `dashboard.py` source.

**Libraries & Pages Grounding:**
- **UI/Pages:** We use **Streamlit** exclusively. `hub.py` is the global monitor. `dashboard.py` is the isolated per-strategy view.
- **Database:** We use **Supabase (PostgreSQL)** via `psycopg2`. All DB interactions MUST route through `stocking_app/db.py` (`TradingRepository`) using the disconnect retry wrappers.
- **Data & Concurrency:** `yfinance` with async bounded timeouts (`data_fetcher.py`) and `ProcessPoolExecutor` (`engine.py`).

**My Specific Request:**
[INSERT YOUR REQUEST HERE]

**Execution Rules:**
- Do not make assumptions about ticker suffixes (e.g., `.US`, `.NS`); respect the active strategy's config.
- Any new complex code must gracefully handle cloud network drops.
- Once completed, please update the machine-readable graph in `.tasks/` and log the accomplishment in `history/phase-1-current.md`.
