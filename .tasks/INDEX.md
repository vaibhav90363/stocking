# TASKS GRAPH

This directory (`.tasks/`) contains the machine-readable task graph. Agents use this to understand what to do next and what it depends on.

**Structure:**
- `active/`: Tasks currently being worked on.
- `blocked/`: Tasks waiting on dependencies.
- `done/`: Completed tasks.

Always check `active/` when starting a new session to pick up where the last agent left off.
Move tasks to `done/` when completed.
