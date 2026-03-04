# Task 001: Implement Agent-First Repository Structure

## Status: Active
## Depends On: None
## Blocks: Future Agent work

## Context
The repository needs a structure that allows AI Agents to easily read context, understand rules, and pick up workflows without human hand-holding.

## Files to Touch
- `/AGENT.md` (create)
- `/CONTEXT.md` (create)
- `/.agent/*` (create rules, patterns, known-issues, decisions)
- `/.tasks/*` (create task tracking graph)
- `/history/*` (create project phases)
- `/stocking_app/README.md` (create local context)
- `/strategies/README.md` (create local context)

## Definition of Done
- All files listed above exist and contain accurate contextual information based on recent project history (Supabase migration, multi-strategy split, Hub vs Dashboard).
- The repo requires no human instruction to onboard a new agent.

## Do Not Touch
- Any `.py` source files.
- Existing `.log` or database files.
