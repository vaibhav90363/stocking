#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p .runtime logs

PY_BIN="python3"
ST_BIN="streamlit"
if [[ -x ".venv/bin/python" ]]; then
  PY_BIN=".venv/bin/python"
fi
if [[ -x ".venv/bin/streamlit" ]]; then
  ST_BIN=".venv/bin/streamlit"
fi

if [[ -f .runtime/engine.pid ]] && kill -0 "$(cat .runtime/engine.pid)" 2>/dev/null; then
  echo "Engine already running (pid $(cat .runtime/engine.pid))"
else
  nohup "$PY_BIN" -m stocking_app.cli run-engine > logs/engine.log 2>&1 &
  echo $! > .runtime/engine.pid
  echo "Started engine pid $(cat .runtime/engine.pid)"
fi

if [[ -f .runtime/dashboard.pid ]] && kill -0 "$(cat .runtime/dashboard.pid)" 2>/dev/null; then
  echo "Dashboard already running (pid $(cat .runtime/dashboard.pid))"
else
  nohup "$ST_BIN" run dashboard.py --server.headless true --server.port 8501 > logs/dashboard.log 2>&1 &
  echo $! > .runtime/dashboard.pid
  echo "Started dashboard pid $(cat .runtime/dashboard.pid)"
fi

echo "Dashboard URL: http://localhost:8501"
