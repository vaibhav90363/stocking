#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

stop_pid() {
  local file="$1"
  local name="$2"
  if [[ -f "$file" ]]; then
    local pid
    pid="$(cat "$file")"
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" || true
      echo "Stopped $name ($pid)"
    else
      echo "$name not running"
    fi
    rm -f "$file"
  else
    echo "$name pid file not found"
  fi
}

stop_pid .runtime/engine.pid "engine"
stop_pid .runtime/dashboard.pid "dashboard"
