#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:?usage: stop-cluster.sh ROOT}"
SIGNAL="${2:-TERM}"

for id in 1 2 3; do
  pid_file="$ROOT/node-$id.pid"

  if [ -f "$pid_file" ]; then
    pid="$(cat "$pid_file")"

    if kill -0 "$pid" 2>/dev/null; then
      kill "-$SIGNAL" "$pid" 2>/dev/null || true
    fi
  fi
done

for id in 1 2 3; do
  pid_file="$ROOT/node-$id.pid"

  if [ -f "$pid_file" ]; then
    pid="$(cat "$pid_file")"
    wait "$pid" 2>/dev/null || true
  fi
done
