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

# Nodes are launched in detached sessions, so the shell cannot use `wait` to
# observe their exit. Do not let the next benchmark phase race the old node's
# leadership handoff and connection-drain shutdown while its ports are still
# bound. The timeout is deliberately finite so a hung node remains an explicit
# gate failure instead of blocking the harness forever.
deadline=$((SECONDS + 30))
while :; do
  remaining=0
  for id in 1 2 3; do
    pid_file="$ROOT/node-$id.pid"

    if [ -f "$pid_file" ]; then
      pid="$(cat "$pid_file")"
      if kill -0 "$pid" 2>/dev/null; then
        remaining=1
      fi
    fi
  done

  if [ "$remaining" -eq 0 ]; then
    exit 0
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    printf 'timed out waiting for detached cluster nodes to exit: %s\n' "$ROOT" >&2
    exit 1
  fi
  sleep 0.1
done
