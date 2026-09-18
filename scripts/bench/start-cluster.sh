#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:?usage: start-cluster.sh ROOT}"
BIN="${RAGNORDB_BIN:-./target/release/ragnordb}"

for id in 1 2 3; do
  # Detach the benchmark node into its own session. The PID file and
  # stop-cluster.sh become the lifecycle authority, so a caller can invoke
  # this helper from another wrapper without its process-group cleanup
  # terminating the nodes immediately after this script returns.
  setsid "$BIN" node \
    --config "$ROOT/node-$id.toml" \
    > "$ROOT/node-$id.log" 2>&1 < /dev/null &

  echo $! > "$ROOT/node-$id.pid"
done
