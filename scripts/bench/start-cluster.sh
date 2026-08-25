#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:?usage: start-cluster.sh ROOT}"
BIN="${RAGNORDB_BIN:-./target/release/ragnordb}"

for id in 1 2 3; do
  "$BIN" node \
    --config "$ROOT/node-$id.toml" \
    > "$ROOT/node-$id.log" 2>&1 &

  echo $! > "$ROOT/node-$id.pid"
done
