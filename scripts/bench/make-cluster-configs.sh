#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:?usage: make-cluster-configs.sh ROOT [SNAPSHOT_INTERVAL_ENTRIES] [SNAPSHOT_MIN_ELAPSED_MS]}"

SNAPSHOT_INTERVAL_ENTRIES="${2:-1000000}"
SNAPSHOT_MIN_ELAPSED_MS="${3:-300000}"

mkdir -p "$ROOT"

for id in 1 2 3; do
  sql_port=$((7100 + id))
  admin_port=$((7200 + id))

  mkdir -p "$ROOT/node-$id"

  cat > "$ROOT/node-$id.toml" <<CFG
node_id = $id
data_dir = "$ROOT/node-$id"
listen_addr = "127.0.0.1:$sql_port"
admin_addr = "127.0.0.1:$admin_port"

max_connections = 128
statement_timeout_ms = 30000
shutdown_grace_period_ms = 5000
statement_logging = "off"

cluster_id = "m4-benchmark"
bootstrap = true

snapshot_interval_entries = $SNAPSHOT_INTERVAL_ENTRIES
snapshot_interval_bytes = 1073741824
snapshot_min_elapsed_ms = $SNAPSHOT_MIN_ELAPSED_MS
max_snapshot_file_bytes = 536870912
snapshot_chunk_bytes = 1048576

[[seed_nodes]]
id = 1
raft_addr = "127.0.0.1:7001"
snapshot_addr = "127.0.0.1:7051"
sql_addr = "127.0.0.1:7101"
admin_addr = "127.0.0.1:7201"

[[seed_nodes]]
id = 2
raft_addr = "127.0.0.1:7002"
snapshot_addr = "127.0.0.1:7052"
sql_addr = "127.0.0.1:7102"
admin_addr = "127.0.0.1:7202"

[[seed_nodes]]
id = 3
raft_addr = "127.0.0.1:7003"
snapshot_addr = "127.0.0.1:7053"
sql_addr = "127.0.0.1:7103"
admin_addr = "127.0.0.1:7203"
CFG
done
