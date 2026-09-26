#!/usr/bin/env bash
set -euo pipefail

# Produce one immutable Milestone-6 performance evidence directory. The script
# never overwrites an existing run and never edits the frozen Phase-5A runner.
OUTPUT_DIR="${1:?usage: run-m6-baseline.sh OUTPUT_DIR}"
if [ -e "$OUTPUT_DIR" ]; then
  printf "refusing to overwrite existing output directory: %s\n" "$OUTPUT_DIR" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"
mkdir -p "$OUTPUT_DIR"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
BENCH_BIN="${RAGNORDB_BENCH_BIN:-$REPO_ROOT/target/release/ragnordb-bench}"
SERVER_BIN="${RAGNORDB_BIN:-$REPO_ROOT/target/release/ragnordb}"
ROWS="${M6_ROWS:-1000}"
VALUE_BYTES="${M6_VALUE_BYTES:-256}"
LOAD_BATCH_SIZE="${M6_LOAD_BATCH_SIZE:-100}"
SECONDS_PER_CASE="${M6_SECONDS:-15}"
WARMUP="${M6_WARMUP:-200}"
TIMEOUT_MS="${M6_TIMEOUT_MS:-10000}"
SEED="${M6_SEED:-42}"
CLIENT_COUNTS="${M6_CLIENT_COUNTS:-1 2 8 16 32 64}"
CRITERION_SAMPLE_SIZE="${M6_CRITERION_SAMPLE_SIZE:-10}"
CRITERION_WARMUP_SECONDS="${M6_CRITERION_WARMUP_SECONDS:-1}"
CRITERION_MEASUREMENT_SECONDS="${M6_CRITERION_MEASUREMENT_SECONDS:-2}"
CLUSTER_DIR="$OUTPUT_DIR/cluster"

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    printf "required command is missing: %s\n" "$1" >&2
    exit 2
  }
}

for command_name in cargo curl date git jq lscpu ps rustc uname; do
  require_command "$command_name"
done

if [ ! -x "$BENCH_BIN" ] || [ ! -x "$SERVER_BIN" ]; then
  printf "release binaries are missing; build ragnordb-cli and ragnordb-bench first\n" >&2
  exit 2
fi

git rev-parse HEAD > "$OUTPUT_DIR/git-commit.txt"
git branch --show-current > "$OUTPUT_DIR/git-branch.txt"
git status --short > "$OUTPUT_DIR/git-status.txt"
git diff --stat > "$OUTPUT_DIR/git-diff-stat.txt"
git diff --name-status > "$OUTPUT_DIR/git-diff-name-status.txt"
git diff --binary > "$OUTPUT_DIR/git-diff.patch"
git ls-files --others --exclude-standard > "$OUTPUT_DIR/git-untracked.txt"
uname -a > "$OUTPUT_DIR/uname.txt"
lscpu > "$OUTPUT_DIR/lscpu.txt"
rustc --version --verbose > "$OUTPUT_DIR/rustc.txt"
cargo --version > "$OUTPUT_DIR/cargo.txt"
awk "/^(MemTotal|MemAvailable|SwapTotal|SwapFree):/ { print }" /proc/meminfo > "$OUTPUT_DIR/memory.txt"
{
  printf "run_id=%s\n" "$RUN_ID"
  printf "repo_root=%s\n" "$REPO_ROOT"
  printf "bench_binary=%s\n" "$BENCH_BIN"
  printf "server_binary=%s\n" "$SERVER_BIN"
  printf "rows=%s\n" "$ROWS"
  printf "value_bytes=%s\n" "$VALUE_BYTES"
  printf "load_batch_size=%s\n" "$LOAD_BATCH_SIZE"
  printf "seconds_per_case=%s\n" "$SECONDS_PER_CASE"
  printf "warmup_per_client=%s\n" "$WARMUP"
  printf "timeout_ms=%s\n" "$TIMEOUT_MS"
  printf "seed=%s\n" "$SEED"
  printf "client_counts=%s\n" "$CLIENT_COUNTS"
  printf "criterion_sample_size=%s\n" "$CRITERION_SAMPLE_SIZE"
  printf "criterion_warmup_seconds=%s\n" "$CRITERION_WARMUP_SECONDS"
  printf "criterion_measurement_seconds=%s\n" "$CRITERION_MEASUREMENT_SECONDS"
} > "$OUTPUT_DIR/parameters.txt"

echo "running Criterion milestone6"
cargo bench -p ragnordb-bench --bench milestone6 -- \
  --noplot \
  --sample-size "$CRITERION_SAMPLE_SIZE" \
  --warm-up-time "$CRITERION_WARMUP_SECONDS" \
  --measurement-time "$CRITERION_MEASUREMENT_SECONDS" \
  --save-baseline "m6-$RUN_ID" \
  > "$OUTPUT_DIR/criterion-milestone6.txt" 2>&1
if [ -d "$REPO_ROOT/target/criterion" ]; then
  cp -a "$REPO_ROOT/target/criterion" "$OUTPUT_DIR/criterion-data"
fi

echo "running frozen Phase 5A regression gate"
PHASE5A_CLIENT_COUNTS="${PHASE5A_CLIENT_COUNTS:-1 2 8 16 32 64 128 256}" \
PHASE5A_SECONDS="${PHASE5A_SECONDS:-$SECONDS_PER_CASE}" \
PHASE5A_WARMUP="${PHASE5A_WARMUP:-$WARMUP}" \
scripts/bench/run-phase5a-baseline.sh "$OUTPUT_DIR/phase5a"

scripts/bench/make-cluster-configs.sh "$CLUSTER_DIR" 1000000 300000
RAGNORDB_BIN="$SERVER_BIN" scripts/bench/start-cluster.sh "$CLUSTER_DIR"
cleanup_cluster() {
  scripts/bench/stop-cluster.sh "$CLUSTER_DIR" TERM >/dev/null 2>&1 || true
}
trap cleanup_cluster EXIT

wait_for_group_leader() {
  local group_id="$1"
  local attempt node_id status_json
  for attempt in $(seq 1 240); do
    for node_id in 1 2 3; do
      status_json="$(curl -fsS --max-time 2 "http://127.0.0.1:$((7200 + node_id))/status/groups" 2>/dev/null || true)"
      if printf "%s" "$status_json" | jq -e --argjson group_id "$group_id" \
          "any(.multiraft.groups[]?; .raft_group_id == \$group_id and .role == \"leader\")" \
          >/dev/null 2>&1; then
        printf "%s\n" "$node_id"
        return 0
      fi
    done
    sleep 0.25
  done
  printf "timed out waiting for Raft group %s leader\n" "$group_id" >&2
  return 1
}

save_statuses() {
  local label="$1"
  local node_id
  for node_id in 1 2 3; do
    curl -fsS "http://127.0.0.1:$((7200 + node_id))/status" > "$OUTPUT_DIR/status-$label-node-$node_id.json"
    curl -fsS "http://127.0.0.1:$((7200 + node_id))/status/groups" > "$OUTPUT_DIR/status-groups-$label-node-$node_id.json"
    curl -fsS "http://127.0.0.1:$((7200 + node_id))/metrics" > "$OUTPUT_DIR/metrics-$label-node-$node_id.txt"
  done
}

leader_node="$(wait_for_group_leader 2)"
leader_addr="127.0.0.1:$((7100 + leader_node))"

# Create one-tablet benchmark tables. The table list is the participant-count
# control: each table is a distinct metadata-owned tablet in the default setup.
for table_number in 0 1 2 3 4 5 6 7; do
  "$BENCH_BIN" load \
    --addr "$leader_addr" \
    --table "bench_$table_number" \
    --protocol v2 \
    --client-id "$((9000 + table_number))" \
    --session-epoch 1 \
    --rows "$ROWS" \
    --batch-size "$LOAD_BATCH_SIZE" \
    --value-bytes "$VALUE_BYTES" \
    --create-table \
    > "$OUTPUT_DIR/load-bench-$table_number.json" \
    2> "$OUTPUT_DIR/load-bench-$table_number.stderr"
done
save_statuses after-load

run_case() {
  local label="$1"
  local tables="$2"
  local workload="$3"
  local clients="$4"
  local txn_writes="$5"
  local contention="$6"
  local client_id="$7"
  local leader
  leader="$(wait_for_group_leader 2)"
  local addr="127.0.0.1:$((7100 + leader))"
  "$BENCH_BIN" run \
    --addr "$addr" \
    --table "${tables%%,*}" \
    --participant-tables "$tables" \
    --protocol v2 \
    --client-id "$client_id" \
    --session-epoch 1 \
    --workload "$workload" \
    --clients "$clients" \
    --seconds "$SECONDS_PER_CASE" \
    --warmup "$WARMUP" \
    --rows "$ROWS" \
    --value-bytes "$VALUE_BYTES" \
    --txn-writes "$txn_writes" \
    --contention "$contention" \
    --timeout-ms "$TIMEOUT_MS" \
    --seed "$SEED" \
    --histogram-out "$OUTPUT_DIR/$label.histogram.tsv" \
    > "$OUTPUT_DIR/$label.json" \
    2> "$OUTPUT_DIR/$label.stderr"
  jq -e ".valid_run == true" "$OUTPUT_DIR/$label.json" >/dev/null
  save_statuses "$label"
}

case_id=10000
for clients in $CLIENT_COUNTS; do
  run_case "single-shard-c${clients}-w1" "bench_0" single-shard-txn "$clients" 1 disjoint "$case_id"
  case_id=$((case_id + 1))
done

for write_count in 4 16 64; do
  run_case "single-shard-c8-w${write_count}" "bench_0" single-shard-txn 8 "$write_count" disjoint "$case_id"
  case_id=$((case_id + 1))
done

for participant_count in 2 4 8; do
  table_list="bench_0"
  for table_number in $(seq 1 $((participant_count - 1))); do
    table_list="$table_list,bench_$table_number"
  done
  run_case "cross-shard-p${participant_count}" "$table_list" cross-shard-txn 8 "$participant_count" disjoint "$case_id"
  case_id=$((case_id + 1))
done

for distribution in disjoint moderate hotspot; do
  run_case "contention-${distribution}" bench_0 txn-contention 32 1 "$distribution" "$case_id"
  case_id=$((case_id + 1))
done

# Crash/recovery evidence: stop the exact PID-file cluster while a transaction
# workload is active, restart the same durable cluster, and retain both the
# client result and post-restart status/metrics. This is deliberately a separate
# case because a killed client is not a valid latency sample.
crash_dir="$OUTPUT_DIR/crash-recovery"
mkdir -p "$crash_dir"
leader_node="$(wait_for_group_leader 2)"
leader_addr="127.0.0.1:$((7100 + leader_node))"
set +e
"$BENCH_BIN" run \
  --addr "$leader_addr" \
  --table bench_0 \
  --participant-tables bench_0,bench_1,bench_2,bench_3 \
  --protocol v2 \
  --client-id 70001 \
  --session-epoch 1 \
  --workload cross-shard-txn \
  --clients 8 \
  --seconds "${M6_CRASH_SECONDS:-60}" \
  --warmup 0 \
  --rows "$ROWS" \
  --value-bytes "$VALUE_BYTES" \
  --txn-writes 4 \
  --contention disjoint \
  --timeout-ms "$TIMEOUT_MS" \
  --seed "$SEED" \
  > "$crash_dir/client-before-stop.json" 2> "$crash_dir/client-before-stop.stderr" &
crash_client_pid=$!
sleep "${M6_FAILURE_AFTER_SECONDS:-3}"
scripts/bench/stop-cluster.sh "$CLUSTER_DIR" TERM > "$crash_dir/stop.log" 2>&1
wait "$crash_client_pid" || true
set -e
RAGNORDB_BIN="$SERVER_BIN" scripts/bench/start-cluster.sh "$CLUSTER_DIR"
wait_for_group_leader 2 >/dev/null
save_statuses after-recovery
recovery_leader="$(wait_for_group_leader 2)"
"$BENCH_BIN" run \
  --addr "127.0.0.1:$((7100 + recovery_leader))" \
  --table bench_0 \
  --protocol v2 \
  --client-id 71001 \
  --session-epoch 1 \
  --workload single-shard-txn \
  --clients 1 \
  --seconds 2 \
  --warmup 0 \
  --rows "$ROWS" \
  --value-bytes "$VALUE_BYTES" \
  --txn-writes 1 \
  --contention disjoint \
  --timeout-ms "$TIMEOUT_MS" \
  --seed "$SEED" \
  > "$crash_dir/recovery-probe.json" \
  2> "$crash_dir/recovery-probe.stderr"
save_statuses final
printf "Milestone 6 baseline completed in %s\n" "$OUTPUT_DIR"
