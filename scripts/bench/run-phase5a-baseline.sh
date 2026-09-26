#!/usr/bin/env bash
set -euo pipefail

# Produce one immutable Phase 5A.0 evidence directory. The caller must provide
# a new path so a later run cannot silently replace an earlier baseline.
OUTPUT_DIR="${1:?usage: run-phase5a-baseline.sh OUTPUT_DIR}"
if [ -e "$OUTPUT_DIR" ]; then
  printf 'refusing to overwrite existing output directory: %s\n' "$OUTPUT_DIR" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
BENCH_BIN="$REPO_ROOT/target/release/ragnordb-bench"
SERVER_BIN="$REPO_ROOT/target/release/ragnordb"
DATASET_ROWS="${PHASE5A_ROWS:-1000}"
VALUE_BYTES="${PHASE5A_VALUE_BYTES:-256}"
LOAD_BATCH_SIZE="${PHASE5A_LOAD_BATCH_SIZE:-100}"
LIVE_SECONDS="${PHASE5A_SECONDS:-5}"
WARMUP="${PHASE5A_WARMUP:-25}"
SCAN_ROWS="${PHASE5A_SCAN_ROWS:-100}"
TIMEOUT_MS="${PHASE5A_TIMEOUT_MS:-10000}"
SEED="${PHASE5A_SEED:-42}"
CLIENT_COUNTS="${PHASE5A_CLIENT_COUNTS:-1 2 8}"
CRITERION_SAMPLE_SIZE="${PHASE5A_CRITERION_SAMPLE_SIZE:-10}"
CRITERION_WARMUP_SECONDS="${PHASE5A_CRITERION_WARMUP_SECONDS:-1}"
CRITERION_MEASUREMENT_SECONDS="${PHASE5A_CRITERION_MEASUREMENT_SECONDS:-1}"
CLUSTER_DIR="$OUTPUT_DIR/cluster"

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'required command is missing: %s\n' "$1" >&2
    exit 2
  }
}

for command_name in cargo curl date df git jq lscpu ps rustc sha256sum uname; do
  require_command "$command_name"
done

SOURCE_SHA="$(git rev-parse HEAD)"
SOURCE_STATUS="$(git status --porcelain)"
if [ -n "$SOURCE_STATUS" ] && [ "${RAGNORDB_ALLOW_DIRTY_BENCH:-0}" != "1" ]; then
  printf 'refusing official benchmark from a dirty worktree\n' >&2
  git status --short >&2
  exit 2
fi

RAGNORDB_BUILD_REVISION="$SOURCE_SHA"
if [ -n "$SOURCE_STATUS" ]; then
  RAGNORDB_BUILD_REVISION="${SOURCE_SHA}-dirty"
fi
export RAGNORDB_BUILD_REVISION

mkdir -p "$OUTPUT_DIR"
cargo build \
  --release \
  --locked \
  -p ragnordb-cli \
  -p ragnordb-bench \
  > "$OUTPUT_DIR/release-build.txt" 2>&1

if [ ! -x "$BENCH_BIN" ] || [ ! -x "$SERVER_BIN" ]; then
  printf 'release build completed without the expected benchmark binaries\n' >&2
  exit 2
fi

printf '%s\n' "$SOURCE_SHA" > "$OUTPUT_DIR/git-commit.txt"
git branch --show-current > "$OUTPUT_DIR/git-branch.txt"
git status --short > "$OUTPUT_DIR/git-status.txt"
git diff --stat > "$OUTPUT_DIR/git-diff-stat.txt"
git diff --name-status > "$OUTPUT_DIR/git-diff-name-status.txt"
uname -a > "$OUTPUT_DIR/uname.txt"
lscpu > "$OUTPUT_DIR/lscpu.txt"
rustc --version --verbose > "$OUTPUT_DIR/rustc.txt"
cargo --version > "$OUTPUT_DIR/cargo.txt"
df -T "$REPO_ROOT" > "$OUTPUT_DIR/disk.txt"
awk '/^(MemTotal|MemAvailable|SwapTotal|SwapFree):/ { print }' /proc/meminfo > "$OUTPUT_DIR/memory.txt"
sha256sum "$SERVER_BIN" > "$OUTPUT_DIR/server-binary.sha256"
sha256sum "$BENCH_BIN" > "$OUTPUT_DIR/bench-binary.sha256"
"$SERVER_BIN" status --addr 127.0.0.1:1 > "$OUTPUT_DIR/server-build-info.txt" 2>&1 || true

{
  printf 'run_id=%s\n' "$RUN_ID"
  printf 'build_revision=%s\n' "$RAGNORDB_BUILD_REVISION"
  printf 'repo_root=%s\n' "$REPO_ROOT"
  printf 'bench_binary=%s\n' "$BENCH_BIN"
  printf 'server_binary=%s\n' "$SERVER_BIN"
  printf 'dataset_rows=%s\n' "$DATASET_ROWS"
  printf 'value_bytes=%s\n' "$VALUE_BYTES"
  printf 'load_batch_size=%s\n' "$LOAD_BATCH_SIZE"
  printf 'live_seconds=%s\n' "$LIVE_SECONDS"
  printf 'warmup_operations_per_client=%s\n' "$WARMUP"
  printf 'scan_rows=%s\n' "$SCAN_ROWS"
  printf 'timeout_ms=%s\n' "$TIMEOUT_MS"
  printf 'seed=%s\n' "$SEED"
  printf 'client_counts=%s\n' "$CLIENT_COUNTS"
  printf 'criterion_sample_size=%s\n' "$CRITERION_SAMPLE_SIZE"
  printf 'criterion_warmup_seconds=%s\n' "$CRITERION_WARMUP_SECONDS"
  printf 'criterion_measurement_seconds=%s\n' "$CRITERION_MEASUREMENT_SECONDS"
} > "$OUTPUT_DIR/parameters.txt"

cargo bench -p ragnordb-bench --bench milestone4 -- \
  --noplot \
  --sample-size "$CRITERION_SAMPLE_SIZE" \
  --warm-up-time "$CRITERION_WARMUP_SECONDS" \
  --measurement-time "$CRITERION_MEASUREMENT_SECONDS" \
  --save-baseline "phase5a-$RUN_ID" \
  > "$OUTPUT_DIR/criterion-milestone4.txt" 2>&1

cargo bench -p ragnordb-bench --bench phase5a -- \
  --noplot \
  --sample-size "$CRITERION_SAMPLE_SIZE" \
  --warm-up-time "$CRITERION_WARMUP_SECONDS" \
  --measurement-time "$CRITERION_MEASUREMENT_SECONDS" \
  --save-baseline "phase5a-$RUN_ID" \
  > "$OUTPUT_DIR/criterion-phase5a.txt" 2>&1

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
      # The bounded /status response intentionally omits the full group list;
      # readiness needs the explicit diagnostic endpoint so it cannot confuse
      # an active host with an elected metadata leader.
      status_json="$(curl -fsS --max-time 2 "http://127.0.0.1:$((7200 + node_id))/status/groups" 2>/dev/null || true)"
      if printf '%s' "$status_json" \
          | jq -e --argjson group_id "$group_id" \
            'any(.multiraft.groups[]?; .raft_group_id == $group_id and .role == "leader")' \
          >/dev/null 2>&1; then
        printf '%s\n' "$node_id"
        return 0
      fi
    done
    sleep 0.25
  done

  printf 'timed out waiting for Raft group %s leader\n' "$group_id" >&2
  return 1
}

save_statuses() {
  local label="$1"
  local node_id

  for node_id in 1 2 3; do
    curl -fsS "http://127.0.0.1:$((7200 + node_id))/status" \
      > "$OUTPUT_DIR/status-$label-node-$node_id.json"
    curl -fsS "http://127.0.0.1:$((7200 + node_id))/status/groups" \
      > "$OUTPUT_DIR/status-groups-$label-node-$node_id.json"
    curl -fsS "http://127.0.0.1:$((7200 + node_id))/metrics" \
      > "$OUTPUT_DIR/metrics-$label-node-$node_id.txt"
  done
}

proc_snapshot() {
  local pid="$1"
  local destination="$2"

  {
    printf '[proc_status]\n'
    if [ -r "/proc/$pid/status" ]; then
      awk '/^(Name|State|VmRSS|VmHWM|Threads|voluntary_ctxt_switches|nonvoluntary_ctxt_switches):/ { print }' \
        "/proc/$pid/status"
    else
      printf 'process_not_present=%s\n' "$pid"
    fi
    printf '\n[proc_io]\n'
    if [ -r "/proc/$pid/io" ]; then
      awk '/^(rchar|wchar|syscr|syscw|read_bytes|write_bytes|cancelled_write_bytes):/ { print }' \
        "/proc/$pid/io"
    else
      printf 'process_io_not_present=%s\n' "$pid"
    fi
    printf '\n[ps]\n'
    ps -o pid=,ppid=,stat=,nlwp=,psr=,etimes=,pcpu=,pmem=,cmd= -p "$pid" || true
  } > "$destination"
}

metric_pidstat=""
metric_perf=""
metric_sched=""
metric_strace=""

start_process_metrics() {
  local pid="$1"
  local duration="$2"
  local label="$3"

  proc_snapshot "$pid" "$OUTPUT_DIR/metrics-$label-before.txt"

  if command -v pidstat >/dev/null 2>&1; then
    pidstat -h -p "$pid" 1 "$duration" > "$OUTPUT_DIR/metrics-$label-pidstat.txt" 2>&1 &
    metric_pidstat=$!
  else
    metric_pidstat=""
  fi

  if command -v perf >/dev/null 2>&1; then
    perf stat -p "$pid" \
      -e task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions,cache-misses \
      -o "$OUTPUT_DIR/metrics-$label-perf.txt" -- sleep "$duration" \
      > "$OUTPUT_DIR/metrics-$label-perf.stdout.txt" 2>&1 &
    metric_perf=$!

    perf stat -p "$pid" \
      -e sched:sched_switch,sched:sched_wakeup \
      -o "$OUTPUT_DIR/metrics-$label-scheduler.txt" -- sleep "$duration" \
      > "$OUTPUT_DIR/metrics-$label-scheduler.stdout.txt" 2>&1 &
    metric_sched=$!
  else
    metric_perf=""
    metric_sched=""
  fi

  if command -v strace >/dev/null 2>&1; then
    strace -c -f -p "$pid" -o "$OUTPUT_DIR/metrics-$label-strace.txt" \
      > "$OUTPUT_DIR/metrics-$label-strace.stdout.txt" 2>&1 &
    metric_strace=$!
  else
    metric_strace=""
  fi
}

finish_process_metrics() {
  local pid="$1"
  local label="$2"

  if [ -n "$metric_strace" ]; then
    kill -INT "$metric_strace" 2>/dev/null || true
    wait "$metric_strace" 2>/dev/null || true
    metric_strace=""
  fi
  if [ -n "$metric_pidstat" ]; then
    wait "$metric_pidstat" 2>/dev/null || true
    metric_pidstat=""
  fi
  if [ -n "$metric_perf" ]; then
    wait "$metric_perf" 2>/dev/null || true
    metric_perf=""
  fi
  if [ -n "$metric_sched" ]; then
    wait "$metric_sched" 2>/dev/null || true
    metric_sched=""
  fi

  proc_snapshot "$pid" "$OUTPUT_DIR/metrics-$label-after.txt"
}

wait_for_live_readiness() {
  local label="$1"
  local leader_addr="$2"
  local attempt

  # Raft leadership is published before the tablet's current-term activation
  # boundary is complete. Require a successful end-to-end read before starting
  # a measured workload so intentional activation-time NOT_LEADER responses do
  # not become benchmark failures.
  for attempt in $(seq 1 120); do
    if "$BENCH_BIN" run \
      --addr "$leader_addr" \
      --table bench \
      --protocol v2 \
      --client-id "$((400000 + attempt))" \
      --session-epoch "$attempt" \
      --workload point-read \
      --clients 1 \
      --seconds 1 \
      --warmup 0 \
      --rows "$DATASET_ROWS" \
      --value-bytes "$VALUE_BYTES" \
      --scan-rows "$SCAN_ROWS" \
      --timeout-ms "$TIMEOUT_MS" \
      --seed "$SEED" \
      > "$OUTPUT_DIR/$label-readiness.json" \
      2> "$OUTPUT_DIR/$label-readiness.stderr"
    then
      return 0
    fi
    sleep 0.25
  done

  printf 'live workload readiness did not converge: %s\n' "$label" >&2
  return 1
}

run_live() {
  local label="$1"
  shift
  local leader_pid leader_addr
  local exit_code=0
  local leader_node

  # Group 2 is the reserved metadata group in the current bootstrap contract.
  leader_node="$(wait_for_group_leader 2)"
  wait_for_group_leader 3 >/dev/null
  leader_addr="127.0.0.1:$((7100 + leader_node))"
  wait_for_live_readiness "$label" "$leader_addr"
  leader_pid="$(cat "$CLUSTER_DIR/node-$leader_node.pid")"
  start_process_metrics "$leader_pid" "$LIVE_SECONDS" "$label"
  "$BENCH_BIN" run --addr "$leader_addr" "$@" > "$OUTPUT_DIR/$label.json" 2> "$OUTPUT_DIR/$label.stderr" || exit_code=$?
  finish_process_metrics "$leader_pid" "$label"
  printf 'leader_node=%s\n' "$leader_node" > "$OUTPUT_DIR/$label-leader.txt"

  if [ "$exit_code" -ne 0 ]; then
    printf 'live benchmark failed: %s (see %s.json and %s.stderr)\n' \
      "$label" "$label" "$label" >&2
    return "$exit_code"
  fi

  if ! jq -e '.valid_run == true' "$OUTPUT_DIR/$label.json" >/dev/null 2>&1; then
    printf 'live benchmark did not produce a valid JSON report: %s\n' "$label" >&2
    return 1
  fi
}

metadata_leader_node="$(wait_for_group_leader 2)"
metadata_leader_addr="127.0.0.1:$((7100 + metadata_leader_node))"

"$BENCH_BIN" load \
  --addr "$metadata_leader_addr" \
  --protocol v2 \
  --client-id 9001 \
  --session-epoch 1 \
  --rows "$DATASET_ROWS" \
  --batch-size "$LOAD_BATCH_SIZE" \
  --value-bytes "$VALUE_BYTES" \
  --create-table \
  > "$OUTPUT_DIR/load.json" 2> "$OUTPUT_DIR/load.stderr"
jq -e '.loaded_rows > 0' "$OUTPUT_DIR/load.json" >/dev/null
wait_for_group_leader 3 >/dev/null
save_statuses after-load

for clients in $CLIENT_COUNTS; do
  run_live "sql-parallelism-c$clients" \
    --protocol v2 \
    --client-id "$((10000 + clients * 100))" \
    --session-epoch 1 \
    --workload point-read \
    --clients "$clients" \
    --seconds "$LIVE_SECONDS" \
    --warmup "$WARMUP" \
    --rows "$DATASET_ROWS" \
    --value-bytes "$VALUE_BYTES" \
    --scan-rows "$SCAN_ROWS" \
    --timeout-ms "$TIMEOUT_MS" \
    --seed "$SEED"
done

run_live range-scan \
  --protocol v2 \
  --client-id 20001 \
  --session-epoch 1 \
  --workload range-scan \
  --clients 1 \
  --seconds "$LIVE_SECONDS" \
  --warmup "$WARMUP" \
  --rows "$DATASET_ROWS" \
  --value-bytes "$VALUE_BYTES" \
  --scan-rows "$SCAN_ROWS" \
  --timeout-ms "$TIMEOUT_MS" \
  --seed "$SEED"

leader_node="$(wait_for_group_leader 2)"
wait_for_group_leader 3 >/dev/null
metadata_leader_addr="127.0.0.1:$((7100 + leader_node))"
leader_pid="$(cat "$CLUSTER_DIR/node-$leader_node.pid")"
start_process_metrics "$leader_pid" "$LIVE_SECONDS" rpc-hol
set +e
"$BENCH_BIN" run \
  --addr "$metadata_leader_addr" \
  --protocol v2 \
  --client-id 30001 \
  --session-epoch 1 \
  --workload range-scan \
  --clients 1 \
  --seconds "$LIVE_SECONDS" \
  --warmup "$WARMUP" \
  --rows "$DATASET_ROWS" \
  --value-bytes "$VALUE_BYTES" \
  --scan-rows "$SCAN_ROWS" \
  --timeout-ms "$TIMEOUT_MS" \
  --seed "$SEED" \
  > "$OUTPUT_DIR/rpc-hol-range.json" 2> "$OUTPUT_DIR/rpc-hol-range.stderr" &
range_pid=$!
sleep 0.25
"$BENCH_BIN" run \
  --addr "$metadata_leader_addr" \
  --protocol v2 \
  --client-id 31001 \
  --session-epoch 1 \
  --workload point-read \
  --clients 1 \
  --seconds "$LIVE_SECONDS" \
  --warmup "$WARMUP" \
  --rows "$DATASET_ROWS" \
  --value-bytes "$VALUE_BYTES" \
  --scan-rows "$SCAN_ROWS" \
  --timeout-ms "$TIMEOUT_MS" \
  --seed "$SEED" \
  > "$OUTPUT_DIR/rpc-hol-point.json" 2> "$OUTPUT_DIR/rpc-hol-point.stderr"
point_exit_code=$?
wait "$range_pid"
range_exit_code=$?
set -e
finish_process_metrics "$leader_pid" rpc-hol
printf 'leader_node=%s\n' "$leader_node" > "$OUTPUT_DIR/rpc-hol-leader.txt"
if [ "$point_exit_code" -ne 0 ] || [ "$range_exit_code" -ne 0 ]; then
  printf 'RPC HOL run failed: point=%s range=%s\n' "$point_exit_code" "$range_exit_code" >&2
  exit 1
fi

save_statuses final
printf 'Phase 5A.0 baseline completed in %s\n' "$OUTPUT_DIR"
