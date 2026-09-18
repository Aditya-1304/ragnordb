#!/usr/bin/env bash
set -euo pipefail

# Capture the Phase 5A.11 release gate as one immutable evidence directory.
# The gate composes the Phase 5A.0 workload wrapper with independent-tablet
# and overload runs required by the release checklist.
OUTPUT_DIR="${1:?usage: run-phase5a11-gate.sh OUTPUT_DIR}"
if [ -e "$OUTPUT_DIR" ]; then
  printf 'refusing to overwrite existing output directory: %s\n' "$OUTPUT_DIR" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RAFT_ROOT="$(cd "$REPO_ROOT/../Papers/raft" && pwd)"
WAL_ROOT="$(cd "$REPO_ROOT/../wal" && pwd)"
BLOOM_ROOT="$(cd "$REPO_ROOT/../bloom-bloom" && pwd)"
mkdir -p "$OUTPUT_DIR"
cd "$REPO_ROOT"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
BENCH_BIN="${RAGNORDB_BENCH_BIN:-$REPO_ROOT/target/release/ragnordb-bench}"
SERVER_BIN="${RAGNORDB_BIN:-$REPO_ROOT/target/release/ragnordb}"
TABLE_ROWS="${PHASE5A11_ROWS:-256}"
TABLE_COUNT="${PHASE5A11_TABLES:-4}"
LIVE_SECONDS="${PHASE5A11_SECONDS:-5}"
WARMUP="${PHASE5A11_WARMUP:-10}"
TIMEOUT_MS="${PHASE5A11_TIMEOUT_MS:-10000}"
CRITERION_SAMPLE_SIZE="${PHASE5A11_CRITERION_SAMPLE_SIZE:-10}"
CRITERION_WARMUP_SECONDS="${PHASE5A11_CRITERION_WARMUP_SECONDS:-1}"
CRITERION_MEASUREMENT_SECONDS="${PHASE5A11_CRITERION_MEASUREMENT_SECONDS:-1}"
CLUSTER_DIR="$OUTPUT_DIR/independent-cluster"

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'required command is missing: %s\n' "$1" >&2
    exit 2
  }
}

for command_name in cargo curl date df git jq lscpu ps rustc uname; do
  require_command "$command_name"
done

if [ ! -x "$BENCH_BIN" ] || [ ! -x "$SERVER_BIN" ]; then
  printf 'release binaries are missing; build ragnordb-cli and ragnordb-bench first\n' >&2
  exit 2
fi

git -C "$REPO_ROOT" rev-parse HEAD > "$OUTPUT_DIR/git-commit.txt"
git -C "$REPO_ROOT" branch --show-current > "$OUTPUT_DIR/git-branch.txt"
git -C "$REPO_ROOT" status --short > "$OUTPUT_DIR/git-status.txt"
git -C "$REPO_ROOT" diff --stat > "$OUTPUT_DIR/git-diff-stat.txt"
git -C "$REPO_ROOT" diff --name-status > "$OUTPUT_DIR/git-diff-name-status.txt"
git -C "$RAFT_ROOT" rev-parse HEAD > "$OUTPUT_DIR/raft-commit.txt"
git -C "$WAL_ROOT" rev-parse HEAD > "$OUTPUT_DIR/wal-commit.txt"
git -C "$BLOOM_ROOT" rev-parse HEAD > "$OUTPUT_DIR/bloom-commit.txt"
git -C "$RAFT_ROOT" status --porcelain > "$OUTPUT_DIR/raft-status.txt"
git -C "$WAL_ROOT" status --porcelain > "$OUTPUT_DIR/wal-status.txt"
git -C "$BLOOM_ROOT" status --porcelain > "$OUTPUT_DIR/bloom-status.txt"

for dependency_status in \
  "$OUTPUT_DIR/raft-status.txt" \
  "$OUTPUT_DIR/wal-status.txt" \
  "$OUTPUT_DIR/bloom-status.txt"
do
  if [ -s "$dependency_status" ]; then
    printf 'dependency repository is dirty: %s\n' "$dependency_status" >&2
    exit 1
  fi
done

uname -a > "$OUTPUT_DIR/uname.txt"
lscpu > "$OUTPUT_DIR/lscpu.txt"
rustc --version --verbose > "$OUTPUT_DIR/rustc.txt"
cargo --version > "$OUTPUT_DIR/cargo.txt"
df -T "$REPO_ROOT" > "$OUTPUT_DIR/disk.txt"
awk '/^(MemTotal|MemAvailable|SwapTotal|SwapFree):/ { print }' /proc/meminfo > "$OUTPUT_DIR/memory.txt"

{
  printf 'run_id=%s\n' "$RUN_ID"
  printf 'repo_root=%s\n' "$REPO_ROOT"
  printf 'raft_root=%s\n' "$RAFT_ROOT"
  printf 'independent_table_rows=%s\n' "$TABLE_ROWS"
  printf 'independent_table_count=%s\n' "$TABLE_COUNT"
  printf 'live_seconds=%s\n' "$LIVE_SECONDS"
  printf 'warmup_operations_per_client=%s\n' "$WARMUP"
  printf 'timeout_ms=%s\n' "$TIMEOUT_MS"
  printf 'criterion_sample_size=%s\n' "$CRITERION_SAMPLE_SIZE"
  printf 'criterion_warmup_seconds=%s\n' "$CRITERION_WARMUP_SECONDS"
  printf 'criterion_measurement_seconds=%s\n' "$CRITERION_MEASUREMENT_SECONDS"
} > "$OUTPUT_DIR/parameters.txt"

run_checked() {
  local label="$1"
  shift
  "$@" > "$OUTPUT_DIR/$label.stdout" 2> "$OUTPUT_DIR/$label.stderr"
}

run_checked ragnordb-fmt cargo fmt --all -- --check
run_checked ragnordb-tests cargo test --workspace -- --test-threads=1
run_checked ragnordb-clippy cargo clippy --workspace --all-targets -- -D warnings
run_checked raft-fmt bash -c "cd '$RAFT_ROOT' && cargo fmt -- --check"
run_checked raft-tests bash -c "cd '$RAFT_ROOT' && cargo test -- --test-threads=1"
run_checked ragnordb-release-build cargo build --release -p ragnordb-cli -p ragnordb-bench

PHASE5A_ROWS="${PHASE5A_ROWS:-1000}" \
PHASE5A_VALUE_BYTES="${PHASE5A_VALUE_BYTES:-256}" \
PHASE5A_LOAD_BATCH_SIZE="${PHASE5A_LOAD_BATCH_SIZE:-100}" \
PHASE5A_SECONDS="${PHASE5A_SECONDS:-5}" \
PHASE5A_WARMUP="${PHASE5A_WARMUP:-25}" \
PHASE5A_SCAN_ROWS="${PHASE5A_SCAN_ROWS:-100}" \
PHASE5A_TIMEOUT_MS="${PHASE5A_TIMEOUT_MS:-10000}" \
PHASE5A_CLIENT_COUNTS="${PHASE5A_CLIENT_COUNTS:-1 2 8}" \
PHASE5A_CRITERION_SAMPLE_SIZE="$CRITERION_SAMPLE_SIZE" \
PHASE5A_CRITERION_WARMUP_SECONDS="$CRITERION_WARMUP_SECONDS" \
PHASE5A_CRITERION_MEASUREMENT_SECONDS="$CRITERION_MEASUREMENT_SECONDS" \
  "$SCRIPT_DIR/run-phase5a-baseline.sh" "$OUTPUT_DIR/baseline"

mkdir -p "$CLUSTER_DIR"
scripts/bench/make-cluster-configs.sh "$CLUSTER_DIR" 1000000 300000 \
  > "$OUTPUT_DIR/independent-config.stdout" \
  2> "$OUTPUT_DIR/independent-config.stderr"
RAGNORDB_BIN="$SERVER_BIN" scripts/bench/start-cluster.sh "$CLUSTER_DIR" \
  > "$OUTPUT_DIR/independent-start.stdout" \
  2> "$OUTPUT_DIR/independent-start.stderr"

cleanup_cluster() {
  scripts/bench/stop-cluster.sh "$CLUSTER_DIR" TERM \
    > "$OUTPUT_DIR/independent-stop.stdout" \
    2> "$OUTPUT_DIR/independent-stop.stderr" || true
}
trap cleanup_cluster EXIT

wait_for_metadata_leader() {
  local attempt node_id status_json
  for attempt in $(seq 1 240); do
    for node_id in 1 2 3; do
      # Leadership is available only from the explicit per-group diagnostic
      # endpoint; /status is deliberately bounded to aggregate/top-K data.
      status_json="$(curl -fsS --max-time 2 "http://127.0.0.1:$((7200 + node_id))/status/groups" 2>/dev/null || true)"
      if printf '%s' "$status_json" \
        | jq -e 'any(.multiraft.groups[]?; .raft_group_id == 2 and .role == "leader")' \
        >/dev/null 2>&1; then
        printf '%s\n' "$node_id"
        return 0
      fi
    done
    sleep 0.25
  done
  printf 'timed out waiting for metadata-group leader\n' >&2
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

metadata_leader_node="$(wait_for_metadata_leader)"
metadata_leader_addr="127.0.0.1:$((7100 + metadata_leader_node))"

for table_number in $(seq 1 "$TABLE_COUNT"); do
  table_name="independent_$table_number"
  "$BENCH_BIN" load \
    --addr "$metadata_leader_addr" \
    --table "$table_name" \
    --protocol v2 \
    --client-id "$((50000 + table_number))" \
    --session-epoch 1 \
    --rows "$TABLE_ROWS" \
    --batch-size 64 \
    --value-bytes 64 \
    --create-table \
    > "$OUTPUT_DIR/load-$table_name.json" \
    2> "$OUTPUT_DIR/load-$table_name.stderr"
  jq -e ".loaded_rows == $TABLE_ROWS" "$OUTPUT_DIR/load-$table_name.json" >/dev/null
done

save_statuses independent-before
leader_pid="$(cat "$CLUSTER_DIR/node-$metadata_leader_node.pid")"
proc_snapshot "$leader_pid" "$OUTPUT_DIR/independent-process-before.txt"

run_one_table() {
  local table_name="$1"
  local client_id="$2"
  local seed="$3"

  local readiness_attempt
  for readiness_attempt in $(seq 1 120); do
    if "$BENCH_BIN" run \
      --addr "$metadata_leader_addr" \
      --table "$table_name" \
      --protocol v2 \
      --client-id "$((700000 + readiness_attempt))" \
      --session-epoch "$readiness_attempt" \
      --workload point-read \
      --clients 1 \
      --seconds 1 \
      --warmup 0 \
      --rows "$TABLE_ROWS" \
      --value-bytes 64 \
      --scan-rows "$TABLE_ROWS" \
      --timeout-ms "$TIMEOUT_MS" \
      --seed "$seed" \
      > "$OUTPUT_DIR/independent-$table_name-readiness.json" \
      2> "$OUTPUT_DIR/independent-$table_name-readiness.stderr"
    then
      break
    fi
    if [ "$readiness_attempt" -eq 120 ]; then
      printf 'independent workload readiness did not converge: %s\n' "$table_name" >&2
      return 1
    fi
    sleep 0.25
  done

  "$BENCH_BIN" run \
    --addr "$metadata_leader_addr" \
    --table "$table_name" \
    --protocol v2 \
    --client-id "$client_id" \
    --session-epoch 1 \
    --workload point-read \
    --clients 2 \
    --seconds "$LIVE_SECONDS" \
    --warmup "$WARMUP" \
    --rows "$TABLE_ROWS" \
    --value-bytes 64 \
    --scan-rows "$TABLE_ROWS" \
    --timeout-ms "$TIMEOUT_MS" \
    --seed "$seed" \
    > "$OUTPUT_DIR/independent-$table_name.json" \
    2> "$OUTPUT_DIR/independent-$table_name.stderr"
}

"$BENCH_BIN" run \
  --addr "$metadata_leader_addr" \
  --table independent_1 \
  --protocol v2 \
  --client-id 60001 \
  --session-epoch 1 \
  --workload point-read \
  --clients 1 \
  --seconds "$LIVE_SECONDS" \
  --warmup "$WARMUP" \
  --rows "$TABLE_ROWS" \
  --value-bytes 64 \
  --scan-rows "$TABLE_ROWS" \
  --timeout-ms "$TIMEOUT_MS" \
  --seed 42 \
  > "$OUTPUT_DIR/independent-single.json" \
  2> "$OUTPUT_DIR/independent-single.stderr"

run_status=0
table_pids=()
for table_number in $(seq 1 "$TABLE_COUNT"); do
  table_name="independent_$table_number"
  run_one_table "$table_name" "$((61000 + table_number * 100))" "$((42 + table_number))" &
  table_pids+=("$!")
done
set +e
for table_pid in "${table_pids[@]}"; do
  wait "$table_pid"
  table_status=$?
  if [ "$table_status" -ne 0 ]; then
    run_status=1
  fi
done
set -e
if [ "$run_status" -ne 0 ]; then
  printf 'independent-table throughput run failed\n' >&2
  exit 1
fi

proc_snapshot "$leader_pid" "$OUTPUT_DIR/independent-process-after.txt"
save_statuses independent-after

for report in "$OUTPUT_DIR/independent-single.json" "$OUTPUT_DIR"/independent-independent_*.json; do
  jq -e '.valid_run == true and .failed_operations == 0' "$report" >/dev/null
done

# Drive many clients at one tablet. Sample the detailed status endpoint while
# the workload is in flight as well as after it drains; a post-run snapshot
# alone could miss a transient queue-growth violation.
mkdir -p "$OUTPUT_DIR/overload-status-samples"
"$BENCH_BIN" run \
  --addr "$metadata_leader_addr" \
  --table independent_1 \
  --protocol v2 \
  --client-id 70001 \
  --session-epoch 1 \
  --workload point-read \
  --clients 64 \
  --seconds "$LIVE_SECONDS" \
  --warmup "$WARMUP" \
  --rows "$TABLE_ROWS" \
  --value-bytes 64 \
  --scan-rows "$TABLE_ROWS" \
  --timeout-ms "$TIMEOUT_MS" \
  --seed 84 \
  > "$OUTPUT_DIR/overload.json" \
  2> "$OUTPUT_DIR/overload.stderr" &
overload_pid=$!

sample_number=0
while kill -0 "$overload_pid" 2>/dev/null; do
  sample_label="$(printf '%03d' "$sample_number")"
  for node_id in 1 2 3; do
    curl -fsS --max-time 2 "http://127.0.0.1:$((7200 + node_id))/status/groups" \
      > "$OUTPUT_DIR/overload-status-samples/status-groups-$sample_label-node-$node_id.json"
  done
  sample_number=$((sample_number + 1))
  sleep 0.25
done
set +e
wait "$overload_pid"
overload_exit_code=$?
set -e
if [ "$overload_exit_code" -ne 0 ]; then
  printf 'overload benchmark failed with exit code %s\n' "$overload_exit_code" >&2
  exit "$overload_exit_code"
fi
jq -e '.valid_run == true and .failed_operations == 0' "$OUTPUT_DIR/overload.json" >/dev/null
save_statuses overload-after

for status_file in \
  "$OUTPUT_DIR"/overload-status-samples/status-groups-*.json \
  "$OUTPUT_DIR"/status-groups-overload-after-node-*.json; do
  jq -e '
    (.multiraft | type == "object") and
    (.multiraft.groups | type == "array") and
    ((.multiraft.pending_message_count // 0) <= 8192) and
    ((.multiraft.pending_message_bytes // 0) <= 67108864) and
    ((.multiraft.pending_persistence_groups // 0) <= 64) and
    ((.multiraft.pending_persistence_records // 0) <= 4096) and
    ((.multiraft.pending_persistence_bytes // 0) <= 67108864) and
    all(.multiraft.groups[]?;
      ((.pending_messages // 0) <= 2048) and
      ((.pending_message_bytes // 0) <= 16777216) and
      ((.apply_backlog_entries // 0) <= 4096) and
      ((.apply_backlog_bytes // 0) <= 67108864))
  ' "$status_file" >/dev/null
done

printf 'PASS\n' > "$OUTPUT_DIR/gate-result.txt"
printf 'Phase 5A.11 gate completed in %s\n' "$OUTPUT_DIR"
