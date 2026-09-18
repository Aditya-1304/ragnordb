#  baseline and invariants

This document freezes the pre-optimization evidence for Milestone 5A. It is a
measurement record, not evidence that the later runtime architecture phases
are complete. The production execution path was not changed by this phase.

## Reproduction

Run from the `ragnordb/` workspace root:

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo build --release -p ragnordb-cli -p ragnordb-bench
scripts/bench/run-phase5a-baseline.sh /tmp/ragnordb-phase5a-baseline-<run-id>
```

The wrapper refuses to overwrite an existing output directory. It records the
commit, branch, dirty-worktree state, compiler, kernel, CPU, memory, disk,
benchmark parameters, Criterion output, three-node configuration, node
status, Prometheus metrics, process snapshots, `pidstat`, `perf`, and `strace`
outputs. The live phase uses V2 requests with unique client identities and
monotonic request sequences.

The frozen run below was executed on 2026-09-13 from commit
`6d9a18b185993b174060a978f75c0ab96989c5b2`, branch
`feat/metadata-group_sharding`, with a dirty worktree containing only the
benchmark changes listed by the wrapper. Its evidence directory is:

```text
/tmp/ragnordb-phase5a-baseline-20260913-final
```

Parameters were 1,000 rows, 256-byte values, load batches of 100, five-second
closed-loop measured windows, 25 warmup operations per client, a 100-row scan,
10,000 ms request timeout, seed 42, and client counts 1, 2, and 8. Criterion
used 10 samples, one-second warmup, and one-second measurement per case.

## Coverage matrix

| Baseline boundary | Evidence | Interpretation |
| --- | --- | --- |
| `multiraft_density` | `phase5a` Criterion target: host scheduler turns and status snapshots at 1, 100, 1,000, and 10,000 groups | Measures the current host scheduler/status implementation with registered idle groups. It is a local boundary baseline, not a 50,000-group production claim. |
| `sql_parallelism` | Live V2 point-read runs at 1, 2, and 8 clients | Measures end-to-end closed-loop SQL behavior through the current server path. |
| `rpc_hol` | Live range scan concurrent with a point-read run | Measures point-read latency while another connection continuously submits range scans. |
| `persist_vs_apply` | `phase5a` Criterion target: logical A-WAL adapter append and tablet apply at 1, 10, and 100 writes | Separates the local persistence-admission model from state-machine apply cost. The logical adapter is intentionally not presented as fsync latency. |
| replication pipeline | `phase5a` Criterion target: three-replica in-memory proposal-to-apply | Existing deterministic simulated replication path; it does not replace a live network/Raft run. |
| routing | `phase5a` Criterion target: canonical primary-key point route and full-scan route | Uses the public `TabletRouter` API and canonical primary-key bytes. |
| WAL | `phase5a` Criterion target: filesystem append-and-sync of a 256-byte record | Uses the real A-WAL `WalHandle` and filesystem segment directory. |
| transport HOL | Live concurrent SQL HOL run plus `phase5a` RPC protobuf encode/decode | The live run is the HOL evidence; the local codec numbers are supporting serialization baselines only. |

## Local Criterion baseline

The values below are Criterion's `[low, estimate, high]` interval. They are
reported in the units emitted by Criterion.

| Group | Case | Result |
| --- | --- | --- |
| `multiraft_density` | scheduler turn / 1 | `[227.99 ns, 230.18 ns, 232.14 ns]` |
| `multiraft_density` | scheduler turn / 100 | `[22.831 us, 23.061 us, 23.306 us]` |
| `multiraft_density` | scheduler turn / 1,000 | `[562.63 us, 565.47 us, 569.76 us]` |
| `multiraft_density` | scheduler turn / 10,000 | `[6.5404 ms, 6.6294 ms, 6.6846 ms]` |
| `multiraft_density` | status snapshot / 1 | `[42.309 ns, 42.547 ns, 42.776 ns]` |
| `multiraft_density` | status snapshot / 100 | `[2.8640 us, 2.8793 us, 2.8922 us]` |
| `multiraft_density` | status snapshot / 1,000 | `[29.649 us, 29.741 us, 29.868 us]` |
| `multiraft_density` | status snapshot / 10,000 | `[369.60 us, 379.58 us, 386.42 us]` |
| `persist_vs_apply` | logical append / 1, 10, 100 | `[23.187, 22.877, 23.545 ns]` estimates |
| `persist_vs_apply` | tablet apply / 1, 10, 100 | `[1.7723, 13.710, 196.60 us]` estimates |
| replication pipeline | three-replica proposal to apply | `[16.924 us, 17.248 us, 17.492 us]` |
| routing | point route | `[157.28 ns, 158.74 ns, 161.23 ns]` |
| routing | full scan route | `[23.342 ns, 23.514 ns, 23.773 ns]` |
| WAL | filesystem append and sync / 256 bytes | `[8.7642 us, 8.8438 us, 8.9129 us]` |
| transport HOL | RPC encode/decode / 64 bytes | `[123.59 ns, 125.63 ns, 126.77 ns]` |
| transport HOL | RPC encode/decode / 1,024 bytes | `[189.26 ns, 191.46 ns, 193.76 ns]` |
| transport HOL | RPC encode/decode / 16,384 bytes | `[1.1848 us, 1.2238 us, 1.2637 us]` |

The complete raw output remains in
`criterion-phase5a.txt`; the existing Milestone 4 reference output is in
`criterion-milestone4.txt` in the evidence directory.

## Live SQL baseline

All live reports had `valid_run=true`, zero failed operations, and zero error
counts. Latencies are client-observed microseconds; throughput is successful
operations per second.

| Workload | Clients | Successful ops/s | p50 | p95 | p99 | Request bytes | Response bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| point read | 1 | 121.41 | 8,383 | 10,487 | 10,679 | 118,360 | 215,789 |
| point read | 2 | 122.28 | 16,751 | 20,879 | 21,119 | 119,179 | 217,557 |
| point read | 8 | 124.45 | 65,151 | 73,151 | 75,711 | 121,385 | 223,217 |
| range scan | 1 | 57.71 | 17,487 | 20,111 | 21,807 | 56,495 | 7,653,876 |
| range scan during HOL | 1 | 38.34 | 25,967 | 30,143 | 32,159 | 37,483 | 5,084,928 |
| point read during HOL | 1 | 44.93 | 25,823 | 30,191 | 31,039 | 43,706 | 79,855 |

Setup loaded 1,000 rows in 9.23 seconds. The final three-node status showed
metadata group 1 led by node 1 and tablet group 3 led by node 1, with tablet
commit and applied index both at 11 on all three replicas. No group had
pending work or pending messages at the final status capture.

These numbers are a baseline for comparisons, not a scalability or production
capacity claim. In particular, the point-read result is effectively flat in
throughput while tail latency rises with client count, which is evidence to
preserve when evaluating the later SQL execution and ownership changes.

## Resource and observability evidence

The wrapper captured the following for the node selected as the metadata-group
leader during each run:

- `perf stat`: task-clock, context switches, CPU migrations, page faults,
  cycles, instructions, and cache misses;
- `pidstat`: per-second user/system CPU and wait percentages;
- `/proc/<pid>/status`: RSS, high-water RSS, thread count, and voluntary/non-
  voluntary context switches;
- `/proc/<pid>/io`: character I/O, syscall read/write counts, and storage I/O
  counters;
- `/status` and `/metrics`: Raft group frontiers, pending queues, durability,
  SQL, transaction, WAL, and checkpoint counters.

For the final 8-client point-read run, the selected server process reported 42
threads before and 43 after the window, RSS from 20,124 KiB to 20,492 KiB,
and 634.34 ms of task-clock over 5.00 seconds. The process-level counter
sample recorded 54 page faults; exact values for every run are in the
`metrics-*` files.

Two requested observability dimensions are not exposed by this checkout and
are therefore recorded as gaps rather than inferred:

- exact allocator allocation/free counts: only RSS/HWM proxies were captured;
- lock-contention and scheduler wakeup counts: there is no dedicated
  application metric, `perf` scheduler tracepoints were blocked by the host,
  and `strace` attachment was blocked by ptrace policy.

The `/proc` syscall and byte counters plus the benchmark's exact wire-byte
totals remain valid collected evidence, but they must not be described as
server-internal encoding/copy allocation counters. Adding those counters is a
separate instrumentation task for a later approved phase.

## Invariants checked

- The full workspace test suite remained green before and after the benchmark
  harness work.
- The benchmark uses V2 identity, session epoch, and monotonically increasing
  request sequences; setup retries reuse the same sequence for the same logical
  insert request.
- The live setup waits for metadata and tablet-group leadership and retries
  only retryable tablet activation responses.
- The benchmark never treats a proposal as successful without a successful SQL
  response and marks any failed measured operation as an invalid run.
- The current 3-node configuration is fresh per run, with high snapshot
  thresholds so snapshot work does not contaminate this baseline.
- Existing production Raft, WAL, recovery, MVCC, transaction, snapshot, and
  replicated-tablet code was not edited for Phase 5A.0.
