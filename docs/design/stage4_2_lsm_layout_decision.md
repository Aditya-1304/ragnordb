# Stage 4.2: LSM layout decision

Status: **decision and key-format gate complete**. Candidate B is selected for
V1. This closes Stage 4.2's prototype/decision/design work; it does not start
Stage 4.3 or claim that a durable LSM exists.

## Decision

Select **Candidate B: separate logical Default, Write, and Lock trees inside
one tablet-replica storage lineage**. The physical identity is
`(tablet_id, raft_group_id, replica_id)`. All families share one recovery
frontier, MANIFEST lineage, coherent generation, and future atomic
`CommandDelta` publication boundary. Node-level memory, block cache, I/O,
compaction scheduling, and background workers remain shared.

The selected V1 comparator, namespace IDs, key bytes, operating defaults, and
recovery direction are specified in [storage-format.md](../storage-format.md).
The production codec is `crates/ragnordb-storage/src/lsm/internal_key.rs`.

## Candidates compared

| Candidate | Ordered storage arrangement | Expected strength | Main cost |
|---|---|---|---|
| A — unified row-first | One ordered tree, sorted by logical row, record namespace, then version | Rows and their histories are adjacent; strongest latest-point and batch-commit timings in this model | Lock checks traverse/interrogate the mixed row history; one merged run carries every record kind |
| B — separate families | Distinct Default, Write, and Lock trees under one tablet storage generation; benchmark metadata used a shared metadata map | Direct lock lookup and write-history traversal; independently compactable record families; the same publication boundary can cover all trees | More tree/run coordination and cross-family publication work; latest-point and batch-commit timings trail A modestly |
| C — unified family-first | One ordered tree sorted by family, then row, then version | Family locality improves long scans and cache/block selectivity | A point operation may cross more key ranges; lock and latest-point costs are worse in this model |

Candidate B does **not** mean three database instances. The selected namespace
map also reserves Metadata and Index logical families beneath the same
tablet-replica lifetime. Rollback witnesses remain in Write; range tombstones
are colocated in Write. No namespace owns an independent MANIFEST or recovery
authority.

## Workload matrix and operations

Each profile feeds the same deterministic logical records to A, B, and C.
Criterion operations use the five 100,000-row profiles; the million-row
profile runs through the modeled block/cache/compaction path.

| Profile | Rows | Versions/key | Payload | Key order | Additional shape |
|---|---:|---:|---:|---|---|
| `oltp_normal_seq` | 100k | 4 | 96 B | Sequential | 1% locks, 1% rollback witnesses, 0.25% deletes |
| `history_heavy_random` | 100k | 16 | 64 B | Random | 1% locks and rollback witnesses; historical timestamps exercised |
| `large_rows` | 100k | 4 | 1 KiB | Sequential | No locks, rollbacks, or deletes |
| `lock_heavy` | 100k | 4 | 128 B | Random | 10% locks, 1% rollback witnesses |
| `write_churn` | 100k | 8 | 256 B | Sequential | 1% locks, 10% rollback witnesses, 5% deletes |
| `scan_heavy_million` | 1m | 4 | 128 B | Sequential | 0.1% locks and rollback witnesses; 10/100/1k/10k row scans modeled |

The comparison measured latest, middle, old, and missing point reads; lock
hit/miss; rollback-witness lookup and publication; prewrite validation; batch
intent commit; 10/100/1,000-row scans; sorted-run merge; modeled cold/warm
block touches, cache hits/misses, bytes read, flush bytes, and compaction write
amplification. Candidate code and its permanent benchmark tests remain under
`crates/ragnordb-bench` and are experimental; none of their codec bytes are a
production format.

## Measured evidence

Criterion medians are from the normal sequential OLTP profile. The sample count
was 10; these are short in-memory-model timings, not production latency SLOs.

| Operation | A | B | C | Readout |
|---|---:|---:|---:|---|
| Latest point hit | 0.413 µs | 0.478 µs | 0.765 µs | A leads; B is 15.7% slower than A and 37.5% faster than C |
| Middle historical read | 0.429 µs | 0.341 µs | 0.558 µs | B leads |
| Old historical read | 0.398 µs | 0.375 µs | 0.575 µs | B leads slightly |
| Point miss | 0.479 µs | 0.463 µs | 0.529 µs | B leads slightly |
| Lock hit/miss pair | 0.473 µs | 0.238 µs | 0.550 µs | B is about 2.0x faster than A and 2.3x faster than C |
| Rollback witness lookup | 0.115 µs | 0.118 µs | 0.377 µs | A and B are close; C is substantially slower |
| Prewrite validation | 0.416 µs | 0.344 µs | 0.795 µs | B leads |
| Intent commit, batch 32 | 75.5 µs | 84.7 µs | 89.8 µs | A leads; B is 12.1% slower than A |

Modeled physical evidence uses 4 KiB blocks, a 16 MiB shared cache, 4 MiB
flush accounting, four L0 runs before compaction, 10:1 level ratio, and four
modeled levels. These are benchmark parameters; only the four-run trigger and
10:1 ratio are also selected as separate V1 operating defaults, for the
rationales in `storage-format.md`. The four-level model is **not** the V1 level
count.

| Modeled result | A | B | C |
|---|---:|---:|---:|
| 10k-row cold scan bytes, 1m-row profile | 9.24 MB | 9.17 MB | 6.81 MB |
| Cold block misses for that scan | 2,308 | 2,249 | 1,668 |
| Compaction write amplification, 1m-row profile | 2.488x | 2.154x | 2.217x |
| Compaction write amplification, 16-version history profile | 2.241x | 1.298x | 2.250x |

C has the strongest long-scan byte result: about 26% fewer cold bytes than B in
the million-row model. B nevertheless has the clearest OLTP/transaction profile
and the lowest modeled compaction amplification in both reported write-heavy
comparisons. A's latest-read and commit advantages are real in this model, but
B's lock lookup is roughly twice as fast and its prewrite validation is faster.
Those checks occur on the transaction path before intent publication. B also
keeps the logical family operations explicit for the all-family atomic
publication required next.

## Selection and rejected alternatives

### Why B wins

Lock checking is central to prewrite and intent resolution. B's separate Lock
tree halves the modeled lock hit/miss cost versus A, with the middle/old reads,
point miss, prewrite, and modeled compaction results also favoring B. Its
latest-point penalty versus A is about 16%, and batch-32 commit is about 12%
slower. That is a deliberate transaction-path tradeoff, not a claim that B
wins every operation.

The separation is useful only while publication remains one coherent tablet
generation. Stage 4.3 must preflight and publish every applicable Default,
Write, Lock, rollback, primary-status, retry outcome/floor, and processed Raft
index/term edit together. It must never expose new data without its retry
outcome, advance the frontier past partial data, or publish a lock removal
without the corresponding committed Write/Default state.

### Why A is not selected

A is fastest for latest point hits and batch-32 commits, by about 16% and 12%
respectively versus B. It is rejected because the lock hit/miss pair is about
2x slower, prewrite is slower, and its modeled compaction amplification is
higher in the million-row and history-heavy workloads. A remains in the
benchmark as the comparison baseline; the decision can be revisited only with
new measured evidence and a format-version migration plan.

### Why C is not selected

C reduces million-row 10k-scan cold bytes by about 26% versus B. It is about
60% slower on latest point hits, 2.3x slower on lock hit/miss, and about 6%
slower on batch-32 commit. It also has slightly higher modeled million-row
compaction amplification than B. The scan win does not offset the OLTP path
costs for the selected transaction workload.

## Model boundaries

The prototype compares deterministic in-memory `BTreeMap` layouts and uses a
declared model for sorted runs, fixed-size blocks, an LRU cache, flushes, and
leveled-compaction byte accounting. It does not perform SSTable or device I/O;
it does not measure `fsync`, OS page-cache behavior, real segment indexes or
Bloom filters, compression, background scheduling contention, file-open cost,
crash recovery, or cross-family durability. The measured Criterion operations
also include in-memory lookup/publication mechanics and are not end-to-end
transaction timings.

Therefore this gate chooses the logical layout and comparator; it does not
prove real-disk performance or production readiness. The raw run is preserved
at [stage4_2_layout_benchmark_2026-09-27.log](../benchmarks/stage4_2_layout_benchmark_2026-09-27.log),
alongside the experimental benchmark source. It was captured from base HEAD
`5aa87a2` with the A/B/C prototype present as uncommitted working-tree source;
the run did not record a source checksum. The committed raw log and code make
the evidence inspectable, but do not retroactively turn that run into a clean
revision benchmark. Reproduction command:

```sh
cargo bench -p ragnordb-bench --bench lsm_mvcc_layout -- \
  --sample-size 10 --warm-up-time 0.03 --measurement-time 0.06 --noplot
```

## Frozen follow-on implications

The selected internal key is `InternalKeyV1` with bytewise `ComparatorV1`,
namespace-prefixed prefix-free identity framing, and descending big-endian
timestamps. The full namespace table and the initial LSM runtime defaults are
in [storage-format.md](../storage-format.md).

Candidate B adds atomic work at Stage 4.3: every family and metadata edit in a
single tablet command shares one validation and publication result. It does
not add independent recovery or file lineages. Stage 4.4+ retains one
reactor-owned, lazily allocated bounded mutable arena per active tablet, while
immutable flush, block cache, I/O, compaction concurrency, and workers draw
from the node-shared governor/services. Long-lived reads pin one coherent
generation so compaction cannot remove files in use.

The next storage stages still need real SST block/footer and MANIFEST byte
formats, checked bounds, sync ordering, fault injection, reopen/recovery
verification, and real workload measurements. No production SSTable or
MANIFEST writer was added as part of Stage 4.2.
