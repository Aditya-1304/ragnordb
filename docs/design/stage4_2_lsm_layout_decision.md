# Stage 4.2: LSM layout decision

Status: **benchmark corrected; layout decision gate reopened**. Candidate B
was selected from the earlier exploratory run, but that run used non-V1 B keys
and did not repeatedly commit existing intents. The clean corrected run below
does not confirm B as the broad OLTP winner. Keep Stage 4.2 open until the
Candidate A/B trade-off is reconciled; Stage 4.3 must not start yet.

## Prior selection and current status

The previously selected format remains Candidate B: separate logical Default,
Write, and Lock trees inside one tablet-replica storage lineage. The physical
identity is `(tablet_id, raft_group_id, replica_id)`. All families share one
recovery frontier, MANIFEST lineage, coherent generation, and future atomic
`CommandDelta` publication boundary. Node-level memory, block cache, I/O,
compaction scheduling, and background workers remain shared. The corrected
results below reopen the layout decision: A leads most measured OLTP
operations, while B leads lock lookup and modeled compaction amplification.
This closure patch records the evidence and does not silently change the
already frozen production codec.

The selected V1 comparator, namespace IDs, key bytes, operating defaults, and
recovery direction are specified in [storage-format.md](../storage-format.md).
The production codec is `crates/ragnordb-storage/src/lsm/internal_key.rs`.

## Candidates compared

| Candidate | Ordered storage arrangement | Expected strength | Main cost |
|---|---|---|---|
| A — unified row-first | One ordered tree, sorted by logical row, record namespace, then version | Rows and their histories are adjacent; strongest point, prewrite, and batch-commit timings in this corrected model | Lock checks traverse/interrogate the mixed row history; one merged run carries every record kind |
| B — separate families | Distinct Default, Write, and Lock trees under one tablet storage generation; benchmark metadata used a shared metadata map | Direct lock lookup and write-history traversal; independently compactable record families; the same publication boundary can cover all trees | More tree/run coordination and cross-family publication work; corrected point, prewrite, and commit timings trail A |
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
`crates/ragnordb-bench` and remain experimental. Candidate B's logical and
modeled block paths now encode Default, Write, and Lock keys with the exact
production `InternalKeyV1` bytes; Candidate A/C key layouts and all benchmark
value encodings remain experimental.

## Measured evidence

Criterion point estimates are from the normal sequential OLTP profile. The
sample count was 10; these are short in-memory-model timings, not production
latency SLOs. The corrected B path uses exact `InternalKeyV1` bytes, and the
batch commit setup restores 32 existing locks before each timed operation.

| Operation | A | B | C | Readout |
|---|---:|---:|---:|---|
| Latest point hit | 0.514 µs | 0.729 µs | 1.031 µs | A leads; B is 41.7% slower than A |
| Middle historical read | 0.588 µs | 0.804 µs | 0.760 µs | A leads; C slightly beats B |
| Old historical read | 0.530 µs | 0.696 µs | 0.770 µs | A leads |
| Point miss | 0.698 µs | 0.876 µs | 0.759 µs | A leads; C beats B |
| Lock hit/miss pair | 0.546 µs | 0.492 µs | 0.819 µs | B leads A by about 9.9% |
| Rollback witness lookup | 0.194 µs | 0.198 µs | 0.773 µs | A and B are close; C is slower |
| Prewrite validation | 0.554 µs | 0.616 µs | 1.258 µs | A leads B by about 10.0% |
| Intent commit, batch 32 | 96.7 µs | 125.3 µs | 149.5 µs | A leads B by about 22.8%; setup restores real intents |

Modeled physical evidence uses 4 KiB blocks, a 16 MiB shared cache, 4 MiB
flush accounting, four L0 runs before compaction, 10:1 level ratio, and four
modeled levels. These are benchmark parameters; only the four-run trigger and
10:1 ratio are also selected as separate V1 operating defaults, for the
rationales in `storage-format.md`. The four-level model is **not** the V1 level
count.

| Modeled result | A | B | C |
|---|---:|---:|---:|
| 10k-row cold scan bytes, 1m-row profile | 9.24 MB | 9.25 MB | 6.81 MB |
| Cold block misses for that scan | 2,308 | 2,330 | 1,668 |
| Compaction write amplification, 1m-row profile | 2.488x | 2.176x | 2.217x |
| Compaction write amplification, 16-version history profile | 2.241x | 1.293x | 2.250x |

C has the strongest long-scan byte result: about 26% fewer cold bytes than B in
the million-row model. B has the lowest modeled compaction amplification in
both reported write-heavy comparisons, but A leads seven of the eight listed
OLTP operations; B's only lead is the lock hit/miss pair, by about 10%. B's
modeled 10k scan reads slightly more than A and has slightly more cold block
misses. The corrected evidence therefore does not establish B as the broad
OLTP winner. These model trade-offs need to be resolved before Stage 4.2 can
close; the prior B selection remains recorded but is no longer supported by
the earlier timing rationale.

## Corrected decision readout

### Candidate A versus B

The corrected run changes the comparison materially. A is faster for latest,
middle, and old reads; point misses; rollback lookup; prewrite validation; and
the real-intent batch commit. B is about 10% faster on the lock hit/miss pair
and has lower modeled write amplification, particularly on version-heavy
history. This may still justify B, but the previous claim that its transaction
path was broadly stronger is withdrawn. Candidate selection needs to weigh
the actual transaction mix against the modeled compaction benefit.

The separation remains useful only while publication is one coherent tablet
generation. Whichever layout wins, Stage 4.3 must preflight and publish every
applicable Default, Write, Lock, rollback, primary-status, retry outcome/floor,
and processed Raft index/term edit together. It must never expose new data
without its retry outcome, advance the frontier past partial data, or publish
a lock removal without the corresponding committed Write/Default state.

### Candidate C

C remains unattractive for the selected OLTP profile: it is slower than A and B
for lock checking and slower than A for every listed point/transaction
operation, though it gives the best long-scan bytes. Its modeled compaction
amplification is also slightly higher than B in the million-row profile.

## Model boundaries

The prototype compares deterministic in-memory `BTreeMap` layouts and uses a
declared model for sorted runs, fixed-size blocks, an LRU cache, flushes, and
leveled-compaction byte accounting. It does not perform SSTable or device I/O;
it does not measure `fsync`, OS page-cache behavior, real segment indexes or
Bloom filters, compression, background scheduling contention, file-open cost,
crash recovery, or cross-family durability. The measured Criterion operations
also include in-memory lookup/publication mechanics and are not end-to-end
transaction timings.

Therefore this run does not establish real-disk performance or production
readiness. The corrected raw run is preserved at
[stage4_2_layout_benchmark_2026-09-27.log](../benchmarks/stage4_2_layout_benchmark_2026-09-27.log).
It was run from clean source revision `f753714bd740525b414789ab37f57464124676a7`;
the log records SHA-256 hashes for the benchmark and production codec sources,
the compiler and host, and the command. Reproduction command:

```sh
cargo bench -p ragnordb-bench --bench lsm_mvcc_layout -- --noplot
```

## Frozen follow-on implications

The existing production key remains `InternalKeyV1` with bytewise
`ComparatorV1`, namespace-prefixed prefix-free identity framing, and
descending big-endian timestamps while Stage 4.2's layout decision is open.
The full namespace table and initial LSM runtime defaults are in
[storage-format.md](../storage-format.md). Changing the selected physical
layout now would require an explicit format/design decision before any durable
SST writer is built.

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
