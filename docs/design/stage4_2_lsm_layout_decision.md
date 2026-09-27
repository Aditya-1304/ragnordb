# Stage 4.2: LSM layout decision

Status: **Stage 4.2 closed; Candidate B selected for V1**. The corrected
benchmark invalidated the earlier single-profile rationale, so this decision
uses the full five-profile A/B matrix, scan matrix, physical-family ownership
requirements, and explicit trade-offs. Stage 4.3 is the next ordered stage.

## Decision

Select **Candidate B: separate logical Default, Write, and Lock trees inside
one tablet-replica storage lifetime**. The physical identity is
`(tablet_id, raft_group_id, replica_id)`. All families share one recovery
frontier, MANIFEST lineage, coherent generation, and atomic `CommandDelta`
publication boundary. Node-level memory, block cache, I/O, compaction
scheduling, and background workers remain shared. Candidate B is frozen for
V1; this decision does not mean three independent database instances.

The V1 comparator, namespace IDs, key bytes, operating defaults, and recovery
direction are specified in [storage-format.md](../storage-format.md). The
production codec is `crates/ragnordb-storage/src/lsm/internal_key.rs`.

## Candidates compared

| Candidate | Ordered storage arrangement | Expected strength | Main cost |
|---|---|---|---|
| A — unified row-first | One ordered tree, sorted by logical row, record namespace, then version | Strong result in the normal sequential profile; one tree makes publication simpler | Its relative result degrades on retained deep random history, large rows, lock-heavy, and write-churn profiles; families cannot be tuned independently |
| B — separate families | Distinct Default, Write, and Lock trees under one tablet storage generation; benchmark metadata used a shared metadata map | Wins most full-matrix core comparisons and isolates payload, history, and intent access patterns for later family-specific tuning | More tree/run coordination; cross-family publication must stay atomic |
| C — unified family-first | One ordered tree sorted by family, then row, then version | Family locality improves long scans and cache/block selectivity | One shared tree does not provide independent family roots; normal-profile latest, lock, and commit estimates trail B |

Candidate B does **not** mean three database instances. The namespace map also
reserves Metadata and Index logical families beneath the same tablet-replica
lifetime. Rollback witnesses remain in Write; range tombstones are colocated
in Write. No namespace owns an independent MANIFEST or recovery authority.

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

Criterion point estimates use 10 samples per operation. They are short
in-memory-model timings, not production latency SLOs. Candidate B uses exact
`InternalKeyV1` bytes, and the batch commit setup restores 32 existing locks
before each timed operation.

### Core operations across all five 100k-row profiles

The core set is latest/middle/old point reads, point miss, lock hit/miss,
rollback-witness lookup, prewrite validation, and batch-32 intent commit. This
gives 40 A/B point-estimate comparisons: B wins 25 and A wins 15. Taking an
equal-weight geometric mean of `B latency / A latency` gives `0.8833`, or
about 11.7% lower latency for B across this comparison set. This is a
descriptive sanity check; it assigns equal weight to every operation and
profile and is not a workload-frequency model.

| 100k-row profile | A wins | B wins | Geometric mean B/A | Readout |
|---|---:|---:|---:|---|
| Normal sequential, 4 versions | 7 | 1 | 1.198 | B latency is about 19.8% higher than A |
| History-heavy random, 16 versions | 0 | 8 | 0.660 | B is about 34% faster |
| Large rows, 1 KiB payload | 1 | 7 | 0.749 | B is about 25% faster |
| Lock-heavy, 10% locks | 3 | 5 | 0.972 | Near tie; B is about 3% faster |
| Write churn, 8 versions | 4 | 4 | 0.933 | B is about 7% faster |
| **All five profiles** | **15** | **25** | **0.883** | **B is about 11.7% faster overall** |

Against Candidate C on the same 40 core comparisons, B wins 37 and C wins 3;
the equal-weight geometric mean of `C latency / B latency` is `1.662`. This
puts C about 66% slower than B across that comparison set.

The normal sequential profile is a real trade-off: A wins seven of eight
operations there. That profile alone does not represent the complete matrix.
The 16-version history profile is especially relevant when retained MVCC
history is deep;
B wins all eight core comparisons:

| History-heavy operation | A | B |
|---|---:|---:|
| Latest read | 0.583 µs | 0.491 µs |
| Middle read | 0.708 µs | 0.544 µs |
| Old read | 0.747 µs | 0.400 µs |
| Point miss | 0.729 µs | 0.544 µs |
| Lock hit/miss pair | 0.747 µs | 0.360 µs |
| Rollback-witness lookup | 0.170 µs | 0.133 µs |
| Prewrite validation | 0.190 µs | 0.127 µs |
| Intent commit, batch 32 | 173.430 µs | 96.287 µs |

In the large-row profile, B wins latest, old, miss, lock, rollback, prewrite,
and commit; A wins middle-history lookup. Across lock-heavy and write-churn
profiles B remains near parity or ahead on this equal-weight comparison.

### Normal sequential profile (reference, not the selection basis)

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

### Scan operations across all five 100k-row profiles

The scan set is 10/100/1,000-row latest and old scans, for 30 A/B comparisons.
B wins 16 and A wins 14. The equal-weight geometric mean of `B latency / A
latency` is `0.9275`, about 7.3% lower latency for B. Per-profile, history-heavy
scans favor B strongly, large-row and lock-heavy scans favor A, normal scans
are effectively tied, and write-churn scans slightly favor B. This matrix
shows no broad scan advantage for A.

Against C on those same 30 scans, B wins 18 and C wins 12; the equal-weight
geometric mean of `C latency / B latency` is `1.093`, about 9.3% higher latency
for C. C's million-row modeled cold-byte result remains a scan-specific
advantage, not a matrix-wide scan-latency win.

| 100k-row profile | A wins | B wins | Geometric mean B/A | Readout |
|---|---:|---:|---:|---|
| Normal sequential | 3 | 3 | 1.002 | Effectively tied |
| History-heavy random | 0 | 6 | 0.588 | B is about 41% faster |
| Large rows | 5 | 1 | 1.122 | A is faster |
| Lock-heavy | 5 | 1 | 1.060 | A is faster |
| Write churn | 1 | 5 | 0.980 | B is slightly faster |
| **All five profiles** | **14** | **16** | **0.927** | **B is about 7.3% faster overall** |

Modeled physical evidence uses 4 KiB blocks, a 16 MiB shared cache, 4 MiB
flush accounting, four L0 runs before compaction, a 10:1 level ratio, and four
modeled levels. The four-level model is **not** the V1 level count. Compaction
write amplification is secondary evidence, not the primary reason for
selecting B: the model compacts families independently, so which family crosses
its level-capacity threshold changes the modeled result. This reflects the
per-family target policy being considered, but real SST overlap and compaction
have not been implemented or measured.

| Profile | A WA | B WA | Readout |
|---|---:|---:|---|
| Normal sequential | 1.310x | 1.310x | Equal |
| History-heavy random | 2.241x | 1.293x | B lower in this modeled state |
| Large rows | 1.894x | 1.849x | B slightly lower |
| Lock-heavy | 1.096x | 1.096x | Equal |
| Write churn | 1.870x | 1.855x | B slightly lower |
| Million-row scan profile | 2.488x | 2.176x | B lower in this modeled state |

For the million-row profile, modeled 10k-row cold scan bytes are 9.24 MB for A,
9.25 MB for B, and 6.81 MB for C; cold block misses are 2,308, 2,330, and
1,668. A and B are effectively equal on these modeled cold bytes, while C has
a scan-locality advantage that does not decide the broader layout choice.

### Why Candidate B is selected

B wins most core comparisons across the full workload matrix and is materially
better as MVCC history deepens and row payloads grow. The five-profile totals
and history-heavy results are the primary performance evidence. The families
also have distinct roles and access patterns:

- Default holds row payloads and can eventually use value-oriented block,
  compression, and cache policies.
- Write holds compact commit metadata and rollback witnesses, with history
  traversal and filtering needs.
- Lock holds the live intent set and is latency-sensitive.

The separate ordered family roots preserve those future tuning choices. For
example, later measurements may justify larger value-oriented Default blocks,
smaller metadata-oriented Write blocks with stronger point filters, and
higher-priority cache admission for the live Lock set. These are future tuning
hypotheses, not V1 settings frozen by this benchmark. The normal-profile A
advantage is acknowledged and accepted against B's full-matrix result and the
retained-history/large-row profiles. The single-tree publication simplicity of
A is an implementation advantage; B's extra coordination is contained by one
coherent generation and the all-family publication contract in Stage 4.3.

B can require a Write lookup followed by a Default lookup for large values. A
future, separately benchmarked short-value-in-Write optimization may avoid the
second lookup for small payloads. This is not part of Stage 4.2 and must have a
versioned value envelope, defined delete/rollback and recovery semantics, and
the same atomic publication boundary before implementation.

### Atomic publication and resource ownership

Stage 4.3 must validate and publish every applicable Default, Write, Lock,
rollback, primary-status, retry outcome/floor, and processed Raft index/term
edit as one visibility boundary. It must never expose data without its retry
outcome, advance the frontier past partial data, or publish a lock removal
without the matching committed Write/Default state.

The tablet owns one storage lifetime, one generation, one frontier, and one
MANIFEST lineage. Its Default/Write/Lock roots are allocated lazily under one
bounded tablet memory budget. The node shares memory governance, block cache,
I/O scheduling, compaction scheduling, and background workers. Three eager
per-tablet database instances are out of contract.

### Candidate C

C remains unselected. It offers the best modeled million-row scan bytes, but
does not provide B's independent Default/Write/Lock roots and family-specific
tuning boundary. Across the 40 core comparisons, B wins 37 against C; across
the scan comparisons, B wins 18 of 30 and has the lower equal-weight geometric
mean latency. The scan-locality byte result is not enough to select C for this
transaction and MVCC-history workload matrix.

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
descending big-endian timestamps. Candidate B and its full namespace table
are frozen in [storage-format.md](../storage-format.md). Stage 4.3 is next;
changing the selected physical layout would require an explicit storage-format
migration design before any durable SST writer is built.

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
