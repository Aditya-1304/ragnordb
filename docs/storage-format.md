# Tablet-local LSM storage format

Status: Stage 4.2 V1 logical key and physical-design decisions frozen. This
document does not claim that SSTables, MANIFEST publication, memtables, or
durable `CommandDelta` are implemented. Those arrive in the ordered later
stages, with real-file crash/reopen validation before the engine is enabled.

## Ownership and physical layout

The selected layout is Candidate B: separate logical Default, Write, and Lock
trees inside one tablet-replica storage lifetime. A lifetime is identified by
`(tablet_id, raft_group_id, replica_id)`; it has one MANIFEST lineage, one
recovery frontier, one atomic publication boundary, and one selected serving
generation. A node-local registry may locate these lifetimes but does not merge
their recovery positions.

The V1 logical namespace mapping is:

| Namespace | ID | Physical family | Identity and version |
|---|---:|---|---|
| Default/value | `0x10` | Default | Canonical row key + descending `start_ts` |
| Write | `0x11` | Write | Canonical row key + descending write timestamp |
| Lock/intent | `0x12` | Lock | Canonical row key, no timestamp |
| Range tombstone | `0x13` | Write | Canonical start row key + descending delete timestamp; exclusive end is in the value |
| Transaction primary status | `0x20` | Metadata | Non-zero `TxnId`, big-endian |
| Retry/dedup outcome | `0x21` | Metadata | `LogicalCommandId` fields in fixed, big-endian order |
| Retry floor | `0x22` | Metadata | Non-zero client ID (`u128`) + session epoch (`u64`), big-endian |
| Secondary index | `0x30` | Index | Table ID + index ID + framed encoded index key + framed canonical row key + descending MVCC timestamp |
| Unique claim | `0x31` | Index | Table ID + index ID + framed encoded unique value + descending MVCC timestamp |

Rollback witnesses are Write records, not a separate namespace or tree. Their
key uses the transaction `start_ts`; the Write value tag identifies
`Rollback`. Committed Put/Delete records use `commit_ts`. This preserves the
existing MVCC rollback model while making rollback lookup part of the Write
family. Range tombstones share that family; their value stores a canonical
exclusive end key and their logical range is `[start, end)`.

Metadata and Index are logical families under the same tablet-local LSM
lineage. They do not create independent database instances, WALs, MANIFESTs,
recovery frontiers, or publication boundaries. The Stage 4.2 benchmark varies
the three row families; its synthetic metadata/index records are not treated as
evidence for their future payload workload. The benchmark does not model range
tombstone reads or compaction retention; those still need correctness and
real-segment validation in later stages.

The existing canonical row key remains unchanged:

```text
[row namespace: u8 = 0x01]
[table ID: u64 big-endian]
[existing memcomparable primary-key tuple]
```

Internal keys frame that complete row key as a component. They do not rewrite
the row namespace, table ID, or primary-key encoding in `key.rs`.

## ComparatorV1 and InternalKeyV1

`ComparatorV1` is unsigned bytewise lexicographic ordering over the exact
`InternalKeyV1` byte string. No locale, Rust object ordering, native-endian
integer comparison, enum memory layout, or serde representation participates.
Its stable metadata identity is the byte string
`ragnordb.internal-key.bytewise.v1`.

The encoded key is:

```text
[key format: 0x01]
[record namespace: one explicitly assigned byte]
[prefix-free logical identity]
[optional descending timestamp: 8 bytes, big-endian]
```

The logical-identity framing is:

```text
ordinary byte  -> that byte
0x00           -> 0x00 0xff
component end  -> 0x00 0x00
```

This framing preserves the logical byte ordering and prevents a key that is a
prefix of another key from consuming the following timestamp or namespace
bytes. The V1 decoder accepts only `0x00 0xff` as an escaped zero and
`0x00 0x00` as the component terminator. Unknown format versions, unassigned
namespace IDs, malformed escapes, truncated components, invalid identity
shapes, and timestamp suffixes on unversioned namespaces are corruption.

Versioned namespaces append `(!timestamp).to_be_bytes()`. For a fixed logical
identity, newer timestamps therefore compare before older timestamps. The
timestamp coordinate is Default `start_ts`, committed Write `commit_ts`,
rollback witness `start_ts`, range-tombstone delete timestamp, or the selected
index record's MVCC timestamp. Lock and Metadata keys have no timestamp suffix.
Timestamp zero and `u64::MAX` are both valid codec boundaries; semantic MVCC
validation remains responsible for transaction timestamp rules.

Composite index identities use fixed-width big-endian table and index IDs,
followed by independently prefix-free encoded components. A secondary index
also carries the canonical base row key and requires its table ID to match the
identity's table ID. A unique-claim identity ends at the framed unique value;
the owner transaction and base row are stored in its value so exact lookup
remains possible.

The namespace mapping in `lsm/internal_key.rs` is explicit in both directions.
No Rust enum discriminant is serialized. Golden tests in that module freeze
exact bytes for every namespace, including the intentional Write-key identity
shared by committed writes and rollback witnesses.

## Value and record-kind boundary

InternalKeyV1 freezes key identities and ordering. Stage 4.2 does not freeze
value payload bytes. Those will use explicitly assigned
`[value-format-version][record-tag][payload]` envelopes and must not be inferred
from Rust struct layout or serde. The Write value's operation tag will
distinguish Put, Delete, and Rollback. Lock values will include the owning
transaction and intent metadata; range-tombstone values will include the
exclusive end row key. Each value tag and payload must receive exact
golden-byte tests before Stage 4.3 can publish it.

`CommandDelta` publication is one all-family visibility boundary. It must
atomically publish applicable Default, Write, Lock, RangeTombstone, Metadata,
Index, retry-floor, and processed Raft index/term changes. A state where only
some families or the applied frontier are visible is invalid. Recovery selects
one complete MANIFEST generation and its typed `RecoveryFrontier`; it never
combines independently published family generations.

## V1 LSM operating defaults

The numbers below are starting V1 operating defaults chosen for a conventional
leveled LSM. They are engineering defaults, not conclusions from the
in-memory/block model. The Stage 4.5/flush/compaction implementation must
measure them against real files, durability, cache misses, and write stalls
before production enablement. Runtime configuration may lower resource caps;
changing comparator, key framing, or namespace meanings requires a format
version change.

| Decision | V1 default | Rationale and boundary |
|---|---|---|
| Mutable memtable | One reactor-owned, single-writer arena-backed generation per tablet, with separate ordered roots for active families; allocate lazily on the first admitted write; 64 MiB shared maximum | Matches ownership-reactor serialization and keeps cold tablets near-zero. One shared generation keeps family edits and freeze ownership coherent; the node memory governor is authoritative, so the cap is not multiplied by family count or reserved for every cold tablet. |
| Large atomic apply | Compute a checked upper bound including key/value and in-memory index overhead. If a delta does not fit the remaining arena, freeze the current generation and apply to a fresh one. Reject a single delta whose upper bound exceeds 64 MiB before Raft proposal. | Avoids an unbounded large-batch allocation and does not split one command across visibility boundaries. Stage 4.3 must make this admission decision before proposal; apply may not discover the limit after committing the command. |
| Immutable queue | Up to 4 immutable generations per tablet; throttle new user proposals at 2 | Gives flush work a small bounded queue while retaining headroom to schedule storage workers. Before proposal, reserve memory for the complete delta and a future immutable slot; already accepted proposals retain that reservation through apply. |
| L0 organization | Overlap is allowed across L0 sublevels; files in one sublevel/family are non-overlapping; each flush generation forms one sublevel in every non-empty family | Preserves flush ordering without requiring a global merge at each flush. A large flush may emit adjacent non-overlapping files in that sublevel. |
| L0 compaction trigger | Schedule urgent L0 compaction at 4 sublevels; stop admitting new user writes at 12 sublevels | Four is the conventional baseline exercised by the model; 12 is a safety ceiling selected to bound read amplification. Both require real workload validation. |
| Level count and ratio | 7 total levels: L0 plus L1–L6; target size ratio 10:1 | Conventional leveled baseline with room for tablet data growth; not copied from the prototype's 4-level model. |
| L1 base target | 640 MiB; multiply each later level target by 10 | Equals ten 64 MiB target files at L1 and keeps the stated ratio interpretable. These are compaction targets, not hard file-size limits. |
| SST target | 64 MiB | Limits per-file index/filter metadata while keeping flush/compaction output large enough to amortize file operations. Split at key boundaries; never split one atomic record. |
| SST hard maximum | 256 MiB per segment | Four times the target permits bounded oversized output while giving readers a strict allocation/validation ceiling. |
| Data block target and maximum | 16 KiB target; 64 KiB hard maximum, uncompressed | A point-read/cache tradeoff starting point with a finite decode bound. The prototype's 4 KiB block was an experimental model parameter and is not promoted to V1. |
| Filter block maximum | 64 MiB decoded bytes per segment | Provides a strict allocation ceiling; filter bytes are separately charged to a shared node budget. Missing/unsupported/corrupt filter data falls back to exact lookup when surrounding segment metadata is sound. |
| Compaction | Conventional leveled compaction. Prioritize L0 debt; otherwise choose the level with the greatest normalized size debt and compact its oldest eligible range with every overlapping next-level file | Predictable bounded overlap and read amplification. Tiered/universal, value separation, learned indexes, and filter-aware picking remain later measured branches. |
| Compression | No block compression in V1 initial output | Avoids unmeasured CPU and codec compatibility costs. The block envelope reserves an explicit codec ID; enabling a codec requires a measured policy and readable old-block path before output uses it. |
| Checksums | CRC32C for every data/index/filter/metadata block and footer; verify before decoding | `crc32c` is already a workspace dependency and detects accidental corruption with one explicit algorithm ID. Checksums do not replace structural bounds validation. |
| Shared block cache | One node-budgeted cache shared across tablet lifetimes; byte-account buffers plus key/index overhead; admit blocks no larger than the configured cache budget; start with sharded LRU | No per-tablet cache stack or eager allocation. Misses go through the node storage service; reactors never perform filesystem I/O. Clock-style admission remains a later concurrency benchmark. |
| Range tombstones | Write-family records ordered by start key and descending delete timestamp; value contains canonical exclusive end key; semantics are half-open `[start,end)` | Keeps range deletion in the atomic Write publication domain. Compaction must retain tombstones until no protected/read timestamp can observe covered older values. |
| Write stalls | Soft throttle at 2 immutable generations or 4 L0 sublevels; hard pause user-write admission at 4 immutable generations or 12 L0 sublevels | Bounds per-tablet debt and gives the shared scheduler a clear pressure signal. Control-plane/consensus recovery work retains reserved admission capacity. |
| Disk reserve | Preserve `max(5% of storage-volume capacity, 1 GiB)` as node-wide emergency free space | Leaves room for recovery metadata and bounded maintenance. Stop new user-write admission before crossing it; never start a compaction whose worst-case output cannot fit without consuming protected reserve. |

The node storage runtime owns the shared memory governor, block-cache budget,
I/O scheduler/rate limiter, compaction concurrency, and background worker pool.
Per-tablet mutable state, MANIFEST/SST membership, and recovery frontier remain
tablet-replica local. An ownership reactor never blocks on filesystem I/O;
storage workers receive a pinned coherent generation for cache-miss reads,
flush, and compaction. Proposal admission reserves enough accounted memory for
all already accepted commands so a later L0/queue threshold cannot leave a
committed Raft command without bounded apply capacity.

## Segment and recovery direction

The later V1 segment contains independently bounded data blocks, an index
block, an optional filter block, a metadata index, and a footer. The footer and
manifest record format/comparator identity, file identity, key and timestamp
bounds, checksum descriptors, and compatibility version. The segment size and
filter decode limits must be checked before allocating from disk. A missing,
unsupported, oversized, or filter-only-corrupt filter falls back to exact
lookup; corruption that makes file boundaries or key bounds untrustworthy
rejects the segment.

One versioned MANIFEST lineage belongs to each
`(tablet_id, raft_group_id, replica_id)` lifetime. It selects a complete segment
set and one typed recovery frontier. Replicated recovery is authoritative at
the group's applied Raft index/term; the local A-WAL LSN mapping is separate
retention metadata and cannot be compared with another group's Raft progress.
MANIFEST generation publication follows durable segment publication. A
compaction publishes output and its MANIFEST edit before deleting inputs, so
crash recovery selects either the old complete set or the new complete set.

Stage 4.2 does not define or implement the byte offsets for segment headers,
block indexes, footer, or MANIFEST records. Those exact encodings must be
specified with checked bounds and golden bytes before Stage 4.5 writes durable
SSTables. Real SST, crash, reopen, snapshot, and compaction evidence is a later
gate; this document is not that evidence.
