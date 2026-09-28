# Stage 4.4 — lazy bounded memtables

Status: **Stage 4.4 — CLOSED.** This closes lazy bounded memtables, immutable
debt accounting, User throttling, node-budget charging, and the owner-local
nonblocking handoff boundary. The complete Stage 4.4 regression set passes.

The exact workspace all-targets and Clippy commands still report two failures
that reproduce on the unchanged parent revision: the `ragnordb-bench` layout
fixture comparator-order panic and the existing `clippy::manual_clamp` lint at
`lsm/memory.rs:138`. The workspace suite excluding that benchmark package and
Clippy with only that baseline lint allowed both pass. These baseline failures
do not exercise or touch the Stage 4.4 changes.

## Ownership

`MvccEngine` remains the shared MVCC rule layer. `InMemoryMvcc` is the simple
reference/shadow engine used by logical tests and differential comparisons.
Replicated production tablets constructed with `Tablet::new_with_memtable_budget`
use `MemtableMvcc`; snapshot restore and replica startup preserve that production
type. The node budget and mutable generations belong to one tablet-replica
storage lifetime, not to a process-wide map of records. The SQL mirror remains a
materialized reference view and is not authoritative storage.

The memtable backend starts with no active generation. A cold tablet consumes
no active-memtable charge. Its first mutation computes a checked byte bound,
obtains a reservation, and only then publishes owned records. Managed record
bytes include owned key/value bytes and the configured per-record index
allowance; this is a bounded accounting model, not an RSS measurement.

## Runtime limits

These are runtime defaults, not storage-format constants:

| Limit | V1 default |
|---|---:|
| Active mutable generation maximum | 64 MiB per tablet |
| Immutable soft throttle | 2 generations or 128 MiB |
| Immutable hard maximum | 4 generations or 256 MiB |

The byte thresholds are checked products of the active limit and the respective
count. Runtime construction rejects limits whose products overflow. Count and
byte pressure are reported separately; either soft threshold throttles ordinary
user admission, and either hard threshold prevents another freeze. The active
limit bounds a mutable generation, not the total size of a recovered tablet.

## User and progress reservations

`NodeMemtableBudget` is the shared reservation primitive. Each reservation has
one owner, an exact byte amount, and one class:

- **User** — ordinary leader proposals. User reservations and committed User
  charges cannot consume the configured progress reserve.
- **Progress** — already-committed apply, follower apply, snapshot recovery, and
  required control work. Progress reservations can use the protected headroom,
  while still respecting the node-wide total limit.

Reservation occurs before an edit is allocated or published. Publication
transfers only the actually retained amount into a shared charge; unused
reservation bytes are released. Failed admission/publication drops its
reservation. Shrink and transfer use checked arithmetic and fail without
partially changing counters. Since an ambiguous shrink cannot identify the
class of removed records, it releases Progress ownership first and keeps
remaining User bytes conservatively charged. This can throttle User admission
early but preserves capacity for committed Progress work. Charge clones share
one release state.

A leader computes the conservative complete-command delta bound and reserves
both node capacity and an immutable-slot allowance before Raft proposal. The
applier retains that lease under the accepted Raft term/index until application
or until the applied prefix proves the proposal was superseded. Outstanding
leases count against later slot admission. User proposals stop at the soft
immutable threshold, leaving the hard headroom available to already accepted
work. Followers do not run leader proposal admission; their committed apply
uses Progress capacity. Neither path treats a point-in-time pressure sample as
an admission lease.

## Complete-command generation boundary

A freeze is planned and performed by complete tablet storage publication, not
by `MvccBackend::publish_atomic` independently. One generation contains the
MVCC Default, Write, and Lock records and tombstones together with the applicable
transaction-status edits, logical and legacy outcomes, retry-floor edits,
replica storage identity, and greatest represented `RecoveryFrontier`.

The publication owner applies one complete `CommandDelta` atomically. A Raft
entry containing multiple commands is prepared as one delta and cannot be split
by rotation. The active generation freezes immediately before a complete
publication when necessary, or remains active through it; the freeze never
happens between subcommands or between row and metadata edits. Metadata-only
commands and deterministic rejections still consume generation charge and carry
their processed frontier even when they make no row-family edit.

An immutable generation is independently readable and remains charged. Older
immutables continue to contribute values, MVCC history, and locks. Newer
tombstones mask older records rather than allowing deleted values or locks to
reappear. Pinned reads and snapshot capture use one stable generation across a
rotation.

## Nonblocking immutable handoff and retirement

The immutable sink boundary is `ImmutableMemtableSink::try_submit`. It is a
nonblocking ownership offer to a future flush worker; the reactor must not wait
for worker progress. `Full`/`Closed` leave the generation in serving state and
allow later retries. A successful offer also leaves the generation in serving
state and does not reduce queue debt or release any memory.

Stage 4.4 exposes no production retirement operation. Sink acceptance alone is
not a durability proof and cannot remove a generation from serving state or
release its charge. A private `cfg(test)` helper exercises exact charge
retirement mechanics; a compile-fail doctest verifies production callers cannot
invoke it. Automatic freeze treats `Full` as retryable backpressure and records
permanent `Closed`/`Rejected` errors for inspection through
`last_flush_handoff_error()`, without failing or rolling back a valid command.
Stage 4.6 must add a typed durable-publication proof before production retirement
can be enabled.

## Restored snapshot base

Snapshot-restored records are held in a read-only, explicitly accounted base
generation. The base uses Progress-class node memory but does not consume the
64 MiB active mutable-generation limit or the immutable flush-debt count. The
active mutable generation remains absent after restore and is allocated lazily
on the first new mutation. Therefore recovery can accept a restored dataset
larger than 64 MiB when the node's bounded recovery/progress budget can hold the
transitional in-memory base. In Stages 4.5–4.7 this base is replaced by the
selected durable segment generation.

## Explicit deferrals

Stage 4.4 does **not** implement SST data/index/filter blocks, an SST filesystem
writer, Bloom integration, MANIFEST/CURRENT records, fsync/rename publication,
durable immutable retirement, compaction, MVCC garbage collection, or disk
reservations. Those remain Stage 4.5 and later work. No Stage 4.4 closure claim
reservations. Those remain Stage 4.5 and later work.

## Closure verification

The Stage 4.4 verification results on `lsm-storage` are:

| Command | Result |
|---|---|
| `cargo fmt --all --check` | Passed |
| `cargo test -p ragnordb-storage` | Passed |
| `cargo test -p ragnordb-tablet` | Passed |
| `cargo test -p ragnordb-multiraft` | Passed |
| `cargo test -p ragnordb-server` | Passed |
| `cargo test --workspace --all-targets --exclude ragnordb-bench` | Passed |
| `cargo test --workspace --all-targets` | Existing `ragnordb-bench` fixture comparator-order panic; same bench target fails on parent `c1fdcf094588a0b75e23a8895111cf78365d3882` |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Existing `clippy::manual_clamp` at `lsm/memory.rs:138`; reproduced on parent `c1fdcf094588a0b75e23a8895111cf78365d3882` |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings -A clippy::manual_clamp` | Passed; only the reproduced parent lint was allowed |
| `git diff --check` | Passed after this document update |

No Stage 4.5, 4.6, or later physical-storage feature is implemented or claimed
by this closure.
