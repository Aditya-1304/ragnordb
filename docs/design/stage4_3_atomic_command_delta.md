# Stage 4.3: in-memory atomic CommandDelta

Status: **Implementation is in place; Stage 4.3 remains open.** The
cross-table lock-primary validation regression and the lifecycle stress test's
per-statement deadline setup are fixed and covered by focused tests. This stage
provides the owner-local tablet publication boundary needed by the Candidate B
layout. It does not implement a durable LSM, WAL, memtables, SSTables, MANIFEST
publication, or filesystem crash recovery.

## Contract

One committed Raft entry produces one `CommandDelta` for the tablet-replica
lifetime `(tablet_id, raft_group_id, replica_id)`. The delta contains:

- MVCC Default, Write, and Lock record edits; rollback witnesses are Write
  records and are published with intent removal;
- primary transaction-status edits;
- route-independent logical retry outcomes and compatibility outcomes;
- monotonic retry-floor edits; and
- the exact processed Raft index and term.

The `CommandDelta` type and its identity, duplicate-edit, record, size, retry
floor, and contiguous-frontier validation live in
`crates/ragnordb-storage/src/lsm/command_delta.rs`. `ValueRecordV1` provides
explicit bounded V1 value encodings for the implemented record families.
Their golden-byte and malformed-input tests freeze those in-memory codec
contracts as a basis for later durable readers.

A Lock key must belong to the tablet's table. Its `LockRecord.primary_key`
must decode as a canonical row key but may name a different table: a
cross-table participant's intent points back to the transaction's primary row
so it can find the authoritative primary status.

`TabletStateMachine` prepares changes before publication. Its reference
`InMemoryTabletStateBackend` validates the complete delta, checks that record
families agree (including an existing Lock's start timestamp and required
lock transition), then publishes MVCC edits and prevalidated metadata updates
under exclusive tablet ownership. No fallible operation follows the first
visible mutation. A fatal prepare/validation error publishes neither command
state nor frontier. A deterministic command rejection publishes the cached
rejection and consumed frontier together.

The production Raft apply bridge passes each committed entry's exact index and
term into this path. Configuration entries advance the storage frontier with
a frontier-only delta. A committed command batch stages subcommand edits in a
private overlay: deterministic business rejections remain individual results,
while a fatal later subcommand discards the whole staged batch. Successful
batch state and the shared entry frontier publish once.

Proposal admission computes checked upper bounds for a complete command or
batch and rejects values above 64 MiB before Raft proposal. The committed
apply path still validates actual deltas and fails closed if state cannot be
published. The estimate is an in-memory bound, not a durable encoding size or
a reservation from a future node-wide memory governor.

Readers pin one complete tablet generation containing MVCC, statuses,
deduplication outcomes, retry floors, storage identity, and processed
frontier. Local snapshot creation checks that the requested replica and exact
index/term match that pinned generation before encoding the snapshot. Raft
proposal waiters resolve only after side effects and both storage and Raft
applied frontiers advance; the resolver rejects a command outcome beyond its
supplied frontier or at the same index with a different term.

## Validation evidence

The correctness tests cover the boundaries that would otherwise permit
partial state or replay errors:

- invalid late edits and duplicate edits are rejected before backend mutation;
- dangling Write/Default/Lock relationships and mismatched Lock transitions
  are rejected;
- deterministic rejection and mixed success/rejection batches retain one
  frontier, while a fatal later command leaves the whole batch unpublished;
- replayed commands preserve the same logical result without reapplying row
  mutations;
- a pinned reader retains a coherent generation; and
- snapshot creation rejects replica or frontier metadata that does not match
  the pinned generation.

Named regression coverage includes
`backend_rejects_lock_transition_without_matching_start_timestamp_or_removal`,
`fatal_later_batch_subcommand_discards_earlier_prewrite_and_frontier`,
`mixed_batch_publishes_success_rejection_success_at_one_frontier`,
`command_result_waiter_resolves_only_after_frontier_gate`, and
`local_snapshot_rejects_boundary_or_replica_mismatch_with_pinned_generation`.
The cross-family routing contract is covered by
`secondary_table_lock_may_reference_primary_row_in_another_table`; the server
integration cases
`three_node_runtime_admits_concurrent_barriers_and_replicates_sql_commit` and
`repeated_single_shard_commits_do_not_exhaust_lifecycle_registry` exercise the
production paths that exposed the regressions.

## Boundaries retained for later stages

Candidate B remains the Stage 4.2 decision: Default, Write, and Lock are
families beneath one tablet-replica lifetime. This stage uses an in-memory
reference backend; atomicity is provided by serialized owner-local mutation
and prevalidation, not durable multi-family storage.

Range-tombstone, secondary-index, and unique-claim edits are reserved by the
format but are not part of this delta yet. They must join this same complete
publication boundary when implemented. Storage workers, lazy mutable roots,
immutable generations, WAL records, SST blocks, MANIFEST generations, and
crash/reopen proof also remain later-stage work. Snapshot-file checksums and
Raft WAL recovery do not prove durable atomic publication of a future LSM
`CommandDelta`.
