//! deterministic apply boundary for replicated tablet commands
//!
//! Raft decides command order, while this module validates that each committed
//! envelope targets this tablet generation before dispatching its payload. The
//! state machine also owns replicated request deduplication.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::{Deref, DerefMut},
};

use ragnordb_common::{
    Error,
    codec::{TxnStatus, TxnStatusRecord, WriteKind},
    command_codec::{
        CachedTabletCommandOutcome, CachedTabletCommandRejection, CachedTabletCommandRejectionKind,
        CachedTabletCommandResult, ClientDeduplicationSnapshot, CommitCommand,
        ExpirePendingTransactionStatus, HeartbeatTransactionStatus, PrewriteCommand,
        PublishAbortedTransactionStatus, ResolveIntentCommand, RollbackCommand,
        SingleShardCommitCommand, TabletCommand, TabletCommandEnvelope, TabletCommandEnvelopeError,
        TabletStateMachineSnapshot, TabletStateMachineSnapshotError,
    },
    encoding::encode_row,
    ids::{LogicalCommandId, RaftGroupId, ReplicaId, TabletId},
};
use ragnordb_storage::{
    key::decode_row_key,
    lsm::{
        CommandDelta, CommandGenerationMetadata, LegacyOutcomeEdit, LogicalOutcomeEdit,
        MAX_COMMAND_DELTA_BYTES, MemoryReservation, RecoveryFrontier, RetryFloorEdit,
        TabletStorageIdentity, TxnStatusEdit,
    },
    mvcc::{InMemoryMvcc, Mutation, MvccDelta, MvccReadGeneration, MvccStats, MvccStorage},
};

use crate::Tablet;

/// replicated state machine owner for one tablet generation
///
/// wrapping `Tablet` keeps replication metadata out of the existing local
/// execution API. Every command must pass this boundary before a payload is
/// allowed to inspect or mutate MVCC state
///
/// Each instance belongs to one Raft group. The complete `(client, group)` key
/// prevents request sequences from being conflated when node-level diagnostics
/// or future state aggregation observes more than one group.
#[derive(Debug)]
pub struct TabletStateMachine<S = InMemoryMvcc> {
    backend: InMemoryTabletStateBackend<S>,
    epoch: u64,
    raft_group_id: RaftGroupId,
    /// Private batch overlay; it is never part of a published generation.
    staged_command_delta: Option<CommandDelta>,
    /// Capacity retained by the local leader for the committed entry now being applied.
    pending_memtable_reservation: Option<MemoryReservation>,
}

/// Reference backend that owns one complete tablet generation: MVCC records,
/// transaction status, retry outcomes/floors, and its processed Raft frontier.
#[derive(Debug)]
pub struct InMemoryTabletStateBackend<S = InMemoryMvcc> {
    tablet: Tablet<S>,
    storage_identity: TabletStorageIdentity,
    recovery_frontier: Option<RecoveryFrontier>,
    // V2 intentionally retains one outcome per client without time-based
    // eviction. Durable GC requires a protocol-level retry horizon and snapshot
    // watermark; deleting entries earlier could permit an old acknowledged
    // request to execute twice after restart.
    client_deduplication: BTreeMap<ClientDeduplicationKey, ClientDeduplicationState>,
    logical_command_deduplication: BTreeMap<LogicalCommandId, ClientDeduplicationState>,
    /// Durable V2 retry floors keyed by client session. The floor remains
    /// after outcome compaction so an acknowledged request cannot be mistaken
    /// for a new command after a restart or tablet move.
    logical_client_retry_horizons: BTreeMap<(u128, u64), u64>,
    /// Authoritative transaction decisions for transactions whose primary key
    /// is owned by this tablet. This state is replicated and snapshotted with
    /// the primary intent transitions.
    transaction_statuses: BTreeMap<ragnordb_common::ids::TxnId, TxnStatusRecord>,
}

impl<S> Deref for TabletStateMachine<S> {
    type Target = InMemoryTabletStateBackend<S>;

    fn deref(&self) -> &Self::Target {
        &self.backend
    }
}

impl<S> DerefMut for TabletStateMachine<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.backend
    }
}

/// Read contract for one complete tablet generation. All metadata accessors
/// and MVCC reads are served from the same pinned publication.
pub trait TabletReadGeneration: MvccReadGeneration + Send + Sync {
    fn storage_identity(&self) -> TabletStorageIdentity;
    fn transaction_status(&self, txn_id: ragnordb_common::ids::TxnId) -> Option<&TxnStatusRecord>;
    fn logical_outcome(&self, id: &LogicalCommandId) -> Option<&CachedTabletCommandOutcome>;
    fn legacy_outcome(&self, client_id: u128) -> Option<(u64, &CachedTabletCommandOutcome)>;
    fn retry_floor(&self, client_id: u128, session_epoch: u64) -> Option<u64>;
    fn processed_frontier(&self) -> Option<RecoveryFrontier>;
}

/// Complete publication boundary for tablet state. Persistent implementations
/// can replace the reference backend without changing command preparation.
trait TabletStateBackend {
    type PinnedGeneration: TabletReadGeneration + Send + Sync + 'static;

    fn publish_command_delta(
        &mut self,
        delta: CommandDelta,
        reservation: Option<MemoryReservation>,
    ) -> Result<(), TabletCommandApplyError>;
    fn pin_generation(&self) -> Result<Self::PinnedGeneration, TabletCommandApplyError>;
    fn recovery_frontier(&self) -> Option<RecoveryFrontier>;
}

/// Immutable complete generation retained by a reader or snapshot operation.
pub struct PinnedTabletStateGeneration {
    mvcc: Box<dyn MvccReadGeneration + Send + Sync>,
    storage_identity: TabletStorageIdentity,
    transaction_statuses: BTreeMap<ragnordb_common::ids::TxnId, TxnStatusRecord>,
    client_deduplication: BTreeMap<ClientDeduplicationKey, ClientDeduplicationState>,
    logical_command_deduplication: BTreeMap<LogicalCommandId, ClientDeduplicationState>,
    logical_client_retry_horizons: BTreeMap<(u128, u64), u64>,
    recovery_frontier: Option<RecoveryFrontier>,
}

impl<S: MvccStorage> InMemoryTabletStateBackend<S> {
    fn validate_mvcc_record_links(
        &self,
        delta: &CommandDelta,
    ) -> Result<(), TabletCommandApplyError> {
        use ragnordb_common::codec::WriteKind;
        use ragnordb_storage::mvcc::MvccRecordEdit;

        let writes = delta
            .mvcc
            .edits
            .iter()
            .filter_map(|edit| match edit {
                MvccRecordEdit::PutWrite {
                    key,
                    write_ts,
                    write,
                } => Some((key.as_slice(), *write_ts, write)),
                _ => None,
            })
            .collect::<Vec<_>>();

        let mut touched_lock_keys = BTreeSet::new();
        for edit in &delta.mvcc.edits {
            match edit {
                MvccRecordEdit::PutLock { key, .. } | MvccRecordEdit::DeleteLock { key } => {
                    touched_lock_keys.insert(key.clone());
                }
                MvccRecordEdit::PutWrite { key, .. } => {
                    touched_lock_keys.insert(key.clone());
                }
                _ => {}
            }
        }

        // An existing intent may be removed or replaced only when this same
        // delta publishes a Write for its start timestamp. Checking the
        // resulting lock edit as well prevents a direct backend caller from
        // committing a Write while leaving the old intent visible.
        for key in touched_lock_keys {
            let Some(existing_lock) = self
                .tablet
                .storage
                .get_lock_record(&key)
                .map_err(map_database_error)?
            else {
                continue;
            };
            let has_lock_transition = delta.mvcc.edits.iter().any(|edit| {
                matches!(
                    edit,
                    MvccRecordEdit::PutLock { key: edit_key, .. }
                        | MvccRecordEdit::DeleteLock { key: edit_key }
                        if edit_key == &key
                )
            });
            let resolves_existing_intent = writes.iter().any(|(write_key, _, write)| {
                *write_key == key && write.start_timestamp == existing_lock.start_timestamp
            });
            if !has_lock_transition || !resolves_existing_intent {
                return Err(TabletCommandApplyError::CorruptState {
                    reason: "existing intent transition lacks its matching Write and lock edit"
                        .into(),
                });
            }
        }

        for edit in &delta.mvcc.edits {
            match edit {
                MvccRecordEdit::DeleteLock { key }
                    if !writes.iter().any(|(write_key, _, _)| *write_key == key) =>
                {
                    return Err(TabletCommandApplyError::CorruptState {
                        reason: "lock deletion has no matching write or rollback witness".into(),
                    });
                }
                MvccRecordEdit::DeleteDefault { key, start_ts }
                    if !writes.iter().any(|(write_key, _, write)| {
                        *write_key == key
                            && write.start_timestamp == *start_ts
                            && write.op == WriteKind::Rollback
                    }) =>
                {
                    return Err(TabletCommandApplyError::CorruptState {
                        reason: "default deletion has no matching rollback witness".into(),
                    });
                }
                MvccRecordEdit::PutWrite {
                    key,
                    write_ts: _,
                    write,
                } => match write.op {
                    WriteKind::Put => {
                        if self.default_after_delta(delta, key, write.start_timestamp)?
                            .is_none()
                        {
                            return Err(TabletCommandApplyError::CorruptState {
                                reason: "committed Put has no default value in the resulting generation".into(),
                            });
                        }
                    }
                    WriteKind::Delete => {
                        if self.default_after_delta(delta, key, write.start_timestamp)?.is_some() {
                            return Err(TabletCommandApplyError::CorruptState {
                                reason: "committed Delete retains a default value".into(),
                            });
                        }
                    }
                    WriteKind::Rollback => {
                        if !delta.mvcc.edits.iter().any(|candidate| {
                            matches!(candidate, MvccRecordEdit::DeleteLock { key: lock_key } if lock_key == key)
                        }) {
                            return Err(TabletCommandApplyError::CorruptState {
                                reason: "rollback witness is not atomic with intent removal".into(),
                            });
                        }
                    }
                },
                _ => {}
            }
        }
        Ok(())
    }

    fn default_after_delta(
        &self,
        delta: &CommandDelta,
        key: &[u8],
        start_ts: ragnordb_common::ids::Timestamp,
    ) -> Result<Option<Vec<u8>>, TabletCommandApplyError> {
        use ragnordb_storage::mvcc::MvccRecordEdit;
        for edit in delta.mvcc.edits.iter().rev() {
            match edit {
                MvccRecordEdit::PutDefault {
                    key: edit_key,
                    start_ts: edit_ts,
                    row,
                } if edit_key == key && *edit_ts == start_ts => return Ok(Some(row.clone())),
                MvccRecordEdit::DeleteDefault {
                    key: edit_key,
                    start_ts: edit_ts,
                } if edit_key == key && *edit_ts == start_ts => return Ok(None),
                _ => {}
            }
        }
        self.tablet
            .storage
            .get_default_record(key, start_ts)
            .map_err(map_database_error)
    }

    fn publish_generation(
        &mut self,
        delta: CommandDelta,
        reservation: Option<MemoryReservation>,
    ) -> Result<(), TabletCommandApplyError> {
        delta
            .validate(
                self.storage_identity,
                self.recovery_frontier,
                &self.logical_client_retry_horizons,
            )
            .map_err(|error| TabletCommandApplyError::CorruptState {
                reason: format!("prepared command delta failed validation: {error}"),
            })?;
        self.validate_mvcc_record_links(&delta)?;

        let CommandDelta {
            mvcc,
            transaction_status_edits,
            logical_outcome_edits,
            legacy_outcome_edits,
            retry_floor_edits,
            frontier,
        } = delta;
        let frontier = frontier.expect("validated command delta has a frontier");
        let metadata = CommandGenerationMetadata {
            storage_identity: self.storage_identity,
            transaction_status_edits: transaction_status_edits.clone(),
            logical_outcome_edits: logical_outcome_edits.clone(),
            legacy_outcome_edits: legacy_outcome_edits.clone(),
            retry_floor_edits: retry_floor_edits.clone(),
            frontier,
        };
        let freeze_active = self
            .tablet
            .storage
            .command_generation_requires_freeze(&mvcc)
            .map_err(map_publication_error)?;

        // The tablet owner decides when the current generation rolls over.
        // MVCC receives that decision together with the command metadata, so
        // a frozen segment contains the rows and retry/status/frontier edits
        // represented by exactly the same command interval.
        self.tablet
            .storage
            .publish_command_generation_with_reservation(mvcc, metadata, freeze_active, reservation)
            .map_err(map_publication_error)?;

        for edit in transaction_status_edits {
            match edit {
                TxnStatusEdit::Put { txn_id, status } => {
                    self.transaction_statuses.insert(txn_id, status);
                }
            }
        }
        for edit in logical_outcome_edits {
            match edit {
                LogicalOutcomeEdit::Put { id, outcome } => {
                    self.logical_command_deduplication.insert(
                        id,
                        ClientDeduplicationState {
                            last_sequence_applied: 1,
                            cached_outcome: outcome,
                        },
                    );
                }
                LogicalOutcomeEdit::Delete { id } => {
                    self.logical_command_deduplication.remove(&id);
                }
            }
        }
        for edit in legacy_outcome_edits {
            match edit {
                LegacyOutcomeEdit::Put {
                    client_id,
                    last_sequence_applied,
                    outcome,
                } => {
                    self.client_deduplication.insert(
                        ClientDeduplicationKey {
                            client_id,
                            raft_group_id: self.storage_identity.raft_group_id,
                        },
                        ClientDeduplicationState {
                            last_sequence_applied,
                            cached_outcome: outcome,
                        },
                    );
                }
            }
        }
        for edit in retry_floor_edits {
            match edit {
                RetryFloorEdit::Advance {
                    client_id,
                    session_epoch,
                    acknowledged_through,
                } => {
                    self.logical_client_retry_horizons
                        .insert((client_id, session_epoch), acknowledged_through);
                }
            }
        }
        self.recovery_frontier = Some(frontier);
        Ok(())
    }

    fn pin_complete_generation(
        &self,
    ) -> Result<PinnedTabletStateGeneration, TabletCommandApplyError> {
        let mvcc = self
            .tablet
            .storage
            .pin_read_generation()
            .map_err(map_database_error)?;
        Ok(PinnedTabletStateGeneration {
            mvcc,
            storage_identity: self.storage_identity,
            transaction_statuses: self.transaction_statuses.clone(),
            client_deduplication: self.client_deduplication.clone(),
            logical_command_deduplication: self.logical_command_deduplication.clone(),
            logical_client_retry_horizons: self.logical_client_retry_horizons.clone(),
            recovery_frontier: self.recovery_frontier,
        })
    }
}

impl<S: MvccStorage> TabletStateBackend for InMemoryTabletStateBackend<S> {
    type PinnedGeneration = PinnedTabletStateGeneration;

    fn publish_command_delta(
        &mut self,
        delta: CommandDelta,
        reservation: Option<MemoryReservation>,
    ) -> Result<(), TabletCommandApplyError> {
        self.publish_generation(delta, reservation)
    }

    fn pin_generation(&self) -> Result<Self::PinnedGeneration, TabletCommandApplyError> {
        self.pin_complete_generation()
    }

    fn recovery_frontier(&self) -> Option<RecoveryFrontier> {
        self.recovery_frontier
    }
}

impl MvccReadGeneration for PinnedTabletStateGeneration {
    fn get_default(
        &self,
        key: &[u8],
        start_ts: ragnordb_common::ids::Timestamp,
    ) -> ragnordb_common::Result<Option<Vec<u8>>> {
        self.mvcc.get_default(key, start_ts)
    }

    fn get_lock(
        &self,
        key: &[u8],
    ) -> ragnordb_common::Result<Option<ragnordb_common::codec::LockRecord>> {
        self.mvcc.get_lock(key)
    }

    fn get_write(
        &self,
        key: &[u8],
        write_ts: ragnordb_common::ids::Timestamp,
    ) -> ragnordb_common::Result<Option<ragnordb_common::codec::WriteRecord>> {
        self.mvcc.get_write(key, write_ts)
    }

    fn write_page(
        &self,
        key: &[u8],
        lower: std::ops::Bound<ragnordb_common::ids::Timestamp>,
        upper: std::ops::Bound<ragnordb_common::ids::Timestamp>,
        resume_after: Option<ragnordb_common::ids::Timestamp>,
        direction: ragnordb_storage::mvcc::MvccCursorDirection,
        max_records: usize,
    ) -> ragnordb_common::Result<ragnordb_storage::mvcc::MvccWritePage> {
        self.mvcc
            .write_page(key, lower, upper, resume_after, direction, max_records)
    }

    fn key_page(
        &self,
        family: ragnordb_storage::mvcc::MvccKeyFamily,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        max_keys: usize,
    ) -> ragnordb_common::Result<ragnordb_storage::mvcc::MvccKeyPage> {
        self.mvcc
            .key_page(family, start, end, resume_after, max_keys)
    }

    fn lock_page(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        resume_after: Option<&[u8]>,
        max_locks: usize,
        max_bytes: usize,
    ) -> ragnordb_common::Result<ragnordb_storage::mvcc::IntentScanPage> {
        self.mvcc
            .lock_page(start, end, resume_after, max_locks, max_bytes)
    }

    fn recovery_frontier(&self) -> ragnordb_common::Result<Option<RecoveryFrontier>> {
        Ok(self.recovery_frontier)
    }

    fn export_snapshot(
        &self,
    ) -> ragnordb_common::Result<ragnordb_storage::checkpoint::CapturedMvccState> {
        self.mvcc.export_snapshot()
    }
}

impl TabletReadGeneration for PinnedTabletStateGeneration {
    fn storage_identity(&self) -> TabletStorageIdentity {
        self.storage_identity
    }

    fn transaction_status(&self, txn_id: ragnordb_common::ids::TxnId) -> Option<&TxnStatusRecord> {
        self.transaction_statuses.get(&txn_id)
    }

    fn logical_outcome(&self, id: &LogicalCommandId) -> Option<&CachedTabletCommandOutcome> {
        self.logical_command_deduplication
            .get(id)
            .map(|state| &state.cached_outcome)
    }

    fn legacy_outcome(&self, client_id: u128) -> Option<(u64, &CachedTabletCommandOutcome)> {
        self.client_deduplication
            .get(&ClientDeduplicationKey {
                client_id,
                raft_group_id: self.storage_identity.raft_group_id,
            })
            .map(|state| (state.last_sequence_applied, &state.cached_outcome))
    }

    fn retry_floor(&self, client_id: u128, session_epoch: u64) -> Option<u64> {
        self.logical_client_retry_horizons
            .get(&(client_id, session_epoch))
            .copied()
    }

    fn processed_frontier(&self) -> Option<RecoveryFrontier> {
        self.recovery_frontier
    }
}

impl PinnedTabletStateGeneration {
    /// Encode command metadata from this exact pinned generation for inclusion
    /// beside its exported MVCC records in a tablet snapshot.
    pub fn encode_snapshot_state(
        &self,
        tablet_id: TabletId,
        tablet_epoch: u64,
        raft_group_id: RaftGroupId,
    ) -> Result<Vec<u8>, TabletStateMachineSnapshotError> {
        let clients = self
            .client_deduplication
            .iter()
            .map(|(key, state)| {
                debug_assert_eq!(key.raft_group_id, raft_group_id);
                (
                    key.client_id,
                    ClientDeduplicationSnapshot {
                        last_sequence_applied: state.last_sequence_applied,
                        cached_outcome: state.cached_outcome.clone(),
                    },
                )
            })
            .collect();
        let logical_commands = self
            .logical_command_deduplication
            .iter()
            .map(|(id, state)| {
                (
                    *id,
                    ragnordb_common::command_codec::ClientDeduplicationSnapshot {
                        last_sequence_applied: state.last_sequence_applied,
                        cached_outcome: state.cached_outcome.clone(),
                    },
                )
            })
            .collect();

        TabletStateMachineSnapshot::new_with_logical_commands_and_horizons_and_transaction_statuses(
            tablet_id,
            tablet_epoch,
            raft_group_id,
            clients,
            logical_commands,
            self.logical_client_retry_horizons.clone(),
            self.transaction_statuses.clone(),
        )?
        .encode()
    }
}

/// On-demand counts for investigating retained state growth in one tablet.
///
/// The MVCC portion walks the current in-memory version maps. Callers should
/// sample this snapshot at diagnostic checkpoints, not on the transaction
/// apply path.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TabletStateDiagnostics {
    pub mvcc: MvccStats,
    pub legacy_cached_outcomes: usize,
    pub logical_cached_outcomes: usize,
    pub retry_floor_entries: usize,
    pub transaction_status_records: usize,
}

impl<S: MvccStorage> TabletStateMachine<S> {
    /// Create a local reference tablet bound to the reserved replica-1 identity.
    /// Replicated production replicas must use `new_with_replica` with their
    /// authoritative replica descriptor.
    pub fn new_local_reference(
        tablet: Tablet<S>,
        epoch: u64,
        raft_group_id: RaftGroupId,
    ) -> Result<Self, TabletCommandApplyError> {
        Self::new_with_replica(tablet, epoch, raft_group_id, ReplicaId(1))
    }

    /// Bind the state machine to the exact tablet replica lifetime whose Raft
    /// applied frontier will be published with each command delta.
    pub fn new_with_replica(
        tablet: Tablet<S>,
        epoch: u64,
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
    ) -> Result<Self, TabletCommandApplyError> {
        if epoch == 0 {
            return Err(TabletCommandApplyError::ZeroTabletEpoch);
        }
        if raft_group_id.0 == 0 {
            return Err(TabletCommandApplyError::ZeroRaftGroupId);
        }
        if replica_id.0 == 0 {
            return Err(TabletCommandApplyError::ZeroReplicaId);
        }

        let storage_identity = TabletStorageIdentity {
            tablet_id: tablet.id(),
            table_id: tablet.table_id(),
            raft_group_id,
            replica_id,
        };

        Ok(Self {
            backend: InMemoryTabletStateBackend {
                tablet,
                storage_identity,
                recovery_frontier: None,
                client_deduplication: BTreeMap::new(),
                logical_command_deduplication: BTreeMap::new(),
                logical_client_retry_horizons: BTreeMap::new(),
                transaction_statuses: BTreeMap::new(),
            },
            epoch,
            raft_group_id,
            staged_command_delta: None,
            pending_memtable_reservation: None,
        })
    }

    /// Capture a state-growth snapshot for an explicit diagnostics sample.
    pub fn state_diagnostics(&self) -> TabletStateDiagnostics {
        TabletStateDiagnostics {
            mvcc: self.tablet.stats(),
            legacy_cached_outcomes: self.client_deduplication.len(),
            logical_cached_outcomes: self.logical_command_deduplication.len(),
            retry_floor_entries: self.logical_client_retry_horizons.len(),
            transaction_status_records: self.transaction_statuses.len(),
        }
    }

    /// Borrow the tablet state owned by this replicated state machine.
    pub fn tablet(&self) -> &Tablet<S> {
        &self.tablet
    }

    /// return the tablet descriptor epoch represented by this state machine
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// return the Raft group that owns this tablet state machine
    pub fn raft_group_id(&self) -> RaftGroupId {
        self.raft_group_id
    }

    /// Return the complete Raft position covered by the last published tablet
    /// command generation, when this instance has applied a committed entry.
    pub fn recovery_frontier(&self) -> Option<RecoveryFrontier> {
        TabletStateBackend::recovery_frontier(&self.backend)
    }

    /// Pin MVCC records and all tablet command metadata from one published
    /// generation. The reference backend materializes owned memory state;
    /// storage backends may substitute a lightweight immutable handle.
    pub fn pin_generation(&self) -> Result<PinnedTabletStateGeneration, TabletCommandApplyError> {
        TabletStateBackend::pin_generation(&self.backend)
    }

    /// Publish progress for an applied Raft entry that carries no tablet
    /// command, such as a configuration change. Raft remains the owner of the
    /// configuration itself; the tablet generation records only that its
    /// state-machine prefix is complete through this exact entry.
    pub fn apply_frontier_only_at(
        &mut self,
        applied_index: u64,
        applied_term: u64,
    ) -> Result<(), TabletCommandApplyError> {
        if applied_index == 0 || applied_term == 0 {
            return Err(TabletCommandApplyError::InvalidRaftPosition {
                index: applied_index,
                term: applied_term,
            });
        }
        if self.staged_command_delta.is_some() {
            return Err(TabletCommandApplyError::CorruptState {
                reason: "frontier-only entry cannot publish during a staged command batch"
                    .to_string(),
            });
        }

        let frontier = RecoveryFrontier::ReplicatedTablet {
            raft_group_id: self.storage_identity.raft_group_id,
            replica_id: self.storage_identity.replica_id,
            applied_index,
            applied_term,
        };
        self.publish_command_delta(CommandDelta::frontier_only(frontier))
    }

    /// Conservatively estimate the complete sparse storage delta for one
    /// envelope before it is proposed to Raft. The bound includes staged
    /// family edits, retry outcomes/floor pruning, and ordered-index overhead.
    pub fn command_delta_upper_bound(
        &self,
        envelope: &TabletCommandEnvelope,
    ) -> Result<usize, TabletCommandApplyError> {
        let mut size = 64usize;
        match &envelope.command {
            TabletCommand::SingleShardCommit(command) => {
                for write in &command.writes {
                    if let Some(row) = &write.row {
                        add_delta_record_bound(
                            &mut size,
                            write.key.len(),
                            encoded_row_upper_bound(row)?,
                        )?;
                    }
                    add_delta_record_bound(&mut size, write.key.len(), 24)?;
                    add_delta_record_bound(&mut size, write.key.len(), 0)?;
                }
            }
            TabletCommand::Prewrite(command) => {
                for write in &command.writes {
                    if let Some(row) = &write.row {
                        add_delta_record_bound(
                            &mut size,
                            write.key.len(),
                            encoded_row_upper_bound(row)?,
                        )?;
                    }
                    add_delta_record_bound(
                        &mut size,
                        write.key.len(),
                        command
                            .primary_key
                            .len()
                            .checked_add(32)
                            .ok_or_else(|| delta_size_error("lock value byte count overflowed"))?,
                    )?;
                }
                if let Some(status) = &command.pending_status {
                    add_status_record_bound(&mut size, status)?;
                }
            }
            TabletCommand::Commit(command) => {
                for key in &command.keys {
                    add_delta_record_bound(&mut size, key.len(), 24)?;
                    add_delta_record_bound(&mut size, key.len(), 0)?;
                }
                if let Some(status) = &command.committed_status {
                    add_status_record_bound(&mut size, status)?;
                }
            }
            TabletCommand::Rollback(command) => {
                for key in &command.keys {
                    add_delta_record_bound(&mut size, key.len(), 24)?;
                    add_delta_record_bound(&mut size, key.len(), 0)?;
                    add_delta_record_bound(&mut size, key.len(), 0)?;
                }
            }
            TabletCommand::ResolveIntent(command) => {
                for key in &command.keys {
                    add_delta_record_bound(&mut size, key.len(), 24)?;
                    add_delta_record_bound(&mut size, key.len(), 0)?;
                    add_delta_record_bound(&mut size, key.len(), 0)?;
                }
            }
            TabletCommand::PublishAbortedTransactionStatus(command) => {
                add_status_record_bound(&mut size, &command.status_record)?;
            }
            TabletCommand::HeartbeatTransactionStatus(command) => {
                add_status_record_bound(&mut size, &command.next_status)?;
            }
            TabletCommand::ExpirePendingTransactionStatus(command) => {
                add_status_record_bound(&mut size, &command.expected_status)?;
            }
            TabletCommand::Noop(_) | TabletCommand::Catalog(_) => {}
        }

        // Rejected outcomes may contain a bounded diagnostic. Reserve the
        // entire format limit so admission does not depend on the eventual
        // deterministic result.
        add_delta_record_bound(&mut size, 64, 16 + 4096)?;
        if let Some(id) = envelope.logical_command_id
            && let Some(acknowledged_through) = envelope.acknowledged_through
        {
            add_delta_record_bound(&mut size, 32, 32)?;
            let session = (
                id.client_request_id.client_id,
                id.client_request_id.session_epoch,
            );
            let pruned = self
                .logical_command_deduplication
                .keys()
                .filter(|existing| {
                    let request = existing.client_request_id;
                    (request.client_id, request.session_epoch) == session
                        && request.request_sequence <= acknowledged_through
                })
                .count();
            for _ in 0..pruned {
                add_delta_record_bound(&mut size, 64, 0)?;
            }
        }

        if size > MAX_COMMAND_DELTA_BYTES {
            return Err(delta_size_error(format!(
                "estimated command delta {size} exceeds the {MAX_COMMAND_DELTA_BYTES}-byte limit"
            )));
        }
        Ok(size)
    }

    /// Bound a semantic batch by summing each envelope's conservative size.
    /// Shared records may overcount here; the pre-proposal check must err on
    /// the safe side.
    pub fn command_batch_delta_upper_bound(
        &self,
        envelopes: &[TabletCommandEnvelope],
    ) -> Result<usize, TabletCommandApplyError> {
        let mut total = 0usize;
        for envelope in envelopes {
            total = total
                .checked_add(self.command_delta_upper_bound(envelope)?)
                .ok_or_else(|| delta_size_error("command batch estimate overflowed"))?;
            if total > MAX_COMMAND_DELTA_BYTES {
                return Err(delta_size_error(format!(
                    "estimated command batch exceeds the {MAX_COMMAND_DELTA_BYTES}-byte limit"
                )));
            }
        }
        Ok(total)
    }

    /// Return the replica lifetime bound to this tablet storage instance.
    pub fn replica_id(&self) -> ReplicaId {
        self.storage_identity.replica_id
    }

    /// Read a transaction decision only after validating the durable record
    /// against its map key and status invariants.
    pub fn transaction_status(
        &self,
        txn_id: ragnordb_common::ids::TxnId,
    ) -> Result<Option<&TxnStatusRecord>, TabletCommandApplyError> {
        let Some(status) = self.transaction_statuses.get(&txn_id) else {
            return Ok(None);
        };
        status
            .validate()
            .map_err(|reason| TabletCommandApplyError::CorruptState {
                reason: format!("stored transaction status is invalid: {reason}"),
            })?;
        if status.txn_id != txn_id {
            return Err(TabletCommandApplyError::CorruptState {
                reason: format!(
                    "transaction status map key {txn_id:?} differs from record ID {:?}",
                    status.txn_id
                ),
            });
        }
        let primary_tablet_id =
            status
                .primary_tablet_id()
                .map_err(|reason| TabletCommandApplyError::CorruptState {
                    reason: format!("stored transaction status authority is invalid: {reason}"),
                })?;
        let primary_key_matches_table = decode_row_key(&status.primary_key)
            .is_ok_and(|key| key.table_id == self.tablet.table_id());
        if primary_tablet_id != self.tablet.id() || !primary_key_matches_table {
            return Err(TabletCommandApplyError::CorruptState {
                reason: format!(
                    "stored transaction status {txn_id:?} is not owned by tablet {}",
                    self.tablet.id().0
                ),
            });
        }
        Ok(Some(status))
    }

    /// return the next admissible request sequence for one client in this
    /// replicated tablet group
    ///
    /// Runtime-owned clients, such as the leader read-barrier client, must use
    /// this state after snapshot restore or WAL replay. Restarting a volatile
    /// counter at one would otherwise make the first post-restart command stale.
    pub fn next_sequence_for_client(
        &self,
        client_id: u128,
    ) -> Result<u64, TabletCommandApplyError> {
        let key = ClientDeduplicationKey {
            client_id,
            raft_group_id: self.raft_group_id,
        };

        match self.client_deduplication.get(&key) {
            Some(state) => state
                .last_sequence_applied
                .checked_add(1)
                .ok_or(TabletCommandApplyError::RequestSequenceExhausted { client_id }),
            None => Ok(1),
        }
    }

    /// Look up a retained V2 mutation outcome without routing a retry through
    /// the command executor. A missing value is not proof of non-commit; the
    /// caller must first establish that the current ownership generation has
    /// the complete retry-horizon state.
    pub fn logical_command_outcome(
        &self,
        logical_command_id: &LogicalCommandId,
    ) -> Option<&CachedTabletCommandOutcome> {
        self.logical_command_deduplication
            .get(logical_command_id)
            .map(|state| &state.cached_outcome)
    }

    /// Encode replicated command metadata that must accompany a tablet snapshot.
    ///
    /// MVCC data is intentionally owned by the surrounding tablet snapshot. This
    /// image contains the tablet generation, retry-deduplication state, and
    /// primary transaction decisions needed to interpret restored intents.
    pub fn encode_snapshot_state(&self) -> Result<Vec<u8>, TabletStateMachineSnapshotError> {
        let clients = self
            .client_deduplication
            .iter()
            .map(|(key, state)| {
                debug_assert_eq!(key.raft_group_id, self.raft_group_id);
                (
                    key.client_id,
                    ClientDeduplicationSnapshot {
                        last_sequence_applied: state.last_sequence_applied,
                        cached_outcome: state.cached_outcome.clone(),
                    },
                )
            })
            .collect();

        let logical_commands = self
            .logical_command_deduplication
            .clone()
            .into_iter()
            .map(|(logical_command_id, state)| {
                (
                    logical_command_id,
                    ragnordb_common::command_codec::ClientDeduplicationSnapshot {
                        last_sequence_applied: state.last_sequence_applied,
                        cached_outcome: state.cached_outcome,
                    },
                )
            })
            .collect();

        TabletStateMachineSnapshot::new_with_logical_commands_and_horizons_and_transaction_statuses(
            self.tablet.id(),
            self.epoch,
            self.raft_group_id,
            clients,
            logical_commands,
            self.logical_client_retry_horizons.clone(),
            self.transaction_statuses.clone(),
        )?
        .encode()
    }

    /// Restore command metadata for the explicitly local replica-1 reference.
    /// Production recovery must supply the authoritative identity through
    /// `restore_from_snapshot_with_replica`.
    pub fn restore_from_snapshot_for_local_reference(
        tablet: Tablet<S>,
        bytes: &[u8],
    ) -> Result<Self, TabletStateMachineRestoreError> {
        Self::restore_from_snapshot_with_replica(tablet, bytes, ReplicaId(1))
    }

    /// Restore command metadata under the receiving replica's local storage
    /// lifetime. Snapshot source replica identity is not reused when a tablet
    /// is installed as a replacement replica.
    pub fn restore_from_snapshot_with_replica(
        tablet: Tablet<S>,
        bytes: &[u8],
        replica_id: ReplicaId,
    ) -> Result<Self, TabletStateMachineRestoreError> {
        if replica_id.0 == 0 {
            return Err(TabletStateMachineRestoreError::InvalidSnapshot(
                TabletStateMachineSnapshotError::InvalidTxnStatus(
                    "restored tablet replica ID must be non-zero",
                ),
            ));
        }
        let snapshot = TabletStateMachineSnapshot::decode(bytes)?;
        let local_tablet_id = tablet.id();

        if snapshot.tablet_id != local_tablet_id {
            return Err(TabletStateMachineRestoreError::TabletIdMismatch {
                local_tablet_id,
                snapshot_tablet_id: snapshot.tablet_id,
            });
        }

        for status in snapshot.transaction_statuses.values() {
            let primary_row_key = decode_row_key(&status.primary_key).map_err(|_| {
                TabletStateMachineRestoreError::InvalidSnapshot(
                    TabletStateMachineSnapshotError::InvalidTxnStatus(
                        "transaction primary key is not a valid encoded row key",
                    ),
                )
            })?;
            if primary_row_key.table_id != tablet.table_id() {
                return Err(TabletStateMachineRestoreError::InvalidSnapshot(
                    TabletStateMachineSnapshotError::InvalidTxnStatus(
                        "transaction primary key belongs to a different table",
                    ),
                ));
            }
        }

        let client_deduplication = snapshot
            .clients
            .into_iter()
            .map(|(client_id, state)| {
                (
                    ClientDeduplicationKey {
                        client_id,
                        raft_group_id: snapshot.raft_group_id,
                    },
                    ClientDeduplicationState {
                        last_sequence_applied: state.last_sequence_applied,
                        cached_outcome: state.cached_outcome,
                    },
                )
            })
            .collect();

        let logical_command_deduplication = snapshot
            .logical_commands
            .into_iter()
            .map(|(logical_command_id, state)| {
                (
                    logical_command_id,
                    ClientDeduplicationState {
                        last_sequence_applied: state.last_sequence_applied,
                        cached_outcome: state.cached_outcome,
                    },
                )
            })
            .collect();

        let storage_identity = TabletStorageIdentity {
            tablet_id: tablet.id(),
            table_id: tablet.table_id(),
            raft_group_id: snapshot.raft_group_id,
            replica_id,
        };

        Ok(Self {
            backend: InMemoryTabletStateBackend {
                tablet,
                storage_identity,
                recovery_frontier: None,
                client_deduplication,
                logical_command_deduplication,
                logical_client_retry_horizons: snapshot.logical_client_retry_horizons,
                transaction_statuses: snapshot.transaction_statuses,
            },
            epoch: snapshot.tablet_epoch,
            raft_group_id: snapshot.raft_group_id,
            staged_command_delta: None,
            pending_memtable_reservation: None,
        })
    }

    /// Attach the verified snapshot's exact applied position before any suffix
    /// replay. The outer snapshot envelope owns the Raft position; this setter
    /// binds it to the restored tablet generation and local replica lifetime.
    pub fn restore_recovery_frontier(
        &mut self,
        applied_index: u64,
        applied_term: u64,
    ) -> Result<(), TabletCommandApplyError> {
        let frontier = RecoveryFrontier::ReplicatedTablet {
            raft_group_id: self.storage_identity.raft_group_id,
            replica_id: self.storage_identity.replica_id,
            applied_index,
            applied_term,
        };
        frontier
            .validate()
            .map_err(|error| TabletCommandApplyError::CorruptState {
                reason: format!("restored Raft frontier is invalid: {error}"),
            })?;
        if applied_index == 0 || applied_term == 0 {
            return Err(TabletCommandApplyError::CorruptState {
                reason: "restored tablet snapshot must have a non-zero applied frontier"
                    .to_string(),
            });
        }
        self.recovery_frontier = Some(frontier);
        Ok(())
    }

    /// Validate routing and storage-key structure before proposing a command.
    ///
    /// This boundary contains no state-dependent conflict checks. The Ready
    /// runtime can therefore call it before Raft admission, while `apply` calls
    /// it again defensively for recovered or remotely received entries.
    pub fn validate_proposal(
        &self,
        envelope: &TabletCommandEnvelope,
    ) -> Result<(), TabletCommandApplyError> {
        envelope.validate()?;

        let local_tablet_id = self.tablet.id();
        if envelope.tablet_id != local_tablet_id {
            return Err(TabletCommandApplyError::TabletIdMismatch {
                local_tablet_id,
                requested_tablet_id: envelope.tablet_id,
            });
        }
        if envelope.expected_epoch != self.epoch {
            return Err(TabletCommandApplyError::TabletEpochMismatch {
                current_epoch: self.epoch,
                expected_epoch: envelope.expected_epoch,
            });
        }
        if envelope.request_id.raft_group_id != self.raft_group_id {
            return Err(TabletCommandApplyError::RaftGroupMismatch {
                local_raft_group_id: self.raft_group_id,
                requested_raft_group_id: envelope.request_id.raft_group_id,
            });
        }

        match &envelope.command {
            TabletCommand::Prewrite(command) => {
                self.validate_encoded_key(&command.primary_key)?;
                for write in &command.writes {
                    self.validate_owned_key(&write.key)?;
                }
                if let Some(status) = &command.pending_status {
                    self.validate_status_authority(status)?;
                }
            }
            TabletCommand::SingleShardCommit(command) => {
                for write in &command.writes {
                    self.validate_owned_key(&write.key)?;
                }
            }
            TabletCommand::Commit(command) => {
                self.validate_owned_key_slice(&command.keys)?;
                if let Some(status) = &command.committed_status {
                    self.validate_status_authority(status)?;
                }
            }
            TabletCommand::Rollback(command) => {
                self.validate_owned_key_slice(&command.keys)?;
            }
            TabletCommand::ResolveIntent(command) => {
                self.validate_owned_key_slice(&command.keys)?;
            }
            TabletCommand::PublishAbortedTransactionStatus(command) => {
                self.validate_status_authority(&command.status_record)?;
            }
            TabletCommand::HeartbeatTransactionStatus(command) => {
                self.validate_status_authority(&command.expected_status)?;
                self.validate_status_authority(&command.next_status)?;
            }
            TabletCommand::ExpirePendingTransactionStatus(command) => {
                self.validate_status_authority(&command.expected_status)?;
            }
            TabletCommand::Catalog(_) | TabletCommand::Noop(_) => {}
        }

        Ok(())
    }

    /// Test-only adapter for exercising committed apply without a Raft harness.
    ///
    /// It assigns the next contiguous position from the current frontier and
    /// delegates all state mutation to `apply_committed_at`; production builds
    /// expose no position-free tablet command mutation API.
    #[cfg(test)]
    fn apply(
        &mut self,
        envelope: TabletCommandEnvelope,
    ) -> Result<TabletCommandApplyOutcome, TabletCommandApplyError> {
        let (index, term) = match self.recovery_frontier() {
            Some(RecoveryFrontier::ReplicatedTablet {
                applied_index,
                applied_term,
                ..
            }) => (applied_index.saturating_add(1), applied_term),
            _ => (1, 1),
        };
        self.apply_committed_at(envelope, index, term)
    }
    /// Apply one committed Raft command as a complete tablet storage delta.
    ///
    /// The caller passes the exact applied `(index, term)` captured from the
    /// committed proposal position. Command validation and MVCC preparation
    /// remain read-only; row edits, status, retry metadata, cached outcome, and
    /// the recovery frontier cross one owner-local publication boundary.
    ///
    /// This compile-fail example guards the API boundary: production callers
    /// cannot invoke replicated apply without supplying a committed position.
    ///
    /// ```compile_fail
    /// use ragnordb_common::{
    ///     command_codec::{NoopCommand, TabletCommand, TabletCommandEnvelope},
    ///     ids::{RaftGroupId, ReplicaId, RequestId, TableId, TabletId},
    /// };
    /// use ragnordb_tablet::{Tablet, command::TabletStateMachine};
    ///
    /// let tablet_id = TabletId(1);
    /// let group_id = RaftGroupId(1);
    /// let mut state_machine = TabletStateMachine::new_with_replica(
    ///     Tablet::new(tablet_id, TableId(1)).unwrap(),
    ///     1,
    ///     group_id,
    ///     ReplicaId(1),
    /// )
    /// .unwrap();
    /// let envelope = TabletCommandEnvelope::new(
    ///     RequestId { client_id: 1, sequence: 1, raft_group_id: group_id },
    ///     tablet_id,
    ///     1,
    ///     TabletCommand::Noop(NoopCommand),
    /// )
    /// .unwrap();
    /// let _ = state_machine.apply(envelope);
    /// ```
    pub fn apply_committed_at(
        &mut self,
        envelope: TabletCommandEnvelope,
        applied_index: u64,
        applied_term: u64,
    ) -> Result<TabletCommandApplyOutcome, TabletCommandApplyError> {
        if applied_index == 0 || applied_term == 0 {
            return Err(TabletCommandApplyError::InvalidRaftPosition {
                index: applied_index,
                term: applied_term,
            });
        }

        let frontier = RecoveryFrontier::ReplicatedTablet {
            raft_group_id: self.storage_identity.raft_group_id,
            replica_id: self.storage_identity.replica_id,
            applied_index,
            applied_term,
        };
        let mut delta = CommandDelta::frontier_only(frontier);

        if let Err(error) = self.validate_proposal(&envelope) {
            self.publish_command_delta(delta)?;
            return Err(error);
        }

        let logical_command_id = envelope.logical_command_id;
        if let Some(id) = logical_command_id {
            if let Err(error) =
                self.prepare_retry_horizon_edits(&mut delta, id, envelope.acknowledged_through)
            {
                self.publish_command_delta(delta)?;
                return Err(error);
            }
            let session = (
                id.client_request_id.client_id,
                id.client_request_id.session_epoch,
            );
            let floor = envelope
                .acknowledged_through
                .or_else(|| self.retry_floor(session));
            if let Some(acknowledged_through) = floor
                && id.client_request_id.request_sequence <= acknowledged_through
            {
                let error = TabletCommandApplyError::RequestIdExpired {
                    client_id: session.0,
                    session_epoch: session.1,
                    sequence: id.client_request_id.request_sequence,
                    acknowledged_through,
                };
                self.publish_command_delta(delta)?;
                return Err(error);
            }

            if let Some(outcome) = self.logical_outcome(&id) {
                self.publish_command_delta(delta)?;
                return outcome_result(outcome);
            }
        } else {
            let key = ClientDeduplicationKey {
                client_id: envelope.request_id.client_id,
                raft_group_id: envelope.request_id.raft_group_id,
            };
            let sequence = envelope.request_id.sequence;
            if let Some(state) = self.legacy_outcome(key.client_id) {
                if sequence == state.last_sequence_applied {
                    let outcome = state.cached_outcome.clone();
                    self.publish_command_delta(delta)?;
                    return outcome_result(outcome);
                }
                if sequence < state.last_sequence_applied {
                    let error = TabletCommandApplyError::StaleRequestSequence {
                        last_sequence_applied: state.last_sequence_applied,
                        received_sequence: sequence,
                    };
                    self.publish_command_delta(delta)?;
                    return Err(error);
                }
                let expected = state.last_sequence_applied.checked_add(1).ok_or(
                    TabletCommandApplyError::RequestSequenceExhausted {
                        client_id: envelope.request_id.client_id,
                    },
                );
                let expected = match expected {
                    Ok(expected) => expected,
                    Err(error) => {
                        self.publish_command_delta(delta)?;
                        return Err(error);
                    }
                };
                if sequence != expected {
                    let error = TabletCommandApplyError::RequestSequenceGap {
                        last_sequence_applied: state.last_sequence_applied,
                        expected_sequence: expected,
                        received_sequence: sequence,
                    };
                    self.publish_command_delta(delta)?;
                    return Err(error);
                }
            } else if sequence != 1 {
                let error = TabletCommandApplyError::RequestSequenceGap {
                    last_sequence_applied: 0,
                    expected_sequence: 1,
                    received_sequence: sequence,
                };
                self.publish_command_delta(delta)?;
                return Err(error);
            }
        }

        let prepared = match self.prepare_command(envelope.command) {
            Ok(prepared) => prepared,
            Err(error) => {
                if let Some(rejection) = cached_rejection_from_error(&error) {
                    let outcome = CachedTabletCommandOutcome::Rejected(rejection);
                    if let Some(id) = logical_command_id {
                        delta
                            .logical_outcome_edits
                            .push(LogicalOutcomeEdit::Put { id, outcome });
                    } else {
                        delta.legacy_outcome_edits.push(LegacyOutcomeEdit::Put {
                            client_id: envelope.request_id.client_id,
                            last_sequence_applied: envelope.request_id.sequence,
                            outcome,
                        });
                    }
                    self.publish_command_delta(delta)?;
                } else {
                    // Corruption and storage failures do not advance the
                    // durable frontier: the same committed command must be
                    // retried after the underlying fault is repaired.
                    return Err(error);
                }
                return Err(error);
            }
        };

        delta.mvcc = prepared.mvcc;
        delta
            .transaction_status_edits
            .extend(prepared.transaction_status_edits);
        let result = prepared.result;
        let outcome = CachedTabletCommandOutcome::Applied(result.into());
        if let Some(id) = logical_command_id {
            delta
                .logical_outcome_edits
                .push(LogicalOutcomeEdit::Put { id, outcome });
        } else {
            delta.legacy_outcome_edits.push(LegacyOutcomeEdit::Put {
                client_id: envelope.request_id.client_id,
                last_sequence_applied: envelope.request_id.sequence,
                outcome,
            });
        }

        self.publish_command_delta(delta)?;
        Ok(TabletCommandApplyOutcome::applied(result))
    }

    /// Apply one committed command using capacity retained before local
    /// proposal. Followers and recovery use `apply_committed_at` instead.
    pub fn apply_committed_at_with_reservation(
        &mut self,
        envelope: TabletCommandEnvelope,
        applied_index: u64,
        applied_term: u64,
        reservation: Option<MemoryReservation>,
    ) -> Result<TabletCommandApplyOutcome, TabletCommandApplyError> {
        if self.pending_memtable_reservation.is_some() {
            return Err(TabletCommandApplyError::CorruptState {
                reason: "a memtable reservation is already attached to tablet apply".to_string(),
            });
        }
        self.pending_memtable_reservation = reservation;
        let result = self.apply_committed_at(envelope, applied_index, applied_term);
        self.pending_memtable_reservation.take();
        result
    }

    /// Prepare all subcommands against one private sparse overlay and publish
    /// the entry's complete state once at its shared Raft position.
    pub fn apply_committed_batch_at(
        &mut self,
        envelopes: Vec<TabletCommandEnvelope>,
        applied_index: u64,
        applied_term: u64,
    ) -> Result<
        Vec<Result<TabletCommandApplyOutcome, TabletCommandApplyError>>,
        TabletCommandApplyError,
    > {
        if applied_index == 0 || applied_term == 0 {
            return Err(TabletCommandApplyError::InvalidRaftPosition {
                index: applied_index,
                term: applied_term,
            });
        }
        if self.staged_command_delta.is_some() {
            return Err(TabletCommandApplyError::CorruptState {
                reason: "nested tablet command batch publication is not supported".to_string(),
            });
        }

        let frontier = RecoveryFrontier::ReplicatedTablet {
            raft_group_id: self.storage_identity.raft_group_id,
            replica_id: self.storage_identity.replica_id,
            applied_index,
            applied_term,
        };
        self.staged_command_delta = Some(CommandDelta::frontier_only(frontier));

        let mut outcomes = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            match self.apply_committed_at(envelope, applied_index, applied_term) {
                Ok(outcome) => outcomes.push(Ok(outcome)),
                Err(error) if is_deterministic_apply_rejection(&error) => {
                    outcomes.push(Err(error));
                }
                Err(error) => {
                    self.staged_command_delta = None;
                    return Err(error);
                }
            }
        }

        let delta = self
            .staged_command_delta
            .take()
            .expect("batch staging remains installed until publication");
        self.publish_command_delta(delta)?;
        Ok(outcomes)
    }

    /// Apply one committed batch using the leader's retained reservation for
    /// its shared Raft entry.
    pub fn apply_committed_batch_at_with_reservation(
        &mut self,
        envelopes: Vec<TabletCommandEnvelope>,
        applied_index: u64,
        applied_term: u64,
        reservation: Option<MemoryReservation>,
    ) -> Result<
        Vec<Result<TabletCommandApplyOutcome, TabletCommandApplyError>>,
        TabletCommandApplyError,
    > {
        if self.pending_memtable_reservation.is_some() {
            return Err(TabletCommandApplyError::CorruptState {
                reason: "a memtable reservation is already attached to tablet apply".to_string(),
            });
        }
        self.pending_memtable_reservation = reservation;
        let result = self.apply_committed_batch_at(envelopes, applied_index, applied_term);
        self.pending_memtable_reservation.take();
        result
    }

    fn prepare_retry_horizon_edits(
        &self,
        delta: &mut CommandDelta,
        logical_command_id: LogicalCommandId,
        acknowledged_through: Option<u64>,
    ) -> Result<(), TabletCommandApplyError> {
        let Some(acknowledged_through) = acknowledged_through else {
            return Ok(());
        };
        let session = (
            logical_command_id.client_request_id.client_id,
            logical_command_id.client_request_id.session_epoch,
        );
        if let Some(previous) = self.retry_floor(session) {
            if acknowledged_through < previous {
                return Err(TabletCommandApplyError::AcknowledgementRegression {
                    client_id: session.0,
                    session_epoch: session.1,
                    existing: previous,
                    received: acknowledged_through,
                });
            }
            if acknowledged_through == previous {
                return Ok(());
            }
        }
        if acknowledged_through == 0 {
            return Ok(());
        }

        delta.retry_floor_edits.push(RetryFloorEdit::Advance {
            client_id: session.0,
            session_epoch: session.1,
            acknowledged_through,
        });
        let mut retained_ids = self
            .logical_command_deduplication
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        if let Some(staged) = &self.staged_command_delta {
            for edit in &staged.logical_outcome_edits {
                match edit {
                    LogicalOutcomeEdit::Put { id, .. } => {
                        retained_ids.insert(*id);
                    }
                    LogicalOutcomeEdit::Delete { id } => {
                        retained_ids.remove(id);
                    }
                }
            }
        }
        for id in retained_ids {
            let request = id.client_request_id;
            if (request.client_id, request.session_epoch) == session
                && request.request_sequence <= acknowledged_through
            {
                delta
                    .logical_outcome_edits
                    .push(LogicalOutcomeEdit::Delete { id });
            }
        }
        Ok(())
    }

    fn retry_floor(&self, session: (u128, u64)) -> Option<u64> {
        self.staged_command_delta
            .as_ref()
            .and_then(|delta| {
                delta
                    .retry_floor_edits
                    .iter()
                    .rev()
                    .find_map(|edit| match edit {
                        RetryFloorEdit::Advance {
                            client_id,
                            session_epoch,
                            acknowledged_through,
                        } if (*client_id, *session_epoch) == session => Some(*acknowledged_through),
                        _ => None,
                    })
            })
            .or_else(|| self.logical_client_retry_horizons.get(&session).copied())
    }

    fn logical_outcome(&self, id: &LogicalCommandId) -> Option<CachedTabletCommandOutcome> {
        if let Some(staged) = &self.staged_command_delta {
            for edit in staged.logical_outcome_edits.iter().rev() {
                match edit {
                    LogicalOutcomeEdit::Put {
                        id: edited_id,
                        outcome,
                    } if edited_id == id => return Some(outcome.clone()),
                    LogicalOutcomeEdit::Delete { id: edited_id } if edited_id == id => {
                        return None;
                    }
                    _ => {}
                }
            }
        }
        self.logical_command_deduplication
            .get(id)
            .map(|state| state.cached_outcome.clone())
    }

    fn legacy_outcome(&self, client_id: u128) -> Option<ClientDeduplicationState> {
        if let Some(staged) = &self.staged_command_delta
            && let Some(state) =
                staged
                    .legacy_outcome_edits
                    .iter()
                    .rev()
                    .find_map(|edit| match edit {
                        LegacyOutcomeEdit::Put {
                            client_id: edited_client,
                            last_sequence_applied,
                            outcome,
                        } if *edited_client == client_id => Some(ClientDeduplicationState {
                            last_sequence_applied: *last_sequence_applied,
                            cached_outcome: outcome.clone(),
                        }),
                        _ => None,
                    })
        {
            return Some(state);
        }
        self.client_deduplication
            .get(&ClientDeduplicationKey {
                client_id,
                raft_group_id: self.raft_group_id,
            })
            .cloned()
    }

    fn transaction_status_current(
        &self,
        txn_id: ragnordb_common::ids::TxnId,
    ) -> Option<TxnStatusRecord> {
        self.staged_command_delta
            .as_ref()
            .and_then(|delta| {
                delta
                    .transaction_status_edits
                    .iter()
                    .rev()
                    .find_map(|edit| match edit {
                        TxnStatusEdit::Put {
                            txn_id: edited_id,
                            status,
                        } if *edited_id == txn_id => Some(status.clone()),
                        _ => None,
                    })
            })
            .or_else(|| self.transaction_statuses.get(&txn_id).cloned())
    }

    fn publish_command_delta(
        &mut self,
        delta: CommandDelta,
    ) -> Result<(), TabletCommandApplyError> {
        if let Some(staged) = &mut self.staged_command_delta {
            merge_command_delta(staged, delta)?;
            return Ok(());
        }
        self.backend
            .publish_command_delta(delta, self.pending_memtable_reservation.take())
    }

    fn prepare_command(
        &self,
        command: TabletCommand,
    ) -> Result<PreparedTabletCommand, TabletCommandApplyError> {
        match command {
            TabletCommand::Noop(_) => Ok(PreparedTabletCommand::without_edits(
                TabletCommandApplyResult::Noop,
            )),
            TabletCommand::SingleShardCommit(command) => self.prepare_single_shard_commit(command),
            TabletCommand::Prewrite(command) => self.prepare_prewrite(command),
            TabletCommand::Commit(command) => self.prepare_commit(command),
            TabletCommand::PublishAbortedTransactionStatus(command) => {
                self.prepare_publish_aborted_transaction_status(command)
            }
            TabletCommand::HeartbeatTransactionStatus(command) => {
                self.prepare_heartbeat_transaction_status(command)
            }
            TabletCommand::ExpirePendingTransactionStatus(command) => {
                self.prepare_expire_pending_transaction_status(command)
            }
            TabletCommand::Rollback(command) => self.prepare_rollback(command),
            TabletCommand::ResolveIntent(command) => self.prepare_resolve_intent(command),
            // Catalog publication is materialized by the server catalog owner.
            // The tablet state machine still deduplicates and orders the command
            // at this exact Raft position.
            TabletCommand::Catalog(_) => Ok(PreparedTabletCommand::without_edits(
                TabletCommandApplyResult::Noop,
            )),
        }
    }

    fn prepare_single_shard_commit(
        &self,
        command: SingleShardCommitCommand,
    ) -> Result<PreparedTabletCommand, TabletCommandApplyError> {
        if command.writes.is_empty() {
            return Err(TabletCommandApplyError::InvalidCommand {
                reason: "single-shard commit requires at least one write".to_string(),
            });
        }

        let mut mutations = BTreeMap::new();

        for write in command.writes {
            self.validate_owned_key(&write.key)?;

            let mutation = mutation_from_command(write.op, write.row)?;

            if mutations.insert(write.key, mutation).is_some() {
                return Err(TabletCommandApplyError::InvalidCommand {
                    reason: "single-shard commit contains a duplicate row key".to_string(),
                });
            }
        }

        let mvcc = if let Some(staged) = &self.staged_command_delta {
            self.tablet.storage.prepare_commit_batch_with_overlay(
                &staged.mvcc,
                command.txn_id,
                command.start_timestamp,
                command.commit_timestamp,
                &mutations,
            )
        } else {
            self.tablet.storage.prepare_commit_batch(
                command.txn_id,
                command.start_timestamp,
                command.commit_timestamp,
                &mutations,
            )
        }
        .map_err(map_database_error)?;

        Ok(PreparedTabletCommand::with_mvcc(
            TabletCommandApplyResult::SingleShardCommit,
            mvcc,
        ))
    }

    fn prepare_prewrite(
        &self,
        command: PrewriteCommand,
    ) -> Result<PreparedTabletCommand, TabletCommandApplyError> {
        let txn_id = command.txn_id;
        let start_timestamp = command.start_timestamp;
        let primary_key = command.primary_key.clone();
        let pending_status = command.pending_status.clone();
        let mut mutations = BTreeMap::new();
        for write in command.writes {
            self.validate_owned_key(&write.key)?;
            let mutation = mutation_from_command(write.op, write.row)?;
            if mutations.insert(write.key, mutation).is_some() {
                return Err(TabletCommandApplyError::InvalidCommand {
                    reason: "prewrite contains a duplicate row key".to_string(),
                });
            }
        }

        let status_transition = self.validate_prewrite_status_transition(
            txn_id,
            start_timestamp,
            &primary_key,
            &mutations,
            pending_status.as_ref(),
        )?;

        let mvcc = if let Some(staged) = &self.staged_command_delta {
            self.tablet.storage.prepare_prewrite_batch_with_overlay(
                &staged.mvcc,
                command.txn_id,
                command.start_timestamp,
                &mutations,
                &command.primary_key,
                command.ttl_ms,
            )
        } else {
            self.tablet.storage.prepare_prewrite_batch(
                command.txn_id,
                command.start_timestamp,
                &mutations,
                &command.primary_key,
                command.ttl_ms,
            )
        }
        .map_err(map_database_error)?;

        let transaction_status_edits = status_transition
            .map(|status| vec![TxnStatusEdit::Put { txn_id, status }])
            .unwrap_or_default();
        Ok(PreparedTabletCommand {
            result: TabletCommandApplyResult::Prewrite,
            mvcc,
            transaction_status_edits,
        })
    }

    fn validate_owned_key(&self, key: &[u8]) -> Result<(), TabletCommandApplyError> {
        let row_key =
            decode_row_key(key).map_err(|error| TabletCommandApplyError::InvalidCommand {
                reason: format!("malformed encoded row key: {error}"),
            })?;

        if row_key.table_id != self.tablet.table_id() {
            return Err(TabletCommandApplyError::InvalidCommand {
                reason: format!(
                    "row belongs to table {}, but tablet {} owns table {}",
                    row_key.table_id.0,
                    self.tablet.id().0,
                    self.tablet.table_id().0
                ),
            });
        }

        Ok(())
    }

    fn validate_encoded_key(&self, key: &[u8]) -> Result<(), TabletCommandApplyError> {
        decode_row_key(key)
            .map(|_| ())
            .map_err(|error| TabletCommandApplyError::InvalidCommand {
                reason: format!("malformed encoded row key: {error}"),
            })
    }

    fn validate_status_authority(
        &self,
        status: &TxnStatusRecord,
    ) -> Result<(), TabletCommandApplyError> {
        status
            .validate()
            .map_err(|reason| TabletCommandApplyError::InvalidCommand {
                reason: format!("invalid transaction status record: {reason}"),
            })?;
        let primary_tablet_id = status.primary_tablet_id().map_err(|reason| {
            TabletCommandApplyError::InvalidCommand {
                reason: format!("invalid transaction status authority: {reason}"),
            }
        })?;
        if primary_tablet_id != self.tablet.id() {
            return Err(TabletCommandApplyError::InvalidCommand {
                reason: format!(
                    "transaction status belongs to primary tablet {}, but this state machine owns {}",
                    primary_tablet_id.0,
                    self.tablet.id().0
                ),
            });
        }
        self.validate_owned_key(&status.primary_key)
    }

    fn validate_owned_key_slice(&self, keys: &[Vec<u8>]) -> Result<(), TabletCommandApplyError> {
        for key in keys {
            self.validate_owned_key(key)?;
        }
        Ok(())
    }

    fn prepare_commit(
        &self,
        command: CommitCommand,
    ) -> Result<PreparedTabletCommand, TabletCommandApplyError> {
        let txn_id = command.txn_id;
        let start_timestamp = command.start_timestamp;
        let commit_timestamp = command.commit_timestamp;
        let committed_status = command.committed_status.clone();
        let keys = self.validate_owned_keys(command.keys)?;
        let status_transition = self.validate_commit_status_transition(
            txn_id,
            start_timestamp,
            commit_timestamp,
            &keys,
            committed_status.as_ref(),
        )?;

        let mvcc = if let Some(staged) = &self.staged_command_delta {
            self.tablet
                .storage
                .prepare_commit_intents_batch_with_overlay(
                    &staged.mvcc,
                    command.txn_id,
                    command.start_timestamp,
                    command.commit_timestamp,
                    &keys,
                )
        } else {
            self.tablet.storage.prepare_commit_intents_batch(
                command.txn_id,
                command.start_timestamp,
                command.commit_timestamp,
                &keys,
            )
        }
        .map_err(map_database_error)?;

        let transaction_status_edits = status_transition
            .map(|status| vec![TxnStatusEdit::Put { txn_id, status }])
            .unwrap_or_default();
        Ok(PreparedTabletCommand {
            result: TabletCommandApplyResult::Commit,
            mvcc,
            transaction_status_edits,
        })
    }

    fn prepare_publish_aborted_transaction_status(
        &self,
        command: PublishAbortedTransactionStatus,
    ) -> Result<PreparedTabletCommand, TabletCommandApplyError> {
        let status = command.status_record;
        self.validate_status_authority(&status)?;
        let should_replace = self.validate_abort_status_transition(&status)?;
        let transaction_status_edits = if should_replace {
            vec![TxnStatusEdit::Put {
                txn_id: status.txn_id,
                status,
            }]
        } else {
            Vec::new()
        };
        Ok(PreparedTabletCommand {
            result: TabletCommandApplyResult::PublishAbortedTransactionStatus,
            mvcc: MvccDelta::default(),
            transaction_status_edits,
        })
    }

    fn prepare_heartbeat_transaction_status(
        &self,
        command: HeartbeatTransactionStatus,
    ) -> Result<PreparedTabletCommand, TabletCommandApplyError> {
        command
            .validate()
            .map_err(|reason| TabletCommandApplyError::InvalidCommand {
                reason: reason.to_string(),
            })?;

        let expected = &command.expected_status;
        let Some(current) = self.transaction_status_current(expected.txn_id) else {
            return Err(TabletCommandApplyError::WriteConflict {
                reason: "transaction status disappeared before heartbeat apply".to_string(),
            });
        };
        if &current != expected {
            return Err(TabletCommandApplyError::WriteConflict {
                reason: "transaction status changed before heartbeat apply".to_string(),
            });
        }

        Ok(PreparedTabletCommand {
            result: TabletCommandApplyResult::HeartbeatTransactionStatus,
            mvcc: MvccDelta::default(),
            transaction_status_edits: vec![TxnStatusEdit::Put {
                txn_id: expected.txn_id,
                status: command.next_status,
            }],
        })
    }

    fn prepare_expire_pending_transaction_status(
        &self,
        command: ExpirePendingTransactionStatus,
    ) -> Result<PreparedTabletCommand, TabletCommandApplyError> {
        command
            .validate()
            .map_err(|reason| TabletCommandApplyError::InvalidCommand {
                reason: reason.to_string(),
            })?;

        let expected = &command.expected_status;
        let aborted = TxnStatusRecord {
            status: TxnStatus::Aborted,
            commit_timestamp: None,
            ..expected.clone()
        };
        let Some(current) = self.transaction_status_current(expected.txn_id) else {
            return Err(TabletCommandApplyError::WriteConflict {
                reason: "transaction status disappeared before expiry apply".to_string(),
            });
        };
        if current == aborted {
            return Ok(PreparedTabletCommand::without_edits(
                TabletCommandApplyResult::PublishAbortedTransactionStatus,
            ));
        }
        if &current != expected {
            return Err(TabletCommandApplyError::WriteConflict {
                reason: "transaction status changed before expiry apply".to_string(),
            });
        }

        Ok(PreparedTabletCommand {
            result: TabletCommandApplyResult::PublishAbortedTransactionStatus,
            mvcc: MvccDelta::default(),
            transaction_status_edits: vec![TxnStatusEdit::Put {
                txn_id: expected.txn_id,
                status: aborted,
            }],
        })
    }

    fn validate_prewrite_status_transition(
        &self,
        txn_id: ragnordb_common::ids::TxnId,
        start_timestamp: ragnordb_common::ids::Timestamp,
        primary_key: &[u8],
        writes: &BTreeMap<Vec<u8>, Mutation>,
        status: Option<&TxnStatusRecord>,
    ) -> Result<Option<TxnStatusRecord>, TabletCommandApplyError> {
        let existing_status = self.transaction_status_current(txn_id);
        let existing = existing_status.as_ref();
        let Some(status) = status else {
            if existing.is_some_and(|record| {
                record.primary_key.as_slice() == primary_key
                    && writes.contains_key(&record.primary_key)
            }) {
                return Err(TabletCommandApplyError::InvalidCommand {
                    reason: "primary prewrite is missing its pending status record".to_string(),
                });
            }
            return Ok(None);
        };

        self.validate_status_authority(status)?;
        if status.txn_id != txn_id || status.start_timestamp != start_timestamp {
            return Err(TabletCommandApplyError::InvalidCommand {
                reason: "pending status identity does not match prewrite".to_string(),
            });
        }
        if status.primary_key != primary_key || !writes.contains_key(&status.primary_key) {
            return Err(TabletCommandApplyError::InvalidCommand {
                reason: "pending status primary key must be included in primary prewrite"
                    .to_string(),
            });
        }

        match existing {
            None => Ok(Some(status.clone())),
            Some(existing) if existing == status => Ok(None),
            Some(existing) if !same_status_identity(existing, status) => {
                Err(TabletCommandApplyError::CorruptState {
                    reason: format!(
                        "transaction status identity changed for transaction {txn_id:?}"
                    ),
                })
            }
            Some(existing) if existing.status != TxnStatus::Pending => {
                Err(TabletCommandApplyError::InvalidCommand {
                    reason: "terminal transaction status cannot return to pending".to_string(),
                })
            }
            Some(_) => Err(TabletCommandApplyError::InvalidCommand {
                reason: "pending transaction status changed after publication".to_string(),
            }),
        }
    }

    fn validate_commit_status_transition(
        &self,
        txn_id: ragnordb_common::ids::TxnId,
        start_timestamp: ragnordb_common::ids::Timestamp,
        commit_timestamp: ragnordb_common::ids::Timestamp,
        keys: &BTreeSet<Vec<u8>>,
        status: Option<&TxnStatusRecord>,
    ) -> Result<Option<TxnStatusRecord>, TabletCommandApplyError> {
        let existing_status = self.transaction_status_current(txn_id);
        let existing = existing_status.as_ref();
        let Some(status) = status else {
            if let Some(existing) = existing
                && keys.contains(&existing.primary_key)
            {
                return Err(TabletCommandApplyError::InvalidCommand {
                    reason: "primary commit is missing its committed status record".to_string(),
                });
            }
            return Ok(None);
        };

        self.validate_status_authority(status)?;
        if status.txn_id != txn_id
            || status.start_timestamp != start_timestamp
            || status.commit_timestamp != Some(commit_timestamp)
        {
            return Err(TabletCommandApplyError::InvalidCommand {
                reason: "committed status identity does not match commit".to_string(),
            });
        }
        if !keys.contains(&status.primary_key) {
            return Err(TabletCommandApplyError::InvalidCommand {
                reason: "primary commit must include its status record primary key".to_string(),
            });
        }

        match existing {
            None => Err(TabletCommandApplyError::InvalidCommand {
                reason: "committed status requires an existing pending status".to_string(),
            }),
            Some(existing) if !same_status_identity(existing, status) => {
                Err(TabletCommandApplyError::CorruptState {
                    reason: format!(
                        "transaction status identity changed for transaction {txn_id:?}"
                    ),
                })
            }
            Some(existing) if existing.status == TxnStatus::Pending => Ok(Some(status.clone())),
            Some(existing) if existing == status => Ok(None),
            Some(existing) if existing.status == TxnStatus::Aborted => {
                Err(TabletCommandApplyError::InvalidCommand {
                    reason: "aborted transaction cannot be committed".to_string(),
                })
            }
            Some(_) => Err(TabletCommandApplyError::InvalidCommand {
                reason: "committed transaction status conflicts with the existing decision"
                    .to_string(),
            }),
        }
    }

    fn validate_abort_status_transition(
        &self,
        status: &TxnStatusRecord,
    ) -> Result<bool, TabletCommandApplyError> {
        let Some(existing) = self.transaction_status_current(status.txn_id) else {
            return Err(TabletCommandApplyError::InvalidCommand {
                reason: "aborted status requires an existing pending status".to_string(),
            });
        };
        if !same_status_identity(&existing, status) {
            return Err(TabletCommandApplyError::CorruptState {
                reason: format!(
                    "transaction status identity changed for transaction {:?}",
                    status.txn_id
                ),
            });
        }
        match existing.status {
            TxnStatus::Pending => Ok(true),
            TxnStatus::Aborted if &existing == status => Ok(false),
            TxnStatus::Aborted => Err(TabletCommandApplyError::InvalidCommand {
                reason: "aborted transaction status conflicts with the existing decision"
                    .to_string(),
            }),
            TxnStatus::Committed => Err(TabletCommandApplyError::InvalidCommand {
                reason: "committed transaction cannot be aborted".to_string(),
            }),
        }
    }

    fn prepare_rollback(
        &self,
        command: RollbackCommand,
    ) -> Result<PreparedTabletCommand, TabletCommandApplyError> {
        let keys = self.validate_owned_keys(command.keys)?;

        let mvcc = if let Some(staged) = &self.staged_command_delta {
            self.tablet
                .storage
                .prepare_rollback_intents_batch_with_overlay(
                    &staged.mvcc,
                    command.txn_id,
                    command.start_timestamp,
                    &keys,
                )
        } else {
            self.tablet.storage.prepare_rollback_intents_batch(
                command.txn_id,
                command.start_timestamp,
                &keys,
            )
        }
        .map_err(map_database_error)?;

        Ok(PreparedTabletCommand::with_mvcc(
            TabletCommandApplyResult::Rollback,
            mvcc,
        ))
    }

    fn prepare_resolve_intent(
        &self,
        command: ResolveIntentCommand,
    ) -> Result<PreparedTabletCommand, TabletCommandApplyError> {
        let keys = self.validate_owned_keys(command.keys)?;

        let mvcc = match (command.resolved_status, command.commit_timestamp) {
            (TxnStatus::Committed, Some(commit_timestamp)) => {
                if let Some(staged) = &self.staged_command_delta {
                    self.tablet
                        .storage
                        .prepare_commit_intents_batch_with_overlay(
                            &staged.mvcc,
                            command.txn_id,
                            command.start_timestamp,
                            commit_timestamp,
                            &keys,
                        )
                } else {
                    self.tablet.storage.prepare_commit_intents_batch(
                        command.txn_id,
                        command.start_timestamp,
                        commit_timestamp,
                        &keys,
                    )
                }
                .map_err(map_database_error)?
            }

            (TxnStatus::Aborted, None) => if let Some(staged) = &self.staged_command_delta {
                self.tablet
                    .storage
                    .prepare_rollback_intents_batch_with_overlay(
                        &staged.mvcc,
                        command.txn_id,
                        command.start_timestamp,
                        &keys,
                    )
            } else {
                self.tablet.storage.prepare_rollback_intents_batch(
                    command.txn_id,
                    command.start_timestamp,
                    &keys,
                )
            }
            .map_err(map_database_error)?,

            (TxnStatus::Pending, _) => {
                return Err(TabletCommandApplyError::InvalidCommand {
                    reason: "pending transaction cannot be resolved".to_string(),
                });
            }

            (TxnStatus::Committed, None) => {
                return Err(TabletCommandApplyError::InvalidCommand {
                    reason: "committed intent resolution requires commit_timestamp".to_string(),
                });
            }

            (TxnStatus::Aborted, Some(_)) => {
                return Err(TabletCommandApplyError::InvalidCommand {
                    reason: "aborted intent resolution must not contain commit_timestamp"
                        .to_string(),
                });
            }
        };

        Ok(PreparedTabletCommand::with_mvcc(
            TabletCommandApplyResult::ResolveIntent,
            mvcc,
        ))
    }

    fn validate_owned_keys(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<BTreeSet<Vec<u8>>, TabletCommandApplyError> {
        if keys.is_empty() {
            return Err(TabletCommandApplyError::InvalidCommand {
                reason: "participant command requires at least one row key".to_string(),
            });
        }
        let mut unique = BTreeSet::new();
        for key in keys {
            self.validate_owned_key(&key)?;
            if !unique.insert(key) {
                return Err(TabletCommandApplyError::InvalidCommand {
                    reason: "participant command contains a duplicate row key".to_string(),
                });
            }
        }
        Ok(unique)
    }
}

fn same_status_identity(existing: &TxnStatusRecord, next: &TxnStatusRecord) -> bool {
    existing.txn_id == next.txn_id
        && existing.start_timestamp == next.start_timestamp
        && existing.primary_key == next.primary_key
        && existing.participant_tablet_ids == next.participant_tablet_ids
}

fn delta_size_error(reason: impl Into<String>) -> TabletCommandApplyError {
    TabletCommandApplyError::InvalidCommand {
        reason: reason.into(),
    }
}

fn add_delta_record_bound(
    total: &mut usize,
    key_bytes: usize,
    value_bytes: usize,
) -> Result<(), TabletCommandApplyError> {
    let charged = key_bytes
        .checked_add(value_bytes)
        .and_then(|payload| payload.checked_mul(2))
        .and_then(|payload| payload.checked_add(128))
        .ok_or_else(|| delta_size_error("command delta record size overflowed"))?;
    *total = total
        .checked_add(charged)
        .ok_or_else(|| delta_size_error("command delta total size overflowed"))?;
    Ok(())
}

fn encoded_row_upper_bound(
    row: &ragnordb_common::codec::Row,
) -> Result<usize, TabletCommandApplyError> {
    u32::try_from(row.values.len())
        .map_err(|_| delta_size_error("row contains too many values for V1 encoding"))?;
    let mut size = 5usize;
    for value in &row.values {
        let encoded = match value {
            ragnordb_common::codec::Value::Int(_) => 9,
            ragnordb_common::codec::Value::Text(text) => 5usize
                .checked_add(text.len())
                .ok_or_else(|| delta_size_error("row text size overflowed"))?,
            ragnordb_common::codec::Value::Bool(_) => 2,
            ragnordb_common::codec::Value::Null => 1,
        };
        size = size
            .checked_add(encoded)
            .ok_or_else(|| delta_size_error("encoded row size overflowed"))?;
    }
    Ok(size)
}

fn add_status_record_bound(
    total: &mut usize,
    status: &TxnStatusRecord,
) -> Result<(), TabletCommandApplyError> {
    let participant_bytes = status
        .participant_tablet_ids
        .len()
        .checked_mul(8)
        .ok_or_else(|| delta_size_error("transaction participant size overflowed"))?;
    let value_bytes = status
        .primary_key
        .len()
        .checked_add(participant_bytes)
        .and_then(|size| size.checked_add(64))
        .ok_or_else(|| delta_size_error("transaction status size overflowed"))?;
    add_delta_record_bound(total, 8, value_bytes)
}

fn is_deterministic_apply_rejection(error: &TabletCommandApplyError) -> bool {
    matches!(
        error,
        TabletCommandApplyError::RaftGroupMismatch { .. }
            | TabletCommandApplyError::TabletIdMismatch { .. }
            | TabletCommandApplyError::TabletEpochMismatch { .. }
            | TabletCommandApplyError::StaleRequestSequence { .. }
            | TabletCommandApplyError::RequestSequenceGap { .. }
            | TabletCommandApplyError::RequestSequenceExhausted { .. }
            | TabletCommandApplyError::RequestIdExpired { .. }
            | TabletCommandApplyError::AcknowledgementRegression { .. }
            | TabletCommandApplyError::InvalidCommand { .. }
            | TabletCommandApplyError::WriteConflict { .. }
            | TabletCommandApplyError::UnsupportedCommand { .. }
    )
}

fn merge_command_delta(
    staged: &mut CommandDelta,
    mut next: CommandDelta,
) -> Result<(), TabletCommandApplyError> {
    for edit in next.mvcc.edits.drain(..) {
        let identity = mvcc_edit_identity(&edit);
        staged
            .mvcc
            .edits
            .retain(|existing| mvcc_edit_identity(existing) != identity);
        staged.mvcc.edits.push(edit);
    }

    for edit in next.transaction_status_edits {
        let txn_id = match &edit {
            TxnStatusEdit::Put { txn_id, .. } => *txn_id,
        };
        staged.transaction_status_edits.retain(|existing| {
            !matches!(existing, TxnStatusEdit::Put { txn_id: existing_id, .. } if *existing_id == txn_id)
        });
        staged.transaction_status_edits.push(edit);
    }

    for edit in next.logical_outcome_edits {
        let id = match &edit {
            LogicalOutcomeEdit::Put { id, .. } | LogicalOutcomeEdit::Delete { id } => *id,
        };
        staged
            .logical_outcome_edits
            .retain(|existing| match existing {
                LogicalOutcomeEdit::Put {
                    id: existing_id, ..
                }
                | LogicalOutcomeEdit::Delete { id: existing_id } => *existing_id != id,
            });
        staged.logical_outcome_edits.push(edit);
    }

    for edit in next.legacy_outcome_edits {
        let client_id = match &edit {
            LegacyOutcomeEdit::Put { client_id, .. } => *client_id,
        };
        staged.legacy_outcome_edits.retain(|existing| {
            !matches!(existing, LegacyOutcomeEdit::Put { client_id: existing_id, .. } if *existing_id == client_id)
        });
        staged.legacy_outcome_edits.push(edit);
    }

    for edit in next.retry_floor_edits {
        match edit {
            RetryFloorEdit::Advance {
                client_id,
                session_epoch,
                acknowledged_through,
            } => {
                let key = (client_id, session_epoch);
                if let Some(RetryFloorEdit::Advance {
                    acknowledged_through: existing,
                    ..
                }) = staged.retry_floor_edits.iter_mut().find(|existing| {
                    matches!(existing, RetryFloorEdit::Advance { client_id: existing_client, session_epoch: existing_epoch, .. } if (*existing_client, *existing_epoch) == key)
                }) {
                    *existing = (*existing).max(acknowledged_through);
                } else {
                    staged.retry_floor_edits.push(RetryFloorEdit::Advance {
                        client_id,
                        session_epoch,
                        acknowledged_through,
                    });
                }
            }
        }
    }

    if next.frontier.is_some() {
        staged.frontier = next.frontier;
    }

    let estimated = staged.encoded_size_upper_bound().map_err(|error| {
        TabletCommandApplyError::StorageFailure {
            reason: format!("staged command delta size overflowed: {error}"),
        }
    })?;
    if estimated > MAX_COMMAND_DELTA_BYTES {
        return Err(TabletCommandApplyError::StorageFailure {
            reason: format!(
                "staged command delta exceeds the {MAX_COMMAND_DELTA_BYTES}-byte limit"
            ),
        });
    }
    Ok(())
}

fn mvcc_edit_identity(edit: &ragnordb_storage::mvcc::MvccRecordEdit) -> (u8, &[u8], u64) {
    use ragnordb_storage::mvcc::MvccRecordEdit;
    match edit {
        MvccRecordEdit::PutDefault { key, start_ts, .. }
        | MvccRecordEdit::DeleteDefault { key, start_ts } => (0, key, start_ts.0),
        MvccRecordEdit::PutLock { key, .. } | MvccRecordEdit::DeleteLock { key } => (1, key, 0),
        MvccRecordEdit::PutWrite { key, write_ts, .. } => (2, key, write_ts.0),
    }
}

fn mutation_from_command(
    op: WriteKind,
    row: Option<ragnordb_common::codec::Row>,
) -> Result<Mutation, TabletCommandApplyError> {
    match (op, row) {
        (WriteKind::Put, Some(row)) => encode_row(&row)
            .map(Mutation::Put)
            .map_err(map_database_error),

        (WriteKind::Delete, None) => Ok(Mutation::Delete),

        (WriteKind::Put, None) => Err(TabletCommandApplyError::InvalidCommand {
            reason: "Put command requires a complete row".to_string(),
        }),

        (WriteKind::Delete, Some(_)) => Err(TabletCommandApplyError::InvalidCommand {
            reason: "Delete command must not contain a row".to_string(),
        }),

        (WriteKind::Rollback, _) => Err(TabletCommandApplyError::InvalidCommand {
            reason: "Rollback is not a valid write mutation payload".to_string(),
        }),
    }
}

fn map_database_error(error: Error) -> TabletCommandApplyError {
    match error {
        Error::InvalidArgument(reason) => TabletCommandApplyError::InvalidCommand { reason },

        Error::WriteConflict(reason) => TabletCommandApplyError::WriteConflict { reason },

        Error::CorruptData(reason) => TabletCommandApplyError::CorruptState { reason },

        error => TabletCommandApplyError::StorageFailure {
            reason: error.to_string(),
        },
    }
}

fn map_publication_error(error: Error) -> TabletCommandApplyError {
    match error {
        Error::CorruptData(reason) => TabletCommandApplyError::CorruptState { reason },
        error => TabletCommandApplyError::StorageFailure {
            reason: error.to_string(),
        },
    }
}

fn outcome_result(
    outcome: CachedTabletCommandOutcome,
) -> Result<TabletCommandApplyOutcome, TabletCommandApplyError> {
    match outcome {
        CachedTabletCommandOutcome::Applied(result) => {
            Ok(TabletCommandApplyOutcome::deduplicated(result.into()))
        }
        CachedTabletCommandOutcome::Rejected(rejection) => {
            Err(error_from_cached_rejection(&rejection))
        }
    }
}

fn cached_rejection_from_error(
    error: &TabletCommandApplyError,
) -> Option<CachedTabletCommandRejection> {
    let (kind, reason) = match error {
        TabletCommandApplyError::InvalidCommand { reason } => (
            CachedTabletCommandRejectionKind::InvalidCommand,
            reason.clone(),
        ),
        TabletCommandApplyError::WriteConflict { reason } => (
            CachedTabletCommandRejectionKind::WriteConflict,
            reason.clone(),
        ),
        TabletCommandApplyError::UnsupportedCommand { command } => (
            CachedTabletCommandRejectionKind::UnsupportedCommand,
            command.clone(),
        ),
        _ => return None,
    };
    Some(CachedTabletCommandRejection { kind, reason })
}

fn error_from_cached_rejection(
    rejection: &CachedTabletCommandRejection,
) -> TabletCommandApplyError {
    match rejection.kind {
        CachedTabletCommandRejectionKind::InvalidCommand => {
            TabletCommandApplyError::InvalidCommand {
                reason: rejection.reason.clone(),
            }
        }
        CachedTabletCommandRejectionKind::WriteConflict => TabletCommandApplyError::WriteConflict {
            reason: rejection.reason.clone(),
        },
        CachedTabletCommandRejectionKind::UnsupportedCommand => {
            TabletCommandApplyError::UnsupportedCommand {
                command: rejection.reason.clone(),
            }
        }
    }
}

/// last successfully applied request and result for one client in this Raft
/// group’s sequence namespace
#[derive(Debug, Clone, PartialEq, Eq)]
struct ClientDeduplicationState {
    last_sequence_applied: u64,
    cached_outcome: CachedTabletCommandOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ClientDeduplicationKey {
    client_id: u128,
    raft_group_id: RaftGroupId,
}

/// Sparse result of command validation. No public state changes until the
/// command delta containing these edits reaches its single publication point.
struct PreparedTabletCommand {
    result: TabletCommandApplyResult,
    mvcc: MvccDelta,
    transaction_status_edits: Vec<TxnStatusEdit>,
}

impl PreparedTabletCommand {
    fn without_edits(result: TabletCommandApplyResult) -> Self {
        Self {
            result,
            mvcc: MvccDelta::default(),
            transaction_status_edits: Vec::new(),
        }
    }

    fn with_mvcc(result: TabletCommandApplyResult, mvcc: MvccDelta) -> Self {
        Self {
            result,
            mvcc,
            transaction_status_edits: Vec::new(),
        }
    }
}

/// result and provenance for one state machine apply attempt
///
/// callers return `result` to the client in both cases. The `deduplicated` flag
/// lets proposal tracking and diagnostics distinguish a fresh transition from
/// an exact retry served by replicated state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TabletCommandApplyOutcome {
    pub result: TabletCommandApplyResult,
    pub deduplicated: bool,
}

impl TabletCommandApplyOutcome {
    fn applied(result: TabletCommandApplyResult) -> Self {
        Self {
            result,
            deduplicated: false,
        }
    }

    fn deduplicated(result: TabletCommandApplyResult) -> Self {
        Self {
            result,
            deduplicated: true,
        }
    }
}

/// deterministic result produced by applying a replicated tablet command
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TabletCommandApplyResult {
    ///  no-op passed target validation and intentionally changed no MVCC
    /// state. Later phases use this result for replicated barriers
    Noop,

    /// A complete single-tablet write batch committed atomically.
    SingleShardCommit,

    /// One distributed transaction intent was installed atomically.
    Prewrite,

    /// One distributed intent was committed into a visible MVCC write.
    Commit,

    /// One distributed intent was removed and protected by a rollback marker.
    Rollback,

    /// One intent was resolved according to the durable transaction status.
    ResolveIntent,

    /// One pending transaction was durably marked aborted after participant
    /// rollback completed.
    PublishAbortedTransactionStatus,

    /// One pending transaction lease was durably extended at its status tablet.
    HeartbeatTransactionStatus,
}

impl From<TabletCommandApplyResult> for CachedTabletCommandResult {
    fn from(result: TabletCommandApplyResult) -> Self {
        match result {
            TabletCommandApplyResult::Noop => Self::Noop,
            TabletCommandApplyResult::SingleShardCommit => Self::SingleShardCommit,
            TabletCommandApplyResult::Prewrite => Self::Prewrite,
            TabletCommandApplyResult::Commit => Self::Commit,
            TabletCommandApplyResult::Rollback => Self::Rollback,
            TabletCommandApplyResult::ResolveIntent => Self::ResolveIntent,
            TabletCommandApplyResult::PublishAbortedTransactionStatus => {
                Self::PublishAbortedTransactionStatus
            }
            TabletCommandApplyResult::HeartbeatTransactionStatus => {
                Self::HeartbeatTransactionStatus
            }
        }
    }
}

impl From<CachedTabletCommandResult> for TabletCommandApplyResult {
    fn from(result: CachedTabletCommandResult) -> Self {
        match result {
            CachedTabletCommandResult::Noop => Self::Noop,
            CachedTabletCommandResult::SingleShardCommit => Self::SingleShardCommit,
            CachedTabletCommandResult::Prewrite => Self::Prewrite,
            CachedTabletCommandResult::Commit => Self::Commit,
            CachedTabletCommandResult::Rollback => Self::Rollback,
            CachedTabletCommandResult::ResolveIntent => Self::ResolveIntent,
            CachedTabletCommandResult::PublishAbortedTransactionStatus => {
                Self::PublishAbortedTransactionStatus
            }
            CachedTabletCommandResult::HeartbeatTransactionStatus => {
                Self::HeartbeatTransactionStatus
            }
        }
    }
}

/// failure while restoring command metadata into a tablet state machine
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TabletStateMachineRestoreError {
    #[error(
        "tablet snapshot belongs to tablet {snapshot_tablet_id:?}, but the supplied tablet is {local_tablet_id:?}"
    )]
    TabletIdMismatch {
        local_tablet_id: TabletId,
        snapshot_tablet_id: TabletId,
    },

    #[error("invalid tablet state-machine snapshot: {0}")]
    InvalidSnapshot(#[from] TabletStateMachineSnapshotError),
}

/// deterministic rejection returned by the tablet apply boundary
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum TabletCommandApplyError {
    #[error("tablet state-machine epoch must be non-zero")]
    ZeroTabletEpoch,

    #[error("tablet state-machine Raft group ID must be non-zero")]
    ZeroRaftGroupId,

    #[error("tablet state-machine replica ID must be non-zero")]
    ZeroReplicaId,

    #[error("committed Raft position is invalid: term={term}, index={index}")]
    InvalidRaftPosition { index: u64, term: u64 },

    #[error(
        "request targets Raft group {requested_raft_group_id:?}, but this state machine owns {local_raft_group_id:?}"
    )]
    RaftGroupMismatch {
        local_raft_group_id: RaftGroupId,
        requested_raft_group_id: RaftGroupId,
    },

    #[error(
        "tablet command targets tablet {requested_tablet_id:?}, but this state machine owns {local_tablet_id:?}"
    )]
    TabletIdMismatch {
        local_tablet_id: TabletId,
        requested_tablet_id: TabletId,
    },

    #[error(
        "tablet command expects epoch {expected_epoch}, but the current tablet epoch is {current_epoch}"
    )]
    TabletEpochMismatch {
        current_epoch: u64,
        expected_epoch: u64,
    },

    #[error(
        "request sequence {received_sequence} is stale; client state has already applied sequence {last_sequence_applied}"
    )]
    StaleRequestSequence {
        last_sequence_applied: u64,
        received_sequence: u64,
    },

    #[error(
        "request sequence gap after {last_sequence_applied}: expected {expected_sequence}, received {received_sequence}"
    )]
    RequestSequenceGap {
        last_sequence_applied: u64,
        expected_sequence: u64,
        received_sequence: u64,
    },

    #[error("request sequence space is exhausted for client {client_id:#034x}")]
    RequestSequenceExhausted { client_id: u128 },

    #[error(
        "request identity client {client_id:#034x}, session {session_epoch}, sequence {sequence} expired at acknowledgement {acknowledged_through}"
    )]
    RequestIdExpired {
        client_id: u128,
        session_epoch: u64,
        sequence: u64,
        acknowledged_through: u64,
    },

    #[error(
        "request acknowledgement regressed for client {client_id:#034x}, session {session_epoch}: existing {existing}, received {received}"
    )]
    AcknowledgementRegression {
        client_id: u128,
        session_epoch: u64,
        existing: u64,
        received: u64,
    },

    #[error("invalid tablet command: {reason}")]
    InvalidCommand { reason: String },

    #[error("tablet command encountered a write conflict: {reason}")]
    WriteConflict { reason: String },

    #[error("tablet command detected corrupt MVCC state: {reason}")]
    CorruptState { reason: String },

    #[error("tablet storage could not execute the command: {reason}")]
    StorageFailure { reason: String },

    #[error("tablet command payload {command} is not implemented by this state machine")]
    UnsupportedCommand { command: String },

    #[error("invalid tablet command envelope: {0}")]
    InvalidEnvelope(#[from] TabletCommandEnvelopeError),
}

#[cfg(test)]
mod tests {
    use ragnordb_common::{
        Error,
        codec::{Row, TxnStatus, TxnStatusRecord, Value, WriteKind, WriteRecord},
        command_codec::{
            CachedTabletCommandOutcome, CachedTabletCommandRejection,
            CachedTabletCommandRejectionKind, CachedTabletCommandResult, CommitCommand,
            ExpirePendingTransactionStatus, HeartbeatTransactionStatus, NoopCommand,
            PrewriteCommand, PublishAbortedTransactionStatus, ResolveIntentCommand,
            RollbackCommand, SingleShardCommitCommand, TabletCommand, TabletCommandEnvelope,
            WriteEntry,
        },
        ids::{
            ClientRequestId, CommandKind, LogicalCommandId, RaftGroupId, ReplicaId, RequestId,
            TableId, TabletId, Timestamp, TxnId,
        },
    };
    use ragnordb_storage::mvcc::{MvccReadGeneration, MvccRecordEdit, MvccStorage};
    use ragnordb_storage::{
        key::{encode_row_key, make_row_key},
        lsm::{NodeMemtableBudget, RecoveryFrontier, RetryFloorEdit},
    };
    use ragnordb_txn::Transaction;

    use super::{
        CommandDelta, LegacyOutcomeEdit, LogicalOutcomeEdit, MvccDelta, TabletCommandApplyError,
        TabletCommandApplyOutcome, TabletCommandApplyResult, TabletReadGeneration,
        TabletStateBackend, TabletStateMachine, TabletStateMachineRestoreError, TxnStatusEdit,
    };
    use crate::Tablet;
    use ragnordb_storage::lsm::MAX_COMMAND_DELTA_BYTES;

    const LOCAL_TABLET_ID: TabletId = TabletId(41);
    const LOCAL_TABLET_EPOCH: u64 = 7;
    const LOCAL_RAFT_GROUP_ID: RaftGroupId = RaftGroupId(91);

    fn state_machine() -> TabletStateMachine {
        state_machine_for(LOCAL_TABLET_ID)
    }

    fn state_machine_for(tablet_id: TabletId) -> TabletStateMachine {
        state_machine_for_group(tablet_id, LOCAL_RAFT_GROUP_ID)
    }

    fn state_machine_for_group(
        tablet_id: TabletId,
        raft_group_id: RaftGroupId,
    ) -> TabletStateMachine {
        let tablet = Tablet::new(tablet_id, TableId(9)).unwrap();
        TabletStateMachine::new_local_reference(tablet, LOCAL_TABLET_EPOCH, raft_group_id).unwrap()
    }

    fn noop_envelope(tablet_id: TabletId, expected_epoch: u64) -> TabletCommandEnvelope {
        noop_envelope_for_sequence(tablet_id, expected_epoch, 1)
    }

    fn noop_envelope_for_sequence(
        tablet_id: TabletId,
        expected_epoch: u64,
        sequence: u64,
    ) -> TabletCommandEnvelope {
        command_envelope_for(
            tablet_id,
            expected_epoch,
            sequence,
            TabletCommand::Noop(NoopCommand),
        )
    }

    fn command_envelope(sequence: u64, command: TabletCommand) -> TabletCommandEnvelope {
        command_envelope_for(LOCAL_TABLET_ID, LOCAL_TABLET_EPOCH, sequence, command)
    }

    fn command_envelope_for(
        tablet_id: TabletId,
        expected_epoch: u64,
        sequence: u64,
        command: TabletCommand,
    ) -> TabletCommandEnvelope {
        command_envelope_for_group(
            tablet_id,
            expected_epoch,
            LOCAL_RAFT_GROUP_ID,
            sequence,
            command,
        )
    }

    fn command_envelope_for_group(
        tablet_id: TabletId,
        expected_epoch: u64,
        raft_group_id: RaftGroupId,
        sequence: u64,
        command: TabletCommand,
    ) -> TabletCommandEnvelope {
        TabletCommandEnvelope::new(
            RequestId {
                client_id: 0xf5b4_81ab_9b67_4418_ba82_b49c_e371_007d,
                sequence,
                raft_group_id,
            },
            tablet_id,
            expected_epoch,
            command,
        )
        .unwrap()
    }

    fn test_row(id: i64, name: &str) -> Row {
        Row {
            values: vec![Value::Int(id), Value::Text(name.to_string())],
        }
    }

    /// Realistic bug caught: an individually valid large mutation must be
    /// rejected before Raft admission if its complete atomic generation cannot
    /// fit within the bounded publication budget.
    #[test]
    fn oversized_single_command_delta_is_rejected_by_admission_estimator() {
        let state_machine = state_machine();
        let key = encode_row_key(&make_row_key(TableId(9), &[Value::Int(77)]).unwrap()).unwrap();
        let row = Row {
            values: vec![Value::Text("x".repeat(MAX_COMMAND_DELTA_BYTES / 2))],
        };
        let envelope = command_envelope(
            1,
            TabletCommand::SingleShardCommit(SingleShardCommitCommand {
                txn_id: TxnId(77),
                start_timestamp: Timestamp(10),
                commit_timestamp: Timestamp(11),
                writes: vec![WriteEntry {
                    key,
                    row: Some(row),
                    op: WriteKind::Put,
                }],
            }),
        );

        let error = state_machine
            .command_delta_upper_bound(&envelope)
            .unwrap_err();

        assert!(matches!(
            error,
            TabletCommandApplyError::InvalidCommand { .. }
        ));
    }

    /// Realistic bug caught: a committed configuration entry between two
    /// commands must not leave storage recovery one Raft position behind.
    #[test]
    fn frontier_only_raft_entry_keeps_storage_prefix_contiguous() {
        let mut state_machine = state_machine();
        state_machine
            .apply_committed_at(noop_envelope(LOCAL_TABLET_ID, LOCAL_TABLET_EPOCH), 1, 1)
            .unwrap();

        state_machine.apply_frontier_only_at(2, 1).unwrap();
        assert_eq!(
            state_machine.recovery_frontier(),
            Some(RecoveryFrontier::ReplicatedTablet {
                raft_group_id: LOCAL_RAFT_GROUP_ID,
                replica_id: ReplicaId(1),
                applied_index: 2,
                applied_term: 1,
            })
        );

        state_machine
            .apply_committed_at(
                noop_envelope_for_sequence(LOCAL_TABLET_ID, LOCAL_TABLET_EPOCH, 2),
                3,
                1,
            )
            .unwrap();
        assert_eq!(
            state_machine.recovery_frontier(),
            Some(RecoveryFrontier::ReplicatedTablet {
                raft_group_id: LOCAL_RAFT_GROUP_ID,
                replica_id: ReplicaId(1),
                applied_index: 3,
                applied_term: 1,
            })
        );
    }

    /// Realistic bug caught: replica-local snapshots and recovery frontiers
    /// must distinguish otherwise identical tablet state on replicas 1, 2, and 3.
    #[test]
    fn replica_frontiers_are_bound_to_each_replica_lifetime() {
        for replica_id in 1..=3 {
            let tablet = Tablet::new(LOCAL_TABLET_ID, TableId(9)).unwrap();
            let mut state_machine = TabletStateMachine::new_with_replica(
                tablet,
                LOCAL_TABLET_EPOCH,
                LOCAL_RAFT_GROUP_ID,
                ReplicaId(replica_id),
            )
            .unwrap();
            state_machine
                .apply_committed_at(noop_envelope(LOCAL_TABLET_ID, LOCAL_TABLET_EPOCH), 1, 1)
                .unwrap();

            assert_eq!(
                state_machine.recovery_frontier(),
                Some(RecoveryFrontier::ReplicatedTablet {
                    raft_group_id: LOCAL_RAFT_GROUP_ID,
                    replica_id: ReplicaId(replica_id),
                    applied_index: 1,
                    applied_term: 1,
                })
            );
        }
    }

    /// Realistic bug caught: each successful or rejected command in a mixed
    /// MVCC history must publish its retry result and exact frontier with the
    /// row, intent, and rollback-witness edits from that same command.
    #[test]
    fn committed_delta_history_keeps_mvcc_and_retry_families_coherent() {
        let mut complete_delta = state_machine();
        let key =
            |id| encode_row_key(&make_row_key(TableId(9), &[Value::Int(id)]).unwrap()).unwrap();
        let first_key = key(1);
        let second_key = key(2);
        let third_key = key(3);
        let commands = vec![
            TabletCommand::SingleShardCommit(SingleShardCommitCommand {
                txn_id: TxnId(1),
                start_timestamp: Timestamp(10),
                commit_timestamp: Timestamp(11),
                writes: vec![WriteEntry {
                    key: first_key.clone(),
                    row: Some(test_row(1, "committed")),
                    op: WriteKind::Put,
                }],
            }),
            TabletCommand::Prewrite(PrewriteCommand {
                txn_id: TxnId(2),
                start_timestamp: Timestamp(20),
                writes: vec![WriteEntry {
                    key: second_key.clone(),
                    row: Some(test_row(2, "prepared")),
                    op: WriteKind::Put,
                }],
                primary_key: second_key.clone(),
                ttl_ms: 30_000,
                pending_status: None,
            }),
            TabletCommand::Commit(CommitCommand {
                txn_id: TxnId(2),
                start_timestamp: Timestamp(20),
                commit_timestamp: Timestamp(21),
                keys: vec![second_key.clone()],
                committed_status: None,
            }),
            TabletCommand::Prewrite(PrewriteCommand {
                txn_id: TxnId(3),
                start_timestamp: Timestamp(30),
                writes: vec![WriteEntry {
                    key: third_key.clone(),
                    row: Some(test_row(3, "rolled back")),
                    op: WriteKind::Put,
                }],
                primary_key: third_key.clone(),
                ttl_ms: 30_000,
                pending_status: None,
            }),
            TabletCommand::Rollback(RollbackCommand {
                txn_id: TxnId(3),
                start_timestamp: Timestamp(30),
                keys: vec![third_key],
            }),
            TabletCommand::Prewrite(PrewriteCommand {
                txn_id: TxnId(4),
                start_timestamp: Timestamp(40),
                writes: vec![WriteEntry {
                    key: first_key.clone(),
                    row: Some(test_row(1, "new intent")),
                    op: WriteKind::Put,
                }],
                primary_key: first_key.clone(),
                ttl_ms: 30_000,
                pending_status: None,
            }),
            TabletCommand::Prewrite(PrewriteCommand {
                txn_id: TxnId(5),
                start_timestamp: Timestamp(41),
                writes: vec![WriteEntry {
                    key: first_key.clone(),
                    row: Some(test_row(1, "conflicting intent")),
                    op: WriteKind::Put,
                }],
                primary_key: first_key.clone(),
                ttl_ms: 30_000,
                pending_status: None,
            }),
            TabletCommand::Rollback(RollbackCommand {
                txn_id: TxnId(4),
                start_timestamp: Timestamp(40),
                keys: vec![first_key],
            }),
        ];

        let expected_results = [
            Some(TabletCommandApplyResult::SingleShardCommit),
            Some(TabletCommandApplyResult::Prewrite),
            Some(TabletCommandApplyResult::Commit),
            Some(TabletCommandApplyResult::Prewrite),
            Some(TabletCommandApplyResult::Rollback),
            Some(TabletCommandApplyResult::Prewrite),
            None,
            Some(TabletCommandApplyResult::Rollback),
        ];
        for (offset, (command, expected_result)) in
            commands.into_iter().zip(expected_results).enumerate()
        {
            let sequence = offset as u64 + 1;
            let result =
                complete_delta.apply_committed_at(command_envelope(sequence, command), sequence, 1);
            if let Some(expected_result) = expected_result {
                assert_eq!(
                    result.unwrap().result,
                    expected_result,
                    "command {sequence}"
                );
            } else {
                assert!(matches!(
                    result,
                    Err(TabletCommandApplyError::WriteConflict { .. })
                ));
            }
            assert_eq!(
                complete_delta.recovery_frontier(),
                Some(RecoveryFrontier::ReplicatedTablet {
                    raft_group_id: LOCAL_RAFT_GROUP_ID,
                    replica_id: ReplicaId(1),
                    applied_index: sequence,
                    applied_term: 1,
                })
            );
        }

        assert_eq!(complete_delta.tablet().stats().default_versions, 2);
        assert_eq!(complete_delta.tablet().stats().write_records, 4);
        assert_eq!(complete_delta.tablet().stats().locks, 0);
        assert_eq!(complete_delta.transaction_statuses.len(), 0);
        assert_eq!(complete_delta.logical_client_retry_horizons.len(), 0);
        assert_eq!(complete_delta.logical_command_deduplication.len(), 0);
        assert_eq!(complete_delta.client_deduplication.len(), 1);
        assert_eq!(
            complete_delta
                .client_deduplication
                .values()
                .next()
                .unwrap()
                .last_sequence_applied,
            8
        );
    }

    /// Realistic bug caught: a reader that retains generation N must not see
    /// row or retry metadata published in generation N+1, even when the next
    /// command updates the same tablet owner.
    #[test]
    fn pinned_tablet_generation_keeps_mvcc_retry_state_and_frontier_coherent() {
        let mut state_machine = state_machine();
        let first_key =
            encode_row_key(&make_row_key(TableId(9), &[Value::Int(1)]).unwrap()).unwrap();
        let second_key =
            encode_row_key(&make_row_key(TableId(9), &[Value::Int(2)]).unwrap()).unwrap();
        let make_commit = |id, key| {
            TabletCommand::SingleShardCommit(SingleShardCommitCommand {
                txn_id: TxnId(id as u64),
                start_timestamp: Timestamp(id as u64),
                commit_timestamp: Timestamp(id as u64 + 10),
                writes: vec![WriteEntry {
                    key,
                    row: Some(test_row(id, "pinned")),
                    op: WriteKind::Put,
                }],
            })
        };

        state_machine
            .apply_committed_at(command_envelope(1, make_commit(1, first_key.clone())), 1, 1)
            .unwrap();
        let old_generation = state_machine.pin_generation().unwrap();
        state_machine
            .apply_committed_at(
                command_envelope(2, make_commit(2, second_key.clone())),
                2,
                1,
            )
            .unwrap();

        assert!(
            old_generation
                .read(&first_key, Timestamp(20))
                .unwrap()
                .is_some()
        );
        assert!(
            old_generation
                .read(&second_key, Timestamp(20))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            old_generation.processed_frontier(),
            Some(RecoveryFrontier::ReplicatedTablet {
                raft_group_id: LOCAL_RAFT_GROUP_ID,
                replica_id: ReplicaId(1),
                applied_index: 1,
                applied_term: 1,
            })
        );
        assert_eq!(
            old_generation
                .legacy_outcome(0xf5b4_81ab_9b67_4418_ba82_b49c_e371_007d)
                .unwrap()
                .0,
            1
        );
        assert_eq!(
            state_machine.recovery_frontier(),
            Some(RecoveryFrontier::ReplicatedTablet {
                raft_group_id: LOCAL_RAFT_GROUP_ID,
                replica_id: ReplicaId(1),
                applied_index: 2,
                applied_term: 1,
            })
        );
        assert!(
            state_machine
                .tablet()
                .storage()
                .read(&second_key, Timestamp(20))
                .unwrap()
                .is_some()
        );
    }

    /// A published prewrite must expose its row intent, primary status,
    /// logical retry outcome, retry floor, and exact Raft position in one
    /// pinned generation.
    #[test]
    fn successful_delta_pins_data_status_retry_and_frontier_together() {
        let mut state_machine = state_machine();
        let key = encode_row_key(&make_row_key(TableId(9), &[Value::Int(80)]).unwrap()).unwrap();
        let txn_id = TxnId(80);
        let status = TxnStatusRecord {
            txn_id,
            start_timestamp: Timestamp(40),
            commit_timestamp: None,
            status: TxnStatus::Pending,
            primary_key: key.clone(),
            participant_tablet_ids: vec![LOCAL_TABLET_ID.0],
            last_heartbeat_timestamp: None,
            lease_deadline_ms: None,
        };
        let logical_id = LogicalCommandId {
            client_request_id: ClientRequestId {
                client_id: 0x80,
                session_epoch: 4,
                request_sequence: 2,
            },
            command_ordinal: 1,
            kind: CommandKind::Prewrite,
        };
        let mut envelope = TabletCommandEnvelope::new_with_logical_command_id(
            RequestId {
                client_id: 0x80,
                sequence: 1,
                raft_group_id: LOCAL_RAFT_GROUP_ID,
            },
            logical_id,
            LOCAL_TABLET_ID,
            LOCAL_TABLET_EPOCH,
            TabletCommand::Prewrite(PrewriteCommand {
                txn_id,
                start_timestamp: Timestamp(40),
                writes: vec![WriteEntry {
                    key: key.clone(),
                    row: Some(test_row(80, "atomic generation")),
                    op: WriteKind::Put,
                }],
                primary_key: key.clone(),
                ttl_ms: 30_000,
                pending_status: Some(status.clone()),
            }),
        )
        .unwrap();
        envelope.acknowledged_through = Some(1);

        state_machine.apply_committed_at(envelope, 1, 2).unwrap();
        let generation = state_machine.pin_generation().unwrap();

        assert!(
            generation
                .get_default(&key, Timestamp(40))
                .unwrap()
                .is_some()
        );
        assert_eq!(generation.get_lock(&key).unwrap().unwrap().txn_id, txn_id);
        assert_eq!(generation.transaction_status(txn_id), Some(&status));
        assert!(matches!(
            generation.logical_outcome(&logical_id),
            Some(CachedTabletCommandOutcome::Applied(
                CachedTabletCommandResult::Prewrite
            ))
        ));
        assert_eq!(generation.retry_floor(0x80, 4), Some(1));
        assert_eq!(
            generation.processed_frontier(),
            Some(RecoveryFrontier::ReplicatedTablet {
                raft_group_id: LOCAL_RAFT_GROUP_ID,
                replica_id: ReplicaId(1),
                applied_index: 1,
                applied_term: 2,
            })
        );
    }

    /// Realistic bug caught: a committed Put record must never publish if its
    /// referenced Default payload is absent from both the current generation
    /// and the same command delta.
    #[test]
    fn backend_rejects_dangling_write_before_publishing_any_family() {
        let mut state_machine = state_machine();
        let key = encode_row_key(&make_row_key(TableId(9), &[Value::Int(3)]).unwrap()).unwrap();
        let status = TxnStatusRecord {
            txn_id: TxnId(77),
            start_timestamp: Timestamp(20),
            commit_timestamp: None,
            status: TxnStatus::Pending,
            primary_key: key.clone(),
            participant_tablet_ids: vec![LOCAL_TABLET_ID.0],
            last_heartbeat_timestamp: None,
            lease_deadline_ms: None,
        };
        let logical_id = LogicalCommandId {
            client_request_id: ClientRequestId {
                client_id: 0xabc,
                session_epoch: 2,
                request_sequence: 3,
            },
            command_ordinal: 1,
            kind: CommandKind::Prewrite,
        };
        let rejection = CachedTabletCommandOutcome::Rejected(CachedTabletCommandRejection {
            kind: CachedTabletCommandRejectionKind::WriteConflict,
            reason: "prepared rejection must remain private".to_string(),
        });
        let delta = CommandDelta {
            mvcc: MvccDelta {
                edits: vec![MvccRecordEdit::PutWrite {
                    key: key.clone(),
                    write_ts: Timestamp(20),
                    write: WriteRecord {
                        start_timestamp: Timestamp(10),
                        commit_timestamp: Timestamp(20),
                        op: WriteKind::Put,
                    },
                }],
            },
            frontier: Some(RecoveryFrontier::ReplicatedTablet {
                raft_group_id: LOCAL_RAFT_GROUP_ID,
                replica_id: ReplicaId(1),
                applied_index: 1,
                applied_term: 1,
            }),
            transaction_status_edits: vec![TxnStatusEdit::Put {
                txn_id: status.txn_id,
                status,
            }],
            logical_outcome_edits: vec![LogicalOutcomeEdit::Put {
                id: logical_id,
                outcome: rejection.clone(),
            }],
            legacy_outcome_edits: vec![LegacyOutcomeEdit::Put {
                client_id: 0xdef,
                last_sequence_applied: 1,
                outcome: rejection,
            }],
            retry_floor_edits: vec![RetryFloorEdit::Advance {
                client_id: 0xabc,
                session_epoch: 2,
                acknowledged_through: 3,
            }],
        };

        assert!(
            state_machine
                .backend
                .publish_command_delta(delta, None)
                .is_err()
        );
        assert!(
            state_machine
                .tablet()
                .storage()
                .read(&key, Timestamp(20))
                .unwrap()
                .is_none()
        );
        assert_eq!(state_machine.recovery_frontier(), None);
        assert_eq!(state_machine.transaction_statuses.len(), 0);
        assert_eq!(state_machine.logical_command_deduplication.len(), 0);
        assert_eq!(state_machine.client_deduplication.len(), 0);
        assert_eq!(state_machine.logical_client_retry_horizons.len(), 0);
    }

    /// A command that fails during its first preparation step must not publish
    /// row edits, cached results, retry state, or the committed frontier.
    #[test]
    fn early_prepare_failure_leaves_all_replicated_state_unpublished() {
        let mut state_machine = state_machine();
        let key = encode_row_key(&make_row_key(TableId(9), &[Value::Int(31)]).unwrap()).unwrap();
        let status = TxnStatusRecord {
            txn_id: TxnId(31),
            start_timestamp: Timestamp(40),
            commit_timestamp: None,
            status: TxnStatus::Pending,
            primary_key: key.clone(),
            participant_tablet_ids: vec![LOCAL_TABLET_ID.0],
            last_heartbeat_timestamp: None,
            lease_deadline_ms: None,
        };
        state_machine
            .apply_committed_at(
                command_envelope(
                    1,
                    TabletCommand::Prewrite(PrewriteCommand {
                        txn_id: status.txn_id,
                        start_timestamp: status.start_timestamp,
                        writes: vec![WriteEntry {
                            key: key.clone(),
                            row: Some(test_row(31, "pending")),
                            op: WriteKind::Put,
                        }],
                        primary_key: key.clone(),
                        ttl_ms: 30_000,
                        pending_status: Some(status.clone()),
                    }),
                ),
                1,
                1,
            )
            .unwrap();
        let changed_status = TxnStatusRecord {
            participant_tablet_ids: vec![LOCAL_TABLET_ID.0, 42],
            ..status.clone()
        };
        let error = state_machine
            .apply_committed_at(
                command_envelope(
                    2,
                    TabletCommand::Prewrite(PrewriteCommand {
                        txn_id: status.txn_id,
                        start_timestamp: status.start_timestamp,
                        writes: vec![WriteEntry {
                            key: key.clone(),
                            row: Some(test_row(31, "must not replace")),
                            op: WriteKind::Put,
                        }],
                        primary_key: key.clone(),
                        ttl_ms: 30_000,
                        pending_status: Some(changed_status),
                    }),
                ),
                2,
                1,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            TabletCommandApplyError::CorruptState { .. }
        ));
        assert_eq!(
            state_machine.recovery_frontier(),
            Some(RecoveryFrontier::ReplicatedTablet {
                raft_group_id: LOCAL_RAFT_GROUP_ID,
                replica_id: ReplicaId(1),
                applied_index: 1,
                applied_term: 1,
            })
        );
        assert_eq!(state_machine.tablet().stats().locks, 1);
        assert_eq!(state_machine.tablet().stats().default_versions, 1);
        assert_eq!(state_machine.tablet().stats().write_records, 0);
        assert_eq!(
            state_machine.transaction_statuses.get(&status.txn_id),
            Some(&status)
        );
        assert_eq!(state_machine.logical_command_deduplication.len(), 0);
        assert_eq!(state_machine.client_deduplication.len(), 1);
        assert_eq!(state_machine.logical_client_retry_horizons.len(), 0);
    }

    /// A backend caller must not remove a pending intent with a write from a
    /// different transaction, or publish that write while leaving the intent
    /// live. Both shapes would make the lock and Write family describe
    /// different transaction histories.
    #[test]
    fn backend_rejects_lock_transition_without_matching_start_timestamp_or_removal() {
        let mut state_machine = state_machine();
        let key = encode_row_key(&make_row_key(TableId(9), &[Value::Int(33)]).unwrap()).unwrap();
        let prewrite = PrewriteCommand {
            txn_id: TxnId(33),
            start_timestamp: Timestamp(40),
            writes: vec![WriteEntry {
                key: key.clone(),
                row: Some(test_row(33, "locked")),
                op: WriteKind::Put,
            }],
            primary_key: key.clone(),
            ttl_ms: 30_000,
            pending_status: None,
        };
        state_machine
            .apply_committed_at(command_envelope(1, TabletCommand::Prewrite(prewrite)), 1, 1)
            .unwrap();
        let original_lock = state_machine
            .tablet()
            .storage()
            .get_lock_record(&key)
            .unwrap()
            .unwrap();

        let delta_for = |edits| CommandDelta {
            mvcc: MvccDelta { edits },
            frontier: Some(RecoveryFrontier::ReplicatedTablet {
                raft_group_id: LOCAL_RAFT_GROUP_ID,
                replica_id: ReplicaId(1),
                applied_index: 2,
                applied_term: 1,
            }),
            ..CommandDelta::default()
        };

        let mismatched_removal = delta_for(vec![
            MvccRecordEdit::DeleteLock { key: key.clone() },
            MvccRecordEdit::PutWrite {
                key: key.clone(),
                write_ts: Timestamp(51),
                write: WriteRecord {
                    start_timestamp: Timestamp(49),
                    commit_timestamp: Timestamp(51),
                    op: WriteKind::Delete,
                },
            },
        ]);
        assert!(
            state_machine
                .backend
                .publish_command_delta(mismatched_removal, None)
                .is_err()
        );

        let write_without_removal = delta_for(vec![MvccRecordEdit::PutWrite {
            key: key.clone(),
            write_ts: Timestamp(52),
            write: WriteRecord {
                start_timestamp: Timestamp(40),
                commit_timestamp: Timestamp(52),
                op: WriteKind::Put,
            },
        }]);
        assert!(
            state_machine
                .backend
                .publish_command_delta(write_without_removal, None)
                .is_err()
        );

        assert_eq!(
            state_machine
                .tablet()
                .storage()
                .get_lock_record(&key)
                .unwrap(),
            Some(original_lock)
        );
        assert!(
            state_machine
                .tablet()
                .storage()
                .get_write_record(&key, Timestamp(51))
                .unwrap()
                .is_none()
        );
        assert!(
            state_machine
                .tablet()
                .storage()
                .get_write_record(&key, Timestamp(52))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            state_machine.recovery_frontier(),
            Some(RecoveryFrontier::ReplicatedTablet {
                raft_group_id: LOCAL_RAFT_GROUP_ID,
                replica_id: ReplicaId(1),
                applied_index: 1,
                applied_term: 1,
            })
        );
    }

    /// Realistic bug caught: applying a committed command after its active
    /// memtable reservation fails must not publish row state or its Raft
    /// frontier as separate steps.
    #[test]
    fn memtable_reservation_failure_keeps_command_delta_and_frontier_unpublished() {
        let budget = NodeMemtableBudget::new(1024).unwrap();
        let tablet =
            Tablet::new_with_memtable_budget(LOCAL_TABLET_ID, TableId(9), budget.clone(), 1)
                .unwrap();
        let mut state_machine = TabletStateMachine::new_local_reference(
            tablet,
            LOCAL_TABLET_EPOCH,
            LOCAL_RAFT_GROUP_ID,
        )
        .unwrap();
        let key = encode_row_key(&make_row_key(TableId(9), &[Value::Int(71)]).unwrap()).unwrap();
        let command = TabletCommand::SingleShardCommit(SingleShardCommitCommand {
            txn_id: TxnId(71),
            start_timestamp: Timestamp(71),
            commit_timestamp: Timestamp(72),
            writes: vec![WriteEntry {
                key: key.clone(),
                row: Some(test_row(71, "bounded")),
                op: WriteKind::Put,
            }],
        });

        assert!(
            state_machine
                .apply_committed_at(command_envelope(1, command), 1, 1)
                .is_err()
        );

        assert_eq!(state_machine.tablet().stats().default_versions, 0);
        assert_eq!(
            state_machine
                .tablet()
                .storage()
                .get_default_record(&key, Timestamp(71))
                .unwrap(),
            None
        );
        assert_eq!(state_machine.recovery_frontier(), None);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[test]
    fn apply_rejects_stale_tablet_epoch_before_payload_dispatch() {
        let mut state_machine = state_machine();

        let error = state_machine
            .apply(noop_envelope(LOCAL_TABLET_ID, LOCAL_TABLET_EPOCH - 1))
            .unwrap_err();

        assert_eq!(
            error,
            TabletCommandApplyError::TabletEpochMismatch {
                current_epoch: LOCAL_TABLET_EPOCH,
                expected_epoch: LOCAL_TABLET_EPOCH - 1,
            }
        );

        // Rejection must not consume the request sequence. The same request is
        // still fresh when routed using the current tablet epoch.
        let result = state_machine
            .apply(noop_envelope(LOCAL_TABLET_ID, LOCAL_TABLET_EPOCH))
            .unwrap();

        assert_eq!(
            result,
            TabletCommandApplyOutcome {
                result: TabletCommandApplyResult::Noop,
                deduplicated: false,
            }
        );
    }

    #[test]
    fn apply_rejects_command_for_another_tablet() {
        let mut state_machine = state_machine();
        let requested_tablet_id = TabletId(LOCAL_TABLET_ID.0 + 1);

        let error = state_machine
            .apply(noop_envelope(requested_tablet_id, LOCAL_TABLET_EPOCH))
            .unwrap_err();

        assert_eq!(
            error,
            TabletCommandApplyError::TabletIdMismatch {
                local_tablet_id: LOCAL_TABLET_ID,
                requested_tablet_id,
            }
        );
    }

    #[test]
    fn exact_request_retry_returns_cached_result_without_reapplying() {
        let mut state_machine = state_machine();

        let first = state_machine
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                1,
            ))
            .unwrap();

        let retry = state_machine
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                1,
            ))
            .unwrap();

        assert_eq!(
            first,
            TabletCommandApplyOutcome {
                result: TabletCommandApplyResult::Noop,
                deduplicated: false,
            }
        );

        assert_eq!(
            retry,
            TabletCommandApplyOutcome {
                result: TabletCommandApplyResult::Noop,
                deduplicated: true,
            }
        );
    }

    #[test]
    fn logical_command_retry_deduplicates_even_when_transport_sequence_changes() {
        let mut state_machine = state_machine();
        let logical_command_id = LogicalCommandId {
            client_request_id: ClientRequestId {
                client_id: 0x55,
                session_epoch: 3,
                request_sequence: 1,
            },
            command_ordinal: 1,
            kind: CommandKind::Noop,
        };

        let first = TabletCommandEnvelope::new_with_logical_command_id(
            RequestId {
                client_id: 0x55,
                sequence: 1,
                raft_group_id: LOCAL_RAFT_GROUP_ID,
            },
            logical_command_id,
            LOCAL_TABLET_ID,
            LOCAL_TABLET_EPOCH,
            TabletCommand::Noop(NoopCommand),
        )
        .unwrap();
        let retry = TabletCommandEnvelope::new_with_logical_command_id(
            RequestId {
                client_id: 0x55,
                sequence: 9,
                raft_group_id: LOCAL_RAFT_GROUP_ID,
            },
            logical_command_id,
            LOCAL_TABLET_ID,
            LOCAL_TABLET_EPOCH,
            TabletCommand::Noop(NoopCommand),
        )
        .unwrap();

        assert!(!state_machine.apply(first).unwrap().deduplicated);
        assert!(state_machine.apply(retry).unwrap().deduplicated);
    }

    #[test]
    fn acknowledged_logical_command_is_expired_after_durable_watermark() {
        let mut state_machine = state_machine();
        let logical_command_id = LogicalCommandId {
            client_request_id: ClientRequestId {
                client_id: 0x56,
                session_epoch: 3,
                request_sequence: 1,
            },
            command_ordinal: 1,
            kind: CommandKind::Noop,
        };
        let first = TabletCommandEnvelope::new_with_logical_command_id(
            RequestId {
                client_id: 0x56,
                sequence: 1,
                raft_group_id: LOCAL_RAFT_GROUP_ID,
            },
            logical_command_id,
            LOCAL_TABLET_ID,
            LOCAL_TABLET_EPOCH,
            TabletCommand::Noop(NoopCommand),
        )
        .unwrap();
        state_machine.apply(first).unwrap();

        let mut acknowledged_retry = TabletCommandEnvelope::new_with_logical_command_id(
            RequestId {
                client_id: 0x56,
                sequence: 1,
                raft_group_id: LOCAL_RAFT_GROUP_ID,
            },
            logical_command_id,
            LOCAL_TABLET_ID,
            LOCAL_TABLET_EPOCH,
            TabletCommand::Noop(NoopCommand),
        )
        .unwrap();
        acknowledged_retry.acknowledged_through = Some(1);

        assert_eq!(
            state_machine.apply(acknowledged_retry).unwrap_err(),
            TabletCommandApplyError::RequestIdExpired {
                client_id: 0x56,
                session_epoch: 3,
                sequence: 1,
                acknowledged_through: 1,
            }
        );

        let snapshot = state_machine.encode_snapshot_state().unwrap();
        let mut restored = TabletStateMachine::restore_from_snapshot_for_local_reference(
            Tablet::new(LOCAL_TABLET_ID, TableId(1)).unwrap(),
            &snapshot,
        )
        .unwrap();
        let retry_after_restart = TabletCommandEnvelope::new_with_logical_command_id(
            RequestId {
                client_id: 0x56,
                sequence: 1,
                raft_group_id: LOCAL_RAFT_GROUP_ID,
            },
            logical_command_id,
            LOCAL_TABLET_ID,
            LOCAL_TABLET_EPOCH,
            TabletCommand::Noop(NoopCommand),
        )
        .unwrap();
        assert!(matches!(
            restored.apply(retry_after_restart),
            Err(TabletCommandApplyError::RequestIdExpired { .. })
        ));
    }

    #[test]
    fn request_sequence_gap_is_rejected_without_consuming_missing_sequence() {
        let mut state_machine = state_machine();

        state_machine
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                1,
            ))
            .unwrap();

        let error = state_machine
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                3,
            ))
            .unwrap_err();

        assert_eq!(
            error,
            TabletCommandApplyError::RequestSequenceGap {
                last_sequence_applied: 1,
                expected_sequence: 2,
                received_sequence: 3,
            }
        );

        let missing = state_machine
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                2,
            ))
            .unwrap();

        assert_eq!(
            missing,
            TabletCommandApplyOutcome {
                result: TabletCommandApplyResult::Noop,
                deduplicated: false,
            }
        );
    }

    #[test]
    fn client_request_sequences_are_scoped_to_each_raft_group() {
        let second_tablet_id = TabletId(LOCAL_TABLET_ID.0 + 1);
        let second_group_id = RaftGroupId(LOCAL_RAFT_GROUP_ID.0 + 1);
        let mut first_group = state_machine_for(LOCAL_TABLET_ID);
        let mut second_group = state_machine_for_group(second_tablet_id, second_group_id);

        let first_result = first_group
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                1,
            ))
            .unwrap();

        let second_result = second_group
            .apply(command_envelope_for_group(
                second_tablet_id,
                LOCAL_TABLET_EPOCH,
                second_group_id,
                1,
                TabletCommand::Noop(NoopCommand),
            ))
            .unwrap();

        assert!(!first_result.deduplicated);
        assert!(!second_result.deduplicated);

        let wrong_group = command_envelope_for_group(
            LOCAL_TABLET_ID,
            LOCAL_TABLET_EPOCH,
            second_group_id,
            2,
            TabletCommand::Noop(NoopCommand),
        );
        assert!(matches!(
            first_group.apply(wrong_group),
            Err(TabletCommandApplyError::RaftGroupMismatch { .. })
        ));

        // Routing rejection does not consume sequence two in the local group.
        first_group
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                2,
            ))
            .unwrap();
    }

    /// Realistic bug caught: malformed storage-key bytes pass protobuf checks,
    /// enter the Raft log, and are later mistaken for group-fatal corruption
    /// during committed apply.
    #[test]
    fn proposal_validation_rejects_malformed_storage_key_without_consuming_sequence() {
        let mut state_machine = state_machine();
        let malformed = vec![0xff];
        let invalid = command_envelope(
            1,
            TabletCommand::Prewrite(PrewriteCommand {
                txn_id: TxnId(88),
                start_timestamp: Timestamp(500),
                writes: vec![WriteEntry {
                    key: malformed.clone(),
                    row: Some(test_row(88, "invalid")),
                    op: WriteKind::Put,
                }],
                primary_key: malformed,
                ttl_ms: 30_000,
                pending_status: None,
            }),
        );

        assert!(matches!(
            state_machine.validate_proposal(&invalid),
            Err(TabletCommandApplyError::InvalidCommand { .. })
        ));
        assert!(matches!(
            state_machine.apply(invalid),
            Err(TabletCommandApplyError::InvalidCommand { .. })
        ));

        state_machine
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                1,
            ))
            .unwrap();
    }

    #[test]
    fn snapshot_restore_preserves_cached_request_result() {
        let mut original = state_machine();

        original
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                1,
            ))
            .unwrap();

        let snapshot = original.encode_snapshot_state().unwrap();

        let tablet = Tablet::new(LOCAL_TABLET_ID, TableId(9)).unwrap();
        let mut restored =
            TabletStateMachine::restore_from_snapshot_for_local_reference(tablet, &snapshot)
                .unwrap();

        // The last acknowledged request must be served from restored deduplication
        // state instead of being dispatched as a fresh state transition.
        let retry = restored
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                1,
            ))
            .unwrap();

        assert_eq!(
            retry,
            TabletCommandApplyOutcome {
                result: TabletCommandApplyResult::Noop,
                deduplicated: true,
            }
        );

        // Restoration must also preserve the next expected sequence.
        let next = restored
            .apply(noop_envelope_for_sequence(
                LOCAL_TABLET_ID,
                LOCAL_TABLET_EPOCH,
                2,
            ))
            .unwrap();

        assert!(!next.deduplicated);
    }

    #[test]
    fn snapshot_restore_rejects_state_for_another_tablet() {
        let original = state_machine();
        let snapshot = original.encode_snapshot_state().unwrap();

        let other_tablet_id = TabletId(LOCAL_TABLET_ID.0 + 1);
        let other_tablet = Tablet::new(other_tablet_id, TableId(9)).unwrap();

        let error =
            TabletStateMachine::restore_from_snapshot_for_local_reference(other_tablet, &snapshot)
                .unwrap_err();

        assert_eq!(
            error,
            TabletStateMachineRestoreError::TabletIdMismatch {
                local_tablet_id: other_tablet_id,
                snapshot_tablet_id: LOCAL_TABLET_ID,
            }
        );
    }

    #[test]
    fn single_shard_commit_applies_complete_row_batch() {
        let mut state_machine = state_machine();
        let first_key = make_row_key(TableId(9), &[Value::Int(1)]).unwrap();
        let second_key = make_row_key(TableId(9), &[Value::Int(2)]).unwrap();
        let first_row = test_row(1, "Ada");
        let second_row = test_row(2, "Grace");

        let command = SingleShardCommitCommand {
            txn_id: TxnId(11),
            start_timestamp: Timestamp(20),
            commit_timestamp: Timestamp(30),
            writes: vec![
                WriteEntry {
                    key: encode_row_key(&first_key).unwrap(),
                    row: Some(first_row.clone()),
                    op: WriteKind::Put,
                },
                WriteEntry {
                    key: encode_row_key(&second_key).unwrap(),
                    row: Some(second_row.clone()),
                    op: WriteKind::Put,
                },
            ],
        };

        let outcome = state_machine
            .apply(command_envelope(
                1,
                TabletCommand::SingleShardCommit(command),
            ))
            .unwrap();

        assert_eq!(outcome.result, TabletCommandApplyResult::SingleShardCommit);
        assert!(!outcome.deduplicated);

        let reader = Transaction::new(TxnId(99), Timestamp(31)).unwrap();

        assert_eq!(
            state_machine.tablet().get(&reader, &first_key).unwrap(),
            Some(first_row)
        );
        assert_eq!(
            state_machine.tablet().get(&reader, &second_key).unwrap(),
            Some(second_row)
        );
    }

    #[test]
    fn prewrite_atomically_installs_default_value_and_lock() {
        let mut state_machine = state_machine();
        let key = make_row_key(TableId(9), &[Value::Int(7)]).unwrap();
        let encoded_key = encode_row_key(&key).unwrap();

        let command = PrewriteCommand {
            txn_id: TxnId(12),
            start_timestamp: Timestamp(40),
            writes: vec![WriteEntry {
                key: encoded_key.clone(),
                row: Some(test_row(7, "Lin")),
                op: WriteKind::Put,
            }],
            primary_key: encoded_key,
            ttl_ms: 30_000,
            pending_status: None,
        };

        let outcome = state_machine
            .apply(command_envelope(1, TabletCommand::Prewrite(command)))
            .unwrap();

        assert_eq!(outcome.result, TabletCommandApplyResult::Prewrite);
        assert_eq!(state_machine.tablet().stats().default_versions, 1);
        assert_eq!(state_machine.tablet().stats().locks, 1);

        let reader = Transaction::new(TxnId(100), Timestamp(50)).unwrap();

        assert!(matches!(
            state_machine.tablet().get(&reader, &key),
            Err(Error::WriteConflict(_))
        ));
    }

    /// Realistic bug caught: when Raft compacts the entries that published a
    /// primary decision, restoring only request deduplication loses the status
    /// readers need to resolve the surviving MVCC intent safely.
    #[test]
    fn primary_transaction_status_survives_snapshot_restore_and_terminal_apply() {
        let mut state_machine = state_machine();
        let key = make_row_key(TableId(9), &[Value::Int(74)]).unwrap();
        let encoded_key = encode_row_key(&key).unwrap();
        let pending_status = TxnStatusRecord {
            txn_id: TxnId(123),
            start_timestamp: Timestamp(440),
            commit_timestamp: None,
            status: TxnStatus::Pending,
            primary_key: encoded_key.clone(),
            participant_tablet_ids: vec![LOCAL_TABLET_ID.0, 42],
            last_heartbeat_timestamp: None,
            lease_deadline_ms: None,
        };

        state_machine
            .apply(command_envelope(
                1,
                TabletCommand::Prewrite(PrewriteCommand {
                    txn_id: pending_status.txn_id,
                    start_timestamp: pending_status.start_timestamp,
                    writes: vec![WriteEntry {
                        key: encoded_key.clone(),
                        row: Some(test_row(74, "pending")),
                        op: WriteKind::Put,
                    }],
                    primary_key: encoded_key.clone(),
                    ttl_ms: 30_000,
                    pending_status: Some(pending_status.clone()),
                }),
            ))
            .unwrap();

        let pending_snapshot = state_machine.encode_snapshot_state().unwrap();
        let restored_pending = TabletStateMachine::restore_from_snapshot_for_local_reference(
            Tablet::new(LOCAL_TABLET_ID, TableId(9)).unwrap(),
            &pending_snapshot,
        )
        .unwrap();
        assert_eq!(
            restored_pending
                .transaction_status(pending_status.txn_id)
                .unwrap(),
            Some(&pending_status)
        );

        let mismatched_commit_status = TxnStatusRecord {
            commit_timestamp: Some(Timestamp(450)),
            status: TxnStatus::Committed,
            participant_tablet_ids: vec![LOCAL_TABLET_ID.0, 43],
            ..pending_status.clone()
        };
        assert!(matches!(
            state_machine.apply(command_envelope(
                2,
                TabletCommand::Commit(CommitCommand {
                    txn_id: mismatched_commit_status.txn_id,
                    start_timestamp: mismatched_commit_status.start_timestamp,
                    commit_timestamp: Timestamp(450),
                    keys: vec![encoded_key.clone()],
                    committed_status: Some(mismatched_commit_status),
                }),
            )),
            Err(TabletCommandApplyError::CorruptState { .. })
        ));
        assert_eq!(state_machine.tablet().stats().locks, 1);
        assert_eq!(state_machine.tablet().stats().write_records, 0);

        let committed_status = TxnStatusRecord {
            commit_timestamp: Some(Timestamp(450)),
            status: TxnStatus::Committed,
            ..pending_status.clone()
        };
        state_machine
            .apply(command_envelope(
                2,
                TabletCommand::Commit(CommitCommand {
                    txn_id: committed_status.txn_id,
                    start_timestamp: committed_status.start_timestamp,
                    commit_timestamp: Timestamp(450),
                    keys: vec![encoded_key],
                    committed_status: Some(committed_status.clone()),
                }),
            ))
            .unwrap();

        let committed_snapshot = state_machine.encode_snapshot_state().unwrap();
        let restored_committed = TabletStateMachine::restore_from_snapshot_for_local_reference(
            Tablet::new(LOCAL_TABLET_ID, TableId(9)).unwrap(),
            &committed_snapshot,
        )
        .unwrap();
        assert_eq!(
            restored_committed
                .transaction_status(committed_status.txn_id)
                .unwrap(),
            Some(&committed_status)
        );
        assert_eq!(state_machine.tablet().stats().locks, 0);
        assert_eq!(state_machine.tablet().stats().write_records, 1);
    }

    #[test]
    fn abort_status_requires_pending_identity_and_is_terminal() {
        let mut state_machine = state_machine();
        let key = make_row_key(TableId(9), &[Value::Int(75)]).unwrap();
        let encoded_key = encode_row_key(&key).unwrap();
        let pending = TxnStatusRecord {
            txn_id: TxnId(124),
            start_timestamp: Timestamp(460),
            commit_timestamp: None,
            status: TxnStatus::Pending,
            primary_key: encoded_key.clone(),
            participant_tablet_ids: vec![LOCAL_TABLET_ID.0, 42],
            last_heartbeat_timestamp: None,
            lease_deadline_ms: None,
        };
        state_machine
            .apply(command_envelope(
                1,
                TabletCommand::Prewrite(PrewriteCommand {
                    txn_id: pending.txn_id,
                    start_timestamp: pending.start_timestamp,
                    writes: vec![WriteEntry {
                        key: encoded_key.clone(),
                        row: Some(test_row(75, "aborted")),
                        op: WriteKind::Put,
                    }],
                    primary_key: encoded_key.clone(),
                    ttl_ms: 30_000,
                    pending_status: Some(pending.clone()),
                }),
            ))
            .unwrap();

        // This primary participant has durably applied its rollback before the
        // separate command publishes the terminal status decision.
        state_machine
            .apply(command_envelope(
                2,
                TabletCommand::Rollback(RollbackCommand {
                    txn_id: pending.txn_id,
                    start_timestamp: pending.start_timestamp,
                    keys: vec![encoded_key.clone()],
                }),
            ))
            .unwrap();
        let aborted = TxnStatusRecord {
            status: TxnStatus::Aborted,
            ..pending.clone()
        };
        let abort_command =
            TabletCommand::PublishAbortedTransactionStatus(PublishAbortedTransactionStatus {
                status_record: aborted.clone(),
            });
        state_machine
            .apply(command_envelope(3, abort_command.clone()))
            .unwrap();
        assert_eq!(
            state_machine.transaction_status(aborted.txn_id).unwrap(),
            Some(&aborted)
        );

        // A new request carrying the same terminal decision is idempotent,
        // while a later committed decision cannot replace it.
        state_machine
            .apply(command_envelope(4, abort_command))
            .unwrap();
        let conflicting_commit = TxnStatusRecord {
            commit_timestamp: Some(Timestamp(470)),
            status: TxnStatus::Committed,
            ..pending
        };
        assert!(matches!(
            state_machine.apply(command_envelope(
                5,
                TabletCommand::Commit(CommitCommand {
                    txn_id: conflicting_commit.txn_id,
                    start_timestamp: conflicting_commit.start_timestamp,
                    commit_timestamp: Timestamp(470),
                    keys: vec![encoded_key],
                    committed_status: Some(conflicting_commit),
                }),
            )),
            Err(TabletCommandApplyError::InvalidCommand { .. })
        ));
        assert_eq!(state_machine.tablet().stats().locks, 0);
        assert_eq!(
            state_machine.transaction_status(aborted.txn_id).unwrap(),
            Some(&aborted)
        );
    }

    /// Realistic bug caught: a cleaner that reads an expired record must not
    /// abort after a concurrent heartbeat has extended the same transaction.
    #[test]
    fn heartbeat_and_expiry_commands_serialize_on_the_exact_pending_status() {
        let mut state_machine = state_machine();
        let key = make_row_key(TableId(9), &[Value::Int(76)]).unwrap();
        let encoded_key = encode_row_key(&key).unwrap();
        let pending = TxnStatusRecord {
            txn_id: TxnId(125),
            start_timestamp: Timestamp(480),
            commit_timestamp: None,
            status: TxnStatus::Pending,
            primary_key: encoded_key.clone(),
            participant_tablet_ids: vec![LOCAL_TABLET_ID.0, 42],
            last_heartbeat_timestamp: Some(Timestamp(480)),
            lease_deadline_ms: Some(100),
        };
        state_machine
            .apply(command_envelope(
                1,
                TabletCommand::Prewrite(PrewriteCommand {
                    txn_id: pending.txn_id,
                    start_timestamp: pending.start_timestamp,
                    writes: vec![WriteEntry {
                        key: encoded_key.clone(),
                        row: Some(test_row(76, "leased")),
                        op: WriteKind::Put,
                    }],
                    primary_key: encoded_key,
                    ttl_ms: 30_000,
                    pending_status: Some(pending.clone()),
                }),
            ))
            .unwrap();

        let renewed = TxnStatusRecord {
            last_heartbeat_timestamp: Some(Timestamp(481)),
            lease_deadline_ms: Some(200),
            ..pending.clone()
        };
        state_machine
            .apply(command_envelope(
                2,
                TabletCommand::HeartbeatTransactionStatus(HeartbeatTransactionStatus {
                    expected_status: pending.clone(),
                    next_status: renewed.clone(),
                    now_ms: 99,
                }),
            ))
            .unwrap();
        assert_eq!(
            state_machine.transaction_status(pending.txn_id).unwrap(),
            Some(&renewed)
        );

        assert!(matches!(
            state_machine.apply(command_envelope(
                3,
                TabletCommand::ExpirePendingTransactionStatus(ExpirePendingTransactionStatus {
                    expected_status: pending,
                    now_ms: 100,
                },),
            )),
            Err(TabletCommandApplyError::WriteConflict { .. })
        ));
        assert_eq!(
            state_machine.transaction_status(renewed.txn_id).unwrap(),
            Some(&renewed)
        );

        state_machine
            .apply(command_envelope(
                4,
                TabletCommand::ExpirePendingTransactionStatus(ExpirePendingTransactionStatus {
                    expected_status: renewed.clone(),
                    now_ms: 200,
                }),
            ))
            .unwrap();
        let aborted = TxnStatusRecord {
            status: TxnStatus::Aborted,
            ..renewed.clone()
        };
        assert_eq!(
            state_machine.transaction_status(renewed.txn_id).unwrap(),
            Some(&aborted)
        );
        assert!(matches!(
            state_machine.apply(command_envelope(
                5,
                TabletCommand::HeartbeatTransactionStatus(HeartbeatTransactionStatus {
                    expected_status: renewed.clone(),
                    next_status: TxnStatusRecord {
                        last_heartbeat_timestamp: Some(Timestamp(482)),
                        lease_deadline_ms: Some(300),
                        ..renewed.clone()
                    },
                    now_ms: 150,
                }),
            )),
            Err(TabletCommandApplyError::WriteConflict { .. })
        ));
    }

    /// Realistic bug caught: a coordinator sends one phase request for two
    /// keys on the same participant tablet, and deduplication must not discard
    /// the second key as an apparent retry of the first.
    #[test]
    fn participant_phase_batches_all_keys_under_one_request_id() {
        let mut state_machine = state_machine();
        let first_key = make_row_key(TableId(9), &[Value::Int(70)]).unwrap();
        let second_key = make_row_key(TableId(9), &[Value::Int(71)]).unwrap();
        let first_encoded = encode_row_key(&first_key).unwrap();
        let second_encoded = encode_row_key(&second_key).unwrap();

        state_machine
            .apply(command_envelope(
                1,
                TabletCommand::Prewrite(PrewriteCommand {
                    txn_id: TxnId(120),
                    start_timestamp: Timestamp(400),
                    writes: vec![
                        WriteEntry {
                            key: first_encoded.clone(),
                            row: Some(test_row(70, "first")),
                            op: WriteKind::Put,
                        },
                        WriteEntry {
                            key: second_encoded.clone(),
                            row: Some(test_row(71, "second")),
                            op: WriteKind::Put,
                        },
                    ],
                    primary_key: first_encoded.clone(),
                    ttl_ms: 30_000,
                    pending_status: None,
                }),
            ))
            .unwrap();

        assert_eq!(state_machine.tablet().stats().locks, 2);
        state_machine
            .apply(command_envelope(
                2,
                TabletCommand::Commit(CommitCommand {
                    txn_id: TxnId(120),
                    start_timestamp: Timestamp(400),
                    commit_timestamp: Timestamp(410),
                    keys: vec![first_encoded, second_encoded],
                    committed_status: None,
                }),
            ))
            .unwrap();
        assert_eq!(state_machine.tablet().stats().locks, 0);
        assert_eq!(state_machine.tablet().stats().write_records, 2);
    }

    /// Realistic bug caught: a batch installs the first intent before a later
    /// key reports a conflict, leaving a partially executed participant phase
    /// that an exact request retry cannot safely interpret.
    #[test]
    fn participant_prewrite_conflict_is_atomic_and_cached_for_the_whole_batch() {
        let mut state_machine = state_machine();
        let first_key = make_row_key(TableId(9), &[Value::Int(72)]).unwrap();
        let second_key = make_row_key(TableId(9), &[Value::Int(73)]).unwrap();
        let first_encoded = encode_row_key(&first_key).unwrap();
        let second_encoded = encode_row_key(&second_key).unwrap();

        // A different transaction owns the second key before the participant
        // batch arrives.
        state_machine
            .apply(command_envelope(
                1,
                TabletCommand::Prewrite(PrewriteCommand {
                    txn_id: TxnId(121),
                    start_timestamp: Timestamp(420),
                    writes: vec![WriteEntry {
                        key: second_encoded.clone(),
                        row: Some(test_row(73, "owner")),
                        op: WriteKind::Put,
                    }],
                    primary_key: second_encoded.clone(),
                    ttl_ms: 30_000,
                    pending_status: None,
                }),
            ))
            .unwrap();

        let participant = command_envelope(
            2,
            TabletCommand::Prewrite(PrewriteCommand {
                txn_id: TxnId(122),
                start_timestamp: Timestamp(430),
                writes: vec![
                    WriteEntry {
                        key: first_encoded,
                        row: Some(test_row(72, "must-not-leak")),
                        op: WriteKind::Put,
                    },
                    WriteEntry {
                        key: second_encoded,
                        row: Some(test_row(73, "contender")),
                        op: WriteKind::Put,
                    },
                ],
                primary_key: encode_row_key(&first_key).unwrap(),
                ttl_ms: 30_000,
                pending_status: None,
            }),
        );

        let first_error = state_machine.apply(participant.clone()).unwrap_err();
        assert!(matches!(
            first_error,
            TabletCommandApplyError::WriteConflict { .. }
        ));
        assert_eq!(state_machine.tablet().stats().locks, 1);
        assert_eq!(state_machine.tablet().stats().default_versions, 1);

        let reader = Transaction::new(TxnId(999), Timestamp(440)).unwrap();
        assert_eq!(
            state_machine.tablet().get(&reader, &first_key).unwrap(),
            None
        );

        assert_eq!(state_machine.apply(participant).unwrap_err(), first_error);
        assert_eq!(state_machine.tablet().stats().locks, 1);
        assert_eq!(state_machine.tablet().stats().default_versions, 1);
    }

    /// A semantically identical prewrite with a fresh request sequence must succeed
    /// without adding another intent or changing the participant's MVCC state.
    #[test]
    fn successful_prewrite_replay_with_fresh_request_sequence_is_idempotent() {
        let mut state_machine = state_machine();
        let key = make_row_key(TableId(9), &[Value::Int(77)]).unwrap();
        let encoded_key = encode_row_key(&key).unwrap();

        let prewrite = PrewriteCommand {
            txn_id: TxnId(126),
            start_timestamp: Timestamp(480),
            writes: vec![WriteEntry {
                key: encoded_key.clone(),
                row: Some(test_row(77, "fresh replay")),
                op: WriteKind::Put,
            }],
            primary_key: encoded_key,
            ttl_ms: 30_000,
            pending_status: None,
        };

        let first = state_machine
            .apply(command_envelope(
                1,
                TabletCommand::Prewrite(prewrite.clone()),
            ))
            .unwrap();

        assert_eq!(first.result, TabletCommandApplyResult::Prewrite);
        let state_after_first = state_machine.tablet().stats();

        // Sequence two is a new command request, so this exercises the storage
        // layer's semantic retry handling rather than request-ID result caching.
        let replay = state_machine
            .apply(command_envelope(2, TabletCommand::Prewrite(prewrite)))
            .unwrap();

        assert_eq!(replay.result, TabletCommandApplyResult::Prewrite);
        let state_after_replay = state_machine.tablet().stats();

        assert_eq!(
            state_after_replay.default_versions,
            state_after_first.default_versions
        );
        assert_eq!(state_after_replay.locks, state_after_first.locks);
        assert_eq!(
            state_after_replay.write_records,
            state_after_first.write_records
        );
        assert_eq!(state_after_replay.default_versions, 1);
        assert_eq!(state_after_replay.locks, 1);
        assert_eq!(state_after_replay.write_records, 0);
    }

    #[test]
    fn commit_resolves_prewrite_and_is_safe_to_replay() {
        let mut state_machine = state_machine();
        let key = make_row_key(TableId(9), &[Value::Int(8)]).unwrap();
        let encoded_key = encode_row_key(&key).unwrap();
        let row = test_row(8, "Edsger");

        state_machine
            .apply(command_envelope(
                1,
                TabletCommand::Prewrite(PrewriteCommand {
                    txn_id: TxnId(13),
                    start_timestamp: Timestamp(60),
                    writes: vec![WriteEntry {
                        key: encoded_key.clone(),
                        row: Some(row.clone()),
                        op: WriteKind::Put,
                    }],
                    primary_key: encoded_key.clone(),
                    ttl_ms: 30_000,
                    pending_status: None,
                }),
            ))
            .unwrap();

        let commit = CommitCommand {
            txn_id: TxnId(13),
            start_timestamp: Timestamp(60),
            commit_timestamp: Timestamp(70),
            keys: vec![encoded_key],
            committed_status: None,
        };

        let outcome = state_machine
            .apply(command_envelope(2, TabletCommand::Commit(commit.clone())))
            .unwrap();

        assert_eq!(outcome.result, TabletCommandApplyResult::Commit);
        assert_eq!(state_machine.tablet().stats().locks, 0);
        assert_eq!(state_machine.tablet().stats().write_records, 1);

        // A semantically identical command with a different request sequence
        // still must not create a duplicate physical MVCC version.
        let replay = state_machine
            .apply(command_envelope(3, TabletCommand::Commit(commit)))
            .unwrap();

        assert_eq!(replay.result, TabletCommandApplyResult::Commit);
        assert_eq!(state_machine.tablet().stats().write_records, 1);

        let reader = Transaction::new(TxnId(101), Timestamp(71)).unwrap();

        assert_eq!(
            state_machine.tablet().get(&reader, &key).unwrap(),
            Some(row)
        );
    }

    #[test]
    fn rollback_removes_intent_and_blocks_a_delayed_prewrite() {
        let mut state_machine = state_machine();
        let key = make_row_key(TableId(9), &[Value::Int(9)]).unwrap();
        let encoded_key = encode_row_key(&key).unwrap();

        let prewrite = PrewriteCommand {
            txn_id: TxnId(14),
            start_timestamp: Timestamp(80),
            writes: vec![WriteEntry {
                key: encoded_key.clone(),
                row: Some(test_row(9, "Barbara")),
                op: WriteKind::Put,
            }],
            primary_key: encoded_key.clone(),
            ttl_ms: 30_000,
            pending_status: None,
        };

        state_machine
            .apply(command_envelope(
                1,
                TabletCommand::Prewrite(prewrite.clone()),
            ))
            .unwrap();

        let outcome = state_machine
            .apply(command_envelope(
                2,
                TabletCommand::Rollback(RollbackCommand {
                    txn_id: TxnId(14),
                    start_timestamp: Timestamp(80),
                    keys: vec![encoded_key],
                }),
            ))
            .unwrap();

        assert_eq!(outcome.result, TabletCommandApplyResult::Rollback);
        assert_eq!(state_machine.tablet().stats().default_versions, 0);
        assert_eq!(state_machine.tablet().stats().locks, 0);
        assert_eq!(state_machine.tablet().stats().write_records, 1);

        let delayed = command_envelope(3, TabletCommand::Prewrite(prewrite));
        let error = state_machine.apply(delayed.clone()).unwrap_err();

        assert!(matches!(
            error,
            TabletCommandApplyError::WriteConflict { .. }
        ));

        // The deterministic rejection consumes sequence three and survives in
        // the replicated snapshot, so an exact retry cannot execute again.
        assert_eq!(state_machine.apply(delayed.clone()).unwrap_err(), error);
        let snapshot = state_machine.encode_snapshot_state().unwrap();
        let tablet = Tablet::new(LOCAL_TABLET_ID, TableId(9)).unwrap();
        let mut restored =
            TabletStateMachine::restore_from_snapshot_for_local_reference(tablet, &snapshot)
                .unwrap();
        assert_eq!(restored.apply(delayed).unwrap_err(), error);

        assert_eq!(
            state_machine
                .apply(noop_envelope_for_sequence(
                    LOCAL_TABLET_ID,
                    LOCAL_TABLET_EPOCH,
                    4,
                ))
                .unwrap()
                .result,
            TabletCommandApplyResult::Noop
        );
    }

    #[test]
    fn resolve_intent_applies_the_recorded_abort_outcome() {
        let mut state_machine = state_machine();
        let key = make_row_key(TableId(9), &[Value::Int(10)]).unwrap();
        let encoded_key = encode_row_key(&key).unwrap();

        state_machine
            .apply(command_envelope(
                1,
                TabletCommand::Prewrite(PrewriteCommand {
                    txn_id: TxnId(15),
                    start_timestamp: Timestamp(90),
                    writes: vec![WriteEntry {
                        key: encoded_key.clone(),
                        row: Some(test_row(10, "Margaret")),
                        op: WriteKind::Put,
                    }],
                    primary_key: encoded_key.clone(),
                    ttl_ms: 30_000,
                    pending_status: None,
                }),
            ))
            .unwrap();

        let outcome = state_machine
            .apply(command_envelope(
                2,
                TabletCommand::ResolveIntent(ResolveIntentCommand {
                    txn_id: TxnId(15),
                    start_timestamp: Timestamp(90),
                    keys: vec![encoded_key],
                    resolved_status: TxnStatus::Aborted,
                    commit_timestamp: None,
                }),
            ))
            .unwrap();

        assert_eq!(outcome.result, TabletCommandApplyResult::ResolveIntent);
        assert_eq!(state_machine.tablet().stats().default_versions, 0);
        assert_eq!(state_machine.tablet().stats().locks, 0);
        assert_eq!(state_machine.tablet().stats().write_records, 1);

        let reader = Transaction::new(TxnId(102), Timestamp(100)).unwrap();
        assert_eq!(state_machine.tablet().get(&reader, &key).unwrap(), None);
    }
}
