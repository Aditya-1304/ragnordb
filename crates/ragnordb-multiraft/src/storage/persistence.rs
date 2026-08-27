//! ordered shared-WAL persistence for one Raft replica lifetime
//!
//! snapshot pointers precede dependent entries, and HardState is always last.
//! No logical state is published until A-WAL synchronizes the complete batch.

use raft::{
    entry::LogEntry,
    types::{ConfState, HardState},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use wal::{
    error::{BatchAppendFailure, WalError},
    io::directory::SegmentDirectory,
    lsn::Lsn,
    types::RecordType,
    wal::{BatchAppendResult, WalHandle},
};

use super::{
    codec::{
        RaftHardStateRecord, RaftLogEntryCodecError, RaftLogEntryRecord, RaftReplicaIdentity,
        RaftSnapshotPointerRecord, RaftStableStateCodecError, SnapshotTransitionError,
        validate_hard_state_successor, validate_snapshot_successor,
    },
    recovery::RecoveredRaftReplica,
    view::{RaftLogViewError, RaftReplicaLogView},
};
use ragnordb_common::{
    durability::{DurabilityFailureKind, DurabilityGate},
    wal_registry::SharedWalRecordType,
};

/// permanent user record identities reserved for Raft storage in shared A-WAL
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftWalRecordType {
    LogEntry,
    HardState,
    SnapshotPointer,
}

impl RaftWalRecordType {
    /// return the stable A-WAL record identifier for this payload schema
    pub const fn as_wal_record_type(self) -> RecordType {
        let id = match self {
            Self::LogEntry => SharedWalRecordType::RaftLogEntry
                .as_wal_record_type()
                .as_u16(),
            Self::HardState => SharedWalRecordType::RaftHardState
                .as_wal_record_type()
                .as_u16(),
            Self::SnapshotPointer => SharedWalRecordType::RaftSnapshotPointer
                .as_wal_record_type()
                .as_u16(),
        };

        RecordType::new(id)
    }

    /// classify a shared WAL record without claiming unrelated user records
    pub const fn from_wal_record_type(record_type: RecordType) -> Option<Self> {
        match SharedWalRecordType::classify(record_type) {
            Some(SharedWalRecordType::RaftLogEntry) => Some(Self::LogEntry),
            Some(SharedWalRecordType::RaftHardState) => Some(Self::HardState),
            Some(SharedWalRecordType::RaftSnapshotPointer) => Some(Self::SnapshotPointer),
            _ => None,
        }
    }
}

/// Opaque lifetime guard for one shared-WAL retention pin.
///
/// The database layer intentionally does not depend on the sibling WAL
/// implementation's concrete guard type. Holding this value keeps the
/// corresponding physical WAL prefix protected until the snapshot boundary
/// operation has completed.
pub trait RaftWalRetentionPin: std::fmt::Debug {}

impl<T: std::fmt::Debug> RaftWalRetentionPin for T {}

/// Minimal public A-WAL boundary required by Raft persistence.
pub trait RaftWal {
    fn append_batch_and_sync(
        &mut self,
        records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure>;

    /// protect the WAL prefix needed by a snapshot or follower catch-up
    /// operation. Lightweight test WALs use the default no op guard
    fn acquire_retention_pin(
        &self,
        _holder_name: &str,
        _min_lsn: Lsn,
    ) -> Result<Box<dyn RaftWalRetentionPin>, String> {
        Ok(Box::new(()))
    }

    /// prune physical WAL segments after a node-wide owner has established
    /// that every registered Raft group has advanced beyond `floor`.
    ///
    /// The default is deliberately inert for lightweight test WALs and for
    /// storage implementations that do not own physical segment reclamation.
    fn prune_before(&mut self, _floor: Lsn) -> Result<usize, String> {
        Ok(0)
    }
}

impl<D, C> RaftWal for WalHandle<D, C>
where
    D: SegmentDirectory + Clone,
{
    fn append_batch_and_sync(
        &mut self,
        records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure> {
        WalHandle::append_batch_and_sync(self, records)
    }

    fn acquire_retention_pin(
        &self,
        holder_name: &str,
        min_lsn: Lsn,
    ) -> Result<Box<dyn RaftWalRetentionPin>, String> {
        WalHandle::acquire_retention_pin(self, holder_name, min_lsn)
            .map(|guard| Box::new(guard) as Box<dyn RaftWalRetentionPin>)
            .map_err(|error| error.to_string())
    }

    fn prune_before(&mut self, floor: Lsn) -> Result<usize, String> {
        WalHandle::set_min_retention_lsn(self, floor).map_err(|error| error.to_string())?;
        WalHandle::truncate_segments_before(self, floor).map_err(|error| error.to_string())
    }
}

/// Node-wide owner of the single serialized Raft persistence boundary.
///
/// Every group receives a lightweight handle to this owner. An uncertain batch
/// outcome permanently fences all handles until restart and shared recovery.
pub struct NodeRaftWal<W> {
    state: Arc<Mutex<NodeRaftWalState<W>>>,
}

struct NodeRaftWalState<W> {
    wal: W,
    recovery_required: bool,
    durability_gate: Option<DurabilityGate>,
    database_retention_floor: Option<Lsn>,
    retention_floors: BTreeMap<RaftReplicaIdentity, Option<Lsn>>,
    retention_registry_sealed: bool,
    last_pruned_floor: Lsn,
}

/// Refresh the node-wide recovery fence from every authority that can observe
/// uncertainty in the physically shared A-WAL.
///
/// Raft is not the only writer of A-WAL. Database commits, catalog-cache
/// publication, checkpoints, and retention operations share the same durable
/// prefix. Once the common DurabilityGate is fenced, every Raft writer must
/// stop even if the uncertainty was first observed outside NodeRaftWal.
fn refresh_recovery_fence<W>(state: &mut NodeRaftWalState<W>) -> bool {
    if state.recovery_required {
        return true;
    }

    if state
        .durability_gate
        .as_ref()
        .is_some_and(|gate| !gate.is_healthy())
    {
        state.recovery_required = true;
    }

    state.recovery_required
}

impl<W> NodeRaftWal<W> {
    pub fn new(wal: W) -> Self {
        Self {
            state: Arc::new(Mutex::new(NodeRaftWalState {
                wal,
                recovery_required: false,
                durability_gate: None,
                database_retention_floor: None,
                retention_floors: BTreeMap::new(),
                retention_registry_sealed: false,
                last_pruned_floor: Lsn::ZERO,
            })),
        }
    }

    /// Construct the shared WAL owner with the database durability gate that
    /// must be fenced whenever a Raft append has an uncertain outcome.
    pub fn with_durability_gate(wal: W, durability_gate: DurabilityGate) -> Self {
        let owner = Self::new(wal);
        owner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .durability_gate = Some(durability_gate);
        owner
    }

    /// Publish the database checkpoint replay floor to the same node-wide
    /// retention owner used by every Raft replica.
    pub fn advance_database_retention(&self, floor: Lsn) -> Result<usize, String>
    where
        W: RaftWal,
    {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "node-wide Raft WAL lock is poisoned".to_string())?;
        if refresh_recovery_fence(&mut state) {
            return Err("the shared node durability gate requires recovery".to_string());
        }
        if !state.retention_registry_sealed {
            return Err("retention registry must be sealed before pruning".to_string());
        }
        if state
            .database_retention_floor
            .is_some_and(|previous| floor < previous)
        {
            return Err("database retention floor cannot move backwards".to_string());
        }
        state.database_retention_floor = Some(floor);
        prune_to_slowest_floor(&mut state)
    }

    /// register one replica lifetime before a shared-WAL retention pass.
    ///
    /// Registration is explicit because a shared WAL can contain groups that
    /// are not currently active in a local runtime. Such groups must remain
    /// part of the minimum-floor calculation until recovery has reconstructed
    /// the complete local group set.
    pub fn group_writer_for(
        &self,
        identity: RaftReplicaIdentity,
    ) -> Result<NodeRaftWalHandle<W>, String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "node-wide Raft WAL lock is poisoned".to_string())?;
        if refresh_recovery_fence(&mut state) {
            return Err("the shared node durability gate requires recovery".to_string());
        }
        if state.retention_registry_sealed {
            return Err(
                "cannot register a Raft group after retention registry sealing".to_string(),
            );
        }
        state.retention_floors.entry(identity).or_insert(None);
        Ok(NodeRaftWalHandle {
            state: Arc::clone(&self.state),
            owner: Some(identity),
        })
    }

    /// seal registration after shared recovery has discovered every local
    /// Raft replica lifetime that may be represented in candidate segments.
    pub fn seal_retention_registry(&self) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "node-wide Raft WAL lock is poisoned".to_string())?;
        if refresh_recovery_fence(&mut state) {
            return Err("the shared node durability gate requires recovery".to_string());
        }
        state.retention_registry_sealed = true;
        Ok(())
    }

    pub fn recovery_required(&self) -> bool {
        self.state
            .lock()
            .map(|mut state| refresh_recovery_fence(&mut state))
            .unwrap_or(true)
    }
}

impl<W> Clone for NodeRaftWal<W> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

pub struct NodeRaftWalHandle<W> {
    state: Arc<Mutex<NodeRaftWalState<W>>>,
    owner: Option<RaftReplicaIdentity>,
}

impl<W> Clone for NodeRaftWalHandle<W> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            owner: self.owner,
        }
    }
}

impl<W: RaftWal> RaftWal for NodeRaftWalHandle<W> {
    fn append_batch_and_sync(
        &mut self,
        records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| BatchAppendFailure::NotStaged(WalError::BrokenDurabilityContract))?;
        if refresh_recovery_fence(&mut state) {
            return Err(BatchAppendFailure::NotStaged(
                WalError::BrokenDurabilityContract,
            ));
        }
        let result = state.wal.append_batch_and_sync(records);
        if matches!(result, Err(BatchAppendFailure::OutcomeUnknown { .. }))
            || result
                .as_ref()
                .err()
                .is_some_and(|error| error.wal_error().requires_recovery())
        {
            state.recovery_required = true;
            if let Some(gate) = &state.durability_gate {
                gate.require_recovery(
                    DurabilityFailureKind::RecoveryRequired,
                    "shared A-WAL Raft persistence outcome is uncertain",
                );
            }
        }
        result
    }

    fn acquire_retention_pin(
        &self,
        holder_name: &str,
        min_lsn: Lsn,
    ) -> Result<Box<dyn RaftWalRetentionPin>, String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "node-wide Raft WAL lock is poisoned".to_string())?;

        state.wal.acquire_retention_pin(holder_name, min_lsn)
    }

    fn prune_before(&mut self, floor: Lsn) -> Result<usize, String> {
        let owner = self.owner.ok_or_else(|| {
            "retention pruning requires an identity-bound group writer".to_string()
        })?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| "node-wide Raft WAL lock is poisoned".to_string())?;

        if refresh_recovery_fence(&mut state) {
            return Err("the shared node durability gate requires recovery".to_string());
        }
        if !state.retention_registry_sealed {
            return Err("retention registry must be sealed before pruning".to_string());
        }

        let registered_floor = state
            .retention_floors
            .get_mut(&owner)
            .ok_or_else(|| "group writer is not registered for retention pruning".to_string())?;
        if registered_floor.is_some_and(|previous| floor < previous) {
            return Err("Raft retention floor cannot move backwards".to_string());
        }
        *registered_floor = Some(floor);

        prune_to_slowest_floor(&mut state)
    }
}

fn prune_to_slowest_floor<W: RaftWal>(state: &mut NodeRaftWalState<W>) -> Result<usize, String> {
    let Some(mut floors) = state
        .retention_floors
        .values()
        .copied()
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(0);
    };
    if let Some(database_floor) = state.database_retention_floor {
        floors.push(database_floor);
    }
    let Some(slowest_floor) = floors.into_iter().min() else {
        return Ok(0);
    };
    if slowest_floor <= state.last_pruned_floor {
        return Ok(0);
    }
    match state.wal.prune_before(slowest_floor) {
        Ok(removed) => {
            state.last_pruned_floor = slowest_floor;
            Ok(removed)
        }
        Err(reason) => {
            // Physical pruning may have removed a safe prefix before reporting
            // failure. The current fail-stop model cannot prove the resulting
            // on-disk boundary, so every shared-WAL user must stop immediately.
            state.recovery_required = true;
            if let Some(gate) = &state.durability_gate {
                gate.require_recovery(
                    DurabilityFailureKind::RecoveryRequired,
                    format!(
                        "shared A-WAL retention mutation failed at floor {}: {reason}",
                        slowest_floor.as_u64()
                    ),
                );
            }
            Err(reason)
        }
    }
}

/// one logical persistence generation supplied by the future Ready loop
#[derive(Debug, Clone)]
pub struct RaftPersistenceBatch {
    /// Snapshot file pointer, already synchronized before this WAL batch.
    pub snapshot: Option<RaftSnapshotPointerRecord>,
    pub entries: Vec<LogEntry<Vec<u8>>>,
    pub hard_state: Option<HardState>,
}

/// exact durable interval and record count for one successful batch
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftPersistedBatch {
    pub start_lsn: Option<Lsn>,
    pub end_lsn: Option<Lsn>,
    pub record_count: usize,
}

/// durable storage owner for one Raft replica lifetime
pub struct RaftWalStorage<W> {
    wal: W,
    identity: RaftReplicaIdentity,
    log_view: RaftReplicaLogView,
    conf_state: Option<ConfState>,
    hard_state: Option<HardState>,
    hard_state_lsn: Option<Lsn>,
    durable_end_lsn: Option<Lsn>,
    recovery_required: bool,
    snapshot: Option<RaftSnapshotPointerRecord>,
    snapshot_pointer_lsn: Option<Lsn>,
}

impl<W: RaftWal> RaftWalStorage<W> {
    /// bind one WAL writer to exactly one group and replica lifetime
    pub fn new(wal: W, identity: RaftReplicaIdentity) -> Self {
        Self {
            wal,
            identity,
            log_view: RaftReplicaLogView::new(identity),
            conf_state: None,
            hard_state: None,
            hard_state_lsn: None,
            durable_end_lsn: None,
            recovery_required: false,
            snapshot: None,
            snapshot_pointer_lsn: None,
        }
    }

    pub fn wal(&self) -> &W {
        &self.wal
    }

    pub fn log_view(&self) -> &RaftReplicaLogView {
        &self.log_view
    }

    pub fn conf_state(&self) -> Option<&ConfState> {
        self.conf_state.as_ref()
    }

    pub fn hard_state(&self) -> Option<&HardState> {
        self.hard_state.as_ref()
    }

    pub fn snapshot(&self) -> Option<&RaftSnapshotPointerRecord> {
        self.snapshot.as_ref()
    }

    pub fn durable_end_lsn(&self) -> Option<Lsn> {
        self.durable_end_lsn
    }

    /// Return the oldest physical record required to reconstruct this replica.
    ///
    /// Snapshot files are not discoverable recovery authorities without their
    /// WAL pointer. Retention must therefore preserve the pointer, stable state,
    /// and retained suffix rather than using the end of durable storage.
    pub fn minimum_recovery_lsn(&self) -> Option<Lsn> {
        [
            self.snapshot_pointer_lsn,
            self.hard_state_lsn,
            self.log_view.first_retained_lsn(),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// retain the physical WAL prefix required by this replica while a
    /// snapshot boundary or follower catch-up operation is in flight
    pub fn acquire_retention_pin(
        &self,
        holder_name: &str,
        min_lsn: Lsn,
    ) -> Result<Box<dyn RaftWalRetentionPin>, String> {
        self.wal.acquire_retention_pin(holder_name, min_lsn)
    }

    /// release the WAL prefix below a snapshot boundary after every local
    /// group has supplied its durable retention floor.
    pub fn release_retention(&mut self, floor: Lsn) -> Result<usize, String> {
        self.wal.prune_before(floor)
    }

    pub fn recovery_required(&self) -> bool {
        self.recovery_required
    }

    /// persist one ordered generation and publish it only after exact sync
    pub fn persist(
        &mut self,
        batch: RaftPersistenceBatch,
    ) -> Result<RaftPersistedBatch, RaftPersistenceError> {
        if self.recovery_required {
            return Err(RaftPersistenceError::RecoveryRequired);
        }

        let PreparedBatch {
            records,
            entry_records,
            snapshot,
            hard_state,
        } = self.prepare_batch(batch)?;

        if records.is_empty() {
            return Ok(RaftPersistedBatch {
                start_lsn: None,
                end_lsn: None,
                record_count: 0,
            });
        }

        let borrowed_records: Vec<_> = records
            .iter()
            .map(|record| (record.kind.as_wal_record_type(), record.payload.as_slice()))
            .collect();
        let extents = match self.wal.append_batch_and_sync(&borrowed_records) {
            Ok(extents) => extents,
            Err(BatchAppendFailure::NotStaged(source)) => {
                let recovery_required = source.requires_recovery();
                self.recovery_required |= recovery_required;
                return Err(RaftPersistenceError::NotStaged {
                    recovery_required,
                    reason: source.to_string(),
                });
            }
            Err(BatchAppendFailure::OutcomeUnknown { result, source }) => {
                self.recovery_required = true;
                return Err(RaftPersistenceError::OutcomeUnknown {
                    start_lsn: result
                        .record_extents
                        .first()
                        .map(|extent| extent.start_lsn)
                        .unwrap_or(Lsn::ZERO),
                    end_lsn: result.final_end_lsn,
                    reason: source.to_string(),
                });
            }
        };
        let extents = extents.record_extents;

        if extents.len() != records.len() {
            self.recovery_required = true;
            return Err(RaftPersistenceError::PostSyncInvariant(
                "A-WAL returned a different extent count for a successful batch".to_string(),
            ));
        }

        let start_lsn = extents.first().map(|extent| extent.start_lsn).ok_or(
            RaftPersistenceError::InternalInvariant(
                "non-empty persistence batch produced no WAL extents",
            ),
        )?;

        let end_lsn = extents.last().map(|extent| extent.end_lsn).ok_or(
            RaftPersistenceError::InternalInvariant(
                "non-empty persistence batch produced no final WAL extent",
            ),
        )?;

        let mut durable_view = self.log_view.clone();
        let mut extent_offset = 0;
        let snapshot_pointer_lsn = snapshot.as_ref().map(|_| extents[0].start_lsn);
        let hard_state_lsn = hard_state
            .as_ref()
            .and_then(|_| extents.last().map(|extent| extent.start_lsn));

        if let Some(snapshot) = &snapshot {
            durable_view
                .install_snapshot(
                    snapshot.last_included_index,
                    snapshot.last_included_term,
                    extents[0].start_lsn,
                )
                .map_err(|error| {
                    self.recovery_required = true;
                    RaftPersistenceError::PostSyncInvariant(error.to_string())
                })?;
            extent_offset = 1;
        }

        for (record, extent) in entry_records
            .into_iter()
            .zip(extents.iter().skip(extent_offset))
        {
            durable_view
                .replay(record, extent.start_lsn)
                .map_err(|error| {
                    self.recovery_required = true;
                    RaftPersistenceError::PostSyncInvariant(error.to_string())
                })?;
        }

        self.log_view = durable_view;

        if let Some(snapshot) = snapshot {
            self.conf_state = Some(snapshot.conf_state.clone());
            self.hard_state = Some(match self.hard_state.take() {
                Some(mut state) if state.current_term >= snapshot.last_included_term => {
                    state.commit = state.commit.max(snapshot.last_included_index);
                    state
                }
                Some(_) | None => HardState {
                    current_term: snapshot.last_included_term,
                    voted_for: None,
                    commit: snapshot.last_included_index,
                },
            });
            self.snapshot = Some(snapshot);
            self.snapshot_pointer_lsn = snapshot_pointer_lsn;
        }

        if let Some(hard_state) = hard_state {
            self.log_view
                .advance_commit(hard_state.commit)
                .map_err(|error| {
                    self.recovery_required = true;
                    RaftPersistenceError::PostSyncInvariant(error.to_string())
                })?;
            self.hard_state = Some(hard_state);
            self.hard_state_lsn = hard_state_lsn;
        }

        self.durable_end_lsn = Some(end_lsn);

        Ok(RaftPersistedBatch {
            start_lsn: Some(start_lsn),
            end_lsn: Some(end_lsn),
            record_count: records.len(),
        })
    }

    fn prepare_batch(
        &self,
        batch: RaftPersistenceBatch,
    ) -> Result<PreparedBatch, RaftPersistenceError> {
        if batch.snapshot.is_some() && batch.hard_state.is_none() {
            return Err(RaftPersistenceError::SnapshotWithoutHardState);
        }

        let mut records = Vec::new();
        let mut entry_records = Vec::with_capacity(batch.entries.len());
        let mut preview = self.log_view.clone();

        let mut preview_lsn = preview.last_replayed_lsn().unwrap_or(Lsn::ZERO).as_u64();

        let snapshot = if let Some(snapshot) = batch.snapshot {
            snapshot.validate()?;
            if snapshot.identity != self.identity {
                return Err(RaftPersistenceError::SnapshotIdentityMismatch {
                    expected: self.identity,
                    received: snapshot.identity,
                });
            }
            validate_snapshot_successor(self.snapshot.as_ref(), &snapshot)?;
            preview_lsn = preview_lsn
                .checked_add(1)
                .ok_or(RaftPersistenceError::PreviewLsnExhausted)?;
            preview.install_snapshot(
                snapshot.last_included_index,
                snapshot.last_included_term,
                Lsn::new(preview_lsn),
            )?;
            records.push(PreparedRecord {
                kind: RaftWalRecordType::SnapshotPointer,
                payload: snapshot.encode()?,
            });
            Some(snapshot)
        } else {
            None
        };

        for entry in batch.entries {
            preview_lsn = preview_lsn
                .checked_add(1)
                .ok_or(RaftPersistenceError::PreviewLsnExhausted)?;

            let record = RaftLogEntryRecord::from_core(self.identity, entry)?;

            preview.replay(record.clone(), Lsn::new(preview_lsn))?;

            records.push(PreparedRecord {
                kind: RaftWalRecordType::LogEntry,
                payload: record.encode()?,
            });

            entry_records.push(record);
        }

        let hard_state = if let Some(hard_state) = batch.hard_state {
            validate_hard_state_successor(self.hard_state.as_ref(), &hard_state)?;
            if let Some(snapshot) = &snapshot {
                if hard_state.current_term < snapshot.last_included_term {
                    return Err(RaftPersistenceError::HardStateBeforeSnapshotTerm {
                        current_term: hard_state.current_term,
                        snapshot_term: snapshot.last_included_term,
                    });
                }
                if hard_state.commit < snapshot.last_included_index {
                    return Err(RaftPersistenceError::HardStateBeforeSnapshotCommit {
                        commit_index: hard_state.commit,
                        snapshot_index: snapshot.last_included_index,
                    });
                }
            }
            if hard_state.commit > preview.last_index().unwrap_or(0) {
                return Err(RaftPersistenceError::CommitBeyondLog {
                    commit_index: hard_state.commit,
                    last_log_index: preview.last_index().unwrap_or(0),
                });
            }

            let record = RaftHardStateRecord::from_core(self.identity, hard_state.clone())?;

            records.push(PreparedRecord {
                kind: RaftWalRecordType::HardState,
                payload: record.encode()?,
            });

            Some(hard_state)
        } else {
            None
        };

        let maximum_log_term = preview
            .entries()
            .map(|entry| entry.record.term)
            .max()
            .unwrap_or(0);
        let snapshot_term = preview
            .snapshot_boundary()
            .map(|(_, term)| term)
            .unwrap_or(0);
        let maximum_observed_term = maximum_log_term.max(snapshot_term);
        let effective_current_term = hard_state
            .as_ref()
            .map(|state| state.current_term)
            .or_else(|| self.hard_state.as_ref().map(|state| state.current_term))
            .unwrap_or(0)
            .max(snapshot_term);

        if effective_current_term < maximum_observed_term {
            return Err(RaftPersistenceError::HardStateBeforeLogTerm {
                current_term: effective_current_term,
                maximum_log_term: maximum_observed_term,
            });
        }

        Ok(PreparedBatch {
            records,
            entry_records,
            snapshot,
            hard_state,
        })
    }

    /// reconstruct the live persistence writer from one fully recovered replica
    ///
    /// the WAL frontier is supplied explicitly by startup because a replica's
    /// latest Raft record is not necessarily the node's latest physical WAL
    /// record. Other Raft groups and database records may be interleaved in the
    /// shared WAL
    pub fn from_recovered(
        wal: W,
        recovered: &RecoveredRaftReplica,
        durable_end_lsn: Lsn,
    ) -> Result<Self, RaftPersistenceError> {
        if let Some(last_replayed_lsn) = recovered.log_view().last_replayed_lsn()
            && durable_end_lsn <= last_replayed_lsn
        {
            return Err(RaftPersistenceError::RecoveredFrontierBeforeLastRecord {
                durable_end_lsn,
                last_replayed_lsn,
            });
        }

        Ok(Self {
            wal,
            identity: recovered.identity(),
            log_view: recovered.log_view().clone(),
            conf_state: recovered.conf_state().cloned(),
            hard_state: recovered.hard_state().cloned(),
            hard_state_lsn: recovered.hard_state_lsn(),
            durable_end_lsn: Some(durable_end_lsn),
            recovery_required: false,
            snapshot: recovered.snapshot().cloned(),
            snapshot_pointer_lsn: recovered.snapshot_pointer_lsn(),
        })
    }
}

struct PreparedRecord {
    kind: RaftWalRecordType,
    payload: Vec<u8>,
}

struct PreparedBatch {
    records: Vec<PreparedRecord>,
    entry_records: Vec<RaftLogEntryRecord>,
    snapshot: Option<RaftSnapshotPointerRecord>,
    hard_state: Option<HardState>,
}

/// failure while preparing, appending, or synchronizing one Raft generation
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RaftPersistenceError {
    #[error("Raft WAL storage requires restart and recovery")]
    RecoveryRequired,

    #[error("a durable Raft snapshot pointer requires HardState in the same batch")]
    SnapshotWithoutHardState,

    #[error("invalid Raft log entry: {0}")]
    InvalidLogEntry(#[from] RaftLogEntryCodecError),

    #[error("invalid Raft stable state: {0}")]
    InvalidStableState(#[from] RaftStableStateCodecError),

    #[error("invalid Raft snapshot transition: {0}")]
    InvalidSnapshotTransition(#[from] SnapshotTransitionError),

    #[error("invalid Raft log transition: {0}")]
    InvalidLogTransition(#[from] RaftLogViewError),

    #[error(
        "HardState commit index {commit_index} exceeds durable log index \
         {last_log_index}"
    )]
    CommitBeyondLog {
        commit_index: u64,
        last_log_index: u64,
    },

    #[error("preview LSN space is exhausted")]
    PreviewLsnExhausted,

    #[error("Raft WAL append was not staged: {reason}")]
    NotStaged {
        recovery_required: bool,
        reason: String,
    },

    #[error(
        "Raft WAL persistence outcome is unknown for \
         [{start_lsn:?}, {end_lsn:?}): {reason}"
    )]
    OutcomeUnknown {
        start_lsn: Lsn,
        end_lsn: Lsn,
        reason: String,
    },

    #[error("durable Raft batch violated a prevalidated invariant: {0}")]
    PostSyncInvariant(String),

    #[error("internal Raft persistence invariant failed: {0}")]
    InternalInvariant(&'static str),

    #[error("Raft snapshot belongs to {received:?}, but storage owns {expected:?}")]
    SnapshotIdentityMismatch {
        expected: RaftReplicaIdentity,
        received: RaftReplicaIdentity,
    },

    #[error("HardState term {current_term} is below snapshot term {snapshot_term}")]
    HardStateBeforeSnapshotTerm {
        current_term: u64,
        snapshot_term: u64,
    },

    #[error("HardState commit {commit_index} is below snapshot index {snapshot_index}")]
    HardStateBeforeSnapshotCommit {
        commit_index: u64,
        snapshot_index: u64,
    },

    #[error(
        "HardState term {current_term} is below the maximum durable Raft term +         {maximum_log_term}"
    )]
    HardStateBeforeLogTerm {
        current_term: u64,
        maximum_log_term: u64,
    },

    #[error(
        "recovered WAL frontier {durable_end_lsn:?} does not cover \
     the last Raft record at {last_replayed_lsn:?}"
    )]
    RecoveredFrontierBeforeLastRecord {
        durable_end_lsn: Lsn,
        last_replayed_lsn: Lsn,
    },
}
