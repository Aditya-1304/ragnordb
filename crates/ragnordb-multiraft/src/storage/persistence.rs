//! ordered shared-WAL persistence for one Raft replica lifetime
//!
//! snapshot pointers precede dependent entries, and HardState is always last.
//! No logical state is published until A-WAL synchronizes the complete batch.

use raft::{
    entry::LogEntry,
    types::{ConfState, HardState},
};
use std::sync::{Arc, Mutex};
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
    view::{RaftLogViewError, RaftReplicaLogView},
};
use ragnordb_common::wal_registry::SharedWalRecordType;

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

/// minimal public A-WAL boundary required by Raft persistence
pub trait RaftWal {
    fn append_batch_and_sync(
        &mut self,
        records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure>;
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
}

impl<W> NodeRaftWal<W> {
    pub fn new(wal: W) -> Self {
        Self {
            state: Arc::new(Mutex::new(NodeRaftWalState {
                wal,
                recovery_required: false,
            })),
        }
    }

    pub fn group_writer(&self) -> NodeRaftWalHandle<W> {
        NodeRaftWalHandle {
            state: Arc::clone(&self.state),
        }
    }

    pub fn recovery_required(&self) -> bool {
        self.state
            .lock()
            .map(|state| state.recovery_required)
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
}

impl<W> Clone for NodeRaftWalHandle<W> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
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
        if state.recovery_required {
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
        }
        result
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
    durable_end_lsn: Option<Lsn>,
    recovery_required: bool,
    snapshot: Option<RaftSnapshotPointerRecord>,
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
            durable_end_lsn: None,
            recovery_required: false,
            snapshot: None,
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
        }

        if let Some(hard_state) = hard_state {
            self.log_view
                .advance_commit(hard_state.commit)
                .map_err(|error| {
                    self.recovery_required = true;
                    RaftPersistenceError::PostSyncInvariant(error.to_string())
                })?;
            self.hard_state = Some(hard_state);
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

        Ok(PreparedBatch {
            records,
            entry_records,
            snapshot,
            hard_state,
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
}
