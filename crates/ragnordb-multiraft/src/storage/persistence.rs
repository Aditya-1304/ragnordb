//! ordered shared-WAL persistence for one Raft replica lifetime
//!
//! the writer appends entries first, configuration state next, and HardState
//! last. No logical view or stable state is published until the complete final
//! record extent has been synchronized through its exact end LSN

use raft::{
    entry::LogEntry,
    types::{ConfState, HardState},
};
use wal::{
    error::{AppendFailure, WalError},
    io::directory::SegmentDirectory,
    lsn::Lsn,
    types::{RecordType, record_types::USER_MIN},
    wal::{AppendResult, WalHandle},
};

use super::{
    codec::{
        RaftConfStateRecord, RaftHardStateRecord, RaftLogEntryCodecError, RaftLogEntryRecord,
        RaftReplicaIdentity, RaftStableStateCodecError,
    },
    view::{RaftLogViewError, RaftReplicaLogView},
};

const RAFT_LOG_ENTRY_RECORD_ID: u16 = USER_MIN + 8;
const RAFT_CONF_STATE_RECORD_ID: u16 = USER_MIN + 9;
const RAFT_HARD_STATE_RECORD_ID: u16 = USER_MIN + 10;

/// permanent user record identities reserved for Raft storage in shared A-WAL
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftWalRecordType {
    LogEntry,
    ConfState,
    HardState,
}

impl RaftWalRecordType {
    /// return the stable A-WAL record identifier for this payload schema
    pub const fn as_wal_record_type(self) -> RecordType {
        let id = match self {
            Self::LogEntry => RAFT_LOG_ENTRY_RECORD_ID,
            Self::ConfState => RAFT_CONF_STATE_RECORD_ID,
            Self::HardState => RAFT_HARD_STATE_RECORD_ID,
        };

        RecordType::new(id)
    }
}

/// minimal public A-WAL boundary required by Raft persistence
pub trait RaftWal {
    fn append(
        &mut self,
        record_type: RecordType,
        payload: &[u8],
    ) -> Result<AppendResult, AppendFailure>;

    fn sync_through(&mut self, end_lsn: Lsn) -> Result<(), WalError>;
}

impl<D, C> RaftWal for WalHandle<D, C>
where
    D: SegmentDirectory + Clone,
{
    fn append(
        &mut self,
        record_type: RecordType,
        payload: &[u8],
    ) -> Result<AppendResult, AppendFailure> {
        WalHandle::append(self, record_type, payload)
    }

    fn sync_through(&mut self, end_lsn: Lsn) -> Result<(), WalError> {
        WalHandle::sync_through(self, end_lsn)
    }
}

/// one logical persistence generation supplied by the future Ready loop
#[derive(Debug, Clone)]
pub struct RaftPersistenceBatch {
    pub entries: Vec<LogEntry<Vec<u8>>>,
    pub conf_state: Option<ConfState>,
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
            conf_state,
            hard_state,
        } = self.prepare_batch(batch)?;

        if records.is_empty() {
            return Ok(RaftPersistedBatch {
                start_lsn: None,
                end_lsn: None,
                record_count: 0,
            });
        }

        let mut extents = Vec::with_capacity(records.len());

        for record in &records {
            match self
                .wal
                .append(record.kind.as_wal_record_type(), &record.payload)
            {
                Ok(extent) => extents.push(extent),
                Err(failure) => {
                    return Err(self.map_append_failure(failure, &extents));
                }
            }
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

        if let Err(source) = self.wal.sync_through(end_lsn) {
            self.recovery_required = true;

            return Err(RaftPersistenceError::OutcomeUnknown {
                start_lsn,
                end_lsn,
                reason: source.to_string(),
            });
        }

        let mut durable_view = self.log_view.clone();

        for (record, extent) in entry_records.into_iter().zip(extents.iter()) {
            durable_view
                .replay(record, extent.start_lsn)
                .map_err(|error| {
                    self.recovery_required = true;
                    RaftPersistenceError::PostSyncInvariant(error.to_string())
                })?;
        }

        self.log_view = durable_view;

        if let Some(conf_state) = conf_state {
            self.conf_state = Some(conf_state);
        }

        if let Some(hard_state) = hard_state {
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

        let conf_state = if let Some(conf_state) = batch.conf_state {
            let record = RaftConfStateRecord::from_core(self.identity, conf_state.clone())?;

            records.push(PreparedRecord {
                kind: RaftWalRecordType::ConfState,
                payload: record.encode()?,
            });

            Some(conf_state)
        } else {
            None
        };

        let hard_state = if let Some(hard_state) = batch.hard_state {
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
            conf_state,
            hard_state,
        })
    }

    fn map_append_failure(
        &mut self,
        failure: AppendFailure,
        prior_extents: &[AppendResult],
    ) -> RaftPersistenceError {
        match failure {
            AppendFailure::NotStaged(source) if prior_extents.is_empty() => {
                let recovery_required = source.requires_recovery();

                if recovery_required {
                    self.recovery_required = true;
                }

                RaftPersistenceError::NotStaged {
                    recovery_required,
                    reason: source.to_string(),
                }
            }

            AppendFailure::NotStaged(source) => {
                self.recovery_required = true;

                let first = prior_extents.first().expect("checked non-empty");
                let last = prior_extents.last().expect("checked non-empty");

                RaftPersistenceError::OutcomeUnknown {
                    start_lsn: first.start_lsn,
                    end_lsn: last.end_lsn,
                    reason: source.to_string(),
                }
            }

            AppendFailure::OutcomeUnknown { extent, source } => {
                self.recovery_required = true;

                let start_lsn = prior_extents
                    .first()
                    .map(|prior| prior.start_lsn)
                    .unwrap_or(extent.start_lsn);

                RaftPersistenceError::OutcomeUnknown {
                    start_lsn,
                    end_lsn: extent.end_lsn,
                    reason: source.to_string(),
                }
            }
        }
    }
}

struct PreparedRecord {
    kind: RaftWalRecordType,
    payload: Vec<u8>,
}

struct PreparedBatch {
    records: Vec<PreparedRecord>,
    entry_records: Vec<RaftLogEntryRecord>,
    conf_state: Option<ConfState>,
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
}
