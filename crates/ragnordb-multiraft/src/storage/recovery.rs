//! one pass reconstruction of every Raft replica lifetime in the shared WAL
//!
//! recovery consumes a single forward-only cursor. Records are decoded first
//! and then routed by their complete `(raft_group_id, replica_id)` identity so
//! reused log indexes from different replica lifetimes can never collide

use std::collections::BTreeMap;

use raft::types::{ConfState, HardState};
use wal::{
    error::WalError,
    io::segment_file::SegmentFile,
    lsn::Lsn,
    wal::iterator::{WalIterator, WalRecord},
};

use super::{
    codec::{
        RaftConfStateRecord, RaftHardStateRecord, RaftLogEntryCodecError, RaftLogEntryRecord,
        RaftReplicaIdentity, RaftStableStateCodecError,
    },
    persistence::RaftWalRecordType,
    view::{RaftLogViewError, RaftReplicaLogView},
};

/// forward only record source used by shared WAL recovery
///
/// the abstraction keeps recovery deterministic in tests while the production
/// implementation consumes A-WAL's public snapshot iterator directly
pub trait RaftWalRecoverySource {
    fn next_record(&mut self) -> Result<Option<WalRecord>, WalError>;
}

impl<F: SegmentFile> RaftWalRecoverySource for WalIterator<F> {
    fn next_record(&mut self) -> Result<Option<WalRecord>, WalError> {
        self.next()
    }
}

/// recovered durable state for exactly one Raft replica lifetime
#[derive(Debug)]
pub struct RecoveredRaftReplica {
    log_view: RaftReplicaLogView,
    conf_state: Option<ConfState>,
    hard_state: Option<HardState>,
}

impl RecoveredRaftReplica {
    fn new(identity: RaftReplicaIdentity) -> Self {
        Self {
            log_view: RaftReplicaLogView::new(identity),
            conf_state: None,
            hard_state: None,
        }
    }

    /// return the immutable identity owning this recovered state
    pub fn identity(&self) -> RaftReplicaIdentity {
        self.log_view.identity()
    }

    /// return the reconstructed identity-scoped log view
    pub fn log_view(&self) -> &RaftReplicaLogView {
        &self.log_view
    }

    /// return the latest valid membership record for this lifetime
    pub fn conf_state(&self) -> Option<&ConfState> {
        self.conf_state.as_ref()
    }

    /// return the latest valid election and commit state
    pub fn hard_state(&self) -> Option<&HardState> {
        self.hard_state.as_ref()
    }
}

/// every Raft replica lifetime reconstructed by one shared WAL scan
#[derive(Debug, Default)]
pub struct RecoveredRaftStorage {
    replicas: BTreeMap<RaftReplicaIdentity, RecoveredRaftReplica>,
    scanned_records: usize,
    last_scanned_lsn: Option<Lsn>,
}

impl RecoveredRaftStorage {
    /// return the number of distinct recovered replica lifetimes
    pub fn len(&self) -> usize {
        self.replicas.len()
    }

    /// return whether no Raft-owned records were recovered
    pub fn is_empty(&self) -> bool {
        self.replicas.is_empty()
    }

    /// look up state using the complete replica-lifetime identity
    pub fn replica(&self, identity: RaftReplicaIdentity) -> Option<&RecoveredRaftReplica> {
        self.replicas.get(&identity)
    }

    /// iterate over all recovered replica lifetimes
    pub fn replicas(&self) -> impl Iterator<Item = (&RaftReplicaIdentity, &RecoveredRaftReplica)> {
        self.replicas.iter()
    }

    /// return the number of shared-WAL records consumed
    pub fn scanned_records(&self) -> usize {
        self.scanned_records
    }

    /// return the final LSN consumed from the shared WAL stream
    pub fn last_scanned_lsn(&self) -> Option<Lsn> {
        self.last_scanned_lsn
    }

    fn replica_mut(&mut self, identity: RaftReplicaIdentity) -> &mut RecoveredRaftReplica {
        self.replicas
            .entry(identity)
            .or_insert_with(|| RecoveredRaftReplica::new(identity))
    }
}

///  every Raft-owned record encountered by one forward WAL scan
///
/// Unrelated A-WAL records are deliberately ignored. Any malformed Raft-owned
/// record fails recovery closed instead of silently dropping durable state
pub fn recover_raft_storage<S: RaftWalRecoverySource>(
    source: &mut S,
) -> Result<RecoveredRaftStorage, RaftStorageRecoveryError> {
    let mut recovered = RecoveredRaftStorage::default();

    while let Some(record) = source.next_record()? {
        if let Some(previous_lsn) = recovered.last_scanned_lsn
            && record.lsn <= previous_lsn
        {
            return Err(RaftStorageRecoveryError::NonIncreasingWalLsn {
                previous: previous_lsn,
                received: record.lsn,
            });
        }

        recovered.scanned_records += 1;
        recovered.last_scanned_lsn = Some(record.lsn);

        let Some(record_kind) = RaftWalRecordType::from_wal_record_type(record.record_type) else {
            continue;
        };

        match record_kind {
            RaftWalRecordType::LogEntry => {
                let entry = RaftLogEntryRecord::decode(&record.payload)?;

                recovered
                    .replica_mut(entry.identity)
                    .log_view
                    .replay(entry, record.lsn)
                    .map_err(|source| RaftStorageRecoveryError::InvalidLogTransition {
                        lsn: record.lsn,
                        source,
                    })?;
            }

            RaftWalRecordType::ConfState => {
                let conf_state = RaftConfStateRecord::decode(&record.payload)?;

                recovered.replica_mut(conf_state.identity).conf_state = Some(conf_state.to_core()?);
            }

            RaftWalRecordType::HardState => {
                let hard_state = RaftHardStateRecord::decode(&record.payload)?;

                let identity = hard_state.identity;
                let hard_state = hard_state.to_core()?;
                let replica = recovered.replica_mut(identity);

                let last_log_index = replica.log_view.last_index().unwrap_or(0);

                if hard_state.commit > last_log_index {
                    return Err(
                        RaftStorageRecoveryError::HardStateCommitBeyondRecoveredLog {
                            identity,
                            commit_index: hard_state.commit,
                            last_log_index,
                            lsn: record.lsn,
                        },
                    );
                }

                replica.hard_state = Some(hard_state);
            }
        }
    }

    Ok(recovered)
}

/// corruption or I/O failure encountered during shared-WAL Raft recovery
#[derive(Debug, thiserror::Error)]
pub enum RaftStorageRecoveryError {
    #[error("shared-WAL replay failed: {0}")]
    Wal(#[from] WalError),

    #[error("invalid durable Raft log entry: {0}")]
    InvalidLogEntry(#[from] RaftLogEntryCodecError),

    #[error("invalid durable Raft stable state: {0}")]
    InvalidStableState(#[from] RaftStableStateCodecError),

    #[error(
        "Raft WAL LSN must increase: previous \
         {previous:?}, received {received:?}"
    )]
    NonIncreasingWalLsn { previous: Lsn, received: Lsn },

    #[error(
        "invalid Raft log transition at WAL LSN \
         {lsn:?}: {source}"
    )]
    InvalidLogTransition { lsn: Lsn, source: RaftLogViewError },

    #[error(
        "Raft HardState for {identity:?} at WAL LSN \
         {lsn:?} commits index {commit_index}, but only \
         index {last_log_index} has been recovered"
    )]
    HardStateCommitBeyondRecoveredLog {
        identity: RaftReplicaIdentity,
        commit_index: u64,
        last_log_index: u64,
        lsn: Lsn,
    },
}
