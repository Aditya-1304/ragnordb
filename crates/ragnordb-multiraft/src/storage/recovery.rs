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
        DurableRaftEntryPayload, RaftHardStateRecord, RaftLogEntryCodecError, RaftLogEntryRecord,
        RaftReplicaIdentity, RaftSnapshotPointerRecord, RaftStableStateCodecError,
        SnapshotTransitionError, validate_hard_state_successor, validate_snapshot_successor,
    },
    frontier::RaftProgress,
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
    progress: RaftProgress,
    snapshot: Option<RaftSnapshotPointerRecord>,
}

impl RecoveredRaftReplica {
    fn new(identity: RaftReplicaIdentity) -> Self {
        Self {
            log_view: RaftReplicaLogView::new(identity),
            conf_state: None,
            hard_state: None,
            progress: RaftProgress::default(),
            snapshot: None,
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

    /// return the latest recovered truncation and applied frontiers
    pub fn progress(&self) -> RaftProgress {
        self.progress
    }

    pub fn snapshot(&self) -> Option<&RaftSnapshotPointerRecord> {
        self.snapshot.as_ref()
    }

    /// Normalize restart term state after the complete recoverable prefix is
    /// known. A crash can preserve a higher-term entry while losing the
    /// trailing HardState that originally carried that term, so restarting
    /// with the older term or vote would violate Raft term/vote safety.
    fn normalize_hard_state_term(&mut self) {
        let snapshot_term = self
            .log_view
            .snapshot_boundary()
            .map(|(_, term)| term)
            .unwrap_or(0);
        let retained_entry_term = self
            .log_view
            .entries()
            .map(|entry| entry.record.term)
            .max()
            .unwrap_or(0);
        let maximum_observed_term = snapshot_term.max(retained_entry_term);

        if maximum_observed_term == 0 {
            return;
        }

        match self.hard_state.as_mut() {
            Some(state) if state.current_term < maximum_observed_term => {
                state.current_term = maximum_observed_term;
                state.voted_for = None;
            }
            Some(_) => {}
            None => {
                self.hard_state = Some(HardState {
                    current_term: maximum_observed_term,
                    voted_for: None,
                    commit: self.log_view.committed_index(),
                });
            }
        }
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

    /// Route one physical WAL record into its Raft-owned recovery state.
    /// Non-Raft records still advance the shared physical scan frontier.
    pub fn observe_record(&mut self, record: WalRecord) -> Result<(), RaftStorageRecoveryError> {
        if let Some(previous_lsn) = self.last_scanned_lsn
            && record.lsn <= previous_lsn
        {
            return Err(RaftStorageRecoveryError::NonIncreasingWalLsn {
                previous: previous_lsn,
                received: record.lsn,
            });
        }
        self.scanned_records += 1;
        self.last_scanned_lsn = Some(record.lsn);
        let Some(record_kind) = RaftWalRecordType::from_wal_record_type(record.record_type) else {
            return Ok(());
        };
        match record_kind {
            RaftWalRecordType::LogEntry => {
                let entry = RaftLogEntryRecord::decode(&record.payload)?;
                let replica = self.replica_mut(entry.identity);
                if entry.index <= replica.progress.truncated_through_index {
                    return Err(RaftStorageRecoveryError::EntryAtOrBelowTruncation {
                        identity: entry.identity,
                        entry_index: entry.index,
                        truncated_through_index: replica.progress.truncated_through_index,
                        lsn: record.lsn,
                    });
                }
                replica
                    .log_view
                    .replay(entry, record.lsn)
                    .map_err(|source| RaftStorageRecoveryError::InvalidLogTransition {
                        lsn: record.lsn,
                        source,
                    })?;
            }
            RaftWalRecordType::HardState => {
                let hard_state = RaftHardStateRecord::decode(&record.payload)?;
                let identity = hard_state.identity;
                let hard_state = hard_state.to_core()?;
                let replica = self.replica_mut(identity);
                validate_hard_state_successor(replica.hard_state.as_ref(), &hard_state)?;
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
                replica
                    .log_view
                    .advance_commit(hard_state.commit)
                    .map_err(|source| RaftStorageRecoveryError::InvalidLogTransition {
                        lsn: record.lsn,
                        source,
                    })?;
                replica.hard_state = Some(hard_state);
            }
            RaftWalRecordType::SnapshotPointer => {
                let snapshot = RaftSnapshotPointerRecord::decode(&record.payload)?;
                let identity = snapshot.identity;
                let replica = self.replica_mut(identity);

                validate_snapshot_successor(replica.snapshot.as_ref(), &snapshot).map_err(
                    |error| match error {
                        SnapshotTransitionError::ConflictingPointer { index } => {
                            RaftStorageRecoveryError::ConflictingSnapshotPointer {
                                identity,
                                index,
                                lsn: record.lsn,
                            }
                        }
                        SnapshotTransitionError::IndexRegression { previous, received } => {
                            RaftStorageRecoveryError::InvalidLogTransition {
                                lsn: record.lsn,
                                source: RaftLogViewError::SnapshotRegression {
                                    current: previous,
                                    received,
                                },
                            }
                        }
                    },
                )?;

                replica
                    .log_view
                    .install_snapshot(
                        snapshot.last_included_index,
                        snapshot.last_included_term,
                        record.lsn,
                    )
                    .map_err(|source| RaftStorageRecoveryError::InvalidLogTransition {
                        lsn: record.lsn,
                        source,
                    })?;

                replica.progress = RaftProgress {
                    truncated_through_index: snapshot.last_included_index,
                    truncated_through_term: snapshot.last_included_term,
                    applied_index: snapshot.applied_index,
                };

                replica.hard_state = Some(match replica.hard_state.take() {
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

                replica.conf_state = Some(snapshot.conf_state.clone());
                replica.snapshot = Some(snapshot);
            }
        }
        Ok(())
    }

    /// Finalize membership after the shared scan reaches its validated end.
    pub fn finish_configurations(
        &mut self,
        initial_configurations: &BTreeMap<RaftReplicaIdentity, ConfState>,
    ) -> Result<(), RaftStorageRecoveryError> {
        for (identity, replica) in &mut self.replicas {
            replica.normalize_hard_state_term();
            let mut conf_state = replica
                .conf_state
                .clone()
                .or_else(|| initial_configurations.get(identity).cloned());
            let commit = replica
                .hard_state
                .as_ref()
                .map(|state| state.commit)
                .unwrap_or(0);
            for entry in replica.log_view.entries() {
                if entry.record.index > commit {
                    break;
                }
                if let DurableRaftEntryPayload::Configuration(change) = entry.record.payload {
                    let current = conf_state.as_ref().ok_or(
                        RaftStorageRecoveryError::MissingInitialConfiguration {
                            identity: *identity,
                            index: entry.record.index,
                        },
                    )?;
                    conf_state = Some(current.apply(&change).map_err(|source| {
                        RaftStorageRecoveryError::InvalidCommittedConfiguration {
                            identity: *identity,
                            index: entry.record.index,
                            reason: format!("{source:?}"),
                        }
                    })?);
                }
            }
            replica.conf_state = conf_state;
        }
        Ok(())
    }
}

///  every Raft-owned record encountered by one forward WAL scan
///
/// Unrelated A-WAL records are deliberately ignored. Any malformed Raft-owned
/// record fails recovery closed instead of silently dropping durable state
pub fn recover_raft_storage<S: RaftWalRecoverySource>(
    source: &mut S,
) -> Result<RecoveredRaftStorage, RaftStorageRecoveryError> {
    recover_raft_storage_with_configurations(source, &BTreeMap::new())
}

/// Recover all replicas in one pass and derive membership from a snapshot or
/// the exactly-once bootstrap configuration plus committed change entries.
pub fn recover_raft_storage_with_configurations<S: RaftWalRecoverySource>(
    source: &mut S,
    initial_configurations: &BTreeMap<RaftReplicaIdentity, ConfState>,
) -> Result<RecoveredRaftStorage, RaftStorageRecoveryError> {
    let mut recovered = RecoveredRaftStorage::default();

    while let Some(record) = source.next_record()? {
        recovered.observe_record(record)?;
    }

    recovered.finish_configurations(initial_configurations)?;

    Ok(recovered)
}

/// corruption or I/O failure encountered during shared WAL Raft recovery
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
         {lsn:?} commits index {commit_index}, but \
         only index {last_log_index} has been recovered"
    )]
    HardStateCommitBeyondRecoveredLog {
        identity: RaftReplicaIdentity,
        commit_index: u64,
        last_log_index: u64,
        lsn: Lsn,
    },

    #[error(
        "Raft entry {entry_index} for {identity:?} \
         at WAL LSN {lsn:?} is at or below \
         truncation boundary {truncated_through_index}"
    )]
    EntryAtOrBelowTruncation {
        identity: RaftReplicaIdentity,
        entry_index: u64,
        truncated_through_index: u64,
        lsn: Lsn,
    },

    #[error(
        "Raft configuration entry {index} for {identity:?} has no bootstrap or snapshot configuration"
    )]
    MissingInitialConfiguration {
        identity: RaftReplicaIdentity,
        index: u64,
    },

    #[error("invalid committed Raft configuration entry {index} for {identity:?}: {reason}")]
    InvalidCommittedConfiguration {
        identity: RaftReplicaIdentity,
        index: u64,
        reason: String,
    },

    #[error(
        "conflicting Raft snapshot pointers for {identity:?} at index {index} \
     encountered at WAL LSN {lsn:?}"
    )]
    ConflictingSnapshotPointer {
        identity: RaftReplicaIdentity,
        index: u64,
        lsn: Lsn,
    },
}
