//! identity scoped logical Raft log reconstructed from shared WAL records
//!
//! records enter this view in strictly increasing WAL LSN order. A conflicting
//! higher-term entry replaces the suffix beginning at its index, which is the
//! V1 durable truncation representation defined by the database format

use std::collections::BTreeMap;

use wal::lsn::Lsn;

use super::codec::{RaftLogEntryCodecError, RaftLogEntryRecord, RaftReplicaIdentity};

/// one logical Raft entry and the physical shared-WAL location that supplied it
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredRaftLogEntry {
    pub record: RaftLogEntryRecord,
    pub lsn: Lsn,
}

/// result of admitting one WAL record into a replica's logical log view
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftLogReplayOutcome {
    /// the record extended the logical log by one contiguous entry
    Appended,

    /// the record exactly repeated the existing index, term, and payload
    IdempotentReplay,

    /// higher term entry replaced its index and every stale later entry
    ReplacedSuffix { removed_entries: usize },
}

/// recovered log state for exactly one `(raft_group_id, replica_id)` lifetime
#[derive(Debug, Clone)]
pub struct RaftReplicaLogView {
    identity: RaftReplicaIdentity,
    entries: BTreeMap<u64, RecoveredRaftLogEntry>,
    last_replayed_lsn: Option<Lsn>,
    snapshot_index: u64,
    snapshot_term: u64,
    committed_index: u64,
}

impl RaftReplicaLogView {
    /// create an empty logical view for one validated replica lifetime
    pub fn new(identity: RaftReplicaIdentity) -> Self {
        Self {
            identity,
            entries: BTreeMap::new(),
            last_replayed_lsn: None,
            snapshot_index: 0,
            snapshot_term: 0,
            committed_index: 0,
        }
    }

    /// return the immutable identity that owns every entry in this view.
    pub fn identity(&self) -> RaftReplicaIdentity {
        self.identity
    }

    /// return the number of entries retained after suffix replacement
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// return whether this replica has no reconstructed entries
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// return the first retained log index
    pub fn first_index(&self) -> Option<u64> {
        self.entries.first_key_value().map(|(index, _)| *index)
    }

    /// return the last retained log index
    pub fn last_index(&self) -> Option<u64> {
        self.entries
            .last_key_value()
            .map(|(index, _)| *index)
            .or((self.snapshot_index != 0).then_some(self.snapshot_index))
    }

    /// look up one entry without consulting another replica lifetime
    pub fn entry(&self, index: u64) -> Option<&RecoveredRaftLogEntry> {
        self.entries.get(&index)
    }

    pub const fn snapshot_boundary(&self) -> Option<(u64, u64)> {
        if self.snapshot_index == 0 {
            None
        } else {
            Some((self.snapshot_index, self.snapshot_term))
        }
    }

    pub const fn committed_index(&self) -> u64 {
        self.committed_index
    }

    /// Advance the recovered commit frontier after a validated HardState.
    pub fn advance_commit(&mut self, commit: u64) -> Result<(), RaftLogViewError> {
        if commit < self.committed_index {
            return Err(RaftLogViewError::CommitRegression {
                previous: self.committed_index,
                received: commit,
            });
        }
        self.committed_index = commit;
        Ok(())
    }

    /// install a durable snapshot base before replaying its retained suffix
    pub fn install_snapshot(
        &mut self,
        index: u64,
        term: u64,
        lsn: Lsn,
    ) -> Result<(), RaftLogViewError> {
        if index == 0 || term == 0 {
            return Err(RaftLogViewError::InvalidSnapshotBoundary { index, term });
        }
        if index < self.snapshot_index {
            return Err(RaftLogViewError::SnapshotRegression {
                current: self.snapshot_index,
                received: index,
            });
        }

        if index == self.snapshot_index && term != self.snapshot_term {
            return Err(RaftLogViewError::SnapshotBoundaryTermChanged {
                index,
                previous_term: self.snapshot_term,
                received_term: term,
            });
        }

        if index > self.snapshot_index && self.snapshot_term != 0 && term < self.snapshot_term {
            return Err(RaftLogViewError::SnapshotTermRegression {
                previous_index: self.snapshot_index,
                previous_term: self.snapshot_term,
                received_index: index,
                received_term: term,
            });
        }

        if let Some(entry) = self.entries.get(&index)
            && entry.record.term != term
        {
            return Err(RaftLogViewError::SnapshotBoundaryTermMismatch {
                index,
                expected_term: entry.record.term,
                received_term: term,
            });
        }

        if let Some(previous_lsn) = self.last_replayed_lsn
            && lsn <= previous_lsn
        {
            return Err(RaftLogViewError::NonIncreasingLsn {
                previous: previous_lsn,
                received: lsn,
            });
        }
        self.entries = self.entries.split_off(&index.saturating_add(1));
        self.snapshot_index = index;
        self.snapshot_term = term;
        self.committed_index = self.committed_index.max(index);
        self.last_replayed_lsn = Some(lsn);
        Ok(())
    }

    /// iterate over the retained logical log in increasing index order
    pub fn entries(&self) -> impl Iterator<Item = &RecoveredRaftLogEntry> {
        self.entries.values()
    }

    /// return the latest WAL LSN consumed by this view
    pub fn last_replayed_lsn(&self) -> Option<Lsn> {
        self.last_replayed_lsn
    }

    /// apply one decoded WAL record using V1 suffix-overwrite rules
    ///
    /// every fallible check occurs before changing the view. A rejected record
    /// therefore leaves both the logical entries and replay frontier unchanged
    pub fn replay(
        &mut self,
        record: RaftLogEntryRecord,
        lsn: Lsn,
    ) -> Result<RaftLogReplayOutcome, RaftLogViewError> {
        record.validate()?;

        if record.identity != self.identity {
            return Err(RaftLogViewError::IdentityMismatch {
                expected: self.identity,
                received: record.identity,
            });
        }

        if let Some(previous_lsn) = self.last_replayed_lsn
            && lsn <= previous_lsn
        {
            return Err(RaftLogViewError::NonIncreasingLsn {
                previous: previous_lsn,
                received: lsn,
            });
        }

        let index = record.index;

        if index <= self.snapshot_index {
            return Err(RaftLogViewError::EntryAtOrBelowSnapshot {
                index,
                snapshot_index: self.snapshot_index,
            });
        }

        if let Some(existing) = self.entries.get(&index) {
            if record.term == existing.record.term {
                if record.payload != existing.record.payload {
                    return Err(RaftLogViewError::ConflictingPayload {
                        index,
                        term: record.term,
                    });
                }

                self.entries
                    .insert(index, RecoveredRaftLogEntry { record, lsn });

                self.last_replayed_lsn = Some(lsn);
                return Ok(RaftLogReplayOutcome::IdempotentReplay);
            }

            if index <= self.committed_index {
                return Err(RaftLogViewError::CommittedPrefixOverwrite {
                    index,
                    committed_index: self.committed_index,
                });
            }

            // A different term means that the local entry and every later
            // entry belong to a stale suffix. Raft conflict resolution does
            // not require the replacement term to be higher; it only must be
            // compatible with the retained predecessor or snapshot boundary.
            let predecessor_term = self
                .entries
                .get(&(index - 1))
                .map(|entry| entry.record.term)
                .or_else(|| (index == self.snapshot_index + 1).then_some(self.snapshot_term));

            if let Some(previous_term) = predecessor_term
                && record.term < previous_term
            {
                return Err(RaftLogViewError::LogTermRegression {
                    previous_index: index - 1,
                    previous_term,
                    received_index: index,
                    received_term: record.term,
                });
            }

            let removed_entries = self.entries.range(index..).count();

            self.entries.split_off(&index);

            self.entries
                .insert(index, RecoveredRaftLogEntry { record, lsn });

            self.last_replayed_lsn = Some(lsn);
            return Ok(RaftLogReplayOutcome::ReplacedSuffix { removed_entries });
        }

        if self.entries.is_empty() && self.snapshot_index == 0 && index != 1 {
            return Err(RaftLogViewError::MissingLogPrefix {
                expected_first_index: 1,
                received_first_index: index,
            });
        }

        if let Some(last_index) = self.last_index() {
            let expected_index = last_index
                .checked_add(1)
                .ok_or(RaftLogViewError::LogIndexExhausted { last_index })?;

            if index != expected_index {
                return Err(RaftLogViewError::NonContiguousAppend {
                    expected_index,
                    received_index: index,
                });
            }
        }

        let predecessor_term = self
            .entries
            .get(&(index - 1))
            .map(|entry| entry.record.term)
            .or_else(|| (index == self.snapshot_index + 1).then_some(self.snapshot_term));

        if let Some(previous_term) = predecessor_term
            && record.term < previous_term
        {
            return Err(RaftLogViewError::LogTermRegression {
                previous_index: index - 1,
                previous_term,
                received_index: index,
                received_term: record.term,
            });
        }

        self.entries
            .insert(index, RecoveredRaftLogEntry { record, lsn });

        self.last_replayed_lsn = Some(lsn);
        Ok(RaftLogReplayOutcome::Appended)
    }
}

/// invalid replay transition while reconstructing one replica's logical log
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RaftLogViewError {
    #[error("invalid durable Raft log entry: {0}")]
    InvalidRecord(#[from] RaftLogEntryCodecError),

    #[error(
        "Raft log entry belongs to {received:?}, \
         but this view owns {expected:?}"
    )]
    IdentityMismatch {
        expected: RaftReplicaIdentity,
        received: RaftReplicaIdentity,
    },

    #[error(
        "WAL replay LSN must increase: previous {previous:?}, \
         received {received:?}"
    )]
    NonIncreasingLsn { previous: Lsn, received: Lsn },

    #[error(
        "Raft log append is not contiguous: expected index \
         {expected_index}, received {received_index}"
    )]
    NonContiguousAppend {
        expected_index: u64,
        received_index: u64,
    },

    #[error(
        "Raft log has no snapshot base and starts at index {received_first_index}; \
         expected {expected_first_index}"
    )]
    MissingLogPrefix {
        expected_first_index: u64,
        received_first_index: u64,
    },

    #[error("Raft log index space is exhausted after index {last_index}")]
    LogIndexExhausted { last_index: u64 },

    #[error(
        "Raft log index {index} term {term} has different \
         durable payload bytes"
    )]
    ConflictingPayload { index: u64, term: u64 },

    #[error(
        "Raft log term regressed at index {index}: current \
         {current_term}, received {received_term}"
    )]
    TermRegression {
        index: u64,
        current_term: u64,
        received_term: u64,
    },

    #[error("Raft entry {index} is at or below snapshot boundary {snapshot_index}")]
    EntryAtOrBelowSnapshot { index: u64, snapshot_index: u64 },

    #[error("Raft entry {index} would overwrite committed prefix through {committed_index}")]
    CommittedPrefixOverwrite { index: u64, committed_index: u64 },

    #[error("Raft commit index regressed from {previous} to {received}")]
    CommitRegression { previous: u64, received: u64 },

    #[error("invalid Raft snapshot boundary index {index}, term {term}")]
    InvalidSnapshotBoundary { index: u64, term: u64 },

    #[error("Raft snapshot boundary regressed from {current} to {received}")]
    SnapshotRegression { current: u64, received: u64 },

    #[error(
        "Raft snapshot boundary at index {index} has term {received_term}, \
         but the retained entry has term {expected_term}"
    )]
    SnapshotBoundaryTermMismatch {
        index: u64,
        expected_term: u64,
        received_term: u64,
    },

    #[error(
        "Raft log term regressed from index {previous_index} term {previous_term} \
     to index {received_index} term {received_term}"
    )]
    LogTermRegression {
        previous_index: u64,
        previous_term: u64,
        received_index: u64,
        received_term: u64,
    },

    #[error(
        "Raft snapshot boundary term changed at index {index}: \
     previous {previous_term}, received {received_term}"
    )]
    SnapshotBoundaryTermChanged {
        index: u64,
        previous_term: u64,
        received_term: u64,
    },

    #[error(
        "Raft snapshot term regressed from index {previous_index} term {previous_term} \
     to index {received_index} term {received_term}"
    )]
    SnapshotTermRegression {
        previous_index: u64,
        previous_term: u64,
        received_index: u64,
        received_term: u64,
    },
}
