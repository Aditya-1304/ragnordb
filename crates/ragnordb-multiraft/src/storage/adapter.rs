//! read only adapter from acknowledged ragnordb durability into raft traits
//!
//! raft initializes its private logical overlay from these adapters. mutation
//! methods intentionally fail because Ready plus A-WAL sync is the only
//! database persistence authority; trait calls must not create a second path

use std::collections::BTreeMap;

use raft::{
    entry::LogEntry,
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{ConfState, HardState, LogIndex, Term},
};

use super::{codec::RaftLogEntryCodecError, recovery::RecoveredRaftReplica};

/// durable log view supplied to the raft core during initialization
#[derive(Clone, Debug)]
pub struct RaftLogStoreAdapter {
    entries: BTreeMap<LogIndex, LogEntry<Vec<u8>>>,
    truncated_through_index: LogIndex,
    truncated_through_term: Term,
}

impl RaftLogStoreAdapter {
    fn from_recovered(replica: &RecoveredRaftReplica) -> Result<Self, RaftStorageAdapterError> {
        let progress = replica.progress();
        let mut entries = BTreeMap::new();

        for recovered in replica.log_view().entries() {
            if recovered.record.index > progress.truncated_through_index {
                let entry = recovered.record.to_core()?;

                entries.insert(entry.index, entry);
            }
        }

        Ok(Self {
            entries,
            truncated_through_index: progress.truncated_through_index,
            truncated_through_term: progress.truncated_through_term,
        })
    }

    fn reject_mutation(operation: &str) -> ! {
        panic!(
            "Raft durable adapter is read-only: \
             {operation} must be persisted through \
             Ready and A-WAL"
        )
    }
}

impl LogStore<Vec<u8>> for RaftLogStoreAdapter {
    fn first_index(&self) -> LogIndex {
        self.entries
            .first_key_value()
            .map(|(index, _)| *index)
            .unwrap_or_else(|| self.truncated_through_index.saturating_add(1))
    }

    fn last_index(&self) -> LogIndex {
        self.entries
            .last_key_value()
            .map(|(index, _)| *index)
            .unwrap_or(self.truncated_through_index)
    }

    fn term(&self, index: LogIndex) -> Option<Term> {
        if self.truncated_through_index != 0 && index == self.truncated_through_index {
            return Some(self.truncated_through_term);
        }

        self.entries.get(&index).map(|entry| entry.term)
    }

    fn entry(&self, index: LogIndex) -> Option<LogEntry<Vec<u8>>> {
        self.entries.get(&index).cloned()
    }

    fn entries(&self, from: LogIndex, max: usize) -> Vec<LogEntry<Vec<u8>>> {
        if max == 0 {
            return Vec::new();
        }

        self.entries
            .range(from.max(self.first_index())..)
            .take(max)
            .map(|(_, entry)| entry.clone())
            .collect()
    }

    fn append(&mut self, _entries: &[LogEntry<Vec<u8>>]) {
        Self::reject_mutation("append")
    }

    fn truncate_suffix(&mut self, _from: LogIndex) {
        Self::reject_mutation("truncate_suffix")
    }

    fn compact(&mut self, _through: LogIndex) {
        Self::reject_mutation("compact")
    }

    fn install_snapshot(&mut self, _last_included_index: LogIndex, _last_included_term: Term) {
        Self::reject_mutation("install_snapshot")
    }
}

/// durable stable state view supplied to the raft core at initialization
#[derive(Debug, Clone)]
pub struct RaftStableStoreAdapter {
    hard_state: HardState,
    conf_state: Option<ConfState>,
}

impl StableStore for RaftStableStoreAdapter {
    fn hard_state(&self) -> HardState {
        self.hard_state.clone()
    }

    fn set_hard_state(&mut self, _hard_state: HardState) {
        RaftLogStoreAdapter::reject_mutation("set_hard_state")
    }

    fn conf_state(&self) -> Option<ConfState> {
        self.conf_state.clone()
    }

    fn set_conf_state(&mut self, _conf_state: ConfState) {
        RaftLogStoreAdapter::reject_mutation("set_conf_state")
    }
}

/// pair of public Raft storage traits built from one recovered lifetime
#[derive(Debug, Clone)]
pub struct RaftStorageAdapters {
    pub log: RaftLogStoreAdapter,
    pub stable: RaftStableStoreAdapter,
}

impl RaftStorageAdapters {
    /// convert acknowledged state into Raft initialization stores
    pub fn from_recovered(replica: &RecoveredRaftReplica) -> Result<Self, RaftStorageAdapterError> {
        Ok(Self {
            log: RaftLogStoreAdapter::from_recovered(replica)?,
            stable: RaftStableStoreAdapter {
                hard_state: replica.hard_state().cloned().unwrap_or_default(),
                conf_state: replica.conf_state().cloned(),
            },
        })
    }
}

/// recovered bytes could not be converted into the Raft public API
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RaftStorageAdapterError {
    #[error("invalid recovered Raft log entry: {0}")]
    InvalidLogEntry(#[from] RaftLogEntryCodecError),
}
