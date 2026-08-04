//! permanent record identities for the node wide shared A-WAL
//!
//! every database subsystem classifies records through this registry. Keeping
//! the numeric assignments in the lowest shared crate prevents independent
//! recovery paths from reusing an on-disk identifier for different schemas

use wal::types::{RecordType, record_types::USER_MIN};

pub const RAFT_HARD_STATE_ID: u16 = USER_MIN + 1;
pub const RAFT_LOG_ENTRY_ID: u16 = USER_MIN + 2;
pub const DATABASE_SNAPSHOT_POINTER_ID: u16 = USER_MIN + 3;
pub const TABLET_SNAPSHOT_CHUNK_ID: u16 = USER_MIN + 4;
pub const SINGLE_NODE_TXN_COMMIT_ID: u16 = USER_MIN + 5;
pub const CATALOG_UPDATE_ID: u16 = USER_MIN + 6;
pub const CHECKPOINT_MARKER_ID: u16 = USER_MIN + 7;
pub const RAFT_SNAPSHOT_POINTER_ID: u16 = USER_MIN + 8;

/// semantic owner selected for one permanent user-defined WAL record ID
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedWalRecordType {
    RaftHardState,
    RaftLogEntry,
    DatabaseSnapshotPointer,
    TabletSnapshotChunk,
    SingleNodeTxnCommit,
    CatalogUpdate,
    CheckpointMarker,
    RaftSnapshotPointer,
}

impl SharedWalRecordType {
    pub const fn as_wal_record_type(self) -> RecordType {
        RecordType::new(match self {
            Self::RaftHardState => RAFT_HARD_STATE_ID,
            Self::RaftLogEntry => RAFT_LOG_ENTRY_ID,
            Self::DatabaseSnapshotPointer => DATABASE_SNAPSHOT_POINTER_ID,
            Self::TabletSnapshotChunk => TABLET_SNAPSHOT_CHUNK_ID,
            Self::SingleNodeTxnCommit => SINGLE_NODE_TXN_COMMIT_ID,
            Self::CatalogUpdate => CATALOG_UPDATE_ID,
            Self::CheckpointMarker => CHECKPOINT_MARKER_ID,
            Self::RaftSnapshotPointer => RAFT_SNAPSHOT_POINTER_ID,
        })
    }

    /// Classify a permanent user record while leaving A-WAL metadata alone.
    pub const fn classify(record_type: RecordType) -> Option<Self> {
        match record_type.as_u16() {
            RAFT_HARD_STATE_ID => Some(Self::RaftHardState),
            RAFT_LOG_ENTRY_ID => Some(Self::RaftLogEntry),
            DATABASE_SNAPSHOT_POINTER_ID => Some(Self::DatabaseSnapshotPointer),
            TABLET_SNAPSHOT_CHUNK_ID => Some(Self::TabletSnapshotChunk),
            SINGLE_NODE_TXN_COMMIT_ID => Some(Self::SingleNodeTxnCommit),
            CATALOG_UPDATE_ID => Some(Self::CatalogUpdate),
            CHECKPOINT_MARKER_ID => Some(Self::CheckpointMarker),
            RAFT_SNAPSHOT_POINTER_ID => Some(Self::RaftSnapshotPointer),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Realistic bug caught: two subsystems independently reuse or renumber a
    /// permanent user-record ID and healthy WAL bytes become undecodable after
    /// an upgrade.
    #[test]
    fn permanent_shared_wal_registry_is_frozen_and_unique() {
        let expected = [
            (SharedWalRecordType::RaftHardState, USER_MIN + 1),
            (SharedWalRecordType::RaftLogEntry, USER_MIN + 2),
            (SharedWalRecordType::DatabaseSnapshotPointer, USER_MIN + 3),
            (SharedWalRecordType::TabletSnapshotChunk, USER_MIN + 4),
            (SharedWalRecordType::SingleNodeTxnCommit, USER_MIN + 5),
            (SharedWalRecordType::CatalogUpdate, USER_MIN + 6),
            (SharedWalRecordType::CheckpointMarker, USER_MIN + 7),
            (SharedWalRecordType::RaftSnapshotPointer, USER_MIN + 8),
        ];
        let mut assigned = std::collections::BTreeSet::new();

        for (kind, expected_id) in expected {
            let record_type = kind.as_wal_record_type();
            assert_eq!(record_type.as_u16(), expected_id);
            assert!(record_type.is_user_defined());
            assert!(assigned.insert(expected_id));
            assert_eq!(SharedWalRecordType::classify(record_type), Some(kind));
        }
    }
}
