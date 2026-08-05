//! node wide semantic recovery dispatcher for the shared A-WAL
//!
//! one physical cursor feeds both database and Raft recovery owners. Each
//! subsystem decodes only its permanent record IDs, so startup never rescans
//! the complete WAL independently for every hosted replica or subsystem

use std::collections::BTreeMap;

use raft::types::ConfState;
use ragnordb_storage::recovery::{RecoveryState, decode_recovery_record};

use super::{
    codec::RaftReplicaIdentity,
    recovery::{RaftStorageRecoveryError, RaftWalRecoverySource, RecoveredRaftStorage},
};

/// private startup state published only after the complete shared scan passes
#[derive(Debug)]
pub struct RecoveredNodeStorage {
    pub database: RecoveryState,
    pub raft: RecoveredRaftStorage,
}

/// recover from the beginning of the retained WAL
///
/// this remains the correct entry point when no database checkpoint has been
/// selected. Checkpoint-aware startup must use
/// `recover_shared_storage_from_state`
pub fn recover_shared_storage<S: RaftWalRecoverySource>(
    source: &mut S,
    initial_configurations: &BTreeMap<RaftReplicaIdentity, ConfState>,
) -> Result<RecoveredNodeStorage, SharedStorageRecoveryError> {
    recover_shared_storage_from_state(source, RecoveryState::new(), initial_configurations)
}

/// replay one checkpoint-aware WAL suffix through both database and Raft
/// recovery owners.
///
/// The caller must select and validate the database checkpoint before opening
/// `source` at its exact replay boundary. The supplied database state must be
/// restored from that validated checkpoint.
///
/// The state remains private until the complete suffix reaches its validated
/// end. If database or Raft replay fails, no partially recovered state is
/// returned.
pub fn recover_shared_storage_from_state<S: RaftWalRecoverySource>(
    source: &mut S,
    mut database: RecoveryState,
    initial_configurations: &BTreeMap<RaftReplicaIdentity, ConfState>,
) -> Result<RecoveredNodeStorage, SharedStorageRecoveryError> {
    let mut raft = RecoveredRaftStorage::default();

    while let Some(record) = source
        .next_record()
        .map_err(RaftStorageRecoveryError::from)?
    {
        if let Some(database_record) =
            decode_recovery_record(record.lsn, record.record_type, &record.payload)?
        {
            database.apply_record(&database_record)?;
        }

        raft.observe_record(record)?;
    }

    raft.finish_configurations(initial_configurations)?;

    Ok(RecoveredNodeStorage { database, raft })
}

#[derive(Debug, thiserror::Error)]
pub enum SharedStorageRecoveryError {
    #[error("database recovery failed: {0}")]
    Database(#[from] ragnordb_common::Error),

    #[error("Raft recovery failed: {0}")]
    Raft(#[from] RaftStorageRecoveryError),
}
