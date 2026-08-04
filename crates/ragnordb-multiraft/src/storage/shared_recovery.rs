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

pub fn recover_shared_storage<S: RaftWalRecoverySource>(
    source: &mut S,
    initial_configurations: &BTreeMap<RaftReplicaIdentity, ConfState>,
) -> Result<RecoveredNodeStorage, SharedStorageRecoveryError> {
    let mut database = RecoveryState::new();
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
