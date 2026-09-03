//! authoritative construction of one tablet replica from durable state
//!
//! new groups derive their initial quorum from an exactly-once bootstrap file
//! Existing groups load that durable authority, reconstruct Raft from one
//! shared-WAL recovery result, restore any referenced tablet snapshot, and
//! replay only the committed suffix not represented by the snapshot

use raft::{
    core::{node::RaftNode, ready::Ready},
    storage::mem::MemStorage,
};
use ragnordb_common::{
    ids::ReplicaId,
    raft_bootstrap::{RaftGroupBootstrap, RaftGroupBootstrapError},
};
use ragnordb_tablet::{
    Tablet,
    command::{TabletCommandApplyError, TabletStateMachine},
    snapshot::{
        FileTabletSnapshotStore, TabletSnapshotInstallError, TabletSnapshotInstallTarget,
        TabletSnapshotPointer, TabletSnapshotStoreError, restore_verified_snapshot,
    },
};
use wal::lsn::Lsn;

use crate::{
    bootstrap::{
        BootstrapGroupError, BootstrapStore, bootstrap_group_exactly_once,
        load_durable_group_bootstrap,
    },
    runtime::{AppliedRaftFrontier, RaftReadyLoop, ReadyLoopError},
    snapshot::{TabletSnapshotIntegrationError, raft_pointer_for_tablet},
    storage::{
        adapter::{RaftLogStoreAdapter, RaftStableStoreAdapter, RaftStorageAdapters},
        codec::{DurableRaftEntryPayload, RaftReplicaIdentity},
        persistence::{RaftPersistenceError, RaftWal, RaftWalStorage},
        recovery::RecoveredRaftReplica,
    },
    tablet_apply::{TabletApplyError, TabletCommandApplier},
};

/// Ready loop type returned for a newly bootstrapped group
pub type BootstrappedTabletReadyLoop<W> =
    RaftReadyLoop<W, MemStorage<Vec<u8>, Vec<u8>>, MemStorage<Vec<u8>, Vec<u8>>>;

/// readyloop type reconstructed from acknowledged durable Raft adapters
pub type RecoveredTabletReadyLoop<W> =
    RaftReadyLoop<W, RaftLogStoreAdapter, RaftStableStoreAdapter>;

/// new tablet replica whose bootstrap Ready has already crossed persistence
pub struct BootstrappedTabletReplica<W: RaftWal> {
    pub bootstrap: RaftGroupBootstrap,
    pub ready_loop: BootstrappedTabletReadyLoop<W>,
    pub tablet: TabletCommandApplier,
    /// Host may release these messages only after receiving this value. A
    /// durable bootstrap can legitimately produce no Ready records when a
    /// process restarts before Raft has emitted any persistent state.
    pub initial_ready: Option<Ready<Vec<u8>, Vec<u8>>>,
}

/// existing tablet replica reconstructed entirely from durable authorities
pub struct RecoveredTabletReplica<W: RaftWal> {
    pub bootstrap: RaftGroupBootstrap,
    pub ready_loop: RecoveredTabletReadyLoop<W>,
    pub tablet: TabletCommandApplier,
}

/// return the identity and initial configuration used while the shared WAL is
/// reconstructed. Later committed configuration entries supersede this initial
/// state inside `RecoveredRaftStorage::finish_configurations`
pub fn initial_recovery_configuration(
    bootstrap: &RaftGroupBootstrap,
    local_replica_id: ReplicaId,
) -> Result<(RaftReplicaIdentity, raft::types::ConfState), TabletReplicaStartupError> {
    bootstrap.validate()?;
    if !bootstrap.replica_to_node.contains_key(&local_replica_id) {
        return Err(TabletReplicaStartupError::ReplicaMissingFromBootstrap(
            local_replica_id,
        ));
    }

    let identity = RaftReplicaIdentity::new(bootstrap.raft_group_id, local_replica_id)
        .map_err(|error| TabletReplicaStartupError::Identity(error.to_string()))?;
    Ok((identity, bootstrap.to_core_conf_state()?))
}

/// create a new group through the exactly-once bootstrap authority and persist
/// its initial Ready before returning it to transport or proposal routing
pub fn bootstrap_tablet_replica<W, S>(
    bootstrap_store: &mut S,
    requested: &RaftGroupBootstrap,
    local_replica_id: ReplicaId,
    wal: W,
    target: &TabletSnapshotInstallTarget,
    election_timeout: u64,
    heartbeat_interval: u64,
) -> Result<BootstrappedTabletReplica<W>, TabletReplicaStartupError>
where
    W: RaftWal,
    S: BootstrapStore,
{
    bootstrap_group_exactly_once(bootstrap_store, requested)?;
    let bootstrap = load_durable_group_bootstrap(bootstrap_store, requested.raft_group_id)?
        .ok_or(TabletReplicaStartupError::MissingDurableBootstrap)?;
    validate_target(&bootstrap, local_replica_id, target)?;

    let (identity, conf_state) = initial_recovery_configuration(&bootstrap, local_replica_id)?;
    let raft = RaftNode::bootstrap(
        local_replica_id
            .to_raft()
            .map_err(|reason| TabletReplicaStartupError::Identity(reason.to_string()))?,
        conf_state,
        MemStorage::new(),
        MemStorage::new(),
        election_timeout,
        heartbeat_interval,
    )
    .map_err(|error| TabletReplicaStartupError::RaftInitialization(format!("{error:?}")))?;

    let tablet = Tablet::new(target.tablet_id, target.table_id)
        .map_err(|error| TabletReplicaStartupError::Tablet(error.to_string()))?;
    let tablet = TabletStateMachine::new(tablet, target.tablet_epoch, target.raft_group_id)?;
    let mut ready_loop = RaftReadyLoop::new(raft, RaftWalStorage::new(wal, identity));
    let initial_ready = ready_loop.persist_next_ready(None)?;

    Ok(BootstrappedTabletReplica {
        bootstrap,
        ready_loop,
        tablet: TabletCommandApplier::new(tablet),
        initial_ready,
    })
}

/// reconstruct an existing tablet replica from durable bootstrap, snapshot, and
/// shared WAL state without accepting static seed membership as authority
#[allow(clippy::too_many_arguments)]
pub fn recover_tablet_replica<W: RaftWal>(
    bootstrap: RaftGroupBootstrap,
    local_replica_id: ReplicaId,
    wal: W,
    durable_end_lsn: Lsn,
    recovered: &RecoveredRaftReplica,
    snapshot_store: &FileTabletSnapshotStore,
    target: &TabletSnapshotInstallTarget,
    election_timeout: u64,
    heartbeat_interval: u64,
) -> Result<RecoveredTabletReplica<W>, TabletReplicaStartupError> {
    validate_target(&bootstrap, local_replica_id, target)?;
    let (identity, _) = initial_recovery_configuration(&bootstrap, local_replica_id)?;
    if recovered.identity() != identity {
        return Err(TabletReplicaStartupError::RecoveredIdentityMismatch);
    }

    let (mut tablet, mut applied_frontier) = match recovered.snapshot() {
        Some(raft_pointer) => {
            let image = snapshot_store.load_verified_by_name(&raft_pointer.file_name)?;
            let tablet_pointer = TabletSnapshotPointer {
                metadata: image.metadata.clone(),
                file_name: raft_pointer.file_name.clone(),
            };
            let expected_raft_pointer = raft_pointer_for_tablet(identity, &tablet_pointer)?;
            if &expected_raft_pointer != raft_pointer {
                return Err(TabletReplicaStartupError::SnapshotPointerMismatch);
            }

            let restored = restore_verified_snapshot(&image, target)?;
            (
                TabletCommandApplier::new(restored.state_machine),
                Some(AppliedRaftFrontier::new(
                    restored.frontier.index,
                    restored.frontier.term,
                )),
            )
        }
        None => {
            if recovered.progress().truncated_through_index != 0 {
                return Err(TabletReplicaStartupError::MissingSnapshotForCompactedLog {
                    truncated_through: recovered.progress().truncated_through_index,
                });
            }
            let tablet = Tablet::new(target.tablet_id, target.table_id)
                .map_err(|error| TabletReplicaStartupError::Tablet(error.to_string()))?;
            let state_machine =
                TabletStateMachine::new(tablet, target.tablet_epoch, target.raft_group_id)?;
            (TabletCommandApplier::new(state_machine), None)
        }
    };

    let commit = recovered
        .hard_state()
        .map(|state| state.commit)
        .unwrap_or(0);
    let mut applied_index = applied_frontier.map(|frontier| frontier.index).unwrap_or(0);

    for entry in recovered.log_view().entries() {
        if entry.record.index <= applied_index {
            continue;
        }
        if entry.record.index > commit {
            break;
        }

        let expected_index = applied_index.saturating_add(1);
        if entry.record.index != expected_index {
            return Err(TabletReplicaStartupError::CommittedSuffixGap {
                expected: expected_index,
                received: entry.record.index,
            });
        }

        if let DurableRaftEntryPayload::Normal(command) = &entry.record.payload {
            tablet.apply_committed(
                crate::proposal::ProposalPosition {
                    term: entry.record.term,
                    index: entry.record.index,
                },
                command,
            )?;
        }

        applied_index = entry.record.index;
        applied_frontier = Some(AppliedRaftFrontier::new(
            entry.record.index,
            entry.record.term,
        ));
    }

    if applied_index != commit {
        return Err(TabletReplicaStartupError::CommitNotReconstructed {
            commit,
            applied: applied_index,
        });
    }

    let adapters = RaftStorageAdapters::from_recovered(recovered)
        .map_err(|error| TabletReplicaStartupError::RaftAdapter(error.to_string()))?;
    let raft = RaftNode::restart(
        local_replica_id
            .to_raft()
            .map_err(|reason| TabletReplicaStartupError::Identity(reason.to_string()))?,
        adapters.log,
        adapters.stable,
        election_timeout,
        heartbeat_interval,
    )
    .map_err(|error| TabletReplicaStartupError::RaftInitialization(format!("{error:?}")))?;
    let persistence = RaftWalStorage::from_recovered(wal, recovered, durable_end_lsn)?;
    let mut ready_loop = RaftReadyLoop::new(raft, persistence);

    if let Some(frontier) = applied_frontier {
        ready_loop.advance_applied_frontier(frontier)?;
    }

    Ok(RecoveredTabletReplica {
        bootstrap,
        ready_loop,
        tablet,
    })
}

fn validate_target(
    bootstrap: &RaftGroupBootstrap,
    local_replica_id: ReplicaId,
    target: &TabletSnapshotInstallTarget,
) -> Result<(), TabletReplicaStartupError> {
    bootstrap.validate()?;
    if bootstrap.cluster_id != target.cluster_id {
        return Err(TabletReplicaStartupError::ClusterMismatch);
    }
    if bootstrap.raft_group_id != target.raft_group_id {
        return Err(TabletReplicaStartupError::RaftGroupMismatch);
    }
    if !bootstrap.replica_to_node.contains_key(&local_replica_id) {
        return Err(TabletReplicaStartupError::ReplicaMissingFromBootstrap(
            local_replica_id,
        ));
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum TabletReplicaStartupError {
    #[error("durable bootstrap failed: {0}")]
    Bootstrap(#[from] BootstrapGroupError),
    #[error("invalid durable bootstrap: {0}")]
    BootstrapEnvelope(#[from] RaftGroupBootstrapError),
    #[error("durable bootstrap record is missing")]
    MissingDurableBootstrap,
    #[error("bootstrap did not produce an initial Ready generation")]
    MissingBootstrapReady,
    #[error("replica {0:?} is not present in the durable bootstrap mapping")]
    ReplicaMissingFromBootstrap(ReplicaId),
    #[error("replica startup cluster does not match durable bootstrap")]
    ClusterMismatch,
    #[error("replica startup Raft group does not match durable bootstrap")]
    RaftGroupMismatch,
    #[error("recovered Raft identity does not match durable bootstrap")]
    RecoveredIdentityMismatch,
    #[error("invalid Raft replica identity: {0}")]
    Identity(String),
    #[error("Raft initialization failed: {0}")]
    RaftInitialization(String),
    #[error("Raft storage adapter failed: {0}")]
    RaftAdapter(String),
    #[error("tablet construction failed: {0}")]
    Tablet(String),
    #[error("tablet command failed during durable replay: {0}")]
    TabletCommand(#[from] TabletCommandApplyError),
    #[error("committed tablet replay failed: {0}")]
    TabletApply(#[from] TabletApplyError),
    #[error("tablet snapshot store failed: {0}")]
    SnapshotStore(#[from] TabletSnapshotStoreError),
    #[error("tablet snapshot restore failed: {0}")]
    SnapshotRestore(#[from] TabletSnapshotInstallError),
    #[error("tablet/Raft snapshot integration failed: {0}")]
    SnapshotIntegration(#[from] TabletSnapshotIntegrationError),
    #[error("tablet snapshot metadata does not exactly match the durable Raft pointer")]
    SnapshotPointerMismatch,
    #[error("compacted Raft log through {truncated_through} has no recoverable tablet snapshot")]
    MissingSnapshotForCompactedLog { truncated_through: u64 },
    #[error("committed suffix has a gap: expected index {expected}, received {received}")]
    CommittedSuffixGap { expected: u64, received: u64 },
    #[error("HardState commit {commit} was reconstructed only through index {applied}")]
    CommitNotReconstructed { commit: u64, applied: u64 },
    #[error("Raft persistence initialization failed: {0}")]
    Persistence(#[from] RaftPersistenceError),
    #[error("Raft Ready-loop initialization failed: {0}")]
    Ready(#[from] ReadyLoopError),
}
