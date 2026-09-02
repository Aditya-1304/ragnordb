//! Metadata Raft state-machine adapter and desired-membership reconciliation.
//!
//! The catalog crate owns metadata semantics. This module owns the boundary
//! between those semantics and the reusable Raft Ready runtime.
//!
//! Phase 5.1 deliberately stops before actually bootstrapping/registering the
//! metadata group. Phase 5.1a will construct a RaftReadyLoop using this state
//! machine through the already-frozen Phase 5.0 MultiRaft host.

use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
};

use raft::{
    core::node::RaftNode,
    storage::mem::MemStorage,
    types::{ConfState, Snapshot},
};

use wal::lsn::Lsn;

use ragnordb_catalog::{MetadataApplyOutcome, MetadataState};

use ragnordb_common::{
    ids::{NodeId, ReplicaId},
    metadata_codec::{
        DesiredReplica, DesiredReplicaPlacement, DesiredReplicaRole, MetadataCommand,
        MetadataCommandCodecError, MetadataSnapshot, NodeDescriptor,
    },
    raft_bootstrap::{RaftGroupBootstrap, RaftGroupBootstrapError},
};

use crate::{
    host::{HostedRaftGroup, ReadyLoopHostedGroup},
    runtime::{
        AppliedRaftFrontier, FileRaftSnapshotStore, RaftReadyLoop, RaftReadyStateMachine,
        RaftSnapshotStore, ReadyLoopError,
    },
    storage::{
        adapter::RaftStorageAdapters,
        codec::{DurableRaftEntryPayload, RaftReplicaIdentity},
        persistence::{RaftPersistenceError, RaftWal, RaftWalStorage},
        recovery::RecoveredRaftReplica,
    },
};

/// Result produced by the most recently applied metadata log entry.
///
/// Phase 5.2 can correlate this with the proposal waiter. Keeping it here now
/// prevents ordinary metadata-domain rejection from being expressed as a
/// state-machine failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedMetadataCommand {
    /// Request identity, when the committed entry came from a client-bearing
    /// metadata envelope. Legacy startup entries may not carry one.
    pub request_id: Option<ragnordb_common::ids::RequestId>,

    pub index: u64,
    pub outcome: MetadataApplyOutcome,
}

/// Read-only publication boundary between the metadata Ready owner and the
/// rest of the node.
///
/// `MetadataRaftStateMachine` remains the sole mutation authority. This handle
/// only receives clones after committed apply/snapshot restore, so routing and
/// startup code cannot bypass Raft by mutating metadata directly.
#[derive(Clone, Default)]
pub struct MetadataRuntimeHandle {
    state: Arc<RwLock<MetadataState>>,

    applied_results: Arc<Mutex<VecDeque<AppliedMetadataCommand>>>,
}

impl MetadataRuntimeHandle {
    pub fn state_snapshot(&self) -> MetadataState {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn cluster_id(&self) -> Option<String> {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .cluster_id()
            .map(str::to_string)
    }

    pub fn node(&self, node_id: NodeId) -> Option<NodeDescriptor> {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .node(node_id)
            .cloned()
    }

    pub fn take_applied_results(&self) -> Vec<AppliedMetadataCommand> {
        self.applied_results
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .drain(..)
            .collect()
    }

    fn publish_state(&self, state: &MetadataState) {
        *self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = state.clone();
    }

    fn publish_results(&self, results: impl IntoIterator<Item = AppliedMetadataCommand>) {
        self.applied_results
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend(results);
    }
}

/// Raft-owned wrapper around the deterministic metadata projection.
///
/// State may change only through committed `apply()` calls or verified snapshot
/// restoration. Callers receive only a shared reference to the state.
#[derive(Debug, Default)]
pub struct MetadataRaftStateMachine {
    state: MetadataState,

    /// Ordered results produced by committed metadata entries.
    ///
    /// A single Ready generation can contain several committed commands.
    /// Keeping only the latest result would lose proposal outcomes and make it
    /// impossible for Phase 5.2 to correlate every committed proposal with its
    /// deterministic state-machine result.
    ///
    /// The owning metadata runtime must drain this queue after each Ready
    /// generation. Recovery may drain and discard outcomes because proposal
    /// waiters are process-local and do not survive restart.
    applied_results: VecDeque<AppliedMetadataCommand>,
}

impl MetadataRaftStateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(&self) -> &MetadataState {
        &self.state
    }

    pub fn pending_apply_results(&self) -> usize {
        self.applied_results.len()
    }

    /// Remove every result produced since the previous drain, preserving Raft log
    /// order.
    ///
    /// The metadata runtime will use the log index to complete the matching
    /// proposal waiter. This keeps state-machine semantics deterministic while
    /// proposal ownership remains outside the replicated metadata projection.
    pub fn take_applied_results(&mut self) -> Vec<AppliedMetadataCommand> {
        self.applied_results.drain(..).collect()
    }

    /// Encode the complete metadata projection for a Raft snapshot.
    ///
    /// The surrounding Raft/tablet snapshot code remains responsible for file
    /// durability, checksum publication, index/term, and ConfState.
    pub fn encode_snapshot(&self) -> Result<Vec<u8>, MetadataStateMachineError> {
        self.state
            .to_snapshot()
            .encode()
            .map_err(MetadataStateMachineError::SnapshotEncode)
    }
}

impl RaftReadyStateMachine for MetadataRaftStateMachine {
    type Error = MetadataStateMachineError;

    fn restore_snapshot(&mut self, snapshot: &Snapshot<Vec<u8>>) -> Result<(), Self::Error> {
        let metadata_snapshot = MetadataSnapshot::decode(&snapshot.data)
            .map_err(MetadataStateMachineError::SnapshotDecode)?;

        self.state = MetadataState::from_snapshot(metadata_snapshot)
            .map_err(|error| MetadataStateMachineError::SnapshotState(error.to_string()))?;

        // Proposal completion state is volatile. Snapshot restoration rebuilds
        // authoritative metadata but must not invent a response for a proposal
        // waiter that did not survive restart.
        self.applied_results.clear();

        Ok(())
    }

    fn apply(&mut self, index: u64, command: &[u8]) -> Result<(), Self::Error> {
        let (request_id, command) = MetadataCommand::decode_with_optional_request_id(command)
            .map_err(MetadataStateMachineError::CommandDecode)?;

        let outcome = match &request_id {
            Some(request_id) => self
                .state
                .apply_with_request_id(request_id.clone(), command),
            None => self.state.apply(command),
        };

        self.applied_results.push_back(AppliedMetadataCommand {
            request_id,
            index,
            outcome,
        });

        // A MetadataApplyOutcome::Rejected is a deterministic command result,
        // not corruption. Returning Err here would quarantine the entire
        // metadata Raft group through RaftReadyLoop.
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataStateMachineError {
    #[error("metadata Raft command decode failed: {0}")]
    CommandDecode(MetadataCommandCodecError),

    #[error("metadata snapshot encode failed: {0}")]
    SnapshotEncode(MetadataCommandCodecError),

    #[error("metadata snapshot decode failed: {0}")]
    SnapshotDecode(MetadataCommandCodecError),

    #[error("metadata snapshot state is invalid: {0}")]
    SnapshotState(String),
}

/// State-machine wrapper used by the physical MultiRaft host.
///
/// The inner state machine remains deterministic and unaware of threads,
/// networking, or proposal ownership. This wrapper only publishes committed
/// observations after the inner mutation has completed successfully.
struct HostedMetadataStateMachine {
    inner: MetadataRaftStateMachine,
    runtime: MetadataRuntimeHandle,
}

impl HostedMetadataStateMachine {
    fn new(inner: MetadataRaftStateMachine, runtime: MetadataRuntimeHandle) -> Self {
        runtime.publish_state(inner.state());

        Self { inner, runtime }
    }

    fn publish_after_apply(&mut self) {
        self.runtime.publish_state(self.inner.state());

        self.runtime
            .publish_results(self.inner.take_applied_results());
    }
}

impl RaftReadyStateMachine for HostedMetadataStateMachine {
    type Error = MetadataStateMachineError;

    fn restore_snapshot(&mut self, snapshot: &Snapshot<Vec<u8>>) -> Result<(), Self::Error> {
        self.inner.restore_snapshot(snapshot)?;

        self.runtime.publish_state(self.inner.state());

        // Proposal waiters do not survive snapshot/restart recovery.
        // The inner restore already clears its volatile result queue.
        Ok(())
    }

    fn apply(&mut self, index: u64, command: &[u8]) -> Result<(), Self::Error> {
        self.inner.apply(index, command)?;

        self.publish_after_apply();

        Ok(())
    }
}

/// Construct a genuinely new metadata replica from an already durable
/// RaftGroupBootstrap.
///
/// The caller MUST install/fsync the bootstrap before calling this function.
/// We intentionally do not accept the static seed list here; membership has
/// already crossed its durable authority boundary.
pub fn bootstrap_metadata_group<W>(
    bootstrap: &RaftGroupBootstrap,
    local_replica_id: ReplicaId,
    wal: W,
    snapshot_root: PathBuf,
    election_timeout: u64,
    heartbeat_interval: u64,
) -> Result<(Box<dyn HostedRaftGroup>, MetadataRuntimeHandle), MetadataReplicaStartupError>
where
    W: RaftWal + Send + 'static,
{
    bootstrap.validate()?;

    if !bootstrap.replica_to_node.contains_key(&local_replica_id) {
        return Err(MetadataReplicaStartupError::ReplicaMissingFromBootstrap(
            local_replica_id,
        ));
    }

    let identity = RaftReplicaIdentity::new(bootstrap.raft_group_id, local_replica_id)
        .map_err(|source| MetadataReplicaStartupError::Identity(source.to_string()))?;

    let conf_state = bootstrap.to_core_conf_state()?;

    let raft = RaftNode::bootstrap(
        local_replica_id
            .to_raft()
            .map_err(|reason| MetadataReplicaStartupError::Identity(reason.to_string()))?,
        conf_state,
        MemStorage::<Vec<u8>, Vec<u8>>::new(),
        MemStorage::<Vec<u8>, Vec<u8>>::new(),
        election_timeout,
        heartbeat_interval,
    )
    .map_err(|error| MetadataReplicaStartupError::RaftInitialization(format!("{error:?}")))?;

    let ready_loop = RaftReadyLoop::new(raft, RaftWalStorage::new(wal, identity));

    let snapshot_store = FileRaftSnapshotStore::new(snapshot_root)
        .map_err(|source| MetadataReplicaStartupError::SnapshotStore(source.to_string()))?;

    let runtime = MetadataRuntimeHandle::default();

    let state_machine =
        HostedMetadataStateMachine::new(MetadataRaftStateMachine::new(), runtime.clone());

    // Do not manually drain the bootstrap Ready here.
    //
    // The Phase-5.0 HostedRaftGroup pre-drains Ready before its first tick,
    // step, or proposal. Because the group is not driven until after
    // MultiRaftHost::activate(), initial Ready durability remains ordered behind
    // complete local replica registration.
    let group = ReadyLoopHostedGroup::new(ready_loop, state_machine, snapshot_store);

    Ok((Box::new(group), runtime))
}

/// Reconstruct the metadata projection from its durable snapshot plus committed
/// A-WAL suffix, then restart the same Raft replica.
///
/// Static seed membership is intentionally absent from this function. The
/// supplied bootstrap and the recovered committed ConfState are the only
/// membership authorities.
#[allow(clippy::too_many_arguments)]
pub fn recover_metadata_group<W>(
    bootstrap: &RaftGroupBootstrap,
    local_replica_id: ReplicaId,
    wal: W,
    durable_end_lsn: Lsn,
    recovered: &RecoveredRaftReplica,
    snapshot_root: PathBuf,
    election_timeout: u64,
    heartbeat_interval: u64,
) -> Result<(Box<dyn HostedRaftGroup>, MetadataRuntimeHandle), MetadataReplicaStartupError>
where
    W: RaftWal + Send + 'static,
{
    bootstrap.validate()?;

    if !bootstrap.replica_to_node.contains_key(&local_replica_id) {
        return Err(MetadataReplicaStartupError::ReplicaMissingFromBootstrap(
            local_replica_id,
        ));
    }

    let identity = RaftReplicaIdentity::new(bootstrap.raft_group_id, local_replica_id)
        .map_err(|source| MetadataReplicaStartupError::Identity(source.to_string()))?;

    if recovered.identity() != identity {
        return Err(MetadataReplicaStartupError::RecoveredIdentityMismatch);
    }

    let mut snapshot_store = FileRaftSnapshotStore::new(snapshot_root)
        .map_err(|source| MetadataReplicaStartupError::SnapshotStore(source.to_string()))?;

    let mut state_machine = MetadataRaftStateMachine::new();

    let mut applied_frontier = None;

    let mut applied_index = 0_u64;

    if let Some(pointer) = recovered.snapshot() {
        let snapshot = snapshot_store
            .load_verified(pointer)
            .map_err(|source| MetadataReplicaStartupError::SnapshotStore(source.to_string()))?;

        state_machine.restore_snapshot(&snapshot)?;

        applied_index = snapshot.last_included_index;

        applied_frontier = Some(AppliedRaftFrontier::new(
            snapshot.last_included_index,
            snapshot.last_included_term,
        ));
    } else if recovered.progress().truncated_through_index != 0 {
        return Err(
            MetadataReplicaStartupError::MissingSnapshotForCompactedLog {
                truncated_through: recovered.progress().truncated_through_index,
            },
        );
    }

    let commit = recovered
        .hard_state()
        .map(|state| state.commit)
        .unwrap_or(0);

    for entry in recovered.log_view().entries() {
        if entry.record.index <= applied_index {
            continue;
        }

        if entry.record.index > commit {
            break;
        }

        let expected = applied_index.saturating_add(1);

        if entry.record.index != expected {
            return Err(MetadataReplicaStartupError::CommittedSuffixGap {
                expected,
                received: entry.record.index,
            });
        }

        if let DurableRaftEntryPayload::Normal(command) = &entry.record.payload {
            state_machine.apply(entry.record.index, command)?;
        }

        applied_index = entry.record.index;

        applied_frontier = Some(AppliedRaftFrontier::new(
            entry.record.index,
            entry.record.term,
        ));
    }

    if applied_index != commit {
        return Err(MetadataReplicaStartupError::CommitNotReconstructed {
            commit,
            applied: applied_index,
        });
    }

    // Recovery reconstructs state, not process-local proposal completions.
    let _ = state_machine.take_applied_results();

    let adapters = RaftStorageAdapters::from_recovered(recovered)
        .map_err(|source| MetadataReplicaStartupError::RaftAdapter(source.to_string()))?;

    let raft = RaftNode::restart(
        local_replica_id
            .to_raft()
            .map_err(|reason| MetadataReplicaStartupError::Identity(reason.to_string()))?,
        adapters.log,
        adapters.stable,
        election_timeout,
        heartbeat_interval,
    )
    .map_err(|error| MetadataReplicaStartupError::RaftInitialization(format!("{error:?}")))?;

    let persistence = RaftWalStorage::from_recovered(wal, recovered, durable_end_lsn)?;

    let mut ready_loop = RaftReadyLoop::new(raft, persistence);

    if let Some(frontier) = applied_frontier {
        ready_loop.advance_applied_frontier(frontier)?;
    }

    let runtime = MetadataRuntimeHandle::default();

    let state_machine = HostedMetadataStateMachine::new(state_machine, runtime.clone());

    let group = ReadyLoopHostedGroup::new(ready_loop, state_machine, snapshot_store);

    Ok((Box::new(group), runtime))
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataReplicaStartupError {
    #[error("invalid metadata Raft bootstrap: {0}")]
    Bootstrap(#[from] RaftGroupBootstrapError),

    #[error("replica {0:?} is not present in the durable metadata bootstrap")]
    ReplicaMissingFromBootstrap(ReplicaId),

    #[error("recovered metadata Raft identity does not match durable bootstrap")]
    RecoveredIdentityMismatch,

    #[error("invalid metadata replica identity: {0}")]
    Identity(String),

    #[error("metadata Raft initialization failed: {0}")]
    RaftInitialization(String),

    #[error("metadata Raft storage adapter failed: {0}")]
    RaftAdapter(String),

    #[error("metadata snapshot store failed: {0}")]
    SnapshotStore(String),

    #[error("metadata state-machine failed: {0}")]
    StateMachine(#[from] MetadataStateMachineError),

    #[error(
        "metadata Raft log is compacted through {truncated_through} but has no durable state-machine snapshot"
    )]
    MissingSnapshotForCompactedLog { truncated_through: u64 },

    #[error("committed metadata suffix has a gap: expected index {expected}, received {received}")]
    CommittedSuffixGap { expected: u64, received: u64 },

    #[error("metadata HardState commit {commit} reconstructed only through index {applied}")]
    CommitNotReconstructed { commit: u64, applied: u64 },

    #[error("metadata Raft persistence initialization failed: {0}")]
    Persistence(#[from] RaftPersistenceError),

    #[error("metadata Ready-loop initialization failed: {0}")]
    Ready(#[from] ReadyLoopError),
}

/// One deterministic reconciliation step.
///
/// Metadata's configuration epoch and Raft ConfState's version are distinct
/// counters. Carry both so a later executor can prove it is still acting on the
/// state that the planner observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataReconcileAction {
    pub metadata_configuration_epoch: u64,
    pub expected_conf_state_version: u64,
    pub kind: MetadataReconcileActionKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataReconcileActionKind {
    /// Missing desired replicas are always introduced as learners first, even
    /// when the final desired role is voter.
    AddLearner {
        replica_id: ReplicaId,
        node_id: NodeId,
    },

    PromoteLearner {
        replica_id: ReplicaId,
        node_id: NodeId,
    },

    RemoveReplica {
        replica_id: ReplicaId,
    },
}

/// Compute at most one membership operation required to move committed Raft
/// configuration toward committed metadata desired placement.
///
/// This function does not mutate Raft and does not propose ConfChange entries.
/// Execution belongs to Phase 5.10.
///
/// Ordering is intentionally conservative:
///
/// 1. add a missing desired voter as learner,
/// 2. promote desired voters,
/// 3. add desired learners,
/// 4. remove replicas no longer desired.
///
/// Therefore metadata never asks the Raft group to remove its old voter before
/// the replacement voter exists.
pub fn next_reconcile_action(
    desired: &DesiredReplicaPlacement,
    observed: &ConfState,
) -> Result<Option<MetadataReconcileAction>, MetadataReconcileError> {
    desired
        .validate()
        .map_err(|error| MetadataReconcileError::InvalidDesiredPlacement(error.to_string()))?;

    observed
        .validate()
        .map_err(|error| MetadataReconcileError::InvalidObservedConfState(format!("{error:?}")))?;

    if !observed.outgoing_voters.is_empty() {
        return Err(MetadataReconcileError::JointConsensusInProgress);
    }

    let desired_by_raft_id: BTreeMap<raft::types::ReplicaId, &DesiredReplica> = desired
        .replicas
        .iter()
        .map(|replica| {
            let raft_id = replica.replica_id.to_raft().map_err(|reason| {
                MetadataReconcileError::InvalidDesiredReplica {
                    replica_id: replica.replica_id,
                    reason: reason.to_string(),
                }
            })?;

            Ok((raft_id, replica))
        })
        .collect::<Result<BTreeMap<_, _>, MetadataReconcileError>>()?;

    let action = |kind| MetadataReconcileAction {
        metadata_configuration_epoch: desired.configuration_epoch,

        expected_conf_state_version: observed.version,

        kind,
    };

    // A desired voter that does not exist must first enter as learner.
    for (raft_replica_id, replica) in &desired_by_raft_id {
        if replica.role == DesiredReplicaRole::Voter && !observed.contains(*raft_replica_id) {
            return Ok(Some(action(MetadataReconcileActionKind::AddLearner {
                replica_id: replica.replica_id,
                node_id: replica.node_id,
            })));
        }
    }

    // Only caught-up learners may later actually be promoted. Match-index
    // eligibility is checked by the Phase 5.10 executor; this pure planner only
    // identifies the desired structural transition.
    for (raft_replica_id, replica) in &desired_by_raft_id {
        if replica.role == DesiredReplicaRole::Voter && observed.is_learner(*raft_replica_id) {
            return Ok(Some(action(MetadataReconcileActionKind::PromoteLearner {
                replica_id: replica.replica_id,
                node_id: replica.node_id,
            })));
        }
    }

    // Desired learners that do not exist can now be added without delaying
    // creation/promotion of replacement voters.
    for (raft_replica_id, replica) in &desired_by_raft_id {
        if replica.role == DesiredReplicaRole::Learner && !observed.contains(*raft_replica_id) {
            return Ok(Some(action(MetadataReconcileActionKind::AddLearner {
                replica_id: replica.replica_id,
                node_id: replica.node_id,
            })));
        }
    }

    // The reusable Raft core deliberately has no direct Voter -> Learner
    // transition. Such a topology request must replace that replica lifetime.
    for (raft_replica_id, replica) in &desired_by_raft_id {
        if replica.role == DesiredReplicaRole::Learner && observed.is_voter(*raft_replica_id) {
            return Err(MetadataReconcileError::DesiredLearnerIsCommittedVoter(
                replica.replica_id,
            ));
        }
    }

    // Only after every desired voter exists in the correct role do we retire
    // obsolete members.
    for raft_replica_id in observed.replication_targets() {
        if !desired_by_raft_id.contains_key(&raft_replica_id) {
            return Ok(Some(action(MetadataReconcileActionKind::RemoveReplica {
                replica_id: ReplicaId::from_raft(raft_replica_id),
            })));
        }
    }

    Ok(None)
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MetadataReconcileError {
    #[error("invalid desired placement: {0}")]
    InvalidDesiredPlacement(String),

    #[error(
        "desired replica {} is invalid: {reason}",
        .replica_id.0
    )]
    InvalidDesiredReplica {
        replica_id: ReplicaId,
        reason: String,
    },

    #[error("observed Raft ConfState is invalid: {0}")]
    InvalidObservedConfState(String),

    #[error("membership reconciliation is paused while joint consensus is active")]
    JointConsensusInProgress,

    #[error(
        "desired learner {} is already a committed voter; replace its replica lifetime instead of demoting it",
        .0.0
    )]
    DesiredLearnerIsCommittedVoter(ReplicaId),
}
