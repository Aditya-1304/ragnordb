//! Metadata Raft state-machine adapter and desired-membership reconciliation.
//!
//! The catalog crate owns metadata semantics. This module owns the boundary
//! between those semantics and the reusable Raft Ready runtime.
//!
//! Phase 5.1 deliberately stops before actually bootstrapping/registering the
//! metadata group. Phase 5.1a will construct a RaftReadyLoop using this state
//! machine through the already-frozen Phase 5.0 MultiRaft host.

use std::collections::BTreeMap;

use raft::types::{ConfState, Snapshot};

use ragnordb_catalog::{MetadataApplyOutcome, MetadataState};

use ragnordb_common::{
    ids::{NodeId, ReplicaId},
    metadata_codec::{
        DesiredReplica, DesiredReplicaPlacement, DesiredReplicaRole, MetadataCommand,
        MetadataCommandCodecError, MetadataSnapshot,
    },
};

use crate::runtime::RaftReadyStateMachine;

/// Result produced by the most recently applied metadata log entry.
///
/// Phase 5.2 can correlate this with the proposal waiter. Keeping it here now
/// prevents ordinary metadata-domain rejection from being expressed as a
/// state-machine failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedMetadataCommand {
    pub index: u64,
    pub outcome: MetadataApplyOutcome,
}

/// Raft-owned wrapper around the deterministic metadata projection.
///
/// State may change only through committed `apply()` calls or verified snapshot
/// restoration. Callers receive only a shared reference to the state.
#[derive(Debug, Default)]
pub struct MetadataRaftStateMachine {
    state: MetadataState,
    last_applied: Option<AppliedMetadataCommand>,
}

impl MetadataRaftStateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(&self) -> &MetadataState {
        &self.state
    }

    pub fn last_applied(&self) -> Option<&AppliedMetadataCommand> {
        self.last_applied.as_ref()
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
        self.last_applied = None;

        Ok(())
    }

    fn apply(&mut self, index: u64, command: &[u8]) -> Result<(), Self::Error> {
        let command =
            MetadataCommand::decode(command).map_err(MetadataStateMachineError::CommandDecode)?;

        let outcome = self.state.apply(command);

        self.last_applied = Some(AppliedMetadataCommand { index, outcome });

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
