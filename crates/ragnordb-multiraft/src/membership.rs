//! Placement reconciliation decisions for one Raft group.
//!
//! Metadata placement is intent; `ConfState` is observed consensus state. This
//! module is the small, side-effect-free boundary between the two. It never
//! mutates a Raft core and never treats a proposal index as a completed
//! membership transition.

use std::collections::BTreeMap;

use raft::types::{ConfChange, ConfChangeKind, ConfState};
use ragnordb_common::{
    ids::{NodeId, ReplicaId},
    metadata_codec::{DesiredReplicaPlacement, DesiredReplicaRole},
};

use crate::meta::{
    MetadataReconcileAction, MetadataReconcileActionKind, MetadataReconcileError,
    next_reconcile_action,
};

/// Runtime observations needed before one membership action can be proposed.
///
/// `target_prepared` is supplied by the lifecycle owner after the target has a
/// durable Creating record and route. The target's own Ready owner remains the
/// final authority for persistence/apply progress; this map is only the
/// scheduler's latest detached observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipObservation {
    pub desired: DesiredReplicaPlacement,
    pub observed: ConfState,
    pub local_replica_id: ReplicaId,
    pub leader_replica_id: Option<ReplicaId>,
    pub commit_index: u64,
    pub replica_match_indices: BTreeMap<ReplicaId, u64>,
    pub target_apply_indices: BTreeMap<ReplicaId, u64>,
    pub target_prepared: BTreeMap<ReplicaId, bool>,
    pub target_snapshot_pending: BTreeMap<ReplicaId, bool>,
    pub target_quarantined: BTreeMap<ReplicaId, bool>,
    pub same_replica_lifetime: BTreeMap<ReplicaId, bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipDecision {
    Action(MetadataReconcileAction),
    WaitForCatchUp {
        replica_id: ReplicaId,
        match_index: u64,
        commit_index: u64,
    },
    LeaderTransferRequired {
        replica_id: ReplicaId,
    },
    PausedJointConsensus,
    Noop,
}

/// Plan one safe transition from fresh observations.
pub fn plan_membership_reconciliation(
    observation: &MembershipObservation,
) -> Result<MembershipDecision, MetadataReconcileError> {
    let Some(action) = (match next_reconcile_action(&observation.desired, &observation.observed) {
        Err(MetadataReconcileError::JointConsensusInProgress) => {
            return Ok(MembershipDecision::PausedJointConsensus);
        }
        result => result?,
    }) else {
        return Ok(MembershipDecision::Noop);
    };

    match &action.kind {
        MetadataReconcileActionKind::AddLearner { replica_id, .. } => {
            if !observation
                .target_prepared
                .get(replica_id)
                .copied()
                .unwrap_or(false)
            {
                return Ok(MembershipDecision::WaitForCatchUp {
                    replica_id: *replica_id,
                    match_index: 0,
                    commit_index: observation.commit_index,
                });
            }
        }
        MetadataReconcileActionKind::PromoteLearner { replica_id, .. } => {
            let match_index = observation
                .replica_match_indices
                .get(replica_id)
                .copied()
                .unwrap_or(0);
            let apply_index = observation
                .target_apply_indices
                .get(replica_id)
                .copied()
                .unwrap_or(0);
            let not_ready = !observation
                .target_prepared
                .get(replica_id)
                .copied()
                .unwrap_or(false)
                || observation
                    .target_snapshot_pending
                    .get(replica_id)
                    .copied()
                    .unwrap_or(false)
                || observation
                    .target_quarantined
                    .get(replica_id)
                    .copied()
                    .unwrap_or(false)
                || !observation
                    .same_replica_lifetime
                    .get(replica_id)
                    .copied()
                    .unwrap_or(false);
            if not_ready
                || match_index < observation.commit_index
                || apply_index < observation.commit_index
            {
                return Ok(MembershipDecision::WaitForCatchUp {
                    replica_id: *replica_id,
                    match_index,
                    commit_index: observation.commit_index,
                });
            }
        }
        MetadataReconcileActionKind::RemoveReplica { replica_id } => {
            if observation.leader_replica_id == Some(*replica_id) {
                return Ok(MembershipDecision::LeaderTransferRequired {
                    replica_id: *replica_id,
                });
            }
            if observation.observed.voters.len() <= 1 {
                return Err(MetadataReconcileError::WouldRemoveLastVoter);
            }

            let desired_voters = observation
                .desired
                .replicas
                .iter()
                .filter(|replica| replica.role == DesiredReplicaRole::Voter)
                .map(|replica| replica.replica_id.to_raft())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|reason| MetadataReconcileError::InvalidDesiredReplica {
                    replica_id: *replica_id,
                    reason: reason.to_string(),
                })?;
            if !desired_voters
                .iter()
                .all(|replica_id| observation.observed.is_voter(*replica_id))
            {
                return Err(MetadataReconcileError::ReplacementNotCommitted);
            }
            if let Some((replica_id, match_index)) = desired_voters.iter().find_map(|replica_id| {
                let match_index = observation
                    .replica_match_indices
                    .get(&ReplicaId::from_raft(*replica_id))
                    .copied()
                    .unwrap_or(0);
                (match_index < observation.commit_index)
                    .then_some((ReplicaId::from_raft(*replica_id), match_index))
            }) {
                return Ok(MembershipDecision::WaitForCatchUp {
                    replica_id,
                    match_index,
                    commit_index: observation.commit_index,
                });
            }
            if !observation
                .same_replica_lifetime
                .get(replica_id)
                .copied()
                .unwrap_or(false)
            {
                return Err(MetadataReconcileError::ReplicaLifetimeMismatch(*replica_id));
            }
        }
    }

    Ok(MembershipDecision::Action(action))
}

/// Convert a planned action into the exact Raft proposal and re-check its
/// optimistic ConfState version immediately before admission.
pub fn conf_change_for_action(
    action: &MetadataReconcileAction,
    observed: &ConfState,
) -> Result<ConfChange, MetadataReconcileError> {
    if observed.version != action.expected_conf_state_version {
        return Err(MetadataReconcileError::StaleConfState {
            expected: action.expected_conf_state_version,
            observed: observed.version,
        });
    }
    let kind = match action.kind {
        MetadataReconcileActionKind::AddLearner { replica_id, .. } => {
            ConfChangeKind::AddLearner(replica_id.to_raft().map_err(|reason| {
                MetadataReconcileError::InvalidDesiredReplica {
                    replica_id,
                    reason: reason.to_string(),
                }
            })?)
        }
        MetadataReconcileActionKind::PromoteLearner { replica_id, .. } => {
            ConfChangeKind::PromoteLearner(replica_id.to_raft().map_err(|reason| {
                MetadataReconcileError::InvalidDesiredReplica {
                    replica_id,
                    reason: reason.to_string(),
                }
            })?)
        }
        MetadataReconcileActionKind::RemoveReplica { replica_id } => {
            ConfChangeKind::RemoveReplica(replica_id.to_raft().map_err(|reason| {
                MetadataReconcileError::InvalidDesiredReplica {
                    replica_id,
                    reason: reason.to_string(),
                }
            })?)
        }
    };
    Ok(ConfChange {
        expected_version: action.expected_conf_state_version,
        kind,
    })
}

/// Keep the node-directory lookup close to reconciliation so a metadata
/// action cannot silently target a physical node that is no longer routable.
pub fn desired_node_for_action(
    action: &MetadataReconcileAction,
    desired: &DesiredReplicaPlacement,
) -> Option<NodeId> {
    match action.kind {
        MetadataReconcileActionKind::AddLearner { node_id, .. }
        | MetadataReconcileActionKind::PromoteLearner { node_id, .. } => Some(node_id),
        MetadataReconcileActionKind::RemoveReplica { replica_id } => desired
            .replicas
            .iter()
            .find(|replica| replica.replica_id == replica_id)
            .map(|replica| replica.node_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ragnordb_common::metadata_codec::{DesiredReplica, PlacementPolicy};

    fn desired(replicas: Vec<DesiredReplica>) -> DesiredReplicaPlacement {
        DesiredReplicaPlacement {
            tablet_id: ragnordb_common::ids::TabletId(9),
            configuration_epoch: 4,
            replicas,
            placement_policy: PlacementPolicy::for_replica_count(2),
        }
    }

    fn observation(desired: DesiredReplicaPlacement, observed: ConfState) -> MembershipObservation {
        MembershipObservation {
            desired,
            observed,
            local_replica_id: ReplicaId(1),
            leader_replica_id: None,
            commit_index: 10,
            replica_match_indices: BTreeMap::new(),
            target_apply_indices: BTreeMap::new(),
            target_prepared: BTreeMap::new(),
            target_snapshot_pending: BTreeMap::new(),
            target_quarantined: BTreeMap::new(),
            same_replica_lifetime: BTreeMap::new(),
        }
    }

    /// Realistic bug caught: a placement scheduler must not emit AddLearner
    /// until the target has durably recorded its joining lifetime and route.
    #[test]
    fn add_learner_waits_for_target_preparation() {
        let desired = desired(vec![
            DesiredReplica {
                replica_id: ReplicaId(1),
                node_id: NodeId(11),
                role: DesiredReplicaRole::Voter,
            },
            DesiredReplica {
                replica_id: ReplicaId(2),
                node_id: NodeId(12),
                role: DesiredReplicaRole::Voter,
            },
        ]);
        let observed = ConfState::new(7, [ReplicaId(1).to_raft().unwrap()], []).unwrap();

        let decision = plan_membership_reconciliation(&observation(desired, observed)).unwrap();

        assert_eq!(
            decision,
            MembershipDecision::WaitForCatchUp {
                replica_id: ReplicaId(2),
                match_index: 0,
                commit_index: 10,
            }
        );
    }

    /// Realistic bug caught: a target that has received Raft entries but has
    /// not crossed the leader's committed and locally-applied frontier must
    /// remain a learner; promoting it early can expose incomplete state.
    #[test]
    fn promotion_waits_for_match_and_apply_frontiers() {
        let desired = desired(vec![
            DesiredReplica {
                replica_id: ReplicaId(1),
                node_id: NodeId(11),
                role: DesiredReplicaRole::Voter,
            },
            DesiredReplica {
                replica_id: ReplicaId(2),
                node_id: NodeId(12),
                role: DesiredReplicaRole::Voter,
            },
        ]);
        let observed = ConfState::new(
            8,
            [ReplicaId(1).to_raft().unwrap()],
            [ReplicaId(2).to_raft().unwrap()],
        )
        .unwrap();
        let mut pending = observation(desired.clone(), observed.clone());
        pending.target_prepared.insert(ReplicaId(2), true);
        pending.same_replica_lifetime.insert(ReplicaId(2), true);
        pending.replica_match_indices.insert(ReplicaId(2), 9);
        pending.target_apply_indices.insert(ReplicaId(2), 9);

        assert_eq!(
            plan_membership_reconciliation(&pending).unwrap(),
            MembershipDecision::WaitForCatchUp {
                replica_id: ReplicaId(2),
                match_index: 9,
                commit_index: 10,
            }
        );

        let mut ready = observation(desired, observed);
        ready.target_prepared.insert(ReplicaId(2), true);
        ready.same_replica_lifetime.insert(ReplicaId(2), true);
        ready.replica_match_indices.insert(ReplicaId(2), 10);
        ready.target_apply_indices.insert(ReplicaId(2), 10);

        assert!(matches!(
            plan_membership_reconciliation(&ready).unwrap(),
            MembershipDecision::Action(MetadataReconcileAction {
                kind: MetadataReconcileActionKind::PromoteLearner {
                    replica_id: ReplicaId(2),
                    node_id: NodeId(12),
                },
                expected_conf_state_version: 8,
                ..
            })
        ));
    }

    /// Realistic bug caught: removing the current leader without a transfer
    /// path strands the group in a state where the scheduler cannot safely
    /// make progress. Phase 5.10 must defer that action to Phase 5.11.
    #[test]
    fn leader_removal_is_typed_as_deferred_transfer() {
        let mut desired = desired(vec![DesiredReplica {
            replica_id: ReplicaId(2),
            node_id: NodeId(12),
            role: DesiredReplicaRole::Voter,
        }]);
        desired.placement_policy = PlacementPolicy::for_replica_count(1);
        let observed = ConfState::new(
            9,
            [
                ReplicaId(1).to_raft().unwrap(),
                ReplicaId(2).to_raft().unwrap(),
            ],
            [],
        )
        .unwrap();
        let mut observation = observation(desired, observed);
        observation.leader_replica_id = Some(ReplicaId(1));
        observation.same_replica_lifetime.insert(ReplicaId(1), true);

        assert_eq!(
            plan_membership_reconciliation(&observation).unwrap(),
            MembershipDecision::LeaderTransferRequired {
                replica_id: ReplicaId(1)
            }
        );
    }

    /// Realistic bug caught: even a committed replacement may have fallen
    /// behind again before old-voter removal is proposed. Removal must wait on
    /// the current replication frontier, not only on historical promotion.
    #[test]
    fn removal_waits_for_current_replacement_catch_up() {
        let mut desired = desired(vec![DesiredReplica {
            replica_id: ReplicaId(2),
            node_id: NodeId(12),
            role: DesiredReplicaRole::Voter,
        }]);
        desired.placement_policy = PlacementPolicy::for_replica_count(1);
        let observed = ConfState::new(
            9,
            [
                ReplicaId(1).to_raft().unwrap(),
                ReplicaId(2).to_raft().unwrap(),
            ],
            [],
        )
        .unwrap();
        let mut pending = observation(desired.clone(), observed.clone());
        pending.leader_replica_id = Some(ReplicaId(2));
        pending.same_replica_lifetime.insert(ReplicaId(1), true);
        pending.same_replica_lifetime.insert(ReplicaId(2), true);
        pending.replica_match_indices.insert(ReplicaId(2), 9);

        assert_eq!(
            plan_membership_reconciliation(&pending).unwrap(),
            MembershipDecision::WaitForCatchUp {
                replica_id: ReplicaId(2),
                match_index: 9,
                commit_index: 10,
            }
        );

        pending.replica_match_indices.insert(ReplicaId(2), 10);
        assert!(matches!(
            plan_membership_reconciliation(&pending).unwrap(),
            MembershipDecision::Action(MetadataReconcileAction {
                kind: MetadataReconcileActionKind::RemoveReplica {
                    replica_id: ReplicaId(1)
                },
                ..
            })
        ));
    }

    /// Realistic bug caught: a delayed reconciliation decision must never be
    /// applied to a newer committed ConfState version.
    #[test]
    fn stale_conf_state_rejects_the_proposal_conversion() {
        let action = MetadataReconcileAction {
            metadata_configuration_epoch: 4,
            expected_conf_state_version: 7,
            kind: MetadataReconcileActionKind::AddLearner {
                replica_id: ReplicaId(2),
                node_id: NodeId(12),
            },
        };
        let observed = ConfState::new(8, [ReplicaId(1).to_raft().unwrap()], []).unwrap();

        assert!(matches!(
            conf_change_for_action(&action, &observed),
            Err(MetadataReconcileError::StaleConfState {
                expected: 7,
                observed: 8,
            })
        ));
    }
}
