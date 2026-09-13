//! Read-only node drain and decommission planning.
//!
//! Metadata remains the authority for desired placement and node lifecycle;
//! the MultiRaft host remains the authority for local committed membership and
//! leadership observation. This module joins those two snapshots without
//! mutating either one. A node is never reported safe for final decommission
//! while either authority still contains a live obligation for it.

use std::collections::BTreeSet;

use ragnordb_catalog::MetadataState;
use ragnordb_common::{
    ids::{NodeId, RaftGroupId, ReplicaId, TabletId},
    metadata_codec::{DesiredReplicaRole, NodeLifecycle},
};
use ragnordb_multiraft::host::{MultiRaftHostStatus, MultiRaftRole};

/// A condition that prevents the local node from reaching a safe terminal
/// lifecycle. Each variant identifies the authority that still requires work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeDrainBlocker {
    NodeNotRegistered,
    HostStatusUnavailable,
    ReplicaStillDesired {
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
    },
    ReplicaNotMaterialized {
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
    },
    LeaderTransferRequired {
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
    },
    PreferredLeaderOnDrainingNode {
        raft_group_id: RaftGroupId,
    },
    ReplicaStillInCommittedConfState {
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
    },
    JointConsensusInProgress {
        raft_group_id: RaftGroupId,
    },
    NoEligibleReplacement {
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
    },
    HostedReplicaNotInMetadata {
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
    },
}

/// Group-scoped drain facts used by the admin API and later durable jobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDrainGroupStatus {
    pub raft_group_id: RaftGroupId,
    pub tablet_id: Option<TabletId>,
    pub replica_id: ReplicaId,
    pub desired_role: Option<DesiredReplicaRole>,
    pub local_group_present: bool,
    pub local_role: Option<MultiRaftRole>,
    pub local_leader: bool,
    pub local_replica_in_committed_conf_state: bool,
    pub replacement_candidates: Vec<NodeId>,
    pub blockers: Vec<NodeDrainBlocker>,
}

/// Point-in-time preflight for one physical node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDrainStatus {
    pub node_id: NodeId,
    pub lifecycle: Option<NodeLifecycle>,
    pub desired_replica_count: usize,
    pub hosted_group_count: usize,
    pub leader_group_count: usize,
    pub groups: Vec<NodeDrainGroupStatus>,
    pub blockers: Vec<NodeDrainBlocker>,
    pub ready_for_decommissioned: bool,
}

/// Compute the administrative drain view from independently authoritative
/// snapshots. The caller must provide the local host status for the same
/// physical node represented by `node_id`.
pub fn compute_node_drain_status(
    metadata: &MetadataState,
    host_status: Option<&MultiRaftHostStatus>,
    node_id: NodeId,
) -> NodeDrainStatus {
    let lifecycle = metadata.node(node_id).map(|node| node.lifecycle);
    let Some(_) = lifecycle else {
        return NodeDrainStatus {
            node_id,
            lifecycle: None,
            desired_replica_count: 0,
            hosted_group_count: host_status.map_or(0, |status| status.groups.len()),
            leader_group_count: 0,
            groups: Vec::new(),
            blockers: vec![NodeDrainBlocker::NodeNotRegistered],
            ready_for_decommissioned: false,
        };
    };

    let host_status_matches_node = host_status.is_some_and(|status| status.node_id == node_id);
    let host_groups = if host_status_matches_node {
        host_status.map_or(&[][..], |status| status.groups.as_slice())
    } else {
        &[][..]
    };
    let mut groups = Vec::new();
    let mut blockers = if host_status_matches_node {
        Vec::new()
    } else {
        vec![NodeDrainBlocker::HostStatusUnavailable]
    };

    for tablet in metadata.tablets() {
        let Some(placement) = metadata.desired_placement(tablet.tablet_id) else {
            continue;
        };

        for desired_replica in placement
            .replicas
            .iter()
            .filter(|replica| replica.node_id == node_id)
        {
            let group_status = host_groups.iter().find(|group| {
                group.identity.raft_group_id == tablet.raft_group_id
                    && group.identity.replica_id == desired_replica.replica_id
            });
            let local_group_present = group_status.is_some();
            let local_role = group_status.and_then(|group| group.role);
            let local_leader = local_role == Some(MultiRaftRole::Leader);
            let local_replica_in_committed_conf_state = group_status.is_some_and(|group| {
                group.voters.contains(&desired_replica.replica_id)
                    || group.learners.contains(&desired_replica.replica_id)
                    || group.outgoing_voters.contains(&desired_replica.replica_id)
            });

            let replacement_candidates = if desired_replica.role == DesiredReplicaRole::Voter {
                eligible_replacement_nodes(metadata, placement, node_id)
            } else {
                Vec::new()
            };

            let mut blockers = vec![NodeDrainBlocker::ReplicaStillDesired {
                raft_group_id: tablet.raft_group_id,
                replica_id: desired_replica.replica_id,
            }];
            if !local_group_present {
                blockers.push(NodeDrainBlocker::ReplicaNotMaterialized {
                    raft_group_id: tablet.raft_group_id,
                    replica_id: desired_replica.replica_id,
                });
            }
            if local_leader {
                blockers.push(NodeDrainBlocker::LeaderTransferRequired {
                    raft_group_id: tablet.raft_group_id,
                    replica_id: desired_replica.replica_id,
                });
            }
            if placement
                .placement_policy
                .preferred_leader_nodes
                .contains(&node_id)
            {
                blockers.push(NodeDrainBlocker::PreferredLeaderOnDrainingNode {
                    raft_group_id: tablet.raft_group_id,
                });
            }
            if local_replica_in_committed_conf_state {
                blockers.push(NodeDrainBlocker::ReplicaStillInCommittedConfState {
                    raft_group_id: tablet.raft_group_id,
                    replica_id: desired_replica.replica_id,
                });
            }
            if group_status.is_some_and(|group| !group.outgoing_voters.is_empty()) {
                blockers.push(NodeDrainBlocker::JointConsensusInProgress {
                    raft_group_id: tablet.raft_group_id,
                });
            }
            if desired_replica.role == DesiredReplicaRole::Voter
                && replacement_candidates.is_empty()
            {
                blockers.push(NodeDrainBlocker::NoEligibleReplacement {
                    raft_group_id: tablet.raft_group_id,
                    replica_id: desired_replica.replica_id,
                });
            }

            groups.push(NodeDrainGroupStatus {
                raft_group_id: tablet.raft_group_id,
                tablet_id: Some(tablet.tablet_id),
                replica_id: desired_replica.replica_id,
                desired_role: Some(desired_replica.role),
                local_group_present,
                local_role,
                local_leader,
                local_replica_in_committed_conf_state,
                replacement_candidates,
                blockers,
            });
        }
    }

    for group in host_groups {
        let represented_by_desired_metadata = groups.iter().any(|desired| {
            desired.raft_group_id == group.identity.raft_group_id
                && desired.replica_id == group.identity.replica_id
        });
        if !represented_by_desired_metadata {
            let blocker = NodeDrainBlocker::HostedReplicaNotInMetadata {
                raft_group_id: group.identity.raft_group_id,
                replica_id: group.identity.replica_id,
            };
            groups.push(NodeDrainGroupStatus {
                raft_group_id: group.identity.raft_group_id,
                tablet_id: None,
                replica_id: group.identity.replica_id,
                desired_role: None,
                local_group_present: true,
                local_role: group.role,
                local_leader: group.role == Some(MultiRaftRole::Leader),
                local_replica_in_committed_conf_state: group
                    .voters
                    .contains(&group.identity.replica_id)
                    || group.learners.contains(&group.identity.replica_id)
                    || group.outgoing_voters.contains(&group.identity.replica_id),
                replacement_candidates: Vec::new(),
                blockers: vec![blocker.clone()],
            });
        }
    }

    blockers.extend(
        groups
            .iter()
            .flat_map(|group| group.blockers.iter().cloned()),
    );
    let leader_group_count = groups.iter().filter(|group| group.local_leader).count();

    NodeDrainStatus {
        node_id,
        lifecycle,
        desired_replica_count: groups
            .iter()
            .filter(|group| group.desired_role.is_some())
            .count(),
        hosted_group_count: host_groups.len(),
        leader_group_count,
        groups,
        ready_for_decommissioned: lifecycle == Some(NodeLifecycle::Decommissioning)
            && blockers.is_empty(),
        blockers,
    }
}

fn eligible_replacement_nodes(
    metadata: &MetadataState,
    placement: &ragnordb_common::metadata_codec::DesiredReplicaPlacement,
    removed_node_id: NodeId,
) -> Vec<NodeId> {
    let survivors = placement
        .replicas
        .iter()
        .filter(|replica| replica.node_id != removed_node_id)
        .cloned()
        .collect::<Vec<_>>();
    let existing_nodes = survivors
        .iter()
        .map(|replica| replica.node_id)
        .collect::<BTreeSet<_>>();

    metadata
        .nodes()
        .filter(|candidate| {
            candidate.lifecycle == NodeLifecycle::Active
                && candidate.node_id != removed_node_id
                && !existing_nodes.contains(&candidate.node_id)
                && placement
                    .placement_policy
                    .required_storage_class
                    .as_ref()
                    .is_none_or(|required| candidate.storage_class == *required)
        })
        .filter(|candidate| {
            let mut replacement = survivors.clone();
            replacement.push(ragnordb_common::metadata_codec::DesiredReplica {
                replica_id: ReplicaId(u64::MAX),
                node_id: candidate.node_id,
                role: DesiredReplicaRole::Voter,
            });
            satisfies_policy(metadata, &replacement, &placement.placement_policy)
        })
        .map(|candidate| candidate.node_id)
        .collect()
}

fn satisfies_policy(
    metadata: &MetadataState,
    replicas: &[ragnordb_common::metadata_codec::DesiredReplica],
    policy: &ragnordb_common::metadata_codec::PlacementPolicy,
) -> bool {
    let voter_count = replicas
        .iter()
        .filter(|replica| replica.role == DesiredReplicaRole::Voter)
        .count();
    if voter_count != policy.replication_factor as usize {
        return false;
    }

    for (required, values) in [
        (
            policy.min_distinct_regions,
            replicas
                .iter()
                .filter_map(|replica| metadata.node(replica.node_id)?.region.as_deref())
                .collect::<BTreeSet<_>>(),
        ),
        (
            policy.min_distinct_zones,
            replicas
                .iter()
                .filter_map(|replica| metadata.node(replica.node_id)?.zone.as_deref())
                .collect::<BTreeSet<_>>(),
        ),
        (
            policy.min_distinct_racks,
            replicas
                .iter()
                .filter_map(|replica| metadata.node(replica.node_id)?.rack.as_deref())
                .collect::<BTreeSet<_>>(),
        ),
    ] {
        if values.len() < required as usize {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use ragnordb_common::{
        catalog_codec::{ColumnDefinition, DataType, TableDefinition},
        ids::{ColumnId, TableId},
        metadata_codec::{
            DesiredReplica, DesiredReplicaPlacement, MetadataCommand, NodeDescriptor,
            PartitionSpec, PlacementPolicy, TabletDescriptor,
        },
    };
    use ragnordb_multiraft::host::MultiRaftGroupStatus;

    fn node(node_id: u64, base_port: u16) -> NodeDescriptor {
        NodeDescriptor {
            node_id: NodeId(node_id),
            raft_addr: format!("127.0.0.1:{base_port}"),
            snapshot_addr: format!("127.0.0.1:{}", base_port + 50),
            sql_addr: format!("127.0.0.1:{}", base_port + 100),
            admin_addr: format!("127.0.0.1:{}", base_port + 200),
            region: Some(format!("region-{node_id}")),
            zone: Some(format!("zone-{node_id}")),
            rack: Some(format!("rack-{node_id}")),
            storage_class: "default".to_string(),
            lifecycle: NodeLifecycle::Active,
        }
    }

    fn metadata_with_placement(include_replacement: bool) -> MetadataState {
        let mut metadata = MetadataState::new();
        assert_eq!(
            metadata.apply(MetadataCommand::ClusterInitialized {
                cluster_id: "cluster-a".to_string(),
            }),
            ragnordb_catalog::MetadataApplyOutcome::Applied
        );
        for descriptor in [node(11, 7001), node(12, 7002)] {
            assert_eq!(
                metadata.apply(MetadataCommand::RegisterNode(descriptor)),
                ragnordb_catalog::MetadataApplyOutcome::Applied
            );
        }
        if include_replacement {
            assert_eq!(
                metadata.apply(MetadataCommand::RegisterNode(node(13, 7003))),
                ragnordb_catalog::MetadataApplyOutcome::Applied
            );
        }

        assert_eq!(
            metadata.apply(MetadataCommand::CreateTable {
                table: TableDefinition {
                    table_id: 7,
                    name: "accounts".to_string(),
                    columns: vec![ColumnDefinition {
                        column_id: ColumnId(1),
                        name: "id".to_string(),
                        ty: DataType::Int,
                        nullable: false,
                    }],
                    primary_key_column_ids: vec![ColumnId(1)],
                    schema_version: 1,
                    tablet_count: 1,
                },
            }),
            ragnordb_catalog::MetadataApplyOutcome::Applied
        );
        assert_eq!(
            metadata.apply(MetadataCommand::CreateTablet {
                tablet: TabletDescriptor {
                    tablet_id: TabletId(17),
                    table_id: TableId(7),
                    raft_group_id: RaftGroupId(23),
                    tablet_epoch: 1,
                    partition: PartitionSpec::Hash {
                        bucket: 0,
                        bucket_count: 1,
                    },
                },
            }),
            ragnordb_catalog::MetadataApplyOutcome::Applied
        );
        assert_eq!(
            metadata.apply(MetadataCommand::SetDesiredReplicaPlacement(
                DesiredReplicaPlacement {
                    tablet_id: TabletId(17),
                    configuration_epoch: 1,
                    replicas: vec![
                        DesiredReplica {
                            replica_id: ReplicaId(31),
                            node_id: NodeId(11),
                            role: DesiredReplicaRole::Voter,
                        },
                        DesiredReplica {
                            replica_id: ReplicaId(32),
                            node_id: NodeId(12),
                            role: DesiredReplicaRole::Voter,
                        },
                    ],
                    placement_policy: PlacementPolicy::for_replica_count(2),
                },
            )),
            ragnordb_catalog::MetadataApplyOutcome::Applied
        );
        assert_eq!(
            metadata.apply(MetadataCommand::SetNodeLifecycle {
                node_id: NodeId(11),
                lifecycle: NodeLifecycle::Draining,
            }),
            ragnordb_catalog::MetadataApplyOutcome::Applied
        );
        metadata
    }

    fn host_status() -> MultiRaftHostStatus {
        MultiRaftHostStatus {
            node_id: NodeId(11),
            state: ragnordb_multiraft::host::MultiRaftHostState::Active,
            pending_message_count: 0,
            pending_message_bytes: 0,
            groups: vec![MultiRaftGroupStatus {
                identity: ragnordb_multiraft::storage::codec::RaftReplicaIdentity {
                    raft_group_id: RaftGroupId(23),
                    replica_id: ReplicaId(31),
                },
                role: Some(MultiRaftRole::Leader),
                leader_replica_id: Some(ReplicaId(31)),
                term: 4,
                commit_index: 10,
                last_log_index: 10,
                applied_index: 10,
                snapshot_index: 0,
                uncommitted_bytes: 0,
                replication_inflight_bytes: 0,
                pending_work: false,
                pending_messages: 0,
                pending_message_bytes: 0,
                quarantine_reason: None,
                conf_state_version: Some(2),
                joining: false,
                voters: vec![ReplicaId(31), ReplicaId(32)],
                learners: Vec::new(),
                outgoing_voters: Vec::new(),
                replica_match_indices: vec![(ReplicaId(31), 10), (ReplicaId(32), 10)],
                pending_conf_change_index: None,
                last_conf_change: None,
                last_removed_replica: None,
            }],
        }
    }

    #[test]
    fn drain_status_surfaces_leader_and_replacement_blockers() {
        let metadata = metadata_with_placement(true);
        let status = compute_node_drain_status(&metadata, Some(&host_status()), NodeId(11));

        assert_eq!(status.lifecycle, Some(NodeLifecycle::Draining));
        assert_eq!(status.desired_replica_count, 1);
        assert_eq!(status.leader_group_count, 1);
        assert_eq!(status.groups[0].replacement_candidates, vec![NodeId(13)]);
        assert!(
            status
                .blockers
                .contains(&NodeDrainBlocker::LeaderTransferRequired {
                    raft_group_id: RaftGroupId(23),
                    replica_id: ReplicaId(31),
                })
        );
        assert!(!status.ready_for_decommissioned);
    }

    #[test]
    fn drain_status_reports_missing_failure_safe_replacement() {
        let metadata = metadata_with_placement(false);
        let status = compute_node_drain_status(&metadata, Some(&host_status()), NodeId(11));

        assert!(status.groups[0].replacement_candidates.is_empty());
        assert!(
            status
                .blockers
                .contains(&NodeDrainBlocker::NoEligibleReplacement {
                    raft_group_id: RaftGroupId(23),
                    replica_id: ReplicaId(31),
                })
        );
    }

    #[test]
    fn drain_status_fails_closed_for_untracked_local_membership() {
        let metadata = metadata_with_placement(true);
        let mut host = host_status();
        host.groups.push(MultiRaftGroupStatus {
            identity: ragnordb_multiraft::storage::codec::RaftReplicaIdentity {
                raft_group_id: RaftGroupId(99),
                replica_id: ReplicaId(51),
            },
            role: Some(MultiRaftRole::Follower),
            leader_replica_id: Some(ReplicaId(52)),
            term: 1,
            commit_index: 4,
            last_log_index: 4,
            applied_index: 4,
            snapshot_index: 0,
            uncommitted_bytes: 0,
            replication_inflight_bytes: 0,
            pending_work: false,
            pending_messages: 0,
            pending_message_bytes: 0,
            quarantine_reason: None,
            conf_state_version: Some(1),
            joining: false,
            voters: vec![ReplicaId(51), ReplicaId(52)],
            learners: Vec::new(),
            outgoing_voters: Vec::new(),
            replica_match_indices: Vec::new(),
            pending_conf_change_index: None,
            last_conf_change: None,
            last_removed_replica: None,
        });

        let status = compute_node_drain_status(&metadata, Some(&host), NodeId(11));

        assert!(
            status
                .blockers
                .contains(&NodeDrainBlocker::HostedReplicaNotInMetadata {
                    raft_group_id: RaftGroupId(99),
                    replica_id: ReplicaId(51),
                })
        );
        assert!(!status.ready_for_decommissioned);
    }
}
