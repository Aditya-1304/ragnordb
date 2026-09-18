use ragnordb_catalog::{
    MetadataApplyOutcome, MetadataRejection, MetadataState, MetadataTableCreated,
};

use ragnordb_common::{
    catalog_codec::{ColumnDefinition, DataType, TableDefinition},
    ids::{
        ClientRequestId, ColumnId, CommandKind, LogicalCommandId, NodeId, RaftGroupId, ReplicaId,
        RequestId, TableId, TabletId, Timestamp,
    },
    metadata_codec::{
        CreateTableRequest, DesiredReplica, DesiredReplicaPlacement, DesiredReplicaRole,
        MetadataCommand, NodeDescriptor, NodeLifecycle, PartitionSpec, PlacementPolicy,
        TabletDescriptor,
    },
};

fn create_request(name: &str) -> CreateTableRequest {
    CreateTableRequest {
        table_name: name.to_string(),

        columns: vec![
            ColumnDefinition {
                column_id: ColumnId(1),
                name: "id".to_string(),
                ty: DataType::Int,
                nullable: false,
            },
            ColumnDefinition {
                column_id: ColumnId(2),
                name: "value".to_string(),
                ty: DataType::Text,
                nullable: true,
            },
        ],

        primary_key_column_ids: vec![ColumnId(1)],
    }
}

fn table() -> TableDefinition {
    TableDefinition {
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
        tablet_count: 2,
    }
}

fn node(id: u64, base_port: u16) -> NodeDescriptor {
    NodeDescriptor {
        node_id: NodeId(id),

        raft_addr: format!("127.0.0.1:{base_port}"),

        snapshot_addr: format!("127.0.0.1:{}", base_port + 50),

        sql_addr: format!("127.0.0.1:{}", base_port + 100),

        admin_addr: format!("127.0.0.1:{}", base_port + 200),
        region: None,
        zone: None,
        rack: None,
        storage_class: "default".to_string(),
        lifecycle: NodeLifecycle::Active,
    }
}

fn tablet() -> TabletDescriptor {
    TabletDescriptor {
        tablet_id: TabletId(17),
        table_id: TableId(7),
        raft_group_id: RaftGroupId(23),
        tablet_epoch: 1,

        partition: PartitionSpec::Hash {
            bucket: 0,
            bucket_count: 2,
        },
    }
}

fn bootstrap_state() -> MetadataState {
    let mut state = MetadataState::new();

    assert_eq!(
        state.apply(MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        },),
        MetadataApplyOutcome::Applied,
    );

    for node in [node(11, 7001), node(12, 7002), node(13, 7003)] {
        assert_eq!(
            state.apply(MetadataCommand::RegisterNode(node),),
            MetadataApplyOutcome::Applied,
        );
    }

    assert_eq!(
        state.apply(MetadataCommand::CreateTable { table: table() },),
        MetadataApplyOutcome::Applied,
    );

    assert_eq!(
        state.apply(MetadataCommand::CreateTablet { tablet: tablet() },),
        MetadataApplyOutcome::Applied,
    );

    state
}

fn initialized_state_with_nodes() -> MetadataState {
    let mut state = MetadataState::new();

    assert_eq!(
        state.apply(MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        }),
        MetadataApplyOutcome::Applied,
    );

    for node in [node(11, 7001), node(12, 7002), node(13, 7003)] {
        assert_eq!(
            state.apply(MetadataCommand::RegisterNode(node)),
            MetadataApplyOutcome::Applied,
        );
    }

    state
}

#[test]
fn atomic_create_table_publishes_complete_initial_topology() {
    let mut state = MetadataState::new();

    assert_eq!(
        state.apply(MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        }),
        MetadataApplyOutcome::Applied,
    );

    for node in [node(11, 7001), node(12, 7002), node(13, 7003)] {
        assert_eq!(
            state.apply(MetadataCommand::RegisterNode(node)),
            MetadataApplyOutcome::Applied,
        );
    }

    assert_eq!(
        state.apply(MetadataCommand::CreateTableTopology(create_request(
            "accounts"
        ))),
        MetadataApplyOutcome::TableCreated(ragnordb_catalog::MetadataTableCreated {
            table_id: TableId(2),
            tablet_id: TabletId(2),
            raft_group_id: RaftGroupId(3),
        },),
    );

    let table = state.table(TableId(2)).unwrap();
    assert_eq!(table.name, "accounts");
    assert_eq!(table.schema_version, 1);
    assert_eq!(table.tablet_count, 1);

    assert_eq!(
        state.tablet(TabletId(2)).unwrap().partition,
        PartitionSpec::Range {
            start_key: Vec::new(),
            end_key: Vec::new(),
        }
    );

    let placement = state.desired_placement(TabletId(2)).unwrap();
    assert_eq!(placement.configuration_epoch, 1);
    assert_eq!(placement.placement_policy.replication_factor, 3);
    assert_eq!(placement.replicas.len(), 3);
    assert_eq!(placement.replicas[0].node_id, NodeId(11));
    assert_eq!(placement.replicas[1].node_id, NodeId(12));
    assert_eq!(placement.replicas[2].node_id, NodeId(13));

    assert_eq!(state.allocator_state().max_table_id, 2);
    assert_eq!(state.allocator_state().max_tablet_id, 2);
    assert_eq!(state.allocator_state().max_raft_group_id, 3);
}

#[test]
fn initial_topology_spreads_voters_across_known_failure_domains() {
    let mut state = MetadataState::new();
    assert_eq!(
        state.apply(MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        }),
        MetadataApplyOutcome::Applied
    );
    for (id, zone) in [(11, "zone-a"), (12, "zone-b"), (13, "zone-c")] {
        let mut descriptor = node(id, 7100 + id as u16);
        descriptor.region = Some("region-a".to_string());
        descriptor.zone = Some(zone.to_string());
        descriptor.rack = Some(format!("rack-{zone}"));
        assert_eq!(
            state.apply(MetadataCommand::RegisterNode(descriptor)),
            MetadataApplyOutcome::Applied
        );
    }

    let outcome = state.apply(MetadataCommand::CreateTableTopology(create_request(
        "spread",
    )));
    let MetadataApplyOutcome::TableCreated(created) = outcome else {
        panic!("expected a topology allocation");
    };
    let placement = state.desired_placement(created.tablet_id).unwrap();
    assert_eq!(
        placement
            .replicas
            .iter()
            .map(|replica| replica.node_id)
            .collect::<Vec<_>>(),
        vec![NodeId(11), NodeId(12), NodeId(13)]
    );
    assert_eq!(placement.placement_policy.min_distinct_regions, 1);
    assert_eq!(placement.placement_policy.min_distinct_zones, 3);
    assert_eq!(placement.placement_policy.min_distinct_racks, 3);
}

#[test]
fn metadata_deduplication_includes_session_epoch() {
    let mut state = initialized_state_with_nodes();
    let first = LogicalCommandId {
        client_request_id: ClientRequestId {
            client_id: 99,
            session_epoch: 1,
            request_sequence: 1,
        },
        command_ordinal: 1,
        kind: CommandKind::Catalog,
    };
    let second = LogicalCommandId {
        client_request_id: ClientRequestId {
            client_id: 99,
            session_epoch: 2,
            request_sequence: 1,
        },
        command_ordinal: 1,
        kind: CommandKind::Catalog,
    };

    assert!(matches!(
        state.apply_with_logical_command_id(
            first,
            MetadataCommand::CreateTableTopology(create_request("epoch_one"))
        ),
        MetadataApplyOutcome::TableCreated(_)
    ));
    assert!(matches!(
        state.apply_with_logical_command_id(
            second,
            MetadataCommand::CreateTableTopology(create_request("epoch_two"))
        ),
        MetadataApplyOutcome::TableCreated(_)
    ));
    assert!(state.table(TableId(2)).is_some());
    assert!(state.table(TableId(3)).is_some());
}

#[test]
fn atomic_create_table_uses_canonical_node_id_order() {
    let mut state = MetadataState::new();

    assert_eq!(
        state.apply(MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        }),
        MetadataApplyOutcome::Applied,
    );

    for descriptor in [node(30, 8700), node(10, 8100), node(20, 8400)] {
        assert_eq!(
            state.apply(MetadataCommand::RegisterNode(descriptor)),
            MetadataApplyOutcome::Applied,
        );
    }

    assert!(matches!(
        state.apply(MetadataCommand::CreateTableTopology(create_request(
            "ordered"
        ))),
        MetadataApplyOutcome::TableCreated(_)
    ));

    let placement = state.desired_placement(TabletId(2)).unwrap();

    assert_eq!(
        placement
            .replicas
            .iter()
            .map(|replica| (replica.replica_id, replica.node_id))
            .collect::<Vec<_>>(),
        vec![
            (ReplicaId(1), NodeId(10)),
            (ReplicaId(2), NodeId(20)),
            (ReplicaId(3), NodeId(30)),
        ],
    );
}

#[test]
fn atomic_create_table_caps_initial_replication_at_three_nodes() {
    let mut state = MetadataState::new();

    assert_eq!(
        state.apply(MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        }),
        MetadataApplyOutcome::Applied,
    );

    for descriptor in [
        node(10, 8100),
        node(20, 8400),
        node(30, 8700),
        node(40, 9000),
    ] {
        assert_eq!(
            state.apply(MetadataCommand::RegisterNode(descriptor)),
            MetadataApplyOutcome::Applied,
        );
    }

    assert!(matches!(
        state.apply(MetadataCommand::CreateTableTopology(create_request(
            "bounded"
        ))),
        MetadataApplyOutcome::TableCreated(_)
    ));

    let placement = state.desired_placement(TabletId(2)).unwrap();

    assert_eq!(placement.replicas.len(), 3);
    assert!(
        placement
            .replicas
            .iter()
            .all(|replica| replica.node_id != NodeId(40))
    );
}

/// Realistic bug caught:
///
/// A node must be drainable without changing its stable network identity.
/// Treating the lifecycle transition as a registration conflict would leave
/// metadata unable to stop new placement on a node that is being retired.
#[test]
fn registered_node_can_advance_lifecycle_without_changing_directory_identity() {
    let mut state = MetadataState::new();

    assert_eq!(
        state.apply(MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        }),
        MetadataApplyOutcome::Applied,
    );

    let active = node(11, 7001);
    assert_eq!(
        state.apply(MetadataCommand::RegisterNode(active.clone())),
        MetadataApplyOutcome::Applied,
    );

    let mut draining = active;
    draining.lifecycle = NodeLifecycle::Draining;

    assert_eq!(
        state.apply(MetadataCommand::RegisterNode(draining)),
        MetadataApplyOutcome::Applied,
    );
    assert_eq!(
        state.node(NodeId(11)).unwrap().lifecycle,
        NodeLifecycle::Draining
    );
}

/// Realistic bug caught:
///
/// A decommission request that skips the drain and migration phases could
/// make metadata report a terminal node lifecycle while desired placements
/// still reference live replicas on that node.
#[test]
fn lifecycle_transition_cannot_skip_required_safety_phases() {
    let mut state = MetadataState::new();

    assert_eq!(
        state.apply(MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        }),
        MetadataApplyOutcome::Applied,
    );

    let active = node(11, 7001);
    assert_eq!(
        state.apply(MetadataCommand::RegisterNode(active.clone())),
        MetadataApplyOutcome::Applied,
    );

    let mut decommissioned = active;
    decommissioned.lifecycle = NodeLifecycle::Decommissioned;

    assert!(matches!(
        state.apply(MetadataCommand::RegisterNode(decommissioned)),
        MetadataApplyOutcome::Rejected(MetadataRejection::InvalidNodeLifecycleTransition {
            node_id: NodeId(11),
            from: "active",
            to: "decommissioned",
        })
    ));
    assert_eq!(
        state.node(NodeId(11)).unwrap().lifecycle,
        NodeLifecycle::Active
    );
}

/// Realistic bug caught:
///
/// An administrative lifecycle command must be idempotent while preserving
/// the endpoint directory and must not provide a second path around the
/// ordered drain protocol.
#[test]
fn lifecycle_control_is_idempotent_and_preserves_directory_identity() {
    let mut state = MetadataState::new();

    assert_eq!(
        state.apply(MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        }),
        MetadataApplyOutcome::Applied,
    );

    let active = node(11, 7001);
    assert_eq!(
        state.apply(MetadataCommand::RegisterNode(active.clone())),
        MetadataApplyOutcome::Applied,
    );
    assert_eq!(
        state.apply(MetadataCommand::SetNodeLifecycle {
            node_id: NodeId(11),
            lifecycle: NodeLifecycle::Draining,
        }),
        MetadataApplyOutcome::Applied,
    );
    assert_eq!(
        state.apply(MetadataCommand::SetNodeLifecycle {
            node_id: NodeId(11),
            lifecycle: NodeLifecycle::Draining,
        }),
        MetadataApplyOutcome::AlreadyApplied,
    );
    assert_eq!(state.node(NodeId(11)).unwrap().raft_addr, active.raft_addr);
    let restored = MetadataState::from_snapshot(state.to_snapshot()).unwrap();
    assert_eq!(
        restored.node(NodeId(11)).unwrap().lifecycle,
        NodeLifecycle::Draining
    );
    assert!(matches!(
        state.apply(MetadataCommand::SetNodeLifecycle {
            node_id: NodeId(11),
            lifecycle: NodeLifecycle::Decommissioned,
        }),
        MetadataApplyOutcome::Rejected(MetadataRejection::InvalidNodeLifecycleTransition {
            node_id: NodeId(11),
            from: "draining",
            to: "decommissioned",
        })
    ));
}

#[test]
fn terminal_lifecycle_requires_metadata_replica_obligations_to_be_removed() {
    let mut state = bootstrap_state();
    let original = DesiredReplicaPlacement {
        tablet_id: TabletId(17),
        configuration_epoch: 1,
        placement_policy: PlacementPolicy::for_replica_count(1),
        replicas: vec![DesiredReplica {
            replica_id: ReplicaId(31),
            node_id: NodeId(11),
            role: DesiredReplicaRole::Voter,
        }],
    };
    assert_eq!(
        state.apply(MetadataCommand::SetDesiredReplicaPlacement(original)),
        MetadataApplyOutcome::Applied
    );
    for lifecycle in [NodeLifecycle::Draining, NodeLifecycle::Decommissioning] {
        assert_eq!(
            state.apply(MetadataCommand::SetNodeLifecycle {
                node_id: NodeId(11),
                lifecycle,
            }),
            MetadataApplyOutcome::Applied
        );
    }

    assert!(matches!(
        state.apply(MetadataCommand::SetNodeLifecycle {
            node_id: NodeId(11),
            lifecycle: NodeLifecycle::Decommissioned,
        }),
        MetadataApplyOutcome::Rejected(MetadataRejection::NodeStillExpected {
            node_id: NodeId(11),
            raft_group_id: RaftGroupId(23),
            replica_id: ReplicaId(31),
        })
    ));

    assert_eq!(
        state.apply(MetadataCommand::SetDesiredReplicaPlacement(
            DesiredReplicaPlacement {
                tablet_id: TabletId(17),
                configuration_epoch: 2,
                placement_policy: PlacementPolicy::for_replica_count(1),
                replicas: vec![DesiredReplica {
                    replica_id: ReplicaId(32),
                    node_id: NodeId(12),
                    role: DesiredReplicaRole::Voter,
                }],
            },
        )),
        MetadataApplyOutcome::Applied
    );
    assert_eq!(
        state.apply(MetadataCommand::SetNodeLifecycle {
            node_id: NodeId(11),
            lifecycle: NodeLifecycle::Decommissioned,
        }),
        MetadataApplyOutcome::Applied
    );
}

#[test]
fn client_retry_session_epoch_and_horizon_survive_metadata_snapshot() {
    let mut state = MetadataState::new();
    assert_eq!(
        state.apply(MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        }),
        MetadataApplyOutcome::Applied,
    );

    assert_eq!(
        state.apply(MetadataCommand::RegisterClient {
            client_id: 55,
            requested_session_epoch: 0,
        }),
        MetadataApplyOutcome::ClientRegistered {
            client_id: 55,
            session_epoch: 1,
        },
    );
    assert_eq!(
        state.apply(MetadataCommand::RenewClient {
            client_id: 55,
            session_epoch: 1,
            acknowledged_through: 3,
        }),
        MetadataApplyOutcome::ClientRenewed,
    );
    assert!(matches!(
        state.apply(MetadataCommand::RenewClient {
            client_id: 55,
            session_epoch: 0,
            acknowledged_through: 4,
        }),
        MetadataApplyOutcome::Rejected(MetadataRejection::InvalidCommand(_))
    ));

    let recovered = MetadataState::from_snapshot(state.to_snapshot()).unwrap();
    let session = recovered.client_session(55).unwrap();
    assert_eq!(session.session_epoch, 1);
    assert_eq!(session.acknowledged_through, 3);
    assert_eq!(session.first_retained_sequence, 4);
}

#[test]
fn rejected_duplicate_table_name_does_not_advance_allocators() {
    let mut state = initialized_state_with_nodes();

    assert!(matches!(
        state.apply(MetadataCommand::CreateTableTopology(create_request(
            "accounts"
        ))),
        MetadataApplyOutcome::TableCreated(_)
    ));

    let before = state.allocator_state();

    assert_eq!(
        state.apply(MetadataCommand::CreateTableTopology(create_request(
            "accounts"
        ))),
        MetadataApplyOutcome::Rejected(MetadataRejection::TableNameConflict(
            "accounts".to_string(),
        )),
    );

    assert_eq!(
        state.allocator_state(),
        before,
        "rejected CREATE TABLE must consume no cluster-global identity",
    );
}

#[test]
fn metadata_request_replay_returns_the_original_create_without_reallocation() {
    let mut state = initialized_state_with_nodes();
    let request_id = RequestId {
        client_id: 0x42,
        sequence: 1,
        raft_group_id: RaftGroupId(2),
    };
    let request = create_request("accounts");

    let first = state.apply_with_request_id(
        request_id.clone(),
        MetadataCommand::CreateTableTopology(request.clone()),
    );
    let before_replay = state.allocator_state();
    let replay = state.apply_with_request_id(
        request_id,
        MetadataCommand::CreateTableTopology(request.clone()),
    );

    assert_eq!(replay, first);
    assert_eq!(state.allocator_state(), before_replay);

    // A separate CREATE TABLE request with the same schema is independent and
    // must not be mistaken for a retry of the first request.
    let independent = state.apply_with_request_id(
        RequestId {
            client_id: 0x43,
            sequence: 1,
            raft_group_id: RaftGroupId(2),
        },
        MetadataCommand::CreateTableTopology(request),
    );

    assert_eq!(
        independent,
        MetadataApplyOutcome::Rejected(MetadataRejection::TableNameConflict(
            "accounts".to_string(),
        )),
    );
    assert_eq!(state.allocator_state(), before_replay);
}

#[test]
fn metadata_request_deduplication_survives_snapshot_restore() {
    let mut state = initialized_state_with_nodes();
    let request_id = RequestId {
        client_id: 0x52,
        sequence: 1,
        raft_group_id: RaftGroupId(2),
    };
    let request = create_request("accounts");

    let first = state.apply_with_request_id(
        request_id.clone(),
        MetadataCommand::CreateTableTopology(request.clone()),
    );
    let allocator = state.allocator_state();

    let restored = MetadataState::from_snapshot(state.to_snapshot()).unwrap();
    let replay = restored
        .clone()
        .apply_with_request_id(request_id, MetadataCommand::CreateTableTopology(request));

    assert_eq!(replay, first);
    assert_eq!(restored.allocator_state(), allocator);
}

#[test]
fn sequential_atomic_creates_receive_unique_monotonic_ids() {
    let mut state = initialized_state_with_nodes();

    let first = state.apply(MetadataCommand::CreateTableTopology(create_request(
        "alpha",
    )));
    let second = state.apply(MetadataCommand::CreateTableTopology(create_request("beta")));

    assert_eq!(
        first,
        MetadataApplyOutcome::TableCreated(MetadataTableCreated {
            table_id: TableId(2),
            tablet_id: TabletId(2),
            raft_group_id: RaftGroupId(3),
        }),
    );

    assert_eq!(
        second,
        MetadataApplyOutcome::TableCreated(MetadataTableCreated {
            table_id: TableId(3),
            tablet_id: TabletId(3),
            raft_group_id: RaftGroupId(4),
        }),
    );
}

#[test]
fn metadata_snapshot_preserves_identity_high_water_marks() {
    let mut state = initialized_state_with_nodes();

    assert!(matches!(
        state.apply(MetadataCommand::CreateTableTopology(create_request(
            "alpha"
        ))),
        MetadataApplyOutcome::TableCreated(_)
    ));
    assert!(matches!(
        state.apply(MetadataCommand::CreateTableTopology(create_request("beta"))),
        MetadataApplyOutcome::TableCreated(_)
    ));

    let before = state.allocator_state();
    let encoded = state.to_snapshot().encode().unwrap();
    let decoded = ragnordb_common::metadata_codec::MetadataSnapshot::decode(&encoded).unwrap();
    let mut recovered = MetadataState::from_snapshot(decoded).unwrap();

    assert_eq!(recovered.allocator_state(), before);

    assert_eq!(
        recovered.apply(MetadataCommand::CreateTableTopology(create_request(
            "gamma"
        ))),
        MetadataApplyOutcome::TableCreated(MetadataTableCreated {
            table_id: TableId(4),
            tablet_id: TabletId(4),
            raft_group_id: RaftGroupId(5),
        }),
    );
}

#[test]
fn create_table_without_registered_nodes_is_atomic_rejection() {
    let mut state = MetadataState::new();

    assert_eq!(
        state.apply(MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        }),
        MetadataApplyOutcome::Applied,
    );

    let before = state.allocator_state();

    assert_eq!(
        state.apply(MetadataCommand::CreateTableTopology(create_request(
            "orphan"
        ))),
        MetadataApplyOutcome::Rejected(MetadataRejection::NoRegisteredNodes),
    );

    assert!(state.table(TableId(2)).is_none());
    assert_eq!(state.allocator_state(), before);
}

#[test]
fn legacy_creation_commands_advance_identity_high_water_marks() {
    let state = bootstrap_state();

    assert_eq!(
        state.allocator_state(),
        ragnordb_common::metadata_codec::MetadataAllocatorState {
            max_table_id: 7,
            max_tablet_id: 17,
            max_raft_group_id: 23,
            max_replica_id: 0,
        },
    );
}

#[test]
fn committed_metadata_replay_preserves_tablet_partition_and_desired_placement() {
    let mut state = bootstrap_state();

    let placement = DesiredReplicaPlacement {
        tablet_id: TabletId(17),

        configuration_epoch: 1,
        placement_policy: PlacementPolicy::for_replica_count(1),

        replicas: vec![
            DesiredReplica {
                replica_id: ReplicaId(31),
                node_id: NodeId(11),
                role: DesiredReplicaRole::Voter,
            },
            DesiredReplica {
                replica_id: ReplicaId(32),
                node_id: NodeId(12),
                role: DesiredReplicaRole::Learner,
            },
        ],
    };

    assert_eq!(
        state.apply(MetadataCommand::SetDesiredReplicaPlacement(
            placement.clone(),
        ),),
        MetadataApplyOutcome::Applied,
    );

    assert_eq!(state.cluster_id(), Some("cluster-a"));

    assert_eq!(
        state.tablet(TabletId(17)).unwrap().partition,
        PartitionSpec::Hash {
            bucket: 0,
            bucket_count: 2,
        }
    );

    assert_eq!(state.desired_placement(TabletId(17)), Some(&placement));
}

#[test]
fn stale_metadata_command_is_a_rejection_not_a_state_machine_failure() {
    let mut state = bootstrap_state();

    let epoch_one = DesiredReplicaPlacement {
        tablet_id: TabletId(17),

        configuration_epoch: 1,
        placement_policy: PlacementPolicy::for_replica_count(1),

        replicas: vec![DesiredReplica {
            replica_id: ReplicaId(31),
            node_id: NodeId(11),
            role: DesiredReplicaRole::Voter,
        }],
    };

    assert_eq!(
        state.apply(MetadataCommand::SetDesiredReplicaPlacement(epoch_one,),),
        MetadataApplyOutcome::Applied,
    );

    let stale = DesiredReplicaPlacement {
        tablet_id: TabletId(17),

        configuration_epoch: 3,
        placement_policy: PlacementPolicy::for_replica_count(1),

        replicas: vec![DesiredReplica {
            replica_id: ReplicaId(31),
            node_id: NodeId(11),
            role: DesiredReplicaRole::Voter,
        }],
    };

    assert!(matches!(
        state.apply(MetadataCommand::SetDesiredReplicaPlacement(stale,),),
        MetadataApplyOutcome::Rejected(MetadataRejection::PlacementEpochMismatch {
            expected: 2,
            received: 3,
            ..
        })
    ));
}

#[test]
fn desired_placement_can_revert_before_replica_removal_is_committed() {
    let mut state = bootstrap_state();

    let initial = DesiredReplicaPlacement {
        tablet_id: TabletId(17),
        configuration_epoch: 1,
        placement_policy: PlacementPolicy::for_replica_count(1),
        replicas: vec![DesiredReplica {
            replica_id: ReplicaId(31),
            node_id: NodeId(11),
            role: DesiredReplicaRole::Voter,
        }],
    };

    assert_eq!(
        state.apply(MetadataCommand::SetDesiredReplicaPlacement(initial)),
        MetadataApplyOutcome::Applied,
    );

    // Metadata expresses the intention to replace replica 31 with 32.
    //
    // This does not prove that group 23 has committed RemoveReplica(31).
    let replacement = DesiredReplicaPlacement {
        tablet_id: TabletId(17),
        configuration_epoch: 2,
        placement_policy: PlacementPolicy::for_replica_count(1),
        replicas: vec![DesiredReplica {
            replica_id: ReplicaId(32),
            node_id: NodeId(12),
            role: DesiredReplicaRole::Voter,
        }],
    };

    assert_eq!(
        state.apply(MetadataCommand::SetDesiredReplicaPlacement(replacement)),
        MetadataApplyOutcome::Applied,
    );

    assert!(
        !state.is_replica_retired(RaftGroupId(23), ReplicaId(31),),
        "desired placement changes must not retire a replica before \
         committed Raft membership removal"
    );

    // Reconciliation may fail before any ConfChange commits. Metadata must
    // therefore be able to change its desired topology again while replica 31
    // remains a legitimate committed member.
    let reverted = DesiredReplicaPlacement {
        tablet_id: TabletId(17),
        configuration_epoch: 3,
        placement_policy: PlacementPolicy::for_replica_count(2),
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
    };

    assert_eq!(
        state.apply(MetadataCommand::SetDesiredReplicaPlacement(reverted)),
        MetadataApplyOutcome::Applied,
    );
}

#[test]
fn replica_retirement_requires_the_second_proof_and_is_idempotent() {
    let mut state = bootstrap_state();

    state.apply(MetadataCommand::SetDesiredReplicaPlacement(
        DesiredReplicaPlacement {
            tablet_id: TabletId(17),
            configuration_epoch: 1,
            placement_policy: PlacementPolicy::for_replica_count(1),
            replicas: vec![DesiredReplica {
                replica_id: ReplicaId(31),
                node_id: NodeId(11),
                role: DesiredReplicaRole::Voter,
            }],
        },
    ));

    let replacement = DesiredReplicaPlacement {
        tablet_id: TabletId(17),
        configuration_epoch: 2,
        placement_policy: PlacementPolicy::for_replica_count(1),
        replicas: vec![DesiredReplica {
            replica_id: ReplicaId(32),
            node_id: NodeId(12),
            role: DesiredReplicaRole::Voter,
        }],
    };
    assert_eq!(
        state.apply(MetadataCommand::SetDesiredReplicaPlacement(replacement)),
        MetadataApplyOutcome::Applied
    );

    let retirement = MetadataCommand::RecordReplicaRetirement {
        raft_group_id: RaftGroupId(23),
        replica_id: ReplicaId(31),
        desired_configuration_epoch: 2,
        removed_conf_state_version: 3,
        removal_index: 41,
        removal_term: 7,
    };

    assert_eq!(
        state.apply(retirement.clone()),
        MetadataApplyOutcome::Applied
    );
    assert_eq!(
        state.apply(retirement),
        MetadataApplyOutcome::AlreadyApplied
    );
    assert!(state.is_replica_retired(RaftGroupId(23), ReplicaId(31)));
    assert_eq!(
        state
            .retired_replica(RaftGroupId(23), ReplicaId(31))
            .unwrap()
            .removal_index,
        41
    );

    let conflict = MetadataCommand::RecordReplicaRetirement {
        raft_group_id: RaftGroupId(23),
        replica_id: ReplicaId(31),
        desired_configuration_epoch: 2,
        removed_conf_state_version: 4,
        removal_index: 42,
        removal_term: 8,
    };
    assert!(matches!(
        state.apply(conflict),
        MetadataApplyOutcome::Rejected(MetadataRejection::RetirementProofConflict { .. })
    ));
}

#[test]
fn draining_node_cannot_be_retained_as_a_preferred_leader() {
    let mut state = bootstrap_state();
    let mut draining = node(11, 7001);
    draining.lifecycle = NodeLifecycle::Draining;
    assert_eq!(
        state.apply(MetadataCommand::RegisterNode(draining)),
        MetadataApplyOutcome::Applied,
    );

    let placement = DesiredReplicaPlacement {
        tablet_id: TabletId(17),
        configuration_epoch: 1,
        placement_policy: PlacementPolicy {
            preferred_leader_nodes: vec![NodeId(11)],
            ..PlacementPolicy::for_replica_count(1)
        },
        replicas: vec![DesiredReplica {
            replica_id: ReplicaId(31),
            node_id: NodeId(12),
            role: DesiredReplicaRole::Voter,
        }],
    };

    assert!(matches!(
        state.apply(MetadataCommand::SetDesiredReplicaPlacement(placement)),
        MetadataApplyOutcome::Rejected(MetadataRejection::NodeNotEligible {
            node_id: NodeId(11),
            ..
        })
    ));
}

#[test]
fn replacement_placement_can_retain_a_draining_source_until_membership_removal() {
    let mut state = bootstrap_state();
    assert_eq!(
        state.apply(MetadataCommand::SetDesiredReplicaPlacement(
            DesiredReplicaPlacement {
                tablet_id: TabletId(17),
                configuration_epoch: 1,
                placement_policy: PlacementPolicy::for_replica_count(1),
                replicas: vec![DesiredReplica {
                    replica_id: ReplicaId(31),
                    node_id: NodeId(11),
                    role: DesiredReplicaRole::Voter,
                }],
            },
        )),
        MetadataApplyOutcome::Applied,
    );
    assert_eq!(
        state.apply(MetadataCommand::SetNodeLifecycle {
            node_id: NodeId(11),
            lifecycle: NodeLifecycle::Draining,
        }),
        MetadataApplyOutcome::Applied,
    );

    // Realistic bug caught: rejecting the source from this transitional
    // placement would force metadata to remove a voter before its replacement
    // has joined the Raft group, reducing quorum during an ordinary drain.
    let transitional = DesiredReplicaPlacement {
        tablet_id: TabletId(17),
        configuration_epoch: 2,
        placement_policy: PlacementPolicy::for_replica_count(2),
        replicas: vec![
            DesiredReplica {
                replica_id: ReplicaId(31),
                node_id: NodeId(11),
                role: DesiredReplicaRole::Voter,
            },
            DesiredReplica {
                replica_id: ReplicaId(33),
                node_id: NodeId(12),
                role: DesiredReplicaRole::Voter,
            },
        ],
    };

    assert_eq!(
        state.apply(MetadataCommand::SetDesiredReplicaPlacement(
            transitional.clone()
        )),
        MetadataApplyOutcome::Applied,
    );
    assert_eq!(state.desired_placement(TabletId(17)), Some(&transitional));
}

#[test]
fn replica_allocator_does_not_reuse_ids_across_drain_epochs() {
    let mut state = bootstrap_state();
    let source = DesiredReplica {
        replica_id: ReplicaId(31),
        node_id: NodeId(11),
        role: DesiredReplicaRole::Voter,
    };
    assert_eq!(
        state.apply(MetadataCommand::SetDesiredReplicaPlacement(
            DesiredReplicaPlacement {
                tablet_id: TabletId(17),
                configuration_epoch: 1,
                placement_policy: PlacementPolicy::for_replica_count(1),
                replicas: vec![source.clone()],
            },
        )),
        MetadataApplyOutcome::Applied
    );
    assert_eq!(state.next_replica_id(), Some(ReplicaId(32)));

    assert_eq!(
        state.apply(MetadataCommand::SetNodeLifecycle {
            node_id: NodeId(11),
            lifecycle: NodeLifecycle::Draining,
        }),
        MetadataApplyOutcome::Applied
    );
    assert_eq!(
        state.apply(MetadataCommand::SetDesiredReplicaPlacement(
            DesiredReplicaPlacement {
                tablet_id: TabletId(17),
                configuration_epoch: 2,
                placement_policy: PlacementPolicy::for_replica_count(2),
                replicas: vec![
                    source,
                    DesiredReplica {
                        replica_id: ReplicaId(32),
                        node_id: NodeId(12),
                        role: DesiredReplicaRole::Voter,
                    },
                ],
            },
        )),
        MetadataApplyOutcome::Applied
    );
    assert_eq!(state.next_replica_id(), Some(ReplicaId(33)));

    assert_eq!(
        state.apply(MetadataCommand::SetDesiredReplicaPlacement(
            DesiredReplicaPlacement {
                tablet_id: TabletId(17),
                configuration_epoch: 3,
                placement_policy: PlacementPolicy::for_replica_count(1),
                replicas: vec![DesiredReplica {
                    replica_id: ReplicaId(32),
                    node_id: NodeId(12),
                    role: DesiredReplicaRole::Voter,
                }],
            },
        )),
        MetadataApplyOutcome::Applied
    );
    assert_eq!(
        state.next_replica_id(),
        Some(ReplicaId(33)),
        "a removed source identity must remain above the allocator floor"
    );
}

#[test]
fn schema_update_is_additive_and_preserves_existing_row_contract() {
    let mut state = bootstrap_state();

    let version_two = TableDefinition {
        columns: vec![
            ColumnDefinition {
                column_id: ColumnId(1),
                name: "id".to_string(),
                ty: DataType::Int,
                nullable: false,
            },
            ColumnDefinition {
                column_id: ColumnId(2),
                name: "note".to_string(),
                ty: DataType::Text,
                nullable: true,
            },
        ],

        schema_version: 2,

        ..table()
    };

    assert_eq!(
        state.apply(MetadataCommand::UpdateTableSchema {
            expected_schema_version: 1,
            table: version_two.clone(),
        },),
        MetadataApplyOutcome::Applied,
    );

    // Exact replay remains idempotent even though expected version 1 is stale
    // after the first application.
    assert_eq!(
        state.apply(MetadataCommand::UpdateTableSchema {
            expected_schema_version: 1,
            table: version_two,
        },),
        MetadataApplyOutcome::AlreadyApplied,
    );

    assert_eq!(state.table(TableId(7)).unwrap().schema_version, 2);
}

#[test]
fn metadata_snapshot_roundtrip_preserves_replica_tombstones() {
    let mut state = bootstrap_state();

    state.apply(MetadataCommand::SetDesiredReplicaPlacement(
        DesiredReplicaPlacement {
            tablet_id: TabletId(17),

            configuration_epoch: 1,
            placement_policy: PlacementPolicy::for_replica_count(1),

            replicas: vec![
                DesiredReplica {
                    replica_id: ReplicaId(31),
                    node_id: NodeId(11),
                    role: DesiredReplicaRole::Voter,
                },
                DesiredReplica {
                    replica_id: ReplicaId(32),
                    node_id: NodeId(12),
                    role: DesiredReplicaRole::Learner,
                },
            ],
        },
    ));

    state.apply(MetadataCommand::SetDesiredReplicaPlacement(
        DesiredReplicaPlacement {
            tablet_id: TabletId(17),

            configuration_epoch: 2,
            placement_policy: PlacementPolicy::for_replica_count(1),

            replicas: vec![DesiredReplica {
                replica_id: ReplicaId(32),
                node_id: NodeId(12),
                role: DesiredReplicaRole::Voter,
            }],
        },
    ));

    let encoded = state.to_snapshot().encode().unwrap();

    let decoded = ragnordb_common::metadata_codec::MetadataSnapshot::decode(&encoded).unwrap();

    let recovered = MetadataState::from_snapshot(decoded).unwrap();

    assert_eq!(recovered.cluster_id(), Some("cluster-a"));

    assert!(
        !recovered.is_replica_retired(RaftGroupId(23), ReplicaId(31),),
        "Phase 5.1 must not tombstone on desired placement alone"
    );

    assert_eq!(
        recovered
            .desired_placement(TabletId(17))
            .unwrap()
            .configuration_epoch,
        2
    );
}

#[test]
fn timestamp_reservation_is_monotonic_deduplicated_and_snapshot_durable() {
    let mut state = MetadataState::new();

    assert_eq!(
        state.apply(MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        }),
        MetadataApplyOutcome::Applied,
    );

    let request_id = RequestId {
        client_id: 91,
        sequence: 1,
        raft_group_id: RaftGroupId(2),
    };
    let command = MetadataCommand::ReserveTimestamps {
        reserved_until: Timestamp(100),
    };

    assert_eq!(
        state.apply_with_request_id(request_id, command.clone()),
        MetadataApplyOutcome::TimestampsReserved {
            reserved_from: Timestamp(1),
            reserved_until: Timestamp(100),
        },
    );
    assert_eq!(state.timestamp_reserved_until(), Timestamp(100));

    assert_eq!(
        state.apply(MetadataCommand::ReserveTimestamps {
            reserved_until: Timestamp(99),
        }),
        MetadataApplyOutcome::Rejected(MetadataRejection::TimestampReservationRegressed {
            current: Timestamp(100),
            received: Timestamp(99),
        }),
    );
    assert_eq!(state.timestamp_reserved_until(), Timestamp(100));

    let replay_request_id = RequestId {
        client_id: 91,
        sequence: 1,
        raft_group_id: RaftGroupId(2),
    };
    assert_eq!(
        state.apply_with_request_id(
            replay_request_id,
            MetadataCommand::ReserveTimestamps {
                reserved_until: Timestamp(200),
            },
        ),
        MetadataApplyOutcome::TimestampsReserved {
            reserved_from: Timestamp(1),
            reserved_until: Timestamp(100),
        },
    );
    assert_eq!(state.timestamp_reserved_until(), Timestamp(100));

    let recovered = MetadataState::from_snapshot(state.to_snapshot()).unwrap();
    assert_eq!(recovered.timestamp_reserved_until(), Timestamp(100));
}
