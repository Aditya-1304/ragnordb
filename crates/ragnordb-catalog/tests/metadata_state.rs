use ragnordb_catalog::{
    MetadataApplyOutcome, MetadataRejection, MetadataState, MetadataTableCreated,
};

use ragnordb_common::{
    catalog_codec::{ColumnDefinition, DataType, TableDefinition},
    ids::{ColumnId, NodeId, RaftGroupId, ReplicaId, TableId, TabletId},
    metadata_codec::{
        CreateTableRequest, DesiredReplica, DesiredReplicaPlacement, DesiredReplicaRole,
        MetadataCommand, NodeDescriptor, PartitionSpec, TabletDescriptor,
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
        PartitionSpec::Hash {
            bucket: 0,
            bucket_count: 1,
        }
    );

    let placement = state.desired_placement(TabletId(2)).unwrap();
    assert_eq!(placement.configuration_epoch, 1);
    assert_eq!(placement.replicas.len(), 3);
    assert_eq!(placement.replicas[0].node_id, NodeId(11));
    assert_eq!(placement.replicas[1].node_id, NodeId(12));
    assert_eq!(placement.replicas[2].node_id, NodeId(13));

    assert_eq!(state.allocator_state().max_table_id, 2);
    assert_eq!(state.allocator_state().max_tablet_id, 2);
    assert_eq!(state.allocator_state().max_raft_group_id, 3);
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
        },
    );
}

#[test]
fn committed_metadata_replay_preserves_tablet_partition_and_desired_placement() {
    let mut state = bootstrap_state();

    let placement = DesiredReplicaPlacement {
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
