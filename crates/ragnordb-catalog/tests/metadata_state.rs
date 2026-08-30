use ragnordb_catalog::{MetadataApplyOutcome, MetadataRejection, MetadataState};

use ragnordb_common::{
    catalog_codec::{ColumnDefinition, DataType, TableDefinition},
    ids::{ColumnId, NodeId, RaftGroupId, ReplicaId, TableId, TabletId},
    metadata_codec::{
        DesiredReplica, DesiredReplicaPlacement, DesiredReplicaRole, MetadataCommand,
        NodeDescriptor, PartitionSpec, TabletDescriptor,
    },
};

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
