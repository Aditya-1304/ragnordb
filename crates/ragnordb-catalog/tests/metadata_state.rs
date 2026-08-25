use ragnordb_catalog::{MetadataApplyOutcome, MetadataState};
use ragnordb_common::{
    catalog_codec::{ColumnDefinition, DataType, TableDefinition},
    ids::{ColumnId, NodeId, RaftGroupId, ReplicaId, TableId, TabletId},
    metadata_codec::{
        DesiredReplica, DesiredReplicaPlacement, DesiredReplicaRole, MetadataCommand,
        NodeDescriptor, TabletDescriptor,
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
        tablet_count: 1,
    }
}

/// A metadata log replay could publish table and tablet records without their
/// desired placement, or collapse desired placement into current Raft
/// membership. After a crash, a reconciler would then either have no work or
/// could treat an uncommitted replica as a voter.
#[test]
fn committed_metadata_replay_preserves_desired_placement_separately_from_tablet_identity() {
    let mut state = MetadataState::new();

    assert_eq!(
        state
            .apply(MetadataCommand::ClusterInitialized {
                cluster_id: "cluster-a".to_string(),
            })
            .expect("cluster initialization must apply"),
        MetadataApplyOutcome::Applied
    );
    assert_eq!(
        state
            .apply(MetadataCommand::RegisterNode(NodeDescriptor {
                node_id: NodeId(11),
                endpoint: "127.0.0.1:7101".to_string(),
            }))
            .expect("node registration must apply"),
        MetadataApplyOutcome::Applied
    );
    assert_eq!(
        state
            .apply(MetadataCommand::CreateTable { table: table() })
            .expect("table creation must apply"),
        MetadataApplyOutcome::Applied
    );
    assert_eq!(
        state
            .apply(MetadataCommand::CreateTablet {
                tablet: TabletDescriptor {
                    tablet_id: TabletId(17),
                    table_id: TableId(7),
                    raft_group_id: RaftGroupId(23),
                    tablet_epoch: 1,
                    schema_version: 1,
                },
            })
            .expect("tablet creation must apply"),
        MetadataApplyOutcome::Applied
    );

    let placement = DesiredReplicaPlacement {
        tablet_id: TabletId(17),
        configuration_epoch: 1,
        replicas: vec![DesiredReplica {
            replica_id: ReplicaId(31),
            node_id: NodeId(11),
            role: DesiredReplicaRole::Voter,
        }],
    };
    assert_eq!(
        state
            .apply(MetadataCommand::SetDesiredReplicaPlacement(
                placement.clone()
            ))
            .expect("desired placement must apply"),
        MetadataApplyOutcome::Applied
    );

    assert_eq!(state.cluster_id(), Some("cluster-a"));
    assert_eq!(
        state.table(TableId(7)).expect("table must exist").name,
        "accounts"
    );
    assert_eq!(
        state
            .tablet(TabletId(17))
            .expect("tablet must exist")
            .raft_group_id,
        RaftGroupId(23)
    );
    assert_eq!(state.desired_placement(TabletId(17)), Some(&placement));
}

/// A recovered metadata log can contain the command that was already applied
/// before a crash. Treating that exact replay as a conflict loses recovery
/// availability, while accepting a changed command at the same epoch can make
/// metadata claim a topology the Raft group never committed.
#[test]
fn exact_replay_is_idempotent_but_conflicting_cluster_and_stale_placement_are_rejected() {
    let mut state = MetadataState::new();
    let initialized = MetadataCommand::ClusterInitialized {
        cluster_id: "cluster-a".to_string(),
    };

    assert_eq!(
        state.apply(initialized.clone()).unwrap(),
        MetadataApplyOutcome::Applied
    );
    assert_eq!(
        state.apply(initialized).unwrap(),
        MetadataApplyOutcome::AlreadyApplied
    );
    assert!(
        state
            .apply(MetadataCommand::ClusterInitialized {
                cluster_id: "cluster-b".to_string(),
            })
            .is_err()
    );

    state
        .apply(MetadataCommand::RegisterNode(NodeDescriptor {
            node_id: NodeId(11),
            endpoint: "127.0.0.1:7101".to_string(),
        }))
        .unwrap();
    state
        .apply(MetadataCommand::CreateTable { table: table() })
        .unwrap();
    state
        .apply(MetadataCommand::CreateTablet {
            tablet: TabletDescriptor {
                tablet_id: TabletId(17),
                table_id: TableId(7),
                raft_group_id: RaftGroupId(23),
                tablet_epoch: 1,
                schema_version: 1,
            },
        })
        .unwrap();

    let epoch_two = DesiredReplicaPlacement {
        tablet_id: TabletId(17),
        configuration_epoch: 2,
        replicas: vec![DesiredReplica {
            replica_id: ReplicaId(31),
            node_id: NodeId(11),
            role: DesiredReplicaRole::Voter,
        }],
    };
    state
        .apply(MetadataCommand::SetDesiredReplicaPlacement(
            epoch_two.clone(),
        ))
        .unwrap();
    assert_eq!(
        state
            .apply(MetadataCommand::SetDesiredReplicaPlacement(epoch_two))
            .unwrap(),
        MetadataApplyOutcome::AlreadyApplied
    );
    assert!(
        state
            .apply(MetadataCommand::SetDesiredReplicaPlacement(
                DesiredReplicaPlacement {
                    tablet_id: TabletId(17),
                    configuration_epoch: 1,
                    replicas: vec![DesiredReplica {
                        replica_id: ReplicaId(31),
                        node_id: NodeId(11),
                        role: DesiredReplicaRole::Voter,
                    }],
                }
            ))
            .is_err()
    );
}

/// A delayed schema command from an old coordinator could otherwise overwrite
/// a newer committed definition. That would make nodes that replayed the same
/// Raft log disagree about the schema used to decode tablet rows.
#[test]
fn schema_update_advances_exactly_one_version_and_rejects_a_stale_precondition() {
    let mut state = MetadataState::new();
    state
        .apply(MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        })
        .unwrap();
    state
        .apply(MetadataCommand::CreateTable { table: table() })
        .unwrap();

    let version_two = TableDefinition {
        schema_version: 2,
        ..table()
    };
    let update = MetadataCommand::UpdateTableSchema {
        expected_schema_version: 1,
        table: version_two.clone(),
    };
    assert_eq!(
        state.apply(update.clone()).unwrap(),
        MetadataApplyOutcome::Applied
    );
    assert_eq!(
        state.apply(update).unwrap(),
        MetadataApplyOutcome::AlreadyApplied
    );
    assert_eq!(state.table(TableId(7)).unwrap().schema_version, 2);

    assert!(
        state
            .apply(MetadataCommand::UpdateTableSchema {
                expected_schema_version: 1,
                table: TableDefinition {
                    schema_version: 3,
                    ..version_two
                },
            })
            .is_err()
    );
}
