use ragnordb_common::{
    catalog_codec::{ColumnDefinition, DataType, TableDefinition},
    ids::{ColumnId, NodeId, RaftGroupId, ReplicaId, TableId, TabletId},
    metadata_codec::{
        DesiredReplica, DesiredReplicaPlacement, DesiredReplicaRole, MetadataCommand,
        MetadataCommandCodecError, NodeDescriptor, TabletDescriptor,
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

/// Realistic bug caught:
///
/// If the bytes proposed to the metadata Raft group do not retain placement
/// role, epoch, and tablet identity exactly, a restart can reconcile a
/// different topology than the one that was committed.
#[test]
fn metadata_commands_roundtrip_all_committed_topology_fields() {
    let commands = vec![
        MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        },
        MetadataCommand::RegisterNode(NodeDescriptor {
            node_id: NodeId(11),
            endpoint: "127.0.0.1:7101".to_string(),
        }),
        MetadataCommand::CreateTable { table: table() },
        MetadataCommand::CreateTablet {
            tablet: TabletDescriptor {
                tablet_id: TabletId(17),
                table_id: TableId(7),
                raft_group_id: RaftGroupId(23),
                tablet_epoch: 1,
                schema_version: 1,
            },
        },
        MetadataCommand::SetDesiredReplicaPlacement(DesiredReplicaPlacement {
            tablet_id: TabletId(17),
            configuration_epoch: 2,
            replicas: vec![DesiredReplica {
                replica_id: ReplicaId(31),
                node_id: NodeId(11),
                role: DesiredReplicaRole::Voter,
            }],
        }),
        MetadataCommand::UpdateTableSchema {
            expected_schema_version: 1,
            table: TableDefinition {
                schema_version: 2,
                ..table()
            },
        },
    ];

    for command in commands {
        let encoded = command.encode().unwrap();
        assert_eq!(MetadataCommand::decode(&encoded).unwrap(), command);
    }
}

/// Realistic bug caught:
///
/// Protobuf preserves repeated-field order. Accepting a non-canonical replica
/// list would allow the same desired placement to acquire multiple durable
/// byte representations and could make replay or hashing disagree by node.
#[test]
fn noncanonical_replica_order_is_rejected_before_proposal() {
    let command = MetadataCommand::SetDesiredReplicaPlacement(DesiredReplicaPlacement {
        tablet_id: TabletId(17),
        configuration_epoch: 1,
        replicas: vec![
            DesiredReplica {
                replica_id: ReplicaId(32),
                node_id: NodeId(12),
                role: DesiredReplicaRole::Voter,
            },
            DesiredReplica {
                replica_id: ReplicaId(31),
                node_id: NodeId(11),
                role: DesiredReplicaRole::Learner,
            },
        ],
    });

    assert_eq!(
        command.encode().unwrap_err(),
        MetadataCommandCodecError::ReplicaPlacementNotCanonical
    );
}

/// Realistic bug caught:
///
/// A node upgraded with new metadata-command semantics could otherwise replay
/// an unsupported committed record as though it were the current format,
/// producing an incompatible metadata projection after recovery.
#[test]
fn unsupported_metadata_command_version_is_rejected() {
    let command = MetadataCommand::ClusterInitialized {
        cluster_id: "cluster-a".to_string(),
    };
    let mut proto = command.to_proto();
    proto.format_version = 2;

    assert_eq!(
        MetadataCommand::from_proto(proto).unwrap_err(),
        MetadataCommandCodecError::UnsupportedVersion(2)
    );
}
