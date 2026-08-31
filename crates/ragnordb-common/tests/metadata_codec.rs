use prost::Message;
use ragnordb_common::{
    catalog_codec::{ColumnDefinition, DataType, TableDefinition},
    ids::{ColumnId, NodeId, RaftGroupId, ReplicaId, TableId, TabletId},
    metadata_codec::{
        CreateTableRequest, DesiredReplica, DesiredReplicaPlacement, DesiredReplicaRole,
        LEGACY_METADATA_SNAPSHOT_VERSION, METADATA_SNAPSHOT_VERSION, MetadataAllocatorState,
        MetadataCommand, MetadataCommandCodecError, MetadataSnapshot, NodeDescriptor,
        PartitionSpec, RetiredReplicaLifetime, TabletDescriptor,
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

fn node(node_id: u64, base_port: u16) -> NodeDescriptor {
    NodeDescriptor {
        node_id: NodeId(node_id),

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

fn placement() -> DesiredReplicaPlacement {
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
    }
}

#[test]
fn metadata_v2_commands_roundtrip_every_authoritative_field() {
    let commands = vec![
        MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        },
        MetadataCommand::RegisterNode(node(11, 7001)),
        MetadataCommand::CreateTable { table: table() },
        MetadataCommand::CreateTablet { tablet: tablet() },
        MetadataCommand::CreateTableTopology(CreateTableRequest {
            table_name: "new_table".to_string(),
            columns: vec![ColumnDefinition {
                column_id: ColumnId(1),
                name: "id".to_string(),
                ty: DataType::Int,
                nullable: false,
            }],
            primary_key_column_ids: vec![ColumnId(1)],
        }),
        MetadataCommand::SetDesiredReplicaPlacement(placement()),
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

#[test]
fn old_experimental_metadata_command_version_is_rejected() {
    let command = MetadataCommand::ClusterInitialized {
        cluster_id: "cluster-a".to_string(),
    };

    let mut proto = command.to_proto();

    proto.format_version = 1;

    assert_eq!(
        MetadataCommand::from_proto(proto).unwrap_err(),
        MetadataCommandCodecError::UnsupportedVersion(1)
    );
}

#[test]
fn desired_placement_requires_canonical_replica_order_and_a_voter() {
    let noncanonical = MetadataCommand::SetDesiredReplicaPlacement(DesiredReplicaPlacement {
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
        noncanonical.encode().unwrap_err(),
        MetadataCommandCodecError::ReplicaPlacementNotCanonical
    );

    let no_voter = MetadataCommand::SetDesiredReplicaPlacement(DesiredReplicaPlacement {
        tablet_id: TabletId(17),

        configuration_epoch: 1,

        replicas: vec![DesiredReplica {
            replica_id: ReplicaId(31),
            node_id: NodeId(11),
            role: DesiredReplicaRole::Learner,
        }],
    });

    assert_eq!(
        no_voter.encode().unwrap_err(),
        MetadataCommandCodecError::PlacementHasNoVoter
    );
}

#[test]
fn invalid_hash_partition_is_rejected_before_proposal() {
    let command = MetadataCommand::CreateTablet {
        tablet: TabletDescriptor {
            partition: PartitionSpec::Hash {
                bucket: 2,
                bucket_count: 2,
            },

            ..tablet()
        },
    };

    assert_eq!(
        command.encode().unwrap_err(),
        MetadataCommandCodecError::InvalidHashBucket {
            bucket: 2,
            bucket_count: 2,
        }
    );
}

#[test]
fn tablet_cannot_claim_metadata_raft_group() {
    let command = MetadataCommand::CreateTablet {
        tablet: TabletDescriptor {
            raft_group_id: RaftGroupId(2),
            ..tablet()
        },
    };

    assert_eq!(
        command.encode().unwrap_err(),
        MetadataCommandCodecError::MetadataRaftGroupAssignedToTablet(RaftGroupId(2)),
    );
}

#[test]
fn metadata_snapshot_roundtrips_retired_replica_lifetimes() {
    let snapshot = MetadataSnapshot {
        cluster_id: Some("cluster-a".to_string()),

        nodes: vec![node(11, 7001), node(12, 7002)],

        tables: vec![table()],

        tablets: vec![tablet()],

        desired_placements: vec![placement()],

        retired_replicas: vec![RetiredReplicaLifetime {
            raft_group_id: RaftGroupId(23),
            replica_id: ReplicaId(30),
        }],

        allocator: MetadataAllocatorState {
            max_table_id: 7,
            max_tablet_id: 17,
            max_raft_group_id: 23,
        },
    };

    let encoded = snapshot.encode().unwrap();

    assert_eq!(MetadataSnapshot::decode(&encoded).unwrap(), snapshot);
}

#[test]
fn metadata_snapshot_rejects_noncanonical_node_order() {
    let snapshot = MetadataSnapshot {
        cluster_id: Some("cluster-a".to_string()),

        nodes: vec![node(12, 7002), node(11, 7001)],

        tables: Vec::new(),
        tablets: Vec::new(),
        desired_placements: Vec::new(),
        retired_replicas: Vec::new(),

        allocator: MetadataAllocatorState::initial(),
    };

    assert_eq!(
        snapshot.encode().unwrap_err(),
        MetadataCommandCodecError::NonCanonicalSnapshot("nodes")
    );
}

#[test]
fn phase_5_1_snapshot_without_allocator_derives_safe_high_water_marks() {
    let legacy_tablet = TabletDescriptor {
        raft_group_id: RaftGroupId(1),
        ..tablet()
    };

    let snapshot = MetadataSnapshot {
        cluster_id: Some("cluster-a".to_string()),
        nodes: vec![node(11, 7001), node(12, 7002)],
        tables: vec![table()],
        tablets: vec![legacy_tablet],
        desired_placements: vec![placement()],
        retired_replicas: Vec::new(),
        allocator: MetadataAllocatorState {
            max_table_id: 100,
            max_tablet_id: 200,
            max_raft_group_id: 300,
        },
    };

    let mut proto = ragnordb_common::proto::metadata::MetadataSnapshot::decode(
        snapshot.encode().unwrap().as_slice(),
    )
    .unwrap();
    proto.format_version = LEGACY_METADATA_SNAPSHOT_VERSION;
    proto.allocator_state = None;

    let decoded = MetadataSnapshot::decode(&proto.encode_to_vec()).unwrap();

    assert_eq!(
        decoded.allocator,
        MetadataAllocatorState {
            max_table_id: 7,
            max_tablet_id: 17,
            max_raft_group_id: 2,
        },
    );
}

#[test]
fn current_metadata_snapshot_requires_allocator_state() {
    let snapshot = MetadataSnapshot {
        cluster_id: Some("cluster-a".to_string()),
        nodes: vec![node(11, 7001)],
        tables: Vec::new(),
        tablets: Vec::new(),
        desired_placements: Vec::new(),
        retired_replicas: Vec::new(),
        allocator: MetadataAllocatorState::initial(),
    };

    let mut proto = ragnordb_common::proto::metadata::MetadataSnapshot::decode(
        snapshot.encode().unwrap().as_slice(),
    )
    .unwrap();

    assert_eq!(proto.format_version, METADATA_SNAPSHOT_VERSION);
    proto.allocator_state = None;

    assert_eq!(
        MetadataSnapshot::decode(&proto.encode_to_vec()).unwrap_err(),
        MetadataCommandCodecError::MissingField("snapshot.allocator_state"),
    );
}

#[test]
fn transitional_v1_snapshot_with_allocator_state_remains_readable() {
    let snapshot = MetadataSnapshot {
        cluster_id: Some("cluster-a".to_string()),
        nodes: vec![node(11, 7001)],
        tables: Vec::new(),
        tablets: Vec::new(),
        desired_placements: Vec::new(),
        retired_replicas: Vec::new(),
        allocator: MetadataAllocatorState {
            max_table_id: 10,
            max_tablet_id: 20,
            max_raft_group_id: 30,
        },
    };

    let mut proto = ragnordb_common::proto::metadata::MetadataSnapshot::decode(
        snapshot.encode().unwrap().as_slice(),
    )
    .unwrap();
    proto.format_version = LEGACY_METADATA_SNAPSHOT_VERSION;

    assert_eq!(
        MetadataSnapshot::decode(&proto.encode_to_vec()).unwrap(),
        snapshot
    );
}
