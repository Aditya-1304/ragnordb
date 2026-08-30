//! Durable codec and domain types for metadata Raft commands and snapshots.
//!
//! The metadata log contains desired topology and catalog state only.
//! Transient leader observations and each Raft group's committed `ConfState`
//! have separate authorities and are intentionally absent from this format.

use std::{collections::BTreeSet, net::SocketAddr};

use prost::Message;

use crate::{
    catalog_codec::TableDefinition,
    ids::{NodeId, RaftGroupId, ReplicaId, TabletId},
    proto::metadata,
};

/// First production metadata-command format.
///
/// Version 1 existed only as premature pre-Phase-5 experimental code and is
/// intentionally rejected rather than silently reinterpreted.
pub const METADATA_COMMAND_VERSION: u32 = 2;

/// First metadata state-machine snapshot format.
pub const METADATA_SNAPSHOT_VERSION: u32 = 1;

/// Deterministic transition proposed to the metadata Raft group.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataCommand {
    ClusterInitialized {
        cluster_id: String,
    },

    RegisterNode(NodeDescriptor),

    CreateTable {
        table: TableDefinition,
    },

    CreateTablet {
        tablet: TabletDescriptor,
    },

    SetDesiredReplicaPlacement(DesiredReplicaPlacement),

    UpdateTableSchema {
        expected_schema_version: u64,
        table: TableDefinition,
    },
}

/// Durable physical-node directory entry.
///
/// Node identity is stable. Addresses belong to the physical node, not to a
/// tablet replica or one particular Raft group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDescriptor {
    pub node_id: NodeId,
    pub raft_addr: String,
    pub snapshot_addr: String,
    pub sql_addr: String,
    pub admin_addr: String,
}

/// V1 partition identity.
///
/// Phase 5.3 will consume this metadata for actual routing. Defining it here
/// prevents routing from inventing another, non-replicated tablet map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionSpec {
    Hash { bucket: u32, bucket_count: u32 },
}

/// Stable tablet-to-Raft-group assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletDescriptor {
    pub tablet_id: TabletId,
    pub table_id: crate::ids::TableId,
    pub raft_group_id: RaftGroupId,
    pub tablet_epoch: u64,
    pub partition: PartitionSpec,
}

/// Requested final role after reconciliation completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DesiredReplicaRole {
    Voter,
    Learner,
}

/// One desired consensus identity and its physical host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredReplica {
    pub replica_id: ReplicaId,
    pub node_id: NodeId,
    pub role: DesiredReplicaRole,
}

/// Desired membership for one tablet at one metadata epoch.
///
/// Replicas are strictly ascending by ReplicaId. Canonical ordering matters
/// because the same logical metadata transition must have one durable byte
/// representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredReplicaPlacement {
    pub tablet_id: TabletId,
    pub configuration_epoch: u64,
    pub replicas: Vec<DesiredReplica>,
}

/// Permanent record that one group-local replica lifetime ended.
///
/// The pair is required because ReplicaId is scoped to one Raft group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RetiredReplicaLifetime {
    pub raft_group_id: RaftGroupId,
    pub replica_id: ReplicaId,
}

/// Canonical state-machine snapshot.
///
/// Semantic references such as "tablet references existing table" are checked
/// by `MetadataState::from_snapshot`; this common layer validates wire shape,
/// nested values, and canonical repeated-field ordering.
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataSnapshot {
    pub cluster_id: Option<String>,
    pub nodes: Vec<NodeDescriptor>,
    pub tables: Vec<TableDefinition>,
    pub tablets: Vec<TabletDescriptor>,
    pub desired_placements: Vec<DesiredReplicaPlacement>,
    pub retired_replicas: Vec<RetiredReplicaLifetime>,
}

impl MetadataCommand {
    /// Encode only a structurally valid command.
    pub fn encode(&self) -> Result<Vec<u8>, MetadataCommandCodecError> {
        self.validate()?;
        Ok(self.to_proto().encode_to_vec())
    }

    /// Decode and validate one command read from the Raft log.
    pub fn decode(bytes: &[u8]) -> Result<Self, MetadataCommandCodecError> {
        let proto = metadata::MetadataCommand::decode(bytes)
            .map_err(|error| MetadataCommandCodecError::Decode(error.to_string()))?;

        Self::from_proto(proto)
    }

    pub fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        match self {
            Self::ClusterInitialized { cluster_id } => validate_cluster_id(cluster_id),

            Self::RegisterNode(node) => node.validate(),

            Self::CreateTable { table } => validate_table(table),

            Self::CreateTablet { tablet } => tablet.validate(),

            Self::SetDesiredReplicaPlacement(placement) => placement.validate(),

            Self::UpdateTableSchema {
                expected_schema_version,
                table,
            } => {
                if *expected_schema_version == 0 {
                    return Err(MetadataCommandCodecError::ZeroExpectedSchemaVersion);
                }

                validate_table(table)
            }
        }
    }

    pub fn to_proto(&self) -> metadata::MetadataCommand {
        use metadata::metadata_command::Command;

        let command = match self {
            Self::ClusterInitialized { cluster_id } => {
                Command::ClusterInitialized(metadata::ClusterInitialized {
                    cluster_id: cluster_id.clone(),
                })
            }

            Self::RegisterNode(node) => Command::RegisterNode(metadata::RegisterNode {
                node: Some(node.to_proto()),
            }),

            Self::CreateTable { table } => Command::CreateTable(metadata::CreateTable {
                table: Some(table.to_proto()),
            }),

            Self::CreateTablet { tablet } => Command::CreateTablet(metadata::CreateTablet {
                tablet: Some(tablet.to_proto()),
            }),

            Self::SetDesiredReplicaPlacement(placement) => {
                Command::SetDesiredReplicaPlacement(placement.to_proto())
            }

            Self::UpdateTableSchema {
                expected_schema_version,
                table,
            } => Command::UpdateTableSchema(metadata::UpdateTableSchema {
                expected_schema_version: *expected_schema_version,
                table: Some(table.to_proto()),
            }),
        };

        metadata::MetadataCommand {
            format_version: METADATA_COMMAND_VERSION,
            command: Some(command),
        }
    }

    pub fn from_proto(proto: metadata::MetadataCommand) -> Result<Self, MetadataCommandCodecError> {
        use metadata::metadata_command::Command;

        if proto.format_version != METADATA_COMMAND_VERSION {
            return Err(MetadataCommandCodecError::UnsupportedVersion(
                proto.format_version,
            ));
        }

        let command = match proto.command {
            Some(Command::ClusterInitialized(command)) => Self::ClusterInitialized {
                cluster_id: command.cluster_id,
            },

            Some(Command::RegisterNode(command)) => {
                Self::RegisterNode(NodeDescriptor::from_proto(command.node.ok_or(
                    MetadataCommandCodecError::MissingField("register_node.node"),
                )?)?)
            }

            Some(Command::CreateTable(command)) => Self::CreateTable {
                table: TableDefinition::from_proto(command.table.ok_or(
                    MetadataCommandCodecError::MissingField("create_table.table"),
                )?)
                .map_err(MetadataCommandCodecError::InvalidTable)?,
            },

            Some(Command::CreateTablet(command)) => Self::CreateTablet {
                tablet: TabletDescriptor::from_proto(command.tablet.ok_or(
                    MetadataCommandCodecError::MissingField("create_tablet.tablet"),
                )?)?,
            },

            Some(Command::SetDesiredReplicaPlacement(command)) => {
                Self::SetDesiredReplicaPlacement(DesiredReplicaPlacement::from_proto(command)?)
            }

            Some(Command::UpdateTableSchema(command)) => Self::UpdateTableSchema {
                expected_schema_version: command.expected_schema_version,

                table: TableDefinition::from_proto(command.table.ok_or(
                    MetadataCommandCodecError::MissingField("update_table_schema.table"),
                )?)
                .map_err(MetadataCommandCodecError::InvalidTable)?,
            },

            None => {
                return Err(MetadataCommandCodecError::MissingField("command"));
            }
        };

        command.validate()?;

        Ok(command)
    }
}

impl NodeDescriptor {
    pub fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        if self.node_id.0 == 0 {
            return Err(MetadataCommandCodecError::ZeroNodeId);
        }

        let endpoints = [
            ("raft_addr", self.raft_addr.as_str()),
            ("snapshot_addr", self.snapshot_addr.as_str()),
            ("sql_addr", self.sql_addr.as_str()),
            ("admin_addr", self.admin_addr.as_str()),
        ];

        let mut unique = BTreeSet::new();

        for (field, endpoint) in endpoints {
            validate_socket_addr(field, endpoint)?;

            if !unique.insert(endpoint) {
                return Err(MetadataCommandCodecError::DuplicateNodeEndpoint(
                    endpoint.to_string(),
                ));
            }
        }

        Ok(())
    }

    fn to_proto(&self) -> metadata::NodeDescriptor {
        metadata::NodeDescriptor {
            node_id: Some(self.node_id.to_proto()),
            raft_addr: self.raft_addr.clone(),
            snapshot_addr: self.snapshot_addr.clone(),
            sql_addr: self.sql_addr.clone(),
            admin_addr: self.admin_addr.clone(),
        }
    }

    fn from_proto(proto: metadata::NodeDescriptor) -> Result<Self, MetadataCommandCodecError> {
        let node = Self {
            node_id: NodeId::from_proto(
                proto
                    .node_id
                    .ok_or(MetadataCommandCodecError::MissingField("node.node_id"))?,
            ),
            raft_addr: proto.raft_addr,
            snapshot_addr: proto.snapshot_addr,
            sql_addr: proto.sql_addr,
            admin_addr: proto.admin_addr,
        };

        node.validate()?;

        Ok(node)
    }
}

impl PartitionSpec {
    pub fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        match self {
            Self::Hash {
                bucket,
                bucket_count,
            } => {
                if *bucket_count == 0 {
                    return Err(MetadataCommandCodecError::ZeroPartitionCount);
                }

                if *bucket >= *bucket_count {
                    return Err(MetadataCommandCodecError::InvalidHashBucket {
                        bucket: *bucket,
                        bucket_count: *bucket_count,
                    });
                }
            }
        }

        Ok(())
    }

    fn to_proto(&self) -> metadata::PartitionSpec {
        use metadata::partition_spec::Kind;

        let kind = match self {
            Self::Hash {
                bucket,
                bucket_count,
            } => Kind::Hash(metadata::HashPartition {
                bucket: *bucket,
                bucket_count: *bucket_count,
            }),
        };

        metadata::PartitionSpec { kind: Some(kind) }
    }

    fn from_proto(proto: metadata::PartitionSpec) -> Result<Self, MetadataCommandCodecError> {
        use metadata::partition_spec::Kind;

        let partition = match proto.kind {
            Some(Kind::Hash(hash)) => Self::Hash {
                bucket: hash.bucket,
                bucket_count: hash.bucket_count,
            },

            None => {
                return Err(MetadataCommandCodecError::MissingField(
                    "tablet.partition.kind",
                ));
            }
        };

        partition.validate()?;

        Ok(partition)
    }
}

impl TabletDescriptor {
    pub fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        if self.tablet_id.0 == 0 {
            return Err(MetadataCommandCodecError::ZeroTabletId);
        }

        if self.table_id.0 == 0 {
            return Err(MetadataCommandCodecError::ZeroTableId);
        }

        if self.raft_group_id.0 == 0 {
            return Err(MetadataCommandCodecError::ZeroRaftGroupId);
        }

        if self.tablet_epoch == 0 {
            return Err(MetadataCommandCodecError::ZeroTabletEpoch);
        }

        self.partition.validate()
    }

    fn to_proto(&self) -> metadata::TabletDescriptor {
        metadata::TabletDescriptor {
            tablet_id: Some(self.tablet_id.to_proto()),
            table_id: Some(self.table_id.to_proto()),
            raft_group_id: Some(self.raft_group_id.to_proto()),
            tablet_epoch: self.tablet_epoch,
            partition: Some(self.partition.to_proto()),
        }
    }

    fn from_proto(proto: metadata::TabletDescriptor) -> Result<Self, MetadataCommandCodecError> {
        let tablet = Self {
            tablet_id: TabletId::from_proto(
                proto
                    .tablet_id
                    .ok_or(MetadataCommandCodecError::MissingField("tablet.tablet_id"))?,
            ),

            table_id: crate::ids::TableId::from_proto(
                proto
                    .table_id
                    .ok_or(MetadataCommandCodecError::MissingField("tablet.table_id"))?,
            ),

            raft_group_id: RaftGroupId::from_proto(proto.raft_group_id.ok_or(
                MetadataCommandCodecError::MissingField("tablet.raft_group_id"),
            )?),

            tablet_epoch: proto.tablet_epoch,

            partition: PartitionSpec::from_proto(
                proto
                    .partition
                    .ok_or(MetadataCommandCodecError::MissingField("tablet.partition"))?,
            )?,
        };

        tablet.validate()?;

        Ok(tablet)
    }
}

impl DesiredReplicaPlacement {
    pub fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        if self.tablet_id.0 == 0 {
            return Err(MetadataCommandCodecError::ZeroTabletId);
        }

        if self.configuration_epoch == 0 {
            return Err(MetadataCommandCodecError::ZeroConfigurationEpoch);
        }

        if self.replicas.is_empty() {
            return Err(MetadataCommandCodecError::EmptyReplicaPlacement);
        }

        let mut nodes = BTreeSet::new();
        let mut previous_replica = None;
        let mut voter_count = 0_usize;

        for replica in &self.replicas {
            if replica.replica_id.0 == 0 {
                return Err(MetadataCommandCodecError::ZeroReplicaId);
            }

            if replica.node_id.0 == 0 {
                return Err(MetadataCommandCodecError::ZeroNodeId);
            }

            if previous_replica >= Some(replica.replica_id) {
                return Err(MetadataCommandCodecError::ReplicaPlacementNotCanonical);
            }

            if !nodes.insert(replica.node_id) {
                return Err(MetadataCommandCodecError::DuplicatePlacementNode(
                    replica.node_id,
                ));
            }

            if replica.role == DesiredReplicaRole::Voter {
                voter_count += 1;
            }

            previous_replica = Some(replica.replica_id);
        }

        if voter_count == 0 {
            return Err(MetadataCommandCodecError::PlacementHasNoVoter);
        }

        Ok(())
    }

    fn to_proto(&self) -> metadata::SetDesiredReplicaPlacement {
        metadata::SetDesiredReplicaPlacement {
            tablet_id: Some(self.tablet_id.to_proto()),

            configuration_epoch: self.configuration_epoch,

            replicas: self
                .replicas
                .iter()
                .map(|replica| metadata::DesiredReplica {
                    replica_id: Some(replica.replica_id.to_proto()),

                    node_id: Some(replica.node_id.to_proto()),

                    role: match replica.role {
                        DesiredReplicaRole::Voter => metadata::DesiredReplicaRole::Voter as i32,

                        DesiredReplicaRole::Learner => metadata::DesiredReplicaRole::Learner as i32,
                    },
                })
                .collect(),
        }
    }

    fn from_proto(
        proto: metadata::SetDesiredReplicaPlacement,
    ) -> Result<Self, MetadataCommandCodecError> {
        let placement = Self {
            tablet_id: TabletId::from_proto(proto.tablet_id.ok_or(
                MetadataCommandCodecError::MissingField("desired_placement.tablet_id"),
            )?),

            configuration_epoch: proto.configuration_epoch,

            replicas: proto
                .replicas
                .into_iter()
                .map(|replica| {
                    Ok(DesiredReplica {
                        replica_id: ReplicaId::from_proto(replica.replica_id.ok_or(
                            MetadataCommandCodecError::MissingField(
                                "desired_placement.replicas.replica_id",
                            ),
                        )?),

                        node_id: NodeId::from_proto(replica.node_id.ok_or(
                            MetadataCommandCodecError::MissingField(
                                "desired_placement.replicas.node_id",
                            ),
                        )?),

                        role: match metadata::DesiredReplicaRole::try_from(replica.role) {
                            Ok(metadata::DesiredReplicaRole::Voter) => DesiredReplicaRole::Voter,

                            Ok(metadata::DesiredReplicaRole::Learner) => {
                                DesiredReplicaRole::Learner
                            }

                            _ => {
                                return Err(MetadataCommandCodecError::InvalidReplicaRole);
                            }
                        },
                    })
                })
                .collect::<Result<Vec<_>, MetadataCommandCodecError>>()?,
        };

        placement.validate()?;

        Ok(placement)
    }
}

impl RetiredReplicaLifetime {
    fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        if self.raft_group_id.0 == 0 {
            return Err(MetadataCommandCodecError::ZeroRaftGroupId);
        }

        if self.replica_id.0 == 0 {
            return Err(MetadataCommandCodecError::ZeroReplicaId);
        }

        Ok(())
    }

    fn to_proto(self) -> metadata::RetiredReplicaLifetime {
        metadata::RetiredReplicaLifetime {
            raft_group_id: Some(self.raft_group_id.to_proto()),
            replica_id: Some(self.replica_id.to_proto()),
        }
    }

    fn from_proto(
        proto: metadata::RetiredReplicaLifetime,
    ) -> Result<Self, MetadataCommandCodecError> {
        let value = Self {
            raft_group_id: RaftGroupId::from_proto(proto.raft_group_id.ok_or(
                MetadataCommandCodecError::MissingField("retired_replica.raft_group_id"),
            )?),

            replica_id: ReplicaId::from_proto(proto.replica_id.ok_or(
                MetadataCommandCodecError::MissingField("retired_replica.replica_id"),
            )?),
        };

        value.validate()?;

        Ok(value)
    }
}

impl MetadataSnapshot {
    pub fn encode(&self) -> Result<Vec<u8>, MetadataCommandCodecError> {
        self.validate()?;
        Ok(self.to_proto().encode_to_vec())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, MetadataCommandCodecError> {
        let proto = metadata::MetadataSnapshot::decode(bytes)
            .map_err(|error| MetadataCommandCodecError::SnapshotDecode(error.to_string()))?;

        Self::from_proto(proto)
    }

    pub fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        match &self.cluster_id {
            Some(cluster_id) => {
                validate_cluster_id(cluster_id)?;
            }

            None => {
                if !self.nodes.is_empty()
                    || !self.tables.is_empty()
                    || !self.tablets.is_empty()
                    || !self.desired_placements.is_empty()
                    || !self.retired_replicas.is_empty()
                {
                    return Err(MetadataCommandCodecError::UninitializedSnapshotHasState);
                }
            }
        }

        for node in &self.nodes {
            node.validate()?;
        }

        for table in &self.tables {
            validate_table(table)?;
        }

        for tablet in &self.tablets {
            tablet.validate()?;
        }

        for placement in &self.desired_placements {
            placement.validate()?;
        }

        for retired in &self.retired_replicas {
            retired.validate()?;
        }

        if !strictly_ascending(&self.nodes, |node| node.node_id) {
            return Err(MetadataCommandCodecError::NonCanonicalSnapshot("nodes"));
        }

        if !strictly_ascending(&self.tables, |table| table.table_id) {
            return Err(MetadataCommandCodecError::NonCanonicalSnapshot("tables"));
        }

        if !strictly_ascending(&self.tablets, |tablet| tablet.tablet_id) {
            return Err(MetadataCommandCodecError::NonCanonicalSnapshot("tablets"));
        }

        if !strictly_ascending(&self.desired_placements, |placement| placement.tablet_id) {
            return Err(MetadataCommandCodecError::NonCanonicalSnapshot(
                "desired_placements",
            ));
        }

        if !strictly_ascending(&self.retired_replicas, |retired| {
            (retired.raft_group_id, retired.replica_id)
        }) {
            return Err(MetadataCommandCodecError::NonCanonicalSnapshot(
                "retired_replicas",
            ));
        }

        Ok(())
    }

    fn to_proto(&self) -> metadata::MetadataSnapshot {
        metadata::MetadataSnapshot {
            format_version: METADATA_SNAPSHOT_VERSION,

            initialized: self.cluster_id.is_some(),

            cluster_id: self.cluster_id.clone().unwrap_or_default(),

            nodes: self.nodes.iter().map(NodeDescriptor::to_proto).collect(),

            tables: self.tables.iter().map(TableDefinition::to_proto).collect(),

            tablets: self
                .tablets
                .iter()
                .map(TabletDescriptor::to_proto)
                .collect(),

            desired_placements: self
                .desired_placements
                .iter()
                .map(DesiredReplicaPlacement::to_proto)
                .collect(),

            retired_replicas: self
                .retired_replicas
                .iter()
                .copied()
                .map(RetiredReplicaLifetime::to_proto)
                .collect(),
        }
    }

    fn from_proto(proto: metadata::MetadataSnapshot) -> Result<Self, MetadataCommandCodecError> {
        if proto.format_version != METADATA_SNAPSHOT_VERSION {
            return Err(MetadataCommandCodecError::UnsupportedSnapshotVersion(
                proto.format_version,
            ));
        }

        let cluster_id = if proto.initialized {
            Some(proto.cluster_id)
        } else {
            if !proto.cluster_id.is_empty() {
                return Err(MetadataCommandCodecError::UninitializedSnapshotHasClusterId);
            }

            None
        };

        let snapshot = Self {
            cluster_id,

            nodes: proto
                .nodes
                .into_iter()
                .map(NodeDescriptor::from_proto)
                .collect::<Result<Vec<_>, _>>()?,

            tables: proto
                .tables
                .into_iter()
                .map(|table| {
                    TableDefinition::from_proto(table)
                        .map_err(MetadataCommandCodecError::InvalidTable)
                })
                .collect::<Result<Vec<_>, _>>()?,

            tablets: proto
                .tablets
                .into_iter()
                .map(TabletDescriptor::from_proto)
                .collect::<Result<Vec<_>, _>>()?,

            desired_placements: proto
                .desired_placements
                .into_iter()
                .map(DesiredReplicaPlacement::from_proto)
                .collect::<Result<Vec<_>, _>>()?,

            retired_replicas: proto
                .retired_replicas
                .into_iter()
                .map(RetiredReplicaLifetime::from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        };

        snapshot.validate()?;

        Ok(snapshot)
    }
}

fn strictly_ascending<T, K, F>(values: &[T], mut key: F) -> bool
where
    K: Ord,
    F: FnMut(&T) -> K,
{
    values.windows(2).all(|pair| key(&pair[0]) < key(&pair[1]))
}

fn validate_cluster_id(cluster_id: &str) -> Result<(), MetadataCommandCodecError> {
    if cluster_id.trim().is_empty() {
        return Err(MetadataCommandCodecError::EmptyClusterId);
    }

    Ok(())
}

fn validate_socket_addr(field: &'static str, value: &str) -> Result<(), MetadataCommandCodecError> {
    if value.trim().is_empty() {
        return Err(MetadataCommandCodecError::EmptyNodeEndpoint(field));
    }

    value
        .parse::<SocketAddr>()
        .map_err(|_| MetadataCommandCodecError::InvalidSocketAddress {
            field,
            value: value.to_string(),
        })?;

    Ok(())
}

fn validate_table(table: &TableDefinition) -> Result<(), MetadataCommandCodecError> {
    if table.table_id == 0 {
        return Err(MetadataCommandCodecError::ZeroTableId);
    }

    if table.name.trim().is_empty() {
        return Err(MetadataCommandCodecError::EmptyTableName);
    }

    if table.schema_version == 0 {
        return Err(MetadataCommandCodecError::ZeroSchemaVersion);
    }

    if table.tablet_count == 0 {
        return Err(MetadataCommandCodecError::ZeroTabletCount);
    }

    Ok(())
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MetadataCommandCodecError {
    #[error("unsupported metadata command format version {0}")]
    UnsupportedVersion(u32),

    #[error("unsupported metadata snapshot format version {0}")]
    UnsupportedSnapshotVersion(u32),

    #[error("metadata command protobuf decode failed: {0}")]
    Decode(String),

    #[error("metadata snapshot protobuf decode failed: {0}")]
    SnapshotDecode(String),

    #[error("metadata value is missing {0}")]
    MissingField(&'static str),

    #[error("metadata cluster ID cannot be empty")]
    EmptyClusterId,

    #[error("metadata node ID must be non-zero")]
    ZeroNodeId,

    #[error("metadata endpoint {0} cannot be empty")]
    EmptyNodeEndpoint(&'static str),

    #[error("metadata endpoint {field} is not a valid socket address: {value}")]
    InvalidSocketAddress { field: &'static str, value: String },

    #[error("a physical node cannot bind multiple services to metadata endpoint {0}")]
    DuplicateNodeEndpoint(String),

    #[error("metadata table ID must be non-zero")]
    ZeroTableId,

    #[error("metadata table name cannot be empty")]
    EmptyTableName,

    #[error("metadata schema version must be non-zero")]
    ZeroSchemaVersion,

    #[error("metadata expected schema version must be non-zero")]
    ZeroExpectedSchemaVersion,

    #[error("metadata table must have at least one tablet")]
    ZeroTabletCount,

    #[error("metadata table is invalid: {0}")]
    InvalidTable(&'static str),

    #[error("metadata tablet ID must be non-zero")]
    ZeroTabletId,

    #[error("metadata Raft group ID must be non-zero")]
    ZeroRaftGroupId,

    #[error("metadata tablet epoch must be non-zero")]
    ZeroTabletEpoch,

    #[error("hash partition bucket count must be non-zero")]
    ZeroPartitionCount,

    #[error("hash partition bucket {bucket} is outside bucket count {bucket_count}")]
    InvalidHashBucket { bucket: u32, bucket_count: u32 },

    #[error("metadata configuration epoch must be non-zero")]
    ZeroConfigurationEpoch,

    #[error("metadata replica placement cannot be empty")]
    EmptyReplicaPlacement,

    #[error("metadata desired placement must contain at least one voter")]
    PlacementHasNoVoter,

    #[error("metadata replica ID must be non-zero")]
    ZeroReplicaId,

    #[error("metadata replica placement must be strictly ordered by replica ID")]
    ReplicaPlacementNotCanonical,

    #[error(
        "metadata replica placement assigns node {} more than once",
        .0.0
    )]
    DuplicatePlacementNode(NodeId),

    #[error("metadata replica role is invalid or unspecified")]
    InvalidReplicaRole,

    #[error("uninitialized metadata snapshot contains replicated state")]
    UninitializedSnapshotHasState,

    #[error("uninitialized metadata snapshot contains a cluster ID")]
    UninitializedSnapshotHasClusterId,

    #[error("metadata snapshot field {0} is not in canonical ascending order")]
    NonCanonicalSnapshot(&'static str),
}
