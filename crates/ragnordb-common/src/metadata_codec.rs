//! durable commands for the replicated metadata state machine
//!
//! metadata commands describe desired cluster topology. They deliberately do
//! not encode a Raft group's current leader or committed membership, because
//! those are respectively live observation state and durable Raft `ConfState`

use std::collections::BTreeSet;

use prost::Message;

use crate::{
    catalog_codec::TableDefinition,
    ids::{NodeId, RaftGroupId, ReplicaId, TableId, TabletId},
    proto::metadata,
};

/// first compatible wire format for metadata Raft commands
pub const METADATA_COMMAND_VERSION: u32 = 1;

/// deterministic state transition proposed to the metadata Raft group
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

/// durable physical-node directory entry
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDescriptor {
    pub node_id: NodeId,
    pub endpoint: String,
}

/// durable tablet-to-Raft-group assignment
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletDescriptor {
    pub tablet_id: TabletId,
    pub table_id: TableId,
    pub raft_group_id: RaftGroupId,
    pub tablet_epoch: u64,
    pub schema_version: u64,
}

/// the requested role for a replica after reconciliation completes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DesiredReplicaRole {
    Voter,
    Learner,
}

/// One desired replica lifetime and its physical host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredReplica {
    pub replica_id: ReplicaId,
    pub node_id: NodeId,
    pub role: DesiredReplicaRole,
}

/// Desired membership for a tablet at one metadata configuration epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredReplicaPlacement {
    pub tablet_id: TabletId,
    pub configuration_epoch: u64,
    /// Strictly ascending by replica ID so a logical placement has one wire form.
    pub replicas: Vec<DesiredReplica>,
}

impl MetadataCommand {
    /// Encode only a valid command so malformed metadata cannot become a
    /// committed byte sequence through an in-process caller.
    pub fn encode(&self) -> Result<Vec<u8>, MetadataCommandCodecError> {
        self.validate()?;
        Ok(self.to_proto().encode_to_vec())
    }

    /// Decode and validate bytes received from the Raft log or recovery.
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
            None => return Err(MetadataCommandCodecError::MissingField("command")),
        };

        command.validate()?;
        Ok(command)
    }
}

impl NodeDescriptor {
    fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        if self.node_id.0 == 0 {
            return Err(MetadataCommandCodecError::ZeroNodeId);
        }
        if self.endpoint.trim().is_empty() {
            return Err(MetadataCommandCodecError::EmptyNodeEndpoint);
        }
        Ok(())
    }

    fn to_proto(&self) -> metadata::NodeDescriptor {
        metadata::NodeDescriptor {
            node_id: Some(self.node_id.to_proto()),
            endpoint: self.endpoint.clone(),
        }
    }

    fn from_proto(proto: metadata::NodeDescriptor) -> Result<Self, MetadataCommandCodecError> {
        let node = Self {
            node_id: NodeId::from_proto(
                proto
                    .node_id
                    .ok_or(MetadataCommandCodecError::MissingField("node.node_id"))?,
            ),
            endpoint: proto.endpoint,
        };
        node.validate()?;
        Ok(node)
    }
}

impl TabletDescriptor {
    fn validate(&self) -> Result<(), MetadataCommandCodecError> {
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
        if self.schema_version == 0 {
            return Err(MetadataCommandCodecError::ZeroSchemaVersion);
        }
        Ok(())
    }

    fn to_proto(&self) -> metadata::TabletDescriptor {
        metadata::TabletDescriptor {
            tablet_id: Some(self.tablet_id.to_proto()),
            table_id: Some(self.table_id.to_proto()),
            raft_group_id: Some(self.raft_group_id.to_proto()),
            tablet_epoch: self.tablet_epoch,
            schema_version: self.schema_version,
        }
    }

    fn from_proto(proto: metadata::TabletDescriptor) -> Result<Self, MetadataCommandCodecError> {
        let tablet = Self {
            tablet_id: TabletId::from_proto(
                proto
                    .tablet_id
                    .ok_or(MetadataCommandCodecError::MissingField("tablet.tablet_id"))?,
            ),
            table_id: TableId::from_proto(
                proto
                    .table_id
                    .ok_or(MetadataCommandCodecError::MissingField("tablet.table_id"))?,
            ),
            raft_group_id: RaftGroupId::from_proto(proto.raft_group_id.ok_or(
                MetadataCommandCodecError::MissingField("tablet.raft_group_id"),
            )?),
            tablet_epoch: proto.tablet_epoch,
            schema_version: proto.schema_version,
        };
        tablet.validate()?;
        Ok(tablet)
    }
}

impl DesiredReplicaPlacement {
    fn validate(&self) -> Result<(), MetadataCommandCodecError> {
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
            previous_replica = Some(replica.replica_id);
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
                            _ => return Err(MetadataCommandCodecError::InvalidReplicaRole),
                        },
                    })
                })
                .collect::<Result<Vec<_>, MetadataCommandCodecError>>()?,
        };
        placement.validate()?;
        Ok(placement)
    }
}

fn validate_cluster_id(cluster_id: &str) -> Result<(), MetadataCommandCodecError> {
    if cluster_id.trim().is_empty() {
        return Err(MetadataCommandCodecError::EmptyClusterId);
    }
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

    #[error("metadata command protobuf decode failed: {0}")]
    Decode(String),

    #[error("metadata command is missing {0}")]
    MissingField(&'static str),

    #[error("metadata cluster ID cannot be empty")]
    EmptyClusterId,

    #[error("metadata node ID must be non-zero")]
    ZeroNodeId,

    #[error("metadata node endpoint cannot be empty")]
    EmptyNodeEndpoint,

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

    #[error("metadata configuration epoch must be non-zero")]
    ZeroConfigurationEpoch,

    #[error("metadata replica placement cannot be empty")]
    EmptyReplicaPlacement,

    #[error("metadata replica ID must be non-zero")]
    ZeroReplicaId,

    #[error("metadata replica placement must be strictly ordered by replica ID")]
    ReplicaPlacementNotCanonical,

    #[error("metadata replica placement assigns node {} more than once", .0.0)]
    DuplicatePlacementNode(NodeId),

    #[error("metadata replica role is invalid or unspecified")]
    InvalidReplicaRole,
}
