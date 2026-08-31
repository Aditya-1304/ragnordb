//! Durable codec and domain types for metadata Raft commands and snapshots.
//!
//! The metadata log contains desired topology and catalog state only.
//! Transient leader observations and each Raft group's committed `ConfState`
//! have separate authorities and are intentionally absent from this format.

use std::{collections::BTreeSet, net::SocketAddr};

use prost::Message;

use crate::{
    catalog_codec::{ColumnDefinition, TableDefinition},
    ids::{ColumnId, NodeId, RaftGroupId, ReplicaId, RequestId, TableId, TabletId},
    proto::metadata,
};

/// First production metadata-command format.
///
/// Version 1 existed only as premature pre-Phase-5 experimental code and is
/// intentionally rejected rather than silently reinterpreted.
pub const METADATA_COMMAND_VERSION: u32 = 2;

/// Version of the request-bearing metadata proposal envelope.
pub const METADATA_COMMAND_ENVELOPE_VERSION: u32 = 1;

/// Snapshot format before metadata-owned allocation history was persisted.
pub const LEGACY_METADATA_SNAPSHOT_VERSION: u32 = 1;

/// Snapshot format carrying explicit identity high-water marks.
///
/// Version 2 prevents an older reader from silently ignoring allocator history
/// that may no longer be derivable from visible objects after metadata deletion.
pub const METADATA_SNAPSHOT_VERSION: u32 = 2;

/// Compatibility high-water marks reserved by the M4 runtime and metadata
/// Raft group. New metadata allocations begin strictly above these values.
pub const INITIAL_METADATA_TABLE_HIGH_WATER: u64 = 1;
pub const INITIAL_METADATA_TABLET_HIGH_WATER: u64 = 1;

/// Durable identity occupied by the metadata Raft group. Tablet metadata must
/// never assign this group to a SQL tablet.
pub const RESERVED_LEGACY_RAFT_GROUP_ID: RaftGroupId = RaftGroupId(1);

pub const RESERVED_METADATA_RAFT_GROUP_ID: RaftGroupId = RaftGroupId(2);

pub const INITIAL_METADATA_RAFT_GROUP_HIGH_WATER: u64 = RESERVED_METADATA_RAFT_GROUP_ID.0;

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

    /// Atomically allocate and publish one table with its initial topology.
    CreateTableTopology(CreateTableRequest),

    SetDesiredReplicaPlacement(DesiredReplicaPlacement),

    UpdateTableSchema {
        expected_schema_version: u64,
        table: TableDefinition,
    },
}

/// Unallocated schema semantics submitted to the metadata state machine.
///
/// Cluster-global identities and initial topology are deliberately absent:
/// only the committed metadata transition may assign them.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableRequest {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key_column_ids: Vec<ColumnId>,
}

/// Durable metadata proposal envelope carrying the identity used for retry
/// deduplication.
///
/// The command payload remains unallocated. The metadata state machine assigns
/// table, tablet, and Raft identities only when this envelope is applied in
/// the committed log order.
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataCommandEnvelope {
    pub format_version: u32,
    pub request_id: RequestId,
    pub command: MetadataCommand,
}

impl MetadataCommandEnvelope {
    pub fn new(
        request_id: RequestId,
        command: MetadataCommand,
    ) -> Result<Self, MetadataCommandCodecError> {
        let envelope = Self {
            format_version: METADATA_COMMAND_ENVELOPE_VERSION,
            request_id,
            command,
        };

        envelope.validate()?;
        Ok(envelope)
    }

    pub fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        if self.format_version != METADATA_COMMAND_ENVELOPE_VERSION {
            return Err(MetadataCommandCodecError::UnsupportedEnvelopeVersion(
                self.format_version,
            ));
        }

        if self.request_id.client_id == 0 {
            return Err(MetadataCommandCodecError::InvalidRequestId(
                "client ID must be non-zero",
            ));
        }

        if self.request_id.sequence == 0 {
            return Err(MetadataCommandCodecError::InvalidRequestId(
                "request sequence must be non-zero",
            ));
        }

        if self.request_id.raft_group_id != RESERVED_METADATA_RAFT_GROUP_ID {
            return Err(MetadataCommandCodecError::RequestGroupMismatch {
                expected: RESERVED_METADATA_RAFT_GROUP_ID,
                received: self.request_id.raft_group_id,
            });
        }

        self.command.validate()
    }

    pub fn encode(&self) -> Result<Vec<u8>, MetadataCommandCodecError> {
        self.validate()?;
        Ok(self.to_proto().encode_to_vec())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, MetadataCommandCodecError> {
        let proto = metadata::MetadataCommand::decode(bytes)
            .map_err(|error| MetadataCommandCodecError::Decode(error.to_string()))?;

        Self::from_proto(proto)
    }

    fn to_proto(&self) -> metadata::MetadataCommand {
        let mut proto = self.command.to_proto();
        proto.request_id = Some(self.request_id.to_proto());
        proto.envelope_version = self.format_version;
        proto
    }

    fn from_proto(proto: metadata::MetadataCommand) -> Result<Self, MetadataCommandCodecError> {
        if proto.envelope_version != METADATA_COMMAND_ENVELOPE_VERSION {
            return Err(MetadataCommandCodecError::UnsupportedEnvelopeVersion(
                proto.envelope_version,
            ));
        }

        let request_id = RequestId::from_proto(proto.request_id.clone().ok_or(
            MetadataCommandCodecError::MissingField("metadata_command.request_id"),
        )?)
        .map_err(MetadataCommandCodecError::InvalidRequestId)?;

        let command = MetadataCommand::from_proto(proto)?;
        Self::new(request_id, command)
    }
}

/// Result retained for one request identity in the replicated metadata state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataCachedOutcome {
    Applied,
    AlreadyApplied,
    TableCreated {
        table_id: TableId,
        tablet_id: TabletId,
        raft_group_id: RaftGroupId,
    },
    Rejected(String),
}

/// One request identity and its deterministic result retained in snapshots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRequestDeduplication {
    pub request_id: RequestId,
    pub outcome: MetadataCachedOutcome,
}

/// Monotonic identity high-water marks owned by metadata.
///
/// These are high-water marks rather than next values, so removing a visible
/// object cannot make its durable identity available for reuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataAllocatorState {
    pub max_table_id: u64,
    pub max_tablet_id: u64,
    pub max_raft_group_id: u64,
}

impl MetadataAllocatorState {
    pub const fn initial() -> Self {
        Self {
            max_table_id: INITIAL_METADATA_TABLE_HIGH_WATER,
            max_tablet_id: INITIAL_METADATA_TABLET_HIGH_WATER,
            max_raft_group_id: INITIAL_METADATA_RAFT_GROUP_HIGH_WATER,
        }
    }
}

impl Default for MetadataAllocatorState {
    fn default() -> Self {
        Self::initial()
    }
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
    pub allocator: MetadataAllocatorState,

    pub request_deduplication: Vec<MetadataRequestDeduplication>,
}

impl MetadataCommand {
    /// Encode only a structurally valid command.
    pub fn encode(&self) -> Result<Vec<u8>, MetadataCommandCodecError> {
        self.validate()?;
        Ok(self.to_proto().encode_to_vec())
    }

    /// Decode and validate one command read from the Raft log.
    pub fn decode(bytes: &[u8]) -> Result<Self, MetadataCommandCodecError> {
        let (_, command) = Self::decode_with_optional_request_id(bytes)?;

        Ok(command)
    }

    /// Decode a metadata command while preserving an optional request identity.
    ///
    /// The optional form is required for replaying pre-Slice-2 bootstrap
    /// entries, which intentionally have no client request identity.
    pub fn decode_with_optional_request_id(
        bytes: &[u8],
    ) -> Result<(Option<RequestId>, Self), MetadataCommandCodecError> {
        let proto = metadata::MetadataCommand::decode(bytes)
            .map_err(|error| MetadataCommandCodecError::Decode(error.to_string()))?;

        let request_id = proto
            .request_id
            .clone()
            .map(RequestId::from_proto)
            .transpose()
            .map_err(MetadataCommandCodecError::InvalidRequestId)?;

        let envelope_version = proto.envelope_version;
        let command = Self::from_proto(proto)?;

        if let Some(request_id) = &request_id {
            if envelope_version != METADATA_COMMAND_ENVELOPE_VERSION {
                return Err(MetadataCommandCodecError::UnsupportedEnvelopeVersion(
                    envelope_version,
                ));
            }

            // State-machine replay uses this optional decoder directly rather
            // than constructing an envelope first. Reapply the envelope
            // identity checks here so malformed committed request IDs cannot
            // enter the durable deduplication map.
            MetadataCommandEnvelope {
                format_version: METADATA_COMMAND_ENVELOPE_VERSION,
                request_id: request_id.clone(),
                command: command.clone(),
            }
            .validate()?;
        }

        Ok((request_id, command))
    }

    pub fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        match self {
            Self::ClusterInitialized { cluster_id } => validate_cluster_id(cluster_id),

            Self::RegisterNode(node) => node.validate(),

            Self::CreateTable { table } => validate_table(table),

            Self::CreateTablet { tablet } => tablet.validate(),

            Self::CreateTableTopology(request) => request.validate(),

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

            Self::CreateTableTopology(request) => Command::CreateTableTopology(request.to_proto()),

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
            request_id: None,
            envelope_version: 0,
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

            Some(Command::CreateTableTopology(command)) => {
                Self::CreateTableTopology(CreateTableRequest::from_proto(command)?)
            }

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

impl CreateTableRequest {
    /// Validate the schema semantics that can be checked without metadata
    /// state. The state machine repeats full table validation after assigning
    /// the identities it owns.
    pub fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        if self.table_name.trim().is_empty() {
            return Err(MetadataCommandCodecError::EmptyTableName);
        }

        if self.columns.is_empty() {
            return Err(MetadataCommandCodecError::EmptyTableColumns);
        }

        let mut column_ids = BTreeSet::new();
        let mut column_names = BTreeSet::new();

        for column in &self.columns {
            if column.column_id.0 == 0 {
                return Err(MetadataCommandCodecError::InvalidTable(
                    "column ID must be non-zero",
                ));
            }

            if column.name.trim().is_empty() {
                return Err(MetadataCommandCodecError::InvalidTable(
                    "column name cannot be empty",
                ));
            }

            if !column_ids.insert(column.column_id) {
                return Err(MetadataCommandCodecError::InvalidTable(
                    "column IDs must be unique",
                ));
            }

            if !column_names.insert(column.name.as_str()) {
                return Err(MetadataCommandCodecError::InvalidTable(
                    "column names must be unique",
                ));
            }
        }

        if self.primary_key_column_ids.is_empty() {
            return Err(MetadataCommandCodecError::InvalidTable(
                "primary key must contain at least one column",
            ));
        }

        let mut primary_key_ids = BTreeSet::new();

        for column_id in &self.primary_key_column_ids {
            if column_id.0 == 0 {
                return Err(MetadataCommandCodecError::InvalidTable(
                    "primary-key column ID must be non-zero",
                ));
            }

            if !primary_key_ids.insert(*column_id) {
                return Err(MetadataCommandCodecError::InvalidTable(
                    "primary-key column IDs must be unique",
                ));
            }

            let column = self
                .columns
                .iter()
                .find(|column| column.column_id == *column_id)
                .ok_or(MetadataCommandCodecError::InvalidTable(
                    "primary-key column ID must reference a declared column",
                ))?;

            if column.nullable {
                return Err(MetadataCommandCodecError::InvalidTable(
                    "primary-key columns cannot be nullable",
                ));
            }
        }

        Ok(())
    }

    fn to_proto(&self) -> metadata::CreateTableTopology {
        metadata::CreateTableTopology {
            table_name: self.table_name.clone(),
            columns: self
                .columns
                .iter()
                .map(ColumnDefinition::to_proto)
                .collect(),
            primary_key_column_ids: self
                .primary_key_column_ids
                .iter()
                .map(|column_id| column_id.0)
                .collect(),
        }
    }

    fn from_proto(proto: metadata::CreateTableTopology) -> Result<Self, MetadataCommandCodecError> {
        let request = Self {
            table_name: proto.table_name,
            columns: proto
                .columns
                .into_iter()
                .map(|column| {
                    ColumnDefinition::from_proto(column)
                        .map_err(MetadataCommandCodecError::InvalidTable)
                })
                .collect::<Result<Vec<_>, _>>()?,
            primary_key_column_ids: proto
                .primary_key_column_ids
                .into_iter()
                .map(ColumnId)
                .collect(),
        };

        request.validate()?;

        Ok(request)
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

        // Group 2 is the metadata state machine's own Raft group. Allowing a
        // tablet to claim it would alias two independent authorities onto one
        // log and persistence namespace. Group 1 remains readable for the
        // legacy M4 descriptor format; the new atomic allocator starts at 3.
        if self.raft_group_id == RESERVED_METADATA_RAFT_GROUP_ID {
            return Err(
                MetadataCommandCodecError::MetadataRaftGroupAssignedToTablet(self.raft_group_id),
            );
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

impl MetadataAllocatorState {
    fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        if self.max_table_id < INITIAL_METADATA_TABLE_HIGH_WATER {
            return Err(MetadataCommandCodecError::AllocatorBelowReservedFloor(
                "table",
            ));
        }

        if self.max_tablet_id < INITIAL_METADATA_TABLET_HIGH_WATER {
            return Err(MetadataCommandCodecError::AllocatorBelowReservedFloor(
                "tablet",
            ));
        }

        if self.max_raft_group_id < INITIAL_METADATA_RAFT_GROUP_HIGH_WATER {
            return Err(MetadataCommandCodecError::AllocatorBelowReservedFloor(
                "raft_group",
            ));
        }

        Ok(())
    }

    fn to_proto(self) -> metadata::MetadataAllocatorState {
        metadata::MetadataAllocatorState {
            max_table_id: self.max_table_id,
            max_tablet_id: self.max_tablet_id,
            max_raft_group_id: self.max_raft_group_id,
        }
    }

    fn from_proto(
        proto: metadata::MetadataAllocatorState,
    ) -> Result<Self, MetadataCommandCodecError> {
        let allocator = Self {
            max_table_id: proto.max_table_id,
            max_tablet_id: proto.max_tablet_id,
            max_raft_group_id: proto.max_raft_group_id,
        };

        allocator.validate()?;

        Ok(allocator)
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
                    || !self.request_deduplication.is_empty()
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

        for request in &self.request_deduplication {
            request.validate()?;
        }

        self.allocator.validate()?;

        let visible_max_table_id = self
            .tables
            .iter()
            .map(|table| table.table_id)
            .max()
            .unwrap_or(INITIAL_METADATA_TABLE_HIGH_WATER);

        if self.allocator.max_table_id < visible_max_table_id {
            return Err(MetadataCommandCodecError::AllocatorBelowVisibleState {
                kind: "table",
                high_water: self.allocator.max_table_id,
                visible: visible_max_table_id,
            });
        }

        let visible_max_tablet_id = self
            .tablets
            .iter()
            .map(|tablet| tablet.tablet_id.0)
            .max()
            .unwrap_or(INITIAL_METADATA_TABLET_HIGH_WATER);

        if self.allocator.max_tablet_id < visible_max_tablet_id {
            return Err(MetadataCommandCodecError::AllocatorBelowVisibleState {
                kind: "tablet",
                high_water: self.allocator.max_tablet_id,
                visible: visible_max_tablet_id,
            });
        }

        let visible_max_raft_group_id = self
            .tablets
            .iter()
            .map(|tablet| tablet.raft_group_id.0)
            .chain(
                self.retired_replicas
                    .iter()
                    .map(|retired| retired.raft_group_id.0),
            )
            .max()
            .unwrap_or(INITIAL_METADATA_RAFT_GROUP_HIGH_WATER);

        if self.allocator.max_raft_group_id < visible_max_raft_group_id {
            return Err(MetadataCommandCodecError::AllocatorBelowVisibleState {
                kind: "raft_group",
                high_water: self.allocator.max_raft_group_id,
                visible: visible_max_raft_group_id,
            });
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

        if !strictly_ascending(&self.request_deduplication, |request| {
            request.request_id.clone()
        }) {
            return Err(MetadataCommandCodecError::NonCanonicalSnapshot(
                "request_deduplication",
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

            allocator_state: Some(self.allocator.to_proto()),

            request_deduplication: self
                .request_deduplication
                .iter()
                .map(MetadataRequestDeduplication::to_proto)
                .collect(),
        }
    }

    fn from_proto(proto: metadata::MetadataSnapshot) -> Result<Self, MetadataCommandCodecError> {
        let snapshot_version = proto.format_version;

        if snapshot_version != LEGACY_METADATA_SNAPSHOT_VERSION
            && snapshot_version != METADATA_SNAPSHOT_VERSION
        {
            return Err(MetadataCommandCodecError::UnsupportedSnapshotVersion(
                snapshot_version,
            ));
        }

        let metadata::MetadataSnapshot {
            initialized,
            cluster_id,
            nodes,
            tables,
            tablets,
            desired_placements,
            retired_replicas,
            allocator_state,
            request_deduplication,
            ..
        } = proto;

        let cluster_id = if initialized {
            Some(cluster_id)
        } else {
            if !cluster_id.is_empty() {
                return Err(MetadataCommandCodecError::UninitializedSnapshotHasClusterId);
            }

            None
        };

        let nodes = nodes
            .into_iter()
            .map(NodeDescriptor::from_proto)
            .collect::<Result<Vec<_>, _>>()?;

        let tables = tables
            .into_iter()
            .map(|table| {
                TableDefinition::from_proto(table).map_err(MetadataCommandCodecError::InvalidTable)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let tablets = tablets
            .into_iter()
            .map(TabletDescriptor::from_proto)
            .collect::<Result<Vec<_>, _>>()?;

        let desired_placements = desired_placements
            .into_iter()
            .map(DesiredReplicaPlacement::from_proto)
            .collect::<Result<Vec<_>, _>>()?;

        let retired_replicas = retired_replicas
            .into_iter()
            .map(RetiredReplicaLifetime::from_proto)
            .collect::<Result<Vec<_>, _>>()?;

        let request_deduplication = request_deduplication
            .into_iter()
            .map(MetadataRequestDeduplication::from_proto)
            .collect::<Result<Vec<_>, _>>()?;

        let allocator = match (snapshot_version, allocator_state) {
            (METADATA_SNAPSHOT_VERSION, Some(allocator))
            | (LEGACY_METADATA_SNAPSHOT_VERSION, Some(allocator)) => {
                MetadataAllocatorState::from_proto(allocator)?
            }

            (METADATA_SNAPSHOT_VERSION, None) => {
                return Err(MetadataCommandCodecError::MissingField(
                    "snapshot.allocator_state",
                ));
            }

            (LEGACY_METADATA_SNAPSHOT_VERSION, None) => MetadataAllocatorState {
                max_table_id: tables
                    .iter()
                    .map(|table| table.table_id)
                    .max()
                    .unwrap_or(INITIAL_METADATA_TABLE_HIGH_WATER)
                    .max(INITIAL_METADATA_TABLE_HIGH_WATER),

                max_tablet_id: tablets
                    .iter()
                    .map(|tablet| tablet.tablet_id.0)
                    .max()
                    .unwrap_or(INITIAL_METADATA_TABLET_HIGH_WATER)
                    .max(INITIAL_METADATA_TABLET_HIGH_WATER),

                max_raft_group_id: tablets
                    .iter()
                    .map(|tablet| tablet.raft_group_id.0)
                    .chain(
                        retired_replicas
                            .iter()
                            .map(|retired| retired.raft_group_id.0),
                    )
                    .max()
                    .unwrap_or(INITIAL_METADATA_RAFT_GROUP_HIGH_WATER)
                    .max(INITIAL_METADATA_RAFT_GROUP_HIGH_WATER),
            },

            _ => unreachable!("snapshot version checked above"),
        };

        let snapshot = Self {
            cluster_id,
            nodes,
            tables,
            tablets,
            desired_placements,
            retired_replicas,
            allocator,
            request_deduplication,
        };

        snapshot.validate()?;

        Ok(snapshot)
    }
}

impl MetadataRequestDeduplication {
    fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        if self.request_id.client_id == 0 {
            return Err(MetadataCommandCodecError::InvalidRequestId(
                "client ID must be non-zero",
            ));
        }

        if self.request_id.sequence == 0 {
            return Err(MetadataCommandCodecError::InvalidRequestId(
                "request sequence must be non-zero",
            ));
        }

        if self.request_id.raft_group_id != RESERVED_METADATA_RAFT_GROUP_ID {
            return Err(MetadataCommandCodecError::RequestGroupMismatch {
                expected: RESERVED_METADATA_RAFT_GROUP_ID,
                received: self.request_id.raft_group_id,
            });
        }

        match &self.outcome {
            MetadataCachedOutcome::Applied | MetadataCachedOutcome::AlreadyApplied => {}

            MetadataCachedOutcome::TableCreated {
                table_id,
                tablet_id,
                raft_group_id,
            } => {
                if table_id.0 <= INITIAL_METADATA_TABLE_HIGH_WATER {
                    return Err(MetadataCommandCodecError::InvalidCachedOutcome(
                        "cached CREATE TABLE result uses a reserved table ID",
                    ));
                }

                if tablet_id.0 <= INITIAL_METADATA_TABLET_HIGH_WATER {
                    return Err(MetadataCommandCodecError::InvalidCachedOutcome(
                        "cached CREATE TABLE result uses a reserved tablet ID",
                    ));
                }

                if raft_group_id.0 == 0 {
                    return Err(MetadataCommandCodecError::ZeroRaftGroupId);
                }

                if *raft_group_id == RESERVED_LEGACY_RAFT_GROUP_ID
                    || *raft_group_id == RESERVED_METADATA_RAFT_GROUP_ID
                {
                    return Err(
                        MetadataCommandCodecError::MetadataRaftGroupAssignedToTablet(
                            *raft_group_id,
                        ),
                    );
                }
            }

            MetadataCachedOutcome::Rejected(reason) if reason.trim().is_empty() => {
                return Err(MetadataCommandCodecError::InvalidCachedOutcome(
                    "rejection reason cannot be empty",
                ));
            }

            MetadataCachedOutcome::Rejected(_) => {}
        }

        Ok(())
    }

    fn to_proto(&self) -> metadata::MetadataRequestDeduplication {
        let (outcome_kind, table_id, tablet_id, raft_group_id, rejection) = match &self.outcome {
            MetadataCachedOutcome::Applied => (
                metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeApplied,
                0,
                0,
                0,
                String::new(),
            ),
            MetadataCachedOutcome::AlreadyApplied => (
                metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeAlreadyApplied,
                0,
                0,
                0,
                String::new(),
            ),
            MetadataCachedOutcome::TableCreated {
                table_id,
                tablet_id,
                raft_group_id,
            } => (
                metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeTableCreated,
                table_id.0,
                tablet_id.0,
                raft_group_id.0,
                String::new(),
            ),
            MetadataCachedOutcome::Rejected(reason) => (
                metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeRejected,
                0,
                0,
                0,
                reason.clone(),
            ),
        };

        metadata::MetadataRequestDeduplication {
            request_id: Some(self.request_id.to_proto()),
            outcome_kind: outcome_kind as i32,
            table_id,
            tablet_id,
            raft_group_id,
            rejection,
        }
    }

    fn from_proto(
        proto: metadata::MetadataRequestDeduplication,
    ) -> Result<Self, MetadataCommandCodecError> {
        let request_id = RequestId::from_proto(proto.request_id.ok_or(
            MetadataCommandCodecError::MissingField("request_deduplication.request_id"),
        )?)
        .map_err(MetadataCommandCodecError::InvalidRequestId)?;

        let outcome_kind = metadata::MetadataCachedOutcomeKind::try_from(proto.outcome_kind)
            .map_err(|_| MetadataCommandCodecError::InvalidCachedOutcome("unknown outcome kind"))?;

        let outcome = match outcome_kind {
            metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeApplied => {
                MetadataCachedOutcome::Applied
            }
            metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeAlreadyApplied => {
                MetadataCachedOutcome::AlreadyApplied
            }
            metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeTableCreated => {
                MetadataCachedOutcome::TableCreated {
                    table_id: TableId(proto.table_id),
                    tablet_id: TabletId(proto.tablet_id),
                    raft_group_id: RaftGroupId(proto.raft_group_id),
                }
            }
            metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeRejected => {
                MetadataCachedOutcome::Rejected(proto.rejection)
            }
            metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeUnspecified => {
                return Err(MetadataCommandCodecError::InvalidCachedOutcome(
                    "outcome kind is unspecified",
                ));
            }
        };

        let request = Self {
            request_id,
            outcome,
        };
        request.validate()?;
        Ok(request)
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

    #[error("unsupported metadata command envelope format version {0}")]
    UnsupportedEnvelopeVersion(u32),

    #[error("unsupported metadata snapshot format version {0}")]
    UnsupportedSnapshotVersion(u32),

    #[error("metadata command protobuf decode failed: {0}")]
    Decode(String),

    #[error("metadata snapshot protobuf decode failed: {0}")]
    SnapshotDecode(String),

    #[error("metadata value is missing {0}")]
    MissingField(&'static str),

    #[error("invalid metadata request ID: {0}")]
    InvalidRequestId(&'static str),

    #[error(
        "metadata request belongs to Raft group {}, expected metadata group {}",
        received.0,
        expected.0
    )]
    RequestGroupMismatch {
        expected: RaftGroupId,
        received: RaftGroupId,
    },

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

    #[error("metadata CREATE TABLE requires at least one column")]
    EmptyTableColumns,

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

    #[error(
        "Raft group {} is reserved for cluster metadata and cannot be assigned to a tablet",
        .0.0
    )]
    MetadataRaftGroupAssignedToTablet(RaftGroupId),

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

    #[error("metadata {0} allocator is below its reserved identity floor")]
    AllocatorBelowReservedFloor(&'static str),

    #[error("metadata {kind} allocator high-water {high_water} is below visible ID {visible}")]
    AllocatorBelowVisibleState {
        kind: &'static str,
        high_water: u64,
        visible: u64,
    },

    #[error("uninitialized metadata snapshot contains replicated state")]
    UninitializedSnapshotHasState,

    #[error("uninitialized metadata snapshot contains a cluster ID")]
    UninitializedSnapshotHasClusterId,

    #[error("metadata snapshot field {0} is not in canonical ascending order")]
    NonCanonicalSnapshot(&'static str),

    #[error("metadata cached outcome is invalid: {0}")]
    InvalidCachedOutcome(&'static str),
}
