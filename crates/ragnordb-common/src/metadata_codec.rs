//! Durable codec and domain types for metadata Raft commands and snapshots.
//!
//! The metadata log contains desired topology and catalog state only.
//! Transient leader observations and each Raft group's committed `ConfState`
//! have separate authorities and are intentionally absent from this format.

use std::{collections::BTreeSet, net::SocketAddr};

use prost::Message;

use crate::{
    catalog_codec::{ColumnDefinition, TableDefinition},
    ids::{
        ClientRequestId, ColumnId, CommandKind, LogicalCommandId, NodeId, RaftGroupId, ReplicaId,
        RequestId, TableId, TabletId,
    },
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

    /// Allocate or re-establish a durable client retry session. A zero
    /// requested epoch asks metadata to allocate the next epoch for the
    /// client; non-zero values are accepted only when no conflicting session
    /// already exists.
    RegisterClient {
        client_id: u128,
        requested_session_epoch: u64,
    },

    /// Advance the durable acknowledgement floor for one client session.
    RenewClient {
        client_id: u128,
        session_epoch: u64,
        acknowledged_through: u64,
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

    /// Durable metadata proof that a Raft replica lifetime was removed from
    /// its group. This never replaces the Raft configuration entry; it is the
    /// second independent proof required before local destruction.
    RecordReplicaRetirement {
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
        desired_configuration_epoch: u64,
        removed_conf_state_version: u64,
        removal_index: u64,
        removal_term: u64,
    },

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
    /// Proposal/RPC correlation identity. This field is not the durable
    /// metadata deduplication key because it is scoped to a Raft group and
    /// does not carry a client session epoch.
    pub request_id: RequestId,
    /// Topology-independent identity retained by metadata state and snapshots.
    pub logical_command_id: LogicalCommandId,
    pub command: MetadataCommand,
}

impl MetadataCommandEnvelope {
    pub fn new(
        request_id: RequestId,
        command: MetadataCommand,
    ) -> Result<Self, MetadataCommandCodecError> {
        let logical_command_id = compatibility_metadata_logical_id(&request_id);
        Self::new_with_logical_command_id(request_id, logical_command_id, command)
    }

    pub fn new_with_logical_command_id(
        request_id: RequestId,
        logical_command_id: LogicalCommandId,
        command: MetadataCommand,
    ) -> Result<Self, MetadataCommandCodecError> {
        let envelope = Self {
            format_version: METADATA_COMMAND_ENVELOPE_VERSION,
            request_id,
            logical_command_id,
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

        self.logical_command_id
            .validate()
            .map_err(MetadataCommandCodecError::InvalidLogicalCommandId)?;

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
        proto.logical_command_id = Some(self.logical_command_id.to_proto());
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

        let logical_command_id = proto
            .logical_command_id
            .clone()
            .map(LogicalCommandId::from_proto)
            .transpose()
            .map_err(MetadataCommandCodecError::InvalidLogicalCommandId)?
            .unwrap_or_else(|| compatibility_metadata_logical_id(&request_id));

        let command = MetadataCommand::from_proto(proto)?;
        Self::new_with_logical_command_id(request_id, logical_command_id, command)
    }
}

/// Result retained for one request identity in the replicated metadata state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataCachedOutcome {
    Applied,
    AlreadyApplied,
    ClientRegistered {
        client_id: u128,
        session_epoch: u64,
    },
    ClientRenewed,
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
    pub logical_command_id: LogicalCommandId,
    pub outcome: MetadataCachedOutcome,
}

/// Durable retry-session state owned by metadata Raft.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataClientSession {
    pub client_id: u128,
    pub session_epoch: u64,
    pub acknowledged_through: u64,
    pub first_retained_sequence: u64,
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
    pub region: Option<String>,
    pub zone: Option<String>,
    pub rack: Option<String>,
    pub storage_class: String,
    pub lifecycle: NodeLifecycle,
}

/// Durable node lifecycle owned by metadata. Local replica lifecycle is a
/// separate resource-cleanup state and must not be used as a placement signal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NodeLifecycle {
    #[default]
    Active,
    Draining,
    Decommissioning,
    Decommissioned,
    Tombstoned,
}

/// V1 partition identity.
///
/// Phase 5.3 will consume this metadata for actual routing. Defining it here
/// prevents routing from inventing another, non-replicated tablet map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionSpec {
    Hash {
        bucket: u32,
        bucket_count: u32,
    },
    /// Ordered half-open range. An empty start/end is an unbounded side.
    Range {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
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

/// Durable placement constraints for one tablet's desired membership.
///
/// The policy describes the safety properties metadata expects the
/// reconciliation worker to preserve. It is intentionally separate from the
/// currently committed Raft configuration: a group may temporarily contain
/// extra learners while a safe replacement is being brought up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementPolicy {
    pub replication_factor: u32,
    pub min_distinct_regions: u32,
    pub min_distinct_zones: u32,
    pub min_distinct_racks: u32,
    pub required_storage_class: Option<String>,
    pub preferred_leader_nodes: Vec<NodeId>,
}

impl Default for PlacementPolicy {
    fn default() -> Self {
        Self {
            replication_factor: 1,
            min_distinct_regions: 0,
            min_distinct_zones: 0,
            min_distinct_racks: 0,
            required_storage_class: None,
            preferred_leader_nodes: Vec::new(),
        }
    }
}

impl PlacementPolicy {
    pub fn for_replica_count(replication_factor: usize) -> Self {
        Self {
            replication_factor: replication_factor as u32,
            ..Self::default()
        }
    }

    fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        if self.replication_factor == 0 {
            return Err(MetadataCommandCodecError::ZeroReplicationFactor);
        }

        for (minimum, label) in [
            (self.min_distinct_regions, "regions"),
            (self.min_distinct_zones, "zones"),
            (self.min_distinct_racks, "racks"),
        ] {
            if minimum > self.replication_factor {
                return Err(
                    MetadataCommandCodecError::PlacementDomainCountExceedsReplication {
                        domain: label,
                        count: minimum,
                        replication_factor: self.replication_factor,
                    },
                );
            }
        }

        if self
            .required_storage_class
            .as_ref()
            .is_some_and(|storage_class| storage_class.trim().is_empty())
        {
            return Err(MetadataCommandCodecError::EmptyRequiredStorageClass);
        }

        let mut preferred_nodes = BTreeSet::new();
        for node_id in &self.preferred_leader_nodes {
            if node_id.0 == 0 || !preferred_nodes.insert(*node_id) {
                return Err(MetadataCommandCodecError::InvalidLeaderPreference);
            }
        }

        Ok(())
    }
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
    pub placement_policy: PlacementPolicy,
}

/// Permanent record that one group-local replica lifetime ended.
///
/// The pair is required because ReplicaId is scoped to one Raft group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RetiredReplicaLifetime {
    pub raft_group_id: RaftGroupId,
    pub replica_id: ReplicaId,
    pub desired_configuration_epoch: u64,
    pub removed_conf_state_version: u64,
    pub removal_index: u64,
    pub removal_term: u64,
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
    pub client_sessions: Vec<MetadataClientSession>,
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
        let (request_id, _logical_command_id, command) = Self::decode_with_request_identity(bytes)?;
        Ok((request_id, command))
    }

    /// Decode a metadata command while preserving both the legacy proposal
    /// correlation identity and the topology-independent durable identity.
    pub fn decode_with_request_identity(
        bytes: &[u8],
    ) -> Result<(Option<RequestId>, Option<LogicalCommandId>, Self), MetadataCommandCodecError>
    {
        let proto = metadata::MetadataCommand::decode(bytes)
            .map_err(|error| MetadataCommandCodecError::Decode(error.to_string()))?;

        let request_id = proto
            .request_id
            .clone()
            .map(RequestId::from_proto)
            .transpose()
            .map_err(MetadataCommandCodecError::InvalidRequestId)?;

        let logical_command_id = proto
            .logical_command_id
            .clone()
            .map(LogicalCommandId::from_proto)
            .transpose()
            .map_err(MetadataCommandCodecError::InvalidLogicalCommandId)?
            .or_else(|| request_id.as_ref().map(compatibility_metadata_logical_id));

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
                logical_command_id: logical_command_id
                    .expect("request identity exists when validating an envelope"),
                command: command.clone(),
            }
            .validate()?;
        }

        Ok((request_id, logical_command_id, command))
    }

    pub fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        match self {
            Self::ClusterInitialized { cluster_id } => validate_cluster_id(cluster_id),

            Self::RegisterClient {
                client_id,
                requested_session_epoch,
            } => {
                validate_client_id(*client_id)?;
                if *requested_session_epoch == 0 {
                    // Zero is the explicit "allocate the next epoch" value.
                    Ok(())
                } else {
                    Ok(())
                }
            }

            Self::RenewClient {
                client_id,
                session_epoch,
                ..
            } => {
                validate_client_id(*client_id)?;
                if *session_epoch == 0 {
                    return Err(MetadataCommandCodecError::ZeroClientSessionEpoch);
                }
                Ok(())
            }

            Self::RegisterNode(node) => node.validate(),

            Self::CreateTable { table } => validate_table(table),

            Self::CreateTablet { tablet } => tablet.validate(),

            Self::CreateTableTopology(request) => request.validate(),

            Self::SetDesiredReplicaPlacement(placement) => placement.validate(),

            Self::RecordReplicaRetirement {
                raft_group_id,
                replica_id,
                desired_configuration_epoch,
                removed_conf_state_version,
                removal_index,
                removal_term,
            } => validate_replica_retirement(
                *raft_group_id,
                *replica_id,
                *desired_configuration_epoch,
                *removed_conf_state_version,
                *removal_index,
                *removal_term,
            ),

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

            Self::RegisterClient {
                client_id,
                requested_session_epoch,
            } => Command::RegisterClient(metadata::RegisterClient {
                client_id: client_id.to_le_bytes().to_vec(),
                requested_session_epoch: *requested_session_epoch,
            }),

            Self::RenewClient {
                client_id,
                session_epoch,
                acknowledged_through,
            } => Command::RenewClient(metadata::RenewClient {
                client_id: client_id.to_le_bytes().to_vec(),
                session_epoch: *session_epoch,
                acknowledged_through: *acknowledged_through,
            }),

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

            Self::RecordReplicaRetirement {
                raft_group_id,
                replica_id,
                desired_configuration_epoch,
                removed_conf_state_version,
                removal_index,
                removal_term,
            } => Command::RecordReplicaRetirement(metadata::RecordReplicaRetirement {
                raft_group_id: Some(raft_group_id.to_proto()),
                replica_id: Some(replica_id.to_proto()),
                desired_configuration_epoch: *desired_configuration_epoch,
                removed_conf_state_version: *removed_conf_state_version,
                removal_index: *removal_index,
                removal_term: *removal_term,
            }),

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
            logical_command_id: None,
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

            Some(Command::RegisterClient(command)) => Self::RegisterClient {
                client_id: decode_client_id(&command.client_id)?,
                requested_session_epoch: command.requested_session_epoch,
            },

            Some(Command::RenewClient(command)) => Self::RenewClient {
                client_id: decode_client_id(&command.client_id)?,
                session_epoch: command.session_epoch,
                acknowledged_through: command.acknowledged_through,
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

            Some(Command::RecordReplicaRetirement(command)) => Self::RecordReplicaRetirement {
                raft_group_id: RaftGroupId::from_proto(command.raft_group_id.ok_or(
                    MetadataCommandCodecError::MissingField(
                        "record_replica_retirement.raft_group_id",
                    ),
                )?),
                replica_id: ReplicaId::from_proto(command.replica_id.ok_or(
                    MetadataCommandCodecError::MissingField("record_replica_retirement.replica_id"),
                )?),
                desired_configuration_epoch: command.desired_configuration_epoch,
                removed_conf_state_version: command.removed_conf_state_version,
                removal_index: command.removal_index,
                removal_term: command.removal_term,
            },

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

        if self.storage_class.trim().is_empty() {
            return Err(MetadataCommandCodecError::EmptyNodeStorageClass);
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
            region: self.region.clone().unwrap_or_default(),
            zone: self.zone.clone().unwrap_or_default(),
            rack: self.rack.clone().unwrap_or_default(),
            storage_class: self.storage_class.clone(),
            lifecycle: match self.lifecycle {
                NodeLifecycle::Active => metadata::NodeLifecycle::Active,
                NodeLifecycle::Draining => metadata::NodeLifecycle::Draining,
                NodeLifecycle::Decommissioning => metadata::NodeLifecycle::Decommissioning,
                NodeLifecycle::Decommissioned => metadata::NodeLifecycle::Decommissioned,
                NodeLifecycle::Tombstoned => metadata::NodeLifecycle::Tombstoned,
            } as i32,
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
            region: (!proto.region.is_empty()).then_some(proto.region),
            zone: (!proto.zone.is_empty()).then_some(proto.zone),
            rack: (!proto.rack.is_empty()).then_some(proto.rack),
            storage_class: if proto.storage_class.is_empty() {
                "default".to_string()
            } else {
                proto.storage_class
            },
            lifecycle: match metadata::NodeLifecycle::try_from(proto.lifecycle) {
                Ok(metadata::NodeLifecycle::Active) | Ok(metadata::NodeLifecycle::Unspecified) => {
                    NodeLifecycle::Active
                }
                Ok(metadata::NodeLifecycle::Draining) => NodeLifecycle::Draining,
                Ok(metadata::NodeLifecycle::Decommissioning) => NodeLifecycle::Decommissioning,
                Ok(metadata::NodeLifecycle::Decommissioned) => NodeLifecycle::Decommissioned,
                Ok(metadata::NodeLifecycle::Tombstoned) => NodeLifecycle::Tombstoned,
                Err(_) => return Err(MetadataCommandCodecError::InvalidNodeLifecycle),
            },
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
            Self::Range { start_key, end_key } => {
                if !end_key.is_empty() && start_key >= end_key {
                    return Err(MetadataCommandCodecError::InvalidKeyRange);
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
            Self::Range { start_key, end_key } => Kind::Range(metadata::RangePartition {
                start_key: start_key.clone(),
                end_key: end_key.clone(),
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

            Some(Kind::Range(range)) => Self::Range {
                start_key: range.start_key,
                end_key: range.end_key,
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
        self.placement_policy.validate()?;

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

        if voter_count != self.placement_policy.replication_factor as usize {
            return Err(MetadataCommandCodecError::ReplicationFactorMismatch {
                expected: self.placement_policy.replication_factor,
                received: voter_count as u32,
            });
        }

        Ok(())
    }

    fn to_proto(&self) -> metadata::SetDesiredReplicaPlacement {
        metadata::SetDesiredReplicaPlacement {
            tablet_id: Some(self.tablet_id.to_proto()),

            configuration_epoch: self.configuration_epoch,

            placement_policy: Some(metadata::PlacementPolicy {
                replication_factor: self.placement_policy.replication_factor,
                min_distinct_regions: self.placement_policy.min_distinct_regions,
                min_distinct_zones: self.placement_policy.min_distinct_zones,
                min_distinct_racks: self.placement_policy.min_distinct_racks,
                required_storage_class: self
                    .placement_policy
                    .required_storage_class
                    .clone()
                    .unwrap_or_default(),
                preferred_leader_nodes: self
                    .placement_policy
                    .preferred_leader_nodes
                    .iter()
                    .map(|node_id| node_id.to_proto())
                    .collect(),
            }),

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
        let replicas = proto
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

                        Ok(metadata::DesiredReplicaRole::Learner) => DesiredReplicaRole::Learner,

                        _ => {
                            return Err(MetadataCommandCodecError::InvalidReplicaRole);
                        }
                    },
                })
            })
            .collect::<Result<Vec<_>, MetadataCommandCodecError>>()?;

        let voter_count = replicas
            .iter()
            .filter(|replica| replica.role == DesiredReplicaRole::Voter)
            .count() as u32;
        let placement_policy = proto
            .placement_policy
            .map(|policy| PlacementPolicy {
                replication_factor: if policy.replication_factor == 0 {
                    voter_count
                } else {
                    policy.replication_factor
                },
                min_distinct_regions: policy.min_distinct_regions,
                min_distinct_zones: policy.min_distinct_zones,
                min_distinct_racks: policy.min_distinct_racks,
                required_storage_class: (!policy.required_storage_class.is_empty())
                    .then_some(policy.required_storage_class),
                preferred_leader_nodes: policy
                    .preferred_leader_nodes
                    .into_iter()
                    .map(NodeId::from_proto)
                    .collect(),
            })
            .unwrap_or_else(|| PlacementPolicy::for_replica_count(voter_count as usize));

        let placement = Self {
            tablet_id: TabletId::from_proto(proto.tablet_id.ok_or(
                MetadataCommandCodecError::MissingField("desired_placement.tablet_id"),
            )?),

            configuration_epoch: proto.configuration_epoch,
            replicas,
            placement_policy,
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

        validate_replica_retirement_fields(
            self.desired_configuration_epoch,
            self.removed_conf_state_version,
            self.removal_index,
            self.removal_term,
        )?;

        Ok(())
    }

    fn to_proto(self) -> metadata::RetiredReplicaLifetime {
        metadata::RetiredReplicaLifetime {
            raft_group_id: Some(self.raft_group_id.to_proto()),
            replica_id: Some(self.replica_id.to_proto()),
            desired_configuration_epoch: self.desired_configuration_epoch,
            removed_conf_state_version: self.removed_conf_state_version,
            removal_index: self.removal_index,
            removal_term: self.removal_term,
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
            desired_configuration_epoch: proto.desired_configuration_epoch,
            removed_conf_state_version: proto.removed_conf_state_version,
            removal_index: proto.removal_index,
            removal_term: proto.removal_term,
        };

        value.validate()?;

        Ok(value)
    }
}

fn validate_replica_retirement(
    raft_group_id: RaftGroupId,
    replica_id: ReplicaId,
    desired_configuration_epoch: u64,
    removed_conf_state_version: u64,
    removal_index: u64,
    removal_term: u64,
) -> Result<(), MetadataCommandCodecError> {
    if raft_group_id.0 == 0 {
        return Err(MetadataCommandCodecError::ZeroRaftGroupId);
    }
    if replica_id.0 == 0 {
        return Err(MetadataCommandCodecError::ZeroReplicaId);
    }
    validate_replica_retirement_fields(
        desired_configuration_epoch,
        removed_conf_state_version,
        removal_index,
        removal_term,
    )
}

fn validate_replica_retirement_fields(
    desired_configuration_epoch: u64,
    removed_conf_state_version: u64,
    removal_index: u64,
    removal_term: u64,
) -> Result<(), MetadataCommandCodecError> {
    if desired_configuration_epoch == 0 {
        return Err(MetadataCommandCodecError::ZeroConfigurationEpoch);
    }
    if removed_conf_state_version == 0 {
        return Err(MetadataCommandCodecError::ZeroConfStateVersion);
    }
    if removal_index == 0 {
        return Err(MetadataCommandCodecError::ZeroRemovalIndex);
    }
    if removal_term == 0 {
        return Err(MetadataCommandCodecError::ZeroRemovalTerm);
    }
    Ok(())
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
                    || !self.client_sessions.is_empty()
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

        for client_session in &self.client_sessions {
            client_session.validate()?;
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
            request.logical_command_id
        }) {
            return Err(MetadataCommandCodecError::NonCanonicalSnapshot(
                "request_deduplication",
            ));
        }

        if !strictly_ascending(&self.client_sessions, |session| session.client_id) {
            return Err(MetadataCommandCodecError::NonCanonicalSnapshot(
                "client_sessions",
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

            client_sessions: self
                .client_sessions
                .iter()
                .map(MetadataClientSession::to_proto)
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
            client_sessions,
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

        let client_sessions = client_sessions
            .into_iter()
            .map(MetadataClientSession::from_proto)
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
            client_sessions,
        };

        snapshot.validate()?;

        Ok(snapshot)
    }
}

impl MetadataClientSession {
    fn validate(&self) -> Result<(), MetadataCommandCodecError> {
        validate_client_id(self.client_id)?;
        if self.session_epoch == 0 {
            return Err(MetadataCommandCodecError::ZeroClientSessionEpoch);
        }
        if self.first_retained_sequence == 0 {
            return Err(MetadataCommandCodecError::ZeroRetryHorizon);
        }
        if self.first_retained_sequence <= self.acknowledged_through {
            return Err(MetadataCommandCodecError::InvalidRetryHorizon);
        }
        Ok(())
    }

    fn to_proto(&self) -> metadata::ClientSession {
        metadata::ClientSession {
            client_id: self.client_id.to_le_bytes().to_vec(),
            session_epoch: self.session_epoch,
            acknowledged_through: self.acknowledged_through,
            first_retained_sequence: self.first_retained_sequence,
        }
    }

    fn from_proto(proto: metadata::ClientSession) -> Result<Self, MetadataCommandCodecError> {
        let session = Self {
            client_id: decode_client_id(&proto.client_id)?,
            session_epoch: proto.session_epoch,
            acknowledged_through: proto.acknowledged_through,
            first_retained_sequence: proto.first_retained_sequence,
        };
        session.validate()?;
        Ok(session)
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
            MetadataCachedOutcome::Applied
            | MetadataCachedOutcome::AlreadyApplied
            | MetadataCachedOutcome::ClientRenewed => {}

            MetadataCachedOutcome::ClientRegistered {
                client_id,
                session_epoch,
            } => {
                validate_client_id(*client_id)?;
                if *session_epoch == 0 {
                    return Err(MetadataCommandCodecError::ZeroClientSessionEpoch);
                }
            }

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
        let (outcome_kind, table_id, tablet_id, raft_group_id, rejection, client_id, session_epoch) =
            match &self.outcome {
                MetadataCachedOutcome::Applied => (
                    metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeApplied,
                    0,
                    0,
                    0,
                    String::new(),
                    Vec::new(),
                    0,
                ),
                MetadataCachedOutcome::AlreadyApplied => (
                    metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeAlreadyApplied,
                    0,
                    0,
                    0,
                    String::new(),
                    Vec::new(),
                    0,
                ),
                MetadataCachedOutcome::ClientRegistered {
                    client_id,
                    session_epoch,
                } => (
                    metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeClientRegistered,
                    0,
                    0,
                    0,
                    String::new(),
                    client_id.to_le_bytes().to_vec(),
                    *session_epoch,
                ),
                MetadataCachedOutcome::ClientRenewed => (
                    metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeClientRenewed,
                    0,
                    0,
                    0,
                    String::new(),
                    Vec::new(),
                    0,
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
                    Vec::new(),
                    0,
                ),
                MetadataCachedOutcome::Rejected(reason) => (
                    metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeRejected,
                    0,
                    0,
                    0,
                    reason.clone(),
                    Vec::new(),
                    0,
                ),
            };

        metadata::MetadataRequestDeduplication {
            request_id: Some(self.request_id.to_proto()),
            outcome_kind: outcome_kind as i32,
            table_id,
            tablet_id,
            raft_group_id,
            rejection,
            client_id,
            session_epoch,
            logical_command_id: Some(self.logical_command_id.to_proto()),
        }
    }

    fn from_proto(
        proto: metadata::MetadataRequestDeduplication,
    ) -> Result<Self, MetadataCommandCodecError> {
        let request_id = RequestId::from_proto(proto.request_id.ok_or(
            MetadataCommandCodecError::MissingField("request_deduplication.request_id"),
        )?)
        .map_err(MetadataCommandCodecError::InvalidRequestId)?;

        let logical_command_id = proto
            .logical_command_id
            .clone()
            .map(LogicalCommandId::from_proto)
            .transpose()
            .map_err(MetadataCommandCodecError::InvalidLogicalCommandId)?
            .unwrap_or_else(|| compatibility_metadata_logical_id(&request_id));

        let outcome_kind = metadata::MetadataCachedOutcomeKind::try_from(proto.outcome_kind)
            .map_err(|_| MetadataCommandCodecError::InvalidCachedOutcome("unknown outcome kind"))?;

        let outcome = match outcome_kind {
            metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeApplied => {
                MetadataCachedOutcome::Applied
            }
            metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeAlreadyApplied => {
                MetadataCachedOutcome::AlreadyApplied
            }
            metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeClientRegistered => {
                MetadataCachedOutcome::ClientRegistered {
                    client_id: decode_client_id(&proto.client_id)?,
                    session_epoch: proto.session_epoch,
                }
            }
            metadata::MetadataCachedOutcomeKind::MetadataCachedOutcomeClientRenewed => {
                MetadataCachedOutcome::ClientRenewed
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
            logical_command_id,
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

fn validate_client_id(client_id: u128) -> Result<(), MetadataCommandCodecError> {
    if client_id == 0 {
        return Err(MetadataCommandCodecError::ZeroClientId);
    }
    Ok(())
}

/// Preserve decode compatibility for pre-V2 metadata entries that carried
/// only a group-qualified RequestId. New proposals always supply the real
/// session-bearing logical identity through the envelope constructor.
fn compatibility_metadata_logical_id(request_id: &RequestId) -> LogicalCommandId {
    LogicalCommandId {
        client_request_id: ClientRequestId {
            client_id: request_id.client_id,
            session_epoch: 1,
            request_sequence: request_id.sequence,
        },
        command_ordinal: 1,
        kind: CommandKind::Catalog,
    }
}

fn decode_client_id(bytes: &[u8]) -> Result<u128, MetadataCommandCodecError> {
    if bytes.len() != 16 {
        return Err(MetadataCommandCodecError::InvalidClientId);
    }
    let client_id = u128::from_le_bytes(
        bytes
            .try_into()
            .map_err(|_| MetadataCommandCodecError::InvalidClientId)?,
    );
    validate_client_id(client_id)?;
    Ok(client_id)
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

    #[error("invalid metadata logical command ID: {0}")]
    InvalidLogicalCommandId(&'static str),

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

    #[error("metadata retirement ConfState version must be non-zero")]
    ZeroConfStateVersion,

    #[error("metadata retirement removal index must be non-zero")]
    ZeroRemovalIndex,

    #[error("metadata retirement removal term must be non-zero")]
    ZeroRemovalTerm,

    #[error("metadata endpoint {0} cannot be empty")]
    EmptyNodeEndpoint(&'static str),

    #[error("metadata endpoint {field} is not a valid socket address: {value}")]
    InvalidSocketAddress { field: &'static str, value: String },

    #[error("a physical node cannot bind multiple services to metadata endpoint {0}")]
    DuplicateNodeEndpoint(String),

    #[error("metadata node storage class cannot be empty")]
    EmptyNodeStorageClass,

    #[error("metadata node lifecycle is invalid or unspecified")]
    InvalidNodeLifecycle,

    #[error("placement policy contains a duplicate or zero preferred leader node")]
    InvalidLeaderPreference,

    #[error("metadata client ID must be non-zero")]
    ZeroClientId,

    #[error("metadata client ID must be exactly 16 bytes")]
    InvalidClientId,

    #[error("metadata client session epoch must be non-zero")]
    ZeroClientSessionEpoch,

    #[error("metadata retry horizon must retain at least one sequence")]
    ZeroRetryHorizon,

    #[error("metadata retry horizon is inconsistent")]
    InvalidRetryHorizon,

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

    #[error("metadata tablet key range must have start < end")]
    InvalidKeyRange,

    #[error("metadata configuration epoch must be non-zero")]
    ZeroConfigurationEpoch,

    #[error("metadata replica placement cannot be empty")]
    EmptyReplicaPlacement,

    #[error("metadata desired placement must contain at least one voter")]
    PlacementHasNoVoter,

    #[error("metadata placement replication factor must be non-zero")]
    ZeroReplicationFactor,

    #[error(
        "metadata placement requires {count} distinct {domain}, but replication factor is {replication_factor}"
    )]
    PlacementDomainCountExceedsReplication {
        domain: &'static str,
        count: u32,
        replication_factor: u32,
    },

    #[error("metadata placement required storage class cannot be empty")]
    EmptyRequiredStorageClass,

    #[error("metadata placement contains {received} voters, but policy requires {expected}")]
    ReplicationFactorMismatch { expected: u32, received: u32 },

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
