use std::collections::{BTreeMap, BTreeSet};

use super::command_codec::TabletCommand;
use crate::ids::{
    LogicalCommandId, NodeId, RaftGroupId, ReplicaId, RequestId, TabletId, Timestamp,
};
use crate::proto::rpc;

/// A logical inter-node message on the multiplexed TCP transport.
///
/// The physical transport wraps this value in the versioned frame
/// `[version:u8][msg_type:u8][raft_group_id:u64][len:u32 LE]` before appending
/// the payload. Keeping the logical payload separate lets Raft continue using
/// its existing envelope codec while RPC messages carry typed protobuf bytes.
///
/// msg_type determines how the payload is decoded:
///   0x01 — Raft consensus message (AppendEntries, Vote, etc.)
///   0x02 — TabletCommandRequest
///   0x03 — TabletCommandResponse
///   0x04 — MetadataRequest
///   0x05 — MetadataResponse
///   0x06 — TabletReadRequest
///   0x07 — TabletOutcomeQueryRequest
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcFrame {
    pub msg_type: MessageType,
    pub raft_group_id: RaftGroupId,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    RaftConsensus,
    TabletCommandRequest,
    TabletCommandResponse,
    MetadataRequest,
    MetadataResponse,
    TabletReadRequest,
    TabletOutcomeQueryRequest,
}

impl MessageType {
    /// Return the stable one-byte discriminator used by the physical
    /// multiplexed transport. Keeping this mapping beside the protobuf
    /// mapping prevents a new enum variant from silently acquiring a wire
    /// value that the listener does not understand.
    pub const fn wire_value(self) -> u8 {
        match self {
            Self::RaftConsensus => 0x01,
            Self::TabletCommandRequest => 0x02,
            Self::TabletCommandResponse => 0x03,
            Self::MetadataRequest => 0x04,
            Self::MetadataResponse => 0x05,
            Self::TabletReadRequest => 0x06,
            Self::TabletOutcomeQueryRequest => 0x07,
        }
    }

    pub fn from_wire_value(value: u8) -> Result<Self, &'static str> {
        match value {
            0x01 => Ok(Self::RaftConsensus),
            0x02 => Ok(Self::TabletCommandRequest),
            0x03 => Ok(Self::TabletCommandResponse),
            0x04 => Ok(Self::MetadataRequest),
            0x05 => Ok(Self::MetadataResponse),
            0x06 => Ok(Self::TabletReadRequest),
            0x07 => Ok(Self::TabletOutcomeQueryRequest),
            _ => Err("unknown RPC message type"),
        }
    }

    pub fn to_proto(&self) -> rpc::MessageType {
        match self {
            MessageType::RaftConsensus => rpc::MessageType::RaftConsensus,
            MessageType::TabletCommandRequest => rpc::MessageType::TabletCommandRequest,
            MessageType::TabletCommandResponse => rpc::MessageType::TabletCommandResponse,
            MessageType::MetadataRequest => rpc::MessageType::MetadataRequest,
            MessageType::MetadataResponse => rpc::MessageType::MetadataResponse,
            MessageType::TabletReadRequest => rpc::MessageType::TabletReadRequest,
            MessageType::TabletOutcomeQueryRequest => rpc::MessageType::TabletOutcomeQueryRequest,
        }
    }

    pub fn from_proto(proto: rpc::MessageType) -> Result<Self, &'static str> {
        match proto {
            rpc::MessageType::RaftConsensus => Ok(MessageType::RaftConsensus),
            rpc::MessageType::TabletCommandRequest => Ok(MessageType::TabletCommandRequest),
            rpc::MessageType::TabletCommandResponse => Ok(MessageType::TabletCommandResponse),
            rpc::MessageType::MetadataRequest => Ok(MessageType::MetadataRequest),
            rpc::MessageType::MetadataResponse => Ok(MessageType::MetadataResponse),
            rpc::MessageType::TabletReadRequest => Ok(MessageType::TabletReadRequest),
            rpc::MessageType::TabletOutcomeQueryRequest => {
                Ok(MessageType::TabletOutcomeQueryRequest)
            }
            rpc::MessageType::Unspecified => Err("unspecified message type"),
        }
    }
}

impl RpcFrame {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.raft_group_id.0 == 0 {
            return Err("RPC Raft group ID must be non-zero");
        }
        if self.payload.is_empty() {
            return Err("RPC payload must be non-empty");
        }
        Ok(())
    }

    pub fn to_proto(&self) -> rpc::RpcFrame {
        rpc::RpcFrame {
            msg_type: self.msg_type.to_proto() as i32,
            raft_group_id: Some(self.raft_group_id.to_proto()),
            payload: self.payload.clone(),
        }
    }

    pub fn from_proto(proto: rpc::RpcFrame) -> Result<Self, &'static str> {
        let frame = RpcFrame {
            msg_type: MessageType::from_proto(
                rpc::MessageType::try_from(proto.msg_type).map_err(|_| "invalid msg_type")?,
            )?,
            raft_group_id: RaftGroupId::from_proto(
                proto.raft_group_id.ok_or("missing raft_group_id")?,
            ),
            payload: proto.payload,
        };
        frame.validate()?;
        Ok(frame)
    }
}

/// this is a tablet command request sent from a gateway to a tablet leader
/// it includes the RequestId for idempotent retry deduplication
pub struct TabletCommandRequest {
    pub request_id: RequestId,
    pub logical_command_id: Option<LogicalCommandId>,
    pub acknowledged_through: Option<u64>,
    pub tablet_id: TabletId,
    pub tablet_epoch: u64,
    pub command: TabletCommand,
}

impl TabletCommandRequest {
    pub fn to_proto(&self) -> Result<rpc::TabletCommandRequest, &'static str> {
        Ok(rpc::TabletCommandRequest {
            request_id: Some(self.request_id.to_proto()),
            logical_command_id: self.logical_command_id.map(|id| id.to_proto()),
            acknowledged_through: self.acknowledged_through,
            tablet_id: Some(self.tablet_id.to_proto()),
            tablet_epoch: self.tablet_epoch,
            command: Some(self.command.to_proto()?),
        })
    }

    pub fn from_proto(proto: rpc::TabletCommandRequest) -> Result<Self, &'static str> {
        let request_id = RequestId::from_proto(proto.request_id.ok_or("missing request_id")?)?;
        let tablet_id = TabletId::from_proto(proto.tablet_id.ok_or("missing tablet_id")?);
        if request_id.client_id == 0
            || request_id.sequence == 0
            || request_id.raft_group_id.0 == 0
            || tablet_id.0 == 0
            || proto.tablet_epoch == 0
        {
            return Err("tablet command request contains a reserved zero identity");
        }

        Ok(TabletCommandRequest {
            request_id,
            logical_command_id: proto
                .logical_command_id
                .map(LogicalCommandId::from_proto)
                .transpose()?,
            acknowledged_through: proto.acknowledged_through,
            tablet_id,
            tablet_epoch: proto.tablet_epoch,
            command: TabletCommand::from_proto(proto.command.ok_or("missing command")?)?,
        })
    }
}

/// Read-only query for the durable result of a topology-independent mutation.
/// The request ID is transport correlation only; the logical command ID is the
/// identity whose outcome is being inspected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletOutcomeQueryRequest {
    pub request_id: RequestId,
    pub logical_command_id: LogicalCommandId,
    pub tablet_id: TabletId,
    pub tablet_epoch: u64,
}

impl TabletOutcomeQueryRequest {
    pub fn to_proto(&self) -> rpc::TabletOutcomeQueryRequest {
        rpc::TabletOutcomeQueryRequest {
            request_id: Some(self.request_id.to_proto()),
            logical_command_id: Some(self.logical_command_id.to_proto()),
            tablet_id: Some(self.tablet_id.to_proto()),
            tablet_epoch: self.tablet_epoch,
        }
    }

    pub fn from_proto(proto: rpc::TabletOutcomeQueryRequest) -> Result<Self, &'static str> {
        let request_id = RequestId::from_proto(proto.request_id.ok_or("missing request_id")?)?;
        let logical_command_id = LogicalCommandId::from_proto(
            proto
                .logical_command_id
                .ok_or("missing logical_command_id")?,
        )?;
        let tablet_id = TabletId::from_proto(proto.tablet_id.ok_or("missing tablet_id")?);
        if request_id.client_id == 0
            || request_id.sequence == 0
            || request_id.raft_group_id.0 == 0
            || tablet_id.0 == 0
            || proto.tablet_epoch == 0
        {
            return Err("tablet outcome query contains a reserved zero identity");
        }
        logical_command_id.validate()?;

        Ok(Self {
            request_id,
            logical_command_id,
            tablet_id,
            tablet_epoch: proto.tablet_epoch,
        })
    }
}

/// Point read issued by a gateway after metadata has selected one tablet.
///
/// The row key and tablet generation are carried together so a delayed request
/// cannot be interpreted against a replacement tablet that reused the same
/// physical route. The read timestamp is an MVCC snapshot boundary and is
/// never allocated by the tablet itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletReadRequest {
    pub request_id: RequestId,
    pub logical_command_id: Option<LogicalCommandId>,
    pub tablet_id: TabletId,
    pub tablet_epoch: u64,
    pub row_key: crate::ids::RowKey,
    pub read_timestamp: Timestamp,
}

impl TabletReadRequest {
    pub fn to_proto(&self) -> rpc::TabletReadRequest {
        rpc::TabletReadRequest {
            request_id: Some(self.request_id.to_proto()),
            logical_command_id: self.logical_command_id.map(|id| id.to_proto()),
            tablet_id: Some(self.tablet_id.to_proto()),
            tablet_epoch: self.tablet_epoch,
            row_key: Some(self.row_key.to_proto()),
            read_timestamp: Some(self.read_timestamp.to_proto()),
        }
    }

    pub fn from_proto(proto: rpc::TabletReadRequest) -> Result<Self, &'static str> {
        let request_id = RequestId::from_proto(proto.request_id.ok_or("missing request_id")?)?;
        let tablet_id = TabletId::from_proto(proto.tablet_id.ok_or("missing tablet_id")?);
        let row_key = crate::ids::RowKey::from_proto(proto.row_key.ok_or("missing row_key")?)?;
        let read_timestamp =
            Timestamp::from_proto(proto.read_timestamp.ok_or("missing read_timestamp")?);

        if request_id.client_id == 0
            || request_id.sequence == 0
            || request_id.raft_group_id.0 == 0
            || tablet_id.0 == 0
            || proto.tablet_epoch == 0
            || row_key.table_id.0 == 0
        {
            return Err("tablet read request contains a reserved zero identity");
        }
        if read_timestamp.0 == 0 {
            return Err("tablet read timestamp must be non-zero");
        }

        Ok(Self {
            request_id,
            logical_command_id: proto
                .logical_command_id
                .map(LogicalCommandId::from_proto)
                .transpose()?,
            tablet_id,
            tablet_epoch: proto.tablet_epoch,
            row_key,
            read_timestamp,
        })
    }
}

/// The response from a tablet after applying a command.
///
/// Fields match the V1 client wire protocol's error taxonomy:
///   success       — whether the command succeeded
///   error_message — human-readable description
///   error_code    — canonical error code (WRITE_CONFLICT, NOT_LEADER, etc.)
///   retryable     — whether the client can retry with the same RequestId
///   result_data   — output data (e.g., row bytes for a read)
#[derive(Debug, Clone, PartialEq)]
pub struct TabletCommandResponse {
    pub request_id: RequestId,
    pub success: bool,
    pub error_message: String,
    pub error_code: String,
    pub retryable: bool,
    pub result_data: Vec<u8>,
    pub found: bool,
    pub leader_replica_id: Option<ReplicaId>,
    pub current_tablet_epoch: Option<u64>,
    pub expected_tablet_epoch: Option<u64>,
}

impl TabletCommandResponse {
    pub fn to_proto(&self) -> rpc::TabletCommandResponse {
        rpc::TabletCommandResponse {
            request_id: Some(self.request_id.to_proto()),
            success: self.success,
            error_message: self.error_message.clone(),
            error_code: self.error_code.clone(),
            retryable: self.retryable,
            result_data: self.result_data.clone(),
            found: self.found,
            leader_replica_id: self.leader_replica_id.map(|id| id.0).unwrap_or(0),
            current_tablet_epoch: self.current_tablet_epoch.unwrap_or(0),
            expected_tablet_epoch: self.expected_tablet_epoch.unwrap_or(0),
        }
    }

    pub fn from_proto(proto: rpc::TabletCommandResponse) -> Result<Self, &'static str> {
        Ok(TabletCommandResponse {
            request_id: RequestId::from_proto(proto.request_id.ok_or("missing request_id")?)?,
            success: proto.success,
            error_message: proto.error_message,
            error_code: proto.error_code,
            retryable: proto.retryable,
            result_data: proto.result_data,
            found: proto.found,
            leader_replica_id: (proto.leader_replica_id != 0)
                .then_some(ReplicaId(proto.leader_replica_id)),
            current_tablet_epoch: (proto.current_tablet_epoch != 0)
                .then_some(proto.current_tablet_epoch),
            expected_tablet_epoch: (proto.expected_tablet_epoch != 0)
                .then_some(proto.expected_tablet_epoch),
        })
    }
}

/// Requests the gateway/router sends to the metadata Raft group
pub enum MetadataRequest {
    AllocateTimestamp,
    LookupTablet { table_id: u64, key: Vec<u8> },
    LookupSchema { table_id: u64 },
}

impl MetadataRequest {
    pub fn to_proto(&self) -> rpc::MetadataRequest {
        let request = match self {
            MetadataRequest::AllocateTimestamp => Some(
                rpc::metadata_request::Request::AllocateTimestamp(rpc::AllocateTimestampRequest {}),
            ),
            MetadataRequest::LookupTablet { table_id, key } => Some(
                rpc::metadata_request::Request::LookupTablet(rpc::LookupTabletRequest {
                    table_id: *table_id,
                    key: key.clone(),
                }),
            ),
            MetadataRequest::LookupSchema { table_id } => Some(
                rpc::metadata_request::Request::LookupSchema(rpc::LookupSchemaRequest {
                    table_id: *table_id,
                }),
            ),
        };
        rpc::MetadataRequest { request }
    }

    pub fn from_proto(proto: rpc::MetadataRequest) -> Result<Self, &'static str> {
        match proto.request {
            Some(rpc::metadata_request::Request::AllocateTimestamp(_)) => {
                Ok(MetadataRequest::AllocateTimestamp)
            }
            Some(rpc::metadata_request::Request::LookupTablet(req)) => {
                Ok(MetadataRequest::LookupTablet {
                    table_id: req.table_id,
                    key: req.key,
                })
            }
            Some(rpc::metadata_request::Request::LookupSchema(req)) => {
                Ok(MetadataRequest::LookupSchema {
                    table_id: req.table_id,
                })
            }
            None => Err("missing metadata request"),
        }
    }
}

/// Resolves one Raft consensus identity to its physical transport destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaRoute {
    pub replica_id: ReplicaId,
    pub node_id: NodeId,
}

/// The immutable routing information returned by metadata for one tablet.
///
/// A leader hint is deliberately represented separately from the replica
/// placement map. Metadata owns placement; leadership changes are Raft state
/// and therefore remain a cacheable, retryable hint at the gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletRoute {
    pub raft_group_id: RaftGroupId,
    pub tablet_id: TabletId,
    pub tablet_epoch: u64,
    pub leader_replica_id: ReplicaId,
    pub replicas: Vec<ReplicaRoute>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TabletRouteError {
    #[error("tablet route Raft group ID must be non-zero")]
    ZeroRaftGroupId,
    #[error("tablet route epoch must be non-zero")]
    ZeroTabletEpoch,
    #[error("tablet ID must be non-zero")]
    ZeroTabletId,
    #[error("leader replica ID must be non-zero")]
    ZeroLeaderReplicaId,
    #[error("tablet route must contain at least one replica")]
    EmptyReplicas,
    #[error("tablet route contains a zero replica or node ID")]
    ZeroReplicaIdentity,
    #[error("tablet route contains duplicate replica ID {0:?}")]
    DuplicateReplica(ReplicaId),
    #[error("tablet route contains duplicate node ID {0:?}")]
    DuplicateNode(NodeId),
    #[error("leader replica {0:?} is absent from the tablet route")]
    LeaderAbsent(ReplicaId),
    #[error("metadata response does not contain a tablet route")]
    NotTabletResponse,
    #[error("tablet route cache cannot install an empty topology")]
    EmptyTopology,
    #[error("tablet route cache contains duplicate tablet ID {0:?}")]
    DuplicateTablet(TabletId),
    #[error("tablet route cache is missing tablet ID {0:?}")]
    MissingTablet(TabletId),
    #[error("tablet route cache received unexpected tablet ID {0:?}")]
    UnexpectedTablet(TabletId),
}

impl TabletRoute {
    pub fn validate(&self) -> Result<(), TabletRouteError> {
        if self.raft_group_id.0 == 0 {
            return Err(TabletRouteError::ZeroRaftGroupId);
        }
        if self.tablet_id.0 == 0 {
            return Err(TabletRouteError::ZeroTabletId);
        }
        if self.tablet_epoch == 0 {
            return Err(TabletRouteError::ZeroTabletEpoch);
        }
        if self.leader_replica_id.0 == 0 {
            return Err(TabletRouteError::ZeroLeaderReplicaId);
        }
        if self.replicas.is_empty() {
            return Err(TabletRouteError::EmptyReplicas);
        }

        let mut replica_ids = BTreeSet::new();
        let mut node_ids = BTreeSet::new();
        for route in &self.replicas {
            if route.replica_id.0 == 0 || route.node_id.0 == 0 {
                return Err(TabletRouteError::ZeroReplicaIdentity);
            }
            if !replica_ids.insert(route.replica_id) {
                return Err(TabletRouteError::DuplicateReplica(route.replica_id));
            }
            if !node_ids.insert(route.node_id) {
                return Err(TabletRouteError::DuplicateNode(route.node_id));
            }
        }

        if !replica_ids.contains(&self.leader_replica_id) {
            return Err(TabletRouteError::LeaderAbsent(self.leader_replica_id));
        }

        Ok(())
    }

    pub fn node_for_replica(&self, replica_id: ReplicaId) -> Option<NodeId> {
        self.replicas
            .iter()
            .find(|route| route.replica_id == replica_id)
            .map(|route| route.node_id)
    }

    pub fn leader_node(&self) -> Result<NodeId, TabletRouteError> {
        self.validate()?;
        self.node_for_replica(self.leader_replica_id)
            .ok_or(TabletRouteError::LeaderAbsent(self.leader_replica_id))
    }
}

/// Atomically replace the gateway's immutable tablet routing view.
///
/// Callers provide the expected tablet IDs from one committed metadata
/// snapshot. Requiring an exact set makes a partially refreshed topology
/// impossible to publish to request routing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TabletRouteCache {
    routes: BTreeMap<TabletId, TabletRoute>,
}

impl TabletRouteCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn replace_exact(
        &mut self,
        expected_tablets: &BTreeSet<TabletId>,
        routes: impl IntoIterator<Item = TabletRoute>,
    ) -> Result<(), TabletRouteError> {
        if expected_tablets.is_empty() {
            return Err(TabletRouteError::EmptyTopology);
        }

        let mut replacement = BTreeMap::new();
        for route in routes {
            route.validate()?;
            if replacement.insert(route.tablet_id, route.clone()).is_some() {
                return Err(TabletRouteError::DuplicateTablet(route.tablet_id));
            }
        }

        for tablet_id in expected_tablets {
            if !replacement.contains_key(tablet_id) {
                return Err(TabletRouteError::MissingTablet(*tablet_id));
            }
        }
        if replacement.len() != expected_tablets.len() {
            let unexpected = replacement
                .keys()
                .find(|tablet_id| !expected_tablets.contains(tablet_id))
                .copied()
                .expect("length mismatch implies an unexpected tablet");
            return Err(TabletRouteError::UnexpectedTablet(unexpected));
        }

        self.routes = replacement;
        Ok(())
    }

    pub fn insert(&mut self, route: TabletRoute) -> Result<(), TabletRouteError> {
        route.validate()?;
        self.routes.insert(route.tablet_id, route);
        Ok(())
    }

    pub fn get(&self, tablet_id: TabletId) -> Option<&TabletRoute> {
        self.routes.get(&tablet_id)
    }

    pub fn node_for_replica(&self, tablet_id: TabletId, replica_id: ReplicaId) -> Option<NodeId> {
        self.get(tablet_id)?.node_for_replica(replica_id)
    }

    /// Update only the soft leader hint after a successful request or a
    /// `NotLeader` response. Placement remains unchanged and an unknown
    /// replica can never be promoted by a stale response.
    pub fn update_leader(
        &mut self,
        tablet_id: TabletId,
        leader_replica_id: ReplicaId,
    ) -> Result<(), TabletRouteError> {
        let route = self
            .routes
            .get_mut(&tablet_id)
            .ok_or(TabletRouteError::MissingTablet(tablet_id))?;
        if route.node_for_replica(leader_replica_id).is_none() {
            return Err(TabletRouteError::LeaderAbsent(leader_replica_id));
        }
        route.leader_replica_id = leader_replica_id;
        Ok(())
    }

    pub fn leader_node(&self, tablet_id: TabletId) -> Result<NodeId, TabletRouteError> {
        self.routes
            .get(&tablet_id)
            .ok_or(TabletRouteError::MissingTablet(tablet_id))?
            .leader_node()
    }

    pub fn remove(&mut self, tablet_id: TabletId) -> Option<TabletRoute> {
        self.routes.remove(&tablet_id)
    }

    pub fn len(&self) -> usize {
        self.routes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

/// Responses from the metadata Raft group.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataResponse {
    AllocateTimestamp {
        timestamp: Timestamp,
    },
    LookupTablet {
        raft_group_id: RaftGroupId,
        tablet_id: TabletId,
        tablet_epoch: u64,
        leader_replica_id: ReplicaId,
        replicas: Vec<ReplicaRoute>,
    },
    LookupSchema {
        schema_bytes: Vec<u8>,
        schema_version: u64,
    },
}

impl MetadataResponse {
    pub fn tablet_route(&self) -> Result<TabletRoute, TabletRouteError> {
        match self {
            Self::LookupTablet {
                raft_group_id,
                tablet_id,
                tablet_epoch,
                leader_replica_id,
                replicas,
            } => {
                let route = TabletRoute {
                    raft_group_id: *raft_group_id,
                    tablet_id: *tablet_id,
                    tablet_epoch: *tablet_epoch,
                    leader_replica_id: *leader_replica_id,
                    replicas: replicas.clone(),
                };
                route.validate()?;
                Ok(route)
            }
            _ => Err(TabletRouteError::NotTabletResponse),
        }
    }

    pub fn to_proto(&self) -> rpc::MetadataResponse {
        let response = match self {
            MetadataResponse::AllocateTimestamp { timestamp } => {
                Some(rpc::metadata_response::Response::AllocateTimestamp(
                    rpc::AllocateTimestampResponse {
                        timestamp: Some(timestamp.to_proto()),
                    },
                ))
            }
            MetadataResponse::LookupTablet {
                raft_group_id,
                tablet_id,
                tablet_epoch,
                leader_replica_id,
                replicas,
            } => Some(rpc::metadata_response::Response::LookupTablet(
                rpc::LookupTabletResponse {
                    tablet_id: Some(tablet_id.to_proto()),
                    leader_replica_id: Some(leader_replica_id.to_proto()),
                    raft_group_id: Some(raft_group_id.to_proto()),
                    tablet_epoch: *tablet_epoch,
                    replicas: replicas
                        .iter()
                        .map(|route| rpc::ReplicaRoute {
                            replica_id: Some(route.replica_id.to_proto()),
                            node_id: Some(route.node_id.to_proto()),
                        })
                        .collect(),
                },
            )),
            MetadataResponse::LookupSchema {
                schema_bytes,
                schema_version,
            } => Some(rpc::metadata_response::Response::LookupSchema(
                rpc::LookupSchemaResponse {
                    schema_bytes: schema_bytes.clone(),
                    schema_version: *schema_version,
                },
            )),
        };
        rpc::MetadataResponse { response }
    }

    pub fn from_proto(proto: rpc::MetadataResponse) -> Result<Self, &'static str> {
        match proto.response {
            Some(rpc::metadata_response::Response::AllocateTimestamp(resp)) => {
                Ok(MetadataResponse::AllocateTimestamp {
                    timestamp: Timestamp::from_proto(resp.timestamp.ok_or("missing timestamp")?),
                })
            }
            Some(rpc::metadata_response::Response::LookupTablet(resp)) => {
                let raft_group_id =
                    RaftGroupId::from_proto(resp.raft_group_id.ok_or("missing raft_group_id")?);
                if raft_group_id.0 == 0 || resp.tablet_epoch == 0 {
                    return Err("tablet route identity must be non-zero");
                }
                let leader_replica_id = ReplicaId::from_proto(
                    resp.leader_replica_id.ok_or("missing leader_replica_id")?,
                );
                if leader_replica_id.0 == 0 {
                    return Err("leader replica ID must be non-zero");
                }

                let mut replicas = Vec::with_capacity(resp.replicas.len());
                for route in resp.replicas {
                    let replica_id =
                        ReplicaId::from_proto(route.replica_id.ok_or("missing route replica_id")?);
                    let node_id = NodeId::from_proto(route.node_id.ok_or("missing route node_id")?);
                    if replica_id.0 == 0 || node_id.0 == 0 {
                        return Err("replica routes must contain non-zero identities");
                    }
                    if replicas
                        .iter()
                        .any(|existing: &ReplicaRoute| existing.replica_id == replica_id)
                    {
                        return Err("duplicate replica ID in tablet route");
                    }
                    replicas.push(ReplicaRoute {
                        replica_id,
                        node_id,
                    });
                }
                if !replicas
                    .iter()
                    .any(|route| route.replica_id == leader_replica_id)
                {
                    return Err("leader replica is absent from tablet routes");
                }

                let route = TabletRoute {
                    raft_group_id,
                    tablet_id: TabletId::from_proto(resp.tablet_id.ok_or("missing tablet_id")?),
                    tablet_epoch: resp.tablet_epoch,
                    leader_replica_id,
                    replicas: replicas.clone(),
                };
                route.validate().map_err(|_| "invalid tablet route")?;

                Ok(MetadataResponse::LookupTablet {
                    raft_group_id,
                    tablet_id: route.tablet_id,
                    tablet_epoch: route.tablet_epoch,
                    leader_replica_id,
                    replicas,
                })
            }

            Some(rpc::metadata_response::Response::LookupSchema(resp)) => {
                Ok(MetadataResponse::LookupSchema {
                    schema_bytes: resp.schema_bytes,
                    schema_version: resp.schema_version,
                })
            }
            None => Err("missing metadata response"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_type_unspecified_rejected() {
        assert!(MessageType::from_proto(rpc::MessageType::Unspecified).is_err());
    }

    #[test]
    fn rpc_frame_roundtrip() {
        let frame = RpcFrame {
            msg_type: MessageType::TabletCommandRequest,
            raft_group_id: RaftGroupId(5),
            payload: vec![1, 2, 3, 4],
        };
        let proto = frame.to_proto();
        let decoded = RpcFrame::from_proto(proto).unwrap();
        assert!(matches!(
            decoded.msg_type,
            MessageType::TabletCommandRequest
        ));
        assert_eq!(decoded.raft_group_id.0, 5);
        assert_eq!(decoded.payload, vec![1, 2, 3, 4]);
    }

    #[test]
    fn rpc_frame_rejects_zero_group_or_empty_payload() {
        let zero_group = rpc::RpcFrame {
            msg_type: rpc::MessageType::MetadataRequest as i32,
            raft_group_id: Some(RaftGroupId(0).to_proto()),
            payload: vec![1],
        };
        assert!(RpcFrame::from_proto(zero_group).is_err());

        let empty_payload = rpc::RpcFrame {
            msg_type: rpc::MessageType::MetadataRequest as i32,
            raft_group_id: Some(RaftGroupId(2).to_proto()),
            payload: Vec::new(),
        };
        assert!(RpcFrame::from_proto(empty_payload).is_err());
    }

    #[test]
    fn tablet_command_request_roundtrip() {
        use crate::command_codec::{CommitCommand, TabletCommand};
        let req = TabletCommandRequest {
            request_id: RequestId {
                client_id: 12345,
                sequence: 1,
                raft_group_id: RaftGroupId(5),
            },
            logical_command_id: None,
            acknowledged_through: None,
            tablet_id: TabletId(9),
            tablet_epoch: 3,
            command: TabletCommand::Commit(CommitCommand {
                txn_id: crate::ids::TxnId(1),
                start_timestamp: Timestamp(100),
                commit_timestamp: Timestamp(105),
                keys: vec![b"/table/1/pk/1".to_vec()],
            }),
        };
        let proto = req.to_proto().unwrap();
        let decoded = TabletCommandRequest::from_proto(proto).unwrap();
        assert_eq!(decoded.request_id.sequence, 1);
        assert_eq!(decoded.tablet_id, TabletId(9));
        assert_eq!(decoded.tablet_epoch, 3);
        assert!(matches!(decoded.command, TabletCommand::Commit(_)));
    }

    #[test]
    fn tablet_outcome_query_roundtrip_preserves_logical_identity() {
        let logical_command_id = crate::ids::LogicalCommandId {
            client_request_id: crate::ids::ClientRequestId {
                client_id: 77,
                session_epoch: 4,
                request_sequence: 9,
            },
            command_ordinal: 1,
            kind: crate::ids::CommandKind::SingleShardCommit,
        };
        let request = TabletOutcomeQueryRequest {
            request_id: RequestId {
                client_id: 77,
                sequence: 10,
                raft_group_id: RaftGroupId(8),
            },
            logical_command_id,
            tablet_id: TabletId(12),
            tablet_epoch: 3,
        };

        let decoded = TabletOutcomeQueryRequest::from_proto(request.to_proto()).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn tablet_read_request_roundtrip_preserves_generation_and_snapshot() {
        let request = TabletReadRequest {
            request_id: RequestId {
                client_id: 77,
                sequence: 4,
                raft_group_id: RaftGroupId(8),
            },
            logical_command_id: None,
            tablet_id: TabletId(12),
            tablet_epoch: 9,
            row_key: crate::ids::RowKey {
                table_id: crate::ids::TableId(12),
                primary_key_bytes: b"pk".to_vec(),
            },
            read_timestamp: Timestamp(100),
        };
        let decoded = TabletReadRequest::from_proto(request.to_proto()).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn tablet_requests_reject_zero_generation_before_dispatch() {
        let command = rpc::TabletCommandRequest {
            request_id: Some(
                RequestId {
                    client_id: 1,
                    sequence: 1,
                    raft_group_id: RaftGroupId(8),
                }
                .to_proto(),
            ),
            logical_command_id: None,
            acknowledged_through: None,
            tablet_id: Some(TabletId(12).to_proto()),
            tablet_epoch: 0,
            command: Some(
                crate::command_codec::TabletCommand::Noop(crate::command_codec::NoopCommand)
                    .to_proto()
                    .unwrap(),
            ),
        };
        assert!(matches!(
            TabletCommandRequest::from_proto(command),
            Err("tablet command request contains a reserved zero identity")
        ));
    }

    #[test]
    fn tablet_command_response_roundtrip() {
        let resp = TabletCommandResponse {
            request_id: RequestId {
                client_id: 999,
                sequence: 5,
                raft_group_id: RaftGroupId(5),
            },
            success: true,
            error_message: String::new(),
            error_code: String::new(),
            retryable: false,
            result_data: vec![10, 20, 30],
            found: true,
            leader_replica_id: Some(ReplicaId(2)),
            current_tablet_epoch: None,
            expected_tablet_epoch: None,
        };
        let proto = resp.to_proto();
        let decoded = TabletCommandResponse::from_proto(proto).unwrap();
        assert!(decoded.success);
        assert_eq!(decoded.result_data, vec![10, 20, 30]);
        assert!(decoded.found);
        assert_eq!(decoded.leader_replica_id, Some(ReplicaId(2)));
    }

    #[test]
    fn metadata_request_allocate_ts_roundtrip() {
        let req = MetadataRequest::AllocateTimestamp;
        let proto = req.to_proto();
        let decoded = MetadataRequest::from_proto(proto).unwrap();
        assert!(matches!(decoded, MetadataRequest::AllocateTimestamp));
    }

    #[test]
    fn metadata_request_lookup_tablet_roundtrip() {
        let req = MetadataRequest::LookupTablet {
            table_id: 100,
            key: b"pk1".to_vec(),
        };
        let proto = req.to_proto();
        let decoded = MetadataRequest::from_proto(proto).unwrap();
        assert!(matches!(
            decoded,
            MetadataRequest::LookupTablet { table_id: 100, .. }
        ));
    }

    #[test]
    fn metadata_request_lookup_schema_roundtrip() {
        let req = MetadataRequest::LookupSchema { table_id: 200 };
        let proto = req.to_proto();
        let decoded = MetadataRequest::from_proto(proto).unwrap();
        assert!(matches!(
            decoded,
            MetadataRequest::LookupSchema { table_id: 200 }
        ));
    }

    #[test]
    fn metadata_request_missing_rejected() {
        let proto = rpc::MetadataRequest { request: None };
        assert!(MetadataRequest::from_proto(proto).is_err());
    }

    #[test]
    fn metadata_response_allocate_ts_roundtrip() {
        let resp = MetadataResponse::AllocateTimestamp {
            timestamp: Timestamp(500),
        };
        let proto = resp.to_proto();
        let decoded = MetadataResponse::from_proto(proto).unwrap();
        assert!(
            matches!(decoded, MetadataResponse::AllocateTimestamp { timestamp } if timestamp.0 == 500)
        );
    }

    #[test]
    fn metadata_response_lookup_tablet_roundtrip() {
        let resp = MetadataResponse::LookupTablet {
            raft_group_id: RaftGroupId(7),
            tablet_id: TabletId(10),
            tablet_epoch: 2,
            leader_replica_id: ReplicaId(11),
            replicas: vec![
                ReplicaRoute {
                    replica_id: ReplicaId(11),
                    node_id: NodeId(1),
                },
                ReplicaRoute {
                    replica_id: ReplicaId(12),
                    node_id: NodeId(2),
                },
                ReplicaRoute {
                    replica_id: ReplicaId(13),
                    node_id: NodeId(3),
                },
            ],
        };
        let proto = resp.to_proto();
        let decoded = MetadataResponse::from_proto(proto).unwrap();
        assert!(
            matches!(decoded, MetadataResponse::LookupTablet { tablet_id, .. } if tablet_id.0 == 10)
        );
    }

    #[test]
    fn tablet_route_rejects_incomplete_replica_identity() {
        let route = TabletRoute {
            raft_group_id: RaftGroupId(7),
            tablet_id: TabletId(10),
            tablet_epoch: 2,
            leader_replica_id: ReplicaId(11),
            replicas: vec![ReplicaRoute {
                replica_id: ReplicaId(12),
                node_id: NodeId(2),
            }],
        };

        assert_eq!(
            route.validate(),
            Err(TabletRouteError::LeaderAbsent(ReplicaId(11)))
        );
    }

    #[test]
    fn tablet_route_cache_does_not_publish_partial_topology() {
        let mut cache = TabletRouteCache::new();
        let expected = BTreeSet::from([TabletId(10), TabletId(20)]);
        let route = TabletRoute {
            raft_group_id: RaftGroupId(7),
            tablet_id: TabletId(10),
            tablet_epoch: 2,
            leader_replica_id: ReplicaId(11),
            replicas: vec![ReplicaRoute {
                replica_id: ReplicaId(11),
                node_id: NodeId(1),
            }],
        };

        assert_eq!(
            cache.replace_exact(&expected, [route]),
            Err(TabletRouteError::MissingTablet(TabletId(20)))
        );
        assert!(cache.is_empty());
    }

    #[test]
    fn tablet_route_cache_updates_only_known_leader_hints() {
        let mut cache = TabletRouteCache::new();
        cache
            .insert(TabletRoute {
                raft_group_id: RaftGroupId(7),
                tablet_id: TabletId(10),
                tablet_epoch: 2,
                leader_replica_id: ReplicaId(11),
                replicas: vec![
                    ReplicaRoute {
                        replica_id: ReplicaId(11),
                        node_id: NodeId(1),
                    },
                    ReplicaRoute {
                        replica_id: ReplicaId(12),
                        node_id: NodeId(2),
                    },
                ],
            })
            .unwrap();

        cache.update_leader(TabletId(10), ReplicaId(12)).unwrap();
        assert_eq!(cache.leader_node(TabletId(10)), Ok(NodeId(2)));
        assert_eq!(
            cache.update_leader(TabletId(10), ReplicaId(99)),
            Err(TabletRouteError::LeaderAbsent(ReplicaId(99)))
        );
        assert_eq!(cache.leader_node(TabletId(10)), Ok(NodeId(2)));
    }

    #[test]
    fn metadata_tablet_response_rejects_duplicate_physical_routes() {
        let response = rpc::MetadataResponse {
            response: Some(rpc::metadata_response::Response::LookupTablet(
                rpc::LookupTabletResponse {
                    tablet_id: Some(TabletId(10).to_proto()),
                    leader_replica_id: Some(ReplicaId(11).to_proto()),
                    raft_group_id: Some(RaftGroupId(7).to_proto()),
                    tablet_epoch: 2,
                    replicas: vec![
                        rpc::ReplicaRoute {
                            replica_id: Some(ReplicaId(11).to_proto()),
                            node_id: Some(NodeId(1).to_proto()),
                        },
                        rpc::ReplicaRoute {
                            replica_id: Some(ReplicaId(12).to_proto()),
                            node_id: Some(NodeId(1).to_proto()),
                        },
                    ],
                },
            )),
        };

        assert!(MetadataResponse::from_proto(response).is_err());
    }

    #[test]
    fn message_type_wire_values_are_stable_and_unknown_values_rejected() {
        assert_eq!(MessageType::MetadataRequest.wire_value(), 0x04);
        assert_eq!(
            MessageType::from_wire_value(0x05),
            Ok(MessageType::MetadataResponse)
        );
        assert!(MessageType::from_wire_value(0xff).is_err());
    }

    #[test]
    fn metadata_response_lookup_schema_roundtrip() {
        let resp = MetadataResponse::LookupSchema {
            schema_bytes: vec![0xAA, 0xBB],
            schema_version: 3,
        };
        let proto = resp.to_proto();
        let decoded = MetadataResponse::from_proto(proto).unwrap();
        assert!(matches!(
            decoded,
            MetadataResponse::LookupSchema {
                schema_version: 3,
                ..
            }
        ));
    }

    #[test]
    fn metadata_response_missing_rejected() {
        let proto = rpc::MetadataResponse { response: None };
        assert!(MetadataResponse::from_proto(proto).is_err());
    }
}
