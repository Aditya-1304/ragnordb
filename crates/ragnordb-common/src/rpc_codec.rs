use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use super::command_codec::TabletCommand;
use crate::ids::{
    LogicalCommandId, NodeId, RaftGroupId, ReplicaId, RequestId, TableId, TabletId, Timestamp,
};
use crate::proto::rpc;
use prost::Message;

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
///   0x08 — TabletScanRequest
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
    TabletScanRequest,
    ReplicaJoinRequest,
    ReplicaJoinResponse,
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
            Self::TabletScanRequest => 0x08,
            Self::ReplicaJoinRequest => 0x09,
            Self::ReplicaJoinResponse => 0x0A,
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
            0x08 => Ok(Self::TabletScanRequest),
            0x09 => Ok(Self::ReplicaJoinRequest),
            0x0A => Ok(Self::ReplicaJoinResponse),
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
            MessageType::TabletScanRequest => rpc::MessageType::TabletScanRequest,
            MessageType::ReplicaJoinRequest => rpc::MessageType::ReplicaJoinRequest,
            MessageType::ReplicaJoinResponse => rpc::MessageType::ReplicaJoinResponse,
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
            rpc::MessageType::TabletScanRequest => Ok(MessageType::TabletScanRequest),
            rpc::MessageType::ReplicaJoinRequest => Ok(MessageType::ReplicaJoinRequest),
            rpc::MessageType::ReplicaJoinResponse => Ok(MessageType::ReplicaJoinResponse),
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
            rpc_attempt_id: None,
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
            rpc_attempt_id: None,
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
    /// Conservative remaining budget for one node-to-node forward. It is
    /// transport metadata, not part of logical request identity.
    pub deadline_remaining_ms: Option<u64>,
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
            rpc_attempt_id: None,
            deadline_remaining_ms: self.deadline_remaining_ms,
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
        if proto.deadline_remaining_ms == Some(0) {
            return Err("tablet read deadline must be non-zero");
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
            deadline_remaining_ms: proto.deadline_remaining_ms,
        })
    }
}

/// Maximum number of rows admitted into one tablet scan batch. The request
/// carries a caller-selected lower cap, but never a value above this protocol
/// bound; this keeps one malformed or malicious request from forcing an
/// unbounded response allocation at a tablet.
pub const MAX_TABLET_SCAN_ROWS: u32 = 65_536;

/// Maximum raw key-plus-row bytes admitted into one tablet scan batch.
pub const MAX_TABLET_SCAN_BYTES: u32 = 8 * 1024 * 1024;

/// A bounded read over one logical half-open tablet span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletScanRequest {
    pub request_id: RequestId,
    pub tablet_id: TabletId,
    pub tablet_epoch: u64,
    /// Inclusive lower logical bound. `None` means unbounded below.
    pub start_key: Option<Vec<u8>>,
    /// Exclusive upper logical bound. `None` means unbounded above.
    pub end_key: Option<Vec<u8>>,
    /// The last key already delivered. The next row must compare greater than
    /// this key, which makes retries and topology refreshes resumable without
    /// replaying a completed logical row.
    pub resume_after: Option<Vec<u8>>,
    pub read_timestamp: Timestamp,
    pub max_rows: u32,
    pub max_bytes: u32,
    /// Physical transport-attempt correlation. It is not part of logical scan
    /// identity and may change when the same scan request is retried.
    pub rpc_attempt_id: Option<u64>,
    /// Conservative remaining budget for one node-to-node forward. It is
    /// transport metadata, not part of logical scan identity.
    pub deadline_remaining_ms: Option<u64>,
}

impl TabletScanRequest {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.request_id.client_id == 0
            || self.request_id.sequence == 0
            || self.request_id.raft_group_id.0 == 0
            || self.tablet_id.0 == 0
            || self.tablet_epoch == 0
        {
            return Err("tablet scan request contains a reserved zero identity");
        }
        if self.read_timestamp.0 == 0 {
            return Err("tablet scan read timestamp must be non-zero");
        }
        if self.max_rows == 0 || self.max_rows > MAX_TABLET_SCAN_ROWS {
            return Err("tablet scan max_rows is outside the allowed range");
        }
        if self.max_bytes == 0 || self.max_bytes > MAX_TABLET_SCAN_BYTES {
            return Err("tablet scan max_bytes is outside the allowed range");
        }
        if let (Some(start_key), Some(end_key)) = (&self.start_key, &self.end_key)
            && start_key >= end_key
        {
            return Err("tablet scan logical bounds must be ordered");
        }
        if let Some(resume_after) = &self.resume_after {
            if self
                .start_key
                .as_ref()
                .is_some_and(|start_key| resume_after < start_key)
            {
                return Err("scan resume_after must not precede start_key");
            }
            if self
                .end_key
                .as_ref()
                .is_some_and(|end_key| resume_after >= end_key)
            {
                return Err("scan resume_after must be strictly before end_key");
            }
        }
        if self
            .rpc_attempt_id
            .is_some_and(|attempt_id| attempt_id == 0)
        {
            return Err("tablet scan RPC attempt ID must be non-zero");
        }
        if self
            .deadline_remaining_ms
            .is_some_and(|deadline| deadline == 0)
        {
            return Err("tablet scan deadline must be non-zero");
        }
        Ok(())
    }

    pub fn to_proto(&self) -> rpc::TabletScanRequest {
        rpc::TabletScanRequest {
            request_id: Some(self.request_id.to_proto()),
            tablet_id: Some(self.tablet_id.to_proto()),
            tablet_epoch: self.tablet_epoch,
            start_key: self.start_key.clone(),
            end_key: self.end_key.clone(),
            resume_after: self.resume_after.clone(),
            read_timestamp: Some(self.read_timestamp.to_proto()),
            max_rows: self.max_rows,
            max_bytes: self.max_bytes,
            rpc_attempt_id: self.rpc_attempt_id,
            deadline_remaining_ms: self.deadline_remaining_ms,
        }
    }

    pub fn from_proto(proto: rpc::TabletScanRequest) -> Result<Self, &'static str> {
        let request = Self {
            request_id: RequestId::from_proto(proto.request_id.ok_or("missing request_id")?)?,
            tablet_id: TabletId::from_proto(proto.tablet_id.ok_or("missing tablet_id")?),
            tablet_epoch: proto.tablet_epoch,
            start_key: proto.start_key,
            end_key: proto.end_key,
            resume_after: proto.resume_after,
            read_timestamp: Timestamp::from_proto(
                proto.read_timestamp.ok_or("missing read_timestamp")?,
            ),
            max_rows: proto.max_rows,
            max_bytes: proto.max_bytes,
            rpc_attempt_id: proto.rpc_attempt_id,
            deadline_remaining_ms: proto.deadline_remaining_ms,
        };
        request.validate()?;
        Ok(request)
    }
}

/// One logically ordered row returned by a tablet scan. `key` is the logical
/// ordering key used for resume progress; `row` is the canonical encoded row
/// payload consumed by the executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletScanRow {
    pub key: Vec<u8>,
    pub row: Vec<u8>,
}

impl TabletScanRow {
    pub fn to_proto(&self) -> rpc::TabletScanRow {
        rpc::TabletScanRow {
            key: self.key.clone(),
            row: self.row.clone(),
        }
    }

    pub fn from_proto(proto: rpc::TabletScanRow) -> Self {
        Self {
            key: proto.key,
            row: proto.row,
        }
    }

    pub fn byte_len(&self) -> usize {
        self.key.len().saturating_add(self.row.len())
    }
}

/// One bounded tablet response batch. A non-exhausted batch must publish the
/// last delivered key as `next_resume_after`; the next request can then use it
/// as an exclusive cursor after a timeout, leader retry, or range refresh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletScanBatch {
    pub rows: Vec<TabletScanRow>,
    pub next_resume_after: Option<Vec<u8>>,
    pub exhausted: bool,
}

impl TabletScanBatch {
    pub fn to_proto(&self) -> rpc::TabletScanBatch {
        rpc::TabletScanBatch {
            rows: self.rows.iter().map(TabletScanRow::to_proto).collect(),
            next_resume_after: self.next_resume_after.clone(),
            exhausted: self.exhausted,
        }
    }

    pub fn from_proto(proto: rpc::TabletScanBatch) -> Result<Self, &'static str> {
        let batch = Self {
            rows: proto
                .rows
                .into_iter()
                .map(TabletScanRow::from_proto)
                .collect(),
            next_resume_after: proto.next_resume_after,
            exhausted: proto.exhausted,
        };
        batch.validate()?;
        Ok(batch)
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.rows.windows(2).any(|rows| rows[0].key >= rows[1].key) {
            return Err("tablet scan rows must be strictly ordered by key");
        }
        match (&self.rows.last(), &self.next_resume_after) {
            (Some(last_row), Some(next_resume_after)) if last_row.key != *next_resume_after => {
                Err("tablet scan resume cursor must equal the last row key")
            }
            (None, Some(_)) => Err("empty tablet scan batch cannot publish a resume cursor"),
            (Some(_), None) if !self.exhausted => {
                Err("non-exhausted tablet scan batch must publish a resume cursor")
            }
            _ => Ok(()),
        }
    }

    pub fn validate_for(&self, request: &TabletScanRequest) -> Result<(), &'static str> {
        request.validate()?;
        self.validate()?;
        if self.rows.len() > request.max_rows as usize {
            return Err("tablet scan batch exceeds max_rows");
        }
        if self.byte_len() > request.max_bytes as usize {
            return Err("tablet scan batch exceeds max_bytes");
        }
        for row in &self.rows {
            if request
                .start_key
                .as_ref()
                .is_some_and(|start_key| row.key < *start_key)
            {
                return Err("tablet scan row is before start_key");
            }
            if request
                .end_key
                .as_ref()
                .is_some_and(|end_key| row.key >= *end_key)
            {
                return Err("tablet scan row is at or beyond end_key");
            }
            if request
                .resume_after
                .as_ref()
                .is_some_and(|resume_after| row.key <= *resume_after)
            {
                return Err("tablet scan row is not after resume_after");
            }
        }
        if let Some(next_resume_after) = &self.next_resume_after
            && request
                .end_key
                .as_ref()
                .is_some_and(|end_key| next_resume_after >= end_key)
        {
            return Err("tablet scan resume cursor must be before end_key");
        }
        Ok(())
    }

    pub fn byte_len(&self) -> usize {
        self.rows.iter().map(TabletScanRow::byte_len).sum()
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
            rpc_attempt_id: None,
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
    ProposeCommand(MetadataProposalRequest),
    ProposeConfChange(MetadataConfChangeRequest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataProposalRequest {
    pub request_id: RequestId,
    pub command_envelope: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataConfChangeRequest {
    pub expected_conf_state_version: u64,
    pub replica_id: ReplicaId,
    pub remove_replica: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataProposalOutcome {
    Applied,
    AlreadyApplied,
    ClientRegistered {
        session_epoch: u64,
    },
    ClientRenewed,
    TableCreated {
        table_id: TableId,
        tablet_id: TabletId,
        raft_group_id: RaftGroupId,
    },
    TimestampsReserved {
        reserved_from: Timestamp,
        reserved_until: Timestamp,
    },
    TimestampReservationRegressed {
        current: Timestamp,
        received: Timestamp,
    },
    Rejected {
        reason: String,
    },
}

impl MetadataProposalOutcome {
    pub fn to_proto(&self) -> rpc::MetadataProposalOutcome {
        let (
            kind,
            client_id,
            session_epoch,
            table_id,
            tablet_id,
            raft_group_id,
            rejection,
            timestamp_reserved_until,
            timestamp_current,
            timestamp_received,
            timestamp_reserved_from,
        ) = match self {
            Self::Applied => (
                rpc::metadata_proposal_outcome::Kind::Applied,
                0,
                0,
                0,
                0,
                0,
                String::new(),
                0,
                0,
                0,
                0,
            ),
            Self::AlreadyApplied => (
                rpc::metadata_proposal_outcome::Kind::AlreadyApplied,
                0,
                0,
                0,
                0,
                0,
                String::new(),
                0,
                0,
                0,
                0,
            ),
            Self::ClientRegistered { session_epoch } => (
                rpc::metadata_proposal_outcome::Kind::ClientRegistered,
                0,
                *session_epoch,
                0,
                0,
                0,
                String::new(),
                0,
                0,
                0,
                0,
            ),
            Self::ClientRenewed => (
                rpc::metadata_proposal_outcome::Kind::ClientRenewed,
                0,
                0,
                0,
                0,
                0,
                String::new(),
                0,
                0,
                0,
                0,
            ),
            Self::TableCreated {
                table_id,
                tablet_id,
                raft_group_id,
            } => (
                rpc::metadata_proposal_outcome::Kind::TableCreated,
                0,
                0,
                table_id.0,
                tablet_id.0,
                raft_group_id.0,
                String::new(),
                0,
                0,
                0,
                0,
            ),
            Self::TimestampsReserved {
                reserved_from,
                reserved_until,
            } => (
                rpc::metadata_proposal_outcome::Kind::TimestampsReserved,
                0,
                0,
                0,
                0,
                0,
                String::new(),
                reserved_until.0,
                0,
                0,
                reserved_from.0,
            ),
            Self::TimestampReservationRegressed { current, received } => (
                rpc::metadata_proposal_outcome::Kind::TimestampReservationRegressed,
                0,
                0,
                0,
                0,
                0,
                String::new(),
                0,
                current.0,
                received.0,
                0,
            ),
            Self::Rejected { reason } => (
                rpc::metadata_proposal_outcome::Kind::Rejected,
                0,
                0,
                0,
                0,
                0,
                reason.clone(),
                0,
                0,
                0,
                0,
            ),
        };
        rpc::MetadataProposalOutcome {
            kind: kind as i32,
            client_id,
            session_epoch,
            table_id,
            tablet_id,
            raft_group_id,
            rejection,
            timestamp_reserved_until,
            timestamp_current,
            timestamp_received,
            timestamp_reserved_from,
        }
    }

    pub fn from_proto(proto: rpc::MetadataProposalOutcome) -> Result<Self, &'static str> {
        match rpc::metadata_proposal_outcome::Kind::try_from(proto.kind)
            .map_err(|_| "invalid metadata proposal outcome kind")?
        {
            rpc::metadata_proposal_outcome::Kind::Applied => Ok(Self::Applied),
            rpc::metadata_proposal_outcome::Kind::AlreadyApplied => Ok(Self::AlreadyApplied),
            rpc::metadata_proposal_outcome::Kind::ClientRegistered => {
                if proto.session_epoch == 0 {
                    return Err("client registration outcome has zero session epoch");
                }
                Ok(Self::ClientRegistered {
                    session_epoch: proto.session_epoch,
                })
            }
            rpc::metadata_proposal_outcome::Kind::ClientRenewed => Ok(Self::ClientRenewed),
            rpc::metadata_proposal_outcome::Kind::TableCreated => {
                if proto.table_id == 0 || proto.tablet_id == 0 || proto.raft_group_id == 0 {
                    return Err("table-created outcome contains a zero identity");
                }
                Ok(Self::TableCreated {
                    table_id: TableId(proto.table_id),
                    tablet_id: TabletId(proto.tablet_id),
                    raft_group_id: RaftGroupId(proto.raft_group_id),
                })
            }
            rpc::metadata_proposal_outcome::Kind::TimestampsReserved => {
                if proto.timestamp_reserved_until == 0 {
                    return Err("timestamp reservation outcome has zero frontier");
                }
                Ok(Self::TimestampsReserved {
                    reserved_from: Timestamp(proto.timestamp_reserved_from),
                    reserved_until: Timestamp(proto.timestamp_reserved_until),
                })
            }
            rpc::metadata_proposal_outcome::Kind::TimestampReservationRegressed => {
                if proto.timestamp_current == 0 || proto.timestamp_received == 0 {
                    return Err("timestamp regression outcome contains a zero frontier");
                }
                Ok(Self::TimestampReservationRegressed {
                    current: Timestamp(proto.timestamp_current),
                    received: Timestamp(proto.timestamp_received),
                })
            }
            rpc::metadata_proposal_outcome::Kind::Rejected => Ok(Self::Rejected {
                reason: proto.rejection,
            }),
            rpc::metadata_proposal_outcome::Kind::Unspecified => {
                Err("unspecified metadata proposal outcome")
            }
        }
    }
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
            MetadataRequest::ProposeCommand(request) => Some(
                rpc::metadata_request::Request::ProposeCommand(rpc::MetadataProposalRequest {
                    rpc_attempt_id: None,
                    request_id: Some(request.request_id.to_proto()),
                    command_envelope: request.command_envelope.clone(),
                }),
            ),
            MetadataRequest::ProposeConfChange(request) => Some(
                rpc::metadata_request::Request::ProposeConfChange(rpc::MetadataConfChangeRequest {
                    rpc_attempt_id: None,
                    expected_conf_state_version: request.expected_conf_state_version,
                    replica_id: Some(request.replica_id.to_proto()),
                    remove_replica: request.remove_replica,
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
            Some(rpc::metadata_request::Request::ProposeCommand(req)) => {
                Ok(MetadataRequest::ProposeCommand(MetadataProposalRequest {
                    request_id: RequestId::from_proto(
                        req.request_id
                            .ok_or("missing metadata proposal request_id")?,
                    )?,
                    command_envelope: req.command_envelope,
                }))
            }
            Some(rpc::metadata_request::Request::ProposeConfChange(req)) => {
                let replica_id = ReplicaId::from_proto(
                    req.replica_id
                        .ok_or("missing metadata ConfChange replica_id")?,
                );
                if req.expected_conf_state_version == 0 || replica_id.0 == 0 {
                    return Err("metadata ConfChange identity must be non-zero");
                }
                Ok(MetadataRequest::ProposeConfChange(
                    MetadataConfChangeRequest {
                        expected_conf_state_version: req.expected_conf_state_version,
                        replica_id,
                        remove_replica: req.remove_replica,
                    },
                ))
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
    pub replicas: Arc<[ReplicaRoute]>,
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
    #[error("tablet route has no cached leader hint")]
    LeaderUnknown,
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
        for route in self.replicas.iter() {
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
    leader_hints: BTreeMap<TabletId, Option<ReplicaId>>,
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
        let mut replacement_hints = BTreeMap::new();
        for route in routes {
            route.validate()?;
            let tablet_id = route.tablet_id;
            let leader_replica_id = route.leader_replica_id;
            if replacement.insert(tablet_id, route).is_some() {
                return Err(TabletRouteError::DuplicateTablet(tablet_id));
            }
            replacement_hints.insert(tablet_id, Some(leader_replica_id));
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
        self.leader_hints = replacement_hints;
        Ok(())
    }

    pub fn insert(&mut self, route: TabletRoute) -> Result<(), TabletRouteError> {
        route.validate()?;
        let tablet_id = route.tablet_id;
        let leader_replica_id = route.leader_replica_id;
        self.routes.insert(tablet_id, route);
        self.leader_hints.insert(tablet_id, Some(leader_replica_id));
        Ok(())
    }

    /// Return the immutable metadata route snapshot for a tablet. Its
    /// `leader_replica_id` is the snapshot's seed hint; callers that need the
    /// current cache state must use [`Self::leader_hint`] or
    /// [`Self::leader_node`].
    pub fn get(&self, tablet_id: TabletId) -> Option<&TabletRoute> {
        self.routes.get(&tablet_id)
    }

    pub fn node_for_replica(&self, tablet_id: TabletId, replica_id: ReplicaId) -> Option<NodeId> {
        self.get(tablet_id)?.node_for_replica(replica_id)
    }

    /// Return the current soft leader hint without exposing the immutable
    /// route snapshot. `None` means the cache deliberately has no usable
    /// leader hint after a failed or stale request.
    pub fn leader_hint(&self, tablet_id: TabletId) -> Result<Option<ReplicaId>, TabletRouteError> {
        self.leader_hints
            .get(&tablet_id)
            .copied()
            .ok_or(TabletRouteError::MissingTablet(tablet_id))
    }

    /// Update the soft leader hint from an authoritative status or metadata
    /// refresh. Placement remains unchanged and an unknown replica can never
    /// be promoted. Responses from a possibly stale request should use
    /// [`Self::update_leader_if_current`] instead.
    pub fn update_leader(
        &mut self,
        tablet_id: TabletId,
        leader_replica_id: ReplicaId,
    ) -> Result<(), TabletRouteError> {
        let route = self
            .routes
            .get(&tablet_id)
            .ok_or(TabletRouteError::MissingTablet(tablet_id))?;
        validate_leader_replica(route, leader_replica_id)?;
        self.leader_hints.insert(tablet_id, Some(leader_replica_id));
        Ok(())
    }

    /// Apply a leader hint only if the cache still contains the expected
    /// previous hint. This compare-and-update boundary prevents a delayed
    /// `NotLeader` response from overwriting a newer status or response hint.
    /// Both the rejected replica and the replacement hint must belong to the
    /// immutable route; a rejected update leaves all cache state unchanged.
    pub fn update_leader_if_current(
        &mut self,
        tablet_id: TabletId,
        expected_leader_replica_id: Option<ReplicaId>,
        leader_replica_id: Option<ReplicaId>,
    ) -> Result<bool, TabletRouteError> {
        let route = self
            .routes
            .get(&tablet_id)
            .ok_or(TabletRouteError::MissingTablet(tablet_id))?;
        if let Some(expected_leader_replica_id) = expected_leader_replica_id {
            validate_leader_replica(route, expected_leader_replica_id)?;
        }
        if let Some(leader_replica_id) = leader_replica_id {
            validate_leader_replica(route, leader_replica_id)?;
        }

        let current = self
            .leader_hints
            .get(&tablet_id)
            .copied()
            .ok_or(TabletRouteError::MissingTablet(tablet_id))?;
        if current != expected_leader_replica_id {
            return Ok(false);
        }

        self.leader_hints.insert(tablet_id, leader_replica_id);
        Ok(true)
    }

    /// Invalidate a hint only when the rejected replica is still the cached
    /// hint. A delayed response from an older attempt therefore becomes a
    /// no-op instead of erasing a newer leader observation.
    pub fn invalidate_leader(
        &mut self,
        tablet_id: TabletId,
        rejected_leader_replica_id: ReplicaId,
    ) -> Result<bool, TabletRouteError> {
        self.update_leader_if_current(tablet_id, Some(rejected_leader_replica_id), None)
    }

    pub fn leader_node(&self, tablet_id: TabletId) -> Result<NodeId, TabletRouteError> {
        let route = self
            .routes
            .get(&tablet_id)
            .ok_or(TabletRouteError::MissingTablet(tablet_id))?;
        let leader_replica_id = self
            .leader_hint(tablet_id)?
            .ok_or(TabletRouteError::LeaderUnknown)?;
        route
            .node_for_replica(leader_replica_id)
            .ok_or(TabletRouteError::LeaderAbsent(leader_replica_id))
    }

    pub fn remove(&mut self, tablet_id: TabletId) -> Option<TabletRoute> {
        let route = self.routes.remove(&tablet_id);
        self.leader_hints.remove(&tablet_id);
        route
    }

    pub fn len(&self) -> usize {
        self.routes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

fn validate_leader_replica(
    route: &TabletRoute,
    leader_replica_id: ReplicaId,
) -> Result<(), TabletRouteError> {
    if leader_replica_id.0 == 0 {
        return Err(TabletRouteError::ZeroLeaderReplicaId);
    }
    if route.node_for_replica(leader_replica_id).is_none() {
        return Err(TabletRouteError::LeaderAbsent(leader_replica_id));
    }
    Ok(())
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
    ProposeCommand {
        request_id: RequestId,
        success: bool,
        error_code: String,
        error_message: String,
        outcome: Option<MetadataProposalOutcome>,
        leader_replica_id: Option<ReplicaId>,
    },
    ProposeConfChange {
        success: bool,
        error_code: String,
        error_message: String,
        leader_replica_id: Option<ReplicaId>,
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
                    replicas: replicas.clone().into(),
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
            MetadataResponse::ProposeCommand {
                request_id,
                success,
                error_code,
                error_message,
                outcome,
                leader_replica_id,
            } => Some(rpc::metadata_response::Response::ProposeCommand(
                rpc::MetadataProposalResponse {
                    rpc_attempt_id: None,
                    request_id: Some(request_id.to_proto()),
                    success: *success,
                    error_code: error_code.clone(),
                    error_message: error_message.clone(),
                    outcome: outcome
                        .as_ref()
                        .map(|outcome| outcome.to_proto().encode_to_vec())
                        .unwrap_or_default(),
                    leader_replica_id: leader_replica_id.map(|id| id.0).unwrap_or(0),
                },
            )),
            MetadataResponse::ProposeConfChange {
                success,
                error_code,
                error_message,
                leader_replica_id,
            } => Some(rpc::metadata_response::Response::ProposeConfChange(
                rpc::MetadataConfChangeResponse {
                    rpc_attempt_id: None,
                    success: *success,
                    error_code: error_code.clone(),
                    error_message: error_message.clone(),
                    leader_replica_id: leader_replica_id.map(|id| id.0).unwrap_or(0),
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
                    replicas: replicas.clone().into(),
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
            Some(rpc::metadata_response::Response::ProposeCommand(resp)) => {
                let outcome = if resp.outcome.is_empty() {
                    None
                } else {
                    Some(MetadataProposalOutcome::from_proto(
                        rpc::MetadataProposalOutcome::decode(resp.outcome.as_slice())
                            .map_err(|_| "invalid metadata proposal outcome")?,
                    )?)
                };
                Ok(MetadataResponse::ProposeCommand {
                    request_id: RequestId::from_proto(
                        resp.request_id
                            .ok_or("missing metadata proposal response request_id")?,
                    )?,
                    success: resp.success,
                    error_code: resp.error_code,
                    error_message: resp.error_message,
                    outcome,
                    leader_replica_id: (resp.leader_replica_id != 0)
                        .then_some(ReplicaId(resp.leader_replica_id)),
                })
            }
            Some(rpc::metadata_response::Response::ProposeConfChange(resp)) => {
                Ok(MetadataResponse::ProposeConfChange {
                    success: resp.success,
                    error_code: resp.error_code,
                    error_message: resp.error_message,
                    leader_replica_id: (resp.leader_replica_id != 0)
                        .then_some(ReplicaId(resp.leader_replica_id)),
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
            deadline_remaining_ms: Some(123),
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
            rpc_attempt_id: None,
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
    fn metadata_conf_change_roundtrip_preserves_removal_witness() {
        let request = MetadataRequest::ProposeConfChange(MetadataConfChangeRequest {
            expected_conf_state_version: 9,
            replica_id: ReplicaId(4),
            remove_replica: true,
        });
        let decoded = MetadataRequest::from_proto(request.to_proto()).unwrap();
        assert!(matches!(
            decoded,
            MetadataRequest::ProposeConfChange(MetadataConfChangeRequest {
                expected_conf_state_version: 9,
                replica_id: ReplicaId(4),
                remove_replica: true,
            })
        ));

        let response = MetadataResponse::ProposeConfChange {
            success: false,
            error_code: "NOT_LEADER".to_string(),
            error_message: "leader is elsewhere".to_string(),
            leader_replica_id: Some(ReplicaId(2)),
        };
        let decoded = MetadataResponse::from_proto(response.to_proto()).unwrap();
        assert!(matches!(
            decoded,
            MetadataResponse::ProposeConfChange {
                success: false,
                leader_replica_id: Some(ReplicaId(2)),
                ..
            }
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
            }]
            .into(),
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
            }]
            .into(),
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
                ]
                .into(),
            })
            .unwrap();

        cache.update_leader(TabletId(10), ReplicaId(12)).unwrap();
        assert_eq!(cache.leader_node(TabletId(10)), Ok(NodeId(2)));
        assert_eq!(
            cache.get(TabletId(10)).unwrap().leader_replica_id,
            ReplicaId(11)
        );
        assert_eq!(
            cache.update_leader(TabletId(10), ReplicaId(99)),
            Err(TabletRouteError::LeaderAbsent(ReplicaId(99)))
        );
        assert_eq!(cache.leader_node(TabletId(10)), Ok(NodeId(2)));
    }

    /// Realistic bug caught: a delayed `NotLeader` response from an older
    /// attempt could invalidate or replace a newer leader hint. The cache
    /// must apply a response only when the rejected replica is still the
    /// current hint, while retaining immutable route identity and placement.
    #[test]
    fn tablet_route_cache_applies_not_leader_updates_only_to_current_hint() {
        let mut cache = TabletRouteCache::new();
        let route = TabletRoute {
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
            ]
            .into(),
        };
        cache.insert(route.clone()).unwrap();

        assert_eq!(
            cache.update_leader_if_current(TabletId(10), Some(ReplicaId(11)), Some(ReplicaId(12)),),
            Ok(true)
        );
        assert_eq!(cache.leader_hint(TabletId(10)), Ok(Some(ReplicaId(12))));

        assert_eq!(
            cache.update_leader_if_current(TabletId(10), Some(ReplicaId(11)), Some(ReplicaId(13)),),
            Ok(false)
        );
        assert_eq!(cache.leader_hint(TabletId(10)), Ok(Some(ReplicaId(12))));

        assert_eq!(
            cache.update_leader_if_current(TabletId(10), Some(ReplicaId(12)), Some(ReplicaId(99)),),
            Err(TabletRouteError::LeaderAbsent(ReplicaId(99)))
        );
        assert_eq!(cache.leader_hint(TabletId(10)), Ok(Some(ReplicaId(12))));

        assert_eq!(
            cache.invalidate_leader(TabletId(10), ReplicaId(12)),
            Ok(true)
        );
        assert_eq!(cache.leader_hint(TabletId(10)), Ok(None));
        assert_eq!(
            cache.leader_node(TabletId(10)),
            Err(TabletRouteError::LeaderUnknown)
        );

        assert_eq!(
            cache.invalidate_leader(TabletId(10), ReplicaId(11)),
            Ok(false)
        );
        let cached_route = cache.get(TabletId(10)).unwrap();
        assert_eq!(cached_route.raft_group_id, route.raft_group_id);
        assert_eq!(cached_route.tablet_id, route.tablet_id);
        assert_eq!(cached_route.tablet_epoch, route.tablet_epoch);
        assert_eq!(cached_route.replicas, route.replicas);
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
        assert_eq!(MessageType::TabletScanRequest.wire_value(), 0x08);
        assert_eq!(
            MessageType::from_wire_value(0x05),
            Ok(MessageType::MetadataResponse)
        );
        assert!(MessageType::from_wire_value(0xff).is_err());
    }

    #[test]
    fn tablet_scan_request_and_batch_roundtrip_preserves_snapshot_progress_and_caps() {
        let request = TabletScanRequest {
            request_id: RequestId {
                client_id: 77,
                sequence: 12,
                raft_group_id: RaftGroupId(8),
            },
            tablet_id: TabletId(12),
            tablet_epoch: 9,
            start_key: Some(b"a".to_vec()),
            end_key: Some(b"z".to_vec()),
            resume_after: Some(b"m".to_vec()),
            read_timestamp: Timestamp(100),
            max_rows: 2,
            max_bytes: 64,
            rpc_attempt_id: Some(41),
            deadline_remaining_ms: Some(456),
        };
        let batch = TabletScanBatch {
            rows: vec![
                TabletScanRow {
                    key: b"n".to_vec(),
                    row: b"row-n".to_vec(),
                },
                TabletScanRow {
                    key: b"o".to_vec(),
                    row: b"row-o".to_vec(),
                },
            ],
            next_resume_after: Some(b"o".to_vec()),
            exhausted: false,
        };

        let decoded_request = TabletScanRequest::from_proto(request.to_proto()).unwrap();
        let decoded_batch = TabletScanBatch::from_proto(batch.to_proto()).unwrap();

        assert_eq!(decoded_request, request);
        assert_eq!(decoded_batch, batch);
        assert!(decoded_batch.validate_for(&decoded_request).is_ok());
    }

    #[test]
    fn tablet_scan_request_rejects_non_exclusive_or_unbounded_progress() {
        let request = rpc::TabletScanRequest {
            request_id: Some(
                RequestId {
                    client_id: 1,
                    sequence: 1,
                    raft_group_id: RaftGroupId(8),
                }
                .to_proto(),
            ),
            tablet_id: Some(TabletId(12).to_proto()),
            tablet_epoch: 1,
            start_key: Some(b"a".to_vec()),
            end_key: Some(b"z".to_vec()),
            resume_after: Some(b"z".to_vec()),
            read_timestamp: Some(Timestamp(10).to_proto()),
            max_rows: 1,
            max_bytes: 32,
            rpc_attempt_id: Some(1),
            deadline_remaining_ms: None,
        };

        assert_eq!(
            TabletScanRequest::from_proto(request),
            Err("scan resume_after must be strictly before end_key")
        );
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
