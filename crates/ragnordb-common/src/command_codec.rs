use super::catalog_codec::TableDefinition as TableDef;
use super::codec::{Row, WriteKind};
use crate::ids::{RaftGroupId, RequestId, TabletId, Timestamp, TxnId};
use prost::Message;
use std::collections::BTreeMap;

use crate::proto::command;
/// the tablet command envelope format accepted
pub const TABLET_COMMAND_ENVELOPE_VERSION: u32 = 2;

/// tablet state machine snapshot format
pub const TABLET_STATE_MACHINE_SNAPSHOT_VERSION: u32 = 2;

/// durable identity and routing metadata for one replicated tablet command
///
/// complete envelope is proposed to Raft. Keeping the request identity,
/// tablet identity, and expected epoch beside the payload ensures that live
/// apply and recovery replay make the same deduplication and stale-route
/// decisions
#[derive(Debug, Clone, PartialEq)]
pub struct TabletCommandEnvelope {
    pub format_version: u32,
    pub request_id: RequestId,
    pub tablet_id: TabletId,
    pub expected_epoch: u64,
    pub command: TabletCommand,
}

impl TabletCommandEnvelope {
    /// build a V1 envelope after validating its routing metadata and payload
    pub fn new(
        request_id: RequestId,
        tablet_id: TabletId,
        expected_epoch: u64,
        command: TabletCommand,
    ) -> Result<Self, TabletCommandEnvelopeError> {
        let envelope = Self {
            format_version: TABLET_COMMAND_ENVELOPE_VERSION,
            request_id,
            tablet_id,
            expected_epoch,
            command,
        };

        envelope.validate()?;
        Ok(envelope)
    }

    /// validate invariants required before an envelope enters the Raft log
    pub fn validate(&self) -> Result<(), TabletCommandEnvelopeError> {
        self.validate_metadata()?;

        self.command
            .to_proto()
            .map(|_| ())
            .map_err(TabletCommandEnvelopeError::InvalidCommand)
    }

    fn validate_metadata(&self) -> Result<(), TabletCommandEnvelopeError> {
        if self.format_version != TABLET_COMMAND_ENVELOPE_VERSION {
            return Err(TabletCommandEnvelopeError::UnsupportedVersion(
                self.format_version,
            ));
        }

        if self.tablet_id.0 == 0 {
            return Err(TabletCommandEnvelopeError::ZeroTabletId);
        }

        if self.expected_epoch == 0 {
            return Err(TabletCommandEnvelopeError::ZeroExpectedEpoch);
        }
        if self.request_id.client_id == 0 {
            return Err(TabletCommandEnvelopeError::InvalidRequestId(
                "client ID must be non-zero",
            ));
        }
        if self.request_id.sequence == 0 {
            return Err(TabletCommandEnvelopeError::InvalidRequestId(
                "request sequence must be non-zero",
            ));
        }
        if self.request_id.raft_group_id.0 == 0 {
            return Err(TabletCommandEnvelopeError::InvalidRequestId(
                "Raft group ID must be non-zero",
            ));
        }

        Ok(())
    }

    /// encode the complete durable proposal payload
    pub fn encode(&self) -> Result<Vec<u8>, TabletCommandEnvelopeError> {
        Ok(self.to_proto()?.encode_to_vec())
    }

    /// decode and validate bytes recovered from Raft storage
    pub fn decode(bytes: &[u8]) -> Result<Self, TabletCommandEnvelopeError> {
        let proto = command::TabletCommandEnvelope::decode(bytes)
            .map_err(|error| TabletCommandEnvelopeError::Decode(error.to_string()))?;

        Self::from_proto(proto)
    }

    pub fn to_proto(&self) -> Result<command::TabletCommandEnvelope, TabletCommandEnvelopeError> {
        self.validate_metadata()?;

        Ok(command::TabletCommandEnvelope {
            format_version: self.format_version,
            request_id: Some(self.request_id.to_proto()),
            tablet_id: Some(self.tablet_id.to_proto()),
            expected_epoch: self.expected_epoch,
            command: Some(
                self.command
                    .to_proto()
                    .map_err(TabletCommandEnvelopeError::InvalidCommand)?,
            ),
        })
    }

    pub fn from_proto(
        proto: command::TabletCommandEnvelope,
    ) -> Result<Self, TabletCommandEnvelopeError> {
        let envelope = Self {
            format_version: proto.format_version,
            request_id: RequestId::from_proto(
                proto
                    .request_id
                    .ok_or(TabletCommandEnvelopeError::MissingField("request_id"))?,
            )
            .map_err(TabletCommandEnvelopeError::InvalidRequestId)?,
            tablet_id: TabletId::from_proto(
                proto
                    .tablet_id
                    .ok_or(TabletCommandEnvelopeError::MissingField("tablet_id"))?,
            ),
            expected_epoch: proto.expected_epoch,
            command: TabletCommand::from_proto(
                proto
                    .command
                    .ok_or(TabletCommandEnvelopeError::MissingField("command"))?,
            )
            .map_err(TabletCommandEnvelopeError::InvalidCommand)?,
        };

        envelope.validate()?;
        Ok(envelope)
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum TabletCommandEnvelopeError {
    #[error("unsupported tablet command envelope version {0}")]
    UnsupportedVersion(u32),

    #[error("tablet command envelope contains the reserved tablet ID zero")]
    ZeroTabletId,

    #[error("tablet command envelope expected epoch must be non-zero")]
    ZeroExpectedEpoch,

    #[error("tablet command envelope is missing required field {0}")]
    MissingField(&'static str),

    #[error("invalid request ID: {0}")]
    InvalidRequestId(&'static str),

    #[error("invalid tablet command: {0}")]
    InvalidCommand(&'static str),

    #[error("cannot decode tablet command envelope: {0}")]
    Decode(String),
}

/// durable cached result for one successfully applied tablet command
///
/// every result has an explicit protobuf value because these values are stored
/// in replicated tablet snapshots and must retain their meaning across upgrades
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachedTabletCommandResult {
    Noop,
    SingleShardCommit,
    Prewrite,
    Commit,
    Rollback,
    ResolveIntent,
}

impl CachedTabletCommandResult {
    fn to_proto(self) -> command::CachedTabletCommandResult {
        match self {
            Self::Noop => command::CachedTabletCommandResult::Noop,
            Self::SingleShardCommit => command::CachedTabletCommandResult::SingleShardCommit,
            Self::Prewrite => command::CachedTabletCommandResult::Prewrite,
            Self::Commit => command::CachedTabletCommandResult::Commit,
            Self::Rollback => command::CachedTabletCommandResult::Rollback,
            Self::ResolveIntent => command::CachedTabletCommandResult::ResolveIntent,
        }
    }

    fn from_proto(
        result: command::CachedTabletCommandResult,
    ) -> Result<Self, TabletStateMachineSnapshotError> {
        match result {
            command::CachedTabletCommandResult::Noop => Ok(Self::Noop),
            command::CachedTabletCommandResult::SingleShardCommit => Ok(Self::SingleShardCommit),
            command::CachedTabletCommandResult::Prewrite => Ok(Self::Prewrite),
            command::CachedTabletCommandResult::Commit => Ok(Self::Commit),
            command::CachedTabletCommandResult::Rollback => Ok(Self::Rollback),
            command::CachedTabletCommandResult::ResolveIntent => Ok(Self::ResolveIntent),
            command::CachedTabletCommandResult::Unspecified => {
                Err(TabletStateMachineSnapshotError::UnspecifiedCachedResult)
            }
        }
    }
}

/// durable per client deduplication state within one Raft group
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientDeduplicationSnapshot {
    pub last_sequence_applied: u64,
    pub cached_outcome: CachedTabletCommandOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CachedTabletCommandOutcome {
    Applied(CachedTabletCommandResult),
    Rejected(CachedTabletCommandRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedTabletCommandRejection {
    pub kind: CachedTabletCommandRejectionKind,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachedTabletCommandRejectionKind {
    InvalidCommand,
    WriteConflict,
    UnsupportedCommand,
}

/// versioned snapshot of replicated tablet command metadata
///
/// the surrounding tablet snapshot owns MVCC bytes and Raft metadata. This
/// value preserves the tablet generation and retry state that must be restored
/// before post-snapshot commands are applied
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletStateMachineSnapshot {
    pub format_version: u32,
    pub tablet_id: TabletId,
    pub tablet_epoch: u64,
    pub raft_group_id: RaftGroupId,
    pub clients: BTreeMap<u128, ClientDeduplicationSnapshot>,
}

impl TabletStateMachineSnapshot {
    pub fn new(
        tablet_id: TabletId,
        tablet_epoch: u64,
        raft_group_id: RaftGroupId,
        clients: BTreeMap<u128, ClientDeduplicationSnapshot>,
    ) -> Result<Self, TabletStateMachineSnapshotError> {
        let snapshot = Self {
            format_version: TABLET_STATE_MACHINE_SNAPSHOT_VERSION,
            tablet_id,
            tablet_epoch,
            raft_group_id,
            clients,
        };

        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn validate(&self) -> Result<(), TabletStateMachineSnapshotError> {
        if self.format_version != TABLET_STATE_MACHINE_SNAPSHOT_VERSION {
            return Err(TabletStateMachineSnapshotError::UnsupportedVersion(
                self.format_version,
            ));
        }

        if self.tablet_id.0 == 0 {
            return Err(TabletStateMachineSnapshotError::ZeroTabletId);
        }

        if self.tablet_epoch == 0 {
            return Err(TabletStateMachineSnapshotError::ZeroTabletEpoch);
        }

        if self.raft_group_id.0 == 0 {
            return Err(TabletStateMachineSnapshotError::ZeroRaftGroupId);
        }

        if self
            .clients
            .values()
            .any(|client| client.last_sequence_applied == 0)
        {
            return Err(TabletStateMachineSnapshotError::ZeroRequestSequence);
        }

        Ok(())
    }

    /// encode a deterministic protobuf image for inclusion in a tablet
    /// snapshot. `BTreeMap` ordering fixes the repeated-client entry order
    pub fn encode(&self) -> Result<Vec<u8>, TabletStateMachineSnapshotError> {
        self.validate()?;
        Ok(self.to_proto().encode_to_vec())
    }

    /// decode and validate command metadata recovered from a tablet snapshot
    pub fn decode(bytes: &[u8]) -> Result<Self, TabletStateMachineSnapshotError> {
        let proto = command::TabletStateMachineSnapshot::decode(bytes)
            .map_err(|error| TabletStateMachineSnapshotError::Decode(error.to_string()))?;

        Self::from_proto(proto)
    }

    fn to_proto(&self) -> command::TabletStateMachineSnapshot {
        command::TabletStateMachineSnapshot {
            format_version: self.format_version,
            tablet_id: Some(self.tablet_id.to_proto()),
            tablet_epoch: self.tablet_epoch,
            raft_group_id: Some(self.raft_group_id.to_proto()),
            clients: self
                .clients
                .iter()
                .map(|(client_id, state)| command::ClientDeduplicationSnapshot {
                    last_request_id: Some(
                        RequestId {
                            client_id: *client_id,
                            sequence: state.last_sequence_applied,
                            raft_group_id: self.raft_group_id,
                        }
                        .to_proto(),
                    ),
                    cached_result: match &state.cached_outcome {
                        CachedTabletCommandOutcome::Applied(result) => result.to_proto() as i32,
                        CachedTabletCommandOutcome::Rejected(_) => {
                            command::CachedTabletCommandResult::Unspecified as i32
                        }
                    },
                    cached_rejection: match &state.cached_outcome {
                        CachedTabletCommandOutcome::Applied(_) => None,
                        CachedTabletCommandOutcome::Rejected(rejection) => Some(
                            command::CachedTabletCommandRejection {
                                kind: match rejection.kind {
                                    CachedTabletCommandRejectionKind::InvalidCommand => command::CachedTabletCommandRejectionKind::InvalidCommand,
                                    CachedTabletCommandRejectionKind::WriteConflict => command::CachedTabletCommandRejectionKind::WriteConflict,
                                    CachedTabletCommandRejectionKind::UnsupportedCommand => command::CachedTabletCommandRejectionKind::UnsupportedCommand,
                                } as i32,
                                reason: rejection.reason.clone(),
                            },
                        ),
                    },
                })
                .collect(),
        }
    }

    fn from_proto(
        proto: command::TabletStateMachineSnapshot,
    ) -> Result<Self, TabletStateMachineSnapshotError> {
        let tablet_id = TabletId::from_proto(
            proto
                .tablet_id
                .ok_or(TabletStateMachineSnapshotError::MissingField("tablet_id"))?,
        );
        let raft_group_id = RaftGroupId::from_proto(proto.raft_group_id.ok_or(
            TabletStateMachineSnapshotError::MissingField("raft_group_id"),
        )?);

        let mut clients = BTreeMap::new();

        for client in proto.clients {
            let request_id = RequestId::from_proto(client.last_request_id.ok_or(
                TabletStateMachineSnapshotError::MissingField("clients.last_request_id"),
            )?)
            .map_err(TabletStateMachineSnapshotError::InvalidRequestId)?;

            let cached_outcome = if let Some(rejection) = client.cached_rejection {
                if client.cached_result != command::CachedTabletCommandResult::Unspecified as i32 {
                    return Err(TabletStateMachineSnapshotError::MultipleCachedOutcomes);
                }
                let kind = match command::CachedTabletCommandRejectionKind::try_from(rejection.kind)
                    .map_err(|_| {
                        TabletStateMachineSnapshotError::InvalidCachedRejection(rejection.kind)
                    })? {
                    command::CachedTabletCommandRejectionKind::InvalidCommand => {
                        CachedTabletCommandRejectionKind::InvalidCommand
                    }
                    command::CachedTabletCommandRejectionKind::WriteConflict => {
                        CachedTabletCommandRejectionKind::WriteConflict
                    }
                    command::CachedTabletCommandRejectionKind::UnsupportedCommand => {
                        CachedTabletCommandRejectionKind::UnsupportedCommand
                    }
                    command::CachedTabletCommandRejectionKind::Unspecified => {
                        return Err(TabletStateMachineSnapshotError::InvalidCachedRejection(
                            rejection.kind,
                        ));
                    }
                };
                CachedTabletCommandOutcome::Rejected(CachedTabletCommandRejection {
                    kind,
                    reason: rejection.reason,
                })
            } else {
                CachedTabletCommandOutcome::Applied(CachedTabletCommandResult::from_proto(
                    command::CachedTabletCommandResult::try_from(client.cached_result).map_err(
                        |_| {
                            TabletStateMachineSnapshotError::InvalidCachedResult(
                                client.cached_result,
                            )
                        },
                    )?,
                )?)
            };

            if request_id.raft_group_id != raft_group_id {
                return Err(TabletStateMachineSnapshotError::RequestGroupMismatch {
                    snapshot: raft_group_id,
                    request: request_id.raft_group_id,
                });
            }

            if clients
                .insert(
                    request_id.client_id,
                    ClientDeduplicationSnapshot {
                        last_sequence_applied: request_id.sequence,
                        cached_outcome,
                    },
                )
                .is_some()
            {
                return Err(TabletStateMachineSnapshotError::DuplicateClient(
                    request_id.client_id,
                ));
            }
        }

        let snapshot = Self {
            format_version: proto.format_version,
            tablet_id,
            tablet_epoch: proto.tablet_epoch,
            raft_group_id,
            clients,
        };

        snapshot.validate()?;
        Ok(snapshot)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TabletStateMachineSnapshotError {
    #[error("unsupported tablet state-machine snapshot version {0}")]
    UnsupportedVersion(u32),

    #[error("tablet state-machine snapshot contains the reserved tablet ID zero")]
    ZeroTabletId,

    #[error("tablet state-machine snapshot epoch must be non-zero")]
    ZeroTabletEpoch,

    #[error("tablet state-machine snapshot contains reserved Raft group ID zero")]
    ZeroRaftGroupId,

    #[error("tablet snapshot group {snapshot:?} contains request from group {request:?}")]
    RequestGroupMismatch {
        snapshot: RaftGroupId,
        request: RaftGroupId,
    },

    #[error("tablet state-machine snapshot contains request sequence zero")]
    ZeroRequestSequence,

    #[error("tablet state-machine snapshot is missing required field {0}")]
    MissingField(&'static str),

    #[error("invalid request ID in tablet state-machine snapshot: {0}")]
    InvalidRequestId(&'static str),

    #[error("tablet state-machine snapshot contains duplicate client {0:#034x}")]
    DuplicateClient(u128),

    #[error("tablet state-machine snapshot contains unknown cached result {0}")]
    InvalidCachedResult(i32),

    #[error("tablet state-machine snapshot contains unknown cached rejection {0}")]
    InvalidCachedRejection(i32),

    #[error("tablet state-machine snapshot contains both a cached result and rejection")]
    MultipleCachedOutcomes,

    #[error("tablet state-machine snapshot contains an unspecified cached result")]
    UnspecifiedCachedResult,

    #[error("cannot decode tablet state-machine snapshot: {0}")]
    Decode(String),
}

/// replicated participant-wide prewrite for a distributed transaction
///
/// Put carries a complete SQL row. A Delete carries no row. Applying the
/// command atomically installs the default-value entry and corresponding lock
#[derive(Debug, Clone, PartialEq)]
pub struct PrewriteCommand {
    pub txn_id: TxnId,
    pub start_timestamp: Timestamp,
    pub writes: Vec<WriteEntry>,
    pub primary_key: Vec<u8>,
    pub ttl_ms: u64,
}

impl PrewriteCommand {
    pub fn to_proto(&self) -> Result<command::PrewriteCommand, &'static str> {
        validate_txn_start(self.txn_id, self.start_timestamp)?;
        validate_write_entries(&self.writes)?;
        if self.primary_key.is_empty() {
            return Err("prewrite primary key must not be empty");
        }
        if self.ttl_ms == 0 {
            return Err("prewrite lock TTL must be non-zero");
        }

        Ok(command::PrewriteCommand {
            txn_id: Some(self.txn_id.to_proto()),
            start_timestamp: Some(self.start_timestamp.to_proto()),
            primary_key: self.primary_key.clone(),
            ttl_ms: self.ttl_ms,
            writes: self
                .writes
                .iter()
                .map(WriteEntry::to_proto)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    pub fn from_proto(proto: command::PrewriteCommand) -> Result<Self, &'static str> {
        let writes = proto
            .writes
            .into_iter()
            .map(WriteEntry::from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        validate_write_entries(&writes)?;

        Ok(Self {
            txn_id: TxnId::from_proto(proto.txn_id.ok_or("missing txn_id")?),
            start_timestamp: Timestamp::from_proto(
                proto.start_timestamp.ok_or("missing start_timestamp")?,
            ),
            writes,
            primary_key: proto.primary_key,
            ttl_ms: proto.ttl_ms,
        })
    }
}

/// commit every key owned by one tablet participant.
///
/// this removes lock/{key}
#[derive(Debug, Clone, PartialEq)]
pub struct CommitCommand {
    pub txn_id: TxnId,
    pub start_timestamp: Timestamp,
    pub commit_timestamp: Timestamp,
    pub keys: Vec<Vec<u8>>,
}

impl CommitCommand {
    pub fn to_proto(&self) -> Result<command::CommitCommand, &'static str> {
        validate_txn_start(self.txn_id, self.start_timestamp)?;
        if self.commit_timestamp.0 <= self.start_timestamp.0 {
            return Err("commit timestamp must be greater than start timestamp");
        }
        validate_keys(&self.keys)?;
        Ok(command::CommitCommand {
            txn_id: Some(self.txn_id.to_proto()),
            start_timestamp: Some(self.start_timestamp.to_proto()),
            commit_timestamp: Some(self.commit_timestamp.to_proto()),
            keys: self.keys.clone(),
        })
    }

    pub fn from_proto(proto: command::CommitCommand) -> Result<Self, &'static str> {
        Ok(CommitCommand {
            txn_id: TxnId::from_proto(proto.txn_id.ok_or("missing txn_id")?),
            start_timestamp: Timestamp::from_proto(
                proto.start_timestamp.ok_or("missing start_timestamp")?,
            ),
            commit_timestamp: Timestamp::from_proto(
                proto.commit_timestamp.ok_or("missing commit_timestamp")?,
            ),
            keys: proto.keys,
        })
    }
}

/// roll back every key owned by one tablet participant
///
/// removes lock/{key} and writes a rollback record so
/// late prewrite or commot messages are ignored
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackCommand {
    pub txn_id: TxnId,
    pub start_timestamp: Timestamp,
    pub keys: Vec<Vec<u8>>,
}

impl RollbackCommand {
    pub fn to_proto(&self) -> Result<command::RollbackCommand, &'static str> {
        validate_txn_start(self.txn_id, self.start_timestamp)?;
        validate_keys(&self.keys)?;
        Ok(command::RollbackCommand {
            txn_id: Some(self.txn_id.to_proto()),
            start_timestamp: Some(self.start_timestamp.to_proto()),
            keys: self.keys.clone(),
        })
    }

    pub fn from_proto(proto: command::RollbackCommand) -> Result<Self, &'static str> {
        Ok(RollbackCommand {
            txn_id: TxnId::from_proto(proto.txn_id.ok_or("missing txn_id")?),
            start_timestamp: Timestamp::from_proto(
                proto.start_timestamp.ok_or("missing start_timestamp")?,
            ),
            keys: proto.keys,
        })
    }
}

/// one complete row mutation within a shard commit batch
#[derive(Debug, Clone, PartialEq)]
pub struct WriteEntry {
    pub key: Vec<u8>,
    pub row: Option<Row>,
    pub op: WriteKind,
}

impl WriteEntry {
    pub fn to_proto(&self) -> Result<command::WriteEntry, &'static str> {
        validate_command_mutation(self.op, self.row.as_ref())?;

        Ok(command::WriteEntry {
            key: self.key.clone(),
            op: self.op.to_proto() as i32,
            row: self.row.as_ref().map(Row::to_proto),
        })
    }

    pub fn from_proto(proto: command::WriteEntry) -> Result<Self, &'static str> {
        let op = WriteKind::from_proto(
            crate::proto::mvcc::WriteKind::try_from(proto.op).map_err(|_| "invalid op")?,
        )?;

        let row = proto.row.map(Row::from_proto).transpose()?;
        validate_command_mutation(op, row.as_ref())?;

        Ok(Self {
            key: proto.key,
            row,
            op,
        })
    }
}

fn validate_write_entries(writes: &[WriteEntry]) -> Result<(), &'static str> {
    if writes.is_empty() {
        return Err("participant command requires at least one write");
    }
    let mut keys = std::collections::BTreeSet::new();
    for write in writes {
        if write.key.is_empty() {
            return Err("participant command contains an empty row key");
        }
        validate_command_mutation(write.op, write.row.as_ref())?;
        if !keys.insert(write.key.as_slice()) {
            return Err("participant command contains a duplicate row key");
        }
    }
    Ok(())
}

fn validate_keys(keys: &[Vec<u8>]) -> Result<(), &'static str> {
    if keys.is_empty() {
        return Err("participant command requires at least one row key");
    }
    let mut unique = std::collections::BTreeSet::new();
    for key in keys {
        if key.is_empty() {
            return Err("participant command contains an empty row key");
        }
        if !unique.insert(key.as_slice()) {
            return Err("participant command contains a duplicate row key");
        }
    }
    Ok(())
}

fn validate_txn_start(txn_id: TxnId, start_timestamp: Timestamp) -> Result<(), &'static str> {
    if txn_id.0 == 0 {
        return Err("transaction ID must be non-zero");
    }
    if start_timestamp.0 == 0 {
        return Err("transaction start timestamp must be non-zero");
    }
    Ok(())
}

fn validate_command_mutation(op: WriteKind, row: Option<&Row>) -> Result<(), &'static str> {
    match (op, row) {
        (WriteKind::Put, Some(_)) | (WriteKind::Delete, None) => Ok(()),

        (WriteKind::Put, None) => Err("Put command requires a complete row"),

        (WriteKind::Delete, Some(_)) => Err("Delete command must not contain a row"),

        (WriteKind::Rollback, _) => Err("Rollback is not a valid write mutation payload"),
    }
}

/// Atomic commit for a single-tablet transaction
///
/// all writes, lock removals, and write creations happens in only
/// one raft proposed command. this is the optional path when all
/// keys live on the same tablet
#[derive(Debug, Clone, PartialEq)]
pub struct SingleShardCommitCommand {
    pub txn_id: TxnId,
    pub start_timestamp: Timestamp,
    pub commit_timestamp: Timestamp,
    pub writes: Vec<WriteEntry>,
}
impl SingleShardCommitCommand {
    pub fn to_proto(&self) -> Result<command::SingleShardCommitCommand, &'static str> {
        validate_txn_start(self.txn_id, self.start_timestamp)?;
        if self.commit_timestamp.0 <= self.start_timestamp.0 {
            return Err("commit timestamp must be greater than start timestamp");
        }
        validate_write_entries(&self.writes)?;
        Ok(command::SingleShardCommitCommand {
            txn_id: Some(self.txn_id.to_proto()),
            start_timestamp: Some(self.start_timestamp.to_proto()),
            commit_timestamp: Some(self.commit_timestamp.to_proto()),
            writes: self
                .writes
                .iter()
                .map(WriteEntry::to_proto)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    pub fn from_proto(proto: command::SingleShardCommitCommand) -> Result<Self, &'static str> {
        let writes = proto
            .writes
            .into_iter()
            .map(WriteEntry::from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SingleShardCommitCommand {
            txn_id: TxnId::from_proto(proto.txn_id.ok_or("missing txn_id")?),
            start_timestamp: Timestamp::from_proto(
                proto.start_timestamp.ok_or("missing start_timestamp")?,
            ),
            commit_timestamp: Timestamp::from_proto(
                proto.commit_timestamp.ok_or("missing commit_timestamp")?,
            ),
            writes,
        })
    }
}

/// Replicated command used to resolve an abandoned or completed intent.
///
/// A committed transaction must carry its commit timestamp so the tablet can
/// roll the intent forward. An aborted transaction must not carry one because
/// rollback creates no committed MVCC version.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolveIntentCommand {
    pub txn_id: TxnId,
    pub start_timestamp: Timestamp,
    pub keys: Vec<Vec<u8>>,
    pub resolved_status: crate::codec::TxnStatus,
    pub commit_timestamp: Option<Timestamp>,
}

impl ResolveIntentCommand {
    /// Convert a validated intent-resolution command to protobuf.
    pub fn to_proto(&self) -> Result<command::ResolveIntentCommand, &'static str> {
        validate_txn_start(self.txn_id, self.start_timestamp)?;
        validate_keys(&self.keys)?;
        validate_resolved_status(
            self.resolved_status,
            self.start_timestamp,
            self.commit_timestamp,
        )?;

        Ok(command::ResolveIntentCommand {
            txn_id: Some(self.txn_id.to_proto()),
            start_timestamp: Some(self.start_timestamp.to_proto()),
            keys: self.keys.clone(),
            resolved_status: self.resolved_status.to_proto() as i32,
            commit_timestamp: self.commit_timestamp.map(|timestamp| timestamp.to_proto()),
        })
    }

    /// Decode and validate an intent-resolution command.
    pub fn from_proto(proto: command::ResolveIntentCommand) -> Result<Self, &'static str> {
        let resolved_status = crate::codec::TxnStatus::from_proto(
            crate::proto::mvcc::TxnStatus::try_from(proto.resolved_status)
                .map_err(|_| "invalid resolved status")?,
        )?;

        let commit_timestamp = proto.commit_timestamp.map(Timestamp::from_proto);

        let start_timestamp =
            Timestamp::from_proto(proto.start_timestamp.ok_or("missing start_timestamp")?);

        validate_resolved_status(resolved_status, start_timestamp, commit_timestamp)?;

        Ok(Self {
            txn_id: TxnId::from_proto(proto.txn_id.ok_or("missing txn_id")?),
            start_timestamp,
            keys: proto.keys,
            resolved_status,
            commit_timestamp,
        })
    }
}

fn validate_resolved_status(
    status: crate::codec::TxnStatus,
    start_timestamp: Timestamp,
    commit_timestamp: Option<Timestamp>,
) -> Result<(), &'static str> {
    match (status, commit_timestamp) {
        (crate::codec::TxnStatus::Committed, Some(commit_timestamp))
            if commit_timestamp > start_timestamp =>
        {
            Ok(())
        }

        (crate::codec::TxnStatus::Committed, Some(_)) => {
            Err("committed intent resolution requires commit_timestamp \
                 greater than start_timestamp")
        }

        (crate::codec::TxnStatus::Aborted, None) => Ok(()),

        (crate::codec::TxnStatus::Committed, None) => {
            Err("committed intent resolution requires commit_timestamp")
        }

        (crate::codec::TxnStatus::Aborted, Some(_)) => {
            Err("aborted intent resolution must not contain \
                 commit_timestamp")
        }

        (crate::codec::TxnStatus::Pending, _) => Err("pending transaction cannot be resolved"),
    }
}

/// catalog change command, can be anything like CREATE TABLE
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableOperation {
    pub table_def: TableDef,
}

impl CreateTableOperation {
    pub fn to_proto(&self) -> command::CreateTableOperation {
        command::CreateTableOperation {
            table_definition: Some(self.table_def.to_proto()),
        }
    }

    pub fn from_proto(proto: command::CreateTableOperation) -> Result<Self, &'static str> {
        Ok(CreateTableOperation {
            table_def: TableDef::from_proto(
                proto.table_definition.ok_or("missing table_definition")?,
            )?,
        })
    }
}

/// A raft no-op commands that is used for linearizablle
/// read barrier
/// also used as a heartbeat/ping in the raft group
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogCommand {
    pub operation: CatalogOperation,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CatalogOperation {
    CreateTable(CreateTableOperation),
}

impl CatalogCommand {
    pub fn to_proto(&self) -> command::CatalogCommand {
        let operation = match &self.operation {
            CatalogOperation::CreateTable(op) => Some(
                command::catalog_command::Operation::CreateTable(op.to_proto()),
            ),
        };

        command::CatalogCommand { operation }
    }

    pub fn from_proto(proto: command::CatalogCommand) -> Result<Self, &'static str> {
        let operation = match proto.operation {
            Some(command::catalog_command::Operation::CreateTable(op)) => {
                CatalogOperation::CreateTable(CreateTableOperation::from_proto(op)?)
            }
            None => return Err("missing catalog operation"),
        };

        Ok(CatalogCommand { operation })
    }
}

/// A raft no-op command used for the linearizable read barrier
/// also used as a heartbear.ping in the raft group
#[derive(Debug, Clone, PartialEq)]
pub struct NoopCommand;

impl NoopCommand {
    pub fn to_proto(&self) -> command::NoopCommand {
        command::NoopCommand {}
    }

    pub fn from_proto(_proto: command::NoopCommand) -> Result<Self, &'static str> {
        Ok(NoopCommand)
    }
}

/// this is the most topp level enum for every command
/// a tablet's raft state machine can process
///
/// every variant must be:
///   - deterministic (same bytes → same state transition)
///   - serializable to protobuf
///   - idempotent (safe to apply twice under request dedup)
#[derive(Debug, Clone, PartialEq)]
pub enum TabletCommand {
    Prewrite(PrewriteCommand),
    Commit(CommitCommand),
    Rollback(RollbackCommand),
    SingleShardCommit(SingleShardCommitCommand),
    ResolveIntent(ResolveIntentCommand),
    Catalog(CatalogCommand),
    Noop(NoopCommand),
}

impl TabletCommand {
    pub fn to_proto(&self) -> Result<command::TabletCommand, &'static str> {
        let command = match self {
            TabletCommand::Prewrite(command) => Some(command::tablet_command::Command::Prewrite(
                command.to_proto()?,
            )),

            TabletCommand::Commit(command) => Some(command::tablet_command::Command::Commit(
                command.to_proto()?,
            )),
            TabletCommand::Rollback(command) => Some(command::tablet_command::Command::Rollback(
                command.to_proto()?,
            )),
            TabletCommand::SingleShardCommit(command) => Some(
                command::tablet_command::Command::SingleShardCommit(command.to_proto()?),
            ),
            TabletCommand::ResolveIntent(command) => Some(
                command::tablet_command::Command::ResolveIntent(command.to_proto()?),
            ),
            TabletCommand::Catalog(command) => Some(
                command::tablet_command::Command::CatalogUpdate(command.to_proto()),
            ),
            TabletCommand::Noop(command) => {
                Some(command::tablet_command::Command::Noop(command.to_proto()))
            }
        };

        Ok(command::TabletCommand { command })
    }

    pub fn from_proto(proto: command::TabletCommand) -> Result<Self, &'static str> {
        match proto.command {
            Some(command::tablet_command::Command::Prewrite(c)) => {
                Ok(TabletCommand::Prewrite(PrewriteCommand::from_proto(c)?))
            }
            Some(command::tablet_command::Command::Commit(c)) => {
                Ok(TabletCommand::Commit(CommitCommand::from_proto(c)?))
            }
            Some(command::tablet_command::Command::Rollback(c)) => {
                Ok(TabletCommand::Rollback(RollbackCommand::from_proto(c)?))
            }
            Some(command::tablet_command::Command::SingleShardCommit(c)) => Ok(
                TabletCommand::SingleShardCommit(SingleShardCommitCommand::from_proto(c)?),
            ),
            Some(command::tablet_command::Command::ResolveIntent(c)) => Ok(
                TabletCommand::ResolveIntent(ResolveIntentCommand::from_proto(c)?),
            ),
            Some(command::tablet_command::Command::CatalogUpdate(c)) => {
                Ok(TabletCommand::Catalog(CatalogCommand::from_proto(c)?))
            }
            Some(command::tablet_command::Command::Noop(c)) => {
                Ok(TabletCommand::Noop(NoopCommand::from_proto(c)?))
            }
            None => Err("missing tablet command"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::catalog_codec::{ColumnDefinition, DataType};
    use super::*;
    use crate::codec::{Row, TxnStatus, Value, WriteKind};
    use crate::ids::ColumnId;

    #[test]
    fn prewrite_command_roundtrip() {
        let command = PrewriteCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            writes: vec![WriteEntry {
                key: b"/table/1/pk/1".to_vec(),
                row: Some(Row {
                    values: vec![Value::Int(1), Value::Text("Ada".to_string())],
                }),
                op: WriteKind::Put,
            }],
            primary_key: b"/table/1/pk/1".to_vec(),
            ttl_ms: 30_000,
        };

        let proto = command.to_proto().unwrap();
        let decoded = PrewriteCommand::from_proto(proto).unwrap();

        assert_eq!(decoded.txn_id.0, 1);
        assert_eq!(decoded.start_timestamp.0, 100);
        assert_eq!(decoded.writes[0].row.as_ref().unwrap().values.len(), 2);
        assert_eq!(decoded.writes[0].op, WriteKind::Put);
    }

    #[test]
    fn commit_command_roundtrip() {
        let cmd = CommitCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            commit_timestamp: Timestamp(105),
            keys: vec![b"/table/1/pk/1".to_vec()],
        };
        let proto = cmd.to_proto().unwrap();
        let decoded = CommitCommand::from_proto(proto).unwrap();
        assert_eq!(decoded.commit_timestamp.0, 105);
    }

    #[test]
    fn rollback_command_roundtrip() {
        let cmd = RollbackCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            keys: vec![b"/table/1/pk/1".to_vec()],
        };
        let proto = cmd.to_proto().unwrap();
        let decoded = RollbackCommand::from_proto(proto).unwrap();
        assert_eq!(decoded.txn_id.0, 1);
    }

    #[test]
    fn write_entry_roundtrip() {
        let entry = WriteEntry {
            key: b"/table/1/pk/1".to_vec(),
            row: Some(Row {
                values: vec![Value::Int(42)],
            }),
            op: WriteKind::Put,
        };

        let proto = entry.to_proto().unwrap();
        let decoded = WriteEntry::from_proto(proto).unwrap();

        assert!(matches!(
            decoded.row.unwrap().values.as_slice(),
            [Value::Int(42)]
        ));
    }

    #[test]
    fn single_shard_commit_roundtrip() {
        let command = SingleShardCommitCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            commit_timestamp: Timestamp(110),
            writes: vec![
                WriteEntry {
                    key: b"/table/1/pk/1".to_vec(),
                    row: Some(Row {
                        values: vec![Value::Int(1), Value::Text("Ada".to_string())],
                    }),
                    op: WriteKind::Put,
                },
                WriteEntry {
                    key: b"/table/1/pk/2".to_vec(),
                    row: Some(Row {
                        values: vec![Value::Int(2), Value::Text("Bob".to_string())],
                    }),
                    op: WriteKind::Put,
                },
            ],
        };

        let proto = command.to_proto().unwrap();
        let decoded = SingleShardCommitCommand::from_proto(proto).unwrap();

        assert_eq!(decoded.writes.len(), 2);
    }

    #[test]
    fn resolve_intent_roundtrip() {
        let cmd = ResolveIntentCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            keys: vec![b"/table/1/pk/1".to_vec()],
            resolved_status: TxnStatus::Committed,
            commit_timestamp: Some(Timestamp(105)),
        };
        let proto = cmd.to_proto().unwrap();
        let decoded = ResolveIntentCommand::from_proto(proto).unwrap();
        assert!(matches!(decoded.resolved_status, TxnStatus::Committed));
    }

    #[test]
    fn create_table_operation_roundtrip() {
        let op = CreateTableOperation {
            table_def: TableDef {
                table_id: 100,
                name: "users".to_string(),
                columns: vec![ColumnDefinition {
                    column_id: ColumnId(1),
                    name: "id".to_string(),
                    ty: DataType::Int,
                    nullable: false,
                }],
                primary_key_column_ids: vec![ColumnId(1)],
                schema_version: 1,
                tablet_count: 4,
            },
        };
        let proto = op.to_proto();
        let decoded = CreateTableOperation::from_proto(proto).unwrap();
        assert_eq!(decoded.table_def.table_id, 100);
        assert_eq!(decoded.table_def.columns.len(), 1);
    }

    #[test]
    fn catalog_command_roundtrip() {
        let cmd = CatalogCommand {
            operation: CatalogOperation::CreateTable(CreateTableOperation {
                table_def: TableDef {
                    table_id: 200,
                    name: "orders".to_string(),
                    columns: vec![],
                    primary_key_column_ids: vec![ColumnId(1)],
                    schema_version: 1,
                    tablet_count: 2,
                },
            }),
        };
        let proto = cmd.to_proto();
        let decoded = CatalogCommand::from_proto(proto).unwrap();
        assert!(matches!(
            decoded.operation,
            CatalogOperation::CreateTable(_)
        ));
    }

    #[test]
    fn noop_command_roundtrip() {
        let cmd = NoopCommand;
        let proto = cmd.to_proto();
        let decoded = NoopCommand::from_proto(proto).unwrap();
        assert!(matches!(decoded, NoopCommand));
    }

    #[test]
    fn tablet_command_prewrite_roundtrip() {
        let cmd = TabletCommand::Prewrite(PrewriteCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            writes: vec![WriteEntry {
                key: b"/table/1/pk/1".to_vec(),
                row: Some(Row {
                    values: vec![Value::Int(1)],
                }),
                op: WriteKind::Put,
            }],
            primary_key: b"/table/1/pk/1".to_vec(),
            ttl_ms: 30_000,
        });
        let proto = cmd.to_proto().unwrap();
        let decoded = TabletCommand::from_proto(proto).unwrap();
        assert!(matches!(decoded, TabletCommand::Prewrite(_)));
    }

    #[test]
    fn tablet_command_commit_roundtrip() {
        let cmd = TabletCommand::Commit(CommitCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            commit_timestamp: Timestamp(105),
            keys: vec![b"/table/1/pk/1".to_vec()],
        });
        let proto = cmd.to_proto().unwrap();
        let decoded = TabletCommand::from_proto(proto).unwrap();
        assert!(matches!(decoded, TabletCommand::Commit(_)));
    }

    #[test]
    fn tablet_command_rollback_roundtrip() {
        let cmd = TabletCommand::Rollback(RollbackCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            keys: vec![b"/table/1/pk/1".to_vec()],
        });
        let proto = cmd.to_proto().unwrap();
        let decoded = TabletCommand::from_proto(proto).unwrap();
        assert!(matches!(decoded, TabletCommand::Rollback(_)));
    }

    #[test]
    fn tablet_command_noop_roundtrip() {
        let cmd = TabletCommand::Noop(NoopCommand);
        let proto = cmd.to_proto().unwrap();
        let decoded = TabletCommand::from_proto(proto).unwrap();
        assert!(matches!(decoded, TabletCommand::Noop(_)));
    }

    #[test]
    fn tablet_command_missing_rejected() {
        let proto = command::TabletCommand { command: None };
        assert!(TabletCommand::from_proto(proto).is_err());
    }

    #[test]
    fn aborted_intent_resolution_has_no_commit_timestamp() {
        let command = ResolveIntentCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            keys: vec![b"/table/1/pk/1".to_vec()],
            resolved_status: TxnStatus::Aborted,
            commit_timestamp: None,
        };

        let proto = command.to_proto().unwrap();
        let decoded = ResolveIntentCommand::from_proto(proto).unwrap();

        assert_eq!(decoded.commit_timestamp, None);
        assert!(matches!(decoded.resolved_status, TxnStatus::Aborted));
    }

    #[test]
    fn pending_transaction_cannot_be_resolved() {
        let command = ResolveIntentCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            keys: vec![b"/table/1/pk/1".to_vec()],
            resolved_status: TxnStatus::Pending,
            commit_timestamp: None,
        };

        let error = command.to_proto().unwrap_err();

        assert_eq!(error, "pending transaction cannot be resolved");
    }

    #[test]
    fn committed_resolution_requires_commit_timestamp() {
        let command = ResolveIntentCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            keys: vec![b"/table/1/pk/1".to_vec()],
            resolved_status: TxnStatus::Committed,
            commit_timestamp: None,
        };

        let error = command.to_proto().unwrap_err();

        assert_eq!(
            error,
            "committed intent resolution requires commit_timestamp"
        );
    }

    #[test]
    fn committed_resolution_requires_newer_timestamp() {
        let command = ResolveIntentCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            keys: vec![b"/table/1/pk/1".to_vec()],
            resolved_status: TxnStatus::Committed,
            commit_timestamp: Some(Timestamp(100)),
        };

        let error = command.to_proto().unwrap_err();

        assert!(error.contains("greater than start_timestamp"));
    }

    #[test]
    fn aborted_resolution_rejects_commit_timestamp() {
        let command = ResolveIntentCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            keys: vec![b"/table/1/pk/1".to_vec()],
            resolved_status: TxnStatus::Aborted,
            commit_timestamp: Some(Timestamp(105)),
        };

        let error = command.to_proto().unwrap_err();

        assert!(error.contains("must not contain commit_timestamp"));
    }

    #[test]
    fn tablet_command_envelope_roundtrip_preserves_apply_identity() {
        let envelope = TabletCommandEnvelope::new(
            RequestId {
                client_id: 0x8f4f_5692_3c11_4dc8_a53f_418a_62d3_97e1,
                sequence: 7,
                raft_group_id: RaftGroupId(12),
            },
            TabletId(41),
            3,
            TabletCommand::Noop(NoopCommand),
        )
        .unwrap();

        let encoded = envelope.encode().unwrap();
        let decoded = TabletCommandEnvelope::decode(&encoded).unwrap();

        assert_eq!(decoded, envelope);
    }

    #[test]
    fn tablet_command_envelope_rejects_unknown_format_version() {
        let envelope = TabletCommandEnvelope::new(
            RequestId {
                client_id: 0x62a6_26f5_7849_46ee_8329_c983_ec15_29f4,
                sequence: 1,
                raft_group_id: RaftGroupId(12),
            },
            TabletId(9),
            1,
            TabletCommand::Noop(NoopCommand),
        )
        .unwrap();

        let mut proto = envelope.to_proto().unwrap();
        proto.format_version = TABLET_COMMAND_ENVELOPE_VERSION + 1;

        let error = TabletCommandEnvelope::decode(&proto.encode_to_vec()).unwrap_err();

        assert_eq!(
            error,
            TabletCommandEnvelopeError::UnsupportedVersion(TABLET_COMMAND_ENVELOPE_VERSION + 1)
        );
    }
}
