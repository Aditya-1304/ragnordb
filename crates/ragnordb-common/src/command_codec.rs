use super::catalog_codec::TableDefinition as TableDef;
use super::codec::{Row, TxnStatus, TxnStatusRecord, WriteKind};
use crate::ids::{LogicalCommandId, RaftGroupId, RequestId, TabletId, Timestamp, TxnId};
use prost::Message;
use std::collections::{BTreeMap, BTreeSet};

use crate::proto::command;
/// the tablet command envelope format accepted
pub const TABLET_COMMAND_ENVELOPE_VERSION: u32 = 2;

/// Version of the bounded multi-command proposal envelope.
pub const TABLET_COMMAND_BATCH_ENVELOPE_VERSION: u32 = 1;

/// Keep one batch small enough that one Raft proposal does not monopolize the
/// owner reactor or turn a single malformed entry into a large recovery unit.
pub const MAX_TABLET_COMMAND_BATCH_COMMANDS: usize = 32;

/// Maximum encoded size accepted for a multi-command proposal.
pub const MAX_TABLET_COMMAND_BATCH_BYTES: usize = 1024 * 1024;

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
    /// Optional V2 identity. The legacy RequestId remains required on the
    /// compatibility wire path because Raft proposal correlation still uses
    /// its group-qualified form.
    pub logical_command_id: Option<LogicalCommandId>,
    /// Monotonic client acknowledgement watermark carried into the replicated
    /// command log. Applying it and compacting older outcomes must be one
    /// durable state-machine transition so expiry survives restart.
    pub acknowledged_through: Option<u64>,
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
            logical_command_id: None,
            acknowledged_through: None,
            command,
        };

        envelope.validate()?;
        Ok(envelope)
    }

    /// Build an envelope carrying a topology-independent V2 logical identity.
    pub fn new_with_logical_command_id(
        request_id: RequestId,
        logical_command_id: LogicalCommandId,
        tablet_id: TabletId,
        expected_epoch: u64,
        command: TabletCommand,
    ) -> Result<Self, TabletCommandEnvelopeError> {
        Self::new_with_logical_command_id_and_ack(
            request_id,
            logical_command_id,
            tablet_id,
            expected_epoch,
            None,
            command,
        )
    }

    /// Build a V2 envelope carrying the caller's acknowledged retry floor.
    pub fn new_with_logical_command_id_and_ack(
        request_id: RequestId,
        logical_command_id: LogicalCommandId,
        tablet_id: TabletId,
        expected_epoch: u64,
        acknowledged_through: Option<u64>,
        command: TabletCommand,
    ) -> Result<Self, TabletCommandEnvelopeError> {
        let envelope = Self {
            format_version: TABLET_COMMAND_ENVELOPE_VERSION,
            request_id,
            tablet_id,
            expected_epoch,
            logical_command_id: Some(logical_command_id),
            acknowledged_through,
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
        if let Some(logical_command_id) = self.logical_command_id {
            logical_command_id
                .validate()
                .map_err(TabletCommandEnvelopeError::InvalidLogicalCommandId)?;
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
            logical_command_id: self.logical_command_id.map(|id| id.to_proto()),
            acknowledged_through: self.acknowledged_through,
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
            logical_command_id: proto
                .logical_command_id
                .map(LogicalCommandId::from_proto)
                .transpose()
                .map_err(TabletCommandEnvelopeError::InvalidLogicalCommandId)?,
            acknowledged_through: proto.acknowledged_through,
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

    #[error("invalid logical command ID: {0}")]
    InvalidLogicalCommandId(&'static str),

    #[error("invalid tablet command: {0}")]
    InvalidCommand(&'static str),

    #[error("cannot decode tablet command envelope: {0}")]
    Decode(String),
}

/// A validated batch of complete tablet command envelopes.
///
/// Batching is intentionally defined above the individual command envelope,
/// rather than by concatenating protobuf payloads. This keeps recovery able to
/// validate every subcommand and lets proposal correlation retain one
/// topology-independent identity per subcommand.
#[derive(Debug, Clone, PartialEq)]
pub struct TabletCommandBatchEnvelope {
    pub format_version: u32,
    pub commands: Vec<TabletCommandEnvelope>,
}

impl TabletCommandBatchEnvelope {
    pub fn new(
        commands: Vec<TabletCommandEnvelope>,
    ) -> Result<Self, TabletCommandBatchEnvelopeError> {
        let batch = Self {
            format_version: TABLET_COMMAND_BATCH_ENVELOPE_VERSION,
            commands,
        };
        batch.validate()?;
        Ok(batch)
    }

    /// Validate all structural invariants before a batch enters Raft.
    pub fn validate(&self) -> Result<(), TabletCommandBatchEnvelopeError> {
        if self.format_version != TABLET_COMMAND_BATCH_ENVELOPE_VERSION {
            return Err(TabletCommandBatchEnvelopeError::UnsupportedVersion(
                self.format_version,
            ));
        }
        if self.commands.len() < 2 {
            return Err(TabletCommandBatchEnvelopeError::TooFewCommands(
                self.commands.len(),
            ));
        }
        if self.commands.len() > MAX_TABLET_COMMAND_BATCH_COMMANDS {
            return Err(TabletCommandBatchEnvelopeError::TooManyCommands(
                self.commands.len(),
            ));
        }

        let first = self
            .commands
            .first()
            .expect("minimum batch length was validated above");
        let target_tablet = first.tablet_id;
        let target_epoch = first.expected_epoch;
        let target_group = first.request_id.raft_group_id;
        let mut request_ids = BTreeSet::new();
        let mut logical_command_ids = BTreeSet::new();

        for (index, envelope) in self.commands.iter().enumerate() {
            envelope.validate().map_err(|source| {
                TabletCommandBatchEnvelopeError::InvalidEnvelope {
                    index,
                    reason: source.to_string(),
                }
            })?;

            if envelope.tablet_id != target_tablet {
                return Err(TabletCommandBatchEnvelopeError::MismatchedTablet {
                    expected: target_tablet,
                    received: envelope.tablet_id,
                });
            }
            if envelope.expected_epoch != target_epoch {
                return Err(TabletCommandBatchEnvelopeError::MismatchedEpoch {
                    expected: target_epoch,
                    received: envelope.expected_epoch,
                });
            }
            if envelope.request_id.raft_group_id != target_group {
                return Err(TabletCommandBatchEnvelopeError::MismatchedRaftGroup {
                    expected: target_group,
                    received: envelope.request_id.raft_group_id,
                });
            }
            if !envelope.command.is_batchable() {
                return Err(TabletCommandBatchEnvelopeError::NonBatchableCommand {
                    index,
                    command: envelope.command.kind_name(),
                });
            }
            if !request_ids.insert(envelope.request_id.clone()) {
                return Err(TabletCommandBatchEnvelopeError::DuplicateRequestId(
                    envelope.request_id.clone(),
                ));
            }
            if let Some(logical_command_id) = envelope.logical_command_id
                && !logical_command_ids.insert(logical_command_id)
            {
                return Err(TabletCommandBatchEnvelopeError::DuplicateLogicalCommandId(
                    logical_command_id,
                ));
            }
        }

        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, TabletCommandBatchEnvelopeError> {
        self.validate()?;
        let encoded = self.to_proto().encode_to_vec();
        if encoded.len() > MAX_TABLET_COMMAND_BATCH_BYTES {
            return Err(TabletCommandBatchEnvelopeError::TooLarge {
                encoded_bytes: encoded.len(),
                max_bytes: MAX_TABLET_COMMAND_BATCH_BYTES,
            });
        }
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, TabletCommandBatchEnvelopeError> {
        if bytes.len() > MAX_TABLET_COMMAND_BATCH_BYTES {
            return Err(TabletCommandBatchEnvelopeError::TooLarge {
                encoded_bytes: bytes.len(),
                max_bytes: MAX_TABLET_COMMAND_BATCH_BYTES,
            });
        }
        let proto = command::TabletCommandBatchEnvelope::decode(bytes)
            .map_err(|error| TabletCommandBatchEnvelopeError::Decode(error.to_string()))?;
        if proto.commands.len() > MAX_TABLET_COMMAND_BATCH_COMMANDS {
            return Err(TabletCommandBatchEnvelopeError::TooManyCommands(
                proto.commands.len(),
            ));
        }
        Self::from_proto(proto)
    }

    fn to_proto(&self) -> command::TabletCommandBatchEnvelope {
        command::TabletCommandBatchEnvelope {
            format_version: self.format_version,
            commands: self
                .commands
                .iter()
                .map(|command| {
                    command
                        .to_proto()
                        .expect("validated batch contains valid command envelopes")
                })
                .collect(),
        }
    }

    fn from_proto(
        proto: command::TabletCommandBatchEnvelope,
    ) -> Result<Self, TabletCommandBatchEnvelopeError> {
        let commands = proto
            .commands
            .into_iter()
            .enumerate()
            .map(|(index, command)| {
                TabletCommandEnvelope::from_proto(command).map_err(|source| {
                    TabletCommandBatchEnvelopeError::InvalidEnvelope {
                        index,
                        reason: source.to_string(),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let batch = Self {
            format_version: proto.format_version,
            commands,
        };
        batch.validate()?;
        Ok(batch)
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum TabletCommandBatchEnvelopeError {
    #[error("unsupported tablet command batch envelope version {0}")]
    UnsupportedVersion(u32),

    #[error("tablet command batch must contain at least two commands, received {0}")]
    TooFewCommands(usize),

    #[error("tablet command batch contains too many commands: {0}")]
    TooManyCommands(usize),

    #[error("tablet command batch command {index} is invalid: {reason}")]
    InvalidEnvelope { index: usize, reason: String },

    #[error("tablet command batch targets tablet {received:?}, expected {expected:?}")]
    MismatchedTablet {
        expected: TabletId,
        received: TabletId,
    },

    #[error("tablet command batch targets epoch {received}, expected {expected}")]
    MismatchedEpoch { expected: u64, received: u64 },

    #[error("tablet command batch targets Raft group {received:?}, expected {expected:?}")]
    MismatchedRaftGroup {
        expected: RaftGroupId,
        received: RaftGroupId,
    },

    #[error("tablet command batch command {index} ({command}) cannot be batched")]
    NonBatchableCommand { index: usize, command: &'static str },

    #[error("tablet command batch repeats request ID {0:?}")]
    DuplicateRequestId(RequestId),

    #[error("tablet command batch repeats logical command ID {0:?}")]
    DuplicateLogicalCommandId(LogicalCommandId),

    #[error("tablet command batch encodes to {encoded_bytes} bytes, maximum is {max_bytes}")]
    TooLarge {
        encoded_bytes: usize,
        max_bytes: usize,
    },

    #[error("cannot decode tablet command batch envelope: {0}")]
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
    PublishAbortedTransactionStatus,
    HeartbeatTransactionStatus,
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
            Self::PublishAbortedTransactionStatus => {
                command::CachedTabletCommandResult::PublishAbortedTransactionStatus
            }
            Self::HeartbeatTransactionStatus => {
                command::CachedTabletCommandResult::HeartbeatTransactionStatus
            }
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
            command::CachedTabletCommandResult::PublishAbortedTransactionStatus => {
                Ok(Self::PublishAbortedTransactionStatus)
            }
            command::CachedTabletCommandResult::HeartbeatTransactionStatus => {
                Ok(Self::HeartbeatTransactionStatus)
            }
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

impl CachedTabletCommandOutcome {
    /// Encode one retained outcome for the read-only outcome-query RPC. The
    /// query payload intentionally reuses the snapshot outcome representation
    /// so the same validation rules protect both recovery and live lookup.
    pub fn encode_for_outcome_query(&self) -> Result<Vec<u8>, TabletStateMachineSnapshotError> {
        Ok(outcome_to_proto(self).encode_to_vec())
    }

    /// Decode an outcome returned by a tablet owner without executing a new
    /// command. A malformed outcome is corruption, never an invitation to
    /// retry the mutation.
    pub fn decode_from_outcome_query(
        bytes: &[u8],
    ) -> Result<Self, TabletStateMachineSnapshotError> {
        let proto = command::LogicalCommandDeduplicationSnapshot::decode(bytes)
            .map_err(|error| TabletStateMachineSnapshotError::Decode(error.to_string()))?;
        outcome_from_proto(proto.cached_result, proto.cached_rejection)
    }
}

fn outcome_to_proto(
    outcome: &CachedTabletCommandOutcome,
) -> command::LogicalCommandDeduplicationSnapshot {
    command::LogicalCommandDeduplicationSnapshot {
        logical_command_id: None,
        cached_result: match outcome {
            CachedTabletCommandOutcome::Applied(result) => result.to_proto() as i32,
            CachedTabletCommandOutcome::Rejected(_) => {
                command::CachedTabletCommandResult::Unspecified as i32
            }
        },
        cached_rejection: match outcome {
            CachedTabletCommandOutcome::Applied(_) => None,
            CachedTabletCommandOutcome::Rejected(rejection) => {
                Some(command::CachedTabletCommandRejection {
                    kind: match rejection.kind {
                        CachedTabletCommandRejectionKind::InvalidCommand => {
                            command::CachedTabletCommandRejectionKind::InvalidCommand
                        }
                        CachedTabletCommandRejectionKind::WriteConflict => {
                            command::CachedTabletCommandRejectionKind::WriteConflict
                        }
                        CachedTabletCommandRejectionKind::UnsupportedCommand => {
                            command::CachedTabletCommandRejectionKind::UnsupportedCommand
                        }
                    } as i32,
                    reason: rejection.reason.clone(),
                })
            }
        },
    }
}

fn outcome_from_proto(
    cached_result: i32,
    cached_rejection: Option<command::CachedTabletCommandRejection>,
) -> Result<CachedTabletCommandOutcome, TabletStateMachineSnapshotError> {
    if let Some(rejection) = cached_rejection {
        if cached_result != command::CachedTabletCommandResult::Unspecified as i32 {
            return Err(TabletStateMachineSnapshotError::MultipleCachedOutcomes);
        }
        let kind = match command::CachedTabletCommandRejectionKind::try_from(rejection.kind)
            .map_err(|_| TabletStateMachineSnapshotError::InvalidCachedRejection(rejection.kind))?
        {
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
        return Ok(CachedTabletCommandOutcome::Rejected(
            CachedTabletCommandRejection {
                kind,
                reason: rejection.reason,
            },
        ));
    }

    Ok(CachedTabletCommandOutcome::Applied(
        CachedTabletCommandResult::from_proto(
            command::CachedTabletCommandResult::try_from(cached_result)
                .map_err(|_| TabletStateMachineSnapshotError::InvalidCachedResult(cached_result))?,
        )?,
    ))
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
/// value preserves the tablet generation, retry state, and durable transaction
/// decisions that must be restored before post-snapshot commands are applied
#[derive(Debug, Clone, PartialEq)]
pub struct TabletStateMachineSnapshot {
    pub format_version: u32,
    pub tablet_id: TabletId,
    pub tablet_epoch: u64,
    pub raft_group_id: RaftGroupId,
    pub clients: BTreeMap<u128, ClientDeduplicationSnapshot>,
    pub logical_commands: BTreeMap<LogicalCommandId, ClientDeduplicationSnapshot>,
    /// Durable compaction watermarks keyed by `(client_id, session_epoch)`.
    /// Retaining the watermark after deleting outcomes makes old retries fail
    /// closed after snapshot restore.
    pub logical_client_retry_horizons: BTreeMap<(u128, u64), u64>,
    /// Durable transaction decisions owned by this tablet's Raft state machine.
    /// Older snapshots omit these records and decode as an empty map.
    pub transaction_statuses: BTreeMap<TxnId, TxnStatusRecord>,
}

impl Eq for TabletStateMachineSnapshot {}

impl TabletStateMachineSnapshot {
    pub fn new(
        tablet_id: TabletId,
        tablet_epoch: u64,
        raft_group_id: RaftGroupId,
        clients: BTreeMap<u128, ClientDeduplicationSnapshot>,
    ) -> Result<Self, TabletStateMachineSnapshotError> {
        Self::new_with_logical_commands(
            tablet_id,
            tablet_epoch,
            raft_group_id,
            clients,
            BTreeMap::new(),
        )
    }

    pub fn new_with_logical_commands(
        tablet_id: TabletId,
        tablet_epoch: u64,
        raft_group_id: RaftGroupId,
        clients: BTreeMap<u128, ClientDeduplicationSnapshot>,
        logical_commands: BTreeMap<LogicalCommandId, ClientDeduplicationSnapshot>,
    ) -> Result<Self, TabletStateMachineSnapshotError> {
        Self::new_with_logical_commands_and_horizons(
            tablet_id,
            tablet_epoch,
            raft_group_id,
            clients,
            logical_commands,
            BTreeMap::new(),
        )
    }

    pub fn new_with_logical_commands_and_horizons(
        tablet_id: TabletId,
        tablet_epoch: u64,
        raft_group_id: RaftGroupId,
        clients: BTreeMap<u128, ClientDeduplicationSnapshot>,
        logical_commands: BTreeMap<LogicalCommandId, ClientDeduplicationSnapshot>,
        logical_client_retry_horizons: BTreeMap<(u128, u64), u64>,
    ) -> Result<Self, TabletStateMachineSnapshotError> {
        Self::new_with_logical_commands_and_horizons_and_transaction_statuses(
            tablet_id,
            tablet_epoch,
            raft_group_id,
            clients,
            logical_commands,
            logical_client_retry_horizons,
            BTreeMap::new(),
        )
    }

    pub fn new_with_logical_commands_and_horizons_and_transaction_statuses(
        tablet_id: TabletId,
        tablet_epoch: u64,
        raft_group_id: RaftGroupId,
        clients: BTreeMap<u128, ClientDeduplicationSnapshot>,
        logical_commands: BTreeMap<LogicalCommandId, ClientDeduplicationSnapshot>,
        logical_client_retry_horizons: BTreeMap<(u128, u64), u64>,
        transaction_statuses: BTreeMap<TxnId, TxnStatusRecord>,
    ) -> Result<Self, TabletStateMachineSnapshotError> {
        let snapshot = Self {
            format_version: TABLET_STATE_MACHINE_SNAPSHOT_VERSION,
            tablet_id,
            tablet_epoch,
            raft_group_id,
            clients,
            logical_commands,
            logical_client_retry_horizons,
            transaction_statuses,
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

        for logical_command_id in self.logical_commands.keys() {
            logical_command_id
                .validate()
                .map_err(TabletStateMachineSnapshotError::InvalidLogicalCommandId)?;
        }

        for (client_id, session_epoch) in self.logical_client_retry_horizons.keys() {
            if *client_id == 0 {
                return Err(TabletStateMachineSnapshotError::ZeroRetryHorizonClientId);
            }
            if *session_epoch == 0 {
                return Err(TabletStateMachineSnapshotError::ZeroRetryHorizonSessionEpoch);
            }
        }

        for (txn_id, status) in &self.transaction_statuses {
            status
                .validate()
                .map_err(TabletStateMachineSnapshotError::InvalidTxnStatus)?;
            if *txn_id != status.txn_id {
                return Err(TabletStateMachineSnapshotError::TxnStatusKeyMismatch {
                    key: *txn_id,
                    status: status.txn_id,
                });
            }
            let primary_tablet_id = status
                .primary_tablet_id()
                .map_err(TabletStateMachineSnapshotError::InvalidTxnStatus)?;
            if primary_tablet_id != self.tablet_id {
                return Err(TabletStateMachineSnapshotError::TxnStatusTabletMismatch {
                    txn_id: *txn_id,
                    snapshot_tablet_id: self.tablet_id,
                    status_tablet_id: primary_tablet_id,
                });
            }
        }

        Ok(())
    }

    /// encode a deterministic protobuf image for inclusion in a tablet
    /// snapshot. `BTreeMap` ordering fixes the repeated-client entry order
    pub fn encode(&self) -> Result<Vec<u8>, TabletStateMachineSnapshotError> {
        self.validate()?;
        Ok(self.to_proto()?.encode_to_vec())
    }

    /// decode and validate command metadata recovered from a tablet snapshot
    pub fn decode(bytes: &[u8]) -> Result<Self, TabletStateMachineSnapshotError> {
        let proto = command::TabletStateMachineSnapshot::decode(bytes)
            .map_err(|error| TabletStateMachineSnapshotError::Decode(error.to_string()))?;

        Self::from_proto(proto)
    }

    fn to_proto(
        &self,
    ) -> Result<command::TabletStateMachineSnapshot, TabletStateMachineSnapshotError> {
        Ok(command::TabletStateMachineSnapshot {
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
            logical_commands: self
                .logical_commands
                .iter()
                .map(|(logical_command_id, state)| {
                    command::LogicalCommandDeduplicationSnapshot {
                        logical_command_id: Some(logical_command_id.to_proto()),
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
                    }
                })
                .collect(),
            logical_client_retry_horizons: self
                .logical_client_retry_horizons
                .iter()
                .map(|((client_id, session_epoch), acknowledged_through)| {
                    command::LogicalClientRetryHorizon {
                        client_id: client_id.to_le_bytes().to_vec(),
                        session_epoch: *session_epoch,
                        acknowledged_through: *acknowledged_through,
                    }
                })
                .collect(),
            transaction_statuses: self
                .transaction_statuses
                .values()
                .map(|status| {
                    status
                        .to_proto()
                        .map_err(TabletStateMachineSnapshotError::InvalidTxnStatus)
                })
                .collect::<Result<Vec<_>, _>>()?,
        })
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

        let mut logical_commands = BTreeMap::new();
        for logical_command in proto.logical_commands {
            let logical_command_id =
                LogicalCommandId::from_proto(logical_command.logical_command_id.ok_or(
                    TabletStateMachineSnapshotError::MissingField(
                        "logical_commands.logical_command_id",
                    ),
                )?)
                .map_err(TabletStateMachineSnapshotError::InvalidLogicalCommandId)?;

            let cached_outcome = if let Some(rejection) = logical_command.cached_rejection {
                if logical_command.cached_result
                    != command::CachedTabletCommandResult::Unspecified as i32
                {
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
                    command::CachedTabletCommandResult::try_from(logical_command.cached_result)
                        .map_err(|_| {
                            TabletStateMachineSnapshotError::InvalidCachedResult(
                                logical_command.cached_result,
                            )
                        })?,
                )?)
            };

            if logical_commands
                .insert(
                    logical_command_id,
                    ClientDeduplicationSnapshot {
                        last_sequence_applied: 1,
                        cached_outcome,
                    },
                )
                .is_some()
            {
                return Err(TabletStateMachineSnapshotError::DuplicateLogicalCommand(
                    logical_command_id,
                ));
            }
        }

        let mut logical_client_retry_horizons = BTreeMap::new();
        for horizon in proto.logical_client_retry_horizons {
            if horizon.client_id.len() != 16 {
                return Err(TabletStateMachineSnapshotError::InvalidRetryHorizonClientId);
            }
            let client_id = u128::from_le_bytes(
                horizon
                    .client_id
                    .as_slice()
                    .try_into()
                    .expect("retry horizon client ID length was checked"),
            );
            if client_id == 0 {
                return Err(TabletStateMachineSnapshotError::ZeroRetryHorizonClientId);
            }
            if horizon.session_epoch == 0 {
                return Err(TabletStateMachineSnapshotError::ZeroRetryHorizonSessionEpoch);
            }
            if logical_client_retry_horizons
                .insert(
                    (client_id, horizon.session_epoch),
                    horizon.acknowledged_through,
                )
                .is_some()
            {
                return Err(TabletStateMachineSnapshotError::DuplicateRetryHorizon {
                    client_id,
                    session_epoch: horizon.session_epoch,
                });
            }
        }

        let mut transaction_statuses = BTreeMap::new();
        for status in proto.transaction_statuses {
            let status = TxnStatusRecord::from_proto(status)
                .map_err(TabletStateMachineSnapshotError::InvalidTxnStatus)?;
            let txn_id = status.txn_id;
            if transaction_statuses.insert(txn_id, status).is_some() {
                return Err(TabletStateMachineSnapshotError::DuplicateTxnStatus(txn_id));
            }
        }

        let snapshot = Self {
            format_version: proto.format_version,
            tablet_id,
            tablet_epoch: proto.tablet_epoch,
            raft_group_id,
            clients,
            logical_commands,
            logical_client_retry_horizons,
            transaction_statuses,
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

    #[error("tablet state-machine snapshot contains duplicate logical command {0:?}")]
    DuplicateLogicalCommand(LogicalCommandId),

    #[error("invalid logical command ID in tablet state-machine snapshot: {0}")]
    InvalidLogicalCommandId(&'static str),

    #[error("tablet state-machine snapshot contains unknown cached result {0}")]
    InvalidCachedResult(i32),

    #[error("tablet state-machine snapshot contains unknown cached rejection {0}")]
    InvalidCachedRejection(i32),

    #[error("tablet state-machine snapshot contains both a cached result and rejection")]
    MultipleCachedOutcomes,

    #[error("tablet snapshot contains a retry horizon for client ID zero")]
    ZeroRetryHorizonClientId,

    #[error("tablet snapshot contains a retry horizon with session epoch zero")]
    ZeroRetryHorizonSessionEpoch,

    #[error("tablet snapshot retry horizon client ID must be exactly 16 bytes")]
    InvalidRetryHorizonClientId,

    #[error(
        "tablet snapshot contains duplicate retry horizon for client {client_id:#034x}, session epoch {session_epoch}"
    )]
    DuplicateRetryHorizon { client_id: u128, session_epoch: u64 },

    #[error("tablet state-machine snapshot contains an unspecified cached result")]
    UnspecifiedCachedResult,

    #[error("tablet state-machine snapshot contains duplicate status for transaction {0:?}")]
    DuplicateTxnStatus(TxnId),

    #[error("tablet snapshot status map key {key:?} does not match record transaction {status:?}")]
    TxnStatusKeyMismatch { key: TxnId, status: TxnId },

    #[error(
        "transaction status {txn_id:?} is owned by tablet {status_tablet_id:?}, not snapshot tablet {snapshot_tablet_id:?}"
    )]
    TxnStatusTabletMismatch {
        txn_id: TxnId,
        snapshot_tablet_id: TabletId,
        status_tablet_id: TabletId,
    },

    #[error("invalid transaction status in tablet state-machine snapshot: {0}")]
    InvalidTxnStatus(&'static str),

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
    /// Present only on the primary participant so the pending decision and
    /// first primary intent enter the same Raft apply transition.
    pub pending_status: Option<TxnStatusRecord>,
}

impl PrewriteCommand {
    /// Validate the semantic metadata and complete mutation batch carried by
    /// a prewrite command before it crosses a Raft or recovery boundary.
    pub fn validate(&self) -> Result<(), &'static str> {
        validate_txn_start(self.txn_id, self.start_timestamp)?;
        validate_write_entries(&self.writes)?;
        if self.primary_key.is_empty() {
            return Err("prewrite primary key must not be empty");
        }
        if self.ttl_ms == 0 {
            return Err("prewrite lock TTL must be non-zero");
        }
        if let Some(status) = &self.pending_status {
            status.validate()?;
            if status.txn_id != self.txn_id
                || status.start_timestamp != self.start_timestamp
                || status.status != TxnStatus::Pending
                || status.commit_timestamp.is_some()
                || status.primary_key != self.primary_key
            {
                return Err("pending status does not match the primary prewrite");
            }
            if !self
                .writes
                .iter()
                .any(|write| write.key == status.primary_key)
            {
                return Err("primary prewrite must include the status record primary key");
            }
        }

        Ok(())
    }

    pub fn to_proto(&self) -> Result<command::PrewriteCommand, &'static str> {
        self.validate()?;

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
            pending_status: self
                .pending_status
                .as_ref()
                .map(TxnStatusRecord::to_proto)
                .transpose()?,
        })
    }

    pub fn from_proto(proto: command::PrewriteCommand) -> Result<Self, &'static str> {
        let txn_id = TxnId::from_proto(proto.txn_id.ok_or("missing txn_id")?);
        let start_timestamp =
            Timestamp::from_proto(proto.start_timestamp.ok_or("missing start_timestamp")?);
        let writes = proto
            .writes
            .into_iter()
            .map(WriteEntry::from_proto)
            .collect::<Result<Vec<_>, _>>()?;

        let command = Self {
            txn_id,
            start_timestamp,
            writes,
            primary_key: proto.primary_key,
            ttl_ms: proto.ttl_ms,
            pending_status: proto
                .pending_status
                .map(TxnStatusRecord::from_proto)
                .transpose()?,
        };
        command.validate()?;
        Ok(command)
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
    /// Present only on the primary participant; state-machine apply publishes
    /// this decision in the same transition that commits the primary intent.
    pub committed_status: Option<TxnStatusRecord>,
}

impl CommitCommand {
    /// Validate the transaction identity, timestamp relationship, and complete
    /// participant key batch before the command crosses a Raft or recovery
    /// boundary.
    pub fn validate(&self) -> Result<(), &'static str> {
        validate_txn_start(self.txn_id, self.start_timestamp)?;
        if self.commit_timestamp.0 <= self.start_timestamp.0 {
            return Err("commit timestamp must be greater than start timestamp");
        }
        validate_keys(&self.keys)?;
        if let Some(status) = &self.committed_status {
            status.validate()?;
            if status.txn_id != self.txn_id
                || status.start_timestamp != self.start_timestamp
                || status.commit_timestamp != Some(self.commit_timestamp)
                || status.status != TxnStatus::Committed
            {
                return Err("committed status does not match the primary commit");
            }
            if !self.keys.iter().any(|key| key == &status.primary_key) {
                return Err("primary commit must include the status record primary key");
            }
        }
        Ok(())
    }

    pub fn to_proto(&self) -> Result<command::CommitCommand, &'static str> {
        self.validate()?;
        Ok(command::CommitCommand {
            txn_id: Some(self.txn_id.to_proto()),
            start_timestamp: Some(self.start_timestamp.to_proto()),
            commit_timestamp: Some(self.commit_timestamp.to_proto()),
            keys: self.keys.clone(),
            committed_status: self
                .committed_status
                .as_ref()
                .map(TxnStatusRecord::to_proto)
                .transpose()?,
        })
    }

    pub fn from_proto(proto: command::CommitCommand) -> Result<Self, &'static str> {
        let command = CommitCommand {
            txn_id: TxnId::from_proto(proto.txn_id.ok_or("missing txn_id")?),
            start_timestamp: Timestamp::from_proto(
                proto.start_timestamp.ok_or("missing start_timestamp")?,
            ),
            commit_timestamp: Timestamp::from_proto(
                proto.commit_timestamp.ok_or("missing commit_timestamp")?,
            ),
            keys: proto.keys,
            committed_status: proto
                .committed_status
                .map(TxnStatusRecord::from_proto)
                .transpose()?,
        };
        command.validate()?;
        Ok(command)
    }
}

/// Publish the aborted decision after the coordinator has applied participant
/// rollback. The status authority performs no MVCC change for this command.
#[derive(Debug, Clone, PartialEq)]
pub struct PublishAbortedTransactionStatus {
    pub status_record: TxnStatusRecord,
}

impl PublishAbortedTransactionStatus {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.status_record.validate()?;
        if self.status_record.status != TxnStatus::Aborted
            || self.status_record.commit_timestamp.is_some()
        {
            return Err("abort publication requires an aborted status without commit timestamp");
        }
        Ok(())
    }

    pub fn to_proto(&self) -> Result<command::PublishAbortedTransactionStatus, &'static str> {
        self.validate()?;
        Ok(command::PublishAbortedTransactionStatus {
            status_record: Some(self.status_record.to_proto()?),
        })
    }

    pub fn from_proto(
        proto: command::PublishAbortedTransactionStatus,
    ) -> Result<Self, &'static str> {
        let command = Self {
            status_record: TxnStatusRecord::from_proto(
                proto.status_record.ok_or("missing status_record")?,
            )?,
        };
        command.validate()?;
        Ok(command)
    }
}

/// Fenced renewal of a pending transaction lease at its status authority.
///
/// The command carries both the observed and proposed status plus one fixed
/// wall-clock sample. Replicated apply therefore rejects stale heartbeats,
/// terminal decisions, and renewals submitted after the old lease expired.
#[derive(Debug, Clone, PartialEq)]
pub struct HeartbeatTransactionStatus {
    pub expected_status: TxnStatusRecord,
    pub next_status: TxnStatusRecord,
    pub now_ms: u64,
}

impl HeartbeatTransactionStatus {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.expected_status.validate()?;
        self.next_status.validate()?;
        if self.now_ms == 0 {
            return Err("heartbeat apply time must be non-zero");
        }
        if self.expected_status.status != TxnStatus::Pending
            || self.next_status.status != TxnStatus::Pending
            || self.expected_status.commit_timestamp.is_some()
            || self.next_status.commit_timestamp.is_some()
        {
            return Err("heartbeat requires pending status records");
        }
        if !same_transaction_status_identity(&self.expected_status, &self.next_status) {
            return Err("heartbeat changed transaction status identity");
        }
        let current_deadline = self
            .expected_status
            .lease_deadline_ms
            .ok_or("heartbeat requires an existing lease deadline")?;
        if self.now_ms >= current_deadline {
            return Err("heartbeat apply time is at or after the current lease deadline");
        }
        let next_deadline = self
            .next_status
            .lease_deadline_ms
            .ok_or("heartbeat requires a next lease deadline")?;
        if next_deadline < current_deadline {
            return Err("heartbeat lease deadline must not regress");
        }
        let previous_timestamp = self
            .expected_status
            .last_heartbeat_timestamp
            .unwrap_or(self.expected_status.start_timestamp);
        let next_timestamp = self
            .next_status
            .last_heartbeat_timestamp
            .ok_or("heartbeat requires a next heartbeat timestamp")?;
        if next_timestamp <= previous_timestamp {
            return Err("heartbeat timestamp must advance monotonically");
        }
        Ok(())
    }

    pub fn to_proto(&self) -> Result<command::HeartbeatTransactionStatus, &'static str> {
        self.validate()?;
        Ok(command::HeartbeatTransactionStatus {
            expected_status: Some(self.expected_status.to_proto()?),
            next_status: Some(self.next_status.to_proto()?),
            now_ms: self.now_ms,
        })
    }

    pub fn from_proto(proto: command::HeartbeatTransactionStatus) -> Result<Self, &'static str> {
        let heartbeat = Self {
            expected_status: TxnStatusRecord::from_proto(
                proto.expected_status.ok_or("missing expected_status")?,
            )?,
            next_status: TxnStatusRecord::from_proto(
                proto.next_status.ok_or("missing next_status")?,
            )?,
            now_ms: proto.now_ms,
        };
        heartbeat.validate()?;
        Ok(heartbeat)
    }
}

/// Publish a replicated abort decision only if the observed pending lease is
/// still the current, expired status record.
#[derive(Debug, Clone, PartialEq)]
pub struct ExpirePendingTransactionStatus {
    pub expected_status: TxnStatusRecord,
    pub now_ms: u64,
}

impl ExpirePendingTransactionStatus {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.expected_status.validate()?;
        if self.now_ms == 0 {
            return Err("expiry apply time must be non-zero");
        }
        if self.expected_status.status != TxnStatus::Pending
            || self.expected_status.commit_timestamp.is_some()
        {
            return Err("expiry requires a pending status record");
        }
        let deadline = self
            .expected_status
            .lease_deadline_ms
            .ok_or("expiry requires an existing lease deadline")?;
        if self.now_ms < deadline {
            return Err("expiry apply time precedes the pending lease deadline");
        }
        Ok(())
    }

    pub fn to_proto(&self) -> Result<command::ExpirePendingTransactionStatus, &'static str> {
        self.validate()?;
        Ok(command::ExpirePendingTransactionStatus {
            expected_status: Some(self.expected_status.to_proto()?),
            now_ms: self.now_ms,
        })
    }

    pub fn from_proto(
        proto: command::ExpirePendingTransactionStatus,
    ) -> Result<Self, &'static str> {
        let expiry = Self {
            expected_status: TxnStatusRecord::from_proto(
                proto.expected_status.ok_or("missing expected_status")?,
            )?,
            now_ms: proto.now_ms,
        };
        expiry.validate()?;
        Ok(expiry)
    }
}

fn same_transaction_status_identity(left: &TxnStatusRecord, right: &TxnStatusRecord) -> bool {
    left.txn_id == right.txn_id
        && left.start_timestamp == right.start_timestamp
        && left.primary_key == right.primary_key
        && left.participant_tablet_ids == right.participant_tablet_ids
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
    /// Validate the transaction identity and complete rollback key batch
    /// before the command crosses a Raft or recovery boundary.
    pub fn validate(&self) -> Result<(), &'static str> {
        validate_txn_start(self.txn_id, self.start_timestamp)?;
        validate_keys(&self.keys)
    }

    pub fn to_proto(&self) -> Result<command::RollbackCommand, &'static str> {
        self.validate()?;
        Ok(command::RollbackCommand {
            txn_id: Some(self.txn_id.to_proto()),
            start_timestamp: Some(self.start_timestamp.to_proto()),
            keys: self.keys.clone(),
        })
    }

    pub fn from_proto(proto: command::RollbackCommand) -> Result<Self, &'static str> {
        let command = RollbackCommand {
            txn_id: TxnId::from_proto(proto.txn_id.ok_or("missing txn_id")?),
            start_timestamp: Timestamp::from_proto(
                proto.start_timestamp.ok_or("missing start_timestamp")?,
            ),
            keys: proto.keys,
        };
        command.validate()?;
        Ok(command)
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
    PublishAbortedTransactionStatus(PublishAbortedTransactionStatus),
    HeartbeatTransactionStatus(HeartbeatTransactionStatus),
    ExpirePendingTransactionStatus(ExpirePendingTransactionStatus),
}

impl TabletCommand {
    /// Return whether this command may share one Raft entry with adjacent
    /// mutation commands for the same tablet. Catalog changes, barriers, and
    /// other ordering boundaries intentionally remain standalone entries.
    pub fn is_batchable(&self) -> bool {
        matches!(
            self,
            TabletCommand::Prewrite(_)
                | TabletCommand::Commit(_)
                | TabletCommand::Rollback(_)
                | TabletCommand::SingleShardCommit(_)
                | TabletCommand::ResolveIntent(_)
        )
    }

    fn kind_name(&self) -> &'static str {
        match self {
            TabletCommand::Prewrite(_) => "prewrite",
            TabletCommand::Commit(_) => "commit",
            TabletCommand::Rollback(_) => "rollback",
            TabletCommand::SingleShardCommit(_) => "single-shard-commit",
            TabletCommand::ResolveIntent(_) => "resolve-intent",
            TabletCommand::Catalog(_) => "catalog",
            TabletCommand::Noop(_) => "noop",
            TabletCommand::PublishAbortedTransactionStatus(_) => {
                "publish-aborted-transaction-status"
            }
            TabletCommand::HeartbeatTransactionStatus(_) => "heartbeat-transaction-status",
            TabletCommand::ExpirePendingTransactionStatus(_) => "expire-pending-transaction-status",
        }
    }

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
            TabletCommand::PublishAbortedTransactionStatus(command) => Some(
                command::tablet_command::Command::PublishAbortedTransactionStatus(
                    command.to_proto()?,
                ),
            ),
            TabletCommand::HeartbeatTransactionStatus(command) => Some(
                command::tablet_command::Command::HeartbeatTransactionStatus(command.to_proto()?),
            ),
            TabletCommand::ExpirePendingTransactionStatus(command) => Some(
                command::tablet_command::Command::ExpirePendingTransactionStatus(
                    command.to_proto()?,
                ),
            ),
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
            Some(command::tablet_command::Command::PublishAbortedTransactionStatus(c)) => {
                Ok(TabletCommand::PublishAbortedTransactionStatus(
                    PublishAbortedTransactionStatus::from_proto(c)?,
                ))
            }
            Some(command::tablet_command::Command::HeartbeatTransactionStatus(c)) => {
                Ok(TabletCommand::HeartbeatTransactionStatus(
                    HeartbeatTransactionStatus::from_proto(c)?,
                ))
            }
            Some(command::tablet_command::Command::ExpirePendingTransactionStatus(c)) => {
                Ok(TabletCommand::ExpirePendingTransactionStatus(
                    ExpirePendingTransactionStatus::from_proto(c)?,
                ))
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
    use crate::ids::{ClientRequestId, ColumnId, CommandKind, LogicalCommandId};

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
            pending_status: None,
        };

        let proto = command.to_proto().unwrap();
        let decoded = PrewriteCommand::from_proto(proto).unwrap();

        assert_eq!(decoded.txn_id.0, 1);
        assert_eq!(decoded.start_timestamp.0, 100);
        assert_eq!(decoded.writes[0].row.as_ref().unwrap().values.len(), 2);
        assert_eq!(decoded.writes[0].op, WriteKind::Put);
    }

    #[test]
    fn transaction_status_command_metadata_roundtrips() {
        let primary_key = b"/table/1/pk/1".to_vec();
        let pending = TxnStatusRecord {
            txn_id: TxnId(15),
            start_timestamp: Timestamp(100),
            commit_timestamp: None,
            status: TxnStatus::Pending,
            primary_key: primary_key.clone(),
            participant_tablet_ids: vec![7, 8],
            last_heartbeat_timestamp: None,
            lease_deadline_ms: None,
        };
        let prewrite = TabletCommand::Prewrite(PrewriteCommand {
            txn_id: pending.txn_id,
            start_timestamp: pending.start_timestamp,
            writes: vec![WriteEntry {
                key: primary_key.clone(),
                row: Some(Row {
                    values: vec![Value::Int(1)],
                }),
                op: WriteKind::Put,
            }],
            primary_key: primary_key.clone(),
            ttl_ms: 30_000,
            pending_status: Some(pending.clone()),
        });

        let decoded_prewrite = TabletCommand::from_proto(prewrite.to_proto().unwrap()).unwrap();
        assert_eq!(decoded_prewrite, prewrite);

        let committed = TxnStatusRecord {
            commit_timestamp: Some(Timestamp(110)),
            status: TxnStatus::Committed,
            ..pending.clone()
        };
        let commit = TabletCommand::Commit(CommitCommand {
            txn_id: committed.txn_id,
            start_timestamp: committed.start_timestamp,
            commit_timestamp: Timestamp(110),
            keys: vec![primary_key.clone()],
            committed_status: Some(committed.clone()),
        });
        let decoded_commit = TabletCommand::from_proto(commit.to_proto().unwrap()).unwrap();
        assert_eq!(decoded_commit, commit);

        let aborted = TxnStatusRecord {
            status: TxnStatus::Aborted,
            ..pending
        };
        let abort =
            TabletCommand::PublishAbortedTransactionStatus(PublishAbortedTransactionStatus {
                status_record: aborted,
            });
        assert_eq!(
            TabletCommand::from_proto(abort.to_proto().unwrap()).unwrap(),
            abort
        );
    }

    #[test]
    fn prewrite_from_proto_rejects_invalid_transaction_metadata() {
        let command = PrewriteCommand {
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
            pending_status: None,
        };

        let mut invalid_transaction = command.to_proto().unwrap();
        invalid_transaction.txn_id = Some(TxnId(0).to_proto());
        assert_eq!(
            PrewriteCommand::from_proto(invalid_transaction),
            Err("transaction ID must be non-zero")
        );

        let mut invalid_primary = command.to_proto().unwrap();
        invalid_primary.primary_key.clear();
        assert_eq!(
            PrewriteCommand::from_proto(invalid_primary),
            Err("prewrite primary key must not be empty")
        );

        let mut invalid_ttl = command.to_proto().unwrap();
        invalid_ttl.ttl_ms = 0;
        assert_eq!(
            PrewriteCommand::from_proto(invalid_ttl),
            Err("prewrite lock TTL must be non-zero")
        );
    }

    #[test]
    fn commit_command_roundtrip() {
        let cmd = CommitCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            commit_timestamp: Timestamp(105),
            keys: vec![b"/table/1/pk/1".to_vec()],
            committed_status: None,
        };
        let proto = cmd.to_proto().unwrap();
        let decoded = CommitCommand::from_proto(proto).unwrap();
        assert_eq!(decoded.commit_timestamp.0, 105);
    }

    #[test]
    fn commit_from_proto_rejects_invalid_metadata() {
        let command = CommitCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            commit_timestamp: Timestamp(105),
            keys: vec![b"/table/1/pk/1".to_vec()],
            committed_status: None,
        };

        let mut invalid_transaction = command.to_proto().unwrap();
        invalid_transaction.txn_id = Some(TxnId(0).to_proto());
        assert_eq!(
            CommitCommand::from_proto(invalid_transaction),
            Err("transaction ID must be non-zero")
        );

        let mut invalid_timestamp = command.to_proto().unwrap();
        invalid_timestamp.commit_timestamp = Some(Timestamp(100).to_proto());
        assert_eq!(
            CommitCommand::from_proto(invalid_timestamp),
            Err("commit timestamp must be greater than start timestamp")
        );

        let mut empty_keys = command.to_proto().unwrap();
        empty_keys.keys.clear();
        assert_eq!(
            CommitCommand::from_proto(empty_keys),
            Err("participant command requires at least one row key")
        );
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
    fn rollback_from_proto_rejects_invalid_metadata() {
        let command = RollbackCommand {
            txn_id: TxnId(1),
            start_timestamp: Timestamp(100),
            keys: vec![b"/table/1/pk/1".to_vec()],
        };

        let mut invalid_transaction = command.to_proto().unwrap();
        invalid_transaction.txn_id = Some(TxnId(0).to_proto());
        assert_eq!(
            RollbackCommand::from_proto(invalid_transaction),
            Err("transaction ID must be non-zero")
        );

        let mut invalid_start_timestamp = command.to_proto().unwrap();
        invalid_start_timestamp.start_timestamp = Some(Timestamp(0).to_proto());
        assert_eq!(
            RollbackCommand::from_proto(invalid_start_timestamp),
            Err("transaction start timestamp must be non-zero")
        );

        let mut empty_keys = command.to_proto().unwrap();
        empty_keys.keys.clear();
        assert_eq!(
            RollbackCommand::from_proto(empty_keys),
            Err("participant command requires at least one row key")
        );
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
            pending_status: None,
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
            committed_status: None,
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
    fn tablet_command_envelope_roundtrip_preserves_logical_identity() {
        let logical_command_id = LogicalCommandId {
            client_request_id: ClientRequestId {
                client_id: 17,
                session_epoch: 2,
                request_sequence: 3,
            },
            command_ordinal: 1,
            kind: CommandKind::Noop,
        };
        let envelope = TabletCommandEnvelope::new_with_logical_command_id(
            RequestId {
                client_id: 17,
                sequence: 3,
                raft_group_id: RaftGroupId(12),
            },
            logical_command_id,
            TabletId(41),
            3,
            TabletCommand::Noop(NoopCommand),
        )
        .unwrap();

        assert_eq!(
            TabletCommandEnvelope::decode(&envelope.encode().unwrap()).unwrap(),
            envelope
        );
    }

    #[test]
    fn tablet_command_envelope_allows_transport_route_changes() {
        let logical_command_id = LogicalCommandId {
            client_request_id: ClientRequestId {
                client_id: 17,
                session_epoch: 2,
                request_sequence: 8,
            },
            command_ordinal: 1,
            kind: CommandKind::Noop,
        };

        let envelope = TabletCommandEnvelope::new_with_logical_command_id(
            RequestId {
                // The transport identity is correlation/routing metadata. It
                // may change when the command is forwarded to another Raft
                // group or tablet; the logical identity remains stable.
                client_id: 99,
                sequence: 3,
                raft_group_id: RaftGroupId(12),
            },
            logical_command_id,
            TabletId(41),
            3,
            TabletCommand::Noop(NoopCommand),
        )
        .unwrap();

        assert_eq!(envelope.logical_command_id, Some(logical_command_id));
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

    #[test]
    fn cached_outcome_query_roundtrip_preserves_rejection_semantics() {
        let outcome = CachedTabletCommandOutcome::Rejected(CachedTabletCommandRejection {
            kind: CachedTabletCommandRejectionKind::WriteConflict,
            reason: "committed version is newer".to_string(),
        });

        let decoded = CachedTabletCommandOutcome::decode_from_outcome_query(
            &outcome.encode_for_outcome_query().unwrap(),
        )
        .unwrap();

        assert_eq!(decoded, outcome);
    }

    /// Realistic bug caught: a tablet snapshot can be installed after Raft has
    /// compacted the entries that published a transaction decision. If the
    /// snapshot codec drops that decision, a restarted reader can no longer
    /// distinguish a committed intent from an unresolved one.
    #[test]
    fn snapshot_roundtrip_preserves_transaction_status_records() {
        use crate::{codec::TxnStatusRecord, ids::TxnId};

        let mut bytes = TabletStateMachineSnapshot::new_with_logical_commands_and_horizons(
            TabletId(9),
            1,
            RaftGroupId(11),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
        )
        .unwrap()
        .encode()
        .unwrap();
        assert!(
            TabletStateMachineSnapshot::decode(&bytes)
                .unwrap()
                .transaction_statuses
                .is_empty(),
            "legacy snapshot without status records must restore an empty status map"
        );
        let status = TxnStatusRecord {
            txn_id: TxnId(12),
            start_timestamp: Timestamp(40),
            commit_timestamp: Some(Timestamp(50)),
            status: TxnStatus::Committed,
            primary_key: b"primary-key".to_vec(),
            participant_tablet_ids: vec![9, 22],
            last_heartbeat_timestamp: None,
            lease_deadline_ms: None,
        };
        let status_bytes = status.to_proto().unwrap().encode_to_vec();

        // Field 8 is reserved for transaction status records in the replicated
        // state-machine snapshot. Constructing the wire field directly keeps
        // this regression test executable before the domain schema understands
        // it, so RED proves that the existing codec actually discards it.
        append_varint((8 << 3) | 2, &mut bytes);
        append_varint(status_bytes.len() as u64, &mut bytes);
        bytes.extend_from_slice(&status_bytes);

        let recovered = TabletStateMachineSnapshot::decode(&bytes).unwrap();
        let reencoded = recovered.encode().unwrap();

        assert!(
            reencoded
                .windows(status_bytes.len())
                .any(|window| window == status_bytes),
            "snapshot roundtrip dropped the transaction status record"
        );
    }

    fn append_varint(mut value: u64, output: &mut Vec<u8>) {
        while value >= 0x80 {
            output.push((value as u8) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }
}
