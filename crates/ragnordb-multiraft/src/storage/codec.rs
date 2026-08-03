//! versioned durable codec for one identity bound Raft log entry
//!
//! every entry records both the Raft group and replica lifetime. Log indexes
//! are meaningful only inside that identity pair and must never be used as a
//! global shared WAL key

use prost::Message;
use raft::{
    entry::{EntryPayload, LogEntry},
    types::{ConfChange, ConfChangeKind},
};
use ragnordb_common::{
    ids::{RaftGroupId, ReplicaId},
    proto::raft as raft_proto,
};

/// durable format accepted for Raft log-entry records
pub const RAFT_LOG_ENTRY_RECORD_VERSION: u32 = 1;

/// one Raft replica lifetime inside a logical Raft group
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RaftReplicaIdentity {
    pub raft_group_id: RaftGroupId,
    pub replica_id: ReplicaId,
}

impl RaftReplicaIdentity {
    /// a validated storage identity
    pub fn new(
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
    ) -> Result<Self, RaftLogEntryCodecError> {
        let identity = Self {
            raft_group_id,
            replica_id,
        };

        identity.validate()?;
        Ok(identity)
    }

    fn validate(self) -> Result<(), RaftLogEntryCodecError> {
        if self.raft_group_id.0 == 0 {
            return Err(RaftLogEntryCodecError::ZeroRaftGroupId);
        }

        if self.replica_id.0 == 0 {
            return Err(RaftLogEntryCodecError::ZeroReplicaId);
        }

        Ok(())
    }
}

/// durable payload carried by one Raft log entry
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurableRaftEntryPayload {
    /// versioned tablet-command envelope bytes owned by the database host
    Normal(Vec<u8>),

    /// membership transition interpreted exclusively by the Raft core
    Configuration(ConfChange),
}

/// durable, identity-bound representation of one Raft log entry
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftLogEntryRecord {
    pub format_version: u32,
    pub identity: RaftReplicaIdentity,
    pub index: u64,
    pub term: u64,
    pub payload: DurableRaftEntryPayload,
}

impl RaftLogEntryRecord {
    /// convert a core log entry into the V1 database owned durable record
    pub fn from_core(
        identity: RaftReplicaIdentity,
        entry: LogEntry<Vec<u8>>,
    ) -> Result<Self, RaftLogEntryCodecError> {
        let payload = match entry.payload {
            EntryPayload::Normal(command) => DurableRaftEntryPayload::Normal(command),

            EntryPayload::Configuration(change) => DurableRaftEntryPayload::Configuration(change),
        };

        let record = Self {
            format_version: RAFT_LOG_ENTRY_RECORD_VERSION,
            identity,
            index: entry.index,
            term: entry.term,
            payload,
        };

        record.validate()?;
        Ok(record)
    }

    /// rebuild the public Raft core entry after durable decoding
    pub fn to_core(&self) -> Result<LogEntry<Vec<u8>>, RaftLogEntryCodecError> {
        self.validate()?;

        let (payload, encoded_len) = match &self.payload {
            DurableRaftEntryPayload::Normal(command) => {
                (EntryPayload::Normal(command.clone()), command.len())
            }

            DurableRaftEntryPayload::Configuration(change) => {
                let encoded_len = configuration_change_to_proto(*change).encoded_len();

                (EntryPayload::Configuration(*change), encoded_len)
            }
        };

        Ok(LogEntry {
            index: self.index,
            term: self.term,
            encoded_len,
            payload,
        })
    }

    /// encode one validated record for storage inside an A-WAL user record
    pub fn encode(&self) -> Result<Vec<u8>, RaftLogEntryCodecError> {
        self.validate()?;
        Ok(self.to_proto().encode_to_vec())
    }

    /// decode and validate one record returned by A-WAL recovery
    pub fn decode(bytes: &[u8]) -> Result<Self, RaftLogEntryCodecError> {
        let proto = raft_proto::RaftLogEntryRecord::decode(bytes)
            .map_err(|error| RaftLogEntryCodecError::Decode(error.to_string()))?;

        Self::from_proto(proto)
    }

    fn validate(&self) -> Result<(), RaftLogEntryCodecError> {
        if self.format_version != RAFT_LOG_ENTRY_RECORD_VERSION {
            return Err(RaftLogEntryCodecError::UnsupportedVersion(
                self.format_version,
            ));
        }

        self.identity.validate()?;

        if self.index == 0 {
            return Err(RaftLogEntryCodecError::ZeroLogIndex);
        }

        if self.term == 0 {
            return Err(RaftLogEntryCodecError::ZeroTerm);
        }

        if let DurableRaftEntryPayload::Configuration(change) = self.payload {
            validate_configuration_change(change)?;
        }

        Ok(())
    }

    fn to_proto(&self) -> raft_proto::RaftLogEntryRecord {
        let payload = match &self.payload {
            DurableRaftEntryPayload::Normal(command) => {
                raft_proto::raft_log_entry_record::Payload::NormalCommand(command.clone())
            }

            DurableRaftEntryPayload::Configuration(change) => {
                raft_proto::raft_log_entry_record::Payload::ConfigurationChange(
                    configuration_change_to_proto(*change),
                )
            }
        };

        raft_proto::RaftLogEntryRecord {
            format_version: self.format_version,
            raft_group_id: Some(self.identity.raft_group_id.to_proto()),
            replica_id: Some(self.identity.replica_id.to_proto()),
            index: self.index,
            term: self.term,
            payload: Some(payload),
        }
    }

    fn from_proto(proto: raft_proto::RaftLogEntryRecord) -> Result<Self, RaftLogEntryCodecError> {
        let raft_group_id = RaftGroupId::from_proto(
            proto
                .raft_group_id
                .ok_or(RaftLogEntryCodecError::MissingField("raft_group_id"))?,
        );

        let replica_id = ReplicaId::from_proto(
            proto
                .replica_id
                .ok_or(RaftLogEntryCodecError::MissingField("replica_id"))?,
        );

        let payload = match proto
            .payload
            .ok_or(RaftLogEntryCodecError::MissingField("payload"))?
        {
            raft_proto::raft_log_entry_record::Payload::NormalCommand(command) => {
                DurableRaftEntryPayload::Normal(command)
            }

            raft_proto::raft_log_entry_record::Payload::ConfigurationChange(change) => {
                DurableRaftEntryPayload::Configuration(configuration_change_from_proto(change)?)
            }
        };

        let record = Self {
            format_version: proto.format_version,
            identity: RaftReplicaIdentity {
                raft_group_id,
                replica_id,
            },
            index: proto.index,
            term: proto.term,
            payload,
        };

        record.validate()?;
        Ok(record)
    }
}

fn validate_configuration_change(change: ConfChange) -> Result<(), RaftLogEntryCodecError> {
    if change.expected_version == 0 {
        return Err(RaftLogEntryCodecError::ZeroConfigurationVersion);
    }

    Ok(())
}

fn configuration_change_to_proto(change: ConfChange) -> raft_proto::RaftConfigurationChange {
    let (kind, replica_id) = match change.kind {
        ConfChangeKind::AddLearner(replica_id) => (
            raft_proto::RaftConfigurationChangeKind::AddLearner,
            replica_id,
        ),

        ConfChangeKind::PromoteLearner(replica_id) => (
            raft_proto::RaftConfigurationChangeKind::PromoteLearner,
            replica_id,
        ),

        ConfChangeKind::RemoveReplica(replica_id) => (
            raft_proto::RaftConfigurationChangeKind::RemoveReplica,
            replica_id,
        ),
    };

    raft_proto::RaftConfigurationChange {
        expected_version: change.expected_version,
        kind: kind as i32,
        replica_id: Some(ReplicaId::from_raft(replica_id).to_proto()),
    }
}

fn configuration_change_from_proto(
    proto: raft_proto::RaftConfigurationChange,
) -> Result<ConfChange, RaftLogEntryCodecError> {
    let kind = raft_proto::RaftConfigurationChangeKind::try_from(proto.kind)
        .map_err(|_| RaftLogEntryCodecError::UnknownConfigurationKind(proto.kind))?;

    let replica_id = ReplicaId::from_proto(proto.replica_id.ok_or(
        RaftLogEntryCodecError::MissingField("configuration_change.replica_id"),
    )?)
    .to_raft()
    .map_err(|_| RaftLogEntryCodecError::ZeroConfigurationReplicaId)?;

    let kind = match kind {
        raft_proto::RaftConfigurationChangeKind::AddLearner => {
            ConfChangeKind::AddLearner(replica_id)
        }

        raft_proto::RaftConfigurationChangeKind::PromoteLearner => {
            ConfChangeKind::PromoteLearner(replica_id)
        }

        raft_proto::RaftConfigurationChangeKind::RemoveReplica => {
            ConfChangeKind::RemoveReplica(replica_id)
        }

        raft_proto::RaftConfigurationChangeKind::Unspecified => {
            return Err(RaftLogEntryCodecError::UnspecifiedConfigurationKind);
        }
    };

    let change = ConfChange {
        expected_version: proto.expected_version,
        kind,
    };

    validate_configuration_change(change)?;
    Ok(change)
}

/// Invalid or corrupt durable Raft log-entry record.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RaftLogEntryCodecError {
    #[error("unsupported Raft log-entry record version {0}")]
    UnsupportedVersion(u32),

    #[error("Raft log-entry record is missing required field {0}")]
    MissingField(&'static str),

    #[error("Raft log-entry record contains reserved Raft group ID zero")]
    ZeroRaftGroupId,

    #[error("Raft log-entry record contains reserved replica ID zero")]
    ZeroReplicaId,

    #[error("Raft log-entry record contains reserved log index zero")]
    ZeroLogIndex,

    #[error("Raft log-entry record contains reserved term zero")]
    ZeroTerm,

    #[error("Raft configuration change contains expected version zero")]
    ZeroConfigurationVersion,

    #[error("Raft configuration change contains reserved replica ID zero")]
    ZeroConfigurationReplicaId,

    #[error("Raft configuration change kind is unspecified")]
    UnspecifiedConfigurationKind,

    #[error("unknown Raft configuration change kind {0}")]
    UnknownConfigurationKind(i32),

    #[error("cannot decode durable Raft log-entry record: {0}")]
    Decode(String),
}
