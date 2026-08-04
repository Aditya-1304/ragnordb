//! versioned durable codec for one identity bound Raft log entry
//!
//! every entry records both the Raft group and replica lifetime. Log indexes
//! are meaningful only inside that identity pair and must never be used as a
//! global shared WAL key
use prost::Message;
use raft::{
    entry::{EntryPayload, LogEntry},
    types::{ConfChange, ConfChangeKind, ConfState, HardState},
};
use ragnordb_common::{
    ids::{RaftGroupId, ReplicaId},
    proto::raft as raft_proto,
};
use std::{collections::BTreeSet, path::Path};

/// durable format accepted for Raft log-entry records
pub const RAFT_LOG_ENTRY_RECORD_VERSION: u32 = 1;

/// durable format accepted for Raft stable-state records
pub const RAFT_STABLE_STATE_RECORD_VERSION: u32 = 1;
pub const RAFT_SNAPSHOT_POINTER_RECORD_VERSION: u32 = 1;

/// Durable reference to a snapshot file synchronized before its WAL pointer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftSnapshotPointerRecord {
    pub format_version: u32,
    pub identity: RaftReplicaIdentity,
    pub snapshot_id: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub applied_index: u64,
    pub conf_state: ConfState,
    pub size_bytes: u64,
    pub checksum: [u8; 32],
    pub file_name: String,
}

impl RaftSnapshotPointerRecord {
    pub fn encode(&self) -> Result<Vec<u8>, RaftStableStateCodecError> {
        self.validate()?;
        Ok(raft_proto::RaftSnapshotPointerRecord {
            format_version: self.format_version,
            raft_group_id: Some(self.identity.raft_group_id.to_proto()),
            replica_id: Some(self.identity.replica_id.to_proto()),
            snapshot_id: self.snapshot_id,
            last_included_index: self.last_included_index,
            last_included_term: self.last_included_term,
            applied_index: self.applied_index,
            configuration_version: self.conf_state.version,
            voters: encode_core_replicas(&self.conf_state.voters),
            learners: encode_core_replicas(&self.conf_state.learners),
            outgoing_voters: encode_core_replicas(&self.conf_state.outgoing_voters),
            size_bytes: self.size_bytes,
            checksum: self.checksum.to_vec(),
            file_name: self.file_name.clone(),
        }
        .encode_to_vec())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RaftStableStateCodecError> {
        let proto = raft_proto::RaftSnapshotPointerRecord::decode(bytes)
            .map_err(|error| RaftStableStateCodecError::Decode(error.to_string()))?;
        let identity = decode_stable_identity(proto.raft_group_id, proto.replica_id)?;
        let checksum: [u8; 32] = proto
            .checksum
            .try_into()
            .map_err(|_| RaftStableStateCodecError::InvalidSnapshotChecksumLength)?;
        let record = Self {
            format_version: proto.format_version,
            identity,
            snapshot_id: proto.snapshot_id,
            last_included_index: proto.last_included_index,
            last_included_term: proto.last_included_term,
            applied_index: proto.applied_index,
            conf_state: ConfState {
                version: proto.configuration_version,
                voters: decode_core_replicas("voters", proto.voters)?,
                learners: decode_core_replicas("learners", proto.learners)?,
                outgoing_voters: decode_core_replicas("outgoing_voters", proto.outgoing_voters)?,
            },
            size_bytes: proto.size_bytes,
            checksum,
            file_name: proto.file_name,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), RaftStableStateCodecError> {
        if self.format_version != RAFT_SNAPSHOT_POINTER_RECORD_VERSION {
            return Err(RaftStableStateCodecError::UnsupportedVersion(
                self.format_version,
            ));
        }
        self.identity
            .validate()
            .map_err(RaftStableStateCodecError::InvalidIdentity)?;
        if self.snapshot_id == 0 {
            return Err(RaftStableStateCodecError::ZeroSnapshotId);
        }
        if self.last_included_index == 0 || self.last_included_term == 0 {
            return Err(RaftStableStateCodecError::InvalidSnapshotBoundary);
        }
        if self.applied_index != self.last_included_index {
            return Err(RaftStableStateCodecError::SnapshotAppliedIndexMismatch {
                last_included_index: self.last_included_index,
                applied_index: self.applied_index,
            });
        }
        let mut components = Path::new(&self.file_name).components();
        let is_single_safe_component = matches!(
            (components.next(), components.next()),
            (Some(std::path::Component::Normal(_)), None)
        );
        if self.size_bytes == 0 || !is_single_safe_component {
            return Err(RaftStableStateCodecError::InvalidSnapshotFileMetadata);
        }
        self.conf_state
            .validate()
            .map_err(|error| RaftStableStateCodecError::InvalidConfState(format!("{error:?}")))
    }
}

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

    /// validate a record constructed directly by an in process caller
    ///
    /// recovery obtains the same validation through `decode`, while the live
    /// persistence path uses this boundary before admitting a record to its
    /// identity-scoped log view
    pub fn validate(&self) -> Result<(), RaftLogEntryCodecError> {
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

    /// Validate a record constructed directly by an in-process caller.
    ///
    /// Recovery obtains the same validation through `decode`, while the live
    /// persistence path uses this boundary before admitting a record to its
    /// identity-scoped log view.
    pub fn validate(&self) -> Result<(), RaftLogEntryCodecError> {
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

/// durable, identity-bound election and commit state
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftHardStateRecord {
    pub format_version: u32,
    pub identity: RaftReplicaIdentity,
    pub hard_state: HardState,
}

impl RaftHardStateRecord {
    /// convert validated Raft-core state into its durable V1 representation
    pub fn from_core(
        identity: RaftReplicaIdentity,
        hard_state: HardState,
    ) -> Result<Self, RaftStableStateCodecError> {
        let record = Self {
            format_version: RAFT_STABLE_STATE_RECORD_VERSION,
            identity,
            hard_state,
        };

        record.validate()?;
        Ok(record)
    }

    /// return the public Raft-core state represented by this record
    pub fn to_core(&self) -> Result<HardState, RaftStableStateCodecError> {
        self.validate()?;
        Ok(self.hard_state.clone())
    }

    /// encode a validated stable-state record for A-WAL
    pub fn encode(&self) -> Result<Vec<u8>, RaftStableStateCodecError> {
        self.validate()?;

        Ok(raft_proto::RaftHardStateRecord {
            format_version: self.format_version,
            raft_group_id: Some(self.identity.raft_group_id.to_proto()),
            replica_id: Some(self.identity.replica_id.to_proto()),
            current_term: self.hard_state.current_term,
            voted_for: self
                .hard_state
                .voted_for
                .map(|replica_id| ReplicaId::from_raft(replica_id).to_proto()),
            commit_index: self.hard_state.commit,
        }
        .encode_to_vec())
    }

    /// Decode stable state returned by shared-WAL recovery.
    pub fn decode(bytes: &[u8]) -> Result<Self, RaftStableStateCodecError> {
        let proto = raft_proto::RaftHardStateRecord::decode(bytes)
            .map_err(|error| RaftStableStateCodecError::Decode(error.to_string()))?;

        let identity = decode_stable_identity(proto.raft_group_id, proto.replica_id)?;

        let voted_for = proto
            .voted_for
            .map(ReplicaId::from_proto)
            .map(ReplicaId::to_raft)
            .transpose()
            .map_err(|_| RaftStableStateCodecError::ZeroVotedForReplicaId)?;

        let record = Self {
            format_version: proto.format_version,
            identity,
            hard_state: HardState {
                current_term: proto.current_term,
                voted_for,
                commit: proto.commit_index,
            },
        };

        record.validate()?;
        Ok(record)
    }

    /// Validate state created directly by the live persistence path.
    pub fn validate(&self) -> Result<(), RaftStableStateCodecError> {
        validate_stable_header(self.format_version, self.identity)?;

        if self.hard_state.current_term == 0 {
            return Err(RaftStableStateCodecError::ZeroCurrentTerm);
        }

        Ok(())
    }
}

/// validate a durable HardState transition before WAL admission or recovery.
pub fn validate_hard_state_successor(
    previous: Option<&HardState>,
    received: &HardState,
) -> Result<(), RaftStableStateCodecError> {
    if received.current_term == 0 {
        return Err(RaftStableStateCodecError::ZeroCurrentTerm);
    }
    let Some(previous) = previous else {
        return Ok(());
    };
    if received.current_term < previous.current_term {
        return Err(RaftStableStateCodecError::TermRegression {
            previous: previous.current_term,
            received: received.current_term,
        });
    }
    if received.commit < previous.commit {
        return Err(RaftStableStateCodecError::CommitRegression {
            previous: previous.commit,
            received: received.commit,
        });
    }
    if received.current_term == previous.current_term {
        match (previous.voted_for, received.voted_for) {
            (Some(previous_vote), Some(received_vote)) if previous_vote != received_vote => {
                return Err(RaftStableStateCodecError::VoteChangedInTerm {
                    term: received.current_term,
                    previous: previous_vote.get(),
                    received: received_vote.get(),
                });
            }
            (Some(previous_vote), None) => {
                return Err(RaftStableStateCodecError::VoteClearedInTerm {
                    term: received.current_term,
                    previous: previous_vote.get(),
                });
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_stable_header(
    format_version: u32,
    identity: RaftReplicaIdentity,
) -> Result<(), RaftStableStateCodecError> {
    if format_version != RAFT_STABLE_STATE_RECORD_VERSION {
        return Err(RaftStableStateCodecError::UnsupportedVersion(
            format_version,
        ));
    }

    identity
        .validate()
        .map_err(RaftStableStateCodecError::InvalidIdentity)
}

fn decode_stable_identity(
    raft_group_id: Option<ragnordb_common::proto::ids::RaftGroupId>,
    replica_id: Option<ragnordb_common::proto::ids::ReplicaId>,
) -> Result<RaftReplicaIdentity, RaftStableStateCodecError> {
    let raft_group_id = RaftGroupId::from_proto(
        raft_group_id.ok_or(RaftStableStateCodecError::MissingField("raft_group_id"))?,
    );

    let replica_id = ReplicaId::from_proto(
        replica_id.ok_or(RaftStableStateCodecError::MissingField("replica_id"))?,
    );

    RaftReplicaIdentity::new(raft_group_id, replica_id)
        .map_err(RaftStableStateCodecError::InvalidIdentity)
}

fn encode_core_replicas(
    replicas: &BTreeSet<raft::types::ReplicaId>,
) -> Vec<ragnordb_common::proto::ids::ReplicaId> {
    replicas
        .iter()
        .copied()
        .map(ReplicaId::from_raft)
        .map(|replica_id| replica_id.to_proto())
        .collect()
}

fn decode_core_replicas(
    field: &'static str,
    replicas: Vec<ragnordb_common::proto::ids::ReplicaId>,
) -> Result<BTreeSet<raft::types::ReplicaId>, RaftStableStateCodecError> {
    let mut decoded = BTreeSet::new();

    for replica_id in replicas {
        let replica_id = ReplicaId::from_proto(replica_id)
            .to_raft()
            .map_err(|_| RaftStableStateCodecError::ZeroMembershipReplicaId { field })?;

        if !decoded.insert(replica_id) {
            return Err(RaftStableStateCodecError::DuplicateMembershipReplica {
                field,
                replica_id: replica_id.get(),
            });
        }
    }

    Ok(decoded)
}

/// Invalid or corrupt durable Raft stable-state record.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RaftStableStateCodecError {
    #[error("unsupported Raft stable-state record version {0}")]
    UnsupportedVersion(u32),

    #[error("Raft stable-state record is missing required field {0}")]
    MissingField(&'static str),

    #[error("invalid Raft stable-state identity: {0}")]
    InvalidIdentity(RaftLogEntryCodecError),

    #[error("Raft HardState contains reserved current term zero")]
    ZeroCurrentTerm,

    #[error("Raft HardState term regressed from {previous} to {received}")]
    TermRegression { previous: u64, received: u64 },

    #[error("Raft HardState commit regressed from {previous} to {received}")]
    CommitRegression { previous: u64, received: u64 },

    #[error("Raft HardState vote changed in term {term} from {previous} to {received}")]
    VoteChangedInTerm {
        term: u64,
        previous: u64,
        received: u64,
    },

    #[error("Raft HardState vote for {previous} was cleared in term {term}")]
    VoteClearedInTerm { term: u64, previous: u64 },

    #[error("Raft HardState vote contains reserved replica ID zero")]
    ZeroVotedForReplicaId,

    #[error("Raft snapshot pointer contains reserved snapshot ID zero")]
    ZeroSnapshotId,

    #[error("Raft snapshot pointer has an invalid included index or term")]
    InvalidSnapshotBoundary,

    #[error(
        "Raft snapshot applies through index {applied_index}, but its included boundary is {last_included_index}"
    )]
    SnapshotAppliedIndexMismatch {
        last_included_index: u64,
        applied_index: u64,
    },

    #[error("Raft snapshot pointer has invalid file name or length metadata")]
    InvalidSnapshotFileMetadata,

    #[error("Raft snapshot checksum must contain exactly 32 bytes")]
    InvalidSnapshotChecksumLength,

    #[error("Raft {field} contains reserved replica ID zero")]
    ZeroMembershipReplicaId { field: &'static str },

    #[error("Raft {field} contains duplicate replica ID {replica_id}")]
    DuplicateMembershipReplica {
        field: &'static str,
        replica_id: u64,
    },

    #[error("invalid Raft ConfState: {0}")]
    InvalidConfState(String),

    #[error("cannot decode durable Raft stable-state record: {0}")]
    Decode(String),
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
