//! versioned durable truncation and state-machine apply frontiers
//!
//! truncation boundary retains the term at the removed index. The Raft core
//! needs that term to validate the entry immediately following a snapshot or
//! compacted prefix without exposing the removed entry itself

use prost::Message;
use ragnordb_common::{
    ids::{RaftGroupId, ReplicaId},
    proto::raft as raft_proto,
};

use super::codec::{RaftLogEntryCodecError, RaftReplicaIdentity};

/// durable format accepted for Raft progress records
pub const RAFT_PROGRESS_RECORD_VERSION: u32 = 1;

/// logical frontiers acknowledged for one Raft replica lifetime
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RaftProgress {
    pub truncated_through_index: u64,
    pub truncated_through_term: u64,
    pub applied_index: u64,
}

impl RaftProgress {
    /// construct and validate a logical frontier
    pub fn new(
        truncated_through_index: u64,
        truncated_through_term: u64,
        applied_index: u64,
    ) -> Result<Self, RaftProgressError> {
        let progress = Self {
            truncated_through_index,
            truncated_through_term,
            applied_index,
        };

        progress.validate()?;
        Ok(progress)
    }

    /// validate the relationship between truncation and apply state
    pub fn validate(self) -> Result<(), RaftProgressError> {
        match (self.truncated_through_index, self.truncated_through_term) {
            (0, 0) => {}

            (0, term) => {
                return Err(RaftProgressError::TermWithoutBoundary { term });
            }

            (index, 0) => {
                return Err(RaftProgressError::BoundaryWithoutTerm { index });
            }

            _ => {}
        }

        if self.truncated_through_index > self.applied_index {
            return Err(RaftProgressError::TruncationBeyondApplied {
                truncated_through_index: self.truncated_through_index,
                applied_index: self.applied_index,
            });
        }

        Ok(())
    }

    /// require both durable frontiers to move monotonically
    pub fn validate_successor(self, previous: Self) -> Result<(), RaftProgressError> {
        self.validate()?;

        if self.truncated_through_index < previous.truncated_through_index {
            return Err(RaftProgressError::TruncationRegression {
                previous: previous.truncated_through_index,
                received: self.truncated_through_index,
            });
        }

        if self.applied_index < previous.applied_index {
            return Err(RaftProgressError::AppliedRegression {
                previous: previous.applied_index,
                received: self.applied_index,
            });
        }

        if self.truncated_through_index == previous.truncated_through_index
            && self.truncated_through_term != previous.truncated_through_term
        {
            return Err(RaftProgressError::BoundaryTermChanged {
                index: self.truncated_through_index,
                previous_term: previous.truncated_through_term,
                received_term: self.truncated_through_term,
            });
        }

        Ok(())
    }
}

/// identity bound durable representation of one logical frontier update
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftProgressRecord {
    pub format_version: u32,
    pub identity: RaftReplicaIdentity,
    pub progress: RaftProgress,
}

impl RaftProgressRecord {
    /// bind validated progress to one immutable replica lifetime
    pub fn new(
        identity: RaftReplicaIdentity,
        progress: RaftProgress,
    ) -> Result<Self, RaftProgressError> {
        let record = Self {
            format_version: RAFT_PROGRESS_RECORD_VERSION,
            identity,
            progress,
        };

        record.validate()?;
        Ok(record)
    }

    /// encode a validated progress record for shared A-WAL
    pub fn encode(self) -> Result<Vec<u8>, RaftProgressError> {
        self.validate()?;

        Ok(raft_proto::RaftProgressRecord {
            format_version: self.format_version,
            raft_group_id: Some(self.identity.raft_group_id.to_proto()),
            replica_id: Some(self.identity.replica_id.to_proto()),
            truncated_through_index: self.progress.truncated_through_index,
            truncated_through_term: self.progress.truncated_through_term,
            applied_index: self.progress.applied_index,
        }
        .encode_to_vec())
    }

    /// decode and validate a record encountered during recovery
    pub fn decode(bytes: &[u8]) -> Result<Self, RaftProgressError> {
        let proto = raft_proto::RaftProgressRecord::decode(bytes)
            .map_err(|error| RaftProgressError::Decode(error.to_string()))?;

        let raft_group_id = RaftGroupId::from_proto(
            proto
                .raft_group_id
                .ok_or(RaftProgressError::MissingField("raft_group_id"))?,
        );

        let replica_id = ReplicaId::from_proto(
            proto
                .replica_id
                .ok_or(RaftProgressError::MissingField("replica_id"))?,
        );

        let identity = RaftReplicaIdentity::new(raft_group_id, replica_id)
            .map_err(RaftProgressError::InvalidIdentity)?;

        let progress = RaftProgress {
            truncated_through_index: proto.truncated_through_index,
            truncated_through_term: proto.truncated_through_term,
            applied_index: proto.applied_index,
        };

        let record = Self {
            format_version: proto.format_version,
            identity,
            progress,
        };

        record.validate()?;
        Ok(record)
    }

    fn validate(self) -> Result<(), RaftProgressError> {
        if self.format_version != RAFT_PROGRESS_RECORD_VERSION {
            return Err(RaftProgressError::UnsupportedVersion(self.format_version));
        }

        self.identity
            .validate()
            .map_err(RaftProgressError::InvalidIdentity)?;

        self.progress.validate()
    }
}

/// invalid or corrupt durable Raft frontier state
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RaftProgressError {
    #[error("unsupported Raft progress record version {0}")]
    UnsupportedVersion(u32),

    #[error("Raft progress record is missing required field {0}")]
    MissingField(&'static str),

    #[error("invalid Raft progress identity: {0}")]
    InvalidIdentity(RaftLogEntryCodecError),

    #[error(
        "truncation boundary index zero carries \
         nonzero term {term}"
    )]
    TermWithoutBoundary { term: u64 },

    #[error(
        "truncation boundary index {index} carries \
         reserved term zero"
    )]
    BoundaryWithoutTerm { index: u64 },

    #[error(
        "truncation boundary \
         {truncated_through_index} exceeds applied \
         index {applied_index}"
    )]
    TruncationBeyondApplied {
        truncated_through_index: u64,
        applied_index: u64,
    },

    #[error(
        "truncation frontier regressed from \
         {previous} to {received}"
    )]
    TruncationRegression { previous: u64, received: u64 },

    #[error(
        "applied frontier regressed from \
         {previous} to {received}"
    )]
    AppliedRegression { previous: u64, received: u64 },

    #[error(
        "truncation boundary {index} changed term \
         from {previous_term} to {received_term}"
    )]
    BoundaryTermChanged {
        index: u64,
        previous_term: u64,
        received_term: u64,
    },

    #[error("cannot decode durable Raft progress record: {0}")]
    Decode(String),
}
