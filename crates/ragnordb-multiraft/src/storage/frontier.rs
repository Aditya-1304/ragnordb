//! versioned durable truncation and state-machine apply frontiers
//!
//! truncation boundary retains the term at the removed index. The Raft core
//! needs that term to validate the entry immediately following a snapshot or
//! compacted prefix without exposing the removed entry itself

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

/// invalid or corrupt durable Raft frontier state
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RaftProgressError {
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
