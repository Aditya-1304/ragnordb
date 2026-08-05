//! safety state for leader served latest reads
//!
//! a leader cannot serve a latest read merely because it currently has the
//! Leader role. It must first establish a current-term Raft ordering point and
//! wait until that exact barrier entry has been applied locally

/// exact Raft position of a current-term read barrier or no op
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadBarrierPosition {
    pub term: u64,
    pub index: u64,
}

/// errors raised while establishing a leader read barrier
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LeaderReadGateError {
    #[error("leader term must be non-zero")]
    ZeroLeaderTerm,

    #[error("leader term regressed from {previous} to {received}")]
    TermRegression { previous: u64, received: u64 },

    #[error("read barrier position must be non-zero: term={term}, index={index}")]
    InvalidBarrierPosition { term: u64, index: u64 },

    #[error("read barrier term {barrier_term} does not match current leader term {leader_term}")]
    BarrierTermMismatch { leader_term: u64, barrier_term: u64 },

    #[error("no leader term has been established")]
    NoLeaderTerm,

    #[error("a read barrier is already awaiting apply")]
    BarrierAlreadyPending,

    #[error("no read barrier is awaiting apply")]
    NoPendingBarrier,

    #[error(
        "applied barrier position does not match the registered barrier: \
         expected {expected:?}, received {received:?}"
    )]
    AppliedPositionMismatch {
        expected: ReadBarrierPosition,
        received: ReadBarrierPosition,
    },
}

/// tracks whether a leader has earned permission to serve latest reads
#[derive(Debug, Default)]
pub struct LeaderReadGate {
    leader_term: Option<u64>,
    pending_barrier: Option<ReadBarrierPosition>,
    applied_barrier: Option<ReadBarrierPosition>,
}

impl LeaderReadGate {
    /// construct a closed read gate
    pub const fn new() -> Self {
        Self {
            leader_term: None,
            pending_barrier: None,
            applied_barrier: None,
        }
    }

    /// start a new leader term and invalidate all barriers from older terms
    pub fn on_leader_elected(&mut self, term: u64) -> Result<(), LeaderReadGateError> {
        if term == 0 {
            return Err(LeaderReadGateError::ZeroLeaderTerm);
        }

        if let Some(previous) = self.leader_term
            && term < previous
        {
            return Err(LeaderReadGateError::TermRegression {
                previous,
                received: term,
            });
        }

        self.leader_term = Some(term);
        self.pending_barrier = None;
        self.applied_barrier = None;

        Ok(())
    }

    /// register the current-term no-op or read barrier
    pub fn register_barrier(
        &mut self,
        position: ReadBarrierPosition,
    ) -> Result<(), LeaderReadGateError> {
        if position.term == 0 || position.index == 0 {
            return Err(LeaderReadGateError::InvalidBarrierPosition {
                term: position.term,
                index: position.index,
            });
        }

        let leader_term = self.leader_term.ok_or(LeaderReadGateError::NoLeaderTerm)?;

        if position.term != leader_term {
            return Err(LeaderReadGateError::BarrierTermMismatch {
                leader_term,
                barrier_term: position.term,
            });
        }

        if self.pending_barrier.is_some() && self.applied_barrier.is_none() {
            return Err(LeaderReadGateError::BarrierAlreadyPending);
        }

        self.pending_barrier = Some(position);
        self.applied_barrier = None;

        Ok(())
    }

    /// open the gate only after the exact registered position is applied
    pub fn mark_barrier_applied(
        &mut self,
        position: ReadBarrierPosition,
    ) -> Result<(), LeaderReadGateError> {
        let expected = self
            .pending_barrier
            .ok_or(LeaderReadGateError::NoPendingBarrier)?;

        if expected != position {
            return Err(LeaderReadGateError::AppliedPositionMismatch {
                expected,
                received: position,
            });
        }

        self.applied_barrier = Some(position);

        Ok(())
    }

    /// return whether latest reads are safe for the observed leader term
    pub fn can_serve_latest(&self, observed_term: u64) -> bool {
        observed_term != 0
            && self.leader_term == Some(observed_term)
            && self.pending_barrier.is_some()
            && self.pending_barrier == self.applied_barrier
    }

    /// return the currently tracked leader term
    pub const fn leader_term(&self) -> Option<u64> {
        self.leader_term
    }

    /// return the registered barrier, if any
    pub const fn pending_barrier(&self) -> Option<ReadBarrierPosition> {
        self.pending_barrier
    }
}
