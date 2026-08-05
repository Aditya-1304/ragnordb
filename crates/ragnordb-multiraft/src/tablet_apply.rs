//! typed bridge from committed Raft entries to one tablet state machine
//!
//! raft owns ordering and durability. This module owns only the boundary that
//! decodes a committed command, invokes deterministic tablet apply, and
//! preserves the resulting RequestId and Raft position for proposal tracking

use raft::types::LogIndex;
use ragnordb_common::{
    command_codec::{TabletCommandEnvelope, TabletCommandEnvelopeError},
    ids::RequestId,
};
use ragnordb_storage::mvcc::{InMemoryMvcc, MvccStorage};
use ragnordb_tablet::command::{
    TabletCommandApplyError, TabletCommandApplyOutcome, TabletStateMachine,
};

use crate::proposal::{ProposalPosition, ProposalRegistry, ProposalRegistryError};

/// result produced after one committed tablet command has applied successfully
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedTabletCommand {
    /// client identity preserved from the durable command envelope
    pub request_id: RequestId,

    /// exact Raft term and index of the applied entry
    pub position: ProposalPosition,

    /// deterministic tablet result, including deduplication provenance
    pub outcome: TabletCommandApplyOutcome,
}

impl AppliedTabletCommand {
    /// resolve the matching proposal waiter after tablet apply succeeds
    pub fn resolve(
        self,
        registry: &mut ProposalRegistry<TabletCommandApplyOutcome>,
    ) -> Result<(), ProposalRegistryError> {
        registry.resolve_applied(&self.request_id, self.position, self.outcome)
    }
}

/// errors raised before a committed tablet entry can produce an apply result
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TabletApplyError {
    #[error("committed Raft entry has an invalid position: term={term}, index={index}")]
    InvalidRaftPosition { term: u64, index: LogIndex },

    #[error("committed tablet command envelope is invalid: {0}")]
    InvalidEnvelope(#[from] TabletCommandEnvelopeError),

    #[error("tablet command apply failed: {0}")]
    Apply(#[from] TabletCommandApplyError),
}

/// applies committed command bytes to one replicated tablet state machine
#[derive(Debug)]
pub struct TabletCommandApplier<S = InMemoryMvcc> {
    state_machine: TabletStateMachine<S>,
}

impl<S: MvccStorage> TabletCommandApplier<S> {
    /// construct an applier around an already validated tablet state machine
    pub fn new(state_machine: TabletStateMachine<S>) -> Self {
        Self { state_machine }
    }

    /// borrow the underlying tablet state machine for diagnostics and reads
    pub fn state_machine(&self) -> &TabletStateMachine<S> {
        &self.state_machine
    }

    /// mutably borrow the underlying tablet state machine for controlled
    /// snapshot or lifecycle operations
    pub fn state_machine_mut(&mut self) -> &mut TabletStateMachine<S> {
        &mut self.state_machine
    }

    /// decode and apply one committed Raft entry
    ///
    /// this method intentionally performs no proposal admission. The entry is
    /// already committed, so only deterministic decode, routing validation,
    /// deduplication, and MVCC application are allowed here
    pub fn apply_committed(
        &mut self,
        position: ProposalPosition,
        command: &[u8],
    ) -> Result<AppliedTabletCommand, TabletApplyError> {
        if position.term == 0 || position.index == 0 {
            return Err(TabletApplyError::InvalidRaftPosition {
                term: position.term,
                index: position.index,
            });
        }

        let envelope = TabletCommandEnvelope::decode(command)?;
        let request_id = envelope.request_id.clone();
        let outcome = self.state_machine.apply(envelope)?;

        Ok(AppliedTabletCommand {
            request_id,
            position,
            outcome,
        })
    }
}
