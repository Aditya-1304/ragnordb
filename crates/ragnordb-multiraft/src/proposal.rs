//! client proposal lifecycle tracking for one Raft group
//!
//! raft proposal acceptance and commit-index movement are not client-visible
//! success conditions. A proposal completes successfully only when the
//! corresponding committed entry has been applied by the tablet state machine
//!
//! the registry is deliberately transport-neutral. The future server layer can
//! adapt the returned ticket to a Tokio or RPC response channel without moving
//! proposal correctness into the network code

use std::{
    collections::HashMap,
    sync::mpsc::{self, Receiver, RecvError, RecvTimeoutError, Sender, TryRecvError},
    time::{Duration, Instant},
};

use raft::types::{LogIndex, Term};
use ragnordb_common::ids::RequestId;

/// raft position captured when a client proposal is admitted
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProposalPosition {
    pub term: Term,
    pub index: LogIndex,
}

/// a proposal failure that is safe for the client to retry
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalFailure {
    /// the local node lost leadership before a known apply result existed
    LeadershipLost {
        proposed_term: Term,
        observed_term: Term,
    },

    /// The client deadline elapsed before the proposal produced an apply result
    DeadlineExceeded,
}

/// Completion delivered to the proposal response waiter
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalCompletion<R, E = ()> {
    /// The matching committed entry was applied by the state machine
    Applied {
        request_id: RequestId,
        position: ProposalPosition,
        result: R,
    },

    /// matching committed entry was applied as a deterministic rejection
    ///
    /// this is a known, non-retryable database outcome. It is distinct from a
    /// retryable host failure because replaying the same `RequestId` must return
    /// the same rejection from replicated deduplication state
    Rejected {
        request_id: RequestId,
        position: ProposalPosition,
        rejection: E,
    },

    /// The proposal did not produce a known apply result on this attempt
    Retryable {
        request_id: RequestId,
        position: ProposalPosition,
        failure: ProposalFailure,
    },
}

/// errors raised by proposal bookkeeping
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProposalRegistryError {
    #[error("request {request_id:?} is already being tracked")]
    DuplicateRequest { request_id: RequestId },

    #[error("request {request_id:?} is not being tracked")]
    UnknownRequest { request_id: RequestId },

    #[error(
        "apply position mismatch for request {request_id:?}: \
         expected {expected:?}, received {received:?}"
    )]
    ApplyPositionMismatch {
        request_id: RequestId,
        expected: ProposalPosition,
        received: ProposalPosition,
    },

    #[error("response channel for request {request_id:?} is closed")]
    ResponseChannelClosed { request_id: RequestId },
}

/// client side handle for one tracked proposal
pub struct ProposalTicket<R, E = ()> {
    request_id: RequestId,
    position: ProposalPosition,
    deadline: Instant,
    receiver: Receiver<ProposalCompletion<R, E>>,
}

impl<R, E> ProposalTicket<R, E> {
    /// Return the request identity associated with this response waiter.
    pub fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    /// Return the Raft position captured when the proposal was admitted.
    pub const fn position(&self) -> ProposalPosition {
        self.position
    }

    /// Return the deadline assigned to this proposal attempt.
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Wait for the apply result or an explicit retryable completion.
    pub fn recv(self) -> Result<ProposalCompletion<R, E>, RecvError> {
        self.receiver.recv()
    }

    /// Wait for a bounded period for the proposal completion.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ProposalCompletion<R, E>, RecvTimeoutError> {
        self.receiver.recv_timeout(timeout)
    }

    /// Inspect the proposal without blocking the caller.
    pub fn try_recv(&self) -> Result<ProposalCompletion<R, E>, TryRecvError> {
        self.receiver.try_recv()
    }
}

struct PendingProposal<R, E> {
    position: ProposalPosition,
    deadline: Instant,
    sender: Sender<ProposalCompletion<R, E>>,
}

/// Tracks proposals admitted by one Raft group.
pub struct ProposalRegistry<R, E = ()> {
    pending: HashMap<RequestId, PendingProposal<R, E>>,
}

impl<R, E> ProposalRegistry<R, E> {
    /// Construct an empty proposal registry.
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    /// Register a proposal after `RaftNode::propose_with_size` succeeds.
    ///
    /// Registering after successful Raft admission ensures that every tracked
    /// proposal has the exact term and log index assigned by the Raft core.
    pub fn register(
        &mut self,
        request_id: RequestId,
        position: ProposalPosition,
        deadline: Instant,
    ) -> Result<ProposalTicket<R, E>, ProposalRegistryError> {
        if self.pending.contains_key(&request_id) {
            return Err(ProposalRegistryError::DuplicateRequest { request_id });
        }

        let (sender, receiver) = mpsc::channel();

        self.pending.insert(
            request_id.clone(),
            PendingProposal {
                position,
                deadline,
                sender,
            },
        );

        Ok(ProposalTicket {
            request_id,
            position,
            deadline,
            receiver,
        })
    }

    /// Resolve a proposal only after its exact log entry has been applied.
    ///
    /// Position validation happens before removing the pending request so a
    /// malformed or stale apply notification cannot lose the real waiter.
    pub fn resolve_applied(
        &mut self,
        request_id: &RequestId,
        applied_position: ProposalPosition,
        result: R,
    ) -> Result<(), ProposalRegistryError> {
        let Some(expected_position) = self.pending.get(request_id).map(|pending| pending.position)
        else {
            return Err(ProposalRegistryError::UnknownRequest {
                request_id: request_id.clone(),
            });
        };

        if expected_position != applied_position {
            return Err(ProposalRegistryError::ApplyPositionMismatch {
                request_id: request_id.clone(),
                expected: expected_position,
                received: applied_position,
            });
        }

        let Some(pending) = self.pending.remove(request_id) else {
            return Err(ProposalRegistryError::UnknownRequest {
                request_id: request_id.clone(),
            });
        };

        let completion = ProposalCompletion::Applied {
            request_id: request_id.clone(),
            position: applied_position,
            result,
        };

        pending
            .sender
            .send(completion)
            .map_err(|_| ProposalRegistryError::ResponseChannelClosed {
                request_id: request_id.clone(),
            })
    }

    /// resolve a proposal after its committed entry reaches a deterministic,
    /// non retryable state machine rejection
    ///
    /// position validation is identical to the successful apply path: an
    /// unrelated entry must never consume the client's actual response waiter
    pub fn resolve_rejected(
        &mut self,
        request_id: &RequestId,
        applied_position: ProposalPosition,
        rejection: E,
    ) -> Result<(), ProposalRegistryError> {
        let Some(expected_position) = self.pending.get(request_id).map(|pending| pending.position)
        else {
            return Err(ProposalRegistryError::UnknownRequest {
                request_id: request_id.clone(),
            });
        };

        if expected_position != applied_position {
            return Err(ProposalRegistryError::ApplyPositionMismatch {
                request_id: request_id.clone(),
                expected: expected_position,
                received: applied_position,
            });
        }

        let Some(pending) = self.pending.remove(request_id) else {
            return Err(ProposalRegistryError::UnknownRequest {
                request_id: request_id.clone(),
            });
        };

        let completion = ProposalCompletion::Rejected {
            request_id: request_id.clone(),
            position: applied_position,
            rejection,
        };

        pending
            .sender
            .send(completion)
            .map_err(|_| ProposalRegistryError::ResponseChannelClosed {
                request_id: request_id.clone(),
            })
    }

    /// complete every outstanding proposal when this node loses leadership
    ///
    /// The state machine entry may still apply later on another node. That
    /// later retry is reconciled through the same RequestId and replicated
    /// deduplication state in the next slice
    pub fn mark_leadership_lost(&mut self, observed_term: Term) -> usize {
        let pending = std::mem::take(&mut self.pending);
        let count = pending.len();

        for (request_id, pending) in pending {
            let _ = pending.sender.send(ProposalCompletion::Retryable {
                request_id,
                position: pending.position,
                failure: ProposalFailure::LeadershipLost {
                    proposed_term: pending.position.term,
                    observed_term,
                },
            });
        }

        count
    }

    /// Complete proposals whose client deadlines have elapsed.
    pub fn expire_deadlines(&mut self, now: Instant) -> usize {
        let expired = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.deadline <= now)
            .map(|(request_id, _)| request_id.clone())
            .collect::<Vec<_>>();

        let mut completed = 0;

        for request_id in expired {
            let Some(pending) = self.pending.remove(&request_id) else {
                continue;
            };

            let _ = pending.sender.send(ProposalCompletion::Retryable {
                request_id,
                position: pending.position,
                failure: ProposalFailure::DeadlineExceeded,
            });

            completed += 1;
        }

        completed
    }

    /// return the number of proposals still awaiting apply or retry completion
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// return whether a request already has an outstanding response waiter.
    pub fn is_pending(&self, request_id: &RequestId) -> bool {
        self.pending.contains_key(request_id)
    }
}

impl<R, E> Default for ProposalRegistry<R, E> {
    fn default() -> Self {
        Self::new()
    }
}
