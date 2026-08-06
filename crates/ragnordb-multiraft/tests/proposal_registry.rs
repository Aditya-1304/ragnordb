use std::{
    sync::mpsc::TryRecvError,
    time::{Duration, Instant},
};

use ragnordb_common::ids::{RaftGroupId, RequestId};
use ragnordb_multiraft::proposal::{
    ProposalCompletion, ProposalFailure, ProposalPosition, ProposalRegistry, ProposalRegistryError,
};

fn request_id() -> RequestId {
    RequestId {
        client_id: 41,
        sequence: 1,
        raft_group_id: RaftGroupId(101),
    }
}

/// Realistic bug caught:
///
/// Proposal acceptance or Raft commit-index movement must not be reported as
/// client success. The response becomes successful only after the matching
/// committed log entry has been applied by the tablet state machine.
#[test]
fn proposal_completes_only_after_matching_apply_result() {
    let mut registry = ProposalRegistry::<&'static str>::new();
    let request_id = request_id();
    let position = ProposalPosition { term: 3, index: 7 };

    let ticket = registry
        .register(
            request_id.clone(),
            position,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    assert!(matches!(ticket.try_recv(), Err(TryRecvError::Empty)));

    registry
        .resolve_applied(&request_id, position, "committed")
        .unwrap();

    assert_eq!(
        ticket.try_recv().unwrap(),
        ProposalCompletion::Applied {
            request_id,
            position,
            result: "committed",
        }
    );
}

/// Realistic bug caught:
///
/// A deterministic database rejection must complete the exact proposal as a
/// known non-retryable result. Treating it as retryable could make a client
/// issue a new request identity and execute a logically rejected command again.
#[test]
fn deterministic_rejection_completes_the_matching_proposal() {
    let mut registry = ProposalRegistry::<&'static str, &'static str>::new();
    let request_id = request_id();
    let position = ProposalPosition { term: 3, index: 7 };

    let ticket = registry
        .register(
            request_id.clone(),
            position,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    registry
        .resolve_rejected(&request_id, position, "write conflict")
        .unwrap();

    assert_eq!(
        ticket.try_recv().unwrap(),
        ProposalCompletion::Rejected {
            request_id,
            position,
            rejection: "write conflict",
        }
    );
}

/// Realistic bug caught:
///
/// An apply event for the wrong term or index must not consume the pending
/// proposal, otherwise the real apply result could be lost permanently.
#[test]
fn mismatched_apply_position_keeps_proposal_pending() {
    let mut registry = ProposalRegistry::<&'static str>::new();
    let request_id = request_id();
    let expected = ProposalPosition { term: 3, index: 7 };
    let received = ProposalPosition { term: 3, index: 8 };

    let ticket = registry
        .register(
            request_id.clone(),
            expected,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    assert_eq!(
        registry
            .resolve_applied(&request_id, received, "wrong")
            .unwrap_err(),
        ProposalRegistryError::ApplyPositionMismatch {
            request_id: request_id.clone(),
            expected,
            received,
        }
    );

    registry
        .resolve_applied(&request_id, expected, "correct")
        .unwrap();

    assert!(matches!(
        ticket.try_recv(),
        Ok(ProposalCompletion::Applied {
            result: "correct",
            ..
        })
    ));
}

/// Realistic bug caught:
///
/// Leadership loss before a known apply result must not be converted into a
/// successful response. The client must retry using the same RequestId.
#[test]
fn leadership_loss_returns_retryable_result() {
    let mut registry = ProposalRegistry::<&'static str>::new();
    let request_id = request_id();
    let position = ProposalPosition { term: 3, index: 7 };

    let ticket = registry
        .register(
            request_id.clone(),
            position,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    assert_eq!(registry.mark_leadership_lost(4), 1);

    assert_eq!(
        ticket.try_recv().unwrap(),
        ProposalCompletion::Retryable {
            request_id,
            position,
            failure: ProposalFailure::LeadershipLost {
                proposed_term: 3,
                observed_term: 4,
            },
        }
    );
}

/// Realistic bug caught:
///
/// A proposal that exceeds its client deadline must complete explicitly as
/// retryable instead of leaving a response waiter permanently blocked.
#[test]
fn expired_deadline_returns_retryable_result() {
    let mut registry = ProposalRegistry::<&'static str>::new();
    let request_id = request_id();
    let position = ProposalPosition { term: 3, index: 7 };

    let ticket = registry
        .register(
            request_id.clone(),
            position,
            Instant::now().checked_sub(Duration::from_secs(1)).unwrap(),
        )
        .unwrap();

    assert_eq!(registry.expire_deadlines(Instant::now()), 1);

    assert_eq!(
        ticket.try_recv().unwrap(),
        ProposalCompletion::Retryable {
            request_id,
            position,
            failure: ProposalFailure::DeadlineExceeded,
        }
    );
}
