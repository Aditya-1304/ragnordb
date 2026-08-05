use std::time::{Duration, Instant};

use ragnordb_common::{
    command_codec::{NoopCommand, TabletCommand, TabletCommandEnvelope},
    ids::{RaftGroupId, RequestId, TableId, TabletId},
};
use ragnordb_multiraft::{
    proposal::{ProposalCompletion, ProposalPosition, ProposalRegistry},
    tablet_apply::{TabletApplyError, TabletCommandApplier},
};
use ragnordb_tablet::{
    Tablet,
    command::{
        TabletCommandApplyError, TabletCommandApplyOutcome, TabletCommandApplyResult,
        TabletStateMachine,
    },
};

const TABLET_ID: TabletId = TabletId(41);
const TABLE_ID: TableId = TableId(9);
const RAFT_GROUP_ID: RaftGroupId = RaftGroupId(91);
const TABLET_EPOCH: u64 = 7;

fn request_id() -> RequestId {
    RequestId {
        client_id: 41,
        sequence: 1,
        raft_group_id: RAFT_GROUP_ID,
    }
}

fn applier() -> TabletCommandApplier {
    let tablet = Tablet::new(TABLET_ID, TABLE_ID).unwrap();
    let state_machine = TabletStateMachine::new(tablet, TABLET_EPOCH, RAFT_GROUP_ID).unwrap();

    TabletCommandApplier::new(state_machine)
}

fn noop_bytes(request_id: RequestId, tablet_id: TabletId) -> Vec<u8> {
    TabletCommandEnvelope::new(
        request_id,
        tablet_id,
        TABLET_EPOCH,
        TabletCommand::Noop(NoopCommand),
    )
    .unwrap()
    .encode()
    .unwrap()
}

/// Realistic bug caught:
///
/// A committed tablet command must preserve its RequestId and exact Raft
/// position so the proposal waiter can be resolved only by the matching apply.
#[test]
fn committed_entry_resolves_proposal_from_tablet_apply_result() {
    let mut applier = applier();
    let request_id = request_id();
    let position = ProposalPosition { term: 3, index: 7 };
    let command = noop_bytes(request_id.clone(), TABLET_ID);

    let mut registry = ProposalRegistry::<TabletCommandApplyOutcome>::new();
    let ticket = registry
        .register(
            request_id.clone(),
            position,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    let applied = applier.apply_committed(position, &command).unwrap();

    assert_eq!(applied.request_id, request_id);
    assert_eq!(applied.position, position);
    assert_eq!(
        applied.outcome,
        TabletCommandApplyOutcome {
            result: TabletCommandApplyResult::Noop,
            deduplicated: false,
        }
    );

    applied.resolve(&mut registry).unwrap();

    assert_eq!(
        ticket.try_recv().unwrap(),
        ProposalCompletion::Applied {
            request_id,
            position,
            result: TabletCommandApplyOutcome {
                result: TabletCommandApplyResult::Noop,
                deduplicated: false,
            },
        }
    );
}

/// Realistic bug caught:
///
/// Corrupt committed bytes must fail before reaching the tablet state machine.
/// The following valid sequence-1 command must still be accepted, proving the
/// malformed entry did not consume replicated request state.
#[test]
fn malformed_committed_entry_does_not_consume_request_sequence() {
    let mut applier = applier();
    let position = ProposalPosition { term: 3, index: 7 };

    assert!(matches!(
        applier.apply_committed(position, b"not-a-tablet-command"),
        Err(TabletApplyError::InvalidEnvelope(_))
    ));

    let command = noop_bytes(request_id(), TABLET_ID);
    let applied = applier.apply_committed(position, &command).unwrap();

    assert!(!applied.outcome.deduplicated);
}

/// Realistic bug caught:
///
/// The bridge must not bypass TabletStateMachine routing checks. A command for
/// another tablet must remain a deterministic apply error, not a successful
/// proposal completion.
#[test]
fn committed_entry_for_another_tablet_is_rejected() {
    let mut applier = applier();
    let position = ProposalPosition { term: 3, index: 7 };
    let command = noop_bytes(request_id(), TabletId(TABLET_ID.0 + 1));

    assert_eq!(
        applier.apply_committed(position, &command).unwrap_err(),
        TabletApplyError::Apply(TabletCommandApplyError::TabletIdMismatch {
            local_tablet_id: TABLET_ID,
            requested_tablet_id: TabletId(TABLET_ID.0 + 1),
        })
    );
}
