use std::time::{Duration, Instant};

use prost::Message;
use ragnordb_common::{
    codec::{Row, Value, WriteKind},
    command_codec::{
        NoopCommand, SingleShardCommitCommand, TabletCommand, TabletCommandBatchEnvelope,
        TabletCommandEnvelope, WriteEntry,
    },
    ids::{RaftGroupId, RequestId, TableId, TabletId},
    proto::command,
};
use ragnordb_multiraft::{
    proposal::{ProposalCompletion, ProposalPosition, ProposalRegistry},
    tablet_apply::{CommittedTabletCommandDisposition, TabletApplyError, TabletCommandApplier},
};
use ragnordb_storage::key::{encode_row_key, make_row_key};
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

    let mut registry =
        ProposalRegistry::<TabletCommandApplyOutcome, TabletCommandApplyError>::new();
    let ticket = registry
        .register(
            request_id.clone(),
            position,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    let CommittedTabletCommandDisposition::Applied(applied) =
        applier.apply_committed(position, &command).unwrap()
    else {
        panic!("valid no-op was unexpectedly rejected");
    };

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
    let disposition = applier.apply_committed(position, &command).unwrap();

    assert!(matches!(
        disposition,
        CommittedTabletCommandDisposition::Applied(applied) if !applied.outcome.deduplicated
    ));
}

/// Realistic bug caught:
///
/// The bridge must not bypass TabletStateMachine routing checks. A command for
/// another tablet consumes its committed Raft position as a deterministic
/// rejection, but must never be reported as a successful proposal.
#[test]
fn committed_entry_for_another_tablet_is_rejected() {
    let mut applier = applier();
    let position = ProposalPosition { term: 3, index: 7 };
    let command = noop_bytes(request_id(), TabletId(TABLET_ID.0 + 1));

    assert!(matches!(
        applier.apply_committed(position, &command).unwrap(),
        CommittedTabletCommandDisposition::Rejected(rejected)
            if rejected.rejection
                == TabletCommandApplyError::TabletIdMismatch {
                    local_tablet_id: TABLET_ID,
                    requested_tablet_id: TabletId(TABLET_ID.0 + 1),
                }
    ));
}

/// Realistic bug caught:
///
/// A batched Raft entry must apply every already-routed mutation in FIFO order
/// and return one independently correlated disposition per subcommand. This
/// catches an applier that decodes only the first envelope or advances the
/// batch as if it had one client identity.
#[test]
fn committed_batch_applies_each_subcommand_at_the_shared_position() {
    let mut applier = applier();
    let first_key = encode_row_key(&make_row_key(TABLE_ID, &[Value::Int(1)]).unwrap()).unwrap();
    let second_key = encode_row_key(&make_row_key(TABLE_ID, &[Value::Int(2)]).unwrap()).unwrap();

    let first_id = RequestId {
        client_id: 41,
        sequence: 1,
        raft_group_id: RAFT_GROUP_ID,
    };
    let second_id = RequestId {
        client_id: 42,
        sequence: 1,
        raft_group_id: RAFT_GROUP_ID,
    };
    let first = TabletCommandEnvelope::new(
        first_id.clone(),
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::SingleShardCommit(SingleShardCommitCommand {
            txn_id: ragnordb_common::ids::TxnId(1),
            start_timestamp: ragnordb_common::ids::Timestamp(10),
            commit_timestamp: ragnordb_common::ids::Timestamp(20),
            writes: vec![WriteEntry {
                key: first_key,
                row: Some(Row {
                    values: vec![Value::Int(1)],
                }),
                op: WriteKind::Put,
            }],
        }),
    )
    .unwrap();
    let second = TabletCommandEnvelope::new(
        second_id.clone(),
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::SingleShardCommit(SingleShardCommitCommand {
            txn_id: ragnordb_common::ids::TxnId(2),
            start_timestamp: ragnordb_common::ids::Timestamp(11),
            commit_timestamp: ragnordb_common::ids::Timestamp(21),
            writes: vec![WriteEntry {
                key: second_key,
                row: Some(Row {
                    values: vec![Value::Int(2)],
                }),
                op: WriteKind::Put,
            }],
        }),
    )
    .unwrap();
    let command = TabletCommandBatchEnvelope::new(vec![first, second])
        .unwrap()
        .encode()
        .unwrap();
    let position = ProposalPosition { term: 3, index: 8 };

    let ragnordb_multiraft::tablet_apply::CommittedTabletCommandEntry::Batch(dispositions) =
        applier.apply_committed_entry(position, &command).unwrap()
    else {
        panic!("a batch entry was not reported as a batch");
    };

    assert_eq!(dispositions.len(), 2);
    assert!(matches!(
        &dispositions[0],
        CommittedTabletCommandDisposition::Applied(applied)
            if applied.request_id == first_id && applied.position == position
    ));
    assert!(matches!(
        &dispositions[1],
        CommittedTabletCommandDisposition::Applied(applied)
            if applied.request_id == second_id && applied.position == position
    ));
}

/// Realistic bug caught:
///
/// A malformed later subcommand must not allow an earlier valid mutation to
/// execute before the batch is rejected. Reapplying that valid command as a
/// standalone committed entry must therefore still be a fresh apply.
#[test]
fn malformed_committed_batch_is_rejected_before_any_subcommand_applies() {
    let mut applier = applier();
    let key = encode_row_key(&make_row_key(TABLE_ID, &[Value::Int(3)]).unwrap()).unwrap();
    let request_id = request_id();
    let first = TabletCommandEnvelope::new(
        request_id.clone(),
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::SingleShardCommit(SingleShardCommitCommand {
            txn_id: ragnordb_common::ids::TxnId(3),
            start_timestamp: ragnordb_common::ids::Timestamp(30),
            commit_timestamp: ragnordb_common::ids::Timestamp(40),
            writes: vec![WriteEntry {
                key,
                row: Some(Row {
                    values: vec![Value::Int(3)],
                }),
                op: WriteKind::Put,
            }],
        }),
    )
    .unwrap();
    let malformed = command::TabletCommandEnvelope {
        format_version: 2,
        request_id: Some(
            RequestId {
                client_id: 43,
                sequence: 1,
                raft_group_id: RAFT_GROUP_ID,
            }
            .to_proto(),
        ),
        tablet_id: Some(TABLET_ID.to_proto()),
        expected_epoch: TABLET_EPOCH,
        command: None,
        logical_command_id: None,
        acknowledged_through: None,
    };
    let batch = command::TabletCommandBatchEnvelope {
        format_version: 1,
        commands: vec![first.to_proto().unwrap(), malformed],
    }
    .encode_to_vec();

    assert!(matches!(
        applier.apply_committed_entry(ProposalPosition { term: 3, index: 9 }, &batch),
        Err(TabletApplyError::InvalidBatch { .. })
    ));

    let standalone = first.encode().unwrap();
    assert!(matches!(
        applier
            .apply_committed(ProposalPosition { term: 3, index: 10 }, &standalone)
            .unwrap(),
        CommittedTabletCommandDisposition::Applied(applied) if !applied.outcome.deduplicated
    ));
}
