use std::time::{Duration, Instant};

use prost::Message;
use ragnordb_common::{
    codec::{Row, Value, WriteKind},
    command_codec::{
        CommitCommand, NoopCommand, PrewriteCommand, SingleShardCommitCommand, TabletCommand,
        TabletCommandBatchEnvelope, TabletCommandEnvelope, WriteEntry,
    },
    ids::{RaftGroupId, ReplicaId, RequestId, TableId, TabletId},
    proto::command,
};
use ragnordb_multiraft::{
    proposal::{ProposalCompletion, ProposalPosition, ProposalRegistry},
    tablet_apply::{
        CommittedTabletCommandDisposition, CommittedTabletCommandEntry, RejectedTabletCommand,
        TabletApplyError, TabletCommandApplier,
    },
};
use ragnordb_storage::{
    key::{encode_row_key, make_row_key},
    lsm::RecoveryFrontier,
    mvcc::MvccStorage,
};
use ragnordb_tablet::{
    Tablet,
    command::{
        TabletCommandApplyError, TabletCommandApplyOutcome, TabletCommandApplyResult,
        TabletReadGeneration, TabletStateMachine,
    },
    snapshot::{
        AppliedTabletFrontier, TabletSnapshotConfState, TabletSnapshotInstallTarget,
        generate_local_snapshot, restore_verified_snapshot,
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
    let state_machine =
        TabletStateMachine::new_local_reference(tablet, TABLET_EPOCH, RAFT_GROUP_ID).unwrap();

    TabletCommandApplier::new(state_machine)
}

fn applier_with_frontier(index: u64, term: u64) -> TabletCommandApplier {
    let mut applier = applier();
    applier
        .state_machine_mut()
        .restore_recovery_frontier(index, term)
        .unwrap();
    applier
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
    let mut applier = applier_with_frontier(6, 2);
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
        applier.state_machine().recovery_frontier(),
        Some(RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RAFT_GROUP_ID,
            replica_id: ReplicaId(1),
            applied_index: position.index,
            applied_term: position.term,
        })
    );
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

#[test]
fn committed_write_rejection_caches_result_and_advances_complete_frontier() {
    let mut applier = applier();
    let key = encode_row_key(&make_row_key(TABLE_ID, &[Value::Int(7)]).unwrap()).unwrap();
    let first_request = request_id();
    let first = TabletCommandEnvelope::new(
        first_request.clone(),
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::SingleShardCommit(SingleShardCommitCommand {
            txn_id: ragnordb_common::ids::TxnId(1),
            start_timestamp: ragnordb_common::ids::Timestamp(10),
            commit_timestamp: ragnordb_common::ids::Timestamp(100),
            writes: vec![WriteEntry {
                key: key.clone(),
                row: Some(Row {
                    values: vec![Value::Int(7)],
                }),
                op: WriteKind::Put,
            }],
        }),
    )
    .unwrap()
    .encode()
    .unwrap();
    let rejected_request = RequestId {
        client_id: first_request.client_id,
        sequence: 2,
        raft_group_id: RAFT_GROUP_ID,
    };
    let rejected = TabletCommandEnvelope::new(
        rejected_request.clone(),
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::SingleShardCommit(SingleShardCommitCommand {
            txn_id: ragnordb_common::ids::TxnId(2),
            start_timestamp: ragnordb_common::ids::Timestamp(20),
            commit_timestamp: ragnordb_common::ids::Timestamp(50),
            writes: vec![WriteEntry {
                key: key.clone(),
                row: Some(Row {
                    values: vec![Value::Int(8)],
                }),
                op: WriteKind::Put,
            }],
        }),
    )
    .unwrap()
    .encode()
    .unwrap();

    applier
        .apply_committed(ProposalPosition { term: 2, index: 1 }, &first)
        .unwrap();
    let first_rejection = applier
        .apply_committed(ProposalPosition { term: 2, index: 2 }, &rejected)
        .unwrap();
    assert!(matches!(
        first_rejection,
        CommittedTabletCommandDisposition::Rejected(_)
    ));
    assert_eq!(
        applier.state_machine().recovery_frontier(),
        Some(RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RAFT_GROUP_ID,
            replica_id: ReplicaId(1),
            applied_index: 2,
            applied_term: 2,
        })
    );

    let retry = applier
        .apply_committed(ProposalPosition { term: 3, index: 3 }, &rejected)
        .unwrap();
    assert!(matches!(
        retry,
        CommittedTabletCommandDisposition::Rejected(RejectedTabletCommand {
            rejection: TabletCommandApplyError::WriteConflict { .. },
            ..
        })
    ));
    assert_eq!(
        applier.state_machine().recovery_frontier(),
        Some(RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RAFT_GROUP_ID,
            replica_id: ReplicaId(1),
            applied_index: 3,
            applied_term: 3,
        })
    );
    assert_eq!(
        applier
            .state_machine()
            .tablet()
            .storage()
            .read(&key, ragnordb_common::ids::Timestamp(100))
            .unwrap(),
        Some(
            ragnordb_common::encoding::encode_row(&Row {
                values: vec![Value::Int(7)],
            })
            .unwrap()
        )
    );
}

/// Realistic bug caught:
///
/// Corrupt committed bytes must fail before reaching the tablet state machine.
/// The following valid sequence-1 command must still be accepted, proving the
/// malformed entry did not consume replicated request state.
#[test]
fn malformed_committed_entry_does_not_consume_request_sequence() {
    let mut applier = applier_with_frontier(6, 2);
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
    let mut applier = applier_with_frontier(6, 2);
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
    let mut applier = applier_with_frontier(7, 2);
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
/// Commands inside one committed batch execute in order against the staged
/// effects of earlier commands, while the complete batch publishes once at
/// its single Raft position. A prewrite followed by its commit must therefore
/// produce a committed row without exposing an intermediate lock generation.
#[test]
fn committed_batch_prewrite_then_commit_uses_one_staged_generation() {
    let mut applier = applier_with_frontier(11, 3);
    let key = encode_row_key(&make_row_key(TABLE_ID, &[Value::Int(55)]).unwrap()).unwrap();
    let prewrite = TabletCommandEnvelope::new(
        RequestId {
            client_id: 501,
            sequence: 1,
            raft_group_id: RAFT_GROUP_ID,
        },
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::Prewrite(PrewriteCommand {
            txn_id: ragnordb_common::ids::TxnId(501),
            start_timestamp: ragnordb_common::ids::Timestamp(10),
            writes: vec![WriteEntry {
                key: key.clone(),
                row: Some(Row {
                    values: vec![Value::Int(55)],
                }),
                op: WriteKind::Put,
            }],
            primary_key: key.clone(),
            ttl_ms: 30_000,
            pending_status: None,
        }),
    )
    .unwrap();
    let commit = TabletCommandEnvelope::new(
        RequestId {
            client_id: 502,
            sequence: 1,
            raft_group_id: RAFT_GROUP_ID,
        },
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::Commit(CommitCommand {
            txn_id: ragnordb_common::ids::TxnId(501),
            start_timestamp: ragnordb_common::ids::Timestamp(10),
            commit_timestamp: ragnordb_common::ids::Timestamp(20),
            keys: vec![key.clone()],
            committed_status: None,
        }),
    )
    .unwrap();
    let entry = TabletCommandBatchEnvelope::new(vec![prewrite, commit])
        .unwrap()
        .encode()
        .unwrap();
    let position = ProposalPosition { term: 4, index: 12 };

    let CommittedTabletCommandEntry::Batch(dispositions) =
        applier.apply_committed_entry(position, &entry).unwrap()
    else {
        panic!("batch entry was not reported as a batch");
    };

    assert!(
        dispositions.iter().all(|disposition| matches!(
            disposition,
            CommittedTabletCommandDisposition::Applied(_)
        ))
    );
    assert_eq!(
        applier.state_machine().recovery_frontier(),
        Some(RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RAFT_GROUP_ID,
            replica_id: ReplicaId(1),
            applied_index: position.index,
            applied_term: position.term,
        })
    );
    assert!(
        applier
            .state_machine()
            .tablet()
            .storage()
            .scan_intent_page(None, None, None, 10)
            .unwrap()
            .locks
            .is_empty()
    );
    assert_eq!(
        applier
            .state_machine()
            .tablet()
            .storage()
            .read(&key, ragnordb_common::ids::Timestamp(20))
            .unwrap(),
        Some(
            ragnordb_common::encoding::encode_row(&Row {
                values: vec![Value::Int(55)],
            })
            .unwrap()
        )
    );
}

/// Realistic bug caught: a deterministic conflict between two successful
/// subcommands must be cached inside the same entry publication, and the entry
/// advances one shared frontier only after all three outcomes are prepared.
#[test]
fn mixed_batch_publishes_success_rejection_success_at_one_frontier() {
    let mut applier = applier();
    let locked_key = encode_row_key(&make_row_key(TABLE_ID, &[Value::Int(70)]).unwrap()).unwrap();
    let first_key = encode_row_key(&make_row_key(TABLE_ID, &[Value::Int(71)]).unwrap()).unwrap();
    let last_key = encode_row_key(&make_row_key(TABLE_ID, &[Value::Int(72)]).unwrap()).unwrap();
    let initial_prewrite = TabletCommandEnvelope::new(
        RequestId {
            client_id: 700,
            sequence: 1,
            raft_group_id: RAFT_GROUP_ID,
        },
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::Prewrite(PrewriteCommand {
            txn_id: ragnordb_common::ids::TxnId(700),
            start_timestamp: ragnordb_common::ids::Timestamp(10),
            writes: vec![WriteEntry {
                key: locked_key.clone(),
                row: Some(Row {
                    values: vec![Value::Int(70)],
                }),
                op: WriteKind::Put,
            }],
            primary_key: locked_key.clone(),
            ttl_ms: 30_000,
            pending_status: None,
        }),
    )
    .unwrap();
    applier
        .apply_committed_entry(
            ProposalPosition { term: 4, index: 1 },
            &initial_prewrite.encode().unwrap(),
        )
        .unwrap();

    let commit = |client_id, txn_id, key: Vec<u8>, value| {
        TabletCommandEnvelope::new(
            RequestId {
                client_id,
                sequence: 1,
                raft_group_id: RAFT_GROUP_ID,
            },
            TABLET_ID,
            TABLET_EPOCH,
            TabletCommand::SingleShardCommit(SingleShardCommitCommand {
                txn_id: ragnordb_common::ids::TxnId(txn_id),
                start_timestamp: ragnordb_common::ids::Timestamp(txn_id),
                commit_timestamp: ragnordb_common::ids::Timestamp(txn_id + 1),
                writes: vec![WriteEntry {
                    key,
                    row: Some(Row {
                        values: vec![Value::Int(value)],
                    }),
                    op: WriteKind::Put,
                }],
            }),
        )
        .unwrap()
    };
    let first = commit(710, 20, first_key.clone(), 71);
    let conflict = TabletCommandEnvelope::new(
        RequestId {
            client_id: 711,
            sequence: 1,
            raft_group_id: RAFT_GROUP_ID,
        },
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::Prewrite(PrewriteCommand {
            txn_id: ragnordb_common::ids::TxnId(711),
            start_timestamp: ragnordb_common::ids::Timestamp(30),
            writes: vec![WriteEntry {
                key: locked_key.clone(),
                row: Some(Row {
                    values: vec![Value::Int(700)],
                }),
                op: WriteKind::Put,
            }],
            primary_key: locked_key.clone(),
            ttl_ms: 30_000,
            pending_status: None,
        }),
    )
    .unwrap();
    let last = commit(712, 40, last_key.clone(), 72);
    let batch =
        TabletCommandBatchEnvelope::new(vec![first.clone(), conflict.clone(), last.clone()])
            .unwrap()
            .encode()
            .unwrap();
    let position = ProposalPosition { term: 4, index: 2 };

    let CommittedTabletCommandEntry::Batch(dispositions) =
        applier.apply_committed_entry(position, &batch).unwrap()
    else {
        panic!("mixed committed entry was not reported as a batch");
    };
    assert!(matches!(
        &dispositions[0],
        CommittedTabletCommandDisposition::Applied(_)
    ));
    let conflict_error = match &dispositions[1] {
        CommittedTabletCommandDisposition::Rejected(rejected) => {
            assert_eq!(rejected.request_id, conflict.request_id);
            rejected.rejection.clone()
        }
        CommittedTabletCommandDisposition::Applied(_) => {
            panic!("the second subcommand must deterministically conflict")
        }
    };
    assert!(matches!(
        conflict_error,
        TabletCommandApplyError::WriteConflict { .. }
    ));
    assert!(matches!(
        &dispositions[2],
        CommittedTabletCommandDisposition::Applied(_)
    ));
    assert_eq!(
        applier.state_machine().recovery_frontier(),
        Some(RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RAFT_GROUP_ID,
            replica_id: ReplicaId(1),
            applied_index: 2,
            applied_term: 4,
        })
    );

    assert_eq!(
        applier
            .state_machine()
            .tablet()
            .storage()
            .read(&first_key, ragnordb_common::ids::Timestamp(21))
            .unwrap(),
        Some(
            ragnordb_common::encoding::encode_row(&Row {
                values: vec![Value::Int(71)],
            })
            .unwrap()
        )
    );
    assert_eq!(
        applier
            .state_machine()
            .tablet()
            .storage()
            .read(&last_key, ragnordb_common::ids::Timestamp(41))
            .unwrap(),
        Some(
            ragnordb_common::encoding::encode_row(&Row {
                values: vec![Value::Int(72)],
            })
            .unwrap()
        )
    );

    // Reconstruct the complete serving generation, then replay the same three
    // requests after the original apply reply has been discarded.
    let image = generate_local_snapshot(
        applier.state_machine(),
        "stage4.3-mixed-batch",
        ReplicaId(1),
        704,
        TabletSnapshotConfState::new(7, [ReplicaId(1), ReplicaId(2), ReplicaId(3)], [], [])
            .unwrap(),
        AppliedTabletFrontier::new(2, 4),
    )
    .unwrap();
    let target = TabletSnapshotInstallTarget {
        cluster_id: "stage4.3-mixed-batch".to_string(),
        raft_group_id: RAFT_GROUP_ID,
        replica_id: ReplicaId(1),
        tablet_id: TABLET_ID,
        table_id: TABLE_ID,
        tablet_epoch: TABLET_EPOCH,
    };
    let restored = restore_verified_snapshot(&image, &target)
        .unwrap()
        .state_machine;
    let mut restored_applier = TabletCommandApplier::new(restored);
    assert_eq!(
        restored_applier.state_machine().recovery_frontier(),
        Some(RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RAFT_GROUP_ID,
            replica_id: ReplicaId(1),
            applied_index: 2,
            applied_term: 4,
        })
    );
    let retry_batch = TabletCommandBatchEnvelope::new(vec![first, conflict, last])
        .unwrap()
        .encode()
        .unwrap();
    let CommittedTabletCommandEntry::Batch(replayed) = restored_applier
        .apply_committed_entry(ProposalPosition { term: 4, index: 3 }, &retry_batch)
        .unwrap()
    else {
        panic!("replayed command entry was not reported as a batch");
    };
    assert!(matches!(
        &replayed[0],
        CommittedTabletCommandDisposition::Applied(applied) if applied.outcome.deduplicated
    ));
    assert!(matches!(
        &replayed[1],
        CommittedTabletCommandDisposition::Rejected(rejected)
            if rejected.rejection == conflict_error
    ));
    assert!(matches!(
        &replayed[2],
        CommittedTabletCommandDisposition::Applied(applied) if applied.outcome.deduplicated
    ));
    assert_eq!(
        restored_applier
            .state_machine()
            .tablet()
            .stats()
            .default_versions,
        3
    );
    assert_eq!(restored_applier.state_machine().tablet().stats().locks, 1);
    assert_eq!(
        restored_applier.state_machine().recovery_frontier(),
        Some(RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RAFT_GROUP_ID,
            replica_id: ReplicaId(1),
            applied_index: 3,
            applied_term: 4,
        })
    );
}

/// Realistic bug caught:
///
/// The old bridge published each subcommand before preparing the next one.
/// A later fatal state-integrity error could therefore leave an earlier
/// subcommand visible while the Raft entry had no complete processed
/// frontier. A failed batch must discard all sparse staged edits.
#[test]
fn fatal_later_batch_subcommand_discards_earlier_prewrite_and_frontier() {
    let mut applier = applier_with_frontier(12, 3);
    let generation_before_entry = applier.state_machine().pin_generation().unwrap();
    let key = encode_row_key(&make_row_key(TABLE_ID, &[Value::Int(56)]).unwrap()).unwrap();
    let make_prewrite = |client_id, ttl_ms| {
        TabletCommandEnvelope::new(
            RequestId {
                client_id,
                sequence: 1,
                raft_group_id: RAFT_GROUP_ID,
            },
            TABLET_ID,
            TABLET_EPOCH,
            TabletCommand::Prewrite(PrewriteCommand {
                txn_id: ragnordb_common::ids::TxnId(506),
                start_timestamp: ragnordb_common::ids::Timestamp(10),
                writes: vec![WriteEntry {
                    key: key.clone(),
                    row: Some(Row {
                        values: vec![Value::Int(56)],
                    }),
                    op: WriteKind::Put,
                }],
                primary_key: key.clone(),
                ttl_ms,
                pending_status: None,
            }),
        )
        .unwrap()
    };
    let entry = TabletCommandBatchEnvelope::new(vec![
        make_prewrite(506, 30_000),
        make_prewrite(507, 60_000),
    ])
    .unwrap()
    .encode()
    .unwrap();

    assert!(matches!(
        applier.apply_committed_entry(ProposalPosition { term: 4, index: 13 }, &entry),
        Err(TabletApplyError::FatalApply(
            TabletCommandApplyError::CorruptState { .. }
        ))
    ));
    assert!(
        applier
            .state_machine()
            .tablet()
            .storage()
            .scan_intent_page(None, None, None, 10)
            .unwrap()
            .locks
            .is_empty()
    );
    assert_eq!(
        applier.state_machine().recovery_frontier(),
        Some(RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RAFT_GROUP_ID,
            replica_id: ReplicaId(1),
            applied_index: 12,
            applied_term: 3,
        })
    );
    assert_eq!(
        generation_before_entry.processed_frontier(),
        Some(RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RAFT_GROUP_ID,
            replica_id: ReplicaId(1),
            applied_index: 12,
            applied_term: 3,
        })
    );
}

/// Realistic bug caught:
///
/// A malformed later subcommand must not allow an earlier valid mutation to
/// execute before the batch is rejected. Reapplying that valid command as a
/// standalone committed entry must therefore still be a fresh apply.
#[test]
fn malformed_committed_batch_is_rejected_before_any_subcommand_applies() {
    let mut applier = applier_with_frontier(8, 2);
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
            .apply_committed(ProposalPosition { term: 3, index: 9 }, &standalone)
            .unwrap(),
        CommittedTabletCommandDisposition::Applied(applied) if !applied.outcome.deduplicated
    ));
}
