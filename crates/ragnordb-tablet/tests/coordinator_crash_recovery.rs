use ragnordb_common::{
    codec::{Row, TxnStatus, TxnStatusRecord, Value, WriteKind},
    command_codec::{
        CommitCommand, PrewriteCommand, ResolveIntentCommand, TabletCommand, TabletCommandEnvelope,
        WriteEntry,
    },
    ids::{
        ParticipantCommandPhase, RaftGroupId, ReplicaId, RequestId, TableId, TabletId, Timestamp,
        TxnId, participant_logical_command_id,
    },
};
use ragnordb_storage::{
    key::{encode_row_key, make_row_key},
    mvcc::MvccStorage,
};
use ragnordb_tablet::{
    IntentAwareRead, Tablet,
    command::{TabletCommandApplyResult, TabletStateMachine},
    snapshot::{
        AppliedTabletFrontier, TabletSnapshotConfState, TabletSnapshotInstallTarget,
        generate_local_snapshot, restore_verified_snapshot,
    },
};
use ragnordb_txn::{
    AuthoritativeTransactionLease, IntentResolutionDecision, Transaction, plan_intent_resolution,
};

const TABLE_ID: TableId = TableId(9);
const TABLET_EPOCH: u64 = 7;
const CLUSTER_ID: &str = "ragnordb-phase-6-9";

fn row_key(id: i64) -> ragnordb_common::ids::RowKey {
    make_row_key(TABLE_ID, &[Value::Int(id)]).unwrap()
}

fn encoded_key(id: i64) -> Vec<u8> {
    encode_row_key(&row_key(id)).unwrap()
}

fn row(id: i64, value: &str) -> Row {
    Row {
        values: vec![Value::Int(id), Value::Text(value.to_string())],
    }
}

fn conf_state() -> TabletSnapshotConfState {
    TabletSnapshotConfState::new(7, [ReplicaId(1), ReplicaId(2), ReplicaId(3)], [], []).unwrap()
}

fn state_machine(tablet_id: TabletId, raft_group_id: RaftGroupId) -> TabletStateMachine {
    let tablet = Tablet::new(tablet_id, TABLE_ID).unwrap();
    TabletStateMachine::new(tablet, TABLET_EPOCH, raft_group_id).unwrap()
}

fn restart_from_durable_snapshot(
    state_machine: &TabletStateMachine,
    replica_id: ReplicaId,
    snapshot_id: u64,
) -> TabletStateMachine {
    let image = generate_local_snapshot(
        state_machine,
        CLUSTER_ID,
        replica_id,
        snapshot_id,
        conf_state(),
        AppliedTabletFrontier::new(snapshot_id + 10, 3),
    )
    .unwrap();

    let target = TabletSnapshotInstallTarget {
        cluster_id: CLUSTER_ID.to_string(),
        raft_group_id: state_machine.raft_group_id(),
        tablet_id: state_machine.tablet().id(),
        table_id: TABLE_ID,
        tablet_epoch: TABLET_EPOCH,
    };

    restore_verified_snapshot(&image, &target)
        .unwrap()
        .state_machine
}

fn request_id(client_id: u128, sequence: u64, raft_group_id: RaftGroupId) -> RequestId {
    RequestId {
        client_id,
        sequence,
        raft_group_id,
    }
}

fn prewrite_envelope(
    tablet_id: TabletId,
    raft_group_id: RaftGroupId,
    txn_id: TxnId,
    key: Vec<u8>,
    primary_key: Vec<u8>,
    value: Row,
    request_sequence: u64,
) -> TabletCommandEnvelope {
    TabletCommandEnvelope::new(
        request_id(0x100, request_sequence, raft_group_id),
        tablet_id,
        TABLET_EPOCH,
        TabletCommand::Prewrite(PrewriteCommand {
            txn_id,
            start_timestamp: Timestamp(100),
            writes: vec![WriteEntry {
                key: key.clone(),
                row: Some(value),
                op: WriteKind::Put,
            }],
            primary_key,
            ttl_ms: 30_000,
        }),
    )
    .unwrap()
}

fn commit_envelope(
    tablet_id: TabletId,
    raft_group_id: RaftGroupId,
    txn_id: TxnId,
    key: Vec<u8>,
    request_sequence: u64,
) -> TabletCommandEnvelope {
    TabletCommandEnvelope::new(
        request_id(0x100, request_sequence, raft_group_id),
        tablet_id,
        TABLET_EPOCH,
        TabletCommand::Commit(CommitCommand {
            txn_id,
            start_timestamp: Timestamp(100),
            commit_timestamp: Timestamp(200),
            keys: vec![key],
        }),
    )
    .unwrap()
}

fn status(
    txn_id: TxnId,
    primary_key: Vec<u8>,
    state: TxnStatus,
    commit_timestamp: Option<Timestamp>,
    lease_deadline_ms: Option<u64>,
) -> TxnStatusRecord {
    TxnStatusRecord {
        txn_id,
        start_timestamp: Timestamp(100),
        commit_timestamp,
        status: state,
        primary_key,
        participant_tablet_ids: vec![1, 2],
        last_heartbeat_timestamp: Some(Timestamp(110)),
        lease_deadline_ms,
    }
}

fn resolve_after_authoritative_status(
    state_machine: &mut TabletStateMachine,
    txn_id: TxnId,
    key: Vec<u8>,
    status: &TxnStatusRecord,
    request_sequence: u64,
) -> TabletCommandApplyResult {
    let lock = state_machine
        .tablet()
        .storage()
        .intent_for_read(&key, Timestamp(150))
        .unwrap()
        .expect("the recovered participant must still expose its intent");
    let decision = plan_intent_resolution(&key, &lock, status).unwrap();
    let IntentResolutionDecision::Resolve(plan) = decision else {
        panic!("terminal status must produce a replicated resolution plan");
    };

    assert_eq!(plan.command.txn_id, txn_id);
    let envelope = TabletCommandEnvelope::new_with_logical_command_id(
        request_id(0x300, request_sequence, state_machine.raft_group_id()),
        plan.logical_command_id,
        state_machine.tablet().id(),
        TABLET_EPOCH,
        TabletCommand::ResolveIntent(plan.command),
    )
    .unwrap();

    state_machine.apply(envelope).unwrap().result
}

/// Realistic bug caught: after a coordinator disappears with only prewrites
/// durable, recovery must preserve every participant intent until the status
/// authority publishes `Aborted`; local age alone must not erase a lock.
#[test]
fn crash_after_prewrite_recovers_and_rolls_back_all_visible_intents() {
    let txn_id = TxnId(601);
    let primary_key = encoded_key(601);
    let secondary_key = encoded_key(602);
    let primary_group = RaftGroupId(601);
    let secondary_group = RaftGroupId(602);

    let mut primary = state_machine(TabletId(1), primary_group);
    let mut secondary = state_machine(TabletId(2), secondary_group);
    primary
        .apply(prewrite_envelope(
            TabletId(1),
            primary_group,
            txn_id,
            primary_key.clone(),
            primary_key.clone(),
            row(601, "primary"),
            1,
        ))
        .unwrap();
    secondary
        .apply(prewrite_envelope(
            TabletId(2),
            secondary_group,
            txn_id,
            secondary_key.clone(),
            primary_key.clone(),
            row(602, "secondary"),
            1,
        ))
        .unwrap();

    let mut primary = restart_from_durable_snapshot(&primary, ReplicaId(1), 601);
    let mut secondary = restart_from_durable_snapshot(&secondary, ReplicaId(2), 602);

    let pending = status(
        txn_id,
        primary_key.clone(),
        TxnStatus::Pending,
        None,
        Some(10),
    );
    let lease = AuthoritativeTransactionLease::new(10, 5).unwrap();
    let reader = Transaction::new(TxnId(9001), Timestamp(150)).unwrap();

    assert!(matches!(
        secondary
            .tablet()
            .get_with_intent_status_and_lease(&reader, &row_key(602), &pending, lease, 20)
            .unwrap(),
        IntentAwareRead::LeaseExpired {
            lease_deadline_ms: 10,
            ..
        }
    ));

    // The lease observation is not itself an abort. Only this authoritative
    // status transition permits the participant rollback commands below.
    let aborted = status(
        txn_id,
        primary_key.clone(),
        TxnStatus::Aborted,
        None,
        Some(10),
    );
    assert_eq!(
        resolve_after_authoritative_status(&mut primary, txn_id, primary_key.clone(), &aborted, 2,),
        TabletCommandApplyResult::ResolveIntent
    );
    assert_eq!(
        resolve_after_authoritative_status(
            &mut secondary,
            txn_id,
            secondary_key.clone(),
            &aborted,
            2,
        ),
        TabletCommandApplyResult::ResolveIntent
    );

    assert_eq!(primary.tablet().stats().locks, 0);
    assert_eq!(secondary.tablet().stats().locks, 0);
    assert_eq!(primary.tablet().stats().write_records, 1);
    assert_eq!(secondary.tablet().stats().write_records, 1);

    // A delayed participant message must not resurrect either intent after
    // the terminal rollback has been durably applied.
    let late_primary = prewrite_envelope(
        TabletId(1),
        primary_group,
        txn_id,
        primary_key.clone(),
        primary_key.clone(),
        row(601, "late-primary"),
        2,
    );
    let late_secondary = prewrite_envelope(
        TabletId(2),
        secondary_group,
        txn_id,
        secondary_key.clone(),
        primary_key,
        row(602, "late-secondary"),
        2,
    );
    assert!(matches!(
        primary.apply(late_primary),
        Err(ragnordb_tablet::command::TabletCommandApplyError::WriteConflict { .. })
    ));
    assert!(matches!(
        secondary.apply(late_secondary),
        Err(ragnordb_tablet::command::TabletCommandApplyError::WriteConflict { .. })
    ));
    assert_eq!(
        secondary.tablet().get(&reader, &row_key(602)).unwrap(),
        None
    );
}

/// Realistic bug caught: when the primary status is already committed but a
/// participant crashed before secondary cleanup, readers must derive the
/// forward-resolution command from that durable status and make the value
/// visible after the participant restarts.
#[test]
fn crash_after_primary_commit_rolls_secondary_forward_from_status() {
    let txn_id = TxnId(603);
    let primary_key = encoded_key(603);
    let secondary_key = encoded_key(604);
    let primary_group = RaftGroupId(603);
    let secondary_group = RaftGroupId(604);

    let mut primary = state_machine(TabletId(1), primary_group);
    let mut secondary = state_machine(TabletId(2), secondary_group);
    primary
        .apply(prewrite_envelope(
            TabletId(1),
            primary_group,
            txn_id,
            primary_key.clone(),
            primary_key.clone(),
            row(603, "primary"),
            1,
        ))
        .unwrap();
    secondary
        .apply(prewrite_envelope(
            TabletId(2),
            secondary_group,
            txn_id,
            secondary_key.clone(),
            primary_key.clone(),
            row(604, "secondary"),
            1,
        ))
        .unwrap();
    primary
        .apply(commit_envelope(
            TabletId(1),
            primary_group,
            txn_id,
            primary_key.clone(),
            2,
        ))
        .unwrap();

    // The secondary snapshot is taken before its cleanup command, which is
    // the crash point being modeled.
    let mut secondary = restart_from_durable_snapshot(&secondary, ReplicaId(2), 603);
    let committed = status(
        txn_id,
        primary_key,
        TxnStatus::Committed,
        Some(Timestamp(200)),
        Some(30_000),
    );
    let reader = Transaction::new(TxnId(9002), Timestamp(250)).unwrap();

    let plan = match secondary
        .tablet()
        .get_with_intent_status(&reader, &row_key(604), &committed)
        .unwrap()
    {
        IntentAwareRead::Resolve(plan) => plan,
        other => panic!("expected committed status to produce a resolution plan: {other:?}"),
    };

    let first = TabletCommandEnvelope::new_with_logical_command_id(
        request_id(0x400, 1, secondary_group),
        plan.logical_command_id,
        TabletId(2),
        TABLET_EPOCH,
        TabletCommand::ResolveIntent(plan.command.clone()),
    )
    .unwrap();
    assert_eq!(
        secondary.apply(first.clone()).unwrap().result,
        TabletCommandApplyResult::ResolveIntent
    );
    assert_eq!(secondary.tablet().stats().locks, 0);
    assert_eq!(
        secondary.tablet().get(&reader, &row_key(604)).unwrap(),
        Some(row(604, "secondary"))
    );

    // A retry with a new transport request identity must be served by the
    // durable logical command identity and must not create another version.
    let replay = TabletCommandEnvelope::new_with_logical_command_id(
        request_id(0x401, 99, secondary_group),
        plan.logical_command_id,
        TabletId(2),
        TABLET_EPOCH,
        TabletCommand::ResolveIntent(ResolveIntentCommand {
            txn_id,
            start_timestamp: Timestamp(100),
            keys: vec![secondary_key],
            resolved_status: TxnStatus::Committed,
            commit_timestamp: Some(Timestamp(200)),
        }),
    )
    .unwrap();
    let outcome = secondary.apply(replay).unwrap();
    assert!(outcome.deduplicated);
    assert_eq!(secondary.tablet().stats().write_records, 1);
}

/// Realistic bug caught: a participant crash before apply must not lose the
/// command, while a crash after apply must not duplicate it when the same
/// logical phase is replayed from a new transport attempt.
#[test]
fn participant_crash_before_and_after_apply_has_one_deterministic_outcome() {
    let txn_id = TxnId(605);
    let key = encoded_key(605);
    let group = RaftGroupId(605);
    let logical_id =
        participant_logical_command_id(txn_id, ParticipantCommandPhase::Prewrite, &key).unwrap();

    let command = TabletCommand::Prewrite(PrewriteCommand {
        txn_id,
        start_timestamp: Timestamp(100),
        writes: vec![WriteEntry {
            key: key.clone(),
            row: Some(row(605, "replayed")),
            op: WriteKind::Put,
        }],
        primary_key: key.clone(),
        ttl_ms: 30_000,
    });

    // Crash before apply: the replacement participant still applies the
    // command once when the recovered Raft entry is replayed.
    let mut before_apply = state_machine(TabletId(2), group);
    let before_apply_envelope = TabletCommandEnvelope::new_with_logical_command_id(
        request_id(0x500, 1, group),
        logical_id,
        TabletId(2),
        TABLET_EPOCH,
        command.clone(),
    )
    .unwrap();
    assert_eq!(
        before_apply.apply(before_apply_envelope).unwrap().result,
        TabletCommandApplyResult::Prewrite
    );
    assert_eq!(before_apply.tablet().stats().locks, 1);

    // Crash after apply: the logical identity is included in the snapshot,
    // so a retry with different transport metadata is a deduplicated replay.
    let mut after_apply = state_machine(TabletId(3), group);
    let applied = TabletCommandEnvelope::new_with_logical_command_id(
        request_id(0x501, 1, group),
        logical_id,
        TabletId(3),
        TABLET_EPOCH,
        command,
    )
    .unwrap();
    after_apply.apply(applied).unwrap();
    let mut after_apply = restart_from_durable_snapshot(&after_apply, ReplicaId(3), 605);
    let replay = TabletCommandEnvelope::new_with_logical_command_id(
        request_id(0x502, 77, group),
        logical_id,
        TabletId(3),
        TABLET_EPOCH,
        TabletCommand::Prewrite(PrewriteCommand {
            txn_id,
            start_timestamp: Timestamp(100),
            writes: vec![WriteEntry {
                key: key.clone(),
                row: Some(row(605, "replayed")),
                op: WriteKind::Put,
            }],
            primary_key: key,
            ttl_ms: 30_000,
        }),
    )
    .unwrap();
    let outcome = after_apply.apply(replay).unwrap();

    assert!(outcome.deduplicated);
    assert_eq!(after_apply.tablet().stats().locks, 1);
    assert_eq!(after_apply.tablet().stats().default_versions, 1);
}
