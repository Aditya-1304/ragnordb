use ragnordb_common::{
    Error,
    codec::{Row, TxnStatus, Value},
    encoding::encode_row,
    ids::{ClientRequestId, RaftGroupId, ReplicaId, TableId, TabletId, Timestamp, TxnId},
};
use ragnordb_storage::key::{encode_row_key, make_row_key};
use ragnordb_txn::{
    DistributedTransactionCoordinator, ParticipantRoute, Transaction, TransactionStatusKey,
};

fn key(value: i64) -> Vec<u8> {
    encode_row_key(&make_row_key(TableId(1), &[Value::Int(value)]).unwrap()).unwrap()
}

fn row(value: i64) -> Vec<u8> {
    encode_row(&Row {
        values: vec![Value::Int(value), Value::Text(format!("value-{value}"))],
    })
    .unwrap()
}

fn route(tablet_id: u64, epoch: u64, raft_group_id: u64) -> ParticipantRoute {
    ParticipantRoute::new(
        TabletId(tablet_id),
        epoch,
        RaftGroupId(raft_group_id),
        ReplicaId(tablet_id),
    )
    .unwrap()
}

fn coordinator_without_status_route() -> DistributedTransactionCoordinator {
    let mut transaction = Transaction::new(TxnId(42), Timestamp(100)).unwrap();
    transaction.buffer_put(key(1), row(1)).unwrap();
    transaction.buffer_put(key(2), row(2)).unwrap();
    transaction.buffer_delete(key(3)).unwrap();

    let primary_key = key(1);
    let mut coordinator = DistributedTransactionCoordinator::new(
        transaction,
        ClientRequestId {
            client_id: 7,
            session_epoch: 3,
            request_sequence: 11,
        },
        primary_key.clone(),
    )
    .unwrap();
    coordinator
        .set_participant_route(primary_key, route(20, 4, 200))
        .unwrap();
    coordinator
        .set_participant_route(key(2), route(10, 5, 100))
        .unwrap();
    coordinator
        .set_participant_route(key(3), route(20, 4, 200))
        .unwrap();
    coordinator
}

fn coordinator() -> DistributedTransactionCoordinator {
    let mut coordinator = coordinator_without_status_route();
    coordinator.set_status_route(route(20, 4, 200)).unwrap();
    coordinator
}

#[test]
fn rollback_plan_fences_all_keys_and_marks_status_aborted() {
    let coordinator = coordinator();

    let plan = coordinator.plan_rollback().unwrap();

    assert_eq!(
        plan.status_key,
        TransactionStatusKey::new(TxnId(42)).unwrap()
    );
    assert_eq!(plan.status_route, route(20, 4, 200));
    assert_eq!(plan.status_record.status, TxnStatus::Aborted);
    assert_eq!(plan.status_record.commit_timestamp, None);
    assert_eq!(plan.status_record.participant_tablet_ids, vec![20, 10]);

    assert_eq!(plan.primary.route, route(20, 4, 200));
    assert_eq!(plan.primary.command.keys, vec![key(1), key(3)]);
    assert_eq!(plan.primary.participant_plans.len(), 2);

    assert_eq!(plan.secondary.len(), 1);
    assert_eq!(plan.secondary[0].route, route(10, 5, 100));
    assert_eq!(plan.secondary[0].command.keys, vec![key(2)]);
    assert_eq!(plan.secondary[0].participant_plans.len(), 1);

    plan.status_record.validate().unwrap();
    plan.primary.command.validate().unwrap();
    for batch in &plan.secondary {
        batch.command.validate().unwrap();
    }
}

#[test]
fn rollback_plan_validates_status_and_participant_routes_before_building() {
    let coordinator = coordinator_without_status_route();

    assert!(matches!(
        coordinator.plan_rollback(),
        Err(Error::InvalidArgument(message)) if message.contains("status route")
    ));
}

#[test]
fn rollback_plan_preserves_logical_identity_after_route_refresh() {
    let mut coordinator = coordinator();
    let before = coordinator.plan_rollback().unwrap();

    coordinator
        .set_participant_route(key(2), route(11, 8, 111))
        .unwrap();
    let after = coordinator.plan_rollback().unwrap();

    assert_eq!(before.primary.command, after.primary.command);
    assert_eq!(
        before.secondary[0].participant_plans[0].command_id,
        after.secondary[0].participant_plans[0].command_id
    );
    assert_eq!(
        before.secondary[0].participant_plans[0].logical_command_id,
        after.secondary[0].participant_plans[0].logical_command_id
    );
    assert_ne!(
        before.secondary[0].participant_plans[0].request_id,
        after.secondary[0].participant_plans[0].request_id
    );
    assert_eq!(after.secondary[0].route, route(11, 8, 111));
    assert_eq!(after.status_record.participant_tablet_ids, vec![20, 11]);
}

#[test]
fn rollback_plan_rejects_conflicting_route_hints_before_batch_creation() {
    let mut coordinator = coordinator();
    coordinator
        .set_participant_route(key(3), route(20, 9, 209))
        .unwrap();

    assert!(matches!(
        coordinator.plan_rollback(),
        Err(Error::InvalidArgument(message)) if message.contains("conflicting route")
    ));
}
