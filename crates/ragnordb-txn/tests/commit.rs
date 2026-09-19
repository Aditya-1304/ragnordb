use ragnordb_common::{
    Error,
    codec::{Row, TxnStatus, Value},
    encoding::encode_row,
    ids::{ClientRequestId, RaftGroupId, ReplicaId, TableId, TabletId, Timestamp, TxnId},
};
use ragnordb_storage::key::{encode_row_key, make_row_key};
use ragnordb_txn::{
    DistributedTransactionCoordinator, LocalTransactionManager, ParticipantRoute, Transaction,
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

fn root_request() -> ClientRequestId {
    ClientRequestId {
        client_id: 7,
        session_epoch: 3,
        request_sequence: 11,
    }
}

fn coordinator_without_status_route() -> DistributedTransactionCoordinator {
    let mut transaction = Transaction::new(TxnId(42), Timestamp(100)).unwrap();
    transaction.buffer_put(key(1), row(1)).unwrap();
    transaction.buffer_put(key(2), row(2)).unwrap();
    transaction.buffer_delete(key(3)).unwrap();

    let primary_key = key(1);
    let mut coordinator =
        DistributedTransactionCoordinator::new(transaction, root_request(), primary_key.clone())
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
fn commit_plan_allocates_timestamp_and_orders_primary_before_secondaries() {
    let coordinator = coordinator();
    let mut timestamps = LocalTransactionManager::new();

    let plan = coordinator.plan_commit(&mut timestamps).unwrap();

    assert_eq!(plan.commit_timestamp, Timestamp(101));
    assert_eq!(plan.status_record.status, TxnStatus::Committed);
    assert_eq!(plan.status_record.commit_timestamp, Some(Timestamp(101)));
    assert_eq!(plan.status_record.participant_tablet_ids, vec![20, 10]);
    assert_eq!(plan.status_route, route(20, 4, 200));

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
fn commit_plan_validates_routes_before_allocating_commit_timestamp() {
    let coordinator = coordinator_without_status_route();
    let mut timestamps = LocalTransactionManager::new();

    assert!(matches!(
        coordinator.plan_commit(&mut timestamps),
        Err(Error::InvalidArgument(message)) if message.contains("status route")
    ));
    assert_eq!(timestamps.last_allocated_timestamp(), Timestamp(0));
}

#[test]
fn commit_plan_rejects_a_status_route_that_does_not_match_the_primary() {
    let mut coordinator = coordinator_without_status_route();
    coordinator.set_status_route(route(99, 1, 999)).unwrap();
    let mut timestamps = LocalTransactionManager::new();

    assert!(matches!(
        coordinator.plan_commit(&mut timestamps),
        Err(Error::InvalidArgument(message)) if message.contains("primary participant route")
    ));
    assert_eq!(timestamps.last_allocated_timestamp(), Timestamp(0));
}

#[test]
fn commit_plan_preserves_logical_command_identity_after_route_refresh() {
    let mut coordinator = coordinator();
    let mut before_timestamps = LocalTransactionManager::new();
    let before = coordinator.plan_commit(&mut before_timestamps).unwrap();

    coordinator
        .set_participant_route(key(2), route(11, 8, 111))
        .unwrap();
    let mut after_timestamps = LocalTransactionManager::new();
    let after = coordinator.plan_commit(&mut after_timestamps).unwrap();

    assert_eq!(before.commit_timestamp, after.commit_timestamp);
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
fn commit_plan_rejects_conflicting_route_hints_before_timestamp_allocation() {
    let mut coordinator = coordinator();
    coordinator
        .set_participant_route(key(3), route(20, 9, 209))
        .unwrap();
    let mut timestamps = LocalTransactionManager::new();

    assert!(matches!(
        coordinator.plan_commit(&mut timestamps),
        Err(Error::InvalidArgument(message)) if message.contains("conflicting route")
    ));
    assert_eq!(timestamps.last_allocated_timestamp(), Timestamp(0));
}
