use ragnordb_common::{
    Error,
    codec::{Row, Value},
    encoding::encode_row,
    ids::{ClientRequestId, RaftGroupId, ReplicaId, TableId, TabletId, Timestamp, TxnId},
};
use ragnordb_storage::key::{encode_row_key, make_row_key};
use ragnordb_txn::{DistributedTransactionCoordinator, ParticipantRoute, Transaction};

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

fn three_key_coordinator() -> DistributedTransactionCoordinator {
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

#[test]
fn prewrite_planner_groups_mutations_by_tablet_in_canonical_order() {
    let coordinator = three_key_coordinator();
    let batches = coordinator.plan_prewrite(30_000).unwrap();

    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].route.tablet_id, TabletId(10));
    assert_eq!(batches[0].command.writes.len(), 1);
    assert_eq!(batches[0].command.writes[0].key, key(2));
    assert_eq!(batches[0].participant_plans.len(), 1);

    assert_eq!(batches[1].route.tablet_id, TabletId(20));
    assert_eq!(
        batches[1]
            .command
            .writes
            .iter()
            .map(|write| write.key.clone())
            .collect::<Vec<_>>(),
        vec![key(1), key(3)]
    );
    assert_eq!(batches[1].participant_plans.len(), 2);

    for batch in &batches {
        batch.command.validate().unwrap();
        assert_eq!(batch.command.txn_id, TxnId(42));
        assert_eq!(batch.command.start_timestamp, Timestamp(100));
        assert_eq!(batch.command.primary_key, key(1));
        assert_eq!(batch.command.ttl_ms, 30_000);
    }
}

#[test]
fn prewrite_planner_rejects_missing_routes_and_primary_not_in_write_set() {
    let mut transaction = Transaction::new(TxnId(42), Timestamp(100)).unwrap();
    transaction.buffer_put(key(1), row(1)).unwrap();
    transaction.buffer_put(key(2), row(2)).unwrap();
    let mut missing_route =
        DistributedTransactionCoordinator::new(transaction, root_request(), key(1)).unwrap();
    missing_route
        .set_participant_route(key(1), route(10, 4, 100))
        .unwrap();

    assert!(matches!(
        missing_route.plan_prewrite(30_000),
        Err(Error::InvalidArgument(message)) if message.contains("current route")
    ));

    let mut only_secondary = Transaction::new(TxnId(43), Timestamp(101)).unwrap();
    only_secondary.buffer_put(key(2), row(2)).unwrap();
    let mut missing_primary =
        DistributedTransactionCoordinator::new(only_secondary, root_request(), key(1)).unwrap();
    missing_primary
        .set_participant_route(key(2), route(10, 4, 100))
        .unwrap();

    assert!(matches!(
        missing_primary.plan_prewrite(30_000),
        Err(Error::InvalidArgument(message)) if message.contains("primary key")
    ));
}

#[test]
fn prewrite_planner_rejects_conflicting_routes_for_one_tablet() {
    let mut coordinator = three_key_coordinator();
    coordinator
        .set_participant_route(key(3), route(20, 9, 209))
        .unwrap();

    assert!(matches!(
        coordinator.plan_prewrite(30_000),
        Err(Error::InvalidArgument(message)) if message.contains("conflicting route")
    ));
}

#[test]
fn prewrite_plan_keeps_logical_identity_when_route_is_refreshed() {
    let primary_key = key(1);
    let mut transaction = Transaction::new(TxnId(42), Timestamp(100)).unwrap();
    transaction.buffer_put(primary_key.clone(), row(1)).unwrap();
    let mut coordinator =
        DistributedTransactionCoordinator::new(transaction, root_request(), primary_key.clone())
            .unwrap();
    coordinator
        .set_participant_route(primary_key.clone(), route(10, 4, 100))
        .unwrap();

    let before = coordinator.plan_prewrite(30_000).unwrap();
    coordinator
        .set_participant_route(primary_key, route(20, 9, 200))
        .unwrap();
    let after = coordinator.plan_prewrite(30_000).unwrap();

    assert_eq!(
        before[0].participant_plans[0].command_id,
        after[0].participant_plans[0].command_id
    );
    assert_eq!(
        before[0].participant_plans[0].logical_command_id,
        after[0].participant_plans[0].logical_command_id
    );
    assert_ne!(
        before[0].participant_plans[0].request_id,
        after[0].participant_plans[0].request_id
    );
    assert_eq!(after[0].route, route(20, 9, 200));
}
