use ragnordb_common::{
    codec::{Row, Value},
    encoding::encode_row,
    ids::{
        ClientRequestId, CommandKind, ParticipantCommandPhase, RaftGroupId, TableId, TabletId,
        Timestamp, TxnId,
    },
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

fn transaction() -> Transaction {
    let mut transaction = Transaction::new(TxnId(42), Timestamp(100)).unwrap();
    transaction.buffer_put(key(1), row(1)).unwrap();
    transaction.buffer_put(key(2), row(2)).unwrap();
    transaction
}

fn root_request() -> ClientRequestId {
    ClientRequestId {
        client_id: 7,
        session_epoch: 3,
        request_sequence: 11,
    }
}

fn route(tablet_id: u64, epoch: u64, raft_group_id: u64) -> ParticipantRoute {
    ParticipantRoute::new(TabletId(tablet_id), epoch, RaftGroupId(raft_group_id)).unwrap()
}

#[test]
fn coordinator_tracks_transaction_state_and_route_hints() {
    let primary_key = key(1);
    let read_key = key(9);
    let mut coordinator =
        DistributedTransactionCoordinator::new(transaction(), root_request(), primary_key.clone())
            .unwrap();

    coordinator.record_read(read_key.clone()).unwrap();
    coordinator
        .set_participant_route(primary_key.clone(), route(10, 4, 100))
        .unwrap();
    coordinator
        .set_participant_route(key(2), route(11, 8, 101))
        .unwrap();
    coordinator.set_status_route(route(10, 4, 100)).unwrap();

    assert_eq!(coordinator.transaction_id(), TxnId(42));
    assert_eq!(coordinator.start_timestamp(), Timestamp(100));
    assert_eq!(coordinator.root_request_id(), root_request());
    assert_eq!(coordinator.primary_key(), primary_key.as_slice());
    assert_eq!(coordinator.read_set().len(), 1);
    assert_eq!(coordinator.write_set().len(), 2);
    assert_eq!(
        coordinator
            .participant_tablets()
            .into_iter()
            .collect::<Vec<_>>(),
        [TabletId(10), TabletId(11)]
    );
    assert_eq!(
        coordinator.status_location().route(),
        Some(route(10, 4, 100))
    );
}

#[test]
fn participant_command_ids_survive_topology_refresh_and_coordinator_rebuild() {
    let primary_key = key(1);
    let second_key = key(2);
    let mut coordinator =
        DistributedTransactionCoordinator::new(transaction(), root_request(), primary_key.clone())
            .unwrap();
    coordinator
        .set_participant_route(primary_key.clone(), route(10, 4, 100))
        .unwrap();

    let before_refresh = coordinator
        .participant_command_id(ParticipantCommandPhase::Prewrite, &primary_key)
        .unwrap();

    coordinator
        .set_participant_route(primary_key.clone(), route(20, 9, 200))
        .unwrap();
    let after_refresh = coordinator
        .participant_command_id(ParticipantCommandPhase::Prewrite, &primary_key)
        .unwrap();

    let rebuilt =
        DistributedTransactionCoordinator::new(transaction(), root_request(), primary_key.clone())
            .unwrap();
    let after_rebuild = rebuilt
        .participant_command_id(ParticipantCommandPhase::Prewrite, &primary_key)
        .unwrap();
    let commit = rebuilt
        .participant_command_id(ParticipantCommandPhase::Commit, &primary_key)
        .unwrap();
    let other_mutation = rebuilt
        .participant_command_id(ParticipantCommandPhase::Prewrite, &second_key)
        .unwrap();

    assert_eq!(before_refresh, after_refresh);
    assert_eq!(before_refresh, after_rebuild);
    assert_ne!(before_refresh, commit);
    assert_ne!(before_refresh, other_mutation);
    assert_eq!(after_refresh.txn_id(), TxnId(42));
    assert_eq!(after_refresh.kind(), CommandKind::Prewrite);
}

#[test]
fn coordinator_rejects_invalid_primary_reads_and_routes() {
    let invalid_transaction = transaction();
    assert!(
        DistributedTransactionCoordinator::new(invalid_transaction, root_request(), Vec::new(),)
            .is_err()
    );

    let mut coordinator =
        DistributedTransactionCoordinator::new(transaction(), root_request(), key(1)).unwrap();

    assert!(coordinator.record_read(Vec::new()).is_err());
    assert!(
        coordinator
            .set_participant_route(key(99), route(10, 4, 100))
            .is_err()
    );
    assert!(ParticipantRoute::new(TabletId(0), 1, RaftGroupId(1)).is_err());
    assert!(ParticipantRoute::new(TabletId(1), 0, RaftGroupId(1)).is_err());
    assert!(ParticipantRoute::new(TabletId(1), 1, RaftGroupId(0)).is_err());
}
