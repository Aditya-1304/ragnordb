use std::collections::BTreeSet;

use ragnordb_common::codec::{TxnStatus, TxnStatusRecord, Value};
use ragnordb_common::{
    Error,
    ids::{RaftGroupId, ReplicaId, TabletId, TxnId},
};
use ragnordb_storage::key::{encode_row_key, make_row_key};
use ragnordb_txn::{
    InMemoryTransactionStatusStore, ParticipantRoute, TransactionStatusKey,
    TransactionStatusLocation, TransactionStatusLookupError, TransactionStatusReader,
    TransactionStatusRouteResolver, TransactionStatusStore,
};

fn primary_key() -> Vec<u8> {
    encode_row_key(&make_row_key(ragnordb_common::ids::TableId(1), &[Value::Int(42)]).unwrap())
        .unwrap()
}

fn route(tablet_id: u64, epoch: u64, group_id: u64, leader_id: u64) -> ParticipantRoute {
    ParticipantRoute::new(
        TabletId(tablet_id),
        epoch,
        RaftGroupId(group_id),
        ReplicaId(leader_id),
    )
    .unwrap()
}

fn pending_record(txn_id: TxnId, primary_key: Vec<u8>) -> TxnStatusRecord {
    TxnStatusRecord {
        txn_id,
        start_timestamp: ragnordb_common::ids::Timestamp(100),
        commit_timestamp: None,
        status: TxnStatus::Pending,
        primary_key,
        participant_tablet_ids: vec![10, 5, 11],
        last_heartbeat_timestamp: Some(ragnordb_common::ids::Timestamp(100)),
        lease_deadline_ms: Some(30_000),
    }
}

#[test]
fn status_key_is_deterministic_and_round_trips() {
    let first = TransactionStatusKey::new(TxnId(42)).unwrap();
    let second = TransactionStatusKey::new(TxnId(42)).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.txn_id(), TxnId(42));
    assert_eq!(
        TransactionStatusKey::from_bytes(first.as_bytes()).unwrap(),
        first
    );
    assert!(first.as_bytes().starts_with(b"/txn-status/"));
    assert!(TransactionStatusKey::new(TxnId(0)).is_err());
}

#[test]
fn status_placement_puts_primary_tablet_first_and_orders_secondaries() {
    let mut location = TransactionStatusLocation::new(TxnId(42), primary_key()).unwrap();
    location.set_route(route(10, 4, 100, 1)).unwrap();

    let participants = BTreeSet::from([TabletId(11), TabletId(5), TabletId(10)]);
    assert_eq!(
        location
            .ordered_participant_tablet_ids(&participants)
            .unwrap(),
        vec![10, 5, 11]
    );
}

struct RefreshingResolver {
    refreshed_route: ParticipantRoute,
    calls: usize,
}

impl TransactionStatusRouteResolver for RefreshingResolver {
    fn resolve_status_route(
        &mut self,
        _primary_key: &[u8],
        _previous_route: Option<ParticipantRoute>,
    ) -> Result<ParticipantRoute, Error> {
        self.calls += 1;
        Ok(self.refreshed_route)
    }
}

struct RefreshingReader {
    attempts: usize,
    observed_keys: Vec<TransactionStatusKey>,
    observed_routes: Vec<ParticipantRoute>,
}

impl TransactionStatusReader for RefreshingReader {
    type Output = TxnStatus;

    fn read_status(
        &mut self,
        route: ParticipantRoute,
        status_key: &TransactionStatusKey,
    ) -> std::result::Result<Option<Self::Output>, TransactionStatusLookupError> {
        self.attempts += 1;
        self.observed_routes.push(route);
        self.observed_keys.push(status_key.clone());
        if self.attempts == 1 {
            return Err(TransactionStatusLookupError::RouteRefreshRequired {
                reason: "status tablet epoch changed".to_string(),
            });
        }
        Ok(Some(TxnStatus::Pending))
    }
}

#[test]
fn status_lookup_refreshes_route_without_changing_durable_status_key() {
    let initial_route = route(10, 4, 100, 1);
    let refreshed_route = route(20, 9, 200, 2);
    let mut location = TransactionStatusLocation::new(TxnId(42), primary_key()).unwrap();
    location.set_route(initial_route).unwrap();
    let durable_key = location.status_key().clone();
    let mut resolver = RefreshingResolver {
        refreshed_route,
        calls: 0,
    };
    let mut reader = RefreshingReader {
        attempts: 0,
        observed_keys: Vec::new(),
        observed_routes: Vec::new(),
    };

    let result = location
        .lookup_with_retry(&mut resolver, &mut reader, 1)
        .unwrap();

    assert_eq!(result, Some(TxnStatus::Pending));
    assert_eq!(resolver.calls, 1);
    assert_eq!(reader.attempts, 2);
    assert_eq!(reader.observed_keys, vec![durable_key.clone(), durable_key]);
    assert_eq!(reader.observed_routes, vec![initial_route, refreshed_route]);
    assert_eq!(location.status_key().txn_id(), TxnId(42));
    assert_eq!(location.route(), Some(refreshed_route));
}

#[test]
fn status_store_is_idempotent_and_rejects_terminal_rewrites() {
    let key = TransactionStatusKey::new(TxnId(42)).unwrap();
    let primary_key = primary_key();
    let mut store = InMemoryTransactionStatusStore::new();
    let pending = pending_record(TxnId(42), primary_key.clone());

    store.write_status(&key, pending.clone()).unwrap();
    store.write_status(&key, pending.clone()).unwrap();
    assert_eq!(store.read_status(&key).unwrap(), Some(pending.clone()));

    let committed = TxnStatusRecord {
        commit_timestamp: Some(ragnordb_common::ids::Timestamp(110)),
        status: TxnStatus::Committed,
        ..pending.clone()
    };
    store.write_status(&key, committed.clone()).unwrap();
    store.write_status(&key, committed.clone()).unwrap();

    let aborted = TxnStatusRecord {
        status: TxnStatus::Aborted,
        ..committed
    };
    assert!(store.write_status(&key, aborted).is_err());

    let foreign_key = TransactionStatusKey::new(TxnId(43)).unwrap();
    assert!(
        store
            .write_status(&foreign_key, pending_record(TxnId(42), primary_key))
            .is_err()
    );
}
