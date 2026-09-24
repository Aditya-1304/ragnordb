use ragnordb_common::{
    Error,
    codec::{TxnStatus, TxnStatusRecord},
    ids::{RaftGroupId, ReplicaId, TableId, TabletId, Timestamp, TxnId},
};
use ragnordb_storage::key::{encode_row_key, make_row_key};
use ragnordb_txn::{
    HeartbeatDecision, InMemoryTransactionStatusStore, ParticipantDispatchError, ParticipantRoute,
    TransactionHeartbeatDispatcher, TransactionHeartbeatOutcome, TransactionHeartbeatPolicy,
    TransactionStatusKey, TransactionStatusLocation, TransactionStatusStore,
    plan_transaction_heartbeat,
};

fn key(id: i64) -> Vec<u8> {
    encode_row_key(&make_row_key(TableId(1), &[ragnordb_common::codec::Value::Int(id)]).unwrap())
        .unwrap()
}

fn route() -> ParticipantRoute {
    ParticipantRoute::new(TabletId(10), 3, RaftGroupId(20), ReplicaId(30)).unwrap()
}

fn pending_status() -> TxnStatusRecord {
    TxnStatusRecord {
        txn_id: TxnId(7),
        start_timestamp: Timestamp(100),
        commit_timestamp: None,
        status: TxnStatus::Pending,
        primary_key: key(1),
        participant_tablet_ids: vec![10, 11],
        last_heartbeat_timestamp: Some(Timestamp(110)),
        lease_deadline_ms: Some(30_000),
    }
}

fn status_location() -> TransactionStatusLocation {
    let mut location =
        TransactionStatusLocation::new(TxnId(7), key(1)).expect("status location must be valid");
    location.set_route(route()).unwrap();
    location
}

#[test]
fn heartbeat_policy_requires_interval_shorter_than_lock_ttl() {
    let error = TransactionHeartbeatPolicy::new(30_000, 30_000, 30_000).unwrap_err();

    assert!(matches!(error, Error::InvalidArgument(message) if message.contains("shorter")));
}

#[test]
fn heartbeat_plan_renews_pending_status_with_explicit_wall_clock_deadline() {
    let status = pending_status();
    let location = status_location();
    let policy = TransactionHeartbeatPolicy::new(30_000, 10_000, 30_000).unwrap();

    let decision =
        plan_transaction_heartbeat(&location, &status, Timestamp(120), 5_000, policy).unwrap();

    let HeartbeatDecision::Renew(plan) = decision else {
        panic!("a newer heartbeat must produce a durable renewal plan");
    };
    assert_eq!(plan.status_key, *location.status_key());
    assert_eq!(plan.status_route, route());
    assert_eq!(
        plan.next_status.last_heartbeat_timestamp,
        Some(Timestamp(120))
    );
    assert_eq!(plan.next_status.lease_deadline_ms, Some(35_000));
}

#[test]
fn stale_heartbeat_cannot_shorten_the_durable_lease() {
    let status = pending_status();
    let location = status_location();
    let policy = TransactionHeartbeatPolicy::new(30_000, 10_000, 30_000).unwrap();

    let error =
        plan_transaction_heartbeat(&location, &status, Timestamp(109), 2_000, policy).unwrap_err();

    assert!(matches!(error, Error::WriteConflict(message) if message.contains("heartbeat")));
}

#[test]
fn late_heartbeat_cannot_resurrect_an_expired_transaction() {
    let status = pending_status();
    let location = status_location();
    let policy = TransactionHeartbeatPolicy::new(30_000, 10_000, 30_000).unwrap();

    let error =
        plan_transaction_heartbeat(&location, &status, Timestamp(120), 30_000, policy).unwrap_err();

    assert!(matches!(error, Error::WriteConflict(message) if message.contains("expired")));
}

#[test]
fn replaying_the_same_durable_heartbeat_is_idempotent() {
    let status = pending_status();
    let location = status_location();
    let policy = TransactionHeartbeatPolicy::new(30_000, 10_000, 30_000).unwrap();

    let decision =
        plan_transaction_heartbeat(&location, &status, Timestamp(110), 0, policy).unwrap();

    assert_eq!(decision, HeartbeatDecision::AlreadyApplied { status });
}

#[test]
fn status_store_rejects_heartbeat_and_lease_regression() {
    let status = pending_status();
    let key = TransactionStatusKey::new(status.txn_id).unwrap();
    let mut store = InMemoryTransactionStatusStore::new();
    store.write_status(&key, status.clone()).unwrap();

    let mut stale = status;
    stale.last_heartbeat_timestamp = Some(Timestamp(109));
    stale.lease_deadline_ms = Some(29_000);

    let error = store.write_status(&key, stale).unwrap_err();

    assert!(matches!(error, Error::WriteConflict(message) if message.contains("heartbeat")));
}

struct RecordingDispatcher;

impl TransactionHeartbeatDispatcher for RecordingDispatcher {
    type Output = &'static str;

    fn dispatch_transaction_heartbeat(
        &mut self,
        _plan: &ragnordb_txn::TransactionHeartbeatPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError> {
        Ok("durably-applied")
    }
}

#[test]
fn heartbeat_dispatch_result_is_published_only_through_the_durable_boundary() {
    let status = pending_status();
    let location = status_location();
    let policy = TransactionHeartbeatPolicy::new(30_000, 10_000, 30_000).unwrap();
    let mut dispatcher = RecordingDispatcher;

    let outcome = ragnordb_txn::heartbeat_with_status(
        &location,
        &status,
        Timestamp(120),
        5_000,
        policy,
        &mut dispatcher,
    )
    .unwrap();

    let TransactionHeartbeatOutcome::Renewed { plan, output } = outcome else {
        panic!("a new heartbeat must cross the durable dispatch boundary");
    };
    assert_eq!(output, "durably-applied");
    assert_eq!(plan.next_status.lease_deadline_ms, Some(35_000));
}
