use ragnordb_common::{
    Error, Result,
    codec::{LockRecord, TxnStatus, TxnStatusRecord, WriteKind},
    ids::{RaftGroupId, ReplicaId, TableId, TabletId, Timestamp, TxnId},
};
use ragnordb_storage::key::{encode_row_key, make_row_key};
use ragnordb_txn::{
    AuthoritativeTransactionLease, IntentResolutionDecision, IntentResolutionDispatcher,
    IntentResolutionLeasePolicy, IntentResolutionOutcome, ParticipantDispatchError,
    ParticipantRoute, PendingIntentDecision, TransactionStatusKey, TransactionStatusLocation,
    TransactionStatusLookupError, TransactionStatusReader, TransactionStatusRouteResolver,
    classify_pending_intent, plan_intent_resolution, resolve_intent_with_status_lookup,
    resolve_intent_with_status_lookup_and_lease,
};

fn key(id: i64) -> Vec<u8> {
    encode_row_key(&make_row_key(TableId(1), &[ragnordb_common::codec::Value::Int(id)]).unwrap())
        .unwrap()
}

fn lock(txn_id: TxnId, start_timestamp: Timestamp, primary_key: Vec<u8>) -> LockRecord {
    LockRecord {
        txn_id,
        primary_key,
        start_timestamp,
        ttl_ms: 30_000,
        op: WriteKind::Put,
    }
}

fn status(
    txn_id: TxnId,
    start_timestamp: Timestamp,
    primary_key: Vec<u8>,
    status: TxnStatus,
    commit_timestamp: Option<Timestamp>,
) -> TxnStatusRecord {
    TxnStatusRecord {
        txn_id,
        start_timestamp,
        commit_timestamp,
        status,
        primary_key,
        participant_tablet_ids: vec![10],
        last_heartbeat_timestamp: Some(start_timestamp),
    }
}

#[test]
fn committed_status_builds_a_terminal_roll_forward_plan() {
    let primary_key = key(1);
    let intent_key = key(2);
    let lock = lock(TxnId(7), Timestamp(100), primary_key.clone());
    let status = status(
        TxnId(7),
        Timestamp(100),
        primary_key,
        TxnStatus::Committed,
        Some(Timestamp(110)),
    );

    let decision = plan_intent_resolution(&intent_key, &lock, &status).unwrap();

    let IntentResolutionDecision::Resolve(plan) = decision else {
        panic!("committed status must produce a terminal resolution plan");
    };
    assert_eq!(plan.command.txn_id, TxnId(7));
    assert_eq!(plan.command.start_timestamp, Timestamp(100));
    assert_eq!(plan.command.keys, vec![intent_key]);
    assert_eq!(plan.command.resolved_status, TxnStatus::Committed);
    assert_eq!(plan.command.commit_timestamp, Some(Timestamp(110)));
}

#[test]
fn aborted_status_builds_a_terminal_rollback_plan() {
    let primary_key = key(1);
    let intent_key = key(2);
    let lock = lock(TxnId(8), Timestamp(200), primary_key.clone());
    let status = status(
        TxnId(8),
        Timestamp(200),
        primary_key,
        TxnStatus::Aborted,
        None,
    );

    let decision = plan_intent_resolution(&intent_key, &lock, &status).unwrap();

    let IntentResolutionDecision::Resolve(plan) = decision else {
        panic!("aborted status must produce a terminal resolution plan");
    };
    assert_eq!(plan.command.resolved_status, TxnStatus::Aborted);
    assert_eq!(plan.command.commit_timestamp, None);
}

#[test]
fn pending_intent_returns_a_bounded_retry_before_the_authoritative_lease_deadline() {
    let primary_key = key(1);
    let intent_key = key(2);
    let lock = lock(TxnId(13), Timestamp(500), primary_key.clone());
    let status = status(
        TxnId(13),
        Timestamp(500),
        primary_key,
        TxnStatus::Pending,
        None,
    );
    let lease = AuthoritativeTransactionLease::new(20_000, 10_000).unwrap();

    let decision = classify_pending_intent(&intent_key, &lock, &status, lease, 15_000).unwrap();

    assert_eq!(
        decision,
        PendingIntentDecision::RetryableConflict {
            retry_after_ms: 5_000,
        }
    );
}

#[test]
fn expired_pending_intent_is_reported_without_inventing_an_abort() {
    let primary_key = key(1);
    let intent_key = key(2);
    let lock = lock(TxnId(14), Timestamp(600), primary_key.clone());
    let status = status(
        TxnId(14),
        Timestamp(600),
        primary_key,
        TxnStatus::Pending,
        None,
    );
    let lease = AuthoritativeTransactionLease::new(20_000, 10_000).unwrap();

    let decision = classify_pending_intent(&intent_key, &lock, &status, lease, 20_000).unwrap();

    assert_eq!(
        decision,
        PendingIntentDecision::Expired {
            lease_deadline_ms: 20_000,
        }
    );
}

#[test]
fn status_identity_mismatch_fails_closed_before_resolution() {
    let primary_key = key(1);
    let intent_key = key(2);
    let lock = lock(TxnId(9), Timestamp(300), primary_key.clone());
    let status = status(
        TxnId(9),
        Timestamp(301),
        primary_key,
        TxnStatus::Committed,
        Some(Timestamp(310)),
    );

    let error = plan_intent_resolution(&intent_key, &lock, &status).unwrap_err();

    assert!(matches!(error, Error::CorruptData(message) if message.contains("start timestamp")));
}

fn route() -> ParticipantRoute {
    ParticipantRoute::new(TabletId(10), 3, RaftGroupId(20), ReplicaId(30)).unwrap()
}

struct FixedRouteResolver;

impl TransactionStatusRouteResolver for FixedRouteResolver {
    fn resolve_status_route(
        &mut self,
        _primary_key: &[u8],
        _previous_route: Option<ParticipantRoute>,
    ) -> Result<ParticipantRoute> {
        Ok(route())
    }
}

struct FixedStatusReader {
    status: TxnStatusRecord,
}

impl TransactionStatusReader for FixedStatusReader {
    type Output = TxnStatusRecord;

    fn read_status(
        &mut self,
        _route: ParticipantRoute,
        _status_key: &TransactionStatusKey,
    ) -> std::result::Result<Option<Self::Output>, TransactionStatusLookupError> {
        Ok(Some(self.status.clone()))
    }
}

struct RecordingDispatcher {
    plan: Option<ragnordb_txn::ResolveIntentPlan>,
}

struct FailingDispatcher;

impl IntentResolutionDispatcher for FailingDispatcher {
    type Output = ();

    fn dispatch_intent_resolution(
        &mut self,
        _plan: &ragnordb_txn::ResolveIntentPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError> {
        panic!("pending intents must not dispatch terminal resolution");
    }
}

impl IntentResolutionDispatcher for RecordingDispatcher {
    type Output = &'static str;

    fn dispatch_intent_resolution(
        &mut self,
        plan: &ragnordb_txn::ResolveIntentPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError> {
        self.plan = Some(plan.clone());
        Ok("durably-applied")
    }
}

#[test]
fn status_lookup_dispatches_only_the_authoritative_terminal_resolution() {
    let primary_key = key(1);
    let intent_key = key(2);
    let lock = lock(TxnId(12), Timestamp(400), primary_key.clone());
    let status = status(
        TxnId(12),
        Timestamp(400),
        primary_key.clone(),
        TxnStatus::Committed,
        Some(Timestamp(410)),
    );
    let mut location = TransactionStatusLocation::new(TxnId(12), primary_key).unwrap();
    location.set_route(route()).unwrap();
    let mut route_resolver = FixedRouteResolver;
    let mut status_reader = FixedStatusReader { status };
    let mut dispatcher = RecordingDispatcher { plan: None };

    let outcome = resolve_intent_with_status_lookup(
        &intent_key,
        &lock,
        &mut location,
        &mut route_resolver,
        &mut status_reader,
        &mut dispatcher,
        1,
    )
    .unwrap();

    let IntentResolutionOutcome::Resolved { plan, output } = outcome else {
        panic!("a committed status must dispatch a terminal resolution");
    };
    assert_eq!(output, "durably-applied");
    assert_eq!(dispatcher.plan, Some(plan));
}

#[test]
fn pending_status_lookup_returns_retryable_conflict_without_dispatch() {
    let primary_key = key(1);
    let intent_key = key(2);
    let lock = lock(TxnId(15), Timestamp(700), primary_key.clone());
    let pending_status = status(
        TxnId(15),
        Timestamp(700),
        primary_key.clone(),
        TxnStatus::Pending,
        None,
    );
    let mut location = TransactionStatusLocation::new(TxnId(15), primary_key).unwrap();
    location.set_route(route()).unwrap();
    let mut route_resolver = FixedRouteResolver;
    let mut status_reader = FixedStatusReader {
        status: pending_status.clone(),
    };
    let mut dispatcher = FailingDispatcher;
    let policy = IntentResolutionLeasePolicy::new(
        1,
        AuthoritativeTransactionLease::new(30_000, 10_000).unwrap(),
        20_000,
    )
    .unwrap();

    let outcome = resolve_intent_with_status_lookup_and_lease(
        &intent_key,
        &lock,
        &mut location,
        &mut route_resolver,
        &mut status_reader,
        &mut dispatcher,
        policy,
    )
    .unwrap();

    assert_eq!(
        outcome,
        IntentResolutionOutcome::RetryableConflict {
            status: pending_status,
            retry_after_ms: 10_000,
        }
    );
}

#[test]
fn expired_status_lookup_reports_expiry_without_dispatching_rollback() {
    let primary_key = key(1);
    let intent_key = key(2);
    let lock = lock(TxnId(16), Timestamp(800), primary_key.clone());
    let pending_status = status(
        TxnId(16),
        Timestamp(800),
        primary_key.clone(),
        TxnStatus::Pending,
        None,
    );
    let mut location = TransactionStatusLocation::new(TxnId(16), primary_key).unwrap();
    location.set_route(route()).unwrap();
    let mut route_resolver = FixedRouteResolver;
    let mut status_reader = FixedStatusReader {
        status: pending_status.clone(),
    };
    let mut dispatcher = FailingDispatcher;
    let policy = IntentResolutionLeasePolicy::new(
        1,
        AuthoritativeTransactionLease::new(30_000, 10_000).unwrap(),
        30_000,
    )
    .unwrap();

    let outcome = resolve_intent_with_status_lookup_and_lease(
        &intent_key,
        &lock,
        &mut location,
        &mut route_resolver,
        &mut status_reader,
        &mut dispatcher,
        policy,
    )
    .unwrap();

    assert_eq!(
        outcome,
        IntentResolutionOutcome::Expired {
            status: pending_status,
            lease_deadline_ms: 30_000,
        }
    );
}
