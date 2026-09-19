use std::collections::BTreeMap;

use ragnordb_common::{
    codec::{TxnStatus, TxnStatusRecord},
    ids::{RaftGroupId, ReplicaId, TableId, TabletId, Timestamp, TxnId},
};
use ragnordb_storage::{
    key::{encode_row_key, make_row_key},
    mvcc::{InMemoryMvcc, Mutation, MvccStorage},
};
use ragnordb_tablet::{
    ExpiredTransactionAbortPlan, IntentCleanerDispatcher, IntentCleanerPolicy, IntentCleanupReport,
    Tablet, clean_intents,
};
use ragnordb_txn::{
    ParticipantDispatchError, ParticipantRoute, ResolveIntentPlan, TransactionStatusKey,
    TransactionStatusLookupError, TransactionStatusReader, TransactionStatusRouteResolver,
};

fn key(id: i64) -> Vec<u8> {
    encode_row_key(&make_row_key(TableId(1), &[ragnordb_common::codec::Value::Int(id)]).unwrap())
        .unwrap()
}

fn route() -> ParticipantRoute {
    ParticipantRoute::new(TabletId(10), 3, RaftGroupId(20), ReplicaId(30)).unwrap()
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
        participant_tablet_ids: vec![10],
        last_heartbeat_timestamp: Some(Timestamp(110)),
        lease_deadline_ms,
    }
}

fn tablet_with_intents(intents: &[(TxnId, Vec<u8>)]) -> Tablet {
    let mut storage = InMemoryMvcc::new();
    for (txn_id, key) in intents {
        storage
            .prewrite(*txn_id, Timestamp(100), key, &Mutation::Delete, key, 30_000)
            .unwrap();
    }
    Tablet::with_storage(TabletId(10), TableId(1), storage).unwrap()
}

struct TestRouteResolver;

impl TransactionStatusRouteResolver for TestRouteResolver {
    fn resolve_status_route(
        &mut self,
        _primary_key: &[u8],
        _previous_route: Option<ParticipantRoute>,
    ) -> ragnordb_common::Result<ParticipantRoute> {
        Ok(route())
    }
}

struct TestStatusReader {
    statuses: BTreeMap<TxnId, TxnStatusRecord>,
}

impl TransactionStatusReader for TestStatusReader {
    type Output = TxnStatusRecord;

    fn read_status(
        &mut self,
        _route: ParticipantRoute,
        key: &TransactionStatusKey,
    ) -> std::result::Result<Option<Self::Output>, TransactionStatusLookupError> {
        Ok(self.statuses.get(&key.txn_id()).cloned())
    }
}

#[derive(Default)]
struct RecordingDispatcher {
    aborts: Vec<ExpiredTransactionAbortPlan>,
    resolutions: Vec<ResolveIntentPlan>,
}

impl IntentCleanerDispatcher for RecordingDispatcher {
    type Output = ();

    fn dispatch_status_abort(
        &mut self,
        plan: &ExpiredTransactionAbortPlan,
    ) -> std::result::Result<(), ParticipantDispatchError> {
        self.aborts.push(plan.clone());
        Ok(())
    }

    fn dispatch_intent_resolution(
        &mut self,
        plan: &ResolveIntentPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError> {
        self.resolutions.push(plan.clone());
        Ok(())
    }
}

fn policy(page_size: usize, max_pages_per_run: usize) -> IntentCleanerPolicy {
    IntentCleanerPolicy::new(page_size, max_pages_per_run, 1).unwrap()
}

#[test]
fn cleaner_resolves_terminal_intents_through_the_durable_dispatch_boundary() {
    let committed_key = key(1);
    let aborted_key = key(2);
    let tablet = tablet_with_intents(&[
        (TxnId(1), committed_key.clone()),
        (TxnId(2), aborted_key.clone()),
    ]);
    let mut status_reader = TestStatusReader {
        statuses: BTreeMap::from([
            (
                TxnId(1),
                status(
                    TxnId(1),
                    committed_key,
                    TxnStatus::Committed,
                    Some(Timestamp(200)),
                    Some(30_000),
                ),
            ),
            (
                TxnId(2),
                status(
                    TxnId(2),
                    aborted_key,
                    TxnStatus::Aborted,
                    None,
                    Some(30_000),
                ),
            ),
        ]),
    };
    let mut route_resolver = TestRouteResolver;
    let mut dispatcher = RecordingDispatcher::default();

    let report = clean_intents(
        &tablet,
        policy(8, 8),
        1_000,
        &mut route_resolver,
        &mut status_reader,
        &mut dispatcher,
    )
    .unwrap();

    assert_eq!(
        report,
        IntentCleanupReport {
            pages_scanned: 1,
            intents_scanned: 2,
            pending_intents: 0,
            uncertain_intents: 0,
            expired_transactions: 0,
            resolved_intents: 2,
            truncated: false,
        }
    );
    assert!(dispatcher.aborts.is_empty());
    assert_eq!(dispatcher.resolutions.len(), 2);
}

#[test]
fn expired_pending_intent_is_aborted_before_rollback_resolution() {
    let intent_key = key(3);
    let tablet = tablet_with_intents(&[(TxnId(3), intent_key.clone())]);
    let mut status_reader = TestStatusReader {
        statuses: BTreeMap::from([(
            TxnId(3),
            status(TxnId(3), intent_key, TxnStatus::Pending, None, Some(10)),
        )]),
    };
    let mut route_resolver = TestRouteResolver;
    let mut dispatcher = RecordingDispatcher::default();

    let report = clean_intents(
        &tablet,
        policy(8, 8),
        20,
        &mut route_resolver,
        &mut status_reader,
        &mut dispatcher,
    )
    .unwrap();

    assert_eq!(report.expired_transactions, 1);
    assert_eq!(report.resolved_intents, 1);
    assert_eq!(dispatcher.aborts.len(), 1);
    assert_eq!(dispatcher.aborts[0].next_status.status, TxnStatus::Aborted);
    assert_eq!(dispatcher.resolutions.len(), 1);
    assert_eq!(
        dispatcher.resolutions[0].command.resolved_status,
        TxnStatus::Aborted
    );
}

#[test]
fn valid_pending_intent_is_left_untouched() {
    let intent_key = key(4);
    let tablet = tablet_with_intents(&[(TxnId(4), intent_key.clone())]);
    let mut status_reader = TestStatusReader {
        statuses: BTreeMap::from([(
            TxnId(4),
            status(TxnId(4), intent_key, TxnStatus::Pending, None, Some(100)),
        )]),
    };
    let mut route_resolver = TestRouteResolver;
    let mut dispatcher = RecordingDispatcher::default();

    let report = clean_intents(
        &tablet,
        policy(8, 8),
        20,
        &mut route_resolver,
        &mut status_reader,
        &mut dispatcher,
    )
    .unwrap();

    assert_eq!(report.pending_intents, 1);
    assert_eq!(report.resolved_intents, 0);
    assert!(dispatcher.aborts.is_empty());
    assert!(dispatcher.resolutions.is_empty());
}

#[test]
fn cleaner_scan_is_bounded_by_page_budget() {
    let first_key = key(5);
    let second_key = key(6);
    let tablet = tablet_with_intents(&[
        (TxnId(5), first_key.clone()),
        (TxnId(6), second_key.clone()),
    ]);
    let mut status_reader = TestStatusReader {
        statuses: BTreeMap::from([
            (
                TxnId(5),
                status(TxnId(5), first_key, TxnStatus::Aborted, None, Some(30_000)),
            ),
            (
                TxnId(6),
                status(TxnId(6), second_key, TxnStatus::Aborted, None, Some(30_000)),
            ),
        ]),
    };
    let mut route_resolver = TestRouteResolver;
    let mut dispatcher = RecordingDispatcher::default();

    let report = clean_intents(
        &tablet,
        policy(1, 1),
        1_000,
        &mut route_resolver,
        &mut status_reader,
        &mut dispatcher,
    )
    .unwrap();

    assert_eq!(report.pages_scanned, 1);
    assert_eq!(report.intents_scanned, 1);
    assert_eq!(report.resolved_intents, 1);
    assert!(report.truncated);
}
