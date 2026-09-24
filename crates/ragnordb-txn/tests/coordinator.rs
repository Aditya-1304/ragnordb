use ragnordb_common::{
    Error,
    codec::{Row, Value},
    encoding::encode_row,
    ids::{
        ClientRequestId, CommandKind, ParticipantCommandPhase, RaftGroupId, ReplicaId, TableId,
        TabletId, Timestamp, TxnId,
    },
};
use ragnordb_storage::key::{encode_row_key, make_row_key};
use ragnordb_txn::{
    DistributedTransactionCoordinator, ParticipantRoute, Transaction, TransactionReadSpan,
};
use ragnordb_txn::{
    ParticipantCommandPlan, ParticipantDispatchError, ParticipantPhaseDispatcher,
    ParticipantRouteRefresher,
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
    route_with_leader(tablet_id, epoch, raft_group_id, tablet_id)
}

fn route_with_leader(
    tablet_id: u64,
    epoch: u64,
    raft_group_id: u64,
    leader_replica_id: u64,
) -> ParticipantRoute {
    ParticipantRoute::new(
        TabletId(tablet_id),
        epoch,
        RaftGroupId(raft_group_id),
        ReplicaId(leader_replica_id),
    )
    .unwrap()
}

fn routed_coordinator() -> DistributedTransactionCoordinator {
    let primary_key = key(1);
    let mut coordinator =
        DistributedTransactionCoordinator::new(transaction(), root_request(), primary_key.clone())
            .unwrap();
    coordinator
        .set_participant_route(primary_key, route(10, 4, 100))
        .unwrap();
    coordinator
        .set_participant_route(key(2), route(11, 5, 101))
        .unwrap();
    coordinator
}

#[test]
fn coordinator_tracks_transaction_state_and_route_hints() {
    let primary_key = key(1);
    let read_key = key(9);
    let mut transaction = transaction();
    transaction.record_read(read_key.clone()).unwrap();
    let read_span = TransactionReadSpan::new(TableId(1), None, None).unwrap();
    transaction.record_read_span(read_span.clone()).unwrap();
    let mut coordinator =
        DistributedTransactionCoordinator::new(transaction, root_request(), primary_key.clone())
            .unwrap();

    // Re-recording a point read at the coordinator boundary remains a set
    // insert, while reads collected by SQL before coordinator construction are
    // carried through unchanged.
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
    assert_eq!(
        coordinator.read_spans().iter().cloned().collect::<Vec<_>>(),
        [read_span]
    );
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
    assert!(ParticipantRoute::new(TabletId(0), 1, RaftGroupId(1), ReplicaId(1)).is_err());
    assert!(ParticipantRoute::new(TabletId(1), 0, RaftGroupId(1), ReplicaId(1)).is_err());
    assert!(ParticipantRoute::new(TabletId(1), 1, RaftGroupId(0), ReplicaId(1)).is_err());
    assert!(ParticipantRoute::new(TabletId(1), 1, RaftGroupId(1), ReplicaId(0)).is_err());
}

#[test]
fn phase_plan_uses_current_route_but_stable_logical_identity() {
    let primary_key = key(1);
    let mut coordinator = routed_coordinator();
    let before = coordinator
        .participant_command_plan(ParticipantCommandPhase::Prewrite, &primary_key)
        .unwrap();

    coordinator
        .set_participant_route(primary_key.clone(), route(20, 9, 200))
        .unwrap();
    let after = coordinator
        .participant_command_plan(ParticipantCommandPhase::Prewrite, &primary_key)
        .unwrap();

    assert_eq!(before.command_id, after.command_id);
    assert_eq!(before.logical_command_id, after.logical_command_id);
    assert_eq!(before.root_request_id, root_request());
    assert_ne!(before.request_id, after.request_id);
    assert_eq!(after.route, route(20, 9, 200));
    assert_eq!(
        coordinator
            .plan_phase(ParticipantCommandPhase::Prewrite)
            .unwrap()
            .len(),
        2
    );
}

struct RefreshOnce {
    refreshed_route: ParticipantRoute,
    calls: usize,
}

impl ParticipantRouteRefresher for RefreshOnce {
    fn refresh_participant_route(
        &mut self,
        _logical_mutation_id: &ragnordb_txn::LogicalMutationId,
        _previous_route: ParticipantRoute,
    ) -> Result<ParticipantRoute, Error> {
        self.calls += 1;
        Ok(self.refreshed_route)
    }
}

struct RefreshingDispatcher {
    attempts: Vec<ParticipantCommandPlan>,
}

impl ParticipantPhaseDispatcher for RefreshingDispatcher {
    type Output = ParticipantCommandPlan;

    fn dispatch(
        &mut self,
        plan: &ParticipantCommandPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError> {
        self.attempts.push(plan.clone());
        if self.attempts.len() == 1 {
            return Err(ParticipantDispatchError::RouteRefreshRequired {
                reason: "tablet epoch changed before admission".to_string(),
            });
        }
        Ok(plan.clone())
    }
}

#[test]
fn phase_execution_refreshes_routes_without_regenerating_command_identity() {
    let mut coordinator = routed_coordinator();
    let mut refresher = RefreshOnce {
        refreshed_route: route_with_leader(10, 4, 100, 99),
        calls: 0,
    };
    let mut dispatcher = RefreshingDispatcher {
        attempts: Vec::new(),
    };

    let outcomes = coordinator
        .execute_phase_with_retry(
            ParticipantCommandPhase::Prewrite,
            &mut refresher,
            &mut dispatcher,
            1,
        )
        .unwrap();

    assert_eq!(outcomes.len(), 2);
    assert_eq!(refresher.calls, 1);
    assert_eq!(dispatcher.attempts.len(), 3);
    assert_eq!(
        dispatcher.attempts[0].logical_command_id,
        dispatcher.attempts[1].logical_command_id
    );
    assert_eq!(
        dispatcher.attempts[0].request_id,
        dispatcher.attempts[1].request_id
    );
    assert_eq!(
        dispatcher.attempts[1].route,
        route_with_leader(10, 4, 100, 99)
    );
}

struct NeverRefresh {
    calls: usize,
}

impl ParticipantRouteRefresher for NeverRefresh {
    fn refresh_participant_route(
        &mut self,
        _logical_mutation_id: &ragnordb_txn::LogicalMutationId,
        _previous_route: ParticipantRoute,
    ) -> Result<ParticipantRoute, Error> {
        self.calls += 1;
        panic!("unknown outcomes must not refresh or retry a route");
    }
}

struct UnknownDispatcher {
    calls: usize,
}

impl ParticipantPhaseDispatcher for UnknownDispatcher {
    type Output = ();

    fn dispatch(
        &mut self,
        _plan: &ParticipantCommandPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError> {
        self.calls += 1;
        Err(ParticipantDispatchError::OutcomeUnknown {
            reason: "the Raft proposal result was lost".to_string(),
        })
    }
}

#[test]
fn phase_execution_does_not_resubmit_an_unknown_outcome() {
    let mut coordinator = routed_coordinator();
    let mut refresher = NeverRefresh { calls: 0 };
    let mut dispatcher = UnknownDispatcher { calls: 0 };

    let error = coordinator
        .execute_phase_with_retry(
            ParticipantCommandPhase::Commit,
            &mut refresher,
            &mut dispatcher,
            3,
        )
        .unwrap_err();

    assert!(matches!(error, Error::RequestOutcomeUnknown { .. }));
    assert_eq!(dispatcher.calls, 1);
    assert_eq!(refresher.calls, 0);
}
