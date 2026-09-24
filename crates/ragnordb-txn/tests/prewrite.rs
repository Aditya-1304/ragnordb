use ragnordb_common::{
    Error,
    codec::{Row, Value},
    command_codec::{PrewriteCommand, TabletCommand, TabletCommandEnvelope},
    encoding::encode_row,
    ids::{ClientRequestId, RaftGroupId, ReplicaId, TableId, TabletId, Timestamp, TxnId},
};
use ragnordb_storage::key::{encode_row_key, make_row_key};
use ragnordb_txn::{
    DistributedTransactionCoordinator, ParticipantDispatchError, ParticipantRoute,
    ParticipantRouteRefresher, PrewriteBatchDispatcher, PrewriteBatchPlan, Transaction,
    TransactionFootprintPolicy,
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

fn three_key_coordinator() -> DistributedTransactionCoordinator {
    three_key_coordinator_with_policy(TransactionFootprintPolicy::default())
}

fn three_key_coordinator_with_policy(
    policy: TransactionFootprintPolicy,
) -> DistributedTransactionCoordinator {
    let mut transaction = Transaction::new_with_policy(TxnId(42), Timestamp(100), policy).unwrap();
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
    coordinator.set_status_route(route(20, 4, 200)).unwrap();
    coordinator
}

fn max_serialized_prewrite_command_bytes(coordinator: &DistributedTransactionCoordinator) -> usize {
    coordinator
        .plan_prewrite(30_000)
        .unwrap()
        .iter()
        .flat_map(|batch| {
            batch
                .command
                .writes
                .iter()
                .zip(&batch.participant_plans)
                .map(|(write, participant)| {
                    let is_primary = write.key == batch.command.primary_key;
                    let mut prewrite = PrewriteCommand {
                        txn_id: batch.command.txn_id,
                        start_timestamp: batch.command.start_timestamp,
                        writes: vec![write.clone()],
                        primary_key: batch.command.primary_key.clone(),
                        ttl_ms: batch.command.ttl_ms,
                        pending_status: is_primary
                            .then(|| batch.command.pending_status.clone())
                            .flatten(),
                    };
                    if let Some(status) = prewrite.pending_status.as_mut() {
                        prewrite.ttl_ms = u64::MAX;
                        status.last_heartbeat_timestamp = Some(status.start_timestamp);
                        status.lease_deadline_ms = Some(u64::MAX);
                    }
                    let envelope = TabletCommandEnvelope::new_with_logical_command_id_and_ack(
                        participant.request_id.clone(),
                        participant.logical_command_id,
                        participant.route.tablet_id,
                        participant.route.tablet_epoch,
                        None,
                        TabletCommand::Prewrite(prewrite),
                    )
                    .unwrap();
                    envelope.encode().unwrap().len()
                })
        })
        .max()
        .unwrap()
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
    missing_route.set_status_route(route(10, 4, 100)).unwrap();

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
    coordinator.set_status_route(route(10, 4, 100)).unwrap();

    let before = coordinator.plan_prewrite(30_000).unwrap();
    coordinator
        .set_participant_route(primary_key, route(20, 9, 200))
        .unwrap();
    coordinator.set_status_route(route(20, 9, 200)).unwrap();
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

struct BatchRefreshOnce {
    refreshed_route: ParticipantRoute,
    calls: usize,
}

impl ParticipantRouteRefresher for BatchRefreshOnce {
    fn refresh_participant_route(
        &mut self,
        _logical_mutation_id: &ragnordb_txn::LogicalMutationId,
        _previous_route: ParticipantRoute,
    ) -> Result<ParticipantRoute, Error> {
        self.calls += 1;
        Ok(self.refreshed_route)
    }
}

struct RefreshingBatchDispatcher {
    attempts: Vec<PrewriteBatchPlan>,
    refresh_once: bool,
}

impl PrewriteBatchDispatcher for RefreshingBatchDispatcher {
    type Output = TabletId;

    fn dispatch_prewrite(
        &mut self,
        plan: &PrewriteBatchPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError> {
        self.attempts.push(plan.clone());
        if self.refresh_once {
            self.refresh_once = false;
            return Err(ParticipantDispatchError::RouteRefreshRequired {
                reason: "participant tablet epoch changed before admission".to_string(),
            });
        }
        Ok(plan.tablet_id())
    }
}

#[test]
fn prewrite_execution_refreshes_and_rebuilds_the_phase_with_stable_identities() {
    let mut coordinator = three_key_coordinator();
    let mut refresher = BatchRefreshOnce {
        refreshed_route: route(30, 6, 110),
        calls: 0,
    };
    let mut dispatcher = RefreshingBatchDispatcher {
        attempts: Vec::new(),
        refresh_once: true,
    };

    let outcomes = coordinator
        .execute_prewrite_with_retry(30_000, &mut refresher, &mut dispatcher, 1)
        .unwrap();

    assert_eq!(outcomes, vec![TabletId(30), TabletId(10)]);
    assert_eq!(refresher.calls, 2);
    assert_eq!(dispatcher.attempts.len(), 3);
    assert_eq!(dispatcher.attempts[0].route, route(20, 4, 200));
    assert_eq!(dispatcher.attempts[1].route, route(30, 6, 110));
    assert_eq!(dispatcher.attempts[2].route, route(10, 5, 100));
    assert_eq!(
        dispatcher.attempts[1]
            .command
            .pending_status
            .as_ref()
            .expect("the primary retry must carry the Pending status")
            .participant_tablet_ids,
        vec![30, 10]
    );
    assert_eq!(
        dispatcher.attempts[0].participant_plans[0].command_id,
        dispatcher.attempts[1].participant_plans[0].command_id
    );
    assert_eq!(
        dispatcher.attempts[0].participant_plans[0].logical_command_id,
        dispatcher.attempts[1].participant_plans[0].logical_command_id
    );
    assert_ne!(
        dispatcher.attempts[0].participant_plans[0].request_id,
        dispatcher.attempts[1].participant_plans[0].request_id
    );
}

struct UnknownBatchDispatcher {
    calls: usize,
}

impl PrewriteBatchDispatcher for UnknownBatchDispatcher {
    type Output = ();

    fn dispatch_prewrite(
        &mut self,
        _plan: &PrewriteBatchPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError> {
        self.calls += 1;
        Err(ParticipantDispatchError::OutcomeUnknown {
            reason: "the participant proposal result was lost".to_string(),
        })
    }
}

struct UnexpectedRefresh {
    calls: usize,
}

impl ParticipantRouteRefresher for UnexpectedRefresh {
    fn refresh_participant_route(
        &mut self,
        _logical_mutation_id: &ragnordb_txn::LogicalMutationId,
        _previous_route: ParticipantRoute,
    ) -> Result<ParticipantRoute, Error> {
        self.calls += 1;
        panic!("unknown prewrite outcomes must not refresh or retry");
    }
}

#[test]
fn prewrite_execution_does_not_resubmit_an_unknown_batch_outcome() {
    let mut coordinator = three_key_coordinator();
    let mut refresher = UnexpectedRefresh { calls: 0 };
    let mut dispatcher = UnknownBatchDispatcher { calls: 0 };

    let error = coordinator
        .execute_prewrite_with_retry(30_000, &mut refresher, &mut dispatcher, 3)
        .unwrap_err();

    assert!(matches!(error, Error::RequestOutcomeUnknown { .. }));
    assert_eq!(dispatcher.calls, 1);
    assert_eq!(refresher.calls, 0);
}

struct RejectingBatchDispatcher {
    calls: usize,
}

impl PrewriteBatchDispatcher for RejectingBatchDispatcher {
    type Output = ();

    fn dispatch_prewrite(
        &mut self,
        _plan: &PrewriteBatchPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError> {
        self.calls += 1;
        Err(ParticipantDispatchError::WriteConflict {
            reason: "a newer committed version exists".to_string(),
        })
    }
}

#[test]
fn prewrite_execution_stops_on_a_deterministic_write_conflict() {
    let mut coordinator = three_key_coordinator();
    let mut refresher = UnexpectedRefresh { calls: 0 };
    let mut dispatcher = RejectingBatchDispatcher { calls: 0 };

    let error = coordinator
        .execute_prewrite_with_retry(30_000, &mut refresher, &mut dispatcher, 1)
        .unwrap_err();

    assert!(matches!(
        error,
        Error::WriteConflict(message) if message.contains("newer committed")
    ));
    assert_eq!(dispatcher.calls, 1);
    assert_eq!(refresher.calls, 0);
}

struct AlwaysRefreshBatchDispatcher {
    calls: usize,
}

impl PrewriteBatchDispatcher for AlwaysRefreshBatchDispatcher {
    type Output = ();

    fn dispatch_prewrite(
        &mut self,
        _plan: &PrewriteBatchPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError> {
        self.calls += 1;
        Err(ParticipantDispatchError::RouteRefreshRequired {
            reason: "leader route is no longer current".to_string(),
        })
    }
}

struct IncrementingRouteRefresher {
    calls: usize,
}

impl ParticipantRouteRefresher for IncrementingRouteRefresher {
    fn refresh_participant_route(
        &mut self,
        _logical_mutation_id: &ragnordb_txn::LogicalMutationId,
        previous_route: ParticipantRoute,
    ) -> Result<ParticipantRoute, Error> {
        self.calls += 1;
        ParticipantRoute::new(
            previous_route.tablet_id,
            previous_route.tablet_epoch + 1,
            previous_route.raft_group_id,
            previous_route.leader_replica_id,
        )
    }
}

#[test]
fn prewrite_execution_bounds_route_refreshes_before_dispatching_again() {
    let mut coordinator = three_key_coordinator();
    let mut refresher = IncrementingRouteRefresher { calls: 0 };
    let mut dispatcher = AlwaysRefreshBatchDispatcher { calls: 0 };

    let error = coordinator
        .execute_prewrite_with_retry(30_000, &mut refresher, &mut dispatcher, 1)
        .unwrap_err();

    assert!(matches!(error, Error::TabletUnavailable { reason } if reason.contains("budget")));
    assert_eq!(dispatcher.calls, 2);
    assert_eq!(refresher.calls, 2);
}

#[test]
fn prewrite_execution_requires_a_nonzero_route_refresh_budget() {
    let mut coordinator = three_key_coordinator();
    let mut refresher = IncrementingRouteRefresher { calls: 0 };
    let mut dispatcher = AlwaysRefreshBatchDispatcher { calls: 0 };

    assert!(matches!(
        coordinator.execute_prewrite_with_retry(30_000, &mut refresher, &mut dispatcher, 0),
        Err(Error::InvalidArgument(message)) if message.contains("budget")
    ));
    assert_eq!(dispatcher.calls, 0);
    assert_eq!(refresher.calls, 0);
}

struct CountingBatchDispatcher {
    calls: usize,
}

impl PrewriteBatchDispatcher for CountingBatchDispatcher {
    type Output = TabletId;

    fn dispatch_prewrite(
        &mut self,
        plan: &PrewriteBatchPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError> {
        self.calls += 1;
        Ok(plan.tablet_id())
    }
}

#[test]
fn participant_and_command_limits_accept_exact_values_and_reject_before_dispatch() {
    // This catches footprint caps being checked after the coordinator has
    // already admitted an earlier participant proposal.
    let mut exact_participant_limit =
        three_key_coordinator_with_policy(TransactionFootprintPolicy {
            max_participant_tablets: 2,
            ..TransactionFootprintPolicy::default()
        });
    let mut refresher = UnexpectedRefresh { calls: 0 };
    let mut dispatcher = CountingBatchDispatcher { calls: 0 };
    exact_participant_limit
        .execute_prewrite_with_retry(30_000, &mut refresher, &mut dispatcher, 1)
        .unwrap();
    assert_eq!(dispatcher.calls, 2);

    let mut over_participant_limit =
        three_key_coordinator_with_policy(TransactionFootprintPolicy {
            max_participant_tablets: 1,
            ..TransactionFootprintPolicy::default()
        });
    let mut dispatcher = CountingBatchDispatcher { calls: 0 };
    assert!(matches!(
        over_participant_limit.execute_prewrite_with_retry(
            30_000,
            &mut refresher,
            &mut dispatcher,
            1,
        ),
        Err(Error::InvalidArgument(message)) if message.contains("participant tablets")
    ));
    assert_eq!(dispatcher.calls, 0);

    let command_limit = max_serialized_prewrite_command_bytes(&three_key_coordinator());
    let mut exact_command_limit = three_key_coordinator_with_policy(TransactionFootprintPolicy {
        max_participant_command_bytes: command_limit,
        ..TransactionFootprintPolicy::default()
    });
    let mut dispatcher = CountingBatchDispatcher { calls: 0 };
    exact_command_limit
        .execute_prewrite_with_retry(30_000, &mut refresher, &mut dispatcher, 1)
        .unwrap();
    assert_eq!(dispatcher.calls, 2);

    let mut over_command_limit = three_key_coordinator_with_policy(TransactionFootprintPolicy {
        max_participant_command_bytes: command_limit - 1,
        ..TransactionFootprintPolicy::default()
    });
    let mut dispatcher = CountingBatchDispatcher { calls: 0 };
    assert!(matches!(
        over_command_limit.execute_prewrite_with_retry(
            30_000,
            &mut refresher,
            &mut dispatcher,
            1,
        ),
        Err(Error::InvalidArgument(message)) if message.contains("participant command bytes")
    ));
    assert_eq!(dispatcher.calls, 0);
}

#[test]
fn expired_transaction_age_rejects_before_participant_dispatch() {
    let mut coordinator = three_key_coordinator_with_policy(TransactionFootprintPolicy {
        max_age_ms: 20,
        ..TransactionFootprintPolicy::default()
    });
    std::thread::sleep(std::time::Duration::from_millis(25));

    let mut refresher = UnexpectedRefresh { calls: 0 };
    let mut dispatcher = CountingBatchDispatcher { calls: 0 };
    assert!(matches!(
        coordinator.execute_prewrite_with_retry(30_000, &mut refresher, &mut dispatcher, 1),
        Err(Error::InvalidArgument(message)) if message.contains("age milliseconds")
    ));
    assert_eq!(dispatcher.calls, 0);
}
