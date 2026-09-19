use ragnordb_common::{
    Error,
    codec::{Row, TxnStatus, Value},
    encoding::encode_row,
    ids::{ClientRequestId, RaftGroupId, ReplicaId, TableId, TabletId, Timestamp, TxnId},
};
use ragnordb_storage::key::{encode_row_key, make_row_key};
use std::collections::VecDeque;

use ragnordb_txn::{
    CommitBatchPlan, CommitPhaseDispatcher, CommitPhasePlan, DistributedTransactionCoordinator,
    LocalTransactionManager, ParticipantDispatchError, ParticipantRoute, Transaction,
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

#[derive(Debug, Clone, Copy)]
enum DispatchAction {
    Success,
    RouteRefresh,
    OutcomeUnknown,
}

struct RecordingCommitDispatcher {
    primary_actions: VecDeque<DispatchAction>,
    secondary_actions: VecDeque<DispatchAction>,
    primary_plans: Vec<CommitPhasePlan>,
    secondary_plans: Vec<CommitBatchPlan>,
}

impl RecordingCommitDispatcher {
    fn new(
        primary_actions: impl IntoIterator<Item = DispatchAction>,
        secondary_actions: impl IntoIterator<Item = DispatchAction>,
    ) -> Self {
        Self {
            primary_actions: primary_actions.into_iter().collect(),
            secondary_actions: secondary_actions.into_iter().collect(),
            primary_plans: Vec::new(),
            secondary_plans: Vec::new(),
        }
    }

    fn action(actions: &mut VecDeque<DispatchAction>) -> DispatchAction {
        actions.pop_front().unwrap_or(DispatchAction::Success)
    }
}

impl CommitPhaseDispatcher for RecordingCommitDispatcher {
    type PrimaryOutput = TabletId;
    type SecondaryOutput = TabletId;

    fn dispatch_primary(
        &mut self,
        plan: &CommitPhasePlan,
    ) -> std::result::Result<Self::PrimaryOutput, ParticipantDispatchError> {
        self.primary_plans.push(plan.clone());
        match Self::action(&mut self.primary_actions) {
            DispatchAction::Success => Ok(plan.primary.tablet_id()),
            DispatchAction::RouteRefresh => Err(ParticipantDispatchError::RouteRefreshRequired {
                reason: "primary route is stale".to_string(),
            }),
            DispatchAction::OutcomeUnknown => Err(ParticipantDispatchError::OutcomeUnknown {
                reason: "primary durable outcome was lost".to_string(),
            }),
        }
    }

    fn dispatch_secondary(
        &mut self,
        plan: &CommitBatchPlan,
    ) -> std::result::Result<Self::SecondaryOutput, ParticipantDispatchError> {
        self.secondary_plans.push(plan.clone());
        match Self::action(&mut self.secondary_actions) {
            DispatchAction::Success => Ok(plan.tablet_id()),
            DispatchAction::RouteRefresh => Err(ParticipantDispatchError::RouteRefreshRequired {
                reason: "secondary route is stale".to_string(),
            }),
            DispatchAction::OutcomeUnknown => Err(ParticipantDispatchError::OutcomeUnknown {
                reason: "secondary durable outcome was lost".to_string(),
            }),
        }
    }
}

struct OneRouteRefresh {
    refreshed_route: ParticipantRoute,
    calls: usize,
}

impl ragnordb_txn::ParticipantRouteRefresher for OneRouteRefresh {
    fn refresh_participant_route(
        &mut self,
        _logical_mutation_id: &ragnordb_txn::LogicalMutationId,
        _previous_route: ParticipantRoute,
    ) -> ragnordb_common::Result<ParticipantRoute> {
        self.calls += 1;
        Ok(self.refreshed_route)
    }
}

struct UnexpectedRouteRefresh;

impl ragnordb_txn::ParticipantRouteRefresher for UnexpectedRouteRefresh {
    fn refresh_participant_route(
        &mut self,
        _logical_mutation_id: &ragnordb_txn::LogicalMutationId,
        _previous_route: ParticipantRoute,
    ) -> ragnordb_common::Result<ParticipantRoute> {
        panic!("unknown commit outcomes must not refresh or retry")
    }
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

#[test]
fn commit_execution_commits_primary_status_before_secondary_batches() {
    let mut coordinator = coordinator();
    let mut timestamps = LocalTransactionManager::new();
    let mut refresher = UnexpectedRouteRefresh;
    let mut dispatcher =
        RecordingCommitDispatcher::new([DispatchAction::Success], [DispatchAction::Success]);

    let outcome = coordinator
        .execute_commit_with_retry(&mut timestamps, &mut refresher, &mut dispatcher, 1)
        .unwrap();

    assert_eq!(outcome.commit_timestamp, Timestamp(101));
    assert_eq!(outcome.primary, TabletId(20));
    assert_eq!(outcome.secondary, vec![TabletId(10)]);
    assert_eq!(dispatcher.primary_plans.len(), 1);
    assert_eq!(dispatcher.secondary_plans.len(), 1);
    assert_eq!(
        dispatcher.primary_plans[0].status_record.commit_timestamp,
        Some(Timestamp(101))
    );
    assert_eq!(dispatcher.primary_plans[0].status_route, route(20, 4, 200));
    assert_eq!(dispatcher.secondary_plans[0].route, route(10, 5, 100));
}

#[test]
fn primary_route_refresh_retries_primary_without_reallocating_commit_timestamp() {
    let mut coordinator = coordinator();
    let mut timestamps = LocalTransactionManager::new();
    let mut refresher = OneRouteRefresh {
        refreshed_route: route(21, 7, 210),
        calls: 0,
    };
    let mut dispatcher = RecordingCommitDispatcher::new(
        [DispatchAction::RouteRefresh, DispatchAction::Success],
        [DispatchAction::Success],
    );

    let outcome = coordinator
        .execute_commit_with_retry(&mut timestamps, &mut refresher, &mut dispatcher, 1)
        .unwrap();

    assert_eq!(outcome.commit_timestamp, Timestamp(101));
    assert_eq!(timestamps.last_allocated_timestamp(), Timestamp(101));
    assert_eq!(refresher.calls, 1);
    assert_eq!(dispatcher.primary_plans.len(), 2);
    assert_eq!(dispatcher.primary_plans[0].primary.route, route(20, 4, 200));
    assert_eq!(dispatcher.primary_plans[1].primary.route, route(21, 7, 210));
    assert_eq!(dispatcher.primary_plans[1].status_route, route(21, 7, 210));
    assert_eq!(dispatcher.primary_plans[1].commit_timestamp, Timestamp(101));
}

#[test]
fn secondary_route_refresh_retries_only_after_primary_commit_and_keeps_identity() {
    let mut coordinator = coordinator();
    let mut timestamps = LocalTransactionManager::new();
    let mut refresher = OneRouteRefresh {
        refreshed_route: route(11, 8, 111),
        calls: 0,
    };
    let mut dispatcher = RecordingCommitDispatcher::new(
        [DispatchAction::Success],
        [DispatchAction::RouteRefresh, DispatchAction::Success],
    );

    let outcome = coordinator
        .execute_commit_with_retry(&mut timestamps, &mut refresher, &mut dispatcher, 1)
        .unwrap();

    assert_eq!(outcome.commit_timestamp, Timestamp(101));
    assert_eq!(outcome.primary, TabletId(20));
    assert_eq!(outcome.secondary, vec![TabletId(11)]);
    assert_eq!(dispatcher.primary_plans.len(), 1);
    assert_eq!(dispatcher.secondary_plans.len(), 2);
    assert_eq!(
        dispatcher.secondary_plans[0].participant_plans[0].logical_command_id,
        dispatcher.secondary_plans[1].participant_plans[0].logical_command_id
    );
    assert_eq!(
        dispatcher.secondary_plans[1].participant_plans[0].command_id,
        dispatcher.secondary_plans[0].participant_plans[0].command_id
    );
    assert_eq!(dispatcher.secondary_plans[1].route, route(11, 8, 111));
    assert_eq!(
        dispatcher.secondary_plans[1].command.commit_timestamp,
        Timestamp(101)
    );
}

#[test]
fn unknown_primary_commit_outcome_stops_without_secondary_dispatch_or_refresh() {
    let mut coordinator = coordinator();
    let mut timestamps = LocalTransactionManager::new();
    let mut refresher = UnexpectedRouteRefresh;
    let mut dispatcher =
        RecordingCommitDispatcher::new([DispatchAction::OutcomeUnknown], [DispatchAction::Success]);

    let error = coordinator
        .execute_commit_with_retry(&mut timestamps, &mut refresher, &mut dispatcher, 1)
        .unwrap_err();

    assert!(matches!(error, Error::RequestOutcomeUnknown { .. }));
    assert_eq!(dispatcher.primary_plans.len(), 1);
    assert!(dispatcher.secondary_plans.is_empty());
}

#[test]
fn unknown_secondary_commit_outcome_stops_without_retrying_later_batches() {
    let mut coordinator = coordinator();
    let mut timestamps = LocalTransactionManager::new();
    let mut refresher = UnexpectedRouteRefresh;
    let mut dispatcher =
        RecordingCommitDispatcher::new([DispatchAction::Success], [DispatchAction::OutcomeUnknown]);

    let error = coordinator
        .execute_commit_with_retry(&mut timestamps, &mut refresher, &mut dispatcher, 1)
        .unwrap_err();

    assert!(matches!(error, Error::RequestOutcomeUnknown { .. }));
    assert_eq!(dispatcher.primary_plans.len(), 1);
    assert_eq!(dispatcher.secondary_plans.len(), 1);
}

#[test]
fn commit_execution_requires_a_nonzero_route_refresh_budget_before_planning() {
    let mut coordinator = coordinator();
    let mut timestamps = LocalTransactionManager::new();
    let mut refresher = UnexpectedRouteRefresh;
    let mut dispatcher = RecordingCommitDispatcher::new([], []);

    let error = coordinator
        .execute_commit_with_retry(&mut timestamps, &mut refresher, &mut dispatcher, 0)
        .unwrap_err();

    assert!(matches!(
        error,
        Error::InvalidArgument(message) if message.contains("route refresh budget")
    ));
    assert_eq!(timestamps.last_allocated_timestamp(), Timestamp(0));
    assert!(dispatcher.primary_plans.is_empty());
}
