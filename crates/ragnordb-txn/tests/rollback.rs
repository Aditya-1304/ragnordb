use std::collections::VecDeque;

use ragnordb_common::{
    Error,
    codec::{Row, TxnStatus, Value},
    encoding::encode_row,
    ids::{ClientRequestId, RaftGroupId, ReplicaId, TableId, TabletId, Timestamp, TxnId},
};
use ragnordb_storage::key::{encode_row_key, make_row_key};
use ragnordb_txn::{
    DistributedTransactionCoordinator, ParticipantDispatchError, ParticipantRoute,
    ParticipantRouteRefresher, RollbackBatchPlan, RollbackPhaseDispatcher, RollbackPhasePlan,
    Transaction, TransactionStatusKey,
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

fn coordinator_without_status_route() -> DistributedTransactionCoordinator {
    let mut transaction = Transaction::new(TxnId(42), Timestamp(100)).unwrap();
    transaction.buffer_put(key(1), row(1)).unwrap();
    transaction.buffer_put(key(2), row(2)).unwrap();
    transaction.buffer_delete(key(3)).unwrap();

    let primary_key = key(1);
    let mut coordinator = DistributedTransactionCoordinator::new(
        transaction,
        ClientRequestId {
            client_id: 7,
            session_epoch: 3,
            request_sequence: 11,
        },
        primary_key.clone(),
    )
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

struct RecordingRollbackDispatcher {
    primary_actions: VecDeque<DispatchAction>,
    secondary_actions: VecDeque<DispatchAction>,
    status_actions: VecDeque<DispatchAction>,
    primary_plans: Vec<RollbackBatchPlan>,
    secondary_plans: Vec<RollbackBatchPlan>,
    status_plans: Vec<RollbackPhasePlan>,
    events: Vec<&'static str>,
}

impl RecordingRollbackDispatcher {
    fn new(
        primary_actions: impl IntoIterator<Item = DispatchAction>,
        secondary_actions: impl IntoIterator<Item = DispatchAction>,
        status_actions: impl IntoIterator<Item = DispatchAction>,
    ) -> Self {
        Self {
            primary_actions: primary_actions.into_iter().collect(),
            secondary_actions: secondary_actions.into_iter().collect(),
            status_actions: status_actions.into_iter().collect(),
            primary_plans: Vec::new(),
            secondary_plans: Vec::new(),
            status_plans: Vec::new(),
            events: Vec::new(),
        }
    }

    fn next(actions: &mut VecDeque<DispatchAction>) -> DispatchAction {
        actions.pop_front().unwrap_or(DispatchAction::Success)
    }

    fn map_action(
        action: DispatchAction,
        route_reason: &'static str,
        unknown_reason: &'static str,
    ) -> std::result::Result<(), ParticipantDispatchError> {
        match action {
            DispatchAction::Success => Ok(()),
            DispatchAction::RouteRefresh => Err(ParticipantDispatchError::RouteRefreshRequired {
                reason: route_reason.to_string(),
            }),
            DispatchAction::OutcomeUnknown => Err(ParticipantDispatchError::OutcomeUnknown {
                reason: unknown_reason.to_string(),
            }),
        }
    }
}

impl RollbackPhaseDispatcher for RecordingRollbackDispatcher {
    type PrimaryOutput = TabletId;
    type SecondaryOutput = TabletId;
    type StatusOutput = TxnStatus;

    fn dispatch_primary_rollback(
        &mut self,
        plan: &RollbackBatchPlan,
    ) -> std::result::Result<Self::PrimaryOutput, ParticipantDispatchError> {
        self.events.push("primary");
        self.primary_plans.push(plan.clone());
        Self::map_action(
            Self::next(&mut self.primary_actions),
            "primary rollback route is stale",
            "primary rollback outcome was lost",
        )
        .map(|()| plan.tablet_id())
    }

    fn dispatch_secondary_rollback(
        &mut self,
        plan: &RollbackBatchPlan,
    ) -> std::result::Result<Self::SecondaryOutput, ParticipantDispatchError> {
        self.events.push("secondary");
        self.secondary_plans.push(plan.clone());
        Self::map_action(
            Self::next(&mut self.secondary_actions),
            "secondary rollback route is stale",
            "secondary rollback outcome was lost",
        )
        .map(|()| plan.tablet_id())
    }

    fn dispatch_status_abort(
        &mut self,
        plan: &RollbackPhasePlan,
    ) -> std::result::Result<Self::StatusOutput, ParticipantDispatchError> {
        self.events.push("status");
        self.status_plans.push(plan.clone());
        Self::map_action(
            Self::next(&mut self.status_actions),
            "status route is stale",
            "aborted status outcome was lost",
        )
        .map(|()| plan.status_record.status)
    }
}

struct OneRouteRefresh {
    refreshed_route: ParticipantRoute,
    calls: usize,
}

impl ParticipantRouteRefresher for OneRouteRefresh {
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

impl ParticipantRouteRefresher for UnexpectedRouteRefresh {
    fn refresh_participant_route(
        &mut self,
        _logical_mutation_id: &ragnordb_txn::LogicalMutationId,
        _previous_route: ParticipantRoute,
    ) -> ragnordb_common::Result<ParticipantRoute> {
        panic!("unexpected rollback route refresh")
    }
}

#[test]
fn rollback_plan_fences_all_keys_and_marks_status_aborted() {
    let coordinator = coordinator();

    let plan = coordinator.plan_rollback().unwrap();

    assert_eq!(
        plan.status_key,
        TransactionStatusKey::new(TxnId(42)).unwrap()
    );
    assert_eq!(plan.status_route, route(20, 4, 200));
    assert_eq!(plan.status_record.status, TxnStatus::Aborted);
    assert_eq!(plan.status_record.commit_timestamp, None);
    assert_eq!(plan.status_record.participant_tablet_ids, vec![20, 10]);

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
fn rollback_plan_validates_status_and_participant_routes_before_building() {
    let coordinator = coordinator_without_status_route();

    assert!(matches!(
        coordinator.plan_rollback(),
        Err(Error::InvalidArgument(message)) if message.contains("status route")
    ));
}

#[test]
fn rollback_plan_preserves_logical_identity_after_route_refresh() {
    let mut coordinator = coordinator();
    let before = coordinator.plan_rollback().unwrap();

    coordinator
        .set_participant_route(key(2), route(11, 8, 111))
        .unwrap();
    let after = coordinator.plan_rollback().unwrap();

    assert_eq!(before.primary.command, after.primary.command);
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
fn rollback_plan_rejects_conflicting_route_hints_before_batch_creation() {
    let mut coordinator = coordinator();
    coordinator
        .set_participant_route(key(3), route(20, 9, 209))
        .unwrap();

    assert!(matches!(
        coordinator.plan_rollback(),
        Err(Error::InvalidArgument(message)) if message.contains("conflicting route")
    ));
}

#[test]
fn rollback_execution_fences_participants_before_publishing_aborted_status() {
    let mut coordinator = coordinator();
    let mut refresher = UnexpectedRouteRefresh;
    let mut dispatcher = RecordingRollbackDispatcher::new(
        [DispatchAction::Success],
        [DispatchAction::Success],
        [DispatchAction::Success],
    );

    let outcome = coordinator
        .execute_rollback_with_retry(&mut refresher, &mut dispatcher, 1)
        .unwrap();

    assert_eq!(outcome.primary, TabletId(20));
    assert_eq!(outcome.secondary, vec![TabletId(10)]);
    assert_eq!(outcome.status, TxnStatus::Aborted);
    assert_eq!(dispatcher.events, vec!["primary", "secondary", "status"]);
    assert_eq!(dispatcher.status_plans.len(), 1);
}

#[test]
fn primary_rollback_route_refresh_rebuilds_the_plan_before_retrying() {
    let mut coordinator = coordinator();
    let mut refresher = OneRouteRefresh {
        refreshed_route: route(21, 7, 210),
        calls: 0,
    };
    let mut dispatcher = RecordingRollbackDispatcher::new(
        [DispatchAction::RouteRefresh, DispatchAction::Success],
        [DispatchAction::Success],
        [DispatchAction::Success],
    );

    coordinator
        .execute_rollback_with_retry(&mut refresher, &mut dispatcher, 1)
        .unwrap();

    // The old primary batch may span child tablets after a split, so every
    // logical mutation in that batch must be resolved independently.
    assert_eq!(refresher.calls, 2);
    assert_eq!(dispatcher.primary_plans.len(), 2);
    assert_eq!(dispatcher.primary_plans[1].route, route(21, 7, 210));
    assert_eq!(dispatcher.status_plans[0].status_route, route(21, 7, 210));
}

#[test]
fn secondary_rollback_route_refresh_replays_safe_batches_without_republishing_status_early() {
    let mut coordinator = coordinator();
    let mut refresher = OneRouteRefresh {
        refreshed_route: route(11, 8, 111),
        calls: 0,
    };
    let mut dispatcher = RecordingRollbackDispatcher::new(
        [DispatchAction::Success, DispatchAction::Success],
        [DispatchAction::RouteRefresh, DispatchAction::Success],
        [DispatchAction::Success],
    );

    coordinator
        .execute_rollback_with_retry(&mut refresher, &mut dispatcher, 1)
        .unwrap();

    assert_eq!(dispatcher.primary_plans.len(), 2);
    assert_eq!(dispatcher.secondary_plans.len(), 2);
    assert_eq!(dispatcher.status_plans.len(), 1);
    assert_eq!(
        dispatcher.events,
        vec!["primary", "secondary", "primary", "secondary", "status"]
    );
    assert_eq!(dispatcher.secondary_plans[1].route, route(11, 8, 111));
    assert_eq!(
        dispatcher.secondary_plans[0].participant_plans[0].logical_command_id,
        dispatcher.secondary_plans[1].participant_plans[0].logical_command_id
    );
}

#[test]
fn status_route_refresh_retries_only_status_after_participant_success() {
    let mut coordinator = coordinator();
    let mut refresher = OneRouteRefresh {
        refreshed_route: route(21, 7, 210),
        calls: 0,
    };
    let mut dispatcher = RecordingRollbackDispatcher::new(
        [DispatchAction::Success],
        [DispatchAction::Success],
        [DispatchAction::RouteRefresh, DispatchAction::Success],
    );

    coordinator
        .execute_rollback_with_retry(&mut refresher, &mut dispatcher, 1)
        .unwrap();

    // Refresh the primary key's current owner as well as every sibling key
    // before storing the refreshed status route.
    assert_eq!(refresher.calls, 2);
    assert_eq!(
        dispatcher.events,
        vec!["primary", "secondary", "status", "status"]
    );
    assert_eq!(dispatcher.primary_plans.len(), 1);
    assert_eq!(dispatcher.secondary_plans.len(), 1);
    assert_eq!(dispatcher.status_plans.len(), 2);
    assert_eq!(dispatcher.status_plans[1].status_route, route(21, 7, 210));
}

#[test]
fn unknown_rollback_outcome_stops_without_status_publication_or_retry() {
    let mut coordinator = coordinator();
    let mut refresher = UnexpectedRouteRefresh;
    let mut dispatcher = RecordingRollbackDispatcher::new(
        [DispatchAction::OutcomeUnknown],
        [DispatchAction::Success],
        [DispatchAction::Success],
    );

    let error = coordinator
        .execute_rollback_with_retry(&mut refresher, &mut dispatcher, 1)
        .unwrap_err();

    assert!(matches!(error, Error::RequestOutcomeUnknown { .. }));
    assert_eq!(dispatcher.primary_plans.len(), 1);
    assert!(dispatcher.secondary_plans.is_empty());
    assert!(dispatcher.status_plans.is_empty());
}

#[test]
fn unknown_status_outcome_stops_after_rollback_batches() {
    let mut coordinator = coordinator();
    let mut refresher = UnexpectedRouteRefresh;
    let mut dispatcher = RecordingRollbackDispatcher::new(
        [DispatchAction::Success],
        [DispatchAction::Success],
        [DispatchAction::OutcomeUnknown],
    );

    let error = coordinator
        .execute_rollback_with_retry(&mut refresher, &mut dispatcher, 1)
        .unwrap_err();

    assert!(matches!(error, Error::RequestOutcomeUnknown { .. }));
    assert_eq!(dispatcher.events, vec!["primary", "secondary", "status"]);
}

#[test]
fn rollback_execution_requires_a_nonzero_route_refresh_budget_before_planning() {
    let mut coordinator = coordinator();
    let mut refresher = UnexpectedRouteRefresh;
    let mut dispatcher = RecordingRollbackDispatcher::new([], [], []);

    let error = coordinator
        .execute_rollback_with_retry(&mut refresher, &mut dispatcher, 0)
        .unwrap_err();

    assert!(matches!(
        error,
        Error::InvalidArgument(message) if message.contains("route refresh budget")
    ));
    assert!(dispatcher.events.is_empty());
}
