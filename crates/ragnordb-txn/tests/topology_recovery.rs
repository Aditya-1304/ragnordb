use std::collections::BTreeMap;

use ragnordb_common::{
    Error,
    codec::{Row, TxnStatus, Value},
    encoding::encode_row,
    ids::{ClientRequestId, RaftGroupId, ReplicaId, TableId, TabletId, Timestamp, TxnId},
};
use ragnordb_storage::key::{encode_row_key, make_row_key};
use ragnordb_txn::{
    CommitBatchPlan, CommitPhaseDispatcher, CommitPhasePlan, DistributedTransactionCoordinator,
    LocalTransactionManager, ParticipantDispatchError, ParticipantRoute, ParticipantRouteRefresher,
    PrewriteBatchDispatcher, PrewriteBatchPlan, RollbackBatchPlan, RollbackPhaseDispatcher,
    RollbackPhasePlan, Transaction,
};

fn key(id: i64) -> Vec<u8> {
    encode_row_key(&make_row_key(TableId(1), &[Value::Int(id)]).unwrap()).unwrap()
}

fn row(id: i64) -> Vec<u8> {
    encode_row(&Row {
        values: vec![Value::Int(id), Value::Text(format!("value-{id}"))],
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

const OLD_ROUTE: ParticipantRoute = ParticipantRoute {
    tablet_id: TabletId(10),
    tablet_epoch: 4,
    raft_group_id: RaftGroupId(100),
    leader_replica_id: ReplicaId(10),
};

fn coordinator() -> (DistributedTransactionCoordinator, Vec<u8>, Vec<u8>) {
    let primary_key = key(1);
    let secondary_key = key(2);
    let mut transaction = Transaction::new(TxnId(42), Timestamp(100)).unwrap();
    transaction.buffer_put(primary_key.clone(), row(1)).unwrap();
    transaction
        .buffer_put(secondary_key.clone(), row(2))
        .unwrap();

    let root_request = ClientRequestId {
        client_id: 7,
        session_epoch: 3,
        request_sequence: 11,
    };
    let mut coordinator =
        DistributedTransactionCoordinator::new(transaction, root_request, primary_key.clone())
            .unwrap();
    coordinator
        .set_participant_route(primary_key.clone(), OLD_ROUTE)
        .unwrap();
    coordinator
        .set_participant_route(secondary_key.clone(), OLD_ROUTE)
        .unwrap();
    coordinator.set_status_route(OLD_ROUTE).unwrap();

    (coordinator, primary_key, secondary_key)
}

struct PerKeyRouteRefresher {
    routes: BTreeMap<Vec<u8>, ParticipantRoute>,
    resolved: Vec<Vec<u8>>,
}

impl ParticipantRouteRefresher for PerKeyRouteRefresher {
    fn refresh_participant_route(
        &mut self,
        logical_mutation_id: &ragnordb_txn::LogicalMutationId,
        _previous_route: ParticipantRoute,
    ) -> Result<ParticipantRoute, Error> {
        let key = logical_mutation_id.as_key().to_vec();
        self.resolved.push(key.clone());
        self.routes.get(&key).copied().ok_or_else(|| {
            Error::InvalidArgument(format!("no current descriptor owns logical key {key:?}"))
        })
    }
}

struct SplitPrewriteDispatcher {
    attempts: Vec<PrewriteBatchPlan>,
    reject_next: bool,
}

impl PrewriteBatchDispatcher for SplitPrewriteDispatcher {
    type Output = TabletId;

    fn dispatch_prewrite(
        &mut self,
        plan: &PrewriteBatchPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError> {
        self.attempts.push(plan.clone());
        if self.reject_next {
            self.reject_next = false;
            return Err(ParticipantDispatchError::RouteRefreshRequired {
                reason: "the old tablet no longer owns the whole batch".to_string(),
            });
        }
        Ok(plan.tablet_id())
    }
}

/// Realistic bug caught: after a previously grouped prewrite batch crosses a
/// tablet split, resolving only its first key routes every mutation to one
/// child and either rejects or loses the mutations owned by the other child.
#[test]
fn prewrite_refresh_regroups_each_key_after_a_split() {
    let (mut coordinator, primary_key, secondary_key) = coordinator();
    let mut refresher = PerKeyRouteRefresher {
        routes: BTreeMap::from([
            (primary_key.clone(), OLD_ROUTE),
            (secondary_key.clone(), OLD_ROUTE),
        ]),
        resolved: Vec::new(),
    };
    let mut before_split_dispatcher = SplitPrewriteDispatcher {
        attempts: Vec::new(),
        reject_next: false,
    };
    assert_eq!(
        coordinator
            .execute_prewrite_with_retry(30_000, &mut refresher, &mut before_split_dispatcher, 1,)
            .unwrap(),
        vec![TabletId(10)]
    );
    let pre_split_batch = before_split_dispatcher.attempts[0].clone();
    assert_eq!(pre_split_batch.route, OLD_ROUTE);
    assert_eq!(pre_split_batch.command.writes.len(), 2);

    // The prewrite has already succeeded at the parent. After split, a retry
    // against its stale route is rejected before apply, then reissued to each
    // current child using the original logical command identities.
    let primary_route = route(20, 5, 200);
    let secondary_route = route(21, 5, 201);
    refresher.routes = BTreeMap::from([
        (primary_key.clone(), primary_route),
        (secondary_key.clone(), secondary_route),
    ]);
    refresher.resolved.clear();
    let mut dispatcher = SplitPrewriteDispatcher {
        attempts: Vec::new(),
        reject_next: true,
    };

    let outcomes = coordinator
        .execute_prewrite_with_retry(30_000, &mut refresher, &mut dispatcher, 1)
        .unwrap();

    assert_eq!(outcomes, vec![TabletId(20), TabletId(21)]);
    assert_eq!(dispatcher.attempts.len(), 3);
    assert_eq!(dispatcher.attempts[0].route, OLD_ROUTE);
    assert_eq!(dispatcher.attempts[0].command.writes.len(), 2);
    assert_eq!(
        refresher.resolved,
        vec![primary_key.clone(), secondary_key.clone()]
    );

    let regrouped = &dispatcher.attempts[1..];
    assert_eq!(regrouped[0].route, primary_route);
    assert_eq!(regrouped[0].command.writes[0].key, primary_key);
    assert_eq!(regrouped[1].route, secondary_route);
    assert_eq!(regrouped[1].command.writes[0].key, secondary_key);

    for key in [&primary_key, &secondary_key] {
        let old_identity = pre_split_batch
            .participant_plans
            .iter()
            .find(|plan| plan.command_id.logical_mutation_id().as_key() == key)
            .unwrap()
            .logical_command_id;
        let new_identity = regrouped
            .iter()
            .flat_map(|batch| &batch.participant_plans)
            .find(|plan| plan.command_id.logical_mutation_id().as_key() == key)
            .unwrap()
            .logical_command_id;
        assert_eq!(old_identity, new_identity);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CommitDispatchEvent {
    Primary {
        route: ParticipantRoute,
        keys: Vec<Vec<u8>>,
        status_route: ParticipantRoute,
        command_ids: Vec<ragnordb_common::ids::LogicalCommandId>,
    },
    Secondary {
        route: ParticipantRoute,
        keys: Vec<Vec<u8>>,
        commit_timestamp: Timestamp,
        command_ids: Vec<ragnordb_common::ids::LogicalCommandId>,
    },
}

struct SplitBeforeCommitDispatcher {
    events: Vec<CommitDispatchEvent>,
}

impl CommitPhaseDispatcher for SplitBeforeCommitDispatcher {
    type PrimaryOutput = ();
    type SecondaryOutput = ();

    fn dispatch_primary(
        &mut self,
        plan: &CommitPhasePlan,
    ) -> std::result::Result<Self::PrimaryOutput, ParticipantDispatchError> {
        self.events.push(CommitDispatchEvent::Primary {
            route: plan.primary.route,
            keys: plan.primary.command.keys.clone(),
            status_route: plan.status_route,
            command_ids: plan
                .primary
                .participant_plans
                .iter()
                .map(|participant| participant.logical_command_id)
                .collect(),
        });
        if self.events.len() == 1 {
            return Err(ParticipantDispatchError::RouteRefreshRequired {
                reason: "split changed primary batch ownership before admission".to_string(),
            });
        }
        Ok(())
    }

    fn dispatch_secondary(
        &mut self,
        plan: &CommitBatchPlan,
    ) -> std::result::Result<Self::SecondaryOutput, ParticipantDispatchError> {
        self.events.push(CommitDispatchEvent::Secondary {
            route: plan.route,
            keys: plan.command.keys.clone(),
            commit_timestamp: plan.command.commit_timestamp,
            command_ids: plan
                .participant_plans
                .iter()
                .map(|participant| participant.logical_command_id)
                .collect(),
        });
        Ok(())
    }
}

/// Realistic bug caught: a split of a prewritten primary batch can move only
/// some keys. Refreshing the batch as one route can place those keys on the
/// wrong tablet and can publish primary/status commit for the wrong grouping.
#[test]
fn split_before_primary_commit_rebuilds_current_batches_and_keeps_primary_first() {
    let (mut coordinator, primary_key, secondary_key) = coordinator();
    let old_prewrite = coordinator.plan_prewrite(30_000).unwrap();
    assert_eq!(old_prewrite.len(), 1);
    assert_eq!(old_prewrite[0].route, OLD_ROUTE);
    assert_eq!(old_prewrite[0].command.writes.len(), 2);

    let primary_route = route(20, 5, 200);
    let secondary_route = route(21, 5, 201);
    let mut refresher = PerKeyRouteRefresher {
        routes: BTreeMap::from([
            (primary_key.clone(), primary_route),
            (secondary_key.clone(), secondary_route),
        ]),
        resolved: Vec::new(),
    };
    let mut dispatcher = SplitBeforeCommitDispatcher { events: Vec::new() };
    let mut timestamps = LocalTransactionManager::new();

    let outcome = coordinator
        .execute_commit_with_retry(&mut timestamps, &mut refresher, &mut dispatcher, 1)
        .unwrap();

    assert_eq!(outcome.commit_timestamp, Timestamp(101));
    assert_eq!(
        refresher.resolved,
        vec![primary_key.clone(), secondary_key.clone()]
    );
    assert_eq!(dispatcher.events.len(), 3);
    assert!(matches!(
        &dispatcher.events[0],
        CommitDispatchEvent::Primary { route, keys, .. }
            if *route == OLD_ROUTE && keys == &vec![primary_key.clone(), secondary_key.clone()]
    ));
    assert!(matches!(
        &dispatcher.events[1],
        CommitDispatchEvent::Primary { route, keys, status_route, .. }
            if *route == primary_route
                && keys == &vec![primary_key.clone()]
                && *status_route == primary_route
    ));
    assert!(matches!(
        &dispatcher.events[2],
        CommitDispatchEvent::Secondary { route, keys, commit_timestamp, .. }
            if *route == secondary_route
                && keys == &vec![secondary_key.clone()]
                && *commit_timestamp == Timestamp(101)
    ));

    let original_ids = match &dispatcher.events[0] {
        CommitDispatchEvent::Primary { command_ids, .. } => command_ids,
        _ => unreachable!(),
    };
    let refreshed_primary_id = match &dispatcher.events[1] {
        CommitDispatchEvent::Primary { command_ids, .. } => command_ids[0],
        _ => unreachable!(),
    };
    let refreshed_secondary_id = match &dispatcher.events[2] {
        CommitDispatchEvent::Secondary { command_ids, .. } => command_ids[0],
        _ => unreachable!(),
    };
    assert_eq!(original_ids[0], refreshed_primary_id);
    assert_eq!(original_ids[1], refreshed_secondary_id);
    assert_eq!(outcome.secondary.len(), 1);
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RollbackDispatchEvent {
    Primary {
        route: ParticipantRoute,
        keys: Vec<Vec<u8>>,
        command_ids: Vec<ragnordb_common::ids::LogicalCommandId>,
    },
    Secondary {
        route: ParticipantRoute,
        keys: Vec<Vec<u8>>,
        command_ids: Vec<ragnordb_common::ids::LogicalCommandId>,
    },
    Status {
        route: ParticipantRoute,
        status: TxnStatus,
    },
}

struct SplitRollbackDispatcher {
    events: Vec<RollbackDispatchEvent>,
}

impl RollbackPhaseDispatcher for SplitRollbackDispatcher {
    type PrimaryOutput = ();
    type SecondaryOutput = ();
    type StatusOutput = ();

    fn dispatch_primary_rollback(
        &mut self,
        plan: &RollbackBatchPlan,
    ) -> std::result::Result<Self::PrimaryOutput, ParticipantDispatchError> {
        self.events.push(RollbackDispatchEvent::Primary {
            route: plan.route,
            keys: plan.command.keys.clone(),
            command_ids: plan
                .participant_plans
                .iter()
                .map(|participant| participant.logical_command_id)
                .collect(),
        });
        if self.events.len() == 1 {
            return Err(ParticipantDispatchError::RouteRefreshRequired {
                reason: "split changed rollback batch ownership before admission".to_string(),
            });
        }
        Ok(())
    }

    fn dispatch_secondary_rollback(
        &mut self,
        plan: &RollbackBatchPlan,
    ) -> std::result::Result<Self::SecondaryOutput, ParticipantDispatchError> {
        self.events.push(RollbackDispatchEvent::Secondary {
            route: plan.route,
            keys: plan.command.keys.clone(),
            command_ids: plan
                .participant_plans
                .iter()
                .map(|participant| participant.logical_command_id)
                .collect(),
        });
        Ok(())
    }

    fn dispatch_status_abort(
        &mut self,
        plan: &RollbackPhasePlan,
    ) -> std::result::Result<Self::StatusOutput, ParticipantDispatchError> {
        self.events.push(RollbackDispatchEvent::Status {
            route: plan.status_route,
            status: plan.status_record.status,
        });
        Ok(())
    }
}

/// Realistic bug caught: a split during abort recovery must fence every
/// current owner before the durable status becomes aborted; refreshing only
/// the batch's first key can leave a moved key without its rollback marker.
#[test]
fn rollback_after_split_regroups_keys_before_publishing_aborted_status() {
    let (mut coordinator, primary_key, secondary_key) = coordinator();
    let primary_route = route(20, 5, 200);
    let secondary_route = route(21, 5, 201);
    let mut refresher = PerKeyRouteRefresher {
        routes: BTreeMap::from([
            (primary_key.clone(), primary_route),
            (secondary_key.clone(), secondary_route),
        ]),
        resolved: Vec::new(),
    };
    let mut dispatcher = SplitRollbackDispatcher { events: Vec::new() };

    coordinator
        .execute_rollback_with_retry(&mut refresher, &mut dispatcher, 1)
        .unwrap();

    assert_eq!(
        refresher.resolved,
        vec![primary_key.clone(), secondary_key.clone()]
    );
    assert_eq!(dispatcher.events.len(), 4);
    assert!(matches!(
        &dispatcher.events[0],
        RollbackDispatchEvent::Primary { route, keys, .. }
            if *route == OLD_ROUTE && keys.len() == 2
    ));
    assert!(matches!(
        &dispatcher.events[1],
        RollbackDispatchEvent::Primary { route, keys, .. }
            if *route == primary_route && keys == &vec![primary_key.clone()]
    ));
    assert!(matches!(
        &dispatcher.events[2],
        RollbackDispatchEvent::Secondary { route, keys, .. }
            if *route == secondary_route && keys == &vec![secondary_key.clone()]
    ));
    assert!(matches!(
        &dispatcher.events[3],
        RollbackDispatchEvent::Status { route, status }
            if *route == primary_route && *status == TxnStatus::Aborted
    ));

    let original_ids = match &dispatcher.events[0] {
        RollbackDispatchEvent::Primary { command_ids, .. } => command_ids,
        _ => unreachable!(),
    };
    let current_primary_id = match &dispatcher.events[1] {
        RollbackDispatchEvent::Primary { command_ids, .. } => command_ids[0],
        _ => unreachable!(),
    };
    let current_secondary_id = match &dispatcher.events[2] {
        RollbackDispatchEvent::Secondary { command_ids, .. } => command_ids[0],
        _ => unreachable!(),
    };
    assert_eq!(original_ids[0], current_primary_id);
    assert_eq!(original_ids[1], current_secondary_id);
}

struct SplitAfterPrimaryCommitDispatcher {
    old_secondary_route: ParticipantRoute,
    durable_status: Option<ragnordb_common::codec::TxnStatusRecord>,
    primary_commits: usize,
    secondary_attempts: Vec<(bool, CommitBatchPlan)>,
    rejected_old_route: bool,
}

impl CommitPhaseDispatcher for SplitAfterPrimaryCommitDispatcher {
    type PrimaryOutput = TabletId;
    type SecondaryOutput = TabletId;

    fn dispatch_primary(
        &mut self,
        plan: &CommitPhasePlan,
    ) -> std::result::Result<Self::PrimaryOutput, ParticipantDispatchError> {
        self.primary_commits += 1;
        self.durable_status = Some(plan.status_record.clone());
        Ok(plan.primary.route.tablet_id)
    }

    fn dispatch_secondary(
        &mut self,
        plan: &CommitBatchPlan,
    ) -> std::result::Result<Self::SecondaryOutput, ParticipantDispatchError> {
        self.secondary_attempts
            .push((self.durable_status.is_some(), plan.clone()));

        if plan.route == self.old_secondary_route && !self.rejected_old_route {
            self.rejected_old_route = true;
            return Err(ParticipantDispatchError::RouteRefreshRequired {
                reason: "secondary tablet split after primary/status commit".to_string(),
            });
        }

        Ok(plan.route.tablet_id)
    }
}

/// Realistic bug caught: if a secondary batch splits after the primary/status
/// commit point, retrying one old batch route for all its keys can strand one
/// child. The retry must fan out by logical key without committing the primary
/// again or changing either participant's command identity/commit timestamp.
#[test]
fn split_after_primary_commit_regroups_secondaries_and_preserves_commit_identity() {
    let primary_key = key(31);
    let first_secondary_key = key(32);
    let second_secondary_key = key(33);
    let primary_route = route(10, 4, 100);
    let old_secondary_route = route(11, 4, 101);
    let first_child_route = route(20, 5, 200);
    let second_child_route = route(21, 5, 201);

    let mut transaction = Transaction::new(TxnId(43), Timestamp(100)).unwrap();
    transaction
        .buffer_put(primary_key.clone(), row(31))
        .unwrap();
    transaction
        .buffer_put(first_secondary_key.clone(), row(32))
        .unwrap();
    transaction
        .buffer_put(second_secondary_key.clone(), row(33))
        .unwrap();
    let root_request = ClientRequestId {
        client_id: 7,
        session_epoch: 3,
        request_sequence: 12,
    };
    let mut coordinator =
        DistributedTransactionCoordinator::new(transaction, root_request, primary_key.clone())
            .unwrap();
    coordinator
        .set_participant_route(primary_key.clone(), primary_route)
        .unwrap();
    coordinator
        .set_participant_route(first_secondary_key.clone(), old_secondary_route)
        .unwrap();
    coordinator
        .set_participant_route(second_secondary_key.clone(), old_secondary_route)
        .unwrap();
    coordinator.set_status_route(primary_route).unwrap();

    let mut refresher = PerKeyRouteRefresher {
        routes: BTreeMap::from([
            (first_secondary_key.clone(), first_child_route),
            (second_secondary_key.clone(), second_child_route),
        ]),
        resolved: Vec::new(),
    };
    let mut dispatcher = SplitAfterPrimaryCommitDispatcher {
        old_secondary_route,
        durable_status: None,
        primary_commits: 0,
        secondary_attempts: Vec::new(),
        rejected_old_route: false,
    };
    let mut timestamps = LocalTransactionManager::new();

    let outcome = coordinator
        .execute_commit_with_retry(&mut timestamps, &mut refresher, &mut dispatcher, 1)
        .unwrap();

    assert_eq!(outcome.commit_timestamp, Timestamp(101));
    assert_eq!(dispatcher.primary_commits, 1);
    assert_eq!(
        dispatcher
            .durable_status
            .as_ref()
            .unwrap()
            .participant_tablet_ids,
        vec![10, 11]
    );
    assert_eq!(
        refresher.resolved,
        vec![first_secondary_key.clone(), second_secondary_key.clone()]
    );
    assert_eq!(outcome.secondary, vec![TabletId(20), TabletId(21)]);
    assert_eq!(dispatcher.secondary_attempts.len(), 3);

    let (primary_was_committed, stale_batch) = &dispatcher.secondary_attempts[0];
    assert!(*primary_was_committed);
    assert_eq!(stale_batch.route, old_secondary_route);
    assert_eq!(stale_batch.command.keys.len(), 2);
    assert_eq!(stale_batch.command.commit_timestamp, Timestamp(101));

    let (_, first_child_batch) = &dispatcher.secondary_attempts[1];
    let (_, second_child_batch) = &dispatcher.secondary_attempts[2];
    assert_eq!(first_child_batch.route, first_child_route);
    assert_eq!(
        first_child_batch.command.keys,
        vec![first_secondary_key.clone()]
    );
    assert_eq!(second_child_batch.route, second_child_route);
    assert_eq!(
        second_child_batch.command.keys,
        vec![second_secondary_key.clone()]
    );
    assert_eq!(first_child_batch.command.commit_timestamp, Timestamp(101));
    assert_eq!(second_child_batch.command.commit_timestamp, Timestamp(101));

    for key in [&first_secondary_key, &second_secondary_key] {
        let old_identity = stale_batch
            .participant_plans
            .iter()
            .find(|plan| plan.command_id.logical_mutation_id().as_key() == key)
            .unwrap()
            .logical_command_id;
        let retried_identity = [&first_child_batch, &second_child_batch]
            .into_iter()
            .flat_map(|batch| &batch.participant_plans)
            .find(|plan| plan.command_id.logical_mutation_id().as_key() == key)
            .unwrap()
            .logical_command_id;
        assert_eq!(old_identity, retried_identity);
    }
}
