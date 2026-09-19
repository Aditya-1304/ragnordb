use std::collections::BTreeMap;

use ragnordb_common::{
    Error, Result,
    codec::{Row, TxnStatus, TxnStatusRecord, Value},
    command_codec::{TabletCommand, TabletCommandEnvelope},
    encoding::encode_row,
    ids::{
        ClientRequestId, LogicalCommandId, RaftGroupId, ReplicaId, RequestId, TableId, TabletId,
        Timestamp, TxnId,
    },
};
use ragnordb_storage::{
    key::{encode_row_key, make_row_key},
    mvcc::MvccStorage,
};
use ragnordb_tablet::{
    Tablet,
    command::{TabletCommandApplyError, TabletStateMachine},
    snapshot::{
        AppliedTabletFrontier, TabletSnapshotConfState, TabletSnapshotInstallTarget,
        generate_local_snapshot, restore_verified_snapshot,
    },
};
use ragnordb_txn::{
    CommitBatchPlan, CommitPhaseDispatcher, CommitPhasePlan, DistributedTransactionCoordinator,
    InMemoryTransactionStatusStore, LocalTransactionManager, ParticipantCommandPlan,
    ParticipantDispatchError, ParticipantRoute, ParticipantRouteRefresher,
    PrewriteBatchDispatcher, PrewriteBatchPlan, Transaction, TransactionStatusKey,
    TransactionStatusLocation, TransactionStatusLookupError, TransactionStatusReader,
    TransactionStatusRouteResolver, TransactionStatusStore, plan_intent_resolution,
};

const TABLE_ID: TableId = TableId(1);
const TABLET_EPOCH: u64 = 4;
const PRIMARY_TABLET: TabletId = TabletId(1);
const SECONDARY_TABLET: TabletId = TabletId(2);

fn row_key(id: i64) -> ragnordb_common::ids::RowKey {
    make_row_key(TABLE_ID, &[Value::Int(id)]).unwrap()
}

fn encoded_key(id: i64) -> Vec<u8> {
    encode_row_key(&row_key(id)).unwrap()
}

fn row(id: i64, value: &str) -> Row {
    Row {
        values: vec![Value::Int(id), Value::Text(value.to_string())],
    }
}

fn encoded_row(id: i64, value: &str) -> Vec<u8> {
    encode_row(&row(id, value)).unwrap()
}

fn primary_route() -> ParticipantRoute {
    ParticipantRoute::new(PRIMARY_TABLET, TABLET_EPOCH, RaftGroupId(101), ReplicaId(1)).unwrap()
}

fn secondary_route() -> ParticipantRoute {
    ParticipantRoute::new(
        SECONDARY_TABLET,
        TABLET_EPOCH,
        RaftGroupId(102),
        ReplicaId(2),
    )
    .unwrap()
}

fn stale_status_route() -> ParticipantRoute {
    ParticipantRoute::new(PRIMARY_TABLET, TABLET_EPOCH - 1, RaftGroupId(101), ReplicaId(1))
        .unwrap()
}

fn root_request() -> ClientRequestId {
    ClientRequestId {
        client_id: 0x900,
        session_epoch: 1,
        request_sequence: 1,
    }
}

fn transaction_and_routes(
    txn_id: TxnId,
) -> (DistributedTransactionCoordinator, Vec<u8>) {
    let primary_key = encoded_key(700);
    let secondary_key = encoded_key(701);
    let mut transaction = Transaction::new(txn_id, Timestamp(100)).unwrap();
    transaction
        .buffer_put(primary_key.clone(), encoded_row(700, "primary"))
        .unwrap();
    transaction
        .buffer_put(secondary_key.clone(), encoded_row(701, "secondary"))
        .unwrap();

    let mut coordinator = DistributedTransactionCoordinator::new(
        transaction,
        root_request(),
        primary_key.clone(),
    )
    .unwrap();
    coordinator
        .set_participant_route(primary_key.clone(), primary_route())
        .unwrap();
    coordinator
        .set_participant_route(secondary_key.clone(), secondary_route())
        .unwrap();
    coordinator.set_status_route(primary_route()).unwrap();

    (coordinator, primary_key)
}

fn pending_status(txn_id: TxnId, primary_key: Vec<u8>) -> TxnStatusRecord {
    TxnStatusRecord {
        txn_id,
        start_timestamp: Timestamp(100),
        commit_timestamp: None,
        status: TxnStatus::Pending,
        primary_key,
        participant_tablet_ids: vec![PRIMARY_TABLET.0, SECONDARY_TABLET.0],
        last_heartbeat_timestamp: Some(Timestamp(101)),
        lease_deadline_ms: Some(10),
    }
}

fn conf_state() -> TabletSnapshotConfState {
    TabletSnapshotConfState::new(7, [ReplicaId(1), ReplicaId(2), ReplicaId(3)], [], []).unwrap()
}

/// Durable cluster state intentionally outlives a gateway process. The
/// gateway owns only coordination and routing handles; participant state and
/// transaction status survive its crash and are reopened by the replacement
/// process below.
struct DurableCluster {
    tablets: BTreeMap<TabletId, TabletStateMachine>,
    status_store: InMemoryTransactionStatusStore,
}

impl DurableCluster {
    fn new() -> Self {
        let primary = TabletStateMachine::new(
            Tablet::new(PRIMARY_TABLET, TABLE_ID).unwrap(),
            TABLET_EPOCH,
            primary_route().raft_group_id,
        )
        .unwrap();
        let secondary = TabletStateMachine::new(
            Tablet::new(SECONDARY_TABLET, TABLE_ID).unwrap(),
            TABLET_EPOCH,
            secondary_route().raft_group_id,
        )
        .unwrap();

        Self {
            tablets: BTreeMap::from([(PRIMARY_TABLET, primary), (SECONDARY_TABLET, secondary)]),
            status_store: InMemoryTransactionStatusStore::new(),
        }
    }

    fn publish_pending(&mut self, txn_id: TxnId, primary_key: Vec<u8>) {
        let key = TransactionStatusKey::new(txn_id).unwrap();
        self.status_store
            .write_status(&key, pending_status(txn_id, primary_key))
            .unwrap();
    }

    fn status(&self, txn_id: TxnId) -> TxnStatusRecord {
        let key = TransactionStatusKey::new(txn_id).unwrap();
        self.status_store
            .read_status(&key)
            .unwrap()
            .expect("the transaction status must survive gateway restart")
    }

    /// Rebuild each participant from the same verified snapshot path used by
    /// replica restart. This is the process-crash boundary: the old gateway
    /// and state-machine instances disappear, while their durable images do
    /// not.
    fn restart_participants(&mut self) {
        let tablet_ids = self.tablets.keys().copied().collect::<Vec<_>>();
        for (offset, tablet_id) in tablet_ids.into_iter().enumerate() {
            let previous = self
                .tablets
                .remove(&tablet_id)
                .expect("tablet listed by the durable cluster must exist");
            let snapshot_id = 800 + offset as u64;
            let image = generate_local_snapshot(
                &previous,
                "ragnordb-phase-6-9-gateway",
                ReplicaId(tablet_id.0),
                snapshot_id,
                conf_state(),
                AppliedTabletFrontier::new(snapshot_id, 1),
            )
            .unwrap();
            let target = TabletSnapshotInstallTarget {
                cluster_id: "ragnordb-phase-6-9-gateway".to_string(),
                raft_group_id: previous.raft_group_id(),
                tablet_id,
                table_id: TABLE_ID,
                tablet_epoch: TABLET_EPOCH,
            };
            let restored = restore_verified_snapshot(&image, &target)
                .unwrap()
                .state_machine;
            self.tablets.insert(tablet_id, restored);
        }
    }
}

/// Replacement gateway process. Its route resolver deliberately returns one
/// stale status route first, forcing the recovery lookup through the bounded
/// refresh path before using the current primary/status route.
struct GatewayProcess<'a> {
    cluster: &'a mut DurableCluster,
    fail_after_second_prewrite: bool,
    fail_before_secondary_commit: bool,
    prewrite_dispatches: usize,
    status_route_refreshes: usize,
}

impl<'a> GatewayProcess<'a> {
    fn new(
        cluster: &'a mut DurableCluster,
        fail_after_second_prewrite: bool,
        fail_before_secondary_commit: bool,
    ) -> Self {
        Self {
            cluster,
            fail_after_second_prewrite,
            fail_before_secondary_commit,
            prewrite_dispatches: 0,
            status_route_refreshes: 0,
        }
    }

    fn apply_planned_command(
        &mut self,
        route: ParticipantRoute,
        participant_plan: &ParticipantCommandPlan,
        command: TabletCommand,
    ) -> std::result::Result<(), ParticipantDispatchError> {
        self.apply_logical_command(
            route,
            participant_plan.logical_command_id,
            participant_plan.request_id.clone(),
            command,
        )
    }

    fn apply_logical_command(
        &mut self,
        route: ParticipantRoute,
        logical_command_id: LogicalCommandId,
        request_id: RequestId,
        command: TabletCommand,
    ) -> std::result::Result<(), ParticipantDispatchError> {
        let envelope = TabletCommandEnvelope::new_with_logical_command_id(
            request_id,
            logical_command_id,
            route.tablet_id,
            route.tablet_epoch,
            command,
        )
        .map_err(|error| ParticipantDispatchError::Rejected {
            reason: error.to_string(),
        })?;

        let state_machine = self
            .cluster
            .tablets
            .get_mut(&route.tablet_id)
            .ok_or_else(|| ParticipantDispatchError::Unavailable {
                reason: format!("tablet {} is unavailable", route.tablet_id.0),
            })?;

        state_machine.apply(envelope).map(|_| ()).map_err(|error| match error {
            TabletCommandApplyError::WriteConflict { reason } => {
                ParticipantDispatchError::WriteConflict { reason }
            }
            other => ParticipantDispatchError::Rejected {
                reason: other.to_string(),
            },
        })
    }

    fn lookup_status(
        &mut self,
        location: &mut TransactionStatusLocation,
    ) -> Result<TxnStatusRecord> {
        let status = self
            .cluster
            .status_store
            .read_status(location.status_key())?
            .ok_or_else(|| Error::TabletUnavailable {
                reason: format!("status for transaction {} is unavailable", location.txn_id().0),
            })?;
        let mut resolver = RestartRouteResolver {
            current: primary_route(),
            served_stale_route: false,
            refreshes: 0,
        };
        let mut reader = GatewayStatusReader {
            current: primary_route(),
            status: Some(status),
        };
        let result = location
            .lookup_with_retry(&mut resolver, &mut reader, 1)?
            .ok_or_else(|| Error::TabletUnavailable {
                reason: format!("status for transaction {} is missing", location.txn_id().0),
            })?;
        self.status_route_refreshes += resolver.refreshes;
        Ok(result)
    }

    fn recover_visible_intents(&mut self, now_ms: u64) -> Result<usize> {
        let mut resolved = 0;
        let tablet_ids = [PRIMARY_TABLET, SECONDARY_TABLET];

        for tablet_id in tablet_ids {
            let intents = self
                .cluster
                .tablets
                .get(&tablet_id)
                .ok_or_else(|| Error::TabletUnavailable {
                    reason: format!("tablet {} is unavailable", tablet_id.0),
                })?
                .tablet()
                .storage()
                .scan_intent_page(None, None, None, 64)?
                .locks;

            for (key, lock) in intents {
                let mut location = TransactionStatusLocation::new(lock.txn_id, lock.primary_key.clone())?;
                let mut status = self.lookup_status(&mut location)?;

                if status.status == TxnStatus::Pending {
                    let deadline = status.lease_deadline_ms.ok_or_else(|| Error::CorruptData(
                        "pending recovery status has no lease deadline".to_string(),
                    ))?;
                    if now_ms < deadline {
                        return Err(Error::TabletUnavailable {
                            reason: format!(
                                "transaction {} is still within its authoritative lease",
                                lock.txn_id.0
                            ),
                        });
                    }

                    let mut aborted = status.clone();
                    aborted.status = TxnStatus::Aborted;
                    aborted.commit_timestamp = None;
                    self.cluster
                        .status_store
                        .write_status(location.status_key(), aborted.clone())?;
                    status = aborted;
                }

                let decision = plan_intent_resolution(&key, &lock, &status)?;
                let ragnordb_txn::IntentResolutionDecision::Resolve(plan) = decision else {
                    return Err(Error::CorruptData(
                        "recovery status remained pending after the lease transition".to_string(),
                    ));
                };
                let route = match tablet_id {
                    PRIMARY_TABLET => primary_route(),
                    SECONDARY_TABLET => secondary_route(),
                    _ => unreachable!("recovery enumerates only known participant tablets"),
                };
                self.apply_logical_command(
                    route,
                    plan.logical_command_id,
                    RequestId {
                        client_id: 0xA00,
                        sequence: resolved as u64 + 1,
                        raft_group_id: route.raft_group_id,
                    },
                    TabletCommand::ResolveIntent(plan.command),
                )
                .map_err(|error| Error::ProposalUnavailable {
                    reason: format!("intent recovery dispatch failed: {error:?}"),
                })?;
                resolved += 1;
            }
        }

        Ok(resolved)
    }
}

impl PrewriteBatchDispatcher for GatewayProcess<'_> {
    type Output = ();

    fn dispatch_prewrite(
        &mut self,
        plan: &PrewriteBatchPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError> {
        self.prewrite_dispatches += 1;
        let participant_plan = plan
            .participant_plans
            .first()
            .ok_or_else(|| ParticipantDispatchError::Rejected {
                reason: "prewrite batch has no participant identity".to_string(),
            })?;
        self.apply_planned_command(
            plan.route,
            participant_plan,
            TabletCommand::Prewrite(plan.command.clone()),
        )?;

        if self.fail_after_second_prewrite && self.prewrite_dispatches == 2 {
            return Err(ParticipantDispatchError::OutcomeUnknown {
                reason: "gateway crashed after the second prewrite applied".to_string(),
            });
        }

        Ok(())
    }
}

impl CommitPhaseDispatcher for GatewayProcess<'_> {
    type PrimaryOutput = ();
    type SecondaryOutput = ();

    fn dispatch_primary(
        &mut self,
        plan: &CommitPhasePlan,
    ) -> std::result::Result<Self::PrimaryOutput, ParticipantDispatchError> {
        let participant_plan = plan
            .primary
            .participant_plans
            .first()
            .ok_or_else(|| ParticipantDispatchError::Rejected {
                reason: "primary commit batch has no participant identity".to_string(),
            })?;
        self.apply_planned_command(
            plan.primary.route,
            participant_plan,
            TabletCommand::Commit(plan.primary.command.clone()),
        )?;

        // The simulation treats this pair as the primary/status durable
        // boundary. A recovery process observes either the pending record or
        // this committed record, never a locally inferred outcome.
        self.cluster
            .status_store
            .write_status(&plan.status_key, plan.status_record.clone())
            .map_err(|error| ParticipantDispatchError::Rejected {
                reason: error.to_string(),
            })?;
        Ok(())
    }

    fn dispatch_secondary(
        &mut self,
        plan: &CommitBatchPlan,
    ) -> std::result::Result<Self::SecondaryOutput, ParticipantDispatchError> {
        if self.fail_before_secondary_commit {
            return Err(ParticipantDispatchError::OutcomeUnknown {
                reason: "gateway crashed after primary/status commit".to_string(),
            });
        }

        let participant_plan = plan
            .participant_plans
            .first()
            .ok_or_else(|| ParticipantDispatchError::Rejected {
                reason: "secondary commit batch has no participant identity".to_string(),
            })?;
        self.apply_planned_command(
            plan.route,
            participant_plan,
            TabletCommand::Commit(plan.command.clone()),
        )?;
        Ok(())
    }
}

struct RestartRouteResolver {
    current: ParticipantRoute,
    served_stale_route: bool,
    refreshes: usize,
}

impl TransactionStatusRouteResolver for RestartRouteResolver {
    fn resolve_status_route(
        &mut self,
        _primary_key: &[u8],
        previous_route: Option<ParticipantRoute>,
    ) -> Result<ParticipantRoute> {
        if previous_route.is_none() && !self.served_stale_route {
            self.served_stale_route = true;
            Ok(stale_status_route())
        } else {
            self.refreshes += usize::from(previous_route.is_some());
            Ok(self.current)
        }
    }
}

struct GatewayStatusReader {
    current: ParticipantRoute,
    status: Option<TxnStatusRecord>,
}

impl TransactionStatusReader for GatewayStatusReader {
    type Output = TxnStatusRecord;

    fn read_status(
        &mut self,
        route: ParticipantRoute,
        _key: &TransactionStatusKey,
    ) -> std::result::Result<Option<Self::Output>, TransactionStatusLookupError> {
        if route != self.current {
            return Err(TransactionStatusLookupError::RouteRefreshRequired {
                reason: "gateway restart retained a stale status route hint".to_string(),
            });
        }
        Ok(self.status.clone())
    }
}

struct UnexpectedRouteRefresh;

impl ParticipantRouteRefresher for UnexpectedRouteRefresh {
    fn refresh_participant_route(
        &mut self,
        _logical_mutation_id: &ragnordb_txn::LogicalMutationId,
        _previous_route: ParticipantRoute,
    ) -> Result<ParticipantRoute> {
        panic!("the crash simulation must not refresh a participant route")
    }
}

#[test]
fn gateway_crash_after_prewrite_restarts_and_recovers_all_participants() {
    let txn_id = TxnId(700);
    let (mut coordinator, primary_key) = transaction_and_routes(txn_id);
    let mut cluster = DurableCluster::new();
    cluster.publish_pending(txn_id, primary_key);

    let result = {
        let mut gateway = GatewayProcess::new(&mut cluster, true, false);
        let mut refresher = UnexpectedRouteRefresh;
        coordinator.execute_prewrite_with_retry(30_000, &mut refresher, &mut gateway, 1)
    };

    assert!(matches!(
        result,
        Err(Error::RequestOutcomeUnknown { .. })
    ));
    assert_eq!(
        cluster
            .tablets
            .values()
            .map(|tablet| tablet.tablet().stats().locks)
            .sum::<usize>(),
        2
    );

    // The gateway process is gone. Participant snapshots and the status
    // tablet are the only recovery inputs available to its replacement.
    cluster.restart_participants();
    let (resolved, refreshes) = {
        let mut recovery = GatewayProcess::new(&mut cluster, false, false);
        let resolved = recovery.recover_visible_intents(20).unwrap();
        (resolved, recovery.status_route_refreshes)
    };

    assert_eq!(resolved, 2);
    assert_eq!(refreshes, 2);
    assert_eq!(cluster.status(txn_id).status, TxnStatus::Aborted);
    assert!(cluster
        .tablets
        .values()
        .all(|tablet| tablet.tablet().stats().locks == 0));
    assert!(cluster
        .tablets
        .values()
        .all(|tablet| tablet.tablet().stats().write_records == 1));
}

#[test]
fn gateway_crash_after_primary_commit_restarts_and_resolves_secondary() {
    let txn_id = TxnId(701);
    let (mut coordinator, primary_key) = transaction_and_routes(txn_id);
    let mut cluster = DurableCluster::new();
    cluster.publish_pending(txn_id, primary_key);

    {
        let mut gateway = GatewayProcess::new(&mut cluster, false, false);
        let mut refresher = UnexpectedRouteRefresh;
        coordinator
            .execute_prewrite_with_retry(30_000, &mut refresher, &mut gateway, 1)
            .unwrap();
    }

    let commit_result = {
        let mut gateway = GatewayProcess::new(&mut cluster, false, true);
        let mut timestamps = LocalTransactionManager::new();
        let mut refresher = UnexpectedRouteRefresh;
        coordinator.execute_commit_with_retry(
            &mut timestamps,
            &mut refresher,
            &mut gateway,
            1,
        )
    };
    assert!(matches!(
        commit_result,
        Err(Error::RequestOutcomeUnknown { .. })
    ));
    assert_eq!(cluster.status(txn_id).status, TxnStatus::Committed);
    assert_eq!(cluster.status(txn_id).commit_timestamp, Some(Timestamp(101)));

    cluster.restart_participants();
    let (resolved, refreshes) = {
        let mut recovery = GatewayProcess::new(&mut cluster, false, false);
        let resolved = recovery.recover_visible_intents(1_000).unwrap();
        (resolved, recovery.status_route_refreshes)
    };

    assert_eq!(resolved, 1);
    assert_eq!(refreshes, 1);
    assert_eq!(cluster.status(txn_id).status, TxnStatus::Committed);
    assert_eq!(cluster.tablets[&PRIMARY_TABLET].tablet().stats().locks, 0);
    assert_eq!(cluster.tablets[&SECONDARY_TABLET].tablet().stats().locks, 0);

    let reader = Transaction::new(TxnId(9000), Timestamp(102)).unwrap();
    assert_eq!(
        cluster.tablets[&PRIMARY_TABLET]
            .tablet()
            .get(&reader, &row_key(700))
            .unwrap(),
        Some(row(700, "primary"))
    );
    assert_eq!(
        cluster.tablets[&SECONDARY_TABLET]
            .tablet()
            .get(&reader, &row_key(701))
            .unwrap(),
        Some(row(701, "secondary"))
    );

}
