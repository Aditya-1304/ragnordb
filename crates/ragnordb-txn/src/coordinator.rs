//! ordered single node durable commit coordination
//!
//! one coordinator owns one commit participant and one semantic durable log
//! Its mutable commit method forms the serialized correctness boundary from
//! complete preflight through durable append and atomic MVCC application

use std::collections::{BTreeMap, BTreeSet};

use ragnordb_common::{
    Error, Result,
    ids::{
        ClientRequestId, CommandKind, LogicalCommandId, ParticipantCommandPhase, RaftGroupId,
        ReplicaId, RequestId, TableId, TabletId, Timestamp, TxnId, participant_logical_command_id,
        request_id_for_logical_command,
    },
};
use ragnordb_storage::{
    key::decode_row_key,
    mvcc::{Mutation, MvccStorage},
    wal::{DurableCommitLog, DurableWalExtent, SingleNodeTxnCommit, WalMutation},
};

use crate::{
    CommitTimestampAllocator, Transaction, TransactionReadSpan, status::TransactionStatusLocation,
};

/// Canonical logical identity for one transaction mutation or read key.
///
/// The encoded row key is the logical identity; it is deliberately not a
/// tablet ID, Raft group ID, or tablet epoch. Those physical values are route
/// hints and may change after a split, merge, replica move, or leader change.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalMutationId(Vec<u8>);

impl LogicalMutationId {
    /// Construct an identity only from a canonical encoded row key.
    pub fn from_key(key: &[u8]) -> Result<Self> {
        decode_row_key(key).map_err(|error| {
            Error::InvalidArgument(format!(
                "logical transaction key is not a canonical row key: {error}"
            ))
        })?;

        Ok(Self(key.to_vec()))
    }

    /// Return the canonical key represented by this logical identity.
    pub fn as_key(&self) -> &[u8] {
        &self.0
    }
}

/// Current physical route hint for one logical transaction participant.
///
/// A route is intentionally replaceable. The coordinator never uses these
/// fields as part of a participant command identity; they only select the
/// current Raft destination and stale-epoch fence for dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ParticipantRoute {
    pub tablet_id: TabletId,
    pub tablet_epoch: u64,
    pub raft_group_id: RaftGroupId,
    pub leader_replica_id: ReplicaId,
}

impl ParticipantRoute {
    pub fn new(
        tablet_id: TabletId,
        tablet_epoch: u64,
        raft_group_id: RaftGroupId,
        leader_replica_id: ReplicaId,
    ) -> Result<Self> {
        if tablet_id.0 == 0 {
            return Err(Error::InvalidArgument(
                "transaction participant tablet ID 0 is reserved".to_string(),
            ));
        }
        if tablet_epoch == 0 {
            return Err(Error::InvalidArgument(
                "transaction participant tablet epoch 0 is reserved".to_string(),
            ));
        }
        if raft_group_id.0 == 0 {
            return Err(Error::InvalidArgument(
                "transaction participant Raft group ID 0 is reserved".to_string(),
            ));
        }
        if leader_replica_id.0 == 0 {
            return Err(Error::InvalidArgument(
                "transaction participant leader replica ID 0 is reserved".to_string(),
            ));
        }

        Ok(Self {
            tablet_id,
            tablet_epoch,
            raft_group_id,
            leader_replica_id,
        })
    }
}

/// Stable participant command identity for one logical mutation.
///
/// This identity is the coordinator's source of truth for retries. It is
/// derived only from the transaction, phase, and logical key, never from the
/// current tablet route. Phase dispatchers adapt it to concrete command
/// envelopes and transport request IDs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParticipantCommandId {
    txn_id: TxnId,
    phase: ParticipantCommandPhase,
    logical_mutation_id: LogicalMutationId,
}

impl ParticipantCommandId {
    fn new(
        txn_id: TxnId,
        phase: ParticipantCommandPhase,
        logical_mutation_id: LogicalMutationId,
    ) -> Self {
        Self {
            txn_id,
            phase,
            logical_mutation_id,
        }
    }

    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    pub fn phase(&self) -> ParticipantCommandPhase {
        self.phase
    }

    pub fn logical_mutation_id(&self) -> &LogicalMutationId {
        &self.logical_mutation_id
    }

    /// Map the transaction phase to the existing tablet command kind.
    pub fn kind(&self) -> CommandKind {
        self.phase.command_kind()
    }

    /// Convert the semantic identity into the existing V2 logical command
    /// identity carried by tablet envelopes.
    pub fn logical_command_id(&self) -> Result<LogicalCommandId> {
        participant_logical_command_id(self.txn_id, self.phase, self.logical_mutation_id.as_key())
            .map_err(|error| Error::InvalidArgument(error.to_string()))
    }

    /// Create the current group-qualified transport identity. A route change
    /// may change this request ID's group field, but never changes the logical
    /// command ID returned above.
    pub fn request_id(&self, route: ParticipantRoute) -> Result<RequestId> {
        let logical_command_id = self.logical_command_id()?;
        request_id_for_logical_command(logical_command_id, route.raft_group_id)
            .map_err(|error| Error::InvalidArgument(error.to_string()))
    }
}

/// One dispatch-ready view of a logical participant command.
///
/// The logical ID is stable across retries and topology changes. The route and
/// group-qualified request ID are regenerated from the current route hint for
/// each attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParticipantCommandPlan {
    pub command_id: ParticipantCommandId,
    pub logical_command_id: LogicalCommandId,
    pub request_id: RequestId,
    pub root_request_id: ClientRequestId,
    pub route: ParticipantRoute,
}

impl ParticipantCommandPlan {
    fn new(
        command_id: ParticipantCommandId,
        root_request_id: ClientRequestId,
        route: ParticipantRoute,
    ) -> Result<Self> {
        let logical_command_id = command_id.logical_command_id()?;
        let request_id = command_id.request_id(route)?;
        Ok(Self {
            command_id,
            logical_command_id,
            request_id,
            root_request_id,
            route,
        })
    }
}

/// Dispatch failures that the coordinator can classify without inspecting a
/// transport-specific error string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParticipantDispatchError {
    /// The current route or epoch is no longer admissible. The coordinator may
    /// refresh the route and retry the same logical command identity.
    RouteRefreshRequired { reason: String },

    /// The proposal may have applied, but its outcome is not known. This is
    /// never safe to resubmit as a new mutation.
    OutcomeUnknown { reason: String },

    /// The participant found a deterministic MVCC conflict. No later batch
    /// may be dispatched because the transaction cannot complete its prewrite
    /// phase without a rollback phase.
    WriteConflict { reason: String },

    /// The participant rejected the command deterministically.
    Rejected { reason: String },

    /// The participant is temporarily unavailable after the route was already
    /// selected. The caller may decide whether to retry the same phase.
    Unavailable { reason: String },
}

/// Resolve a logical participant key to its current physical tablet route.
///
/// `previous_route` is only a cache hint from the last plan. Implementations
/// must resolve ownership for the supplied logical key against the current
/// tablet descriptors; keys that shared a former batch may now have different
/// owners after a split or merge.
pub trait ParticipantRouteRefresher {
    fn refresh_participant_route(
        &mut self,
        logical_mutation_id: &LogicalMutationId,
        previous_route: ParticipantRoute,
    ) -> Result<ParticipantRoute>;
}

/// Dispatch one already-planned participant command.
pub trait ParticipantPhaseDispatcher {
    type Output;

    fn dispatch(
        &mut self,
        plan: &ParticipantCommandPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError>;
}

/// Dispatch one complete prewrite batch to its current participant tablet.
///
/// The dispatcher owns the concrete tablet/RPC transport. The coordinator
/// supplies a complete atomic batch plus the stable logical identities needed
/// by that transport to deduplicate an exact retry.
pub trait PrewriteBatchDispatcher {
    type Output;

    fn dispatch_prewrite(
        &mut self,
        plan: &crate::prewrite::PrewriteBatchPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError>;
}

/// Dispatch the distributed commit decision in its required durability order.
///
/// `dispatch_primary` owns the primary/status commit point: an implementation
/// must durably and replicably publish `plan.status_record` together with the
/// primary commit batch before returning `Ok`. Secondary callbacks run only
/// after that point. Returning `OutcomeUnknown` is terminal for the current
/// call because resubmitting a commit whose apply result is unknown could
/// bypass the caller's outcome-recovery boundary.
pub trait CommitPhaseDispatcher {
    type PrimaryOutput;
    type SecondaryOutput;

    fn dispatch_primary(
        &mut self,
        plan: &crate::commit::CommitPhasePlan,
    ) -> std::result::Result<Self::PrimaryOutput, ParticipantDispatchError>;

    fn dispatch_secondary(
        &mut self,
        plan: &crate::commit::CommitBatchPlan,
    ) -> std::result::Result<Self::SecondaryOutput, ParticipantDispatchError>;
}

/// Dispatch the rollback decision in participant-first order.
///
/// `dispatch_primary_rollback` and `dispatch_secondary_rollback` must return
/// `Ok` only after their rollback command is durably replicated and applied at
/// the participant boundary. `dispatch_status_abort` must return `Ok` only
/// after the aborted transaction-status record is durably replicated. An
/// `OutcomeUnknown` result is terminal because replaying a mutation whose
/// apply result is unknown requires an explicit outcome lookup, not a fresh
/// proposal.
pub trait RollbackPhaseDispatcher {
    type PrimaryOutput;
    type SecondaryOutput;
    type StatusOutput;

    fn dispatch_primary_rollback(
        &mut self,
        plan: &crate::rollback::RollbackBatchPlan,
    ) -> std::result::Result<Self::PrimaryOutput, ParticipantDispatchError>;

    fn dispatch_secondary_rollback(
        &mut self,
        plan: &crate::rollback::RollbackBatchPlan,
    ) -> std::result::Result<Self::SecondaryOutput, ParticipantDispatchError>;

    fn dispatch_status_abort(
        &mut self,
        plan: &crate::rollback::RollbackPhasePlan,
    ) -> std::result::Result<Self::StatusOutput, ParticipantDispatchError>;
}

/// Transaction-local coordinator state for the Phase 6.2 identity boundary.
///
/// This type owns semantic transaction state and current route hints. It does
/// not own a concrete Raft or RPC transport; phase dispatch is performed
/// through caller-supplied batch and route-refresh traits so transport cannot
/// become a second transaction authority.
#[derive(Debug)]
pub struct DistributedTransactionCoordinator {
    transaction: Transaction,
    root_request_id: ClientRequestId,
    primary_key: LogicalMutationId,
    read_set: BTreeSet<LogicalMutationId>,
    read_spans: BTreeSet<TransactionReadSpan>,
    participant_routes: BTreeMap<LogicalMutationId, ParticipantRoute>,
    status_location: TransactionStatusLocation,
}

impl DistributedTransactionCoordinator {
    /// Create coordinator state around one timestamped transaction.
    pub fn new(
        transaction: Transaction,
        root_request_id: ClientRequestId,
        primary_key: Vec<u8>,
    ) -> Result<Self> {
        root_request_id
            .validate()
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        let primary_key = LogicalMutationId::from_key(&primary_key)?;
        let status_location =
            TransactionStatusLocation::new(transaction.id(), primary_key.as_key().to_vec())?;
        let read_set = transaction
            .read_keys()
            .iter()
            .map(|key| LogicalMutationId::from_key(key))
            .collect::<Result<BTreeSet<_>>>()?;
        let read_spans = transaction.read_spans().clone();

        Ok(Self {
            transaction,
            root_request_id,
            primary_key,
            read_set,
            read_spans,
            participant_routes: BTreeMap::new(),
            status_location,
        })
    }

    pub fn transaction_id(&self) -> TxnId {
        self.transaction.id()
    }

    pub fn start_timestamp(&self) -> Timestamp {
        self.transaction.start_ts()
    }

    /// The client identity retained for root-operation status and retry.
    pub fn root_request_id(&self) -> ClientRequestId {
        self.root_request_id
    }

    pub fn primary_key(&self) -> &[u8] {
        self.primary_key.as_key()
    }

    pub fn transaction(&self) -> &Transaction {
        &self.transaction
    }

    pub fn read_set(&self) -> &BTreeSet<LogicalMutationId> {
        &self.read_set
    }

    /// Logical table ranges read by the transaction before commit planning.
    ///
    /// The coordinator retains these spans for later footprint accounting;
    /// this milestone does not use them to change conflict semantics.
    pub fn read_spans(&self) -> &BTreeSet<TransactionReadSpan> {
        &self.read_spans
    }

    pub fn write_set(&self) -> &BTreeMap<Vec<u8>, Mutation> {
        self.transaction.write_set()
    }

    pub fn status_location(&self) -> &TransactionStatusLocation {
        &self.status_location
    }

    /// Record one canonical logical read key exactly once.
    pub fn record_read(&mut self, key: Vec<u8>) -> Result<()> {
        self.read_set.insert(LogicalMutationId::from_key(&key)?);
        Ok(())
    }

    /// Add a logical range to the coordinator's transaction footprint.
    pub fn record_read_span(&mut self, span: TransactionReadSpan) {
        self.read_spans.insert(span);
    }

    /// Replace the current route hint for one write participant.
    pub fn set_participant_route(&mut self, key: Vec<u8>, route: ParticipantRoute) -> Result<()> {
        let logical_mutation_id = LogicalMutationId::from_key(&key)?;
        if !self.transaction.write_set().contains_key(&key) {
            return Err(Error::InvalidArgument(
                "participant route requires a key in the transaction write set".to_string(),
            ));
        }

        self.participant_routes.insert(logical_mutation_id, route);
        Ok(())
    }

    pub fn participant_route(&self, key: &[u8]) -> Option<&ParticipantRoute> {
        let logical_mutation_id = LogicalMutationId::from_key(key).ok()?;
        self.participant_routes.get(&logical_mutation_id)
    }

    /// Return the current physical tablets represented by route hints.
    pub fn participant_tablets(&self) -> BTreeSet<TabletId> {
        self.participant_routes
            .values()
            .map(|route| route.tablet_id)
            .collect()
    }

    pub fn participant_routes(&self) -> &BTreeMap<LogicalMutationId, ParticipantRoute> {
        &self.participant_routes
    }

    /// Refresh the semantic status key's physical route hint.
    pub fn set_status_route(&mut self, route: ParticipantRoute) -> Result<()> {
        self.status_location.set_route(route)
    }

    /// Derive a stable command identity for one transaction write key.
    pub fn participant_command_id(
        &self,
        phase: ParticipantCommandPhase,
        key: &[u8],
    ) -> Result<ParticipantCommandId> {
        let logical_mutation_id = LogicalMutationId::from_key(key)?;
        self.participant_command_id_for_logical(phase, logical_mutation_id)
    }

    /// Build one dispatch plan using the route currently cached for a write.
    pub fn participant_command_plan(
        &self,
        phase: ParticipantCommandPhase,
        key: &[u8],
    ) -> Result<ParticipantCommandPlan> {
        let logical_mutation_id = LogicalMutationId::from_key(key)?;
        let route = self
            .participant_routes
            .get(&logical_mutation_id)
            .copied()
            .ok_or_else(|| {
                Error::InvalidArgument(
                    "participant command requires a current route hint".to_string(),
                )
            })?;
        let command_id = self.participant_command_id_for_logical(phase, logical_mutation_id)?;
        ParticipantCommandPlan::new(command_id, self.root_request_id, route)
    }

    /// Build plans in canonical write-key order. The order is only a stable
    /// local dispatch order; logical command IDs remain independent of it.
    pub fn plan_phase(
        &self,
        phase: ParticipantCommandPhase,
    ) -> Result<Vec<ParticipantCommandPlan>> {
        self.transaction
            .write_set()
            .keys()
            .map(|key| self.participant_command_plan(phase, key))
            .collect()
    }

    /// Build validated prewrite batches grouped by their current participant
    /// tablet. This is a pure planning operation: no Raft proposal, tablet
    /// mutation, or transaction acknowledgement occurs here.
    pub fn plan_prewrite(&self, ttl_ms: u64) -> Result<Vec<crate::prewrite::PrewriteBatchPlan>> {
        crate::prewrite::plan_prewrite(self, ttl_ms)
    }

    /// Build the commit timestamp, durable status outcome, and primary-first
    /// participant batches without dispatching any command.
    pub fn plan_commit<M: CommitTimestampAllocator>(
        &self,
        timestamp_manager: &mut M,
    ) -> Result<crate::commit::CommitPhasePlan> {
        crate::commit::plan_commit(self, timestamp_manager)
    }

    /// Build a validated aborted status outcome and rollback batches without
    /// dispatching a command or mutating participant state.
    pub fn plan_rollback(&self) -> Result<crate::rollback::RollbackPhasePlan> {
        crate::rollback::plan_rollback(self)
    }

    /// Execute rollback batches before publishing the durable aborted status.
    ///
    /// A route refresh rebuilds the complete rollback plan from the stable
    /// logical transaction state. If a secondary route changes after an
    /// earlier batch succeeds, the participant phase restarts from the
    /// primary; exact rollback command identities make already-applied
    /// batches safe to replay at the participant's idempotent rollback
    /// boundary. The aborted status is published only after all participant
    /// batches report durable success. An unknown outcome is terminal and is
    /// returned without a retry or status publication.
    pub fn execute_rollback_with_retry<D, R>(
        &mut self,
        refresher: &mut R,
        dispatcher: &mut D,
        max_route_refreshes: usize,
    ) -> Result<
        crate::rollback::RollbackExecutionOutcome<
            D::PrimaryOutput,
            D::SecondaryOutput,
            D::StatusOutput,
        >,
    >
    where
        D: RollbackPhaseDispatcher,
        R: ParticipantRouteRefresher,
    {
        if max_route_refreshes == 0 {
            return Err(Error::InvalidArgument(
                "rollback route refresh budget must be non-zero".to_string(),
            ));
        }

        let mut route_refreshes = 0;
        let mut plan = self.plan_rollback()?;
        let (primary_outcome, secondary_outcomes) = loop {
            let primary_outcome = loop {
                match dispatcher.dispatch_primary_rollback(&plan.primary) {
                    Ok(outcome) => break outcome,
                    Err(ParticipantDispatchError::RouteRefreshRequired { reason }) => {
                        self.refresh_rollback_batch_route(
                            &plan.primary,
                            refresher,
                            true,
                            &mut route_refreshes,
                            max_route_refreshes,
                            reason,
                        )?;
                        plan = self.plan_rollback()?;
                    }
                    Err(ParticipantDispatchError::OutcomeUnknown { reason }) => {
                        return Err(self.rollback_outcome_unknown(&plan.primary, reason));
                    }
                    Err(ParticipantDispatchError::WriteConflict { reason }) => {
                        return Err(Error::WriteConflict(reason));
                    }
                    Err(ParticipantDispatchError::Rejected { reason }) => {
                        return Err(Error::ConstraintViolation(reason));
                    }
                    Err(ParticipantDispatchError::Unavailable { reason }) => {
                        return Err(Error::TabletUnavailable { reason });
                    }
                }
            };

            let mut secondary_outcomes = Vec::with_capacity(plan.secondary.len());
            let mut restart_participant_phase = false;
            for batch in plan.secondary.clone() {
                match dispatcher.dispatch_secondary_rollback(&batch) {
                    Ok(outcome) => secondary_outcomes.push(outcome),
                    Err(ParticipantDispatchError::RouteRefreshRequired { reason }) => {
                        self.refresh_rollback_batch_route(
                            &batch,
                            refresher,
                            false,
                            &mut route_refreshes,
                            max_route_refreshes,
                            reason,
                        )?;
                        plan = self.plan_rollback()?;
                        restart_participant_phase = true;
                        break;
                    }
                    Err(ParticipantDispatchError::OutcomeUnknown { reason }) => {
                        return Err(self.rollback_outcome_unknown(&batch, reason));
                    }
                    Err(ParticipantDispatchError::WriteConflict { reason }) => {
                        return Err(Error::WriteConflict(reason));
                    }
                    Err(ParticipantDispatchError::Rejected { reason }) => {
                        return Err(Error::ConstraintViolation(reason));
                    }
                    Err(ParticipantDispatchError::Unavailable { reason }) => {
                        return Err(Error::TabletUnavailable { reason });
                    }
                }
            }

            if restart_participant_phase {
                continue;
            }

            break (primary_outcome, secondary_outcomes);
        };

        let status_outcome = loop {
            match dispatcher.dispatch_status_abort(&plan) {
                Ok(outcome) => break outcome,
                Err(ParticipantDispatchError::RouteRefreshRequired { reason }) => {
                    self.refresh_rollback_batch_route(
                        &plan.primary,
                        refresher,
                        true,
                        &mut route_refreshes,
                        max_route_refreshes,
                        reason,
                    )?;
                    plan = self.plan_rollback()?;
                }
                Err(ParticipantDispatchError::OutcomeUnknown { reason }) => {
                    return Err(self.rollback_status_outcome_unknown(&plan, reason));
                }
                Err(ParticipantDispatchError::WriteConflict { reason }) => {
                    return Err(Error::WriteConflict(reason));
                }
                Err(ParticipantDispatchError::Rejected { reason }) => {
                    return Err(Error::ConstraintViolation(reason));
                }
                Err(ParticipantDispatchError::Unavailable { reason }) => {
                    return Err(Error::TabletUnavailable { reason });
                }
            }
        };

        Ok(crate::rollback::RollbackExecutionOutcome {
            primary: primary_outcome,
            secondary: secondary_outcomes,
            status: status_outcome,
        })
    }

    fn refresh_rollback_batch_route<R>(
        &mut self,
        batch: &crate::rollback::RollbackBatchPlan,
        refresher: &mut R,
        update_status_route: bool,
        route_refreshes: &mut usize,
        max_route_refreshes: usize,
        reason: String,
    ) -> Result<()>
    where
        R: ParticipantRouteRefresher,
    {
        if *route_refreshes >= max_route_refreshes {
            return Err(Error::TabletUnavailable {
                reason: format!(
                    "rollback route refresh budget exhausted for transaction {}: {reason}",
                    self.transaction.id().0
                ),
            });
        }

        let primary_route = self.refresh_participant_batch_routes(
            &batch.participant_plans,
            batch.route,
            refresher,
        )?;
        if update_status_route {
            self.set_status_route(primary_route.ok_or_else(|| {
                Error::CorruptData(
                    "primary rollback batch omitted the primary logical key".to_string(),
                )
            })?)?;
        }
        *route_refreshes += 1;
        Ok(())
    }

    /// Resolve every logical mutation in a rejected batch against the current
    /// ownership view before rebuilding that phase. A split can leave sibling
    /// keys owned by different tablets, so propagating one anchor key's route
    /// across the old batch would misroute some of its mutations.
    fn refresh_participant_batch_routes<R>(
        &mut self,
        participant_plans: &[ParticipantCommandPlan],
        previous_route: ParticipantRoute,
        refresher: &mut R,
    ) -> Result<Option<ParticipantRoute>>
    where
        R: ParticipantRouteRefresher,
    {
        if participant_plans.is_empty() {
            return Err(Error::CorruptData(
                "participant batch has no logical command identities".to_string(),
            ));
        }

        // Resolve the full key set before mutating coordinator route hints. If
        // metadata lookup fails partway through, the caller retains the old,
        // coherent routing view instead of a partially refreshed transaction.
        let mut refreshed_routes = Vec::with_capacity(participant_plans.len());
        let mut any_route_changed = false;
        let mut primary_route = None;
        for participant_plan in participant_plans {
            let logical_mutation_id = participant_plan.command_id.logical_mutation_id();
            let route = refresher.refresh_participant_route(logical_mutation_id, previous_route)?;
            any_route_changed |= route != previous_route;
            if logical_mutation_id.as_key() == self.primary_key() {
                primary_route = Some(route);
            }
            refreshed_routes.push((logical_mutation_id.as_key().to_vec(), route));
        }

        if !any_route_changed {
            return Err(Error::TabletUnavailable {
                reason: "participant route refresh returned the unchanged route for every key"
                    .to_string(),
            });
        }

        for (key, route) in refreshed_routes {
            self.set_participant_route(key, route)?;
        }

        Ok(primary_route)
    }

    fn rollback_outcome_unknown(
        &self,
        batch: &crate::rollback::RollbackBatchPlan,
        reason: String,
    ) -> Error {
        let logical_command = batch
            .participant_plans
            .first()
            .map(|plan| format!("{:?}", plan.logical_command_id))
            .unwrap_or_else(|| "missing".to_string());
        Error::RequestOutcomeUnknown {
            identity: format!(
                "transaction={} phase={:?} tablet={} logical_command={logical_command}: {reason}",
                self.transaction.id().0,
                ParticipantCommandPhase::Rollback,
                batch.route.tablet_id.0,
            ),
        }
    }

    fn rollback_status_outcome_unknown(
        &self,
        plan: &crate::rollback::RollbackPhasePlan,
        reason: String,
    ) -> Error {
        Error::RequestOutcomeUnknown {
            identity: format!(
                "transaction={} phase={:?} status_tablet={} status_key={:?}: {reason}",
                self.transaction.id().0,
                ParticipantCommandPhase::Rollback,
                plan.status_route.tablet_id.0,
                plan.status_key,
            ),
        }
    }

    /// Execute the commit decision with the primary/status commit point first.
    ///
    /// A route refresh rebuilds the physical plan while retaining the original
    /// commit timestamp and logical command identities. If a secondary route
    /// changes after an earlier secondary succeeded, the rebuilt phase may
    /// present that exact logical command again; participant-side durable
    /// deduplication makes that replay safe. An unknown outcome never retries.
    pub fn execute_commit_with_retry<M, D, R>(
        &mut self,
        timestamp_manager: &mut M,
        refresher: &mut R,
        dispatcher: &mut D,
        max_route_refreshes: usize,
    ) -> Result<crate::commit::CommitExecutionOutcome<D::PrimaryOutput, D::SecondaryOutput>>
    where
        M: CommitTimestampAllocator,
        D: CommitPhaseDispatcher,
        R: ParticipantRouteRefresher,
    {
        if max_route_refreshes == 0 {
            return Err(Error::InvalidArgument(
                "commit route refresh budget must be non-zero".to_string(),
            ));
        }

        let mut route_refreshes = 0;
        let mut plan = self.plan_commit(timestamp_manager)?;
        let primary_outcome = loop {
            match dispatcher.dispatch_primary(&plan) {
                Ok(outcome) => break outcome,
                Err(ParticipantDispatchError::RouteRefreshRequired { reason }) => {
                    self.refresh_commit_batch_route(
                        &plan.primary,
                        refresher,
                        true,
                        &mut route_refreshes,
                        max_route_refreshes,
                        reason,
                    )?;
                    plan = crate::commit::plan_commit_at_timestamp(self, plan.commit_timestamp)?;
                }
                Err(ParticipantDispatchError::OutcomeUnknown { reason }) => {
                    return Err(self.commit_outcome_unknown(
                        ParticipantCommandPhase::Commit,
                        &plan.primary,
                        reason,
                    ));
                }
                Err(ParticipantDispatchError::WriteConflict { reason }) => {
                    return Err(Error::WriteConflict(reason));
                }
                Err(ParticipantDispatchError::Rejected { reason }) => {
                    return Err(Error::ConstraintViolation(reason));
                }
                Err(ParticipantDispatchError::Unavailable { reason }) => {
                    return Err(Error::TabletUnavailable { reason });
                }
            }
        };

        loop {
            let mut secondary_outcomes = Vec::with_capacity(plan.secondary.len());
            let mut restart_secondary_phase = false;

            for batch in plan.secondary.clone() {
                match dispatcher.dispatch_secondary(&batch) {
                    Ok(outcome) => secondary_outcomes.push(outcome),
                    Err(ParticipantDispatchError::RouteRefreshRequired { reason }) => {
                        self.refresh_commit_batch_route(
                            &batch,
                            refresher,
                            false,
                            &mut route_refreshes,
                            max_route_refreshes,
                            reason,
                        )?;
                        plan =
                            crate::commit::plan_commit_at_timestamp(self, plan.commit_timestamp)?;
                        restart_secondary_phase = true;
                        break;
                    }
                    Err(ParticipantDispatchError::OutcomeUnknown { reason }) => {
                        return Err(self.commit_outcome_unknown(
                            ParticipantCommandPhase::Commit,
                            &batch,
                            reason,
                        ));
                    }
                    Err(ParticipantDispatchError::WriteConflict { reason }) => {
                        return Err(Error::WriteConflict(reason));
                    }
                    Err(ParticipantDispatchError::Rejected { reason }) => {
                        return Err(Error::ConstraintViolation(reason));
                    }
                    Err(ParticipantDispatchError::Unavailable { reason }) => {
                        return Err(Error::TabletUnavailable { reason });
                    }
                }
            }

            if restart_secondary_phase {
                continue;
            }

            return Ok(crate::commit::CommitExecutionOutcome {
                commit_timestamp: plan.commit_timestamp,
                primary: primary_outcome,
                secondary: secondary_outcomes,
            });
        }
    }

    fn refresh_commit_batch_route<R>(
        &mut self,
        batch: &crate::commit::CommitBatchPlan,
        refresher: &mut R,
        update_status_route: bool,
        route_refreshes: &mut usize,
        max_route_refreshes: usize,
        reason: String,
    ) -> Result<()>
    where
        R: ParticipantRouteRefresher,
    {
        if *route_refreshes >= max_route_refreshes {
            return Err(Error::TabletUnavailable {
                reason: format!(
                    "commit route refresh budget exhausted for transaction {}: {reason}",
                    self.transaction.id().0
                ),
            });
        }

        let primary_route = self.refresh_participant_batch_routes(
            &batch.participant_plans,
            batch.route,
            refresher,
        )?;
        if update_status_route {
            self.set_status_route(primary_route.ok_or_else(|| {
                Error::CorruptData(
                    "primary commit batch omitted the primary logical key".to_string(),
                )
            })?)?;
        }
        *route_refreshes += 1;
        Ok(())
    }

    fn commit_outcome_unknown(
        &self,
        phase: ParticipantCommandPhase,
        batch: &crate::commit::CommitBatchPlan,
        reason: String,
    ) -> Error {
        let logical_command = batch
            .participant_plans
            .first()
            .map(|plan| format!("{:?}", plan.logical_command_id))
            .unwrap_or_else(|| "missing".to_string());
        Error::RequestOutcomeUnknown {
            identity: format!(
                "transaction={} phase={phase:?} tablet={} logical_command={logical_command}: {reason}",
                self.transaction.id().0,
                batch.route.tablet_id.0,
            ),
        }
    }

    /// Dispatch every planned prewrite batch with bounded route-refresh
    /// retries.
    ///
    /// A route refresh restarts the complete phase from a fresh grouping so a
    /// topology change cannot leave the coordinator dispatching an old batch
    /// layout. Previously applied batches may be presented again with their
    /// original logical identities; the participant's durable deduplication
    /// boundary must make that exact replay idempotent. An unknown outcome is
    /// terminal for this call and is never replayed without an outcome lookup.
    /// Rollback of earlier successful batches is intentionally Phase 6.6.
    pub fn execute_prewrite_with_retry<D, R>(
        &mut self,
        ttl_ms: u64,
        refresher: &mut R,
        dispatcher: &mut D,
        max_route_refreshes: usize,
    ) -> Result<Vec<D::Output>>
    where
        D: PrewriteBatchDispatcher,
        R: ParticipantRouteRefresher,
    {
        if max_route_refreshes == 0 {
            return Err(Error::InvalidArgument(
                "prewrite route refresh budget must be non-zero".to_string(),
            ));
        }

        let mut route_refreshes = 0;
        loop {
            let mut batches = self.plan_prewrite(ttl_ms)?;
            let primary_key = self.primary_key().to_vec();
            batches.sort_by_key(|batch| {
                !batch.participant_plans.iter().any(|plan| {
                    plan.command_id.logical_mutation_id().as_key() == primary_key.as_slice()
                })
            });
            let mut outcomes = Vec::with_capacity(batches.len());
            let mut restart_phase = false;

            for batch in batches {
                match dispatcher.dispatch_prewrite(&batch) {
                    Ok(outcome) => outcomes.push(outcome),
                    Err(ParticipantDispatchError::RouteRefreshRequired { reason }) => {
                        if route_refreshes >= max_route_refreshes {
                            return Err(Error::TabletUnavailable {
                                reason: format!(
                                    "prewrite route refresh budget exhausted for transaction {}: {reason}",
                                    self.transaction.id().0
                                ),
                            });
                        }

                        let refreshes_primary = batch.participant_plans.iter().any(|plan| {
                            plan.command_id.logical_mutation_id().as_key() == self.primary_key()
                        });
                        let primary_route = self.refresh_participant_batch_routes(
                            &batch.participant_plans,
                            batch.route,
                            refresher,
                        )?;
                        if refreshes_primary {
                            self.set_status_route(primary_route.ok_or_else(|| {
                                Error::CorruptData(
                                    "primary prewrite batch omitted the primary logical key"
                                        .to_string(),
                                )
                            })?)?;
                        }

                        route_refreshes += 1;
                        restart_phase = true;
                        break;
                    }
                    Err(ParticipantDispatchError::OutcomeUnknown { reason }) => {
                        let logical_command_id = batch
                            .participant_plans
                            .first()
                            .map(|plan| plan.logical_command_id)
                            .ok_or_else(|| {
                                Error::CorruptData(
                                    "prewrite batch has no participant command identity"
                                        .to_string(),
                                )
                            })?;
                        return Err(Error::RequestOutcomeUnknown {
                            identity: format!(
                                "transaction={} phase={:?} tablet={} logical_command={logical_command_id:?}: {reason}",
                                self.transaction.id().0,
                                ParticipantCommandPhase::Prewrite,
                                batch.route.tablet_id.0,
                            ),
                        });
                    }
                    Err(ParticipantDispatchError::WriteConflict { reason }) => {
                        return Err(Error::WriteConflict(reason));
                    }
                    Err(ParticipantDispatchError::Rejected { reason }) => {
                        return Err(Error::ConstraintViolation(reason));
                    }
                    Err(ParticipantDispatchError::Unavailable { reason }) => {
                        return Err(Error::TabletUnavailable { reason });
                    }
                }
            }

            if !restart_phase {
                return Ok(outcomes);
            }
        }
    }

    /// Execute one participant phase with bounded route-refresh retries.
    ///
    /// A route refresh only rebuilds the physical request view. If a dispatch
    /// outcome becomes unknown, this method stops immediately and returns a
    /// lookup-required error; it never creates a fresh command identity.
    pub fn execute_phase_with_retry<D, R>(
        &mut self,
        phase: ParticipantCommandPhase,
        refresher: &mut R,
        dispatcher: &mut D,
        max_route_refreshes: usize,
    ) -> Result<Vec<D::Output>>
    where
        D: ParticipantPhaseDispatcher,
        R: ParticipantRouteRefresher,
    {
        if max_route_refreshes == 0 {
            return Err(Error::InvalidArgument(
                "participant phase retry budget must be non-zero".to_string(),
            ));
        }

        let keys = self
            .transaction
            .write_set()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut outcomes = Vec::with_capacity(keys.len());

        for key in keys {
            let mut refreshes = 0;
            loop {
                let plan = self.participant_command_plan(phase, &key)?;
                match dispatcher.dispatch(&plan) {
                    Ok(outcome) => {
                        outcomes.push(outcome);
                        break;
                    }
                    Err(ParticipantDispatchError::RouteRefreshRequired { reason }) => {
                        if refreshes >= max_route_refreshes {
                            return Err(Error::TabletUnavailable {
                                reason: format!(
                                    "participant route refresh budget exhausted for transaction {}: {reason}",
                                    self.transaction.id().0
                                ),
                            });
                        }

                        let refreshed_route = refresher.refresh_participant_route(
                            plan.command_id.logical_mutation_id(),
                            plan.route,
                        )?;
                        if refreshed_route == plan.route {
                            return Err(Error::TabletUnavailable {
                                reason: "participant route refresh returned the unchanged route"
                                    .to_string(),
                            });
                        }
                        self.set_participant_route(key.clone(), refreshed_route)?;
                        refreshes += 1;
                    }
                    Err(ParticipantDispatchError::OutcomeUnknown { reason }) => {
                        return Err(Error::RequestOutcomeUnknown {
                            identity: format!(
                                "transaction={} phase={:?} logical_command={:?}: {reason}",
                                self.transaction.id().0,
                                phase,
                                plan.logical_command_id
                            ),
                        });
                    }
                    Err(ParticipantDispatchError::WriteConflict { reason }) => {
                        return Err(Error::WriteConflict(reason));
                    }
                    Err(ParticipantDispatchError::Rejected { reason }) => {
                        return Err(Error::ConstraintViolation(reason));
                    }
                    Err(ParticipantDispatchError::Unavailable { reason }) => {
                        return Err(Error::TabletUnavailable { reason });
                    }
                }
            }
        }

        Ok(outcomes)
    }

    fn participant_command_id_for_logical(
        &self,
        phase: ParticipantCommandPhase,
        logical_mutation_id: LogicalMutationId,
    ) -> Result<ParticipantCommandId> {
        if !self
            .transaction
            .write_set()
            .contains_key(logical_mutation_id.as_key())
        {
            return Err(Error::InvalidArgument(
                "participant command requires a key in the transaction write set".to_string(),
            ));
        }

        Ok(ParticipantCommandId::new(
            self.transaction.id(),
            phase,
            logical_mutation_id,
        ))
    }
}

/// storage participant controlled by the ordered commit coordinator
///
/// implementations must perform complete mutation-free validation in
/// `validate_commit` and atomically apply the same immutable transaction in
/// `apply_commit`
pub trait SingleNodeCommitParticipant {
    /// Return the single table owned by this participant.
    fn table_id(&self) -> TableId;

    /// Return the catalog schema revision used to encode transaction rows.
    ///
    /// Single-node mode currently has no schema evolution, so existing
    /// participants use the initial revision. Future schema-aware participants
    /// can override this without changing the durable commit coordinator.
    fn schema_version(&self) -> u64 {
        1
    }

    /// Validate the complete transaction without changing visible state.
    fn validate_commit(&self, transaction: &Transaction) -> Result<()>;

    /// Atomically apply a transaction whose commit record is already durable.
    fn apply_commit(
        &mut self,
        transaction: &Transaction,
        commit_timestamp: Timestamp,
    ) -> Result<usize>;
}

/// Direct MVCC participant retained for storage-level coordinator use.
///
/// Executor integration uses `Tablet` as the participant so reads, statement
/// buffering, preflight, and durable application all observe one MVCC store.
pub struct OwnedMvccParticipant<S>
where
    S: MvccStorage,
{
    table_id: TableId,
    storage: S,
}

impl<S> OwnedMvccParticipant<S>
where
    S: MvccStorage,
{
    fn new(table_id: TableId, storage: S) -> Result<Self> {
        if table_id.0 == 0 {
            return Err(Error::InvalidArgument(
                "commit participant table ID 0 is reserved".to_string(),
            ));
        }

        Ok(Self { table_id, storage })
    }

    /// Borrow the underlying MVCC store for reads and diagnostics.
    pub fn storage(&self) -> &S {
        &self.storage
    }
}

impl<S> SingleNodeCommitParticipant for OwnedMvccParticipant<S>
where
    S: MvccStorage,
{
    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn validate_commit(&self, transaction: &Transaction) -> Result<()> {
        for encoded_key in transaction.write_set().keys() {
            let row_key = decode_row_key(encoded_key).map_err(|error| {
                Error::InvalidArgument(format!(
                    "transaction contains a noncanonical row key: {error}"
                ))
            })?;

            if row_key.table_id != self.table_id {
                return Err(Error::InvalidArgument(format!(
                    "transaction row belongs to table {}, but commit \
                     participant owns table {}",
                    row_key.table_id.0, self.table_id.0
                )));
            }
        }

        self.storage.validate_commit_batch(
            transaction.id(),
            transaction.start_ts(),
            transaction.write_set(),
        )
    }

    fn apply_commit(
        &mut self,
        transaction: &Transaction,
        commit_timestamp: Timestamp,
    ) -> Result<usize> {
        self.storage.commit_batch(
            transaction.id(),
            transaction.start_ts(),
            commit_timestamp,
            transaction.write_set(),
        )
    }
}

/// Published outcome of one completed local commit operation.
#[must_use = "commit outcomes contain the published transaction state"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SingleNodeCommitOutcome {
    pub transaction_id: TxnId,
    pub commit_timestamp: Option<Timestamp>,
    pub committed_writes: usize,
    pub wal_extent: Option<DurableWalExtent>,
}

/// Serialized writer for one table's commit participant and durable log.
///
/// The coordinator is intentionally not cloneable and exposes no mutable
/// participant access. Requiring `&mut self` for `commit` prevents another
/// writer from modifying MVCC state between preflight and application.
pub struct SingleNodeCommitCoordinator<P, W>
where
    P: SingleNodeCommitParticipant,
    W: DurableCommitLog,
{
    participant: P,
    commit_log: W,
    recovery_required_reason: Option<String>,
}

impl<S, W> SingleNodeCommitCoordinator<OwnedMvccParticipant<S>, W>
where
    S: MvccStorage,
    W: DurableCommitLog,
{
    /// Construct a coordinator directly around an MVCC implementation.
    ///
    /// This preserves the storage-level Phase 3.2.5 API.
    pub fn new(table_id: TableId, storage: S, commit_log: W) -> Result<Self> {
        Self::with_participant(OwnedMvccParticipant::new(table_id, storage)?, commit_log)
    }

    /// Borrow the directly owned MVCC store.
    pub fn storage(&self) -> &S {
        self.participant.storage()
    }
}

impl<P, W> SingleNodeCommitCoordinator<P, W>
where
    P: SingleNodeCommitParticipant,
    W: DurableCommitLog,
{
    /// Construct a coordinator around a semantic commit participant.
    pub fn with_participant(participant: P, commit_log: W) -> Result<Self> {
        if participant.table_id().0 == 0 {
            return Err(Error::InvalidArgument(
                "commit coordinator table ID 0 is reserved".to_string(),
            ));
        }

        Ok(Self {
            participant,
            commit_log,
            recovery_required_reason: None,
        })
    }

    pub fn table_id(&self) -> TableId {
        self.participant.table_id()
    }

    /// Borrow the participant for reads and transaction buffering.
    ///
    /// Mutable access remains private to the commit coordinator.
    pub fn participant(&self) -> &P {
        &self.participant
    }

    /// Replace the semantic durability sink used by future commits.
    ///
    /// Startup uses this narrow hook to connect coordinators reconstructed from
    /// database recovery to the replicated tablet host. The participant and its
    /// recovery state remain unchanged; only commits admitted after this call
    /// cross the new durability boundary.
    pub fn replace_commit_log(&mut self, commit_log: W) {
        self.commit_log = commit_log;
    }

    /// Materialize a commit that was already made authoritative by Raft.
    ///
    /// Followers use this path to keep the SQL execution mirror synchronized.
    /// It deliberately performs no local log append: the matching committed
    /// Raft entry in the shared A-WAL is the durable authority.
    pub fn apply_replicated_commit(
        &mut self,
        transaction: &Transaction,
        commit_timestamp: Timestamp,
    ) -> Result<usize> {
        self.validate_transaction_metadata(transaction)?;
        self.ensure_write_path_available()?;
        self.participant.validate_commit(transaction)?;
        self.validate_allocated_commit_timestamp(transaction.start_ts(), commit_timestamp)?;

        let expected_writes = transaction.len();
        let applied_writes = self
            .participant
            .apply_commit(transaction, commit_timestamp)
            .map_err(|source| {
                self.stop_for_recovery(format!(
                    "replicated commit for transaction {} at timestamp {} failed during MVCC application: {}",
                    transaction.id().0,
                    commit_timestamp.0,
                    source
                ))
            })?;

        if applied_writes != expected_writes {
            return Err(self.stop_for_recovery(format!(
                "replicated commit for transaction {} applied {} mutations, but its Raft command contains {}",
                transaction.id().0, applied_writes, expected_writes
            )));
        }

        Ok(applied_writes)
    }

    pub fn requires_recovery(&self) -> bool {
        self.recovery_required_reason.is_some()
    }

    /// Commit one local transaction through the complete ordered boundary.
    pub fn commit<A>(
        &mut self,
        transaction: Transaction,
        mut timestamp_allocator: A,
    ) -> Result<SingleNodeCommitOutcome>
    where
        A: CommitTimestampAllocator,
    {
        self.validate_transaction_metadata(&transaction)?;

        if transaction.is_empty() {
            return Ok(SingleNodeCommitOutcome {
                transaction_id: transaction.id(),
                commit_timestamp: None,
                committed_writes: 0,
                wal_extent: None,
            });
        }

        self.ensure_write_path_available()?;

        self.participant.validate_commit(&transaction)?;

        let commit_timestamp =
            timestamp_allocator.finalize_commit_timestamp(transaction.start_ts())?;

        self.validate_allocated_commit_timestamp(transaction.start_ts(), commit_timestamp)?;

        let durable_record = self.build_commit_record(&transaction, commit_timestamp);

        let wal_extent = match self.commit_log.append_single_node_commit(&durable_record) {
            Ok(extent) => extent,

            Err(error @ Error::CommitOutcomeUnknown { .. }) => {
                if self.recovery_required_reason.is_none() {
                    self.recovery_required_reason = Some(error.to_string());
                }

                return Err(error);
            }

            Err(error) => return Err(error),
        };

        let expected_writes = transaction.len();

        let applied_writes = match self
            .participant
            .apply_commit(&transaction, commit_timestamp)
        {
            Ok(applied_writes) => applied_writes,

            Err(source) => {
                let reason = format!(
                    "durable commit for transaction {} at timestamp {} \
                     failed during atomic MVCC application: {}",
                    transaction.id().0,
                    commit_timestamp.0,
                    source
                );

                return Err(self.stop_for_recovery(reason));
            }
        };

        if applied_writes != expected_writes {
            let reason = format!(
                "durable commit for transaction {} applied {} mutations, \
                 but its WAL record contains {}",
                transaction.id().0,
                applied_writes,
                expected_writes
            );

            return Err(self.stop_for_recovery(reason));
        }

        Ok(SingleNodeCommitOutcome {
            transaction_id: transaction.id(),
            commit_timestamp: Some(commit_timestamp),
            committed_writes: applied_writes,
            wal_extent: Some(wal_extent),
        })
    }

    fn validate_transaction_metadata(&self, transaction: &Transaction) -> Result<()> {
        if transaction.id().0 == 0 {
            return Err(Error::InvalidArgument(
                "commit transaction ID 0 is reserved".to_string(),
            ));
        }

        if transaction.start_ts().0 == 0 {
            return Err(Error::InvalidArgument(
                "commit start timestamp 0 is reserved".to_string(),
            ));
        }

        Ok(())
    }

    fn validate_allocated_commit_timestamp(
        &self,
        start_timestamp: Timestamp,
        commit_timestamp: Timestamp,
    ) -> Result<()> {
        if commit_timestamp.0 == 0 || commit_timestamp <= start_timestamp {
            return Err(Error::Configuration(format!(
                "commit timestamp {} does not advance start timestamp {}",
                commit_timestamp.0, start_timestamp.0
            )));
        }

        Ok(())
    }

    fn build_commit_record(
        &self,
        transaction: &Transaction,
        commit_timestamp: Timestamp,
    ) -> SingleNodeTxnCommit {
        let writes = transaction
            .write_set()
            .iter()
            .map(|(key, mutation)| {
                let mutation = match mutation {
                    Mutation::Put(row) => WalMutation::Put(row.clone()),

                    Mutation::Delete => WalMutation::Delete,
                };

                (key.clone(), mutation)
            })
            .collect::<BTreeMap<_, _>>();

        SingleNodeTxnCommit {
            table_id: self.participant.table_id(),
            txn_id: transaction.id(),
            start_timestamp: transaction.start_ts(),
            commit_timestamp,
            schema_version: self.participant.schema_version(),
            writes,
        }
    }

    fn ensure_write_path_available(&self) -> Result<()> {
        if let Some(reason) = &self.recovery_required_reason {
            return Err(Error::RecoveryRequired {
                reason: reason.clone(),
            });
        }

        Ok(())
    }

    fn stop_for_recovery(&mut self, reason: String) -> Error {
        if self.recovery_required_reason.is_none() {
            self.recovery_required_reason = Some(reason);
        }

        Error::RecoveryRequired {
            reason: self
                .recovery_required_reason
                .clone()
                .expect("recovery reason was initialized above"),
        }
    }
}
