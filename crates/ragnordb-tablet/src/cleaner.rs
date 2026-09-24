//! Bounded background cleanup for transaction intents.
//!
//! A cleaner is deliberately a read/dispatch coordinator. It enumerates
//! intents through the MVCC scan boundary, consults the authoritative status
//! route, and sends terminal changes through the same replicated command
//! boundary used by foreground resolution. It never deletes a lock directly
//! and never treats local lock age as proof that a transaction expired.

use std::collections::BTreeMap;

use ragnordb_common::{
    Error, Result,
    codec::{TxnStatus, TxnStatusRecord},
    ids::TxnId,
};
use ragnordb_storage::mvcc::MvccStorage;
use ragnordb_txn::{
    IntentResolutionDecision, ParticipantDispatchError, ParticipantRoute, ResolveIntentPlan,
    TransactionStatusKey, TransactionStatusLocation, TransactionStatusReader,
    TransactionStatusRouteResolver, plan_intent_resolution,
};

use crate::Tablet;

/// Work limits for one cleaner pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntentCleanerPolicy {
    /// Maximum number of intents read from one MVCC page.
    pub page_size: usize,

    /// Maximum number of pages inspected by one pass.
    pub max_pages_per_run: usize,

    /// Maximum route refreshes allowed for each status lookup.
    pub max_route_refreshes: usize,
}

impl IntentCleanerPolicy {
    pub fn new(
        page_size: usize,
        max_pages_per_run: usize,
        max_route_refreshes: usize,
    ) -> Result<Self> {
        if page_size == 0 || max_pages_per_run == 0 || max_route_refreshes == 0 {
            return Err(Error::InvalidArgument(
                "intent cleaner limits must be non-zero".to_string(),
            ));
        }

        Ok(Self {
            page_size,
            max_pages_per_run,
            max_route_refreshes,
        })
    }
}

/// Counters produced by one bounded cleaner pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IntentCleanupReport {
    pub pages_scanned: usize,
    pub intents_scanned: usize,
    pub pending_intents: usize,
    pub uncertain_intents: usize,
    pub expired_transactions: usize,
    pub resolved_intents: usize,
    pub truncated: bool,
}

/// Conditional status transition required before cleaning an expired pending
/// transaction.
///
/// The status dispatcher must compare `expected_status` with the current
/// durable record before publishing `next_status`. This prevents a concurrent
/// heartbeat or terminal decision from being overwritten by a stale cleaner
/// observation.
#[derive(Debug, Clone, PartialEq)]
pub struct ExpiredTransactionAbortPlan {
    pub status_key: TransactionStatusKey,
    pub status_route: ParticipantRoute,
    pub expected_status: TxnStatusRecord,
    pub next_status: TxnStatusRecord,
}

/// Durable dispatch boundary owned by the tablet/server integration.
pub trait IntentCleanerDispatcher {
    type Output;

    /// Publish `next_status` only if `expected_status` is still authoritative.
    fn dispatch_status_abort(
        &mut self,
        plan: &ExpiredTransactionAbortPlan,
    ) -> std::result::Result<(), ParticipantDispatchError>;

    /// Dispatch the terminal intent-resolution command after status publication.
    fn dispatch_intent_resolution(
        &mut self,
        plan: &ResolveIntentPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError>;
}

/// Scan one tablet's intents and resolve only authoritative terminal outcomes.
pub fn clean_intents<S, R, L, D>(
    tablet: &Tablet<S>,
    policy: IntentCleanerPolicy,
    now_ms: u64,
    route_resolver: &mut R,
    status_reader: &mut L,
    dispatcher: &mut D,
) -> Result<IntentCleanupReport>
where
    S: MvccStorage,
    R: TransactionStatusRouteResolver,
    L: TransactionStatusReader<Output = TxnStatusRecord>,
    D: IntentCleanerDispatcher,
{
    let mut report = IntentCleanupReport::default();
    let mut resume_after = None;
    let mut expired_statuses = BTreeMap::<TxnId, TxnStatusRecord>::new();

    for _ in 0..policy.max_pages_per_run {
        let page = tablet.storage().scan_intent_page(
            None,
            None,
            resume_after.as_deref(),
            policy.page_size,
        )?;
        report.pages_scanned += 1;

        for (key, lock) in &page.locks {
            report.intents_scanned += 1;

            let mut status_location =
                TransactionStatusLocation::new(lock.txn_id, lock.primary_key.clone())?;
            let status = status_location
                .lookup_with_retry(route_resolver, status_reader, policy.max_route_refreshes)?
                .ok_or_else(|| Error::TabletUnavailable {
                    reason: format!(
                        "transaction status for intent owner {} is not currently visible",
                        lock.txn_id.0
                    ),
                })?;

            let status = match status.status {
                TxnStatus::Pending => {
                    let Some(lease_deadline_ms) = status.lease_deadline_ms else {
                        report.uncertain_intents += 1;
                        continue;
                    };

                    if now_ms < lease_deadline_ms {
                        report.pending_intents += 1;
                        continue;
                    }

                    if let Some(status) = expired_statuses.get(&lock.txn_id) {
                        status.clone()
                    } else {
                        let plan = expired_abort_plan(&status_location, &status)?;
                        dispatcher
                            .dispatch_status_abort(&plan)
                            .map_err(map_dispatch_error)?;
                        let next_status = plan.next_status.clone();
                        expired_statuses.insert(lock.txn_id, next_status.clone());
                        report.expired_transactions += 1;
                        next_status
                    }
                }
                TxnStatus::Committed | TxnStatus::Aborted => status,
            };

            let IntentResolutionDecision::Resolve(plan) =
                plan_intent_resolution(key, lock, &status)?
            else {
                return Err(Error::CorruptData(
                    "cleaner produced a non-terminal intent status".to_string(),
                ));
            };

            dispatcher
                .dispatch_intent_resolution(&plan)
                .map_err(map_dispatch_error)?;
            report.resolved_intents += 1;
        }

        if !page.has_more {
            return Ok(report);
        }

        let last_key = page
            .locks
            .last()
            .map(|(key, _)| key.clone())
            .ok_or_else(|| {
                Error::CorruptData(
                    "intent scan page reported more rows without a continuation key".to_string(),
                )
            })?;
        resume_after = Some(last_key);
    }

    report.truncated = true;
    Ok(report)
}

fn expired_abort_plan(
    status_location: &TransactionStatusLocation,
    expected_status: &TxnStatusRecord,
) -> Result<ExpiredTransactionAbortPlan> {
    if expected_status.status != TxnStatus::Pending {
        return Err(Error::CorruptData(
            "expired cleaner status must still be pending".to_string(),
        ));
    }

    let status_route = status_location.route().ok_or_else(|| {
        Error::InvalidArgument("expired cleaner status requires a current route".to_string())
    })?;
    let mut next_status = expected_status.clone();
    next_status.status = TxnStatus::Aborted;
    next_status.commit_timestamp = None;
    next_status.validate().map_err(|error| {
        Error::InvalidArgument(format!("invalid cleaner abort status: {error}"))
    })?;

    Ok(ExpiredTransactionAbortPlan {
        status_key: status_location.status_key().clone(),
        status_route,
        expected_status: expected_status.clone(),
        next_status,
    })
}

fn map_dispatch_error(error: ParticipantDispatchError) -> Error {
    match error {
        ParticipantDispatchError::RouteRefreshRequired { reason }
        | ParticipantDispatchError::Unavailable { reason } => Error::TabletUnavailable { reason },
        ParticipantDispatchError::OutcomeUnknown { reason } => {
            Error::ProposalUnavailable { reason }
        }
        ParticipantDispatchError::WriteConflict { reason } => Error::WriteConflict(reason),
        ParticipantDispatchError::Rejected { reason } => Error::InvalidArgument(reason),
    }
}
