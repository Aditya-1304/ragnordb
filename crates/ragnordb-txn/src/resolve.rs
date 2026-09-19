//! Read-time resolution of terminal distributed transaction intents.
//!
//! A reader may discover an intent after the coordinator has already stopped
//! making progress. The transaction status record is the authority for the
//! outcome; the participant lock is only evidence that a particular key still
//! needs that outcome applied. This module validates both records and builds a
//! deterministic replicated command without mutating tablet state directly.

use ragnordb_common::{
    Error, Result,
    codec::{LockRecord, TxnStatus, TxnStatusRecord},
    command_codec::ResolveIntentCommand,
    ids::{LogicalCommandId, ParticipantCommandPhase, participant_logical_command_id},
};

use crate::{
    ParticipantDispatchError,
    coordinator::LogicalMutationId,
    status::{TransactionStatusLocation, TransactionStatusReader, TransactionStatusRouteResolver},
};

/// A validated terminal command that can resolve one participant intent.
///
/// The logical command identity is derived only from the transaction, phase,
/// and row key. It remains stable if the participant moves to another tablet,
/// Raft group, or epoch; the dispatcher is responsible for deriving the
/// current transport route from that identity.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolveIntentPlan {
    /// The logical row key whose lock is being resolved.
    pub key: Vec<u8>,

    /// The durable participant command to submit through Raft.
    pub command: ResolveIntentCommand,

    /// The topology-independent identity used for retry deduplication.
    pub logical_command_id: LogicalCommandId,
}

/// Result of validating the authoritative status for one visible intent.
#[derive(Debug, Clone, PartialEq)]
pub enum IntentResolutionDecision {
    /// The transaction is still active. No terminal command may be issued.
    Pending { status: TxnStatusRecord },

    /// The status is terminal and the returned command may be durably applied.
    Resolve(ResolveIntentPlan),
}

/// Build a terminal resolution command from one lock and its authoritative
/// transaction status.
///
/// A status mismatch is treated as corruption rather than as an abort. The
/// safe failure mode is to leave the intent in place until the status lookup
/// path is repaired; guessing an abort could destroy a transaction that has
/// already crossed its durable commit point.
pub fn plan_intent_resolution(
    key: &[u8],
    lock: &LockRecord,
    status: &TxnStatusRecord,
) -> Result<IntentResolutionDecision> {
    lock.validate()
        .map_err(|error| Error::CorruptData(format!("invalid intent lock: {error}")))?;
    LogicalMutationId::from_key(key)?;
    LogicalMutationId::from_key(&lock.primary_key).map_err(|error| {
        Error::CorruptData(format!("intent lock has an invalid primary key: {error}"))
    })?;
    status
        .validate()
        .map_err(|error| Error::CorruptData(format!("invalid transaction status: {error}")))?;

    if status.txn_id != lock.txn_id {
        return Err(Error::CorruptData(format!(
            "intent transaction ID {} does not match status transaction ID {}",
            lock.txn_id.0, status.txn_id.0
        )));
    }
    if status.start_timestamp != lock.start_timestamp {
        return Err(Error::CorruptData(format!(
            "intent start timestamp {} does not match status start timestamp {}",
            lock.start_timestamp.0, status.start_timestamp.0
        )));
    }
    if status.primary_key != lock.primary_key {
        return Err(Error::CorruptData(
            "intent primary key does not match the transaction status primary key".to_string(),
        ));
    }

    if status.status == TxnStatus::Pending {
        return Ok(IntentResolutionDecision::Pending {
            status: status.clone(),
        });
    }

    let command = ResolveIntentCommand {
        txn_id: lock.txn_id,
        start_timestamp: lock.start_timestamp,
        keys: vec![key.to_vec()],
        resolved_status: status.status,
        commit_timestamp: status.commit_timestamp,
    };
    command
        .to_proto()
        .map_err(|error| Error::InvalidArgument(format!("invalid intent resolution: {error}")))?;

    let logical_command_id =
        participant_logical_command_id(lock.txn_id, ParticipantCommandPhase::ResolveIntent, key)
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;

    Ok(IntentResolutionDecision::Resolve(ResolveIntentPlan {
        key: key.to_vec(),
        command,
        logical_command_id,
    }))
}

/// Dispatch boundary for a terminal intent-resolution command.
///
/// Implementations must return success only after the command has crossed the
/// normal tablet Raft durability and apply boundary. A read path must not call
/// the MVCC mutation methods directly because that would create state on one
/// replica without a replicated log entry.
pub trait IntentResolutionDispatcher {
    type Output;

    fn dispatch_intent_resolution(
        &mut self,
        plan: &ResolveIntentPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError>;
}

/// Outcome of a status lookup and, for terminal states, durable resolution.
#[derive(Debug, Clone, PartialEq)]
pub enum IntentResolutionOutcome<O> {
    /// The authoritative status is still pending, so no resolution command was
    /// submitted and the reader must apply its pending-intent policy.
    Pending { status: TxnStatusRecord },

    /// The terminal command was durably dispatched by the supplied boundary.
    Resolved { plan: ResolveIntentPlan, output: O },
}

/// Look up status, validate it against the lock, and dispatch a terminal
/// resolution through the caller's replicated participant boundary.
pub fn resolve_intent_with_status_lookup<R, L, D>(
    key: &[u8],
    lock: &LockRecord,
    status_location: &mut TransactionStatusLocation,
    route_resolver: &mut R,
    status_reader: &mut L,
    dispatcher: &mut D,
    max_route_refreshes: usize,
) -> Result<IntentResolutionOutcome<D::Output>>
where
    R: TransactionStatusRouteResolver,
    L: TransactionStatusReader<Output = TxnStatusRecord>,
    D: IntentResolutionDispatcher,
{
    if status_location.txn_id() != lock.txn_id || status_location.primary_key() != lock.primary_key
    {
        return Err(Error::CorruptData(
            "status lookup location does not identify the intent owner".to_string(),
        ));
    }

    let status = status_location
        .lookup_with_retry(route_resolver, status_reader, max_route_refreshes)?
        .ok_or_else(|| Error::TabletUnavailable {
            reason: format!(
                "transaction status for intent owner {} is not currently visible",
                lock.txn_id.0
            ),
        })?;

    match plan_intent_resolution(key, lock, &status)? {
        IntentResolutionDecision::Pending { status } => {
            Ok(IntentResolutionOutcome::Pending { status })
        }
        IntentResolutionDecision::Resolve(plan) => {
            let output = dispatcher
                .dispatch_intent_resolution(&plan)
                .map_err(map_dispatch_error)?;
            Ok(IntentResolutionOutcome::Resolved { plan, output })
        }
    }
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
