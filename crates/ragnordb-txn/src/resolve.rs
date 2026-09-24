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

/// Fenced wall-clock lease information supplied by the transaction-status
/// authority.
///
/// MVCC timestamps order versions; they do not measure elapsed milliseconds.
/// The reader therefore accepts an expiry decision only when a status/lease
/// authority provides an explicit deadline. The heartbeat interval is a
/// bounded retry hint and is never used to infer that a transaction is dead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthoritativeTransactionLease {
    /// Wall-clock deadline after which the status authority may expire a lease.
    pub deadline_ms: u64,

    /// Maximum interval before a pending reader should check the authority
    /// again while the lease remains valid.
    pub heartbeat_interval_ms: u64,
}

impl AuthoritativeTransactionLease {
    pub fn new(deadline_ms: u64, heartbeat_interval_ms: u64) -> Result<Self> {
        if heartbeat_interval_ms == 0 {
            return Err(Error::InvalidArgument(
                "transaction heartbeat interval must be non-zero".to_string(),
            ));
        }

        Ok(Self {
            deadline_ms,
            heartbeat_interval_ms,
        })
    }

    fn retry_after_ms(self, now_ms: u64) -> u64 {
        self.deadline_ms
            .saturating_sub(now_ms)
            .min(self.heartbeat_interval_ms)
            .max(1)
    }
}

/// Inputs that keep one lease-aware status lookup bounded and deterministic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IntentResolutionLeasePolicy {
    /// Maximum number of route refreshes allowed during the status lookup.
    pub max_route_refreshes: usize,

    /// Fenced lease state returned by the status authority.
    pub lease: AuthoritativeTransactionLease,

    /// Wall-clock observation used only for comparing against `lease.deadline_ms`.
    pub now_ms: u64,
}

impl IntentResolutionLeasePolicy {
    pub fn new(
        max_route_refreshes: usize,
        lease: AuthoritativeTransactionLease,
        now_ms: u64,
    ) -> Result<Self> {
        if max_route_refreshes == 0 {
            return Err(Error::InvalidArgument(
                "transaction status lookup retry budget must be non-zero".to_string(),
            ));
        }

        Ok(Self {
            max_route_refreshes,
            lease,
            now_ms,
        })
    }
}

/// Action available while the authoritative transaction status remains
/// pending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingIntentDecision {
    /// The reader must not resolve the intent and should retry after this
    /// bounded interval.
    RetryableConflict { retry_after_ms: u64 },

    /// The fenced lease has elapsed, but the pending status still has to be
    /// transitioned to `Aborted` by the status authority before a participant
    /// may publish a rollback command.
    Expired { lease_deadline_ms: u64 },
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

/// Classify a pending intent using a lease decision supplied by the
/// transaction-status authority.
///
/// Expiry is deliberately an observation, not an implicit abort. A participant
/// cannot safely remove a lock merely because its local clock says the lock is
/// old: the coordinator may have renewed the lease, or a prior heartbeat may
/// already be durable on the status tablet. The caller must publish an
/// authoritative aborted status and then call `plan_intent_resolution` again.
pub fn classify_pending_intent(
    key: &[u8],
    lock: &LockRecord,
    status: &TxnStatusRecord,
    lease: AuthoritativeTransactionLease,
    now_ms: u64,
) -> Result<PendingIntentDecision> {
    match plan_intent_resolution(key, lock, status)? {
        IntentResolutionDecision::Pending { .. } if now_ms >= lease.deadline_ms => {
            Ok(PendingIntentDecision::Expired {
                lease_deadline_ms: lease.deadline_ms,
            })
        }
        IntentResolutionDecision::Pending { .. } => Ok(PendingIntentDecision::RetryableConflict {
            retry_after_ms: lease.retry_after_ms(now_ms),
        }),
        IntentResolutionDecision::Resolve(_) => Err(Error::InvalidArgument(
            "pending intent classification requires a pending transaction status".to_string(),
        )),
    }
}

/// Return the recommended read retry interval for a lock that is still
/// pending. This is only a scheduling hint; it is not an expiry decision.
pub fn pending_intent_retry_after_ms(lock: &LockRecord) -> Result<u64> {
    lock.validate()
        .map_err(|error| Error::CorruptData(format!("invalid intent lock: {error}")))?;
    Ok((lock.ttl_ms / 3).max(1))
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
    /// submitted. Callers using the lease-aware entry point receive the more
    /// specific retryable or expired outcome below.
    Pending { status: TxnStatusRecord },

    /// The status remains pending and the caller should retry the authoritative
    /// status lookup after the returned bounded interval.
    RetryableConflict {
        status: TxnStatusRecord,
        retry_after_ms: u64,
    },

    /// The authoritative lease deadline elapsed while the status still said
    /// pending. No participant command was submitted; status ownership must
    /// publish an aborted record before rollback is allowed.
    Expired {
        status: TxnStatusRecord,
        lease_deadline_ms: u64,
    },

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
    let status = lookup_intent_status(
        lock,
        status_location,
        route_resolver,
        status_reader,
        max_route_refreshes,
    )?;

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

/// Look up an intent owner and apply the bounded pending/expiry policy using a
/// fenced lease supplied by the status authority.
pub fn resolve_intent_with_status_lookup_and_lease<R, L, D>(
    key: &[u8],
    lock: &LockRecord,
    status_location: &mut TransactionStatusLocation,
    route_resolver: &mut R,
    status_reader: &mut L,
    dispatcher: &mut D,
    policy: IntentResolutionLeasePolicy,
) -> Result<IntentResolutionOutcome<D::Output>>
where
    R: TransactionStatusRouteResolver,
    L: TransactionStatusReader<Output = TxnStatusRecord>,
    D: IntentResolutionDispatcher,
{
    let status = lookup_intent_status(
        lock,
        status_location,
        route_resolver,
        status_reader,
        policy.max_route_refreshes,
    )?;

    match plan_intent_resolution(key, lock, &status)? {
        IntentResolutionDecision::Pending { status } => {
            match classify_pending_intent(key, lock, &status, policy.lease, policy.now_ms)? {
                PendingIntentDecision::RetryableConflict { retry_after_ms } => {
                    Ok(IntentResolutionOutcome::RetryableConflict {
                        status,
                        retry_after_ms,
                    })
                }
                PendingIntentDecision::Expired { lease_deadline_ms } => {
                    Ok(IntentResolutionOutcome::Expired {
                        status,
                        lease_deadline_ms,
                    })
                }
            }
        }
        IntentResolutionDecision::Resolve(plan) => {
            let output = dispatcher
                .dispatch_intent_resolution(&plan)
                .map_err(map_dispatch_error)?;
            Ok(IntentResolutionOutcome::Resolved { plan, output })
        }
    }
}

fn lookup_intent_status<R, L>(
    lock: &LockRecord,
    status_location: &mut TransactionStatusLocation,
    route_resolver: &mut R,
    status_reader: &mut L,
    max_route_refreshes: usize,
) -> Result<TxnStatusRecord>
where
    R: TransactionStatusRouteResolver,
    L: TransactionStatusReader<Output = TxnStatusRecord>,
{
    if status_location.txn_id() != lock.txn_id || status_location.primary_key() != lock.primary_key
    {
        return Err(Error::CorruptData(
            "status lookup location does not identify the intent owner".to_string(),
        ));
    }

    status_location
        .lookup_with_retry(route_resolver, status_reader, max_route_refreshes)?
        .ok_or_else(|| Error::TabletUnavailable {
            reason: format!(
                "transaction status for intent owner {} is not currently visible",
                lock.txn_id.0
            ),
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
