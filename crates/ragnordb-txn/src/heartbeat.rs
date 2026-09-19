//! Durable heartbeat planning for active distributed transactions.
//!
//! A heartbeat is a status-tablet mutation, not a local timer update. This
//! module validates the pending transaction identity, computes a fenced
//! wall-clock lease deadline, and hands the resulting status record to the
//! caller's normal replicated status boundary. It never mutates a local status
//! store or participant lock directly.

use ragnordb_common::{
    Error, Result,
    codec::{TxnStatus, TxnStatusRecord},
    ids::Timestamp,
};

use crate::{
    ParticipantDispatchError,
    coordinator::ParticipantRoute,
    status::{TransactionStatusKey, TransactionStatusLocation},
};

/// Heartbeat timing contract for one distributed transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionHeartbeatPolicy {
    /// TTL carried by participant locks.
    pub lock_ttl_ms: u64,

    /// Coordinator heartbeat cadence. It must be shorter than the lock TTL.
    pub heartbeat_interval_ms: u64,

    /// Duration granted to the transaction after a successful heartbeat.
    pub lease_duration_ms: u64,
}

impl TransactionHeartbeatPolicy {
    pub fn new(
        lock_ttl_ms: u64,
        heartbeat_interval_ms: u64,
        lease_duration_ms: u64,
    ) -> Result<Self> {
        if lock_ttl_ms == 0 || heartbeat_interval_ms == 0 || lease_duration_ms == 0 {
            return Err(Error::InvalidArgument(
                "heartbeat timing values must be non-zero".to_string(),
            ));
        }
        if heartbeat_interval_ms >= lock_ttl_ms {
            return Err(Error::InvalidArgument(
                "heartbeat interval must be shorter than lock TTL".to_string(),
            ));
        }
        if heartbeat_interval_ms >= lease_duration_ms {
            return Err(Error::InvalidArgument(
                "heartbeat interval must be shorter than lease duration".to_string(),
            ));
        }

        Ok(Self {
            lock_ttl_ms,
            heartbeat_interval_ms,
            lease_duration_ms,
        })
    }

    fn lease_deadline_ms(self, now_ms: u64) -> Result<u64> {
        now_ms.checked_add(self.lease_duration_ms).ok_or_else(|| {
            Error::InvalidArgument("heartbeat lease deadline overflowed u64".to_string())
        })
    }
}

/// Validated status-tablet update produced by a heartbeat.
#[derive(Debug, Clone, PartialEq)]
pub struct TransactionHeartbeatPlan {
    /// Deterministic durable status key for the transaction.
    pub status_key: TransactionStatusKey,

    /// Current physical route for the status tablet. It is a transport hint;
    /// route refresh remains the dispatcher's responsibility.
    pub status_route: ParticipantRoute,

    /// Status record after the heartbeat has been durably applied.
    pub next_status: TxnStatusRecord,

    /// Logical heartbeat timestamp allocated by the timestamp authority.
    pub heartbeat_timestamp: Timestamp,

    /// Wall-clock deadline written into `next_status`.
    pub lease_deadline_ms: u64,
}

/// Result of comparing a heartbeat with the status already visible at the
/// authoritative status tablet.
#[derive(Debug, Clone, PartialEq)]
pub enum HeartbeatDecision {
    /// The status tablet already contains this exact heartbeat update.
    AlreadyApplied { status: TxnStatusRecord },

    /// The caller may submit this pending-status update through Raft.
    Renew(TransactionHeartbeatPlan),
}

/// Result of crossing the durable status dispatch boundary.
#[derive(Debug)]
pub enum TransactionHeartbeatOutcome<O> {
    /// The heartbeat status update was durably dispatched and applied.
    Renewed {
        plan: TransactionHeartbeatPlan,
        output: O,
    },

    /// A retry observed the same heartbeat already durably applied.
    AlreadyApplied { status: TxnStatusRecord },
}

/// Dispatch boundary for a heartbeat status update.
pub trait TransactionHeartbeatDispatcher {
    type Output;

    /// Return success only after the status record crossed the normal Raft
    /// durability and apply boundary. A successful local write is not enough.
    fn dispatch_transaction_heartbeat(
        &mut self,
        plan: &TransactionHeartbeatPlan,
    ) -> std::result::Result<Self::Output, ParticipantDispatchError>;
}

/// Build a fenced heartbeat update for a pending transaction status.
pub fn plan_transaction_heartbeat(
    status_location: &TransactionStatusLocation,
    current_status: &TxnStatusRecord,
    heartbeat_timestamp: Timestamp,
    now_ms: u64,
    policy: TransactionHeartbeatPolicy,
) -> Result<HeartbeatDecision> {
    current_status
        .validate()
        .map_err(|error| Error::CorruptData(format!("invalid transaction status: {error}")))?;

    if status_location.txn_id() != current_status.txn_id
        || status_location.primary_key() != current_status.primary_key
    {
        return Err(Error::CorruptData(
            "heartbeat status location does not identify the transaction owner".to_string(),
        ));
    }

    if current_status.status != TxnStatus::Pending {
        return Err(Error::WriteConflict(
            "terminal transaction status cannot receive a heartbeat".to_string(),
        ));
    }

    if heartbeat_timestamp.0 == 0 {
        return Err(Error::InvalidArgument(
            "heartbeat timestamp must be non-zero".to_string(),
        ));
    }
    if heartbeat_timestamp <= current_status.start_timestamp {
        return Err(Error::InvalidArgument(
            "heartbeat timestamp must be newer than the transaction start timestamp".to_string(),
        ));
    }

    let previous_heartbeat = current_status
        .last_heartbeat_timestamp
        .unwrap_or(current_status.start_timestamp);
    let proposed_deadline = policy.lease_deadline_ms(now_ms)?;

    if heartbeat_timestamp < previous_heartbeat {
        return Err(Error::WriteConflict(
            "heartbeat timestamp is older than the durable heartbeat".to_string(),
        ));
    }

    if heartbeat_timestamp == previous_heartbeat {
        if current_status.lease_deadline_ms == Some(proposed_deadline) {
            return Ok(HeartbeatDecision::AlreadyApplied {
                status: current_status.clone(),
            });
        }
        return Err(Error::WriteConflict(
            "heartbeat replay carries a different lease deadline".to_string(),
        ));
    }

    if current_status
        .lease_deadline_ms
        .is_some_and(|deadline| now_ms >= deadline)
    {
        return Err(Error::WriteConflict(
            "heartbeat arrived after the durable transaction lease expired".to_string(),
        ));
    }

    let lease_deadline_ms = current_status
        .lease_deadline_ms
        .unwrap_or_default()
        .max(proposed_deadline);
    let mut next_status = current_status.clone();
    next_status.last_heartbeat_timestamp = Some(heartbeat_timestamp);
    next_status.lease_deadline_ms = Some(lease_deadline_ms);
    next_status
        .validate()
        .map_err(|error| Error::InvalidArgument(format!("invalid heartbeat status: {error}")))?;

    let status_route = status_location.route().ok_or_else(|| {
        Error::InvalidArgument("heartbeat planning requires a current status route".to_string())
    })?;

    Ok(HeartbeatDecision::Renew(TransactionHeartbeatPlan {
        status_key: status_location.status_key().clone(),
        status_route,
        next_status,
        heartbeat_timestamp,
        lease_deadline_ms,
    }))
}

/// Plan and dispatch one heartbeat through the caller's durable status
/// boundary. Replaying an already-applied heartbeat is a successful no-op.
pub fn heartbeat_with_status<D>(
    status_location: &TransactionStatusLocation,
    current_status: &TxnStatusRecord,
    heartbeat_timestamp: Timestamp,
    now_ms: u64,
    policy: TransactionHeartbeatPolicy,
    dispatcher: &mut D,
) -> Result<TransactionHeartbeatOutcome<D::Output>>
where
    D: TransactionHeartbeatDispatcher,
{
    match plan_transaction_heartbeat(
        status_location,
        current_status,
        heartbeat_timestamp,
        now_ms,
        policy,
    )? {
        HeartbeatDecision::AlreadyApplied { status } => {
            Ok(TransactionHeartbeatOutcome::AlreadyApplied { status })
        }
        HeartbeatDecision::Renew(plan) => {
            let output = dispatcher
                .dispatch_transaction_heartbeat(&plan)
                .map_err(map_dispatch_error)?;
            Ok(TransactionHeartbeatOutcome::Renewed { plan, output })
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
