//! transaction local state
//!
//! a transaction owns a stable MVCC start timestamp and a deterministic,
//! ordered write set. reads use `start_ts` for snapshot visibility while
//! the tablet layer checks this write set first to provide
//! read-your-writes
//!
//! Distributed phase dispatch, commit planning, rollback planning and
//! execution, transaction-status placement, and bounded intent resolution are
//! implemented at their current milestone boundaries. Heartbeat publication
//! and background scanning remain separate lifecycle responsibilities.
mod commit;
mod coordinator;
mod heartbeat;
mod manager;
mod prewrite;
mod resolve;
mod rollback;
mod status;

pub use commit::{CommitBatchPlan, CommitExecutionOutcome, CommitPhasePlan};
pub use coordinator::{
    CommitPhaseDispatcher, DistributedTransactionCoordinator, LogicalMutationId,
    OwnedMvccParticipant, ParticipantCommandId, ParticipantCommandPlan, ParticipantDispatchError,
    ParticipantPhaseDispatcher, ParticipantRoute, ParticipantRouteRefresher,
    PrewriteBatchDispatcher, RollbackPhaseDispatcher, SingleNodeCommitCoordinator,
    SingleNodeCommitOutcome, SingleNodeCommitParticipant,
};
pub use heartbeat::{
    HeartbeatDecision, TransactionHeartbeatDispatcher, TransactionHeartbeatOutcome,
    TransactionHeartbeatPlan, TransactionHeartbeatPolicy, heartbeat_with_status,
    plan_transaction_heartbeat,
};
pub use manager::{
    CommitTimestampAllocator, ConcurrentReservedTimestampTransactionManager,
    ConcurrentTimestampOracle, LocalTransactionManager, ReservedTimestampTransactionManager,
    SharedTransactionManager, SharedTransactionManagerHandle, TimestampOracle,
    TimestampOracleStats, TimestampReservation, TimestampReservationProvider, TransactionManager,
};
pub use prewrite::PrewriteBatchPlan;
pub use resolve::{
    AuthoritativeTransactionLease, IntentResolutionDecision, IntentResolutionDispatcher,
    IntentResolutionLeasePolicy, IntentResolutionOutcome, PendingIntentDecision, ResolveIntentPlan,
    classify_pending_intent, pending_intent_retry_after_ms, plan_intent_resolution,
    resolve_intent_with_status_lookup, resolve_intent_with_status_lookup_and_lease,
};
pub use rollback::{RollbackBatchPlan, RollbackExecutionOutcome, RollbackPhasePlan};
pub use status::{
    InMemoryTransactionStatusStore, TransactionStatusKey, TransactionStatusLocation,
    TransactionStatusLookupError, TransactionStatusReader, TransactionStatusRouteResolver,
    TransactionStatusStore,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use ragnordb_common::{
    Error, Result,
    encoding::decode_row,
    ids::{TableId, Timestamp, TxnId},
};
use ragnordb_storage::{key::decode_row_key, mvcc::Mutation};

/// Client-side state for one active transaction.
#[derive(Debug)]
pub struct Transaction {
    id: TxnId,
    start_ts: Timestamp,
    created_at: Instant,
    footprint_policy: TransactionFootprintPolicy,
    writes: BTreeMap<Vec<u8>, Mutation>,
    read_keys: BTreeSet<Vec<u8>>,
    read_spans: BTreeSet<TransactionReadSpan>,
}

/// Configurable limits for transaction-local memory and distributed prewrite
/// admission. `Transaction::new` uses conservative defaults; server code may
/// supply the node's configured policy with `Transaction::new_with_policy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionFootprintPolicy {
    pub max_age_ms: u64,
    pub max_write_bytes: usize,
    pub max_write_keys: usize,
    pub max_read_spans: usize,
    pub max_participant_tablets: usize,
    pub max_intent_accounting_bytes: usize,
    pub max_participant_command_bytes: usize,
}

impl Default for TransactionFootprintPolicy {
    fn default() -> Self {
        Self {
            max_age_ms: 60_000,
            max_write_bytes: 64 * 1024 * 1024,
            max_write_keys: 100_000,
            max_read_spans: 4_096,
            max_participant_tablets: 256,
            max_intent_accounting_bytes: 64 * 1024 * 1024,
            max_participant_command_bytes: 1024 * 1024,
        }
    }
}

impl TransactionFootprintPolicy {
    /// Validate the policy before a transaction starts retaining caller data.
    pub fn validate(self) -> Result<()> {
        if self.max_age_ms == 0
            || self.max_write_bytes == 0
            || self.max_write_keys == 0
            || self.max_read_spans == 0
            || self.max_participant_tablets == 0
            || self.max_intent_accounting_bytes == 0
            || self.max_participant_command_bytes == 0
        {
            return Err(Error::InvalidArgument(
                "transaction footprint limits must all be non-zero".to_string(),
            ));
        }
        Ok(())
    }
}

/// Bounded transaction diagnostics used by status surfaces and admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionFootprint {
    pub age_ms: u64,
    pub write_bytes: usize,
    pub write_keys: usize,
    pub read_spans: usize,
    pub participant_tablets: usize,
    pub intent_accounting_bytes: usize,
    pub participant_command_bytes: usize,
}

/// Logical allocator charge for the lock, transaction ownership, and MVCC
/// index/accounting entries retained for each unique buffered key.
const INTENT_ACCOUNTING_OVERHEAD_BYTES: usize = 64;

/// A table-scoped logical read interval retained for transaction accounting.
///
/// Bounds use the same canonical encoded row-key bytes as the tablet routing
/// layer. Recording this footprint does not add a new conflict or isolation
/// rule; it gives the coordinator stable logical input for later limits and
/// validation work.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionReadSpan {
    pub table_id: TableId,
    pub start_key: Option<Vec<u8>>,
    pub end_key: Option<Vec<u8>>,
}

impl TransactionReadSpan {
    pub fn new(
        table_id: TableId,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
    ) -> Result<Self> {
        if table_id.0 == 0 {
            return Err(Error::InvalidArgument(
                "transaction read span table ID 0 is reserved".to_string(),
            ));
        }
        for (name, key) in [("start", start_key.as_deref()), ("end", end_key.as_deref())] {
            if let Some(key) = key {
                let row_key = decode_row_key(key).map_err(|error| {
                    Error::InvalidArgument(format!(
                        "transaction read span {name} bound is not a canonical row key: {error}"
                    ))
                })?;
                if row_key.table_id != table_id {
                    return Err(Error::InvalidArgument(format!(
                        "transaction read span {name} bound belongs to table {}, expected {}",
                        row_key.table_id.0, table_id.0
                    )));
                }
            }
        }
        if let (Some(start), Some(end)) = (&start_key, &end_key)
            && start >= end
        {
            return Err(Error::InvalidArgument(
                "transaction read span must be a non-empty half-open interval".to_string(),
            ));
        }

        Ok(Self {
            table_id,
            start_key,
            end_key,
        })
    }
}

impl Transaction {
    /// Start an empty transaction at a timestamp allocated by the timestamp
    /// authority.
    pub fn new(id: TxnId, start_ts: Timestamp) -> Result<Self> {
        Self::new_with_policy(id, start_ts, TransactionFootprintPolicy::default())
    }

    /// Start an empty transaction with caller-supplied resource limits.
    pub fn new_with_policy(
        id: TxnId,
        start_ts: Timestamp,
        footprint_policy: TransactionFootprintPolicy,
    ) -> Result<Self> {
        if id.0 == 0 {
            return Err(Error::InvalidArgument(
                "transaction ID 0 is reserved".to_string(),
            ));
        }

        if start_ts.0 == 0 {
            return Err(Error::InvalidArgument(
                "transaction start timestamp 0 is reserved".to_string(),
            ));
        }

        footprint_policy.validate()?;

        Ok(Self {
            id,
            start_ts,
            created_at: Instant::now(),
            footprint_policy,
            writes: BTreeMap::new(),
            read_keys: BTreeSet::new(),
            read_spans: BTreeSet::new(),
        })
    }

    /// Return the limits that govern this transaction.
    pub fn footprint_policy(&self) -> TransactionFootprintPolicy {
        self.footprint_policy
    }

    /// Install a server-configured policy before this transaction starts
    /// collecting reads or writes. Late replacement is rejected so smaller
    /// limits cannot be applied retroactively to already-admitted state.
    pub fn set_footprint_policy(&mut self, policy: TransactionFootprintPolicy) -> Result<()> {
        policy.validate()?;
        if !self.writes.is_empty() || !self.read_keys.is_empty() || !self.read_spans.is_empty() {
            return Err(Error::InvalidArgument(
                "transaction footprint policy must be installed before reads or writes".to_string(),
            ));
        }
        self.footprint_policy = policy;
        Ok(())
    }

    /// Return the transaction's current bounded resource accounting.
    pub fn footprint(&self) -> TransactionFootprint {
        let (write_bytes, intent_accounting_bytes) = write_footprint(&self.writes);
        TransactionFootprint {
            age_ms: self.created_at.elapsed().as_millis().min(u64::MAX as u128) as u64,
            write_bytes,
            write_keys: self.writes.len(),
            read_spans: self.read_keys.len().saturating_add(self.read_spans.len()),
            participant_tablets: 0,
            intent_accounting_bytes,
            participant_command_bytes: 0,
        }
    }

    /// Return the globally unique transaction identifier.
    pub fn id(&self) -> TxnId {
        self.id
    }

    /// Return the snapshot timestamp used by reads.
    pub fn start_ts(&self) -> Timestamp {
        self.start_ts
    }

    /// Return the pending mutation for one canonical encoded row key.
    pub fn pending_write(&self, key: &[u8]) -> Option<&Mutation> {
        self.writes.get(key)
    }

    /// Return the complete deterministic write set.
    pub fn write_set(&self) -> &BTreeMap<Vec<u8>, Mutation> {
        &self.writes
    }

    /// Return the canonical logical point keys read by this transaction.
    pub fn read_keys(&self) -> &BTreeSet<Vec<u8>> {
        &self.read_keys
    }

    /// Return table-scoped logical read spans retained for coordinator
    /// bookkeeping and later footprint limits.
    pub fn read_spans(&self) -> &BTreeSet<TransactionReadSpan> {
        &self.read_spans
    }

    /// Record one logical point read exactly once.
    pub fn record_read(&mut self, key: Vec<u8>) -> Result<()> {
        self.validate_age()?;
        decode_row_key(&key).map_err(|error| {
            Error::InvalidArgument(format!(
                "transaction read key is not a canonical encoded row key: {error}"
            ))
        })?;
        let is_new = !self.read_keys.contains(&key);
        let current_reads = self.read_keys.len().saturating_add(self.read_spans.len());
        if is_new && current_reads >= self.footprint_policy.max_read_spans {
            return Err(footprint_limit_error(
                "read spans",
                self.footprint_policy.max_read_spans,
                current_reads.saturating_add(1),
            ));
        }
        self.read_keys.insert(key);
        Ok(())
    }

    /// Record one logical range read exactly once, subject to the configured
    /// span and transaction-age limits.
    pub fn record_read_span(&mut self, span: TransactionReadSpan) -> Result<()> {
        self.validate_age()?;
        let is_new = !self.read_spans.contains(&span);
        let current_reads = self.read_keys.len().saturating_add(self.read_spans.len());
        if is_new && current_reads >= self.footprint_policy.max_read_spans {
            return Err(footprint_limit_error(
                "read spans",
                self.footprint_policy.max_read_spans,
                current_reads.saturating_add(1),
            ));
        }
        self.read_spans.insert(span);
        Ok(())
    }

    /// Return the number of distinct rows modified by the transaction.
    pub fn len(&self) -> usize {
        self.writes.len()
    }

    /// Return whether the transaction has no buffered mutations.
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    /// Buffer an insert or update.
    ///
    /// Rewriting the same key replaces its previous pending mutation, so the
    /// write set always represents the transaction's final intended state.
    pub fn buffer_put(&mut self, key: Vec<u8>, row: Vec<u8>) -> Result<()> {
        let mutation = Mutation::Put(row);
        validate_buffered_mutation(&key, &mutation)?;
        self.check_projected_writes(std::iter::once((&key, &mutation)))?;
        self.writes.insert(key, mutation);
        Ok(())
    }

    /// Buffer a row deletion.
    pub fn buffer_delete(&mut self, key: Vec<u8>) -> Result<()> {
        let mutation = Mutation::Delete;
        validate_buffered_mutation(&key, &mutation)?;
        self.check_projected_writes(std::iter::once((&key, &mutation)))?;
        self.writes.insert(key, mutation);
        Ok(())
    }

    /// Atomically merge one validated mutation batch into the write set.
    ///
    /// Every key and row is validated before the transaction is modified. If
    /// validation fails, both mutations from earlier statements and the write
    /// set visible before this call remain unchanged. A mutation for a key that
    /// was already buffered by an earlier statement replaces that mutation,
    /// preserving the transaction's final-write-wins semantics.
    pub fn buffer_batch(&mut self, writes: BTreeMap<Vec<u8>, Mutation>) -> Result<()> {
        for (key, mutation) in &writes {
            validate_buffered_mutation(key, mutation)?;
        }

        self.check_projected_writes(writes.iter())?;

        self.writes.extend(writes);
        Ok(())
    }

    pub fn validate_age(&self) -> Result<()> {
        self.validate_age_at(std::time::Instant::now())
    }

    fn validate_age_at(&self, now: std::time::Instant) -> Result<()> {
        let elapsed = now.saturating_duration_since(self.created_at);
        if elapsed > std::time::Duration::from_millis(self.footprint_policy.max_age_ms) {
            let age_ms = elapsed.as_millis().min(u64::MAX as u128) as u64;
            return Err(footprint_limit_error(
                "age milliseconds",
                self.footprint_policy.max_age_ms as usize,
                age_ms.min(usize::MAX as u64) as usize,
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_distributed_footprint(
        &self,
        participant_tablets: usize,
        participant_command_bytes: usize,
        status_accounting_bytes: usize,
        check_age: bool,
    ) -> Result<TransactionFootprint> {
        if check_age {
            self.validate_age()?;
        }
        let mut footprint = self.footprint();
        footprint.participant_tablets = participant_tablets;
        footprint.participant_command_bytes = participant_command_bytes;
        footprint.intent_accounting_bytes = footprint
            .intent_accounting_bytes
            .saturating_add(status_accounting_bytes);
        check_limit(
            "write bytes",
            self.footprint_policy.max_write_bytes,
            footprint.write_bytes,
        )?;
        check_limit(
            "write keys",
            self.footprint_policy.max_write_keys,
            footprint.write_keys,
        )?;
        check_limit(
            "read spans",
            self.footprint_policy.max_read_spans,
            footprint.read_spans,
        )?;
        check_limit(
            "participant tablets",
            self.footprint_policy.max_participant_tablets,
            footprint.participant_tablets,
        )?;
        check_limit(
            "intent/accounting bytes",
            self.footprint_policy.max_intent_accounting_bytes,
            footprint.intent_accounting_bytes,
        )?;
        check_limit(
            "participant command bytes",
            self.footprint_policy.max_participant_command_bytes,
            footprint.participant_command_bytes,
        )?;
        Ok(footprint)
    }

    fn check_projected_writes<'a>(
        &self,
        additions: impl Iterator<Item = (&'a Vec<u8>, &'a Mutation)>,
    ) -> Result<()> {
        self.validate_age()?;
        let mut projected_bytes = self.footprint().write_bytes;
        let mut projected_keys = self.writes.len();
        let mut projected_intent_bytes = self.footprint().intent_accounting_bytes;
        for (key, mutation) in additions {
            if let Some(previous) = self.writes.get(key.as_slice()) {
                let (old_write_bytes, old_intent_bytes) = mutation_footprint(key, previous);
                projected_bytes = projected_bytes.saturating_sub(old_write_bytes);
                projected_intent_bytes = projected_intent_bytes.saturating_sub(old_intent_bytes);
            } else {
                projected_keys = projected_keys.saturating_add(1);
            }
            let (write_bytes, intent_bytes) = mutation_footprint(key, mutation);
            projected_bytes = projected_bytes.saturating_add(write_bytes);
            projected_intent_bytes = projected_intent_bytes.saturating_add(intent_bytes);
        }

        check_limit(
            "write bytes",
            self.footprint_policy.max_write_bytes,
            projected_bytes,
        )?;
        check_limit(
            "write keys",
            self.footprint_policy.max_write_keys,
            projected_keys,
        )?;
        check_limit(
            "intent/accounting bytes",
            self.footprint_policy.max_intent_accounting_bytes,
            projected_intent_bytes,
        )
    }

    /// Consume the transaction and return its complete write set.
    ///
    /// Commit consumes transactions so they cannot accidentally be reused
    /// after a successful or failed commit attempt.
    pub fn into_write_set(self) -> BTreeMap<Vec<u8>, Mutation> {
        self.writes
    }
}

fn validate_buffered_mutation(key: &[u8], mutation: &Mutation) -> Result<()> {
    decode_row_key(key).map(|_| ()).map_err(|error| {
        Error::InvalidArgument(format!(
            "transaction mutation key is not a canonical encoded row key: \
             {error}"
        ))
    })?;

    if let Mutation::Put(row) = mutation {
        decode_row(row).map_err(|error| {
            Error::InvalidArgument(format!(
                "transaction Put does not contain a canonical encoded row: \
                 {error}"
            ))
        })?;
    }

    Ok(())
}

fn write_footprint(writes: &BTreeMap<Vec<u8>, Mutation>) -> (usize, usize) {
    writes.iter().fold(
        (0usize, 0usize),
        |(write_bytes, intent_bytes), (key, mutation)| {
            let (payload_bytes, mutation_intent_bytes) = mutation_footprint(key, mutation);
            (
                write_bytes.saturating_add(payload_bytes),
                intent_bytes.saturating_add(mutation_intent_bytes),
            )
        },
    )
}

fn mutation_footprint(key: &[u8], mutation: &Mutation) -> (usize, usize) {
    let row_bytes = match mutation {
        Mutation::Put(row) => row.len(),
        Mutation::Delete => 0,
    };
    let payload_bytes = key.len().saturating_add(row_bytes);
    (
        payload_bytes,
        payload_bytes.saturating_add(INTENT_ACCOUNTING_OVERHEAD_BYTES),
    )
}

fn check_limit(name: &str, limit: usize, actual: usize) -> Result<()> {
    if actual > limit {
        return Err(footprint_limit_error(name, limit, actual));
    }
    Ok(())
}

fn footprint_limit_error(name: &str, limit: usize, actual: usize) -> Error {
    Error::InvalidArgument(format!(
        "transaction {name} footprint {actual} exceeds configured limit {limit}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ragnordb_common::{
        codec::{Row, Value},
        encoding::encode_row,
        ids::TableId,
    };
    use ragnordb_storage::key::{encode_row_key, make_row_key};

    fn encoded_key(id: i64) -> Vec<u8> {
        encode_row_key(&make_row_key(TableId(1), &[Value::Int(id)]).unwrap()).unwrap()
    }

    fn encoded_row(id: i64, name: &str) -> Vec<u8> {
        encode_row(&Row {
            values: vec![Value::Int(id), Value::Text(name.to_string())],
        })
        .unwrap()
    }

    #[test]
    fn transaction_requires_nonzero_identity_and_timestamp() {
        assert!(matches!(
            Transaction::new(TxnId(0), Timestamp(1),).unwrap_err(),
            Error::InvalidArgument(_)
        ));

        assert!(matches!(
            Transaction::new(TxnId(1), Timestamp(0),).unwrap_err(),
            Error::InvalidArgument(_)
        ));
    }

    #[test]
    fn latest_mutation_replaces_earlier_mutation() {
        let key = encoded_key(1);
        let row = encoded_row(1, "first");
        let mut transaction = Transaction::new(TxnId(1), Timestamp(1)).unwrap();

        transaction.buffer_put(key.clone(), row.clone()).unwrap();

        assert_eq!(transaction.pending_write(&key), Some(&Mutation::Put(row)));

        transaction.buffer_delete(key.clone()).unwrap();

        assert_eq!(transaction.pending_write(&key), Some(&Mutation::Delete));
        assert_eq!(transaction.len(), 1);
    }

    #[test]
    fn write_set_uses_encoded_key_order() {
        let first = encoded_key(1);
        let second = encoded_key(2);
        let mut transaction = Transaction::new(TxnId(1), Timestamp(1)).unwrap();

        transaction
            .buffer_put(second.clone(), encoded_row(2, "second"))
            .unwrap();

        transaction
            .buffer_put(first.clone(), encoded_row(1, "first"))
            .unwrap();

        let keys = transaction.write_set().keys().cloned().collect::<Vec<_>>();

        assert_eq!(keys, vec![first, second]);
    }

    #[test]
    fn malformed_rows_are_rejected_before_buffering() {
        let mut transaction = Transaction::new(TxnId(1), Timestamp(1)).unwrap();

        let error = transaction
            .buffer_put(encoded_key(1), vec![0xff])
            .unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert!(transaction.is_empty());
    }

    #[test]
    fn write_footprint_rejection_preserves_the_existing_write_set() {
        // This catches a real partial-buffering bug: a large later SQL batch
        // must not retain its early keys after a later key exceeds the cap.
        let existing_key = encoded_key(1);
        let existing_row = encoded_row(1, "existing");
        let valid_key = encoded_key(2);
        let valid_row = encoded_row(2, "valid");
        let oversized_key = encoded_key(3);
        let oversized_row = encoded_row(3, "this row pushes the batch over its byte cap");
        let existing_bytes = existing_key.len() + existing_row.len();
        let exact_addition_bytes = valid_key.len() + valid_row.len();
        let mut transaction = Transaction::new_with_policy(
            TxnId(1),
            Timestamp(1),
            TransactionFootprintPolicy {
                max_write_bytes: existing_bytes + exact_addition_bytes,
                ..TransactionFootprintPolicy::default()
            },
        )
        .unwrap();
        transaction
            .buffer_put(existing_key.clone(), existing_row.clone())
            .unwrap();
        transaction
            .buffer_put(valid_key.clone(), valid_row.clone())
            .unwrap();

        let mut batch = BTreeMap::new();
        batch.insert(valid_key.clone(), Mutation::Put(valid_row.clone()));
        batch.insert(oversized_key.clone(), Mutation::Put(oversized_row));

        let error = transaction.buffer_batch(batch).unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert_eq!(transaction.write_set().len(), 2);
        assert_eq!(
            transaction.pending_write(&existing_key),
            Some(&Mutation::Put(existing_row))
        );
        assert_eq!(
            transaction.pending_write(&valid_key),
            Some(&Mutation::Put(valid_row))
        );
        assert_eq!(transaction.pending_write(&oversized_key), None);
    }

    #[test]
    fn point_reads_share_the_bounded_read_span_budget() {
        // Point keys are retained for serializable conflict validation. They
        // must consume the configured read budget just like range spans or a
        // transaction can grow its read set without a limit.
        let mut transaction = Transaction::new_with_policy(
            TxnId(1),
            Timestamp(1),
            TransactionFootprintPolicy {
                max_read_spans: 1,
                ..TransactionFootprintPolicy::default()
            },
        )
        .unwrap();
        transaction.record_read(encoded_key(1)).unwrap();

        let error = transaction.record_read(encoded_key(2)).unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(message) if message.contains("read spans")));
        assert_eq!(transaction.footprint().read_spans, 1);
    }

    #[test]
    fn write_key_and_intent_accounting_caps_accept_the_boundary_and_reject_one_over() {
        // This catches transactions that fit a byte budget while exceeding
        // the separate per-intent accounting budget or distinct-key budget.
        let first_key = encoded_key(1);
        let first_row = encoded_row(1, "first");
        let second_key = encoded_key(2);
        let second_row = encoded_row(2, "second");
        let third_key = encoded_key(3);
        let third_row = encoded_row(3, "third");

        let mut key_limited = Transaction::new_with_policy(
            TxnId(1),
            Timestamp(1),
            TransactionFootprintPolicy {
                max_write_keys: 2,
                ..TransactionFootprintPolicy::default()
            },
        )
        .unwrap();
        key_limited
            .buffer_put(first_key.clone(), first_row.clone())
            .unwrap();
        key_limited
            .buffer_put(second_key.clone(), second_row.clone())
            .unwrap();
        assert!(
            key_limited
                .buffer_put(third_key.clone(), third_row.clone())
                .is_err()
        );
        assert_eq!(key_limited.write_set().len(), 2);

        let exact_intent_bytes = first_key
            .len()
            .saturating_add(first_row.len())
            .saturating_add(INTENT_ACCOUNTING_OVERHEAD_BYTES);
        let mut exact = Transaction::new_with_policy(
            TxnId(2),
            Timestamp(2),
            TransactionFootprintPolicy {
                max_intent_accounting_bytes: exact_intent_bytes,
                ..TransactionFootprintPolicy::default()
            },
        )
        .unwrap();
        exact
            .buffer_put(first_key.clone(), first_row.clone())
            .unwrap();
        assert_eq!(
            exact.footprint().intent_accounting_bytes,
            exact_intent_bytes
        );

        let mut one_over = Transaction::new_with_policy(
            TxnId(3),
            Timestamp(3),
            TransactionFootprintPolicy {
                max_intent_accounting_bytes: exact_intent_bytes - 1,
                ..TransactionFootprintPolicy::default()
            },
        )
        .unwrap();
        assert!(one_over.buffer_put(first_key, first_row).is_err());
        assert!(one_over.is_empty());
    }

    #[test]
    fn transaction_age_allows_the_exact_limit_and_rejects_one_millisecond_over() {
        // Exact-time injection avoids a sleep race and protects the public
        // boundary rule from an accidental `>=` change.
        let transaction = Transaction::new_with_policy(
            TxnId(1),
            Timestamp(1),
            TransactionFootprintPolicy {
                max_age_ms: 10,
                ..TransactionFootprintPolicy::default()
            },
        )
        .unwrap();
        let created_at = transaction.created_at;

        assert!(
            transaction
                .validate_age_at(created_at + std::time::Duration::from_millis(10))
                .is_ok()
        );
        assert!(
            transaction
                .validate_age_at(created_at + std::time::Duration::from_millis(11))
                .is_err()
        );
    }

    #[test]
    fn invalid_batch_is_atomic_and_preserves_earlier_writes() {
        let existing_key = encoded_key(1);
        let valid_batch_key = encoded_key(2);
        let invalid_batch_key = encoded_key(3);
        let existing_row = encoded_row(1, "existing");
        let mut transaction = Transaction::new(TxnId(1), Timestamp(1)).unwrap();

        transaction
            .buffer_put(existing_key.clone(), existing_row.clone())
            .unwrap();

        let mut batch = BTreeMap::new();
        batch.insert(
            valid_batch_key.clone(),
            Mutation::Put(encoded_row(2, "valid")),
        );
        batch.insert(invalid_batch_key.clone(), Mutation::Put(vec![0xff]));

        let error = transaction.buffer_batch(batch).unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert_eq!(
            transaction.pending_write(&existing_key),
            Some(&Mutation::Put(existing_row))
        );
        assert_eq!(transaction.pending_write(&valid_batch_key), None);
        assert_eq!(transaction.pending_write(&invalid_batch_key), None);
        assert_eq!(transaction.len(), 1);
    }
}
