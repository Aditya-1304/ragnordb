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
    CommitTimestampAllocator, LocalTransactionManager, ReservedTimestampTransactionManager,
    TimestampOracle, TimestampOracleStats, TimestampReservation, TimestampReservationProvider,
    TransactionManager,
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

use std::collections::{BTreeMap, BTreeSet};

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
    writes: BTreeMap<Vec<u8>, Mutation>,
    read_keys: BTreeSet<Vec<u8>>,
    read_spans: BTreeSet<TransactionReadSpan>,
}

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

        Ok(Self {
            id,
            start_ts,
            writes: BTreeMap::new(),
            read_keys: BTreeSet::new(),
            read_spans: BTreeSet::new(),
        })
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
        decode_row_key(&key).map_err(|error| {
            Error::InvalidArgument(format!(
                "transaction read key is not a canonical encoded row key: {error}"
            ))
        })?;
        self.read_keys.insert(key);
        Ok(())
    }

    /// Record one logical range read exactly once.
    pub fn record_read_span(&mut self, span: TransactionReadSpan) {
        self.read_spans.insert(span);
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
        self.writes.insert(key, mutation);
        Ok(())
    }

    /// Buffer a row deletion.
    pub fn buffer_delete(&mut self, key: Vec<u8>) -> Result<()> {
        let mutation = Mutation::Delete;
        validate_buffered_mutation(&key, &mutation)?;
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

        self.writes.extend(writes);
        Ok(())
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
