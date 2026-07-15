//! local transaction identity and timestamp allocation
//!
//! session code depends on the `TransctionManager` boundary instead of
//! allocating transction metadata itself. we currently use the in-memory
//! implementation; later metadata raft timestamp service can implement
//! the same boundary without changing session transaction semantics

use crate::Transaction;
use ragnordb_common::{
    Error, Result,
    ids::{Timestamp, TxnId},
};

/// Allocates transaction identities and MVCC timestamps.
///
/// Implementations must guarantee:
///
/// - transaction IDs are nonzero and never reused,
/// - start timestamps are nonzero and monotonically increasing,
/// - commit timestamps are greater than their transaction's start timestamp.
pub trait TransactionManager {
    /// Begin a transaction with a newly allocated identity and start timestamp.
    fn begin_transaction(&mut self) -> Result<Transaction>;

    /// Allocate a commit timestamp strictly greater than `start_ts`.
    fn allocate_commit_timestamp(&mut self, start_ts: Timestamp) -> Result<Timestamp>;
}

/// In-memory transaction manager for local Milestone 2 execution.
///
/// Allocation begins at one because zero is reserved by the shared ID and
/// timestamp contracts. This state is intentionally not durable; durable,
/// replicated timestamp allocation belongs to the metadata Raft group in a
/// later milestone.
#[derive(Debug, Default)]
pub struct LocalTransactionManager {
    last_transaction_id: u64,
    last_timestamp: u64,
}

impl LocalTransactionManager {
    /// Construct an empty local allocator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the most recently allocated transaction identifier.
    ///
    /// Zero means no transaction has been allocated yet.
    pub fn last_allocated_transaction_id(&self) -> TxnId {
        TxnId(self.last_transaction_id)
    }

    /// Return the most recently allocated MVCC timestamp.
    ///
    /// Zero means no timestamp has been allocated yet.
    pub fn last_allocated_timestamp(&self) -> Timestamp {
        Timestamp(self.last_timestamp)
    }

    fn allocate_transaction_id(&mut self) -> Result<TxnId> {
        let next = self.last_transaction_id.checked_add(1).ok_or_else(|| {
            Error::Configuration(
                "local transaction ID allocator has exhausted the u64 ID space".to_string(),
            )
        })?;

        self.last_transaction_id = next;
        Ok(TxnId(next))
    }

    fn allocate_timestamp_after(&mut self, minimum_exclusive: Timestamp) -> Result<Timestamp> {
        let allocation_floor = self.last_timestamp.max(minimum_exclusive.0);

        let next = allocation_floor.checked_add(1).ok_or_else(|| {
            Error::Configuration(
                "local timestamp allocator has exhausted the u64 timestamp space".to_string(),
            )
        })?;

        self.last_timestamp = next;
        Ok(Timestamp(next))
    }
}

impl TransactionManager for LocalTransactionManager {
    fn begin_transaction(&mut self) -> Result<Transaction> {
        let transaction_id = self.allocate_transaction_id()?;
        let start_ts = self.allocate_timestamp_after(Timestamp(0))?;

        Transaction::new(transaction_id, start_ts)
    }

    fn allocate_commit_timestamp(&mut self, start_ts: Timestamp) -> Result<Timestamp> {
        self.allocate_timestamp_after(start_ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_manager_allocates_monotonic_transaction_metadata() {
        let mut manager = LocalTransactionManager::new();

        let first = manager.begin_transaction().unwrap();

        assert_eq!(first.id(), TxnId(1));
        assert_eq!(first.start_ts(), Timestamp(1));

        let first_commit = manager.allocate_commit_timestamp(first.start_ts()).unwrap();

        assert_eq!(first_commit, Timestamp(2));

        let second = manager.begin_transaction().unwrap();

        assert_eq!(second.id(), TxnId(2));
        assert_eq!(second.start_ts(), Timestamp(3));
    }

    #[test]
    fn commit_timestamp_is_always_newer_than_start_timestamp() {
        let mut manager = LocalTransactionManager::new();

        let commit_ts = manager.allocate_commit_timestamp(Timestamp(100)).unwrap();

        assert_eq!(commit_ts, Timestamp(101));
        assert!(commit_ts > Timestamp(100));
    }

    #[test]
    fn allocator_diagnostics_report_current_state() {
        let mut manager = LocalTransactionManager::new();

        assert_eq!(manager.last_allocated_transaction_id(), TxnId(0));
        assert_eq!(manager.last_allocated_timestamp(), Timestamp(0));

        let transaction = manager.begin_transaction().unwrap();

        assert_eq!(manager.last_allocated_transaction_id(), transaction.id());
        assert_eq!(manager.last_allocated_timestamp(), transaction.start_ts());
    }

    #[test]
    fn timestamp_exhaustion_returns_an_error_without_wrapping() {
        let mut manager = LocalTransactionManager::new();

        let error = manager
            .allocate_commit_timestamp(Timestamp(u64::MAX))
            .unwrap_err();

        assert!(matches!(error, Error::Configuration(_)));
        assert_eq!(manager.last_allocated_timestamp(), Timestamp(0));
    }
}
