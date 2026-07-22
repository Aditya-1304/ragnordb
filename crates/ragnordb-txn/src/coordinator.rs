//! ordered single-node durable commit coordination
//!
//! one coordinator owns one table's mutable MVCC store and semantic durable
//! commit log. Its mutable commit method forms the serialized correctness
//! boundary from complete preflight through durable WAL append and atomic MVCC
//! application

use std::collections::BTreeMap;

use ragnordb_common::{
    Error, Result,
    ids::{TableId, Timestamp, TxnId},
};
use ragnordb_storage::{
    key::decode_row_key,
    mvcc::{Mutation, MvccStorage},
    wal::{DurableCommitLog, DurableWalExtent, SingleNodeTxnCommit, WalMutation},
};

use crate::{Transaction, TransactionManager};

/// published outcome of one completed local commit operation
///
/// Write commits include both their visibility timestamp and exact durable WAL
/// extent. Read only commits contain neither because they do not enter the
/// durable write path
#[must_use = "commit outcomes contain the published transaction state"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SingleNodeCommitOutcome {
    /// Transaction whose state was consumed by this commit attempt
    pub transaction_id: TxnId,

    /// Final visibility timestamp, or `None` for a read-only transaction
    pub commit_timestamp: Option<Timestamp>,

    /// Number of mutations atomically installed in MVCC
    pub committed_writes: usize,

    /// Exact durable commit-record extent, or `None` for a read-only transaction
    pub wal_extent: Option<DurableWalExtent>,
}

/// Serialized writer for one table's local MVCC and WAL commit boundary
///
/// The coordinator is intentionally not cloneable and exposes only immutable
/// access to its storage. Requiring `&mut self` for `commit` prevents another
/// local writer from changing MVCC state between preflight and application.
/// A concurrent owner may place the complete coordinator behind one mutex or
/// actor without splitting the ordered operation
pub struct SingleNodeCommitCoordinator<S, W>
where
    S: MvccStorage,
    W: DurableCommitLog,
{
    table_id: TableId,
    storage: S,
    commit_log: W,
    recovery_required_reason: Option<String>,
}

impl<S, W> SingleNodeCommitCoordinator<S, W>
where
    S: MvccStorage,
    W: DurableCommitLog,
{
    /// coordinator for one nonzero table identity
    pub fn new(table_id: TableId, storage: S, commit_log: W) -> Result<Self> {
        if table_id.0 == 0 {
            return Err(Error::InvalidArgument(
                "commit coordinator table ID 0 is reserved".to_string(),
            ));
        }

        Ok(Self {
            table_id,
            storage,
            commit_log,
            recovery_required_reason: None,
        })
    }

    /// return the table exclusively owned by this coordinator
    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    /// borrow the MVCC store for reads and diagnostics
    ///
    /// mutable storage access is deliberately not exposed because it would let
    /// callers bypass the serialized preflight-through-apply boundary
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// return whether the local write path has been stopped for recovery
    pub fn requires_recovery(&self) -> bool {
        self.recovery_required_reason.is_some()
    }

    /// this commits one local transaction using the required durable ordering
    ///
    /// The operation performs:
    ///
    /// 1. transaction and table-ownership validation
    /// 2. complete mutation-free MVCC preflight
    /// 3. final commit timestamp allocation
    /// 4. semantic commit-record construction
    /// 5. synchronous durable-log append
    /// 6. atomic MVCC batch application
    /// 7. publication of the completed commit outcome
    ///
    /// the transaction is consumed on every result so an unknown outcome cannot
    /// accidentally reuse its write set
    pub fn commit<M>(
        &mut self,
        transaction: Transaction,
        transaction_manager: &mut M,
    ) -> Result<SingleNodeCommitOutcome>
    where
        M: TransactionManager,
    {
        self.validate_transaction_metadata(&transaction)?;

        // read only work does not interact with the stopped write path, allocate
        // a commit timestamp, append a WAL record, or mutate MVCC state
        if transaction.is_empty() {
            return Ok(SingleNodeCommitOutcome {
                transaction_id: transaction.id(),
                commit_timestamp: None,
                committed_writes: 0,
                wal_extent: None,
            });
        }

        self.ensure_write_path_available()?;

        // the coordinator owns the mutable storage, so this validated state
        // cannot be changed by another writer before the apply step below
        self.storage.validate_commit_batch(
            transaction.id(),
            transaction.start_ts(),
            transaction.write_set(),
        )?;

        let commit_timestamp =
            transaction_manager.allocate_commit_timestamp(transaction.start_ts())?;

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

        let applied_writes = match self.storage.commit_batch(
            transaction.id(),
            transaction.start_ts(),
            commit_timestamp,
            transaction.write_set(),
        ) {
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

        for encoded_key in transaction.write_set().keys() {
            let row_key = decode_row_key(encoded_key).map_err(|error| {
                Error::InvalidArgument(format!(
                    "transaction contains a noncanonical row key: {error}"
                ))
            })?;

            if row_key.table_id != self.table_id {
                return Err(Error::InvalidArgument(format!(
                    "transaction row belongs to table {}, but commit \
                     coordinator owns table {}",
                    row_key.table_id.0, self.table_id.0
                )));
            }
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
                "transaction manager returned commit timestamp {}, which \
                 does not advance start timestamp {}",
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
                let durable_mutation = match mutation {
                    Mutation::Put(row) => WalMutation::Put(row.clone()),

                    Mutation::Delete => WalMutation::Delete,
                };

                (key.clone(), durable_mutation)
            })
            .collect::<BTreeMap<_, _>>();

        SingleNodeTxnCommit {
            table_id: self.table_id,
            txn_id: transaction.id(),
            start_timestamp: transaction.start_ts(),
            commit_timestamp,
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
