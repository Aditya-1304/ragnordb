//! ordered single node durable commit coordination
//!
//! one coordinator owns one commit participant and one semantic durable log
//! Its mutable commit method forms the serialized correctness boundary from
//! complete preflight through durable append and atomic MVCC application

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

use crate::{CommitTimestampAllocator, Transaction};

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
