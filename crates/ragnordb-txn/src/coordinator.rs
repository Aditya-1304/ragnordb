//! ordered single node durable commit coordination
//!
//! one coordinator owns one commit participant and one semantic durable log
//! Its mutable commit method forms the serialized correctness boundary from
//! complete preflight through durable append and atomic MVCC application

use std::collections::{BTreeMap, BTreeSet};

use ragnordb_common::{
    Error, Result,
    ids::{
        ClientRequestId, CommandKind, ParticipantCommandPhase, RaftGroupId, TableId, TabletId,
        Timestamp, TxnId,
    },
};
use ragnordb_storage::{
    key::decode_row_key,
    mvcc::{Mutation, MvccStorage},
    wal::{DurableCommitLog, DurableWalExtent, SingleNodeTxnCommit, WalMutation},
};

use crate::{CommitTimestampAllocator, Transaction};

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
}

impl ParticipantRoute {
    pub fn new(tablet_id: TabletId, tablet_epoch: u64, raft_group_id: RaftGroupId) -> Result<Self> {
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

        Ok(Self {
            tablet_id,
            tablet_epoch,
            raft_group_id,
        })
    }
}

/// Semantic location of a transaction's primary/status record.
///
/// The primary key is authoritative. The route is only a cacheable hint and
/// is refreshed after topology changes, so a status lookup never depends on a
/// tablet ID surviving a split or merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionStatusLocation {
    primary_key: LogicalMutationId,
    route: Option<ParticipantRoute>,
}

impl TransactionStatusLocation {
    fn new(primary_key: Vec<u8>) -> Result<Self> {
        Ok(Self {
            primary_key: LogicalMutationId::from_key(&primary_key)?,
            route: None,
        })
    }

    pub fn primary_key(&self) -> &[u8] {
        self.primary_key.as_key()
    }

    pub fn route(&self) -> Option<ParticipantRoute> {
        self.route
    }
}

/// Stable participant command identity for one logical mutation.
///
/// This identity is the coordinator's source of truth for retries. It is
/// derived only from the transaction, phase, and logical key, never from the
/// current tablet route. Slice 2 will adapt it to concrete command envelopes
/// and transport request IDs.
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
        match self.phase {
            ParticipantCommandPhase::Prewrite => CommandKind::Prewrite,
            ParticipantCommandPhase::Commit => CommandKind::Commit,
            ParticipantCommandPhase::Rollback => CommandKind::Rollback,
            ParticipantCommandPhase::ResolveIntent => CommandKind::ResolveIntent,
        }
    }
}

/// Transaction-local coordinator state for the Phase 6.2 identity boundary.
///
/// This type owns semantic transaction state and current route hints. It does
/// not submit Raft commands or perform prewrite/commit yet; those dispatch
/// responsibilities are deliberately reserved for the next slice so route
/// refresh cannot accidentally become a second transaction authority.
#[derive(Debug)]
pub struct DistributedTransactionCoordinator {
    transaction: Transaction,
    root_request_id: ClientRequestId,
    primary_key: LogicalMutationId,
    read_set: BTreeSet<LogicalMutationId>,
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
        let status_location = TransactionStatusLocation::new(primary_key.as_key().to_vec())?;

        Ok(Self {
            transaction,
            root_request_id,
            primary_key,
            read_set: BTreeSet::new(),
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
        self.status_location.route = Some(route);
        Ok(())
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
