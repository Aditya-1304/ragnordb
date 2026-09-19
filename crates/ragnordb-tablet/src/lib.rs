//! Transaction-aware tablet operations.
//!
//! for now a tablet owns exactly one table
//! Later sharding work will extend ownership validation to a range or hash
//! partition without changing the transaction or MVCC representations.
//!
//! The tablet connects domain row keys and rows to MVCC storage, providing
//! point operations, ordered scans, read-your-writes, and atomic local commits.
//!
//! This right now does not replicate commands or persist them to WAL.

pub mod command;
pub mod read;
pub mod router;
pub mod snapshot;

pub use router::{
    HashTabletPartitioner, ScanProgress, ScanSpan, SpanSet, TabletRouter, TabletScanFragment,
};
use std::{
    collections::BTreeMap,
    ops::Bound::{Excluded, Included, Unbounded},
};

use ragnordb_common::{
    Error, Result,
    codec::{Row, TxnStatusRecord},
    encoding::{decode_row, encode_row},
    ids::{RowKey, TableId, TabletId, Timestamp},
};
use ragnordb_storage::{
    key::{decode_row_key, encode_row_key},
    mvcc::{InMemoryMvcc, Mutation, MvccStats, MvccStorage},
};
use ragnordb_txn::{
    AuthoritativeTransactionLease, IntentResolutionDecision, IntentResolutionLeasePolicy,
    PendingIntentDecision, ResolveIntentPlan, SingleNodeCommitParticipant, Transaction,
    TransactionStatusLocation, TransactionStatusReader, TransactionStatusRouteResolver,
    classify_pending_intent, pending_intent_retry_after_ms, plan_intent_resolution,
};

/// One logical row mutation waiting to be added to a transaction.
///
/// Keys and rows remain in their domain representation until the owning
/// tablet validates ownership and converts the complete statement batch into
/// canonical storage bytes.
#[derive(Debug, Clone, PartialEq)]
pub enum RowMutation {
    /// Insert a new row or replace a row already visible to the transaction.
    Put { key: RowKey, row: Row },

    /// Make a row absent from the transaction's view.
    Delete { key: RowKey },
}

/// One bounded tablet scan response in domain row representation.
#[derive(Debug, Clone, PartialEq)]
pub struct TabletScanPage {
    /// Rows visible at the transaction snapshot, ordered by canonical row key.
    pub rows: Vec<(RowKey, Row)>,

    /// Whether another visible row remains after the final returned key.
    pub has_more: bool,
}

/// Result of a point read that encountered a transaction intent.
///
/// Terminal intents are returned as a plan instead of being changed in place.
/// The caller must dispatch that plan through the participant tablet's normal
/// Raft path and retry the read after the command is applied. This preserves
/// one replicated durability boundary for both foreground reads and cleanup.
#[derive(Debug, Clone, PartialEq)]
pub enum IntentAwareRead {
    /// The snapshot contains a row, or the row is absent.
    Visible(Option<Row>),

    /// The authoritative status is terminal and the intent can be resolved.
    Resolve(ResolveIntentPlan),

    /// The authoritative status is still pending; no resolve command is safe.
    /// The retry interval is only a scheduling hint and does not imply that
    /// the local reader may expire or abort the transaction.
    Pending {
        status: TxnStatusRecord,
        retry_after_ms: u64,
    },

    /// The supplied authoritative lease elapsed while status was still
    /// pending. The status tablet must publish `Aborted` before a rollback
    /// command can be planned.
    LeaseExpired {
        status: TxnStatusRecord,
        lease_deadline_ms: u64,
    },
}

/// A logical tablet backed by an MVCC storage implementation.
#[derive(Debug)]
pub struct Tablet<S = InMemoryMvcc> {
    id: TabletId,
    table_id: TableId,
    storage: S,
}

impl Tablet<InMemoryMvcc> {
    /// Construct an empty in-memory tablet for one table.
    pub fn new(id: TabletId, table_id: TableId) -> Result<Self> {
        Self::with_storage(id, table_id, InMemoryMvcc::new())
    }
}

impl<S: MvccStorage> Tablet<S> {
    /// Construct a tablet using a supplied MVCC storage implementation.
    pub fn with_storage(id: TabletId, table_id: TableId, storage: S) -> Result<Self> {
        if id.0 == 0 {
            return Err(Error::InvalidArgument(
                "tablet ID 0 is reserved".to_string(),
            ));
        }

        if table_id.0 == 0 {
            return Err(Error::InvalidArgument("table ID 0 is reserved".to_string()));
        }

        Ok(Self {
            id,
            table_id,
            storage,
        })
    }

    /// Return the stable tablet identifier.
    pub fn id(&self) -> TabletId {
        self.id
    }

    /// Return the table owned by this tablet.
    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    /// borrow the tablet's storage for immutable recovery image capture
    ///
    /// Mutable access remains private to the tablet and durable commit
    /// coordinator, so callers cannot bypass commit ordering
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Read one row using the transaction snapshot and pending write set.
    pub fn get(&self, transaction: &Transaction, key: &RowKey) -> Result<Option<Row>> {
        self.validate_row_key(key)?;

        let encoded_key = encode_row_key(key)?;

        if let Some(mutation) = transaction.pending_write(&encoded_key) {
            return decode_pending_mutation(mutation);
        }

        self.storage
            .read(&encoded_key, transaction.start_ts())?
            .map(|row| decode_row(&row))
            .transpose()
    }

    /// Read one row while exposing terminal intent resolution to the caller.
    ///
    /// The status record must already have been fetched from the authoritative
    /// primary/status location. If the status is committed or aborted, this
    /// method returns a deterministic `ResolveIntentPlan`; it never applies the
    /// command locally. Pending status is returned explicitly with a bounded
    /// retry hint.
    pub fn get_with_intent_status(
        &self,
        transaction: &Transaction,
        key: &RowKey,
        status: &TxnStatusRecord,
    ) -> Result<IntentAwareRead> {
        self.get_with_intent_status_at_lease(transaction, key, status, None, 0)
    }

    /// Read one row while applying an authoritative lease observation to a
    /// pending intent.
    ///
    /// The lease is supplied by the transaction-status authority and uses a
    /// wall-clock deadline. This method reports expiry but does not synthesize
    /// an aborted status or mutate the participant. Once the status authority
    /// durably publishes `Aborted`, the ordinary terminal resolution path can
    /// create the replicated rollback command.
    pub fn get_with_intent_status_and_lease(
        &self,
        transaction: &Transaction,
        key: &RowKey,
        status: &TxnStatusRecord,
        lease: AuthoritativeTransactionLease,
        now_ms: u64,
    ) -> Result<IntentAwareRead> {
        self.get_with_intent_status_at_lease(transaction, key, status, Some(lease), now_ms)
    }

    fn get_with_intent_status_at_lease(
        &self,
        transaction: &Transaction,
        key: &RowKey,
        status: &TxnStatusRecord,
        lease: Option<AuthoritativeTransactionLease>,
        now_ms: u64,
    ) -> Result<IntentAwareRead> {
        self.validate_row_key(key)?;

        let encoded_key = encode_row_key(key)?;

        if let Some(mutation) = transaction.pending_write(&encoded_key) {
            return Ok(IntentAwareRead::Visible(decode_pending_mutation(mutation)?));
        }

        let Some(lock) = self
            .storage
            .intent_for_read(&encoded_key, transaction.start_ts())?
        else {
            let visible = self
                .storage
                .read(&encoded_key, transaction.start_ts())?
                .map(|row| decode_row(&row))
                .transpose()?;
            return Ok(IntentAwareRead::Visible(visible));
        };

        match plan_intent_resolution(&encoded_key, &lock, status)? {
            IntentResolutionDecision::Pending { status } => {
                let pending = match lease {
                    Some(lease) => {
                        classify_pending_intent(&encoded_key, &lock, &status, lease, now_ms)?
                    }
                    None => PendingIntentDecision::RetryableConflict {
                        retry_after_ms: pending_intent_retry_after_ms(&lock)?,
                    },
                };

                match pending {
                    PendingIntentDecision::RetryableConflict { retry_after_ms } => {
                        Ok(IntentAwareRead::Pending {
                            status,
                            retry_after_ms,
                        })
                    }
                    PendingIntentDecision::Expired { lease_deadline_ms } => {
                        Ok(IntentAwareRead::LeaseExpired {
                            status,
                            lease_deadline_ms,
                        })
                    }
                }
            }
            IntentResolutionDecision::Resolve(plan) => Ok(IntentAwareRead::Resolve(plan)),
        }
    }

    /// Read one row while resolving the transaction status through its current
    /// primary/status route.
    ///
    /// Status lookup is bounded and route-refreshable. A missing status never
    /// becomes an inferred abort, and a pending status is returned to the
    /// caller as a bounded retry conflict. Terminal outcomes are still
    /// returned as a plan so the caller can submit them through Raft.
    pub fn get_with_intent_status_lookup<R, L>(
        &self,
        transaction: &Transaction,
        key: &RowKey,
        status_location: &mut TransactionStatusLocation,
        route_resolver: &mut R,
        status_reader: &mut L,
        max_route_refreshes: usize,
    ) -> Result<IntentAwareRead>
    where
        R: TransactionStatusRouteResolver,
        L: TransactionStatusReader<Output = TxnStatusRecord>,
    {
        self.validate_row_key(key)?;
        let encoded_key = encode_row_key(key)?;

        if let Some(mutation) = transaction.pending_write(&encoded_key) {
            return Ok(IntentAwareRead::Visible(decode_pending_mutation(mutation)?));
        }

        if self
            .storage
            .intent_for_read(&encoded_key, transaction.start_ts())?
            .is_none()
        {
            let visible = self
                .storage
                .read(&encoded_key, transaction.start_ts())?
                .map(|row| decode_row(&row))
                .transpose()?;
            return Ok(IntentAwareRead::Visible(visible));
        }

        let status = status_location
            .lookup_with_retry(route_resolver, status_reader, max_route_refreshes)?
            .ok_or_else(|| Error::TabletUnavailable {
                reason: format!(
                    "transaction status for intent owner {} is not currently visible",
                    status_location.txn_id().0
                ),
            })?;

        self.get_with_intent_status(transaction, key, &status)
    }

    /// Read one row through the authoritative status route while applying a
    /// fenced lease observation to pending intents.
    pub fn get_with_intent_status_lookup_and_lease<R, L>(
        &self,
        transaction: &Transaction,
        key: &RowKey,
        status_location: &mut TransactionStatusLocation,
        route_resolver: &mut R,
        status_reader: &mut L,
        policy: IntentResolutionLeasePolicy,
    ) -> Result<IntentAwareRead>
    where
        R: TransactionStatusRouteResolver,
        L: TransactionStatusReader<Output = TxnStatusRecord>,
    {
        self.validate_row_key(key)?;
        let encoded_key = encode_row_key(key)?;

        if let Some(mutation) = transaction.pending_write(&encoded_key) {
            return Ok(IntentAwareRead::Visible(decode_pending_mutation(mutation)?));
        }

        if self
            .storage
            .intent_for_read(&encoded_key, transaction.start_ts())?
            .is_none()
        {
            let visible = self
                .storage
                .read(&encoded_key, transaction.start_ts())?
                .map(|row| decode_row(&row))
                .transpose()?;
            return Ok(IntentAwareRead::Visible(visible));
        }

        let status = status_location
            .lookup_with_retry(route_resolver, status_reader, policy.max_route_refreshes)?
            .ok_or_else(|| Error::TabletUnavailable {
                reason: format!(
                    "transaction status for intent owner {} is not currently visible",
                    status_location.txn_id().0
                ),
            })?;

        self.get_with_intent_status_at_lease(
            transaction,
            key,
            &status,
            Some(policy.lease),
            policy.now_ms,
        )
    }

    /// Validate, encode, and atomically buffer one statement's row mutations.
    ///
    /// No transaction state is changed until every row key has passed tablet
    /// ownership validation and every row has been encoded successfully. The
    /// batch also rejects duplicate keys so affected-row counts cannot diverge
    /// from the number of distinct buffered mutations.
    pub fn buffer_batch<I>(&self, transaction: &mut Transaction, mutations: I) -> Result<()>
    where
        I: IntoIterator<Item = RowMutation>,
    {
        let mut writes = BTreeMap::new();

        for mutation in mutations {
            let (key, mutation) = match mutation {
                RowMutation::Put { key, row } => {
                    self.validate_row_key(&key)?;
                    (encode_row_key(&key)?, Mutation::Put(encode_row(&row)?))
                }

                RowMutation::Delete { key } => {
                    self.validate_row_key(&key)?;
                    (encode_row_key(&key)?, Mutation::Delete)
                }
            };

            if writes.insert(key, mutation).is_some() {
                return Err(Error::InvalidArgument(
                    "tablet mutation batch contains a duplicate row key".to_string(),
                ));
            }
        }

        transaction.buffer_batch(writes)
    }

    /// Buffer a row insertion.
    ///
    /// A concurrent insert after the transaction's start timestamp is detected
    /// during commit validation.
    pub fn insert(&self, transaction: &mut Transaction, key: &RowKey, row: &Row) -> Result<()> {
        self.validate_row_key(key)?;

        if self.get(transaction, key)?.is_some() {
            return Err(Error::ConstraintViolation(
                "cannot insert a row whose primary key already exists".to_string(),
            ));
        }

        self.buffer_batch(
            transaction,
            std::iter::once(RowMutation::Put {
                key: key.clone(),
                row: row.clone(),
            }),
        )
    }

    /// Buffer an update when the row exists in the transaction's view.
    ///
    /// Returns `false` when the row is absent.
    pub fn update(&self, transaction: &mut Transaction, key: &RowKey, row: &Row) -> Result<bool> {
        self.validate_row_key(key)?;

        if self.get(transaction, key)?.is_none() {
            return Ok(false);
        }

        self.buffer_batch(
            transaction,
            std::iter::once(RowMutation::Put {
                key: key.clone(),
                row: row.clone(),
            }),
        )?;

        Ok(true)
    }

    /// Buffer a delete when the row exists in the transaction's view.
    ///
    /// Returns `false` when the row is absent.
    pub fn delete(&self, transaction: &mut Transaction, key: &RowKey) -> Result<bool> {
        self.validate_row_key(key)?;

        if self.get(transaction, key)?.is_none() {
            return Ok(false);
        }

        self.buffer_batch(
            transaction,
            std::iter::once(RowMutation::Delete { key: key.clone() }),
        )?;

        Ok(true)
    }

    /// Scan a half-open row-key range using the transaction snapshot.
    ///
    /// Pending writes are overlaid after reading committed MVCC state, which
    /// provides read-your-writes for inserts, updates, and deletes.
    pub fn scan(
        &self,
        transaction: &Transaction,
        start: Option<&RowKey>,
        end: Option<&RowKey>,
    ) -> Result<Vec<(RowKey, Row)>> {
        if let Some(start) = start {
            self.validate_row_key(start)?;
        }

        if let Some(end) = end {
            self.validate_row_key(end)?;
        }

        let start = start.map(encode_row_key).transpose()?;
        let end = end.map(encode_row_key).transpose()?;

        validate_scan_order(start.as_deref(), end.as_deref())?;

        let committed =
            self.storage
                .scan(start.as_deref(), end.as_deref(), transaction.start_ts())?;

        let mut visible = committed.into_iter().collect::<BTreeMap<_, _>>();

        for (key, mutation) in transaction.write_set() {
            let row_key = decode_transaction_row_key(key)?;
            self.validate_row_key(&row_key)?;

            if !key_is_in_range(key, start.as_deref(), end.as_deref()) {
                continue;
            }

            match mutation {
                Mutation::Put(row) => {
                    visible.insert(key.clone(), row.clone());
                }

                Mutation::Delete => {
                    visible.remove(key);
                }
            }
        }

        visible
            .into_iter()
            .map(|(key, row)| {
                let row_key = decode_row_key(&key)?;

                if row_key.table_id != self.table_id {
                    return Err(Error::CorruptData(format!(
                        "tablet {} owns table {}, but its storage contains \
                         a row for table {}",
                        self.id.0, self.table_id.0, row_key.table_id.0
                    )));
                }

                Ok((row_key, decode_row(&row)?))
            })
            .collect()
    }

    /// Scan one bounded page while preserving read-your-writes semantics.
    ///
    /// The continuation key is exclusive. Committed rows and the transaction's
    /// ordered pending write set are merged by encoded key, with pending
    /// mutations winning on equal keys. Storage pages are fetched lazily when
    /// a pending delete or a page boundary would otherwise hide later rows.
    pub fn scan_page(
        &self,
        transaction: &Transaction,
        start: Option<&RowKey>,
        end: Option<&RowKey>,
        resume_after: Option<&RowKey>,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<TabletScanPage> {
        if max_rows == 0 {
            return Err(Error::InvalidArgument(
                "scan page max_rows must be greater than zero".to_string(),
            ));
        }
        if max_bytes == 0 {
            return Err(Error::InvalidArgument(
                "scan page max_bytes must be greater than zero".to_string(),
            ));
        }

        if let Some(start) = start {
            self.validate_row_key(start)?;
        }
        if let Some(end) = end {
            self.validate_row_key(end)?;
        }
        if let Some(resume_after) = resume_after {
            self.validate_row_key(resume_after)?;
        }

        let start = start.map(encode_row_key).transpose()?;
        let end = end.map(encode_row_key).transpose()?;
        let resume_after = resume_after.map(encode_row_key).transpose()?;
        validate_scan_order(start.as_deref(), end.as_deref())?;

        if let (Some(resume_after), Some(end)) = (resume_after.as_deref(), end.as_deref())
            && resume_after >= end
        {
            return Ok(TabletScanPage {
                rows: Vec::new(),
                has_more: false,
            });
        }

        // Validate all buffered keys before beginning the merge. This retains
        // the tablet ownership boundary of the unbounded scan API even when a
        // malformed or foreign mutation falls outside this particular page.
        for key in transaction.write_set().keys() {
            let row_key = decode_transaction_row_key(key)?;
            self.validate_row_key(&row_key)?;
        }

        let pending_lower = tablet_scan_lower_bound(start.as_deref(), resume_after.as_deref());
        let pending_upper = end.as_ref().map_or(Unbounded, |end| Excluded(end.clone()));
        let mut pending = transaction
            .write_set()
            .range((pending_lower, pending_upper))
            .peekable();
        let mut next_pending = pending.next();

        // The storage page is an internal merge input. Keep its row bound but
        // remove its byte boundary so an oversized committed row that is
        // hidden by a pending delete or replaced by a smaller pending Put does
        // not fail before the overlay can decide its visibility. The returned
        // tablet page still enforces `max_bytes` below.
        let mut storage_page = self.storage.scan_page(
            start.as_deref(),
            end.as_deref(),
            resume_after.as_deref(),
            transaction.start_ts(),
            max_rows,
            usize::MAX,
        )?;
        let mut storage_index = 0_usize;
        let mut rows = Vec::new();
        let mut encoded_bytes = 0_usize;

        loop {
            if storage_index == storage_page.rows.len() && storage_page.has_more {
                let last_key = storage_page
                    .rows
                    .last()
                    .map(|(key, _)| key.clone())
                    .ok_or_else(|| {
                        Error::CorruptData(
                            "MVCC scan page reported more rows without a continuation key"
                                .to_string(),
                        )
                    })?;
                storage_page = self.storage.scan_page(
                    start.as_deref(),
                    end.as_deref(),
                    Some(&last_key),
                    transaction.start_ts(),
                    max_rows,
                    usize::MAX,
                )?;
                storage_index = 0;
                if storage_page.rows.is_empty() && storage_page.has_more {
                    return Err(Error::CorruptData(
                        "MVCC scan page made no progress while reporting more rows".to_string(),
                    ));
                }
                continue;
            }

            let pending_key = next_pending.map(|(key, _)| key.as_slice());
            let storage_key = storage_page
                .rows
                .get(storage_index)
                .map(|(key, _)| key.as_slice());
            if pending_key.is_none() && storage_key.is_none() {
                return Ok(TabletScanPage {
                    rows,
                    has_more: false,
                });
            }

            let take_pending = match (pending_key, storage_key) {
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some(pending_key), Some(storage_key)) => pending_key <= storage_key,
                (None, None) => unreachable!("both scan sources were checked above"),
            };
            let take_storage = match (pending_key, storage_key) {
                (Some(_), None) => false,
                (None, Some(_)) => true,
                (Some(pending_key), Some(storage_key)) => storage_key <= pending_key,
                (None, None) => unreachable!("both scan sources were checked above"),
            };

            let key = if take_pending {
                pending_key
                    .expect("pending source selected without a pending key")
                    .to_vec()
            } else {
                storage_key
                    .expect("storage source selected without a storage key")
                    .to_vec()
            };

            let pending_mutation = if take_pending {
                let (_, mutation) = next_pending
                    .take()
                    .expect("pending source selected without a pending mutation");
                next_pending = pending.next();
                Some(mutation.clone())
            } else {
                None
            };
            let storage_row = if take_storage {
                let row = storage_page
                    .rows
                    .get(storage_index)
                    .expect("storage source selected without a storage row")
                    .1
                    .clone();
                storage_index += 1;
                Some(row)
            } else {
                None
            };

            let row = match pending_mutation {
                Some(Mutation::Put(row)) => row,
                Some(Mutation::Delete) => continue,
                None => storage_row.expect("storage row is required without a pending mutation"),
            };

            let row_bytes = key.len().checked_add(row.len()).ok_or_else(|| {
                Error::InvalidArgument("scan page encoded byte count overflowed".to_string())
            })?;
            let next_bytes = encoded_bytes.checked_add(row_bytes).ok_or_else(|| {
                Error::InvalidArgument("scan page encoded byte count overflowed".to_string())
            })?;
            if rows.len() >= max_rows || next_bytes > max_bytes {
                if rows.is_empty() {
                    return Err(Error::InvalidArgument(
                        "scan page byte budget is smaller than the first encoded row".to_string(),
                    ));
                }
                return Ok(TabletScanPage {
                    rows,
                    has_more: true,
                });
            }

            let row_key = decode_row_key(&key)?;
            self.validate_row_key(&row_key)?;
            let row = decode_row(&row)?;
            encoded_bytes = next_bytes;
            rows.push((row_key, row));
        }
    }

    /// validate a non empty transaction write set against tablet ownership and
    /// mvcc conflict state without consuming or applying the transaction
    ///
    /// commit cooridnator calls this method before allocating the final commit
    /// timestamp or appending transaction record to WAL
    pub fn validate_commit(&self, transaction: &Transaction) -> Result<()> {
        if transaction.is_empty() {
            return Err(Error::InvalidArgument(
                "tablet write commit requires at least one mutation".to_string(),
            ));
        }

        for key in transaction.write_set().keys() {
            let row_key = decode_transaction_row_key(key)?;
            self.validate_row_key(&row_key)?;
        }

        self.storage.validate_commit_batch(
            transaction.id(),
            transaction.start_ts(),
            transaction.write_set(),
        )
    }

    /// Atomically apply a previouslly validated transaction write set
    ///
    /// The method repeats preflight validation defensively because it remains a
    /// public storage boundary. durable coordinator will serialize
    /// preflight, WAL synchronization, and this application step so no second
    /// writer can invalidate the checked MVCC history between those operations
    pub fn commit(&mut self, transaction: Transaction, commit_ts: Timestamp) -> Result<usize> {
        for key in transaction.write_set().keys() {
            let row_key = decode_transaction_row_key(key)?;
            self.validate_row_key(&row_key)?;
        }

        let txn_id = transaction.id();
        let start_ts = transaction.start_ts();
        let writes = transaction.into_write_set();

        self.storage
            .commit_batch(txn_id, start_ts, commit_ts, &writes)
    }

    /// Abort an uncommitted local transaction by discarding its write set.
    ///
    /// Phase 2.6 does not require a rollback record because local buffered
    /// mutations have not been exposed to storage. Distributed prewrites will
    /// require durable rollback records in Milestone 6.
    pub fn rollback(&self, transaction: Transaction) -> usize {
        transaction.len()
    }

    /// Return current storage diagnostics.
    pub fn stats(&self) -> MvccStats {
        self.storage.stats()
    }

    fn validate_row_key(&self, key: &RowKey) -> Result<()> {
        if key.table_id != self.table_id {
            return Err(Error::InvalidArgument(format!(
                "row belongs to table {}, but tablet {} owns table {}",
                key.table_id.0, self.id.0, self.table_id.0
            )));
        }

        Ok(())
    }
}

impl<S> SingleNodeCommitParticipant for Tablet<S>
where
    S: MvccStorage,
{
    fn table_id(&self) -> TableId {
        self.table_id
    }

    fn validate_commit(&self, transaction: &Transaction) -> Result<()> {
        Tablet::validate_commit(self, transaction)
    }

    fn apply_commit(
        &mut self,
        transaction: &Transaction,
        commit_timestamp: Timestamp,
    ) -> Result<usize> {
        for key in transaction.write_set().keys() {
            let row_key = decode_transaction_row_key(key)?;
            self.validate_row_key(&row_key)?;
        }

        self.storage.commit_batch(
            transaction.id(),
            transaction.start_ts(),
            commit_timestamp,
            transaction.write_set(),
        )
    }
}

fn decode_transaction_row_key(key: &[u8]) -> Result<RowKey> {
    decode_row_key(key).map_err(|error| {
        Error::InvalidArgument(format!(
            "transaction contains a noncanonical encoded row key: {error}"
        ))
    })
}

fn decode_pending_mutation(mutation: &Mutation) -> Result<Option<Row>> {
    match mutation {
        Mutation::Put(row) => decode_row(row).map(Some),
        Mutation::Delete => Ok(None),
    }
}

fn validate_scan_order(start: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
    if let (Some(start), Some(end)) = (start, end)
        && start >= end
    {
        return Err(Error::InvalidArgument(
            "scan start key must be less than scan end key".to_string(),
        ));
    }

    Ok(())
}

fn key_is_in_range(key: &[u8], start: Option<&[u8]>, end: Option<&[u8]>) -> bool {
    start.is_none_or(|start| key >= start) && end.is_none_or(|end| key < end)
}

fn tablet_scan_lower_bound(
    start: Option<&[u8]>,
    resume_after: Option<&[u8]>,
) -> std::ops::Bound<Vec<u8>> {
    match (start, resume_after) {
        (None, None) => Unbounded,
        (Some(start), None) => Included(start.to_vec()),
        (None, Some(resume_after)) => Excluded(resume_after.to_vec()),
        (Some(start), Some(resume_after)) if resume_after < start => Included(start.to_vec()),
        (Some(_), Some(resume_after)) => Excluded(resume_after.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ragnordb_common::{
        codec::{TxnStatus, TxnStatusRecord, Value},
        ids::TxnId,
    };
    use ragnordb_storage::{
        key::{encode_row_key, make_row_key},
        mvcc::{Mutation, MvccStorage},
    };

    fn key(id: i64) -> RowKey {
        key_for_table(TableId(1), id)
    }

    fn key_for_table(table_id: TableId, id: i64) -> RowKey {
        make_row_key(table_id, &[Value::Int(id)]).unwrap()
    }

    fn row(id: i64, name: &str) -> Row {
        Row {
            values: vec![Value::Int(id), Value::Text(name.to_string())],
        }
    }

    fn transaction(id: u64, start_ts: u64) -> Transaction {
        Transaction::new(TxnId(id), Timestamp(start_ts)).unwrap()
    }

    fn tablet() -> Tablet {
        Tablet::new(TabletId(1), TableId(1)).unwrap()
    }

    #[test]
    fn pending_insert_is_visible_to_get_and_scan() {
        let tablet = tablet();
        let mut txn = transaction(1, 1);
        let row_key = key(1);
        let value = row(1, "pending");

        tablet.insert(&mut txn, &row_key, &value).unwrap();

        assert_eq!(tablet.get(&txn, &row_key).unwrap(), Some(value.clone()));

        assert_eq!(
            tablet.scan(&txn, None, None).unwrap(),
            vec![(row_key, value)]
        );
    }

    #[test]
    fn committed_rows_are_visible_to_new_transactions() {
        let mut tablet = tablet();
        let row_key = key(1);
        let value = row(1, "committed");
        let mut writer = transaction(1, 1);

        tablet.insert(&mut writer, &row_key, &value).unwrap();

        assert_eq!(tablet.commit(writer, Timestamp(2)).unwrap(), 1);

        let reader = transaction(2, 3);

        assert_eq!(tablet.get(&reader, &row_key).unwrap(), Some(value));
    }

    #[test]
    fn read_path_returns_a_terminal_resolution_plan_for_a_committed_intent() {
        let mut tablet = tablet();
        let row_key = key(1);
        let encoded_key = encode_row_key(&row_key).unwrap();
        let primary_key = encode_row_key(&key(2)).unwrap();
        let encoded_row = encode_row(&row(1, "intent")).unwrap();

        tablet
            .storage
            .prewrite(
                TxnId(7),
                Timestamp(100),
                &encoded_key,
                &Mutation::Put(encoded_row),
                &primary_key,
                30_000,
            )
            .unwrap();

        let reader = transaction(8, 100);
        let status = TxnStatusRecord {
            txn_id: TxnId(7),
            start_timestamp: Timestamp(100),
            commit_timestamp: Some(Timestamp(110)),
            status: TxnStatus::Committed,
            primary_key,
            participant_tablet_ids: vec![1],
            last_heartbeat_timestamp: None,
        };

        let outcome = tablet
            .get_with_intent_status(&reader, &row_key, &status)
            .unwrap();

        let IntentAwareRead::Resolve(plan) = outcome else {
            panic!("committed intent must be surfaced for durable resolution");
        };
        assert_eq!(plan.command.resolved_status, TxnStatus::Committed);
        assert_eq!(plan.command.commit_timestamp, Some(Timestamp(110)));
        assert_eq!(plan.command.keys, vec![encoded_key]);
    }

    #[test]
    fn read_path_returns_a_rollback_plan_for_an_aborted_intent() {
        let mut tablet = tablet();
        let row_key = key(3);
        let encoded_key = encode_row_key(&row_key).unwrap();
        let primary_key = encode_row_key(&key(4)).unwrap();

        tablet
            .storage
            .prewrite(
                TxnId(9),
                Timestamp(120),
                &encoded_key,
                &Mutation::Put(encode_row(&row(3, "intent")).unwrap()),
                &primary_key,
                30_000,
            )
            .unwrap();

        let status = TxnStatusRecord {
            txn_id: TxnId(9),
            start_timestamp: Timestamp(120),
            commit_timestamp: None,
            status: TxnStatus::Aborted,
            primary_key,
            participant_tablet_ids: vec![1],
            last_heartbeat_timestamp: None,
        };

        let outcome = tablet
            .get_with_intent_status(&transaction(10, 120), &row_key, &status)
            .unwrap();

        let IntentAwareRead::Resolve(plan) = outcome else {
            panic!("aborted intent must be surfaced for durable rollback");
        };
        assert_eq!(plan.command.resolved_status, TxnStatus::Aborted);
        assert_eq!(plan.command.commit_timestamp, None);
    }

    #[test]
    fn read_path_keeps_pending_intents_out_of_terminal_resolution() {
        let mut tablet = tablet();
        let row_key = key(5);
        let encoded_key = encode_row_key(&row_key).unwrap();
        let primary_key = encode_row_key(&key(6)).unwrap();

        tablet
            .storage
            .prewrite(
                TxnId(11),
                Timestamp(140),
                &encoded_key,
                &Mutation::Put(encode_row(&row(5, "pending")).unwrap()),
                &primary_key,
                30_000,
            )
            .unwrap();

        let status = TxnStatusRecord {
            txn_id: TxnId(11),
            start_timestamp: Timestamp(140),
            commit_timestamp: None,
            status: TxnStatus::Pending,
            primary_key,
            participant_tablet_ids: vec![1],
            last_heartbeat_timestamp: Some(Timestamp(145)),
        };

        let outcome = tablet
            .get_with_intent_status(&transaction(12, 140), &row_key, &status)
            .unwrap();

        let IntentAwareRead::Pending { retry_after_ms, .. } = outcome else {
            panic!("pending intent must remain a retryable read conflict");
        };
        assert_eq!(retry_after_ms, 10_000);
    }

    #[test]
    fn expired_lease_is_reported_without_local_rollback() {
        let mut tablet = tablet();
        let row_key = key(7);
        let encoded_key = encode_row_key(&row_key).unwrap();
        let primary_key = encode_row_key(&key(8)).unwrap();

        tablet
            .storage
            .prewrite(
                TxnId(13),
                Timestamp(150),
                &encoded_key,
                &Mutation::Put(encode_row(&row(7, "expired")).unwrap()),
                &primary_key,
                30_000,
            )
            .unwrap();

        let status = TxnStatusRecord {
            txn_id: TxnId(13),
            start_timestamp: Timestamp(150),
            commit_timestamp: None,
            status: TxnStatus::Pending,
            primary_key,
            participant_tablet_ids: vec![1],
            last_heartbeat_timestamp: Some(Timestamp(155)),
        };
        let lease = AuthoritativeTransactionLease::new(20_000, 10_000).unwrap();

        let outcome = tablet
            .get_with_intent_status_and_lease(
                &transaction(14, 150),
                &row_key,
                &status,
                lease,
                20_000,
            )
            .unwrap();

        assert!(matches!(
            outcome,
            IntentAwareRead::LeaseExpired {
                lease_deadline_ms: 20_000,
                ..
            }
        ));
        assert!(
            tablet
                .storage
                .intent_for_read(&encoded_key, Timestamp(150))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn point_snapshot_remains_stable_after_newer_commit() {
        let mut tablet = tablet();
        let row_key = key(1);
        let original = row(1, "original");
        let updated = row(1, "updated");

        let mut seed = transaction(1, 1);
        tablet.insert(&mut seed, &row_key, &original).unwrap();
        tablet.commit(seed, Timestamp(2)).unwrap();

        let old_snapshot = transaction(2, 3);

        let mut writer = transaction(3, 4);
        assert!(tablet.update(&mut writer, &row_key, &updated).unwrap());
        tablet.commit(writer, Timestamp(5)).unwrap();

        assert_eq!(tablet.get(&old_snapshot, &row_key).unwrap(), Some(original));

        let fresh_snapshot = transaction(4, 6);

        assert_eq!(
            tablet.get(&fresh_snapshot, &row_key).unwrap(),
            Some(updated)
        );
    }

    #[test]
    fn scan_snapshot_remains_stable_after_newer_commit() {
        let mut tablet = tablet();
        let first_key = key(1);
        let second_key = key(2);
        let third_key = key(3);
        let original_second = row(2, "original");
        let updated_second = row(2, "updated");

        let mut seed = transaction(1, 1);
        tablet
            .insert(&mut seed, &first_key, &row(1, "first"))
            .unwrap();
        tablet
            .insert(&mut seed, &second_key, &original_second)
            .unwrap();
        tablet.commit(seed, Timestamp(2)).unwrap();

        let old_snapshot = transaction(2, 3);

        let mut writer = transaction(3, 4);
        assert!(
            tablet
                .update(&mut writer, &second_key, &updated_second,)
                .unwrap()
        );
        tablet
            .insert(&mut writer, &third_key, &row(3, "third"))
            .unwrap();
        tablet.commit(writer, Timestamp(5)).unwrap();

        assert_eq!(
            tablet.scan(&old_snapshot, None, None).unwrap(),
            vec![
                (first_key.clone(), row(1, "first")),
                (second_key.clone(), original_second),
            ]
        );

        let fresh_snapshot = transaction(4, 6);

        assert_eq!(
            tablet.scan(&fresh_snapshot, None, None).unwrap(),
            vec![
                (first_key, row(1, "first")),
                (second_key, updated_second),
                (third_key, row(3, "third")),
            ]
        );
    }

    #[test]
    fn concurrent_writers_conflict_at_commit() {
        let mut tablet = tablet();
        let row_key = key(1);
        let first_value = row(1, "first");
        let second_value = row(1, "second");

        let mut first = transaction(1, 1);
        tablet.insert(&mut first, &row_key, &first_value).unwrap();

        let mut second = transaction(2, 2);
        tablet.insert(&mut second, &row_key, &second_value).unwrap();

        tablet.commit(second, Timestamp(3)).unwrap();

        let error = tablet.commit(first, Timestamp(4)).unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));

        let reader = transaction(3, 5);

        assert_eq!(tablet.get(&reader, &row_key).unwrap(), Some(second_value));
    }

    #[test]
    fn conflicting_multi_key_commit_is_atomic() {
        let mut tablet = tablet();
        let first_key = key(1);
        let second_key = key(2);

        let mut loser = transaction(1, 1);
        tablet
            .insert(&mut loser, &first_key, &row(1, "loser"))
            .unwrap();
        tablet
            .insert(&mut loser, &second_key, &row(2, "loser"))
            .unwrap();

        let mut winner = transaction(2, 2);
        tablet
            .insert(&mut winner, &second_key, &row(2, "winner"))
            .unwrap();
        tablet.commit(winner, Timestamp(3)).unwrap();

        let error = tablet.commit(loser, Timestamp(4)).unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));

        let reader = transaction(3, 5);

        assert_eq!(tablet.get(&reader, &first_key).unwrap(), None);
        assert_eq!(
            tablet.get(&reader, &second_key).unwrap(),
            Some(row(2, "winner"))
        );
    }

    #[test]
    fn delete_is_visible_locally_and_becomes_tombstone() {
        let mut tablet = tablet();
        let row_key = key(1);
        let value = row(1, "value");

        let mut seed = transaction(1, 1);
        tablet.insert(&mut seed, &row_key, &value).unwrap();
        tablet.commit(seed, Timestamp(2)).unwrap();

        let old_snapshot = transaction(2, 3);
        let mut deleter = transaction(3, 4);

        assert!(tablet.delete(&mut deleter, &row_key).unwrap());
        assert_eq!(tablet.get(&deleter, &row_key).unwrap(), None);

        tablet.commit(deleter, Timestamp(5)).unwrap();

        assert_eq!(tablet.get(&old_snapshot, &row_key).unwrap(), Some(value));

        let fresh_snapshot = transaction(4, 6);

        assert_eq!(tablet.get(&fresh_snapshot, &row_key).unwrap(), None);
    }

    #[test]
    fn scan_overlays_pending_mutations() {
        let mut tablet = tablet();
        let first_key = key(1);
        let second_key = key(2);
        let third_key = key(3);

        let mut seed = transaction(1, 1);
        tablet
            .insert(&mut seed, &first_key, &row(1, "first"))
            .unwrap();
        tablet
            .insert(&mut seed, &second_key, &row(2, "second"))
            .unwrap();
        tablet.commit(seed, Timestamp(2)).unwrap();

        let mut txn = transaction(2, 3);

        assert!(tablet.delete(&mut txn, &first_key).unwrap());
        assert!(
            tablet
                .update(&mut txn, &second_key, &row(2, "updated"),)
                .unwrap()
        );
        tablet
            .insert(&mut txn, &third_key, &row(3, "third"))
            .unwrap();

        assert_eq!(
            tablet.scan(&txn, None, None).unwrap(),
            vec![
                (second_key, row(2, "updated")),
                (third_key, row(3, "third")),
            ]
        );
    }

    #[test]
    fn scan_page_fills_after_pending_delete_and_continues_without_duplicates() {
        // Regression: overlaying a bounded committed page after the fact can
        // return a short page and then repeat or omit the next committed row.
        let mut tablet = tablet();
        let first_key = key(1);
        let second_key = key(2);
        let third_key = key(3);
        let fourth_key = key(4);

        let mut seed = transaction(1, 1);
        tablet
            .insert(&mut seed, &first_key, &row(1, "first"))
            .unwrap();
        tablet
            .insert(&mut seed, &second_key, &row(2, "second"))
            .unwrap();
        tablet
            .insert(&mut seed, &third_key, &row(3, "third"))
            .unwrap();
        tablet.commit(seed, Timestamp(2)).unwrap();

        let mut txn = transaction(2, 3);
        assert!(tablet.delete(&mut txn, &first_key).unwrap());
        assert!(
            tablet
                .update(&mut txn, &second_key, &row(2, "updated"))
                .unwrap()
        );
        tablet
            .insert(&mut txn, &fourth_key, &row(4, "fourth"))
            .unwrap();

        let first_page = tablet
            .scan_page(&txn, None, None, None, 2, usize::MAX)
            .unwrap();
        assert_eq!(
            first_page.rows,
            vec![
                (second_key.clone(), row(2, "updated")),
                (third_key.clone(), row(3, "third"))
            ]
        );
        assert!(first_page.has_more);

        let second_page = tablet
            .scan_page(
                &txn,
                None,
                None,
                Some(&first_page.rows.last().unwrap().0),
                2,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(second_page.rows, vec![(fourth_key, row(4, "fourth"))]);
        assert!(!second_page.has_more);
    }

    #[test]
    fn tablet_rejects_foreign_row_keys() {
        let tablet = tablet();
        let foreign_key = key_for_table(TableId(2), 1);
        let txn = transaction(1, 1);

        let error = tablet.get(&txn, &foreign_key).unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
    }

    #[test]
    fn tablet_rejects_foreign_scan_boundaries() {
        let tablet = tablet();
        let foreign_key = key_for_table(TableId(2), 1);
        let txn = transaction(1, 1);

        let error = tablet.scan(&txn, Some(&foreign_key), None).unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
    }

    #[test]
    fn mixed_table_transaction_cannot_commit() {
        let mut tablet = tablet();
        let local_key = key(1);
        let foreign_key = key_for_table(TableId(2), 2);
        let mut txn = transaction(1, 1);

        tablet
            .insert(&mut txn, &local_key, &row(1, "local"))
            .unwrap();

        txn.buffer_put(
            encode_row_key(&foreign_key).unwrap(),
            encode_row(&row(2, "foreign")).unwrap(),
        )
        .unwrap();

        let error = tablet.commit(txn, Timestamp(2)).unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));

        // Ownership validation occurs before MVCC application, so the valid
        // local mutation must not have been partially committed.
        let reader = transaction(2, 3);

        assert_eq!(tablet.get(&reader, &local_key).unwrap(), None);
    }

    #[test]
    fn rollback_discards_local_write_set() {
        let tablet = tablet();
        let mut txn = transaction(1, 1);

        tablet.insert(&mut txn, &key(1), &row(1, "first")).unwrap();
        tablet.insert(&mut txn, &key(2), &row(2, "second")).unwrap();

        assert_eq!(tablet.rollback(txn), 2);

        let reader = transaction(2, 2);

        assert!(tablet.scan(&reader, None, None).unwrap().is_empty());
    }

    #[test]
    fn rejected_row_mutation_batch_preserves_transaction_state() {
        let tablet = tablet();
        let existing_key = key(1);
        let batch_key = key(2);
        let foreign_key = key_for_table(TableId(2), 3);
        let mut txn = transaction(1, 1);

        tablet
            .insert(&mut txn, &existing_key, &row(1, "existing"))
            .unwrap();

        let error = tablet
            .buffer_batch(
                &mut txn,
                vec![
                    RowMutation::Put {
                        key: batch_key.clone(),
                        row: row(2, "valid"),
                    },
                    RowMutation::Put {
                        key: foreign_key,
                        row: row(3, "foreign"),
                    },
                ],
            )
            .unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert_eq!(
            tablet.get(&txn, &existing_key).unwrap(),
            Some(row(1, "existing"))
        );
        assert_eq!(tablet.get(&txn, &batch_key).unwrap(), None);
        assert_eq!(txn.len(), 1);
    }
}
