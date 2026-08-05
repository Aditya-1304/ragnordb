//! In memory multi version concurrency control
//!
//! the engine maintains the three logical maps used by RagnorDB's MVCC model:
//!
//! ```text
//! default/{row_key}/{start_ts} -> encoded row
//! lock/{row_key}               -> uncommitted lock record
//! write/{row_key}/{commit_ts}  -> committed write record
//! ```
//!
//! `default` stores row payloads by transaction start timestamp
//! `write` stores the commit history used by snapshot readers.
//! `put` record points back to its payload in `default`;
//! `Delete` is a tombstone; `Rollback` prevents a delayed transaction
//! message from resurrecting an absorbed write
//!
//! For committed `Put` and `Delete` records, `write_ts` is the commit
//! timestamp and must be greater than the transaction start timestamp.
//!
//! A `Rollback` record has no independently allocated commit timestamp.
//! Consequently, it is stored at the aborted transaction's start timestamp,
//! and its `commit_timestamp` field also contains that start timestamp. The
//! field name is retained for compatibility with the existing shared codec.
//!
//! this commits buffered single tablet transaction directly after
//! validating the entire batch.
//! Distributed prewrite, lock resolution, transction status records
//! Raft application, WAL durability, and garbage collection are
//! intentionally deferred to their later milestones
//!
//! the existing raft `WriteEntry` currently stores one `Value`,
//! while `Mutation::Put` stores a complete canonical encoded row
//! those representation must be aligned before `SingleShardCommit` is wired
//! into raft

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound::{self, Excluded, Included, Unbounded},
};

use crate::key::decode_row_key;

use ragnordb_common::{
    Error, Result,
    codec::{LockRecord, WriteKind, WriteRecord},
    encoding::decode_row,
    ids::{TableId, Timestamp, TxnId},
    proto::snapshot as snapshot_proto,
};

use crate::checkpoint::CapturedMvccState;

/// Owned range boundaries used for canonical encoded row-key scans.
type EncodedScanBounds = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// A transaction local mutation waiting to be committed
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    /// insert or replace a row using its canonical encoded representation
    Put(Vec<u8>),

    /// make the row absent for snapshots at or after the commit timestamp
    Delete,
}

impl Mutation {
    fn write_kind(&self) -> WriteKind {
        match self {
            Self::Put(_) => WriteKind::Put,
            Self::Delete => WriteKind::Delete,
        }
    }
}

/// diagnostic counters for the in memory MVCC engine
///
/// these counters describe logical in memory state they are not durability
/// or replication metrics
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MvccStats {
    /// number of distinct row keys with at least one default value
    pub default_keys: usize,

    /// total number of default value version
    pub default_versions: usize,

    /// number of unresolved locks
    pub locks: usize,

    /// number of unresolved locks
    pub write_keys: usize,

    /// total number of Put, Delete and Rollback write records.
    pub write_records: usize,
}

/// Storage contract required by the transaction-aware tablet layer.
///
/// All keys passed to this trait must be complete canonical row-key encodings
/// produced by `ragnordb_storage::key::encode_row_key`.
pub trait MvccStorage {
    /// Read the row version visible at `read_ts`.
    fn read(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Vec<u8>>>;

    /// Scan the half-open encoded-key range `[start, end)` at `read_ts`.
    ///
    /// Returned rows must be ordered by canonical encoded row key.
    fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        read_ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Atomically install one distributed transaction intent.
    ///
    /// An exact replay is idempotent. A conflicting lock, a newer committed write,
    /// or an existing rollback marker rejects the prewrite without changing either
    /// the default-value or lock map.
    ///
    /// Backends that have not implemented distributed intents fail explicitly.
    fn prewrite(
        &mut self,
        _txn_id: TxnId,
        _start_ts: Timestamp,
        _key: &[u8],
        _mutation: &Mutation,
        _primary_key: &[u8],
        _ttl_ms: u64,
    ) -> Result<()> {
        Err(Error::NotImplemented(
            "distributed prewrite is not supported by this MVCC backend",
        ))
    }

    /// Atomically install every intent owned by one tablet participant.
    fn prewrite_batch(
        &mut self,
        _txn_id: TxnId,
        _start_ts: Timestamp,
        _mutations: &BTreeMap<Vec<u8>, Mutation>,
        _primary_key: &[u8],
        _ttl_ms: u64,
    ) -> Result<()> {
        Err(Error::NotImplemented(
            "distributed prewrite batches are not supported by this MVCC backend",
        ))
    }

    /// commit one previously installed distributed transaction intent
    ///
    /// an exact replay succeeds without creating a second write version. A
    /// missing, conflicting, or rolled-back intent fails without mutation
    fn commit_intent(
        &mut self,
        _txn_id: TxnId,
        _start_ts: Timestamp,
        _commit_ts: Timestamp,
        _key: &[u8],
    ) -> Result<()> {
        Err(Error::NotImplemented(
            "distributed intent commit is not supported by this MVCC backend",
        ))
    }

    /// Atomically commit all participant intents or leave all keys unchanged.
    fn commit_intents_batch(
        &mut self,
        _txn_id: TxnId,
        _start_ts: Timestamp,
        _commit_ts: Timestamp,
        _keys: &BTreeSet<Vec<u8>>,
    ) -> Result<()> {
        Err(Error::NotImplemented(
            "distributed intent commit batches are not supported by this MVCC backend",
        ))
    }

    /// roll back one distributed transaction intent and persist its tombstone
    ///
    /// the rollback marker prevents a delayed prewrite or commit from
    /// resurrecting an aborted transaction. Exact replays are idempotent
    fn rollback_intent(&mut self, _txn_id: TxnId, _start_ts: Timestamp, _key: &[u8]) -> Result<()> {
        Err(Error::NotImplemented(
            "distributed intent rollback is not supported by this MVCC backend",
        ))
    }

    /// Atomically roll back all participant intents or leave all keys unchanged.
    fn rollback_intents_batch(
        &mut self,
        _txn_id: TxnId,
        _start_ts: Timestamp,
        _keys: &BTreeSet<Vec<u8>>,
    ) -> Result<()> {
        Err(Error::NotImplemented(
            "distributed intent rollback batches are not supported by this MVCC backend",
        ))
    }

    /// Atomically validate and commit a transaction's complete mutation set.
    ///
    /// No mutation may be applied when validation of any key fails.
    fn commit_batch(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<usize>;

    /// validate a transactions complete mutatin set without changing MVCC state
    ///
    /// this boundary intentionally does not accept a commit timestamp. conflict
    /// validation must finish before the durable commit coordinator allocates
    /// the final visibility timestamp and appends the transction to WAL
    ///
    /// an empty mutation set is valid at this generic storage boundary
    /// the tablet or commit coordinator rejects empty write commits while
    /// handling read only transaction without entering the durable write path
    fn validate_commit_batch(
        &self,
        txn_id: TxnId,
        start_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<()>;

    /// Return current diagnostic counters.
    fn stats(&self) -> MvccStats;
}

/// In-memory implementation of RagnorDB's MVCC maps.
///
/// Mutation methods require exclusive access. A future tablet state-machine
/// actor will own and serialize access to this structure.
#[derive(Debug, Clone, Default)]
pub struct InMemoryMvcc {
    /// `row_key -> start_ts -> encoded row`.
    default: BTreeMap<Vec<u8>, BTreeMap<Timestamp, Vec<u8>>>,

    /// `row_key -> unresolved lock`.
    ///
    /// Locks participate in reads and commit validation. Public distributed
    /// lock creation and resolution belong to Milestone 6.
    locks: BTreeMap<Vec<u8>, LockRecord>,

    /// `row_key -> write_ts -> write record`.
    writes: BTreeMap<Vec<u8>, BTreeMap<Timestamp, WriteRecord>>,
}

/// MVCC state reconstructed from one validated snapshot table
///
/// the observed maxima are compared with the snapshot's declared allocator
/// high-water marks before recovery may publish the restored state
pub(crate) struct RestoredMvccState {
    pub(crate) storage: InMemoryMvcc,
    pub(crate) max_transaction_id: TxnId,
    pub(crate) max_timestamp: Timestamp,
}

impl InMemoryMvcc {
    /// Construct an empty in-memory MVCC engine.
    pub fn new() -> Self {
        Self::default()
    }

    /// clone the complete logical MVCC maps into an immutable snapshot image
    ///
    /// the owning database runtime calls this only while holding its serialized
    /// commit/catalog barrier. Flattening the ordered maps here fixes both the
    /// state image and its deterministic protobuf ordering before that barrier
    /// is released
    pub fn capture_snapshot_state(&self) -> CapturedMvccState {
        let default_values = self
            .default
            .iter()
            .flat_map(|(key, versions)| {
                versions.iter().map(move |(start_timestamp, row)| {
                    snapshot_proto::DefaultValueEntry {
                        key: key.clone(),
                        start_timestamp: Some(start_timestamp.to_proto()),
                        row: row.clone(),
                    }
                })
            })
            .collect();

        let locks = self
            .locks
            .iter()
            .map(|(key, record)| snapshot_proto::LockEntry {
                key: key.clone(),
                record: Some(record.to_proto()),
            })
            .collect();

        let writes = self
            .writes
            .iter()
            .flat_map(|(key, versions)| {
                versions.iter().map(
                    move |(write_timestamp, record)| snapshot_proto::WriteEntry {
                        key: key.clone(),
                        write_timestamp: Some(write_timestamp.to_proto()),
                        record: Some(record.to_proto()),
                    },
                )
            })
            .collect();

        CapturedMvccState::new(default_values, locks, writes)
    }

    /// Reconstruct one table's complete MVCC maps from snapshot entries.
    ///
    /// Duplicate map keys, cross-table row keys, malformed rows, invalid
    /// records, and `Put` writes without their referenced default value are
    /// rejected before the state can enter recovery staging.
    pub(crate) fn from_snapshot_table(
        table_id: TableId,
        table: &snapshot_proto::SnapshotTable,
    ) -> Result<RestoredMvccState> {
        let mut storage = Self::new();
        let mut max_transaction_id = TxnId(0);
        let mut max_timestamp = Timestamp(0);

        for entry in &table.default_values {
            validate_snapshot_row_key(table_id, &entry.key, "default value")?;

            let start_timestamp = entry
                .start_timestamp
                .as_ref()
                .cloned()
                .map(Timestamp::from_proto)
                .ok_or_else(|| {
                    Error::CorruptData(
                        "snapshot default value is missing its start timestamp".to_string(),
                    )
                })?;

            if start_timestamp.0 == 0 {
                return Err(Error::CorruptData(
                    "snapshot default value contains reserved timestamp 0".to_string(),
                ));
            }

            decode_row(&entry.row).map_err(|source| {
                Error::CorruptData(format!(
                    "snapshot default value contains an invalid encoded row: {source}"
                ))
            })?;

            let previous = storage
                .default
                .entry(entry.key.clone())
                .or_default()
                .insert(start_timestamp, entry.row.clone());

            if previous.is_some() {
                return Err(Error::CorruptData(format!(
                    "snapshot contains duplicate default value for table {} at \
                     start timestamp {}",
                    table_id.0, start_timestamp.0
                )));
            }

            max_timestamp = Timestamp(max_timestamp.0.max(start_timestamp.0));
        }

        for entry in &table.locks {
            validate_snapshot_row_key(table_id, &entry.key, "lock")?;

            let record = entry
                .record
                .as_ref()
                .cloned()
                .ok_or_else(|| Error::CorruptData("snapshot lock record is missing".to_string()))
                .and_then(|record| {
                    LockRecord::from_proto(record).map_err(|message| {
                        Error::CorruptData(format!("snapshot lock record is invalid: {message}"))
                    })
                })?;

            if record.txn_id.0 == 0 || record.start_timestamp.0 == 0 {
                return Err(Error::CorruptData(
                    "snapshot lock contains a reserved transaction ID or timestamp 0".to_string(),
                ));
            }

            if storage
                .locks
                .insert(entry.key.clone(), record.clone())
                .is_some()
            {
                return Err(Error::CorruptData(format!(
                    "snapshot contains duplicate lock for table {}",
                    table_id.0
                )));
            }

            max_transaction_id = TxnId(max_transaction_id.0.max(record.txn_id.0));
            max_timestamp = Timestamp(max_timestamp.0.max(record.start_timestamp.0));
        }

        for entry in &table.writes {
            validate_snapshot_row_key(table_id, &entry.key, "write")?;

            let write_timestamp = entry
                .write_timestamp
                .as_ref()
                .cloned()
                .map(Timestamp::from_proto)
                .ok_or_else(|| {
                    Error::CorruptData("snapshot write is missing its write timestamp".to_string())
                })?;
            let record = entry
                .record
                .as_ref()
                .cloned()
                .ok_or_else(|| Error::CorruptData("snapshot write record is missing".to_string()))
                .and_then(|record| {
                    WriteRecord::from_proto(record).map_err(|message| {
                        Error::CorruptData(format!("snapshot write record is invalid: {message}"))
                    })
                })?;

            validate_write_record(write_timestamp, &record)?;

            let previous = storage
                .writes
                .entry(entry.key.clone())
                .or_default()
                .insert(write_timestamp, record.clone());

            if previous.is_some() {
                return Err(Error::CorruptData(format!(
                    "snapshot contains duplicate write for table {} at timestamp {}",
                    table_id.0, write_timestamp.0
                )));
            }

            max_timestamp = Timestamp(
                max_timestamp
                    .0
                    .max(record.start_timestamp.0)
                    .max(write_timestamp.0),
            );
        }

        for (key, versions) in &storage.writes {
            for record in versions.values() {
                if record.op == WriteKind::Put
                    && !storage
                        .default
                        .get(key)
                        .is_some_and(|values| values.contains_key(&record.start_timestamp))
                {
                    return Err(Error::CorruptData(format!(
                        "snapshot Put for table {} references missing default value \
                         at start timestamp {}",
                        table_id.0, record.start_timestamp.0
                    )));
                }
            }
        }

        Ok(RestoredMvccState {
            storage,
            max_transaction_id,
            max_timestamp,
        })
    }

    fn read_visible_version(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Vec<u8>>> {
        if let Some(lock) = self.locks.get(key)
            && lock.start_timestamp <= read_ts
        {
            return Err(Error::WriteConflict(format!(
                "row is locked by transaction {} at start timestamp {}",
                lock.txn_id.0, lock.start_timestamp.0
            )));
        }

        let Some(write_versions) = self.writes.get(key) else {
            return Ok(None);
        };

        for (stored_write_ts, write) in write_versions.range(..=read_ts).rev() {
            validate_write_record(*stored_write_ts, write)?;

            match write.op {
                WriteKind::Put => {
                    let row = self
                        .default
                        .get(key)
                        .and_then(|versions| versions.get(&write.start_timestamp))
                        .ok_or_else(|| {
                            Error::CorruptData(format!(
                                "Put at write timestamp {} references missing default \
                                 value at start timestamp {}",
                                stored_write_ts.0, write.start_timestamp.0
                            ))
                        })?;

                    // Validate persisted bytes at the storage boundary so
                    // damaged row data cannot be returned as committed data.
                    decode_row(row)?;

                    return Ok(Some(row.clone()));
                }

                WriteKind::Delete => {
                    // A Delete is a visible tombstone. Once encountered, older
                    // committed versions remain hidden.
                    return Ok(None);
                }

                WriteKind::Rollback => {
                    // A Rollback describes an aborted transaction rather than
                    // a logical deletion. Continue to older committed records.
                }
            }
        }

        Ok(None)
    }

    fn validate_mutation(
        &self,
        key: &[u8],
        mutation: &Mutation,
        start_ts: Timestamp,
    ) -> Result<()> {
        validate_encoded_key_argument(key, "mutation key")?;

        match mutation {
            Mutation::Put(row) => {
                decode_row(row).map_err(|error| {
                    Error::InvalidArgument(format!(
                        "Put mutation does not contain a canonical encoded row: {error}"
                    ))
                })?;

                if let Some(existing) = self
                    .default
                    .get(key)
                    .and_then(|versions| versions.get(&start_ts))
                    && existing != row
                {
                    return Err(Error::CorruptData(format!(
                        "start timestamp {} already has a different default value",
                        start_ts.0
                    )));
                }
            }

            Mutation::Delete => {
                if self
                    .default
                    .get(key)
                    .is_some_and(|versions| versions.contains_key(&start_ts))
                {
                    return Err(Error::CorruptData(format!(
                        "Delete at start timestamp {} conflicts with an existing \
                         default value for the same transaction",
                        start_ts.0
                    )));
                }
            }
        }

        Ok(())
    }

    fn validate_lock(
        &self,
        key: &[u8],
        mutation: &Mutation,
        txn_id: TxnId,
        start_ts: Timestamp,
    ) -> Result<()> {
        let Some(lock) = self.locks.get(key) else {
            return Ok(());
        };

        if lock.txn_id != txn_id || lock.start_timestamp != start_ts {
            return Err(Error::WriteConflict(format!(
                "row is locked by transaction {} at start timestamp {}",
                lock.txn_id.0, lock.start_timestamp.0
            )));
        }

        if lock.op != mutation.write_kind() {
            return Err(Error::CorruptData(format!(
                "transaction {} has a lock operation inconsistent with its mutation",
                txn_id.0
            )));
        }

        Ok(())
    }

    fn validate_write_history(&self, key: &[u8], start_ts: Timestamp) -> Result<()> {
        let Some(write_versions) = self.writes.get(key) else {
            return Ok(());
        };

        for (stored_write_ts, write) in write_versions {
            validate_write_record(*stored_write_ts, write)?;

            if write.op == WriteKind::Rollback && write.start_timestamp == start_ts {
                return Err(Error::WriteConflict(format!(
                    "transaction starting at timestamp {} was already rolled back",
                    start_ts.0
                )));
            }
        }

        if let Some((conflicting_write_ts, _)) = write_versions
            .range((Excluded(start_ts), Unbounded))
            .rev()
            .find(|(_, write)| write.op != WriteKind::Rollback)
        {
            return Err(Error::WriteConflict(format!(
                "row was modified at timestamp {} after transaction start \
             timestamp {}",
                conflicting_write_ts.0, start_ts.0
            )));
        }

        Ok(())
    }

    /// the finalized commit timestamp can be inserted after every existing
    /// write map entry
    ///
    /// snapshot conflicts are checked by `validate_write_history` before timestamp
    /// allocation. This second check protects the physical MVCC ordering invariant
    /// when applying an already finalized durable commit
    fn validate_commit_timestamp(&self, key: &[u8], commit_ts: Timestamp) -> Result<()> {
        let Some(write_versions) = self.writes.get(key) else {
            return Ok(());
        };

        if let Some((latest_write_ts, _)) = write_versions.last_key_value()
            && *latest_write_ts >= commit_ts
        {
            return Err(Error::WriteConflict(format!(
                "commit timestamp {} does not advance the row's latest write \
             timestamp {}",
                commit_ts.0, latest_write_ts.0
            )));
        }

        Ok(())
    }

    fn is_exactly_applied(
        &self,
        key: &[u8],
        mutation: &Mutation,
        start_ts: Timestamp,
        commit_ts: Timestamp,
    ) -> Result<bool> {
        let Some(write) = self
            .writes
            .get(key)
            .and_then(|versions| versions.get(&commit_ts))
        else {
            return Ok(false);
        };

        validate_write_record(commit_ts, write)?;

        if write.start_timestamp != start_ts || write.op != mutation.write_kind() {
            return Err(Error::CorruptData(format!(
                "write timestamp {} is already occupied by a different write",
                commit_ts.0
            )));
        }

        if let Mutation::Put(expected_row) = mutation {
            let stored_row = self
                .default
                .get(key)
                .and_then(|versions| versions.get(&start_ts))
                .ok_or_else(|| {
                    Error::CorruptData(format!(
                        "replayed Put at timestamp {} has no default value",
                        commit_ts.0
                    ))
                })?;

            if stored_row != expected_row {
                return Err(Error::CorruptData(format!(
                    "replayed Put at timestamp {} references different row bytes",
                    commit_ts.0
                )));
            }
        }

        Ok(true)
    }

    fn validate_batch_replay(
        &self,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
        start_ts: Timestamp,
        commit_ts: Timestamp,
    ) -> Result<bool> {
        let mut applied = 0;

        for (key, mutation) in mutations {
            if self.is_exactly_applied(key, mutation, start_ts, commit_ts)? {
                applied += 1;
            }
        }

        if applied == 0 {
            return Ok(false);
        }

        if applied == mutations.len() {
            return Ok(true);
        }

        Err(Error::CorruptData(format!(
            "transaction at start timestamp {} is only partially present at \
             write timestamp {}",
            start_ts.0, commit_ts.0
        )))
    }

    /// restore one tablet's complete MVCC state from validated snapshot entries
    ///
    /// the tablet owns its table identity separately from the catalog definition,
    /// so this path intentionally restores only the MVCC records required by the
    /// tablet state machine
    pub fn restore_from_snapshot_entries(
        table_id: TableId,
        default_values: Vec<snapshot_proto::DefaultValueEntry>,
        locks: Vec<snapshot_proto::LockEntry>,
        writes: Vec<snapshot_proto::WriteEntry>,
    ) -> Result<Self> {
        let table = snapshot_proto::SnapshotTable {
            definition: None,
            default_values,
            locks,
            writes,
        };

        Ok(Self::from_snapshot_table(table_id, &table)?.storage)
    }
}

impl MvccStorage for InMemoryMvcc {
    fn read(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Vec<u8>>> {
        validate_encoded_key_argument(key, "read key")?;
        self.read_visible_version(key, read_ts)
    }

    fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        read_ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (lower, upper) = encoded_scan_bounds(start, end)?;

        // Locks must participate even when their key has no committed history.
        // Otherwise a scan could silently pass a locked insertion.
        let mut candidates = BTreeSet::new();

        for (key, _) in self.writes.range((lower.clone(), upper.clone())) {
            candidates.insert(key.clone());
        }

        for (key, _) in self.locks.range((lower, upper)) {
            candidates.insert(key.clone());
        }

        let mut rows = Vec::new();

        for key in candidates {
            if let Some(row) = self.read_visible_version(&key, read_ts)? {
                rows.push((key, row));
            }
        }

        Ok(rows)
    }

    fn validate_commit_batch(
        &self,
        txn_id: TxnId,
        start_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<()> {
        validate_commit_preflight_metadata(txn_id, start_ts)?;

        // this will validate the entire representation before consulting conflict state
        for (key, mutation) in mutations {
            self.validate_mutation(key, mutation, start_ts)?;
        }

        for (key, mutation) in mutations {
            self.validate_lock(key, mutation, txn_id, start_ts)?;
            self.validate_write_history(key, start_ts)?;
        }

        Ok(())
    }

    fn commit_batch(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<usize> {
        validate_commit_metadata(txn_id, start_ts, commit_ts)?;

        // validate persisted representations before considering an idempotent
        // replay; corrupt replay input must never be accepted merely because a
        // matching timestamp exists in the write map
        for (key, mutation) in mutations {
            self.validate_mutation(key, mutation, start_ts)?;
        }

        if mutations.is_empty() {
            return Ok(0);
        }

        // identical batch is a safe deterministic replay A
        // partially present batch is impossible after atomic application and
        // therefore represents corrupted state
        if self.validate_batch_replay(mutations, start_ts, commit_ts)? {
            return Ok(mutations.len());
        }

        self.validate_commit_batch(txn_id, start_ts, mutations)?;

        // commit timestamps are finalized only after snapshot-conflict
        // preflight; Validate their insertion position before changing any map
        for key in mutations.keys() {
            self.validate_commit_timestamp(key, commit_ts)?;
        }

        // Every fallible validation step has completed. The following section
        // performs the complete in-memory state transition without exposing a
        // partially applied mutation batch.
        for (key, mutation) in mutations {
            match mutation {
                Mutation::Put(row) => {
                    self.default
                        .entry(key.clone())
                        .or_default()
                        .insert(start_ts, row.clone());
                }

                Mutation::Delete => {
                    // Deletes have no default payload. Their committed write
                    // record is the tombstone.
                }
            }

            self.writes.entry(key.clone()).or_default().insert(
                commit_ts,
                WriteRecord {
                    start_timestamp: start_ts,
                    commit_timestamp: commit_ts,
                    op: mutation.write_kind(),
                },
            );

            if self
                .locks
                .get(key)
                .is_some_and(|lock| lock.txn_id == txn_id && lock.start_timestamp == start_ts)
            {
                self.locks.remove(key);
            }
        }

        Ok(mutations.len())
    }

    fn stats(&self) -> MvccStats {
        MvccStats {
            default_keys: self.default.len(),
            default_versions: self.default.values().map(BTreeMap::len).sum(),
            locks: self.locks.len(),
            write_keys: self.writes.len(),
            write_records: self.writes.values().map(BTreeMap::len).sum(),
        }
    }

    fn prewrite(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        key: &[u8],
        mutation: &Mutation,
        primary_key: &[u8],
        ttl_ms: u64,
    ) -> Result<()> {
        validate_commit_preflight_metadata(txn_id, start_ts)?;
        validate_encoded_key_argument(primary_key, "prewrite primary key")?;

        if ttl_ms == 0 {
            return Err(Error::InvalidArgument(
                "prewrite lock TTL must be non-zero".to_string(),
            ));
        }

        self.validate_mutation(key, mutation, start_ts)?;
        self.validate_write_history(key, start_ts)?;

        let expected_lock = LockRecord {
            txn_id,
            primary_key: primary_key.to_vec(),
            start_timestamp: start_ts,
            ttl_ms,
            op: mutation.write_kind(),
        };

        if let Some(existing_lock) = self.locks.get(key) {
            if existing_lock.txn_id != txn_id || existing_lock.start_timestamp != start_ts {
                return Err(Error::WriteConflict(format!(
                    "row is locked by transaction {} at start timestamp {}",
                    existing_lock.txn_id.0, existing_lock.start_timestamp.0
                )));
            }

            if existing_lock != &expected_lock {
                return Err(Error::CorruptData(format!(
                    "transaction {} replayed prewrite with different lock metadata",
                    txn_id.0
                )));
            }

            if let Mutation::Put(expected_row) = mutation {
                let stored_row = self
                    .default
                    .get(key)
                    .and_then(|versions| versions.get(&start_ts))
                    .ok_or_else(|| {
                        Error::CorruptData(format!(
                            "transaction {} has a Put lock without its default value",
                            txn_id.0
                        ))
                    })?;

                if stored_row != expected_row {
                    return Err(Error::CorruptData(format!(
                        "transaction {} replayed prewrite with different row bytes",
                        txn_id.0
                    )));
                }
            }

            return Ok(());
        }

        if self
            .default
            .get(key)
            .is_some_and(|versions| versions.contains_key(&start_ts))
        {
            return Err(Error::CorruptData(format!(
                "transaction {} has a default value without its prewrite lock",
                txn_id.0
            )));
        }

        // All validation is complete. The following insertions are infallible
        // in-memory mutations and publish the value and lock as one operation.
        if let Mutation::Put(row) = mutation {
            self.default
                .entry(key.to_vec())
                .or_default()
                .insert(start_ts, row.clone());
        }

        self.locks.insert(key.to_vec(), expected_lock);

        Ok(())
    }

    fn prewrite_batch(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
        primary_key: &[u8],
        ttl_ms: u64,
    ) -> Result<()> {
        if mutations.is_empty() {
            return Err(Error::InvalidArgument(
                "distributed prewrite batch must contain at least one mutation".to_string(),
            ));
        }
        let mut staged = self.clone();
        for (key, mutation) in mutations {
            staged.prewrite(txn_id, start_ts, key, mutation, primary_key, ttl_ms)?;
        }
        *self = staged;
        Ok(())
    }

    fn commit_intent(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        key: &[u8],
    ) -> Result<()> {
        validate_commit_metadata(txn_id, start_ts, commit_ts)?;
        validate_encoded_key_argument(key, "intent commit key")?;

        // A committed write is the durable replay witness after its lock has
        // been removed. Validate the complete referenced state before treating
        // the command as an idempotent success.
        if let Some(write) = self
            .writes
            .get(key)
            .and_then(|versions| versions.get(&commit_ts))
        {
            validate_write_record(commit_ts, write)?;

            if let Some(versions) = self.writes.get(key) {
                for (other_write_ts, other) in versions {
                    validate_write_record(*other_write_ts, other)?;
                    if *other_write_ts != commit_ts && other.start_timestamp == start_ts {
                        return Err(Error::CorruptData(format!(
                            "transaction starting at timestamp {} has multiple durable outcomes at write timestamps {} and {}",
                            start_ts.0, commit_ts.0, other_write_ts.0
                        )));
                    }
                }
            }

            if write.start_timestamp != start_ts || write.op == WriteKind::Rollback {
                return Err(Error::CorruptData(format!(
                    "write timestamp {} is occupied by another transaction outcome",
                    commit_ts.0
                )));
            }

            if self
                .locks
                .get(key)
                .is_some_and(|lock| lock.start_timestamp == start_ts)
            {
                return Err(Error::CorruptData(format!(
                    "committed transaction at timestamp {} still has an intent lock",
                    commit_ts.0
                )));
            }

            match write.op {
                WriteKind::Put => {
                    let row = self
                        .default
                        .get(key)
                        .and_then(|versions| versions.get(&start_ts))
                        .ok_or_else(|| {
                            Error::CorruptData(format!(
                                "committed Put at timestamp {} has no default value",
                                commit_ts.0
                            ))
                        })?;

                    decode_row(row)?;
                }

                WriteKind::Delete => {
                    if self
                        .default
                        .get(key)
                        .is_some_and(|versions| versions.contains_key(&start_ts))
                    {
                        return Err(Error::CorruptData(format!(
                            "committed Delete at timestamp {} retains a default value",
                            commit_ts.0
                        )));
                    }
                }

                WriteKind::Rollback => unreachable!("handled above"),
            }

            return Ok(());
        }

        let lock = self.locks.get(key).cloned().ok_or_else(|| {
            Error::WriteConflict(format!(
                "transaction {} has no intent to commit at start timestamp {}",
                txn_id.0, start_ts.0
            ))
        })?;

        if lock.txn_id != txn_id || lock.start_timestamp != start_ts {
            return Err(Error::WriteConflict(format!(
                "row is locked by transaction {} at start timestamp {}",
                lock.txn_id.0, lock.start_timestamp.0
            )));
        }

        let mutation = match lock.op {
            WriteKind::Put => {
                let row = self
                    .default
                    .get(key)
                    .and_then(|versions| versions.get(&start_ts))
                    .ok_or_else(|| {
                        Error::CorruptData(format!(
                            "transaction {} has a Put lock without its default value",
                            txn_id.0
                        ))
                    })?
                    .clone();

                decode_row(&row)?;
                Mutation::Put(row)
            }

            WriteKind::Delete => {
                if self
                    .default
                    .get(key)
                    .is_some_and(|versions| versions.contains_key(&start_ts))
                {
                    return Err(Error::CorruptData(format!(
                        "transaction {} has a Delete lock with a default value",
                        txn_id.0
                    )));
                }

                Mutation::Delete
            }

            WriteKind::Rollback => {
                return Err(Error::CorruptData(format!(
                    "transaction {} has an invalid Rollback intent lock",
                    txn_id.0
                )));
            }
        };

        let mutations = BTreeMap::from([(key.to_vec(), mutation)]);
        self.commit_batch(txn_id, start_ts, commit_ts, &mutations)?;

        Ok(())
    }

    fn commit_intents_batch(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        keys: &BTreeSet<Vec<u8>>,
    ) -> Result<()> {
        if keys.is_empty() {
            return Err(Error::InvalidArgument(
                "distributed commit batch must contain at least one key".to_string(),
            ));
        }
        let mut staged = self.clone();
        for key in keys {
            staged.commit_intent(txn_id, start_ts, commit_ts, key)?;
        }
        *self = staged;
        Ok(())
    }

    fn rollback_intent(&mut self, txn_id: TxnId, start_ts: Timestamp, key: &[u8]) -> Result<()> {
        validate_commit_preflight_metadata(txn_id, start_ts)?;
        validate_encoded_key_argument(key, "intent rollback key")?;

        if let Some(write_versions) = self.writes.get(key) {
            for (stored_write_ts, write) in write_versions {
                validate_write_record(*stored_write_ts, write)?;

                if write.start_timestamp == start_ts && write.op != WriteKind::Rollback {
                    return Err(Error::WriteConflict(format!(
                        "transaction starting at timestamp {} is already committed",
                        start_ts.0
                    )));
                }
            }

            if let Some(rollback) = write_versions.get(&start_ts) {
                if rollback.start_timestamp != start_ts || rollback.op != WriteKind::Rollback {
                    return Err(Error::CorruptData(format!(
                        "write timestamp {} is occupied by a non-rollback outcome",
                        start_ts.0
                    )));
                }

                if self
                    .locks
                    .get(key)
                    .is_some_and(|lock| lock.start_timestamp == start_ts)
                    || self
                        .default
                        .get(key)
                        .is_some_and(|versions| versions.contains_key(&start_ts))
                {
                    return Err(Error::CorruptData(format!(
                        "rolled-back transaction at timestamp {} retains intent state",
                        start_ts.0
                    )));
                }

                return Ok(());
            }
        }

        let locked_op = match self.locks.get(key) {
            Some(lock) if lock.txn_id != txn_id || lock.start_timestamp != start_ts => {
                return Err(Error::WriteConflict(format!(
                    "row is locked by transaction {} at start timestamp {}",
                    lock.txn_id.0, lock.start_timestamp.0
                )));
            }

            Some(lock) => Some(lock.op),
            None => None,
        };

        match locked_op {
            Some(WriteKind::Put) => {
                let row = self
                    .default
                    .get(key)
                    .and_then(|versions| versions.get(&start_ts))
                    .ok_or_else(|| {
                        Error::CorruptData(format!(
                            "transaction {} has a Put lock without its default value",
                            txn_id.0
                        ))
                    })?;

                decode_row(row)?;
            }

            Some(WriteKind::Delete) | None => {
                if self
                    .default
                    .get(key)
                    .is_some_and(|versions| versions.contains_key(&start_ts))
                {
                    return Err(Error::CorruptData(format!(
                        "transaction {} has a default value without a matching Put lock",
                        txn_id.0
                    )));
                }
            }

            Some(WriteKind::Rollback) => {
                return Err(Error::CorruptData(format!(
                    "transaction {} has an invalid Rollback intent lock",
                    txn_id.0
                )));
            }
        }

        // All validation is complete. Remove the intent state and publish the
        // rollback marker as one serialized in-memory transition.
        if locked_op == Some(WriteKind::Put) {
            let remove_key = if let Some(versions) = self.default.get_mut(key) {
                versions.remove(&start_ts);
                versions.is_empty()
            } else {
                false
            };

            if remove_key {
                self.default.remove(key);
            }
        }

        self.locks.remove(key);

        self.writes.entry(key.to_vec()).or_default().insert(
            start_ts,
            WriteRecord {
                start_timestamp: start_ts,
                commit_timestamp: start_ts,
                op: WriteKind::Rollback,
            },
        );

        Ok(())
    }

    fn rollback_intents_batch(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        keys: &BTreeSet<Vec<u8>>,
    ) -> Result<()> {
        if keys.is_empty() {
            return Err(Error::InvalidArgument(
                "distributed rollback batch must contain at least one key".to_string(),
            ));
        }
        let mut staged = self.clone();
        for key in keys {
            staged.rollback_intent(txn_id, start_ts, key)?;
        }
        *self = staged;
        Ok(())
    }
}

fn validate_snapshot_row_key(table_id: TableId, key: &[u8], context: &str) -> Result<()> {
    let row_key = decode_row_key(key).map_err(|source| {
        Error::CorruptData(format!(
            "snapshot {context} has an invalid row key: {source}"
        ))
    })?;

    if row_key.table_id != table_id {
        return Err(Error::CorruptData(format!(
            "snapshot {context} row key belongs to table {}, expected table {}",
            row_key.table_id.0, table_id.0
        )));
    }

    Ok(())
}

fn validate_commit_preflight_metadata(txn_id: TxnId, start_ts: Timestamp) -> Result<()> {
    if txn_id.0 == 0 {
        return Err(Error::InvalidArgument(
            "transaction ID 0 is reserved".to_string(),
        ));
    }

    if start_ts.0 == 0 {
        return Err(Error::InvalidArgument(
            "transaction start timestamp 0 is reserved".to_string(),
        ));
    }

    Ok(())
}

fn validate_commit_metadata(
    txn_id: TxnId,
    start_ts: Timestamp,
    commit_ts: Timestamp,
) -> Result<()> {
    validate_commit_preflight_metadata(txn_id, start_ts)?;

    if commit_ts.0 == 0 {
        return Err(Error::InvalidArgument(
            "transaction commit timestamp 0 is reserved".to_string(),
        ));
    }

    if commit_ts <= start_ts {
        return Err(Error::InvalidArgument(format!(
            "commit timestamp {} must be greater than start timestamp {}",
            commit_ts.0, start_ts.0
        )));
    }

    Ok(())
}

/// Validate one record against the timestamp used as its write-map key.
///
/// `Put` and `Delete` use a true commit timestamp. `Rollback` uses `start_ts`
/// because the rollback command does not allocate or carry a commit timestamp.
fn validate_write_record(stored_write_ts: Timestamp, write: &WriteRecord) -> Result<()> {
    if write.commit_timestamp != stored_write_ts {
        return Err(Error::CorruptData(format!(
            "write-map timestamp {} does not match record timestamp {}",
            stored_write_ts.0, write.commit_timestamp.0
        )));
    }

    if write.start_timestamp.0 == 0 {
        return Err(Error::CorruptData(
            "write record contains reserved start timestamp 0".to_string(),
        ));
    }

    match write.op {
        WriteKind::Put | WriteKind::Delete => {
            if write.commit_timestamp <= write.start_timestamp {
                return Err(Error::CorruptData(format!(
                    "committed write timestamp {} does not follow start \
                     timestamp {}",
                    write.commit_timestamp.0, write.start_timestamp.0
                )));
            }
        }

        WriteKind::Rollback => {
            if write.commit_timestamp != write.start_timestamp {
                return Err(Error::CorruptData(format!(
                    "rollback record timestamp {} must equal start timestamp {}",
                    write.commit_timestamp.0, write.start_timestamp.0
                )));
            }
        }
    }

    Ok(())
}

fn validate_encoded_key_argument(key: &[u8], context: &str) -> Result<()> {
    decode_row_key(key).map(|_| ()).map_err(|error| {
        Error::InvalidArgument(format!(
            "{context} is not a canonical encoded row key: {error}"
        ))
    })
}

fn encoded_scan_bounds(start: Option<&[u8]>, end: Option<&[u8]>) -> Result<EncodedScanBounds> {
    if let Some(start) = start {
        validate_encoded_key_argument(start, "scan start key")?;
    }

    if let Some(end) = end {
        validate_encoded_key_argument(end, "scan end key")?;
    }

    if let (Some(start), Some(end)) = (start, end)
        && start >= end
    {
        return Err(Error::InvalidArgument(
            "scan start key must be less than scan end key".to_string(),
        ));
    }

    Ok((
        start.map_or(Unbounded, |key| Included(key.to_vec())),
        end.map_or(Unbounded, |key| Excluded(key.to_vec())),
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

    use crate::key::{encode_row_key, make_row_key};

    fn encoded_key(id: i64) -> Vec<u8> {
        encode_row_key(&make_row_key(TableId(1), &[Value::Int(id)]).unwrap()).unwrap()
    }

    fn encoded_row(id: i64, name: &str) -> Vec<u8> {
        encode_row(&Row {
            values: vec![Value::Int(id), Value::Text(name.to_string())],
        })
        .unwrap()
    }

    fn put_batch(key: Vec<u8>, row: Vec<u8>) -> BTreeMap<Vec<u8>, Mutation> {
        BTreeMap::from([(key, Mutation::Put(row))])
    }

    fn delete_batch(key: Vec<u8>) -> BTreeMap<Vec<u8>, Mutation> {
        BTreeMap::from([(key, Mutation::Delete)])
    }

    #[test]
    fn snapshot_reads_select_latest_visible_version() {
        let key = encoded_key(1);
        let first = encoded_row(1, "first");
        let second = encoded_row(1, "second");
        let mut engine = InMemoryMvcc::new();

        engine
            .commit_batch(
                TxnId(1),
                Timestamp(1),
                Timestamp(2),
                &put_batch(key.clone(), first.clone()),
            )
            .unwrap();

        engine
            .commit_batch(
                TxnId(2),
                Timestamp(3),
                Timestamp(4),
                &put_batch(key.clone(), second.clone()),
            )
            .unwrap();

        assert_eq!(engine.read(&key, Timestamp(1)).unwrap(), None);
        assert_eq!(
            engine.read(&key, Timestamp(2)).unwrap(),
            Some(first.clone())
        );
        assert_eq!(engine.read(&key, Timestamp(3)).unwrap(), Some(first));
        assert_eq!(engine.read(&key, Timestamp(4)).unwrap(), Some(second));
    }

    #[test]
    fn delete_tombstone_hides_only_newer_snapshots() {
        let key = encoded_key(1);
        let row = encoded_row(1, "visible");
        let mut engine = InMemoryMvcc::new();

        engine
            .commit_batch(
                TxnId(1),
                Timestamp(1),
                Timestamp(2),
                &put_batch(key.clone(), row.clone()),
            )
            .unwrap();

        engine
            .commit_batch(
                TxnId(2),
                Timestamp(3),
                Timestamp(4),
                &delete_batch(key.clone()),
            )
            .unwrap();

        assert_eq!(engine.read(&key, Timestamp(3)).unwrap(), Some(row));
        assert_eq!(engine.read(&key, Timestamp(4)).unwrap(), None);
    }

    #[test]
    fn rollback_is_stored_at_start_timestamp_and_skipped_by_reads() {
        let key = encoded_key(1);
        let original = encoded_row(1, "original");
        let delayed = encoded_row(1, "delayed");
        let mut engine = InMemoryMvcc::new();

        engine
            .commit_batch(
                TxnId(1),
                Timestamp(1),
                Timestamp(2),
                &put_batch(key.clone(), original.clone()),
            )
            .unwrap();

        engine.writes.entry(key.clone()).or_default().insert(
            Timestamp(3),
            WriteRecord {
                start_timestamp: Timestamp(3),
                commit_timestamp: Timestamp(3),
                op: WriteKind::Rollback,
            },
        );

        assert_eq!(engine.read(&key, Timestamp(3)).unwrap(), Some(original));

        let error = engine
            .commit_batch(
                TxnId(2),
                Timestamp(3),
                Timestamp(5),
                &put_batch(key, delayed),
            )
            .unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));
    }

    #[test]
    fn malformed_rollback_timestamp_is_corruption() {
        let key = encoded_key(1);
        let mut engine = InMemoryMvcc::new();

        engine.writes.entry(key.clone()).or_default().insert(
            Timestamp(4),
            WriteRecord {
                start_timestamp: Timestamp(3),
                commit_timestamp: Timestamp(4),
                op: WriteKind::Rollback,
            },
        );

        let error = engine.read(&key, Timestamp(4)).unwrap_err();

        assert!(matches!(error, Error::CorruptData(_)));
    }

    #[test]
    fn write_conflict_rejects_entire_batch() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        let winner = encoded_row(2, "winner");
        let mut engine = InMemoryMvcc::new();

        engine
            .commit_batch(
                TxnId(2),
                Timestamp(4),
                Timestamp(5),
                &put_batch(second_key.clone(), winner.clone()),
            )
            .unwrap();

        let losing_batch = BTreeMap::from([
            (first_key.clone(), Mutation::Put(encoded_row(1, "loser"))),
            (second_key.clone(), Mutation::Put(encoded_row(2, "loser"))),
        ]);

        let error = engine
            .commit_batch(TxnId(1), Timestamp(3), Timestamp(6), &losing_batch)
            .unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));
        assert_eq!(engine.read(&first_key, Timestamp(6)).unwrap(), None);
        assert_eq!(
            engine.read(&second_key, Timestamp(6)).unwrap(),
            Some(winner)
        );
    }

    #[test]
    fn visible_lock_causes_retryable_conflict() {
        let key = encoded_key(1);
        let mut engine = InMemoryMvcc::new();

        engine.locks.insert(
            key.clone(),
            LockRecord {
                txn_id: TxnId(9),
                primary_key: key.clone(),
                start_timestamp: Timestamp(5),
                ttl_ms: 3_000,
                op: WriteKind::Put,
            },
        );

        assert_eq!(engine.read(&key, Timestamp(4)).unwrap(), None);

        let error = engine.read(&key, Timestamp(5)).unwrap_err();
        assert!(matches!(error, Error::WriteConflict(_)));
    }

    #[test]
    fn same_transaction_lock_is_removed_after_commit() {
        let key = encoded_key(1);
        let row = encoded_row(1, "value");
        let mut engine = InMemoryMvcc::new();

        engine.locks.insert(
            key.clone(),
            LockRecord {
                txn_id: TxnId(1),
                primary_key: key.clone(),
                start_timestamp: Timestamp(1),
                ttl_ms: 3_000,
                op: WriteKind::Put,
            },
        );

        engine
            .commit_batch(
                TxnId(1),
                Timestamp(1),
                Timestamp(2),
                &put_batch(key.clone(), row.clone()),
            )
            .unwrap();

        assert_eq!(engine.stats().locks, 0);
        assert_eq!(engine.read(&key, Timestamp(2)).unwrap(), Some(row));
    }

    #[test]
    fn scan_is_ordered_and_includes_lock_only_candidates() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        let third_key = encoded_key(3);
        let first_row = encoded_row(1, "first");
        let third_row = encoded_row(3, "third");
        let mut engine = InMemoryMvcc::new();

        let committed = BTreeMap::from([
            (third_key.clone(), Mutation::Put(third_row.clone())),
            (first_key.clone(), Mutation::Put(first_row.clone())),
        ]);

        engine
            .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &committed)
            .unwrap();

        assert_eq!(
            engine.scan(None, None, Timestamp(2)).unwrap(),
            vec![
                (first_key.clone(), first_row),
                (third_key.clone(), third_row),
            ]
        );

        engine.locks.insert(
            second_key.clone(),
            LockRecord {
                txn_id: TxnId(2),
                primary_key: second_key,
                start_timestamp: Timestamp(3),
                ttl_ms: 3_000,
                op: WriteKind::Put,
            },
        );

        let error = engine.scan(None, None, Timestamp(3)).unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));
    }

    #[test]
    fn scan_respects_half_open_bounds() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        let third_key = encoded_key(3);
        let mut engine = InMemoryMvcc::new();

        let mutations = BTreeMap::from([
            (first_key, Mutation::Put(encoded_row(1, "first"))),
            (second_key.clone(), Mutation::Put(encoded_row(2, "second"))),
            (third_key.clone(), Mutation::Put(encoded_row(3, "third"))),
        ]);

        engine
            .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations)
            .unwrap();

        let rows = engine
            .scan(Some(&second_key), Some(&third_key), Timestamp(2))
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, second_key);
    }

    #[test]
    fn scan_rejects_invalid_order() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        let engine = InMemoryMvcc::new();

        let error = engine
            .scan(Some(&second_key), Some(&first_key), Timestamp(1))
            .unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));

        let error = engine
            .scan(Some(&first_key), Some(&first_key), Timestamp(1))
            .unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
    }

    #[test]
    fn malformed_mutation_key_is_rejected() {
        let mut engine = InMemoryMvcc::new();
        let mutations = BTreeMap::from([(vec![0xff], Mutation::Put(encoded_row(1, "value")))]);

        let error = engine
            .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations)
            .unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert_eq!(engine.stats(), MvccStats::default());
    }

    #[test]
    fn malformed_put_row_is_rejected() {
        let mut engine = InMemoryMvcc::new();
        let mutations = BTreeMap::from([(encoded_key(1), Mutation::Put(vec![0xff]))]);

        let error = engine
            .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations)
            .unwrap_err();

        assert!(matches!(error, Error::InvalidArgument(_)));
        assert_eq!(engine.stats(), MvccStats::default());
    }

    #[test]
    fn exact_multi_key_replay_is_idempotent() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        let mutations = BTreeMap::from([
            (first_key, Mutation::Put(encoded_row(1, "first"))),
            (second_key, Mutation::Put(encoded_row(2, "second"))),
        ]);
        let mut engine = InMemoryMvcc::new();

        assert_eq!(
            engine
                .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations,)
                .unwrap(),
            2
        );

        assert_eq!(
            engine
                .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations,)
                .unwrap(),
            2
        );

        assert_eq!(engine.stats().default_versions, 2);
        assert_eq!(engine.stats().write_records, 2);
    }

    #[test]
    fn partial_replay_is_reported_as_corruption() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        let mutations = BTreeMap::from([
            (first_key, Mutation::Put(encoded_row(1, "first"))),
            (second_key.clone(), Mutation::Put(encoded_row(2, "second"))),
        ]);
        let mut engine = InMemoryMvcc::new();

        engine
            .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations)
            .unwrap();

        engine
            .writes
            .get_mut(&second_key)
            .unwrap()
            .remove(&Timestamp(2));

        let error = engine
            .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations)
            .unwrap_err();

        assert!(matches!(error, Error::CorruptData(_)));
    }

    #[test]
    fn missing_default_value_is_corruption() {
        let key = encoded_key(1);
        let mut engine = InMemoryMvcc::new();

        engine.writes.entry(key.clone()).or_default().insert(
            Timestamp(2),
            WriteRecord {
                start_timestamp: Timestamp(1),
                commit_timestamp: Timestamp(2),
                op: WriteKind::Put,
            },
        );

        let error = engine.read(&key, Timestamp(2)).unwrap_err();

        assert!(matches!(error, Error::CorruptData(_)));
    }

    #[test]
    fn commit_requires_monotonic_nonzero_metadata() {
        let batch = put_batch(encoded_key(1), encoded_row(1, "value"));
        let mut engine = InMemoryMvcc::new();

        assert!(matches!(
            engine
                .commit_batch(TxnId(0), Timestamp(1), Timestamp(2), &batch,)
                .unwrap_err(),
            Error::InvalidArgument(_)
        ));

        assert!(matches!(
            engine
                .commit_batch(TxnId(1), Timestamp(2), Timestamp(2), &batch,)
                .unwrap_err(),
            Error::InvalidArgument(_)
        ));
    }

    /// Ensures an unresolved lock owned by another transaction is discovered
    /// during the mutation-free commit preflight.
    ///
    /// Realistic bug caught:
    ///
    /// Deferring lock validation until MVCC application could leave a durable WAL
    /// record for a transaction that must lose to an existing lock owner.
    #[test]
    fn preflight_rejects_conflicting_lock_without_mutating_state() {
        let key = encoded_key(1);
        let row = encoded_row(1, "pending");
        let mut engine = InMemoryMvcc::new();

        engine.locks.insert(
            key.clone(),
            LockRecord {
                txn_id: TxnId(9),
                primary_key: key.clone(),
                start_timestamp: Timestamp(1),
                ttl_ms: 3_000,
                op: WriteKind::Put,
            },
        );

        let mutations = put_batch(key, row);
        let stats_before = engine.stats();

        let error = engine
            .validate_commit_batch(TxnId(1), Timestamp(1), &mutations)
            .unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));
        assert_eq!(engine.stats(), stats_before);
    }

    /// Ensures a durable rollback marker prevents the corresponding transaction
    /// from being accepted during commit preflight.
    ///
    /// Realistic bug caught:
    ///
    /// A delayed commit request could otherwise be appended after recovery had
    /// already established that the transaction was rolled back.
    #[test]
    fn preflight_rejects_transaction_with_rollback_record() {
        let key = encoded_key(1);
        let mut engine = InMemoryMvcc::new();

        engine.writes.entry(key.clone()).or_default().insert(
            Timestamp(1),
            WriteRecord {
                start_timestamp: Timestamp(1),
                commit_timestamp: Timestamp(1),
                op: WriteKind::Rollback,
            },
        );

        let mutations = put_batch(key, encoded_row(1, "delayed"));
        let stats_before = engine.stats();

        let error = engine
            .validate_commit_batch(TxnId(1), Timestamp(1), &mutations)
            .unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));
        assert_eq!(engine.stats(), stats_before);
    }

    /// Realistic bug caught: an exact commit replay sees its requested write
    /// record and returns success without noticing another durable outcome for
    /// the same transaction start timestamp.
    #[test]
    fn commit_replay_rejects_multiple_durable_outcomes_for_start_timestamp() {
        let key = encoded_key(1);
        let row = encoded_row(1, "committed");
        let mut engine = InMemoryMvcc::new();
        engine
            .prewrite(
                TxnId(44),
                Timestamp(100),
                &key,
                &Mutation::Put(row),
                &key,
                3_000,
            )
            .unwrap();
        engine
            .commit_intent(TxnId(44), Timestamp(100), Timestamp(110), &key)
            .unwrap();

        engine.writes.entry(key.clone()).or_default().insert(
            Timestamp(100),
            WriteRecord {
                start_timestamp: Timestamp(100),
                commit_timestamp: Timestamp(100),
                op: WriteKind::Rollback,
            },
        );

        let error = engine
            .commit_intent(TxnId(44), Timestamp(100), Timestamp(110), &key)
            .unwrap_err();
        assert!(matches!(error, Error::CorruptData(_)));
    }
}
