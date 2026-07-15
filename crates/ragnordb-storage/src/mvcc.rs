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
//! this commits buffered single tablet transaction directly after
//! validating the entire batch.
//! Distributed prewrite, lock resolution, transction status records
//! Raft application, WAL durability, and garbage collection are
//! intentionally deferred to their later milestones

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound::{Excluded, Included, Unbounded},
};

use ragnordb_common::{
    Error, Result,
    codec::{LockRecord, WriteKind, WriteRecord},
    encoding::decode_row,
    ids::{Timestamp, TxnId},
};

use crate::key::decode_row_key;

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
/// All keys passed to this trait are complete canonical row-key encodings from
/// `ragnordb_storage::key::encode_row_key`.
pub trait MvccStorage {
    /// Read the row version visible at `read_ts`.
    fn read(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Vec<u8>>>;

    /// Scan the half-open encoded-key range `[start, end)` at `read_ts`.
    ///
    /// Returned rows are ordered by canonical encoded row key.
    fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        read_ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Atomically validate and commit a transaction's complete mutation set.
    ///
    /// Implementations must not apply any mutation when validation of any key
    /// fails.
    fn commit_batch(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<usize>;

    /// Return current diagnostic counters.
    fn stats(&self) -> MvccStats;
}

/// In-memory implementation of RagnorDB's MVCC maps.
///
/// Mutation methods require `&mut self`. The future tablet state-machine actor
/// will own and serialize access to this structure. This phase therefore does
/// not introduce locking primitives or claim cross-thread transaction safety.
#[derive(Debug, Default)]
pub struct InMemoryMvcc {
    /// `row_key -> start_ts -> encoded row`.
    default: BTreeMap<Vec<u8>, BTreeMap<Timestamp, Vec<u8>>>,

    /// `row_key -> unresolved lock`.
    ///
    /// Locks are observed by reads and commit validation in this phase. Their
    /// distributed creation and resolution protocol belongs to the later
    /// Percolator transaction milestone.
    locks: BTreeMap<Vec<u8>, LockRecord>,

    /// `row_key -> commit_ts -> write record`.
    writes: BTreeMap<Vec<u8>, BTreeMap<Timestamp, WriteRecord>>,
}

impl InMemoryMvcc {
    /// Construct an empty in-memory MVCC engine.
    pub fn new() -> Self {
        Self::default()
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

        for (stored_commit_ts, write) in write_versions.range(..=read_ts).rev() {
            validate_write_record(*stored_commit_ts, write)?;

            match write.op {
                WriteKind::Put => {
                    let row = self
                        .default
                        .get(key)
                        .and_then(|versions| versions.get(&write.start_timestamp))
                        .ok_or_else(|| {
                            Error::CorruptData(format!(
                                "Put committed at timestamp {} references missing default \
                                 value at start timestamp {}",
                                stored_commit_ts.0, write.start_timestamp.0
                            ))
                        })?;

                    // Validate persisted bytes at the storage boundary. This
                    // prevents damaged row data from being returned as if it
                    // were a valid committed value.
                    decode_row(row)?;

                    return Ok(Some(row.clone()));
                }

                WriteKind::Delete => {
                    // A delete is a visible tombstone. Once encountered, older
                    // Put records must remain hidden from this snapshot.
                    return Ok(None);
                }

                WriteKind::Rollback => {
                    // A rollback marker describes an aborted transaction, not
                    // a logical deletion. Continue searching for an older
                    // committed Put or Delete record.
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
                        "Delete mutation at start timestamp {} conflicts with an existing \
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
                "transaction {} has a lock operation inconsistent with its buffered mutation",
                txn_id.0
            )));
        }

        Ok(())
    }

    fn validate_write_history(
        &self,
        key: &[u8],
        start_ts: Timestamp,
        commit_ts: Timestamp,
    ) -> Result<()> {
        let Some(write_versions) = self.writes.get(key) else {
            return Ok(());
        };

        for (stored_commit_ts, write) in write_versions {
            validate_write_record(*stored_commit_ts, write)?;

            if write.op == WriteKind::Rollback && write.start_timestamp == start_ts {
                return Err(Error::WriteConflict(format!(
                    "transaction starting at timestamp {} was already rolled back",
                    start_ts.0
                )));
            }
        }

        if let Some((latest_commit_ts, _)) = write_versions.last_key_value()
            && *latest_commit_ts >= commit_ts
        {
            return Err(Error::WriteConflict(format!(
                "commit timestamp {} does not advance the row's latest write timestamp {}",
                commit_ts.0, latest_commit_ts.0
            )));
        }

        if let Some((conflicting_commit_ts, _)) = write_versions
            .range((Excluded(start_ts), Unbounded))
            .rev()
            .find(|(_, write)| write.op != WriteKind::Rollback)
        {
            return Err(Error::WriteConflict(format!(
                "row was modified at timestamp {} after transaction start timestamp {}",
                conflicting_commit_ts.0, start_ts.0
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
                "commit timestamp {} is already occupied by a different write",
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
                        "replayed Put at commit timestamp {} has no default value",
                        commit_ts.0
                    ))
                })?;

            if stored_row != expected_row {
                return Err(Error::CorruptData(format!(
                    "replayed Put at commit timestamp {} references different row bytes",
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
            "transaction at start timestamp {} is only partially present at commit \
             timestamp {}",
            start_ts.0, commit_ts.0
        )))
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

        // Locks are included in the candidate set even when a key has no
        // committed history. Otherwise a scan could silently pass an
        // unresolved insertion lock.
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

    fn commit_batch(
        &mut self,
        txn_id: TxnId,
        start_ts: Timestamp,
        commit_ts: Timestamp,
        mutations: &BTreeMap<Vec<u8>, Mutation>,
    ) -> Result<usize> {
        validate_commit_metadata(txn_id, start_ts, commit_ts)?;

        for (key, mutation) in mutations {
            self.validate_mutation(key, mutation, start_ts)?;
        }

        if mutations.is_empty() {
            return Ok(0);
        }

        // A completely identical batch is a safe deterministic replay. A
        // partially present batch indicates impossible state for this atomic
        // engine and is treated as corruption.
        if self.validate_batch_replay(mutations, start_ts, commit_ts)? {
            return Ok(mutations.len());
        }

        // Validate every key before changing any map. This guarantees that
        // conflicts and malformed mutations cannot produce partial commits.
        for (key, mutation) in mutations {
            self.validate_lock(key, mutation, txn_id, start_ts)?;
            self.validate_write_history(key, start_ts, commit_ts)?;
        }

        for (key, mutation) in mutations {
            match mutation {
                Mutation::Put(row) => {
                    self.default
                        .entry(key.clone())
                        .or_default()
                        .insert(start_ts, row.clone());
                }

                Mutation::Delete => {
                    // Deletes store no default payload. Their write record is
                    // the tombstone that hides older committed values.
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
}

fn validate_commit_metadata(
    txn_id: TxnId,
    start_ts: Timestamp,
    commit_ts: Timestamp,
) -> Result<()> {
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

fn validate_write_record(stored_commit_ts: Timestamp, write: &WriteRecord) -> Result<()> {
    if write.commit_timestamp != stored_commit_ts {
        return Err(Error::CorruptData(format!(
            "write-map timestamp {} does not match record commit timestamp {}",
            stored_commit_ts.0, write.commit_timestamp.0
        )));
    }

    if write.start_timestamp.0 == 0 {
        return Err(Error::CorruptData(
            "write record contains reserved start timestamp 0".to_string(),
        ));
    }

    if write.commit_timestamp <= write.start_timestamp {
        return Err(Error::CorruptData(format!(
            "write record commit timestamp {} does not follow start timestamp {}",
            write.commit_timestamp.0, write.start_timestamp.0
        )));
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

fn encoded_scan_bounds(
    start: Option<&[u8]>,
    end: Option<&[u8]>,
) -> Result<(std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>)> {
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
        let key = make_row_key(TableId(1), &[Value::Int(id)]).unwrap();
        encode_row_key(&key).unwrap()
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
    fn snapshot_reads_select_the_latest_visible_version() {
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
    fn delete_tombstone_hides_only_snapshots_at_or_after_commit() {
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
        assert_eq!(engine.read(&key, Timestamp(10)).unwrap(), None);
    }

    #[test]
    fn rollback_is_skipped_by_reads_but_blocks_delayed_commit() {
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
            Timestamp(4),
            WriteRecord {
                start_timestamp: Timestamp(3),
                commit_timestamp: Timestamp(4),
                op: WriteKind::Rollback,
            },
        );

        assert_eq!(engine.read(&key, Timestamp(5)).unwrap(), Some(original));

        let error = engine
            .commit_batch(
                TxnId(2),
                Timestamp(3),
                Timestamp(6),
                &put_batch(key, delayed),
            )
            .unwrap_err();

        assert!(matches!(error, Error::WriteConflict(_)));
    }

    #[test]
    fn write_conflict_rejects_the_entire_batch() {
        let first_key = encoded_key(1);
        let second_key = encoded_key(2);
        let winner = encoded_row(2, "winner");
        let loser_first = encoded_row(1, "loser");
        let loser_second = encoded_row(2, "loser");
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
            (first_key.clone(), Mutation::Put(loser_first)),
            (second_key.clone(), Mutation::Put(loser_second)),
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
    fn visible_lock_causes_a_retryable_conflict() {
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
    fn scan_is_ordered_and_does_not_skip_lock_only_keys() {
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
    fn missing_default_value_is_reported_as_corruption() {
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
    fn exact_batch_replay_is_idempotent() {
        let key = encoded_key(1);
        let mutations = put_batch(key, encoded_row(1, "value"));
        let mut engine = InMemoryMvcc::new();

        assert_eq!(
            engine
                .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations,)
                .unwrap(),
            1
        );

        assert_eq!(
            engine
                .commit_batch(TxnId(1), Timestamp(1), Timestamp(2), &mutations,)
                .unwrap(),
            1
        );

        assert_eq!(
            engine.stats(),
            MvccStats {
                default_keys: 1,
                default_versions: 1,
                locks: 0,
                write_keys: 1,
                write_records: 1,
            }
        );
    }

    #[test]
    fn commit_metadata_requires_monotonic_nonzero_timestamps() {
        let engine_key = encoded_key(1);
        let batch = put_batch(engine_key, encoded_row(1, "value"));
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
}
