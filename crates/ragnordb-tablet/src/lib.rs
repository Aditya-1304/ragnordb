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

use std::collections::BTreeMap;

use ragnordb_common::{
    Error, Result,
    codec::Row,
    encoding::{decode_row, encode_row},
    ids::{RowKey, TableId, TabletId, Timestamp},
};
use ragnordb_storage::{
    key::{decode_row_key, encode_row_key},
    mvcc::{InMemoryMvcc, Mutation, MvccStats, MvccStorage},
};
use ragnordb_txn::Transaction;

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

#[cfg(test)]
mod tests {
    use super::*;
    use ragnordb_common::{codec::Value, ids::TxnId};
    use ragnordb_storage::key::make_row_key;

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
