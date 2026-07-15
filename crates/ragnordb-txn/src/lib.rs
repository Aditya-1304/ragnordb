//! transaction local state
//!
//! a transaction owns a stable MVCC start timestamp and a deterministic,
//! ordered write set. reads use `start_ts` for snapshot visibility while
//! the tablet layer checks this write set first to provide
//! read-your-writes
//!
//! Distributed participants, heartbeats, transaction status records and
//! coordinator retry state belong to later implementation for now

use std::collections::BTreeMap;

use ragnordb_common::{
    Error, Result,
    encoding::decode_row,
    ids::{Timestamp, TxnId},
};
use ragnordb_storage::{key::decode_row_key, mvcc::Mutation};

/// Client-side state for one active transaction.
#[derive(Debug)]
pub struct Transaction {
    id: TxnId,
    start_ts: Timestamp,
    writes: BTreeMap<Vec<u8>, Mutation>,
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
        validate_buffered_key(&key)?;

        decode_row(&row).map_err(|error| {
            Error::InvalidArgument(format!(
                "transaction Put does not contain a canonical encoded row: \
                 {error}"
            ))
        })?;

        self.writes.insert(key, Mutation::Put(row));
        Ok(())
    }

    /// Buffer a row deletion.
    pub fn buffer_delete(&mut self, key: Vec<u8>) -> Result<()> {
        validate_buffered_key(&key)?;
        self.writes.insert(key, Mutation::Delete);
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

fn validate_buffered_key(key: &[u8]) -> Result<()> {
    decode_row_key(key).map(|_| ()).map_err(|error| {
        Error::InvalidArgument(format!(
            "transaction mutation key is not a canonical encoded row key: \
             {error}"
        ))
    })
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
}
