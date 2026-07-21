use std::collections::BTreeMap;

use ragnordb_common::{
    Error,
    codec::{Row, Value},
    encoding::encode_row,
    ids::{TableId, Timestamp, TxnId},
};
use ragnordb_storage::{
    key::{encode_row_key, make_row_key},
    mvcc::{InMemoryMvcc, Mutation, MvccStorage},
};

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

/// Ensures commit preflight validates a mutation batch without applying it.
///
/// Realistic bug caught:
///
/// A validation API that reuses the current commit path incorrectly could make
/// rows visible before the corresponding WAL record has been appended and
/// synchronized.
#[test]
fn valid_commit_preflight_does_not_apply_mvcc_state() {
    let storage = InMemoryMvcc::new();
    let key = encoded_key(1);
    let mutations = put_batch(key.clone(), encoded_row(1, "pending"));
    let stats_before = storage.stats();

    storage
        .validate_commit_batch(TxnId(1), Timestamp(1), &mutations)
        .unwrap();

    assert_eq!(storage.stats(), stats_before);
    assert_eq!(storage.read(&key, Timestamp(2)).unwrap(), None);
}

/// Ensures snapshot write conflicts are rejected during commit preflight.
///
/// Realistic bug caught:
///
/// If conflict detection occurs only inside the final MVCC apply operation, the
/// commit coordinator could allocate a timestamp and append an invalid commit
/// record before discovering that a newer committed version already exists.
#[test]
fn conflicting_commit_is_rejected_during_preflight() {
    let mut storage = InMemoryMvcc::new();
    let key = encoded_key(1);

    let winner = put_batch(key.clone(), encoded_row(1, "winner"));

    storage
        .commit_batch(TxnId(2), Timestamp(2), Timestamp(3), &winner)
        .unwrap();

    let loser = put_batch(key.clone(), encoded_row(1, "loser"));
    let stats_before = storage.stats();

    let error = storage
        .validate_commit_batch(TxnId(1), Timestamp(1), &loser)
        .unwrap_err();

    assert!(matches!(error, Error::WriteConflict(_)));

    // Preflight failure must leave the previously committed winner untouched.
    assert_eq!(storage.stats(), stats_before);
    assert_eq!(
        storage.read(&key, Timestamp(4)).unwrap(),
        Some(encoded_row(1, "winner"))
    );
}

/// Ensures reserved transaction metadata is rejected by the preflight boundary.
///
/// Realistic bug caught:
///
/// A coordinator could append a durable record containing reserved transaction
/// metadata if validation occurred only during the later MVCC apply operation.
#[test]
fn preflight_rejects_reserved_transaction_metadata() {
    let storage = InMemoryMvcc::new();
    let mutations = put_batch(encoded_key(1), encoded_row(1, "pending"));

    let zero_transaction_error = storage
        .validate_commit_batch(TxnId(0), Timestamp(1), &mutations)
        .unwrap_err();

    assert!(matches!(zero_transaction_error, Error::InvalidArgument(_)));

    let zero_timestamp_error = storage
        .validate_commit_batch(TxnId(1), Timestamp(0), &mutations)
        .unwrap_err();

    assert!(matches!(zero_timestamp_error, Error::InvalidArgument(_)));
    assert_eq!(storage.stats(), Default::default());
}

/// Ensures malformed keys are rejected before any durable commit is attempted.
///
/// Realistic bug caught:
///
/// Without preflight validation, malformed storage keys could enter durable
/// history and make subsequent recovery unable to route the mutation.
#[test]
fn preflight_rejects_noncanonical_mutation_keys() {
    let storage = InMemoryMvcc::new();
    let mutations = BTreeMap::from([(vec![0xff], Mutation::Put(encoded_row(1, "pending")))]);

    let error = storage
        .validate_commit_batch(TxnId(1), Timestamp(1), &mutations)
        .unwrap_err();

    assert!(matches!(error, Error::InvalidArgument(_)));
    assert_eq!(storage.stats(), Default::default());
}

/// Ensures malformed Put rows are rejected before WAL append.
///
/// Realistic bug caught:
///
/// A canonical row key paired with malformed row bytes could otherwise become
/// durable and later prevent recovery or snapshot reads from decoding the row.
#[test]
fn preflight_rejects_malformed_put_rows() {
    let storage = InMemoryMvcc::new();
    let mutations = put_batch(encoded_key(1), vec![0xff]);

    let error = storage
        .validate_commit_batch(TxnId(1), Timestamp(1), &mutations)
        .unwrap_err();

    assert!(matches!(error, Error::InvalidArgument(_)));
    assert_eq!(storage.stats(), Default::default());
}
