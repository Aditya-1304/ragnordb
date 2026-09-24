use ragnordb_common::{
    codec::{Row, Value, WriteKind},
    encoding::encode_row,
    ids::{TableId, Timestamp, TxnId},
};
use ragnordb_storage::{
    key::{encode_row_key, make_row_key},
    mvcc::{InMemoryMvcc, Mutation, MvccStorage},
};

fn key(id: i64) -> Vec<u8> {
    encode_row_key(&make_row_key(TableId(1), &[Value::Int(id)]).unwrap()).unwrap()
}

fn row(id: i64) -> Vec<u8> {
    encode_row(&Row {
        values: vec![Value::Int(id), Value::Text("intent".to_string())],
    })
    .unwrap()
}

#[test]
fn read_path_exposes_only_locks_that_conflict_with_the_snapshot() {
    let key = key(1);
    let mut storage = InMemoryMvcc::new();
    storage
        .prewrite(
            TxnId(7),
            Timestamp(100),
            &key,
            &Mutation::Put(row(1)),
            &key,
            30_000,
        )
        .unwrap();

    let lock = storage
        .intent_for_read(&key, Timestamp(100))
        .unwrap()
        .expect("the lock must conflict at its start timestamp");
    assert_eq!(lock.txn_id, TxnId(7));
    assert_eq!(lock.start_timestamp, Timestamp(100));
    assert_eq!(lock.op, WriteKind::Put);

    assert_eq!(
        storage.intent_for_read(&key, Timestamp(99)).unwrap(),
        None,
        "a future lock must not affect an older snapshot"
    );
}
