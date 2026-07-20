use std::collections::BTreeMap;

use prost::Message;
use ragnordb_common::{
    Error,
    codec::{Row, Value},
    encoding::encode_row,
    ids::{TableId, Timestamp, TxnId},
    proto::wal as wal_proto,
};
use ragnordb_storage::{
    key::{encode_row_key, make_row_key},
    wal::{SINGLE_NODE_TXN_COMMIT_VERSION, SingleNodeTxnCommit, WalMutation},
};

fn encoded_key(table_id: TableId, id: i64) -> Vec<u8> {
    let row_key = make_row_key(table_id, &[Value::Int(id)]).unwrap();
    encode_row_key(&row_key).unwrap()
}

fn encoded_row(id: i64, name: &str) -> Vec<u8> {
    encode_row(&Row {
        values: vec![Value::Int(id), Value::Text(name.to_string())],
    })
    .unwrap()
}

fn valid_commit() -> SingleNodeTxnCommit {
    let mut writes = BTreeMap::new();

    writes.insert(
        encoded_key(TableId(9), 1),
        WalMutation::Put(encoded_row(1, "alice")),
    );

    writes.insert(encoded_key(TableId(9), 2), WalMutation::Delete);

    SingleNodeTxnCommit {
        table_id: TableId(9),
        txn_id: TxnId(7),
        start_timestamp: Timestamp(11),
        commit_timestamp: Timestamp(12),
        writes,
    }
}

fn valid_proto_commit() -> wal_proto::SingleNodeTxnCommit {
    let encoded = valid_commit().encode().unwrap();

    wal_proto::SingleNodeTxnCommit::decode(encoded.as_slice()).unwrap()
}

fn assert_encode_rejected(mutate: impl FnOnce(&mut SingleNodeTxnCommit), expected_message: &str) {
    let mut record = valid_commit();
    mutate(&mut record);

    let error = record.encode().unwrap_err();

    assert!(matches!(error, Error::InvalidArgument(_)));
    assert!(
        error.to_string().contains(expected_message),
        "unexpected encoding error: {error}"
    );
}

fn assert_decode_rejected(
    mutate: impl FnOnce(&mut wal_proto::SingleNodeTxnCommit),
    expected_message: &str,
) {
    let mut proto = valid_proto_commit();
    mutate(&mut proto);

    let error = SingleNodeTxnCommit::decode(&proto.encode_to_vec()).unwrap_err();

    assert!(matches!(error, Error::CorruptData(_)));
    assert!(
        error.to_string().contains(expected_message),
        "unexpected decoding error: {error}"
    );
}

/// Verifies that all transaction metadata and both mutation kinds survive the
/// durable payload boundary.
///
/// Realistic bug caught:
///
/// Recovery could lose a mutation, change its operation, or reconstruct the
/// transaction with different timestamps or table ownership.
#[test]
fn valid_put_and_delete_commit_round_trips() {
    let original = valid_commit();

    let encoded = original.encode().unwrap();
    let decoded = SingleNodeTxnCommit::decode(&encoded).unwrap();

    assert_eq!(decoded, original);
}

/// Verifies that the V1 decoder remains compatible with bytes written by the
/// original V1 schema.
///
/// Realistic bug caught:
///
/// A protobuf field number or mutation tag could change while new round-trip
/// tests continue passing, leaving existing WAL files unrecoverable.
#[test]
fn v1_single_node_commit_golden_bytes_remain_decodable() {
    const V1_GOLDEN_BYTES: &[u8] = &[
        // Field 1: version = 1.
        0x08, 0x01, // Field 2: TableId { id: 1 }.
        0x12, 0x02, 0x08, 0x01, // Field 3: TxnId { id: 7 }.
        0x1a, 0x02, 0x08, 0x07, // Field 4: start Timestamp { id: 11 }.
        0x22, 0x02, 0x08, 0x0b, // Field 5: commit Timestamp { id: 12 }.
        0x2a, 0x02, 0x08, 0x0c,
        // Field 6: Put at canonical row key table=1, primary-key INT(0).
        0x32, 0x1b, 0x0a, 0x12, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x10, 0x80,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x12, 0x05, 0x01, 0x00, 0x00, 0x00, 0x00,
        // Field 6: Delete at canonical row key table=1, primary-key INT(1).
        0x32, 0x16, 0x0a, 0x12, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x10, 0x80,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x1a, 0x00,
    ];

    let expected_writes = BTreeMap::from([
        (
            encoded_key(TableId(1), 0),
            WalMutation::Put(encode_row(&Row { values: Vec::new() }).unwrap()),
        ),
        (encoded_key(TableId(1), 1), WalMutation::Delete),
    ]);

    let expected = SingleNodeTxnCommit {
        table_id: TableId(1),
        txn_id: TxnId(7),
        start_timestamp: Timestamp(11),
        commit_timestamp: Timestamp(12),
        writes: expected_writes,
    };

    let decoded = SingleNodeTxnCommit::decode(V1_GOLDEN_BYTES).unwrap();

    assert_eq!(decoded, expected);
}

/// Ensures unsupported payload versions cannot be interpreted using V1 rules.
///
/// Realistic bug caught:
///
/// An older binary could replay a newer payload whose fields have different
/// semantics, producing valid-looking but incorrect database state.
#[test]
fn zero_and_unsupported_versions_are_rejected() {
    assert_decode_rejected(
        |proto| proto.version = 0,
        "unsupported SingleNodeTxnCommit version 0",
    );

    assert_decode_rejected(
        |proto| proto.version = SINGLE_NODE_TXN_COMMIT_VERSION + 1,
        "unsupported SingleNodeTxnCommit version 2",
    );
}

/// Ensures reserved identities and impossible timestamp orderings cannot enter
/// or be reconstructed from durable history.
///
/// Realistic bug caught:
///
/// Recovery could accept a commit that the live MVCC path could never create,
/// including versions whose commit timestamp is not newer than their snapshot.
#[test]
fn invalid_transaction_metadata_is_rejected_on_encode_and_decode() {
    assert_encode_rejected(
        |record| record.table_id = TableId(0),
        "table ID 0 is reserved",
    );

    assert_encode_rejected(
        |record| record.txn_id = TxnId(0),
        "transaction ID 0 is reserved",
    );

    assert_encode_rejected(
        |record| record.start_timestamp = Timestamp(0),
        "start timestamp 0 is reserved",
    );

    assert_encode_rejected(
        |record| record.commit_timestamp = Timestamp(0),
        "commit timestamp 0 is reserved",
    );

    assert_encode_rejected(
        |record| record.commit_timestamp = record.start_timestamp,
        "commit timestamp must be greater than start timestamp",
    );

    assert_encode_rejected(
        |record| record.commit_timestamp = Timestamp(record.start_timestamp.0 - 1),
        "commit timestamp must be greater than start timestamp",
    );

    assert_decode_rejected(
        |proto| proto.table_id = Some(TableId(0).to_proto()),
        "table ID 0 is reserved",
    );

    assert_decode_rejected(
        |proto| proto.txn_id = Some(TxnId(0).to_proto()),
        "transaction ID 0 is reserved",
    );

    assert_decode_rejected(
        |proto| proto.start_timestamp = Some(Timestamp(0).to_proto()),
        "start timestamp 0 is reserved",
    );

    assert_decode_rejected(
        |proto| proto.commit_timestamp = Some(Timestamp(0).to_proto()),
        "commit timestamp 0 is reserved",
    );

    assert_decode_rejected(
        |proto| proto.commit_timestamp = Some(Timestamp(11).to_proto()),
        "commit timestamp must be greater than start timestamp",
    );

    assert_decode_rejected(
        |proto| proto.commit_timestamp = Some(Timestamp(10).to_proto()),
        "commit timestamp must be greater than start timestamp",
    );
}

/// Ensures read-only transactions and lost-write commits are not serialized.
///
/// Realistic bug caught:
///
/// A commit-path bug could append durable history for a transaction with no
/// state transition, despite read-only commits being explicit no-ops.
#[test]
fn empty_write_batch_is_rejected_on_encode_and_decode() {
    assert_encode_rejected(
        |record| record.writes.clear(),
        "must contain at least one write",
    );

    assert_decode_rejected(
        |proto| proto.writes.clear(),
        "must contain at least one write",
    );
}

/// Ensures recovery never silently applies only one of two serialized
/// mutations for the same row.
///
/// Realistic bug caught:
///
/// Collecting repeated protobuf writes directly into a map could silently make
/// the final duplicate win, changing the meaning of the durable transaction.
#[test]
fn duplicate_encoded_row_key_is_rejected_during_decode() {
    assert_decode_rejected(
        |proto| {
            let duplicate = proto.writes[0].clone();
            proto.writes.push(duplicate);
        },
        "duplicate encoded row key",
    );
}

/// Ensures malformed row bytes cannot be persisted or reconstructed as a Put.
///
/// Realistic bug caught:
///
/// Corrupt row bytes could become committed MVCC state and fail later during a
/// read, far away from the WAL record that introduced the corruption.
#[test]
fn malformed_put_row_is_rejected_on_encode_and_decode() {
    assert_encode_rejected(
        |record| {
            record.writes = BTreeMap::from([(
                encoded_key(record.table_id, 1),
                WalMutation::Put(vec![0xff]),
            )]);
        },
        "malformed canonical row",
    );

    assert_decode_rejected(
        |proto| {
            proto.writes[0].mutation = Some(wal_proto::wal_write::Mutation::PutRow(vec![0xff]));
        },
        "malformed canonical row",
    );
}

/// Ensures arbitrary byte strings cannot enter the ordered row-key namespace.
///
/// Realistic bug caught:
///
/// A malformed key could bypass table routing and make deterministic scans or
/// replay fail after the WAL record had already been accepted.
#[test]
fn noncanonical_row_key_is_rejected_on_encode_and_decode() {
    assert_encode_rejected(
        |record| {
            record.writes = BTreeMap::from([(vec![0xff], WalMutation::Delete)]);
        },
        "noncanonical row key",
    );

    assert_decode_rejected(
        |proto| proto.writes[0].key = vec![0xff],
        "noncanonical row key",
    );
}

/// Ensures every mutation belongs to the table named by the commit record.
///
/// Realistic bug caught:
///
/// Replay could apply a valid canonical key to the wrong tablet if only the key
/// shape were checked and its embedded table ID were ignored.
#[test]
fn foreign_table_key_is_rejected_on_encode_and_decode() {
    assert_encode_rejected(
        |record| {
            record.writes = BTreeMap::from([(
                encoded_key(TableId(record.table_id.0 + 1), 1),
                WalMutation::Delete,
            )]);
        },
        "does not match commit table",
    );

    assert_decode_rejected(
        |proto| {
            proto.writes[0].key = encoded_key(TableId(10), 1);
        },
        "does not match commit table",
    );
}

/// Ensures recovery cannot guess the operation represented by an incomplete
/// protobuf write.
///
/// Realistic bug caught:
///
/// A missing `oneof` could otherwise be treated as an implicit deletion or an
/// empty Put.
#[test]
fn write_without_mutation_is_rejected() {
    assert_decode_rejected(
        |proto| proto.writes[0].mutation = None,
        "WAL write is missing its mutation",
    );
}
