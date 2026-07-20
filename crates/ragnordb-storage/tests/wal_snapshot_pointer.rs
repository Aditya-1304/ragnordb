use std::collections::BTreeSet;

use prost::Message;
use ragnordb_common::{
    Error,
    ids::{TableId, Timestamp},
    proto::wal as wal_proto,
};
use ragnordb_storage::wal::{SNAPSHOT_POINTER_VERSION, SnapshotPointer};
use wal::lsn::Lsn;

fn valid_pointer() -> SnapshotPointer {
    SnapshotPointer {
        snapshot_id: 7,
        snapshot_timestamp: Timestamp(5),
        replay_from_lsn: Lsn::new(4096),
        relative_path: "snapshots/7.snap".to_string(),
        table_ids: BTreeSet::from([TableId(1), TableId(9)]),
    }
}

fn valid_proto_pointer() -> wal_proto::SnapshotPointer {
    let encoded = valid_pointer().encode().unwrap();

    wal_proto::SnapshotPointer::decode(encoded.as_slice()).unwrap()
}

fn assert_encode_rejected(mutate: impl FnOnce(&mut SnapshotPointer), expected_message: &str) {
    let mut pointer = valid_pointer();
    mutate(&mut pointer);

    let error = pointer.encode().unwrap_err();

    assert!(matches!(error, Error::InvalidArgument(_)));
    assert!(
        error.to_string().contains(expected_message),
        "unexpected encoding error: {error}"
    );
}

fn assert_decode_rejected(
    mutate: impl FnOnce(&mut wal_proto::SnapshotPointer),
    expected_message: &str,
) {
    let mut proto = valid_proto_pointer();
    mutate(&mut proto);

    let error = SnapshotPointer::decode(&proto.encode_to_vec()).unwrap_err();

    assert!(matches!(error, Error::CorruptData(_)));
    assert!(
        error.to_string().contains(expected_message),
        "unexpected decoding error: {error}"
    );
}

/// Verifies that recovery metadata survives the durable payload boundary.
///
/// Realistic bug caught:
///
/// Recovery could open the wrong snapshot, replay from the wrong WAL position,
/// or lose the set of tables represented by the snapshot.
#[test]
fn valid_snapshot_pointer_round_trips() {
    let original = valid_pointer();

    let encoded = original.encode().unwrap();
    let decoded = SnapshotPointer::decode(&encoded).unwrap();

    assert_eq!(decoded, original);
}

/// Verifies that V1 snapshot-pointer bytes remain recoverable.
///
/// Realistic bug caught:
///
/// New encoder and decoder code could agree after protobuf field-number changes
/// while existing snapshot pointers become unreadable.
#[test]
fn v1_snapshot_pointer_golden_bytes_remain_decodable() {
    const V1_GOLDEN_BYTES: &[u8] = &[
        // SnapshotPointer.version = 1.
        0x08, 0x01, // SnapshotPointer.snapshot_id = 7.
        0x10, 0x07, // SnapshotPointer.snapshot_timestamp = Timestamp { id: 5 }.
        0x1a, 0x02, 0x08, 0x05, // SnapshotPointer.replay_from_lsn = 4096.
        0x20, 0x80, 0x20, // SnapshotPointer.relative_path = "snapshots/7.snap".
        0x2a, 0x10, b's', b'n', b'a', b'p', b's', b'h', b'o', b't', b's', b'/', b'7', b'.', b's',
        b'n', b'a', b'p', // SnapshotPointer.table_ids = [TableId(1), TableId(9)].
        0x32, 0x02, 0x08, 0x01, 0x32, 0x02, 0x08, 0x09,
    ];

    let expected = SnapshotPointer {
        snapshot_id: 7,
        snapshot_timestamp: Timestamp(5),
        replay_from_lsn: Lsn::new(4096),
        relative_path: "snapshots/7.snap".to_string(),
        table_ids: BTreeSet::from([TableId(1), TableId(9)]),
    };

    let decoded = SnapshotPointer::decode(V1_GOLDEN_BYTES).unwrap();

    assert_eq!(decoded, expected);
}

/// Ensures recovery never applies unsupported snapshot-pointer semantics.
///
/// Realistic bug caught:
///
/// An older binary could interpret a newer snapshot layout or replay-boundary
/// convention using obsolete V1 rules.
#[test]
fn zero_and_unsupported_snapshot_pointer_versions_are_rejected() {
    assert_decode_rejected(
        |proto| proto.version = 0,
        "unsupported SnapshotPointer version 0",
    );

    assert_decode_rejected(
        |proto| proto.version = SNAPSHOT_POINTER_VERSION + 1,
        "unsupported SnapshotPointer version 2",
    );
}

/// Ensures a snapshot pointer always has a stable identity and MVCC timestamp.
///
/// Realistic bug caught:
///
/// Recovery could accept protobuf default values and accidentally treat an
/// unidentified or untimestamped snapshot as published state.
#[test]
fn invalid_snapshot_identity_and_timestamp_are_rejected() {
    assert_encode_rejected(
        |pointer| pointer.snapshot_id = 0,
        "snapshot ID 0 is reserved",
    );

    assert_encode_rejected(
        |pointer| pointer.snapshot_timestamp = Timestamp(0),
        "snapshot timestamp 0 is reserved",
    );

    assert_decode_rejected(|proto| proto.snapshot_id = 0, "snapshot ID 0 is reserved");

    assert_decode_rejected(
        |proto| {
            proto.snapshot_timestamp = Some(Timestamp(0).to_proto());
        },
        "snapshot timestamp 0 is reserved",
    );

    assert_decode_rejected(
        |proto| proto.snapshot_timestamp = None,
        "missing its snapshot timestamp",
    );
}

/// Ensures snapshot files cannot escape the configured snapshot directory.
///
/// Realistic bug caught:
///
/// A corrupt WAL record could cause recovery to open an absolute path or traverse
/// outside the database directory.
#[test]
fn unsafe_snapshot_paths_are_rejected_on_encode_and_decode() {
    let invalid_paths = [
        "",
        "/tmp/outside.snap",
        "../outside.snap",
        "snapshots/../outside.snap",
        "snapshots//7.snap",
        r"snapshots\7.snap",
        "C:/outside.snap",
    ];

    for path in invalid_paths {
        assert_encode_rejected(
            |pointer| pointer.relative_path = path.to_string(),
            "invalid relative snapshot path",
        );

        assert_decode_rejected(
            |proto| proto.relative_path = path.to_string(),
            "invalid relative snapshot path",
        );
    }
}

/// Ensures the table set contains only unique, nonzero identities.
///
/// Realistic bug caught:
///
/// Duplicate or reserved table IDs could make snapshot inspection disagree with
/// recovery about which tablet states are actually represented.
#[test]
fn invalid_snapshot_table_ids_are_rejected() {
    assert_encode_rejected(
        |pointer| {
            pointer.table_ids.insert(TableId(0));
        },
        "table ID 0",
    );

    assert_decode_rejected(
        |proto| {
            proto.table_ids.push(TableId(0).to_proto());
        },
        "table ID 0",
    );

    assert_decode_rejected(
        |proto| {
            let duplicate = proto.table_ids[0];
            proto.table_ids.push(duplicate);
        },
        "duplicate table ID",
    );
}
