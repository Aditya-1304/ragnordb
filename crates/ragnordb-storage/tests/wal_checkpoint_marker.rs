use prost::Message;
use ragnordb_common::{Error, ids::Timestamp, proto::wal as wal_proto};
use ragnordb_storage::wal::{CHECKPOINT_MARKER_VERSION, CheckpointMarker};
use wal::lsn::Lsn;

fn valid_marker() -> CheckpointMarker {
    CheckpointMarker {
        snapshot_id: 7,
        snapshot_timestamp: Timestamp(5),
        replay_from_lsn: Lsn::new(4096),
    }
}

fn valid_proto_marker() -> wal_proto::CheckpointMarker {
    let encoded = valid_marker().encode().unwrap();

    wal_proto::CheckpointMarker::decode(encoded.as_slice()).unwrap()
}

fn assert_encode_rejected(mutate: impl FnOnce(&mut CheckpointMarker), expected_message: &str) {
    let mut marker = valid_marker();
    mutate(&mut marker);

    let error = marker.encode().unwrap_err();

    assert!(matches!(error, Error::InvalidArgument(_)));
    assert!(
        error.to_string().contains(expected_message),
        "unexpected encoding error: {error}"
    );
}

fn assert_decode_rejected(
    mutate: impl FnOnce(&mut wal_proto::CheckpointMarker),
    expected_message: &str,
) {
    let mut proto = valid_proto_marker();
    mutate(&mut proto);

    let error = CheckpointMarker::decode(&proto.encode_to_vec()).unwrap_err();

    assert!(matches!(error, Error::CorruptData(_)));
    assert!(
        error.to_string().contains(expected_message),
        "unexpected decoding error: {error}"
    );
}

/// Verifies that the recovery critical checkpoint metadata survives durable
/// encoding
///
/// Realistic bug caught:
///
/// Recovery could publish the wrong snapshot identity, MVCC timestamp, or WAL
/// replay boundary after decoding a checkpoint marker
#[test]
fn valid_checkpoint_marker_round_trips() {
    let original = valid_marker();

    let encoded = original.encode().unwrap();
    let decoded = CheckpointMarker::decode(&encoded).unwrap();

    assert_eq!(decoded, original);
}

/// Freezes the V1 checkpoint-marker wire representation
///
/// Realistic bug caught:
///
/// Encoder and decoder changes could agree with each other after protobuf field
/// numbers change while checkpoint markers already stored in WAL become
/// unreadable
#[test]
fn v1_checkpoint_marker_golden_bytes_remain_decodable() {
    const V1_GOLDEN_BYTES: &[u8] = &[
        // CheckpointMarker.version = 1.
        0x08, 0x01, // CheckpointMarker.snapshot_id = 7.
        0x10, 0x07, // CheckpointMarker.snapshot_timestamp = Timestamp { id: 5 }.
        0x1a, 0x02, 0x08, 0x05, // CheckpointMarker.replay_from_lsn = 4096.
        0x20, 0x80, 0x20,
    ];

    let expected = CheckpointMarker {
        snapshot_id: 7,
        snapshot_timestamp: Timestamp(5),
        replay_from_lsn: Lsn::new(4096),
    };

    let decoded = CheckpointMarker::decode(V1_GOLDEN_BYTES).unwrap();

    assert_eq!(decoded, expected);
}

/// Ensures recovery never applies unsupported checkpoint-marker semantics.
///
/// Realistic bug caught:
///
/// An older binary could interpret a newer checkpoint layout using obsolete V1
/// publication and replay rules.
#[test]
fn zero_and_unsupported_checkpoint_marker_versions_are_rejected() {
    assert_decode_rejected(
        |proto| proto.version = 0,
        "unsupported CheckpointMarker version 0",
    );

    assert_decode_rejected(
        |proto| proto.version = CHECKPOINT_MARKER_VERSION + 1,
        "unsupported CheckpointMarker version 2",
    );
}

/// Ensures a checkpoint always references an identified, timestamped snapshot.
///
/// Realistic bug caught:
///
/// Protobuf default values could be accepted as a real snapshot reference,
/// causing recovery to trust incomplete checkpoint metadata.
#[test]
fn invalid_checkpoint_snapshot_metadata_is_rejected() {
    assert_encode_rejected(|marker| marker.snapshot_id = 0, "snapshot ID 0 is reserved");

    assert_encode_rejected(
        |marker| marker.snapshot_timestamp = Timestamp(0),
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

/// Preserves the meaning of an initial checkpoint that skips no WAL history.
///
/// Realistic bug caught:
///
/// Treating LSN zero as missing metadata would prevent publishing a snapshot
/// whose safe recovery boundary is the beginning of the WAL.
#[test]
fn zero_replay_lsn_is_valid() {
    let marker = CheckpointMarker {
        replay_from_lsn: Lsn::ZERO,
        ..valid_marker()
    };

    let encoded = marker.encode().unwrap();
    let decoded = CheckpointMarker::decode(&encoded).unwrap();

    assert_eq!(decoded, marker);
}
