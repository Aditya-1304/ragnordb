use std::collections::BTreeSet;

use ragnordb_common::{
    Error,
    ids::{TableId, Timestamp},
};
use ragnordb_storage::{
    checkpoint::SNAPSHOT_FILE_VERSION,
    recovery::{
        DecodedRecoveryRecord, RecoveryCheckpointCandidate, RecoveryCheckpointSelector,
        RecoveryPayload,
    },
    wal::{CheckpointMarker, SnapshotPointer},
};
use wal::lsn::Lsn;

fn snapshot_pointer(
    snapshot_id: u64,
    snapshot_timestamp: u64,
    replay_from_lsn: u64,
) -> SnapshotPointer {
    SnapshotPointer {
        snapshot_id,
        snapshot_timestamp: Timestamp(snapshot_timestamp),
        replay_from_lsn: Lsn::new(replay_from_lsn),
        relative_path: format!("snapshots/snapshot-{snapshot_id}.ragnor"),
        table_ids: BTreeSet::from([TableId(snapshot_id)]),
        file_length: 128,
        file_checksum_crc32c: 1,
        snapshot_format_version: SNAPSHOT_FILE_VERSION,
    }
}

fn checkpoint_marker(
    snapshot_id: u64,
    snapshot_timestamp: u64,
    replay_from_lsn: u64,
) -> CheckpointMarker {
    CheckpointMarker {
        snapshot_id,
        snapshot_timestamp: Timestamp(snapshot_timestamp),
        replay_from_lsn: Lsn::new(replay_from_lsn),
    }
}

fn pointer_record(lsn: u64, pointer: SnapshotPointer) -> DecodedRecoveryRecord {
    DecodedRecoveryRecord {
        lsn: Lsn::new(lsn),
        payload: RecoveryPayload::SnapshotPointer(pointer),
    }
}

fn marker_record(lsn: u64, marker: CheckpointMarker) -> DecodedRecoveryRecord {
    DecodedRecoveryRecord {
        lsn: Lsn::new(lsn),
        payload: RecoveryPayload::CheckpointMarker(marker),
    }
}

/// verifies that only an exactly matched pointer and marker publish a recovery
/// checkpoint candidate
///
/// Realistic bug caught:
///
/// recovery could treat the newest pointer as published even though its marker
/// was never durably appended. Using that orphan's replay boundary would skip
/// WAL records not represented by any trusted snapshot
#[test]
fn newest_orphan_pointer_does_not_replace_published_checkpoint() {
    let first_pointer = snapshot_pointer(7, 50, 32);
    let first_marker = checkpoint_marker(7, 50, 32);
    let orphan_pointer = snapshot_pointer(8, 60, 112);

    let records = [
        pointer_record(64, first_pointer.clone()),
        marker_record(96, first_marker),
        pointer_record(144, orphan_pointer),
    ];

    let mut selector = RecoveryCheckpointSelector::new();

    for record in &records {
        selector
            .observe_record(record)
            .expect("valid checkpoint history must be accepted");
    }

    assert_eq!(
        selector.selected(),
        Some(&RecoveryCheckpointCandidate {
            pointer_lsn: Lsn::new(64),
            marker_lsn: Lsn::new(96),
            pointer: first_pointer,
        })
    );

    assert_eq!(
        selector
            .selected()
            .expect("published checkpoint must exist")
            .pointer
            .replay_from_lsn,
        Lsn::new(32)
    );
}

/// verifies that a marker cannot publish metadata from a different snapshot
/// boundary
///
/// Realistic bug caught:
///
/// pairing records by snapshot ID alone could combine a pointer and marker with
/// different timestamps or replay boundaries, causing recovery to skip or
/// duplicate durable history
#[test]
fn mismatched_checkpoint_marker_is_rejected_as_corruption() {
    let pointer = snapshot_pointer(7, 50, 32);

    // the marker reuses the snapshot ID but claims a different replay frontier
    let mismatched_marker = checkpoint_marker(7, 50, 48);

    let mut selector = RecoveryCheckpointSelector::new();

    selector
        .observe_record(&pointer_record(64, pointer))
        .expect("pointer must be retained as a pending candidate");

    let error = selector
        .observe_record(&marker_record(96, mismatched_marker))
        .unwrap_err();

    assert!(matches!(
        error,
        Error::CorruptData(message)
            if message.contains("CheckpointMarker")
                && message.contains("snapshot 7")
                && message.contains("WAL LSN 96")
                && message.contains("does not match")
    ));

    assert!(selector.selected().is_none());
}
