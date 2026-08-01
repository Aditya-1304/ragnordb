use std::collections::BTreeSet;

use ragnordb_common::{
    Error,
    ids::{TableId, Timestamp, TxnId},
    proto::snapshot as snapshot_proto,
};
use ragnordb_storage::{
    checkpoint::{publish_checkpoint, publish_snapshot_file},
    wal::{CheckpointMarker, RagnorDbWalAdapter, RagnorDbWalRecordType, SnapshotPointer},
};
use wal::{
    config::{RECORD_HEADER_LEN, SEGMENT_HEADER_LEN, WalConfig},
    io::fault::FaultDirectory,
    lsn::Lsn,
    types::WalIdentity,
    wal::WalHandle,
};

type TestWalHandle = WalHandle<FaultDirectory, ()>;

fn empty_snapshot(snapshot_id: u64) -> snapshot_proto::DatabaseSnapshot {
    snapshot_proto::DatabaseSnapshot {
        snapshot_id,
        snapshot_timestamp: Some(Timestamp(40).to_proto()),
        replay_from_lsn: 4096,
        high_water_marks: Some(snapshot_proto::AllocatorHighWaterMarks {
            max_transaction_id: Some(TxnId(12).to_proto()),
            max_timestamp: Some(Timestamp(40).to_proto()),
            max_table_id: Some(TableId(0).to_proto()),
            max_snapshot_id: snapshot_id,
        }),
        tables: Vec::new(),
    }
}

fn wal_config(path: &std::path::Path) -> WalConfig {
    WalConfig {
        dir: path.to_path_buf(),
        identity: WalIdentity::new(1, 1, 1),
        ..WalConfig::default()
    }
}

fn open_handle(directory: FaultDirectory, config: WalConfig) -> TestWalHandle {
    WalHandle::open(directory, config, ())
        .expect("checkpoint publication WAL must open")
        .0
}

/// Realistic bug caught:
///
/// A checkpoint implementation could expose a retention-safe replay frontier
/// after syncing only the pointer, append the marker first, or encode different
/// snapshot metadata into the two records. A crash after such publication could
/// prune the only WAL prefix capable of rebuilding the database.
#[test]
fn checkpoint_becomes_retention_safe_only_after_matching_wal_pair_is_durable() {
    let data_dir = tempfile::tempdir().expect("temporary data directory must be created");
    let wal_dir = tempfile::tempdir().expect("temporary WAL directory must be created");
    let directory = FaultDirectory::new(wal_dir.path().to_path_buf());
    let handle = open_handle(directory, wal_config(wal_dir.path()));
    let observer = handle.clone();
    let adapter = RagnorDbWalAdapter::new(handle);
    let snapshot = empty_snapshot(7);
    let snapshot_file =
        publish_snapshot_file(data_dir.path(), &snapshot).expect("snapshot file must be durable");

    let published = publish_checkpoint(&adapter, &snapshot_file)
        .expect("matching checkpoint WAL metadata must become durable");

    assert_eq!(published.snapshot_id, snapshot.snapshot_id);
    assert_eq!(
        published.replay_from_lsn,
        Lsn::new(snapshot.replay_from_lsn)
    );
    assert!(published.pointer_extent.end_lsn <= published.marker_extent.start_lsn);
    assert!(observer.durable_lsn() >= published.marker_extent.end_lsn);

    let pointer_record = observer
        .read_at(published.pointer_extent.start_lsn)
        .expect("durable snapshot pointer must be readable");
    let marker_record = observer
        .read_at(published.marker_extent.start_lsn)
        .expect("durable checkpoint marker must be readable");

    assert_eq!(
        pointer_record.record_type,
        RagnorDbWalRecordType::SnapshotPointer.as_wal_record_type()
    );
    assert_eq!(
        marker_record.record_type,
        RagnorDbWalRecordType::CheckpointMarker.as_wal_record_type()
    );

    let pointer =
        SnapshotPointer::decode(&pointer_record.payload).expect("snapshot pointer must decode");
    let marker =
        CheckpointMarker::decode(&marker_record.payload).expect("checkpoint marker must decode");

    assert_eq!(pointer.snapshot_id, marker.snapshot_id);
    assert_eq!(pointer.snapshot_timestamp, marker.snapshot_timestamp);
    assert_eq!(pointer.replay_from_lsn, marker.replay_from_lsn);
    assert_eq!(pointer.relative_path, snapshot_file.relative_path());
    assert_eq!(pointer.file_length, snapshot_file.file_length());
    assert_eq!(
        pointer.file_checksum_crc32c,
        snapshot_file.file_checksum_crc32c()
    );
    assert_eq!(
        pointer.snapshot_format_version,
        snapshot_file.snapshot_format_version()
    );
}

/// Realistic bug caught:
///
/// A marker synchronization failure occurs after the pointer is already
/// durable. Treating that failure as an ordinary retryable error could publish
/// a retention floor even though recovery may observe only an orphan pointer,
/// or append a duplicate marker without first reopening the WAL.
#[test]
fn marker_sync_outcome_unknown_never_returns_a_retention_safe_checkpoint() {
    let data_dir = tempfile::tempdir().expect("temporary data directory must be created");
    let wal_dir = tempfile::tempdir().expect("temporary WAL directory must be created");
    let snapshot = empty_snapshot(9);
    let snapshot_file =
        publish_snapshot_file(data_dir.path(), &snapshot).expect("snapshot file must be durable");

    let expected_pointer = SnapshotPointer {
        snapshot_id: snapshot.snapshot_id,
        snapshot_timestamp: Timestamp(40),
        replay_from_lsn: Lsn::new(snapshot.replay_from_lsn),
        relative_path: snapshot_file.relative_path().to_string(),
        table_ids: BTreeSet::new(),
        file_length: snapshot_file.file_length(),
        file_checksum_crc32c: snapshot_file.file_checksum_crc32c(),
        snapshot_format_version: snapshot_file.snapshot_format_version(),
    };
    let pointer_payload_length = expected_pointer
        .encode()
        .expect("expected pointer must encode")
        .len();
    let directory = FaultDirectory::new(wal_dir.path().to_path_buf());
    let mut config = wal_config(wal_dir.path());

    // Make the first segment exactly large enough for the pointer and its
    // required seal. The smaller marker must therefore roll over to segment 2,
    // where the injected sync failure gives it an indeterminate outcome.
    const SEGMENT_SEAL_PAYLOAD_LENGTH: u64 = 24;
    let seal_record_length = RECORD_HEADER_LEN as u64 + SEGMENT_SEAL_PAYLOAD_LENGTH;
    config.max_record_size =
        u32::try_from(pointer_payload_length).expect("pointer payload length must fit u32");
    config.target_segment_size = SEGMENT_HEADER_LEN
        + RECORD_HEADER_LEN as u64
        + pointer_payload_length as u64
        + seal_record_length;

    directory
        .inject_sync_error(2)
        .expect("marker synchronization fault must be installed");

    let handle = open_handle(directory, config);
    let observer = handle.clone();
    let adapter = RagnorDbWalAdapter::new(handle);
    let error = publish_checkpoint(&adapter, &snapshot_file).unwrap_err();

    let (stage, end_lsn) = match error {
        Error::CheckpointOutcomeUnknown { stage, end_lsn, .. } => (stage, end_lsn),
        other => panic!("expected checkpoint outcome unknown, got {other:?}"),
    };

    assert_eq!(stage, "CheckpointMarker");
    assert!(observer.durable_lsn() < Lsn::new(end_lsn));

    let pointer_record = observer
        .read_at(Lsn::ZERO)
        .expect("the preceding durable pointer must remain readable");

    assert_eq!(
        pointer_record.record_type,
        RagnorDbWalRecordType::SnapshotPointer.as_wal_record_type()
    );
}
