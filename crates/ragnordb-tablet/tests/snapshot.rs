use std::{fs, process};

use ragnordb_common::ids::{RaftGroupId, ReplicaId, TabletId};
use ragnordb_tablet::snapshot::{
    AppliedTabletFrontier, FileTabletSnapshotStore, TabletSnapshotConfState, TabletSnapshotImage,
    TabletSnapshotMetadata, TabletSnapshotMetadataError, TabletSnapshotMetadataInput,
    TabletSnapshotStoreError,
};

fn snapshot_image() -> TabletSnapshotImage {
    let payload = b"tablet snapshot payload".to_vec();

    let conf_state =
        TabletSnapshotConfState::new(7, [ReplicaId(1), ReplicaId(2), ReplicaId(3)], [], [])
            .unwrap();

    let metadata = TabletSnapshotMetadata::for_payload(
        TabletSnapshotMetadataInput {
            cluster_id: "ragnordb-test".to_string(),
            raft_group_id: RaftGroupId(17),
            replica_id: ReplicaId(2),
            tablet_id: TabletId(31),
            tablet_epoch: 4,
            snapshot_id: 9,
            applied_frontier: AppliedTabletFrontier::new(12, 3),
            conf_state,
        },
        &payload,
    )
    .unwrap();

    TabletSnapshotImage::new(metadata, payload).unwrap()
}

/// Catches durable metadata codecs silently dropping tablet identity,
/// tablet epoch, configuration state, or the applied Raft boundary.
#[test]
fn metadata_round_trip_preserves_snapshot_identity_and_boundary() {
    let image = snapshot_image();

    let encoded = image.metadata.encode().unwrap();
    let decoded = TabletSnapshotMetadata::decode(&encoded).unwrap();

    assert_eq!(decoded, image.metadata);
}

/// Catches accepting a snapshot whose file was truncated or whose contents
/// changed after its checksum was calculated.
#[test]
fn snapshot_image_rejects_truncated_and_tampered_payloads() {
    let image = snapshot_image();

    let truncated = image.data[..image.data.len() - 1].to_vec();

    assert_eq!(
        TabletSnapshotImage::new(image.metadata.clone(), truncated),
        Err(TabletSnapshotMetadataError::LengthMismatch {
            expected: image.metadata.total_length,
            actual: image.metadata.total_length - 1,
        })
    );

    let mut tampered = image.data.clone();
    tampered[0] ^= 0x01;

    assert_eq!(
        TabletSnapshotImage::new(image.metadata, tampered),
        Err(TabletSnapshotMetadataError::ChecksumMismatch)
    );
}

/// Catches publishing a valid file under one tablet generation and then
/// accepting it through a pointer for another generation.
#[test]
fn file_store_publishes_idempotently_and_rejects_foreign_epoch() {
    let root = std::env::temp_dir().join(format!("ragnordb-tablet-snapshot-{}", process::id()));

    let _ = fs::remove_dir_all(&root);

    let store = FileTabletSnapshotStore::new(root.clone(), 4096).unwrap();
    let image = snapshot_image();

    let pointer = store.publish(&image).unwrap();

    assert_eq!(store.load_verified(&pointer).unwrap(), image);
    assert_eq!(store.publish(&image).unwrap(), pointer);

    let mut foreign_pointer = pointer.clone();
    foreign_pointer.metadata.tablet_epoch += 1;

    assert!(matches!(
        store.load_verified(&foreign_pointer),
        Err(TabletSnapshotStoreError::FileMetadataMismatch)
    ));

    let _ = fs::remove_dir_all(root);
}
