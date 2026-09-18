use std::{
    fs,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use ragnordb_common::ids::{RaftGroupId, ReplicaId, TabletId};
use ragnordb_tablet::snapshot::FileTabletSnapshotStore;

/// Realistic bug caught: after a replica is tombstoned, stale immutable
/// snapshots and its monotonic allocator must not survive as local state that
/// a future process could accidentally discover or reuse.
#[test]
fn remove_replica_state_deletes_only_the_exact_snapshot_namespace() {
    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
    let suffix = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "ragnordb-tablet-snapshot-cleanup-{}-{timestamp}-{suffix}",
        std::process::id()
    ));
    fs::create_dir(&directory).unwrap();
    let store = FileTabletSnapshotStore::new(&directory, 1024 * 1024).unwrap();

    fs::write(directory.join("tablet-10-101-20-1.snapshot"), b"old").unwrap();
    fs::write(directory.join("tablet-10-101-20.next-snapshot-id"), b"2\n").unwrap();
    fs::write(
        directory.join("tablet-10-101-21-1.snapshot"),
        b"other-tablet",
    )
    .unwrap();
    fs::write(
        directory.join("tablet-11-101-20-1.snapshot"),
        b"other-group",
    )
    .unwrap();

    store
        .remove_replica_state(RaftGroupId(10), ReplicaId(101), TabletId(20))
        .unwrap();

    assert!(!directory.join("tablet-10-101-20-1.snapshot").exists());
    assert!(!directory.join("tablet-10-101-20.next-snapshot-id").exists());
    assert!(directory.join("tablet-10-101-21-1.snapshot").exists());
    assert!(directory.join("tablet-11-101-20-1.snapshot").exists());

    fs::remove_dir_all(directory).unwrap();
}
