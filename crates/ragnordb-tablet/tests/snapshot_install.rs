use std::{
    fs, process,
    sync::atomic::{AtomicU64, Ordering},
};

use ragnordb_common::{
    command_codec::{NoopCommand, TabletCommand, TabletCommandEnvelope},
    ids::{RaftGroupId, ReplicaId, RequestId, TableId, TabletId},
};
use ragnordb_tablet::{
    Tablet,
    command::TabletStateMachine,
    snapshot::{
        AppliedTabletFrontier, FileTabletSnapshotStore, IncomingTabletSnapshotReceiver,
        TabletSnapshotConfState, TabletSnapshotImage, TabletSnapshotInstallError,
        TabletSnapshotInstallTarget, TabletSnapshotReceiveError, generate_local_snapshot,
        install_incoming_snapshot,
    },
};

static NEXT_TEST_ROOT_ID: AtomicU64 = AtomicU64::new(1);

fn request() -> TabletCommandEnvelope {
    TabletCommandEnvelope::new(
        RequestId {
            client_id: 42,
            sequence: 1,
            raft_group_id: RaftGroupId(17),
        },
        TabletId(31),
        4,
        TabletCommand::Noop(NoopCommand),
    )
    .unwrap()
}

fn conf_state() -> TabletSnapshotConfState {
    TabletSnapshotConfState::new(7, [ReplicaId(1), ReplicaId(2), ReplicaId(3)], [], []).unwrap()
}

fn target() -> TabletSnapshotInstallTarget {
    TabletSnapshotInstallTarget {
        cluster_id: "ragnordb-test".to_string(),
        raft_group_id: RaftGroupId(17),
        tablet_id: TabletId(31),
        table_id: TableId(9),
        tablet_epoch: 4,
    }
}

fn snapshot_image() -> TabletSnapshotImage {
    let tablet = Tablet::new(TabletId(31), TableId(9)).unwrap();
    let mut state_machine = TabletStateMachine::new(tablet, 4, RaftGroupId(17)).unwrap();

    state_machine.apply(request()).unwrap();

    generate_local_snapshot(
        &state_machine,
        "ragnordb-test",
        ReplicaId(2),
        9,
        conf_state(),
        AppliedTabletFrontier::new(12, 5),
    )
    .unwrap()
}

fn store(snapshot_id: u64) -> (FileTabletSnapshotStore, std::path::PathBuf) {
    let test_root_id = NEXT_TEST_ROOT_ID.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "ragnordb-incoming-tablet-snapshot-{}-{}-{}",
        process::id(),
        snapshot_id,
        test_root_id,
    ));

    let _ = fs::remove_dir_all(&root);

    (
        FileTabletSnapshotStore::new(root.clone(), 4096).unwrap(),
        root,
    )
}

/// Catches accepting a complete snapshot without restoring replicated
/// deduplication state or without persisting the exact snapshot boundary.
#[test]
fn incoming_snapshot_restores_state_before_reporting_success() {
    let image = snapshot_image();
    let (store, root) = store(image.metadata.snapshot_id);

    let mut receiver =
        IncomingTabletSnapshotReceiver::begin(&store, image.metadata.clone(), 8).unwrap();

    for chunk in image.data.chunks(8) {
        receiver.push_chunk(chunk).unwrap();
    }

    let mut installed =
        install_incoming_snapshot(&store, receiver, &target(), |pointer, frontier| {
            assert_eq!(pointer.metadata.last_included_index, 12);
            assert_eq!(frontier, AppliedTabletFrontier::new(12, 5));
            Ok::<(), &str>(())
        })
        .unwrap();

    assert_eq!(installed.frontier, AppliedTabletFrontier::new(12, 5));

    let retry = installed.state_machine.apply(request()).unwrap();

    assert!(retry.deduplicated);
    assert_eq!(installed.state_machine.tablet().table_id(), TableId(9));

    let _ = fs::remove_dir_all(root);
}

/// Catches a snapshot from another tablet generation being installed into the
/// local tablet.
#[test]
fn incoming_snapshot_rejects_epoch_mismatch() {
    let image = snapshot_image();
    let (store, root) = store(image.metadata.snapshot_id);

    let mut receiver =
        IncomingTabletSnapshotReceiver::begin(&store, image.metadata.clone(), 8).unwrap();

    for chunk in image.data.chunks(8) {
        receiver.push_chunk(chunk).unwrap();
    }

    let mut wrong_target = target();
    wrong_target.tablet_epoch = 99;

    let result =
        install_incoming_snapshot(&store, receiver, &wrong_target, |_, _| Ok::<(), &str>(()));

    assert!(matches!(
        result,
        Err(TabletSnapshotInstallError::TargetEpochMismatch { .. })
    ));

    let _ = fs::remove_dir_all(root);
}

/// Catches truncated incoming transfers being treated as successful installs.
#[test]
fn incoming_snapshot_rejects_truncated_transfer() {
    let image = snapshot_image();
    let (store, root) = store(image.metadata.snapshot_id);

    let mut receiver =
        IncomingTabletSnapshotReceiver::begin(&store, image.metadata.clone(), 8).unwrap();

    for chunk in image.data[..image.data.len() - 1].chunks(8) {
        receiver.push_chunk(chunk).unwrap();
    }

    let result = install_incoming_snapshot(&store, receiver, &target(), |_, _| Ok::<(), &str>(()));

    assert!(matches!(
        result,
        Err(TabletSnapshotInstallError::Receive(
            TabletSnapshotReceiveError::Incomplete { .. }
        ))
    ));

    let _ = fs::remove_dir_all(root);
}

/// Catches reporting success before the snapshot boundary has reached the
/// durable Raft/A-WAL persistence layer.
#[test]
fn incoming_snapshot_requires_boundary_persistence_success() {
    let image = snapshot_image();
    let (store, root) = store(image.metadata.snapshot_id);

    let mut receiver =
        IncomingTabletSnapshotReceiver::begin(&store, image.metadata.clone(), 8).unwrap();

    for chunk in image.data.chunks(8) {
        receiver.push_chunk(chunk).unwrap();
    }

    let result = install_incoming_snapshot(&store, receiver, &target(), |_, _| {
        Err::<(), &str>("durability is uncertain")
    });

    assert!(matches!(
        result,
        Err(TabletSnapshotInstallError::BoundaryPersistence(reason))
            if reason == "durability is uncertain"
    ));

    let _ = fs::remove_dir_all(root);
}
