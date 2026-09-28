#[path = "common/command_apply.rs"]
mod command_apply_test_support;
use command_apply_test_support::ApplyCommittedTestCommand;

use std::{
    fs, process,
    sync::atomic::{AtomicU64, Ordering},
};

use ragnordb_common::{
    codec::{Row, Value, WriteKind},
    command_codec::{
        NoopCommand, SingleShardCommitCommand, TabletCommand, TabletCommandEnvelope, WriteEntry,
    },
    ids::{RaftGroupId, ReplicaId, RequestId, TableId, TabletId, Timestamp, TxnId},
};
use ragnordb_storage::{
    key::{encode_row_key, make_row_key},
    lsm::NodeMemtableBudget,
};
use ragnordb_tablet::{
    Tablet,
    command::TabletStateMachine,
    snapshot::{
        AppliedTabletFrontier, FileTabletSnapshotStore, IncomingTabletSnapshotReceiver,
        TabletSnapshotConfState, TabletSnapshotImage, TabletSnapshotInstallError,
        TabletSnapshotInstallTarget, TabletSnapshotReceiveError, generate_local_snapshot,
        install_incoming_snapshot, restore_verified_snapshot_with_memtable_budget,
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
        replica_id: ReplicaId(1),
        tablet_id: TabletId(31),
        table_id: TableId(9),
        tablet_epoch: 4,
    }
}

fn snapshot_image() -> TabletSnapshotImage {
    let tablet = Tablet::new(TabletId(31), TableId(9)).unwrap();
    let mut state_machine =
        TabletStateMachine::new_local_reference(tablet, 4, RaftGroupId(17)).unwrap();

    state_machine.apply_committed_at(request(), 1, 1).unwrap();
    for index in 2..=12 {
        state_machine
            .apply_frontier_only_at(index, if index == 12 { 5 } else { 1 })
            .unwrap();
    }

    generate_local_snapshot(
        &state_machine,
        "ragnordb-test",
        ReplicaId(1),
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

/// A restored image larger than the mutable limit is kept as a Progress-charged
/// base, not rejected as an oversized active generation.
#[test]
fn restored_snapshot_larger_than_active_limit_uses_accounted_base() {
    let mut source = TabletStateMachine::new_local_reference(
        Tablet::new(TabletId(31), TableId(9)).unwrap(),
        4,
        RaftGroupId(17),
    )
    .unwrap();
    for id in 1..=20 {
        let key = encode_row_key(&make_row_key(TableId(9), &[Value::Int(id)]).unwrap()).unwrap();
        let row = Row {
            values: vec![Value::Int(id), Value::Text("x".repeat(1024))],
        };
        let request = TabletCommandEnvelope::new(
            RequestId {
                client_id: 100 + id as u128,
                sequence: 1,
                raft_group_id: RaftGroupId(17),
            },
            TabletId(31),
            4,
            TabletCommand::SingleShardCommit(SingleShardCommitCommand {
                txn_id: TxnId(id as u64),
                start_timestamp: Timestamp(id as u64),
                commit_timestamp: Timestamp(id as u64 + 100),
                writes: vec![WriteEntry {
                    key,
                    row: Some(row),
                    op: WriteKind::Put,
                }],
            }),
        )
        .unwrap();
        source.apply_committed_at(request, id as u64, 1).unwrap();
    }
    let image = generate_local_snapshot(
        &source,
        "ragnordb-test",
        ReplicaId(1),
        90,
        conf_state(),
        AppliedTabletFrontier::new(20, 1),
    )
    .unwrap();

    let active_limit = 16 * 1024;
    let budget = NodeMemtableBudget::new_with_progress_reserve(128 * 1024, 128 * 1024).unwrap();
    let mut restored = restore_verified_snapshot_with_memtable_budget(
        &image,
        &target(),
        budget.clone(),
        active_limit,
    )
    .unwrap();

    let restored_base_bytes = restored
        .state_machine
        .tablet()
        .storage()
        .restored_base_bytes();
    assert!(restored_base_bytes > active_limit);
    assert_eq!(
        restored
            .state_machine
            .tablet()
            .storage()
            .active_memtable_bytes(),
        0
    );
    assert_eq!(budget.used_bytes(), restored_base_bytes);
    assert_eq!(restored.state_machine.tablet().stats().default_versions, 20);

    let next = TabletCommandEnvelope::new(
        RequestId {
            client_id: 200,
            sequence: 1,
            raft_group_id: RaftGroupId(17),
        },
        TabletId(31),
        4,
        TabletCommand::SingleShardCommit(SingleShardCommitCommand {
            txn_id: TxnId(21),
            start_timestamp: Timestamp(21),
            commit_timestamp: Timestamp(121),
            writes: vec![WriteEntry {
                key: encode_row_key(&make_row_key(TableId(9), &[Value::Int(21)]).unwrap()).unwrap(),
                row: Some(Row {
                    values: vec![Value::Int(21), Value::Text("new active".to_string())],
                }),
                op: WriteKind::Put,
            }],
        }),
    )
    .unwrap();
    restored
        .state_machine
        .apply_committed_at(next, 21, 1)
        .unwrap();
    assert!(
        restored
            .state_machine
            .tablet()
            .storage()
            .active_memtable_bytes()
            > 0
    );

    drop(restored);
    assert_eq!(budget.used_bytes(), 0);
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
