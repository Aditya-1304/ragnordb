use raft::{
    core::node::RaftNode,
    storage::mem::MemStorage,
    types::{HardState, ReplicaId as CoreReplicaId, Snapshot},
};
use ragnordb_common::ids::{RaftGroupId, ReplicaId, TabletId};
use ragnordb_multiraft::{
    runtime::{RaftReadyLoop, RaftReadyStateMachine, RaftSnapshotStore},
    snapshot::{
        SnapshotWorkController, SnapshotWorkError, SnapshotWorkKind, SnapshotWorkLimits,
        TabletSnapshotRaftStore, TabletSnapshotReceiveSession, TabletSnapshotTransfer,
        generate_tablet_snapshot_from_ready_loop, install_incoming_tablet_snapshot,
        persist_tablet_snapshot_boundary_via_ready_loop,
    },
    storage::{
        codec::{RaftReplicaIdentity, RaftSnapshotPointerRecord},
        persistence::{RaftWal, RaftWalRecordType, RaftWalStorage},
    },
};
use ragnordb_tablet::snapshot::{
    AppliedTabletFrontier, FileTabletSnapshotStore, TabletSnapshotConfState, TabletSnapshotImage,
    TabletSnapshotInstallTarget, TabletSnapshotMetadata, TabletSnapshotMetadataInput,
    TabletSnapshotPointer, generate_local_snapshot,
};
use ragnordb_tablet::{Tablet, command::TabletStateMachine};
use std::{
    fs, process,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use wal::{
    error::{BatchAppendFailure, WalError},
    lsn::Lsn,
    types::RecordType,
    wal::{AppendResult, BatchAppendResult},
};

fn pointer() -> TabletSnapshotPointer {
    let payload = b"tablet-state".to_vec();

    let metadata = TabletSnapshotMetadata::for_payload(
        TabletSnapshotMetadataInput {
            cluster_id: "ragnordb-test".to_string(),
            raft_group_id: RaftGroupId(17),
            replica_id: ReplicaId(2),
            tablet_id: TabletId(31),
            tablet_epoch: 4,
            snapshot_id: 9,
            applied_frontier: AppliedTabletFrontier::new(12, 5),
            conf_state: TabletSnapshotConfState::new(
                7,
                [ReplicaId(1), ReplicaId(2), ReplicaId(3)],
                [],
                [],
            )
            .unwrap(),
        },
        &payload,
    )
    .unwrap();

    TabletSnapshotPointer {
        metadata,
        file_name: "tablet-17-2-31-9.snapshot".to_string(),
    }
}

fn image() -> TabletSnapshotImage {
    let pointer = pointer();

    TabletSnapshotImage::new(pointer.metadata, b"tablet-state".to_vec()).unwrap()
}

fn installable_image() -> TabletSnapshotImage {
    let tablet = Tablet::new(TabletId(31), ragnordb_common::ids::TableId(9)).unwrap();
    let state_machine = TabletStateMachine::new(tablet, 4, RaftGroupId(17)).unwrap();

    generate_local_snapshot(
        &state_machine,
        "ragnordb-test",
        ReplicaId(2),
        9,
        TabletSnapshotConfState::new(7, [ReplicaId(1), ReplicaId(2), ReplicaId(3)], [], [])
            .unwrap(),
        AppliedTabletFrontier::new(12, 5),
    )
    .unwrap()
}

#[derive(Debug)]
struct RecordingWal {
    next_lsn: Lsn,
    record_types: Vec<RecordType>,
    fail_with_unknown_outcome: Arc<AtomicBool>,
}

type TestReadyLoop =
    RaftReadyLoop<RecordingWal, MemStorage<Vec<u8>, Vec<u8>>, MemStorage<Vec<u8>, Vec<u8>>>;

#[derive(Default)]
struct RecordingStateMachine;

impl RaftReadyStateMachine for RecordingStateMachine {
    type Error = &'static str;

    fn restore_snapshot(&mut self, _snapshot: &Snapshot<Vec<u8>>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn apply(&mut self, _index: u64, _command: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Default)]
struct UnusedSnapshotStore;

impl RaftSnapshotStore for UnusedSnapshotStore {
    type Error = &'static str;

    fn publish(
        &mut self,
        _identity: RaftReplicaIdentity,
        _snapshot: &Snapshot<Vec<u8>>,
    ) -> Result<RaftSnapshotPointerRecord, Self::Error> {
        Err("snapshot publication is not used by this test")
    }

    fn load_verified(
        &mut self,
        _pointer: &RaftSnapshotPointerRecord,
    ) -> Result<Snapshot<Vec<u8>>, Self::Error> {
        Err("snapshot loading is not used by this test")
    }
}

fn ready_loop() -> TestReadyLoop {
    let node = RaftNode::new(2, Vec::new(), MemStorage::new(), MemStorage::new(), 5, 2);

    RaftReadyLoop::new(
        node,
        RaftWalStorage::new(
            RecordingWal {
                next_lsn: Lsn::new(100),
                record_types: Vec::new(),
                fail_with_unknown_outcome: Arc::new(AtomicBool::new(false)),
            },
            RaftReplicaIdentity::new(RaftGroupId(17), ReplicaId(2)).unwrap(),
        ),
    )
}

fn ready_loop_with_failure_flag() -> (TestReadyLoop, Arc<AtomicBool>) {
    let fail_with_unknown_outcome = Arc::new(AtomicBool::new(false));
    let node = RaftNode::new(2, Vec::new(), MemStorage::new(), MemStorage::new(), 5, 2);
    let storage = RaftWalStorage::new(
        RecordingWal {
            next_lsn: Lsn::new(100),
            record_types: Vec::new(),
            fail_with_unknown_outcome: Arc::clone(&fail_with_unknown_outcome),
        },
        RaftReplicaIdentity::new(RaftGroupId(17), ReplicaId(2)).unwrap(),
    );

    (RaftReadyLoop::new(node, storage), fail_with_unknown_outcome)
}

fn prepare_leader(loop_: &mut TestReadyLoop) {
    loop_.persist_next_ready(None).unwrap();
    let ticks = loop_.raft().current_election_timeout();
    loop_.tick(ticks).unwrap();
    loop_.persist_next_ready(None).unwrap();
}

impl RaftWal for RecordingWal {
    fn append_batch_and_sync(
        &mut self,
        records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure> {
        let mut extents = Vec::with_capacity(records.len());

        for (record_type, payload) in records {
            let start_lsn = self.next_lsn;
            let end_lsn = start_lsn
                .checked_add_bytes(payload.len() as u64 + 32)
                .unwrap();

            self.record_types.push(*record_type);
            self.next_lsn = end_lsn;

            extents.push(AppendResult { start_lsn, end_lsn });
        }

        let result = BatchAppendResult {
            final_end_lsn: extents
                .last()
                .map(|extent| extent.end_lsn)
                .unwrap_or(Lsn::ZERO),
            record_extents: extents,
        };

        if self.fail_with_unknown_outcome.load(Ordering::SeqCst) {
            return Err(BatchAppendFailure::OutcomeUnknown {
                result,
                source: WalError::BrokenDurabilityContract,
            });
        }

        Ok(result)
    }
}

#[test]
fn tablet_transfer_derives_core_metadata_without_transporting_core_snapshot_state() {
    let transfer = TabletSnapshotTransfer::from_image(image()).unwrap();

    assert_eq!(transfer.raft_metadata().snapshot_id, 9);
    assert_eq!(transfer.raft_metadata().last_included_index, 12);
    assert_eq!(transfer.raft_metadata().last_included_term, 5);

    assert_eq!(
        transfer.raft_metadata().conf_state.voters,
        [
            CoreReplicaId::must(1),
            CoreReplicaId::must(2),
            CoreReplicaId::must(3),
        ]
        .into_iter()
        .collect()
    );

    let snapshot = transfer.into_core_snapshot();

    assert_eq!(snapshot.data, b"tablet-state");
    assert_eq!(snapshot.size_bytes, b"tablet-state".len() as u64);
}

/// Catches an unbounded sender that would allow multiple follower catch-ups to
/// consume memory and bandwidth beyond the configured node-local budget.
#[test]
fn tablet_snapshot_sender_is_bounded_and_reports_progress() {
    let work = SnapshotWorkController::new(SnapshotWorkLimits {
        max_generations: 1,
        max_sends: 1,
        max_receives: 1,
        max_installs: 1,
    })
    .unwrap();
    let expected = image();
    let expected_data = expected.data.clone();

    let mut sender = TabletSnapshotTransfer::from_image(expected.clone())
        .unwrap()
        .into_sender(&work, 3)
        .unwrap();

    assert_eq!(work.progress().active_sends, 1);
    assert_eq!(work.progress().send_bytes_total, expected_data.len() as u64);

    assert!(matches!(
        TabletSnapshotTransfer::from_image(expected)
            .unwrap()
            .into_sender(&work, 3),
        Err(
            ragnordb_multiraft::snapshot::TabletSnapshotIntegrationError::Work(
                SnapshotWorkError::LimitReached {
                    kind: SnapshotWorkKind::Send,
                    limit: 1,
                }
            )
        )
    ));

    let mut received = Vec::new();
    while let Some(chunk) = sender.next_chunk() {
        assert!(chunk.len() <= 3);
        received.extend_from_slice(&chunk);
    }

    assert!(sender.is_complete());
    assert_eq!(received, expected_data);
    assert_eq!(work.progress().send_bytes_completed, received.len() as u64);

    drop(sender);
    assert_eq!(work.progress().active_sends, 0);
    assert_eq!(work.progress().rejected_operations, 1);
}

/// Catches a receiver that bypasses the shared admission controller or counts
/// bytes before the tablet receiver has accepted a bounded chunk.
#[test]
fn tablet_snapshot_receiver_is_bounded_and_reports_verified_progress() {
    let root = std::env::temp_dir().join(format!(
        "ragnordb-multiraft-tablet-receive-session-{}",
        process::id()
    ));
    let _ = fs::remove_dir_all(&root);

    let work = SnapshotWorkController::default();
    let store = FileTabletSnapshotStore::new(root.clone(), 4096).unwrap();
    let expected = image();
    let mut receiver =
        TabletSnapshotReceiveSession::begin(&work, &store, expected.metadata.clone(), 3).unwrap();

    for chunk in expected.data.chunks(3) {
        receiver.push_chunk(chunk).unwrap();
    }

    assert_eq!(receiver.finish().unwrap(), expected);

    let progress = work.progress();
    assert_eq!(progress.active_receives, 0);
    assert_eq!(progress.receive_bytes_total, 12);
    assert_eq!(progress.receive_bytes_completed, 12);

    let _ = fs::remove_dir_all(root);
}

/// Realistic bug caught:
///
/// The durable install boundary must consume the bounded receive session, not
/// a raw receiver that lets callers bypass the shared receive admission and
/// progress accounting. Otherwise concurrent snapshot ingestion can exceed
/// the host budget even though the standalone receiver tests remain green.
#[test]
fn durable_install_requires_the_bounded_receive_session() {
    let root = std::env::temp_dir().join(format!(
        "ragnordb-multiraft-tablet-install-session-{}",
        process::id()
    ));
    let _ = fs::remove_dir_all(&root);

    let work = SnapshotWorkController::default();
    let store = FileTabletSnapshotStore::new(root.clone(), 4096).unwrap();
    let expected = installable_image();
    let mut receiver =
        TabletSnapshotReceiveSession::begin(&work, &store, expected.metadata.clone(), 3).unwrap();

    for chunk in expected.data.chunks(3) {
        receiver.push_chunk(chunk).unwrap();
    }

    let mut loop_ = ready_loop();

    let installed = install_incoming_tablet_snapshot(
        &work,
        &store,
        receiver,
        &TabletSnapshotInstallTarget {
            cluster_id: "ragnordb-test".to_string(),
            raft_group_id: RaftGroupId(17),
            tablet_id: TabletId(31),
            table_id: ragnordb_common::ids::TableId(9),
            tablet_epoch: 4,
        },
        &mut loop_,
        HardState {
            current_term: 5,
            voted_for: Some(CoreReplicaId::must(2)),
            commit: 12,
        },
    )
    .unwrap();

    assert_eq!(
        installed.installed.frontier,
        AppliedTabletFrontier::new(12, 5)
    );
    assert_eq!(installed.persisted.record_count, 2);

    let progress = work.progress();
    assert_eq!(progress.active_receives, 0);
    assert_eq!(progress.active_installs, 0);
    assert_eq!(progress.receive_bytes_total, expected.data.len() as u64);
    assert_eq!(progress.receive_bytes_completed, expected.data.len() as u64);
    assert_eq!(progress.install_bytes_total, expected.metadata.total_length);
    assert_eq!(
        progress.install_bytes_completed,
        expected.metadata.total_length
    );

    let _ = fs::remove_dir_all(root);
}

/// Realistic bug caught:
///
/// An uncertain A-WAL result while persisting an incoming snapshot boundary
/// must immediately fence the Ready loop. Returning an ordinary snapshot error
/// would allow later ticks or proposals against storage whose durable contents
/// are unknown.
#[test]
fn uncertain_snapshot_boundary_persistence_fences_the_ready_loop() {
    let root = std::env::temp_dir().join(format!(
        "ragnordb-multiraft-tablet-install-unknown-{}",
        process::id()
    ));
    let _ = fs::remove_dir_all(&root);

    let work = SnapshotWorkController::default();
    let store = FileTabletSnapshotStore::new(root.clone(), 4096).unwrap();
    let expected = installable_image();
    let mut receiver =
        TabletSnapshotReceiveSession::begin(&work, &store, expected.metadata.clone(), 64).unwrap();
    receiver.push_chunk(&expected.data).unwrap();

    let (mut loop_, fail_with_unknown_outcome) = ready_loop_with_failure_flag();
    fail_with_unknown_outcome.store(true, Ordering::SeqCst);

    assert!(
        install_incoming_tablet_snapshot(
            &work,
            &store,
            receiver,
            &TabletSnapshotInstallTarget {
                cluster_id: "ragnordb-test".to_string(),
                raft_group_id: RaftGroupId(17),
                tablet_id: TabletId(31),
                table_id: ragnordb_common::ids::TableId(9),
                tablet_epoch: 4,
            },
            &mut loop_,
            HardState {
                current_term: 5,
                voted_for: Some(CoreReplicaId::must(2)),
                commit: 12,
            },
        )
        .is_err()
    );
    assert_eq!(
        loop_.tick(1).unwrap_err(),
        ragnordb_multiraft::runtime::ReadyLoopError::RecoveryRequired
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn incoming_tablet_boundary_persists_pointer_before_hard_state() {
    let mut loop_ = ready_loop();
    let persisted = persist_tablet_snapshot_boundary_via_ready_loop(
        &mut loop_,
        &pointer(),
        AppliedTabletFrontier::new(12, 5),
        HardState {
            current_term: 5,
            voted_for: Some(CoreReplicaId::must(2)),
            commit: 12,
        },
    )
    .unwrap();

    assert_eq!(persisted.record_count, 2);

    assert_eq!(
        loop_.persistence().wal().record_types,
        vec![
            RaftWalRecordType::SnapshotPointer.as_wal_record_type(),
            RaftWalRecordType::HardState.as_wal_record_type(),
        ]
    );
}

#[test]
fn raft_snapshot_store_reads_tablet_envelopes_as_core_payloads() {
    let root = std::env::temp_dir().join(format!(
        "ragnordb-multiraft-tablet-snapshot-{}",
        process::id()
    ));

    let _ = fs::remove_dir_all(&root);

    let file_store = FileTabletSnapshotStore::new(root.clone(), 4096).unwrap();
    let tablet_image = image();
    let tablet_pointer = file_store.publish(&tablet_image).unwrap();

    let mut raft_store = TabletSnapshotRaftStore::new(file_store, tablet_pointer);

    let transfer = TabletSnapshotTransfer::from_image(tablet_image).unwrap();
    let core_snapshot = transfer.into_core_snapshot();

    let identity = RaftReplicaIdentity::new(RaftGroupId(17), ReplicaId(2)).unwrap();

    let raft_pointer = raft_store.publish(identity, &core_snapshot).unwrap();
    let loaded = raft_store.load_verified(&raft_pointer).unwrap();

    assert_eq!(loaded, core_snapshot);

    let _ = fs::remove_dir_all(root);
}

/// Catches constructing a tablet snapshot from a commit index or current term
/// before the state machine has actually acknowledged the corresponding Ready.
#[test]
fn local_tablet_snapshot_requires_the_ready_loop_applied_frontier() {
    let mut loop_ = ready_loop();
    prepare_leader(&mut loop_);
    let work = SnapshotWorkController::default();

    let tablet =
        ragnordb_tablet::Tablet::new(TabletId(31), ragnordb_common::ids::TableId(9)).unwrap();
    let state_machine =
        ragnordb_tablet::command::TabletStateMachine::new(tablet, 4, RaftGroupId(17)).unwrap();

    let conf_state =
        TabletSnapshotConfState::new(7, [ReplicaId(1), ReplicaId(2), ReplicaId(3)], [], [])
            .unwrap();

    assert!(matches!(
        generate_tablet_snapshot_from_ready_loop(
            &work,
            &loop_,
            &state_machine,
            "ragnordb-test",
            ReplicaId(2),
            9,
            conf_state.clone(),
        ),
        Err(
            ragnordb_multiraft::snapshot::TabletSnapshotIntegrationError::AppliedFrontierUnavailable
        )
    ));

    loop_.propose(b"command".to_vec(), 7).unwrap();
    let mut snapshot_store = UnusedSnapshotStore;
    let mut applied_state_machine = RecordingStateMachine;
    loop_
        .persist_and_apply_next_ready(&mut snapshot_store, &mut applied_state_machine)
        .unwrap();

    let image = generate_tablet_snapshot_from_ready_loop(
        &work,
        &loop_,
        &state_machine,
        "ragnordb-test",
        ReplicaId(2),
        9,
        conf_state,
    )
    .unwrap();

    let frontier = loop_.applied_frontier().unwrap();
    assert_eq!(image.metadata.last_included_index, frontier.index);
    assert_eq!(image.metadata.last_included_term, frontier.term);
}
