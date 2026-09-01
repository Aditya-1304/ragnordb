use std::{fs, path::PathBuf, process};

use raft::{
    core::node::RaftNode,
    message::{Envelope, InstallSnapshotRequest, Message},
    storage::mem::MemStorage,
    types::{ConfState, Snapshot},
};
use ragnordb_common::ids::{NodeId, RaftGroupId, ReplicaId};
use ragnordb_multiraft::{
    host::{MultiRaftHost, MultiRaftTurnBudget, ReadyLoopHostedGroup},
    runtime::{
        AppliedRaftFrontier, FileRaftSnapshotStore, RaftReadyLoop, RaftReadyStateMachine,
        RaftSnapshotStore, ReadyApplyError, ReadyLoopError,
    },
    storage::{
        codec::{RaftReplicaIdentity, RaftSnapshotPointerRecord},
        persistence::{NodeRaftWal, RaftWal, RaftWalStorage},
    },
};
use wal::{
    error::BatchAppendFailure,
    lsn::Lsn,
    types::RecordType,
    wal::{AppendResult, BatchAppendResult},
};

type TestNode =
    RaftNode<Vec<u8>, Vec<u8>, MemStorage<Vec<u8>, Vec<u8>>, MemStorage<Vec<u8>, Vec<u8>>>;

type TestLoop = RaftReadyLoop<TestWal, MemStorage<Vec<u8>, Vec<u8>>, MemStorage<Vec<u8>, Vec<u8>>>;

struct TestWal {
    next_lsn: Lsn,
}

impl TestWal {
    fn new() -> Self {
        Self {
            next_lsn: Lsn::new(100),
        }
    }
}

impl RaftWal for TestWal {
    fn append_batch_and_sync(
        &mut self,
        records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure> {
        let mut extents = Vec::with_capacity(records.len());

        for (_, payload) in records {
            let start_lsn = self.next_lsn;
            let end_lsn = start_lsn
                .checked_add_bytes(payload.len() as u64 + 32)
                .unwrap();

            self.next_lsn = end_lsn;
            extents.push(AppendResult { start_lsn, end_lsn });
        }

        Ok(BatchAppendResult {
            final_end_lsn: extents
                .last()
                .map(|extent| extent.end_lsn)
                .unwrap_or(Lsn::ZERO),
            record_extents: extents,
        })
    }
}

#[derive(Default)]
struct RecordingStateMachine {
    restored: Vec<u64>,
    applied: Vec<(u64, Vec<u8>)>,
    fail_apply: bool,
}

impl RaftReadyStateMachine for RecordingStateMachine {
    type Error = &'static str;

    fn restore_snapshot(&mut self, snapshot: &Snapshot<Vec<u8>>) -> Result<(), Self::Error> {
        self.restored.push(snapshot.last_included_index);
        Ok(())
    }

    fn apply(&mut self, index: u64, command: &[u8]) -> Result<(), Self::Error> {
        if self.fail_apply {
            return Err("application failed");
        }

        self.applied.push((index, command.to_vec()));
        Ok(())
    }
}

#[derive(Default)]
struct MemorySnapshotStore {
    published: Vec<RaftSnapshotPointerRecord>,
    snapshots: Vec<Snapshot<Vec<u8>>>,
}

impl RaftSnapshotStore for MemorySnapshotStore {
    type Error = &'static str;

    fn publish(
        &mut self,
        identity: RaftReplicaIdentity,
        snapshot: &Snapshot<Vec<u8>>,
    ) -> Result<RaftSnapshotPointerRecord, Self::Error> {
        let pointer = RaftSnapshotPointerRecord {
            format_version: 1,
            identity,
            snapshot_id: snapshot.snapshot_id,
            last_included_index: snapshot.last_included_index,
            last_included_term: snapshot.last_included_term,
            applied_index: snapshot.last_included_index,
            conf_state: snapshot.conf_state.clone(),
            size_bytes: snapshot.size_bytes,
            checksum: snapshot.checksum,
            file_name: format!(
                "raft-{}-{}-{}.snapshot",
                identity.raft_group_id.0, identity.replica_id.0, snapshot.snapshot_id
            ),
        };

        self.published.push(pointer.clone());
        self.snapshots.push(snapshot.clone());
        Ok(pointer)
    }

    fn load_verified(
        &mut self,
        pointer: &RaftSnapshotPointerRecord,
    ) -> Result<Snapshot<Vec<u8>>, Self::Error> {
        self.snapshots
            .iter()
            .find(|snapshot| snapshot.snapshot_id == pointer.snapshot_id)
            .cloned()
            .ok_or("snapshot was not published")
    }
}

fn identity() -> RaftReplicaIdentity {
    RaftReplicaIdentity::new(RaftGroupId(101), ReplicaId(1)).unwrap()
}

fn new_loop() -> TestLoop {
    let node: TestNode = RaftNode::new(1, Vec::new(), MemStorage::new(), MemStorage::new(), 5, 2);

    RaftReadyLoop::new(node, RaftWalStorage::new(TestWal::new(), identity()))
}

fn prepare_leader(loop_: &mut TestLoop) {
    loop_.persist_next_ready(None).unwrap();

    let ticks = loop_.raft().current_election_timeout();
    loop_.tick(ticks).unwrap();

    loop_.persist_next_ready(None).unwrap();
}

fn snapshot() -> Snapshot<Vec<u8>> {
    let data = b"verified snapshot".to_vec();

    Snapshot {
        snapshot_id: 5,
        last_included_index: 5,
        last_included_term: 2,
        conf_state: ConfState::new(1, [raft::types::ReplicaId::must(1)], []).unwrap(),
        size_bytes: data.len() as u64,
        checksum: *blake3::hash(&data).as_bytes(),
        data,
    }
}

/// Catches committed commands being applied out of order or the applied
/// frontier advancing before successful state-machine application.
#[test]
fn persisted_ready_applies_committed_entries_before_acknowledging_applied_frontier() {
    let mut loop_ = new_loop();
    prepare_leader(&mut loop_);

    loop_.propose(b"command".to_vec(), 7).unwrap();

    let mut store = MemorySnapshotStore::default();
    let mut state_machine = RecordingStateMachine::default();

    let ready = loop_
        .persist_and_apply_next_ready(&mut store, &mut state_machine)
        .unwrap()
        .unwrap();

    assert_eq!(state_machine.applied, vec![(1, b"command".to_vec())]);
    assert_eq!(loop_.raft().last_applied(), 1);
    assert_eq!(ready.committed_entries.len(), 1);
    assert_eq!(
        loop_.applied_frontier(),
        Some(AppliedRaftFrontier::new(1, ready.committed_entries[0].term))
    );
}

/// Catches a host turn acknowledging a Ready after only part of its apply
/// budget was available, which would either lose the remaining entries or
/// admit a new Raft mutation before the applied frontier is complete.
#[test]
fn host_turn_resumes_a_ready_generation_after_apply_budget_exhaustion() {
    let mut loop_ = new_loop();
    prepare_leader(&mut loop_);
    loop_.propose(b"command".to_vec(), 7).unwrap();

    let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal::new()));
    let _writer = host.issue_group_writer(identity()).unwrap();
    host.register_new_group(Box::new(ReadyLoopHostedGroup::new(
        loop_,
        RecordingStateMachine::default(),
        MemorySnapshotStore::default(),
    )))
    .unwrap();
    host.activate().unwrap();
    host.schedule_group_now(identity().raft_group_id).unwrap();

    let blocked = host
        .run_turn(
            0,
            MultiRaftTurnBudget {
                max_groups: 1,
                max_messages: 0,
                max_ready_generations: 1,
                max_apply_entries: 0,
                max_snapshot_bytes: usize::MAX,
            },
        )
        .unwrap();
    assert_eq!(blocked.apply_entries, 0);
    assert_eq!(blocked.ready_generations, 1);

    let resumed = host
        .run_turn(
            0,
            MultiRaftTurnBudget {
                max_groups: 1,
                max_messages: 0,
                max_ready_generations: 1,
                max_apply_entries: 1,
                max_snapshot_bytes: usize::MAX,
            },
        )
        .unwrap();
    assert_eq!(resumed.apply_entries, 1);
    assert_eq!(resumed.ready_generations, 1);
}

/// Catches an application failure after WAL persistence from being treated as
/// a retryable success.
#[test]
fn state_machine_failure_quarantines_the_group_without_advancing_applied_frontier() {
    let mut loop_ = new_loop();
    prepare_leader(&mut loop_);
    loop_.propose(b"command".to_vec(), 7).unwrap();

    let mut store = MemorySnapshotStore::default();
    let mut state_machine = RecordingStateMachine {
        fail_apply: true,
        ..RecordingStateMachine::default()
    };

    assert!(matches!(
        loop_.persist_and_apply_next_ready(&mut store, &mut state_machine),
        Err(ReadyApplyError::Application { .. })
    ));

    assert_eq!(loop_.raft().last_applied(), 0);
    assert_eq!(loop_.applied_frontier(), None);
    assert_eq!(loop_.tick(1), Err(ReadyLoopError::GroupQuarantined));
}

/// Catches snapshot metadata being acknowledged before the external image is
/// completed, and catches restore/apply ordering around the staged snapshot.
#[test]
fn snapshot_is_verified_restored_and_applied_before_ready_release() {
    let snapshot = snapshot();

    let mut node: TestNode = RaftNode::new(1, vec![2], MemStorage::new(), MemStorage::new(), 5, 2);

    node.step(Envelope {
        from: raft::types::ReplicaId::must(2),
        to: raft::types::ReplicaId::must(1),
        msg: Message::InstallSnapshot(InstallSnapshotRequest::new(
            3,
            raft::types::ReplicaId::must(2),
            snapshot.metadata(),
        )),
    });

    let mut loop_ = RaftReadyLoop::new(node, RaftWalStorage::new(TestWal::new(), identity()));
    let mut store = MemorySnapshotStore::default();
    let mut state_machine = RecordingStateMachine::default();

    let metadata_ready = loop_
        .persist_and_apply_next_ready(&mut store, &mut state_machine)
        .unwrap()
        .unwrap();

    assert_eq!(metadata_ready.snapshot_install, Some(snapshot.metadata()));

    loop_.complete_snapshot_install(snapshot).unwrap();

    let ready = loop_
        .persist_and_apply_next_ready(&mut store, &mut state_machine)
        .unwrap()
        .unwrap();

    assert_eq!(store.published.len(), 1);
    assert_eq!(state_machine.restored, vec![5]);
    assert_eq!(loop_.raft().last_applied(), 5);
    assert_eq!(ready.snapshot.unwrap().last_included_index, 5);
}

/// Catches accepting a snapshot file after its contents changed since
/// publication.
#[test]
fn file_snapshot_store_rejects_tampered_snapshot_bytes() {
    let root = std::env::temp_dir().join(format!("ragnordb-ready-snapshot-{}", process::id()));

    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();

    let mut store = FileRaftSnapshotStore::new(PathBuf::from(&root)).unwrap();
    let snapshot = snapshot();
    let pointer = store.publish(identity(), &snapshot).unwrap();

    fs::write(root.join(&pointer.file_name), b"tampered").unwrap();

    assert!(store.load_verified(&pointer).is_err());

    let _ = fs::remove_dir_all(root);
}
