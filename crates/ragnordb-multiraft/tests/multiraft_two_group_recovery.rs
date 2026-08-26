#![allow(clippy::type_complexity)]

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use raft::{
    core::node::RaftNode,
    storage::mem::MemStorage,
    types::{ConfState, ReplicaId as CoreReplicaId, Snapshot},
};

use ragnordb_common::ids::{NodeId, RaftGroupId, ReplicaId};

use ragnordb_multiraft::{
    host::{MultiRaftHost, ReadyLoopHostedGroup},
    runtime::{RaftReadyLoop, RaftReadyStateMachine, RaftSnapshotStore},
    storage::{
        adapter::RaftStorageAdapters,
        codec::{DurableRaftEntryPayload, RaftReplicaIdentity, RaftSnapshotPointerRecord},
        persistence::{NodeRaftWal, NodeRaftWalHandle, RaftWal, RaftWalStorage},
        recovery::{
            RaftWalRecoverySource, RecoveredRaftReplica, recover_raft_storage_with_configurations,
        },
    },
};

use wal::{
    error::{BatchAppendFailure, WalError},
    lsn::Lsn,
    types::RecordType,
    wal::{AppendResult, BatchAppendResult, iterator::WalRecord},
};

#[derive(Clone)]
struct SharedWal {
    state: Arc<Mutex<SharedWalState>>,
}

struct SharedWalState {
    next_lsn: Lsn,
    durable_end_lsn: Lsn,
    records: Vec<WalRecord>,
}

impl SharedWal {
    fn new() -> (Self, Arc<Mutex<SharedWalState>>) {
        let state = Arc::new(Mutex::new(SharedWalState {
            next_lsn: Lsn::new(100),
            durable_end_lsn: Lsn::ZERO,
            records: Vec::new(),
        }));

        (
            Self {
                state: Arc::clone(&state),
            },
            state,
        )
    }
}

impl RaftWal for SharedWal {
    fn append_batch_and_sync(
        &mut self,
        records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure> {
        let mut state = self.state.lock().unwrap();

        let mut extents = Vec::with_capacity(records.len());

        for (record_type, payload) in records {
            let start_lsn = state.next_lsn;

            let total_len = payload.len().checked_add(32).unwrap();

            let end_lsn = start_lsn
                .checked_add_bytes(u64::try_from(total_len).unwrap())
                .unwrap();

            state.records.push(WalRecord {
                lsn: start_lsn,
                record_type: *record_type,
                payload: payload.to_vec(),
                total_len: u32::try_from(total_len).unwrap(),
            });

            state.next_lsn = end_lsn;
            state.durable_end_lsn = end_lsn;

            extents.push(AppendResult { start_lsn, end_lsn });
        }

        Ok(BatchAppendResult {
            final_end_lsn: extents
                .last()
                .map(|extent| extent.end_lsn)
                .unwrap_or(state.durable_end_lsn),
            record_extents: extents,
        })
    }
}

struct RecordSource {
    records: std::vec::IntoIter<WalRecord>,
}

impl RecordSource {
    fn new(records: Vec<WalRecord>) -> Self {
        Self {
            records: records.into_iter(),
        }
    }
}

impl RaftWalRecoverySource for RecordSource {
    fn next_record(&mut self) -> Result<Option<WalRecord>, WalError> {
        Ok(self.records.next())
    }
}

#[derive(Clone)]
struct RecordingStateMachine {
    applied: Arc<Mutex<Vec<(u64, Vec<u8>)>>>,
}

impl RaftReadyStateMachine for RecordingStateMachine {
    type Error = &'static str;

    fn restore_snapshot(&mut self, _snapshot: &Snapshot<Vec<u8>>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn apply(&mut self, index: u64, command: &[u8]) -> Result<(), Self::Error> {
        self.applied.lock().unwrap().push((index, command.to_vec()));

        Ok(())
    }
}

#[derive(Default)]
struct NoSnapshots;

impl RaftSnapshotStore for NoSnapshots {
    type Error = &'static str;

    fn publish(
        &mut self,
        _identity: RaftReplicaIdentity,
        _snapshot: &Snapshot<Vec<u8>>,
    ) -> Result<RaftSnapshotPointerRecord, Self::Error> {
        Err("snapshot publication is not used")
    }

    fn load_verified(
        &mut self,
        _pointer: &RaftSnapshotPointerRecord,
    ) -> Result<Snapshot<Vec<u8>>, Self::Error> {
        Err("snapshot loading is not used")
    }
}

fn identity(raft_group_id: u64) -> RaftReplicaIdentity {
    RaftReplicaIdentity::new(RaftGroupId(raft_group_id), ReplicaId(1)).unwrap()
}

fn conf_state() -> ConfState {
    ConfState::new(1, [CoreReplicaId::must(1)], []).unwrap()
}

fn new_group(
    identity: RaftReplicaIdentity,
    wal: NodeRaftWalHandle<SharedWal>,
    applied: Arc<Mutex<Vec<(u64, Vec<u8>)>>>,
) -> Box<dyn ragnordb_multiraft::host::HostedRaftGroup> {
    let raft = RaftNode::bootstrap(
        CoreReplicaId::must(1),
        conf_state(),
        MemStorage::<Vec<u8>, Vec<u8>>::new(),
        MemStorage::<Vec<u8>, Vec<u8>>::new(),
        5,
        2,
    )
    .unwrap();

    let ready_loop = RaftReadyLoop::new(raft, RaftWalStorage::new(wal, identity));

    Box::new(ReadyLoopHostedGroup::new(
        ready_loop,
        RecordingStateMachine { applied },
        NoSnapshots,
    ))
}

fn normal_entries(replica: &RecoveredRaftReplica) -> Vec<(u64, Vec<u8>)> {
    replica
        .log_view()
        .entries()
        .filter_map(|entry| match &entry.record.payload {
            DurableRaftEntryPayload::Normal(command) => Some((entry.record.index, command.clone())),

            DurableRaftEntryPayload::Configuration(_) => None,
        })
        .collect()
}

#[test]
fn two_real_ready_loops_share_one_node_wal_and_recover_without_namespace_collision() {
    let identity_10 = identity(10);
    let identity_20 = identity(20);

    // Both groups intentionally reuse ReplicaId(1) and will reuse their local
    // Raft log indexes. Only RaftGroupId separates their durable namespaces.
    assert_eq!(identity_10.replica_id, identity_20.replica_id,);

    let (shared_wal, wal_state) = SharedWal::new();

    let node_wal = NodeRaftWal::new(shared_wal.clone());

    let mut host = MultiRaftHost::new(NodeId(1), node_wal);

    let wal_10 = host.issue_group_writer(identity_10).unwrap();

    let wal_20 = host.issue_group_writer(identity_20).unwrap();

    let applied_10 = Arc::new(Mutex::new(Vec::new()));

    let applied_20 = Arc::new(Mutex::new(Vec::new()));

    host.register_new_group(new_group(identity_10, wal_10, Arc::clone(&applied_10)))
        .unwrap();

    host.register_new_group(new_group(identity_20, wal_20, Arc::clone(&applied_20)))
        .unwrap();

    host.activate().unwrap();

    // Drive both independent single-replica groups through election. Twenty
    // ticks deliberately exceeds the configured randomized election window.
    for _ in 0..20 {
        host.tick_all(1).unwrap();
    }

    host.propose(RaftGroupId(10), b"group-ten".to_vec(), b"group-ten".len())
        .unwrap();

    host.propose(
        RaftGroupId(20),
        b"group-twenty".to_vec(),
        b"group-twenty".len(),
    )
    .unwrap();

    let applied_10 = applied_10.lock().unwrap().clone();

    let applied_20 = applied_20.lock().unwrap().clone();

    assert_eq!(applied_10.len(), 1);
    assert_eq!(applied_20.len(), 1);

    assert_eq!(applied_10[0].1, b"group-ten".to_vec());

    assert_eq!(applied_20[0].1, b"group-twenty".to_vec());

    // The interesting case: both independent groups are allowed to use the
    // same local Raft log index.
    assert_eq!(applied_10[0].0, applied_20[0].0,);

    // Simulate process crash: discard every live Raft object and retain only
    // the bytes that crossed the shared WAL durability boundary.
    drop(host);

    let (records, durable_end_lsn) = {
        let state = wal_state.lock().unwrap();

        (state.records.clone(), state.durable_end_lsn)
    };

    let configurations = BTreeMap::from([(identity_10, conf_state()), (identity_20, conf_state())]);

    let mut source = RecordSource::new(records);

    let recovered = recover_raft_storage_with_configurations(&mut source, &configurations).unwrap();

    assert_eq!(recovered.len(), 2);

    let recovered_10 = recovered.replica(identity_10).unwrap();

    let recovered_20 = recovered.replica(identity_20).unwrap();

    let entries_10 = normal_entries(recovered_10);

    let entries_20 = normal_entries(recovered_20);

    assert_eq!(entries_10, vec![(applied_10[0].0, b"group-ten".to_vec(),)],);

    assert_eq!(
        entries_20,
        vec![(applied_20[0].0, b"group-twenty".to_vec(),)],
    );

    assert_eq!(
        entries_10[0].0, entries_20[0].0,
        "equal log indexes in different Raft groups must remain independent",
    );

    // Finally prove the recovered durable state is valid input to a fresh Raft
    // core for both groups.
    let adapters_10 = RaftStorageAdapters::from_recovered(recovered_10).unwrap();

    let adapters_20 = RaftStorageAdapters::from_recovered(recovered_20).unwrap();

    let restarted_10: RaftNode<Vec<u8>, Vec<u8>, _, _> = RaftNode::restart(
        CoreReplicaId::must(1),
        adapters_10.log,
        adapters_10.stable,
        5,
        2,
    )
    .unwrap();

    let restarted_20: RaftNode<Vec<u8>, Vec<u8>, _, _> = RaftNode::restart(
        CoreReplicaId::must(1),
        adapters_20.log,
        adapters_20.stable,
        5,
        2,
    )
    .unwrap();

    assert_eq!(
        restarted_10.hard_state().commit,
        recovered_10.hard_state().unwrap().commit,
    );

    assert_eq!(
        restarted_20.hard_state().commit,
        recovered_20.hard_state().unwrap().commit,
    );

    assert_ne!(
        durable_end_lsn,
        Lsn::ZERO,
        "test must have crossed a real simulated durable WAL boundary",
    );
}
