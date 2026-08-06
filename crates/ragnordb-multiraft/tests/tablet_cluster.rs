use std::time::{Duration, Instant};

use ragnordb_common::{
    codec::{Row, Value, WriteKind},
    command_codec::{
        CachedTabletCommandOutcome, CachedTabletCommandRejectionKind, NoopCommand, PrewriteCommand,
        SingleShardCommitCommand, TabletCommand, TabletCommandEnvelope, TabletStateMachineSnapshot,
        WriteEntry,
    },
    ids::{RaftGroupId, RequestId, TableId, TabletId, Timestamp, TxnId},
};
use ragnordb_multiraft::{
    proposal::ProposalCompletion,
    snapshot::SnapshotWorkController,
    tablet_cluster::{InMemoryTabletCluster, TabletClusterError},
};
use ragnordb_storage::key::{encode_row_key, make_row_key};
use ragnordb_tablet::{
    Tablet,
    command::{
        TabletCommandApplyError, TabletCommandApplyOutcome, TabletCommandApplyResult,
        TabletStateMachine,
    },
    snapshot::FileTabletSnapshotStore,
};
use ragnordb_txn::Transaction;
use wal::{
    error::{BatchAppendFailure, WalError},
    lsn::Lsn,
    types::RecordType,
    wal::{AppendResult, BatchAppendResult, iterator::WalRecord},
};

use std::{
    collections::BTreeMap,
    fs, process,
    sync::{Arc, Mutex},
};

use raft::{
    core::node::RaftNode,
    storage::mem::MemStorage,
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{ConfState, ReplicaId as CoreReplicaId},
};

use ragnordb_multiraft::{
    proposal::ProposalPosition,
    storage::{
        codec::{DurableRaftEntryPayload, RaftReplicaIdentity},
        persistence::{RaftWal, RaftWalStorage},
        recovery::{RaftWalRecoverySource, recover_raft_storage_with_configurations},
    },
    tablet_apply::TabletCommandApplier,
    tablet_cluster::TabletRaftReadyLoop,
};
const TABLET_ID: TabletId = TabletId(41);
const TABLE_ID: TableId = TableId(9);
const RAFT_GROUP_ID: RaftGroupId = RaftGroupId(91);
const TABLET_EPOCH: u64 = 7;

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

impl ragnordb_multiraft::storage::persistence::RaftWal for TestWal {
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

#[derive(Debug, Clone)]
struct DurableTestWal {
    state: Arc<Mutex<DurableTestWalState>>,
}

#[derive(Debug)]
struct DurableTestWalState {
    next_lsn: Lsn,
    records: Vec<WalRecord>,
}

impl DurableTestWal {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(DurableTestWalState {
                next_lsn: Lsn::new(100),
                records: Vec::new(),
            })),
        }
    }

    fn records(&self) -> Vec<WalRecord> {
        self.state
            .lock()
            .expect("durable test WAL mutex must not be poisoned")
            .records
            .clone()
    }

    fn durable_end_lsn(&self) -> Lsn {
        self.state
            .lock()
            .expect("durable test WAL mutex must not be poisoned")
            .next_lsn
    }
}

impl RaftWal for DurableTestWal {
    fn append_batch_and_sync(
        &mut self,
        records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure> {
        let mut state = self
            .state
            .lock()
            .expect("durable test WAL mutex must not be poisoned");

        let mut extents = Vec::with_capacity(records.len());

        for (record_type, payload) in records {
            let start_lsn = state.next_lsn;
            let end_lsn = start_lsn
                .checked_add_bytes(payload.len() as u64 + 32)
                .expect("test WAL LSN must not overflow");

            state.records.push(WalRecord {
                lsn: start_lsn,
                record_type: *record_type,
                payload: payload.to_vec(),
                total_len: (payload.len() + 32) as u32,
            });

            state.next_lsn = end_lsn;
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

struct DurableRecordSource {
    records: std::vec::IntoIter<WalRecord>,
}

impl DurableRecordSource {
    fn new(records: Vec<WalRecord>) -> Self {
        Self {
            records: records.into_iter(),
        }
    }
}

impl RaftWalRecoverySource for DurableRecordSource {
    fn next_record(&mut self) -> Result<Option<WalRecord>, WalError> {
        Ok(self.records.next())
    }
}

type RestartableCoreRaftNode =
    RaftNode<Vec<u8>, Vec<u8>, MemStorage<Vec<u8>, Vec<u8>>, MemStorage<Vec<u8>, Vec<u8>>>;

fn durable_cluster() -> InMemoryTabletCluster<DurableTestWal> {
    InMemoryTabletCluster::new(
        [
            DurableTestWal::new(),
            DurableTestWal::new(),
            DurableTestWal::new(),
        ],
        TABLET_ID,
        TABLE_ID,
        RAFT_GROUP_ID,
        TABLET_EPOCH,
    )
    .unwrap()
}

fn initial_conf_state() -> ConfState {
    ConfState::new(
        1,
        [
            CoreReplicaId::must(1),
            CoreReplicaId::must(2),
            CoreReplicaId::must(3),
        ],
        [],
    )
    .unwrap()
}

fn rebuild_replica_from_durable_wal(
    wal: DurableTestWal,
    node_id: u64,
) -> (TabletRaftReadyLoop<DurableTestWal>, TabletCommandApplier) {
    let identity =
        RaftReplicaIdentity::new(RAFT_GROUP_ID, ragnordb_common::ids::ReplicaId(node_id)).unwrap();

    let mut source = DurableRecordSource::new(wal.records());

    let configurations = BTreeMap::from([(identity, initial_conf_state())]);

    let recovered = recover_raft_storage_with_configurations(&mut source, &configurations).unwrap();

    let replica = recovered
        .replica(identity)
        .expect("the replica must have durable Raft records");

    let committed_through = replica
        .hard_state()
        .map(|hard_state| hard_state.commit)
        .unwrap_or(0);

    // Reconstruct the core Raft stores from the recovered durable view.  The
    // ready loop owns the WAL-backed persistence boundary, while the core
    // Raft node only needs an in-memory view of the already recovered state.
    let mut log = MemStorage::<Vec<u8>, Vec<u8>>::new();

    if let Some((snapshot_index, snapshot_term)) = replica.log_view().snapshot_boundary() {
        log.install_snapshot(snapshot_index, snapshot_term);
    }

    let recovered_entries = replica
        .log_view()
        .entries()
        .map(|entry| {
            entry
                .record
                .to_core()
                .expect("recovered durable entry must decode as a core Raft entry")
        })
        .collect::<Vec<_>>();
    log.append(&recovered_entries);

    let mut stable = MemStorage::<Vec<u8>, Vec<u8>>::new();
    stable.set_conf_state(
        replica
            .conf_state()
            .cloned()
            .expect("recovered replica must have a configuration"),
    );
    stable.set_hard_state(replica.hard_state().cloned().unwrap_or_default());

    let node: RestartableCoreRaftNode =
        RaftNode::restart(CoreReplicaId::must(node_id), log, stable, 5, 2).unwrap();

    let tablet = Tablet::new(TABLET_ID, TABLE_ID).unwrap();
    let state_machine = TabletStateMachine::new(tablet, TABLET_EPOCH, RAFT_GROUP_ID).unwrap();
    let mut tablet_applier = TabletCommandApplier::new(state_machine);

    for entry in replica.log_view().entries() {
        if entry.record.index > committed_through {
            break;
        }

        if let DurableRaftEntryPayload::Normal(command) = &entry.record.payload {
            tablet_applier
                .apply_committed(
                    ProposalPosition {
                        term: entry.record.term,
                        index: entry.record.index,
                    },
                    command,
                )
                .expect("durable committed tablet commands must replay");
        }
    }

    let durable_end_lsn = wal.durable_end_lsn();

    let persistence = RaftWalStorage::from_recovered(wal, replica, durable_end_lsn).unwrap();

    let mut ready_loop = TabletRaftReadyLoop::new(node, persistence);

    ready_loop
        .advance_applied(committed_through)
        .expect("recovered applied frontier must be acknowledged");

    (ready_loop, tablet_applier)
}

fn request_id_with_sequence(sequence: u64) -> RequestId {
    RequestId {
        client_id: 41,
        sequence,
        raft_group_id: RAFT_GROUP_ID,
    }
}

fn request_id() -> RequestId {
    request_id_with_sequence(1)
}

/// Realistic bug caught:
///
/// A follower whose Raft suffix is no longer retained must not remain
/// permanently behind or be marked caught up merely because ordinary
/// AppendEntries delivery stopped. The leader must publish a verified tablet
/// snapshot and the follower must restore it before applying later entries.
#[test]
fn far_behind_follower_catches_up_through_a_tablet_snapshot() {
    let mut cluster = durable_cluster();
    let leader_id = cluster.elect_leader().unwrap();
    let follower_id = [1, 2, 3]
        .into_iter()
        .find(|node_id| *node_id != leader_id)
        .unwrap();

    cluster.kill_replica(follower_id).unwrap();

    let mut last_key = None;
    let mut last_row = None;

    for sequence in 1..=4 {
        let request_id = request_id_with_sequence(sequence);
        let (command, key, row) = single_shard_command_for(
            request_id.clone(),
            sequence as i64,
            100 + sequence,
            1_000 + sequence,
        );

        let ticket = cluster
            .propose(
                request_id,
                command,
                Instant::now() + Duration::from_secs(30),
            )
            .unwrap();

        assert!(matches!(
            ticket.recv_timeout(Duration::from_secs(1)).unwrap(),
            ProposalCompletion::Applied { .. }
        ));

        last_key = Some(key);
        last_row = Some(row);
    }

    let root = std::env::temp_dir().join(format!(
        "ragnordb-multiraft-cluster-snapshot-{}",
        process::id()
    ));
    let _ = fs::remove_dir_all(&root);
    let store = FileTabletSnapshotStore::new(root.clone(), 4096).unwrap();
    let work = SnapshotWorkController::default();

    cluster
        .publish_tablet_snapshot(leader_id, "ragnordb-test", &store, &work, 3)
        .unwrap();

    cluster.restart_replica(follower_id).unwrap();
    cluster
        .catch_up_replica_with_snapshot(leader_id, follower_id, &store, &work, 3)
        .unwrap();

    assert_eq!(
        cluster.last_applied(follower_id).unwrap(),
        cluster.last_applied(leader_id).unwrap()
    );

    let reader = Transaction::new(TxnId(999), Timestamp(2_000)).unwrap();
    let restored = cluster
        .tablet(follower_id)
        .unwrap()
        .state_machine()
        .tablet()
        .get(&reader, &last_key.unwrap())
        .unwrap();

    assert_eq!(restored, Some(last_row.unwrap()));

    let progress = work.progress();
    assert!(progress.receive_bytes_completed > 0);
    assert_eq!(
        progress.receive_bytes_completed,
        progress.receive_bytes_total
    );
    assert!(progress.install_bytes_completed > 0);
    assert_eq!(
        progress.install_bytes_completed,
        progress.install_bytes_total
    );
    assert_eq!(progress.active_receives, 0);
    assert_eq!(progress.active_installs, 0);

    let _ = fs::remove_dir_all(root);
}

fn single_shard_command_for(
    request_id: RequestId,
    key_value: i64,
    txn_id: u64,
    commit_timestamp: u64,
) -> (Vec<u8>, ragnordb_common::ids::RowKey, Row) {
    let key = make_row_key(TABLE_ID, &[Value::Int(key_value)]).unwrap();

    let row = Row {
        values: vec![Value::Int(key_value), Value::Text("Ada".to_string())],
    };

    let command = SingleShardCommitCommand {
        txn_id: TxnId(txn_id),
        start_timestamp: Timestamp(commit_timestamp.saturating_sub(10)),
        commit_timestamp: Timestamp(commit_timestamp),
        writes: vec![WriteEntry {
            key: encode_row_key(&key).unwrap(),
            row: Some(row.clone()),
            op: WriteKind::Put,
        }],
    };

    let envelope = TabletCommandEnvelope::new(
        request_id,
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::SingleShardCommit(command),
    )
    .unwrap();

    (envelope.encode().unwrap(), key, row)
}

fn single_shard_command(request_id: RequestId) -> (Vec<u8>, ragnordb_common::ids::RowKey, Row) {
    single_shard_command_for(request_id, 1, 11, 30)
}

fn prewrite_command(
    request_id: RequestId,
    txn_id: u64,
    start_timestamp: u64,
    key: &ragnordb_common::ids::RowKey,
    row: Row,
) -> Vec<u8> {
    let encoded_key = encode_row_key(key).unwrap();
    TabletCommandEnvelope::new(
        request_id,
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::Prewrite(PrewriteCommand {
            txn_id: TxnId(txn_id),
            start_timestamp: Timestamp(start_timestamp),
            writes: vec![WriteEntry {
                key: encoded_key.clone(),
                row: Some(row),
                op: WriteKind::Put,
            }],
            primary_key: encoded_key,
            ttl_ms: 30_000,
        }),
    )
    .unwrap()
    .encode()
    .unwrap()
}

fn noop_command(request_id: RequestId) -> Vec<u8> {
    TabletCommandEnvelope::new(
        request_id,
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::Noop(NoopCommand),
    )
    .unwrap()
    .encode()
    .unwrap()
}

fn cluster() -> InMemoryTabletCluster<TestWal> {
    InMemoryTabletCluster::new(
        [TestWal::new(), TestWal::new(), TestWal::new()],
        TABLET_ID,
        TABLE_ID,
        RAFT_GROUP_ID,
        TABLET_EPOCH,
    )
    .unwrap()
}

/// Realistic bug caught:
///
/// A single committed write must be applied on every replica, and the client
/// response must come from state-machine application rather than proposal
/// admission or commit-index movement alone.
#[test]
fn three_node_cluster_replicates_and_applies_one_tablet_write() {
    let mut cluster = cluster();
    let elected_leader = cluster.elect_leader().unwrap();

    let request_id = request_id();
    let (command, key, row) = single_shard_command(request_id.clone());

    let ticket = cluster
        .propose(
            request_id.clone(),
            command,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    let completion = ticket.recv_timeout(Duration::from_secs(1)).unwrap();

    let applied_position = match completion {
        ProposalCompletion::Applied {
            request_id: applied_request_id,
            position,
            result,
        } => {
            assert_eq!(applied_request_id, request_id);
            assert_eq!(
                result,
                TabletCommandApplyOutcome {
                    result: TabletCommandApplyResult::SingleShardCommit,
                    deduplicated: false,
                }
            );
            position
        }

        ProposalCompletion::Retryable { failure, .. } => {
            panic!("proposal unexpectedly became retryable: {failure:?}");
        }

        ProposalCompletion::Rejected { rejection, .. } => {
            panic!("valid proposal was unexpectedly rejected: {rejection}");
        }
    };

    assert_eq!(cluster.leader_id().unwrap(), elected_leader);

    let reader = Transaction::new(TxnId(99), Timestamp(31)).unwrap();

    for replica_id in [1, 2, 3] {
        assert_eq!(
            cluster.last_applied(replica_id).unwrap(),
            applied_position.index
        );

        let visible_row = cluster
            .tablet(replica_id)
            .unwrap()
            .state_machine()
            .tablet()
            .get(&reader, &key)
            .unwrap();

        assert_eq!(visible_row, Some(row.clone()));
    }
}

/// Realistic bug caught:
///
/// A committed write conflict is a deterministic database result, not replica
/// corruption. Every replica must consume the entry, preserve the same cached
/// rejection, remain available for an exact retry, and accept the client's next
/// valid sequence.
#[test]
fn committed_write_conflict_advances_apply_without_quarantining_the_group() {
    let mut cluster = cluster();
    cluster.elect_leader().unwrap();

    let key = make_row_key(TABLE_ID, &[Value::Int(88)]).unwrap();
    let owner_row = Row {
        values: vec![Value::Int(88), Value::Text("owner".to_string())],
    };
    let contender_row = Row {
        values: vec![Value::Int(88), Value::Text("contender".to_string())],
    };

    let owner_request = request_id_with_sequence(1);
    let owner_ticket = cluster
        .propose(
            owner_request.clone(),
            prewrite_command(owner_request, 501, 100, &key, owner_row),
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    assert!(matches!(
        owner_ticket.recv_timeout(Duration::from_secs(1)).unwrap(),
        ProposalCompletion::Applied {
            result: TabletCommandApplyOutcome {
                result: TabletCommandApplyResult::Prewrite,
                deduplicated: false,
            },
            ..
        }
    ));

    let conflict_request = request_id_with_sequence(2);
    let conflict_command =
        prewrite_command(conflict_request.clone(), 502, 110, &key, contender_row);
    let conflict_ticket = cluster
        .propose(
            conflict_request.clone(),
            conflict_command.clone(),
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    let (conflict_position, first_rejection) = match conflict_ticket
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
    {
        ProposalCompletion::Rejected {
            request_id,
            position,
            rejection,
        } => {
            assert_eq!(request_id, conflict_request);
            assert!(matches!(
                rejection,
                TabletCommandApplyError::WriteConflict { .. }
            ));
            (position, rejection)
        }
        completion => panic!("conflicting prewrite returned {completion:?}"),
    };

    let mut replicated_rejection = None;
    for replica_id in [1, 2, 3] {
        assert_eq!(
            cluster.last_applied(replica_id).unwrap(),
            conflict_position.index
        );

        let snapshot = TabletStateMachineSnapshot::decode(
            &cluster
                .tablet(replica_id)
                .unwrap()
                .state_machine()
                .encode_snapshot_state()
                .unwrap(),
        )
        .unwrap();
        let client = snapshot.clients.get(&conflict_request.client_id).unwrap();

        assert_eq!(client.last_sequence_applied, conflict_request.sequence);
        assert!(matches!(
            &client.cached_outcome,
            CachedTabletCommandOutcome::Rejected(rejection)
                if rejection.kind == CachedTabletCommandRejectionKind::WriteConflict
        ));

        if let CachedTabletCommandOutcome::Rejected(rejection) = &client.cached_outcome {
            if let Some(expected) = &replicated_rejection {
                assert_eq!(rejection, expected);
            } else {
                replicated_rejection = Some(rejection.clone());
            }
        }
    }

    let retry_ticket = cluster
        .propose(
            conflict_request.clone(),
            conflict_command,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    match retry_ticket.recv_timeout(Duration::from_secs(1)).unwrap() {
        ProposalCompletion::Rejected {
            request_id,
            rejection,
            ..
        } => {
            assert_eq!(request_id, conflict_request);
            assert_eq!(rejection, first_rejection);
        }
        completion => panic!("exact conflict retry returned {completion:?}"),
    }

    let next_request = request_id_with_sequence(3);
    let next_ticket = cluster
        .propose(
            next_request.clone(),
            noop_command(next_request),
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    let next_position = match next_ticket.recv_timeout(Duration::from_secs(1)).unwrap() {
        ProposalCompletion::Applied {
            position,
            result:
                TabletCommandApplyOutcome {
                    result: TabletCommandApplyResult::Noop,
                    deduplicated: false,
                },
            ..
        } => position,
        completion => panic!("valid sequence after conflict returned {completion:?}"),
    };

    for replica_id in [1, 2, 3] {
        assert_eq!(
            cluster.last_applied(replica_id).unwrap(),
            next_position.index
        );
    }
}

/// Realistic bug caught:
///
/// A write must not be admitted while the group has no leader. The caller
/// receives a retryable routing error instead of creating an untrackable Raft
/// entry on a follower.
#[test]
fn proposal_requires_an_elected_leader() {
    let mut cluster = cluster();
    let request_id = request_id();
    let (command, _, _) = single_shard_command(request_id.clone());

    assert!(matches!(
        cluster.propose(
            request_id,
            command,
            Instant::now() + Duration::from_secs(30),
        ),
        Err(TabletClusterError::NoLeader)
    ));
}

/// Realistic bug caught:
///
/// A leader may disappear after a successful attempt, so the client must retry
/// the same RequestId. The replicated tablet state must return the cached result
/// instead of applying the MVCC mutation twice. A rejoined replica must also
/// catch up with writes committed by the replacement leader.
#[test]
fn leader_failover_preserves_request_identity_and_catches_up_rejoined_replica() {
    let mut cluster = cluster();
    let old_leader = cluster.elect_leader().unwrap();

    let first_request_id = request_id();
    let (first_command, _, _) = single_shard_command(first_request_id.clone());

    let first_ticket = cluster
        .propose(
            first_request_id.clone(),
            first_command.clone(),
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    match first_ticket.recv_timeout(Duration::from_secs(1)).unwrap() {
        ProposalCompletion::Applied { result, .. } => {
            assert_eq!(
                result,
                TabletCommandApplyOutcome {
                    result: TabletCommandApplyResult::SingleShardCommit,
                    deduplicated: false,
                }
            );
        }
        ProposalCompletion::Retryable { failure, .. } => {
            panic!("initial proposal unexpectedly failed: {failure:?}");
        }
        ProposalCompletion::Rejected { rejection, .. } => {
            panic!("initial proposal was unexpectedly rejected: {rejection}");
        }
    }

    cluster.kill_replica(old_leader).unwrap();

    let new_leader = cluster.elect_leader().unwrap();
    assert_ne!(new_leader, old_leader);

    // Retrying the original request must return the replicated cached outcome.
    let retry_ticket = cluster
        .propose(
            first_request_id.clone(),
            first_command,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    match retry_ticket.recv_timeout(Duration::from_secs(1)).unwrap() {
        ProposalCompletion::Applied { result, .. } => {
            assert_eq!(
                result,
                TabletCommandApplyOutcome {
                    result: TabletCommandApplyResult::SingleShardCommit,
                    deduplicated: true,
                }
            );
        }
        ProposalCompletion::Retryable { failure, .. } => {
            panic!("same-RequestId retry unexpectedly failed: {failure:?}");
        }
        ProposalCompletion::Rejected { rejection, .. } => {
            panic!("successful retry was unexpectedly rejected: {rejection}");
        }
    }

    assert_eq!(
        cluster
            .tablet(new_leader)
            .unwrap()
            .state_machine()
            .tablet()
            .stats()
            .write_records,
        1
    );

    // A later request must be accepted by the replacement leader.
    let second_request_id = request_id_with_sequence(2);
    let (second_command, _, _) = single_shard_command_for(second_request_id.clone(), 2, 12, 40);

    let second_ticket = cluster
        .propose(
            second_request_id,
            second_command,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    let second_position = match second_ticket.recv_timeout(Duration::from_secs(1)).unwrap() {
        ProposalCompletion::Applied {
            position, result, ..
        } => {
            assert_eq!(
                result,
                TabletCommandApplyOutcome {
                    result: TabletCommandApplyResult::SingleShardCommit,
                    deduplicated: false,
                }
            );
            position
        }
        ProposalCompletion::Retryable { failure, .. } => {
            panic!("replacement leader proposal failed: {failure:?}");
        }
        ProposalCompletion::Rejected { rejection, .. } => {
            panic!("replacement leader proposal was rejected: {rejection}");
        }
    };

    // Rejoin the old replica and publish the replacement leader's commit
    // frontier through a heartbeat so the stale replica receives the missing log.
    cluster.restart_replica(old_leader).unwrap();
    cluster.tick_replica(new_leader, 2).unwrap();

    assert_eq!(cluster.leader_id().unwrap(), new_leader);

    for replica_id in [1, 2, 3] {
        assert_eq!(
            cluster.last_applied(replica_id).unwrap(),
            second_position.index
        );
    }
}

/// Realistic bug caught:
///
/// An already-expired request must not create a Raft entry that has no valid
/// client response deadline.
#[test]
fn expired_proposal_is_rejected_before_raft_admission() {
    let mut cluster = cluster();
    cluster.elect_leader().unwrap();

    let request_id = request_id();
    let (command, _, _) = single_shard_command(request_id.clone());

    assert!(matches!(
        cluster.propose(
            request_id.clone(),
            command,
            Instant::now() - Duration::from_secs(1),
        ),
        Err(TabletClusterError::ProposalDeadlineExceeded {
            request_id: actual
        }) if actual == request_id
    ));

    for replica_id in [1, 2, 3] {
        assert_eq!(cluster.last_applied(replica_id).unwrap(), 0);
    }
}

/// Realistic bug caught:
///
/// A newly elected leader must not serve latest reads until a current-term
/// no-op has committed and been applied on the tablet replicas.
#[test]
fn leader_requires_an_applied_current_term_barrier_before_latest_reads() {
    let mut cluster = cluster();
    let leader_id = cluster.elect_leader().unwrap();

    assert!(!cluster.latest_reads_ready().unwrap());

    let barrier = cluster.prepare_leader_for_latest_reads().unwrap();

    assert!(cluster.latest_reads_ready().unwrap());
    assert!(barrier.term > 0);
    assert!(barrier.index > 0);
    assert_eq!(cluster.last_applied(leader_id).unwrap(), barrier.index);

    for replica_id in [1, 2, 3] {
        assert_eq!(cluster.last_applied(replica_id).unwrap(), barrier.index);

        // A no-op must not mutate MVCC state.
        assert_eq!(
            cluster
                .tablet(replica_id)
                .unwrap()
                .state_machine()
                .tablet()
                .stats()
                .write_records,
            0
        );
    }
}

/// Realistic bug caught:
///
/// A barrier from the previous leader term must never authorize reads after a
/// new leader is elected.
#[test]
fn new_leader_requires_a_new_current_term_barrier() {
    let mut cluster = cluster();
    let old_leader = cluster.elect_leader().unwrap();

    cluster.prepare_leader_for_latest_reads().unwrap();
    assert!(cluster.latest_reads_ready().unwrap());

    cluster.kill_replica(old_leader).unwrap();

    let new_leader = cluster.elect_leader().unwrap();
    assert_ne!(new_leader, old_leader);
    assert!(!cluster.latest_reads_ready().unwrap());

    cluster.prepare_leader_for_latest_reads().unwrap();

    assert!(cluster.latest_reads_ready().unwrap());
}

/// Realistic bug caught:
///
/// A latest read must establish a fresh Raft barrier before reading MVCC.
/// Reading the leader's local tablet directly could observe state without a
/// quorum-confirmed current-term ordering point.
#[test]
fn latest_read_routes_through_leader_barrier_before_reading_mvcc() {
    let mut cluster = cluster();
    let leader_id = cluster.elect_leader().unwrap();

    let request_id = request_id();
    let (command, key, row) = single_shard_command(request_id.clone());

    let ticket = cluster
        .propose(
            request_id,
            command,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    ticket.recv_timeout(Duration::from_secs(1)).unwrap();

    let reader = Transaction::new(TxnId(99), Timestamp(31)).unwrap();

    let visible_row = cluster
        .latest_read(&reader, &key, Instant::now() + Duration::from_secs(30))
        .unwrap();

    assert_eq!(visible_row, Some(row));
    assert_eq!(cluster.leader_id().unwrap(), leader_id);
    assert!(cluster.latest_reads_ready().unwrap());
}

/// Realistic bug caught:
///
/// An expired latest-read request must not append a read barrier or return
/// local MVCC data after the client's deadline has already elapsed.
#[test]
fn expired_latest_read_is_rejected_before_barrier_proposal() {
    let mut cluster = cluster();
    let leader_id = cluster.elect_leader().unwrap();

    let request_id = request_id();
    let (command, key, _) = single_shard_command(request_id.clone());

    let ticket = cluster
        .propose(
            request_id,
            command,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    ticket.recv_timeout(Duration::from_secs(1)).unwrap();

    let reader = Transaction::new(TxnId(99), Timestamp(31)).unwrap();
    let applied_before = cluster.last_applied(leader_id).unwrap();

    assert!(matches!(
        cluster.latest_read(&reader, &key, Instant::now() - Duration::from_secs(1),),
        Err(TabletClusterError::LatestReadDeadlineExceeded)
    ));

    assert_eq!(cluster.last_applied(leader_id).unwrap(), applied_before);
}

/// Realistic bug caught:
///
/// After leader failover, the old term's read barrier must not authorize a
/// read. The replacement leader must establish a new barrier first.
#[test]
fn latest_read_requires_a_new_barrier_after_leader_failover() {
    let mut cluster = cluster();
    let old_leader = cluster.elect_leader().unwrap();

    let request_id = request_id();
    let (command, key, row) = single_shard_command(request_id.clone());

    let ticket = cluster
        .propose(
            request_id,
            command,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    ticket.recv_timeout(Duration::from_secs(1)).unwrap();

    cluster.kill_replica(old_leader).unwrap();

    let new_leader = cluster.elect_leader().unwrap();
    assert_ne!(old_leader, new_leader);

    let reader = Transaction::new(TxnId(99), Timestamp(31)).unwrap();

    let visible_row = cluster
        .latest_read(&reader, &key, Instant::now() + Duration::from_secs(30))
        .unwrap();

    assert_eq!(visible_row, Some(row));
    assert!(cluster.latest_reads_ready().unwrap());
}

/// Realistic bug caught:
///
/// Restarting a replica must reconstruct a new Raft core and tablet state
/// machine from acknowledged durable records. Reattaching the old in-memory
/// objects would hide lost state and would not prove the restart contract.
#[test]
fn restarted_replica_reconstructs_from_durable_wal_and_catches_up() {
    let mut cluster = durable_cluster();
    let old_leader = cluster.elect_leader().unwrap();

    let first_request_id = request_id();
    let (first_command, _, _) = single_shard_command(first_request_id.clone());

    let first_ticket = cluster
        .propose(
            first_request_id,
            first_command,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    assert!(matches!(
        first_ticket.recv_timeout(Duration::from_secs(1)).unwrap(),
        ProposalCompletion::Applied { .. }
    ));

    cluster.kill_replica(old_leader).unwrap();

    let new_leader = cluster.elect_leader().unwrap();
    assert_ne!(new_leader, old_leader);

    let second_request_id = request_id_with_sequence(2);
    let (second_command, second_key, second_row) =
        single_shard_command_for(second_request_id.clone(), 2, 12, 40);

    let second_ticket = cluster
        .propose(
            second_request_id,
            second_command,
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();

    let second_position = match second_ticket.recv_timeout(Duration::from_secs(1)).unwrap() {
        ProposalCompletion::Applied { position, .. } => position,
        ProposalCompletion::Retryable { failure, .. } => {
            panic!("replacement leader proposal failed: {failure:?}");
        }
        ProposalCompletion::Rejected { rejection, .. } => {
            panic!("replacement leader proposal was rejected: {rejection}");
        }
    };

    let durable_wal = cluster.replica_wal(old_leader).unwrap();

    let (restarted_raft, restarted_tablet) =
        rebuild_replica_from_durable_wal(durable_wal, old_leader);

    cluster
        .restart_replica_from_durable_state(old_leader, restarted_raft, restarted_tablet)
        .unwrap();

    // The replacement leader must publish the missing committed suffix to the
    // newly reconstructed follower through the normal Raft message path.
    cluster.tick_replica(new_leader, 2).unwrap();

    for replica_id in [1, 2, 3] {
        assert_eq!(
            cluster.last_applied(replica_id).unwrap(),
            second_position.index
        );
    }

    let reader = Transaction::new(TxnId(99), Timestamp(41)).unwrap();

    let restored_row = cluster
        .tablet(old_leader)
        .unwrap()
        .state_machine()
        .tablet()
        .get(&reader, &second_key)
        .unwrap();

    assert_eq!(restored_row, Some(second_row));
}
