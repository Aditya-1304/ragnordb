use std::{
    collections::{BTreeMap, BTreeSet},
    fs, process,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use raft::{
    entry::LogEntry,
    types::{HardState, ReplicaId as CoreReplicaId},
};
use ragnordb_common::{
    codec::{Row, Value, WriteKind},
    command_codec::{SingleShardCommitCommand, TabletCommand, TabletCommandEnvelope, WriteEntry},
    ids::{NodeId, RaftGroupId, ReplicaId, RequestId, TableId, TabletId, Timestamp, TxnId},
    raft_bootstrap::RaftGroupBootstrap,
};
use ragnordb_multiraft::{
    bootstrap::FileBootstrapStore,
    replica_startup::{
        bootstrap_tablet_replica, initial_recovery_configuration, recover_tablet_replica,
    },
    runtime::AppliedRaftFrontier,
    snapshot::raft_pointer_for_tablet,
    storage::{
        persistence::{RaftPersistenceBatch, RaftWal, RaftWalStorage},
        recovery::{RaftWalRecoverySource, recover_raft_storage_with_configurations},
    },
    tablet_apply::CommittedTabletCommandDisposition,
};
use ragnordb_storage::key::{encode_row_key, make_row_key};
use ragnordb_tablet::{
    Tablet,
    command::{TabletCommandApplyOutcome, TabletCommandApplyResult, TabletStateMachine},
    snapshot::{
        AppliedTabletFrontier, FileTabletSnapshotStore, TabletSnapshotConfState,
        TabletSnapshotInstallTarget, generate_local_snapshot,
    },
};
use ragnordb_txn::Transaction;
use wal::{
    error::{BatchAppendFailure, WalError},
    lsn::Lsn,
    types::RecordType,
    wal::{AppendResult, BatchAppendResult, iterator::WalRecord},
};

const CLUSTER_ID: &str = "ragnordb-startup-test";
const RAFT_GROUP_ID: RaftGroupId = RaftGroupId(91);
const REPLICA_ID: ReplicaId = ReplicaId(1);
const TABLET_ID: TabletId = TabletId(41);
const TABLE_ID: TableId = TableId(9);
const TABLET_EPOCH: u64 = 7;

#[derive(Clone)]
struct DurableTestWal {
    state: Arc<Mutex<DurableTestWalState>>,
}

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
        self.state.lock().unwrap().records.clone()
    }

    fn durable_end_lsn(&self) -> Lsn {
        self.state.lock().unwrap().next_lsn
    }
}

impl RaftWal for DurableTestWal {
    fn append_batch_and_sync(
        &mut self,
        records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure> {
        let mut state = self.state.lock().unwrap();
        let mut extents = Vec::with_capacity(records.len());

        for (record_type, payload) in records {
            let start_lsn = state.next_lsn;
            let end_lsn = start_lsn
                .checked_add_bytes(payload.len() as u64 + 32)
                .unwrap();
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

struct RecordSource(std::vec::IntoIter<WalRecord>);

impl RaftWalRecoverySource for RecordSource {
    fn next_record(&mut self) -> Result<Option<WalRecord>, WalError> {
        Ok(self.0.next())
    }
}

fn bootstrap() -> RaftGroupBootstrap {
    RaftGroupBootstrap::new(
        CLUSTER_ID.to_string(),
        RAFT_GROUP_ID,
        1,
        BTreeMap::from([
            (ReplicaId(1), NodeId(1)),
            (ReplicaId(2), NodeId(2)),
            (ReplicaId(3), NodeId(3)),
        ]),
        BTreeSet::from([ReplicaId(1), ReplicaId(2), ReplicaId(3)]),
        BTreeSet::new(),
    )
    .unwrap()
}

fn target() -> TabletSnapshotInstallTarget {
    TabletSnapshotInstallTarget {
        cluster_id: CLUSTER_ID.to_string(),
        raft_group_id: RAFT_GROUP_ID,
        tablet_id: TABLET_ID,
        table_id: TABLE_ID,
        tablet_epoch: TABLET_EPOCH,
    }
}

fn request_id(client_id: u128, sequence: u64) -> RequestId {
    RequestId {
        client_id,
        sequence,
        raft_group_id: RAFT_GROUP_ID,
    }
}

fn command(
    client_id: u128,
    sequence: u64,
    key_value: i64,
    txn_id: u64,
    commit_timestamp: u64,
) -> (Vec<u8>, ragnordb_common::ids::RowKey, Row) {
    let key = make_row_key(TABLE_ID, &[Value::Int(key_value)]).unwrap();
    let row = Row {
        values: vec![Value::Int(key_value), Value::Text("Ada".to_string())],
    };
    let envelope = TabletCommandEnvelope::new(
        request_id(client_id, sequence),
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::SingleShardCommit(SingleShardCommitCommand {
            txn_id: TxnId(txn_id),
            start_timestamp: Timestamp(commit_timestamp - 10),
            commit_timestamp: Timestamp(commit_timestamp),
            writes: vec![WriteEntry {
                key: encode_row_key(&key).unwrap(),
                row: Some(row.clone()),
                op: WriteKind::Put,
            }],
        }),
    )
    .unwrap();
    (envelope.encode().unwrap(), key, row)
}

fn unique_directory(label: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("ragnordb-{label}-{}-{nonce}", process::id()))
}

/// Realistic bug caught:
///
/// A new production group must persist its exactly-once bootstrap authority and
/// acknowledge the initial Ready before transport can publish dependent state.
#[test]
fn new_replica_uses_durable_bootstrap_before_becoming_visible() {
    let root = unique_directory("bootstrap-startup");
    let mut store = FileBootstrapStore::open(&root).unwrap();
    let requested = bootstrap();

    let started = bootstrap_tablet_replica(
        &mut store,
        &requested,
        REPLICA_ID,
        DurableTestWal::new(),
        &target(),
        5,
        2,
    )
    .unwrap();

    assert_eq!(started.bootstrap, requested);
    assert_eq!(
        started.ready_loop.raft().durable_conf_state(),
        Some(&requested.to_core_conf_state().unwrap())
    );
    assert_eq!(
        started.tablet.state_machine().raft_group_id(),
        RAFT_GROUP_ID
    );
    assert!(
        started
            .initial_ready
            .as_ref()
            .is_some_and(|ready| ready.conf_state.is_some())
    );

    fs::remove_dir_all(root).unwrap();
}

/// Realistic bug caught:
///
/// Restart after compaction cannot rebuild an empty tablet from the retained
/// Raft suffix. Startup must restore MVCC and deduplication from the referenced
/// snapshot, replay the committed suffix, seed the applied frontier, and keep
/// exact old RequestId retries deterministic.
#[test]
fn restart_restores_snapshot_then_replays_committed_suffix() {
    let requested = bootstrap();
    let (identity, conf_state) = initial_recovery_configuration(&requested, REPLICA_ID).unwrap();
    let wal = DurableTestWal::new();
    let mut persistence = RaftWalStorage::new(wal.clone(), identity);

    let tablet = Tablet::new(TABLET_ID, TABLE_ID).unwrap();
    let mut state_machine = TabletStateMachine::new(tablet, TABLET_EPOCH, RAFT_GROUP_ID).unwrap();
    let (first_command, first_key, first_row) = command(41, 1, 1, 11, 30);
    state_machine
        .apply(TabletCommandEnvelope::decode(&first_command).unwrap())
        .unwrap();

    persistence
        .persist(RaftPersistenceBatch {
            snapshot: None,
            entries: vec![LogEntry::normal_with_size(
                1,
                1,
                first_command.clone(),
                first_command.len(),
            )],
            hard_state: Some(HardState {
                current_term: 1,
                voted_for: Some(CoreReplicaId::must(1)),
                commit: 1,
            }),
        })
        .unwrap();

    let snapshot_root = unique_directory("replica-snapshot-startup");
    let snapshot_store = FileTabletSnapshotStore::new(&snapshot_root, 1024 * 1024).unwrap();
    let snapshot_conf = TabletSnapshotConfState::new(
        conf_state.version,
        conf_state
            .voters
            .iter()
            .map(|replica| ReplicaId(replica.get())),
        conf_state
            .learners
            .iter()
            .map(|replica| ReplicaId(replica.get())),
        conf_state
            .outgoing_voters
            .iter()
            .map(|replica| ReplicaId(replica.get())),
    )
    .unwrap();
    let image = generate_local_snapshot(
        &state_machine,
        CLUSTER_ID,
        REPLICA_ID,
        1,
        snapshot_conf,
        AppliedTabletFrontier::new(1, 1),
    )
    .unwrap();
    let pointer = snapshot_store.publish(&image).unwrap();
    let raft_pointer =
        raft_pointer_for_tablet(persistence.log_view().identity(), &pointer).unwrap();
    persistence
        .persist(RaftPersistenceBatch {
            snapshot: Some(raft_pointer),
            entries: Vec::new(),
            hard_state: Some(HardState {
                current_term: 1,
                voted_for: Some(CoreReplicaId::must(1)),
                commit: 1,
            }),
        })
        .unwrap();

    let (second_command, second_key, second_row) = command(42, 1, 2, 12, 40);
    persistence
        .persist(RaftPersistenceBatch {
            snapshot: None,
            entries: vec![LogEntry::normal_with_size(
                2,
                1,
                second_command.clone(),
                second_command.len(),
            )],
            hard_state: Some(HardState {
                current_term: 1,
                voted_for: Some(CoreReplicaId::must(1)),
                commit: 2,
            }),
        })
        .unwrap();

    let mut source = RecordSource(wal.records().into_iter());
    let configurations = BTreeMap::from([(identity, conf_state)]);
    let recovered = recover_raft_storage_with_configurations(&mut source, &configurations).unwrap();
    let replica = recovered.replica(identity).unwrap();

    let mut started = recover_tablet_replica(
        requested,
        REPLICA_ID,
        wal.clone(),
        wal.durable_end_lsn(),
        replica,
        &snapshot_store,
        &target(),
        5,
        2,
    )
    .unwrap();

    assert_eq!(
        started.ready_loop.applied_frontier(),
        Some(AppliedRaftFrontier::new(2, 1))
    );
    assert_eq!(started.ready_loop.raft().commit_index(), 2);

    let reader = Transaction::new(TxnId(999), Timestamp(50)).unwrap();
    assert_eq!(
        started
            .tablet
            .state_machine()
            .tablet()
            .get(&reader, &first_key)
            .unwrap(),
        Some(first_row)
    );
    assert_eq!(
        started
            .tablet
            .state_machine()
            .tablet()
            .get(&reader, &second_key)
            .unwrap(),
        Some(second_row)
    );

    let retry = started
        .tablet
        .apply_committed(
            ragnordb_multiraft::proposal::ProposalPosition { term: 2, index: 3 },
            &first_command,
        )
        .unwrap();
    assert!(matches!(
        retry,
        CommittedTabletCommandDisposition::Applied(applied)
            if applied.outcome
                == TabletCommandApplyOutcome {
                    result: TabletCommandApplyResult::SingleShardCommit,
                    deduplicated: true,
                }
    ));

    fs::remove_dir_all(snapshot_root).unwrap();
}
