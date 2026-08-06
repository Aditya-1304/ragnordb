use raft::{
    entry::LogEntry,
    types::{ConfState, HardState, ReplicaId as CoreReplicaId},
};
use ragnordb_common::{
    durability::{DurabilityGate, NodeDurabilityState},
    ids::{RaftGroupId, ReplicaId},
};
use ragnordb_multiraft::storage::{
    codec::{
        RAFT_SNAPSHOT_POINTER_RECORD_VERSION, RaftHardStateRecord, RaftReplicaIdentity,
        RaftSnapshotPointerRecord,
    },
    persistence::{
        NodeRaftWal, RaftPersistenceBatch, RaftPersistenceError, RaftWal, RaftWalRecordType,
        RaftWalRetentionPin, RaftWalStorage,
    },
    recovery::{RaftWalRecoverySource, recover_raft_storage},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use wal::{
    error::{BatchAppendFailure, WalError},
    lsn::Lsn,
    types::RecordType,
    wal::{AppendResult, BatchAppendResult, iterator::WalRecord},
};
#[derive(Debug, Clone, PartialEq, Eq)]
enum WalOperation {
    Batch(Vec<RecordType>),
}

struct FakeWal {
    next_lsn: Lsn,
    operations: Vec<WalOperation>,
    records: Vec<WalRecord>,
    fail_sync: bool,
}

impl FakeWal {
    fn healthy() -> Self {
        Self {
            next_lsn: Lsn::new(100),
            operations: Vec::new(),
            records: Vec::new(),
            fail_sync: false,
        }
    }

    fn failing_sync() -> Self {
        Self {
            fail_sync: true,
            ..Self::healthy()
        }
    }
}

impl RaftWal for FakeWal {
    fn append_batch_and_sync(
        &mut self,
        records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure> {
        self.operations.push(WalOperation::Batch(
            records.iter().map(|(kind, _)| *kind).collect(),
        ));
        let mut extents = Vec::new();
        for (record_type, payload) in records {
            let start_lsn = self.next_lsn;
            let end_lsn = start_lsn
                .checked_add_bytes(payload.len() as u64 + 32)
                .unwrap();
            self.records.push(WalRecord {
                lsn: start_lsn,
                record_type: *record_type,
                payload: payload.to_vec(),
                total_len: (payload.len() + 32) as u32,
            });
            self.next_lsn = end_lsn;
            extents.push(AppendResult { start_lsn, end_lsn });
        }
        if self.fail_sync {
            return Err(BatchAppendFailure::OutcomeUnknown {
                result: BatchAppendResult {
                    final_end_lsn: extents.last().unwrap().end_lsn,
                    record_extents: extents,
                },
                source: WalError::BrokenDurabilityContract,
            });
        }
        Ok(BatchAppendResult {
            final_end_lsn: extents.last().unwrap().end_lsn,
            record_extents: extents,
        })
    }
}

#[derive(Debug)]
struct TestRetentionPin(Arc<AtomicUsize>);

impl Drop for TestRetentionPin {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct PinTrackingWal {
    active_pins: Arc<AtomicUsize>,
}

impl RaftWal for PinTrackingWal {
    fn append_batch_and_sync(
        &mut self,
        _records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure> {
        Ok(BatchAppendResult {
            final_end_lsn: Lsn::ZERO,
            record_extents: Vec::new(),
        })
    }

    fn acquire_retention_pin(
        &self,
        _holder_name: &str,
        _min_lsn: Lsn,
    ) -> Result<Box<dyn RaftWalRetentionPin>, String> {
        self.active_pins.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(TestRetentionPin(Arc::clone(&self.active_pins))))
    }
}

#[derive(Debug, Clone)]
struct RetentionTrackingWal {
    pruned_through: Arc<Mutex<Vec<Lsn>>>,
}

impl RaftWal for RetentionTrackingWal {
    fn append_batch_and_sync(
        &mut self,
        _records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure> {
        Ok(BatchAppendResult {
            final_end_lsn: Lsn::ZERO,
            record_extents: Vec::new(),
        })
    }

    fn prune_before(&mut self, floor: Lsn) -> Result<usize, String> {
        self.pruned_through.lock().unwrap().push(floor);
        Ok(1)
    }
}

struct FailingPruneWal {
    inner: FakeWal,
}

impl RaftWal for FailingPruneWal {
    fn append_batch_and_sync(
        &mut self,
        records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure> {
        self.inner.append_batch_and_sync(records)
    }

    fn prune_before(&mut self, _floor: Lsn) -> Result<usize, String> {
        Err("injected partial retention mutation".to_string())
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

fn durable_storage() -> RaftWalStorage<FakeWal> {
    let mut storage = RaftWalStorage::new(FakeWal::healthy(), identity());

    storage
        .persist(RaftPersistenceBatch {
            snapshot: Some(snapshot_pointer()),
            entries: vec![LogEntry::normal_with_size(20, 8, b"twenty".to_vec(), 6)],
            hard_state: Some(HardState {
                current_term: 8,
                voted_for: Some(CoreReplicaId::must(61)),
                commit: 20,
            }),
        })
        .unwrap();

    storage
}

fn identity() -> RaftReplicaIdentity {
    RaftReplicaIdentity::new(RaftGroupId(51), ReplicaId(61)).unwrap()
}

fn conf_state() -> ConfState {
    ConfState::new(1, [CoreReplicaId::must(61)], []).unwrap()
}

fn batch() -> RaftPersistenceBatch {
    RaftPersistenceBatch {
        snapshot: None,
        entries: vec![
            LogEntry::normal_with_size(1, 1, b"one".to_vec(), 3),
            LogEntry::normal_with_size(2, 1, b"two".to_vec(), 3),
        ],
        hard_state: Some(HardState {
            current_term: 1,
            voted_for: Some(CoreReplicaId::must(61)),
            commit: 2,
        }),
    }
}

fn snapshot_pointer() -> RaftSnapshotPointerRecord {
    RaftSnapshotPointerRecord {
        format_version: RAFT_SNAPSHOT_POINTER_RECORD_VERSION,
        identity: identity(),
        snapshot_id: 19,
        last_included_index: 19,
        last_included_term: 7,
        applied_index: 19,
        conf_state: conf_state(),
        size_bytes: 4096,
        checksum: [9; 32],
        file_name: "raft-51-61-19.snapshot".to_string(),
    }
}

#[test]
fn stable_state_codecs_preserve_replica_lifetime_and_core_state() {
    let identity = identity();
    let hard_state = HardState {
        current_term: 7,
        voted_for: Some(CoreReplicaId::must(61)),
        commit: 19,
    };
    let hard_record = RaftHardStateRecord::from_core(identity, hard_state.clone()).unwrap();
    let snapshot = snapshot_pointer();

    let decoded_hard = RaftHardStateRecord::decode(&hard_record.encode().unwrap()).unwrap();
    let decoded_snapshot = RaftSnapshotPointerRecord::decode(&snapshot.encode().unwrap()).unwrap();

    assert_eq!(decoded_hard.identity, identity);
    assert_eq!(decoded_hard.to_core().unwrap(), hard_state);
    assert_eq!(decoded_snapshot, snapshot);
}

/// Realistic bug caught: recovery trusts a pointer that either escapes the
/// snapshot directory or claims tablet state applied through a different
/// frontier than the Raft prefix it replaces.
#[test]
fn snapshot_pointer_rejects_unsafe_file_identity_and_mismatched_applied_frontier() {
    let mut snapshot = snapshot_pointer();
    snapshot.file_name = "../foreign.snapshot".to_string();
    assert!(matches!(
        snapshot.validate(),
        Err(ragnordb_multiraft::storage::codec::RaftStableStateCodecError::InvalidSnapshotFileMetadata)
    ));

    let mut snapshot = snapshot_pointer();
    snapshot.applied_index -= 1;
    assert!(matches!(
        snapshot.validate(),
        Err(ragnordb_multiraft::storage::codec::RaftStableStateCodecError::SnapshotAppliedIndexMismatch { .. })
    ));
}

/// Catches a node-wide WAL wrapper that silently drops snapshot retention pins
/// instead of forwarding them to the shared WAL owner.
#[test]
fn node_wide_wal_forwards_snapshot_retention_pins() {
    let active_pins = Arc::new(AtomicUsize::new(0));
    let node_wal = NodeRaftWal::new(PinTrackingWal {
        active_pins: Arc::clone(&active_pins),
    });
    let handle = node_wal.group_writer();

    let pin = handle
        .acquire_retention_pin("tablet-snapshot", Lsn::new(100))
        .unwrap();

    assert_eq!(active_pins.load(Ordering::SeqCst), 1);
    drop(pin);
    assert_eq!(active_pins.load(Ordering::SeqCst), 0);
}

/// Catches a shared-WAL collector pruning a segment as soon as one Raft group
/// advances, even though another registered group still needs that segment.
#[test]
fn node_wide_retention_prunes_only_through_the_slowest_registered_group() {
    let pruned_through = Arc::new(Mutex::new(Vec::new()));
    let node_wal = NodeRaftWal::new(RetentionTrackingWal {
        pruned_through: Arc::clone(&pruned_through),
    });
    let first_identity = identity();
    let second_identity = RaftReplicaIdentity::new(RaftGroupId(52), ReplicaId(62)).unwrap();

    let first_wal = node_wal.group_writer_for(first_identity).unwrap();
    let second_wal = node_wal.group_writer_for(second_identity).unwrap();
    node_wal.seal_retention_registry().unwrap();

    let mut first = RaftWalStorage::new(first_wal, first_identity);
    let mut second = RaftWalStorage::new(second_wal, second_identity);

    first.release_retention(Lsn::new(400)).unwrap();
    assert!(pruned_through.lock().unwrap().is_empty());

    second.release_retention(Lsn::new(200)).unwrap();
    assert_eq!(*pruned_through.lock().unwrap(), vec![Lsn::new(200)]);

    first.release_retention(Lsn::new(500)).unwrap();
    second.release_retention(Lsn::new(500)).unwrap();
    assert_eq!(
        *pruned_through.lock().unwrap(),
        vec![Lsn::new(200), Lsn::new(500)]
    );
}

/// Realistic bug caught:
///
/// A database checkpoint may advance beyond the oldest Raft record still
/// required for restart. Physical pruning must stop at the slower Raft floor
/// even though the database snapshot can replay from a newer LSN.
#[test]
fn database_checkpoint_retention_cannot_pass_a_raft_replica_floor() {
    let pruned_through = Arc::new(Mutex::new(Vec::new()));
    let node_wal = NodeRaftWal::new(RetentionTrackingWal {
        pruned_through: Arc::clone(&pruned_through),
    });
    let replica_identity = identity();
    let replica_wal = node_wal.group_writer_for(replica_identity).unwrap();
    node_wal.seal_retention_registry().unwrap();
    let mut replica = RaftWalStorage::new(replica_wal, replica_identity);

    node_wal
        .advance_database_retention(Lsn::new(50_000))
        .unwrap();
    assert!(pruned_through.lock().unwrap().is_empty());

    replica.release_retention(Lsn::new(20_000)).unwrap();
    assert_eq!(*pruned_through.lock().unwrap(), vec![Lsn::new(20_000)]);
}

/// Realistic bug caught: after compaction the first retained log entry may be
/// in a later segment than the snapshot pointer that makes the suffix
/// recoverable. Publishing the entry LSN as the group floor can delete the
/// pointer and make a valid snapshot file undiscoverable after restart.
#[test]
fn snapshot_pointer_remains_the_recovery_floor_before_and_after_restart() {
    let mut storage = RaftWalStorage::new(FakeWal::healthy(), identity());
    let persisted = storage
        .persist(RaftPersistenceBatch {
            snapshot: Some(snapshot_pointer()),
            entries: vec![LogEntry::normal_with_size(
                20,
                8,
                b"retained-suffix".to_vec(),
                15,
            )],
            hard_state: Some(HardState {
                current_term: 8,
                voted_for: Some(CoreReplicaId::must(61)),
                commit: 20,
            }),
        })
        .unwrap();
    let pointer_lsn = persisted.start_lsn.unwrap();

    assert!(storage.log_view().first_retained_lsn().unwrap() > pointer_lsn);
    assert_eq!(storage.minimum_recovery_lsn(), Some(pointer_lsn));

    let records = storage.wal().records.clone();
    let durable_end_lsn = storage.wal().next_lsn;
    let mut source = RecordSource::new(records);
    let recovered = recover_raft_storage(&mut source).unwrap();
    let restarted = RaftWalStorage::from_recovered(
        FakeWal::healthy(),
        recovered.replica(identity()).unwrap(),
        durable_end_lsn,
    )
    .unwrap();

    assert_eq!(restarted.snapshot().unwrap(), &snapshot_pointer());
    assert_eq!(restarted.minimum_recovery_lsn(), Some(pointer_lsn));
}

/// Realistic bug caught: physical pruning can mutate several segments and then
/// fail, while the database and other Raft groups continue treating the shared
/// WAL as healthy. The fail-stop model requires one node-wide fence.
#[test]
fn retention_mutation_failure_fences_every_shared_wal_user() {
    let gate = DurabilityGate::new();
    let node_wal = NodeRaftWal::with_durability_gate(
        FailingPruneWal {
            inner: FakeWal::healthy(),
        },
        gate.clone(),
    );
    let replica_identity = identity();
    let replica_wal = node_wal.group_writer_for(replica_identity).unwrap();
    node_wal.seal_retention_registry().unwrap();
    let mut replica = RaftWalStorage::new(replica_wal, replica_identity);

    node_wal
        .advance_database_retention(Lsn::new(50_000))
        .unwrap();
    assert!(replica.release_retention(Lsn::new(20_000)).is_err());
    assert!(node_wal.recovery_required());
    assert!(matches!(
        gate.state(),
        NodeDurabilityState::RecoveryRequired(_)
    ));

    assert!(matches!(
        replica.persist(batch()),
        Err(RaftPersistenceError::NotStaged {
            recovery_required: true,
            ..
        })
    ));
    assert!(
        node_wal
            .advance_database_retention(Lsn::new(60_000))
            .is_err()
    );
    assert!(gate.ensure_healthy().is_err());
}

#[test]
fn persistence_uses_one_ordered_batch_with_hard_state_last() {
    let mut storage = RaftWalStorage::new(FakeWal::healthy(), identity());

    let persisted = storage.persist(batch()).unwrap();
    let end_lsn = persisted.end_lsn.unwrap();

    assert_eq!(
        storage.wal().operations,
        vec![WalOperation::Batch(vec![
            RaftWalRecordType::LogEntry.as_wal_record_type(),
            RaftWalRecordType::LogEntry.as_wal_record_type(),
            RaftWalRecordType::HardState.as_wal_record_type(),
        ])]
    );

    assert_eq!(persisted.record_count, 3);
    assert_eq!(storage.log_view().last_index(), Some(2));
    assert!(storage.conf_state().is_none());
    assert_eq!(storage.hard_state().unwrap().commit, 2);
    assert_eq!(storage.durable_end_lsn(), Some(end_lsn));
    assert!(!storage.recovery_required());
}

#[test]
fn sync_failure_does_not_publish_unacknowledged_raft_state() {
    let mut storage = RaftWalStorage::new(FakeWal::failing_sync(), identity());

    let error = storage.persist(batch()).unwrap_err();

    assert!(matches!(error, RaftPersistenceError::OutcomeUnknown { .. }));
    assert!(storage.log_view().is_empty());
    assert!(storage.conf_state().is_none());
    assert!(storage.hard_state().is_none());
    assert!(storage.durable_end_lsn().is_none());
    assert!(storage.recovery_required());

    assert_eq!(
        storage.persist(batch()).unwrap_err(),
        RaftPersistenceError::RecoveryRequired
    );
}

/// Realistic bug caught: live persistence must reject the same conflicting
/// snapshot pointer that recovery would reject, before a second WAL batch can
/// make the inconsistent history durable.
#[test]
fn persistence_rejects_conflicting_same_index_snapshot_before_wal_append() {
    let mut storage = RaftWalStorage::new(FakeWal::healthy(), identity());
    let first_snapshot = snapshot_pointer();

    storage
        .persist(RaftPersistenceBatch {
            snapshot: Some(first_snapshot.clone()),
            entries: Vec::new(),
            hard_state: None,
        })
        .unwrap();

    let mut conflicting_snapshot = first_snapshot;
    conflicting_snapshot.checksum = [8; 32];

    let error = storage
        .persist(RaftPersistenceBatch {
            snapshot: Some(conflicting_snapshot),
            entries: Vec::new(),
            hard_state: None,
        })
        .unwrap_err();

    assert!(matches!(
        error,
        RaftPersistenceError::InvalidSnapshotTransition(_)
    ));
    assert_eq!(storage.wal().operations.len(), 1);
    assert_eq!(storage.snapshot().unwrap().checksum, [9; 32]);
    assert_eq!(storage.log_view().snapshot_boundary(), Some((19, 7)));
}

/// Realistic bug caught: a batch must not durably publish a log term that is
/// newer than the HardState term that describes the resulting Raft state.
#[test]
fn persistence_rejects_hard_state_below_resulting_log_term_before_wal_append() {
    let mut storage = RaftWalStorage::new(FakeWal::healthy(), identity());

    let error = storage
        .persist(RaftPersistenceBatch {
            snapshot: None,
            entries: vec![LogEntry::normal_with_size(1, 7, b"term-seven".to_vec(), 10)],
            hard_state: Some(HardState {
                current_term: 6,
                voted_for: None,
                commit: 0,
            }),
        })
        .unwrap_err();

    assert_eq!(
        error,
        RaftPersistenceError::HardStateBeforeLogTerm {
            current_term: 6,
            maximum_log_term: 7,
        }
    );
    assert!(storage.wal().operations.is_empty());
    assert!(storage.log_view().is_empty());
    assert!(storage.hard_state().is_none());
    assert!(!storage.recovery_required());
}

/// Realistic bug caught: one group observes an uncertain shared-WAL outcome,
/// but another group continues consuming Ready generations through a separate
/// per-replica writer.
#[test]
fn uncertain_batch_fences_every_group_writer_on_the_node() {
    let gate = DurabilityGate::new();
    let node_wal = NodeRaftWal::with_durability_gate(FakeWal::failing_sync(), gate.clone());
    let mut first = RaftWalStorage::new(node_wal.group_writer(), identity());
    let second_identity = RaftReplicaIdentity::new(RaftGroupId(52), ReplicaId(62)).unwrap();
    let mut second = RaftWalStorage::new(node_wal.group_writer(), second_identity);

    assert!(matches!(
        first.persist(batch()).unwrap_err(),
        RaftPersistenceError::OutcomeUnknown { .. }
    ));
    assert!(node_wal.recovery_required());
    assert!(matches!(
        gate.state(),
        NodeDurabilityState::RecoveryRequired(_)
    ));
    assert!(gate.ensure_healthy().is_err());

    let error = second
        .persist(RaftPersistenceBatch {
            snapshot: None,
            entries: Vec::new(),
            hard_state: Some(HardState {
                current_term: 1,
                voted_for: Some(CoreReplicaId::must(62)),
                commit: 0,
            }),
        })
        .unwrap_err();
    assert!(matches!(
        error,
        RaftPersistenceError::NotStaged {
            recovery_required: true,
            ..
        }
    ));
}

/// Realistic bug caught:
///
/// A process crash may preserve any prefix of the ordered Ready batch. Every
/// such prefix must recover without exposing a HardState, log suffix, snapshot
/// boundary, or applied frontier that depends on a later record.
#[test]
fn every_ready_record_prefix_recovers_without_dangling_dependencies() {
    let storage = durable_storage();
    let records = storage.wal().records.clone();

    assert_eq!(records.len(), 3);

    for prefix_len in 0..=records.len() {
        let mut source = RecordSource::new(records[..prefix_len].to_vec());
        let recovered = recover_raft_storage(&mut source).unwrap();

        let Some(replica) = recovered.replica(identity()) else {
            assert_eq!(prefix_len, 0);
            continue;
        };

        let snapshot_boundary_index = replica
            .log_view()
            .snapshot_boundary()
            .map(|(index, _)| index)
            .unwrap_or(0);

        let mut expected_index = snapshot_boundary_index.saturating_add(1);

        for entry in replica.log_view().entries() {
            assert_eq!(entry.record.index, expected_index);

            expected_index = expected_index
                .checked_add(1)
                .expect("test log index must not overflow");
        }

        let recovered_last_index = replica
            .log_view()
            .last_index()
            .unwrap_or(snapshot_boundary_index);

        if let Some(hard_state) = replica.hard_state() {
            assert!(hard_state.commit <= recovered_last_index);
        }

        assert!(replica.progress().applied_index <= recovered_last_index);
    }
}
