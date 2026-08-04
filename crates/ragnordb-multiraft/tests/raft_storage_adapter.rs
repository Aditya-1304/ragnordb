use std::panic::{AssertUnwindSafe, catch_unwind};

use raft::{
    core::node::RaftNode,
    entry::{EntryPayload, LogEntry},
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{ConfState, HardState, ReplicaId as CoreReplicaId},
};
use ragnordb_common::ids::{RaftGroupId, ReplicaId};
use ragnordb_multiraft::storage::{
    adapter::RaftStorageAdapters,
    codec::{RAFT_SNAPSHOT_POINTER_RECORD_VERSION, RaftReplicaIdentity, RaftSnapshotPointerRecord},
    persistence::{RaftPersistenceBatch, RaftWal, RaftWalRecordType, RaftWalStorage},
    recovery::{RaftWalRecoverySource, recover_raft_storage},
};
use wal::{
    error::{BatchAppendFailure, WalError},
    lsn::Lsn,
    types::RecordType,
    wal::{AppendResult, BatchAppendResult, iterator::WalRecord},
};

struct RecordingWal {
    next_lsn: Lsn,
    records: Vec<WalRecord>,
    synced_through: Vec<Lsn>,
    sync_calls: usize,
    fail_sync: bool,
}

impl RecordingWal {
    fn new() -> Self {
        Self {
            next_lsn: Lsn::new(100),
            records: Vec::new(),
            synced_through: Vec::new(),
            sync_calls: 0,
            fail_sync: false,
        }
    }
}

impl RaftWal for RecordingWal {
    fn append_batch_and_sync(
        &mut self,
        records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure> {
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
        self.sync_calls += 1;
        if let Some(last) = extents.last() {
            self.synced_through.push(last.end_lsn);
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

fn identity() -> RaftReplicaIdentity {
    RaftReplicaIdentity::new(RaftGroupId(81), ReplicaId(91)).unwrap()
}

fn conf_state() -> ConfState {
    ConfState::new(1, [CoreReplicaId::must(91)], []).unwrap()
}

fn durable_storage() -> RaftWalStorage<RecordingWal> {
    durable_storage_with(RecordingWal::new())
}

fn durable_storage_with(wal: RecordingWal) -> RaftWalStorage<RecordingWal> {
    let mut storage = RaftWalStorage::new(wal, identity());

    storage
        .persist(RaftPersistenceBatch {
            snapshot: Some(RaftSnapshotPointerRecord {
                format_version: RAFT_SNAPSHOT_POINTER_RECORD_VERSION,
                identity: identity(),
                snapshot_id: 2,
                last_included_index: 2,
                last_included_term: 1,
                applied_index: 2,
                conf_state: conf_state(),
                size_bytes: 1024,
                checksum: [7; 32],
                file_name: "raft-81-91-2.snapshot".to_string(),
            }),
            entries: vec![LogEntry::normal_with_size(3, 2, b"three".to_vec(), 5)],
            hard_state: Some(HardState {
                current_term: 2,
                voted_for: Some(CoreReplicaId::must(91)),
                commit: 3,
            }),
        })
        .unwrap();

    storage
}

#[test]
fn snapshot_boundary_recovers_into_public_raft_storage_adapters() {
    let storage = durable_storage();
    assert_eq!(
        storage.wal().records.first().unwrap().record_type,
        RaftWalRecordType::SnapshotPointer.as_wal_record_type()
    );

    let mut source = RecordSource::new(storage.wal().records.clone());
    let recovered = recover_raft_storage(&mut source).unwrap();
    let replica = recovered.replica(identity()).unwrap();
    let adapters = RaftStorageAdapters::from_recovered(replica).unwrap();

    assert_eq!(replica.progress().truncated_through_index, 2);
    assert_eq!(replica.progress().applied_index, 2);

    // Index two remains available only as the snapshot/compaction boundary.
    assert_eq!(adapters.log.first_index(), 3);
    assert_eq!(adapters.log.last_index(), 3);
    assert_eq!(adapters.log.term(2), Some(1));
    assert!(adapters.log.entry(2).is_none());

    assert_eq!(
        adapters.log.entry(3).unwrap().payload,
        EntryPayload::Normal(b"three".to_vec())
    );
    assert_eq!(adapters.stable.hard_state().commit, 3);
    assert_eq!(adapters.stable.conf_state(), Some(conf_state()));
}

#[test]
fn raft_log_adapter_rejects_hidden_trait_persistence() {
    let storage = durable_storage();
    let mut source = RecordSource::new(storage.wal().records.clone());
    let recovered = recover_raft_storage(&mut source).unwrap();

    let mut adapters =
        RaftStorageAdapters::from_recovered(recovered.replica(identity()).unwrap()).unwrap();

    let result = catch_unwind(AssertUnwindSafe(|| {
        adapters.log.append(&[LogEntry::normal_with_size(
            4,
            2,
            b"not-durable".to_vec(),
            11,
        )]);
    }));

    assert!(result.is_err());
    assert_eq!(adapters.log.last_index(), 3);
}

/// Realistic bug caught: the read-only acknowledged-durability adapters look
/// correct in isolation but cannot actually initialize a restarted Raft core
/// from a compacted snapshot boundary and retained suffix.
#[test]
fn recovered_adapters_restart_a_real_raft_node() {
    let storage = durable_storage();
    let mut source = RecordSource::new(storage.wal().records.clone());
    let recovered = recover_raft_storage(&mut source).unwrap();
    let adapters =
        RaftStorageAdapters::from_recovered(recovered.replica(identity()).unwrap()).unwrap();

    let node: RaftNode<Vec<u8>, (), _, _> =
        RaftNode::restart(CoreReplicaId::must(91), adapters.log, adapters.stable, 5, 2).unwrap();

    assert_eq!(node.commit_index(), 3);
    assert_eq!(node.last_log_index(), 3);
    assert_eq!(node.conf_state(), &conf_state());
}

/// Realistic bug caught:
///
/// after restart, the live Raft writer must continue from
/// the recovered durable state. Reinitializing it with `new()` would silently
/// discard the recovered suffix, snapshot boundary, stable state, and WAL
/// durability frontier before the next Ready generation
#[test]
fn recovered_wal_storage_is_seeded_before_the_next_ready() {
    let storage = durable_storage();

    let mut source = RecordSource::new(storage.wal().records.clone());
    let recovered = recover_raft_storage(&mut source).unwrap();
    let replica = recovered.replica(identity()).unwrap();

    let durable_end_lsn = Lsn::new(10_000);
    let seeded =
        RaftWalStorage::from_recovered(RecordingWal::new(), replica, durable_end_lsn).unwrap();

    assert_eq!(seeded.log_view().identity(), identity());
    assert_eq!(seeded.log_view().snapshot_boundary(), Some((2, 1)));
    assert_eq!(seeded.log_view().last_index(), Some(3));
    assert_eq!(seeded.hard_state(), replica.hard_state());
    assert_eq!(seeded.conf_state(), replica.conf_state());
    assert_eq!(seeded.snapshot(), replica.snapshot());
    assert_eq!(seeded.durable_end_lsn(), Some(durable_end_lsn));
    assert!(!seeded.recovery_required());
}
