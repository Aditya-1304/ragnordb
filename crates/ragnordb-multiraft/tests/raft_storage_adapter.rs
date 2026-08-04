use std::panic::{AssertUnwindSafe, catch_unwind};

use raft::{
    entry::{EntryPayload, LogEntry},
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{ConfState, HardState, ReplicaId as CoreReplicaId},
};
use ragnordb_common::ids::{RaftGroupId, ReplicaId};
use ragnordb_multiraft::storage::{
    adapter::RaftStorageAdapters,
    codec::RaftReplicaIdentity,
    frontier::RaftProgress,
    persistence::{
        RaftPersistenceBatch, RaftPersistenceError, RaftWal, RaftWalRecordType, RaftWalStorage,
    },
    recovery::{RaftWalRecoverySource, recover_raft_storage},
};
use wal::{
    error::{AppendFailure, WalError},
    lsn::Lsn,
    types::RecordType,
    wal::{AppendResult, iterator::WalRecord},
};

struct RecordingWal {
    next_lsn: Lsn,
    records: Vec<WalRecord>,
    synced_through: Vec<Lsn>,
    sync_calls: usize,
    fail_sync_call: Option<usize>,
}

impl RecordingWal {
    fn new() -> Self {
        Self {
            next_lsn: Lsn::new(100),
            records: Vec::new(),
            synced_through: Vec::new(),
            sync_calls: 0,
            fail_sync_call: None,
        }
    }

    fn failing_progress_sync() -> Self {
        Self {
            fail_sync_call: Some(2),
            ..Self::new()
        }
    }
}

impl RaftWal for RecordingWal {
    fn append(
        &mut self,
        record_type: RecordType,
        payload: &[u8],
    ) -> Result<AppendResult, AppendFailure> {
        let start_lsn = self.next_lsn;
        let end_lsn = start_lsn
            .checked_add_bytes(payload.len() as u64 + 32)
            .unwrap();

        self.records.push(WalRecord {
            lsn: start_lsn,
            record_type,
            payload: payload.to_vec(),
            total_len: (payload.len() + 32) as u32,
        });
        self.next_lsn = end_lsn;

        Ok(AppendResult { start_lsn, end_lsn })
    }

    fn sync_through(&mut self, end_lsn: Lsn) -> Result<(), WalError> {
        self.sync_calls += 1;
        self.synced_through.push(end_lsn);

        if self.fail_sync_call == Some(self.sync_calls) {
            return Err(WalError::BrokenDurabilityContract);
        }

        Ok(())
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
            entries: vec![
                LogEntry::normal_with_size(1, 1, b"one".to_vec(), 3),
                LogEntry::normal_with_size(2, 1, b"two".to_vec(), 3),
                LogEntry::normal_with_size(3, 2, b"three".to_vec(), 5),
            ],
            conf_state: Some(conf_state()),
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
fn durable_progress_recovers_into_public_raft_storage_adapters() {
    let mut storage = durable_storage();
    let progress = RaftProgress::new(2, 1, 3).unwrap();

    let persisted = storage.persist_progress(progress).unwrap();
    let progress_end_lsn = persisted.end_lsn.unwrap();

    assert_eq!(storage.progress(), progress);
    assert_eq!(storage.wal().synced_through.last(), Some(&progress_end_lsn));
    assert_eq!(
        storage.wal().records.last().unwrap().record_type,
        RaftWalRecordType::Progress.as_wal_record_type()
    );

    let mut source = RecordSource::new(storage.wal().records.clone());
    let recovered = recover_raft_storage(&mut source).unwrap();
    let replica = recovered.replica(identity()).unwrap();
    let adapters = RaftStorageAdapters::from_recovered(replica).unwrap();

    assert_eq!(replica.progress(), progress);

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
fn progress_cannot_move_applied_state_beyond_the_durable_commit() {
    let mut storage = durable_storage();

    let error = storage
        .persist_progress(RaftProgress::new(2, 1, 4).unwrap())
        .unwrap_err();

    assert_eq!(
        error,
        RaftPersistenceError::AppliedBeyondDurableCommit {
            applied_index: 4,
            durable_commit: 3,
        }
    );
    assert_eq!(storage.progress(), RaftProgress::default());
    assert_eq!(storage.wal().records.len(), 5);
}

#[test]
fn failed_progress_sync_does_not_publish_the_frontier() {
    let mut storage = durable_storage_with(RecordingWal::failing_progress_sync());

    let error = storage
        .persist_progress(RaftProgress::new(2, 1, 3).unwrap())
        .unwrap_err();

    assert!(matches!(error, RaftPersistenceError::OutcomeUnknown { .. }));
    assert_eq!(storage.progress(), RaftProgress::default());
    assert!(storage.recovery_required());
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
