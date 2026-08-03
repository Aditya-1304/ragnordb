use raft::{
    entry::LogEntry,
    types::{ConfState, HardState, ReplicaId as CoreReplicaId},
};
use ragnordb_common::ids::{RaftGroupId, ReplicaId};
use ragnordb_multiraft::storage::{
    codec::{RaftConfStateRecord, RaftHardStateRecord, RaftReplicaIdentity},
    persistence::{
        RaftPersistenceBatch, RaftPersistenceError, RaftWal, RaftWalRecordType, RaftWalStorage,
    },
};
use wal::{
    error::{AppendFailure, WalError},
    lsn::Lsn,
    types::RecordType,
    wal::AppendResult,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalOperation {
    Append(RecordType),
    SyncThrough(Lsn),
}

struct FakeWal {
    next_lsn: Lsn,
    operations: Vec<WalOperation>,
    fail_sync: bool,
}

impl FakeWal {
    fn healthy() -> Self {
        Self {
            next_lsn: Lsn::new(100),
            operations: Vec::new(),
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
    fn append(
        &mut self,
        record_type: RecordType,
        payload: &[u8],
    ) -> Result<AppendResult, AppendFailure> {
        self.operations.push(WalOperation::Append(record_type));

        let start_lsn = self.next_lsn;
        let end_lsn = start_lsn
            .checked_add_bytes(payload.len() as u64 + 32)
            .unwrap();

        self.next_lsn = end_lsn;

        Ok(AppendResult { start_lsn, end_lsn })
    }

    fn sync_through(&mut self, end_lsn: Lsn) -> Result<(), WalError> {
        self.operations.push(WalOperation::SyncThrough(end_lsn));

        if self.fail_sync {
            return Err(WalError::BrokenDurabilityContract);
        }

        Ok(())
    }
}

fn identity() -> RaftReplicaIdentity {
    RaftReplicaIdentity::new(RaftGroupId(51), ReplicaId(61)).unwrap()
}

fn conf_state() -> ConfState {
    ConfState::new(1, [CoreReplicaId::must(61)], []).unwrap()
}

fn batch() -> RaftPersistenceBatch {
    RaftPersistenceBatch {
        entries: vec![
            LogEntry::normal_with_size(1, 1, b"one".to_vec(), 3),
            LogEntry::normal_with_size(2, 1, b"two".to_vec(), 3),
        ],
        conf_state: Some(conf_state()),
        hard_state: Some(HardState {
            current_term: 1,
            voted_for: Some(CoreReplicaId::must(61)),
            commit: 2,
        }),
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
    let conf_state = conf_state();

    let hard_record = RaftHardStateRecord::from_core(identity, hard_state.clone()).unwrap();
    let conf_record = RaftConfStateRecord::from_core(identity, conf_state.clone()).unwrap();

    let decoded_hard = RaftHardStateRecord::decode(&hard_record.encode().unwrap()).unwrap();
    let decoded_conf = RaftConfStateRecord::decode(&conf_record.encode().unwrap()).unwrap();

    assert_eq!(decoded_hard.identity, identity);
    assert_eq!(decoded_hard.to_core().unwrap(), hard_state);
    assert_eq!(decoded_conf.identity, identity);
    assert_eq!(decoded_conf.to_core().unwrap(), conf_state);
}

#[test]
fn persistence_orders_entries_then_conf_state_then_hard_state_and_exact_sync() {
    let mut storage = RaftWalStorage::new(FakeWal::healthy(), identity());

    let persisted = storage.persist(batch()).unwrap();
    let end_lsn = persisted.end_lsn.unwrap();

    assert_eq!(
        storage.wal().operations,
        vec![
            WalOperation::Append(RaftWalRecordType::LogEntry.as_wal_record_type()),
            WalOperation::Append(RaftWalRecordType::LogEntry.as_wal_record_type()),
            WalOperation::Append(RaftWalRecordType::ConfState.as_wal_record_type()),
            WalOperation::Append(RaftWalRecordType::HardState.as_wal_record_type()),
            WalOperation::SyncThrough(end_lsn),
        ]
    );

    assert_eq!(persisted.record_count, 4);
    assert_eq!(storage.log_view().last_index(), Some(2));
    assert_eq!(storage.conf_state(), Some(&conf_state()));
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
