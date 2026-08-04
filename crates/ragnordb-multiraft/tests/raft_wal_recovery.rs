use raft::{
    entry::LogEntry,
    types::{ConfState, HardState, ReplicaId as CoreReplicaId},
};
use ragnordb_common::ids::{RaftGroupId, ReplicaId};
use ragnordb_multiraft::storage::{
    codec::{
        DurableRaftEntryPayload, RaftConfStateRecord, RaftHardStateRecord, RaftLogEntryRecord,
        RaftReplicaIdentity,
    },
    persistence::RaftWalRecordType,
    recovery::{RaftStorageRecoveryError, RaftWalRecoverySource, recover_raft_storage},
};
use wal::{error::WalError, lsn::Lsn, wal::iterator::WalRecord};

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

fn identity(group: u64, replica: u64) -> RaftReplicaIdentity {
    RaftReplicaIdentity::new(RaftGroupId(group), ReplicaId(replica)).unwrap()
}

fn entry(
    lsn: u64,
    identity: RaftReplicaIdentity,
    index: u64,
    term: u64,
    payload: &[u8],
) -> WalRecord {
    let payload = RaftLogEntryRecord::from_core(
        identity,
        LogEntry::normal_with_size(index, term, payload.to_vec(), payload.len()),
    )
    .unwrap()
    .encode()
    .unwrap();

    wal_record(lsn, RaftWalRecordType::LogEntry, payload)
}

fn conf_state(lsn: u64, identity: RaftReplicaIdentity, version: u64) -> WalRecord {
    let conf_state =
        ConfState::new(version, [CoreReplicaId::must(identity.replica_id.0)], []).unwrap();

    let payload = RaftConfStateRecord::from_core(identity, conf_state)
        .unwrap()
        .encode()
        .unwrap();

    wal_record(lsn, RaftWalRecordType::ConfState, payload)
}

fn hard_state(
    lsn: u64,
    identity: RaftReplicaIdentity,
    current_term: u64,
    commit: u64,
) -> WalRecord {
    let payload = RaftHardStateRecord::from_core(
        identity,
        HardState {
            current_term,
            voted_for: Some(CoreReplicaId::must(identity.replica_id.0)),
            commit,
        },
    )
    .unwrap()
    .encode()
    .unwrap();

    wal_record(lsn, RaftWalRecordType::HardState, payload)
}

fn wal_record(lsn: u64, kind: RaftWalRecordType, payload: Vec<u8>) -> WalRecord {
    WalRecord {
        lsn: Lsn::new(lsn),
        record_type: kind.as_wal_record_type(),
        total_len: payload.len() as u32,
        payload,
    }
}

#[test]
fn one_pass_recovery_keeps_interleaved_replica_lifetimes_isolated() {
    let old_lifetime = identity(71, 81);
    let new_lifetime = identity(71, 82);

    let mut source = RecordSource::new(vec![
        entry(10, old_lifetime, 1, 1, b"old-one"),
        entry(20, new_lifetime, 1, 4, b"new-one"),
        entry(30, old_lifetime, 2, 1, b"stale-two"),
        entry(40, old_lifetime, 1, 2, b"replacement-one"),
        entry(50, old_lifetime, 2, 2, b"replacement-two"),
        conf_state(60, new_lifetime, 3),
        hard_state(70, old_lifetime, 2, 2),
        hard_state(80, new_lifetime, 4, 1),
    ]);

    let recovered = recover_raft_storage(&mut source).unwrap();
    let old = recovered.replica(old_lifetime).unwrap();
    let new = recovered.replica(new_lifetime).unwrap();

    assert_eq!(recovered.len(), 2);
    assert_eq!(old.log_view().last_index(), Some(2));
    assert_eq!(
        &old.log_view().entry(1).unwrap().record.payload,
        &DurableRaftEntryPayload::Normal(b"replacement-one".to_vec())
    );
    assert_eq!(old.hard_state().unwrap().commit, 2);
    assert!(old.conf_state().is_none());

    assert_eq!(new.log_view().last_index(), Some(1));
    assert_eq!(
        &new.log_view().entry(1).unwrap().record.payload,
        &DurableRaftEntryPayload::Normal(b"new-one".to_vec())
    );
    assert_eq!(new.conf_state().unwrap().version, 3);
    assert_eq!(new.hard_state().unwrap().commit, 1);
}

#[test]
fn recovery_rejects_same_term_same_index_with_different_payload() {
    let identity = identity(72, 91);

    let mut source = RecordSource::new(vec![
        entry(10, identity, 1, 5, b"first"),
        entry(20, identity, 1, 5, b"different"),
    ]);

    let error = recover_raft_storage(&mut source).unwrap_err();

    assert!(matches!(
        error,
        RaftStorageRecoveryError::InvalidLogTransition { .. }
    ));
}

#[test]
fn recovery_rejects_hard_state_before_its_committed_entry() {
    let identity = identity(73, 101);

    let mut source = RecordSource::new(vec![
        entry(10, identity, 1, 1, b"one"),
        hard_state(20, identity, 1, 2),
        entry(30, identity, 2, 1, b"two-arrived-too-late"),
    ]);

    let error = recover_raft_storage(&mut source).unwrap_err();

    assert!(matches!(
        error,
        RaftStorageRecoveryError::HardStateCommitBeyondRecoveredLog {
            identity: received,
            commit_index: 2,
            last_log_index: 1,
            ..
        } if received == identity
    ));
}
