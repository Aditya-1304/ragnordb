use raft::{
    entry::{EntryPayload, LogEntry},
    types::{ConfChange, ConfChangeKind, ConfState, HardState, ReplicaId as CoreReplicaId},
};
use ragnordb_common::ids::{RaftGroupId, ReplicaId};
use ragnordb_multiraft::storage::{
    codec::{
        DurableRaftEntryPayload, RaftHardStateRecord, RaftLogEntryRecord, RaftReplicaIdentity,
    },
    persistence::RaftWalRecordType,
    recovery::{
        RaftStorageRecoveryError, RaftWalRecoverySource, recover_raft_storage,
        recover_raft_storage_with_configurations,
    },
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

fn hard_state_with_vote(
    lsn: u64,
    identity: RaftReplicaIdentity,
    current_term: u64,
    voted_for: u64,
    commit: u64,
) -> WalRecord {
    let payload = RaftHardStateRecord::from_core(
        identity,
        HardState {
            current_term,
            voted_for: Some(CoreReplicaId::must(voted_for)),
            commit,
        },
    )
    .unwrap()
    .encode()
    .unwrap();

    wal_record(lsn, RaftWalRecordType::HardState, payload)
}

fn configuration_entry(
    lsn: u64,
    identity: RaftReplicaIdentity,
    index: u64,
    term: u64,
    change: ConfChange,
) -> WalRecord {
    let payload = RaftLogEntryRecord::from_core(
        identity,
        LogEntry {
            index,
            term,
            encoded_len: 0,
            payload: EntryPayload::Configuration(change),
        },
    )
    .unwrap()
    .encode()
    .unwrap();

    wal_record(lsn, RaftWalRecordType::LogEntry, payload)
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
        hard_state(60, old_lifetime, 2, 2),
        hard_state(70, new_lifetime, 4, 1),
    ]);

    let configurations = BTreeMap::from([(
        new_lifetime,
        ConfState::new(3, [CoreReplicaId::must(82)], []).unwrap(),
    )]);
    let recovered = recover_raft_storage_with_configurations(&mut source, &configurations).unwrap();
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

/// Realistic bug caught: a later WAL entry with a higher term could otherwise
/// truncate an index already acknowledged committed by an earlier HardState.
#[test]
fn recovery_rejects_overwrite_of_committed_prefix() {
    let identity = identity(74, 111);
    let mut source = RecordSource::new(vec![
        entry(10, identity, 1, 1, b"one"),
        entry(20, identity, 2, 1, b"committed-two"),
        hard_state(30, identity, 1, 2),
        entry(40, identity, 2, 2, b"replacement-two"),
    ]);

    let error = recover_raft_storage(&mut source).unwrap_err();
    assert!(matches!(
        error,
        RaftStorageRecoveryError::InvalidLogTransition { .. }
    ));
}

/// Realistic bug caught: restart accepts an older term or commit frontier from
/// a later record and silently forgets an already durable election decision.
#[test]
fn recovery_rejects_hard_state_regression() {
    let identity = identity(75, 121);
    let mut source = RecordSource::new(vec![
        entry(10, identity, 1, 2, b"one"),
        hard_state(20, identity, 2, 1),
        hard_state(30, identity, 1, 1),
    ]);

    let error = recover_raft_storage(&mut source).unwrap_err();
    assert!(matches!(
        error,
        RaftStorageRecoveryError::InvalidStableState(_)
    ));
}

/// Realistic bug caught: an independently durable membership record activates
/// a configuration entry even when its committing HardState was lost in the
/// crash prefix.
#[test]
fn configuration_changes_activate_only_after_their_committing_hard_state() {
    let identity = identity(76, 131);
    let initial = ConfState::new(1, [CoreReplicaId::must(131)], []).unwrap();
    let configurations = BTreeMap::from([(identity, initial.clone())]);
    let change = ConfChange {
        expected_version: 1,
        kind: ConfChangeKind::AddLearner(CoreReplicaId::must(132)),
    };

    let mut entry_only = RecordSource::new(vec![configuration_entry(10, identity, 1, 1, change)]);
    let recovered =
        recover_raft_storage_with_configurations(&mut entry_only, &configurations).unwrap();
    assert_eq!(
        recovered.replica(identity).unwrap().conf_state(),
        Some(&initial)
    );

    let mut committed = RecordSource::new(vec![
        configuration_entry(10, identity, 1, 1, change),
        hard_state(20, identity, 1, 1),
    ]);
    let recovered =
        recover_raft_storage_with_configurations(&mut committed, &configurations).unwrap();
    let active = recovered.replica(identity).unwrap().conf_state().unwrap();
    assert_eq!(active.version, 2);
    assert!(active.learners.contains(&CoreReplicaId::must(132)));
}

/// Realistic bug caught: a later HardState silently changes a vote within the
/// same term or moves the durable commit frontier backwards during restart.
#[test]
fn recovery_rejects_same_term_vote_change_and_commit_regression() {
    let identity = identity(77, 141);
    let mut vote_change = RecordSource::new(vec![
        entry(10, identity, 1, 3, b"one"),
        hard_state_with_vote(20, identity, 3, 141, 1),
        hard_state_with_vote(30, identity, 3, 142, 1),
    ]);
    assert!(matches!(
        recover_raft_storage(&mut vote_change),
        Err(RaftStorageRecoveryError::InvalidStableState(_))
    ));

    let mut commit_regression = RecordSource::new(vec![
        entry(10, identity, 1, 3, b"one"),
        entry(20, identity, 2, 3, b"two"),
        hard_state_with_vote(30, identity, 3, 141, 2),
        hard_state_with_vote(40, identity, 3, 141, 1),
    ]);
    assert!(matches!(
        recover_raft_storage(&mut commit_regression),
        Err(RaftStorageRecoveryError::InvalidStableState(_))
    ));
}
use std::collections::BTreeMap;
