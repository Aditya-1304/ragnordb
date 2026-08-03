use raft::entry::LogEntry;
use ragnordb_common::ids::{RaftGroupId, ReplicaId};
use ragnordb_multiraft::storage::{
    codec::{DurableRaftEntryPayload, RaftLogEntryRecord, RaftReplicaIdentity},
    view::{RaftLogReplayOutcome, RaftLogViewError, RaftReplicaLogView},
};
use wal::lsn::Lsn;

fn identity(replica_id: u64) -> RaftReplicaIdentity {
    RaftReplicaIdentity::new(RaftGroupId(31), ReplicaId(replica_id)).unwrap()
}

fn entry(
    identity: RaftReplicaIdentity,
    index: u64,
    term: u64,
    payload: &[u8],
) -> RaftLogEntryRecord {
    RaftLogEntryRecord::from_core(
        identity,
        LogEntry::normal_with_size(index, term, payload.to_vec(), payload.len()),
    )
    .unwrap()
}

#[test]
fn later_term_overwrite_truncates_the_stale_suffix() {
    let identity = identity(41);
    let mut view = RaftReplicaLogView::new(identity);

    view.replay(entry(identity, 1, 1, b"one"), Lsn::new(10))
        .unwrap();

    view.replay(entry(identity, 2, 1, b"old-two"), Lsn::new(20))
        .unwrap();

    view.replay(entry(identity, 3, 1, b"stale-three"), Lsn::new(30))
        .unwrap();

    let outcome = view
        .replay(entry(identity, 2, 2, b"new-two"), Lsn::new(40))
        .unwrap();

    assert_eq!(
        outcome,
        RaftLogReplayOutcome::ReplacedSuffix { removed_entries: 2 }
    );

    assert_eq!(view.first_index(), Some(1));
    assert_eq!(view.last_index(), Some(2));
    assert!(view.entry(3).is_none());

    assert_eq!(
        view.entry(2).unwrap().record.payload,
        DurableRaftEntryPayload::Normal(b"new-two".to_vec())
    );
}

#[test]
fn same_index_and_term_with_different_payload_is_corruption() {
    let identity = identity(42);
    let mut view = RaftReplicaLogView::new(identity);

    view.replay(entry(identity, 7, 3, b"original"), Lsn::new(50))
        .unwrap();

    let error = view
        .replay(entry(identity, 7, 3, b"different"), Lsn::new(60))
        .unwrap_err();

    assert_eq!(
        error,
        RaftLogViewError::ConflictingPayload { index: 7, term: 3 }
    );

    // The rejected record must not advance or mutate recovered state.
    assert_eq!(view.last_replayed_lsn(), Some(Lsn::new(50)));

    assert_eq!(
        view.entry(7).unwrap().record.payload,
        DurableRaftEntryPayload::Normal(b"original".to_vec())
    );
}

#[test]
fn identical_entry_replay_is_idempotent_and_refreshes_its_lsn() {
    let identity = identity(43);
    let mut view = RaftReplicaLogView::new(identity);

    view.replay(entry(identity, 9, 4, b"command"), Lsn::new(70))
        .unwrap();

    let outcome = view
        .replay(entry(identity, 9, 4, b"command"), Lsn::new(80))
        .unwrap();

    assert_eq!(outcome, RaftLogReplayOutcome::IdempotentReplay);
    assert_eq!(view.len(), 1);
    assert_eq!(view.entry(9).unwrap().lsn, Lsn::new(80));
}

#[test]
fn view_rejects_an_entry_from_another_replica_lifetime() {
    let owner = identity(44);
    let other = identity(45);
    let mut view = RaftReplicaLogView::new(owner);

    let error = view
        .replay(entry(other, 1, 1, b"foreign"), Lsn::new(90))
        .unwrap_err();

    assert_eq!(
        error,
        RaftLogViewError::IdentityMismatch {
            expected: owner,
            received: other,
        }
    );

    assert!(view.is_empty());
    assert_eq!(view.last_replayed_lsn(), None);
}
