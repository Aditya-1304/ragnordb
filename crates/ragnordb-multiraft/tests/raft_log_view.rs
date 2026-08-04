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

    view.replay(entry(identity, 1, 3, b"original"), Lsn::new(50))
        .unwrap();

    let error = view
        .replay(entry(identity, 1, 3, b"different"), Lsn::new(60))
        .unwrap_err();

    assert_eq!(
        error,
        RaftLogViewError::ConflictingPayload { index: 1, term: 3 }
    );

    // The rejected record must not advance or mutate recovered state.
    assert_eq!(view.last_replayed_lsn(), Some(Lsn::new(50)));

    assert_eq!(
        view.entry(1).unwrap().record.payload,
        DurableRaftEntryPayload::Normal(b"original".to_vec())
    );
}

#[test]
fn identical_entry_replay_is_idempotent_and_refreshes_its_lsn() {
    let identity = identity(43);
    let mut view = RaftReplicaLogView::new(identity);

    view.replay(entry(identity, 1, 4, b"command"), Lsn::new(70))
        .unwrap();

    let outcome = view
        .replay(entry(identity, 1, 4, b"command"), Lsn::new(80))
        .unwrap();

    assert_eq!(outcome, RaftLogReplayOutcome::IdempotentReplay);
    assert_eq!(view.len(), 1);
    assert_eq!(view.entry(1).unwrap().lsn, Lsn::new(80));
}

/// Realistic bug caught: a replayed older entry must remain idempotent even
/// when a later retained entry has a newer term. Rejecting it would turn a
/// valid WAL replay into false corruption during recovery.
#[test]
fn exact_replay_of_an_older_entry_is_idempotent() {
    let identity = identity(49);
    let mut view = RaftReplicaLogView::new(identity);

    view.replay(entry(identity, 1, 1, b"one"), Lsn::new(90))
        .unwrap();
    view.replay(entry(identity, 2, 2, b"two"), Lsn::new(100))
        .unwrap();

    let outcome = view
        .replay(entry(identity, 1, 1, b"one"), Lsn::new(110))
        .unwrap();

    assert_eq!(outcome, RaftLogReplayOutcome::IdempotentReplay);
    assert_eq!(view.entry(1).unwrap().lsn, Lsn::new(110));
    assert_eq!(view.entry(2).unwrap().record.term, 2);
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

/// Realistic bug caught: accepting index 100 as the first recovered entry
/// without a snapshot hides the loss or accidental deletion of entries 1..99.
#[test]
fn view_rejects_a_missing_log_prefix_without_a_snapshot() {
    let identity = identity(45);
    let mut view = RaftReplicaLogView::new(identity);

    let error = view
        .replay(entry(identity, 100, 1, b"orphaned-suffix"), Lsn::new(100))
        .unwrap_err();

    assert_eq!(
        error,
        RaftLogViewError::MissingLogPrefix {
            expected_first_index: 1,
            received_first_index: 100,
        }
    );
    assert!(view.is_empty());
    assert_eq!(view.last_replayed_lsn(), None);
}

/// Realistic bug caught: rejecting a valid snapshot below the current commit
/// frontier prevents compaction when applied state trails committed state.
#[test]
fn view_allows_snapshot_below_commit_and_retains_the_uncompacted_suffix() {
    let identity = identity(46);
    let mut view = RaftReplicaLogView::new(identity);

    for index in 1..=120 {
        view.replay(entry(identity, index, 1, b"entry"), Lsn::new(index * 10))
            .unwrap();
    }
    view.advance_commit(120).unwrap();

    view.install_snapshot(100, 1, Lsn::new(2_000)).unwrap();

    assert_eq!(view.snapshot_boundary(), Some((100, 1)));
    assert_eq!(view.committed_index(), 120);
    assert!(view.entry(100).is_none());
    assert!(view.entry(101).is_some());
    assert!(view.entry(120).is_some());
    assert_eq!(view.first_index(), Some(101));
    assert_eq!(view.last_index(), Some(120));
}

/// Realistic bug caught: accepting a snapshot pointer whose boundary term
/// disagrees with a retained entry would make the recovered Raft log ambiguous.
#[test]
fn view_rejects_a_snapshot_boundary_term_mismatch() {
    let identity = identity(47);
    let mut view = RaftReplicaLogView::new(identity);
    for index in 1..5 {
        view.replay(entry(identity, index, 3, b"entry"), Lsn::new(index * 10))
            .unwrap();
    }
    view.replay(entry(identity, 5, 3, b"entry"), Lsn::new(50))
        .unwrap();

    let error = view.install_snapshot(5, 4, Lsn::new(60)).unwrap_err();

    assert_eq!(
        error,
        RaftLogViewError::SnapshotBoundaryTermMismatch {
            index: 5,
            expected_term: 3,
            received_term: 4,
        }
    );
    assert_eq!(view.snapshot_boundary(), None);
    assert!(view.entry(5).is_some());
}

#[test]
fn view_rejects_term_regression_across_log_and_snapshot_boundaries() {
    let identity = identity(48);

    let mut log_view = RaftReplicaLogView::new(identity);
    log_view
        .replay(entry(identity, 1, 5, b"term-five"), Lsn::new(10))
        .unwrap();

    let error = log_view
        .replay(entry(identity, 2, 4, b"term-four"), Lsn::new(20))
        .unwrap_err();

    assert_eq!(
        error,
        RaftLogViewError::LogTermRegression {
            previous_index: 1,
            previous_term: 5,
            received_index: 2,
            received_term: 4,
        }
    );

    let mut snapshot_view = RaftReplicaLogView::new(identity);
    snapshot_view
        .replay(entry(identity, 1, 5, b"term-five"), Lsn::new(30))
        .unwrap();
    snapshot_view.install_snapshot(1, 5, Lsn::new(40)).unwrap();

    let error = snapshot_view
        .replay(entry(identity, 2, 4, b"term-four"), Lsn::new(50))
        .unwrap_err();

    assert_eq!(
        error,
        RaftLogViewError::LogTermRegression {
            previous_index: 1,
            previous_term: 5,
            received_index: 2,
            received_term: 4,
        }
    );
}
