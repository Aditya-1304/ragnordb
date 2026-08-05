use std::time::{Duration, Instant};

use ragnordb_common::{
    codec::{Row, Value, WriteKind},
    command_codec::{SingleShardCommitCommand, TabletCommand, TabletCommandEnvelope, WriteEntry},
    ids::{RaftGroupId, RequestId, TableId, TabletId, Timestamp, TxnId},
};
use ragnordb_multiraft::{
    proposal::ProposalCompletion,
    tablet_cluster::{InMemoryTabletCluster, TabletClusterError},
};
use ragnordb_storage::key::{encode_row_key, make_row_key};
use ragnordb_tablet::command::{TabletCommandApplyOutcome, TabletCommandApplyResult};
use ragnordb_txn::Transaction;
use wal::{
    error::BatchAppendFailure,
    lsn::Lsn,
    types::RecordType,
    wal::{AppendResult, BatchAppendResult},
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
