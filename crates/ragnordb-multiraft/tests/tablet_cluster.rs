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

fn request_id() -> RequestId {
    RequestId {
        client_id: 41,
        sequence: 1,
        raft_group_id: RAFT_GROUP_ID,
    }
}

fn single_shard_command(request_id: RequestId) -> (Vec<u8>, ragnordb_common::ids::RowKey, Row) {
    let key = make_row_key(TABLE_ID, &[Value::Int(1)]).unwrap();

    let row = Row {
        values: vec![Value::Int(1), Value::Text("Ada".to_string())],
    };

    let command = SingleShardCommitCommand {
        txn_id: TxnId(11),
        start_timestamp: Timestamp(20),
        commit_timestamp: Timestamp(30),
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
