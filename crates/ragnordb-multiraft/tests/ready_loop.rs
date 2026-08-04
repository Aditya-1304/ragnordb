use raft::{
    core::node::RaftNode,
    storage::mem::MemStorage,
    traits::{log_store::LogStore, stable_store::StableStore},
};
use ragnordb_common::ids::{RaftGroupId, ReplicaId};
use ragnordb_multiraft::{
    runtime::{RaftReadyLoop, ReadyLoopError},
    storage::{
        codec::RaftReplicaIdentity,
        persistence::{RaftWal, RaftWalStorage},
    },
};
use wal::{
    error::{BatchAppendFailure, WalError},
    lsn::Lsn,
    types::RecordType,
    wal::{AppendResult, BatchAppendResult},
};

type TestNode =
    RaftNode<Vec<u8>, Vec<u8>, MemStorage<Vec<u8>, Vec<u8>>, MemStorage<Vec<u8>, Vec<u8>>>;

type TestLoop = RaftReadyLoop<TestWal, MemStorage<Vec<u8>, Vec<u8>>, MemStorage<Vec<u8>, Vec<u8>>>;

struct TestWal {
    next_lsn: Lsn,
    sync_calls: usize,
    fail_with_unknown_outcome: bool,
}

impl TestWal {
    fn new(fail_with_unknown_outcome: bool) -> Self {
        Self {
            next_lsn: Lsn::new(100),
            sync_calls: 0,
            fail_with_unknown_outcome,
        }
    }
}

impl RaftWal for TestWal {
    fn append_batch_and_sync(
        &mut self,
        records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure> {
        self.sync_calls += 1;

        let mut extents = Vec::with_capacity(records.len());

        for (_, payload) in records {
            let start_lsn = self.next_lsn;
            let end_lsn = start_lsn
                .checked_add_bytes(payload.len() as u64 + 32)
                .unwrap();

            self.next_lsn = end_lsn;
            extents.push(AppendResult { start_lsn, end_lsn });
        }

        let result = BatchAppendResult {
            final_end_lsn: extents
                .last()
                .map(|extent| extent.end_lsn)
                .unwrap_or(Lsn::ZERO),
            record_extents: extents,
        };

        if self.fail_with_unknown_outcome {
            return Err(BatchAppendFailure::OutcomeUnknown {
                result,
                source: WalError::BrokenDurabilityContract,
            });
        }

        Ok(result)
    }
}

fn identity() -> RaftReplicaIdentity {
    RaftReplicaIdentity::new(RaftGroupId(101), ReplicaId(1)).unwrap()
}

fn new_loop(fail_with_unknown_outcome: bool) -> TestLoop {
    let node: TestNode = RaftNode::new(1, Vec::new(), MemStorage::new(), MemStorage::new(), 5, 2);

    let storage = RaftWalStorage::new(TestWal::new(fail_with_unknown_outcome), identity());

    RaftReadyLoop::new(node, storage)
}

fn persist_initial_bootstrap_ready(loop_: &mut TestLoop) {
    let acknowledged = loop_
        .persist_next_ready(None)
        .unwrap()
        .expect("initial bootstrap Ready must be acknowledged");

    assert!(acknowledged.conf_state.is_some());
}

/// Realistic bug caught:
///
/// The runtime must not expose a Ready generation until the A-WAL persistence
/// boundary has completed and the exact Ready identifier has been acknowledged
/// by the Raft core.
#[test]
fn ready_is_released_only_after_wal_persistence_and_acknowledgement() {
    let mut loop_ = new_loop(false);
    persist_initial_bootstrap_ready(&mut loop_);

    let election_ticks = loop_.raft().current_election_timeout();
    loop_.tick(election_ticks).unwrap();

    let acknowledged = loop_
        .persist_next_ready(None)
        .unwrap()
        .expect("election Ready must be persisted");

    assert_eq!(loop_.persistence().wal().sync_calls, 1);
    assert_eq!(acknowledged.hard_state.unwrap().current_term, 1);
    assert_eq!(loop_.raft().durable_hard_state().current_term, 1);
    assert!(loop_.raft().recovery_required_ready_id().is_none());
}

/// Realistic bug caught:
///
/// An uncertain shared-WAL result must not be retried as if it were a normal
/// failure. The outstanding Ready remains unknown and the group is fenced until
/// startup recovery reconstructs the durable prefix.
#[test]
fn unknown_ready_persistence_fences_the_group_and_releases_nothing() {
    let mut loop_ = new_loop(true);
    persist_initial_bootstrap_ready(&mut loop_);

    let election_ticks = loop_.raft().current_election_timeout();
    loop_.tick(election_ticks).unwrap();

    assert_eq!(
        loop_.persist_next_ready(None).unwrap_err(),
        ReadyLoopError::RecoveryRequired
    );

    assert!(loop_.raft().recovery_required_ready_id().is_some());

    assert_eq!(
        loop_.persist_next_ready(None).unwrap_err(),
        ReadyLoopError::RecoveryRequired
    );
}
