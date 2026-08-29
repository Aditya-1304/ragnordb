use raft::types::{ConfState, Snapshot};

use ragnordb_catalog::{MetadataApplyOutcome, MetadataRejection};

use ragnordb_common::{
    ids::{NodeId, ReplicaId, TabletId},
    metadata_codec::{
        DesiredReplica, DesiredReplicaPlacement, DesiredReplicaRole, MetadataCommand,
    },
};

use ragnordb_multiraft::{
    meta::{MetadataRaftStateMachine, MetadataReconcileActionKind, next_reconcile_action},
    runtime::RaftReadyStateMachine,
};

#[test]
fn metadata_domain_rejection_does_not_fail_the_raft_state_machine() {
    let mut state_machine = MetadataRaftStateMachine::new();

    state_machine
        .apply(
            1,
            &MetadataCommand::ClusterInitialized {
                cluster_id: "cluster-a".to_string(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();

    // This command is structurally valid but loses the cluster-initialization
    // precondition. It must become a deterministic command rejection instead
    // of a fatal Raft state-machine error.
    state_machine
        .apply(
            2,
            &MetadataCommand::ClusterInitialized {
                cluster_id: "cluster-b".to_string(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();

    assert!(matches!(
        &state_machine.last_applied().unwrap().outcome,
        MetadataApplyOutcome::Rejected(MetadataRejection::ClusterConflict { .. })
    ));

    assert_eq!(state_machine.state().cluster_id(), Some("cluster-a"));
}

#[test]
fn malformed_committed_metadata_bytes_are_fatal_to_state_machine_apply() {
    let mut state_machine = MetadataRaftStateMachine::new();

    assert!(state_machine.apply(1, b"not-a-protobuf-command",).is_err());
}

#[test]
fn metadata_raft_snapshot_restores_exact_projection() {
    let mut source = MetadataRaftStateMachine::new();

    source
        .apply(
            1,
            &MetadataCommand::ClusterInitialized {
                cluster_id: "cluster-a".to_string(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();

    let bytes = source.encode_snapshot().unwrap();

    let conf_state = ConfState::new(1, [raft::types::ReplicaId::must(1)], []).unwrap();

    let snapshot = Snapshot {
        snapshot_id: 10,
        last_included_index: 1,
        last_included_term: 1,
        conf_state,
        size_bytes: bytes.len() as u64,
        checksum: *blake3::hash(&bytes).as_bytes(),
        data: bytes,
    };

    let mut recovered = MetadataRaftStateMachine::new();

    recovered.restore_snapshot(&snapshot).unwrap();

    assert_eq!(recovered.state().cluster_id(), Some("cluster-a"));
}

#[test]
fn reconciliation_adds_promotes_then_removes_in_safe_order() {
    let desired = DesiredReplicaPlacement {
        tablet_id: TabletId(9),

        configuration_epoch: 5,

        replicas: vec![
            DesiredReplica {
                replica_id: ReplicaId(2),
                node_id: NodeId(12),
                role: DesiredReplicaRole::Voter,
            },
            DesiredReplica {
                replica_id: ReplicaId(3),
                node_id: NodeId(13),
                role: DesiredReplicaRole::Learner,
            },
        ],
    };

    // Old replica 1 is currently the only voter.
    let observed = ConfState::new(7, [raft::types::ReplicaId::must(1)], []).unwrap();

    let action = next_reconcile_action(&desired, &observed).unwrap().unwrap();

    assert_eq!(action.metadata_configuration_epoch, 5);

    assert_eq!(action.expected_conf_state_version, 7);

    assert_eq!(
        action.kind,
        MetadataReconcileActionKind::AddLearner {
            replica_id: ReplicaId(2),
            node_id: NodeId(12),
        }
    );

    // Replica 2 was added as learner.
    let observed = ConfState::new(
        8,
        [raft::types::ReplicaId::must(1)],
        [raft::types::ReplicaId::must(2)],
    )
    .unwrap();

    assert_eq!(
        next_reconcile_action(&desired, &observed,)
            .unwrap()
            .unwrap()
            .kind,
        MetadataReconcileActionKind::PromoteLearner {
            replica_id: ReplicaId(2),
            node_id: NodeId(12),
        }
    );

    // Replacement voter is now committed. Desired learner 3 is next.
    let observed = ConfState::new(
        9,
        [
            raft::types::ReplicaId::must(1),
            raft::types::ReplicaId::must(2),
        ],
        [],
    )
    .unwrap();

    assert_eq!(
        next_reconcile_action(&desired, &observed,)
            .unwrap()
            .unwrap()
            .kind,
        MetadataReconcileActionKind::AddLearner {
            replica_id: ReplicaId(3),
            node_id: NodeId(13),
        }
    );

    // All desired replicas now exist, so obsolete voter 1 can be removed.
    let observed = ConfState::new(
        10,
        [
            raft::types::ReplicaId::must(1),
            raft::types::ReplicaId::must(2),
        ],
        [raft::types::ReplicaId::must(3)],
    )
    .unwrap();

    assert_eq!(
        next_reconcile_action(&desired, &observed,)
            .unwrap()
            .unwrap()
            .kind,
        MetadataReconcileActionKind::RemoveReplica {
            replica_id: ReplicaId(1),
        }
    );

    // Final committed ConfState matches desired placement.
    let observed = ConfState::new(
        11,
        [raft::types::ReplicaId::must(2)],
        [raft::types::ReplicaId::must(3)],
    )
    .unwrap();

    assert_eq!(next_reconcile_action(&desired, &observed,).unwrap(), None);
}
