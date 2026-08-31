use raft::types::{ConfState, Snapshot};

use ragnordb_catalog::{MetadataApplyOutcome, MetadataRejection};

use ragnordb_common::{
    catalog_codec::{ColumnDefinition, DataType},
    ids::{ColumnId, NodeId, RaftGroupId, ReplicaId, RequestId, TabletId},
    metadata_codec::{
        CreateTableRequest, DesiredReplica, DesiredReplicaPlacement, DesiredReplicaRole,
        MetadataCommand, MetadataCommandEnvelope, NodeDescriptor,
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

    let results = state_machine.take_applied_results();

    assert_eq!(results.len(), 2);

    assert_eq!(results[0].index, 1);

    assert_eq!(results[0].outcome, MetadataApplyOutcome::Applied,);

    assert_eq!(results[1].index, 2);

    assert!(matches!(
        &results[1].outcome,
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

#[test]
fn multiple_committed_metadata_results_are_preserved_in_log_order() {
    let mut state_machine = MetadataRaftStateMachine::new();

    state_machine
        .apply(
            10,
            &MetadataCommand::ClusterInitialized {
                cluster_id: "cluster-a".to_string(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();

    state_machine
        .apply(
            11,
            &MetadataCommand::ClusterInitialized {
                cluster_id: "cluster-b".to_string(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();

    state_machine
        .apply(
            12,
            &MetadataCommand::ClusterInitialized {
                cluster_id: "cluster-a".to_string(),
            }
            .encode()
            .unwrap(),
        )
        .unwrap();

    let results = state_machine.take_applied_results();

    assert_eq!(
        results
            .iter()
            .map(|result| result.index)
            .collect::<Vec<_>>(),
        vec![10, 11, 12],
    );

    assert_eq!(results[0].outcome, MetadataApplyOutcome::Applied,);

    assert!(matches!(
        results[1].outcome,
        MetadataApplyOutcome::Rejected(MetadataRejection::ClusterConflict { .. }),
    ));

    assert_eq!(results[2].outcome, MetadataApplyOutcome::AlreadyApplied,);

    assert_eq!(state_machine.pending_apply_results(), 0,);
}

#[test]
fn metadata_snapshot_restore_clears_process_local_apply_results() {
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

    assert_eq!(
        recovered.pending_apply_results(),
        0,
        "snapshot recovery must not invent process-local proposal completions",
    );
}

#[test]
fn metadata_state_machine_replays_the_same_request_result() {
    let mut state_machine = MetadataRaftStateMachine::new();
    let request_id = RequestId {
        client_id: 0x63,
        sequence: 1,
        raft_group_id: RaftGroupId(2),
    };

    let node = NodeDescriptor {
        node_id: NodeId(1),
        raft_addr: "127.0.0.1:7101".to_string(),
        snapshot_addr: "127.0.0.1:7151".to_string(),
        sql_addr: "127.0.0.1:7201".to_string(),
        admin_addr: "127.0.0.1:7301".to_string(),
    };
    let create = MetadataCommand::CreateTableTopology(CreateTableRequest {
        table_name: "accounts".to_string(),
        columns: vec![ColumnDefinition {
            column_id: ColumnId(1),
            name: "id".to_string(),
            ty: DataType::Int,
            nullable: false,
        }],
        primary_key_column_ids: vec![ColumnId(1)],
    });

    for (index, command) in [
        MetadataCommand::ClusterInitialized {
            cluster_id: "cluster-a".to_string(),
        },
        MetadataCommand::RegisterNode(node),
    ]
    .into_iter()
    .enumerate()
    {
        state_machine
            .apply((index + 1) as u64, &command.encode().unwrap())
            .unwrap();
    }

    let envelope = MetadataCommandEnvelope::new(request_id.clone(), create.clone()).unwrap();
    let encoded = envelope.encode().unwrap();

    state_machine.apply(3, &encoded).unwrap();
    state_machine.apply(4, &encoded).unwrap();

    let results = state_machine.take_applied_results();

    assert_eq!(results.len(), 4);
    assert_eq!(results[2].request_id, Some(request_id.clone()));
    assert!(matches!(
        results[2].outcome,
        MetadataApplyOutcome::TableCreated(_)
    ));
    assert_eq!(results[3].request_id, Some(request_id));
    assert_eq!(results[3].outcome, results[2].outcome);
}
