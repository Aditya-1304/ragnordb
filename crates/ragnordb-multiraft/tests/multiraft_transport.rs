use std::{
    collections::{BTreeMap, BTreeSet},
    net::TcpListener,
    time::Duration,
};

use raft::{
    message::{
        AppendEntriesRequest, Envelope, Message, PreVoteResponse, ReadIndexRequest,
        RequestVoteResponse,
    },
    types::ReplicaId as CoreReplicaId,
};
use ragnordb_common::{
    ids::{NodeId, RaftGroupId, ReplicaId},
    raft_bootstrap::RaftGroupBootstrap,
    rpc_codec::{MessageType, RpcFrame},
};
use ragnordb_multiraft::transport::{NodeRaftTransport, NodeRaftTransportConfig};

fn unused_address() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn bootstrap(group: u64) -> RaftGroupBootstrap {
    RaftGroupBootstrap::new(
        "multiraft-test".to_string(),
        RaftGroupId(group),
        1,
        BTreeMap::from([(ReplicaId(101), NodeId(1)), (ReplicaId(202), NodeId(2))]),
        BTreeSet::from([ReplicaId(101), ReplicaId(202)]),
        BTreeSet::new(),
    )
    .unwrap()
}

fn local_bootstrap(group: u64) -> RaftGroupBootstrap {
    RaftGroupBootstrap::new(
        "multiraft-test".to_string(),
        RaftGroupId(group),
        1,
        BTreeMap::from([(ReplicaId(101), NodeId(2)), (ReplicaId(202), NodeId(1))]),
        BTreeSet::from([ReplicaId(101), ReplicaId(202)]),
        BTreeSet::new(),
    )
    .unwrap()
}

#[test]
fn wire_demultiplexes_same_replica_ids_across_groups() {
    let node_1_addr = unused_address();
    let node_2_addr = unused_address();

    let endpoint_1 = NodeRaftTransport::bind(
        NodeId(1),
        node_1_addr,
        BTreeMap::from([(NodeId(2), node_2_addr)]),
    )
    .unwrap();
    let endpoint_2 = NodeRaftTransport::bind(
        NodeId(2),
        node_2_addr,
        BTreeMap::from([(NodeId(1), node_1_addr)]),
    )
    .unwrap();

    let group_10 = bootstrap(10);
    let group_20 = bootstrap(20);

    let sender_10 = endpoint_1.transport.register_group(&group_10).unwrap();
    let sender_20 = endpoint_1.transport.register_group(&group_20).unwrap();
    endpoint_2.transport.register_group(&group_10).unwrap();
    endpoint_2.transport.register_group(&group_20).unwrap();

    let envelope = Envelope {
        from: CoreReplicaId::must(101),
        to: CoreReplicaId::must(202),
        msg: Message::PreVoteResponse(PreVoteResponse {
            term: 7,
            vote_granted: true,
        }),
    };

    sender_20.try_send(envelope.clone()).unwrap();
    let received = endpoint_2
        .inbound
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    assert_eq!(received.raft_group_id, RaftGroupId(20));
    assert_eq!(received.envelope, envelope);

    sender_10.try_send(envelope.clone()).unwrap();
    let received = endpoint_2
        .inbound
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    assert_eq!(received.raft_group_id, RaftGroupId(10));
    assert_eq!(received.envelope, envelope);
}

/// A bulk append must not occupy the only local receive lane ahead of an
/// election/control message. The distinction is observable before any Raft
/// group scheduler gets a chance to apply its own priority policy.
#[test]
fn local_transport_prioritizes_control_messages_over_bulk_appends() {
    let endpoint = NodeRaftTransport::bind(NodeId(1), unused_address(), BTreeMap::new()).unwrap();
    let sender = endpoint
        .transport
        .register_group(&local_bootstrap(30))
        .unwrap();

    sender
        .try_send(Envelope {
            from: CoreReplicaId::must(101),
            to: CoreReplicaId::must(202),
            msg: Message::AppendEntries(AppendEntriesRequest {
                term: 1,
                leader_id: CoreReplicaId::must(101),
                generation: 0,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![raft::entry::LogEntry::normal(1, 1, vec![0; 8])],
                leader_commit: 0,
            }),
        })
        .unwrap();
    sender
        .try_send(Envelope {
            from: CoreReplicaId::must(101),
            to: CoreReplicaId::must(202),
            msg: Message::RequestVoteResponse(RequestVoteResponse {
                term: 1,
                vote_granted: true,
            }),
        })
        .unwrap();

    let received = endpoint
        .inbound
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert!(matches!(
        received.envelope.msg,
        Message::RequestVoteResponse(_)
    ));
}

/// Catches classifying ReadIndex traffic as bulk replication work, which would
/// let an append burst delay the quorum confirmation needed by a linearizable
/// read.
#[test]
fn local_transport_prioritizes_read_index_control_over_bulk_appends() {
    let endpoint = NodeRaftTransport::bind(NodeId(1), unused_address(), BTreeMap::new()).unwrap();
    let sender = endpoint
        .transport
        .register_group(&local_bootstrap(33))
        .unwrap();

    sender
        .try_send(Envelope {
            from: CoreReplicaId::must(101),
            to: CoreReplicaId::must(202),
            msg: Message::AppendEntries(AppendEntriesRequest {
                term: 1,
                leader_id: CoreReplicaId::must(101),
                generation: 0,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![raft::entry::LogEntry::normal(1, 1, vec![0; 8])],
                leader_commit: 0,
            }),
        })
        .unwrap();
    sender
        .try_send(Envelope {
            from: CoreReplicaId::must(101),
            to: CoreReplicaId::must(202),
            msg: Message::ReadIndex(ReadIndexRequest {
                term: 1,
                leader_id: CoreReplicaId::must(101),
                request_id: 1,
                context: b"read-control".to_vec(),
            }),
        })
        .unwrap();

    let received = endpoint
        .inbound
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert!(matches!(received.envelope.msg, Message::ReadIndex(_)));
}

#[test]
fn local_transport_rejects_bulk_work_when_the_byte_budget_is_full() {
    let endpoint = NodeRaftTransport::bind_with_config(
        NodeId(1),
        unused_address(),
        BTreeMap::new(),
        NodeRaftTransportConfig {
            max_frame_bytes: 1024,
            control_queue_capacity: 4,
            bulk_queue_capacity: 4,
            control_queue_bytes: 1024,
            bulk_queue_bytes: 1,
            outbound_queue_capacity: 4,
            outbound_queue_bytes: 1024,
            inbound_connection_capacity: 4,
            inbound_connection_workers: 1,
            cluster_id: None,
        },
    )
    .unwrap();
    let sender = endpoint
        .transport
        .register_group(&local_bootstrap(31))
        .unwrap();

    let error = sender
        .try_send(Envelope {
            from: CoreReplicaId::must(101),
            to: CoreReplicaId::must(202),
            msg: Message::AppendEntries(AppendEntriesRequest {
                term: 1,
                leader_id: CoreReplicaId::must(101),
                generation: 0,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![raft::entry::LogEntry::normal(1, 1, vec![0; 8])],
                leader_commit: 0,
            }),
        })
        .expect_err("bulk message must not enter an exhausted byte budget");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

/// Realistic bug caught: a detached replica must not leave a physical route
/// that can deliver stale outbound traffic after the local group is gone.
#[test]
fn unregister_group_removes_all_replica_routes() {
    let endpoint = NodeRaftTransport::bind(NodeId(1), unused_address(), BTreeMap::new()).unwrap();
    let group = local_bootstrap(32);
    let sender = endpoint.transport.register_group(&group).unwrap();
    endpoint.transport.unregister_group(&group).unwrap();

    let error = sender
        .try_send(Envelope {
            from: CoreReplicaId::must(101),
            to: CoreReplicaId::must(202),
            msg: Message::PreVoteResponse(PreVoteResponse {
                term: 1,
                vote_granted: true,
            }),
        })
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
}

#[test]
fn dynamic_routes_are_group_scoped_and_conflicts_are_rejected() {
    let node_2_addr = unused_address();
    let node_3_addr = unused_address();
    let endpoint = NodeRaftTransport::bind(
        NodeId(1),
        unused_address(),
        BTreeMap::from([(NodeId(2), node_2_addr), (NodeId(3), node_3_addr)]),
    )
    .unwrap();

    endpoint
        .transport
        .register_dynamic_route(RaftGroupId(41), ReplicaId(7), NodeId(2))
        .unwrap();
    endpoint
        .transport
        .register_dynamic_route(RaftGroupId(42), ReplicaId(7), NodeId(3))
        .unwrap();
    endpoint
        .transport
        .register_dynamic_route(RaftGroupId(41), ReplicaId(7), NodeId(2))
        .unwrap();

    let conflict = endpoint
        .transport
        .register_dynamic_route(RaftGroupId(41), ReplicaId(7), NodeId(3))
        .unwrap_err();
    assert_eq!(conflict.kind(), std::io::ErrorKind::AlreadyExists);
    let unconfigured = endpoint
        .transport
        .register_dynamic_route(RaftGroupId(43), ReplicaId(7), NodeId(99))
        .unwrap_err();
    assert_eq!(unconfigured.kind(), std::io::ErrorKind::NotFound);
    assert_eq!(
        endpoint
            .transport
            .target_node(RaftGroupId(41), ReplicaId(7))
            .unwrap(),
        NodeId(2)
    );
    assert_eq!(
        endpoint
            .transport
            .target_node(RaftGroupId(42), ReplicaId(7))
            .unwrap(),
        NodeId(3)
    );

    endpoint
        .transport
        .unregister_dynamic_route(RaftGroupId(41), ReplicaId(7), NodeId(2))
        .unwrap();
    assert!(
        endpoint
            .transport
            .target_node(RaftGroupId(41), ReplicaId(7))
            .is_err()
    );
    assert_eq!(
        endpoint
            .transport
            .target_node(RaftGroupId(42), ReplicaId(7))
            .unwrap(),
        NodeId(3)
    );
}

/// Realistic bug caught: the physical listener must demultiplex gateway RPC
/// traffic without feeding it to the Raft scheduler, while retaining the
/// authenticated source node identity for authorization and diagnostics.
#[test]
fn wire_demultiplexes_rpc_frames_and_preserves_source_node() {
    let node_1_addr = unused_address();
    let node_2_addr = unused_address();

    let endpoint_1 = NodeRaftTransport::bind(
        NodeId(1),
        node_1_addr,
        BTreeMap::from([(NodeId(2), node_2_addr)]),
    )
    .unwrap();
    let endpoint_2 = NodeRaftTransport::bind(
        NodeId(2),
        node_2_addr,
        BTreeMap::from([(NodeId(1), node_1_addr)]),
    )
    .unwrap();

    let frame = RpcFrame {
        msg_type: MessageType::MetadataRequest,
        raft_group_id: RaftGroupId(2),
        payload: vec![9, 8, 7],
    };

    endpoint_1
        .transport
        .try_send_rpc(NodeId(2), frame.clone())
        .unwrap();

    let received = endpoint_2
        .rpc_inbound
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    assert_eq!(received.source_node_id, NodeId(1));
    assert_eq!(received.frame, frame);
}

#[test]
fn rpc_frame_respects_configured_maximum_before_queue_admission() {
    let endpoint = NodeRaftTransport::bind_with_config(
        NodeId(1),
        unused_address(),
        BTreeMap::new(),
        NodeRaftTransportConfig {
            max_frame_bytes: 2,
            control_queue_capacity: 4,
            bulk_queue_capacity: 4,
            control_queue_bytes: 1024,
            bulk_queue_bytes: 1024,
            outbound_queue_capacity: 4,
            outbound_queue_bytes: 1024,
            inbound_connection_capacity: 4,
            inbound_connection_workers: 1,
            cluster_id: None,
        },
    )
    .unwrap();

    let error = endpoint
        .transport
        .try_send_rpc(
            NodeId(1),
            RpcFrame {
                msg_type: MessageType::MetadataRequest,
                raft_group_id: RaftGroupId(2),
                payload: vec![1, 2, 3],
            },
        )
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn local_rpc_delivery_uses_the_bounded_rpc_lane() {
    let endpoint = NodeRaftTransport::bind(NodeId(1), unused_address(), BTreeMap::new()).unwrap();
    let frame = RpcFrame {
        msg_type: MessageType::TabletCommandResponse,
        raft_group_id: RaftGroupId(3),
        payload: vec![4, 5, 6],
    };

    endpoint
        .transport
        .try_send_rpc(NodeId(1), frame.clone())
        .unwrap();
    let received = endpoint
        .rpc_inbound
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(received.source_node_id, NodeId(1));
    assert_eq!(received.frame, frame);
    assert!(endpoint.inbound.try_recv().is_err());
}

#[test]
fn rpc_api_rejects_raft_consensus_frames() {
    let endpoint = NodeRaftTransport::bind(NodeId(1), unused_address(), BTreeMap::new()).unwrap();
    let error = endpoint
        .transport
        .try_send_rpc(
            NodeId(1),
            RpcFrame {
                msg_type: MessageType::RaftConsensus,
                raft_group_id: RaftGroupId(3),
                payload: vec![1],
            },
        )
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}
