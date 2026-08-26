use std::{
    collections::{BTreeMap, BTreeSet},
    net::TcpListener,
    time::Duration,
};

use raft::{
    message::{Envelope, Message, PreVoteResponse},
    types::ReplicaId as CoreReplicaId,
};
use ragnordb_common::{
    ids::{NodeId, RaftGroupId, ReplicaId},
    raft_bootstrap::RaftGroupBootstrap,
};
use ragnordb_multiraft::transport::NodeRaftTransport;

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
