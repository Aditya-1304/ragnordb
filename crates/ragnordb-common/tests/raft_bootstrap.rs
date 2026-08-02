use std::collections::{BTreeMap, BTreeSet};

use ragnordb_common::{
    ids::{NodeId, RaftGroupId, ReplicaId, TabletId},
    raft_bootstrap::{RAFT_GROUP_BOOTSTRAP_VERSION, RaftGroupBootstrap, RaftGroupBootstrapError},
    rpc_codec::{MetadataResponse, ReplicaRoute},
};

fn bootstrap() -> RaftGroupBootstrap {
    RaftGroupBootstrap::new(
        "ragnordb-dev".to_owned(),
        RaftGroupId(100),
        1,
        BTreeMap::from([
            (ReplicaId(11), NodeId(1)),
            (ReplicaId(12), NodeId(2)),
            (ReplicaId(13), NodeId(3)),
        ]),
        BTreeSet::from([ReplicaId(11), ReplicaId(12), ReplicaId(13)]),
        BTreeSet::new(),
    )
    .expect("valid bootstrap")
}

#[test]
fn metadata_lookup_preserves_replica_and_routing_identities() {
    let response = MetadataResponse::LookupTablet {
        tablet_id: TabletId(7),
        leader_replica_id: ReplicaId(12),
        replicas: vec![
            ReplicaRoute {
                replica_id: ReplicaId(11),
                node_id: NodeId(1),
            },
            ReplicaRoute {
                replica_id: ReplicaId(12),
                node_id: NodeId(2),
            },
            ReplicaRoute {
                replica_id: ReplicaId(13),
                node_id: NodeId(3),
            },
        ],
    };

    let decoded =
        MetadataResponse::from_proto(response.to_proto()).expect("metadata response should decode");

    assert_eq!(decoded, response);
}

#[test]
fn raft_group_bootstrap_roundtrips_without_losing_membership() {
    let expected = bootstrap();
    let bytes = expected.encode().expect("bootstrap should encode");
    let decoded = RaftGroupBootstrap::decode(&bytes).expect("bootstrap should decode");

    assert_eq!(decoded, expected);
    assert_eq!(decoded.format_version, RAFT_GROUP_BOOTSTRAP_VERSION);

    let conf_state = decoded
        .to_core_conf_state()
        .expect("bootstrap should produce a Raft ConfState");

    assert_eq!(conf_state.version, 1);
    assert!(
        conf_state
            .voters
            .contains(&raft::types::ReplicaId::new(11).expect("valid replica ID"))
    );
}

#[test]
fn bootstrap_rejects_a_voter_without_a_physical_route() {
    let result = RaftGroupBootstrap::new(
        "ragnordb-dev".to_owned(),
        RaftGroupId(100),
        1,
        BTreeMap::from([(ReplicaId(11), NodeId(1))]),
        BTreeSet::from([ReplicaId(11), ReplicaId(12)]),
        BTreeSet::new(),
    );

    assert!(matches!(
        result,
        Err(RaftGroupBootstrapError::MembershipMappingMismatch)
    ));
}
