use raft::{
    entry::{EntryPayload, LogEntry},
    types::{ConfChange, ConfChangeKind, ReplicaId as CoreReplicaId},
};
use ragnordb_common::ids::{RaftGroupId, ReplicaId};
use ragnordb_multiraft::storage::codec::{
    DurableRaftEntryPayload, RaftLogEntryRecord, RaftReplicaIdentity,
};

#[test]
fn same_index_in_distinct_replica_lifetimes_preserves_ownership() {
    let first_identity = RaftReplicaIdentity::new(RaftGroupId(7), ReplicaId(11)).unwrap();
    let second_identity = RaftReplicaIdentity::new(RaftGroupId(7), ReplicaId(12)).unwrap();

    let first = RaftLogEntryRecord::from_core(
        first_identity,
        LogEntry::normal_with_size(9, 3, b"first".to_vec(), 5),
    )
    .unwrap();

    let second = RaftLogEntryRecord::from_core(
        second_identity,
        LogEntry::normal_with_size(9, 4, b"second".to_vec(), 6),
    )
    .unwrap();

    let decoded_first = RaftLogEntryRecord::decode(&first.encode().unwrap()).unwrap();
    let decoded_second = RaftLogEntryRecord::decode(&second.encode().unwrap()).unwrap();

    assert_eq!(decoded_first.identity, first_identity);
    assert_eq!(decoded_second.identity, second_identity);
    assert_eq!(decoded_first.index, decoded_second.index);
    assert_ne!(decoded_first.identity, decoded_second.identity);
}

#[test]
fn configuration_entry_roundtrip_never_becomes_a_normal_command() {
    let identity = RaftReplicaIdentity::new(RaftGroupId(8), ReplicaId(21)).unwrap();

    let change = ConfChange {
        expected_version: 4,
        kind: ConfChangeKind::AddLearner(CoreReplicaId::must(22)),
    };

    let entry = LogEntry {
        index: 15,
        term: 6,
        encoded_len: 0,
        payload: EntryPayload::Configuration(change),
    };

    let record = RaftLogEntryRecord::from_core(identity, entry).unwrap();
    let decoded = RaftLogEntryRecord::decode(&record.encode().unwrap()).unwrap();
    let restored = decoded.to_core().unwrap();

    assert_eq!(
        decoded.payload,
        DurableRaftEntryPayload::Configuration(change)
    );
    assert_eq!(restored.payload, EntryPayload::Configuration(change));
    assert!(restored.encoded_len > 0);
}
