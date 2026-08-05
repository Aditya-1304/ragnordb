use prost::Message;

use ragnordb_common::{
    command_codec::{
        NoopCommand, TabletCommand, TabletCommandEnvelope, TabletStateMachineSnapshot,
    },
    ids::{RaftGroupId, ReplicaId, RequestId, TableId, TabletId},
    proto::snapshot::TabletSnapshotPayload,
};

use ragnordb_tablet::{
    Tablet,
    command::TabletStateMachine,
    snapshot::{
        AppliedTabletFrontier, TabletSnapshotConfState, TabletSnapshotGenerationError,
        generate_local_snapshot,
    },
};

fn state_machine() -> TabletStateMachine {
    let tablet = Tablet::new(TabletId(31), TableId(9)).unwrap();

    TabletStateMachine::new(tablet, 4, RaftGroupId(17)).unwrap()
}

fn conf_state() -> TabletSnapshotConfState {
    TabletSnapshotConfState::new(7, [ReplicaId(1), ReplicaId(2), ReplicaId(3)], [], []).unwrap()
}

/// Catches recording a committed or last-log index instead of the exact
/// state-machine applied frontier supplied by the Raft runtime.
#[test]
fn local_snapshot_records_the_applied_boundary() {
    let mut state_machine = state_machine();

    let request = TabletCommandEnvelope::new(
        RequestId {
            client_id: 42,
            sequence: 1,
            raft_group_id: RaftGroupId(17),
        },
        TabletId(31),
        4,
        TabletCommand::Noop(NoopCommand),
    )
    .unwrap();

    state_machine.apply(request).unwrap();

    let image = generate_local_snapshot(
        &state_machine,
        "ragnordb-test",
        ReplicaId(2),
        9,
        conf_state(),
        AppliedTabletFrontier::new(12, 5),
    )
    .unwrap();

    assert_eq!(image.metadata.last_included_index, 12);
    assert_eq!(image.metadata.last_included_term, 5);

    let payload = TabletSnapshotPayload::decode(image.data.as_slice()).unwrap();

    assert_eq!(payload.format_version, 1);
    assert_eq!(payload.table_id.unwrap().id, 9);

    let restored_state =
        TabletStateMachineSnapshot::decode(payload.tablet_state_machine.as_slice()).unwrap();

    assert_eq!(restored_state.tablet_id.0, 31);
    assert_eq!(restored_state.clients.len(), 1);
}

/// Catches generating or publishing a snapshot before a valid applied
/// index/term boundary exists.
#[test]
fn local_snapshot_rejects_an_invalid_applied_frontier() {
    let state_machine = state_machine();

    let error = generate_local_snapshot(
        &state_machine,
        "ragnordb-test",
        ReplicaId(2),
        9,
        conf_state(),
        AppliedTabletFrontier::new(0, 5),
    )
    .unwrap_err();

    assert_eq!(error, TabletSnapshotGenerationError::ZeroAppliedIndex);
}
