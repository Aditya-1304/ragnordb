use raft::types::{HardState, ReplicaId as CoreReplicaId};
use ragnordb_common::ids::{RaftGroupId, ReplicaId, TabletId};
use ragnordb_multiraft::{
    runtime::RaftSnapshotStore,
    snapshot::{TabletSnapshotRaftStore, TabletSnapshotTransfer, persist_tablet_snapshot_boundary},
    storage::{
        codec::RaftReplicaIdentity,
        persistence::{RaftWal, RaftWalRecordType, RaftWalStorage},
    },
};
use ragnordb_tablet::snapshot::{
    AppliedTabletFrontier, FileTabletSnapshotStore, TabletSnapshotConfState, TabletSnapshotImage,
    TabletSnapshotMetadata, TabletSnapshotPointer,
};
use std::{fs, process};
use wal::{
    error::BatchAppendFailure,
    lsn::Lsn,
    types::RecordType,
    wal::{AppendResult, BatchAppendResult},
};

fn pointer() -> TabletSnapshotPointer {
    let payload = b"tablet-state".to_vec();

    let metadata = TabletSnapshotMetadata::for_payload(
        "ragnordb-test",
        RaftGroupId(17),
        ReplicaId(2),
        TabletId(31),
        4,
        9,
        12,
        5,
        TabletSnapshotConfState::new(7, [ReplicaId(1), ReplicaId(2), ReplicaId(3)], [], [])
            .unwrap(),
        &payload,
    )
    .unwrap();

    TabletSnapshotPointer {
        metadata,
        file_name: "tablet-17-2-31-9.snapshot".to_string(),
    }
}

fn image() -> TabletSnapshotImage {
    let pointer = pointer();

    TabletSnapshotImage::new(pointer.metadata, b"tablet-state".to_vec()).unwrap()
}

#[derive(Debug)]
struct RecordingWal {
    next_lsn: Lsn,
    record_types: Vec<RecordType>,
}

impl RaftWal for RecordingWal {
    fn append_batch_and_sync(
        &mut self,
        records: &[(RecordType, &[u8])],
    ) -> Result<BatchAppendResult, BatchAppendFailure> {
        let mut extents = Vec::with_capacity(records.len());

        for (record_type, payload) in records {
            let start_lsn = self.next_lsn;
            let end_lsn = start_lsn
                .checked_add_bytes(payload.len() as u64 + 32)
                .unwrap();

            self.record_types.push(*record_type);
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

#[test]
fn tablet_transfer_derives_core_metadata_without_transporting_core_snapshot_state() {
    let transfer = TabletSnapshotTransfer::from_image(image()).unwrap();

    assert_eq!(transfer.raft_metadata().snapshot_id, 9);
    assert_eq!(transfer.raft_metadata().last_included_index, 12);
    assert_eq!(transfer.raft_metadata().last_included_term, 5);

    assert_eq!(
        transfer.raft_metadata().conf_state.voters,
        [
            CoreReplicaId::must(1),
            CoreReplicaId::must(2),
            CoreReplicaId::must(3),
        ]
        .into_iter()
        .collect()
    );

    let snapshot = transfer.into_core_snapshot();

    assert_eq!(snapshot.data, b"tablet-state");
    assert_eq!(snapshot.size_bytes, b"tablet-state".len() as u64);
}

#[test]
fn incoming_tablet_boundary_persists_pointer_before_hard_state() {
    let identity = RaftReplicaIdentity::new(RaftGroupId(17), ReplicaId(2)).unwrap();

    let wal = RecordingWal {
        next_lsn: Lsn::new(100),
        record_types: Vec::new(),
    };

    let mut storage = RaftWalStorage::new(wal, identity);

    let persisted = persist_tablet_snapshot_boundary(
        &mut storage,
        &pointer(),
        AppliedTabletFrontier::new(12, 5),
        HardState {
            current_term: 5,
            voted_for: Some(CoreReplicaId::must(2)),
            commit: 12,
        },
    )
    .unwrap();

    assert_eq!(persisted.record_count, 2);

    assert_eq!(
        storage.wal().record_types,
        vec![
            RaftWalRecordType::SnapshotPointer.as_wal_record_type(),
            RaftWalRecordType::HardState.as_wal_record_type(),
        ]
    );
}

#[test]
fn raft_snapshot_store_reads_tablet_envelopes_as_core_payloads() {
    let root = std::env::temp_dir().join(format!(
        "ragnordb-multiraft-tablet-snapshot-{}",
        process::id()
    ));

    let _ = fs::remove_dir_all(&root);

    let file_store = FileTabletSnapshotStore::new(root.clone(), 4096).unwrap();
    let tablet_image = image();
    let tablet_pointer = file_store.publish(&tablet_image).unwrap();

    let mut raft_store = TabletSnapshotRaftStore::new(file_store, tablet_pointer);

    let transfer = TabletSnapshotTransfer::from_image(tablet_image).unwrap();
    let core_snapshot = transfer.into_core_snapshot();

    let identity = RaftReplicaIdentity::new(RaftGroupId(17), ReplicaId(2)).unwrap();

    let raft_pointer = raft_store.publish(identity, &core_snapshot).unwrap();
    let loaded = raft_store.load_verified(&raft_pointer).unwrap();

    assert_eq!(loaded, core_snapshot);

    let _ = fs::remove_dir_all(root);
}
