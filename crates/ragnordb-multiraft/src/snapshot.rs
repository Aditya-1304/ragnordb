//! multi raft integration for tablet snapshot metadata and durability
//!
//! tablet snapshot bytes are produced and received by the tablet crate. This
//! module converts the validated tablet image into the Raft metadata/control
//! shape and provides the concrete A-WAL boundary used by incoming installs

use raft::types::{ConfState, HardState, Snapshot, SnapshotMetadata};
use ragnordb_storage::mvcc::InMemoryMvcc;
use ragnordb_tablet::{
    command::TabletStateMachine,
    snapshot::{
        AppliedTabletFrontier, FileTabletSnapshotStore, IncomingTabletSnapshotReceiver,
        InstalledTabletSnapshot, TabletSnapshotConfState, TabletSnapshotGenerationError,
        TabletSnapshotImage, TabletSnapshotInstallError, TabletSnapshotInstallTarget,
        TabletSnapshotMetadata, TabletSnapshotPointer, generate_local_snapshot,
        install_incoming_snapshot,
    },
};

use crate::runtime::{RaftReadyLoop, RaftSnapshotStore};
use crate::storage::{
    codec::{RaftReplicaIdentity, RaftSnapshotPointerRecord},
    persistence::{
        RaftPersistedBatch, RaftPersistenceBatch, RaftPersistenceError, RaftWal, RaftWalStorage,
    },
};
use raft::traits::{log_store::LogStore, stable_store::StableStore};

/// verified tablet image after it crosses the database/Raft integration
/// boundary
///
/// Network transport exchanges metadata and bounded chunks. The complete image
/// exists only after the tablet receiver verifies length, checksum, and file
/// publication
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletSnapshotTransfer {
    image: TabletSnapshotImage,
    raft_metadata: SnapshotMetadata,
}

impl TabletSnapshotTransfer {
    pub fn from_image(image: TabletSnapshotImage) -> Result<Self, TabletSnapshotIntegrationError> {
        image
            .metadata
            .verify_payload(&image.data)
            .map_err(TabletSnapshotIntegrationError::TabletMetadata)?;

        let raft_metadata = raft_metadata_for_tablet(&image.metadata)?;

        Ok(Self {
            image,
            raft_metadata,
        })
    }

    pub fn raft_metadata(&self) -> &SnapshotMetadata {
        &self.raft_metadata
    }

    pub fn image(&self) -> &TabletSnapshotImage {
        &self.image
    }

    /// convert only an already verified local image into the core Raft value
    pub fn into_core_snapshot(self) -> Snapshot<Vec<u8>> {
        Snapshot {
            snapshot_id: self.raft_metadata.snapshot_id,
            last_included_index: self.raft_metadata.last_included_index,
            last_included_term: self.raft_metadata.last_included_term,
            conf_state: self.raft_metadata.conf_state,
            size_bytes: self.raft_metadata.size_bytes,
            checksum: self.raft_metadata.checksum,
            data: self.image.data,
        }
    }
}

/// convert tablet configuration IDs into the independent core Raft ID type
pub fn core_conf_state_for_tablet(
    conf_state: &ragnordb_tablet::snapshot::TabletSnapshotConfState,
) -> Result<ConfState, TabletSnapshotIntegrationError> {
    let mut core = ConfState::new(
        conf_state.configuration_version,
        conf_state
            .voters
            .iter()
            .map(|replica_id| raft::types::ReplicaId::must(replica_id.0)),
        conf_state
            .learners
            .iter()
            .map(|replica_id| raft::types::ReplicaId::must(replica_id.0)),
    )
    .map_err(|error| TabletSnapshotIntegrationError::CoreConfiguration(format!("{error:?}")))?;

    core.outgoing_voters = conf_state
        .outgoing_voters
        .iter()
        .map(|replica_id| raft::types::ReplicaId::must(replica_id.0))
        .collect();

    core.validate()
        .map_err(|error| TabletSnapshotIntegrationError::CoreConfiguration(format!("{error:?}")))?;

    Ok(core)
}

pub fn raft_metadata_for_tablet(
    metadata: &TabletSnapshotMetadata,
) -> Result<SnapshotMetadata, TabletSnapshotIntegrationError> {
    metadata
        .validate()
        .map_err(TabletSnapshotIntegrationError::TabletMetadata)?;

    Ok(SnapshotMetadata {
        snapshot_id: metadata.snapshot_id,
        last_included_index: metadata.last_included_index,
        last_included_term: metadata.last_included_term,
        conf_state: core_conf_state_for_tablet(&metadata.conf_state)?,
        size_bytes: metadata.total_length,
        checksum: metadata.checksum,
    })
}

/// Convert the published tablet pointer into the durable Raft pointer shape.
pub fn raft_pointer_for_tablet(
    identity: RaftReplicaIdentity,
    pointer: &TabletSnapshotPointer,
) -> Result<RaftSnapshotPointerRecord, TabletSnapshotIntegrationError> {
    pointer
        .metadata
        .validate()
        .map_err(TabletSnapshotIntegrationError::TabletMetadata)?;

    if pointer.metadata.raft_group_id != identity.raft_group_id
        || pointer.metadata.replica_id != identity.replica_id
    {
        return Err(TabletSnapshotIntegrationError::IdentityMismatch);
    }

    let expected_file_name = format!(
        "tablet-{}-{}-{}-{}.snapshot",
        pointer.metadata.raft_group_id.0,
        pointer.metadata.replica_id.0,
        pointer.metadata.tablet_id.0,
        pointer.metadata.snapshot_id,
    );

    if pointer.file_name != expected_file_name {
        return Err(TabletSnapshotIntegrationError::FileNameMismatch);
    }

    let pointer = RaftSnapshotPointerRecord {
        format_version: crate::storage::codec::RAFT_SNAPSHOT_POINTER_RECORD_VERSION,
        identity,
        snapshot_id: pointer.metadata.snapshot_id,
        last_included_index: pointer.metadata.last_included_index,
        last_included_term: pointer.metadata.last_included_term,
        applied_index: pointer.metadata.last_included_index,
        conf_state: core_conf_state_for_tablet(&pointer.metadata.conf_state)?,
        size_bytes: pointer.metadata.total_length,
        checksum: pointer.metadata.checksum,
        file_name: pointer.file_name.clone(),
    };

    pointer
        .validate()
        .map_err(|error| TabletSnapshotIntegrationError::RaftPointer(error.to_string()))?;

    Ok(pointer)
}

/// generate a tablet snapshot from the Ready loop's recorded applied
/// frontier
///
/// the shared references make snapshot capture a single-threaded read-side
/// operation: the caller cannot apply another Ready generation while this
/// function is borrowing the runtime and state machine. The frontier itself
/// comes only from successful Ready application or explicit recovery seeding;
/// a commit index or current term is never substituted
pub fn generate_tablet_snapshot_from_ready_loop<W, LS, SS>(
    ready_loop: &RaftReadyLoop<W, LS, SS>,
    state_machine: &TabletStateMachine<InMemoryMvcc>,
    cluster_id: impl Into<String>,
    replica_id: ragnordb_common::ids::ReplicaId,
    snapshot_id: u64,
    conf_state: TabletSnapshotConfState,
) -> Result<TabletSnapshotImage, TabletSnapshotIntegrationError>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    let frontier = ready_loop
        .applied_frontier()
        .ok_or(TabletSnapshotIntegrationError::AppliedFrontierUnavailable)?;

    generate_local_snapshot(
        state_machine,
        cluster_id,
        replica_id,
        snapshot_id,
        conf_state,
        AppliedTabletFrontier::new(frontier.index, frontier.term),
    )
    .map_err(TabletSnapshotIntegrationError::Generation)
}

/// adapter used by the Ready loop for a published tablet snapshot
///
/// the tablet file contains an envelope. The Raft pointer contains the raw
/// payload length/checksum. This adapter unwraps the envelope before returning
/// a core Snapshot, avoiding an envelope/payload size mismatch
pub struct TabletSnapshotRaftStore {
    store: FileTabletSnapshotStore,
    pointer: TabletSnapshotPointer,
}

impl TabletSnapshotRaftStore {
    pub fn new(store: FileTabletSnapshotStore, pointer: TabletSnapshotPointer) -> Self {
        Self { store, pointer }
    }
}

impl RaftSnapshotStore for TabletSnapshotRaftStore {
    type Error = TabletSnapshotIntegrationError;

    fn publish(
        &mut self,
        identity: RaftReplicaIdentity,
        snapshot: &Snapshot<Vec<u8>>,
    ) -> Result<RaftSnapshotPointerRecord, Self::Error> {
        let expected = raft_metadata_for_tablet(&self.pointer.metadata)?;

        if snapshot.metadata() != expected {
            return Err(TabletSnapshotIntegrationError::CoreMetadataMismatch);
        }

        raft_pointer_for_tablet(identity, &self.pointer)
    }

    fn load_verified(
        &mut self,
        pointer: &RaftSnapshotPointerRecord,
    ) -> Result<Snapshot<Vec<u8>>, Self::Error> {
        let expected = raft_pointer_for_tablet(pointer.identity, &self.pointer)?;

        if pointer != &expected {
            return Err(TabletSnapshotIntegrationError::RaftPointerMismatch);
        }

        let image = self
            .store
            .load_verified(&self.pointer)
            .map_err(|error| TabletSnapshotIntegrationError::TabletStore(error.to_string()))?;

        TabletSnapshotTransfer::from_image(image).map(TabletSnapshotTransfer::into_core_snapshot)
    }
}

/// persist the snapshot pointer and HardState in one ordered A-WAL batch
pub fn persist_tablet_snapshot_boundary<W: RaftWal>(
    storage: &mut RaftWalStorage<W>,
    pointer: &TabletSnapshotPointer,
    frontier: AppliedTabletFrontier,
    hard_state: HardState,
) -> Result<RaftPersistedBatch, TabletSnapshotIntegrationError> {
    if pointer.metadata.last_included_index != frontier.index
        || pointer.metadata.last_included_term != frontier.term
    {
        return Err(TabletSnapshotIntegrationError::FrontierMismatch);
    }

    let identity = storage.log_view().identity();
    let raft_pointer = raft_pointer_for_tablet(identity, pointer)?;

    let persisted = storage.persist(RaftPersistenceBatch {
        snapshot: Some(raft_pointer),
        entries: Vec::new(),
        hard_state: Some(hard_state),
    })?;

    if persisted.record_count != 2 || persisted.end_lsn.is_none() {
        return Err(TabletSnapshotIntegrationError::PersistenceShape);
    }

    Ok(persisted)
}

/// concrete incoming install path using the actual WAL backed storage
pub fn install_incoming_tablet_snapshot<W: RaftWal>(
    store: &FileTabletSnapshotStore,
    receiver: IncomingTabletSnapshotReceiver,
    target: &TabletSnapshotInstallTarget,
    storage: &mut RaftWalStorage<W>,
    hard_state: HardState,
) -> Result<DurableTabletSnapshotInstall, TabletSnapshotIntegrationError> {
    let mut persisted = None;

    let installed = install_incoming_snapshot(store, receiver, target, |pointer, frontier| {
        let batch =
            persist_tablet_snapshot_boundary(storage, pointer, frontier, hard_state.clone())?;

        persisted = Some(batch);

        Ok::<(), TabletSnapshotIntegrationError>(())
    })
    .map_err(TabletSnapshotIntegrationError::Install)?;

    let persisted = persisted.ok_or(TabletSnapshotIntegrationError::PersistenceShape)?;

    Ok(DurableTabletSnapshotInstall {
        installed,
        persisted,
    })
}

#[derive(Debug)]
pub struct DurableTabletSnapshotInstall {
    pub installed: InstalledTabletSnapshot,
    pub persisted: RaftPersistedBatch,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TabletSnapshotIntegrationError {
    #[error("tablet snapshot metadata validation failed: {0}")]
    TabletMetadata(#[from] ragnordb_tablet::snapshot::TabletSnapshotMetadataError),

    #[error("core Raft configuration conversion failed: {0}")]
    CoreConfiguration(String),

    #[error("tablet snapshot belongs to another Raft replica lifetime")]
    IdentityMismatch,

    #[error("tablet snapshot file name is not identity-derived")]
    FileNameMismatch,

    #[error("tablet and core snapshot metadata do not match")]
    CoreMetadataMismatch,

    #[error("Raft snapshot pointer does not match the prepared tablet image")]
    RaftPointerMismatch,

    #[error("tablet snapshot store operation failed: {0}")]
    TabletStore(String),

    #[error("tablet snapshot pointer validation failed: {0}")]
    RaftPointer(String),

    #[error("tablet snapshot boundary does not match its applied frontier")]
    FrontierMismatch,

    #[error("the Ready loop has not recorded an applied frontier")]
    AppliedFrontierUnavailable,

    #[error("tablet snapshot generation failed: {0}")]
    Generation(#[from] TabletSnapshotGenerationError),

    #[error("tablet snapshot install failed: {0}")]
    Install(#[from] TabletSnapshotInstallError),

    #[error("Raft WAL boundary persistence failed: {0}")]
    Persistence(#[from] RaftPersistenceError),

    #[error("Raft WAL boundary did not contain exactly pointer plus HardState")]
    PersistenceShape,
}
