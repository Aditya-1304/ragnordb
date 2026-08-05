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
        TabletSnapshotMetadata, TabletSnapshotPointer, TabletSnapshotReceiveError,
        generate_local_snapshot, install_incoming_snapshot,
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
use std::{
    cell::Cell,
    sync::{Arc, Mutex},
};

/// snapshot operation classes covered by the node-local concurrency budget
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotWorkKind {
    Generation,
    Send,
    Receive,
    Install,
}

impl SnapshotWorkKind {
    const fn index(self) -> usize {
        match self {
            Self::Generation => 0,
            Self::Send => 1,
            Self::Receive => 2,
            Self::Install => 3,
        }
    }

    const fn limit(self, limits: SnapshotWorkLimits) -> usize {
        match self {
            Self::Generation => limits.max_generations,
            Self::Send => limits.max_sends,
            Self::Receive => limits.max_receives,
            Self::Install => limits.max_installs,
        }
    }
}

/// maximum number of simultaneous operations for each snapshot stage
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotWorkLimits {
    pub max_generations: usize,
    pub max_sends: usize,
    pub max_receives: usize,
    pub max_installs: usize,
}

impl Default for SnapshotWorkLimits {
    fn default() -> Self {
        Self {
            max_generations: 1,
            max_sends: 1,
            max_receives: 1,
            max_installs: 1,
        }
    }
}

impl SnapshotWorkLimits {
    pub fn validate(self) -> Result<Self, SnapshotWorkError> {
        if self.max_generations == 0
            || self.max_sends == 0
            || self.max_receives == 0
            || self.max_installs == 0
        {
            return Err(SnapshotWorkError::ZeroLimit);
        }

        Ok(self)
    }
}

/// public byte and concurrency counters for one MultiRaft snapshot host
///
/// Byte counters are cumulative for the lifetime of the controller; active
/// operation counters describe the work currently holding an admission permit
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotWorkProgress {
    pub active_generations: usize,
    pub active_sends: usize,
    pub active_receives: usize,
    pub active_installs: usize,
    pub generation_bytes_total: u64,
    pub generation_bytes_completed: u64,
    pub send_bytes_total: u64,
    pub send_bytes_completed: u64,
    pub receive_bytes_total: u64,
    pub receive_bytes_completed: u64,
    pub install_bytes_total: u64,
    pub install_bytes_completed: u64,
    pub rejected_operations: u64,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SnapshotWorkError {
    #[error("all snapshot concurrency limits must be non-zero")]
    ZeroLimit,

    #[error("snapshot {kind:?} concurrency limit {limit} has been reached")]
    LimitReached {
        kind: SnapshotWorkKind,
        limit: usize,
    },

    #[error("snapshot chunk size must be non-zero")]
    ZeroChunkSize,
}

#[derive(Debug, Default)]
struct SnapshotWorkState {
    active: [usize; 4],
    bytes_total: [u64; 4],
    bytes_completed: [u64; 4],
    rejected_operations: u64,
}

/// node local admission controller for snapshot generation and transfer work
#[derive(Debug, Clone)]
pub struct SnapshotWorkController {
    limits: SnapshotWorkLimits,
    state: Arc<Mutex<SnapshotWorkState>>,
}

impl SnapshotWorkController {
    pub fn new(limits: SnapshotWorkLimits) -> Result<Self, SnapshotWorkError> {
        Ok(Self {
            limits: limits.validate()?,
            state: Arc::new(Mutex::new(SnapshotWorkState::default())),
        })
    }

    pub fn limits(&self) -> SnapshotWorkLimits {
        self.limits
    }

    pub fn acquire(&self, kind: SnapshotWorkKind) -> Result<SnapshotWorkPermit, SnapshotWorkError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let index = kind.index();
        let limit = kind.limit(self.limits);

        if state.active[index] >= limit {
            state.rejected_operations = state.rejected_operations.saturating_add(1);
            return Err(SnapshotWorkError::LimitReached { kind, limit });
        }

        state.active[index] += 1;

        Ok(SnapshotWorkPermit {
            controller: self.clone(),
            kind,
            total_bytes: Cell::new(0),
            completed_bytes: Cell::new(0),
        })
    }

    pub fn progress(&self) -> SnapshotWorkProgress {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        SnapshotWorkProgress {
            active_generations: state.active[0],
            active_sends: state.active[1],
            active_receives: state.active[2],
            active_installs: state.active[3],
            generation_bytes_total: state.bytes_total[0],
            generation_bytes_completed: state.bytes_completed[0],
            send_bytes_total: state.bytes_total[1],
            send_bytes_completed: state.bytes_completed[1],
            receive_bytes_total: state.bytes_total[2],
            receive_bytes_completed: state.bytes_completed[2],
            install_bytes_total: state.bytes_total[3],
            install_bytes_completed: state.bytes_completed[3],
            rejected_operations: state.rejected_operations,
        }
    }
}

impl Default for SnapshotWorkController {
    fn default() -> Self {
        Self::new(SnapshotWorkLimits::default())
            .expect("the default snapshot work limits are valid")
    }
}

/// Permit held for the entire lifetime of one bounded snapshot operation.
#[derive(Debug)]
pub struct SnapshotWorkPermit {
    controller: SnapshotWorkController,
    kind: SnapshotWorkKind,
    total_bytes: Cell<u64>,
    completed_bytes: Cell<u64>,
}

impl SnapshotWorkPermit {
    pub fn set_total_bytes(&self, total_bytes: u64) {
        let mut state = self
            .controller
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let index = self.kind.index();
        let previous_total = self.total_bytes.get();
        let new_total = previous_total.max(total_bytes);

        self.total_bytes.set(new_total);
        state.bytes_total[index] =
            state.bytes_total[index].saturating_add(new_total.saturating_sub(previous_total));
    }

    pub fn note_bytes(&self, completed_bytes: u64) {
        let mut state = self
            .controller
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let index = self.kind.index();
        let remaining = self
            .total_bytes
            .get()
            .saturating_sub(self.completed_bytes.get());
        let admitted = completed_bytes.min(remaining);

        state.bytes_completed[index] = state.bytes_completed[index].saturating_add(admitted);
        self.completed_bytes
            .set(self.completed_bytes.get().saturating_add(admitted));
    }
}

impl Drop for SnapshotWorkPermit {
    fn drop(&mut self) {
        let mut state = self
            .controller
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.active[self.kind.index()] = state.active[self.kind.index()].saturating_sub(1);
    }
}

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

    /// open a bounded outgoing stream for follower catch up
    pub fn into_sender(
        self,
        work: &SnapshotWorkController,
        max_chunk_bytes: u64,
    ) -> Result<TabletSnapshotSender, TabletSnapshotIntegrationError> {
        TabletSnapshotSender::new(work, self.image, max_chunk_bytes)
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

/// bounded outgoing snapshot stream. Chunks are copied so the sender can be
/// handed to a transport without exposing mutable access to the immutable
/// snapshot image
pub struct TabletSnapshotSender {
    image: TabletSnapshotImage,
    next_offset: usize,
    max_chunk_bytes: usize,
    permit: SnapshotWorkPermit,
}

impl TabletSnapshotSender {
    pub fn new(
        work: &SnapshotWorkController,
        image: TabletSnapshotImage,
        max_chunk_bytes: u64,
    ) -> Result<Self, TabletSnapshotIntegrationError> {
        let max_chunk_bytes = usize::try_from(max_chunk_bytes)
            .map_err(|_| TabletSnapshotIntegrationError::ChunkSizeOverflow)?;
        if max_chunk_bytes == 0 {
            return Err(SnapshotWorkError::ZeroChunkSize.into());
        }

        let permit = work.acquire(SnapshotWorkKind::Send)?;
        permit.set_total_bytes(image.data.len() as u64);

        Ok(Self {
            image,
            next_offset: 0,
            max_chunk_bytes,
            permit,
        })
    }

    pub fn metadata(&self) -> &TabletSnapshotMetadata {
        &self.image.metadata
    }

    pub fn next_chunk(&mut self) -> Option<Vec<u8>> {
        if self.next_offset == self.image.data.len() {
            return None;
        }

        let end = self
            .next_offset
            .saturating_add(self.max_chunk_bytes)
            .min(self.image.data.len());
        let chunk = self.image.data[self.next_offset..end].to_vec();
        self.next_offset = end;
        self.permit.note_bytes(chunk.len() as u64);
        Some(chunk)
    }

    pub fn is_complete(&self) -> bool {
        self.next_offset == self.image.data.len()
    }
}

/// bounded incoming snapshot stream used by follower catch up
pub struct TabletSnapshotReceiveSession {
    receiver: IncomingTabletSnapshotReceiver,
    permit: SnapshotWorkPermit,
}

impl TabletSnapshotReceiveSession {
    pub fn begin(
        work: &SnapshotWorkController,
        store: &FileTabletSnapshotStore,
        metadata: TabletSnapshotMetadata,
        max_chunk_bytes: u64,
    ) -> Result<Self, TabletSnapshotIntegrationError> {
        let permit = work.acquire(SnapshotWorkKind::Receive)?;
        permit.set_total_bytes(metadata.total_length);
        let receiver = IncomingTabletSnapshotReceiver::begin(store, metadata, max_chunk_bytes)
            .map_err(TabletSnapshotIntegrationError::Receive)?;

        Ok(Self { receiver, permit })
    }

    pub fn push_chunk(&mut self, chunk: &[u8]) -> Result<(), TabletSnapshotIntegrationError> {
        self.receiver
            .push_chunk(chunk)
            .map_err(TabletSnapshotIntegrationError::Receive)?;
        self.permit.note_bytes(chunk.len() as u64);
        Ok(())
    }

    pub fn finish(self) -> Result<TabletSnapshotImage, TabletSnapshotIntegrationError> {
        self.receiver
            .finish()
            .map_err(TabletSnapshotIntegrationError::Receive)
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
    work: &SnapshotWorkController,
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
    let permit = work.acquire(SnapshotWorkKind::Generation)?;

    let image = generate_local_snapshot(
        state_machine,
        cluster_id,
        replica_id,
        snapshot_id,
        conf_state,
        AppliedTabletFrontier::new(frontier.index, frontier.term),
    )
    .map_err(TabletSnapshotIntegrationError::Generation)?;

    permit.set_total_bytes(image.data.len() as u64);
    permit.note_bytes(image.data.len() as u64);
    Ok(image)
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

    let retention_floor = storage
        .log_view()
        .first_retained_lsn()
        .unwrap_or(wal::lsn::Lsn::ZERO);
    let _retention_pin = storage
        .acquire_retention_pin("tablet-snapshot-boundary", retention_floor)
        .map_err(TabletSnapshotIntegrationError::Retention)?;

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
    work: &SnapshotWorkController,
    store: &FileTabletSnapshotStore,
    receiver: IncomingTabletSnapshotReceiver,
    target: &TabletSnapshotInstallTarget,
    storage: &mut RaftWalStorage<W>,
    hard_state: HardState,
) -> Result<DurableTabletSnapshotInstall, TabletSnapshotIntegrationError> {
    let install_permit = work.acquire(SnapshotWorkKind::Install)?;
    let mut persisted = None;

    let installed = install_incoming_snapshot(store, receiver, target, |pointer, frontier| {
        let batch =
            persist_tablet_snapshot_boundary(storage, pointer, frontier, hard_state.clone())?;

        install_permit.set_total_bytes(pointer.metadata.total_length);

        persisted = Some(batch);

        Ok::<(), TabletSnapshotIntegrationError>(())
    })
    .map_err(TabletSnapshotIntegrationError::Install)?;

    let persisted = persisted.ok_or(TabletSnapshotIntegrationError::PersistenceShape)?;
    install_permit.note_bytes(installed.pointer.metadata.total_length);

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
    #[error("snapshot work admission failed: {0}")]
    Work(#[from] SnapshotWorkError),

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

    #[error("tablet snapshot retention pin failed: {0}")]
    Retention(String),

    #[error("tablet snapshot receive failed: {0}")]
    Receive(#[from] TabletSnapshotReceiveError),

    #[error("snapshot chunk size does not fit in the host usize")]
    ChunkSizeOverflow,

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
