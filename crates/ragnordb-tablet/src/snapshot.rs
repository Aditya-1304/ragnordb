//! tablet snapshot metadata and immutable file publication
//!
//! this module owns the database-specific snapshot contract. Raft remains
//! responsible for consensus metadata and log mechanics; this module binds a
//! snapshot image to the exact cluster, tablet generation, configuration,
//! applied boundary, and payload checksum that produced it

use crate::{Tablet, command::TabletStateMachine};

use ragnordb_storage::mvcc::InMemoryMvcc;

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use prost::Message;

use ragnordb_common::{
    command_codec::TabletStateMachineSnapshot,
    ids::{RaftGroupId, ReplicaId, TableId, TabletId},
    proto::{ids as id_proto, snapshot as snapshot_proto},
};

pub const TABLET_SNAPSHOT_METADATA_VERSION: u32 = 1;
pub const TABLET_SNAPSHOT_STORAGE_FORMAT_VERSION: u32 = 1;

const MAX_CLUSTER_ID_BYTES: usize = 256;

static NEXT_TEMP_FILE_ID: AtomicU64 = AtomicU64::new(1);
const TEMP_FILE_PREFIX: &str = ".ragnordb-tablet-snapshot-tmp-";

/// current payload format for a tablet state image
pub const TABLET_SNAPSHOT_PAYLOAD_VERSION: u32 = 1;

/// the exact Raft boundary through which the tablet state machine has applied
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedTabletFrontier {
    pub index: u64,
    pub term: u64,
}

impl AppliedTabletFrontier {
    pub const fn new(index: u64, term: u64) -> Self {
        Self { index, term }
    }
}

/// all consensus and tablet identity needed to build one snapshot metadata
/// envelope
///
/// keeping this input as a named value prevents the metadata constructor from
/// accepting a positional mixture of IDs, epochs, and Raft boundaries. The
/// applied frontier is deliberately carried as one value so callers cannot
/// accidentally pair an index from one Ready generation with a term from
/// another
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletSnapshotMetadataInput {
    pub cluster_id: String,
    pub raft_group_id: RaftGroupId,
    pub replica_id: ReplicaId,
    pub tablet_id: TabletId,
    pub tablet_epoch: u64,
    pub snapshot_id: u64,
    pub applied_frontier: AppliedTabletFrontier,
    pub conf_state: TabletSnapshotConfState,
}

/// generate one immutable tablet snapshot image
///
/// the caller must obtain this frontier only after the corresponding Raft
/// entries have been successfully applied. This function captures the
/// replicated deduplication state and detached MVCC records into one
/// deterministic protobuf payload before metadata computes its checksum
pub fn generate_local_snapshot(
    state_machine: &TabletStateMachine<InMemoryMvcc>,
    cluster_id: impl Into<String>,
    replica_id: ReplicaId,
    snapshot_id: u64,
    conf_state: TabletSnapshotConfState,
    applied_frontier: AppliedTabletFrontier,
) -> Result<TabletSnapshotImage, TabletSnapshotGenerationError> {
    if applied_frontier.index == 0 {
        return Err(TabletSnapshotGenerationError::ZeroAppliedIndex);
    }

    if applied_frontier.term == 0 {
        return Err(TabletSnapshotGenerationError::ZeroAppliedTerm);
    }

    let tablet_state_machine = state_machine
        .encode_snapshot_state()
        .map_err(|error| TabletSnapshotGenerationError::StateMachineSnapshot(error.to_string()))?;

    let (default_values, locks, writes) = state_machine
        .tablet()
        .storage()
        .capture_snapshot_state()
        .into_snapshot_entries();

    let payload = snapshot_proto::TabletSnapshotPayload {
        format_version: TABLET_SNAPSHOT_PAYLOAD_VERSION,
        table_id: Some(state_machine.tablet().table_id().to_proto()),
        tablet_state_machine,
        default_values,
        locks,
        writes,
    }
    .encode_to_vec();

    let metadata = TabletSnapshotMetadata::for_payload(
        TabletSnapshotMetadataInput {
            cluster_id: cluster_id.into(),
            raft_group_id: state_machine.raft_group_id(),
            replica_id,
            tablet_id: state_machine.tablet().id(),
            tablet_epoch: state_machine.epoch(),
            snapshot_id,
            applied_frontier,
            conf_state,
        },
        &payload,
    )?;

    TabletSnapshotImage::new(metadata, payload).map_err(TabletSnapshotGenerationError::Metadata)
}

/// generate and durably publish a local tablet snapshot
///
/// the file store performs the synchronized temporary-file write and atomic
/// publication already established by Slice 1. Raft pointer persistence is
/// intentionally left to the next integration step
pub fn generate_and_publish_local_snapshot(
    store: &FileTabletSnapshotStore,
    state_machine: &TabletStateMachine<InMemoryMvcc>,
    cluster_id: impl Into<String>,
    replica_id: ReplicaId,
    snapshot_id: u64,
    conf_state: TabletSnapshotConfState,
    applied_frontier: AppliedTabletFrontier,
) -> Result<TabletSnapshotPointer, TabletSnapshotGenerationError> {
    let image = generate_local_snapshot(
        state_machine,
        cluster_id,
        replica_id,
        snapshot_id,
        conf_state,
        applied_frontier,
    )?;

    store
        .publish(&image)
        .map_err(TabletSnapshotGenerationError::Store)
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TabletSnapshotGenerationError {
    #[error("local tablet snapshot has no applied index")]
    ZeroAppliedIndex,

    #[error("local tablet snapshot has no applied term")]
    ZeroAppliedTerm,

    #[error("tablet state-machine snapshot encoding failed: {0}")]
    StateMachineSnapshot(String),

    #[error("tablet snapshot metadata validation failed: {0}")]
    Metadata(#[from] TabletSnapshotMetadataError),

    #[error("tablet snapshot publication failed: {0}")]
    Store(#[from] TabletSnapshotStoreError),
}

/// Configuration state captured together with a tablet snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletSnapshotConfState {
    pub configuration_version: u64,
    pub voters: BTreeSet<ReplicaId>,
    pub learners: BTreeSet<ReplicaId>,
    pub outgoing_voters: BTreeSet<ReplicaId>,
}

impl TabletSnapshotConfState {
    pub fn new(
        configuration_version: u64,
        voters: impl IntoIterator<Item = ReplicaId>,
        learners: impl IntoIterator<Item = ReplicaId>,
        outgoing_voters: impl IntoIterator<Item = ReplicaId>,
    ) -> Result<Self, TabletSnapshotMetadataError> {
        let state = Self {
            configuration_version,
            voters: voters.into_iter().collect(),
            learners: learners.into_iter().collect(),
            outgoing_voters: outgoing_voters.into_iter().collect(),
        };

        state.validate()?;
        Ok(state)
    }

    fn from_proto(
        configuration_version: u64,
        voters: Vec<id_proto::ReplicaId>,
        learners: Vec<id_proto::ReplicaId>,
        outgoing_voters: Vec<id_proto::ReplicaId>,
    ) -> Result<Self, TabletSnapshotMetadataError> {
        let state = Self {
            configuration_version,
            voters: decode_replica_set(voters)?,
            learners: decode_replica_set(learners)?,
            outgoing_voters: decode_replica_set(outgoing_voters)?,
        };

        state.validate()?;
        Ok(state)
    }

    pub fn validate(&self) -> Result<(), TabletSnapshotMetadataError> {
        if self.configuration_version == 0 {
            return Err(TabletSnapshotMetadataError::ZeroConfigurationVersion);
        }

        if self.voters.is_empty() {
            return Err(TabletSnapshotMetadataError::NoVoters);
        }

        if self
            .voters
            .iter()
            .chain(self.learners.iter())
            .chain(self.outgoing_voters.iter())
            .any(|replica_id| replica_id.0 == 0)
        {
            return Err(TabletSnapshotMetadataError::ZeroReplicaIdInConfiguration);
        }

        if let Some(replica_id) = self
            .learners
            .iter()
            .find(|replica_id| {
                self.voters.contains(replica_id) || self.outgoing_voters.contains(replica_id)
            })
            .copied()
        {
            return Err(TabletSnapshotMetadataError::VoterLearnerOverlap(replica_id));
        }

        Ok(())
    }
}

fn decode_replica_set(
    replicas: Vec<id_proto::ReplicaId>,
) -> Result<BTreeSet<ReplicaId>, TabletSnapshotMetadataError> {
    let mut decoded = BTreeSet::new();

    for replica in replicas {
        let replica_id = ReplicaId::from_proto(replica);

        if replica_id.0 == 0 {
            return Err(TabletSnapshotMetadataError::ZeroReplicaId);
        }

        if !decoded.insert(replica_id) {
            return Err(TabletSnapshotMetadataError::DuplicateReplicaId(replica_id));
        }
    }

    Ok(decoded)
}

/// Versioned database-specific metadata for an immutable tablet snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletSnapshotMetadata {
    pub format_version: u32,
    pub cluster_id: String,
    pub raft_group_id: RaftGroupId,
    pub replica_id: ReplicaId,
    pub tablet_id: TabletId,
    pub tablet_epoch: u64,
    pub snapshot_id: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub conf_state: TabletSnapshotConfState,
    pub storage_format_version: u32,
    pub total_length: u64,
    pub checksum: [u8; 32],
}

impl TabletSnapshotMetadata {
    /// Build metadata from the exact immutable payload that will be stored.
    pub fn for_payload(
        input: TabletSnapshotMetadataInput,
        payload: &[u8],
    ) -> Result<Self, TabletSnapshotMetadataError> {
        let total_length = u64::try_from(payload.len())
            .map_err(|_| TabletSnapshotMetadataError::PayloadLengthOverflow)?;

        let TabletSnapshotMetadataInput {
            cluster_id,
            raft_group_id,
            replica_id,
            tablet_id,
            tablet_epoch,
            snapshot_id,
            applied_frontier,
            conf_state,
        } = input;

        let metadata = Self {
            format_version: TABLET_SNAPSHOT_METADATA_VERSION,
            cluster_id,
            raft_group_id,
            replica_id,
            tablet_id,
            tablet_epoch,
            snapshot_id,
            last_included_index: applied_frontier.index,
            last_included_term: applied_frontier.term,
            conf_state,
            storage_format_version: TABLET_SNAPSHOT_STORAGE_FORMAT_VERSION,
            total_length,
            checksum: *blake3::hash(payload).as_bytes(),
        };

        metadata.validate()?;
        Ok(metadata)
    }

    pub fn validate(&self) -> Result<(), TabletSnapshotMetadataError> {
        if self.format_version != TABLET_SNAPSHOT_METADATA_VERSION {
            return Err(TabletSnapshotMetadataError::UnsupportedMetadataVersion(
                self.format_version,
            ));
        }

        if self.storage_format_version != TABLET_SNAPSHOT_STORAGE_FORMAT_VERSION {
            return Err(
                TabletSnapshotMetadataError::UnsupportedStorageFormatVersion(
                    self.storage_format_version,
                ),
            );
        }

        if self.cluster_id.trim().is_empty() {
            return Err(TabletSnapshotMetadataError::EmptyClusterId);
        }

        if self.cluster_id.len() > MAX_CLUSTER_ID_BYTES {
            return Err(TabletSnapshotMetadataError::ClusterIdTooLong);
        }

        if self.raft_group_id.0 == 0 {
            return Err(TabletSnapshotMetadataError::ZeroRaftGroupId);
        }

        if self.replica_id.0 == 0 {
            return Err(TabletSnapshotMetadataError::ZeroReplicaId);
        }

        if self.tablet_id.0 == 0 {
            return Err(TabletSnapshotMetadataError::ZeroTabletId);
        }

        if self.tablet_epoch == 0 {
            return Err(TabletSnapshotMetadataError::ZeroTabletEpoch);
        }

        if self.snapshot_id == 0 {
            return Err(TabletSnapshotMetadataError::ZeroSnapshotId);
        }

        if self.last_included_index == 0 || self.last_included_term == 0 {
            return Err(TabletSnapshotMetadataError::InvalidSnapshotBoundary);
        }

        if self.total_length == 0 {
            return Err(TabletSnapshotMetadataError::ZeroTotalLength);
        }

        self.conf_state.validate()
    }

    pub fn verify_payload(&self, payload: &[u8]) -> Result<(), TabletSnapshotMetadataError> {
        self.validate()?;

        let actual_length = u64::try_from(payload.len())
            .map_err(|_| TabletSnapshotMetadataError::PayloadLengthOverflow)?;

        if actual_length != self.total_length {
            return Err(TabletSnapshotMetadataError::LengthMismatch {
                expected: self.total_length,
                actual: actual_length,
            });
        }

        if *blake3::hash(payload).as_bytes() != self.checksum {
            return Err(TabletSnapshotMetadataError::ChecksumMismatch);
        }

        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, TabletSnapshotMetadataError> {
        self.validate()?;

        Ok(self.to_proto().encode_to_vec())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, TabletSnapshotMetadataError> {
        let proto = snapshot_proto::TabletSnapshotMetadata::decode(bytes)
            .map_err(|error| TabletSnapshotMetadataError::Decode(error.to_string()))?;

        let checksum_length = proto.checksum.len();
        let checksum: [u8; 32] = proto
            .checksum
            .try_into()
            .map_err(|_| TabletSnapshotMetadataError::InvalidChecksumLength(checksum_length))?;

        let metadata = Self {
            format_version: proto.format_version,
            cluster_id: proto.cluster_id,
            raft_group_id: RaftGroupId::from_proto(
                proto
                    .raft_group_id
                    .ok_or(TabletSnapshotMetadataError::MissingField("raft_group_id"))?,
            ),
            replica_id: ReplicaId::from_proto(
                proto
                    .replica_id
                    .ok_or(TabletSnapshotMetadataError::MissingField("replica_id"))?,
            ),
            tablet_id: TabletId::from_proto(
                proto
                    .tablet_id
                    .ok_or(TabletSnapshotMetadataError::MissingField("tablet_id"))?,
            ),
            tablet_epoch: proto.tablet_epoch,
            snapshot_id: proto.snapshot_id,
            last_included_index: proto.last_included_index,
            last_included_term: proto.last_included_term,
            conf_state: TabletSnapshotConfState::from_proto(
                proto.configuration_version,
                proto.voters,
                proto.learners,
                proto.outgoing_voters,
            )?,
            storage_format_version: proto.storage_format_version,
            total_length: proto.total_length,
            checksum,
        };

        metadata.validate()?;
        Ok(metadata)
    }

    fn to_proto(&self) -> snapshot_proto::TabletSnapshotMetadata {
        snapshot_proto::TabletSnapshotMetadata {
            format_version: self.format_version,
            cluster_id: self.cluster_id.clone(),
            raft_group_id: Some(self.raft_group_id.to_proto()),
            replica_id: Some(self.replica_id.to_proto()),
            tablet_id: Some(self.tablet_id.to_proto()),
            tablet_epoch: self.tablet_epoch,
            snapshot_id: self.snapshot_id,
            last_included_index: self.last_included_index,
            last_included_term: self.last_included_term,
            configuration_version: self.conf_state.configuration_version,
            voters: self
                .conf_state
                .voters
                .iter()
                .map(|replica_id| replica_id.to_proto())
                .collect(),
            learners: self
                .conf_state
                .learners
                .iter()
                .map(|replica_id| replica_id.to_proto())
                .collect(),
            outgoing_voters: self
                .conf_state
                .outgoing_voters
                .iter()
                .map(|replica_id| replica_id.to_proto())
                .collect(),
            storage_format_version: self.storage_format_version,
            total_length: self.total_length,
            checksum: self.checksum.to_vec(),
        }
    }
}

/// Verified immutable tablet snapshot bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletSnapshotImage {
    pub metadata: TabletSnapshotMetadata,
    pub data: Vec<u8>,
}

impl TabletSnapshotImage {
    pub fn new(
        metadata: TabletSnapshotMetadata,
        data: Vec<u8>,
    ) -> Result<Self, TabletSnapshotMetadataError> {
        metadata.verify_payload(&data)?;

        Ok(Self { metadata, data })
    }
}

/// Durable pointer to one atomically published tablet snapshot file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletSnapshotPointer {
    pub metadata: TabletSnapshotMetadata,
    pub file_name: String,
}

/// Filesystem-backed immutable tablet snapshot publisher.
pub struct FileTabletSnapshotStore {
    root: PathBuf,
    max_file_bytes: u64,
    boot_identity: u128,
    allocator_lock: Mutex<()>,
}

impl FileTabletSnapshotStore {
    pub fn new(
        root: impl Into<PathBuf>,
        max_file_bytes: u64,
    ) -> Result<Self, TabletSnapshotStoreError> {
        if max_file_bytes == 0 {
            return Err(TabletSnapshotStoreError::InvalidMaxFileBytes);
        }

        let root = root.into();
        fs::create_dir_all(&root).map_err(io_error)?;

        let boot_identity = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| TabletSnapshotStoreError::Clock(error.to_string()))?
            .as_nanos()
            ^ u128::from(process::id());

        let mut removed_temporary_file = false;
        for entry in fs::read_dir(&root).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();

            // Only the store's current and legacy private temporary namespaces
            // are eligible for restart cleanup. Published snapshots, allocator
            // state, and unrelated operator files are never removed.
            let managed_temporary_file = file_name.starts_with(TEMP_FILE_PREFIX)
                || ((file_name.starts_with(".tablet-")
                    || file_name.starts_with(".incoming-tablet-snapshot."))
                    && file_name.ends_with(".tmp"));

            if managed_temporary_file && entry.file_type().map_err(io_error)?.is_file() {
                fs::remove_file(entry.path()).map_err(io_error)?;
                removed_temporary_file = true;
            }
        }

        if removed_temporary_file {
            File::open(&root)
                .and_then(|directory| directory.sync_all())
                .map_err(io_error)?;
        }

        Ok(Self {
            root,
            max_file_bytes,
            boot_identity,
            allocator_lock: Mutex::new(()),
        })
    }

    /// Durably reserve the next monotonic snapshot identity for one replica
    /// lifetime before snapshot generation begins.
    ///
    /// The reservation file is synchronized before the ID is returned. A crash
    /// may therefore leave an unused gap, but can never reuse an identity for a
    /// different immutable snapshot image.
    pub fn allocate_snapshot_id(
        &self,
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
        tablet_id: TabletId,
    ) -> Result<u64, TabletSnapshotStoreError> {
        let _allocator_guard = self
            .allocator_lock
            .lock()
            .map_err(|_| TabletSnapshotStoreError::SnapshotIdAllocatorPoisoned)?;

        if raft_group_id.0 == 0 || replica_id.0 == 0 || tablet_id.0 == 0 {
            return Err(TabletSnapshotStoreError::InvalidSnapshotAllocatorIdentity);
        }

        let allocator_name = format!(
            "tablet-{}-{}-{}.next-snapshot-id",
            raft_group_id.0, replica_id.0, tablet_id.0
        );
        let allocator_path = self.root.join(&allocator_name);

        let next_id = match fs::read_to_string(&allocator_path) {
            Ok(value) => value.trim().parse::<u64>().map_err(|error| {
                TabletSnapshotStoreError::InvalidSnapshotIdState(error.to_string())
            })?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.next_id_after_published_snapshots(raft_group_id, replica_id, tablet_id)?
            }
            Err(error) => return Err(io_error(error)),
        };

        if next_id == 0 {
            return Err(TabletSnapshotStoreError::InvalidSnapshotIdState(
                "next snapshot ID is zero".to_string(),
            ));
        }

        let successor = next_id
            .checked_add(1)
            .ok_or(TabletSnapshotStoreError::SnapshotIdExhausted)?;
        let temporary_path = self.temporary_path(&allocator_name);

        let result = (|| -> Result<(), TabletSnapshotStoreError> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary_path)
                .map_err(io_error)?;
            writeln!(file, "{successor}").map_err(io_error)?;
            file.sync_all().map_err(io_error)?;
            drop(file);

            fs::rename(&temporary_path, &allocator_path).map_err(io_error)?;
            File::open(&self.root)
                .and_then(|directory| directory.sync_all())
                .map_err(io_error)
        })();

        if result.is_err() {
            let _ = fs::remove_file(&temporary_path);
        }
        result?;
        Ok(next_id)
    }

    fn next_id_after_published_snapshots(
        &self,
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
        tablet_id: TabletId,
    ) -> Result<u64, TabletSnapshotStoreError> {
        let prefix = format!(
            "tablet-{}-{}-{}-",
            raft_group_id.0, replica_id.0, tablet_id.0
        );
        let mut maximum = 0_u64;

        for entry in fs::read_dir(&self.root).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            if !entry.file_type().map_err(io_error)?.is_file() {
                continue;
            }

            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            let Some(id) = file_name
                .strip_prefix(&prefix)
                .and_then(|value| value.strip_suffix(".snapshot"))
                .and_then(|value| value.parse::<u64>().ok())
            else {
                continue;
            };
            maximum = maximum.max(id);
        }

        maximum
            .checked_add(1)
            .ok_or(TabletSnapshotStoreError::SnapshotIdExhausted)
    }

    pub fn publish(
        &self,
        image: &TabletSnapshotImage,
    ) -> Result<TabletSnapshotPointer, TabletSnapshotStoreError> {
        image.metadata.verify_payload(&image.data)?;

        let file = snapshot_proto::TabletSnapshotFile {
            metadata: Some(image.metadata.to_proto()),
            data: image.data.clone(),
        };

        let encoded_file = file.encode_to_vec();
        let encoded_length = u64::try_from(encoded_file.len())
            .map_err(|_| TabletSnapshotStoreError::EncodedFileLengthOverflow)?;

        if encoded_length > self.max_file_bytes {
            return Err(TabletSnapshotStoreError::SnapshotFileTooLarge {
                actual: encoded_length,
                limit: self.max_file_bytes,
            });
        }

        let pointer = TabletSnapshotPointer {
            metadata: image.metadata.clone(),
            file_name: Self::file_name(&image.metadata),
        };

        let final_path = self.root.join(&pointer.file_name);

        if final_path.exists() {
            let existing = self.load_verified(&pointer)?;

            if existing != *image {
                return Err(TabletSnapshotStoreError::ConflictingPublishedSnapshot);
            }

            return Ok(pointer);
        }

        let temporary_path = self.temporary_path(&pointer.file_name);

        let publication = (|| -> Result<(), TabletSnapshotStoreError> {
            let mut handle = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary_path)
                .map_err(io_error)?;

            handle.write_all(&encoded_file).map_err(io_error)?;
            handle.sync_all().map_err(io_error)?;
            drop(handle);

            match fs::hard_link(&temporary_path, &final_path) {
                Ok(()) => {
                    fs::remove_file(&temporary_path).map_err(io_error)?;
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    fs::remove_file(&temporary_path).map_err(io_error)?;

                    let existing = self.load_verified(&pointer)?;

                    if existing != *image {
                        return Err(TabletSnapshotStoreError::ConflictingPublishedSnapshot);
                    }

                    return Ok(());
                }
                Err(error) => return Err(io_error(error)),
            }

            // The directory sync makes the atomic file-name publication
            // durable before a Raft snapshot pointer can reference it.
            File::open(&self.root)
                .and_then(|directory| directory.sync_all())
                .map_err(io_error)?;

            Ok(())
        })();

        if publication.is_err() {
            let _ = fs::remove_file(&temporary_path);
        }

        publication?;

        let verified = self.load_verified(&pointer)?;

        if verified != *image {
            return Err(TabletSnapshotStoreError::ConflictingPublishedSnapshot);
        }

        Ok(pointer)
    }

    pub fn load_verified(
        &self,
        pointer: &TabletSnapshotPointer,
    ) -> Result<TabletSnapshotImage, TabletSnapshotStoreError> {
        pointer.metadata.validate()?;

        if pointer.file_name != Self::file_name(&pointer.metadata) {
            return Err(TabletSnapshotStoreError::InvalidFileName);
        }

        let path = self.root.join(&pointer.file_name);
        let file = File::open(path).map_err(io_error)?;

        let file_length = file.metadata().map_err(io_error)?.len();

        if file_length > self.max_file_bytes {
            return Err(TabletSnapshotStoreError::SnapshotFileTooLarge {
                actual: file_length,
                limit: self.max_file_bytes,
            });
        }

        let mut bytes = Vec::new();
        let mut bounded_file = file.take(self.max_file_bytes.saturating_add(1));
        bounded_file.read_to_end(&mut bytes).map_err(io_error)?;

        let actual_length = u64::try_from(bytes.len())
            .map_err(|_| TabletSnapshotStoreError::EncodedFileLengthOverflow)?;

        if actual_length > self.max_file_bytes {
            return Err(TabletSnapshotStoreError::SnapshotFileTooLarge {
                actual: actual_length,
                limit: self.max_file_bytes,
            });
        }

        let file = snapshot_proto::TabletSnapshotFile::decode(bytes.as_slice())
            .map_err(|error| TabletSnapshotStoreError::FileDecode(error.to_string()))?;

        let stored_metadata = file
            .metadata
            .ok_or(TabletSnapshotStoreError::MissingFileMetadata)?;

        if stored_metadata != pointer.metadata.to_proto() {
            return Err(TabletSnapshotStoreError::FileMetadataMismatch);
        }

        TabletSnapshotImage::new(
            TabletSnapshotMetadata::decode(&stored_metadata.encode_to_vec())?,
            file.data,
        )
        .map_err(Into::into)
    }

    /// Load a snapshot referenced by durable Raft metadata when only the safe
    /// identity-derived file name is available at startup.
    ///
    /// The embedded tablet metadata is treated as untrusted until the file name,
    /// bounded file envelope, payload length, and checksum all validate.
    pub fn load_verified_by_name(
        &self,
        file_name: &str,
    ) -> Result<TabletSnapshotImage, TabletSnapshotStoreError> {
        let mut components = Path::new(file_name).components();
        if !matches!(
            (components.next(), components.next()),
            (Some(std::path::Component::Normal(_)), None)
        ) {
            return Err(TabletSnapshotStoreError::InvalidFileName);
        }

        let path = self.root.join(file_name);
        let file = File::open(path).map_err(io_error)?;
        let file_length = file.metadata().map_err(io_error)?.len();

        if file_length > self.max_file_bytes {
            return Err(TabletSnapshotStoreError::SnapshotFileTooLarge {
                actual: file_length,
                limit: self.max_file_bytes,
            });
        }

        let mut bytes = Vec::new();
        file.take(self.max_file_bytes.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(io_error)?;

        let stored = snapshot_proto::TabletSnapshotFile::decode(bytes.as_slice())
            .map_err(|error| TabletSnapshotStoreError::FileDecode(error.to_string()))?;
        let stored_metadata = stored
            .metadata
            .ok_or(TabletSnapshotStoreError::MissingFileMetadata)?;
        let metadata = TabletSnapshotMetadata::decode(&stored_metadata.encode_to_vec())?;

        if file_name != Self::file_name(&metadata) {
            return Err(TabletSnapshotStoreError::InvalidFileName);
        }

        TabletSnapshotImage::new(metadata, stored.data).map_err(Into::into)
    }

    fn file_name(metadata: &TabletSnapshotMetadata) -> String {
        format!(
            "tablet-{}-{}-{}-{}.snapshot",
            metadata.raft_group_id.0,
            metadata.replica_id.0,
            metadata.tablet_id.0,
            metadata.snapshot_id
        )
    }

    fn temporary_path(&self, file_name: &str) -> PathBuf {
        let sequence = NEXT_TEMP_FILE_ID.fetch_add(1, Ordering::Relaxed);

        self.root.join(format!(
            "{TEMP_FILE_PREFIX}{}-{}-{sequence}-{file_name}.tmp",
            self.boot_identity,
            process::id()
        ))
    }
}

fn io_error(error: io::Error) -> TabletSnapshotStoreError {
    TabletSnapshotStoreError::Io(error.to_string())
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TabletSnapshotMetadataError {
    #[error("unsupported tablet snapshot metadata version {0}")]
    UnsupportedMetadataVersion(u32),

    #[error("unsupported tablet snapshot storage format version {0}")]
    UnsupportedStorageFormatVersion(u32),

    #[error("tablet snapshot cluster ID is empty")]
    EmptyClusterId,

    #[error("tablet snapshot cluster ID is too long")]
    ClusterIdTooLong,

    #[error("tablet snapshot contains a zero Raft group ID")]
    ZeroRaftGroupId,

    #[error("tablet snapshot contains a zero replica ID")]
    ZeroReplicaId,

    #[error("tablet snapshot contains a zero tablet ID")]
    ZeroTabletId,

    #[error("tablet snapshot contains a zero tablet epoch")]
    ZeroTabletEpoch,

    #[error("tablet snapshot contains a zero snapshot ID")]
    ZeroSnapshotId,

    #[error("tablet snapshot has an invalid included index/term boundary")]
    InvalidSnapshotBoundary,

    #[error("tablet snapshot contains a zero configuration version")]
    ZeroConfigurationVersion,

    #[error("tablet snapshot configuration has no voters")]
    NoVoters,

    #[error("tablet snapshot configuration contains duplicate replica {0:?}")]
    DuplicateReplicaId(ReplicaId),

    #[error("tablet snapshot configuration contains zero replica ID")]
    ZeroReplicaIdInConfiguration,

    #[error("tablet snapshot learner overlaps voter configuration: {0:?}")]
    VoterLearnerOverlap(ReplicaId),

    #[error("tablet snapshot payload length is zero")]
    ZeroTotalLength,

    #[error("tablet snapshot payload length overflows u64")]
    PayloadLengthOverflow,

    #[error("tablet snapshot checksum has invalid length {0}")]
    InvalidChecksumLength(usize),

    #[error("tablet snapshot metadata is missing {0}")]
    MissingField(&'static str),

    #[error("tablet snapshot metadata decode failed: {0}")]
    Decode(String),

    #[error("tablet snapshot payload length mismatch: expected {expected}, actual {actual}")]
    LengthMismatch { expected: u64, actual: u64 },

    #[error("tablet snapshot payload checksum mismatch")]
    ChecksumMismatch,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TabletSnapshotStoreError {
    #[error("tablet snapshot metadata error: {0}")]
    Metadata(#[from] TabletSnapshotMetadataError),

    #[error("tablet snapshot file I/O failed: {0}")]
    Io(String),

    #[error("tablet snapshot file exceeds configured limit: {actual} > {limit}")]
    SnapshotFileTooLarge { actual: u64, limit: u64 },

    #[error("tablet snapshot file length overflowed u64")]
    EncodedFileLengthOverflow,

    #[error("tablet snapshot file metadata is missing")]
    MissingFileMetadata,

    #[error("tablet snapshot file metadata does not match its pointer")]
    FileMetadataMismatch,

    #[error("tablet snapshot file name is not identity-derived")]
    InvalidFileName,

    #[error("tablet snapshot file protobuf decode failed: {0}")]
    FileDecode(String),

    #[error("a different snapshot already occupies this snapshot identity")]
    ConflictingPublishedSnapshot,

    #[error("maximum snapshot file size must be non-zero")]
    InvalidMaxFileBytes,

    #[error("system clock is unavailable while opening the snapshot store: {0}")]
    Clock(String),

    #[error("snapshot allocator identity contains a zero component")]
    InvalidSnapshotAllocatorIdentity,

    #[error("durable snapshot allocator state is invalid: {0}")]
    InvalidSnapshotIdState(String),

    #[error("tablet snapshot identity space is exhausted")]
    SnapshotIdExhausted,

    #[error("tablet snapshot allocator lock is poisoned")]
    SnapshotIdAllocatorPoisoned,
}

/// Receives one incoming snapshot through bounded sequential chunks.
///
/// Chunks are written to a temporary file. The temporary file is synchronized
/// and checksum-verified before the image is handed to the final publisher.
pub struct IncomingTabletSnapshotReceiver {
    metadata: TabletSnapshotMetadata,
    max_chunk_bytes: u64,
    max_file_bytes: u64,
    received_bytes: u64,
    temporary_path: PathBuf,
    file: Option<File>,
}

impl IncomingTabletSnapshotReceiver {
    pub fn begin(
        store: &FileTabletSnapshotStore,
        metadata: TabletSnapshotMetadata,
        max_chunk_bytes: u64,
    ) -> Result<Self, TabletSnapshotReceiveError> {
        metadata.validate()?;

        if max_chunk_bytes == 0 {
            return Err(TabletSnapshotReceiveError::InvalidChunkLimit);
        }

        if metadata.total_length > store.max_file_bytes {
            return Err(TabletSnapshotReceiveError::PayloadTooLarge {
                actual: metadata.total_length,
                limit: store.max_file_bytes,
            });
        }

        let temporary_path = store.temporary_path("incoming-tablet-snapshot");

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .map_err(incoming_io_error)?;

        Ok(Self {
            metadata,
            max_chunk_bytes,
            max_file_bytes: store.max_file_bytes,
            received_bytes: 0,
            temporary_path,
            file: Some(file),
        })
    }

    /// Append one bounded chunk to the temporary snapshot image.
    pub fn push_chunk(&mut self, chunk: &[u8]) -> Result<(), TabletSnapshotReceiveError> {
        let chunk_bytes =
            u64::try_from(chunk.len()).map_err(|_| TabletSnapshotReceiveError::LengthOverflow)?;

        if chunk_bytes > self.max_chunk_bytes {
            return Err(TabletSnapshotReceiveError::ChunkTooLarge {
                actual: chunk_bytes,
                limit: self.max_chunk_bytes,
            });
        }

        let next_received = self
            .received_bytes
            .checked_add(chunk_bytes)
            .ok_or(TabletSnapshotReceiveError::LengthOverflow)?;

        if next_received > self.metadata.total_length {
            return Err(TabletSnapshotReceiveError::ChunkExceedsDeclaredLength {
                received: self.received_bytes,
                chunk: chunk_bytes,
                total: self.metadata.total_length,
            });
        }

        let file = self
            .file
            .as_mut()
            .ok_or(TabletSnapshotReceiveError::ReceiverClosed)?;

        file.write_all(chunk).map_err(incoming_io_error)?;
        self.received_bytes = next_received;

        Ok(())
    }

    /// Finish the transfer and return a verified immutable image.
    pub fn finish(mut self) -> Result<TabletSnapshotImage, TabletSnapshotReceiveError> {
        if self.received_bytes != self.metadata.total_length {
            return Err(TabletSnapshotReceiveError::Incomplete {
                received: self.received_bytes,
                expected: self.metadata.total_length,
            });
        }

        let file = self
            .file
            .take()
            .ok_or(TabletSnapshotReceiveError::ReceiverClosed)?;

        file.sync_all().map_err(incoming_io_error)?;
        drop(file);

        let file = File::open(&self.temporary_path).map_err(incoming_io_error)?;
        let mut bounded_file = file.take(self.max_file_bytes.saturating_add(1));
        let mut bytes = Vec::new();

        bounded_file
            .read_to_end(&mut bytes)
            .map_err(incoming_io_error)?;

        let actual_length =
            u64::try_from(bytes.len()).map_err(|_| TabletSnapshotReceiveError::LengthOverflow)?;

        if actual_length != self.metadata.total_length {
            return Err(TabletSnapshotReceiveError::TemporaryFileLengthMismatch {
                expected: self.metadata.total_length,
                actual: actual_length,
            });
        }

        let metadata = self.metadata.clone();

        TabletSnapshotImage::new(metadata, bytes).map_err(TabletSnapshotReceiveError::Metadata)
    }
}

impl Drop for IncomingTabletSnapshotReceiver {
    fn drop(&mut self) {
        let _ = self.file.take();
        let _ = fs::remove_file(&self.temporary_path);
    }
}

/// Identity and generation expected by the receiving tablet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TabletSnapshotInstallTarget {
    pub cluster_id: String,
    pub raft_group_id: RaftGroupId,
    pub tablet_id: TabletId,
    pub table_id: TableId,
    pub tablet_epoch: u64,
}

/// Successfully restored tablet state after durable boundary persistence.
#[derive(Debug)]
pub struct InstalledTabletSnapshot {
    pub pointer: TabletSnapshotPointer,
    pub state_machine: TabletStateMachine<InMemoryMvcc>,
    pub frontier: AppliedTabletFrontier,
}

/// Tablet state reconstructed from a verified immutable snapshot image.
///
/// This value is private startup state until the surrounding replica
/// constructor has also validated the Raft pointer, rebuilt the Raft core, and
/// replayed every committed entry after this frontier.
#[derive(Debug)]
pub struct RestoredTabletSnapshot {
    pub state_machine: TabletStateMachine<InMemoryMvcc>,
    pub frontier: AppliedTabletFrontier,
}

/// Restore tablet MVCC and replicated deduplication state from a snapshot whose
/// file envelope and payload checksum have already been verified by the store.
pub fn restore_verified_snapshot(
    image: &TabletSnapshotImage,
    target: &TabletSnapshotInstallTarget,
) -> Result<RestoredTabletSnapshot, TabletSnapshotInstallError> {
    validate_install_target(&image.metadata, target)?;

    let payload = snapshot_proto::TabletSnapshotPayload::decode(image.data.as_slice())
        .map_err(|error| TabletSnapshotInstallError::PayloadDecode(error.to_string()))?;

    if payload.format_version != TABLET_SNAPSHOT_PAYLOAD_VERSION {
        return Err(TabletSnapshotInstallError::UnsupportedPayloadVersion(
            payload.format_version,
        ));
    }

    let payload_table_id = TableId::from_proto(
        payload
            .table_id
            .ok_or(TabletSnapshotInstallError::MissingPayloadTableId)?,
    );

    if payload_table_id != target.table_id {
        return Err(TabletSnapshotInstallError::TableMismatch {
            expected: target.table_id,
            received: payload_table_id,
        });
    }

    let state_machine_snapshot =
        TabletStateMachineSnapshot::decode(payload.tablet_state_machine.as_slice())
            .map_err(|error| TabletSnapshotInstallError::StateMachineDecode(error.to_string()))?;

    if state_machine_snapshot.tablet_id != target.tablet_id
        || state_machine_snapshot.raft_group_id != target.raft_group_id
        || state_machine_snapshot.tablet_epoch != target.tablet_epoch
    {
        return Err(TabletSnapshotInstallError::StateMachineIdentityMismatch);
    }

    let storage = InMemoryMvcc::restore_from_snapshot_entries(
        target.table_id,
        payload.default_values,
        payload.locks,
        payload.writes,
    )
    .map_err(|error| TabletSnapshotInstallError::MvccRestore(error.to_string()))?;

    let tablet = Tablet::with_storage(target.tablet_id, target.table_id, storage)
        .map_err(|error| TabletSnapshotInstallError::TabletRestore(error.to_string()))?;

    let state_machine =
        TabletStateMachine::restore_from_snapshot(tablet, &payload.tablet_state_machine)
            .map_err(|error| TabletSnapshotInstallError::StateMachineRestore(error.to_string()))?;

    Ok(RestoredTabletSnapshot {
        state_machine,
        frontier: AppliedTabletFrontier::new(
            image.metadata.last_included_index,
            image.metadata.last_included_term,
        ),
    })
}

/// Receive, validate, publish, restore, and durably acknowledge a tablet
/// snapshot.
///
/// The persistence callback must append the snapshot pointer, `ConfState`,
/// boundary, and required Raft stable state through the exact A-WAL frontier.
/// Installation is not reported as successful until that callback succeeds.
pub fn install_incoming_snapshot<F, E>(
    store: &FileTabletSnapshotStore,
    receiver: IncomingTabletSnapshotReceiver,
    target: &TabletSnapshotInstallTarget,
    persist_boundary: F,
) -> Result<InstalledTabletSnapshot, TabletSnapshotInstallError>
where
    F: FnOnce(&TabletSnapshotPointer, AppliedTabletFrontier) -> Result<(), E>,
    E: std::fmt::Display,
{
    validate_install_target(&receiver.metadata, target)?;

    let image = receiver
        .finish()
        .map_err(TabletSnapshotInstallError::Receive)?;

    // Publish the verified immutable image before restoring live state. If
    // restoration fails, the group remains quarantined and the durable image
    // remains an unreferenced recovery artifact.
    let pointer = store
        .publish(&image)
        .map_err(TabletSnapshotInstallError::Store)?;

    let restored = restore_verified_snapshot(&image, target)?;

    persist_boundary(&pointer, restored.frontier)
        .map_err(|error| TabletSnapshotInstallError::BoundaryPersistence(error.to_string()))?;

    Ok(InstalledTabletSnapshot {
        pointer,
        state_machine: restored.state_machine,
        frontier: restored.frontier,
    })
}

fn validate_install_target(
    metadata: &TabletSnapshotMetadata,
    target: &TabletSnapshotInstallTarget,
) -> Result<(), TabletSnapshotInstallError> {
    if target.cluster_id.trim().is_empty() {
        return Err(TabletSnapshotInstallError::InvalidTarget(
            "cluster ID must not be empty",
        ));
    }

    if target.raft_group_id.0 == 0 {
        return Err(TabletSnapshotInstallError::InvalidTarget(
            "Raft group ID must be non-zero",
        ));
    }

    if target.tablet_id.0 == 0 {
        return Err(TabletSnapshotInstallError::InvalidTarget(
            "tablet ID must be non-zero",
        ));
    }

    if target.table_id.0 == 0 {
        return Err(TabletSnapshotInstallError::InvalidTarget(
            "table ID must be non-zero",
        ));
    }

    if target.tablet_epoch == 0 {
        return Err(TabletSnapshotInstallError::InvalidTarget(
            "tablet epoch must be non-zero",
        ));
    }

    if metadata.cluster_id != target.cluster_id {
        return Err(TabletSnapshotInstallError::TargetClusterMismatch {
            expected: target.cluster_id.clone(),
            received: metadata.cluster_id.clone(),
        });
    }

    if metadata.raft_group_id != target.raft_group_id {
        return Err(TabletSnapshotInstallError::TargetGroupMismatch {
            expected: target.raft_group_id,
            received: metadata.raft_group_id,
        });
    }

    if metadata.tablet_id != target.tablet_id {
        return Err(TabletSnapshotInstallError::TargetTabletMismatch {
            expected: target.tablet_id,
            received: metadata.tablet_id,
        });
    }

    if metadata.tablet_epoch != target.tablet_epoch {
        return Err(TabletSnapshotInstallError::TargetEpochMismatch {
            expected: target.tablet_epoch,
            received: metadata.tablet_epoch,
        });
    }

    Ok(())
}

fn incoming_io_error(error: io::Error) -> TabletSnapshotReceiveError {
    TabletSnapshotReceiveError::Io(error.to_string())
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TabletSnapshotReceiveError {
    #[error("incoming snapshot metadata is invalid: {0}")]
    Metadata(#[from] TabletSnapshotMetadataError),

    #[error("incoming snapshot chunk limit must be non-zero")]
    InvalidChunkLimit,

    #[error("incoming snapshot payload is too large: {actual} > {limit}")]
    PayloadTooLarge { actual: u64, limit: u64 },

    #[error("incoming snapshot chunk is too large: {actual} > {limit}")]
    ChunkTooLarge { actual: u64, limit: u64 },

    #[error(
        "incoming snapshot chunk exceeds declared length: received {received}, chunk {chunk}, total {total}"
    )]
    ChunkExceedsDeclaredLength {
        received: u64,
        chunk: u64,
        total: u64,
    },

    #[error("incoming snapshot length overflowed u64")]
    LengthOverflow,

    #[error("incoming snapshot receiver is already closed")]
    ReceiverClosed,

    #[error("incoming snapshot is incomplete: received {received}, expected {expected}")]
    Incomplete { received: u64, expected: u64 },

    #[error(
        "incoming snapshot temporary file length mismatch: expected {expected}, actual {actual}"
    )]
    TemporaryFileLengthMismatch { expected: u64, actual: u64 },

    #[error("incoming snapshot temporary file I/O failed: {0}")]
    Io(String),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TabletSnapshotInstallError {
    #[error("incoming snapshot receive failed: {0}")]
    Receive(#[from] TabletSnapshotReceiveError),

    #[error("incoming snapshot publication failed: {0}")]
    Store(#[from] TabletSnapshotStoreError),

    #[error("incoming snapshot target is invalid: {0}")]
    InvalidTarget(&'static str),

    #[error("incoming snapshot cluster mismatch: expected {expected}, received {received}")]
    TargetClusterMismatch { expected: String, received: String },

    #[error("incoming snapshot Raft group mismatch: expected {expected:?}, received {received:?}")]
    TargetGroupMismatch {
        expected: RaftGroupId,
        received: RaftGroupId,
    },

    #[error("incoming snapshot tablet mismatch: expected {expected:?}, received {received:?}")]
    TargetTabletMismatch {
        expected: TabletId,
        received: TabletId,
    },

    #[error("incoming snapshot tablet epoch mismatch: expected {expected}, received {received}")]
    TargetEpochMismatch { expected: u64, received: u64 },

    #[error("incoming snapshot payload decode failed: {0}")]
    PayloadDecode(String),

    #[error("unsupported incoming snapshot payload version {0}")]
    UnsupportedPayloadVersion(u32),

    #[error("incoming snapshot payload is missing its table ID")]
    MissingPayloadTableId,

    #[error("incoming snapshot table mismatch: expected {expected:?}, received {received:?}")]
    TableMismatch {
        expected: TableId,
        received: TableId,
    },

    #[error("incoming state-machine snapshot decode failed: {0}")]
    StateMachineDecode(String),

    #[error("incoming state-machine identity does not match the install target")]
    StateMachineIdentityMismatch,

    #[error("incoming MVCC restore failed: {0}")]
    MvccRestore(String),

    #[error("incoming tablet reconstruction failed: {0}")]
    TabletRestore(String),

    #[error("incoming state-machine restore failed: {0}")]
    StateMachineRestore(String),

    #[error("incoming snapshot boundary persistence failed: {0}")]
    BoundaryPersistence(String),
}
