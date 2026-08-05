//! tablet snapshot metadata and immutable file publication
//!
//! this module owns the database-specific snapshot contract. Raft remains
//! responsible for consensus metadata and log mechanics; this module binds a
//! snapshot image to the exact cluster, tablet generation, configuration,
//! applied boundary, and payload checksum that produced it

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::PathBuf,
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use prost::Message;
use ragnordb_common::{
    ids::{RaftGroupId, ReplicaId, TabletId},
    proto::{ids as id_proto, snapshot as snapshot_proto},
};

pub const TABLET_SNAPSHOT_METADATA_VERSION: u32 = 1;
pub const TABLET_SNAPSHOT_STORAGE_FORMAT_VERSION: u32 = 1;

const MAX_CLUSTER_ID_BYTES: usize = 256;

static NEXT_TEMP_FILE_ID: AtomicU64 = AtomicU64::new(1);

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
        cluster_id: impl Into<String>,
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
        tablet_id: TabletId,
        tablet_epoch: u64,
        snapshot_id: u64,
        last_included_index: u64,
        last_included_term: u64,
        conf_state: TabletSnapshotConfState,
        payload: &[u8],
    ) -> Result<Self, TabletSnapshotMetadataError> {
        let total_length = u64::try_from(payload.len())
            .map_err(|_| TabletSnapshotMetadataError::PayloadLengthOverflow)?;

        let metadata = Self {
            format_version: TABLET_SNAPSHOT_METADATA_VERSION,
            cluster_id: cluster_id.into(),
            raft_group_id,
            replica_id,
            tablet_id,
            tablet_epoch,
            snapshot_id,
            last_included_index,
            last_included_term,
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

        Ok(Self {
            root,
            max_file_bytes,
        })
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

        self.root
            .join(format!(".{file_name}.{}.{}.tmp", process::id(), sequence))
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
}
