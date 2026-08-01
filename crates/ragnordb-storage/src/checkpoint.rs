//! versioned database snapshot file format
//!
//! currently snapshots are immutable recovery images. A fixed-width envelope
//! makes each file self identifying before protobuf decoding and protects the
//! complete logical body with CRC32C. Filesystem publication uses a synchronized
//! temporary file and atomic rename. Consistent-cut capture remains separate

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

use prost::Message;
use ragnordb_catalog::TableSchema;
use ragnordb_common::{
    Error, Result,
    catalog_codec::TableDefinition,
    ids::{TableId, Timestamp},
    proto::snapshot as snapshot_proto,
};
use wal::lsn::Lsn;

use crate::{
    mvcc::InMemoryMvcc,
    wal::{CheckpointMarker, DurableCheckpointLog, DurableWalExtent, SnapshotPointer},
};

/// stable magic prefix identifying a RagnorDB database snapshot file
pub const SNAPSHOT_FILE_MAGIC: [u8; 8] = *b"RGNRSNP\0";

/// current version of both the file envelope and its protobuf body contract
pub const SNAPSHOT_FILE_VERSION: u32 = 1;

/// v1 envelope: magic, version, encoded body length, and body CRC32C
const SNAPSHOT_HEADER_LENGTH: usize = 8 + 4 + 8 + 4;

/// Data-directory child containing immutable database snapshot files.
pub const SNAPSHOT_DIRECTORY_NAME: &str = "snapshots";

/// maximum snapshot file accepted during publication or recovery
///
/// the initial 256 MiB limit keeps startup memory bounded until snapshot
/// decoding becomes streaming
pub const MAX_SNAPSHOT_FILE_BYTES: u64 = 256 * 1024 * 1024;

/// maximum number of tables accepted from one snapshot
pub const MAX_SNAPSHOT_TABLES: usize = 4_096;

/// maximum total MVCC entries accepted from one snapshot
pub const MAX_SNAPSHOT_ENTRIES: usize = 10_000_000;

/// maximum encoded row-key size accepted from snapshot state
pub const MAX_SNAPSHOT_KEY_BYTES: usize = 1024 * 1024;

/// maximum canonical encoded row size accepted from snapshot state
pub const MAX_SNAPSHOT_ROW_BYTES: usize = 16 * 1024 * 1024;
/// a snapshot file that completed file sync, atomic rename, and directory sync
#[must_use = "published snapshot metadata is required by WAL pointer publication"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedSnapshotFile {
    snapshot_id: u64,
    snapshot_timestamp: Timestamp,
    replay_from_lsn: Lsn,
    relative_path: String,
    table_ids: BTreeSet<TableId>,
    file_length: u64,
    file_checksum_crc32c: u32,
    snapshot_format_version: u32,
}

impl PublishedSnapshotFile {
    /// stable snapshot identity represented by the durable file
    pub const fn snapshot_id(&self) -> u64 {
        self.snapshot_id
    }

    /// portable path stored in the subsequent `SnapshotPointer`
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    /// complete encoded file length used by diagnostics and restore validation
    pub const fn file_length(&self) -> u64 {
        self.file_length
    }

    /// CRC32C covering the complete immutable snapshot file
    pub const fn file_checksum_crc32c(&self) -> u32 {
        self.file_checksum_crc32c
    }

    /// version of the snapshot file envelope.
    pub const fn snapshot_format_version(&self) -> u32 {
        self.snapshot_format_version
    }
}

/// checkpoint whose snapshot pointer and marker are both durably published
///
/// the replay frontier becomes eligible for retention only when this value is
/// returned. Actual segment pruning remains owned by the later retention phase
#[must_use = "only a published checkpoint permits WAL retention to advance"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishedCheckpoint {
    /// stable identity shared by the snapshot file and both WAL records
    pub snapshot_id: u64,

    /// first WAL position that recovery must replay after restoring the file
    pub replay_from_lsn: Lsn,

    /// durable extent occupied by the snapshot pointer
    pub pointer_extent: DurableWalExtent,

    /// durable extent occupied by the checkpoint marker
    pub marker_extent: DurableWalExtent,
}
/// detached MVCC image captured from one in memory table
///
/// all collections preserve the underlying `BTreeMap` order. The value owns
/// every encoded key, row, lock, and write record, so later commits cannot
/// mutate a checkpoint image after the serialized capture barrier is released
#[derive(Debug, Clone, PartialEq)]
pub struct CapturedMvccState {
    default_values: Vec<snapshot_proto::DefaultValueEntry>,
    locks: Vec<snapshot_proto::LockEntry>,
    writes: Vec<snapshot_proto::WriteEntry>,
}

impl CapturedMvccState {
    /// a detached MVCC image from deterministically ordered entries
    pub(crate) fn new(
        default_values: Vec<snapshot_proto::DefaultValueEntry>,
        locks: Vec<snapshot_proto::LockEntry>,
        writes: Vec<snapshot_proto::WriteEntry>,
    ) -> Self {
        Self {
            default_values,
            locks,
            writes,
        }
    }

    /// attach the catalog definition captured under the same database barrier
    pub fn into_snapshot_table(self, definition: TableDefinition) -> snapshot_proto::SnapshotTable {
        snapshot_proto::SnapshotTable {
            definition: Some(definition.to_proto()),
            default_values: self.default_values,
            locks: self.locks,
            writes: self.writes,
        }
    }
}

/// durably publish a captured snapshot beneath the database data directory
///
/// publication uses a temporary file in the same directory as the final file,
/// making the rename atomic on the target filesystem. Success means:
///
/// 1. every encoded byte was written to the temporary file,
/// 2. the temporary file was synchronized,
/// 3. it was atomically renamed to the final path,
/// 4. the snapshot directory was synchronized
///
/// no WAL pointer or checkpoint marker is appended here. A returned value is
/// only the durable-file prerequisite consumed by the next publication slice
pub fn publish_snapshot_file(
    data_dir: impl AsRef<Path>,
    snapshot: &snapshot_proto::DatabaseSnapshot,
) -> Result<PublishedSnapshotFile> {
    // validate and encode before touching the filesystem. Invalid caller state
    // must not leave a temporary file that resembles publication progress
    let bytes = encode_snapshot_file(snapshot)?;
    let file_length = u64::try_from(bytes.len()).map_err(|_| Error::SnapshotPublicationFailed {
        reason: "encoded snapshot length does not fit in u64".to_string(),
    })?;

    if file_length > MAX_SNAPSHOT_FILE_BYTES {
        return Err(Error::SnapshotPublicationFailed {
            reason: format!(
                "encoded snapshot length {file_length} exceeds maximum \
                 {MAX_SNAPSHOT_FILE_BYTES}"
            ),
        });
    }

    let file_checksum_crc32c = crc32c::crc32c(&bytes);
    let snapshot_timestamp = snapshot
        .snapshot_timestamp
        .as_ref()
        .cloned()
        .map(Timestamp::from_proto)
        .ok_or_else(|| {
            Error::InvalidArgument("invalid snapshot: snapshot timestamp is missing".to_string())
        })?;
    let table_ids = snapshot
        .tables
        .iter()
        .map(|table| {
            table
                .definition
                .as_ref()
                .map(|definition| TableId(definition.table_id))
                .ok_or_else(|| {
                    Error::InvalidArgument(
                        "invalid snapshot: snapshot table definition is missing".to_string(),
                    )
                })
        })
        .collect::<Result<BTreeSet<_>>>()?;

    let data_dir = data_dir.as_ref();
    let snapshot_dir = data_dir.join(SNAPSHOT_DIRECTORY_NAME);

    fs::create_dir_all(&snapshot_dir).map_err(|source| {
        publication_io_error("create snapshot directory", &snapshot_dir, source)
    })?;

    // persist creation of the snapshot directory itself. This is harmless when
    // the directory already existed and closes the crash window when it did not
    sync_directory(data_dir)?;

    let final_name = format!("snapshot-{}.ragnor", snapshot.snapshot_id);
    let temporary_name = format!(".snapshot-{}.ragnor.tmp", snapshot.snapshot_id);
    let final_path = snapshot_dir.join(&final_name);
    let temporary_path = snapshot_dir.join(temporary_name);
    let mut temporary_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary_path)
        .map_err(|source| {
            publication_io_error("open temporary snapshot", &temporary_path, source)
        })?;

    temporary_file.write_all(&bytes).map_err(|source| {
        publication_io_error("write temporary snapshot", &temporary_path, source)
    })?;
    temporary_file.sync_all().map_err(|source| {
        publication_io_error("sync temporary snapshot", &temporary_path, source)
    })?;

    // close the writer before rename so publication behaves consistently on
    // platforms that do not permit renaming an open file
    drop(temporary_file);

    fs::rename(&temporary_path, &final_path).map_err(|source| {
        publication_io_error("rename snapshot into place", &final_path, source)
    })?;
    sync_directory(&snapshot_dir)?;

    Ok(PublishedSnapshotFile {
        snapshot_id: snapshot.snapshot_id,
        snapshot_timestamp,
        replay_from_lsn: Lsn::new(snapshot.replay_from_lsn),
        relative_path: format!("{SNAPSHOT_DIRECTORY_NAME}/{final_name}"),
        table_ids,
        file_length,
        file_checksum_crc32c,
        snapshot_format_version: SNAPSHOT_FILE_VERSION,
    })
}

/// publish the WAL metadata that makes one durable snapshot a checkpoint
///
/// both records are derived from the immutable metadata captured in
/// `PublishedSnapshotFile`, so the marker cannot drift from its pointer
/// `DurableCheckpointLog` guarantees pointer-before-marker synchronization
pub fn publish_checkpoint<L>(
    log: &L,
    snapshot_file: &PublishedSnapshotFile,
) -> Result<PublishedCheckpoint>
where
    L: DurableCheckpointLog + ?Sized,
{
    let pointer = SnapshotPointer {
        snapshot_id: snapshot_file.snapshot_id,
        snapshot_timestamp: snapshot_file.snapshot_timestamp,
        replay_from_lsn: snapshot_file.replay_from_lsn,
        relative_path: snapshot_file.relative_path.clone(),
        table_ids: snapshot_file.table_ids.clone(),
        file_length: snapshot_file.file_length,
        file_checksum_crc32c: snapshot_file.file_checksum_crc32c,
        snapshot_format_version: snapshot_file.snapshot_format_version,
    };
    let marker = CheckpointMarker {
        snapshot_id: snapshot_file.snapshot_id,
        snapshot_timestamp: snapshot_file.snapshot_timestamp,
        replay_from_lsn: snapshot_file.replay_from_lsn,
    };
    let durable = log.append_checkpoint_records(&pointer, &marker)?;

    Ok(PublishedCheckpoint {
        snapshot_id: snapshot_file.snapshot_id,
        replay_from_lsn: snapshot_file.replay_from_lsn,
        pointer_extent: durable.pointer_extent,
        marker_extent: durable.marker_extent,
    })
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| publication_io_error("sync directory", path, source))
}

fn publication_io_error(operation: &str, path: &Path, source: std::io::Error) -> Error {
    Error::SnapshotPublicationFailed {
        reason: format!("{operation} at {}: {source}", path.display()),
    }
}

/// encode one validated snapshot body as a self-identifying checksummed file
///
/// the caller still owns consistent-cut capture. This function only freezes the
/// supplied immutable body into its durable byte representation
pub fn encode_snapshot_file(snapshot: &snapshot_proto::DatabaseSnapshot) -> Result<Vec<u8>> {
    validate_snapshot(snapshot, SnapshotValidationContext::Caller)?;

    let body = snapshot.encode_to_vec();
    let body_length = u64::try_from(body.len()).map_err(|_| {
        Error::InvalidArgument("snapshot body length does not fit in u64".to_string())
    })?;
    let file_length = SNAPSHOT_HEADER_LENGTH
        .checked_add(body.len())
        .ok_or_else(|| Error::InvalidArgument("snapshot file length overflow".to_string()))?;
    let mut file = Vec::with_capacity(file_length);

    file.extend_from_slice(&SNAPSHOT_FILE_MAGIC);
    file.extend_from_slice(&SNAPSHOT_FILE_VERSION.to_le_bytes());
    file.extend_from_slice(&body_length.to_le_bytes());
    file.extend_from_slice(&crc32c::crc32c(&body).to_le_bytes());
    file.extend_from_slice(&body);

    Ok(file)
}

/// decode and validate one complete snapshot file
///
/// envelope checks run before protobuf decoding so arbitrary, truncated, or
/// corrupted files cannot be mistaken for database state
pub fn decode_snapshot_file(file: &[u8]) -> Result<snapshot_proto::DatabaseSnapshot> {
    let file_length = u64::try_from(file.len())
        .map_err(|_| corrupt_snapshot("snapshot length does not fit in u64"))?;

    if file_length > MAX_SNAPSHOT_FILE_BYTES {
        return Err(corrupt_snapshot(format!(
            "snapshot file length {file_length} exceeds maximum \
             {MAX_SNAPSHOT_FILE_BYTES}"
        )));
    }

    if file.len() < SNAPSHOT_HEADER_LENGTH {
        return Err(corrupt_snapshot(format!(
            "file is {} bytes; V1 header requires {SNAPSHOT_HEADER_LENGTH} bytes",
            file.len()
        )));
    }

    if file[..SNAPSHOT_FILE_MAGIC.len()] != SNAPSHOT_FILE_MAGIC {
        return Err(corrupt_snapshot("snapshot magic does not match RagnorDB"));
    }

    let version = read_u32_le(file, 8);

    if version != SNAPSHOT_FILE_VERSION {
        return Err(corrupt_snapshot(format!(
            "unsupported snapshot file version {version}; expected {SNAPSHOT_FILE_VERSION}"
        )));
    }

    let declared_body_length = read_u64_le(file, 12);
    let body_length = usize::try_from(declared_body_length).map_err(|_| {
        corrupt_snapshot(format!(
            "declared body length {declared_body_length} does not fit this platform"
        ))
    })?;
    let expected_file_length = SNAPSHOT_HEADER_LENGTH
        .checked_add(body_length)
        .ok_or_else(|| corrupt_snapshot("declared snapshot length overflows usize"))?;

    if file.len() != expected_file_length {
        return Err(corrupt_snapshot(format!(
            "file length {} does not match declared length {expected_file_length}",
            file.len()
        )));
    }

    let expected_checksum = read_u32_le(file, 20);
    let body = &file[SNAPSHOT_HEADER_LENGTH..];
    let actual_checksum = crc32c::crc32c(body);

    if actual_checksum != expected_checksum {
        return Err(corrupt_snapshot(format!(
            "snapshot body checksum mismatch: stored {expected_checksum:#010x}, \
             computed {actual_checksum:#010x}"
        )));
    }

    let snapshot = snapshot_proto::DatabaseSnapshot::decode(body).map_err(|error| {
        corrupt_snapshot(format!("failed to decode snapshot protobuf body: {error}"))
    })?;

    validate_snapshot(&snapshot, SnapshotValidationContext::Durable)?;

    Ok(snapshot)
}

/// Read and validate a selected snapshot without trusting its declared size.
///
/// Recovery checks the pointer-bound length and format before allocating the
/// protobuf body. The whole-file checksum is checked before semantic decoding,
/// proving that the file is the exact immutable object originally published by
/// the checkpoint.
pub fn load_snapshot_file_bounded(
    path: impl AsRef<Path>,
    expected_file_length: u64,
    expected_file_checksum_crc32c: u32,
    expected_format_version: u32,
) -> Result<snapshot_proto::DatabaseSnapshot> {
    let path = path.as_ref();

    if expected_format_version != SNAPSHOT_FILE_VERSION {
        return Err(corrupt_snapshot(format!(
            "pointer requires snapshot format version {expected_format_version}; \
             this binary supports {SNAPSHOT_FILE_VERSION}"
        )));
    }

    if expected_file_length > MAX_SNAPSHOT_FILE_BYTES {
        return Err(corrupt_snapshot(format!(
            "pointer declares snapshot length {expected_file_length}, exceeding \
             maximum {MAX_SNAPSHOT_FILE_BYTES}"
        )));
    }

    if expected_file_length < SNAPSHOT_HEADER_LENGTH as u64 {
        return Err(corrupt_snapshot(format!(
            "pointer declares snapshot length {expected_file_length}; header \
             requires {SNAPSHOT_HEADER_LENGTH} bytes"
        )));
    }

    let mut file = File::open(path).map_err(|source| Error::RecoveryFailed {
        reason: format!(
            "failed to open selected snapshot {}: {source}",
            path.display()
        ),
    })?;

    let actual_file_length = file
        .metadata()
        .map_err(|source| Error::RecoveryFailed {
            reason: format!(
                "failed to read selected snapshot metadata {}: {source}",
                path.display()
            ),
        })?
        .len();

    if actual_file_length != expected_file_length {
        return Err(corrupt_snapshot(format!(
            "selected snapshot {} length {actual_file_length} does not match \
             pointer length {expected_file_length}",
            path.display()
        )));
    }

    let mut header = [0u8; SNAPSHOT_HEADER_LENGTH];

    file.read_exact(&mut header)
        .map_err(|source| Error::RecoveryFailed {
            reason: format!(
                "failed to read selected snapshot header {}: {source}",
                path.display()
            ),
        })?;

    if header[..SNAPSHOT_FILE_MAGIC.len()] != SNAPSHOT_FILE_MAGIC {
        return Err(corrupt_snapshot(
            "selected snapshot magic does not match RagnorDB",
        ));
    }

    let file_version = read_u32_le(&header, 8);

    if file_version != expected_format_version {
        return Err(corrupt_snapshot(format!(
            "selected snapshot file version {file_version} does not match \
             pointer version {expected_format_version}"
        )));
    }

    let declared_body_length = read_u64_le(&header, 12);
    let declared_file_length = (SNAPSHOT_HEADER_LENGTH as u64)
        .checked_add(declared_body_length)
        .ok_or_else(|| corrupt_snapshot("declared snapshot length overflows u64"))?;

    if declared_file_length != expected_file_length {
        return Err(corrupt_snapshot(format!(
            "selected snapshot header declares {declared_file_length} bytes, \
             but pointer requires {expected_file_length}"
        )));
    }

    let body_length = usize::try_from(declared_body_length).map_err(|_| {
        corrupt_snapshot(format!(
            "declared snapshot body length {declared_body_length} does not fit this platform"
        ))
    })?;

    let file_capacity = usize::try_from(expected_file_length).map_err(|_| {
        corrupt_snapshot(format!(
            "snapshot file length {expected_file_length} does not fit this platform"
        ))
    })?;

    let mut bytes = Vec::with_capacity(file_capacity);
    bytes.extend_from_slice(&header);

    let mut body = vec![0u8; body_length];

    file.read_exact(&mut body)
        .map_err(|source| Error::RecoveryFailed {
            reason: format!(
                "failed to read selected snapshot body {}: {source}",
                path.display()
            ),
        })?;

    bytes.extend_from_slice(&body);

    let mut trailing = [0u8; 1];
    let trailing_bytes = file
        .read(&mut trailing)
        .map_err(|source| Error::RecoveryFailed {
            reason: format!(
                "failed while checking selected snapshot tail {}: {source}",
                path.display()
            ),
        })?;

    if trailing_bytes != 0 {
        return Err(corrupt_snapshot(format!(
            "selected snapshot {} grew while it was being read",
            path.display()
        )));
    }

    let actual_file_checksum_crc32c = crc32c::crc32c(&bytes);

    if actual_file_checksum_crc32c != expected_file_checksum_crc32c {
        return Err(corrupt_snapshot(format!(
            "selected snapshot whole-file checksum mismatch: pointer \
             {expected_file_checksum_crc32c:#010x}, file \
             {actual_file_checksum_crc32c:#010x}"
        )));
    }

    decode_snapshot_file(&bytes)
}

fn validate_snapshot_resource_limits(
    snapshot: &snapshot_proto::DatabaseSnapshot,
) -> std::result::Result<(), String> {
    if snapshot.tables.len() > MAX_SNAPSHOT_TABLES {
        return Err(format!(
            "snapshot contains {} tables; maximum is {}",
            snapshot.tables.len(),
            MAX_SNAPSHOT_TABLES
        ));
    }

    let mut total_entries = 0usize;

    for table in &snapshot.tables {
        total_entries = total_entries
            .checked_add(table.default_values.len())
            .and_then(|count| count.checked_add(table.locks.len()))
            .and_then(|count| count.checked_add(table.writes.len()))
            .ok_or_else(|| "snapshot MVCC entry count overflow".to_string())?;

        if total_entries > MAX_SNAPSHOT_ENTRIES {
            return Err(format!(
                "snapshot contains more than {MAX_SNAPSHOT_ENTRIES} MVCC entries"
            ));
        }

        for entry in &table.default_values {
            if entry.key.len() > MAX_SNAPSHOT_KEY_BYTES {
                return Err(format!(
                    "snapshot default-value key is {} bytes; maximum is {}",
                    entry.key.len(),
                    MAX_SNAPSHOT_KEY_BYTES
                ));
            }

            if entry.row.len() > MAX_SNAPSHOT_ROW_BYTES {
                return Err(format!(
                    "snapshot row is {} bytes; maximum is {}",
                    entry.row.len(),
                    MAX_SNAPSHOT_ROW_BYTES
                ));
            }
        }

        for entry in &table.locks {
            if entry.key.len() > MAX_SNAPSHOT_KEY_BYTES {
                return Err(format!(
                    "snapshot lock key is {} bytes; maximum is {}",
                    entry.key.len(),
                    MAX_SNAPSHOT_KEY_BYTES
                ));
            }
        }

        for entry in &table.writes {
            if entry.key.len() > MAX_SNAPSHOT_KEY_BYTES {
                return Err(format!(
                    "snapshot write key is {} bytes; maximum is {}",
                    entry.key.len(),
                    MAX_SNAPSHOT_KEY_BYTES
                ));
            }
        }
    }

    Ok(())
}

#[derive(Clone, Copy)]
enum SnapshotValidationContext {
    Caller,
    Durable,
}

fn validate_snapshot(
    snapshot: &snapshot_proto::DatabaseSnapshot,
    context: SnapshotValidationContext,
) -> Result<()> {
    let invalid = |message: String| match context {
        SnapshotValidationContext::Caller => {
            Error::InvalidArgument(format!("invalid snapshot: {message}"))
        }
        SnapshotValidationContext::Durable => {
            corrupt_snapshot(format!("invalid snapshot body: {message}"))
        }
    };

    if let Err(message) = validate_snapshot_resource_limits(snapshot) {
        return Err(invalid(message));
    }

    if snapshot.snapshot_id == 0 {
        return Err(invalid("snapshot ID 0 is reserved".to_string()));
    }

    let snapshot_timestamp = snapshot
        .snapshot_timestamp
        .as_ref()
        .ok_or_else(|| invalid("snapshot timestamp is missing".to_string()))?;

    if snapshot_timestamp.id == 0 {
        return Err(invalid("snapshot timestamp 0 is reserved".to_string()));
    }

    let high_water = snapshot
        .high_water_marks
        .as_ref()
        .ok_or_else(|| invalid("allocator high-water marks are missing".to_string()))?;
    let max_timestamp = high_water
        .max_timestamp
        .as_ref()
        .ok_or_else(|| invalid("maximum timestamp is missing".to_string()))?;
    let max_table_id = high_water
        .max_table_id
        .as_ref()
        .ok_or_else(|| invalid("maximum table ID is missing".to_string()))?;

    let max_transaction_id = high_water
        .max_transaction_id
        .as_ref()
        .ok_or_else(|| invalid("maximum transaction ID is missing".to_string()))?;

    if high_water.max_snapshot_id < snapshot.snapshot_id {
        return Err(invalid(format!(
            "snapshot-ID high-water mark {} is below snapshot ID {}",
            high_water.max_snapshot_id, snapshot.snapshot_id
        )));
    }

    if max_timestamp.id < snapshot_timestamp.id {
        return Err(invalid(format!(
            "timestamp high-water mark {} is below snapshot timestamp {}",
            max_timestamp.id, snapshot_timestamp.id
        )));
    }

    let mut table_ids = BTreeSet::new();

    for table in &snapshot.tables {
        let definition = table
            .definition
            .clone()
            .ok_or_else(|| invalid("captured table definition is missing".to_string()))?;
        let definition = TableDefinition::from_proto(definition)
            .map_err(|message| invalid(format!("invalid table definition: {message}")))?;
        let schema = TableSchema::from_definition(definition)
            .map_err(|error| invalid(format!("invalid table definition: {error}")))?;

        if !table_ids.insert(schema.id.0) {
            return Err(invalid(format!(
                "snapshot contains duplicate table ID {}",
                schema.id.0
            )));
        }

        if max_table_id.id < schema.id.0 {
            return Err(invalid(format!(
                "table-ID high-water mark {} is below captured table ID {}",
                max_table_id.id, schema.id.0
            )));
        }

        let restored = InMemoryMvcc::from_snapshot_table(schema.id, table)
            .map_err(|source| invalid(format!("invalid table MVCC state: {source}")))?;

        if max_transaction_id.id < restored.max_transaction_id.0 {
            return Err(invalid(format!(
                "transaction-ID high-water mark {} is below captured lock \
                 transaction ID {}",
                max_transaction_id.id, restored.max_transaction_id.0
            )));
        }

        if max_timestamp.id < restored.max_timestamp.0 {
            return Err(invalid(format!(
                "timestamp high-water mark {} is below captured MVCC timestamp {}",
                max_timestamp.id, restored.max_timestamp.0
            )));
        }

        if snapshot_timestamp.id < restored.max_timestamp.0 {
            return Err(invalid(format!(
                "snapshot timestamp {} is below captured MVCC timestamp {}",
                snapshot_timestamp.id, restored.max_timestamp.0
            )));
        }
    }

    Ok(())
}

fn read_u32_le(bytes: &[u8], offset: usize) -> u32 {
    let mut encoded = [0_u8; 4];
    encoded.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_le_bytes(encoded)
}

fn read_u64_le(bytes: &[u8], offset: usize) -> u64 {
    let mut encoded = [0_u8; 8];
    encoded.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_le_bytes(encoded)
}

fn corrupt_snapshot(message: impl std::fmt::Display) -> Error {
    Error::CorruptData(format!("invalid database snapshot: {message}"))
}
