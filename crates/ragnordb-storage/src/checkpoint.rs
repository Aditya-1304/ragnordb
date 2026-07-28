//! versioned database snapshot file format
//!
//! currently snapshots are immutable recovery images. A fixed-width envelope
//! makes each file self identifying before protobuf decoding and protects the
//! complete logical body with CRC32C. Filesystem publication uses a synchronized
//! temporary file and atomic rename. Consistent-cut capture remains separate

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
};

use prost::Message;
use ragnordb_catalog::TableSchema;
use ragnordb_common::{
    Error, Result, catalog_codec::TableDefinition, proto::snapshot as snapshot_proto,
};

/// stable magic prefix identifying a RagnorDB database snapshot file
pub const SNAPSHOT_FILE_MAGIC: [u8; 8] = *b"RGNRSNP\0";

/// current version of both the file envelope and its protobuf body contract
pub const SNAPSHOT_FILE_VERSION: u32 = 1;

/// v1 envelope: magic, version, encoded body length, and body CRC32C
const SNAPSHOT_HEADER_LENGTH: usize = 8 + 4 + 8 + 4;

/// Data-directory child containing immutable database snapshot files.
pub const SNAPSHOT_DIRECTORY_NAME: &str = "snapshots";

/// a snapshot file that completed file sync, atomic rename, and directory sync
#[must_use = "published snapshot metadata is required by WAL pointer publication"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedSnapshotFile {
    /// portable path stored in the later `SnapshotPointer` WAL record
    pub relative_path: String,

    /// complete encoded file length used by diagnostics and restore validation
    pub file_length: u64,
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
        relative_path: format!("{SNAPSHOT_DIRECTORY_NAME}/{final_name}"),
        file_length,
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

    if high_water.max_transaction_id.is_none() {
        return Err(invalid("maximum transaction ID is missing".to_string()));
    }

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
