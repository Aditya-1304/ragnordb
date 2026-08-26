//! Exactly-once bootstrap coordination for local Raft group replicas.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use ragnordb_common::{
    ids::RaftGroupId,
    raft_bootstrap::{RaftGroupBootstrap, RaftGroupBootstrapError},
};

/// Result of an atomic bootstrap installation attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapStoreInstall {
    /// The record was newly installed and synchronized.
    Installed,
    /// A record already existed and must be reconciled with the request.
    AlreadyExists(Vec<u8>),
}

/// Durable storage boundary for exactly-once group bootstrap.
///
/// `install_bootstrap_and_sync` atomically installs only when absent and does
/// not return `Installed` until the record is durable. If durability cannot be
/// established, the implementation returns `OutcomeUnknown`; the live host
/// enters recovery instead of retrying the installation.
pub trait BootstrapStore {
    fn load_bootstrap(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<Option<Vec<u8>>, BootstrapStoreError>;

    fn install_bootstrap_and_sync(
        &mut self,
        raft_group_id: RaftGroupId,
        encoded_bootstrap: &[u8],
    ) -> Result<BootstrapStoreInstall, BootstrapStoreError>;
}

/// Filesystem-backed exactly-once bootstrap authority used at node startup.
///
/// Installation writes and synchronizes a private temporary file, atomically
/// links it into the group namespace only when absent, and synchronizes the
/// directory before returning `Installed`. A directory-sync failure is outcome
/// unknown because the final name may survive restart.
#[derive(Debug)]
pub struct FileBootstrapStore {
    directory: PathBuf,
    next_temporary_id: AtomicU64,
}

impl FileBootstrapStore {
    pub fn open(directory: impl Into<PathBuf>) -> Result<Self, BootstrapStoreError> {
        let directory = directory.into();
        fs::create_dir_all(&directory).map_err(|error| {
            BootstrapStoreError::Unavailable(format!(
                "create bootstrap directory {}: {error}",
                directory.display()
            ))
        })?;
        cleanup_temporary_files(&directory).map_err(|error| {
            BootstrapStoreError::Unavailable(format!(
                "clean bootstrap temporary files in {}: {error}",
                directory.display()
            ))
        })?;
        sync_directory(&directory).map_err(|error| {
            BootstrapStoreError::Unavailable(format!(
                "synchronize bootstrap directory {}: {error}",
                directory.display()
            ))
        })?;
        Ok(Self {
            directory,
            next_temporary_id: AtomicU64::new(temporary_id_seed()),
        })
    }

    fn final_path(&self, raft_group_id: RaftGroupId) -> PathBuf {
        self.directory
            .join(format!("raft-group-{}.bootstrap", raft_group_id.0))
    }

    fn read_existing(&self, path: &Path) -> Result<Option<Vec<u8>>, BootstrapStoreError> {
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(BootstrapStoreError::Unavailable(format!(
                    "open bootstrap {}: {error}",
                    path.display()
                )));
            }
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|error| {
            BootstrapStoreError::Unavailable(format!("read bootstrap {}: {error}", path.display()))
        })?;
        Ok(Some(bytes))
    }

    /// discover every authoritative Raft-group bootstrap in this node's
    /// bootstrap directory
    ///
    /// temporary files are never considered authoritative. Every final file is
    /// decoded and its embedded group identity must agree with its filename
    /// Startup fails closed on malformed authoritative files
    pub fn load_all_durable_bootstraps(
        &self,
    ) -> Result<BTreeMap<RaftGroupId, RaftGroupBootstrap>, BootstrapGroupError> {
        let mut bootstraps = BTreeMap::new();

        let entries = fs::read_dir(&self.directory).map_err(|error| {
            BootstrapStoreError::Unavailable(format!(
                "read bootstrap directory {}: {error}",
                self.directory.display()
            ))
        })?;

        for entry in entries {
            let entry = entry.map_err(|error| {
                BootstrapStoreError::Unavailable(format!(
                    "enumerate bootstrap directory {}: {error}",
                    self.directory.display()
                ))
            })?;

            if !entry
                .file_type()
                .map_err(|error| {
                    BootstrapStoreError::Unavailable(format!(
                        "inspect bootstrap entry {}: {error}",
                        entry.path().display()
                    ))
                })?
                .is_file()
            {
                continue;
            }

            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };

            if is_temporary_bootstrap_name(file_name) {
                continue;
            }

            let Some(group_text) = file_name
                .strip_prefix("raft-group-")
                .and_then(|name| name.strip_suffix(".bootstrap"))
            else {
                continue;
            };

            let raw_group_id = group_text.parse::<u64>().map_err(|_| {
                BootstrapStoreError::Unavailable(format!(
                    "invalid authoritative bootstrap filename: {file_name}"
                ))
            })?;

            let filename_group_id = RaftGroupId(raw_group_id);

            let bytes = self.read_existing(&entry.path())?.ok_or_else(|| {
                BootstrapStoreError::Unavailable(format!(
                    "bootstrap disappeared during discovery: {}",
                    entry.path().display()
                ))
            })?;

            let bootstrap = RaftGroupBootstrap::decode(&bytes)?;

            if bootstrap.raft_group_id != filename_group_id {
                return Err(BootstrapGroupError::FileIdentityMismatch {
                    filename_group_id,
                    record_group_id: bootstrap.raft_group_id,
                });
            }

            if bootstraps
                .insert(bootstrap.raft_group_id, bootstrap)
                .is_some()
            {
                return Err(BootstrapGroupError::DuplicateGroup(filename_group_id));
            }
        }

        Ok(bootstraps)
    }
}

impl BootstrapStore for FileBootstrapStore {
    fn load_bootstrap(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<Option<Vec<u8>>, BootstrapStoreError> {
        self.read_existing(&self.final_path(raft_group_id))
    }

    fn install_bootstrap_and_sync(
        &mut self,
        raft_group_id: RaftGroupId,
        encoded_bootstrap: &[u8],
    ) -> Result<BootstrapStoreInstall, BootstrapStoreError> {
        let final_path = self.final_path(raft_group_id);
        if let Some(existing) = self.read_existing(&final_path)? {
            return Ok(BootstrapStoreInstall::AlreadyExists(existing));
        }

        let temporary_id = self.next_temporary_id.fetch_add(1, Ordering::Relaxed);
        let temporary_path = self.directory.join(format!(
            ".raft-group-{}.bootstrap.{}.{}.tmp",
            raft_group_id.0,
            process::id(),
            temporary_id,
        ));
        let mut temporary = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary_path)
            .map_err(|error| {
                BootstrapStoreError::Unavailable(format!(
                    "create temporary bootstrap {}: {error}",
                    temporary_path.display()
                ))
            })?;
        temporary
            .write_all(encoded_bootstrap)
            .and_then(|_| temporary.sync_all())
            .map_err(|error| {
                BootstrapStoreError::Unavailable(format!(
                    "write temporary bootstrap {}: {error}",
                    temporary_path.display()
                ))
            })?;

        match fs::hard_link(&temporary_path, &final_path) {
            Ok(()) => {
                if let Err(error) = sync_directory(&self.directory) {
                    return Err(BootstrapStoreError::OutcomeUnknown(format!(
                        "synchronize installed bootstrap {}: {error}",
                        final_path.display()
                    )));
                }
                let _ = fs::remove_file(&temporary_path);
                Ok(BootstrapStoreInstall::Installed)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(&temporary_path);
                let existing = self.read_existing(&final_path)?.ok_or_else(|| {
                    BootstrapStoreError::Unavailable(format!(
                        "bootstrap {} disappeared after create conflict",
                        final_path.display()
                    ))
                })?;
                Ok(BootstrapStoreInstall::AlreadyExists(existing))
            }
            Err(error) => {
                let _ = fs::remove_file(&temporary_path);
                Err(BootstrapStoreError::Unavailable(format!(
                    "install bootstrap {}: {error}",
                    final_path.display()
                )))
            }
        }
    }
}

fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}

/// Remove only files in the bootstrap directory's private temporary namespace.
/// These files are never authoritative: successful installation creates the
/// final name before the temporary hard link is removed, so an orphaned temp
/// file can be discarded safely during the next node-owned startup.
fn cleanup_temporary_files(directory: &Path) -> io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if is_temporary_bootstrap_name(name) {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn is_temporary_bootstrap_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix(".raft-group-") else {
        return false;
    };
    let Some((group_id, suffix)) = rest.split_once(".bootstrap.") else {
        return false;
    };

    group_id.parse::<u64>().is_ok() && suffix.ends_with(".tmp")
}

fn temporary_id_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BootstrapStoreError {
    #[error("bootstrap storage is unavailable: {0}")]
    Unavailable(String),
    #[error("bootstrap persistence outcome is unknown: {0}")]
    OutcomeUnknown(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapOutcome {
    Installed,
    AlreadyInstalled,
}

/// load the durable bootstrap authority for an existing Raft group
///
/// restart callers intentionally provide only the group identity. Static seed
/// membership is not reconciled here because it must never replace or redefine
/// an already persisted quorum after process restart
pub fn load_durable_group_bootstrap<S: BootstrapStore>(
    store: &S,
    raft_group_id: RaftGroupId,
) -> Result<Option<RaftGroupBootstrap>, BootstrapGroupError> {
    store
        .load_bootstrap(raft_group_id)?
        .map(|bytes| RaftGroupBootstrap::decode(&bytes).map_err(BootstrapGroupError::from))
        .transpose()
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BootstrapGroupError {
    #[error(transparent)]
    Envelope(#[from] RaftGroupBootstrapError),
    #[error(transparent)]
    Storage(#[from] BootstrapStoreError),
    #[error(
        "bootstrap filename identifies group {filename_group_id:?}, \
         but the durable record identifies {record_group_id:?}"
    )]
    FileIdentityMismatch {
        filename_group_id: RaftGroupId,
        record_group_id: RaftGroupId,
    },

    #[error("duplicate durable bootstrap discovered for Raft group {0:?}")]
    DuplicateGroup(RaftGroupId),
}

/// Installs one group's bootstrap exactly once.
///
/// This operation completes before the caller registers the group with the
/// scheduler, dispatches Raft messages, or accepts proposals.
pub fn bootstrap_group_exactly_once<S>(
    store: &mut S,
    requested: &RaftGroupBootstrap,
) -> Result<BootstrapOutcome, BootstrapGroupError>
where
    S: BootstrapStore,
{
    requested.validate()?;

    if let Some(existing_bytes) = store.load_bootstrap(requested.raft_group_id)? {
        return reconcile_existing(&existing_bytes, requested);
    }

    let encoded = requested.encode()?;
    match store.install_bootstrap_and_sync(requested.raft_group_id, &encoded)? {
        BootstrapStoreInstall::Installed => Ok(BootstrapOutcome::Installed),
        BootstrapStoreInstall::AlreadyExists(existing_bytes) => {
            reconcile_existing(&existing_bytes, requested)
        }
    }
}

fn reconcile_existing(
    existing_bytes: &[u8],
    requested: &RaftGroupBootstrap,
) -> Result<BootstrapOutcome, BootstrapGroupError> {
    let existing = RaftGroupBootstrap::decode(existing_bytes)?;
    existing.reconcile(requested)?;
    Ok(BootstrapOutcome::AlreadyInstalled)
}
