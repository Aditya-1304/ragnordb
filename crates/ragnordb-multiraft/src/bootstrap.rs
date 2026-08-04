//! Exactly-once bootstrap coordination for local Raft group replicas.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
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
        sync_directory(&directory).map_err(|error| {
            BootstrapStoreError::Unavailable(format!(
                "synchronize bootstrap directory {}: {error}",
                directory.display()
            ))
        })?;
        Ok(Self {
            directory,
            next_temporary_id: AtomicU64::new(1),
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
            ".raft-group-{}.bootstrap.{}.tmp",
            raft_group_id.0, temporary_id
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

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BootstrapGroupError {
    #[error(transparent)]
    Envelope(#[from] RaftGroupBootstrapError),
    #[error(transparent)]
    Storage(#[from] BootstrapStoreError),
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
