//! exclusive process ownership for one RagnorDB data directory
//!
//! A-WAL coordinates access within one `WalHandle`, but the database server and
//! standalone operational tools are separate processes. This module provides the
//! small cross-process boundary required to prevent those owners from accessing
//! the same mutable storage lifetime concurrently

use std::{
    fs::{File, OpenOptions, TryLockError},
    path::{Path, PathBuf},
};

use ragnordb_common::{Error, Result};

/// stable lock file name stored directly beneath the database data directory
pub const DATA_DIRECTORY_LOCK_FILE: &str = ".ragnordb.lock";

/// process lifetime exclusive ownership of one RagnorDB data directory.
///
/// the operating system releases the advisory lock automatically when this
/// guard is dropped or the owning process terminates. The lock file itself may
/// remain on disk; its existence does not indicate ownership—the active file
/// lock does
#[must_use = "dropping the guard releases exclusive data-directory ownership"]
#[derive(Debug)]
pub struct DataDirectoryLock {
    data_dir: PathBuf,
    _file: File,
}

impl DataDirectoryLock {
    /// attempt to acquire exclusive ownership without waiting
    ///
    /// both the live server and offline tools use the same exclusive mode:
    ///
    /// - the server holds it for the complete `LocalDatabase` lifetime;
    /// - the inspector holds it for the complete inspection lifetime.
    ///
    /// failing immediately gives operators a clear diagnostic instead of
    /// allowing a command to block indefinitely behind a running server
    pub fn acquire(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        let lock_path = data_dir.join(DATA_DIRECTORY_LOCK_FILE);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|source| {
                Error::Configuration(format!(
                    "failed to open data-directory lock {}: {source}",
                    lock_path.display()
                ))
            })?;

        match file.try_lock() {
            Ok(()) => Ok(Self {
                data_dir: data_dir.to_path_buf(),
                _file: file,
            }),

            Err(TryLockError::WouldBlock) => Err(Error::Configuration(format!(
                "data directory {} is already owned by another RagnorDB process; \
                 stop the running node before using offline inspection",
                data_dir.display()
            ))),

            Err(TryLockError::Error(source)) => Err(Error::Configuration(format!(
                "failed to acquire exclusive data-directory lock {}: {source}",
                lock_path.display()
            ))),
        }
    }

    /// Directory whose mutable storage lifetime is protected by this guard.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}
