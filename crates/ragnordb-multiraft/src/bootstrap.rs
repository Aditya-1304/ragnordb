//! Exactly-once bootstrap coordination for local Raft group replicas.

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
