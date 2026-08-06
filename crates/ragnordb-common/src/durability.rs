//! node wide durability health shared by every local database operation
//!
//! A-WAL is physically shared by transaction, catalog, checkpoint, and
//! retention operations. Once any operation makes the authoritative durable
//! prefix uncertain, the complete node must stop serving database work until
//! reopen and recovery reconstruct trustworthy state

use std::sync::{Arc, Mutex};

use crate::result::{Error, Result};

/// stable classification of the first failure that fenced the local node
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityFailureKind {
    WalWriterFatal,
    StorageCorruption,
    CommitOutcomeUnknown,
    CatalogOutcomeUnknown,
    CheckpointOutcomeUnknown,
    RecoveryRequired,
}

impl DurabilityFailureKind {
    /// stable diagnostic name exposed through administrative status
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WalWriterFatal => "wal_writer_fatal",
            Self::StorageCorruption => "storage_corruption",
            Self::CommitOutcomeUnknown => "commit_outcome_unknown",
            Self::CatalogOutcomeUnknown => "catalog_outcome_unknown",
            Self::CheckpointOutcomeUnknown => "checkpoint_outcome_unknown",
            Self::RecoveryRequired => "recovery_required",
        }
    }
}

/// first durability failure retained by the node wide gate
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurabilityFailure {
    kind: DurabilityFailureKind,
    reason: String,
}

impl DurabilityFailure {
    pub const fn kind(&self) -> DurabilityFailureKind {
        self.kind
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// current node wide durability health
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum NodeDurabilityState {
    #[default]
    Healthy,
    RecoveryRequired(DurabilityFailure),
}

/// cloneable node-wide fail stop boundary
///
/// Clones share the same state. The first durability failure is preserved
/// because it is normally the most useful explanation of why recovery became
/// necessary; later secondary errors must not overwrite it
#[derive(Debug, Clone, Default)]
pub struct DurabilityGate {
    state: Arc<Mutex<NodeDurabilityState>>,
}

impl DurabilityGate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(&self) -> NodeDurabilityState {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn is_healthy(&self) -> bool {
        matches!(self.state(), NodeDurabilityState::Healthy)
    }

    /// reject database work after the node has entered fail-stop recovery
    pub fn ensure_healthy(&self) -> Result<()> {
        match self.state() {
            NodeDurabilityState::Healthy => Ok(()),

            NodeDurabilityState::RecoveryRequired(failure) => Err(Error::RecoveryRequired {
                reason: failure.reason,
            }),
        }
    }

    /// fence the node while preserving the first authoritative failure
    pub fn require_recovery(
        &self,
        kind: DurabilityFailureKind,
        reason: impl Into<String>,
    ) -> Error {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if matches!(*state, NodeDurabilityState::Healthy) {
            *state = NodeDurabilityState::RecoveryRequired(DurabilityFailure {
                kind,
                reason: reason.into(),
            });
        }

        let NodeDurabilityState::RecoveryRequired(failure) = &*state else {
            unreachable!("durability state was fenced above");
        };

        Error::RecoveryRequired {
            reason: failure.reason.clone(),
        }
    }

    /// observe a canonical database error and fence only when its typed
    /// classification requires reopen and recovery
    pub fn observe_error(&self, error: &Error) {
        let classification = match error {
            Error::WalAppendNotStaged {
                recovery_required: true,
                ..
            } => Some(DurabilityFailureKind::WalWriterFatal),

            Error::CommitOutcomeUnknown { .. } => Some(DurabilityFailureKind::CommitOutcomeUnknown),

            Error::CatalogOutcomeUnknown { .. } => {
                Some(DurabilityFailureKind::CatalogOutcomeUnknown)
            }

            Error::CheckpointOutcomeUnknown { .. } => {
                Some(DurabilityFailureKind::CheckpointOutcomeUnknown)
            }

            // CorruptData is reserved for bytes that violate a canonical
            // storage, recovery, or internal encoding contract. Once such
            // bytes are observed by a live operation, later reads and writes
            // cannot safely assume that the affected state is isolated.
            Error::CorruptData(_) => Some(DurabilityFailureKind::StorageCorruption),

            Error::RecoveryRequired { .. } => Some(DurabilityFailureKind::RecoveryRequired),

            Error::NotImplemented(_)
            | Error::InvalidArgument(_)
            | Error::NotLeader { .. }
            | Error::ProposalUnavailable { .. }
            | Error::WriteConflict(_)
            | Error::SqlParse(_)
            | Error::UnsupportedSql(_)
            | Error::SchemaMismatch(_)
            | Error::ConstraintViolation(_)
            | Error::Configuration(_)
            | Error::StatementTimeout { .. }
            | Error::WalAppendNotStaged {
                recovery_required: false,
                ..
            }
            | Error::RecoveryFailed { .. }
            | Error::SnapshotPublicationFailed { .. } => None,
        };

        if let Some(kind) = classification {
            let _ = self.require_recovery(kind, error.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_not_staged_rejection_does_not_fence_the_node() {
        let gate = DurabilityGate::new();

        gate.observe_error(&Error::WalAppendNotStaged {
            reason: "payload exceeds configured maximum".to_string(),
            recovery_required: false,
        });

        assert!(gate.is_healthy());
        gate.ensure_healthy().unwrap();
    }

    #[test]
    fn fatal_not_staged_failure_fences_the_node() {
        let gate = DurabilityGate::new();

        gate.observe_error(&Error::WalAppendNotStaged {
            reason: "WAL is in sticky-fatal state".to_string(),
            recovery_required: true,
        });

        assert!(!gate.is_healthy());
        assert!(matches!(
            gate.ensure_healthy().unwrap_err(),
            Error::RecoveryRequired { .. }
        ));
    }

    #[test]
    fn storage_corruption_fences_the_node() {
        let gate = DurabilityGate::new();

        gate.observe_error(&Error::CorruptData(
            "MVCC write references a missing default value".to_string(),
        ));

        let NodeDurabilityState::RecoveryRequired(failure) = gate.state() else {
            panic!("storage corruption must require recovery");
        };

        assert_eq!(failure.kind(), DurabilityFailureKind::StorageCorruption);
        assert_eq!(failure.kind().as_str(), "storage_corruption");
        assert!(matches!(
            gate.ensure_healthy(),
            Err(Error::RecoveryRequired { .. })
        ));
    }

    #[test]
    fn first_durability_failure_is_preserved() {
        let gate = DurabilityGate::new();

        gate.observe_error(&Error::CommitOutcomeUnknown {
            start_lsn: 10,
            end_lsn: 20,
            reason: "sync result is unknown".to_string(),
        });

        gate.observe_error(&Error::RecoveryRequired {
            reason: "secondary failure".to_string(),
        });

        let NodeDurabilityState::RecoveryRequired(failure) = gate.state() else {
            panic!("gate must require recovery");
        };

        assert_eq!(failure.kind(), DurabilityFailureKind::CommitOutcomeUnknown);
        assert!(failure.reason().contains("sync result is unknown"));
        assert!(!failure.reason().contains("secondary failure"));
    }
}
