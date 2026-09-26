//! Typed recovery positions for tablet-local durable storage.
//!
//! A manifest selects one recovery frontier for its complete segment set.
//! Replicated tablets recover according to their applied Raft boundary; a
//! local A-WAL LSN is only a separate retention mapping and is never treated
//! as a cluster-wide ordering value.

use std::{error::Error, fmt};

use ragnordb_common::ids::{RaftGroupId, ReplicaId};
use wal::lsn::Lsn;

/// Recovery position covered by one complete durable manifest generation.
///
/// The enclosing manifest must also bind this value to its tablet identity.
/// For replicated storage, the group and replica fields bind the Raft position
/// to the correct local replica lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryFrontier {
    /// The state is durable through this local A-WAL position.
    SingleNode { replay_from_end_lsn: Lsn },

    /// The state is durable through this tablet replica's applied Raft entry.
    ///
    /// A local A-WAL LSN, when needed for retention, belongs in a separate
    /// `ReplicatedWalMapping`.
    ReplicatedTablet {
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
        applied_index: u64,
        applied_term: u64,
    },
}

impl RecoveryFrontier {
    /// Reject malformed replicated identities or incomplete index/term pairs.
    ///
    /// The zero/zero applied boundary is valid for a newly initialized replica.
    pub fn validate(&self) -> Result<(), RecoveryFrontierError> {
        if let Self::ReplicatedTablet {
            raft_group_id,
            replica_id,
            applied_index,
            applied_term,
        } = self
        {
            if raft_group_id.0 == 0 {
                return Err(RecoveryFrontierError::ZeroRaftGroupId);
            }
            if replica_id.0 == 0 {
                return Err(RecoveryFrontierError::ZeroReplicaId);
            }

            let index_is_zero = *applied_index == 0;
            let term_is_zero = *applied_term == 0;
            if index_is_zero != term_is_zero {
                return Err(RecoveryFrontierError::InvalidAppliedBoundary);
            }
        }

        Ok(())
    }

    /// Validate monotonic advancement within the same recovery mode and
    /// replicated lineage.
    ///
    /// The caller must separately ensure both frontiers belong to the same
    /// manifest/tablet identity. The type deliberately does not compare an
    /// A-WAL LSN with a Raft index.
    pub fn validate_successor_of(&self, previous: &Self) -> Result<(), RecoveryFrontierError> {
        self.validate()?;
        previous.validate()?;

        match (*previous, *self) {
            (
                Self::SingleNode {
                    replay_from_end_lsn: previous_lsn,
                },
                Self::SingleNode {
                    replay_from_end_lsn: current_lsn,
                },
            ) => {
                if current_lsn < previous_lsn {
                    return Err(RecoveryFrontierError::SingleNodeLsnRegression {
                        previous: previous_lsn,
                        current: current_lsn,
                    });
                }
            }
            (
                Self::ReplicatedTablet {
                    raft_group_id: previous_group,
                    replica_id: previous_replica,
                    applied_index: previous_index,
                    applied_term: previous_term,
                },
                Self::ReplicatedTablet {
                    raft_group_id: current_group,
                    replica_id: current_replica,
                    applied_index: current_index,
                    applied_term: current_term,
                },
            ) => {
                if current_group != previous_group || current_replica != previous_replica {
                    return Err(RecoveryFrontierError::LineageMismatch);
                }

                if current_index < previous_index {
                    return Err(RecoveryFrontierError::RaftIndexRegression {
                        previous: previous_index,
                        current: current_index,
                    });
                }

                if current_index == previous_index && current_term != previous_term {
                    return Err(RecoveryFrontierError::RaftTermChangedAtSameIndex {
                        index: current_index,
                        previous: previous_term,
                        current: current_term,
                    });
                }

                if current_index > previous_index && current_term < previous_term {
                    return Err(RecoveryFrontierError::RaftTermRegression {
                        previous: previous_term,
                        current: current_term,
                    });
                }
            }
            _ => return Err(RecoveryFrontierError::ModeMismatch),
        }

        Ok(())
    }
}

/// Local A-WAL position associated with one replicated Raft frontier.
///
/// This mapping is replica-local retention metadata. Its LSN must not be used
/// to compare progress across Raft groups or replicas.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicatedWalMapping {
    pub raft_group_id: RaftGroupId,
    pub replica_id: ReplicaId,
    pub applied_index: u64,
    pub applied_term: u64,
    pub local_wal_lsn: Lsn,
}

impl ReplicatedWalMapping {
    fn frontier(&self) -> RecoveryFrontier {
        RecoveryFrontier::ReplicatedTablet {
            raft_group_id: self.raft_group_id,
            replica_id: self.replica_id,
            applied_index: self.applied_index,
            applied_term: self.applied_term,
        }
    }

    /// Ensure this local mapping names exactly the replicated frontier stored
    /// by the selected manifest.
    pub fn validate_for(&self, frontier: &RecoveryFrontier) -> Result<(), RecoveryFrontierError> {
        self.frontier().validate()?;
        frontier.validate()?;

        match (*frontier, self.frontier()) {
            (
                RecoveryFrontier::ReplicatedTablet {
                    raft_group_id: expected_group,
                    replica_id: expected_replica,
                    applied_index: expected_index,
                    applied_term: expected_term,
                },
                RecoveryFrontier::ReplicatedTablet {
                    raft_group_id: mapped_group,
                    replica_id: mapped_replica,
                    applied_index: mapped_index,
                    applied_term: mapped_term,
                },
            ) => {
                if expected_group != mapped_group || expected_replica != mapped_replica {
                    return Err(RecoveryFrontierError::LineageMismatch);
                }
                if expected_index != mapped_index || expected_term != mapped_term {
                    return Err(RecoveryFrontierError::WalMappingDoesNotMatchFrontier);
                }
                Ok(())
            }
            _ => Err(RecoveryFrontierError::ModeMismatch),
        }
    }

    /// Validate that a later local mapping advances both the Raft boundary and
    /// the corresponding A-WAL position without crossing replica lineages.
    pub fn validate_successor_of(&self, previous: &Self) -> Result<(), RecoveryFrontierError> {
        self.frontier()
            .validate_successor_of(&previous.frontier())?;

        if self.local_wal_lsn < previous.local_wal_lsn {
            return Err(RecoveryFrontierError::WalLsnRegression {
                previous: previous.local_wal_lsn,
                current: self.local_wal_lsn,
            });
        }

        Ok(())
    }
}

/// Invalid or incompatible recovery metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryFrontierError {
    ZeroRaftGroupId,
    ZeroReplicaId,
    InvalidAppliedBoundary,
    ModeMismatch,
    LineageMismatch,
    SingleNodeLsnRegression {
        previous: Lsn,
        current: Lsn,
    },
    RaftIndexRegression {
        previous: u64,
        current: u64,
    },
    RaftTermRegression {
        previous: u64,
        current: u64,
    },
    RaftTermChangedAtSameIndex {
        index: u64,
        previous: u64,
        current: u64,
    },
    WalLsnRegression {
        previous: Lsn,
        current: Lsn,
    },
    WalMappingDoesNotMatchFrontier,
}

impl fmt::Display for RecoveryFrontierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid or incompatible recovery frontier: {self:?}"
        )
    }
}

impl Error for RecoveryFrontierError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_frontiers_reject_cross_mode_comparison() {
        let single_node = RecoveryFrontier::SingleNode {
            replay_from_end_lsn: Lsn(100),
        };
        let replicated = RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RaftGroupId(7),
            replica_id: ReplicaId(3),
            applied_index: 8,
            applied_term: 2,
        };

        assert_eq!(
            replicated.validate_successor_of(&single_node),
            Err(RecoveryFrontierError::ModeMismatch)
        );
    }

    #[test]
    fn replicated_frontier_rejects_replica_lifetime_mismatch() {
        let previous = RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RaftGroupId(7),
            replica_id: ReplicaId(3),
            applied_index: 8,
            applied_term: 2,
        };
        let current = RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RaftGroupId(7),
            replica_id: ReplicaId(4),
            applied_index: 9,
            applied_term: 2,
        };

        assert_eq!(
            current.validate_successor_of(&previous),
            Err(RecoveryFrontierError::LineageMismatch)
        );
    }

    #[test]
    fn replicated_frontier_rejects_index_regression_and_term_rewrite() {
        let previous = RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RaftGroupId(7),
            replica_id: ReplicaId(3),
            applied_index: 8,
            applied_term: 2,
        };
        let regressed = RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RaftGroupId(7),
            replica_id: ReplicaId(3),
            applied_index: 7,
            applied_term: 2,
        };
        let rewritten = RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RaftGroupId(7),
            replica_id: ReplicaId(3),
            applied_index: 8,
            applied_term: 3,
        };

        assert!(matches!(
            regressed.validate_successor_of(&previous),
            Err(RecoveryFrontierError::RaftIndexRegression { .. })
        ));
        assert!(matches!(
            rewritten.validate_successor_of(&previous),
            Err(RecoveryFrontierError::RaftTermChangedAtSameIndex { .. })
        ));
    }

    #[test]
    fn replicated_wal_mapping_must_match_manifest_frontier() {
        let mapping = ReplicatedWalMapping {
            raft_group_id: RaftGroupId(7),
            replica_id: ReplicaId(3),
            applied_index: 8,
            applied_term: 2,
            local_wal_lsn: Lsn(900),
        };
        let frontier = RecoveryFrontier::ReplicatedTablet {
            raft_group_id: RaftGroupId(7),
            replica_id: ReplicaId(3),
            applied_index: 9,
            applied_term: 2,
        };

        assert_eq!(
            mapping.validate_for(&frontier),
            Err(RecoveryFrontierError::WalMappingDoesNotMatchFrontier)
        );
    }
}
