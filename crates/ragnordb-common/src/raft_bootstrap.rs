//! Durable identity and initial membership for one Raft group.
//!
//! This module owns the database-specific binding between consensus replica
//! identities and physical node identities. The reusable Raft crate remains
//! independent of RagnorDB protobufs and physical routing.

use std::collections::{BTreeMap, BTreeSet};

use prost::Message;

use crate::{
    ids::{NodeId, RaftGroupId, ReplicaId},
    proto::raft as raft_proto,
};

pub const RAFT_GROUP_BOOTSTRAP_VERSION: u32 = 1;

/// Durable initial identity and membership authority for one Raft group.
///
/// The bootstrap is persisted exactly once before the group accepts messages
/// or proposals. Subsequent committed configuration entries evolve the core
/// `ConfState`; static process configuration never replaces this record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftGroupBootstrap {
    pub format_version: u32,
    pub cluster_id: String,
    pub raft_group_id: RaftGroupId,
    pub configuration_epoch: u64,
    pub replica_to_node: BTreeMap<ReplicaId, NodeId>,
    pub initial_voters: BTreeSet<ReplicaId>,
    pub initial_learners: BTreeSet<ReplicaId>,
}

impl RaftGroupBootstrap {
    pub fn new(
        cluster_id: String,
        raft_group_id: RaftGroupId,
        configuration_epoch: u64,
        replica_to_node: BTreeMap<ReplicaId, NodeId>,
        initial_voters: BTreeSet<ReplicaId>,
        initial_learners: BTreeSet<ReplicaId>,
    ) -> Result<Self, RaftGroupBootstrapError> {
        let bootstrap = Self {
            format_version: RAFT_GROUP_BOOTSTRAP_VERSION,
            cluster_id,
            raft_group_id,
            configuration_epoch,
            replica_to_node,
            initial_voters,
            initial_learners,
        };
        bootstrap.validate()?;
        Ok(bootstrap)
    }

    pub fn validate(&self) -> Result<(), RaftGroupBootstrapError> {
        if self.format_version != RAFT_GROUP_BOOTSTRAP_VERSION {
            return Err(RaftGroupBootstrapError::UnsupportedVersion(
                self.format_version,
            ));
        }
        if self.cluster_id.trim().is_empty() {
            return Err(RaftGroupBootstrapError::EmptyClusterId);
        }
        if self.raft_group_id.0 == 0 {
            return Err(RaftGroupBootstrapError::ZeroRaftGroupId);
        }
        if self.configuration_epoch == 0 {
            return Err(RaftGroupBootstrapError::ZeroConfigurationEpoch);
        }
        if self.initial_voters.is_empty() {
            return Err(RaftGroupBootstrapError::NoInitialVoters);
        }
        if let Some(replica_id) = self
            .initial_voters
            .intersection(&self.initial_learners)
            .next()
        {
            return Err(RaftGroupBootstrapError::VoterLearnerOverlap(*replica_id));
        }

        let mut physical_nodes = BTreeSet::new();
        for (replica_id, node_id) in &self.replica_to_node {
            if replica_id.0 == 0 {
                return Err(RaftGroupBootstrapError::ZeroReplicaId);
            }
            if node_id.0 == 0 {
                return Err(RaftGroupBootstrapError::ZeroNodeId);
            }
            if !physical_nodes.insert(*node_id) {
                return Err(RaftGroupBootstrapError::DuplicateNode(*node_id));
            }
        }

        if self
            .initial_voters
            .iter()
            .chain(self.initial_learners.iter())
            .any(|replica_id| replica_id.0 == 0)
        {
            return Err(RaftGroupBootstrapError::ZeroReplicaId);
        }

        let configured_members: BTreeSet<_> = self
            .initial_voters
            .union(&self.initial_learners)
            .copied()
            .collect();
        let mapped_members: BTreeSet<_> = self.replica_to_node.keys().copied().collect();
        if configured_members != mapped_members {
            return Err(RaftGroupBootstrapError::MembershipMappingMismatch);
        }

        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, RaftGroupBootstrapError> {
        self.validate()?;
        Ok(self.to_proto().encode_to_vec())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RaftGroupBootstrapError> {
        let proto = raft_proto::RaftGroupBootstrap::decode(bytes)
            .map_err(|error| RaftGroupBootstrapError::Decode(error.to_string()))?;
        Self::from_proto(proto)
    }

    /// Builds Raft membership without exposing RagnorDB protobuf or physical
    /// node identities to the consensus crate.
    pub fn to_core_conf_state(&self) -> Result<raft::types::ConfState, RaftGroupBootstrapError> {
        self.validate()?;
        let voters = self
            .initial_voters
            .iter()
            .copied()
            .map(ReplicaId::to_raft)
            .collect::<Result<Vec<_>, _>>()
            .map_err(RaftGroupBootstrapError::InvalidReplicaId)?;
        let learners = self
            .initial_learners
            .iter()
            .copied()
            .map(ReplicaId::to_raft)
            .collect::<Result<Vec<_>, _>>()
            .map_err(RaftGroupBootstrapError::InvalidReplicaId)?;

        raft::types::ConfState::new(self.configuration_epoch, voters, learners).map_err(|error| {
            RaftGroupBootstrapError::InvalidCoreConfiguration(format!("{error:?}"))
        })
    }

    /// Accepts only an exact replay of recovered durable bootstrap authority.
    pub fn reconcile(&self, requested: &Self) -> Result<(), RaftGroupBootstrapError> {
        self.validate()?;
        requested.validate()?;
        if self == requested {
            return Ok(());
        }
        Err(RaftGroupBootstrapError::BootstrapConflict {
            raft_group_id: self.raft_group_id,
        })
    }

    fn to_proto(&self) -> raft_proto::RaftGroupBootstrap {
        raft_proto::RaftGroupBootstrap {
            format_version: self.format_version,
            cluster_id: self.cluster_id.clone(),
            raft_group_id: Some(self.raft_group_id.to_proto()),
            configuration_epoch: self.configuration_epoch,
            replica_placements: self
                .replica_to_node
                .iter()
                .map(|(replica_id, node_id)| raft_proto::ReplicaPlacement {
                    replica_id: Some(replica_id.to_proto()),
                    node_id: Some(node_id.to_proto()),
                })
                .collect(),
            initial_voters: self
                .initial_voters
                .iter()
                .map(ReplicaId::to_proto)
                .collect(),
            initial_learners: self
                .initial_learners
                .iter()
                .map(ReplicaId::to_proto)
                .collect(),
        }
    }

    fn from_proto(proto: raft_proto::RaftGroupBootstrap) -> Result<Self, RaftGroupBootstrapError> {
        let raft_group_id = RaftGroupId::from_proto(
            proto
                .raft_group_id
                .ok_or(RaftGroupBootstrapError::MissingField("raft_group_id"))?,
        );
        let mut replica_to_node = BTreeMap::new();
        let mut physical_nodes = BTreeSet::new();
        for placement in proto.replica_placements {
            let replica_id = ReplicaId::from_proto(placement.replica_id.ok_or(
                RaftGroupBootstrapError::MissingField("replica_placements.replica_id"),
            )?);
            let node_id = NodeId::from_proto(placement.node_id.ok_or(
                RaftGroupBootstrapError::MissingField("replica_placements.node_id"),
            )?);
            if replica_to_node.insert(replica_id, node_id).is_some() {
                return Err(RaftGroupBootstrapError::DuplicateReplica(replica_id));
            }
            if !physical_nodes.insert(node_id) {
                return Err(RaftGroupBootstrapError::DuplicateNode(node_id));
            }
        }

        let bootstrap = Self {
            format_version: proto.format_version,
            cluster_id: proto.cluster_id,
            raft_group_id,
            configuration_epoch: proto.configuration_epoch,
            replica_to_node,
            initial_voters: decode_membership("initial_voters", proto.initial_voters)?,
            initial_learners: decode_membership("initial_learners", proto.initial_learners)?,
        };
        bootstrap.validate()?;
        Ok(bootstrap)
    }
}

fn decode_membership(
    field: &'static str,
    encoded: Vec<crate::proto::ids::ReplicaId>,
) -> Result<BTreeSet<ReplicaId>, RaftGroupBootstrapError> {
    let mut members = BTreeSet::new();
    for replica_id in encoded {
        let replica_id = ReplicaId::from_proto(replica_id);
        if !members.insert(replica_id) {
            return Err(RaftGroupBootstrapError::DuplicateMembershipEntry { field, replica_id });
        }
    }
    Ok(members)
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RaftGroupBootstrapError {
    #[error("unsupported Raft bootstrap format version {0}")]
    UnsupportedVersion(u32),
    #[error("Raft bootstrap cluster ID must not be empty")]
    EmptyClusterId,
    #[error("Raft group ID must be non-zero")]
    ZeroRaftGroupId,
    #[error("Raft bootstrap configuration epoch must be non-zero")]
    ZeroConfigurationEpoch,
    #[error("Raft bootstrap must contain at least one voter")]
    NoInitialVoters,
    #[error("Raft bootstrap contains the reserved replica ID zero")]
    ZeroReplicaId,
    #[error("Raft bootstrap contains the reserved node ID zero")]
    ZeroNodeId,
    #[error("replica {0:?} appears as both voter and learner")]
    VoterLearnerOverlap(ReplicaId),
    #[error("replica {0:?} has more than one physical-node mapping")]
    DuplicateReplica(ReplicaId),
    #[error("physical node {0:?} hosts more than one replica of this group")]
    DuplicateNode(NodeId),
    #[error("duplicate replica {replica_id:?} in {field}")]
    DuplicateMembershipEntry {
        field: &'static str,
        replica_id: ReplicaId,
    },
    #[error("initial membership and replica-to-node mappings do not match")]
    MembershipMappingMismatch,
    #[error("missing required bootstrap field {0}")]
    MissingField(&'static str),
    #[error("invalid replica identity: {0}")]
    InvalidReplicaId(&'static str),
    #[error("invalid Raft core configuration: {0}")]
    InvalidCoreConfiguration(String),
    #[error("cannot decode durable Raft bootstrap: {0}")]
    Decode(String),
    #[error(
        "durable bootstrap for Raft group {raft_group_id:?} conflicts with requested bootstrap"
    )]
    BootstrapConflict { raft_group_id: RaftGroupId },
}
