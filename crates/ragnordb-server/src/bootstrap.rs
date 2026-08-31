//! Static cluster bootstrap authority for the metadata Raft group.
//!
//! Static seed configuration is used exactly once: when the metadata group has
//! no durable bootstrap record yet.
//!
//! Once installed, the durable `RaftGroupBootstrap` is authoritative for the
//! initial metadata-group membership. Restart must never reconstruct the voter
//! set from a changed configuration file.

use std::collections::{BTreeMap, BTreeSet};

use ragnordb_common::{
    Error, Result,
    ids::{NodeId, RaftGroupId, ReplicaId},
    metadata_codec::NodeDescriptor,
    raft_bootstrap::RaftGroupBootstrap,
};

use ragnordb_multiraft::{
    bootstrap::{FileBootstrapStore, bootstrap_group_exactly_once, load_durable_group_bootstrap},
    storage::{codec::RaftReplicaIdentity, recovery::RecoveredRaftStorage},
};

use crate::config::{NodeConfig, SeedNodeConfig};

/// The existing Milestone-4 tablet permanently owns group 1.
///
/// Reserving group 2 for metadata avoids rewriting existing durable tablet
/// state. must start newly allocated tablet Raft-group IDs above this
/// reserved range.
pub(crate) const METADATA_RAFT_GROUP_ID: RaftGroupId = RaftGroupId(2);

const METADATA_INITIAL_CONFIGURATION_EPOCH: u64 = 1;

#[derive(Debug, Clone)]
pub(crate) struct ResolvedMetadataBootstrap {
    pub bootstrap: RaftGroupBootstrap,
    pub local_replica_id: ReplicaId,

    /// true only when this process durably installed the bootstrap record during
    /// this invocation
    ///
    /// this is diagnostic information only. Membership authority always comes
    /// from `bootstrap`, never from the current static seed list
    pub installed_now: bool,
}

/// resolve the durable bootstrap authority for the metadata Raft group
///
/// fresh cluster:
///
///     static seed config
///         -> canonical RaftGroupBootstrap
///         -> fsync durable bootstrap
///         -> start metadata group
///
/// restart:
///
///     durable RaftGroupBootstrap
///         -> ignore changed static membership
///         -> combine with recovered committed ConfState
///
/// a recovered metadata WAL without its bootstrap is fail stop corruption:
/// membership must never be reconstructed from the current config in that case
pub(crate) fn resolve_metadata_bootstrap(
    config: &NodeConfig,
    recovered: &RecoveredRaftStorage,
) -> Result<ResolvedMetadataBootstrap> {
    let cluster_id = config.cluster_id.as_ref().ok_or_else(|| {
        Error::Configuration("metadata bootstrap requires cluster_id".to_string())
    })?;

    let mut store =
        FileBootstrapStore::open(config.data_dir.join("raft-bootstrap")).map_err(|source| {
            Error::RecoveryFailed {
                reason: format!("open Raft bootstrap store: {source}"),
            }
        })?;

    if let Some(durable) =
        load_durable_group_bootstrap(&store, METADATA_RAFT_GROUP_ID).map_err(|source| {
            Error::RecoveryFailed {
                reason: format!("load metadata Raft bootstrap: {source}"),
            }
        })?
    {
        if &durable.cluster_id != cluster_id {
            return Err(Error::Configuration(format!(
                "data directory belongs to cluster {}, \
                     configured cluster is {}",
                durable.cluster_id, cluster_id,
            )));
        }

        let local_replica_id = durable.replica_on_node(config.node_id).ok_or_else(|| {
            Error::Configuration(format!(
                "node {} is not a member of durable \
                     metadata Raft bootstrap",
                config.node_id.0,
            ))
        })?;

        // current static configuration supplies network locations only
        // requiring addresses for every durable member is safe; deriving the
        // membership set itself from those addresses would not be
        validate_durable_member_addresses(config, &durable)?;

        validate_recovered_metadata_identity(recovered, local_replica_id)?;

        return Ok(ResolvedMetadataBootstrap {
            bootstrap: durable,
            local_replica_id,
            installed_now: false,
        });
    }

    // WAL state proves that this group existed. Recreating the missing
    // bootstrap from today's seed list could silently form a different quorum
    if recovered
        .replicas()
        .any(|(identity, _)| identity.raft_group_id == METADATA_RAFT_GROUP_ID)
    {
        return Err(Error::RecoveryFailed {
            reason: "metadata Raft WAL state exists but its \
                     durable RaftGroupBootstrap is missing"
                .to_string(),
        });
    }

    if !config.bootstrap {
        return Err(Error::Configuration(
            "metadata Raft group is not initialized; \
                 fresh cluster startup requires bootstrap = true"
                .to_string(),
        ));
    }

    let requested = derive_metadata_bootstrap(config)?;

    // This is the irreversible bootstrap boundary
    //
    // FileBootstrapStore publishes the complete file and synchronizes the
    // directory before reporting success. Only after this returns may the
    // metadata Raft core be constructed
    bootstrap_group_exactly_once(&mut store, &requested).map_err(|source| {
        Error::RecoveryFailed {
            reason: format!("persist metadata Raft bootstrap: {source}"),
        }
    })?;

    let durable = load_durable_group_bootstrap(&store, METADATA_RAFT_GROUP_ID)
        .map_err(|source| Error::RecoveryFailed {
            reason: format!("reload durable metadata bootstrap: {source}"),
        })?
        .ok_or_else(|| Error::RecoveryFailed {
            reason: "metadata bootstrap install reported success \
                     but no durable record was found"
                .to_string(),
        })?;

    // never continue from the requested in-memory value. The bytes read back
    // from the durable store are the authority
    if durable != requested {
        return Err(Error::CorruptData(
            "durable metadata bootstrap differs from \
                 the bootstrap just installed"
                .to_string(),
        ));
    }

    let local_replica_id = durable.replica_on_node(config.node_id).ok_or_else(|| {
        Error::Configuration(format!(
            "node {} has no replica in newly installed \
                 metadata bootstrap",
            config.node_id.0,
        ))
    })?;

    Ok(ResolvedMetadataBootstrap {
        bootstrap: durable,
        local_replica_id,
        installed_now: true,
    })
}

/// construct the one canonical initial metadata-group membership
///
/// Replica IDs are deliberately allocated independently of NodeId. The mapping
/// is deterministic because seed nodes are sorted by stable NodeId first
pub(crate) fn derive_metadata_bootstrap(config: &NodeConfig) -> Result<RaftGroupBootstrap> {
    let cluster_id = config.cluster_id.clone().ok_or_else(|| {
        Error::Configuration("metadata bootstrap requires cluster_id".to_string())
    })?;

    if config.seed_nodes.is_empty() {
        return Err(Error::Configuration(
            "metadata bootstrap requires at least one seed node".to_string(),
        ));
    }

    let mut seeds = config.seed_nodes.iter().collect::<Vec<_>>();

    seeds.sort_by_key(|seed| seed.id);

    let mut replica_to_node = BTreeMap::new();

    let mut voters = BTreeSet::new();

    for (index, seed) in seeds.into_iter().enumerate() {
        let ordinal = u64::try_from(index)
            .map_err(|_| {
                Error::Configuration("seed-node count exceeds ReplicaId space".to_string())
            })?
            .checked_add(1)
            .ok_or_else(|| {
                Error::Configuration("metadata ReplicaId space exhausted".to_string())
            })?;

        let replica_id = ReplicaId(ordinal);

        replica_to_node.insert(replica_id, seed.id);

        voters.insert(replica_id);
    }

    RaftGroupBootstrap::new(
        cluster_id,
        METADATA_RAFT_GROUP_ID,
        METADATA_INITIAL_CONFIGURATION_EPOCH,
        replica_to_node,
        voters,
        BTreeSet::new(),
    )
    .map_err(|source| Error::Configuration(format!("invalid metadata bootstrap: {source}")))
}

/// Build the initial replicated node-directory records
///
/// Only nodes present in the durable metadata-group bootstrap are included
/// Extra nodes added to a later config file must not silently become cluster
/// members merely because the process restarted
pub(crate) fn metadata_seed_descriptors(
    config: &NodeConfig,
    bootstrap: &RaftGroupBootstrap,
) -> Result<Vec<NodeDescriptor>> {
    let mut node_ids = bootstrap
        .replica_to_node
        .values()
        .copied()
        .collect::<BTreeSet<_>>();

    let mut descriptors = Vec::with_capacity(node_ids.len());

    for node_id in node_ids.iter() {
        let seed = config
            .seed_nodes
            .iter()
            .find(|seed| seed.id == *node_id)
            .ok_or_else(|| {
                Error::Configuration(format!(
                    "durable metadata member node {} has no \
                     address record in current seed_nodes",
                    node_id.0,
                ))
            })?;

        descriptors.push(node_descriptor(seed));
    }

    // BTreeSet iteration already gives canonical NodeId order.
    node_ids.clear();

    Ok(descriptors)
}

fn node_descriptor(seed: &SeedNodeConfig) -> NodeDescriptor {
    NodeDescriptor {
        node_id: seed.id,
        raft_addr: seed.raft_addr.to_string(),
        snapshot_addr: seed.snapshot_addr.to_string(),
        sql_addr: seed.sql_addr.to_string(),
        admin_addr: seed.admin_addr.to_string(),
    }
}

fn validate_durable_member_addresses(
    config: &NodeConfig,
    bootstrap: &RaftGroupBootstrap,
) -> Result<()> {
    for node_id in bootstrap.replica_to_node.values() {
        if !config.seed_nodes.iter().any(|seed| seed.id == *node_id) {
            return Err(Error::Configuration(format!(
                "durable metadata member node {} is missing \
                     from seed_nodes; static membership is not being \
                     rebuilt, but its transport address is required",
                node_id.0,
            )));
        }
    }

    Ok(())
}

fn validate_recovered_metadata_identity(
    recovered: &RecoveredRaftStorage,
    local_replica_id: ReplicaId,
) -> Result<()> {
    let expected =
        RaftReplicaIdentity::new(METADATA_RAFT_GROUP_ID, local_replica_id).map_err(|source| {
            Error::RecoveryFailed {
                reason: source.to_string(),
            }
        })?;

    for (identity, _) in recovered.replicas() {
        if identity.raft_group_id == METADATA_RAFT_GROUP_ID && *identity != expected {
            return Err(Error::RecoveryFailed {
                reason: format!(
                    "recovered metadata replica identity {:?} \
                         conflicts with durable local identity {:?}",
                    identity, expected,
                ),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, path::PathBuf};

    use tempfile::TempDir;

    use super::*;

    fn seed(id: u64, base: u16) -> SeedNodeConfig {
        SeedNodeConfig {
            id: NodeId(id),
            raft_addr: format!("127.0.0.1:{base}").parse().unwrap(),

            snapshot_addr: format!("127.0.0.1:{}", base + 100).parse().unwrap(),

            sql_addr: format!("127.0.0.1:{}", base + 200).parse().unwrap(),

            admin_addr: format!("127.0.0.1:{}", base + 300).parse().unwrap(),
        }
    }

    fn config(
        root: &TempDir,
        cluster_id: &str,
        bootstrap: bool,
        mut seed_nodes: Vec<SeedNodeConfig>,
    ) -> NodeConfig {
        // Keep local node 10.
        seed_nodes.sort_by_key(|seed| seed.id);

        let local = seed_nodes
            .iter()
            .find(|seed| seed.id == NodeId(10))
            .unwrap();

        NodeConfig {
            node_id: NodeId(10),
            data_dir: root.path().to_path_buf(),
            listen_addr: local.sql_addr,
            admin_addr: local.admin_addr,
            max_connections: 100,
            statement_timeout_ms: 30_000,
            shutdown_grace_period_ms: 5_000,
            statement_logging: crate::config::StatementLogging::MetadataOnly,
            cluster_id: Some(cluster_id.to_string()),
            bootstrap,
            seed_nodes,
            snapshot_interval_entries: 100_000,
            snapshot_interval_bytes: 256 * 1024 * 1024,
            snapshot_min_elapsed_ms: 300_000,
            max_snapshot_file_bytes: 512 * 1024 * 1024,
            snapshot_chunk_bytes: 1024 * 1024,
        }
    }

    #[test]
    fn metadata_replica_assignment_is_deterministic_by_node_id() {
        let root = TempDir::new().unwrap();

        let config = config(
            &root,
            "cluster-a",
            true,
            vec![seed(30, 7300), seed(10, 7100), seed(20, 7200)],
        );

        let bootstrap = derive_metadata_bootstrap(&config).unwrap();

        assert_eq!(bootstrap.raft_group_id, METADATA_RAFT_GROUP_ID,);

        assert_eq!(
            bootstrap.replica_to_node.get(&ReplicaId(1)),
            Some(&NodeId(10)),
        );

        assert_eq!(
            bootstrap.replica_to_node.get(&ReplicaId(2)),
            Some(&NodeId(20)),
        );

        assert_eq!(
            bootstrap.replica_to_node.get(&ReplicaId(3)),
            Some(&NodeId(30)),
        );

        assert_eq!(
            bootstrap.initial_voters,
            BTreeSet::from([ReplicaId(1), ReplicaId(2), ReplicaId(3),]),
        );
    }

    #[test]
    fn restart_does_not_add_new_seed_to_durable_metadata_membership() {
        let root = TempDir::new().unwrap();
        let recovered = RecoveredRaftStorage::default();

        let first = config(
            &root,
            "cluster-a",
            true,
            vec![seed(10, 7100), seed(20, 7200), seed(30, 7300)],
        );

        let durable = resolve_metadata_bootstrap(&first, &recovered).unwrap();

        assert!(durable.installed_now);

        let restarted = config(
            &root,
            "cluster-a",
            false,
            vec![
                seed(10, 7100),
                seed(20, 7200),
                seed(30, 7300),
                seed(40, 7400),
            ],
        );

        let durable_after_restart = resolve_metadata_bootstrap(&restarted, &recovered).unwrap();

        assert!(!durable_after_restart.installed_now);

        assert_eq!(
            durable_after_restart.bootstrap.replica_to_node,
            durable.bootstrap.replica_to_node,
        );

        assert!(
            !durable_after_restart
                .bootstrap
                .replica_to_node
                .values()
                .any(|node_id| { *node_id == NodeId(40) })
        );
    }

    #[test]
    fn wrong_cluster_id_is_rejected_after_bootstrap() {
        let root = TempDir::new().unwrap();
        let recovered = RecoveredRaftStorage::default();

        let first = config(
            &root,
            "cluster-a",
            true,
            vec![seed(10, 7100), seed(20, 7200), seed(30, 7300)],
        );

        resolve_metadata_bootstrap(&first, &recovered).unwrap();

        let wrong = config(
            &root,
            "cluster-b",
            false,
            vec![seed(10, 7100), seed(20, 7200), seed(30, 7300)],
        );

        let error = resolve_metadata_bootstrap(&wrong, &recovered).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("data directory belongs to cluster cluster-a")
        );
    }
}
