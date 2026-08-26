//! Physical-node owner for the minimum Milestone 5 MultiRaft runtime.
//!
//! Phase 5.0 intentionally remains simple. It owns one group-tagged TCP
//! transport, one `NodeRaftWal` authority, local group registration/recovery,
//! and one node-level tick loop. Fair runnable queues and cross-group
//! persistence batching remain Phase 5.4.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use ragnordb_common::{Error, Result, ids::NodeId};
use ragnordb_multiraft::{
    bootstrap::FileBootstrapStore,
    host::{MultiRaftHost, MultiRaftHostError, RoutedRaftMessage},
    snapshot::SnapshotWorkController,
    storage::{
        codec::RaftReplicaIdentity,
        persistence::{NodeRaftWal, RaftWal},
        recovery::RecoveredRaftStorage,
    },
    transport::{NodeRaftEndpoint, NodeRaftTransport},
};
use ragnordb_tablet::snapshot::FileTabletSnapshotStore;
use wal::{io::directory::FsSegmentDirectory, wal::WalHandle};

use crate::{
    config::NodeConfig,
    data_directory_lock::DataDirectoryLock,
    database::SharedLocalDatabase,
    replicated_tablet::{ReplicatedTabletHandle, ReplicatedTabletRuntime},
    snapshot_transport::{NodeSnapshotEndpoint, NodeSnapshotTransport},
};

type LocalWal = WalHandle<FsSegmentDirectory, ()>;

const TICK_INTERVAL: Duration = Duration::from_millis(100);

pub struct MultiRaftRuntime {
    tablet_runtime: Option<ReplicatedTabletRuntime>,

    /// Explicitly retain ownership of the one physical snapshot transport for
    /// the complete node runtime lifetime.
    _snapshot_transport: NodeSnapshotTransport,

    shutdown: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl MultiRaftRuntime {
    /// Build initial `ConfState` authorities for every durable local group
    /// before the one shared-WAL recovery scan.
    ///
    /// Static seed configuration is never substituted for a missing durable
    /// bootstrap during restart.
    pub fn recovery_configurations(
        config: &NodeConfig,
        data_directory_lock: &DataDirectoryLock,
    ) -> Result<BTreeMap<RaftReplicaIdentity, raft::types::ConfState>> {
        if data_directory_lock.data_dir() != config.data_dir.as_path() {
            return Err(Error::Configuration(format!(
                "MultiRaft recovery lock protects {}, configured data directory is {}",
                data_directory_lock.data_dir().display(),
                config.data_dir.display(),
            )));
        }
        let store =
            FileBootstrapStore::open(config.data_dir.join("raft-bootstrap")).map_err(|source| {
                Error::RecoveryFailed {
                    reason: source.to_string(),
                }
            })?;

        let bootstraps =
            store
                .load_all_durable_bootstraps()
                .map_err(|source| Error::RecoveryFailed {
                    reason: source.to_string(),
                })?;

        let mut configurations = BTreeMap::new();

        for bootstrap in bootstraps.values() {
            if let Some(config_cluster_id) = config.cluster_id.as_ref()
                && &bootstrap.cluster_id != config_cluster_id
            {
                return Err(Error::RecoveryFailed {
                    reason: format!(
                        "durable Raft group {} belongs to cluster {}, configured cluster is {}",
                        bootstrap.raft_group_id.0, bootstrap.cluster_id, config_cluster_id,
                    ),
                });
            }

            let Some(replica_id) = bootstrap.replica_on_node(config.node_id) else {
                continue;
            };

            let identity = RaftReplicaIdentity::new(bootstrap.raft_group_id, replica_id).map_err(
                |source| Error::RecoveryFailed {
                    reason: source.to_string(),
                },
            )?;

            let conf_state =
                bootstrap
                    .to_core_conf_state()
                    .map_err(|source| Error::RecoveryFailed {
                        reason: source.to_string(),
                    })?;

            configurations.insert(identity, conf_state);
        }

        Ok(configurations)
    }

    pub fn start_from_shared_recovery(
        config: &NodeConfig,
        wal: LocalWal,
        database: SharedLocalDatabase,
        recovered: RecoveredRaftStorage,
    ) -> Result<Self> {
        let local_seed = config
            .seed_nodes
            .iter()
            .find(|seed| seed.id == config.node_id)
            .ok_or_else(|| {
                Error::Configuration("local node is missing from seed_nodes".to_string())
            })?;

        let node_addresses = config
            .seed_nodes
            .iter()
            .filter(|seed| seed.id != config.node_id)
            .map(|seed| (seed.id, seed.raft_addr))
            .collect::<BTreeMap<NodeId, _>>();

        let snapshot_addresses = config
            .seed_nodes
            .iter()
            .filter(|seed| seed.id != config.node_id)
            .map(|seed| (seed.id, seed.snapshot_addr))
            .collect::<BTreeMap<NodeId, _>>();

        let NodeRaftEndpoint {
            transport,
            inbound,
            local_addr,
        } = NodeRaftTransport::bind(config.node_id, local_seed.raft_addr, node_addresses)
            .map_err(|source| Error::Configuration(format!("bind MultiRaft endpoint: {source}")))?;

        // Snapshot files are already group-qualified on disk. One store
        // instance therefore safely owns cleanup, allocation, and publication
        // for every tablet group on this physical node.
        let snapshot_store = Arc::new(
            FileTabletSnapshotStore::new(
                config.data_dir.join("tablet-snapshots"),
                config.max_snapshot_file_bytes,
            )
            .map_err(|source| Error::RecoveryFailed {
                reason: source.to_string(),
            })?,
        );

        // Snapshot work is bounded at node scope rather than independently per
        // tablet group.
        let snapshot_work = SnapshotWorkController::default();

        let NodeSnapshotEndpoint {
            transport: snapshot_transport,
            local_addr: snapshot_addr,
        } = NodeSnapshotTransport::bind(
            local_seed.snapshot_addr,
            snapshot_addresses,
            snapshot_work.clone(),
            config.snapshot_chunk_bytes,
        )
        .map_err(|source| Error::Configuration(format!("bind node snapshot endpoint: {source}")))?;

        let durability_gate = database
            .try_lock()
            .map_err(|_| {
                Error::Configuration("database is busy during MultiRaft WAL setup".to_string())
            })?
            .durability_gate();

        let node_wal = NodeRaftWal::with_durability_gate(wal.clone(), durability_gate);
        let mut host = MultiRaftHost::from_recovered(config.node_id, node_wal, &recovered);

        let bootstrap = ReplicatedTabletRuntime::resolve_tablet_bootstrap(config, &recovered)?;

        let local_replica_id = bootstrap.replica_on_node(config.node_id).ok_or_else(|| {
            Error::Configuration(format!(
                "node {} has no replica in Raft group {}",
                config.node_id.0, bootstrap.raft_group_id.0,
            ))
        })?;

        let identity = RaftReplicaIdentity::new(bootstrap.raft_group_id, local_replica_id)
            .map_err(|source| Error::Configuration(source.to_string()))?;

        let group_wal = host.issue_group_writer(identity).map_err(host_error)?;
        let group_transport = transport.register_group(&bootstrap).map_err(|source| {
            Error::Configuration(format!("register Raft group transport: {source}"))
        })?;

        let group_snapshot_endpoint = snapshot_transport
            .register_group(
                bootstrap.raft_group_id,
                local_replica_id,
                snapshot_store.clone(),
            )
            .map_err(|source| {
                Error::Configuration(format!(
                    "register snapshot route for Raft group {}: {source}",
                    bootstrap.raft_group_id.0,
                ))
            })?;

        // Bootstrap persistence may finish before registration, but its first
        // messages cannot leave the node until host activation seals retention.
        let start_gate = Arc::new(AtomicBool::new(false));

        let tablet_runtime = ReplicatedTabletRuntime::start_hosted_from_shared_recovery(
            config,
            wal,
            database.clone(),
            bootstrap,
            group_wal,
            group_transport,
            snapshot_store,
            snapshot_work,
            group_snapshot_endpoint,
            &recovered,
            Arc::clone(&start_gate),
        )?;

        let hosted_group = Box::new(tablet_runtime.hosted_group());

        if recovered.replica(identity).is_some() {
            host.register_recovered_group(hosted_group)
                .map_err(host_error)?;
        } else {
            host.register_new_group(hosted_group).map_err(host_error)?;
        }

        // Every recovered replica lifetime must be accounted for before the
        // retention registry can be sealed. No database or Raft retention
        // movement is legal before this boundary.
        host.activate().map_err(host_error)?;

        // Only after host activation has sealed the complete replica registry
        // may the database publish its checkpoint retention floor. The database
        // and all Raft groups now observe the same physical NodeRaftWal state.
        database
            .try_lock()
            .map_err(|_| {
                Error::Configuration(
                    "database is busy while installing node-wide Raft WAL".to_string(),
                )
            })?
            .install_node_wal(host.node_wal())?;

        // Group workers may have persisted their bootstrap Ready while startup
        // was private, but no Ready-generated message may leave the node until
        // both replica registration and database retention ownership are live.
        start_gate.store(true, Ordering::Release);

        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown);
        let worker = thread::Builder::new()
            .name("ragnordb-multiraft-host".to_string())
            .spawn(move || run_host(host, transport, inbound, worker_shutdown))
            .map_err(|source| Error::Configuration(format!("spawn MultiRaft host: {source}")))?;

        tracing::info!(
            node_id = config.node_id.0,
            raft = %local_addr,
            snapshot = %snapshot_addr,
            "minimum MultiRaft host started",
        );

        Ok(Self {
            tablet_runtime: Some(tablet_runtime),
            _snapshot_transport: snapshot_transport,
            shutdown,
            worker: Some(worker),
        })
    }

    pub fn handle(&self) -> Arc<ReplicatedTabletHandle> {
        self.tablet_runtime
            .as_ref()
            .expect("tablet runtime exists while MultiRaft runtime is active")
            .handle()
    }
}

fn run_host<W>(
    mut host: MultiRaftHost<W>,
    transport: NodeRaftTransport,
    inbound: std::sync::mpsc::Receiver<RoutedRaftMessage>,
    shutdown: Arc<AtomicBool>,
) where
    W: RaftWal,
{
    let mut next_tick = Instant::now() + TICK_INTERVAL;

    while !shutdown.load(Ordering::Acquire) {
        while let Ok(message) = inbound.try_recv() {
            match host.route(message) {
                Ok(outbound) => send_outbound(&transport, outbound),
                Err(MultiRaftHostError::RecoveryRequired) => {
                    tracing::error!("shared Raft WAL requires node recovery");
                    return;
                }
                Err(error) => tracing::warn!(
                    error = %error,
                    "Raft group message was rejected",
                ),
            }
        }

        let now = Instant::now();
        if now >= next_tick {
            match host.tick_all(1) {
                Ok(outbound) => send_outbound(&transport, outbound),
                Err(MultiRaftHostError::RecoveryRequired) => {
                    tracing::error!("shared Raft WAL requires node recovery");
                    return;
                }
                Err(error) => tracing::warn!(error = %error, "MultiRaft tick failed"),
            }

            next_tick = now + TICK_INTERVAL;
        }

        thread::sleep(Duration::from_millis(2));
    }
}

fn send_outbound(transport: &NodeRaftTransport, messages: Vec<RoutedRaftMessage>) {
    for message in messages {
        if let Err(source) = transport.try_send(message) {
            tracing::warn!(
                error = %source,
                "Raft message could not be delivered; Raft will retry",
            );
        }
    }
}

fn host_error(error: MultiRaftHostError) -> Error {
    match error {
        MultiRaftHostError::RecoveryRequired => Error::RecoveryFailed {
            reason: "shared Raft WAL requires recovery".to_string(),
        },
        other => Error::Configuration(other.to_string()),
    }
}

impl Drop for MultiRaftRuntime {
    fn drop(&mut self) {
        // Stop physical transport and ticking before stopping group workers.
        self.shutdown.store(true, Ordering::Release);

        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }

        drop(self.tablet_runtime.take());
    }
}
