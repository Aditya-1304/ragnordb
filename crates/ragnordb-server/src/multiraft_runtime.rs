//! Physical-node owner for the minimum Milestone 5 MultiRaft runtime.
//!
//! Phase 5.1a adds the metadata Raft group through exactly the same host,
//! transport, and shared-WAL ownership boundary already established by Phase
//! 5.0.
//!
//! Static seed configuration may create the metadata group's durable bootstrap
//! exactly once. Restart membership comes from that durable bootstrap plus
//! committed Raft ConfState, never from the current seed voter list.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use ragnordb_catalog::{Catalog, MetadataApplyOutcome};
use ragnordb_common::{
    Error, Result,
    ids::{NodeId, RequestId},
    metadata_codec::{
        CreateTableRequest, MetadataCommand, MetadataCommandEnvelope, NodeDescriptor,
        TabletDescriptor,
    },
};

use ragnordb_multiraft::{
    bootstrap::FileBootstrapStore,
    host::{
        MultiRaftHost, MultiRaftHostConfig, MultiRaftHostError, MultiRaftHostStatus,
        MultiRaftTurnBudget, RoutedRaftMessage, SharedMultiRaftHostStatus,
    },
    meta::{MetadataRuntimeHandle, bootstrap_metadata_group, recover_metadata_group},
    snapshot::SnapshotWorkController,
    storage::{
        codec::RaftReplicaIdentity,
        persistence::{NodeRaftWal, RaftWal},
        recovery::RecoveredRaftStorage,
    },
    transport::{NodeRaftEndpoint, NodeRaftInbound, NodeRaftTransport, NodeRaftTransportConfig},
};

use ragnordb_exec::{MetadataTableCreator, MetadataTableTopology, SharedMetadataTableCreator};
use ragnordb_tablet::snapshot::FileTabletSnapshotStore;

use wal::{io::directory::FsSegmentDirectory, wal::WalHandle};

use crate::{
    bootstrap::{METADATA_RAFT_GROUP_ID, metadata_seed_descriptors, resolve_metadata_bootstrap},
    config::NodeConfig,
    data_directory_lock::DataDirectoryLock,
    database::SharedLocalDatabase,
    replica_registry::{LocalReplicaKey, LocalReplicaRegistry},
    replicated_tablet::{ReplicatedTabletHandle, ReplicatedTabletRuntime},
    snapshot_transport::{NodeSnapshotEndpoint, NodeSnapshotTransport},
};

type LocalWal = WalHandle<FsSegmentDirectory, ()>;

const TICK_INTERVAL: Duration = Duration::from_millis(100);

const HOST_GROUP_BUDGET: usize = 64;

const HOST_MESSAGE_BUDGET: usize = 256;

const METADATA_ELECTION_TIMEOUT_TICKS: u64 = 10;

const METADATA_HEARTBEAT_INTERVAL_TICKS: u64 = 3;

const METADATA_BOOTSTRAP_RETRY_INTERVAL: Duration = Duration::from_millis(250);

const METADATA_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

const METADATA_REQUEST_CHANNEL_CAPACITY: usize = 1024;

const METADATA_REQUEST_BUDGET: usize = 64;

enum MetadataHostRequest {
    CreateTable {
        envelope: MetadataCommandEnvelope,
        reply: mpsc::Sender<Result<MetadataApplyOutcome>>,
        deadline: Instant,
    },
}

struct PendingMetadataProposal {
    request_id: RequestId,
    reply: mpsc::Sender<Result<MetadataApplyOutcome>>,
    deadline: Instant,
}

/// Client-side proposal boundary for metadata-owned SQL schema operations.
///
/// The SQL executor never allocates a table identity and never writes through
/// the legacy catalog WAL when this client is installed. A request is accepted
/// only after the metadata host has correlated the committed Raft apply result.
pub struct MetadataProposalClient {
    requests: mpsc::SyncSender<MetadataHostRequest>,
    metadata: MetadataRuntimeHandle,
}

impl MetadataProposalClient {
    fn new(
        requests: mpsc::SyncSender<MetadataHostRequest>,
        metadata: MetadataRuntimeHandle,
    ) -> Self {
        Self { requests, metadata }
    }

    fn table_topology_for_outcome(
        &self,
        outcome: MetadataApplyOutcome,
    ) -> Result<MetadataTableTopology> {
        let MetadataApplyOutcome::TableCreated(created) = outcome else {
            return match outcome {
                MetadataApplyOutcome::Rejected(rejection) => {
                    Err(Error::ConstraintViolation(rejection.to_string()))
                }

                MetadataApplyOutcome::Applied | MetadataApplyOutcome::AlreadyApplied => {
                    Err(Error::CorruptData(
                        "metadata CREATE TABLE apply did not return allocated topology".to_string(),
                    ))
                }

                MetadataApplyOutcome::TableCreated(_) => unreachable!(),
            };
        };

        let state = self.metadata.state_snapshot();
        let table = state.table(created.table_id).ok_or_else(|| {
            Error::CorruptData(format!(
                "metadata CREATE TABLE returned table {} without a table definition",
                created.table_id.0,
            ))
        })?;

        let created_tablet = state.tablet(created.tablet_id).ok_or_else(|| {
            Error::CorruptData(format!(
                "metadata CREATE TABLE returned tablet {} without a tablet descriptor",
                created.tablet_id.0,
            ))
        })?;

        if created_tablet.table_id != created.table_id
            || created_tablet.raft_group_id != created.raft_group_id
        {
            return Err(Error::CorruptData(format!(
                "metadata CREATE TABLE returned topology inconsistent with tablet {}",
                created.tablet_id.0,
            )));
        }

        let tablets = state.tablets_for_table(created.table_id);
        if tablets.len() != table.tablet_count as usize {
            return Err(Error::CorruptData(format!(
                "metadata table {} declares {} tablets but state exposes {} descriptors",
                created.table_id.0,
                table.tablet_count,
                tablets.len()
            )));
        }

        for tablet in &tablets {
            if state.desired_placement(tablet.tablet_id).is_none() {
                return Err(Error::CorruptData(format!(
                    "metadata table {} returned tablet {} without desired placement",
                    created.table_id.0, tablet.tablet_id.0
                )));
            }
        }

        Ok(MetadataTableTopology {
            definition: table.to_definition(),
            tablets,
        })
    }

    fn propose_create_table_topology(
        &self,
        request: CreateTableRequest,
        request_id: RequestId,
        timeout: Duration,
    ) -> Result<MetadataTableTopology> {
        let command = MetadataCommand::CreateTableTopology(request);
        let envelope = MetadataCommandEnvelope::new(request_id.clone(), command)
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::InvalidArgument("metadata request deadline overflowed".into()))?;
        let (reply, response) = mpsc::channel();

        self.requests
            .try_send(MetadataHostRequest::CreateTable {
                envelope,
                reply,
                deadline,
            })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => Error::ProposalUnavailable {
                    reason: "metadata proposal queue is full".to_string(),
                },
                mpsc::TrySendError::Disconnected(_) => Error::ProposalUnavailable {
                    reason: "metadata Raft host is not running".to_string(),
                },
            })?;

        let remaining = deadline.saturating_duration_since(Instant::now());
        let outcome = response
            .recv_timeout(remaining)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => Error::ProposalUnavailable {
                    reason: "metadata CREATE TABLE deadline elapsed before apply".to_string(),
                },
                mpsc::RecvTimeoutError::Disconnected => Error::ProposalUnavailable {
                    reason: "metadata Raft host stopped before CREATE TABLE applied".to_string(),
                },
            })??;

        self.table_topology_for_outcome(outcome)
    }
}

impl MetadataTableCreator for MetadataProposalClient {
    fn create_table(
        &self,
        request: CreateTableRequest,
        request_id: RequestId,
        timeout: Duration,
    ) -> Result<ragnordb_common::catalog_codec::TableDefinition> {
        self.propose_create_table_topology(request, request_id, timeout)
            .map(|topology| topology.definition)
    }

    fn table_descriptors(
        &self,
        table_id: ragnordb_common::ids::TableId,
    ) -> Result<Vec<TabletDescriptor>> {
        let state = self.metadata.state_snapshot();
        let table = state.table(table_id).ok_or_else(|| {
            Error::CorruptData(format!(
                "metadata table {} is missing from the committed state",
                table_id.0
            ))
        })?;
        let tablets = state.tablets_for_table(table_id);

        if tablets.len() != table.tablet_count as usize {
            return Err(Error::CorruptData(format!(
                "metadata table {} declares {} tablets but state exposes {} descriptors",
                table_id.0,
                table.tablet_count,
                tablets.len()
            )));
        }

        for tablet in &tablets {
            if state.desired_placement(tablet.tablet_id).is_none() {
                return Err(Error::CorruptData(format!(
                    "metadata table {} returned tablet {} without desired placement",
                    table_id.0, tablet.tablet_id.0
                )));
            }
        }

        Ok(tablets)
    }

    fn create_table_topology(
        &self,
        request: CreateTableRequest,
        request_id: RequestId,
        timeout: Duration,
    ) -> Result<MetadataTableTopology> {
        self.propose_create_table_topology(request, request_id, timeout)
    }

    fn list_tables(&self) -> Vec<ragnordb_common::catalog_codec::TableDefinition> {
        self.metadata
            .state_snapshot()
            .list_tables()
            .into_iter()
            .map(|table| table.to_definition())
            .collect()
    }
}

pub struct MultiRaftRuntime {
    tablet_runtime: Option<ReplicatedTabletRuntime>,

    metadata: MetadataRuntimeHandle,

    metadata_creator: SharedMetadataTableCreator,

    host_status: SharedMultiRaftHostStatus,

    /// Explicitly retain ownership of the one physical snapshot transport for
    /// the complete node runtime lifetime.
    _snapshot_transport: NodeSnapshotTransport,

    shutdown: Arc<AtomicBool>,

    worker: Option<thread::JoinHandle<()>>,
}

impl MultiRaftRuntime {
    /// Build initial ConfState authorities for every durable local group before
    /// the one shared-WAL recovery scan.
    ///
    /// A changed static seed list is never substituted for an existing durable
    /// bootstrap.
    pub fn recovery_configurations(
        config: &NodeConfig,
        data_directory_lock: &DataDirectoryLock,
    ) -> Result<BTreeMap<RaftReplicaIdentity, raft::types::ConfState>> {
        if data_directory_lock.data_dir() != config.data_dir.as_path() {
            return Err(Error::Configuration(format!(
                "MultiRaft recovery lock protects {}, \
                         configured data directory is {}",
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

        // Load the node-local lifecycle authority before deriving any Raft
        // recovery configuration. A corrupt registry or a registry belonging
        // to another cluster must fail startup closed rather than allowing
        // WAL recovery to proceed from an incomplete local view.
        if let Some(cluster_id) = config.cluster_id.as_deref() {
            LocalReplicaRegistry::open(config.data_dir.join("replica-registry.json"), cluster_id)?;
        }

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
        let cluster_id = config.cluster_id.clone().ok_or_else(|| {
            Error::Configuration("replicated MultiRaft runtime requires cluster_id".to_string())
        })?;

        let registry =
            LocalReplicaRegistry::open(config.data_dir.join("replica-registry.json"), &cluster_id)?;
        registry.validate_recovered_lifetimes(recovered.replicas().map(|(identity, _)| {
            LocalReplicaKey {
                raft_group_id: identity.raft_group_id,
                replica_id: identity.replica_id,
            }
        }))?;

        let local_seed = config
            .seed_nodes
            .iter()
            .find(|seed| seed.id == config.node_id)
            .ok_or_else(|| {
                Error::Configuration("local node is missing from seed_nodes".to_string())
            })?;

        // Resolve/install metadata bootstrap BEFORE creating its Raft core.
        //
        // On restart this returns the already durable initial membership and
        // does not reconcile it against today's seed voter list.
        let resolved_metadata = resolve_metadata_bootstrap(config, &recovered)?;

        let metadata_bootstrap = resolved_metadata.bootstrap.clone();

        let metadata_nodes = metadata_seed_descriptors(config, &metadata_bootstrap)?;

        let metadata_replica_id = resolved_metadata.local_replica_id;

        let metadata_identity =
            RaftReplicaIdentity::new(METADATA_RAFT_GROUP_ID, metadata_replica_id)
                .map_err(|source| Error::Configuration(source.to_string()))?;

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
        } = NodeRaftTransport::bind_with_config(
            config.node_id,
            local_seed.raft_addr,
            node_addresses,
            NodeRaftTransportConfig::default().with_cluster_id(cluster_id.clone()),
        )
        .map_err(|source| Error::Configuration(format!("bind MultiRaft endpoint: {source}")))?;

        let snapshot_store = Arc::new(
            FileTabletSnapshotStore::new(
                config.data_dir.join("tablet-snapshots"),
                config.max_snapshot_file_bytes,
            )
            .map_err(|source| Error::RecoveryFailed {
                reason: source.to_string(),
            })?,
        );

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

        let mut host = MultiRaftHost::from_recovered_with_config(
            config.node_id,
            node_wal,
            &recovered,
            MultiRaftHostConfig::default(),
        )
        .map_err(host_error)?;

        // --------------------------------------------------------------
        // Metadata group.
        // --------------------------------------------------------------

        let metadata_group_wal = host
            .issue_group_writer(metadata_identity)
            .map_err(host_error)?;

        transport
            .register_group(&metadata_bootstrap)
            .map_err(|source| {
                Error::Configuration(format!("register metadata Raft transport: {source}"))
            })?;

        let metadata_snapshot_root = config.data_dir.join("metadata-raft-snapshots");

        let (metadata_group, metadata_handle) = match recovered.replica(metadata_identity) {
            Some(recovered_replica) => recover_metadata_group(
                &metadata_bootstrap,
                metadata_replica_id,
                metadata_group_wal,
                wal.durable_lsn(),
                recovered_replica,
                metadata_snapshot_root,
                METADATA_ELECTION_TIMEOUT_TICKS,
                METADATA_HEARTBEAT_INTERVAL_TICKS,
            )
            .map_err(|source| Error::RecoveryFailed {
                reason: format!("recover metadata Raft group: {source}"),
            })?,

            None => bootstrap_metadata_group(
                &metadata_bootstrap,
                metadata_replica_id,
                metadata_group_wal,
                metadata_snapshot_root,
                METADATA_ELECTION_TIMEOUT_TICKS,
                METADATA_HEARTBEAT_INTERVAL_TICKS,
            )
            .map_err(|source| Error::RecoveryFailed {
                reason: format!("bootstrap metadata Raft group: {source}"),
            })?,
        };

        if recovered.replica(metadata_identity).is_some() {
            host.register_recovered_group(metadata_group)
                .map_err(host_error)?;
        } else {
            host.register_new_group(metadata_group)
                .map_err(host_error)?;
        }

        // --------------------------------------------------------------
        // Existing legacy tablet group.
        // --------------------------------------------------------------

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

        // --------------------------------------------------------------
        // One physical activation boundary.
        // --------------------------------------------------------------

        host.activate().map_err(host_error)?;

        let host_status = Arc::new(RwLock::new(host.status()));

        database
            .try_lock()
            .map_err(|_| {
                Error::Configuration(
                    "database is busy while installing node-wide Raft WAL".to_string(),
                )
            })?
            .install_node_wal(host.node_wal())?;

        // Tablet workers may now release any Ready-dependent messages.
        start_gate.store(true, Ordering::Release);

        let shutdown = Arc::new(AtomicBool::new(false));

        let (metadata_request_tx, metadata_request_rx) =
            mpsc::sync_channel(METADATA_REQUEST_CHANNEL_CAPACITY);
        let metadata_creator: SharedMetadataTableCreator = Arc::new(MetadataProposalClient::new(
            metadata_request_tx,
            metadata_handle.clone(),
        ));

        let worker_shutdown = Arc::clone(&shutdown);

        let worker_metadata = metadata_handle.clone();
        let worker_host_status = Arc::clone(&host_status);

        let (metadata_ready_tx, metadata_ready_rx) = mpsc::sync_channel(1);

        let worker = thread::Builder::new()
            .name("ragnordb-multiraft-host".to_string())
            .spawn(move || {
                run_host(
                    host,
                    transport,
                    inbound,
                    metadata_request_rx,
                    worker_shutdown,
                    worker_metadata,
                    worker_host_status,
                    cluster_id,
                    metadata_nodes,
                    metadata_ready_tx,
                )
            })
            .map_err(|source| Error::Configuration(format!("spawn MultiRaft host: {source}")))?;

        // Client-facing SQL/admin listeners are bound only after this function
        // returns. Therefore a replicated node never advertises successful
        // startup before ClusterInitialized and the initial physical-node
        // directory have actually committed and applied.
        let metadata_start = metadata_ready_rx.recv_timeout(METADATA_STARTUP_TIMEOUT);

        match metadata_start {
            Ok(Ok(())) => {}

            Ok(Err(reason)) => {
                shutdown.store(true, Ordering::Release);

                let _ = worker.join();

                return Err(Error::RecoveryFailed {
                    reason: format!("metadata initialization failed: {reason}"),
                });
            }

            Err(error) => {
                shutdown.store(true, Ordering::Release);

                let _ = worker.join();

                return Err(Error::RecoveryFailed {
                    reason: format!(
                        "metadata initialization did not complete before startup deadline: {error}"
                    ),
                });
            }
        }

        // Bootstrap proposal completions are startup-local. Phase 5.2 begins
        // with a clean queue for externally submitted metadata operations.
        let _ = metadata_handle.take_applied_results();

        tracing::info!(
            node_id = config.node_id.0,
            raft = %local_addr,
            snapshot = %snapshot_addr,
            metadata_group_id =
                METADATA_RAFT_GROUP_ID.0,
            metadata_replica_id =
                metadata_replica_id.0,
            metadata_bootstrap_installed =
                resolved_metadata
                    .installed_now,
            "MultiRaft host started with metadata group",
        );

        Ok(Self {
            tablet_runtime: Some(tablet_runtime),

            metadata: metadata_handle,

            metadata_creator,

            host_status,

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

    /// Read-only committed metadata publication.
    ///
    /// SQL schema analysis uses the publication to refresh its local cache;
    /// proposal completion remains correlated through the metadata host's
    /// request channel.
    pub fn metadata_handle(&self) -> MetadataRuntimeHandle {
        self.metadata.clone()
    }

    /// Return the metadata-owned CREATE TABLE boundary for the SQL executor.
    pub fn metadata_table_creator(&self) -> SharedMetadataTableCreator {
        self.metadata_creator.clone()
    }

    /// Return the last published point-in-time status for every local Raft
    /// group. The host worker remains the sole owner of mutable Raft state.
    pub fn host_status(&self) -> MultiRaftHostStatus {
        self.host_status
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn host_status_handle(&self) -> SharedMultiRaftHostStatus {
        Arc::clone(&self.host_status)
    }
}

#[allow(clippy::too_many_arguments)]
fn run_host<W>(
    mut host: MultiRaftHost<W>,
    transport: NodeRaftTransport,
    inbound: NodeRaftInbound,
    metadata_requests: mpsc::Receiver<MetadataHostRequest>,
    shutdown: Arc<AtomicBool>,
    metadata: MetadataRuntimeHandle,
    host_status: SharedMultiRaftHostStatus,
    cluster_id: String,
    metadata_nodes: Vec<NodeDescriptor>,
    metadata_ready: mpsc::SyncSender<std::result::Result<(), String>>,
) where
    W: RaftWal,
{
    let mut next_tick = Instant::now() + TICK_INTERVAL;

    publish_host_status(&host_status, &host);

    let mut pending_metadata = BTreeMap::<u64, PendingMetadataProposal>::new();
    let mut pending_metadata_by_request = HashMap::<RequestId, u64>::new();

    let mut next_metadata_attempt = Instant::now();

    let mut startup_sender = Some(metadata_ready);

    while !shutdown.load(Ordering::Acquire) {
        if !service_metadata_requests(
            &mut host,
            &transport,
            &metadata_requests,
            &mut pending_metadata,
            &mut pending_metadata_by_request,
            METADATA_REQUEST_BUDGET,
        ) {
            publish_host_status(&host_status, &host);
            fail_pending_metadata(
                &mut pending_metadata,
                &mut pending_metadata_by_request,
                Error::RecoveryRequired {
                    reason: "metadata host entered recovery-required state".to_string(),
                },
            );
            return;
        }

        let mut admitted_messages = 0;
        while admitted_messages < HOST_MESSAGE_BUDGET {
            let Ok(message) = inbound.try_recv() else {
                break;
            };

            match host.enqueue_message(message) {
                Ok(()) => admitted_messages += 1,

                Err(MultiRaftHostError::RecoveryRequired) => {
                    publish_host_status(&host_status, &host);
                    signal_metadata_failure(
                        &mut startup_sender,
                        "shared Raft WAL requires node recovery".to_string(),
                    );

                    tracing::error!("shared Raft WAL requires node recovery");

                    fail_pending_metadata(
                        &mut pending_metadata,
                        &mut pending_metadata_by_request,
                        Error::RecoveryRequired {
                            reason: "shared Raft WAL requires node recovery".to_string(),
                        },
                    );

                    return;
                }

                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "Raft group message was rejected",
                    );
                }
            }
        }

        match host.run_turn(
            0,
            MultiRaftTurnBudget {
                max_groups: HOST_GROUP_BUDGET,
                max_messages: HOST_MESSAGE_BUDGET,
                ..MultiRaftTurnBudget::default()
            },
        ) {
            Ok(turn) => send_outbound(&transport, turn.outbound),

            Err(MultiRaftHostError::RecoveryRequired) => {
                publish_host_status(&host_status, &host);
                signal_metadata_failure(
                    &mut startup_sender,
                    "shared Raft WAL requires node recovery".to_string(),
                );

                tracing::error!("shared Raft WAL requires node recovery");

                fail_pending_metadata(
                    &mut pending_metadata,
                    &mut pending_metadata_by_request,
                    Error::RecoveryRequired {
                        reason: "shared Raft WAL requires node recovery".to_string(),
                    },
                );

                return;
            }

            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "bounded MultiRaft host turn failed",
                );
            }
        }

        let now = Instant::now();

        if now >= next_tick {
            match host.tick_all(1) {
                Ok(outbound) => {
                    send_outbound(&transport, outbound);
                }

                Err(MultiRaftHostError::RecoveryRequired) => {
                    publish_host_status(&host_status, &host);
                    signal_metadata_failure(
                        &mut startup_sender,
                        "shared Raft WAL requires node recovery".to_string(),
                    );

                    tracing::error!("shared Raft WAL requires node recovery");

                    fail_pending_metadata(
                        &mut pending_metadata,
                        &mut pending_metadata_by_request,
                        Error::RecoveryRequired {
                            reason: "shared Raft WAL requires node recovery".to_string(),
                        },
                    );

                    return;
                }

                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "MultiRaft tick failed",
                    );
                }
            }

            next_tick = now + TICK_INTERVAL;
        }

        drain_metadata_results(
            &metadata,
            &mut pending_metadata,
            &mut pending_metadata_by_request,
        );
        expire_metadata_proposals(&mut pending_metadata, &mut pending_metadata_by_request, now);

        if startup_sender.is_some() && now >= next_metadata_attempt {
            match next_metadata_bootstrap_command(&metadata, &cluster_id, &metadata_nodes) {
                Ok(None) => {
                    if let Some(sender) = startup_sender.take() {
                        let _ = sender.send(Ok(()));
                    }
                }

                Ok(Some(command)) => {
                    match command.encode() {
                        Ok(encoded) => {
                            let encoded_len = encoded.len();

                            match host.propose(METADATA_RAFT_GROUP_ID, encoded, encoded_len) {
                                Ok(proposal) => {
                                    send_outbound(&transport, proposal.outbound);
                                }

                                // Followers reject proposals and a retryable
                                // persistence boundary may delay one attempt.
                                // Neither condition is startup corruption.
                                Err(MultiRaftHostError::GroupRejected {
                                    raft_group_id, ..
                                })
                                | Err(MultiRaftHostError::GroupRetryable {
                                    raft_group_id, ..
                                }) if raft_group_id == METADATA_RAFT_GROUP_ID => {}

                                Err(MultiRaftHostError::RecoveryRequired) => {
                                    signal_metadata_failure(
                                        &mut startup_sender,
                                        "shared Raft WAL requires recovery while initializing metadata"
                                            .to_string(),
                                    );

                                    return;
                                }

                                Err(error) => {
                                    signal_metadata_failure(
                                        &mut startup_sender,
                                        format!("metadata bootstrap proposal failed: {error}"),
                                    );

                                    return;
                                }
                            }
                        }

                        Err(error) => {
                            signal_metadata_failure(
                                &mut startup_sender,
                                format!("encode metadata bootstrap command: {error}"),
                            );

                            return;
                        }
                    }
                }

                Err(reason) => {
                    signal_metadata_failure(&mut startup_sender, reason);

                    return;
                }
            }

            next_metadata_attempt = now + METADATA_BOOTSTRAP_RETRY_INTERVAL;
        }

        publish_host_status(&host_status, &host);

        thread::sleep(Duration::from_millis(2));
    }
}

fn publish_host_status(status: &SharedMultiRaftHostStatus, host: &MultiRaftHost<impl RaftWal>) {
    *status
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = host.status();
}

fn service_metadata_requests<W>(
    host: &mut MultiRaftHost<W>,
    transport: &NodeRaftTransport,
    requests: &mpsc::Receiver<MetadataHostRequest>,
    pending: &mut BTreeMap<u64, PendingMetadataProposal>,
    pending_by_request: &mut HashMap<RequestId, u64>,
    max_requests: usize,
) -> bool
where
    W: RaftWal,
{
    let mut serviced = 0;
    while serviced < max_requests {
        let Ok(request) = requests.try_recv() else {
            break;
        };
        serviced += 1;

        match request {
            MetadataHostRequest::CreateTable {
                envelope,
                reply,
                deadline,
            } => {
                let request_id = envelope.request_id.clone();

                if pending_by_request.contains_key(&request_id) {
                    let _ = reply.send(Err(Error::ProposalUnavailable {
                        reason: "metadata request is already pending".to_string(),
                    }));
                    continue;
                }

                if deadline <= Instant::now() {
                    let _ = reply.send(Err(Error::ProposalUnavailable {
                        reason: "metadata CREATE TABLE deadline elapsed before proposal"
                            .to_string(),
                    }));
                    continue;
                }

                let encoded = match envelope.encode() {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        let _ = reply.send(Err(Error::InvalidArgument(error.to_string())));
                        continue;
                    }
                };
                let encoded_len = encoded.len();

                match host.propose(METADATA_RAFT_GROUP_ID, encoded, encoded_len) {
                    Ok(proposal) => {
                        let index = proposal.index;
                        pending.insert(
                            index,
                            PendingMetadataProposal {
                                request_id: request_id.clone(),
                                reply,
                                deadline,
                            },
                        );
                        pending_by_request.insert(request_id, index);
                        send_outbound(transport, proposal.outbound);
                    }

                    Err(MultiRaftHostError::GroupRejected { raft_group_id, .. })
                        if raft_group_id == METADATA_RAFT_GROUP_ID =>
                    {
                        let _ = reply.send(Err(Error::NotLeader { leader_id: None }));
                    }

                    Err(MultiRaftHostError::GroupRetryable { reason, .. }) => {
                        let _ = reply.send(Err(Error::ProposalUnavailable { reason }));
                    }

                    Err(MultiRaftHostError::RecoveryRequired) => {
                        let _ = reply.send(Err(Error::RecoveryRequired {
                            reason: "shared Raft WAL requires node recovery".to_string(),
                        }));
                        return false;
                    }

                    Err(error) => {
                        let _ = reply.send(Err(Error::ProposalUnavailable {
                            reason: error.to_string(),
                        }));
                    }
                }
            }
        }
    }

    true
}

fn drain_metadata_results(
    metadata: &MetadataRuntimeHandle,
    pending: &mut BTreeMap<u64, PendingMetadataProposal>,
    pending_by_request: &mut HashMap<RequestId, u64>,
) {
    for applied in metadata.take_applied_results() {
        let Some(proposal) = pending.remove(&applied.index) else {
            // Startup entries, timed-out requests, and proposals admitted by a
            // prior process lifetime are intentionally not client responses.
            continue;
        };

        pending_by_request.remove(&proposal.request_id);

        let result = if applied.request_id.as_ref() != Some(&proposal.request_id) {
            Err(Error::CorruptData(format!(
                "metadata apply result at index {} carried the wrong request identity",
                applied.index,
            )))
        } else {
            match applied.outcome {
                MetadataApplyOutcome::Rejected(rejection) => {
                    Err(Error::ConstraintViolation(rejection.to_string()))
                }
                outcome => Ok(outcome),
            }
        };

        let _ = proposal.reply.send(result);
    }
}

fn expire_metadata_proposals(
    pending: &mut BTreeMap<u64, PendingMetadataProposal>,
    pending_by_request: &mut HashMap<RequestId, u64>,
    now: Instant,
) {
    let expired = pending
        .iter()
        .filter_map(|(index, proposal)| (proposal.deadline <= now).then_some(*index))
        .collect::<Vec<_>>();

    for index in expired {
        let Some(proposal) = pending.remove(&index) else {
            continue;
        };

        pending_by_request.remove(&proposal.request_id);
        let _ = proposal.reply.send(Err(Error::ProposalUnavailable {
            reason: "metadata CREATE TABLE deadline elapsed before apply".to_string(),
        }));
    }
}

fn fail_pending_metadata(
    pending: &mut BTreeMap<u64, PendingMetadataProposal>,
    pending_by_request: &mut HashMap<RequestId, u64>,
    error: Error,
) {
    let proposals = std::mem::take(pending);
    pending_by_request.clear();

    for proposal in proposals.into_values() {
        let _ = proposal.reply.send(Err(match &error {
            Error::RecoveryRequired { reason } => Error::RecoveryRequired {
                reason: reason.clone(),
            },
            _ => Error::ProposalUnavailable {
                reason: error.to_string(),
            },
        }));
    }
}

fn next_metadata_bootstrap_command(
    metadata: &MetadataRuntimeHandle,
    cluster_id: &str,
    initial_nodes: &[NodeDescriptor],
) -> std::result::Result<Option<MetadataCommand>, String> {
    let state = metadata.state_snapshot();

    match state.cluster_id() {
        None => {
            return Ok(Some(MetadataCommand::ClusterInitialized {
                cluster_id: cluster_id.to_string(),
            }));
        }

        Some(existing) if existing == cluster_id => {}

        Some(existing) => {
            return Err(format!(
                "metadata state belongs to cluster {existing}, configured cluster is {cluster_id}",
            ));
        }
    }

    for expected in initial_nodes {
        match state.node(expected.node_id) {
            None => {
                return Ok(Some(MetadataCommand::RegisterNode(expected.clone())));
            }

            Some(existing) if existing == expected => {}

            Some(existing) => {
                return Err(format!(
                    "durable metadata node {} conflicts with static bootstrap directory: existing={existing:?}, expected={expected:?}",
                    expected.node_id.0,
                ));
            }
        }
    }

    Ok(None)
}

fn signal_metadata_failure(
    sender: &mut Option<mpsc::SyncSender<std::result::Result<(), String>>>,
    reason: String,
) {
    if let Some(sender) = sender.take() {
        let _ = sender.send(Err(reason));
    }
}

fn send_outbound(transport: &NodeRaftTransport, messages: Vec<RoutedRaftMessage>) {
    if let Err(source) = transport.try_send_all(messages) {
        tracing::warn!(
            error = %source,
            "Raft message could not be delivered; Raft will retry",
        );
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
        self.shutdown.store(true, Ordering::Release);

        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }

        drop(self.tablet_runtime.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ragnordb_common::{ids::NodeId, metadata_codec::NodeDescriptor};

    fn node(id: u64) -> NodeDescriptor {
        NodeDescriptor {
            node_id: NodeId(id),
            raft_addr: format!("127.0.0.1:{}", 7000 + id),
            snapshot_addr: format!("127.0.0.1:{}", 7050 + id),
            sql_addr: format!("127.0.0.1:{}", 7100 + id),
            admin_addr: format!("127.0.0.1:{}", 7200 + id),
        }
    }

    #[test]
    fn metadata_initialization_starts_with_cluster_identity() {
        let handle = MetadataRuntimeHandle::default();

        let command = next_metadata_bootstrap_command(&handle, "cluster-a", &[node(1)])
            .unwrap()
            .unwrap();

        assert_eq!(
            command,
            MetadataCommand::ClusterInitialized {
                cluster_id: "cluster-a".to_string(),
            },
        );
    }
}
