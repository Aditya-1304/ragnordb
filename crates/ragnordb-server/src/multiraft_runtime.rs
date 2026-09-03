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
    ids::{NodeId, RequestId, TabletId},
    metadata_codec::{
        CreateTableRequest, DesiredReplicaRole, MetadataCommand, MetadataCommandEnvelope,
        NodeDescriptor, TabletDescriptor,
    },
    raft_bootstrap::RaftGroupBootstrap,
};

use ragnordb_multiraft::{
    bootstrap::{FileBootstrapStore, load_durable_group_bootstrap},
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
use ragnordb_tablet::snapshot::TabletSnapshotInstallTarget;

use wal::{io::directory::FsSegmentDirectory, wal::WalHandle};

use crate::{
    bootstrap::{METADATA_RAFT_GROUP_ID, metadata_seed_descriptors, resolve_metadata_bootstrap},
    config::NodeConfig,
    data_directory_lock::DataDirectoryLock,
    database::SharedLocalDatabase,
    replica_registry::{
        DurableFrontier, LocalReplicaKey, LocalReplicaRecord, LocalReplicaRegistry,
        ReplicaLifecycle,
    },
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

/// Owns metadata-driven tablet workers for the lifetime of the node host.
///
/// The metadata Raft state machine is the placement authority; this controller
/// only materializes replicas assigned to the local physical node. Every
/// identity is persisted as `Creating` before bootstrap. Recovered lifetimes
/// are promoted to `Active` after their Ready owner is registered; a fresh
/// group can remain `Creating` until it has emitted a recovery-visible WAL
/// frontier. The map of runtime guards is also the in-process idempotency
/// fence: a committed metadata replay cannot spawn a second worker for the
/// same replica lifetime.
struct TabletLifecycleManager {
    config: NodeConfig,
    wal: LocalWal,
    database: SharedLocalDatabase,
    transport: NodeRaftTransport,
    snapshot_store: Arc<FileTabletSnapshotStore>,
    snapshot_work: SnapshotWorkController,
    snapshot_transport: NodeSnapshotTransport,
    registry: LocalReplicaRegistry,
    recovered: RecoveredRaftStorage,
    start_gate: Arc<AtomicBool>,
    runtimes: BTreeMap<RaftReplicaIdentity, ReplicatedTabletRuntime>,
}

impl TabletLifecycleManager {
    #[allow(clippy::too_many_arguments)]
    fn new(
        config: NodeConfig,
        wal: LocalWal,
        database: SharedLocalDatabase,
        transport: NodeRaftTransport,
        snapshot_store: Arc<FileTabletSnapshotStore>,
        snapshot_work: SnapshotWorkController,
        snapshot_transport: NodeSnapshotTransport,
        registry: LocalReplicaRegistry,
        recovered: RecoveredRaftStorage,
        start_gate: Arc<AtomicBool>,
    ) -> Self {
        Self {
            config,
            wal,
            database,
            transport,
            snapshot_store,
            snapshot_work,
            snapshot_transport,
            registry,
            recovered,
            start_gate,
            runtimes: BTreeMap::new(),
        }
    }

    /// Materialize all currently desired local tablet replicas.
    ///
    /// Startup calls this while the host is still registering recovered
    /// identities. Subsequent calls run on the host owner thread after
    /// metadata apply and use the active-registration APIs. A failure is
    /// returned to the host loop rather than silently leaving metadata ahead
    /// of the local durable execution state.
    fn reconcile(
        &mut self,
        host: &mut MultiRaftHost<LocalWal>,
        metadata: &MetadataRuntimeHandle,
        active: bool,
    ) -> Result<()> {
        for (identity, runtime) in &self.runtimes {
            let status = runtime.handle().status();
            let key = LocalReplicaKey {
                raft_group_id: identity.raft_group_id,
                replica_id: identity.replica_id,
            };
            let snapshot_frontier = (status.snapshot_index > 0 && status.snapshot_term > 0)
                .then(|| DurableFrontier::new(status.snapshot_index, status.snapshot_term));
            let apply_frontier = (status.applied_index > 0 && status.applied_term > 0)
                .then(|| DurableFrontier::new(status.applied_index, status.applied_term));
            let existing = self.registry.record(key)?.ok_or_else(|| {
                Error::CorruptData(format!(
                    "tablet runtime {:?} has no local registry record",
                    identity
                ))
            })?;
            let snapshot_frontier = snapshot_frontier.or(existing.snapshot_frontier);
            let apply_frontier = apply_frontier.or(existing.apply_frontier);
            if existing.snapshot_frontier != snapshot_frontier
                || existing.apply_frontier != apply_frontier
            {
                self.registry
                    .update_frontiers(key, snapshot_frontier, apply_frontier)?;
            }
            if status.last_log_index > 0 || status.applied_index > 0 {
                self.registry.mark_active(key)?;
            }
        }

        let state = metadata.state_snapshot();
        if state.cluster_id() != Some(self.config.cluster_id.as_deref().unwrap_or_default()) {
            return Ok(());
        }

        let descriptors = state.tablets().cloned().collect::<Vec<_>>();
        for descriptor in descriptors {
            let Some(placement) = state.desired_placement(descriptor.tablet_id).cloned() else {
                return Err(Error::CorruptData(format!(
                    "tablet {} has no desired replica placement",
                    descriptor.tablet_id.0
                )));
            };
            let Some(desired_local) = placement
                .replicas
                .iter()
                .find(|replica| replica.node_id == self.config.node_id)
            else {
                continue;
            };

            let requested_bootstrap = metadata_tablet_bootstrap(
                self.config.cluster_id.as_deref().unwrap_or_default(),
                &descriptor,
                &placement,
            )?;
            let bootstrap_store = FileBootstrapStore::open(
                self.config.data_dir.join("raft-bootstrap"),
            )
            .map_err(|source| Error::RecoveryFailed {
                reason: source.to_string(),
            })?;
            let bootstrap =
                load_durable_group_bootstrap(&bootstrap_store, descriptor.raft_group_id).map_err(
                    |source| Error::RecoveryFailed {
                        reason: source.to_string(),
                    },
                )?;
            let bootstrap = resolve_metadata_tablet_bootstrap(
                requested_bootstrap,
                bootstrap,
                descriptor.tablet_id,
            )?;

            let local_replica_id =
                bootstrap
                    .replica_on_node(self.config.node_id)
                    .ok_or_else(|| {
                        Error::Configuration(format!(
                            "tablet group {} has no local replica assignment",
                            descriptor.raft_group_id.0
                        ))
                    })?;
            if desired_local.replica_id != local_replica_id {
                return Err(Error::RecoveryFailed {
                    reason: format!(
                        "metadata placement for tablet {} disagrees with durable local replica {}",
                        descriptor.tablet_id.0, local_replica_id.0
                    ),
                });
            }

            // SQL executes CREATE TABLE while holding the database owner lock
            // and waits for metadata apply. Do not start a tablet worker from
            // that same critical section: a non-blocking probe lets the
            // metadata response complete, after which the next host turn can
            // safely install the local storage mirror.
            let durability_gate = match self.database.try_lock() {
                Ok(database) => database.durability_gate(),
                Err(_) => continue,
            };

            // From this point onward construction uses the already-acquired
            // gate, so the ordinary SQL owner-lock race cannot leave an issued
            // writer or transport route behind a failed retry. Any later
            // error is a durable/configuration failure and is intentionally
            // surfaced to the host instead of being retried against unknown
            // state.

            let identity = RaftReplicaIdentity::new(descriptor.raft_group_id, local_replica_id)
                .map_err(|source| Error::Configuration(source.to_string()))?;
            if self.runtimes.contains_key(&identity) {
                continue;
            }

            let record = LocalReplicaRecord::new(
                descriptor.raft_group_id,
                local_replica_id,
                descriptor.tablet_id,
                descriptor.table_id,
                descriptor.tablet_epoch,
                ReplicaLifecycle::Creating,
            );
            self.registry.ensure_replica(record)?;

            let group_wal = if active {
                host.issue_group_writer_after_activation(identity)
                    .map_err(host_error)?
            } else {
                host.issue_group_writer(identity).map_err(host_error)?
            };
            let group_transport = self
                .transport
                .register_group(&bootstrap)
                .map_err(|source| {
                    Error::Configuration(format!("register tablet Raft transport: {source}"))
                })?;
            let snapshot_endpoint = self
                .snapshot_transport
                .register_group(
                    descriptor.raft_group_id,
                    local_replica_id,
                    self.snapshot_store.clone(),
                )
                .map_err(|source| {
                    Error::Configuration(format!("register tablet snapshot route: {source}"))
                })?;
            let target = TabletSnapshotInstallTarget {
                cluster_id: self.config.cluster_id.clone().unwrap_or_default(),
                raft_group_id: descriptor.raft_group_id,
                tablet_id: descriptor.tablet_id,
                table_id: descriptor.table_id,
                tablet_epoch: descriptor.tablet_epoch,
            };
            // Slice 2 materializes the replicated tablet state machine. The
            // SQL-side storage/router mirror remains a Phase 5.6 concern and
            // must not reacquire the owner lock while CREATE TABLE completes.
            let runtime = ReplicatedTabletRuntime::start_hosted_tablet_from_shared_recovery(
                &self.config,
                self.wal.clone(),
                self.database.clone(),
                bootstrap,
                target,
                group_wal,
                group_transport,
                self.snapshot_store.clone(),
                self.snapshot_work.clone(),
                snapshot_endpoint,
                &self.recovered,
                self.start_gate.clone(),
                false,
                Some(durability_gate),
            )?;
            let recovered = self.recovered.replica(identity).is_some();
            let hosted_group = Box::new(runtime.hosted_group());
            if active {
                host.register_active_group(hosted_group)
                    .map_err(host_error)?;
            } else if recovered {
                host.register_recovered_group(hosted_group)
                    .map_err(host_error)?;
            } else {
                host.register_new_group(hosted_group).map_err(host_error)?;
            }
            // A freshly bootstrapped Raft group may have an empty initial
            // Ready generation and therefore no shared-WAL record yet. Keep
            // its durable intent in `Creating` until the first recovery-visible
            // frontier exists; this lets restart replay the exact bootstrap
            // authority without falsely claiming an active WAL lifetime.
            if recovered {
                self.registry.mark_active(LocalReplicaKey {
                    raft_group_id: identity.raft_group_id,
                    replica_id: identity.replica_id,
                })?;
            }
            self.runtimes.insert(identity, runtime);
        }
        Ok(())
    }

    /// Reject recovered lifetimes that cannot be explained by the committed
    /// metadata placement or the explicitly supported legacy group. Removal
    /// tombstones are a later phase; silently retaining an unknown lifetime
    /// here would allow stale state to survive a restart without authority.
    fn register_unmaterialized_recovered(
        &mut self,
        metadata_identity: RaftReplicaIdentity,
        legacy_identity: RaftReplicaIdentity,
    ) -> Result<()> {
        let materialized = self
            .runtimes
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        for identity in self.recovered.replicas().map(|(identity, _)| *identity) {
            if identity == metadata_identity
                || identity == legacy_identity
                || materialized.contains(&identity)
            {
                continue;
            }
            return Err(Error::RecoveryFailed {
                reason: format!(
                    "recovered local replica {:?} has no committed metadata placement",
                    identity
                ),
            });
        }
        Ok(())
    }
}

fn metadata_tablet_bootstrap(
    cluster_id: &str,
    descriptor: &TabletDescriptor,
    placement: &ragnordb_common::metadata_codec::DesiredReplicaPlacement,
) -> Result<RaftGroupBootstrap> {
    if descriptor.tablet_id != placement.tablet_id {
        return Err(Error::CorruptData(format!(
            "tablet {} placement references tablet {}",
            descriptor.tablet_id.0, placement.tablet_id.0
        )));
    }
    let mut replica_to_node = BTreeMap::new();
    let mut voters = std::collections::BTreeSet::new();
    let mut learners = std::collections::BTreeSet::new();
    for replica in &placement.replicas {
        if replica_to_node
            .insert(replica.replica_id, replica.node_id)
            .is_some()
        {
            return Err(Error::CorruptData(format!(
                "tablet {} placement repeats replica {}",
                descriptor.tablet_id.0, replica.replica_id.0
            )));
        }
        match replica.role {
            DesiredReplicaRole::Voter => {
                voters.insert(replica.replica_id);
            }
            DesiredReplicaRole::Learner => {
                learners.insert(replica.replica_id);
            }
        }
    }
    RaftGroupBootstrap::new(
        cluster_id.to_string(),
        descriptor.raft_group_id,
        placement.configuration_epoch,
        replica_to_node,
        voters,
        learners,
    )
    .map_err(|source| Error::Configuration(source.to_string()))
}

/// Resolve one tablet's bootstrap without allowing a stale local file to
/// override committed metadata placement. Membership transitions are not part
/// of Slice 2, so any mismatch is a recovery error rather than a best-effort
/// reconciliation.
fn resolve_metadata_tablet_bootstrap(
    requested: RaftGroupBootstrap,
    durable: Option<RaftGroupBootstrap>,
    tablet_id: TabletId,
) -> Result<RaftGroupBootstrap> {
    match durable {
        Some(durable) if durable != requested => Err(Error::RecoveryFailed {
            reason: format!(
                "durable bootstrap for tablet {} conflicts with committed metadata placement",
                tablet_id.0
            ),
        }),
        Some(durable) => Ok(durable),
        None => Ok(requested),
    }
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
            wal.clone(),
            database.clone(),
            bootstrap,
            group_wal,
            group_transport,
            snapshot_store.clone(),
            snapshot_work.clone(),
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

        let mut tablet_lifecycle = TabletLifecycleManager::new(
            config.clone(),
            wal,
            database.clone(),
            transport.clone(),
            snapshot_store,
            snapshot_work,
            snapshot_transport.clone(),
            registry,
            recovered,
            Arc::clone(&start_gate),
        );
        tablet_lifecycle.reconcile(&mut host, &metadata_handle, false)?;
        tablet_lifecycle.register_unmaterialized_recovered(metadata_identity, identity)?;

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
                    tablet_lifecycle,
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
fn run_host(
    mut host: MultiRaftHost<LocalWal>,
    transport: NodeRaftTransport,
    inbound: NodeRaftInbound,
    metadata_requests: mpsc::Receiver<MetadataHostRequest>,
    shutdown: Arc<AtomicBool>,
    metadata: MetadataRuntimeHandle,
    host_status: SharedMultiRaftHostStatus,
    cluster_id: String,
    metadata_nodes: Vec<NodeDescriptor>,
    metadata_ready: mpsc::SyncSender<std::result::Result<(), String>>,
    mut tablet_lifecycle: TabletLifecycleManager,
) {
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
        if let Err(error) = tablet_lifecycle.reconcile(&mut host, &metadata, true) {
            publish_host_status(&host_status, &host);
            signal_metadata_failure(&mut startup_sender, error.to_string());
            fail_pending_metadata(
                &mut pending_metadata,
                &mut pending_metadata_by_request,
                error,
            );
            tracing::error!("metadata tablet lifecycle reconciliation failed");
            return;
        }
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

    use ragnordb_common::{
        ids::{NodeId, RaftGroupId, ReplicaId, TableId, TabletId},
        metadata_codec::{DesiredReplica, DesiredReplicaPlacement, NodeDescriptor, PartitionSpec},
    };

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

    #[test]
    fn metadata_tablet_bootstrap_preserves_committed_membership_roles() {
        let descriptor = TabletDescriptor {
            tablet_id: TabletId(8),
            table_id: TableId(9),
            raft_group_id: RaftGroupId(10),
            tablet_epoch: 1,
            partition: PartitionSpec::Hash {
                bucket: 0,
                bucket_count: 1,
            },
        };
        let placement = DesiredReplicaPlacement {
            tablet_id: descriptor.tablet_id,
            configuration_epoch: 4,
            replicas: vec![
                DesiredReplica {
                    replica_id: ReplicaId(1),
                    node_id: NodeId(3),
                    role: DesiredReplicaRole::Voter,
                },
                DesiredReplica {
                    replica_id: ReplicaId(2),
                    node_id: NodeId(4),
                    role: DesiredReplicaRole::Learner,
                },
            ],
        };

        let bootstrap = metadata_tablet_bootstrap("cluster-a", &descriptor, &placement).unwrap();
        assert_eq!(bootstrap.configuration_epoch, 4);
        assert_eq!(bootstrap.node_for_replica(ReplicaId(1)), Some(NodeId(3)));
        assert_eq!(
            bootstrap.initial_voters,
            [ReplicaId(1)].into_iter().collect()
        );
        assert_eq!(
            bootstrap.initial_learners,
            [ReplicaId(2)].into_iter().collect()
        );
    }

    /// Realistic bug caught: a stale bootstrap file could retain the same
    /// local replica while changing a peer, role, or configuration epoch.
    /// Starting that file would split Raft membership under one group ID.
    #[test]
    fn metadata_tablet_bootstrap_rejects_durable_placement_divergence() {
        let descriptor = TabletDescriptor {
            tablet_id: TabletId(8),
            table_id: TableId(9),
            raft_group_id: RaftGroupId(10),
            tablet_epoch: 1,
            partition: PartitionSpec::Hash {
                bucket: 0,
                bucket_count: 1,
            },
        };
        let requested_placement = DesiredReplicaPlacement {
            tablet_id: descriptor.tablet_id,
            configuration_epoch: 4,
            replicas: vec![DesiredReplica {
                replica_id: ReplicaId(1),
                node_id: NodeId(3),
                role: DesiredReplicaRole::Voter,
            }],
        };
        let stale_placement = DesiredReplicaPlacement {
            configuration_epoch: 5,
            ..requested_placement.clone()
        };
        let requested =
            metadata_tablet_bootstrap("cluster-a", &descriptor, &requested_placement).unwrap();
        let stale = metadata_tablet_bootstrap("cluster-a", &descriptor, &stale_placement).unwrap();

        let error = resolve_metadata_tablet_bootstrap(requested, Some(stale), descriptor.tablet_id)
            .expect_err("stale durable placement must fail closed");
        assert!(matches!(error, Error::RecoveryFailed { .. }));
    }
}
