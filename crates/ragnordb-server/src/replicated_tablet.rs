//! Production owner for the Milestone 4 single replicated tablet.
//!
//! This host deliberately owns one Raft group. Multi-group scheduling belongs
//! to Milestone 5; the correctness boundary here is the complete production
//! path from SQL commit, through Raft Ready durability, to tablet apply.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};

use raft::{
    core::ready::Ready,
    entry::EntryPayload,
    message::{Envelope, Message},
    runtime::transport_tcp::{TcpEndpoint, TcpTransport},
    storage::codec::{CommandCodec, SnapshotCodec},
    traits::{log_store::LogStore, stable_store::StableStore},
};
use ragnordb_catalog::{CatalogLogExtent, CatalogLogRecord, DurableCatalogLog};
use ragnordb_common::{
    Error, Result,
    codec::WriteKind,
    command_codec::{
        NoopCommand, SingleShardCommitCommand, TabletCommand, TabletCommandEnvelope, WriteEntry,
    },
    durability::DurabilityGate,
    encoding::decode_row,
    ids::{RaftGroupId, ReplicaId, RequestId, TableId, TabletId},
    raft_bootstrap::RaftGroupBootstrap,
};
use ragnordb_multiraft::{
    bootstrap::{FileBootstrapStore, load_durable_group_bootstrap},
    proposal::{ProposalCompletion, ProposalRegistry, ProposalTicket},
    replica_startup::{
        bootstrap_tablet_replica, initial_recovery_configuration, recover_tablet_replica,
    },
    runtime::{AppliedRaftFrontier, RaftReadyLoop},
    snapshot::{
        SnapshotWorkController, TabletSnapshotTransfer, generate_tablet_snapshot_from_ready_loop,
        install_incoming_tablet_snapshot, persist_tablet_snapshot_boundary_via_ready_loop,
        raft_pointer_for_tablet,
    },
    storage::{
        codec::RaftReplicaIdentity,
        persistence::{NodeRaftWal, RaftWal},
        recovery::{RecoveredRaftStorage, recover_raft_storage_with_configurations},
    },
    tablet_apply::{CommittedTabletCommandDisposition, TabletCommandApplier},
};
use ragnordb_storage::wal::{
    DurableCommitLog, DurableWalExtent, RagnorDbWalAdapter, SingleNodeTxnCommit, WalMutation,
};
use ragnordb_tablet::{
    command::{TabletCommandApplyError, TabletCommandApplyOutcome},
    snapshot::{
        AppliedTabletFrontier, FileTabletSnapshotStore, TabletSnapshotConfState,
        TabletSnapshotImage, TabletSnapshotInstallTarget,
    },
};
use tracing::{error, info, warn};
use wal::{io::directory::FsSegmentDirectory, lsn::Lsn, wal::WalHandle};

use crate::{
    config::NodeConfig,
    database::SharedLocalDatabase,
    snapshot_transport::{ReceivedTabletSnapshot, SnapshotEndpoint},
};

const GROUP_ID: RaftGroupId = RaftGroupId(1);
const TABLET_ID: TabletId = TabletId(1);
const TABLE_ID: TableId = TableId(1);
const TABLET_EPOCH: u64 = 1;
const ELECTION_TIMEOUT_TICKS: u64 = 10;
const HEARTBEAT_INTERVAL_TICKS: u64 = 3;
const TICK_INTERVAL: Duration = Duration::from_millis(100);
const CHANNEL_CAPACITY: usize = 1_024;
const INTERNAL_BARRIER_CLIENT_NAMESPACE: u64 = 0x5241_474e_4f52_4442;

type LocalWal = WalHandle<FsSegmentDirectory, ()>;
type Completion = ProposalCompletion<TabletCommandApplyOutcome, TabletCommandApplyError>;

/// Transport codec for already encoded command and snapshot payloads.
#[derive(Debug, Clone, Copy, Default)]
struct BytesCodec;

impl CommandCodec<Vec<u8>> for BytesCodec {
    fn encode(&self, command: &Vec<u8>) -> io::Result<Vec<u8>> {
        Ok(command.clone())
    }

    fn decode(&self, bytes: &[u8]) -> io::Result<Vec<u8>> {
        Ok(bytes.to_vec())
    }
}

impl SnapshotCodec<Vec<u8>> for BytesCodec {
    fn encode(&self, snapshot: &Vec<u8>) -> io::Result<Vec<u8>> {
        Ok(snapshot.clone())
    }

    fn decode(&self, bytes: &[u8]) -> io::Result<Vec<u8>> {
        Ok(bytes.to_vec())
    }
}

/// Point-in-time routing state published without exposing the Ready owner.
#[derive(Debug, Clone, Default)]
pub struct ReplicatedTabletStatus {
    pub leader_replica_id: Option<u64>,
    pub term: u64,
    pub commit_index: u64,
    pub last_log_index: u64,
    pub applied_index: u64,
    pub snapshot_index: u64,
    pub serving_leader: bool,
    pub runtime_error: Option<String>,
}

enum HostRequest {
    Commit {
        commit: SingleNodeTxnCommit,
        reply: mpsc::Sender<Result<DurableWalExtent>>,
        deadline: Instant,
    },
    Catalog {
        update: CatalogLogRecord,
        reply: mpsc::Sender<Result<CatalogLogExtent>>,
        deadline: Instant,
    },
    Barrier {
        reply: mpsc::Sender<Result<()>>,
        deadline: Instant,
    },
}

enum ClientReply {
    Commit(mpsc::Sender<Result<DurableWalExtent>>),
    Catalog(mpsc::Sender<Result<CatalogLogExtent>>),
    Barrier(mpsc::Sender<Result<()>>),
}

struct PendingClient {
    ticket: ProposalTicket<TabletCommandApplyOutcome, TabletCommandApplyError>,
    reply: ClientReply,
}

/// Durable catalog-cache boundary owned by the Ready worker.
///
/// Catalog cache publication is part of applying a committed catalog entry,
/// not a SQL-side follow-up. Any uncertain cache append therefore fences the
/// same node-wide durability gate used by database and Raft WAL ownership.
trait CatalogCacheWriter: Send + Sync {
    fn append_catalog_update(&self, update: &CatalogLogRecord) -> Result<CatalogLogExtent>;
}

struct FencedCatalogCache {
    adapter: RagnorDbWalAdapter<FsSegmentDirectory, ()>,
    durability_gate: DurabilityGate,
}

impl CatalogCacheWriter for FencedCatalogCache {
    fn append_catalog_update(&self, update: &CatalogLogRecord) -> Result<CatalogLogExtent> {
        let result = self.adapter.append_catalog_update(update);
        if let Err(error) = &result {
            self.durability_gate.observe_error(error);
        }
        result
    }
}

#[derive(Clone)]
struct SnapshotPolicy {
    interval_entries: u64,
    interval_bytes: u64,
    min_elapsed: Duration,
    applied_bytes: Arc<AtomicU64>,
}

impl SnapshotPolicy {
    fn note_applied(&self, bytes: usize) {
        self.applied_bytes
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    fn is_due(&self, applied_index: u64, snapshot_index: u64, last_snapshot_at: Instant) -> bool {
        last_snapshot_at.elapsed() >= self.min_elapsed
            && (applied_index.saturating_sub(snapshot_index) >= self.interval_entries
                || self.applied_bytes.load(Ordering::Relaxed) >= self.interval_bytes)
    }

    fn reset(&self) {
        self.applied_bytes.store(0, Ordering::Relaxed);
    }
}

/// Cloneable SQL-side handle for one production tablet host.
pub struct ReplicatedTabletHandle {
    requests: SyncSender<HostRequest>,
    status: Arc<RwLock<ReplicatedTabletStatus>>,
}

impl ReplicatedTabletHandle {
    /// Return whether this process currently owns the leader lease implied by
    /// the Raft soft state. The proposal path performs the same check again.
    pub fn is_leader(&self) -> bool {
        self.status().serving_leader
    }

    pub fn status(&self) -> ReplicatedTabletStatus {
        self.status
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Establish an applied current-term ordering point before a latest read.
    pub fn read_barrier(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        let (reply, response) = mpsc::channel();
        self.requests
            .send(HostRequest::Barrier { reply, deadline })
            .map_err(|_| Error::ProposalUnavailable {
                reason: "replicated tablet runtime has stopped".to_string(),
            })?;
        response
            .recv_timeout(timeout)
            .map_err(|_| Error::ProposalUnavailable {
                reason: "read barrier deadline elapsed before apply".to_string(),
            })?
    }
}

impl DurableCommitLog for ReplicatedTabletHandle {
    fn append_single_node_commit(&self, commit: &SingleNodeTxnCommit) -> Result<DurableWalExtent> {
        let timeout = Duration::from_secs(30);
        let deadline = Instant::now() + timeout;
        let (reply, response) = mpsc::channel();
        self.requests
            .send(HostRequest::Commit {
                commit: commit.clone(),
                reply,
                deadline,
            })
            .map_err(|_| Error::ProposalUnavailable {
                reason: "replicated tablet runtime has stopped".to_string(),
            })?;
        response
            .recv_timeout(timeout)
            .map_err(|_| Error::ProposalUnavailable {
                reason: "commit deadline elapsed before tablet apply".to_string(),
            })?
    }
}

impl DurableCatalogLog for ReplicatedTabletHandle {
    fn append_catalog_update(&self, update: &CatalogLogRecord) -> Result<CatalogLogExtent> {
        let timeout = Duration::from_secs(30);
        let deadline = Instant::now() + timeout;
        let (reply, response) = mpsc::channel();
        self.requests
            .send(HostRequest::Catalog {
                update: update.clone(),
                reply,
                deadline,
            })
            .map_err(|_| Error::ProposalUnavailable {
                reason: "replicated tablet runtime has stopped".to_string(),
            })?;
        response
            .recv_timeout(timeout)
            .map_err(|_| Error::ProposalUnavailable {
                reason: "catalog deadline elapsed before tablet apply".to_string(),
            })?
    }
}

/// Lifecycle guard for the background Ready owner and its cloneable SQL handle.
pub struct ReplicatedTabletRuntime {
    handle: Arc<ReplicatedTabletHandle>,
    shutdown: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl ReplicatedTabletRuntime {
    /// Resolve the authoritative initial configuration before the node opens
    /// the shared semantic recovery cursor.
    pub fn recovery_configurations(
        config: &NodeConfig,
    ) -> Result<BTreeMap<RaftReplicaIdentity, raft::types::ConfState>> {
        let requested = requested_bootstrap(config)?;
        let store =
            FileBootstrapStore::open(config.data_dir.join("raft-bootstrap")).map_err(|source| {
                Error::RecoveryFailed {
                    reason: source.to_string(),
                }
            })?;
        let bootstrap = load_durable_group_bootstrap(&store, GROUP_ID)
            .map_err(|source| Error::RecoveryFailed {
                reason: source.to_string(),
            })?
            .unwrap_or(requested);
        let local_replica_id = ReplicaId(config.node_id.0);
        let (identity, conf_state) = initial_recovery_configuration(&bootstrap, local_replica_id)
            .map_err(|source| Error::RecoveryFailed {
            reason: source.to_string(),
        })?;
        Ok(BTreeMap::from([(identity, conf_state)]))
    }

    /// Recover or bootstrap the configured local replica, bind its Raft TCP
    /// endpoint, and start the sole owner of tick/message/Ready/apply ordering.
    pub fn start(
        config: &NodeConfig,
        wal: LocalWal,
        database: SharedLocalDatabase,
    ) -> Result<Self> {
        Self::start_inner(config, wal, database, None)
    }

    /// Start from Raft state produced by the server's one-cursor shared-WAL
    /// recovery pass. No second semantic scan is performed.
    pub fn start_from_shared_recovery(
        config: &NodeConfig,
        wal: LocalWal,
        database: SharedLocalDatabase,
        recovered: RecoveredRaftStorage,
    ) -> Result<Self> {
        Self::start_inner(config, wal, database, Some(recovered))
    }

    fn start_inner(
        config: &NodeConfig,
        wal: LocalWal,
        database: SharedLocalDatabase,
        mut shared_recovered: Option<RecoveredRaftStorage>,
    ) -> Result<Self> {
        let cluster_id = config.cluster_id.clone().ok_or_else(|| {
            Error::Configuration("replicated tablet runtime requires cluster_id".to_string())
        })?;
        let local_seed = config
            .seed_nodes
            .iter()
            .find(|seed| seed.id == config.node_id)
            .ok_or_else(|| {
                Error::Configuration(
                    "local node must appear in seed_nodes to bind its Raft endpoint".to_string(),
                )
            })?;

        let local_replica_id = ReplicaId(config.node_id.0);
        let requested = requested_bootstrap(config)?;

        let target = TabletSnapshotInstallTarget {
            cluster_id: cluster_id.clone(),
            raft_group_id: GROUP_ID,
            tablet_id: TABLET_ID,
            table_id: TABLE_ID,
            tablet_epoch: TABLET_EPOCH,
        };
        let mut bootstrap_store = FileBootstrapStore::open(config.data_dir.join("raft-bootstrap"))
            .map_err(|source| Error::RecoveryFailed {
                reason: source.to_string(),
            })?;
        let snapshot_store = Arc::new(
            FileTabletSnapshotStore::new(
                config.data_dir.join("tablet-snapshots"),
                config.max_snapshot_file_bytes,
            )
            .map_err(|source| Error::RecoveryFailed {
                reason: source.to_string(),
            })?,
        );

        let peers = config
            .seed_nodes
            .iter()
            .filter(|seed| seed.id != config.node_id)
            .map(|seed| {
                ReplicaId(seed.id.0)
                    .to_raft()
                    .map(|replica_id| (replica_id, seed.raft_addr))
            })
            .collect::<std::result::Result<BTreeMap<_, _>, _>>()
            .map_err(|reason| Error::Configuration(reason.to_string()))?;
        let TcpEndpoint {
            transport,
            inbound,
            local_addr,
        } = TcpTransport::bind(
            local_replica_id
                .to_raft()
                .map_err(|reason| Error::Configuration(reason.to_string()))?,
            local_seed.raft_addr,
            peers,
            BytesCodec,
            BytesCodec,
        )
        .map_err(|source| Error::Configuration(format!("bind Raft endpoint: {source}")))?;

        let (request_tx, request_rx) = mpsc::sync_channel(CHANNEL_CAPACITY);
        let status = Arc::new(RwLock::new(ReplicatedTabletStatus::default()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let snapshot_peers = config
            .seed_nodes
            .iter()
            .filter(|seed| seed.id != config.node_id)
            .map(|seed| (seed.id.0, seed.snapshot_addr))
            .collect();
        let snapshot_work = SnapshotWorkController::default();
        let snapshot_endpoint = SnapshotEndpoint::bind(
            local_seed.snapshot_addr,
            snapshot_peers,
            snapshot_store.clone(),
            snapshot_work.clone(),
            config.snapshot_chunk_bytes,
            shutdown.clone(),
        )
        .map_err(|source| Error::Configuration(format!("bind snapshot endpoint: {source}")))?;
        let snapshot_policy = SnapshotPolicy {
            interval_entries: config.snapshot_interval_entries,
            interval_bytes: config.snapshot_interval_bytes,
            min_elapsed: Duration::from_millis(config.snapshot_min_elapsed_ms),
            applied_bytes: Arc::new(AtomicU64::new(0)),
        };
        let handle = Arc::new(ReplicatedTabletHandle {
            requests: request_tx,
            status: status.clone(),
        });

        let durable_bootstrap =
            load_durable_group_bootstrap(&bootstrap_store, GROUP_ID).map_err(|source| {
                Error::RecoveryFailed {
                    reason: source.to_string(),
                }
            })?;
        let worker_shutdown = shutdown.clone();

        let identity = RaftReplicaIdentity::new(GROUP_ID, local_replica_id)
            .map_err(|source| Error::Configuration(source.to_string()))?;
        let durability_gate = database
            .try_lock()
            .map_err(|_| {
                Error::Configuration("database is busy during WAL ownership setup".into())
            })?
            .durability_gate();
        let catalog_cache: Arc<dyn CatalogCacheWriter> = Arc::new(FencedCatalogCache {
            adapter: RagnorDbWalAdapter::new(wal.clone()),
            durability_gate: durability_gate.clone(),
        });
        let node_wal = NodeRaftWal::with_durability_gate(wal.clone(), durability_gate);
        let group_wal = node_wal
            .group_writer_for(identity)
            .map_err(Error::Configuration)?;
        node_wal
            .seal_retention_registry()
            .map_err(Error::Configuration)?;
        database
            .try_lock()
            .map_err(|_| {
                Error::Configuration("database is busy during WAL ownership setup".into())
            })?
            .install_node_wal(node_wal)?;

        let worker = if let Some(bootstrap) = durable_bootstrap {
            // Durable bootstrap is authoritative after restart; changed static
            // membership cannot silently replace it.
            let (identity, conf_state) =
                initial_recovery_configuration(&bootstrap, local_replica_id).map_err(|source| {
                    Error::RecoveryFailed {
                        reason: source.to_string(),
                    }
                })?;
            let recovered = match shared_recovered.take() {
                Some(recovered) => recovered,
                None => {
                    let configurations = BTreeMap::from([(identity, conf_state)]);
                    let mut source =
                        wal.iter_from(Lsn::ZERO)
                            .map_err(|source| Error::RecoveryFailed {
                                reason: format!("open Raft recovery stream: {source}"),
                            })?;
                    recover_raft_storage_with_configurations(&mut source, &configurations).map_err(
                        |source| Error::RecoveryFailed {
                            reason: source.to_string(),
                        },
                    )?
                }
            };
            let replica = recovered
                .replica(identity)
                .ok_or_else(|| Error::RecoveryFailed {
                    reason: "durable bootstrap exists without a matching Raft WAL replica"
                        .to_string(),
                })?;
            install_recovered_catalog(&database, replica)?;
            let recovered_replica = recover_tablet_replica(
                bootstrap,
                local_replica_id,
                group_wal.clone(),
                wal.durable_lsn(),
                replica,
                &snapshot_store,
                &target,
                ELECTION_TIMEOUT_TICKS,
                HEARTBEAT_INTERVAL_TICKS,
            )
            .map_err(|source| Error::RecoveryFailed {
                reason: source.to_string(),
            })?;

            install_recovered_sql_mirror(&database, &recovered_replica.tablet)?;

            spawn_ready_owner(
                recovered_replica.ready_loop,
                recovered_replica.tablet,
                None,
                transport,
                inbound,
                request_rx,
                database,
                status,
                worker_shutdown,
                snapshot_store,
                snapshot_work,
                snapshot_endpoint,
                cluster_id,
                catalog_cache,
                snapshot_policy,
            )
        } else {
            if !config.bootstrap {
                return Err(Error::Configuration(
                    "no durable tablet bootstrap exists and bootstrap=false".to_string(),
                ));
            }
            let bootstrapped = bootstrap_tablet_replica(
                &mut bootstrap_store,
                &requested,
                local_replica_id,
                group_wal,
                &target,
                ELECTION_TIMEOUT_TICKS,
                HEARTBEAT_INTERVAL_TICKS,
            )
            .map_err(|source| Error::RecoveryFailed {
                reason: source.to_string(),
            })?;

            install_recovered_sql_mirror(&database, &bootstrapped.tablet)?;

            spawn_ready_owner(
                bootstrapped.ready_loop,
                bootstrapped.tablet,
                Some(bootstrapped.initial_ready),
                transport,
                inbound,
                request_rx,
                database,
                status,
                worker_shutdown,
                snapshot_store,
                snapshot_work,
                snapshot_endpoint,
                cluster_id,
                catalog_cache,
                snapshot_policy,
            )
        };

        info!(raft = %local_addr, group_id = GROUP_ID.0, "replicated tablet runtime started");
        Ok(Self {
            handle,
            shutdown,
            worker: Some(worker),
        })
    }

    pub fn handle(&self) -> Arc<ReplicatedTabletHandle> {
        self.handle.clone()
    }
}

fn requested_bootstrap(config: &NodeConfig) -> Result<RaftGroupBootstrap> {
    let cluster_id = config.cluster_id.clone().ok_or_else(|| {
        Error::Configuration("replicated tablet runtime requires cluster_id".to_string())
    })?;
    let replica_to_node = config
        .seed_nodes
        .iter()
        .map(|seed| (ReplicaId(seed.id.0), seed.id))
        .collect::<BTreeMap<_, _>>();
    let voters = replica_to_node.keys().copied().collect::<BTreeSet<_>>();
    RaftGroupBootstrap::new(
        cluster_id,
        GROUP_ID,
        1,
        replica_to_node,
        voters,
        BTreeSet::new(),
    )
    .map_err(|source| Error::Configuration(source.to_string()))
}

fn install_recovered_sql_mirror(
    database: &SharedLocalDatabase,
    tablet: &TabletCommandApplier,
) -> Result<()> {
    let storage = tablet.state_machine().tablet().storage().clone();
    let mut database = database.try_lock().map_err(|_| {
        Error::Configuration(
            "database owner is busy while replicated startup installs its recovered tablet"
                .to_string(),
        )
    })?;
    database.install_replicated_storage(TABLE_ID, storage)?;
    Ok(())
}

fn install_recovered_catalog(
    database: &SharedLocalDatabase,
    recovered: &ragnordb_multiraft::storage::recovery::RecoveredRaftReplica,
) -> Result<()> {
    let commit_index = recovered
        .hard_state()
        .map(|hard_state| hard_state.commit)
        .unwrap_or(0);
    let mut database = database.try_lock().map_err(|_| {
        Error::Configuration(
            "database owner is busy while replicated startup restores its catalog".to_string(),
        )
    })?;

    for entry in recovered.log_view().entries() {
        if entry.record.index > commit_index {
            break;
        }
        let ragnordb_multiraft::storage::codec::DurableRaftEntryPayload::Normal(bytes) =
            &entry.record.payload
        else {
            continue;
        };
        let envelope = TabletCommandEnvelope::decode(bytes)
            .map_err(|source| Error::CorruptData(source.to_string()))?;
        if let TabletCommand::Catalog(command) = envelope.command {
            database.apply_replicated_catalog(
                &command,
                ragnordb_common::ids::Timestamp((envelope.request_id.client_id >> 64) as u64),
            )?;
        }
    }
    Ok(())
}

impl Drop for ReplicatedTabletRuntime {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_ready_owner<W, LS, SS>(
    ready_loop: RaftReadyLoop<W, LS, SS>,
    tablet: TabletCommandApplier,
    initial_ready: Option<Ready<Vec<u8>, Vec<u8>>>,
    transport: TcpTransport<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>,
    inbound: Receiver<Envelope<Vec<u8>, Vec<u8>>>,
    requests: Receiver<HostRequest>,
    database: SharedLocalDatabase,
    status: Arc<RwLock<ReplicatedTabletStatus>>,
    shutdown: Arc<AtomicBool>,
    snapshot_store: Arc<FileTabletSnapshotStore>,
    snapshot_work: SnapshotWorkController,
    snapshot_endpoint: SnapshotEndpoint,
    cluster_id: String,
    catalog_cache: Arc<dyn CatalogCacheWriter>,
    snapshot_policy: SnapshotPolicy,
) -> thread::JoinHandle<()>
where
    W: RaftWal + Send + 'static,
    LS: LogStore<Vec<u8>> + Send + 'static,
    SS: StableStore + Send + 'static,
{
    thread::Builder::new()
        .name("ragnordb-tablet-1".to_string())
        .spawn(move || {
            let failure_status = status.clone();
            if let Err(source) = run_ready_owner(
                ready_loop,
                tablet,
                initial_ready,
                transport,
                inbound,
                requests,
                database,
                status,
                shutdown,
                snapshot_store,
                snapshot_work,
                snapshot_endpoint,
                cluster_id,
                catalog_cache,
                snapshot_policy,
            ) {
                error!(error = %source, "replicated tablet runtime stopped");
                failure_status
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .runtime_error = Some(source);
            }
        })
        .expect("replicated tablet worker thread creation must succeed")
}

#[allow(clippy::too_many_arguments)]
fn run_ready_owner<W, LS, SS>(
    mut ready_loop: RaftReadyLoop<W, LS, SS>,
    mut tablet: TabletCommandApplier,
    initial_ready: Option<Ready<Vec<u8>, Vec<u8>>>,
    transport: TcpTransport<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>,
    inbound: Receiver<Envelope<Vec<u8>, Vec<u8>>>,
    requests: Receiver<HostRequest>,
    database: SharedLocalDatabase,
    status: Arc<RwLock<ReplicatedTabletStatus>>,
    shutdown: Arc<AtomicBool>,
    snapshot_store: Arc<FileTabletSnapshotStore>,
    snapshot_work: SnapshotWorkController,
    snapshot_endpoint: SnapshotEndpoint,
    cluster_id: String,
    catalog_cache: Arc<dyn CatalogCacheWriter>,
    snapshot_policy: SnapshotPolicy,
) -> std::result::Result<(), String>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    let mut registry = ProposalRegistry::new();
    let mut clients = Vec::<PendingClient>::new();
    let mut internal_barrier_allocator = InternalBarrierAllocator::default();
    // Processes commonly start together under an orchestrator. A small stable
    // replica-specific offset prevents identical election tick cadences from
    // repeatedly producing split votes after leader failure.
    let tick_interval = TICK_INTERVAL
        + Duration::from_millis(ready_loop.raft().id().get().saturating_mul(7).min(50));
    let mut next_tick = Instant::now() + tick_interval;
    let mut was_leader = false;
    let mut leader_activation = None::<ragnordb_multiraft::proposal::ProposalPosition>;
    let mut latest_snapshot = ready_loop
        .persistence()
        .snapshot()
        .map(|pointer| snapshot_store.load_verified_by_name(&pointer.file_name))
        .transpose()
        .map_err(|error| error.to_string())?;
    let mut last_snapshot_index = latest_snapshot
        .as_ref()
        .map(|image| image.metadata.last_included_index)
        .unwrap_or(0);
    let mut last_snapshot_at = Instant::now();
    let mut snapshot_install_pending = false;
    let mut received_snapshot = None::<ReceivedTabletSnapshot>;

    if let Some(ready) = initial_ready {
        send_messages(
            &transport,
            &snapshot_endpoint,
            &latest_snapshot,
            ready.messages,
        );
    }

    while !shutdown.load(Ordering::Acquire) {
        while let Ok(message) = inbound.try_recv() {
            if matches!(message.msg, Message::InstallSnapshot(_)) {
                snapshot_install_pending = true;
            }
            ready_loop
                .step(message)
                .map_err(|error| error.to_string())?;
            drain_ready(
                &mut ready_loop,
                &mut tablet,
                &mut registry,
                &database,
                &transport,
                &snapshot_endpoint,
                &latest_snapshot,
                catalog_cache.as_ref(),
                &snapshot_policy,
            )?;
        }

        while let Ok(received) = snapshot_endpoint.inbound.try_recv() {
            received_snapshot = Some(received);
        }
        if snapshot_install_pending && let Some(received) = received_snapshot.take() {
            install_received_snapshot(
                received,
                &mut ready_loop,
                &mut tablet,
                &mut registry,
                &database,
                &transport,
                &snapshot_endpoint,
                &snapshot_store,
                &snapshot_work,
                &cluster_id,
                &mut latest_snapshot,
                catalog_cache.as_ref(),
                &snapshot_policy,
            )?;
            internal_barrier_allocator.clear();
            snapshot_install_pending = false;
            last_snapshot_index = latest_snapshot
                .as_ref()
                .map(|image| image.metadata.last_included_index)
                .unwrap_or(last_snapshot_index);
        }

        refresh_leader_activation(
            &mut ready_loop,
            &mut tablet,
            &mut registry,
            &database,
            &transport,
            &snapshot_endpoint,
            &latest_snapshot,
            catalog_cache.as_ref(),
            &snapshot_policy,
            &mut leader_activation,
            &mut internal_barrier_allocator,
        )?;
        let serving_leader = leader_activation.is_some_and(|activation| {
            ready_loop
                .applied_frontier()
                .is_some_and(|frontier| frontier.index >= activation.index)
        });

        while let Ok(request) = requests.try_recv() {
            admit_request(
                request,
                &mut ready_loop,
                &tablet,
                &mut registry,
                &mut clients,
                serving_leader,
                &mut internal_barrier_allocator,
            );
            drain_ready(
                &mut ready_loop,
                &mut tablet,
                &mut registry,
                &database,
                &transport,
                &snapshot_endpoint,
                &latest_snapshot,
                catalog_cache.as_ref(),
                &snapshot_policy,
            )?;
        }

        let now = Instant::now();
        if now >= next_tick {
            ready_loop.tick(1).map_err(|error| error.to_string())?;
            next_tick = now + tick_interval;
            drain_ready(
                &mut ready_loop,
                &mut tablet,
                &mut registry,
                &database,
                &transport,
                &snapshot_endpoint,
                &latest_snapshot,
                catalog_cache.as_ref(),
                &snapshot_policy,
            )?;
        }

        registry.expire_deadlines(now);
        let is_leader = ready_loop.raft().leader_id() == Some(ready_loop.raft().id());
        if was_leader && !is_leader {
            registry.mark_leadership_lost(ready_loop.raft().hard_state().current_term);
            leader_activation = None;
            internal_barrier_allocator.clear();
        }
        was_leader = is_leader;
        forward_completions(&mut clients);
        let serving_leader = is_leader
            && leader_activation.is_some_and(|activation| {
                activation.term == ready_loop.raft().hard_state().current_term
                    && ready_loop
                        .applied_frontier()
                        .is_some_and(|frontier| frontier.index >= activation.index)
            });
        if serving_leader {
            maybe_publish_snapshot(
                &mut ready_loop,
                &mut tablet,
                &mut registry,
                &database,
                &transport,
                &snapshot_endpoint,
                &snapshot_store,
                &snapshot_work,
                &cluster_id,
                &mut latest_snapshot,
                &mut last_snapshot_index,
                catalog_cache.as_ref(),
                &snapshot_policy,
                &mut last_snapshot_at,
            )?;
        }
        publish_status(
            &ready_loop,
            serving_leader,
            latest_snapshot
                .as_ref()
                .map(|image| image.metadata.last_included_index)
                .unwrap_or(0),
            &status,
        );
        thread::sleep(Duration::from_millis(2));
    }

    Ok(())
}

/// Allocates no-op identities for read barriers owned by this local Raft host.
///
/// Tablet command deduplication requires every client sequence to be strictly
/// contiguous. The allocator therefore advances only after Raft has accepted a
/// proposal into its log. Its client identity is scoped to a Raft term, so an
/// uncommitted entry discarded during a leadership change cannot leave a gap
/// for a future leader on this host.
#[derive(Debug, Default)]
struct InternalBarrierAllocator {
    term: Option<u64>,
    next_sequence: Option<u64>,
}

impl InternalBarrierAllocator {
    fn candidate(
        &mut self,
        term: u64,
        tablet: &TabletCommandApplier,
    ) -> std::result::Result<RequestId, TabletCommandApplyError> {
        if self.term != Some(term) {
            let client_id = internal_barrier_client_id(term);
            self.term = Some(term);
            self.next_sequence = Some(tablet.state_machine().next_sequence_for_client(client_id)?);
        }

        self.candidate_for_active_term()
    }

    fn candidate_for_active_term(&self) -> std::result::Result<RequestId, TabletCommandApplyError> {
        let term = self
            .term
            .expect("barrier allocator must select a term before a candidate");
        let client_id = internal_barrier_client_id(term);
        let sequence = self
            .next_sequence
            .ok_or(TabletCommandApplyError::RequestSequenceExhausted { client_id })?;
        Ok(RequestId {
            client_id,
            sequence,
            raft_group_id: GROUP_ID,
        })
    }

    /// Record that the candidate was admitted into the Raft log. This must run
    /// only after `RaftReadyLoop::propose` succeeds; rejected proposals have no
    /// log entry and must retain their candidate sequence for a retry.
    fn record_admission(&mut self, sequence: u64) {
        debug_assert_eq!(self.next_sequence, Some(sequence));
        self.next_sequence = sequence.checked_add(1);
    }

    fn clear(&mut self) {
        self.term = None;
        self.next_sequence = None;
    }

    #[cfg(test)]
    fn activate_term_for_test(&mut self, term: u64, next_sequence: u64) {
        self.term = Some(term);
        self.next_sequence = Some(next_sequence);
    }
}

const fn internal_barrier_client_id(term: u64) -> u128 {
    (INTERNAL_BARRIER_CLIENT_NAMESPACE as u128) << 64 | term as u128
}

fn admit_request<W, LS, SS>(
    request: HostRequest,
    ready_loop: &mut RaftReadyLoop<W, LS, SS>,
    tablet: &TabletCommandApplier,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    clients: &mut Vec<PendingClient>,
    serving_leader: bool,
    internal_barrier_allocator: &mut InternalBarrierAllocator,
) where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    let local_id = ready_loop.raft().id();
    let leader_id = ready_loop.raft().leader_id();
    if leader_id != Some(local_id) || !serving_leader {
        reply_error(
            request,
            Error::NotLeader {
                leader_id: leader_id.map(|replica_id| replica_id.get()),
            },
        );
        return;
    }

    let (envelope, deadline, reply, internal_barrier_sequence) = match request {
        HostRequest::Commit {
            commit,
            reply,
            deadline,
        } => match envelope_from_commit(local_id.get(), commit) {
            Ok(envelope) => (envelope, deadline, ClientReply::Commit(reply), None),
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        },
        HostRequest::Catalog {
            update,
            reply,
            deadline,
        } => match envelope_from_catalog(local_id.get(), &update) {
            Ok(envelope) => (envelope, deadline, ClientReply::Catalog(reply), None),
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        },
        HostRequest::Barrier { reply, deadline } => {
            let request_id = match internal_barrier_allocator
                .candidate(ready_loop.raft().hard_state().current_term, tablet)
            {
                Ok(request_id) => request_id,
                Err(source) => {
                    let _ = reply.send(Err(Error::RecoveryRequired {
                        reason: source.to_string(),
                    }));
                    return;
                }
            };
            let envelope = TabletCommandEnvelope::new(
                request_id.clone(),
                TABLET_ID,
                TABLET_EPOCH,
                TabletCommand::Noop(NoopCommand),
            )
            .map_err(|source| Error::InvalidArgument(source.to_string()));
            match envelope {
                Ok(envelope) => (
                    envelope,
                    deadline,
                    ClientReply::Barrier(reply),
                    Some(request_id.sequence),
                ),
                Err(error) => {
                    let _ = reply.send(Err(error));
                    return;
                }
            }
        }
    };

    if let Err(source) = tablet.state_machine().validate_proposal(&envelope) {
        send_client_error(reply, Error::InvalidArgument(source.to_string()));
        return;
    }
    let request_id = envelope.request_id.clone();
    if registry.is_pending(&request_id) {
        send_client_error(
            reply,
            Error::ProposalUnavailable {
                reason: "an identical request is already awaiting tablet apply".to_string(),
            },
        );
        return;
    }
    let bytes = match envelope.encode() {
        Ok(bytes) => bytes,
        Err(source) => {
            send_client_error(reply, Error::InvalidArgument(source.to_string()));
            return;
        }
    };
    let index = match ready_loop.propose(bytes.clone(), bytes.len()) {
        Ok(index) => index,
        Err(source) => {
            send_client_error(
                reply,
                Error::ProposalUnavailable {
                    reason: source.to_string(),
                },
            );
            return;
        }
    };
    if let Some(sequence) = internal_barrier_sequence {
        internal_barrier_allocator.record_admission(sequence);
    }
    let position = ragnordb_multiraft::proposal::ProposalPosition {
        term: ready_loop.raft().hard_state().current_term,
        index,
    };
    match registry.register(request_id, position, deadline) {
        Ok(ticket) => clients.push(PendingClient { ticket, reply }),
        Err(source) => send_client_error(
            reply,
            Error::ProposalUnavailable {
                reason: source.to_string(),
            },
        ),
    }
}

/// Commit an entry in the newly elected leader's term before exposing it to
/// SQL traffic. This both proves a live quorum and publishes any durable
/// old-term prefix that followers had not learned was committed when the
/// previous leader failed.
#[allow(clippy::too_many_arguments)]
fn refresh_leader_activation<W, LS, SS>(
    ready_loop: &mut RaftReadyLoop<W, LS, SS>,
    tablet: &mut TabletCommandApplier,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    database: &SharedLocalDatabase,
    transport: &TcpTransport<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>,
    snapshot_endpoint: &SnapshotEndpoint,
    latest_snapshot: &Option<TabletSnapshotImage>,
    catalog_cache: &dyn CatalogCacheWriter,
    snapshot_policy: &SnapshotPolicy,
    activation: &mut Option<ragnordb_multiraft::proposal::ProposalPosition>,
    internal_barrier_allocator: &mut InternalBarrierAllocator,
) -> std::result::Result<(), String>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    let local_id = ready_loop.raft().id();
    if ready_loop.raft().leader_id() != Some(local_id) {
        *activation = None;
        return Ok(());
    }

    let term = ready_loop.raft().hard_state().current_term;
    if activation.is_some_and(|position| position.term == term) {
        return Ok(());
    }

    let request_id = internal_barrier_allocator
        .candidate(term, tablet)
        .map_err(|error| error.to_string())?;
    let envelope = TabletCommandEnvelope::new(
        request_id.clone(),
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::Noop(NoopCommand),
    )
    .map_err(|error| error.to_string())?;
    let bytes = envelope.encode().map_err(|error| error.to_string())?;
    let index = ready_loop
        .propose(bytes.clone(), bytes.len())
        .map_err(|error| error.to_string())?;
    internal_barrier_allocator.record_admission(request_id.sequence);
    *activation = Some(ragnordb_multiraft::proposal::ProposalPosition { term, index });
    drain_ready(
        ready_loop,
        tablet,
        registry,
        database,
        transport,
        snapshot_endpoint,
        latest_snapshot,
        catalog_cache,
        snapshot_policy,
    )
}

#[allow(clippy::too_many_arguments)]
fn maybe_publish_snapshot<W, LS, SS>(
    ready_loop: &mut RaftReadyLoop<W, LS, SS>,
    tablet: &mut TabletCommandApplier,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    database: &SharedLocalDatabase,
    transport: &TcpTransport<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>,
    snapshot_endpoint: &SnapshotEndpoint,
    store: &FileTabletSnapshotStore,
    work: &SnapshotWorkController,
    cluster_id: &str,
    latest_snapshot: &mut Option<TabletSnapshotImage>,
    last_snapshot_index: &mut u64,
    catalog_cache: &dyn CatalogCacheWriter,
    snapshot_policy: &SnapshotPolicy,
    last_snapshot_at: &mut Instant,
) -> std::result::Result<(), String>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    let Some(frontier) = ready_loop.applied_frontier() else {
        return Ok(());
    };
    if !snapshot_policy.is_due(frontier.index, *last_snapshot_index, *last_snapshot_at) {
        return Ok(());
    }

    let local_replica_id = ReplicaId(ready_loop.raft().id().get());
    let conf_state = tablet_snapshot_conf_state(ready_loop.raft().conf_state())?;
    let snapshot_id = store
        .allocate_snapshot_id(GROUP_ID, local_replica_id, TABLET_ID)
        .map_err(|error| error.to_string())?;
    let image = generate_tablet_snapshot_from_ready_loop(
        work,
        ready_loop,
        tablet.state_machine(),
        cluster_id,
        local_replica_id,
        snapshot_id,
        conf_state,
    )
    .map_err(|error| error.to_string())?;
    let pointer = store.publish(&image).map_err(|error| error.to_string())?;
    let identity = ready_loop.persistence().log_view().identity();
    let raft_pointer =
        raft_pointer_for_tablet(identity, &pointer).map_err(|error| error.to_string())?;
    persist_tablet_snapshot_boundary_via_ready_loop(
        ready_loop,
        &pointer,
        AppliedTabletFrontier::new(frontier.index, frontier.term),
        ready_loop.raft().hard_state(),
    )
    .map_err(|error| error.to_string())?;
    let core_snapshot = TabletSnapshotTransfer::from_image(image.clone())
        .map_err(|error| error.to_string())?
        .into_core_snapshot();
    ready_loop
        .restore_persisted_snapshot(&raft_pointer, core_snapshot)
        .map_err(|error| error.to_string())?;

    *last_snapshot_index = frontier.index;
    *last_snapshot_at = Instant::now();
    snapshot_policy.reset();
    *latest_snapshot = Some(image);
    drain_ready(
        ready_loop,
        tablet,
        registry,
        database,
        transport,
        snapshot_endpoint,
        latest_snapshot,
        catalog_cache,
        snapshot_policy,
    )?;
    release_replica_retention(ready_loop)?;
    store
        .prune_older_snapshots(&pointer)
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn install_received_snapshot<W, LS, SS>(
    received: ReceivedTabletSnapshot,
    ready_loop: &mut RaftReadyLoop<W, LS, SS>,
    tablet: &mut TabletCommandApplier,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    database: &SharedLocalDatabase,
    transport: &TcpTransport<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>,
    snapshot_endpoint: &SnapshotEndpoint,
    store: &FileTabletSnapshotStore,
    work: &SnapshotWorkController,
    cluster_id: &str,
    latest_snapshot: &mut Option<TabletSnapshotImage>,
    catalog_cache: &dyn CatalogCacheWriter,
    snapshot_policy: &SnapshotPolicy,
) -> std::result::Result<(), String>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    let local_replica_id = ReplicaId(ready_loop.raft().id().get());
    if received.metadata.cluster_id != cluster_id
        || received.metadata.replica_id != local_replica_id
    {
        return Err("received snapshot does not belong to this cluster replica".to_string());
    }
    let target = TabletSnapshotInstallTarget {
        cluster_id: cluster_id.to_string(),
        raft_group_id: GROUP_ID,
        tablet_id: TABLET_ID,
        table_id: TABLE_ID,
        tablet_epoch: TABLET_EPOCH,
    };
    let mut hard_state = ready_loop.raft().hard_state();
    hard_state.commit = hard_state.commit.max(received.metadata.last_included_index);
    let durable = install_incoming_tablet_snapshot(
        work,
        store,
        received.session,
        &target,
        ready_loop,
        hard_state,
    )
    .map_err(|error| error.to_string())?;
    let image = store
        .load_verified(&durable.installed.pointer)
        .map_err(|error| error.to_string())?;
    let core_snapshot = TabletSnapshotTransfer::from_image(image.clone())
        .map_err(|error| error.to_string())?
        .into_core_snapshot();
    ready_loop
        .complete_snapshot_install(core_snapshot)
        .map_err(|error| error.to_string())?;
    let identity = ready_loop.persistence().log_view().identity();
    let raft_pointer = raft_pointer_for_tablet(identity, &durable.installed.pointer)
        .map_err(|error| error.to_string())?;
    let ready = ready_loop
        .persist_ready_after_snapshot_boundary(&raft_pointer)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "completed snapshot install produced no Ready generation".to_string())?;

    *tablet = TabletCommandApplier::new(durable.installed.state_machine);
    database
        .blocking_lock()
        .install_replicated_storage(TABLE_ID, tablet.state_machine().tablet().storage().clone())
        .map_err(|error| error.to_string())?;

    let mut frontier = AppliedRaftFrontier::new(
        image.metadata.last_included_index,
        image.metadata.last_included_term,
    );
    for entry in &ready.committed_entries {
        frontier = AppliedRaftFrontier::new(entry.index, entry.term);
        let EntryPayload::Normal(bytes) = &entry.payload else {
            continue;
        };
        let envelope = TabletCommandEnvelope::decode(bytes).map_err(|error| error.to_string())?;
        let locally_proposed = registry.is_pending(&envelope.request_id);
        let disposition = tablet
            .apply_committed(
                ragnordb_multiraft::proposal::ProposalPosition {
                    term: entry.term,
                    index: entry.index,
                },
                bytes,
            )
            .map_err(|error| error.to_string())?;
        snapshot_policy.note_applied(bytes.len());
        publish_committed_command(
            &envelope,
            locally_proposed,
            disposition,
            registry,
            database,
            catalog_cache,
        )?;
    }
    ready_loop
        .advance_applied_frontier(frontier)
        .map_err(|error| error.to_string())?;
    *latest_snapshot = Some(image);
    snapshot_policy.reset();
    send_messages(
        transport,
        snapshot_endpoint,
        latest_snapshot,
        ready.messages,
    );
    release_replica_retention(ready_loop)?;
    store
        .prune_older_snapshots(&durable.installed.pointer)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn release_replica_retention<W, LS, SS>(
    ready_loop: &mut RaftReadyLoop<W, LS, SS>,
) -> std::result::Result<(), String>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    let floor = ready_loop
        .persistence()
        .minimum_recovery_lsn()
        .unwrap_or(Lsn::ZERO);
    ready_loop
        .release_retention(floor)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn tablet_snapshot_conf_state(
    conf_state: &raft::types::ConfState,
) -> std::result::Result<TabletSnapshotConfState, String> {
    TabletSnapshotConfState::new(
        conf_state.version,
        conf_state
            .voters
            .iter()
            .map(|replica_id| ReplicaId(replica_id.get())),
        conf_state
            .learners
            .iter()
            .map(|replica_id| ReplicaId(replica_id.get())),
        conf_state
            .outgoing_voters
            .iter()
            .map(|replica_id| ReplicaId(replica_id.get())),
    )
    .map_err(|error| error.to_string())
}

fn envelope_from_commit(
    local_id: u64,
    commit: SingleNodeTxnCommit,
) -> Result<TabletCommandEnvelope> {
    if commit.table_id != TABLE_ID {
        return Err(Error::InvalidArgument(format!(
            "Milestone 4 replicated tablet owns table {}, received table {}",
            TABLE_ID.0, commit.table_id.0
        )));
    }
    let writes = commit
        .writes
        .into_iter()
        .map(|(key, mutation)| match mutation {
            WalMutation::Put(row) => Ok(WriteEntry {
                key,
                row: Some(decode_row(&row)?),
                op: WriteKind::Put,
            }),
            WalMutation::Delete => Ok(WriteEntry {
                key,
                row: None,
                op: WriteKind::Delete,
            }),
        })
        .collect::<Result<Vec<_>>>()?;
    let client_id = (u128::from(local_id) << 64) | u128::from(commit.txn_id.0);
    TabletCommandEnvelope::new(
        RequestId {
            client_id,
            sequence: 1,
            raft_group_id: GROUP_ID,
        },
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::SingleShardCommit(SingleShardCommitCommand {
            txn_id: commit.txn_id,
            start_timestamp: commit.start_timestamp,
            commit_timestamp: commit.commit_timestamp,
            writes,
        }),
    )
    .map_err(|source| Error::InvalidArgument(source.to_string()))
}

fn envelope_from_catalog(
    local_id: u64,
    update: &CatalogLogRecord,
) -> Result<TabletCommandEnvelope> {
    let namespace = local_id.rotate_left(17) ^ update.table_id.0;
    let client_id = (u128::from(update.update_timestamp.0) << 64) | u128::from(namespace);
    TabletCommandEnvelope::new(
        RequestId {
            client_id,
            sequence: 1,
            raft_group_id: GROUP_ID,
        },
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::Catalog(update.command.clone()),
    )
    .map_err(|source| Error::InvalidArgument(source.to_string()))
}

/// Publish every durable side effect of an applied command before making its
/// result or applied frontier visible to the rest of the process.
///
/// In particular, a locally proposed catalog update must reach the recoverable
/// catalog cache before its proposal waiter can observe success. Returning an
/// error leaves that waiter pending and prevents the caller from advancing the
/// applied frontier or generating a snapshot past the missing cache record.
fn publish_committed_command(
    envelope: &TabletCommandEnvelope,
    locally_proposed: bool,
    disposition: CommittedTabletCommandDisposition,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    database: &SharedLocalDatabase,
    catalog_cache: &dyn CatalogCacheWriter,
) -> std::result::Result<(), String> {
    if matches!(disposition, CommittedTabletCommandDisposition::Applied(_)) {
        if let TabletCommand::SingleShardCommit(command) = &envelope.command
            && !locally_proposed
        {
            database
                .blocking_lock()
                .apply_replicated_commit(command)
                .map_err(|error| error.to_string())?;
        }

        if let TabletCommand::Catalog(command) = &envelope.command {
            let update_timestamp =
                ragnordb_common::ids::Timestamp((envelope.request_id.client_id >> 64) as u64);
            catalog_cache
                .append_catalog_update(&CatalogLogRecord {
                    table_id: TABLE_ID,
                    update_timestamp,
                    command: command.clone(),
                })
                .map_err(|error| error.to_string())?;

            // Followers do not have a local SQL execution path that publishes
            // the schema, so their in-memory catalog mirror is updated here.
            if !locally_proposed {
                database
                    .blocking_lock()
                    .apply_replicated_catalog(command, update_timestamp)
                    .map_err(|error| error.to_string())?;
            }
        }
    }

    if locally_proposed {
        disposition
            .resolve(registry)
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn drain_ready<W, LS, SS>(
    ready_loop: &mut RaftReadyLoop<W, LS, SS>,
    tablet: &mut TabletCommandApplier,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    database: &SharedLocalDatabase,
    transport: &TcpTransport<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>,
    snapshot_endpoint: &SnapshotEndpoint,
    latest_snapshot: &Option<TabletSnapshotImage>,
    catalog_cache: &dyn CatalogCacheWriter,
    snapshot_policy: &SnapshotPolicy,
) -> std::result::Result<(), String>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    while let Some(ready) = ready_loop
        .persist_next_ready(None)
        .map_err(|error| error.to_string())?
    {
        let mut frontier = None;
        for entry in &ready.committed_entries {
            frontier = Some(AppliedRaftFrontier::new(entry.index, entry.term));
            let EntryPayload::Normal(bytes) = &entry.payload else {
                continue;
            };
            let envelope =
                TabletCommandEnvelope::decode(bytes).map_err(|error| error.to_string())?;
            let locally_proposed = registry.is_pending(&envelope.request_id);
            let disposition = tablet
                .apply_committed(
                    ragnordb_multiraft::proposal::ProposalPosition {
                        term: entry.term,
                        index: entry.index,
                    },
                    bytes,
                )
                .map_err(|error| error.to_string())?;
            snapshot_policy.note_applied(bytes.len());
            publish_committed_command(
                &envelope,
                locally_proposed,
                disposition,
                registry,
                database,
                catalog_cache,
            )?;
        }
        if let Some(frontier) = frontier {
            ready_loop
                .advance_applied_frontier(frontier)
                .map_err(|error| error.to_string())?;
        }
        send_messages(
            transport,
            snapshot_endpoint,
            latest_snapshot,
            ready.messages,
        );
    }
    Ok(())
}

fn send_messages(
    transport: &TcpTransport<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>,
    snapshot_endpoint: &SnapshotEndpoint,
    latest_snapshot: &Option<TabletSnapshotImage>,
    messages: Vec<Envelope<Vec<u8>, Vec<u8>>>,
) {
    for message in messages {
        let target = message.to;
        let carries_snapshot = matches!(message.msg, Message::InstallSnapshot(_));
        if let Err(source) = transport.try_send(message) {
            warn!(
                from = transport.local_id().get(),
                to = target.get(),
                error = %source,
                "Raft message could not be delivered; Raft will retry"
            );
        } else if carries_snapshot && let Some(image) = latest_snapshot.clone() {
            snapshot_endpoint.send(target.get(), image);
        }
    }
}

fn forward_completions(clients: &mut Vec<PendingClient>) {
    let mut pending = Vec::with_capacity(clients.len());
    for client in clients.drain(..) {
        match client.ticket.try_recv() {
            Ok(completion) => forward_completion(client.reply, completion),
            Err(mpsc::TryRecvError::Empty) => pending.push(client),
            Err(mpsc::TryRecvError::Disconnected) => send_client_error(
                client.reply,
                Error::ProposalUnavailable {
                    reason: "proposal completion channel closed".to_string(),
                },
            ),
        }
    }
    *clients = pending;
}

fn forward_completion(reply: ClientReply, completion: Completion) {
    let error = match completion {
        ProposalCompletion::Applied { position, .. } => match reply {
            ClientReply::Commit(sender) => {
                let _ = sender.send(Ok(DurableWalExtent::from_raw(
                    position.index,
                    position.index.saturating_add(1),
                )));
                return;
            }
            ClientReply::Barrier(sender) => {
                let _ = sender.send(Ok(()));
                return;
            }
            ClientReply::Catalog(sender) => {
                let _ = sender.send(Ok(CatalogLogExtent {
                    start_lsn: position.index,
                    end_lsn: position.index.saturating_add(1),
                }));
                return;
            }
        },
        ProposalCompletion::Rejected { rejection, .. } => map_tablet_rejection(rejection),
        ProposalCompletion::Retryable { failure, .. } => Error::ProposalUnavailable {
            reason: format!("{failure:?}"),
        },
    };
    send_client_error(reply, error);
}

fn map_tablet_rejection(rejection: TabletCommandApplyError) -> Error {
    match rejection {
        TabletCommandApplyError::WriteConflict { reason } => Error::WriteConflict(reason),
        other => Error::InvalidArgument(other.to_string()),
    }
}

fn reply_error(request: HostRequest, error: Error) {
    match request {
        HostRequest::Commit { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        HostRequest::Barrier { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        HostRequest::Catalog { reply, .. } => {
            let _ = reply.send(Err(error));
        }
    }
}

fn send_client_error(reply: ClientReply, error: Error) {
    match reply {
        ClientReply::Commit(sender) => {
            let _ = sender.send(Err(error));
        }
        ClientReply::Barrier(sender) => {
            let _ = sender.send(Err(error));
        }
        ClientReply::Catalog(sender) => {
            let _ = sender.send(Err(error));
        }
    }
}

fn publish_status<W, LS, SS>(
    ready_loop: &RaftReadyLoop<W, LS, SS>,
    serving_leader: bool,
    snapshot_index: u64,
    status: &RwLock<ReplicatedTabletStatus>,
) where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    let mut published = status
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    published.leader_replica_id = ready_loop
        .raft()
        .leader_id()
        .map(|replica_id| replica_id.get());
    published.term = ready_loop.raft().hard_state().current_term;
    published.commit_index = ready_loop.raft().hard_state().commit;
    published.last_log_index = ready_loop.raft().last_log_index();
    published.serving_leader = serving_leader;
    published.snapshot_index = snapshot_index;
    published.applied_index = ready_loop
        .applied_frontier()
        .map(|frontier| frontier.index)
        .unwrap_or(0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ragnordb_common::{
        catalog_codec::TableDefinition,
        command_codec::{CatalogCommand, CatalogOperation, CreateTableOperation},
        ids::Timestamp,
    };
    use ragnordb_multiraft::{proposal::ProposalPosition, tablet_apply::AppliedTabletCommand};
    use ragnordb_tablet::command::TabletCommandApplyResult;

    struct OutcomeUnknownCatalogCache;

    impl CatalogCacheWriter for OutcomeUnknownCatalogCache {
        fn append_catalog_update(&self, _update: &CatalogLogRecord) -> Result<CatalogLogExtent> {
            Err(Error::CatalogOutcomeUnknown {
                start_lsn: 40,
                end_lsn: 50,
                reason: "injected synchronization ambiguity".to_string(),
            })
        }
    }

    /// Realistic bug caught: a committed CREATE TABLE could previously resolve
    /// its client waiter before the recoverable catalog cache existed, allowing
    /// a snapshot and crash to permanently lose the acknowledged schema.
    #[test]
    fn catalog_cache_failure_keeps_the_client_proposal_unresolved() {
        let record = CatalogLogRecord {
            table_id: TABLE_ID,
            update_timestamp: Timestamp(7),
            command: CatalogCommand {
                operation: CatalogOperation::CreateTable(CreateTableOperation {
                    table_def: TableDefinition {
                        table_id: TABLE_ID.0,
                        name: "users".to_string(),
                        columns: Vec::new(),
                        primary_key_column_ids: Vec::new(),
                        schema_version: 1,
                        tablet_count: 1,
                    },
                }),
            },
        };
        let envelope = envelope_from_catalog(1, &record).expect("catalog envelope must encode");
        let position = ProposalPosition { term: 2, index: 9 };
        let mut registry = ProposalRegistry::new();
        let ticket = registry
            .register(
                envelope.request_id.clone(),
                position,
                Instant::now() + Duration::from_secs(1),
            )
            .expect("proposal registration must succeed");
        let disposition = CommittedTabletCommandDisposition::Applied(AppliedTabletCommand {
            request_id: envelope.request_id.clone(),
            position,
            outcome: TabletCommandApplyOutcome {
                result: TabletCommandApplyResult::Noop,
                deduplicated: false,
            },
        });

        let error = publish_committed_command(
            &envelope,
            true,
            disposition,
            &mut registry,
            &crate::database::LocalDatabase::shared(),
            &OutcomeUnknownCatalogCache,
        )
        .expect_err("uncertain catalog persistence must stop Ready publication");

        assert!(error.contains("injected synchronization ambiguity"));
        assert_eq!(registry.pending_count(), 1);
        assert!(matches!(ticket.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }

    /// Realistic bug caught: Raft can reject a barrier before appending it when
    /// a proposal budget is exhausted. Retrying that read must reuse the same
    /// client sequence because no command with the first sequence can apply.
    #[test]
    fn rejected_internal_barrier_proposal_reuses_its_candidate_sequence() {
        let mut allocator = InternalBarrierAllocator::default();
        allocator.activate_term_for_test(7, 6);

        let rejected = allocator
            .candidate_for_active_term()
            .expect("the first barrier candidate must be available");
        assert_eq!(rejected.sequence, 6);

        // Model Raft rejecting the proposal before it appends a log entry.
        let retry = allocator
            .candidate_for_active_term()
            .expect("a rejected proposal must leave the candidate reusable");
        assert_eq!(retry.sequence, 6);

        allocator.record_admission(retry.sequence);
        assert_eq!(
            allocator
                .candidate_for_active_term()
                .expect("the following admitted proposal must advance the sequence")
                .sequence,
            7
        );
    }

    /// Realistic bug caught: an old leader can have admitted barriers that are
    /// later discarded by a newer term. If it leads again, its new barriers
    /// must not continue that discarded client sequence.
    #[test]
    fn leadership_term_change_uses_a_fresh_internal_barrier_client() {
        let mut allocator = InternalBarrierAllocator::default();
        allocator.activate_term_for_test(7, 4);

        let old_term = allocator
            .candidate_for_active_term()
            .expect("the old leader must have a barrier candidate");
        allocator.record_admission(old_term.sequence);
        let another_old_term = allocator
            .candidate_for_active_term()
            .expect("the old leader can have another admitted pending barrier");
        allocator.record_admission(another_old_term.sequence);

        allocator.activate_term_for_test(8, 1);
        let new_term = allocator
            .candidate_for_active_term()
            .expect("the new leader term must have a barrier candidate");

        assert_ne!(new_term.client_id, old_term.client_id);
        assert_eq!(new_term.client_id, internal_barrier_client_id(8));
        assert_eq!(new_term.sequence, 1);
    }
}
