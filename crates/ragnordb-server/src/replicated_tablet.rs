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
        atomic::{AtomicBool, Ordering},
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
    storage::{persistence::RaftWal, recovery::recover_raft_storage_with_configurations},
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
const INTERNAL_BARRIER_CLIENT_ID: u128 = 0x0052_4147_4e4f_5244_4242_4152_5249_4552;
const MAX_SNAPSHOT_FILE_BYTES: u64 = 512 * 1024 * 1024;
const SNAPSHOT_CHUNK_BYTES: u64 = 64 * 1024;
const SNAPSHOT_INTERVAL_ENTRIES: u64 = 4;

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

/// Cloneable SQL-side handle for one production tablet host.
pub struct ReplicatedTabletHandle {
    requests: SyncSender<HostRequest>,
    status: Arc<RwLock<ReplicatedTabletStatus>>,
    catalog_cache: Arc<RagnorDbWalAdapter<FsSegmentDirectory, ()>>,
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
        let raft_extent =
            response
                .recv_timeout(timeout)
                .map_err(|_| Error::ProposalUnavailable {
                    reason: "catalog deadline elapsed before tablet apply".to_string(),
                })??;
        // Raft is authoritative. This second record is a derived local cache
        // that keeps SQL schema recovery available after Raft log compaction.
        self.catalog_cache.append_catalog_update(update)?;
        Ok(raft_extent)
    }
}

/// Lifecycle guard for the background Ready owner and its cloneable SQL handle.
pub struct ReplicatedTabletRuntime {
    handle: Arc<ReplicatedTabletHandle>,
    shutdown: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl ReplicatedTabletRuntime {
    /// Recover or bootstrap the configured local replica, bind its Raft TCP
    /// endpoint, and start the sole owner of tick/message/Ready/apply ordering.
    pub fn start(
        config: &NodeConfig,
        wal: LocalWal,
        database: SharedLocalDatabase,
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
        let replica_to_node = config
            .seed_nodes
            .iter()
            .map(|seed| (ReplicaId(seed.id.0), seed.id))
            .collect::<BTreeMap<_, _>>();
        let voters = replica_to_node.keys().copied().collect::<BTreeSet<_>>();
        let requested = RaftGroupBootstrap::new(
            cluster_id.clone(),
            GROUP_ID,
            1,
            replica_to_node,
            voters,
            BTreeSet::new(),
        )
        .map_err(|source| Error::Configuration(source.to_string()))?;

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
                MAX_SNAPSHOT_FILE_BYTES,
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
            SNAPSHOT_CHUNK_BYTES,
            shutdown.clone(),
        )
        .map_err(|source| Error::Configuration(format!("bind snapshot endpoint: {source}")))?;
        let catalog_cache = Arc::new(RagnorDbWalAdapter::new(wal.clone()));
        let handle = Arc::new(ReplicatedTabletHandle {
            requests: request_tx,
            status: status.clone(),
            catalog_cache: catalog_cache.clone(),
        });

        let durable_bootstrap =
            load_durable_group_bootstrap(&bootstrap_store, GROUP_ID).map_err(|source| {
                Error::RecoveryFailed {
                    reason: source.to_string(),
                }
            })?;
        let worker_shutdown = shutdown.clone();

        let worker = if let Some(bootstrap) = durable_bootstrap {
            // Durable bootstrap is authoritative after restart; changed static
            // membership cannot silently replace it.
            let (identity, conf_state) =
                initial_recovery_configuration(&bootstrap, local_replica_id).map_err(|source| {
                    Error::RecoveryFailed {
                        reason: source.to_string(),
                    }
                })?;
            let mut configurations = BTreeMap::new();
            configurations.insert(identity, conf_state);
            let mut source = wal
                .iter_from(Lsn::ZERO)
                .map_err(|source| Error::RecoveryFailed {
                    reason: format!("open Raft recovery stream: {source}"),
                })?;
            let recovered = recover_raft_storage_with_configurations(&mut source, &configurations)
                .map_err(|source| Error::RecoveryFailed {
                    reason: source.to_string(),
                })?;
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
                wal.clone(),
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
                wal,
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
    catalog_cache: Arc<RagnorDbWalAdapter<FsSegmentDirectory, ()>>,
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
    catalog_cache: Arc<RagnorDbWalAdapter<FsSegmentDirectory, ()>>,
) -> std::result::Result<(), String>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    let mut registry = ProposalRegistry::new();
    let mut clients = Vec::<PendingClient>::new();
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
                &catalog_cache,
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
                &catalog_cache,
            )?;
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
            &catalog_cache,
            &mut leader_activation,
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
            );
            drain_ready(
                &mut ready_loop,
                &mut tablet,
                &mut registry,
                &database,
                &transport,
                &snapshot_endpoint,
                &latest_snapshot,
                &catalog_cache,
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
                &catalog_cache,
            )?;
        }

        registry.expire_deadlines(now);
        let is_leader = ready_loop.raft().leader_id() == Some(ready_loop.raft().id());
        if was_leader && !is_leader {
            registry.mark_leadership_lost(ready_loop.raft().hard_state().current_term);
            leader_activation = None;
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
                &catalog_cache,
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

fn admit_request<W, LS, SS>(
    request: HostRequest,
    ready_loop: &mut RaftReadyLoop<W, LS, SS>,
    tablet: &TabletCommandApplier,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    clients: &mut Vec<PendingClient>,
    serving_leader: bool,
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

    let (envelope, deadline, reply) = match request {
        HostRequest::Commit {
            commit,
            reply,
            deadline,
        } => match envelope_from_commit(local_id.get(), commit) {
            Ok(envelope) => (envelope, deadline, ClientReply::Commit(reply)),
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
            Ok(envelope) => (envelope, deadline, ClientReply::Catalog(reply)),
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        },
        HostRequest::Barrier { reply, deadline } => {
            let sequence = match tablet
                .state_machine()
                .next_sequence_for_client(INTERNAL_BARRIER_CLIENT_ID)
            {
                Ok(sequence) => sequence,
                Err(source) => {
                    let _ = reply.send(Err(Error::RecoveryRequired {
                        reason: source.to_string(),
                    }));
                    return;
                }
            };
            let envelope = TabletCommandEnvelope::new(
                RequestId {
                    client_id: INTERNAL_BARRIER_CLIENT_ID,
                    sequence,
                    raft_group_id: GROUP_ID,
                },
                TABLET_ID,
                TABLET_EPOCH,
                TabletCommand::Noop(NoopCommand),
            )
            .map_err(|source| Error::InvalidArgument(source.to_string()));
            match envelope {
                Ok(envelope) => (envelope, deadline, ClientReply::Barrier(reply)),
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
    catalog_cache: &RagnorDbWalAdapter<FsSegmentDirectory, ()>,
    activation: &mut Option<ragnordb_multiraft::proposal::ProposalPosition>,
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

    let sequence = tablet
        .state_machine()
        .next_sequence_for_client(INTERNAL_BARRIER_CLIENT_ID)
        .map_err(|error| error.to_string())?;
    let envelope = TabletCommandEnvelope::new(
        RequestId {
            client_id: INTERNAL_BARRIER_CLIENT_ID,
            sequence,
            raft_group_id: GROUP_ID,
        },
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::Noop(NoopCommand),
    )
    .map_err(|error| error.to_string())?;
    let bytes = envelope.encode().map_err(|error| error.to_string())?;
    let index = ready_loop
        .propose(bytes.clone(), bytes.len())
        .map_err(|error| error.to_string())?;
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
    catalog_cache: &RagnorDbWalAdapter<FsSegmentDirectory, ()>,
) -> std::result::Result<(), String>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    let Some(frontier) = ready_loop.applied_frontier() else {
        return Ok(());
    };
    if frontier.index.saturating_sub(*last_snapshot_index) < SNAPSHOT_INTERVAL_ENTRIES {
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
    )
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
    catalog_cache: &RagnorDbWalAdapter<FsSegmentDirectory, ()>,
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
        let mirrored_commit = match &envelope.command {
            TabletCommand::SingleShardCommit(command) if !locally_proposed => Some(command.clone()),
            _ => None,
        };
        let mirrored_catalog = match &envelope.command {
            TabletCommand::Catalog(command) if !locally_proposed => Some((
                command.clone(),
                ragnordb_common::ids::Timestamp((envelope.request_id.client_id >> 64) as u64),
            )),
            _ => None,
        };
        let disposition = tablet
            .apply_committed(
                ragnordb_multiraft::proposal::ProposalPosition {
                    term: entry.term,
                    index: entry.index,
                },
                bytes,
            )
            .map_err(|error| error.to_string())?;
        if let Some(command) = mirrored_commit
            && matches!(disposition, CommittedTabletCommandDisposition::Applied(_))
        {
            database
                .blocking_lock()
                .apply_replicated_commit(&command)
                .map_err(|error| error.to_string())?;
        }
        if let Some((command, update_timestamp)) = mirrored_catalog
            && matches!(disposition, CommittedTabletCommandDisposition::Applied(_))
        {
            catalog_cache
                .append_catalog_update(&CatalogLogRecord {
                    table_id: TABLE_ID,
                    update_timestamp,
                    command: command.clone(),
                })
                .map_err(|error| error.to_string())?;
            database
                .blocking_lock()
                .apply_replicated_catalog(&command, update_timestamp)
                .map_err(|error| error.to_string())?;
        }
        if locally_proposed {
            disposition
                .resolve(registry)
                .map_err(|error| error.to_string())?;
        }
    }
    ready_loop
        .advance_applied_frontier(frontier)
        .map_err(|error| error.to_string())?;
    *latest_snapshot = Some(image);
    send_messages(
        transport,
        snapshot_endpoint,
        latest_snapshot,
        ready.messages,
    );
    Ok(())
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

#[allow(clippy::too_many_arguments)]
fn drain_ready<W, LS, SS>(
    ready_loop: &mut RaftReadyLoop<W, LS, SS>,
    tablet: &mut TabletCommandApplier,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    database: &SharedLocalDatabase,
    transport: &TcpTransport<Vec<u8>, Vec<u8>, BytesCodec, BytesCodec>,
    snapshot_endpoint: &SnapshotEndpoint,
    latest_snapshot: &Option<TabletSnapshotImage>,
    catalog_cache: &RagnorDbWalAdapter<FsSegmentDirectory, ()>,
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
            let mirrored_commit = match &envelope.command {
                TabletCommand::SingleShardCommit(command) if !locally_proposed => {
                    Some(command.clone())
                }
                _ => None,
            };
            let mirrored_catalog = match &envelope.command {
                TabletCommand::Catalog(command) if !locally_proposed => Some((
                    command.clone(),
                    ragnordb_common::ids::Timestamp((envelope.request_id.client_id >> 64) as u64),
                )),
                _ => None,
            };
            let disposition = tablet
                .apply_committed(
                    ragnordb_multiraft::proposal::ProposalPosition {
                        term: entry.term,
                        index: entry.index,
                    },
                    bytes,
                )
                .map_err(|error| error.to_string())?;

            if let Some(command) = mirrored_commit
                && matches!(disposition, CommittedTabletCommandDisposition::Applied(_))
            {
                database
                    .blocking_lock()
                    .apply_replicated_commit(&command)
                    .map_err(|error| error.to_string())?;
            }
            if let Some((command, update_timestamp)) = mirrored_catalog
                && matches!(disposition, CommittedTabletCommandDisposition::Applied(_))
            {
                catalog_cache
                    .append_catalog_update(&CatalogLogRecord {
                        table_id: TABLE_ID,
                        update_timestamp,
                        command: command.clone(),
                    })
                    .map_err(|error| error.to_string())?;
                database
                    .blocking_lock()
                    .apply_replicated_catalog(&command, update_timestamp)
                    .map_err(|error| error.to_string())?;
            }
            if locally_proposed {
                disposition
                    .resolve(registry)
                    .map_err(|error| error.to_string())?;
            }
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
