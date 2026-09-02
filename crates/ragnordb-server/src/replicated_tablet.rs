//! Production Ready owner for one replicated tablet Raft group.
//!
//! The physical MultiRaft scheduler owns cross-group fairness. This module
//! owns the tablet group's worker boundary: SQL, snapshot, and Ready state
//! transitions remain serialized here while the scheduler interacts through a
//! bounded, non-blocking control bridge.

use std::{
    collections::{BTreeMap, BTreeSet},
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
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{HardState, LogIndex, SnapshotMetadata},
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
    host::{
        HostedGroupError, HostedGroupTurn, HostedRaftGroup, MultiRaftGroupStatus, MultiRaftRole,
        MultiRaftTurnBudget, RaftMessageEnvelope, classify_ready_error,
    },
    proposal::{ProposalCompletion, ProposalRegistry, ProposalTicket},
    replica_startup::{bootstrap_tablet_replica, recover_tablet_replica},
    runtime::{AppliedRaftFrontier, RaftReadyLoop},
    snapshot::{
        PreparedIncomingTabletSnapshotInstall, SnapshotWorkController, SnapshotWorkError,
        SnapshotWorkKind, TabletSnapshotIntegrationError, TabletSnapshotTransfer,
        generate_tablet_snapshot_from_ready_loop, install_incoming_tablet_snapshot,
        persist_tablet_snapshot_boundary_via_ready_loop, prepare_incoming_tablet_snapshot,
        raft_metadata_for_tablet, raft_pointer_for_tablet,
    },
    storage::{
        codec::{RaftReplicaIdentity, RaftSnapshotPointerRecord},
        persistence::{NodeRaftWalHandle, RaftWal},
        recovery::RecoveredRaftStorage,
    },
    tablet_apply::{CommittedTabletCommandDisposition, TabletCommandApplier},
    transport::GroupRaftTransport,
};

use ragnordb_storage::wal::{
    DurableCommitLog, DurableWalExtent, RagnorDbWalAdapter, SingleNodeTxnCommit, WalMutation,
};
use ragnordb_tablet::{
    command::{TabletCommandApplyError, TabletCommandApplyOutcome},
    snapshot::{
        AppliedTabletFrontier, FileTabletSnapshotStore, TabletSnapshotConfState,
        TabletSnapshotImage, TabletSnapshotInstallTarget, TabletSnapshotPointer,
    },
};
use tracing::{error, warn};
use wal::{io::directory::FsSegmentDirectory, lsn::Lsn, wal::WalHandle};

use crate::{
    config::NodeConfig,
    database::SharedLocalDatabase,
    snapshot_transport::{GroupSnapshotEndpoint, ReceivedTabletSnapshot},
};

pub(crate) const TABLET_RAFT_GROUP_ID: RaftGroupId = RaftGroupId(1);
const TABLET_ID: TabletId = TabletId(1);
const TABLE_ID: TableId = TableId(1);
const TABLET_EPOCH: u64 = 1;
const ELECTION_TIMEOUT_TICKS: u64 = 10;
const HEARTBEAT_INTERVAL_TICKS: u64 = 3;
const CHANNEL_CAPACITY: usize = 1_024;
const TABLET_CONTROL_BUDGET: usize = 64;
const TABLET_REQUEST_BUDGET: usize = 64;
const INTERNAL_BARRIER_CLIENT_NAMESPACE: u64 = 0x5241_474e_4f52_4442;

type LocalWal = WalHandle<FsSegmentDirectory, ()>;
type Completion = ProposalCompletion<TabletCommandApplyOutcome, TabletCommandApplyError>;

/// Point-in-time routing state published without exposing the Ready owner.
#[derive(Debug, Clone, Default)]
pub struct ReplicatedTabletStatus {
    pub role: Option<MultiRaftRole>,
    pub leader_replica_id: Option<u64>,
    pub term: u64,
    pub commit_index: u64,
    pub last_log_index: u64,
    pub applied_index: u64,
    pub snapshot_index: u64,
    pub uncommitted_bytes: usize,
    pub replication_inflight_bytes: usize,
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

/// Node-level MultiRaft control messages.
///
/// Client SQL work continues to use `HostRequest`. Raft transport and ticking
/// enter through this separate host-owned channel.
enum RaftHostControlResult {
    Completed,
    Proposed(LogIndex),
}

enum RaftHostControl {
    Tick {
        ticks: u64,
        reply: mpsc::Sender<std::result::Result<RaftHostControlResult, HostedGroupError>>,
    },

    Step {
        message: RaftMessageEnvelope,
        reply: mpsc::Sender<std::result::Result<RaftHostControlResult, HostedGroupError>>,
    },

    Propose {
        command: Vec<u8>,
        encoded_len: usize,
        reply: mpsc::Sender<std::result::Result<RaftHostControlResult, HostedGroupError>>,
    },
}

/// Return a terminal runtime reason only for errors that cross a correctness
/// boundary.
///
/// `Rejected` and `Retryable` are operation outcomes, not replica-lifetime
/// failures. Killing the Ready owner for either one would turn an ordinary
/// rejection into a permanent group quarantine on the next host interaction.
fn fatal_host_control_reason<T>(
    result: &std::result::Result<T, HostedGroupError>,
) -> Option<String> {
    match result {
        Err(error @ HostedGroupError::RecoveryRequired) => Some(error.to_string()),
        Err(error @ HostedGroupError::Group(_)) => Some(error.to_string()),
        Ok(_) | Err(HostedGroupError::Retryable(_)) | Err(HostedGroupError::Rejected(_)) => None,
    }
}

/// Reject one host operation without mutating Raft while an exact snapshot
/// durability lifecycle owns the group's outstanding state.
///
/// Leaving the request queued would keep this group's host-control operation
/// outstanding and prevent the scheduler from making progress on that group.
/// Returning `Retryable` preserves both the snapshot ordering invariant and
/// cross-group failure isolation.
fn reply_snapshot_blocked_host_control(control: RaftHostControl, reason: &str) {
    match control {
        RaftHostControl::Tick { reply, .. } | RaftHostControl::Step { reply, .. } => {
            let _ = reply.send(Err(HostedGroupError::Retryable(reason.to_string())));
        }

        RaftHostControl::Propose { reply, .. } => {
            let _ = reply.send(Err(HostedGroupError::Retryable(reason.to_string())));
        }
    }
}

/// Drain host requests which cannot legally mutate this Raft group while a
/// snapshot persistence retry owns its exact Ready/frontier.
///
/// Requests are answered instead of merely left queued so the synchronous
/// node-level host remains free to service unrelated Raft groups.
fn reject_snapshot_blocked_host_controls(host_control: &Receiver<RaftHostControl>, reason: &str) {
    while let Ok(control) = host_control.try_recv() {
        reply_snapshot_blocked_host_control(control, reason);
    }
}

fn classify_snapshot_integration_error(error: TabletSnapshotIntegrationError) -> HostedGroupError {
    match error {
        TabletSnapshotIntegrationError::ReadyLoop(error) => classify_ready_error(error),
        error @ TabletSnapshotIntegrationError::Work(SnapshotWorkError::LimitReached { .. }) => {
            HostedGroupError::Retryable(error.to_string())
        }
        other => HostedGroupError::Group(other.to_string()),
    }
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

#[allow(clippy::large_enum_variant)]
enum PendingIncomingSnapshotInstall {
    Received {
        expected: SnapshotMetadata,
        received: ReceivedTabletSnapshot,
    },
    BoundaryPending {
        expected: SnapshotMetadata,
        prepared: PreparedIncomingTabletSnapshotInstall,
    },
    ReadyPending {
        expected: SnapshotMetadata,
        prepared: PreparedIncomingTabletSnapshotInstall,
        image: TabletSnapshotImage,
        raft_pointer: RaftSnapshotPointerRecord,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IncomingSnapshotPhase {
    Received,
    BoundaryPending,
    ReadyPending,
}

impl PendingIncomingSnapshotInstall {
    fn phase(&self) -> IncomingSnapshotPhase {
        match self {
            Self::Received { .. } => IncomingSnapshotPhase::Received,
            Self::BoundaryPending { .. } => IncomingSnapshotPhase::BoundaryPending,
            Self::ReadyPending { .. } => IncomingSnapshotPhase::ReadyPending,
        }
    }
}

fn snapshot_phase_blocks_host_control(phase: Option<IncomingSnapshotPhase>) -> bool {
    phase == Some(IncomingSnapshotPhase::ReadyPending)
}

fn snapshot_boundary_hard_state(
    mut current: HardState,
    frontier: AppliedTabletFrontier,
) -> HardState {
    current.commit = current.commit.max(frontier.index);
    current
}

fn prepare_local_snapshot_once<T, E>(
    pending: &mut Option<T>,
    prepare: impl FnOnce() -> std::result::Result<T, E>,
) -> std::result::Result<(), E> {
    if pending.is_none() {
        *pending = Some(prepare()?);
    }

    Ok(())
}

/// Locally generated snapshot whose immutable file is already published but
/// whose Raft/A-WAL boundary has not yet been acknowledged.
///
/// Retryable WAL admission must reuse this exact image and pointer. Generating
/// another immutable snapshot for every NotStaged(false) result would turn
/// ordinary backpressure into unbounded snapshot-file growth.
struct PendingLocalSnapshotPublication {
    frontier: AppliedTabletFrontier,
    image: TabletSnapshotImage,
    pointer: TabletSnapshotPointer,
    raft_pointer: RaftSnapshotPointerRecord,
}

pub(crate) struct ReplicatedTabletGroupProxy {
    identity: RaftReplicaIdentity,
    control: SyncSender<RaftHostControl>,
    status: Arc<RwLock<ReplicatedTabletStatus>>,
    pending: Option<mpsc::Receiver<std::result::Result<RaftHostControlResult, HostedGroupError>>>,
}

impl ReplicatedTabletGroupProxy {
    fn control_unavailable() -> HostedGroupError {
        HostedGroupError::Group("replicated tablet worker has stopped".to_string())
    }

    fn poll_pending(&mut self) -> std::result::Result<Option<HostedGroupTurn>, HostedGroupError> {
        let Some(response) = self.pending.as_ref() else {
            return Ok(None);
        };

        match response.try_recv() {
            Ok(result) => {
                self.pending = None;
                result.map(|_| Some(HostedGroupTurn::default()))
            }
            Err(mpsc::TryRecvError::Empty) => Ok(Some(HostedGroupTurn::default())),
            Err(mpsc::TryRecvError::Disconnected) => {
                self.pending = None;
                Err(Self::control_unavailable())
            }
        }
    }

    fn submit_budgeted(
        &mut self,
        control: impl FnOnce(
            mpsc::Sender<std::result::Result<RaftHostControlResult, HostedGroupError>>,
        ) -> RaftHostControl,
    ) -> std::result::Result<HostedGroupTurn, HostedGroupError> {
        if let Some(turn) = self.poll_pending()? {
            return Ok(turn);
        }

        let (reply, response) = mpsc::channel();
        match self.control.try_send(control(reply)) {
            Ok(()) => {
                self.pending = Some(response);
                Ok(HostedGroupTurn::default())
            }
            Err(mpsc::TrySendError::Full(_)) => Err(HostedGroupError::Retryable(
                "replicated tablet host-control queue is full".to_string(),
            )),
            Err(mpsc::TrySendError::Disconnected(_)) => Err(Self::control_unavailable()),
        }
    }

    fn submit_direct(
        &self,
        control: RaftHostControl,
    ) -> std::result::Result<RaftHostControlResult, HostedGroupError> {
        let (reply, response) = mpsc::channel();
        let control = match control {
            RaftHostControl::Tick { ticks, .. } => RaftHostControl::Tick { ticks, reply },
            RaftHostControl::Step { message, .. } => RaftHostControl::Step { message, reply },
            RaftHostControl::Propose {
                command,
                encoded_len,
                ..
            } => RaftHostControl::Propose {
                command,
                encoded_len,
                reply,
            },
        };

        self.control
            .send(control)
            .map_err(|_| Self::control_unavailable())?;
        response.recv().map_err(|_| Self::control_unavailable())?
    }
}

impl HostedRaftGroup for ReplicatedTabletGroupProxy {
    fn identity(&self) -> RaftReplicaIdentity {
        self.identity
    }

    fn status(&self) -> MultiRaftGroupStatus {
        let status = self
            .status
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();

        MultiRaftGroupStatus {
            identity: self.identity,
            role: status.role,
            leader_replica_id: status.leader_replica_id.map(ReplicaId),
            term: status.term,
            commit_index: status.commit_index,
            last_log_index: status.last_log_index,
            applied_index: status.applied_index,
            snapshot_index: status.snapshot_index,
            uncommitted_bytes: status.uncommitted_bytes,
            replication_inflight_bytes: status.replication_inflight_bytes,
            pending_work: self.has_pending_work(),
            pending_messages: 0,
            pending_message_bytes: 0,
            quarantine_reason: status.runtime_error,
        }
    }

    fn has_pending_work(&self) -> bool {
        self.pending.is_some()
    }

    fn tick_and_drain(
        &mut self,
        ticks: u64,
    ) -> std::result::Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
        if self.pending.is_some() {
            return Err(HostedGroupError::Retryable(
                "replicated tablet operation is still pending".to_string(),
            ));
        }
        self.submit_direct(RaftHostControl::Tick {
            ticks,
            reply: mpsc::channel().0,
        })?;

        // The tablet worker sends its Ready messages through its
        // group-scoped view of the one physical node transport.
        Ok(Vec::new())
    }

    fn step_and_drain(
        &mut self,
        message: RaftMessageEnvelope,
    ) -> std::result::Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
        if self.pending.is_some() {
            return Err(HostedGroupError::Retryable(
                "replicated tablet operation is still pending".to_string(),
            ));
        }
        self.submit_direct(RaftHostControl::Step {
            message,
            reply: mpsc::channel().0,
        })?;

        Ok(Vec::new())
    }

    fn propose_and_drain(
        &mut self,
        command: Vec<u8>,
        encoded_len: usize,
    ) -> std::result::Result<(LogIndex, Vec<RaftMessageEnvelope>), HostedGroupError> {
        if self.pending.is_some() {
            return Err(HostedGroupError::Retryable(
                "replicated tablet operation is still pending".to_string(),
            ));
        }
        let result = self.submit_direct(RaftHostControl::Propose {
            command,
            encoded_len,
            reply: mpsc::channel().0,
        })?;
        let RaftHostControlResult::Proposed(index) = result else {
            return Err(HostedGroupError::Group(
                "tablet proposal control returned a non-proposal result".to_string(),
            ));
        };

        Ok((index, Vec::new()))
    }

    fn tick_and_prepare_budgeted(
        &mut self,
        ticks: u64,
        _budget: MultiRaftTurnBudget,
    ) -> std::result::Result<HostedGroupTurn, HostedGroupError> {
        self.submit_budgeted(|reply| RaftHostControl::Tick { ticks, reply })
    }

    fn step_and_prepare_budgeted(
        &mut self,
        message: RaftMessageEnvelope,
        _budget: MultiRaftTurnBudget,
    ) -> std::result::Result<HostedGroupTurn, HostedGroupError> {
        self.submit_budgeted(|reply| RaftHostControl::Step { message, reply })
    }
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
    identity: RaftReplicaIdentity,
    host_control: SyncSender<RaftHostControl>,
    shutdown: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl ReplicatedTabletRuntime {
    pub(crate) fn hosted_group(&self) -> ReplicatedTabletGroupProxy {
        ReplicatedTabletGroupProxy {
            identity: self.identity,
            control: self.host_control.clone(),
            status: self.handle.status.clone(),
            pending: None,
        }
    }

    /// Resolve the bootstrap authority used to construct the existing M4
    /// tablet as the first group hosted by the M5 MultiRaft node.
    ///
    /// Static seed membership is used only for a genuinely new group. If WAL
    /// state for this group already exists without its durable bootstrap,
    /// startup fails closed rather than silently recreating membership.
    pub(crate) fn resolve_tablet_bootstrap(
        config: &NodeConfig,
        recovered: &RecoveredRaftStorage,
    ) -> Result<RaftGroupBootstrap> {
        let store =
            FileBootstrapStore::open(config.data_dir.join("raft-bootstrap")).map_err(|source| {
                Error::RecoveryFailed {
                    reason: source.to_string(),
                }
            })?;

        if let Some(bootstrap) = load_durable_group_bootstrap(&store, TABLET_RAFT_GROUP_ID)
            .map_err(|source| Error::RecoveryFailed {
                reason: source.to_string(),
            })?
        {
            return Ok(bootstrap);
        }

        let recovered_group_exists = recovered
            .replicas()
            .any(|(identity, _)| identity.raft_group_id == TABLET_RAFT_GROUP_ID);

        if recovered_group_exists {
            return Err(Error::RecoveryFailed {
                reason: format!(
                    "Raft WAL contains group {} state but its \
                     durable bootstrap is missing; refusing \
                     static membership reconstruction",
                    TABLET_RAFT_GROUP_ID.0,
                ),
            });
        }

        if !config.bootstrap {
            return Err(Error::Configuration(format!(
                "Raft group {} has no durable bootstrap and \
                 bootstrap=false",
                TABLET_RAFT_GROUP_ID.0,
            )));
        }

        requested_bootstrap(config)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start_hosted_from_shared_recovery(
        config: &NodeConfig,
        wal: LocalWal,
        database: SharedLocalDatabase,
        bootstrap: RaftGroupBootstrap,
        group_wal: NodeRaftWalHandle<LocalWal>,
        transport: GroupRaftTransport,
        snapshot_store: Arc<FileTabletSnapshotStore>,
        snapshot_work: SnapshotWorkController,
        snapshot_endpoint: GroupSnapshotEndpoint,
        recovered: &RecoveredRaftStorage,
        start_gate: Arc<AtomicBool>,
    ) -> Result<Self> {
        let cluster_id = config.cluster_id.clone().ok_or_else(|| {
            Error::Configuration("replicated tablet runtime requires cluster_id".to_string())
        })?;

        if bootstrap.cluster_id != cluster_id {
            return Err(Error::RecoveryFailed {
                reason: format!(
                    "tablet bootstrap belongs to cluster {}, \
                     configured cluster is {}",
                    bootstrap.cluster_id, cluster_id,
                ),
            });
        }

        let local_replica_id = bootstrap.replica_on_node(config.node_id).ok_or_else(|| {
            Error::Configuration(format!(
                "physical node {} is not assigned a replica \
                     in Raft group {}",
                config.node_id.0, bootstrap.raft_group_id.0,
            ))
        })?;

        let identity = RaftReplicaIdentity::new(bootstrap.raft_group_id, local_replica_id)
            .map_err(|source| Error::Configuration(source.to_string()))?;

        let target = TabletSnapshotInstallTarget {
            cluster_id: cluster_id.clone(),
            raft_group_id: TABLET_RAFT_GROUP_ID,
            tablet_id: TABLET_ID,
            table_id: TABLE_ID,
            tablet_epoch: TABLET_EPOCH,
        };

        let mut bootstrap_store = FileBootstrapStore::open(config.data_dir.join("raft-bootstrap"))
            .map_err(|source| Error::RecoveryFailed {
                reason: source.to_string(),
            })?;

        let durable_bootstrap =
            load_durable_group_bootstrap(&bootstrap_store, TABLET_RAFT_GROUP_ID).map_err(
                |source| Error::RecoveryFailed {
                    reason: source.to_string(),
                },
            )?;

        let (request_tx, request_rx) = mpsc::sync_channel(CHANNEL_CAPACITY);

        let (host_control_tx, host_control_rx) = mpsc::sync_channel(CHANNEL_CAPACITY);

        let status = Arc::new(RwLock::new(ReplicatedTabletStatus::default()));

        let shutdown = Arc::new(AtomicBool::new(false));

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

        let durability_gate = database
            .try_lock()
            .map_err(|_| {
                Error::Configuration("database is busy during tablet startup".to_string())
            })?
            .durability_gate();

        let catalog_cache: Arc<dyn CatalogCacheWriter> = Arc::new(FencedCatalogCache {
            adapter: RagnorDbWalAdapter::new(wal.clone()),
            durability_gate,
        });

        let worker_shutdown = shutdown.clone();

        let worker = if let Some(durable_bootstrap) = durable_bootstrap {
            if durable_bootstrap != bootstrap {
                return Err(Error::RecoveryFailed {
                    reason: format!(
                        "resolved bootstrap for Raft group {} \
                         changed during startup",
                        TABLET_RAFT_GROUP_ID.0,
                    ),
                });
            }

            let replica = recovered
                .replica(identity)
                .ok_or_else(|| Error::RecoveryFailed {
                    reason: format!(
                        "durable bootstrap exists for {:?} \
                             without matching shared-WAL state",
                        identity,
                    ),
                })?;

            install_recovered_catalog(&database, replica)?;

            let recovered_replica = recover_tablet_replica(
                durable_bootstrap,
                local_replica_id,
                group_wal,
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
                host_control_rx,
                request_rx,
                database,
                status,
                worker_shutdown,
                start_gate,
                snapshot_store,
                snapshot_work,
                snapshot_endpoint,
                cluster_id,
                catalog_cache,
                snapshot_policy,
            )
        } else {
            let bootstrapped = bootstrap_tablet_replica(
                &mut bootstrap_store,
                &bootstrap,
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
                host_control_rx,
                request_rx,
                database,
                status,
                worker_shutdown,
                start_gate,
                snapshot_store,
                snapshot_work,
                snapshot_endpoint,
                cluster_id,
                catalog_cache,
                snapshot_policy,
            )
        };

        Ok(Self {
            handle,
            identity,
            host_control: host_control_tx,
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
        TABLET_RAFT_GROUP_ID,
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
    transport: GroupRaftTransport,
    host_control: Receiver<RaftHostControl>,
    requests: Receiver<HostRequest>,
    database: SharedLocalDatabase,
    status: Arc<RwLock<ReplicatedTabletStatus>>,
    shutdown: Arc<AtomicBool>,
    start_gate: Arc<AtomicBool>,
    snapshot_store: Arc<FileTabletSnapshotStore>,
    snapshot_work: SnapshotWorkController,
    snapshot_endpoint: GroupSnapshotEndpoint,
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
                host_control,
                requests,
                database,
                status,
                shutdown,
                start_gate,
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
    transport: GroupRaftTransport,
    host_control: Receiver<RaftHostControl>,
    requests: Receiver<HostRequest>,
    database: SharedLocalDatabase,
    status: Arc<RwLock<ReplicatedTabletStatus>>,
    shutdown: Arc<AtomicBool>,
    start_gate: Arc<AtomicBool>,
    snapshot_store: Arc<FileTabletSnapshotStore>,
    snapshot_work: SnapshotWorkController,
    snapshot_endpoint: GroupSnapshotEndpoint,
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
    let mut expected_snapshot_install: Option<SnapshotMetadata> = None;
    let mut pending_snapshot_install: Option<PendingIncomingSnapshotInstall> = None;
    let mut pending_local_snapshot: Option<PendingLocalSnapshotPublication> = None;

    // Group construction may persist its bootstrap Ready before the physical
    // host has completed recovery registration and sealed retention. Messages
    // must remain private until MultiRaftHost::activate succeeds.
    while !start_gate.load(Ordering::Acquire) {
        if shutdown.load(Ordering::Acquire) {
            return Ok(());
        }

        thread::sleep(Duration::from_millis(1));
    }

    if let Some(ready) = initial_ready {
        send_messages(
            &transport,
            &snapshot_endpoint,
            &latest_snapshot,
            ready.messages,
        );
    }

    while !shutdown.load(Ordering::Acquire) {
        if pending_local_snapshot.is_some() {
            // This snapshot candidate owns a fixed state-machine frontier. Host
            // operations must not mutate the group until its boundary is resolved,
            // but they must receive a Retryable response so this replica cannot stall
            // the physical MultiRaft host.
            reject_snapshot_blocked_host_controls(
                &host_control,
                "local snapshot durability boundary is awaiting retry",
            );
        }
        // A locally generated snapshot whose immutable image has already been
        // published owns a stable state-machine frontier until its A-WAL boundary is
        // resolved.
        //
        // Do not admit Raft messages, ticks, proposals, or SQL work while this
        // candidate is waiting on a retryable persistence result. Advancing applied
        // state before retry would make the retained snapshot stale and could turn
        // benign WAL backpressure into an applied-index regression.
        if pending_local_snapshot.is_some() {
            match maybe_publish_snapshot(
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
                &mut pending_local_snapshot,
                &mut last_snapshot_index,
                catalog_cache.as_ref(),
                &snapshot_policy,
                &mut last_snapshot_at,
            ) {
                Ok(()) => {
                    debug_assert!(
                        pending_local_snapshot.is_none(),
                        "successful local snapshot publication must consume the retained candidate",
                    );
                }

                Err(HostedGroupError::Retryable(error))
                | Err(HostedGroupError::Rejected(error)) => {
                    tracing::debug!(
                        error = %error,
                        "retained local snapshot boundary remains retryable",
                    );

                    thread::sleep(Duration::from_millis(2));
                    continue;
                }

                Err(error) => return Err(error.to_string()),
            }
        }
        // A completed external snapshot owns the outstanding Ready generation.
        // That generation can only be retried with its already-durable snapshot
        // pointer. No tick, step, or proposal may enter generic drain_ready() until
        // this Ready has crossed its exact persistence acknowledgement boundary.
        let post_snapshot_ready_pending = snapshot_phase_blocks_host_control(
            pending_snapshot_install
                .as_ref()
                .map(PendingIncomingSnapshotInstall::phase),
        );

        if post_snapshot_ready_pending {
            reject_snapshot_blocked_host_controls(
                &host_control,
                "post-snapshot Ready persistence is awaiting retry",
            );
        } else {
            let mut serviced_controls = 0;
            while serviced_controls < TABLET_CONTROL_BUDGET {
                let Ok(control) = host_control.try_recv() else {
                    break;
                };
                serviced_controls += 1;

                match control {
                    RaftHostControl::Tick { ticks, reply } => {
                        let result: std::result::Result<(), HostedGroupError> = (|| {
                            let had_pending_ready = ready_loop.has_pending_work();
                            if let Some(metadata) = drain_ready(
                                &mut ready_loop,
                                &mut tablet,
                                &mut registry,
                                &database,
                                &transport,
                                &snapshot_endpoint,
                                &latest_snapshot,
                                catalog_cache.as_ref(),
                                &snapshot_policy,
                            )? {
                                expected_snapshot_install = Some(metadata);
                                pending_snapshot_install = None;
                            }
                            if had_pending_ready {
                                return Ok(());
                            }
                            ready_loop.tick(ticks).map_err(classify_ready_error)?;
                            if let Some(metadata) = drain_ready(
                                &mut ready_loop,
                                &mut tablet,
                                &mut registry,
                                &database,
                                &transport,
                                &snapshot_endpoint,
                                &latest_snapshot,
                                catalog_cache.as_ref(),
                                &snapshot_policy,
                            )? {
                                expected_snapshot_install = Some(metadata);
                                pending_snapshot_install = None;
                            }
                            Ok(())
                        })(
                        );

                        let fatal_reason = fatal_host_control_reason(&result);
                        let _ = reply.send(result.map(|()| RaftHostControlResult::Completed));
                        if let Some(reason) = fatal_reason {
                            return Err(reason);
                        }
                    }

                    RaftHostControl::Step { message, reply } => {
                        let result: std::result::Result<(), HostedGroupError> = (|| {
                            let had_pending_ready = ready_loop.has_pending_work();
                            if let Some(metadata) = drain_ready(
                                &mut ready_loop,
                                &mut tablet,
                                &mut registry,
                                &database,
                                &transport,
                                &snapshot_endpoint,
                                &latest_snapshot,
                                catalog_cache.as_ref(),
                                &snapshot_policy,
                            )? {
                                expected_snapshot_install = Some(metadata);
                                pending_snapshot_install = None;
                            }
                            if had_pending_ready {
                                return Err(HostedGroupError::Retryable(
                                    "a previous Ready generation is still being resumed"
                                        .to_string(),
                                ));
                            }
                            ready_loop.step(message).map_err(classify_ready_error)?;
                            if let Some(metadata) = drain_ready(
                                &mut ready_loop,
                                &mut tablet,
                                &mut registry,
                                &database,
                                &transport,
                                &snapshot_endpoint,
                                &latest_snapshot,
                                catalog_cache.as_ref(),
                                &snapshot_policy,
                            )? {
                                expected_snapshot_install = Some(metadata);
                                pending_snapshot_install = None;
                            }
                            Ok(())
                        })(
                        );

                        let fatal_reason = fatal_host_control_reason(&result);
                        let _ = reply.send(result.map(|()| RaftHostControlResult::Completed));
                        if let Some(reason) = fatal_reason {
                            return Err(reason);
                        }
                    }

                    RaftHostControl::Propose {
                        command,
                        encoded_len,
                        reply,
                    } => {
                        let result: std::result::Result<LogIndex, HostedGroupError> = (|| {
                            let had_pending_ready = ready_loop.has_pending_work();
                            if let Some(metadata) = drain_ready(
                                &mut ready_loop,
                                &mut tablet,
                                &mut registry,
                                &database,
                                &transport,
                                &snapshot_endpoint,
                                &latest_snapshot,
                                catalog_cache.as_ref(),
                                &snapshot_policy,
                            )? {
                                expected_snapshot_install = Some(metadata);
                                pending_snapshot_install = None;
                            }
                            if had_pending_ready {
                                return Err(HostedGroupError::Retryable(
                                    "a previous Ready generation is still being resumed"
                                        .to_string(),
                                ));
                            }
                            let index = ready_loop
                                .propose(command, encoded_len)
                                .map_err(classify_ready_error)?;
                            if let Some(metadata) = drain_ready(
                                &mut ready_loop,
                                &mut tablet,
                                &mut registry,
                                &database,
                                &transport,
                                &snapshot_endpoint,
                                &latest_snapshot,
                                catalog_cache.as_ref(),
                                &snapshot_policy,
                            )? {
                                expected_snapshot_install = Some(metadata);
                                pending_snapshot_install = None;
                            }
                            Ok(index)
                        })(
                        );

                        let fatal_reason = fatal_host_control_reason(&result);
                        let _ = reply.send(result.map(RaftHostControlResult::Proposed));
                        if let Some(reason) = fatal_reason {
                            return Err(reason);
                        }
                    }
                }
            }
        }

        // Snapshot inbound correlation and state machine (retryable, exact metadata)
        {
            let mut inbound_valid: Option<ReceivedTabletSnapshot> = None;
            while let Ok(received) = snapshot_endpoint.inbound.try_recv() {
                let Some(expected) = expected_snapshot_install.as_ref() else {
                    tracing::debug!(
                        snapshot_id = received.metadata.snapshot_id,
                        "discarding snapshot bytes without an accepted Raft install"
                    );
                    continue;
                };
                let actual = match raft_metadata_for_tablet(&received.metadata) {
                    Ok(m) => m,
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            "discarding invalid incoming snapshot metadata"
                        );
                        continue;
                    }
                };
                if &actual != expected {
                    tracing::warn!(
                        expected_snapshot_id = expected.snapshot_id,
                        received_snapshot_id = actual.snapshot_id,
                        "discarding snapshot bytes that do not match the accepted Raft install"
                    );
                    continue;
                }
                if pending_snapshot_install.is_some() || inbound_valid.is_some() {
                    tracing::debug!(
                        "discarding extra snapshot bytes while pending install in progress"
                    );
                    continue;
                }
                inbound_valid = Some(received);
            }
            if let Some(received) = inbound_valid {
                let expected = expected_snapshot_install.clone().expect("validated");
                pending_snapshot_install =
                    Some(PendingIncomingSnapshotInstall::Received { expected, received });
            }
        }

        // Drive pending snapshot install: Received -> BoundaryPending -> ReadyPending -> publish
        if let Some(PendingIncomingSnapshotInstall::Received { expected, received }) =
            pending_snapshot_install.take()
        {
            let install_permit = match snapshot_work.acquire(SnapshotWorkKind::Install) {
                Ok(p) => p,
                Err(SnapshotWorkError::LimitReached { .. }) => {
                    tracing::debug!("snapshot install backpressure: LimitReached, will retry");
                    pending_snapshot_install =
                        Some(PendingIncomingSnapshotInstall::Received { expected, received });
                    thread::sleep(Duration::from_millis(2));
                    continue;
                }
                Err(error) => return Err(error.to_string()),
            };
            let target = TabletSnapshotInstallTarget {
                cluster_id: cluster_id.clone(),
                raft_group_id: TABLET_RAFT_GROUP_ID,
                tablet_id: TABLET_ID,
                table_id: TABLE_ID,
                tablet_epoch: TABLET_EPOCH,
            };
            match prepare_incoming_tablet_snapshot(
                snapshot_store.as_ref(),
                received.session,
                &target,
                install_permit,
            ) {
                Ok(prepared) => {
                    pending_snapshot_install =
                        Some(PendingIncomingSnapshotInstall::BoundaryPending {
                            expected,
                            prepared,
                        });
                }
                Err(error) => {
                    let is_remote = matches!(
                        &error,
                        TabletSnapshotIntegrationError::Install(install_err) if matches!(
                            install_err,
                            ragnordb_tablet::snapshot::TabletSnapshotInstallError::TargetClusterMismatch { .. }
                                | ragnordb_tablet::snapshot::TabletSnapshotInstallError::TargetGroupMismatch { .. }
                                | ragnordb_tablet::snapshot::TabletSnapshotInstallError::TargetTabletMismatch { .. }
                                | ragnordb_tablet::snapshot::TabletSnapshotInstallError::TargetEpochMismatch { .. }
                                | ragnordb_tablet::snapshot::TabletSnapshotInstallError::PayloadDecode(_)
                                | ragnordb_tablet::snapshot::TabletSnapshotInstallError::UnsupportedPayloadVersion(_)
                                | ragnordb_tablet::snapshot::TabletSnapshotInstallError::MissingPayloadTableId
                                | ragnordb_tablet::snapshot::TabletSnapshotInstallError::TableMismatch { .. }
                                | ragnordb_tablet::snapshot::TabletSnapshotInstallError::StateMachineIdentityMismatch
                                | ragnordb_tablet::snapshot::TabletSnapshotInstallError::StateMachineDecode(_)
                                | ragnordb_tablet::snapshot::TabletSnapshotInstallError::MvccRestore(_)
                                | ragnordb_tablet::snapshot::TabletSnapshotInstallError::TabletRestore(_)
                                | ragnordb_tablet::snapshot::TabletSnapshotInstallError::StateMachineRestore(_)
                                | ragnordb_tablet::snapshot::TabletSnapshotInstallError::InvalidTarget(_)
                                | ragnordb_tablet::snapshot::TabletSnapshotInstallError::Receive(_)
                        )
                    );
                    if is_remote {
                        tracing::warn!(error = %error, "rejecting malformed remote snapshot without quarantine");
                    } else {
                        let classified = classify_snapshot_integration_error(error);
                        match classified {
                            HostedGroupError::Retryable(reason) => {
                                tracing::debug!(error = %reason, "snapshot prepare retryable, awaiting retransmission");
                            }
                            HostedGroupError::RecoveryRequired => {
                                return Err(classified.to_string());
                            }
                            HostedGroupError::Group(reason) => return Err(reason),
                            HostedGroupError::Rejected(reason) => {
                                tracing::warn!(reason = %reason, "snapshot prepare rejected");
                            }
                        }
                    }
                }
            }
        }

        if let Some(PendingIncomingSnapshotInstall::BoundaryPending { expected, prepared }) =
            pending_snapshot_install.take()
        {
            // HardState belongs to the current Raft state, not to the lifetime of the
            // transferred image. A retry may occur after a higher term or vote has
            // already been observed, so recompute it immediately before WAL admission.
            let hard_state =
                snapshot_boundary_hard_state(ready_loop.raft().hard_state(), prepared.frontier());
            match persist_tablet_snapshot_boundary_via_ready_loop(
                &mut ready_loop,
                prepared.pointer(),
                prepared.frontier(),
                hard_state,
            ) {
                Ok(_) => {
                    let image = match snapshot_store.load_verified(prepared.pointer()) {
                        Ok(img) => img,
                        Err(error) => {
                            return Err(HostedGroupError::Group(error.to_string()).to_string());
                        }
                    };
                    let core_snapshot = match TabletSnapshotTransfer::from_image(image.clone()) {
                        Ok(t) => t.into_core_snapshot(),
                        Err(error) => {
                            return Err(HostedGroupError::Group(error.to_string()).to_string());
                        }
                    };
                    match ready_loop.complete_snapshot_install(core_snapshot) {
                        Ok(()) => {
                            let identity = ready_loop.persistence().log_view().identity();
                            let raft_pointer =
                                match raft_pointer_for_tablet(identity, prepared.pointer()) {
                                    Ok(p) => p,
                                    Err(error) => {
                                        return Err(
                                            HostedGroupError::Group(error.to_string()).to_string()
                                        );
                                    }
                                };
                            pending_snapshot_install =
                                Some(PendingIncomingSnapshotInstall::ReadyPending {
                                    expected,
                                    prepared,
                                    image,
                                    raft_pointer,
                                });
                        }
                        Err(error) => {
                            let classified = classify_ready_error(error);
                            match classified {
                                HostedGroupError::RecoveryRequired => {
                                    return Err(classified.to_string());
                                }
                                HostedGroupError::Retryable(reason) => {
                                    tracing::debug!(error = %reason, "complete_snapshot_install retryable");
                                    pending_snapshot_install =
                                        Some(PendingIncomingSnapshotInstall::BoundaryPending {
                                            expected,
                                            prepared,
                                        });
                                    thread::sleep(Duration::from_millis(2));
                                    continue;
                                }
                                HostedGroupError::Group(reason) => return Err(reason),
                                HostedGroupError::Rejected(reason) => {
                                    tracing::warn!(reason = %reason, "complete_snapshot_install rejected");
                                    return Err(reason);
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    let classified = classify_snapshot_integration_error(error);
                    match classified {
                        HostedGroupError::RecoveryRequired => return Err(classified.to_string()),
                        HostedGroupError::Retryable(reason) => {
                            tracing::debug!(error = %reason, "snapshot boundary persist retryable");
                            pending_snapshot_install =
                                Some(PendingIncomingSnapshotInstall::BoundaryPending {
                                    expected,
                                    prepared,
                                });
                            thread::sleep(Duration::from_millis(2));
                            continue;
                        }
                        HostedGroupError::Group(reason) => return Err(reason),
                        HostedGroupError::Rejected(reason) => {
                            tracing::warn!(reason = %reason, "snapshot boundary rejected, discarding pending");
                        }
                    }
                }
            }
        }

        if let Some(PendingIncomingSnapshotInstall::ReadyPending {
            expected,
            prepared,
            image,
            raft_pointer,
        }) = pending_snapshot_install.take()
        {
            match ready_loop.persist_ready_after_snapshot_boundary(&raft_pointer) {
                Ok(Some(ready)) => {
                    let installed = prepared.into_installed();
                    // Finalize: install tablet, apply suffix, advance frontier, publish
                    tablet = TabletCommandApplier::new(installed.state_machine);
                    database
                        .blocking_lock()
                        .install_replicated_storage(
                            TABLE_ID,
                            tablet.state_machine().tablet().storage().clone(),
                        )
                        .map_err(|e| e.to_string())?;
                    let mut frontier = AppliedRaftFrontier::new(
                        image.metadata.last_included_index,
                        image.metadata.last_included_term,
                    );
                    for entry in &ready.committed_entries {
                        frontier = AppliedRaftFrontier::new(entry.index, entry.term);
                        let EntryPayload::Normal(bytes) = &entry.payload else {
                            continue;
                        };
                        let envelope =
                            TabletCommandEnvelope::decode(bytes).map_err(|e| e.to_string())?;
                        let locally_proposed = registry.is_pending(&envelope.request_id);
                        let disposition = tablet
                            .apply_committed(
                                ragnordb_multiraft::proposal::ProposalPosition {
                                    term: entry.term,
                                    index: entry.index,
                                },
                                bytes,
                            )
                            .map_err(|e| e.to_string())?;
                        snapshot_policy.note_applied(bytes.len());
                        publish_committed_command(
                            &envelope,
                            locally_proposed,
                            disposition,
                            &mut registry,
                            &database,
                            catalog_cache.as_ref(),
                        )
                        .map_err(|e| e.to_string())?;
                    }
                    ready_loop
                        .advance_applied_frontier(frontier)
                        .map_err(|e| e.to_string())?;
                    latest_snapshot = Some(image.clone());
                    snapshot_policy.reset();
                    send_messages(
                        &transport,
                        &snapshot_endpoint,
                        &latest_snapshot,
                        ready.messages,
                    );
                    release_replica_retention(&mut ready_loop).map_err(|e| e.to_string())?;
                    snapshot_store
                        .prune_older_snapshots(&installed.pointer)
                        .map_err(|e| e.to_string())?;
                    expected_snapshot_install = None;
                    internal_barrier_allocator.clear();
                    last_snapshot_index = latest_snapshot
                        .as_ref()
                        .map(|img| img.metadata.last_included_index)
                        .unwrap_or(last_snapshot_index);
                }
                Ok(None) => {
                    return Err(
                        "completed snapshot install produced no Ready generation".to_string()
                    );
                }
                Err(error) => {
                    let classified = classify_ready_error(error);
                    match classified {
                        HostedGroupError::RecoveryRequired => return Err(classified.to_string()),
                        HostedGroupError::Retryable(reason) => {
                            tracing::debug!(error = %reason, "post-snapshot Ready persist retryable");
                            pending_snapshot_install =
                                Some(PendingIncomingSnapshotInstall::ReadyPending {
                                    expected,
                                    prepared,
                                    image,
                                    raft_pointer,
                                });
                            thread::sleep(Duration::from_millis(2));
                            continue;
                        }
                        other => return Err(other.to_string()),
                    }
                }
            }
        }

        // If a ReadyPending is still pending (retryable), service it before generic drains
        if pending_snapshot_install
            .as_ref()
            .is_some_and(|p| matches!(p, PendingIncomingSnapshotInstall::ReadyPending { .. }))
        {
            thread::sleep(Duration::from_millis(2));
            continue;
        }

        match refresh_leader_activation(
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
        ) {
            Ok(()) => {}
            Err(HostedGroupError::Retryable(error)) | Err(HostedGroupError::Rejected(error)) => {
                tracing::debug!(
                    error = %error,
                    "leader activation refresh is retryable; will retry on next turn"
                );
            }
            Err(error) => return Err(error.to_string()),
        }
        let serving_leader = leader_activation.is_some_and(|activation| {
            ready_loop
                .applied_frontier()
                .is_some_and(|frontier| frontier.index >= activation.index)
        });

        // P1: drain pending Ready before next SQL admission
        match drain_ready(
            &mut ready_loop,
            &mut tablet,
            &mut registry,
            &database,
            &transport,
            &snapshot_endpoint,
            &latest_snapshot,
            catalog_cache.as_ref(),
            &snapshot_policy,
        ) {
            Ok(Some(metadata)) => {
                expected_snapshot_install = Some(metadata);
                pending_snapshot_install = None;
                thread::sleep(Duration::from_millis(2));
                continue;
            }
            Ok(None) => {}
            Err(HostedGroupError::Retryable(error)) | Err(HostedGroupError::Rejected(error)) => {
                tracing::debug!(
                    error = %error,
                    "pending Ready remains blocked before client admission"
                );
                thread::sleep(Duration::from_millis(2));
                continue;
            }
            Err(error) => return Err(error.to_string()),
        }

        let mut admitted_requests = 0;
        while admitted_requests < TABLET_REQUEST_BUDGET {
            let Ok(request) = requests.try_recv() else {
                break;
            };
            admitted_requests += 1;

            admit_request(
                request,
                &mut ready_loop,
                &tablet,
                &mut registry,
                &mut clients,
                serving_leader,
                &mut internal_barrier_allocator,
            );
            match drain_ready(
                &mut ready_loop,
                &mut tablet,
                &mut registry,
                &database,
                &transport,
                &snapshot_endpoint,
                &latest_snapshot,
                catalog_cache.as_ref(),
                &snapshot_policy,
            ) {
                Ok(_) => {}
                Err(HostedGroupError::Retryable(error))
                | Err(HostedGroupError::Rejected(error)) => {
                    tracing::debug!(
                        error = %error,
                        "drain after client admit is retryable; will retry pending Ready"
                    );
                    break;
                }
                Err(error) => return Err(error.to_string()),
            }
        }

        let now = Instant::now();
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
            match maybe_publish_snapshot(
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
                &mut pending_local_snapshot,
                &mut last_snapshot_index,
                catalog_cache.as_ref(),
                &snapshot_policy,
                &mut last_snapshot_at,
            ) {
                Ok(()) => {}
                Err(HostedGroupError::Retryable(error))
                | Err(HostedGroupError::Rejected(error)) => {
                    tracing::debug!(
                        error = %error,
                        "snapshot publication drain is retryable; will retry"
                    );
                }
                Err(error) => return Err(error.to_string()),
            }
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
            raft_group_id: TABLET_RAFT_GROUP_ID,
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
    transport: &GroupRaftTransport,
    snapshot_endpoint: &GroupSnapshotEndpoint,
    latest_snapshot: &Option<TabletSnapshotImage>,
    catalog_cache: &dyn CatalogCacheWriter,
    snapshot_policy: &SnapshotPolicy,
    activation: &mut Option<ragnordb_multiraft::proposal::ProposalPosition>,
    internal_barrier_allocator: &mut InternalBarrierAllocator,
) -> std::result::Result<(), HostedGroupError>
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

    let _ = drain_ready(
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

    let request_id = internal_barrier_allocator
        .candidate(term, tablet)
        .map_err(|error| HostedGroupError::Group(error.to_string()))?;
    let envelope = TabletCommandEnvelope::new(
        request_id.clone(),
        TABLET_ID,
        TABLET_EPOCH,
        TabletCommand::Noop(NoopCommand),
    )
    .map_err(|error| HostedGroupError::Group(error.to_string()))?;
    let bytes = envelope
        .encode()
        .map_err(|error| HostedGroupError::Group(error.to_string()))?;
    let index = ready_loop
        .propose(bytes.clone(), bytes.len())
        .map_err(classify_ready_error)?;
    internal_barrier_allocator.record_admission(request_id.sequence);
    *activation = Some(ragnordb_multiraft::proposal::ProposalPosition { term, index });
    let _ = drain_ready(
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
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn maybe_publish_snapshot<W, LS, SS>(
    ready_loop: &mut RaftReadyLoop<W, LS, SS>,
    tablet: &mut TabletCommandApplier,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    database: &SharedLocalDatabase,
    transport: &GroupRaftTransport,
    snapshot_endpoint: &GroupSnapshotEndpoint,
    store: &FileTabletSnapshotStore,
    work: &SnapshotWorkController,
    cluster_id: &str,
    latest_snapshot: &mut Option<TabletSnapshotImage>,
    pending_local_snapshot: &mut Option<PendingLocalSnapshotPublication>,
    last_snapshot_index: &mut u64,
    catalog_cache: &dyn CatalogCacheWriter,
    snapshot_policy: &SnapshotPolicy,
    last_snapshot_at: &mut Instant,
) -> std::result::Result<(), HostedGroupError>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    if pending_local_snapshot.is_none() {
        let Some(frontier) = ready_loop.applied_frontier() else {
            return Ok(());
        };

        if !snapshot_policy.is_due(frontier.index, *last_snapshot_index, *last_snapshot_at) {
            return Ok(());
        }

        let _ = drain_ready(
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

        // `drain_ready` may have advanced application state, so snapshot the
        // exact frontier observed after the drain rather than the pre-drain
        // candidate.
        let frontier = ready_loop.applied_frontier().ok_or_else(|| {
            HostedGroupError::Group(
                "applied frontier disappeared during snapshot generation".to_string(),
            )
        })?;
        let frontier = AppliedTabletFrontier::new(frontier.index, frontier.term);

        let local_replica_id = ReplicaId(ready_loop.raft().id().get());
        let conf_state = tablet_snapshot_conf_state(ready_loop.raft().conf_state())
            .map_err(|error| HostedGroupError::Group(error.to_string()))?;
        let snapshot_id = store
            .allocate_snapshot_id(TABLET_RAFT_GROUP_ID, local_replica_id, TABLET_ID)
            .map_err(|error| HostedGroupError::Group(error.to_string()))?;
        let image = generate_tablet_snapshot_from_ready_loop(
            work,
            ready_loop,
            tablet.state_machine(),
            cluster_id,
            local_replica_id,
            snapshot_id,
            conf_state,
        )
        .map_err(classify_snapshot_integration_error)?;
        let pointer = store
            .publish(&image)
            .map_err(|error| HostedGroupError::Group(error.to_string()))?;
        let identity = ready_loop.persistence().log_view().identity();
        let raft_pointer = raft_pointer_for_tablet(identity, &pointer)
            .map_err(|error| HostedGroupError::Group(error.to_string()))?;

        prepare_local_snapshot_once(pending_local_snapshot, || {
            Ok::<_, HostedGroupError>(PendingLocalSnapshotPublication {
                frontier,
                image,
                pointer,
                raft_pointer,
            })
        })?;
    }

    {
        let pending = pending_local_snapshot
            .as_ref()
            .expect("local snapshot candidate was prepared above");

        // Like incoming snapshot retry, HardState must be sampled at each WAL
        // attempt because the group may have observed a later term since the
        // image was generated.
        let hard_state =
            snapshot_boundary_hard_state(ready_loop.raft().hard_state(), pending.frontier);

        persist_tablet_snapshot_boundary_via_ready_loop(
            ready_loop,
            &pending.pointer,
            pending.frontier,
            hard_state,
        )
        .map_err(classify_snapshot_integration_error)?;
    }

    let pending = pending_local_snapshot
        .take()
        .expect("persisted local snapshot candidate must exist");

    let core_snapshot = TabletSnapshotTransfer::from_image(pending.image.clone())
        .map_err(|error| HostedGroupError::Group(error.to_string()))?
        .into_core_snapshot();
    ready_loop
        .restore_persisted_snapshot(&pending.raft_pointer, core_snapshot)
        .map_err(classify_ready_error)?;

    *last_snapshot_index = pending.frontier.index;
    *last_snapshot_at = Instant::now();
    snapshot_policy.reset();
    *latest_snapshot = Some(pending.image);
    let _ = drain_ready(
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
    release_replica_retention(ready_loop)
        .map_err(|error| HostedGroupError::Group(error.to_string()))?;
    store
        .prune_older_snapshots(&pending.pointer)
        .map_err(|error| HostedGroupError::Group(error.to_string()))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[allow(dead_code)]
fn install_received_snapshot<W, LS, SS>(
    received: ReceivedTabletSnapshot,
    ready_loop: &mut RaftReadyLoop<W, LS, SS>,
    tablet: &mut TabletCommandApplier,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    database: &SharedLocalDatabase,
    transport: &GroupRaftTransport,
    snapshot_endpoint: &GroupSnapshotEndpoint,
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
        raft_group_id: TABLET_RAFT_GROUP_ID,
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
            raft_group_id: TABLET_RAFT_GROUP_ID,
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
            raft_group_id: TABLET_RAFT_GROUP_ID,
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
    transport: &GroupRaftTransport,
    snapshot_endpoint: &GroupSnapshotEndpoint,
    latest_snapshot: &Option<TabletSnapshotImage>,
    catalog_cache: &dyn CatalogCacheWriter,
    snapshot_policy: &SnapshotPolicy,
) -> std::result::Result<Option<SnapshotMetadata>, HostedGroupError>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    let Some(ready) = ready_loop
        .persist_next_ready(None)
        .map_err(classify_ready_error)?
    else {
        return Ok(None);
    };

    let mut frontier = None;
    for entry in &ready.committed_entries {
        frontier = Some(AppliedRaftFrontier::new(entry.index, entry.term));
        let EntryPayload::Normal(bytes) = &entry.payload else {
            continue;
        };
        let envelope = TabletCommandEnvelope::decode(bytes)
            .map_err(|error| HostedGroupError::Group(error.to_string()))?;
        let locally_proposed = registry.is_pending(&envelope.request_id);
        let disposition = tablet
            .apply_committed(
                ragnordb_multiraft::proposal::ProposalPosition {
                    term: entry.term,
                    index: entry.index,
                },
                bytes,
            )
            .map_err(|error| HostedGroupError::Group(error.to_string()))?;
        snapshot_policy.note_applied(bytes.len());
        publish_committed_command(
            &envelope,
            locally_proposed,
            disposition,
            registry,
            database,
            catalog_cache,
        )
        .map_err(HostedGroupError::Group)?;
    }
    if let Some(frontier) = frontier {
        ready_loop
            .advance_applied_frontier(frontier)
            .map_err(|error| HostedGroupError::Group(error.to_string()))?;
    }
    let snapshot_install = ready.snapshot_install.clone();
    send_messages(
        transport,
        snapshot_endpoint,
        latest_snapshot,
        ready.messages,
    );
    Ok(snapshot_install)
}

fn send_messages(
    transport: &GroupRaftTransport,
    snapshot_endpoint: &GroupSnapshotEndpoint,
    latest_snapshot: &Option<TabletSnapshotImage>,
    messages: Vec<Envelope<Vec<u8>, Vec<u8>>>,
) {
    for message in messages {
        let target_replica = ReplicaId::from_raft(message.to);
        let carries_snapshot = matches!(message.msg, Message::InstallSnapshot(_));
        let target_node = transport.target_node_for_replica(target_replica);

        if let Err(source) = transport.try_send(message) {
            warn!(
                node_id = transport.local_node_id().0,
                group_id = transport.raft_group_id().0,
                replica_id = target_replica.0,
                error = %source,
                "Raft message could not be delivered; Raft will retry",
            );

            continue;
        }

        if carries_snapshot && let Some(image) = latest_snapshot.clone() {
            match target_node {
                Ok(node_id) => {
                    if let Err(source) = snapshot_endpoint.send(node_id, target_replica, image) {
                        warn!(
                            group_id = transport.raft_group_id().0,
                            node_id = node_id.0,
                            replica_id = target_replica.0,
                            error = %source,
                            "tablet snapshot could not be scheduled for transfer",
                        );
                    }
                }

                Err(source) => {
                    warn!(
                        group_id = transport.raft_group_id().0,
                        replica_id = target_replica.0,
                        error = %source,
                        "snapshot peer node could not be resolved",
                    );
                }
            }
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
    published.role = Some((*ready_loop.raft().role()).into());
    published.term = ready_loop.raft().hard_state().current_term;
    published.commit_index = ready_loop.raft().hard_state().commit;
    published.last_log_index = ready_loop.raft().last_log_index();
    published.serving_leader = serving_leader;
    published.snapshot_index = snapshot_index;
    published.applied_index = ready_loop
        .applied_frontier()
        .map(|frontier| frontier.index)
        .unwrap_or(0);
    published.uncommitted_bytes = ready_loop.raft().uncommitted_bytes();
    published.replication_inflight_bytes = ready_loop
        .raft()
        .conf_state()
        .replication_targets()
        .into_iter()
        .filter_map(|replica_id| ready_loop.raft().progress(replica_id))
        .map(|progress| progress.inflight_bytes)
        .sum();
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

    fn snapshot_state_blocks_normal_work(
        incoming_phase: Option<IncomingSnapshotPhase>,
        local_snapshot_pending: bool,
    ) -> bool {
        local_snapshot_pending || incoming_phase == Some(IncomingSnapshotPhase::ReadyPending)
    }

    /// Realistic bug caught: a queued tick could enter the generic Ready path
    /// while the post-snapshot Ready generation was still awaiting durable
    /// persistence, violating the single-outstanding-Ready ownership rule.
    #[test]
    fn post_snapshot_ready_retry_blocks_host_control() {
        assert!(!snapshot_phase_blocks_host_control(Some(
            IncomingSnapshotPhase::Received
        )));
        assert!(!snapshot_phase_blocks_host_control(Some(
            IncomingSnapshotPhase::BoundaryPending
        )));
        assert!(snapshot_phase_blocks_host_control(Some(
            IncomingSnapshotPhase::ReadyPending
        )));

        let (control_tx, control_rx) = mpsc::sync_channel(1);
        let (reply_tx, reply_rx) = mpsc::channel();
        control_tx
            .send(RaftHostControl::Tick {
                ticks: 1,
                reply: reply_tx,
            })
            .expect("the host-control queue must accept the tick");

        let phase = Some(IncomingSnapshotPhase::ReadyPending);
        if !snapshot_phase_blocks_host_control(phase) {
            panic!("host control must remain blocked while post-snapshot Ready is pending");
        }
        assert!(matches!(
            reply_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        // Model successful persistence of the retained post-snapshot Ready.
        let phase = None;
        if !snapshot_phase_blocks_host_control(phase) {
            match control_rx
                .try_recv()
                .expect("the queued tick must be admitted after persistence")
            {
                RaftHostControl::Tick { ticks, reply } => {
                    assert_eq!(ticks, 1);
                    reply
                        .send(Ok(RaftHostControlResult::Completed))
                        .expect("the tick reply must be delivered");
                }
                RaftHostControl::Step { .. } | RaftHostControl::Propose { .. } => {
                    panic!("the test queued a tick")
                }
            }
        }
        assert!(
            reply_rx
                .recv()
                .expect("the admitted tick must complete")
                .is_ok()
        );
    }

    /// Realistic bug caught: retaining the HardState from the first snapshot
    /// boundary attempt could overwrite a later term or vote on retry.
    #[test]
    fn boundary_retry_uses_latest_hard_state() {
        let frontier = AppliedTabletFrontier::new(12, 4);
        let first_attempt = snapshot_boundary_hard_state(
            HardState {
                current_term: 4,
                voted_for: None,
                commit: 9,
            },
            frontier,
        );
        assert_eq!(first_attempt.current_term, 4);
        assert_eq!(first_attempt.commit, 12);

        let retry = snapshot_boundary_hard_state(
            HardState {
                current_term: 5,
                voted_for: None,
                commit: 14,
            },
            frontier,
        );
        assert_eq!(retry.current_term, 5);
        assert_eq!(retry.commit, 14);
    }

    /// Realistic bug caught: retryable A-WAL admission could regenerate and
    /// republish a local snapshot, consuming a new immutable snapshot ID on
    /// every retry instead of retaining the original candidate.
    #[test]
    fn local_snapshot_retry_reuses_same_snapshot_id() {
        #[derive(Debug)]
        struct Candidate {
            snapshot_id: u64,
        }

        let mut next_snapshot_id = 7;
        let mut pending = None;
        prepare_local_snapshot_once(&mut pending, || {
            let snapshot_id = next_snapshot_id;
            next_snapshot_id += 1;
            Ok::<_, HostedGroupError>(Candidate { snapshot_id })
        })
        .expect("the initial snapshot candidate must be prepared");

        let first_wal_attempt =
            ragnordb_multiraft::storage::persistence::RaftPersistenceError::NotStaged {
                recovery_required: false,
                reason: "injected retryable boundary admission".to_string(),
            };
        assert!(matches!(
            first_wal_attempt,
            ragnordb_multiraft::storage::persistence::RaftPersistenceError::NotStaged {
                recovery_required: false,
                ..
            }
        ));

        prepare_local_snapshot_once(
            &mut pending,
            || -> std::result::Result<Candidate, HostedGroupError> {
                panic!("a retry must reuse the retained snapshot candidate")
            },
        )
        .expect("retry must retain the original candidate");

        let persisted = pending
            .take()
            .expect("the successful retry must publish the retained candidate");
        assert_eq!(persisted.snapshot_id, 7);
        assert_eq!(next_snapshot_id, 8);
    }

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

    #[test]
    fn rejected_and_retryable_host_operations_do_not_require_worker_shutdown() {
        let rejected: std::result::Result<(), HostedGroupError> =
            Err(HostedGroupError::Rejected("not leader".to_string()));

        let retryable: std::result::Result<(), HostedGroupError> = Err(
            HostedGroupError::Retryable("persistence temporarily unavailable".to_string()),
        );

        assert_eq!(
            fatal_host_control_reason(&rejected),
            None,
            "ordinary rejection must not terminate the Ready owner",
        );

        assert_eq!(
            fatal_host_control_reason(&retryable),
            None,
            "retryable failure must not terminate the Ready owner",
        );
    }

    #[test]
    fn correctness_failures_require_worker_shutdown() {
        let group_failure: std::result::Result<(), HostedGroupError> = Err(
            HostedGroupError::Group("state-machine apply failed".to_string()),
        );

        let recovery_required: std::result::Result<(), HostedGroupError> =
            Err(HostedGroupError::RecoveryRequired);

        assert!(
            fatal_host_control_reason(&group_failure).is_some(),
            "group-local correctness failure must terminate this Ready owner",
        );

        assert!(
            fatal_host_control_reason(&recovery_required).is_some(),
            "shared-WAL uncertainty must terminate this Ready owner",
        );
    }

    #[test]
    fn retryable_persistence_is_classified_as_retryable() {
        let error = ragnordb_multiraft::runtime::ReadyLoopError::RetryablePersistence(
            ragnordb_multiraft::storage::persistence::RaftPersistenceError::NotStaged {
                recovery_required: false,
                reason: "injected retryable".to_string(),
            },
        );
        assert!(
            matches!(classify_ready_error(error), HostedGroupError::Retryable(_)),
            "RetryablePersistence with recovery_required=false must be Retryable",
        );
    }

    #[test]
    fn pending_ready_is_classified_as_retryable() {
        let error = ragnordb_multiraft::runtime::ReadyLoopError::PendingReady;
        assert!(
            matches!(classify_ready_error(error), HostedGroupError::Retryable(_)),
            "PendingReady must be Retryable to allow pending Ready retry",
        );
    }

    #[test]
    fn pending_local_snapshot_blocks_raft_and_sql_progress() {
        assert!(snapshot_state_blocks_normal_work(None, true,));

        assert!(snapshot_state_blocks_normal_work(
            Some(IncomingSnapshotPhase::ReadyPending),
            false,
        ));

        assert!(!snapshot_state_blocks_normal_work(
            Some(IncomingSnapshotPhase::BoundaryPending),
            false,
        ));
    }

    #[test]
    fn snapshot_retry_returns_retryable_to_host_control() {
        let (control_tx, control_rx) = mpsc::sync_channel(3);

        let (tick_reply_tx, tick_reply_rx) = mpsc::channel();

        let (step_reply_tx, step_reply_rx) = mpsc::channel();

        let (propose_reply_tx, propose_reply_rx) = mpsc::channel();

        control_tx
            .send(RaftHostControl::Tick {
                ticks: 1,
                reply: tick_reply_tx,
            })
            .unwrap();

        let message = RaftMessageEnvelope {
            from: raft::types::ReplicaId::must(2),
            to: raft::types::ReplicaId::must(1),
            msg: Message::AppendEntries(raft::message::AppendEntriesRequest {
                term: 1,
                leader_id: raft::types::ReplicaId::must(2),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            }),
        };

        control_tx
            .send(RaftHostControl::Step {
                message,
                reply: step_reply_tx,
            })
            .unwrap();

        control_tx
            .send(RaftHostControl::Propose {
                command: vec![1, 2, 3],
                encoded_len: 3,
                reply: propose_reply_tx,
            })
            .unwrap();

        reject_snapshot_blocked_host_controls(&control_rx, "snapshot persistence pending");

        assert!(matches!(
            tick_reply_rx.recv().unwrap(),
            Err(HostedGroupError::Retryable(_)),
        ));

        assert!(matches!(
            step_reply_rx.recv().unwrap(),
            Err(HostedGroupError::Retryable(_)),
        ));

        assert!(matches!(
            propose_reply_rx.recv().unwrap(),
            Err(HostedGroupError::Retryable(_)),
        ));
    }
}
