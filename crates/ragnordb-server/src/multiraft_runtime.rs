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
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use ragnordb_catalog::{Catalog, MetadataApplyOutcome, MetadataState};
use ragnordb_common::{
    Error, Result,
    ids::{
        ClientRequestId, CommandKind, LogicalCommandId, NodeId, RaftGroupId, ReplicaId, RequestId,
        Timestamp,
    },
    metadata_codec::{
        CreateTableRequest, DesiredReplica, DesiredReplicaPlacement, DesiredReplicaRole,
        MetadataCommand, MetadataCommandEnvelope, NodeDescriptor, NodeLifecycle, TabletDescriptor,
    },
    raft_bootstrap::RaftGroupBootstrap,
};

use ragnordb_multiraft::{
    bootstrap::{FileBootstrapStore, load_durable_group_bootstrap},
    host::{
        MultiRaftGroupStatus, MultiRaftHost, MultiRaftHostConfig, MultiRaftHostError,
        MultiRaftHostStatus, MultiRaftTurnBudget, RoutedRaftMessage, SharedMultiRaftHostStatus,
    },
    membership::{MembershipDecision, MembershipObservation, plan_membership_reconciliation},
    meta::{
        MetadataReconcileActionKind, MetadataRuntimeHandle, bootstrap_metadata_group,
        recover_metadata_group,
    },
    snapshot::SnapshotWorkController,
    storage::{
        codec::RaftReplicaIdentity,
        persistence::{NodeRaftWal, NodeRaftWalHandle, RaftWal},
        recovery::RecoveredRaftStorage,
    },
    transport::{NodeRaftEndpoint, NodeRaftInbound, NodeRaftTransport, NodeRaftTransportConfig},
};

use ragnordb_exec::{MetadataTableCreator, MetadataTableTopology, SharedMetadataTableCreator};
use ragnordb_tablet::snapshot::FileTabletSnapshotStore;
use ragnordb_tablet::snapshot::TabletSnapshotInstallTarget;
use ragnordb_txn::{TimestampReservation, TimestampReservationProvider};

use wal::{io::directory::FsSegmentDirectory, wal::WalHandle};

use crate::{
    bootstrap::{METADATA_RAFT_GROUP_ID, metadata_seed_descriptors, resolve_metadata_bootstrap},
    config::NodeConfig,
    data_directory_lock::DataDirectoryLock,
    database::SharedLocalDatabase,
    drain_jobs::{DrainJobRegistry, DrainReplicaJobRecord, DrainReplicaJobStage},
    node_lifecycle::eligible_replacement_nodes,
    replica_join::{
        JoiningMembershipWitness, JoiningReplicaLifecycle, JoiningReplicaRecord,
        JoiningReplicaRegistry,
    },
    replica_registry::{
        DurableFrontier, InitialReplicaConfiguration, LocalReplicaKey, LocalReplicaRecord,
        LocalReplicaRegistry, ReplicaLifecycle,
    },
    replicated_tablet::{FixedReactorSet, ReplicatedTabletHandle, ReplicatedTabletRuntime},
    rpc::{
        MetadataRpcClient, ReplicaJoinAdmission, ReplicaJoinAdmissionResult, ReplicaJoinRpcClient,
        RpcState, SharedTabletHandleRegistry, TabletRpcClient, spawn_dispatcher,
    },
    snapshot_transport::{NodeSnapshotEndpoint, NodeSnapshotTransport},
};

type LocalWal = WalHandle<FsSegmentDirectory, ()>;

const TICK_INTERVAL: Duration = Duration::from_millis(100);

const HOST_GROUP_BUDGET: usize = 64;

const HOST_MESSAGE_BUDGET: usize = 256;

const METADATA_ELECTION_TIMEOUT_TICKS: u64 = 10;

const METADATA_HEARTBEAT_INTERVAL_TICKS: u64 = 3;

/// The host gives a transfer enough logical ticks for the target to receive
/// the final frontier and start its election, while keeping proposal fencing
/// bounded during placement reconciliation.
const LEADERSHIP_TRANSFER_TIMEOUT_TICKS: u64 = 20;

/// Shutdown is allowed to hand leadership to already-eligible voters, but it
/// must not hold process teardown open indefinitely when a peer is unavailable.
const SHUTDOWN_LEADERSHIP_TRANSFER_DEADLINE: Duration = Duration::from_secs(2);

const METADATA_BOOTSTRAP_RETRY_INTERVAL: Duration = Duration::from_millis(250);

const METADATA_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

const METADATA_REQUEST_CHANNEL_CAPACITY: usize = 1024;

const METADATA_REQUEST_BUDGET: usize = 64;

const REPLICA_JOIN_REQUEST_CHANNEL_CAPACITY: usize = 128;

/// Wake source shared by the host's local request producers. The host thread
/// is bound after it starts; requests submitted before that point remain in
/// bounded channels and are drained during the first turn.
#[derive(Clone)]
pub(crate) struct HostWake {
    thread: Arc<Mutex<Option<thread::Thread>>>,
}

impl HostWake {
    pub(crate) fn new() -> Self {
        Self {
            thread: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn bind_current_thread(&self) {
        *self
            .thread
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(thread::current());
    }

    pub(crate) fn wake(&self) {
        if let Some(thread) = self
            .thread
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .cloned()
        {
            thread.unpark();
        }
    }
}

/// Converts elapsed wall time into the logical ticks expected by Raft while
/// allowing the host thread to park until the next sparse deadline. The
/// logical clock advances only in whole intervals, so a delayed wakeup may
/// catch up several ticks but can never stretch a configured heartbeat or
/// election timeout.
struct SparseTickClock {
    interval: Duration,
    last_boundary: Instant,
}

impl SparseTickClock {
    fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_boundary: Instant::now(),
        }
    }

    fn take_elapsed_ticks(&mut self, now: Instant) -> u64 {
        let interval_nanos = self.interval.as_nanos().max(1);
        let elapsed_nanos = now.saturating_duration_since(self.last_boundary).as_nanos();
        let ticks = elapsed_nanos / interval_nanos;
        let ticks = u64::try_from(ticks).unwrap_or(u64::MAX);

        if ticks == 0 {
            return 0;
        }

        if ticks <= u64::from(u32::MAX)
            && let Some(advance) = self.interval.checked_mul(ticks as u32)
        {
            self.last_boundary += advance;
        } else {
            // A process paused for more than u32::MAX intervals has already
            // exceeded every configured Raft safety window. Dropping the
            // unrepresentable remainder is preferable to overflowing the
            // duration arithmetic; the next loop still advances normally.
            self.last_boundary = now;
        }

        ticks
    }

    fn duration_until_ticks(&self, ticks: u64) -> Duration {
        let elapsed = self.last_boundary.elapsed();
        if ticks == 0 {
            return Duration::ZERO;
        }

        let target = if ticks <= u64::from(u32::MAX) {
            self.interval
                .checked_mul(ticks as u32)
                .unwrap_or(Duration::from_secs(1))
        } else {
            Duration::from_secs(1)
        };
        target.saturating_sub(elapsed)
    }
}

pub(crate) enum MetadataHostRequest {
    Command {
        envelope: Box<MetadataCommandEnvelope>,
        reply: mpsc::Sender<Result<MetadataApplyOutcome>>,
        deadline: Instant,
    },
    ConfChange {
        expected_conf_state_version: u64,
        replica_id: ReplicaId,
        reply: mpsc::Sender<Result<()>>,
    },
}

struct PendingMetadataProposal {
    request_id: RequestId,
    reply: mpsc::Sender<Result<MetadataApplyOutcome>>,
    deadline: Instant,
}

/// Owns metadata-driven tablet reactor registrations for the lifetime of the node host.
///
/// The metadata Raft state machine is the placement authority; this controller
/// only materializes replicas assigned to the local physical node. Every
/// identity is persisted as `Creating` before bootstrap. Recovered lifetimes
/// are promoted to `Active` after their Ready owner is registered; a fresh
/// group can remain `Creating` until it has emitted a recovery-visible WAL
/// frontier. The map of runtime guards is also the in-process idempotency
/// fence: a committed metadata replay cannot spawn a second reactor owner for the
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
    joins: JoiningReplicaRegistry,
    recovered: RecoveredRaftStorage,
    start_gate: Arc<AtomicBool>,
    reactors: Arc<FixedReactorSet>,
    runtimes: BTreeMap<RaftReplicaIdentity, ReplicatedTabletRuntime>,
    /// Retention handles outlive a detached runtime until its safe WAL floor
    /// has been published. Keeping them here also makes interrupted cleanup
    /// restartable without reopening a second writer for the same identity.
    writers: BTreeMap<RaftReplicaIdentity, NodeRaftWalHandle<LocalWal>>,
    tablet_handles: SharedTabletHandleRegistry,
    pending_retirements: BTreeSet<(RaftGroupId, ReplicaId)>,
    drain_jobs: DrainJobRegistry,
    metadata_control: Option<MetadataProposalClient>,
    pending_drain_proposals: Arc<Mutex<BTreeSet<(RaftGroupId, ReplicaId, u8)>>>,
    pending_metadata_membership_removals: Arc<Mutex<BTreeSet<(ReplicaId, u64)>>>,
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
        joins: JoiningReplicaRegistry,
        recovered: RecoveredRaftStorage,
        start_gate: Arc<AtomicBool>,
        reactors: Arc<FixedReactorSet>,
        tablet_handles: SharedTabletHandleRegistry,
    ) -> Result<Self> {
        let cluster_id = config.cluster_id.as_deref().ok_or_else(|| {
            Error::Configuration("drain jobs require a configured cluster ID".to_string())
        })?;
        let drain_jobs =
            DrainJobRegistry::open(config.data_dir.join("node-drain-jobs.json"), cluster_id)?;
        Ok(Self {
            config,
            wal,
            database,
            transport,
            snapshot_store,
            snapshot_work,
            snapshot_transport,
            registry,
            joins,
            recovered,
            start_gate,
            reactors,
            runtimes: BTreeMap::new(),
            writers: BTreeMap::new(),
            tablet_handles,
            pending_retirements: BTreeSet::new(),
            drain_jobs,
            metadata_control: None,
            pending_drain_proposals: Arc::new(Mutex::new(BTreeSet::new())),
            pending_metadata_membership_removals: Arc::new(Mutex::new(BTreeSet::new())),
        })
    }

    fn install_metadata_control(&mut self, metadata_control: MetadataProposalClient) {
        self.metadata_control = Some(metadata_control);
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
            // Applied indexes advance on every Raft entry. Persisting that
            // diagnostic frontier on every host turn rewrites and fsyncs the
            // complete registry JSON in the steady state. Snapshot creation
            // is the coarse durability checkpoint: retain the last applied
            // frontier in the registry only when a newer durable snapshot
            // boundary is observed.
            let snapshot_checkpoint =
                snapshot_frontier.filter(|frontier| existing.snapshot_frontier != Some(*frontier));
            if snapshot_checkpoint.is_some() {
                self.registry.update_frontiers(
                    key,
                    snapshot_checkpoint,
                    apply_frontier.or(existing.apply_frontier),
                )?;
            }
            if status.last_log_index > 0 || status.applied_index > 0 {
                self.registry.mark_active(key)?;
            }
            if let Some(join) = self
                .joins
                .record(identity.raft_group_id, identity.replica_id)?
            {
                match (join.lifecycle, status.replica_in_conf_state) {
                    (
                        JoiningReplicaLifecycle::Creating
                        | JoiningReplicaLifecycle::RouteRegistered
                        | JoiningReplicaLifecycle::AddLearnerCommitted
                        | JoiningReplicaLifecycle::CatchingUp
                        | JoiningReplicaLifecycle::ReadyToPromote,
                        Some(false),
                    ) => {
                        // A committed removal is the only transition that can
                        // move a live join lifetime to Removed. Retain the
                        // record until metadata records the exact removal
                        // proof and the tombstone cleanup completes.
                        self.joins.advance(
                            join.raft_group_id,
                            join.replica_id,
                            JoiningReplicaLifecycle::Removed,
                        )?;
                    }
                    (_, Some(true)) => {
                        if status.learners.contains(&identity.replica_id.0) {
                            self.joins.advance(
                                join.raft_group_id,
                                join.replica_id,
                                JoiningReplicaLifecycle::AddLearnerCommitted,
                            )?;
                            if status.applied_index < status.commit_index
                                || status.snapshot_install_pending
                                || status.runtime_error.is_some()
                            {
                                self.joins.advance(
                                    join.raft_group_id,
                                    join.replica_id,
                                    JoiningReplicaLifecycle::CatchingUp,
                                )?;
                            }
                        }
                        if status.applied_index >= status.commit_index
                            && !status.snapshot_install_pending
                            && status.runtime_error.is_none()
                        {
                            self.joins.advance(
                                join.raft_group_id,
                                join.replica_id,
                                JoiningReplicaLifecycle::ReadyToPromote,
                            )?;
                        }
                    }
                    _ => {}
                }
            }
        }

        let state = metadata.state_snapshot();
        if state.cluster_id() != Some(self.config.cluster_id.as_deref().unwrap_or_default()) {
            return Ok(());
        }

        self.reconcile_retired(host, &state, active)?;

        // A placement edit can remove a local dynamic replica before the
        // committed RemoveReplica entry reaches it. If its recovered WAL
        // still contains that lifetime, materialize it from the durable join
        // witness so it can apply the removal and expose the exact proof.
        for record in self.joins.records()? {
            if record.physical_node_id != self.config.node_id
                || matches!(
                    record.lifecycle,
                    JoiningReplicaLifecycle::Removed | JoiningReplicaLifecycle::Retired
                )
            {
                continue;
            }
            let identity = RaftReplicaIdentity::new(record.raft_group_id, record.replica_id)
                .map_err(|source| Error::Configuration(source.to_string()))?;
            if self.recovered.replica(identity).is_none() {
                continue;
            }
            let desired_contains_lifetime =
                state
                    .desired_placement(record.tablet_id)
                    .is_some_and(|placement| {
                        placement.replicas.iter().any(|replica| {
                            replica.replica_id == record.replica_id
                                && replica.node_id == record.physical_node_id
                        })
                    });
            if desired_contains_lifetime {
                continue;
            }
            let request = join_request_from_record(&record);
            self.admit_replica_join(host, metadata, self.config.node_id, &request, active, true)?;
        }

        let descriptors = state.tablets().cloned().collect::<Vec<_>>();
        for descriptor in descriptors {
            let Some(placement) = state.desired_placement(descriptor.tablet_id).cloned() else {
                return Err(Error::CorruptData(format!(
                    "tablet {} has no desired replica placement",
                    descriptor.tablet_id.0
                )));
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

            let desired_local = placement
                .replicas
                .iter()
                .find(|replica| replica.node_id == self.config.node_id);
            let durable_local_replica = bootstrap
                .as_ref()
                .and_then(|bootstrap| bootstrap.replica_on_node(self.config.node_id));

            // A later placement epoch may assign this physical node a new
            // replica lifetime. Such a lifetime is admitted only through the
            // durable join registry; it must never be represented by editing
            // the immutable initial bootstrap.
            if let Some(desired_local) = desired_local {
                let dynamic_join = self
                    .joins
                    .record(descriptor.raft_group_id, desired_local.replica_id)?;
                if durable_local_replica != Some(desired_local.replica_id) {
                    let mut dynamic_join_admitted = false;
                    if let Some(record) = dynamic_join {
                        let request = join_request_from_record(&record);
                        self.admit_replica_join(
                            host,
                            metadata,
                            self.config.node_id,
                            &request,
                            active,
                            false,
                        )?;
                        dynamic_join_admitted = true;
                    }
                    if durable_local_replica.is_some() || dynamic_join_admitted {
                        continue;
                    }
                }
            } else if let Some(local_replica_id) = durable_local_replica {
                let identity = RaftReplicaIdentity::new(descriptor.raft_group_id, local_replica_id)
                    .map_err(|source| Error::Configuration(source.to_string()))?;
                let still_committed = self.runtimes.contains_key(&identity)
                    || self
                        .recovered
                        .replica(identity)
                        .and_then(|replica| replica.conf_state())
                        .is_some_and(|conf_state| {
                            conf_state.contains(
                                local_replica_id
                                    .to_raft()
                                    .expect("validated bootstrap replica IDs are non-zero"),
                            )
                        });
                if !still_committed {
                    // The committed removal proof may already be visible in
                    // recovered ConfState while metadata retirement is still
                    // propagating. The later retirement pass owns cleanup.
                    continue;
                }
            } else {
                continue;
            }
            // Once written, the initial bootstrap is immutable historical
            // authority. New desired members are handled by the join record
            // above and by committed ConfState transitions; they must not make
            // this original file appear to have changed.
            let bootstrap = bootstrap.unwrap_or(requested_bootstrap);

            let local_replica_id =
                bootstrap
                    .replica_on_node(self.config.node_id)
                    .ok_or_else(|| {
                        Error::Configuration(format!(
                            "tablet group {} has no local replica assignment",
                            descriptor.raft_group_id.0
                        ))
                    })?;
            if desired_local.is_some_and(|desired| desired.replica_id != local_replica_id) {
                return Err(Error::RecoveryFailed {
                    reason: format!(
                        "metadata placement for tablet {} disagrees with durable local replica {}",
                        descriptor.tablet_id.0, local_replica_id.0
                    ),
                });
            }

            // SQL executes CREATE TABLE while holding the database owner lock
            // and waits for metadata apply. Do not start a tablet reactor from
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
            let retention_writer = group_wal.clone();
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
            // Materialize the replicated tablet state machine without
            // reacquiring the SQL owner lock while CREATE TABLE completes.
            // The gateway reads this runtime through the shared handle registry
            // after the lifecycle owner has registered the group.
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
                self.reactors.clone(),
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
            self.tablet_handles
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(identity.raft_group_id, self.runtimes[&identity].handle());
            self.writers.insert(identity, retention_writer);
        }
        Ok(())
    }

    /// Execute at most one fresh metadata-to-Raft membership transition for
    /// each local leader. The target-preparation RPC is repeated from the
    /// current observation on every pass, so route loss, leadership changes,
    /// and desired-placement edits naturally produce a retry/replan rather
    /// than applying a stale action.
    fn reconcile_membership(
        &mut self,
        host: &mut MultiRaftHost<LocalWal>,
        metadata: &MetadataRuntimeHandle,
        replica_join_rpc: &ReplicaJoinRpcClient,
        metadata_replica_to_node: &BTreeMap<ReplicaId, NodeId>,
    ) -> Result<()> {
        let state = metadata.state_snapshot();
        if state.cluster_id() != Some(self.config.cluster_id.as_deref().unwrap_or_default()) {
            return Ok(());
        }
        let host_status = host.status();
        for group_status in host_status.groups {
            if group_status.role != Some(ragnordb_multiraft::host::MultiRaftRole::Leader) {
                continue;
            }
            let Some(descriptor) = state.tablet_for_raft_group(group_status.identity.raft_group_id)
            else {
                continue;
            };
            let Some(desired) = state.desired_placement(descriptor.tablet_id).cloned() else {
                continue;
            };
            if let Some((removed_replica, _, _, _)) = group_status.last_removed_replica
                && !state.is_replica_retired(group_status.identity.raft_group_id, removed_replica)
            {
                // Keep one removal proof outstanding per group. The Raft
                // core exposes the latest exact removal tuple; proposing a
                // second removal before metadata durably records this one
                // could overwrite the only proof needed to retire the first
                // local lifetime after a crash.
                continue;
            }
            let observed = status_conf_state(&group_status)?;
            let action = match ragnordb_multiraft::meta::next_reconcile_action(&desired, &observed)
            {
                Ok(Some(action)) => action,
                Ok(None)
                | Err(ragnordb_multiraft::meta::MetadataReconcileError::JointConsensusInProgress) =>
                {
                    continue;
                }
                Err(error) => return Err(Error::Configuration(error.to_string())),
            };
            let mut target_prepared = BTreeMap::new();
            let mut target_snapshot_pending = BTreeMap::new();
            let mut target_quarantined = BTreeMap::new();
            let mut target_apply_indices = BTreeMap::new();

            let target = match &action.kind {
                MetadataReconcileActionKind::AddLearner {
                    replica_id,
                    node_id,
                }
                | MetadataReconcileActionKind::PromoteLearner {
                    replica_id,
                    node_id,
                } => Some((*replica_id, *node_id)),
                MetadataReconcileActionKind::RemoveReplica { .. } => None,
            };
            if let Some((target_replica, target_node)) = target {
                let require_caught_up = matches!(
                    &action.kind,
                    MetadataReconcileActionKind::PromoteLearner { .. }
                );
                let request = join_request_from_status(
                    self.config.cluster_id.as_deref().unwrap_or_default(),
                    descriptor,
                    &group_status,
                    target_replica,
                    target_node,
                    require_caught_up,
                );
                let mut prepared = true;
                for node in state
                    .nodes()
                    .filter(|node| node.lifecycle == NodeLifecycle::Active)
                {
                    let result = if node.node_id == self.config.node_id {
                        self.admit_replica_join(
                            host,
                            metadata,
                            self.config.node_id,
                            &request,
                            true,
                            false,
                        )
                    } else {
                        replica_join_rpc.prepare(
                            node.node_id,
                            request.clone(),
                            Duration::from_millis(250),
                        )
                    };
                    if result.is_err() {
                        prepared = false;
                        break;
                    }
                }
                target_prepared.insert(target_replica, prepared);
                target_snapshot_pending.insert(target_replica, false);
                target_quarantined.insert(target_replica, !prepared);
                if prepared && require_caught_up {
                    target_apply_indices.insert(target_replica, group_status.commit_index);
                }
            }

            let observation = MembershipObservation {
                desired,
                observed,
                local_replica_id: group_status.identity.replica_id,
                leader_replica_id: group_status.leader_replica_id,
                commit_index: group_status.commit_index,
                replica_match_indices: group_status.replica_match_indices.iter().copied().collect(),
                target_apply_indices,
                target_prepared,
                target_snapshot_pending,
                target_quarantined,
                same_replica_lifetime: group_status
                    .voters
                    .iter()
                    .chain(group_status.learners.iter())
                    .copied()
                    .map(|replica_id| {
                        (
                            replica_id,
                            !state.is_replica_retired(
                                group_status.identity.raft_group_id,
                                replica_id,
                            ),
                        )
                    })
                    .collect(),
            };
            // Remote preparation can take long enough for metadata placement
            // or node lifecycle to change. Re-read both authorities before
            // proposing; the host performs the final ConfState-version check.
            let latest_state = metadata.state_snapshot();
            if latest_state.tablet(descriptor.tablet_id) != Some(descriptor)
                || latest_state.desired_placement(descriptor.tablet_id)
                    != Some(&observation.desired)
                || target.is_some_and(|(_, node_id)| {
                    latest_state
                        .node(node_id)
                        .is_none_or(|node| node.lifecycle != NodeLifecycle::Active)
                })
            {
                continue;
            }
            match plan_membership_reconciliation(&observation)
                .map_err(|error| Error::Configuration(error.to_string()))?
            {
                MembershipDecision::Action(action) => {
                    match host.propose_reconcile_action(group_status.identity.raft_group_id, action)
                    {
                        Ok(proposal) => send_outbound(&self.transport, proposal.outbound),
                        Err(MultiRaftHostError::GroupRejected { .. })
                        | Err(MultiRaftHostError::GroupRetryable { .. })
                        | Err(MultiRaftHostError::StaleMembershipObservation { .. })
                        | Err(MultiRaftHostError::StaleMembershipAction { .. }) => {}
                        Err(MultiRaftHostError::LeaderTransferRequired { .. }) => {
                            let latest_routes = replica_routes_for_group(
                                &latest_state,
                                group_status.identity.raft_group_id,
                                metadata_replica_to_node,
                            );
                            request_leadership_transfer(
                                host,
                                &self.transport,
                                &group_status,
                                &latest_routes,
                                &active_node_ids(&latest_state),
                                "membership removal",
                            )?;
                        }
                        Err(error) => return Err(host_error(error)),
                    }
                }
                MembershipDecision::WaitForCatchUp { .. }
                | MembershipDecision::PausedJointConsensus
                | MembershipDecision::Noop => {}
                MembershipDecision::LeaderTransferRequired { .. } => {
                    let latest_routes = replica_routes_for_group(
                        &latest_state,
                        group_status.identity.raft_group_id,
                        metadata_replica_to_node,
                    );
                    request_leadership_transfer(
                        host,
                        &self.transport,
                        &group_status,
                        &latest_routes,
                        &active_node_ids(&latest_state),
                        "membership removal",
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Publish the second, metadata-owned half of a removal proof. This is
    /// intentionally separate from the Raft configuration proposal: a crash
    /// between the two commits simply causes this idempotent command to be
    /// proposed again after restart.
    fn reconcile_retirement_records(
        &mut self,
        host: &mut MultiRaftHost<LocalWal>,
        metadata: &MetadataRuntimeHandle,
    ) -> Result<()> {
        let state = metadata.state_snapshot();
        self.pending_retirements
            .retain(|key| !state.is_replica_retired(key.0, key.1));
        for record in self.registry.records()? {
            let key = record.key();
            if state.is_replica_retired(key.raft_group_id, key.replica_id)
                || self
                    .pending_retirements
                    .contains(&(key.raft_group_id, key.replica_id))
            {
                continue;
            }
            let Some(descriptor) = state.tablet_for_raft_group(key.raft_group_id) else {
                continue;
            };
            let Some(placement) = state.desired_placement(descriptor.tablet_id) else {
                continue;
            };
            if placement
                .replicas
                .iter()
                .any(|replica| replica.replica_id == key.replica_id)
            {
                // A desired-placement edit back to this lifetime wins over a
                // stale removal observation until the removal is committed.
                continue;
            }
            let Some(status) = host
                .status()
                .groups
                .into_iter()
                .find(|status| status.identity.raft_group_id == key.raft_group_id)
            else {
                continue;
            };
            let Some((removed_replica, removal_index, removal_term, removed_version)) =
                status.last_removed_replica
            else {
                continue;
            };
            if removed_replica != key.replica_id {
                continue;
            }
            let observed = status_conf_state(&status)?;
            if !conf_state_excludes_replica(&observed, key.replica_id) {
                continue;
            }
            let command = MetadataCommand::RecordReplicaRetirement {
                raft_group_id: key.raft_group_id,
                replica_id: key.replica_id,
                desired_configuration_epoch: placement.configuration_epoch,
                removed_conf_state_version: removed_version,
                removal_index,
                removal_term,
            };
            let encoded = command
                .encode()
                .map_err(|error| Error::CorruptData(error.to_string()))?;
            match host.propose(METADATA_RAFT_GROUP_ID, encoded.clone(), encoded.len()) {
                Ok(proposal) => {
                    send_outbound(&self.transport, proposal.outbound);
                    self.pending_retirements
                        .insert((key.raft_group_id, key.replica_id));
                }
                Err(MultiRaftHostError::GroupRejected { .. })
                | Err(MultiRaftHostError::GroupRetryable { .. }) => {}
                Err(MultiRaftHostError::RecoveryRequired) => {
                    return Err(Error::RecoveryRequired {
                        reason: "metadata Raft entered recovery-required state while recording replica retirement".to_string(),
                    });
                }
                Err(error) => tracing::debug!(
                    error = %error,
                    "replica retirement proposal will be retried",
                ),
            }
        }
        Ok(())
    }

    /// Reconcile the node-level drain workflow from durable metadata and the
    /// local committed Raft observations. Placement edits are submitted through
    /// the asynchronous metadata client, so a follower node never blocks its
    /// own host loop while forwarding a control-plane proposal.
    fn reconcile_node_drain(
        &mut self,
        host: &mut MultiRaftHost<LocalWal>,
        metadata: &MetadataRuntimeHandle,
        metadata_replica_to_node: &BTreeMap<ReplicaId, NodeId>,
    ) -> Result<()> {
        let state = metadata.state_snapshot();
        let Some(node) = state.node(self.config.node_id) else {
            return Ok(());
        };
        if node.lifecycle == NodeLifecycle::Active {
            return Ok(());
        }

        self.ensure_drain_jobs(&state)?;
        let host_status = host.status();
        for job in self.drain_jobs.records()? {
            if job.node_id != self.config.node_id || job.stage == DrainReplicaJobStage::Complete {
                continue;
            }
            self.reconcile_drain_job(&job, &state, &host_status)?;
        }

        self.reconcile_metadata_group_removal(host, &state, metadata_replica_to_node)?;

        let status = crate::node_lifecycle::compute_node_drain_status(
            &state,
            Some(&host_status),
            self.config.node_id,
        );
        match node.lifecycle {
            NodeLifecycle::Decommissioning if status.ready_for_decommissioned => {
                self.submit_node_lifecycle(NodeLifecycle::Decommissioned, 4);
            }
            NodeLifecycle::Decommissioned if status.blockers.is_empty() => {
                self.submit_node_lifecycle(NodeLifecycle::Tombstoned, 5);
            }
            _ => {}
        }
        Ok(())
    }

    /// Create one durable plan per local source lifetime. A voter replacement
    /// is represented in two metadata epochs: first the old voter plus the new
    /// voter (allowing Raft to add/promote safely), then the final placement
    /// without the draining source. Learners can be removed directly.
    fn ensure_drain_jobs(&mut self, state: &MetadataState) -> Result<()> {
        let existing_jobs = self.drain_jobs.records()?;
        let mut reserved_replacement_ids = existing_jobs
            .iter()
            .filter_map(|job| job.replacement_replica_id)
            .collect::<BTreeSet<_>>();
        let mut next_replacement_id = state.next_replica_id();

        for descriptor in state.tablets() {
            let Some(placement) = state.desired_placement(descriptor.tablet_id) else {
                continue;
            };
            for source in placement
                .replicas
                .iter()
                .filter(|replica| replica.node_id == self.config.node_id)
            {
                if state.is_replica_retired(descriptor.raft_group_id, source.replica_id)
                    || self
                        .drain_jobs
                        .record(descriptor.raft_group_id, source.replica_id)?
                        .is_some()
                {
                    continue;
                }

                let preferred_leader_nodes: Vec<NodeId> = placement
                    .placement_policy
                    .preferred_leader_nodes
                    .iter()
                    .copied()
                    .filter(|node_id| *node_id != self.config.node_id)
                    .collect();
                let mut final_policy = placement.placement_policy.clone();
                final_policy.preferred_leader_nodes = preferred_leader_nodes.clone();

                let mut final_placement = placement.clone();
                final_placement.configuration_epoch = placement
                    .configuration_epoch
                    .checked_add(1)
                    .ok_or_else(|| {
                        Error::Configuration("drain placement epoch exhausted".into())
                    })?;
                final_placement.placement_policy = final_policy.clone();
                final_placement
                    .replicas
                    .retain(|replica| replica.replica_id != source.replica_id);

                let (replacement_node_id, replacement_replica_id, replacement_placement) = if source
                    .role
                    == DesiredReplicaRole::Voter
                {
                    let replacement_node_id =
                        eligible_replacement_nodes(state, placement, self.config.node_id)
                            .into_iter()
                            .next()
                            .ok_or_else(|| Error::ProposalUnavailable {
                                reason: format!(
                                    "no eligible replacement for replica {} of group {}",
                                    source.replica_id.0, descriptor.raft_group_id.0
                                ),
                            })?;
                    let replacement_replica_id = loop {
                        let candidate =
                            next_replacement_id.ok_or_else(|| Error::ProposalUnavailable {
                                reason: "replica identity space exhausted during node drain".into(),
                            })?;
                        next_replacement_id = candidate
                            .0
                            .checked_add(1)
                            .filter(|id| *id != 0)
                            .map(ReplicaId);
                        if reserved_replacement_ids.insert(candidate) {
                            break candidate;
                        }
                    };
                    let mut transitional = placement.clone();
                    transitional.configuration_epoch = placement
                        .configuration_epoch
                        .checked_add(1)
                        .ok_or_else(|| {
                            Error::Configuration("drain placement epoch exhausted".into())
                        })?;
                    transitional.placement_policy.replication_factor = transitional
                        .placement_policy
                        .replication_factor
                        .checked_add(1)
                        .ok_or_else(|| {
                            Error::Configuration(
                                "drain temporary replication factor exhausted".into(),
                            )
                        })?;
                    transitional.placement_policy.preferred_leader_nodes = preferred_leader_nodes;
                    transitional.replicas.push(DesiredReplica {
                        replica_id: replacement_replica_id,
                        node_id: replacement_node_id,
                        role: DesiredReplicaRole::Voter,
                    });
                    transitional
                        .replicas
                        .sort_by_key(|replica| replica.replica_id);
                    final_placement.replicas.push(DesiredReplica {
                        replica_id: replacement_replica_id,
                        node_id: replacement_node_id,
                        role: DesiredReplicaRole::Voter,
                    });
                    final_placement
                        .replicas
                        .sort_by_key(|replica| replica.replica_id);
                    final_placement.configuration_epoch = placement
                        .configuration_epoch
                        .checked_add(2)
                        .ok_or_else(|| {
                            Error::Configuration("drain placement epoch exhausted".into())
                        })?;
                    (
                        Some(replacement_node_id),
                        Some(replacement_replica_id),
                        Some(transitional),
                    )
                } else {
                    (None, None, None)
                };

                self.drain_jobs.ensure(DrainReplicaJobRecord {
                    cluster_id: self.config.cluster_id.clone().unwrap_or_default(),
                    node_id: self.config.node_id,
                    raft_group_id: descriptor.raft_group_id,
                    tablet_id: descriptor.tablet_id,
                    source_replica_id: source.replica_id,
                    replacement_node_id,
                    replacement_replica_id,
                    replacement_placement,
                    final_placement,
                    stage: DrainReplicaJobStage::Planned,
                })?;
            }
        }
        Ok(())
    }

    fn reconcile_drain_job(
        &mut self,
        job: &DrainReplicaJobRecord,
        state: &MetadataState,
        host_status: &MultiRaftHostStatus,
    ) -> Result<()> {
        let current = state.desired_placement(job.tablet_id);
        match job.stage {
            DrainReplicaJobStage::Planned => {
                if current == Some(&job.final_placement) {
                    self.drain_jobs
                        .advance(job.key(), DrainReplicaJobStage::SourceRemovalDesired)?;
                } else if job.replacement_placement.as_ref() == current {
                    self.drain_jobs
                        .advance(job.key(), DrainReplicaJobStage::ReplacementDesired)?;
                } else if let Some(replacement) = &job.replacement_placement {
                    self.submit_drain_placement(job, replacement, 1);
                } else {
                    self.submit_drain_placement(job, &job.final_placement, 2);
                }
            }
            DrainReplicaJobStage::ReplacementDesired => {
                if current == Some(&job.final_placement) {
                    self.drain_jobs
                        .advance(job.key(), DrainReplicaJobStage::SourceRemovalDesired)?;
                } else if self.replacement_is_ready(job, host_status) {
                    self.submit_drain_placement(job, &job.final_placement, 2);
                }
            }
            DrainReplicaJobStage::SourceRemovalDesired => {
                if !state.is_replica_retired(job.raft_group_id, job.source_replica_id)
                    && current != Some(&job.final_placement)
                {
                    self.submit_drain_placement(job, &job.final_placement, 2);
                } else if state.is_replica_retired(job.raft_group_id, job.source_replica_id) {
                    self.drain_jobs
                        .advance(job.key(), DrainReplicaJobStage::Retired)?;
                }
            }
            DrainReplicaJobStage::Retired => {
                let still_hosted = host_status.groups.iter().any(|group| {
                    group.identity.raft_group_id == job.raft_group_id
                        && group.identity.replica_id == job.source_replica_id
                });
                if !still_hosted {
                    self.drain_jobs
                        .advance(job.key(), DrainReplicaJobStage::Complete)?;
                }
            }
            DrainReplicaJobStage::Complete => {}
        }
        Ok(())
    }

    fn replacement_is_ready(
        &self,
        job: &DrainReplicaJobRecord,
        host_status: &MultiRaftHostStatus,
    ) -> bool {
        let Some(replacement_replica_id) = job.replacement_replica_id else {
            return false;
        };
        host_status
            .groups
            .iter()
            .find(|group| {
                group.identity.raft_group_id == job.raft_group_id
                    && group.identity.replica_id == job.source_replica_id
            })
            .is_some_and(|group| {
                group.voters.contains(&replacement_replica_id)
                    && group.outgoing_voters.is_empty()
                    && group.pending_conf_change_index.is_none()
                    && group
                        .replica_match_indices
                        .iter()
                        .find(|(replica_id, _)| *replica_id == replacement_replica_id)
                        .is_some_and(|(_, match_index)| *match_index >= group.commit_index)
            })
    }

    fn reconcile_metadata_group_removal(
        &mut self,
        host: &mut MultiRaftHost<LocalWal>,
        state: &MetadataState,
        metadata_replica_to_node: &BTreeMap<ReplicaId, NodeId>,
    ) -> Result<()> {
        let Some(node) = state.node(self.config.node_id) else {
            return Ok(());
        };
        if !matches!(
            node.lifecycle,
            NodeLifecycle::Draining | NodeLifecycle::Decommissioning
        ) {
            return Ok(());
        }
        let Some(status) = host
            .status()
            .groups
            .into_iter()
            .find(|group| group.identity.raft_group_id == METADATA_RAFT_GROUP_ID)
        else {
            return Ok(());
        };
        let local_replica_id = status.identity.replica_id;
        let local_in_conf_state = status.voters.contains(&local_replica_id)
            || status.learners.contains(&local_replica_id)
            || status.outgoing_voters.contains(&local_replica_id);
        if !local_in_conf_state
            || !status.outgoing_voters.is_empty()
            || status.pending_conf_change_index.is_some()
            || status.voters.len() <= 1
        {
            return Ok(());
        }

        // Do not remove this member while another metadata voter is already
        // draining; the metadata group has no tablet placement policy to
        // supply a replacement quorum check.
        let remaining_voters_are_active = status
            .voters
            .iter()
            .filter(|replica_id| **replica_id != local_replica_id)
            .all(|replica_id| {
                metadata_replica_to_node
                    .get(replica_id)
                    .and_then(|node_id| state.node(*node_id))
                    .is_some_and(|node| node.lifecycle == NodeLifecycle::Active)
            });
        if !remaining_voters_are_active {
            return Ok(());
        }

        let expected_version =
            status
                .conf_state_version
                .ok_or_else(|| Error::ProposalUnavailable {
                    reason: "metadata membership version is not published".into(),
                })?;
        self.submit_metadata_group_removal(local_replica_id, expected_version);
        Ok(())
    }

    fn submit_metadata_group_removal(&self, replica_id: ReplicaId, expected_version: u64) {
        let Some(metadata_control) = self.metadata_control.clone() else {
            return;
        };
        let key = (replica_id, expected_version);
        let mut pending = self
            .pending_metadata_membership_removals
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !pending.insert(key) {
            return;
        }
        drop(pending);

        let pending_removals = Arc::clone(&self.pending_metadata_membership_removals);
        let _ = thread::Builder::new()
            .name("ragnordb-metadata-removal".to_string())
            .spawn(move || {
                let result = metadata_control.remove_metadata_replica(
                    replica_id,
                    expected_version,
                    Duration::from_secs(30),
                );
                if let Err(error) = result {
                    tracing::debug!(
                        replica_id = replica_id.0,
                        expected_version,
                        %error,
                        "metadata membership removal will be retried"
                    );
                }
                pending_removals
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&key);
            });
    }

    fn submit_drain_placement(
        &self,
        job: &DrainReplicaJobRecord,
        placement: &DesiredReplicaPlacement,
        phase: u8,
    ) {
        let Some(metadata_control) = self.metadata_control.clone() else {
            return;
        };
        let pending_key = (job.raft_group_id, job.source_replica_id, phase);
        let mut pending = self
            .pending_drain_proposals
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !pending.insert(pending_key) {
            return;
        }
        drop(pending);
        let pending_requests = Arc::clone(&self.pending_drain_proposals);
        let placement = placement.clone();
        let request_id = drain_request_id(
            self.config.node_id,
            job.raft_group_id,
            job.source_replica_id,
            phase,
        );
        let _ = thread::Builder::new()
            .name("ragnordb-drain-proposal".to_string())
            .spawn(move || {
                if let Err(error) = metadata_control.set_desired_replica_placement(
                    placement,
                    request_id,
                    Duration::from_secs(2),
                ) {
                    tracing::debug!(error = %error, "node-drain placement proposal will be retried");
                }
                pending_requests
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&pending_key);
            });
    }

    fn submit_node_lifecycle(&self, lifecycle: NodeLifecycle, phase: u8) {
        let Some(metadata_control) = self.metadata_control.clone() else {
            return;
        };
        let pending_key = (RaftGroupId(0), ReplicaId(self.config.node_id.0), phase);
        let mut pending = self
            .pending_drain_proposals
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !pending.insert(pending_key) {
            return;
        }
        let pending_requests = Arc::clone(&self.pending_drain_proposals);
        let node_id = self.config.node_id;
        let _ = thread::Builder::new()
            .name("ragnordb-drain-lifecycle".to_string())
            .spawn(move || {
                if let Err(error) = metadata_control.set_node_lifecycle(
                    node_id,
                    lifecycle,
                    Duration::from_secs(2),
                ) {
                    tracing::debug!(error = %error, "node-drain lifecycle proposal will be retried");
                }
                pending_requests
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&pending_key);
            });
    }

    /// Admit one post-bootstrap replica request on the host owner thread.
    ///
    /// Every node that receives the control-plane request installs the target
    /// route, while the target node additionally persists its joining lifetime
    /// and constructs the passive Ready owner. Consequently a leader cannot
    /// emit `AppendEntries` or a snapshot for a new replica until the target
    /// has a durable identity, route, WAL writer, and runtime.
    fn admit_replica_join(
        &mut self,
        host: &mut MultiRaftHost<LocalWal>,
        metadata: &MetadataRuntimeHandle,
        source_node_id: NodeId,
        request: &ragnordb_common::proto::rpc::ReplicaJoinRequest,
        active: bool,
        allow_obsolete_recovery: bool,
    ) -> Result<()> {
        let cluster_id = request.cluster_id.as_str();
        let group_id = request
            .raft_group_id
            .map(ragnordb_common::ids::RaftGroupId::from_proto)
            .ok_or_else(|| Error::InvalidArgument("join request has no Raft group".into()))?;
        let tablet_id = request
            .tablet_id
            .map(ragnordb_common::ids::TabletId::from_proto)
            .ok_or_else(|| Error::InvalidArgument("join request has no tablet".into()))?;
        let replica_id = request
            .replica_id
            .map(ragnordb_common::ids::ReplicaId::from_proto)
            .ok_or_else(|| Error::InvalidArgument("join request has no replica".into()))?;
        let node_id = request
            .physical_node_id
            .map(ragnordb_common::ids::NodeId::from_proto)
            .ok_or_else(|| Error::InvalidArgument("join request has no physical node".into()))?;
        if cluster_id != self.config.cluster_id.as_deref().unwrap_or_default() {
            return Err(Error::Configuration(
                "joining request belongs to another cluster".to_string(),
            ));
        }
        let state = metadata.state_snapshot();
        if state.cluster_id() != Some(cluster_id) {
            return Err(Error::Configuration(
                "joining request does not match committed metadata cluster".to_string(),
            ));
        }
        if state
            .node(node_id)
            .is_none_or(|node| node.lifecycle != NodeLifecycle::Active)
        {
            return Err(Error::ProposalUnavailable {
                reason: format!(
                    "joining target node {} is not an active metadata node",
                    node_id.0
                ),
            });
        }
        if source_node_id.0 == 0
            || state
                .node(source_node_id)
                .is_none_or(|node| node.lifecycle != NodeLifecycle::Active)
        {
            return Err(Error::ProposalUnavailable {
                reason: format!(
                    "joining request source node {} is not an active metadata node",
                    source_node_id.0
                ),
            });
        }
        if state.is_replica_retired(group_id, replica_id) {
            return Err(Error::InvalidArgument(format!(
                "replica {} of group {} is permanently retired",
                replica_id.0, group_id.0
            )));
        }
        let descriptor = state.tablet(tablet_id).ok_or_else(|| {
            Error::CorruptData(format!(
                "join request references unknown tablet {}",
                tablet_id.0
            ))
        })?;
        if descriptor.raft_group_id != group_id || descriptor.tablet_epoch != request.tablet_epoch {
            return Err(Error::StaleTabletEpoch {
                current_epoch: descriptor.tablet_epoch,
                expected_epoch: request.tablet_epoch,
            });
        }
        let placement = state.desired_placement(tablet_id).ok_or_else(|| {
            Error::CorruptData(format!("tablet {} has no desired placement", tablet_id.0))
        })?;
        let desired = placement
            .replicas
            .iter()
            .find(|replica| replica.replica_id == replica_id && replica.node_id == node_id);
        if desired.is_none() && !allow_obsolete_recovery {
            return Err(Error::ProposalUnavailable {
                reason: format!(
                    "replica {} on node {} is not in current desired placement",
                    replica_id.0, node_id.0
                ),
            });
        }
        if request.expected_current_conf_state_version == 0
            || request.committed_membership_version != request.expected_current_conf_state_version
        {
            return Err(Error::InvalidArgument(
                "join request has inconsistent membership witness version".to_string(),
            ));
        }
        let voters = request
            .voters
            .iter()
            .cloned()
            .map(ragnordb_common::ids::ReplicaId::from_proto)
            .collect::<std::collections::BTreeSet<_>>();
        let learners = request
            .learners
            .iter()
            .cloned()
            .map(ragnordb_common::ids::ReplicaId::from_proto)
            .collect::<std::collections::BTreeSet<_>>();
        let outgoing_voters = request
            .outgoing_voters
            .iter()
            .cloned()
            .map(ragnordb_common::ids::ReplicaId::from_proto)
            .collect::<std::collections::BTreeSet<_>>();
        if voters.len() != request.voters.len()
            || learners.len() != request.learners.len()
            || outgoing_voters.len() != request.outgoing_voters.len()
        {
            return Err(Error::InvalidArgument(
                "join request membership witness contains duplicate identities".to_string(),
            ));
        }
        let witness = JoiningMembershipWitness {
            version: request.committed_membership_version,
            voters,
            learners,
            outgoing_voters,
        };
        let witness_conf_state = witness.to_core()?;
        let witness = JoiningMembershipWitness::from_core(&witness_conf_state)?;

        if node_id != self.config.node_id {
            // Non-target nodes only install the route. The target persists its
            // Creating record first so a crash cannot leave an addressable
            // replica identity with no durable lifetime fence.
            self.transport
                .register_dynamic_route(group_id, replica_id, node_id)
                .map_err(|error| {
                    Error::Configuration(format!("register joining route: {error}"))
                })?;
            return Ok(());
        }

        if let Some(existing_group) = host
            .status()
            .groups
            .into_iter()
            .find(|status| status.identity.raft_group_id == group_id)
        {
            let existing_conf_state = status_conf_state(&existing_group)?;
            if existing_conf_state != witness_conf_state {
                return Err(Error::ProposalUnavailable {
                    reason: format!(
                        "joining membership witness for group {} is stale on target",
                        group_id.0
                    ),
                });
            }
        }

        if self
            .joins
            .record(group_id, replica_id)?
            .is_some_and(|record| {
                matches!(
                    record.lifecycle,
                    JoiningReplicaLifecycle::Removed | JoiningReplicaLifecycle::Retired
                )
            })
        {
            return Err(Error::InvalidArgument(format!(
                "replica {} of group {} has a terminal joining lifetime",
                replica_id.0, group_id.0
            )));
        }

        let record = JoiningReplicaRecord {
            cluster_id: cluster_id.to_string(),
            raft_group_id: group_id,
            tablet_id,
            tablet_epoch: descriptor.tablet_epoch,
            replica_id,
            physical_node_id: node_id,
            expected_current_conf_state_version: request.expected_current_conf_state_version,
            committed_membership_witness: witness,
            lifecycle: JoiningReplicaLifecycle::Creating,
        };
        self.joins.ensure(record)?;

        let local_record = LocalReplicaRecord::new(
            group_id,
            replica_id,
            tablet_id,
            descriptor.table_id,
            descriptor.tablet_epoch,
            ReplicaLifecycle::Creating,
        );
        self.registry.ensure_replica(local_record)?;
        let identity = RaftReplicaIdentity::new(group_id, replica_id)
            .map_err(|error| Error::Configuration(error.to_string()))?;

        // All configured voters learn the dynamic route. This is deliberately
        // separate from target materialization: a later leader must still be
        // able to send to the learner after leadership changes.
        self.transport
            .register_dynamic_route(group_id, replica_id, node_id)
            .map_err(|error| Error::Configuration(format!("register joining route: {error}")))?;

        if self.runtimes.contains_key(&identity) {
            self.joins.advance(
                group_id,
                replica_id,
                JoiningReplicaLifecycle::RouteRegistered,
            )?;
            if request.require_caught_up {
                self.verify_join_caught_up(identity, request.leader_commit_index)?;
                self.joins.advance(
                    group_id,
                    replica_id,
                    JoiningReplicaLifecycle::ReadyToPromote,
                )?;
            }
            return Ok(());
        }
        if self
            .runtimes
            .keys()
            .any(|existing| existing.raft_group_id == group_id)
        {
            return Err(Error::Configuration(format!(
                "node {} already hosts another lifetime of group {}",
                self.config.node_id.0, group_id.0
            )));
        }

        let durability_gate = self
            .database
            .try_lock()
            .map_err(|_| Error::Configuration("database is busy during replica join".into()))?
            .durability_gate();
        let group_wal = if active {
            host.issue_group_writer_after_activation(identity)
                .map_err(host_error)?
        } else {
            host.issue_group_writer(identity).map_err(host_error)?
        };
        let retention_writer = group_wal.clone();
        let group_transport = self
            .transport
            .register_dynamic_group(group_id, replica_id, node_id)
            .map_err(|error| {
                Error::Configuration(format!("register joining group route: {error}"))
            })?;
        let snapshot_endpoint = self
            .snapshot_transport
            .register_dynamic_group(group_id, replica_id, self.snapshot_store.clone())
            .map_err(|error| {
                Error::Configuration(format!("register joining snapshot route: {error}"))
            })?;
        let target = TabletSnapshotInstallTarget {
            cluster_id: self.config.cluster_id.clone().unwrap_or_default(),
            raft_group_id: group_id,
            tablet_id,
            table_id: descriptor.table_id,
            tablet_epoch: descriptor.tablet_epoch,
        };
        let runtime = ReplicatedTabletRuntime::start_hosted_joining_tablet_from_shared_recovery(
            &self.config,
            self.wal.clone(),
            self.database.clone(),
            replica_id,
            witness_conf_state,
            target,
            group_wal,
            group_transport,
            self.snapshot_store.clone(),
            self.snapshot_work.clone(),
            snapshot_endpoint,
            &self.recovered,
            self.start_gate.clone(),
            self.reactors.clone(),
            Some(durability_gate),
        )?;
        let recovered = self.recovered.replica(identity).is_some();
        if active {
            host.register_active_group(Box::new(runtime.hosted_group()))
                .map_err(host_error)?;
        } else if recovered {
            host.register_recovered_group(Box::new(runtime.hosted_group()))
                .map_err(host_error)?;
        } else {
            host.register_new_group(Box::new(runtime.hosted_group()))
                .map_err(host_error)?;
        }
        self.runtimes.insert(identity, runtime);
        self.tablet_handles
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(group_id, self.runtimes[&identity].handle());
        self.writers.insert(identity, retention_writer);
        self.joins.advance(
            group_id,
            replica_id,
            JoiningReplicaLifecycle::RouteRegistered,
        )?;
        if request.require_caught_up {
            self.verify_join_caught_up(identity, request.leader_commit_index)?;
            self.joins.advance(
                group_id,
                replica_id,
                JoiningReplicaLifecycle::ReadyToPromote,
            )?;
        }
        Ok(())
    }

    fn verify_join_caught_up(
        &self,
        identity: RaftReplicaIdentity,
        leader_commit: u64,
    ) -> Result<()> {
        let status = self
            .runtimes
            .get(&identity)
            .ok_or_else(|| Error::ProposalUnavailable {
                reason: "joining runtime is not materialized".to_string(),
            })?
            .handle()
            .status();
        if status.runtime_error.is_some()
            || status.snapshot_install_pending
            || status.replica_in_conf_state != Some(true)
            || status.applied_index < leader_commit
        {
            return Err(Error::ProposalUnavailable {
                reason: format!(
                    "joining replica {} is not locally caught up: applied={}, commit={}, snapshot_pending={}, in_conf_state={:?}",
                    identity.replica_id.0,
                    status.applied_index,
                    leader_commit,
                    status.snapshot_install_pending,
                    status.replica_in_conf_state,
                ),
            });
        }
        Ok(())
    }

    /// Reconcile local lifetimes whose removal has been durably authorized by
    /// metadata and by the recovered/current Raft ConfState.
    ///
    /// Desired-placement absence is deliberately insufficient: a stale
    /// placement can be observed before the corresponding committed
    /// membership removal. The terminal registry tombstone is published before
    /// route, runtime, snapshot, or retention cleanup so every crash point
    /// resumes from a permanent identity fence.
    fn reconcile_retired(
        &mut self,
        host: &mut MultiRaftHost<LocalWal>,
        metadata: &MetadataState,
        active: bool,
    ) -> Result<()> {
        for record in self.registry.records()? {
            let key = record.key();
            let identity = RaftReplicaIdentity::new(key.raft_group_id, key.replica_id)
                .map_err(|source| Error::Configuration(source.to_string()))?;

            if !metadata.is_replica_retired(key.raft_group_id, key.replica_id) {
                if record.lifecycle == ReplicaLifecycle::Tombstoned {
                    return Err(Error::RecoveryFailed {
                        reason: format!(
                            "local tombstone for replica {} of group {} has no metadata retirement authority",
                            key.replica_id.0, key.raft_group_id.0
                        ),
                    });
                }
                continue;
            }

            if record.lifecycle != ReplicaLifecycle::Tombstoned
                && !self.conf_state_proves_removed(identity)
            {
                // Membership removal may be committed in metadata before the
                // local Ready owner has published the matching ConfState.
                // Keep serving and retry; deleting without that proof would
                // strand a still-voting replica.
                continue;
            }

            if record.lifecycle != ReplicaLifecycle::Tombstoned {
                if let Some(bootstrap) = self.durable_bootstrap(identity)? {
                    let initial =
                        bootstrap
                            .to_core_conf_state()
                            .map_err(|source| Error::RecoveryFailed {
                                reason: source.to_string(),
                            })?;
                    self.registry.set_initial_configuration(
                        key,
                        InitialReplicaConfiguration::from_core(&initial)?,
                    )?;
                }
                self.registry.mark_destroying(key)?;
                // This write is the identity fence. It intentionally precedes
                // every cleanup operation below.
                self.registry.mark_tombstoned(key)?;
            }

            if active {
                // This helper also covers the restart case where cleanup had
                // already removed the in-memory reactor and WAL handle.
                self.ensure_tombstone_writer(host, identity, true)?;
                self.cleanup_retired_replica(record, identity)?;
                if self
                    .joins
                    .record(identity.raft_group_id, identity.replica_id)?
                    .is_some()
                {
                    self.joins.advance(
                        identity.raft_group_id,
                        identity.replica_id,
                        JoiningReplicaLifecycle::Retired,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn conf_state_proves_removed(&self, identity: RaftReplicaIdentity) -> bool {
        if let Some(runtime) = self.runtimes.get(&identity) {
            return runtime.handle().status().replica_in_conf_state == Some(false);
        }

        self.recovered
            .replica(identity)
            .and_then(|replica| replica.conf_state())
            .is_some_and(|conf_state| conf_state_excludes_replica(conf_state, identity.replica_id))
    }

    /// Remove in-process routes and local snapshot artifacts after the
    /// terminal tombstone has been durably published.
    fn cleanup_retired_replica(
        &mut self,
        record: LocalReplicaRecord,
        identity: RaftReplicaIdentity,
    ) -> Result<()> {
        let bootstrap = self.durable_bootstrap(identity)?;

        if let Some(bootstrap) = &bootstrap
            && self
                .registry
                .record(record.key())?
                .and_then(|record| record.initial_configuration)
                .is_none()
        {
            let initial =
                bootstrap
                    .to_core_conf_state()
                    .map_err(|source| Error::RecoveryFailed {
                        reason: source.to_string(),
                    })?;
            self.registry.set_initial_configuration(
                record.key(),
                InitialReplicaConfiguration::from_core(&initial)?,
            )?;
        }

        if let Some(bootstrap) = bootstrap {
            self.transport
                .unregister_group(&bootstrap)
                .map_err(|source| Error::RecoveryFailed {
                    reason: format!("unregister tablet Raft transport: {source}"),
                })?;
        }
        if let Some(join) = self
            .joins
            .record(identity.raft_group_id, identity.replica_id)?
        {
            self.transport
                .unregister_dynamic_route(
                    identity.raft_group_id,
                    identity.replica_id,
                    join.physical_node_id,
                )
                .map_err(|source| Error::RecoveryFailed {
                    reason: format!("unregister dynamic tablet Raft route: {source}"),
                })?;
        }
        self.snapshot_transport
            .unregister_group(identity.raft_group_id, identity.replica_id)
            .map_err(|source| Error::RecoveryFailed {
                reason: format!("unregister tablet snapshot route: {source}"),
            })?;

        // Detach the host proxy before dropping the runtime so no scheduler
        // turn can race with reactor shutdown.
        drop(self.runtimes.remove(&identity));
        self.tablet_handles
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&identity.raft_group_id);

        if let Some(mut writer) = self.writers.remove(&identity) {
            writer
                .prune_before(self.wal.durable_lsn())
                .map_err(|reason| Error::RecoveryFailed {
                    reason: format!("release tombstoned replica WAL retention: {reason}"),
                })?;
            writer
                .release_retention()
                .map_err(|reason| Error::RecoveryFailed {
                    reason: format!("remove tombstoned replica WAL retention: {reason}"),
                })?;
        }

        self.snapshot_store
            .remove_replica_state(record.raft_group_id, record.replica_id, record.tablet_id)
            .map_err(|source| Error::RecoveryFailed {
                reason: format!("remove tombstoned tablet snapshots: {source}"),
            })?;

        let bootstrap_store = FileBootstrapStore::open(self.config.data_dir.join("raft-bootstrap"))
            .map_err(|source| Error::RecoveryFailed {
                reason: source.to_string(),
            })?;
        bootstrap_store
            .remove_durable_bootstrap(identity.raft_group_id)
            .map_err(|source| Error::RecoveryFailed {
                reason: format!("remove tombstoned tablet bootstrap: {source}"),
            })?;
        Ok(())
    }

    fn durable_bootstrap(
        &self,
        identity: RaftReplicaIdentity,
    ) -> Result<Option<RaftGroupBootstrap>> {
        let store = FileBootstrapStore::open(self.config.data_dir.join("raft-bootstrap")).map_err(
            |source| Error::RecoveryFailed {
                reason: source.to_string(),
            },
        )?;
        load_durable_group_bootstrap(&store, identity.raft_group_id).map_err(|source| {
            Error::RecoveryFailed {
                reason: source.to_string(),
            }
        })
    }

    fn ensure_tombstone_writer(
        &mut self,
        host: &mut MultiRaftHost<LocalWal>,
        identity: RaftReplicaIdentity,
        active: bool,
    ) -> Result<()> {
        if let std::collections::btree_map::Entry::Vacant(entry) = self.writers.entry(identity) {
            let writer = if active {
                host.issue_group_writer_after_activation(identity)
                    .map_err(host_error)?
            } else {
                host.issue_group_writer(identity).map_err(host_error)?
            };
            entry.insert(writer);
        }
        if active {
            host.tombstone_group(identity).map_err(host_error)?;
        } else {
            host.register_tombstoned_identity(identity)
                .map_err(host_error)?;
        }
        Ok(())
    }

    /// Account tombstoned registry records that have no corresponding WAL
    /// records after an earlier cleanup. They still need a live admission
    /// fence and an identity-bound retention entry before activation.
    fn register_local_tombstones(
        &mut self,
        host: &mut MultiRaftHost<LocalWal>,
        metadata: &MetadataRuntimeHandle,
    ) -> Result<()> {
        let state = metadata.state_snapshot();
        for record in self.registry.records()? {
            if record.lifecycle != ReplicaLifecycle::Tombstoned {
                continue;
            }
            if !state.is_replica_retired(record.raft_group_id, record.replica_id) {
                return Err(Error::RecoveryFailed {
                    reason: format!(
                        "local tombstone for replica {} of group {} has no metadata retirement authority",
                        record.replica_id.0, record.raft_group_id.0
                    ),
                });
            }
            let identity = RaftReplicaIdentity::new(record.raft_group_id, record.replica_id)
                .map_err(|source| Error::Configuration(source.to_string()))?;
            self.ensure_tombstone_writer(host, identity, false)?;
        }
        Ok(())
    }

    /// Reject recovered lifetimes that cannot be explained by the committed
    /// metadata placement, an explicitly supported legacy group, or a durable
    /// retirement/tombstone record. Retired identities are registered as
    /// tombstones before activation so stale traffic is fenced during startup.
    fn register_unmaterialized_recovered(
        &mut self,
        host: &mut MultiRaftHost<LocalWal>,
        metadata: &MetadataRuntimeHandle,
        metadata_identity: RaftReplicaIdentity,
        legacy_identity: RaftReplicaIdentity,
    ) -> Result<()> {
        let metadata_state = metadata.state_snapshot();
        let materialized = self
            .runtimes
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let recovered_identities = self
            .recovered
            .replicas()
            .map(|(identity, _)| *identity)
            .collect::<Vec<_>>();
        for identity in recovered_identities {
            if identity == metadata_identity
                || identity == legacy_identity
                || materialized.contains(&identity)
            {
                continue;
            }

            let key = LocalReplicaKey {
                raft_group_id: identity.raft_group_id,
                replica_id: identity.replica_id,
            };
            if self.registry.record(key)?.is_some()
                && !metadata_state.is_replica_retired(key.raft_group_id, key.replica_id)
                && self.conf_state_proves_removed(identity)
            {
                // Removal may already be committed locally while the
                // metadata retirement command is still outstanding. Keep the
                // durable recovery image until another active metadata
                // replica publishes that exact proof.
                continue;
            }
            if let Some(record) = self.registry.record(key)?
                && matches!(
                    record.lifecycle,
                    ReplicaLifecycle::Active
                        | ReplicaLifecycle::Destroying
                        | ReplicaLifecycle::Tombstoned
                )
                && metadata_state.is_replica_retired(key.raft_group_id, key.replica_id)
            {
                if !self.conf_state_proves_removed(identity) {
                    return Err(Error::RecoveryFailed {
                        reason: format!(
                            "retired local replica {:?} has no durable ConfState removal proof",
                            identity
                        ),
                    });
                }
                if record.lifecycle != ReplicaLifecycle::Tombstoned {
                    self.registry.mark_destroying(key)?;
                    self.registry.mark_tombstoned(key)?;
                }
                self.ensure_tombstone_writer(host, identity, false)?;
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

/// Return whether a durable Raft configuration has completely removed a
/// replica, including the outgoing voter set used during joint consensus.
fn conf_state_excludes_replica(conf_state: &raft::types::ConfState, replica_id: ReplicaId) -> bool {
    let Ok(replica_id) = replica_id.to_raft() else {
        return false;
    };
    !conf_state.voters.contains(&replica_id)
        && !conf_state.learners.contains(&replica_id)
        && !conf_state.outgoing_voters.contains(&replica_id)
}

fn status_conf_state(status: &MultiRaftGroupStatus) -> Result<raft::types::ConfState> {
    let voters = status
        .voters
        .iter()
        .map(|replica_id| {
            replica_id
                .to_raft()
                .map_err(|reason| Error::CorruptData(reason.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    let learners = status
        .learners
        .iter()
        .map(|replica_id| {
            replica_id
                .to_raft()
                .map_err(|reason| Error::CorruptData(reason.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut state = raft::types::ConfState::new(
        status
            .conf_state_version
            .ok_or_else(|| Error::RecoveryFailed {
                reason: "group status has no ConfState version".to_string(),
            })?,
        voters,
        learners,
    )
    .map_err(|error| Error::CorruptData(format!("invalid group ConfState: {error:?}")))?;
    state.outgoing_voters = status
        .outgoing_voters
        .iter()
        .map(|replica_id| {
            replica_id
                .to_raft()
                .map_err(|reason| Error::CorruptData(reason.to_string()))
        })
        .collect::<Result<_>>()?;
    state
        .validate()
        .map_err(|error| Error::CorruptData(format!("invalid group ConfState: {error:?}")))?;
    Ok(state)
}

fn join_request_from_status(
    cluster_id: &str,
    descriptor: &TabletDescriptor,
    status: &MultiRaftGroupStatus,
    target_replica_id: ReplicaId,
    target_node_id: NodeId,
    require_caught_up: bool,
) -> ragnordb_common::proto::rpc::ReplicaJoinRequest {
    ragnordb_common::proto::rpc::ReplicaJoinRequest {
        rpc_attempt_id: None,
        cluster_id: cluster_id.to_string(),
        raft_group_id: Some(status.identity.raft_group_id.to_proto()),
        tablet_id: Some(descriptor.tablet_id.to_proto()),
        tablet_epoch: descriptor.tablet_epoch,
        replica_id: Some(target_replica_id.to_proto()),
        physical_node_id: Some(target_node_id.to_proto()),
        expected_current_conf_state_version: status.conf_state_version.unwrap_or_default(),
        committed_membership_version: status.conf_state_version.unwrap_or_default(),
        voters: status.voters.iter().map(ReplicaId::to_proto).collect(),
        learners: status.learners.iter().map(ReplicaId::to_proto).collect(),
        outgoing_voters: status
            .outgoing_voters
            .iter()
            .map(ReplicaId::to_proto)
            .collect(),
        leader_commit_index: if require_caught_up {
            status.commit_index
        } else {
            0
        },
        require_caught_up,
    }
}

fn join_request_from_record(
    record: &JoiningReplicaRecord,
) -> ragnordb_common::proto::rpc::ReplicaJoinRequest {
    ragnordb_common::proto::rpc::ReplicaJoinRequest {
        rpc_attempt_id: None,
        cluster_id: record.cluster_id.clone(),
        raft_group_id: Some(record.raft_group_id.to_proto()),
        tablet_id: Some(record.tablet_id.to_proto()),
        tablet_epoch: record.tablet_epoch,
        replica_id: Some(record.replica_id.to_proto()),
        physical_node_id: Some(record.physical_node_id.to_proto()),
        expected_current_conf_state_version: record.expected_current_conf_state_version,
        committed_membership_version: record.committed_membership_witness.version,
        voters: record
            .committed_membership_witness
            .voters
            .iter()
            .map(ReplicaId::to_proto)
            .collect(),
        learners: record
            .committed_membership_witness
            .learners
            .iter()
            .map(ReplicaId::to_proto)
            .collect(),
        outgoing_voters: record
            .committed_membership_witness
            .outgoing_voters
            .iter()
            .map(ReplicaId::to_proto)
            .collect(),
        leader_commit_index: 0,
        require_caught_up: false,
    }
}

/// Derive a stable metadata request identity for one drain phase. The
/// namespace is separate from client/admin requests; replaying the same phase
/// therefore hits metadata deduplication instead of creating another edit.
fn drain_request_id(
    node_id: NodeId,
    raft_group_id: RaftGroupId,
    source_replica_id: ReplicaId,
    phase: u8,
) -> RequestId {
    let sequence = raft_group_id
        .0
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .rotate_left(17)
        ^ source_replica_id.0.rotate_left(31)
        ^ u64::from(phase);
    RequestId {
        client_id: (0xD4A1_0000_0000_0000_u128 << 64) | u128::from(node_id.0),
        sequence: sequence.max(1),
        raft_group_id: METADATA_RAFT_GROUP_ID,
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

/// Client-side proposal boundary for metadata-owned control-plane operations.
///
/// The SQL executor never allocates a table identity and never writes through
/// the legacy catalog WAL when this client is installed. A request is accepted
/// only after the metadata host has correlated the committed Raft apply result.
#[derive(Clone)]
pub struct MetadataProposalClient {
    requests: mpsc::SyncSender<MetadataHostRequest>,
    metadata: MetadataRuntimeHandle,
    metadata_rpc: MetadataRpcClient,
    metadata_nodes: Vec<NodeId>,
    local_node_id: NodeId,
    host_wake: HostWake,
    admin_client_id: u128,
    next_admin_sequence: Arc<AtomicU64>,
}

impl MetadataProposalClient {
    fn new(
        requests: mpsc::SyncSender<MetadataHostRequest>,
        metadata: MetadataRuntimeHandle,
        metadata_rpc: MetadataRpcClient,
        metadata_nodes: Vec<NodeId>,
        local_node_id: NodeId,
        host_wake: HostWake,
    ) -> Self {
        let process_nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(1);
        let admin_client_id = process_nonce
            .wrapping_add(u128::from(local_node_id.0))
            .max(1);

        Self {
            requests,
            metadata,
            metadata_rpc,
            metadata_nodes,
            local_node_id,
            host_wake,
            admin_client_id,
            next_admin_sequence: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Submit one durable physical-node lifecycle transition.
    ///
    /// The request uses a process-scoped administrative identity for proposal
    /// correlation. The lifecycle itself remains idempotent in metadata, so a
    /// retry of the same target state cannot move a node backwards or replay a
    /// completed transition.
    pub fn set_node_lifecycle(
        &self,
        node_id: NodeId,
        lifecycle: NodeLifecycle,
        timeout: Duration,
    ) -> Result<MetadataApplyOutcome> {
        let sequence = self
            .next_admin_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                Error::InvalidArgument("administrative request sequence exhausted".into())
            })?;
        let request_id = RequestId {
            client_id: self.admin_client_id,
            sequence,
            raft_group_id: METADATA_RAFT_GROUP_ID,
        };

        self.propose_metadata_command(
            MetadataCommand::SetNodeLifecycle { node_id, lifecycle },
            request_id,
            None,
            timeout,
        )
    }

    /// Submit one desired-placement checkpoint through the same forwarding and
    /// durable metadata-apply path used by administrative lifecycle changes.
    /// The caller supplies a stable request identity so a retry after a
    /// timeout or process restart cannot create a second topology transition.
    pub fn set_desired_replica_placement(
        &self,
        placement: DesiredReplicaPlacement,
        request_id: RequestId,
        timeout: Duration,
    ) -> Result<MetadataApplyOutcome> {
        self.propose_metadata_command(
            MetadataCommand::SetDesiredReplicaPlacement(placement),
            request_id,
            None,
            timeout,
        )
    }

    /// Reserve a monotonically increasing timestamp frontier through the
    /// metadata Raft group. Completion means the reservation command was
    /// applied, not merely queued, so the returned interval survives restart.
    pub fn reserve_timestamps(
        &self,
        reserved_until: Timestamp,
        timeout: Duration,
    ) -> Result<MetadataApplyOutcome> {
        let sequence = self
            .next_admin_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                Error::InvalidArgument("timestamp reservation sequence exhausted".into())
            })?;
        let request_id = RequestId {
            client_id: self.admin_client_id,
            sequence,
            raft_group_id: METADATA_RAFT_GROUP_ID,
        };

        self.propose_metadata_command(
            MetadataCommand::ReserveTimestamps { reserved_until },
            request_id,
            None,
            timeout,
        )
    }

    /// Durably pin MVCC history for one active transaction or supported
    /// historical reader. The metadata apply result is the visibility boundary
    /// callers must cross before issuing reads at `protected_timestamp`.
    pub fn register_gc_protection(
        &self,
        owner_id: u128,
        protection_id: u128,
        protected_timestamp: Timestamp,
        lease_deadline_ms: u64,
        now_ms: u64,
        timeout: Duration,
    ) -> Result<MetadataApplyOutcome> {
        let request_id = self.next_metadata_admin_request_id()?;
        self.propose_metadata_command(
            MetadataCommand::RegisterGcProtection {
                owner_id,
                protection_id,
                protected_timestamp,
                lease_deadline_ms,
                now_ms,
            },
            request_id,
            None,
            timeout,
        )
    }

    /// Extend a live MVCC history pin without changing its protected
    /// timestamp. The catalog rejects terminal, expired, or regressed leases.
    pub fn renew_gc_protection(
        &self,
        owner_id: u128,
        protection_id: u128,
        lease_deadline_ms: u64,
        now_ms: u64,
        timeout: Duration,
    ) -> Result<MetadataApplyOutcome> {
        let request_id = self.next_metadata_admin_request_id()?;
        self.propose_metadata_command(
            MetadataCommand::RenewGcProtection {
                owner_id,
                protection_id,
                lease_deadline_ms,
                now_ms,
            },
            request_id,
            None,
            timeout,
        )
    }

    /// Release one durable history pin after its reader or transaction has
    /// finished. A process crash instead relies on the replicated lease expiry.
    pub fn release_gc_protection(
        &self,
        owner_id: u128,
        protection_id: u128,
        timeout: Duration,
    ) -> Result<MetadataApplyOutcome> {
        let request_id = self.next_metadata_admin_request_id()?;
        self.propose_metadata_command(
            MetadataCommand::ReleaseGcProtection {
                owner_id,
                protection_id,
            },
            request_id,
            None,
            timeout,
        )
    }

    /// Ask metadata to monotonically advance the global MVCC history boundary.
    /// Its state machine clamps the candidate against every protection live at
    /// `now_ms`; this command is independent of A-WAL retention.
    pub fn advance_gc_safe_point(
        &self,
        candidate: Timestamp,
        now_ms: u64,
        timeout: Duration,
    ) -> Result<MetadataApplyOutcome> {
        let request_id = self.next_metadata_admin_request_id()?;
        self.propose_metadata_command(
            MetadataCommand::AdvanceGcSafePoint { candidate, now_ms },
            request_id,
            None,
            timeout,
        )
    }

    fn next_metadata_admin_request_id(&self) -> Result<RequestId> {
        let sequence = self
            .next_admin_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                Error::InvalidArgument("administrative request sequence exhausted".into())
            })?;
        Ok(RequestId {
            client_id: self.admin_client_id,
            sequence,
            raft_group_id: METADATA_RAFT_GROUP_ID,
        })
    }

    /// Propose removal of one physical metadata member through the metadata
    /// leader. A draining node is commonly a follower, so this path first
    /// attempts the local Ready owner and then uses the authenticated metadata
    /// RPC route to try the other configured metadata members. The RPC only
    /// forwards a proposal request; the receiving host remains the sole owner
    /// allowed to append a Raft ConfChange.
    fn remove_metadata_replica(
        &self,
        replica_id: ReplicaId,
        expected_conf_state_version: u64,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::InvalidArgument("metadata removal deadline overflowed".into()))?;
        let local_result =
            self.propose_local_conf_change(replica_id, expected_conf_state_version, deadline);
        match local_result {
            Ok(()) => Ok(()),
            Err(local_error) if metadata_error_can_forward(&local_error) => {
                let mut last_error = local_error;
                for target in self
                    .metadata_nodes
                    .iter()
                    .copied()
                    .filter(|target| *target != self.local_node_id)
                {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    let response = self.metadata_rpc.propose_conf_change(
                        target,
                        expected_conf_state_version,
                        replica_id,
                        remaining,
                    );
                    match response.and_then(metadata_conf_change_response_to_result) {
                        Ok(()) => return Ok(()),
                        Err(error) => last_error = error,
                    }
                }
                Err(last_error)
            }
            Err(error) => Err(error),
        }
    }

    fn propose_local_conf_change(
        &self,
        replica_id: ReplicaId,
        expected_conf_state_version: u64,
        deadline: Instant,
    ) -> Result<()> {
        let (reply, response) = mpsc::channel();
        self.requests
            .try_send(MetadataHostRequest::ConfChange {
                expected_conf_state_version,
                replica_id,
                reply,
            })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => Error::ProposalUnavailable {
                    reason: "metadata ConfChange queue is full".to_string(),
                },
                mpsc::TrySendError::Disconnected(_) => Error::ProposalUnavailable {
                    reason: "metadata Raft host is not running".to_string(),
                },
            })?;
        self.host_wake.wake();
        let remaining = deadline.saturating_duration_since(Instant::now());
        response
            .recv_timeout(remaining)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => Error::ProposalUnavailable {
                    reason: "metadata ConfChange deadline elapsed before proposal".to_string(),
                },
                mpsc::RecvTimeoutError::Disconnected => Error::ProposalUnavailable {
                    reason: "metadata Raft host stopped before ConfChange proposal".to_string(),
                },
            })?
    }

    fn table_topology_for_outcome(
        &self,
        outcome: MetadataApplyOutcome,
        timeout: Duration,
    ) -> Result<MetadataTableTopology> {
        let MetadataApplyOutcome::TableCreated(created) = outcome else {
            return match outcome {
                MetadataApplyOutcome::Rejected(rejection) => {
                    Err(Error::ConstraintViolation(rejection.to_string()))
                }

                MetadataApplyOutcome::Applied
                | MetadataApplyOutcome::AlreadyApplied
                | MetadataApplyOutcome::ClientRegistered { .. }
                | MetadataApplyOutcome::ClientRenewed
                | MetadataApplyOutcome::TimestampsReserved { .. } => Err(Error::CorruptData(
                    "metadata CREATE TABLE apply did not return allocated topology".to_string(),
                )),

                MetadataApplyOutcome::TableCreated(_) => unreachable!(),
            };
        };

        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            Error::InvalidArgument("metadata topology deadline overflowed".into())
        })?;
        let state = loop {
            let state = self.metadata.state_snapshot();
            if state.table(created.table_id).is_some() && state.tablet(created.tablet_id).is_some()
            {
                break state;
            }
            if Instant::now() >= deadline {
                return Err(Error::ProposalUnavailable {
                    reason: format!(
                        "metadata topology {} was committed remotely but is not visible locally",
                        created.table_id.0
                    ),
                });
            }
            thread::sleep(Duration::from_millis(2));
        };
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
        if tablets.is_empty() {
            return Err(Error::CorruptData(format!(
                "metadata table {} exposes no tablet descriptors",
                created.table_id.0,
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
        logical_request_id: Option<ClientRequestId>,
        timeout: Duration,
    ) -> Result<MetadataTableTopology> {
        self.propose_metadata_command(
            MetadataCommand::CreateTableTopology(request),
            request_id,
            logical_request_id,
            timeout,
        )
        .and_then(|outcome| self.table_topology_for_outcome(outcome, timeout))
    }

    fn propose_metadata_command(
        &self,
        command: MetadataCommand,
        request_id: RequestId,
        logical_request_id: Option<ClientRequestId>,
        timeout: Duration,
    ) -> Result<MetadataApplyOutcome> {
        let envelope = match logical_request_id {
            Some(client_request_id) => MetadataCommandEnvelope::new_with_logical_command_id(
                request_id.clone(),
                LogicalCommandId {
                    client_request_id,
                    command_ordinal: 1,
                    kind: CommandKind::Catalog,
                },
                command,
            ),
            None => MetadataCommandEnvelope::new(request_id.clone(), command),
        }
        .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::InvalidArgument("metadata request deadline overflowed".into()))?;
        let envelope_bytes = envelope
            .encode()
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        let mut last_error = None;
        while !deadline.saturating_duration_since(Instant::now()).is_zero() {
            match self.propose_local(envelope.clone(), deadline) {
                Ok(outcome) => return Ok(outcome),
                Err(error) if metadata_error_can_forward(&error) => last_error = Some(error),
                Err(error) => return Err(error),
            }

            for target in self
                .metadata_nodes
                .iter()
                .copied()
                .filter(|target| *target != self.local_node_id)
            {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let response = self.metadata_rpc.propose(
                    target,
                    METADATA_RAFT_GROUP_ID,
                    ragnordb_common::rpc_codec::MetadataProposalRequest {
                        request_id: request_id.clone(),
                        command_envelope: envelope_bytes.clone(),
                    },
                    remaining,
                );
                match response.and_then(metadata_response_to_outcome) {
                    Ok(outcome) => return Ok(outcome),
                    Err(error) if metadata_error_can_forward(&error) => last_error = Some(error),
                    Err(error) => return Err(error),
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if !remaining.is_zero() {
                thread::sleep(Duration::from_millis(2).min(remaining));
            }
        }

        Err(last_error.unwrap_or_else(|| Error::ProposalUnavailable {
            reason: "metadata proposal deadline elapsed".to_string(),
        }))
    }

    fn propose_local(
        &self,
        envelope: MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataApplyOutcome> {
        let (reply, response) = mpsc::channel();
        self.requests
            .try_send(MetadataHostRequest::Command {
                envelope: Box::new(envelope),
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
        self.host_wake.wake();

        let remaining = deadline.saturating_duration_since(Instant::now());
        response
            .recv_timeout(remaining)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => Error::ProposalUnavailable {
                    reason: "metadata proposal deadline elapsed before apply".to_string(),
                },
                mpsc::RecvTimeoutError::Disconnected => Error::ProposalUnavailable {
                    reason: "metadata Raft host stopped before proposal applied".to_string(),
                },
            })?
    }
}

/// Synchronous adapter used by the SQL transaction manager. The manager holds
/// its own mutex while reserving a range, so this call must not depend on a
/// callback from SQL or on the database mutex that owns the manager.
#[derive(Clone)]
pub struct MetadataTimestampReservationClient {
    metadata: MetadataProposalClient,
    timeout: Duration,
}

impl MetadataTimestampReservationClient {
    pub fn new(metadata: MetadataProposalClient, timeout: Duration) -> Self {
        Self { metadata, timeout }
    }
}

impl TimestampReservationProvider for MetadataTimestampReservationClient {
    fn reserve_timestamps(&mut self, requested_until: Timestamp) -> Result<TimestampReservation> {
        let mut requested_until = requested_until;
        for _ in 0..8 {
            match self
                .metadata
                .reserve_timestamps(requested_until, self.timeout)?
            {
                MetadataApplyOutcome::TimestampsReserved {
                    reserved_from,
                    reserved_until,
                } => {
                    return Ok(TimestampReservation {
                        reserved_from,
                        reserved_until,
                    });
                }
                MetadataApplyOutcome::Rejected(
                    ragnordb_catalog::MetadataRejection::TimestampReservationRegressed {
                        current,
                        ..
                    },
                ) => {
                    requested_until = Timestamp(current.0.checked_add(1).ok_or_else(|| {
                        Error::Configuration(
                            "timestamp reservation frontier is exhausted".to_string(),
                        )
                    })?);
                }
                MetadataApplyOutcome::Rejected(rejection) => {
                    return Err(Error::ConstraintViolation(rejection.to_string()));
                }
                other => {
                    return Err(Error::CorruptData(format!(
                        "timestamp reservation returned unexpected metadata outcome {other:?}"
                    )));
                }
            }
        }
        Err(Error::ProposalUnavailable {
            reason: "timestamp reservation conflicted repeatedly with concurrent owners"
                .to_string(),
        })
    }
}

fn metadata_error_can_forward(error: &Error) -> bool {
    matches!(
        error,
        Error::NotLeader { .. }
            | Error::LeaderUnknown
            | Error::ProposalUnavailable { .. }
            | Error::TabletUnavailable { .. }
    )
}

fn metadata_response_to_outcome(
    response: ragnordb_common::rpc_codec::MetadataResponse,
) -> Result<MetadataApplyOutcome> {
    let ragnordb_common::rpc_codec::MetadataResponse::ProposeCommand {
        success,
        error_code,
        error_message,
        outcome,
        ..
    } = response
    else {
        return Err(Error::CorruptData(
            "metadata RPC returned a non-proposal response".to_string(),
        ));
    };
    if !success {
        return match error_code.as_str() {
            "NOT_LEADER" => Err(Error::NotLeader { leader_id: None }),
            "RECOVERY_REQUIRED" => Err(Error::RecoveryRequired {
                reason: error_message,
            }),
            "METADATA_REJECTED" => Err(Error::ConstraintViolation(error_message)),
            _ => Err(Error::ProposalUnavailable {
                reason: error_message,
            }),
        };
    }
    match outcome.ok_or_else(|| Error::CorruptData("metadata RPC omitted outcome".to_string()))? {
        ragnordb_common::rpc_codec::MetadataProposalOutcome::Applied => {
            Ok(MetadataApplyOutcome::Applied)
        }
        ragnordb_common::rpc_codec::MetadataProposalOutcome::AlreadyApplied => {
            Ok(MetadataApplyOutcome::AlreadyApplied)
        }
        ragnordb_common::rpc_codec::MetadataProposalOutcome::ClientRegistered { session_epoch } => {
            Ok(MetadataApplyOutcome::ClientRegistered {
                client_id: 0,
                session_epoch,
            })
        }
        ragnordb_common::rpc_codec::MetadataProposalOutcome::ClientRenewed => {
            Ok(MetadataApplyOutcome::ClientRenewed)
        }
        ragnordb_common::rpc_codec::MetadataProposalOutcome::TimestampsReserved {
            reserved_from,
            reserved_until,
        } => Ok(MetadataApplyOutcome::TimestampsReserved {
            reserved_from,
            reserved_until,
        }),
        ragnordb_common::rpc_codec::MetadataProposalOutcome::TimestampReservationRegressed {
            current,
            received,
        } => Ok(MetadataApplyOutcome::Rejected(
            ragnordb_catalog::MetadataRejection::TimestampReservationRegressed {
                current,
                received,
            },
        )),
        ragnordb_common::rpc_codec::MetadataProposalOutcome::TableCreated {
            table_id,
            tablet_id,
            raft_group_id,
        } => Ok(MetadataApplyOutcome::TableCreated(
            ragnordb_catalog::MetadataTableCreated {
                table_id,
                tablet_id,
                raft_group_id,
            },
        )),
        ragnordb_common::rpc_codec::MetadataProposalOutcome::Rejected { reason } => {
            Err(Error::ConstraintViolation(reason))
        }
    }
}

fn metadata_conf_change_response_to_result(
    response: ragnordb_common::rpc_codec::MetadataResponse,
) -> Result<()> {
    let ragnordb_common::rpc_codec::MetadataResponse::ProposeConfChange {
        success,
        error_code,
        error_message,
        leader_replica_id,
    } = response
    else {
        return Err(Error::CorruptData(
            "metadata RPC returned a non-ConfChange response".to_string(),
        ));
    };
    if success {
        return Ok(());
    }
    match error_code.as_str() {
        "NOT_LEADER" => Err(Error::NotLeader {
            leader_id: leader_replica_id.map(|replica_id| replica_id.0),
        }),
        "RECOVERY_REQUIRED" => Err(Error::RecoveryRequired {
            reason: error_message,
        }),
        _ => Err(Error::ProposalUnavailable {
            reason: error_message,
        }),
    }
}

impl MetadataTableCreator for MetadataProposalClient {
    fn metadata_generation(&self) -> Option<u64> {
        Some(self.metadata.state_snapshot_with_generation().0)
    }

    fn create_table(
        &self,
        request: CreateTableRequest,
        request_id: RequestId,
        timeout: Duration,
    ) -> Result<ragnordb_common::catalog_codec::TableDefinition> {
        self.propose_create_table_topology(request, request_id, None, timeout)
            .map(|topology| topology.definition)
    }

    fn table_descriptors(
        &self,
        table_id: ragnordb_common::ids::TableId,
    ) -> Result<Vec<TabletDescriptor>> {
        let state = self.metadata.state_snapshot();
        if state.table(table_id).is_none() {
            return Err(Error::CorruptData(format!(
                "metadata table {} is missing from the committed state",
                table_id.0
            )));
        }
        let tablets = state.tablets_for_table(table_id);

        if tablets.is_empty() {
            return Err(Error::CorruptData(format!(
                "metadata table {} exposes no tablet descriptors",
                table_id.0,
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
        self.propose_create_table_topology(request, request_id, None, timeout)
    }

    fn create_table_topology_with_identity(
        &self,
        request: CreateTableRequest,
        request_id: RequestId,
        logical_request_id: Option<ClientRequestId>,
        timeout: Duration,
    ) -> Result<MetadataTableTopology> {
        self.propose_create_table_topology(request, request_id, logical_request_id, timeout)
    }

    fn register_client(
        &self,
        request_id: RequestId,
        client_id: u128,
        requested_session_epoch: u64,
        timeout: Duration,
    ) -> Result<u64> {
        let request_sequence = request_id.sequence;
        let logical_client_id = request_id.client_id;
        let outcome = self.propose_metadata_command(
            MetadataCommand::RegisterClient {
                client_id,
                requested_session_epoch,
            },
            request_id,
            Some(ClientRequestId {
                // Registration uses a control-plane RequestId namespace so it
                // cannot collide with the SQL command carrying the same root
                // sequence on a fresh V2 connection.
                client_id: logical_client_id,
                session_epoch: requested_session_epoch.max(1),
                request_sequence,
            }),
            timeout,
        )?;
        match outcome {
            MetadataApplyOutcome::ClientRegistered { session_epoch, .. } => Ok(session_epoch),
            MetadataApplyOutcome::AlreadyApplied => self
                .metadata
                .state_snapshot()
                .client_session(client_id)
                .map(|session| session.session_epoch)
                .ok_or_else(|| Error::CorruptData("registered client session disappeared".into())),
            MetadataApplyOutcome::Rejected(rejection) => {
                Err(Error::ConstraintViolation(rejection.to_string()))
            }
            other => Err(Error::CorruptData(format!(
                "metadata client registration returned unexpected outcome {other:?}"
            ))),
        }
    }

    fn renew_client(
        &self,
        request_id: RequestId,
        client_id: u128,
        session_epoch: u64,
        acknowledged_through: u64,
        timeout: Duration,
    ) -> Result<()> {
        let request_sequence = acknowledged_through.max(1);
        let logical_client_id = request_id.client_id;
        let outcome = self.propose_metadata_command(
            MetadataCommand::RenewClient {
                client_id,
                session_epoch,
                acknowledged_through,
            },
            request_id,
            Some(ClientRequestId {
                client_id: logical_client_id,
                session_epoch,
                request_sequence,
            }),
            timeout,
        )?;
        match outcome {
            MetadataApplyOutcome::Applied
            | MetadataApplyOutcome::AlreadyApplied
            | MetadataApplyOutcome::ClientRenewed => Ok(()),
            MetadataApplyOutcome::Rejected(rejection) => {
                Err(Error::ConstraintViolation(rejection.to_string()))
            }
            other => Err(Error::CorruptData(format!(
                "metadata client renewal returned unexpected outcome {other:?}"
            ))),
        }
    }

    fn active_client_session_epoch(&self, client_id: u128) -> Result<Option<u64>> {
        Ok(self
            .metadata
            .state_snapshot()
            .client_session(client_id)
            .map(|session| session.session_epoch))
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

    /// Fixed ownership reactors outlive every tablet runtime registered on
    /// them and are dropped only after the host has stopped issuing work.
    _reactors: Arc<FixedReactorSet>,

    metadata: MetadataRuntimeHandle,

    metadata_creator: SharedMetadataTableCreator,

    metadata_control: MetadataProposalClient,

    host_status: SharedMultiRaftHostStatus,

    /// Explicitly retain ownership of the one physical snapshot transport for
    /// the complete node runtime lifetime.
    _snapshot_transport: NodeSnapshotTransport,

    /// Node-level tablet gateway client. The dispatcher owns the receive lane
    /// and correlates remote responses by RequestId.
    tablet_rpc: TabletRpcClient,

    rpc_worker: Option<thread::JoinHandle<()>>,

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
        let registry = config
            .cluster_id
            .as_deref()
            .map(|cluster_id| {
                LocalReplicaRegistry::open(
                    config.data_dir.join("replica-registry.json"),
                    cluster_id,
                )
            })
            .transpose()?;

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

        // A tombstoned group may have already removed its bootstrap file. In
        // that case the registry's immutable initial-membership witness is the
        // only safe source for replaying retained configuration entries.
        if let Some(registry) = registry {
            for record in registry.records()? {
                let Some(initial) = record.initial_configuration else {
                    continue;
                };
                let identity = RaftReplicaIdentity::new(record.raft_group_id, record.replica_id)
                    .map_err(|source| Error::RecoveryFailed {
                        reason: source.to_string(),
                    })?;
                let conf_state = initial.to_core()?;
                if let Some(existing) = configurations.get(&identity)
                    && existing != &conf_state
                {
                    return Err(Error::RecoveryFailed {
                        reason: format!(
                            "registry initial configuration for {:?} conflicts with durable bootstrap",
                            identity
                        ),
                    });
                }
                configurations.insert(identity, conf_state);
            }
        }

        // Dynamic joiners do not have an immutable bootstrap file on the
        // target node. Their durable membership witness is the only safe
        // initial configuration for reconstructing any WAL written before a
        // crash during catch-up.
        if let Some(cluster_id) = config.cluster_id.as_deref() {
            let joins = JoiningReplicaRegistry::open(
                config.data_dir.join("replica-join-registry.json"),
                cluster_id,
            )?;
            for record in joins.records()? {
                let identity = RaftReplicaIdentity::new(record.raft_group_id, record.replica_id)
                    .map_err(|source| Error::RecoveryFailed {
                        reason: source.to_string(),
                    })?;
                let conf_state = record.committed_membership_witness.to_core()?;
                if let Some(existing) = configurations.get(&identity)
                    && existing != &conf_state
                {
                    return Err(Error::RecoveryFailed {
                        reason: format!(
                            "joining membership witness for {:?} conflicts with another recovery authority",
                            identity
                        ),
                    });
                }
                configurations.insert(identity, conf_state);
            }
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
        let joins = JoiningReplicaRegistry::open(
            config.data_dir.join("replica-join-registry.json"),
            &cluster_id,
        )?;
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
            rpc_inbound,
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

        let reactors = FixedReactorSet::new(config.reactor_count)?;

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
            reactors.clone(),
        )?;

        let hosted_group = Box::new(tablet_runtime.hosted_group());

        if recovered.replica(identity).is_some() {
            host.register_recovered_group(hosted_group)
                .map_err(host_error)?;
        } else {
            host.register_new_group(hosted_group).map_err(host_error)?;
        }

        let tablet_handles = Arc::new(RwLock::new(BTreeMap::new()));
        tablet_handles
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(identity.raft_group_id, tablet_runtime.handle());

        let mut tablet_lifecycle = TabletLifecycleManager::new(
            config.clone(),
            wal,
            database.clone(),
            transport.clone(),
            snapshot_store,
            snapshot_work,
            snapshot_transport.clone(),
            registry,
            joins,
            recovered,
            Arc::clone(&start_gate),
            reactors.clone(),
            tablet_handles.clone(),
        )?;
        tablet_lifecycle.reconcile(&mut host, &metadata_handle, false)?;
        tablet_lifecycle.register_unmaterialized_recovered(
            &mut host,
            &metadata_handle,
            metadata_identity,
            identity,
        )?;
        tablet_lifecycle.register_local_tombstones(&mut host, &metadata_handle)?;

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

        // Tablet reactors may now release any Ready-dependent messages.
        start_gate.store(true, Ordering::Release);
        reactors.wake_all();

        let shutdown = Arc::new(AtomicBool::new(false));
        let host_wake = HostWake::new();

        let (metadata_request_tx, metadata_request_rx) =
            mpsc::sync_channel(METADATA_REQUEST_CHANNEL_CAPACITY);
        let rpc_state = RpcState::new();
        let metadata_rpc = MetadataRpcClient::new(transport.clone(), rpc_state.clone());
        let metadata_proposal = MetadataProposalClient::new(
            metadata_request_tx.clone(),
            metadata_handle.clone(),
            metadata_rpc,
            metadata_nodes.iter().map(|node| node.node_id).collect(),
            config.node_id,
            host_wake.clone(),
        );
        tablet_lifecycle.install_metadata_control(metadata_proposal.clone());
        let metadata_creator: SharedMetadataTableCreator = Arc::new(metadata_proposal.clone());

        let worker_shutdown = Arc::clone(&shutdown);

        let (join_request_tx, join_request_rx) =
            mpsc::sync_channel(REPLICA_JOIN_REQUEST_CHANNEL_CAPACITY);
        let replica_join_rpc = ReplicaJoinRpcClient::new(transport.clone(), rpc_state.clone());

        let (tablet_rpc, rpc_worker) = spawn_dispatcher(
            rpc_inbound,
            transport.clone(),
            rpc_state,
            tablet_handles,
            metadata_handle.clone(),
            metadata_request_tx,
            join_request_tx,
            host_wake.clone(),
            shutdown.clone(),
        );

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
                    metadata_bootstrap.replica_to_node.clone(),
                    metadata_ready_tx,
                    tablet_lifecycle,
                    join_request_rx,
                    replica_join_rpc,
                    host_wake,
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

            _reactors: reactors,

            metadata: metadata_handle,

            metadata_creator,

            metadata_control: metadata_proposal,

            host_status,

            _snapshot_transport: snapshot_transport,

            tablet_rpc,

            rpc_worker: Some(rpc_worker),

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

    /// Return the local/remote tablet gateway client used by SQL routing.
    pub fn tablet_rpc_client(&self) -> TabletRpcClient {
        self.tablet_rpc.clone()
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

    /// Return the metadata proposal boundary used by administrative lifecycle
    /// controls. The returned client shares only channels and read-only
    /// publications; the host thread remains the sole Raft owner.
    pub fn metadata_control(&self) -> MetadataProposalClient {
        self.metadata_control.clone()
    }

    /// Return the last published point-in-time status for every local Raft
    /// group. Each tablet reactor remains the sole owner of its mutable state.
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
    metadata_replica_to_node: BTreeMap<ReplicaId, NodeId>,
    metadata_ready: mpsc::SyncSender<std::result::Result<(), String>>,
    mut tablet_lifecycle: TabletLifecycleManager,
    join_requests: mpsc::Receiver<ReplicaJoinAdmission>,
    replica_join_rpc: ReplicaJoinRpcClient,
    host_wake: HostWake,
) {
    host_wake.bind_current_thread();
    inbound.bind_current_thread();
    let mut timer_clock = SparseTickClock::new(TICK_INTERVAL);

    let mut pending_metadata = BTreeMap::<u64, PendingMetadataProposal>::new();
    let mut pending_metadata_by_request = HashMap::<RequestId, u64>::new();

    host.schedule_all_groups_after(1)
        .expect("active MultiRaft host must accept startup timer scheduling");
    publish_host_status(&host_status, &host, pending_metadata.len());

    let mut next_metadata_attempt = Instant::now();

    let mut startup_sender = Some(metadata_ready);

    while !shutdown.load(Ordering::Acquire) {
        if let Err(error) = reconcile_draining_leadership(
            &mut host,
            &transport,
            &metadata,
            &metadata_replica_to_node,
        ) {
            publish_host_status(&host_status, &host, pending_metadata.len());
            signal_metadata_failure(&mut startup_sender, error.to_string());
            fail_pending_metadata(
                &mut pending_metadata,
                &mut pending_metadata_by_request,
                error,
            );
            tracing::error!("draining-node leadership handoff failed");
            return;
        }

        let mut admitted_joins = 0;
        while admitted_joins < REPLICA_JOIN_REQUEST_CHANNEL_CAPACITY {
            let Ok(admission) = join_requests.try_recv() else {
                break;
            };
            admitted_joins += 1;
            let result = tablet_lifecycle.admit_replica_join(
                &mut host,
                &metadata,
                admission.source_node_id,
                &admission.request,
                true,
                false,
            );
            let response = match result {
                Ok(()) => ReplicaJoinAdmissionResult {
                    success: true,
                    error_message: String::new(),
                },
                Err(error) => ReplicaJoinAdmissionResult {
                    success: false,
                    error_message: error.to_string(),
                },
            };
            let _ = admission.reply.send(response);
        }

        if !service_metadata_requests(
            &mut host,
            &transport,
            &metadata_requests,
            &mut pending_metadata,
            &mut pending_metadata_by_request,
            METADATA_REQUEST_BUDGET,
        ) {
            publish_host_status(&host_status, &host, pending_metadata.len());
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
                    publish_host_status(&host_status, &host, pending_metadata.len());
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

        let elapsed_ticks = timer_clock.take_elapsed_ticks(Instant::now());
        match host.run_turn(
            elapsed_ticks,
            MultiRaftTurnBudget {
                max_groups: HOST_GROUP_BUDGET,
                max_messages: HOST_MESSAGE_BUDGET,
                ..MultiRaftTurnBudget::default()
            },
        ) {
            Ok(turn) => send_outbound(&transport, turn.outbound),

            Err(MultiRaftHostError::RecoveryRequired) => {
                publish_host_status(&host_status, &host, pending_metadata.len());
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

        drain_metadata_results(
            &metadata,
            &mut pending_metadata,
            &mut pending_metadata_by_request,
        );
        if let Err(error) = tablet_lifecycle.reconcile(&mut host, &metadata, true) {
            publish_host_status(&host_status, &host, pending_metadata.len());
            signal_metadata_failure(&mut startup_sender, error.to_string());
            fail_pending_metadata(
                &mut pending_metadata,
                &mut pending_metadata_by_request,
                error,
            );
            tracing::error!("metadata tablet lifecycle reconciliation failed");
            return;
        }
        if let Err(error) = tablet_lifecycle.reconcile_membership(
            &mut host,
            &metadata,
            &replica_join_rpc,
            &metadata_replica_to_node,
        ) {
            tracing::warn!(error = %error, "membership reconciliation pass failed; retrying");
        }
        if let Err(error) = tablet_lifecycle.reconcile_retirement_records(&mut host, &metadata) {
            tracing::warn!(error = %error, "replica retirement publication pass failed; retrying");
        }
        if let Err(error) =
            tablet_lifecycle.reconcile_node_drain(&mut host, &metadata, &metadata_replica_to_node)
        {
            tracing::warn!(error = %error, "node-drain orchestration pass failed; retrying");
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

        publish_host_status(&host_status, &host, pending_metadata.len());

        let wait = if host.has_runnable_work() {
            Duration::ZERO
        } else {
            let timer_wait = host
                .next_timer_delay_ticks()
                .map(|ticks| timer_clock.duration_until_ticks(ticks.max(1)))
                .unwrap_or_else(|| Duration::from_secs(1));
            timer_wait.min(next_metadata_attempt.saturating_duration_since(now))
        };

        if wait.is_zero() {
            thread::yield_now();
        } else {
            thread::park_timeout(wait);
        }
    }

    graceful_leadership_handoff(
        &mut host,
        &transport,
        &inbound,
        &metadata,
        &metadata_replica_to_node,
    );

    publish_host_status(&host_status, &host, pending_metadata.len());
}

/// Select the smallest eligible replica ID so transfer decisions are stable
/// across host turns. Eligibility is deliberately stricter than the Raft
/// core's final validation: the scheduler only asks a current, non-joint
/// voter on an Active physical node that already advertises the leader's
/// complete log frontier.
struct TransferTargetState<'a> {
    leader_replica_id: Option<ReplicaId>,
    voters: &'a [ReplicaId],
    learners: &'a [ReplicaId],
    outgoing_voters: &'a [ReplicaId],
    replica_match_indices: &'a [(ReplicaId, u64)],
    last_log_index: u64,
}

fn select_transfer_target(
    state: TransferTargetState<'_>,
    replica_to_node: &BTreeMap<ReplicaId, NodeId>,
    active_nodes: &BTreeSet<NodeId>,
) -> Option<ReplicaId> {
    state
        .voters
        .iter()
        .copied()
        .filter(|replica_id| Some(*replica_id) != state.leader_replica_id)
        .filter(|replica_id| !state.learners.contains(replica_id))
        .filter(|replica_id| !state.outgoing_voters.contains(replica_id))
        .filter(|replica_id| {
            state
                .replica_match_indices
                .iter()
                .any(|(candidate, match_index)| {
                    candidate == replica_id && *match_index >= state.last_log_index
                })
        })
        .filter(|replica_id| {
            replica_to_node
                .get(replica_id)
                .is_some_and(|node_id| active_nodes.contains(node_id))
        })
        .min()
}

fn active_node_ids(state: &MetadataState) -> BTreeSet<NodeId> {
    state
        .nodes()
        .filter(|node| node.lifecycle == NodeLifecycle::Active)
        .map(|node| node.node_id)
        .collect()
}

/// Resolve the physical placement authority used to choose a transfer target.
/// Metadata-group membership comes from its durable bootstrap; tablet-group
/// membership comes from the committed desired placement. Neither mapping is
/// inferred from a NodeId because replica IDs are independently allocated.
fn replica_routes_for_group(
    state: &MetadataState,
    raft_group_id: RaftGroupId,
    metadata_replica_to_node: &BTreeMap<ReplicaId, NodeId>,
) -> BTreeMap<ReplicaId, NodeId> {
    if raft_group_id == METADATA_RAFT_GROUP_ID {
        return metadata_replica_to_node.clone();
    }

    state
        .tablet_for_raft_group(raft_group_id)
        .and_then(|tablet| state.desired_placement(tablet.tablet_id))
        .map(|placement| {
            placement
                .replicas
                .iter()
                .map(|replica| (replica.replica_id, replica.node_id))
                .collect()
        })
        .unwrap_or_default()
}

/// Initiate one bounded transfer and release any targeted control message
/// through the normal physical-node transport. Retryable outcomes are
/// expected while a target is catching up or a previous transfer is active;
/// shared-WAL uncertainty remains fatal and is propagated to the host owner.
fn request_leadership_transfer(
    host: &mut MultiRaftHost<LocalWal>,
    transport: &NodeRaftTransport,
    group_status: &MultiRaftGroupStatus,
    replica_to_node: &BTreeMap<ReplicaId, NodeId>,
    active_nodes: &BTreeSet<NodeId>,
    reason: &str,
) -> Result<()> {
    let Some(target) = select_transfer_target(
        TransferTargetState {
            leader_replica_id: group_status.leader_replica_id,
            voters: &group_status.voters,
            learners: &group_status.learners,
            outgoing_voters: &group_status.outgoing_voters,
            replica_match_indices: &group_status.replica_match_indices,
            last_log_index: group_status.last_log_index,
        },
        replica_to_node,
        active_nodes,
    ) else {
        tracing::debug!(
            raft_group_id = group_status.identity.raft_group_id.0,
            reason,
            "no eligible voter is ready for leadership transfer",
        );
        return Ok(());
    };

    match host.transfer_leadership(
        group_status.identity.raft_group_id,
        target,
        LEADERSHIP_TRANSFER_TIMEOUT_TICKS,
    ) {
        Ok(transfer) => {
            send_outbound(transport, transfer.outbound);
            tracing::info!(
                raft_group_id = group_status.identity.raft_group_id.0,
                target_replica_id = target.0,
                reason,
                "leadership transfer requested",
            );
        }

        Err(MultiRaftHostError::GroupRejected { .. })
        | Err(MultiRaftHostError::GroupRetryable { .. }) => {
            tracing::debug!(
                raft_group_id = group_status.identity.raft_group_id.0,
                target_replica_id = target.0,
                reason,
                "leadership transfer will be retried",
            );
        }

        Err(error) => return Err(host_error(error)),
    }

    Ok(())
}

/// Keep a node that metadata has moved beyond Active from retaining
/// leadership. The check runs before request admission so the transfer fence
/// is installed before another metadata or tablet proposal can be accepted by
/// the local host.
fn reconcile_draining_leadership(
    host: &mut MultiRaftHost<LocalWal>,
    transport: &NodeRaftTransport,
    metadata: &MetadataRuntimeHandle,
    metadata_replica_to_node: &BTreeMap<ReplicaId, NodeId>,
) -> Result<()> {
    let node_id = host.node_id();
    let Some(node) = metadata.node(node_id) else {
        return Ok(());
    };
    if node.lifecycle == NodeLifecycle::Active {
        return Ok(());
    }

    let state = metadata.state_snapshot();
    let active_nodes = active_node_ids(&state);
    for group_status in host.status().groups {
        if group_status.role != Some(ragnordb_multiraft::host::MultiRaftRole::Leader) {
            continue;
        }

        let routes = replica_routes_for_group(
            &state,
            group_status.identity.raft_group_id,
            metadata_replica_to_node,
        );
        request_leadership_transfer(
            host,
            transport,
            &group_status,
            &routes,
            &active_nodes,
            "node lifecycle is no longer Active",
        )?;
    }

    Ok(())
}

fn drain_shutdown_messages(
    host: &mut MultiRaftHost<LocalWal>,
    inbound: &NodeRaftInbound,
) -> Result<()> {
    let mut admitted_messages = 0;
    while admitted_messages < HOST_MESSAGE_BUDGET {
        match inbound.try_recv() {
            Ok(message) => match host.enqueue_message(message) {
                Ok(()) => admitted_messages += 1,
                Err(MultiRaftHostError::RecoveryRequired) => {
                    return Err(Error::RecoveryRequired {
                        reason:
                            "shared Raft WAL requires recovery during shutdown leadership handoff"
                                .to_string(),
                    });
                }
                Err(error) => tracing::debug!(
                    error = %error,
                    "Raft message was not admitted during shutdown handoff",
                ),
            },
            Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => break,
        }
    }

    Ok(())
}

/// Give every local leader a bounded opportunity to hand off before the
/// worker exits. The host continues to process Raft messages and logical
/// ticks during this window, which lets the target receive the final frontier
/// and start an ordinary election if the targeted transfer times out.
fn graceful_leadership_handoff(
    host: &mut MultiRaftHost<LocalWal>,
    transport: &NodeRaftTransport,
    inbound: &NodeRaftInbound,
    metadata: &MetadataRuntimeHandle,
    metadata_replica_to_node: &BTreeMap<ReplicaId, NodeId>,
) {
    let deadline = Instant::now() + SHUTDOWN_LEADERSHIP_TRANSFER_DEADLINE;
    let mut timer_clock = SparseTickClock::new(TICK_INTERVAL);

    loop {
        if let Err(error) = drain_shutdown_messages(host, inbound) {
            tracing::error!(error = %error, "shutdown leadership handoff hit recovery-required state");
            break;
        }

        let state = metadata.state_snapshot();
        let active_nodes = active_node_ids(&state);
        let leaders = host
            .status()
            .groups
            .into_iter()
            .filter(|group_status| {
                group_status.role == Some(ragnordb_multiraft::host::MultiRaftRole::Leader)
            })
            .collect::<Vec<_>>();

        if leaders.is_empty() {
            tracing::info!("all local Raft leadership was handed off before shutdown");
            break;
        }

        for group_status in &leaders {
            let routes = replica_routes_for_group(
                &state,
                group_status.identity.raft_group_id,
                metadata_replica_to_node,
            );
            if let Err(error) = request_leadership_transfer(
                host,
                transport,
                group_status,
                &routes,
                &active_nodes,
                "graceful shutdown",
            ) {
                tracing::warn!(
                    error = %error,
                    raft_group_id = group_status.identity.raft_group_id.0,
                    "shutdown leadership transfer could not be started",
                );
            }
        }

        let elapsed_ticks = timer_clock.take_elapsed_ticks(Instant::now());
        match host.run_turn(
            elapsed_ticks,
            MultiRaftTurnBudget {
                max_groups: HOST_GROUP_BUDGET,
                max_messages: HOST_MESSAGE_BUDGET,
                ..MultiRaftTurnBudget::default()
            },
        ) {
            Ok(turn) => send_outbound(transport, turn.outbound),
            Err(error) => tracing::debug!(error = %error, "shutdown host turn failed"),
        }

        let now = Instant::now();
        if now >= deadline {
            break;
        }

        let wait = if host.has_runnable_work() {
            Duration::ZERO
        } else {
            let timer_wait = host
                .next_timer_delay_ticks()
                .map(|ticks| timer_clock.duration_until_ticks(ticks.max(1)))
                .unwrap_or_else(|| Duration::from_secs(1));
            timer_wait.min(deadline.saturating_duration_since(now))
        };
        if wait.is_zero() {
            thread::yield_now();
        } else {
            thread::park_timeout(wait);
        }
    }

    let remaining = host
        .status()
        .groups
        .into_iter()
        .filter(|group_status| {
            group_status.role == Some(ragnordb_multiraft::host::MultiRaftRole::Leader)
        })
        .map(|group_status| group_status.identity.raft_group_id.0)
        .collect::<Vec<_>>();
    if !remaining.is_empty() {
        tracing::warn!(
            raft_group_ids = ?remaining,
            "shutdown leadership handoff deadline elapsed; peers will use ordinary election",
        );
    }
}

fn publish_host_status(
    status: &SharedMultiRaftHostStatus,
    host: &MultiRaftHost<impl RaftWal + Send + 'static>,
    metadata_pending_proposals: usize,
) {
    let mut snapshot = host.status();
    if let Some(metadata_group) = snapshot
        .groups
        .iter_mut()
        .find(|group| group.identity.raft_group_id == METADATA_RAFT_GROUP_ID)
    {
        metadata_group.pending_proposals = metadata_pending_proposals;
    }
    *status
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = snapshot;
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
    W: RaftWal + Send + 'static,
{
    let mut serviced = 0;
    while serviced < max_requests {
        let Ok(request) = requests.try_recv() else {
            break;
        };
        serviced += 1;

        match request {
            MetadataHostRequest::Command {
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
                        reason: "metadata proposal deadline elapsed before proposal".to_string(),
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
            MetadataHostRequest::ConfChange {
                expected_conf_state_version,
                replica_id,
                reply,
            } => {
                let raft_replica_id = match replica_id.to_raft() {
                    Ok(replica_id) => replica_id,
                    Err(reason) => {
                        let _ = reply.send(Err(Error::Configuration(format!(
                            "invalid metadata ConfChange replica identity: {reason}"
                        ))));
                        continue;
                    }
                };
                match host.propose_conf_change(
                    METADATA_RAFT_GROUP_ID,
                    raft::types::ConfChange {
                        expected_version: expected_conf_state_version,
                        kind: raft::types::ConfChangeKind::RemoveReplica(raft_replica_id),
                    },
                ) {
                    Ok(proposal) => {
                        send_outbound(transport, proposal.outbound);
                        let _ = reply.send(Ok(()));
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
                MetadataApplyOutcome::Rejected(
                    rejection
                    @ ragnordb_catalog::MetadataRejection::TimestampReservationRegressed {
                        ..
                    },
                ) => Ok(MetadataApplyOutcome::Rejected(rejection)),
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
            reason: "metadata proposal deadline elapsed before apply".to_string(),
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

            Some(existing) if same_metadata_node_directory(existing, expected) => {
                // Lifecycle is durable metadata state, not static bootstrap
                // identity. A restart of a draining or terminal node must
                // accept the persisted lifecycle while still rejecting any
                // endpoint or locality drift.
            }

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

fn same_metadata_node_directory(left: &NodeDescriptor, right: &NodeDescriptor) -> bool {
    left.node_id == right.node_id
        && left.raft_addr == right.raft_addr
        && left.snapshot_addr == right.snapshot_addr
        && left.sql_addr == right.sql_addr
        && left.admin_addr == right.admin_addr
        && left.region == right.region
        && left.zone == right.zone
        && left.rack == right.rack
        && left.storage_class == right.storage_class
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

        if let Some(worker) = self.rpc_worker.take() {
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
        metadata_codec::{
            DesiredReplica, DesiredReplicaPlacement, NodeDescriptor, NodeLifecycle, PartitionSpec,
            PlacementPolicy,
        },
    };

    fn node(id: u64) -> NodeDescriptor {
        NodeDescriptor {
            node_id: NodeId(id),
            raft_addr: format!("127.0.0.1:{}", 7000 + id),
            snapshot_addr: format!("127.0.0.1:{}", 7050 + id),
            sql_addr: format!("127.0.0.1:{}", 7100 + id),
            admin_addr: format!("127.0.0.1:{}", 7200 + id),
            region: None,
            zone: None,
            rack: None,
            storage_class: "default".to_string(),
            lifecycle: NodeLifecycle::Active,
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

    /// Realistic bug caught: a restarted draining node must retain the
    /// metadata lifecycle from its durable directory rather than failing
    /// bootstrap because static seed configuration still describes it as
    /// Active.
    #[test]
    fn metadata_bootstrap_directory_match_ignores_lifecycle_state() {
        let expected = node(1);
        let mut persisted = expected.clone();
        persisted.lifecycle = NodeLifecycle::Draining;

        assert!(same_metadata_node_directory(&persisted, &expected));

        persisted.sql_addr = "127.0.0.1:9999".to_string();
        assert!(!same_metadata_node_directory(&persisted, &expected));
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
            placement_policy: PlacementPolicy::for_replica_count(1),
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

    /// Realistic bug caught: a leadership handoff must not select a learner,
    /// a lagging voter, a joint-consensus voter, or a node already leaving the
    /// cluster. Selecting any of those targets can make a planned removal
    /// loop indefinitely or hand leadership to a replica that cannot safely
    /// serve the committed log frontier.
    #[test]
    fn leadership_transfer_target_requires_caught_up_active_voter() {
        let voters = [ReplicaId(1), ReplicaId(2), ReplicaId(3), ReplicaId(4)];
        let learners = [ReplicaId(5)];
        let outgoing_voters = [ReplicaId(4)];
        let replica_match_indices = [
            (ReplicaId(1), 100),
            (ReplicaId(2), 100),
            (ReplicaId(3), 99),
            (ReplicaId(4), 100),
            (ReplicaId(5), 100),
        ];
        let target = select_transfer_target(
            TransferTargetState {
                leader_replica_id: Some(ReplicaId(1)),
                voters: &voters,
                learners: &learners,
                outgoing_voters: &outgoing_voters,
                replica_match_indices: &replica_match_indices,
                last_log_index: 100,
            },
            &BTreeMap::from([
                (ReplicaId(1), NodeId(1)),
                (ReplicaId(2), NodeId(2)),
                (ReplicaId(3), NodeId(3)),
                (ReplicaId(4), NodeId(4)),
                (ReplicaId(5), NodeId(5)),
            ]),
            &BTreeSet::from([NodeId(1), NodeId(2), NodeId(3), NodeId(5)]),
        );

        assert_eq!(target, Some(ReplicaId(2)));
    }

    /// Realistic bug caught: a replica in the outgoing voter set is still
    /// participating in joint consensus and must not be destroyed merely
    /// because it disappeared from the current voter set.
    #[test]
    fn conf_state_removal_proof_rejects_joint_consensus_members() {
        let mut conf_state = raft::types::ConfState::new(
            4,
            [raft::types::ReplicaId::must(101)],
            [raft::types::ReplicaId::must(202)],
        )
        .unwrap();
        conf_state
            .outgoing_voters
            .insert(raft::types::ReplicaId::must(303));

        assert!(!conf_state_excludes_replica(&conf_state, ReplicaId(303)));
        assert!(!conf_state_excludes_replica(&conf_state, ReplicaId(202)));
        assert!(conf_state_excludes_replica(&conf_state, ReplicaId(404)));
    }

    /// Realistic bug caught: once tombstone cleanup removes a bootstrap file,
    /// startup must still derive the initial configuration from the durable
    /// registry witness instead of treating retained configuration entries as
    /// unrecoverable.
    #[test]
    fn recovery_configuration_uses_registry_witness_after_bootstrap_removal() {
        let data = tempfile::tempdir().unwrap();
        let mut config = NodeConfig::new(
            NodeId(7),
            data.path().to_path_buf(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .unwrap();
        config.cluster_id = Some("cluster-a".to_string());

        let bootstrap = RaftGroupBootstrap::new(
            "cluster-a".to_string(),
            RaftGroupId(10),
            1,
            BTreeMap::from([(ReplicaId(101), NodeId(7)), (ReplicaId(202), NodeId(8))]),
            [ReplicaId(101), ReplicaId(202)].into_iter().collect(),
            std::collections::BTreeSet::new(),
        )
        .unwrap();
        let bootstrap_store = FileBootstrapStore::open(data.path().join("raft-bootstrap")).unwrap();
        let mut registry =
            LocalReplicaRegistry::open(data.path().join("replica-registry.json"), "cluster-a")
                .unwrap();
        let record = LocalReplicaRecord::new(
            RaftGroupId(10),
            ReplicaId(101),
            TabletId(20),
            TableId(30),
            1,
            ReplicaLifecycle::Creating,
        );
        registry.ensure_replica(record.clone()).unwrap();
        registry.mark_destroying(record.key()).unwrap();
        registry.mark_tombstoned(record.key()).unwrap();
        registry
            .set_initial_configuration(
                record.key(),
                InitialReplicaConfiguration::from_core(&bootstrap.to_core_conf_state().unwrap())
                    .unwrap(),
            )
            .unwrap();
        drop(registry);
        let mut bootstrap_store = bootstrap_store;
        ragnordb_multiraft::bootstrap::bootstrap_group_exactly_once(
            &mut bootstrap_store,
            &bootstrap,
        )
        .unwrap();
        bootstrap_store
            .remove_durable_bootstrap(bootstrap.raft_group_id)
            .unwrap();

        let lock = DataDirectoryLock::acquire(&config.data_dir).unwrap();
        let recovered = MultiRaftRuntime::recovery_configurations(&config, &lock).unwrap();
        assert_eq!(
            recovered[&RaftReplicaIdentity::new(RaftGroupId(10), ReplicaId(101)).unwrap()].voters,
            bootstrap.to_core_conf_state().unwrap().voters
        );
    }
}
