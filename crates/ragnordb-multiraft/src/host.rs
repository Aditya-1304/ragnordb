//! Physical-node host for the independent Raft replicas assigned to one server.
//!
//! The host owns the cross-group admission boundary. It schedules bounded group
//! operations, keeps inbound work tagged by group, and preserves the per-group
//! Ready ordering delegated to each [`HostedRaftGroup`]. Prepared Ready records
//! enter one bounded persistence service, which coalesces eligible groups into
//! an ordered shared A-WAL sync. Shared A-WAL uncertainty still fences every
//! local replica.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex, RwLock, mpsc},
    thread,
};

use raft::{
    core::{
        node::{
            LeadershipTransferError, LeadershipTransferStatus, ProposeError, RaftError,
            SnapshotInstallError, StepError,
        },
        read_index::{ReadIndexError, ReadState},
        ready::AdvanceError,
    },
    message::Envelope,
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{ConfChange, ConfState, LogIndex, Role, Term},
};
use ragnordb_common::ids::{NodeId, RaftGroupId, ReplicaId};

use crate::{
    membership::conf_change_for_action,
    meta::{MetadataReconcileAction, MetadataReconcileActionKind},
    runtime::{
        RaftReadyLoop, RaftReadyStateMachine, RaftSnapshotStore, ReadyApplyError, ReadyLoopError,
    },
    storage::{
        codec::RaftReplicaIdentity,
        persistence::{NodeRaftWal, NodeRaftWalHandle, RaftWal},
        recovery::RecoveredRaftStorage,
    },
};
use wal::{error::BatchAppendFailure, types::RecordType, wal::BatchAppendResult};

/// Wire envelope used by the byte-oriented Raft runtime.
pub type RaftMessageEnvelope = Envelope<Vec<u8>, Vec<u8>>;

/// An inter-node Raft envelope tagged with its logical group.
///
/// Raft-core envelopes identify replicas but intentionally do not identify a
/// logical group. A physical node may host multiple replica lifetimes, so the
/// transport boundary must carry this tag before the envelope reaches a local
/// `RaftReadyLoop`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedRaftMessage {
    pub raft_group_id: RaftGroupId,
    pub envelope: RaftMessageEnvelope,
}

/// Result of admitting one client command to a particular hosted group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostedProposal {
    pub index: LogIndex,
    pub outbound: Vec<RoutedRaftMessage>,
}

/// Result of requesting leadership transfer for one hosted Raft group.
///
/// A transfer has no log index because it is a control-plane protocol action,
/// not a replicated database command. Its targeted control message still
/// crosses the host's normal group routing boundary before it is released.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostedLeadershipTransfer {
    pub status: LeadershipTransferStatus,
    pub outbound: Vec<RoutedRaftMessage>,
}

/// Work limits for one physical-node host turn.
///
/// `max_groups` limits the number of group operations, while
/// `max_messages` limits inbound message operations. The remaining limits are
/// passed to group implementations through [`HostedGroupTurn`] accounting;
/// this keeps the node-level scheduler independent from tablet-specific apply
/// and snapshot implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultiRaftTurnBudget {
    pub max_groups: usize,
    pub max_messages: usize,
    pub max_ready_generations: usize,
    pub max_apply_entries: usize,
    pub max_apply_bytes: usize,
    pub max_snapshot_bytes: usize,
}

impl Default for MultiRaftTurnBudget {
    fn default() -> Self {
        Self {
            max_groups: 64,
            max_messages: 256,
            max_ready_generations: 1,
            max_apply_entries: 128,
            max_apply_bytes: 4 * 1024 * 1024,
            max_snapshot_bytes: 4 * 1024 * 1024,
        }
    }
}

/// Admission limits owned by one physical MultiRaft host.
///
/// Transport queues bound bytes before the host owns a message. These limits
/// protect the second boundary: messages already admitted to a slow group's
/// scheduler queue. Without both limits, a peer can continuously refill the
/// host while that group is waiting on a retryable Ready or persistence step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultiRaftHostConfig {
    pub max_pending_messages: usize,
    pub max_pending_message_bytes: usize,
    pub max_pending_group_messages: usize,
    pub max_pending_group_message_bytes: usize,
    pub max_proposal_bytes: usize,
    /// Maximum number of prepared Ready generations retained by the
    /// node-local persistence service before the next group is deferred.
    pub max_pending_persistence_groups: usize,
    /// Maximum number of A-WAL records retained by the persistence service
    /// while a cross-group batch is being assembled.
    pub max_pending_persistence_records: usize,
    /// Maximum encoded payload bytes retained by the persistence service while
    /// a cross-group batch is being assembled.
    pub max_pending_persistence_bytes: usize,
}

impl Default for MultiRaftHostConfig {
    fn default() -> Self {
        Self {
            max_pending_messages: 8 * 1024,
            max_pending_message_bytes: 64 * 1024 * 1024,
            max_pending_group_messages: 2 * 1024,
            max_pending_group_message_bytes: 16 * 1024 * 1024,
            max_proposal_bytes: 16 * 1024 * 1024,
            max_pending_persistence_groups: 64,
            max_pending_persistence_records: 4096,
            max_pending_persistence_bytes: 64 * 1024 * 1024,
        }
    }
}

impl MultiRaftHostConfig {
    fn validate(self) -> Result<(), MultiRaftHostError> {
        if self.max_pending_messages == 0
            || self.max_pending_message_bytes == 0
            || self.max_pending_group_messages == 0
            || self.max_pending_group_message_bytes == 0
            || self.max_proposal_bytes == 0
            || self.max_pending_persistence_groups == 0
            || self.max_pending_persistence_records == 0
            || self.max_pending_persistence_bytes == 0
        {
            return Err(MultiRaftHostError::InvalidConfiguration(
                "MultiRaft host limits must be non-zero".to_string(),
            ));
        }

        if self.max_pending_group_messages > self.max_pending_messages
            || self.max_pending_group_message_bytes > self.max_pending_message_bytes
        {
            return Err(MultiRaftHostError::InvalidConfiguration(
                "per-group pending limits cannot exceed node limits".to_string(),
            ));
        }

        Ok(())
    }
}

/// Role reported by one hosted Raft replica in the node status snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultiRaftRole {
    Leader,
    Follower,
    Candidate,
}

impl MultiRaftRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Leader => "leader",
            Self::Follower => "follower",
            Self::Candidate => "candidate",
        }
    }
}

impl From<Role> for MultiRaftRole {
    fn from(role: Role) -> Self {
        match role {
            Role::Leader => Self::Leader,
            Role::Follower => Self::Follower,
            Role::Candidate => Self::Candidate,
        }
    }
}

/// Read-only status for one local Raft group and replica lifetime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiRaftGroupStatus {
    pub identity: RaftReplicaIdentity,
    pub role: Option<MultiRaftRole>,
    pub leader_replica_id: Option<ReplicaId>,
    pub term: u64,
    pub commit_index: u64,
    pub last_log_index: u64,
    pub applied_index: u64,
    pub snapshot_index: u64,
    pub uncommitted_bytes: usize,
    pub replication_inflight_bytes: usize,
    pub pending_work: bool,
    /// Client proposals admitted by this group that still await an applied,
    /// rejected, or retryable terminal outcome.
    pub pending_proposals: usize,
    /// Incrementally tracked state-machine work waiting behind the contiguous
    /// applied frontier. These counters are diagnostic and also expose when
    /// admission is being throttled by the apply boundary.
    pub apply_backlog_entries: usize,
    pub apply_backlog_bytes: usize,
    pub apply_backlog_age_ms: u64,
    pub apply_backlog_generations: usize,
    pub pending_messages: usize,
    pub pending_message_bytes: usize,
    pub quarantine_reason: Option<String>,
    /// Durable membership epoch currently observed by the Raft core.
    pub conf_state_version: Option<u64>,
    /// True only for an explicitly bootstrapped passive joiner. A removed
    /// member is not relabeled as a joiner.
    pub joining: bool,
    pub voters: Vec<ReplicaId>,
    pub learners: Vec<ReplicaId>,
    pub outgoing_voters: Vec<ReplicaId>,
    /// Latest per-replica replication match frontier, including the local
    /// replica. This is diagnostic state and never drives host placement.
    pub replica_match_indices: Vec<(ReplicaId, u64)>,
    /// First unapplied configuration entry, if one is outstanding.
    pub pending_conf_change_index: Option<u64>,
    /// Exact latest committed configuration entry `(index, term)`, when the
    /// core has retained that proof for post-removal metadata retirement.
    pub last_conf_change: Option<(u64, u64)>,
    /// Exact `(replica, index, term, resulting_conf_state_version)` proof for
    /// the latest committed removal, if one is known.
    pub last_removed_replica: Option<(ReplicaId, u64, u64, u64)>,
}

/// Lifecycle state of the physical MultiRaft host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultiRaftHostState {
    Registering,
    Active,
    RecoveryRequired,
}

impl MultiRaftHostState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Registering => "registering",
            Self::Active => "active",
            Self::RecoveryRequired => "recovery_required",
        }
    }
}

/// Point-in-time node status. The group list is authoritative for every local
/// group, including groups that are quarantined and therefore no longer
/// runnable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiRaftHostStatus {
    pub node_id: NodeId,
    pub state: MultiRaftHostState,
    pub pending_message_count: usize,
    pub pending_message_bytes: usize,
    pub pending_persistence_groups: usize,
    pub pending_persistence_records: usize,
    pub pending_persistence_bytes: usize,
    pub groups: Vec<MultiRaftGroupStatus>,
}

/// Bounded per-group load information suitable for continuously published
/// node metrics. Full group diagnostics remain available through
/// MultiRaftHostStatus when an operator explicitly requests them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiRaftGroupLoad {
    pub identity: RaftReplicaIdentity,
    pub role: Option<MultiRaftRole>,
    pub pending_proposals: usize,
    pub pending_messages: usize,
    pub pending_message_bytes: usize,
}

/// Aggregate host metrics with a bounded top-K view of queue pressure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiRaftHostSummary {
    pub node_id: NodeId,
    pub state: MultiRaftHostState,
    pub pending_message_count: usize,
    pub pending_message_bytes: usize,
    pub pending_persistence_groups: usize,
    pub pending_persistence_records: usize,
    pub pending_persistence_bytes: usize,
    pub pending_proposal_count: usize,
    pub group_count: usize,
    pub leader_count: usize,
    pub candidate_count: usize,
    pub quarantined_group_count: usize,
    pub top_groups: Vec<MultiRaftGroupLoad>,
}

impl MultiRaftHostStatus {
    /// Project detailed status into a bounded aggregate. The cap prevents a
    /// continuously sampled admin/metrics response from scaling with the
    /// number of hosted tablets.
    pub fn bounded_summary(&self, max_top_groups: usize) -> MultiRaftHostSummary {
        const MAX_TOP_GROUPS: usize = 16;
        let mut top_groups = self
            .groups
            .iter()
            .filter(|group| {
                group.pending_proposals != 0
                    || group.pending_messages != 0
                    || group.pending_message_bytes != 0
            })
            .map(|group| MultiRaftGroupLoad {
                identity: group.identity,
                role: group.role,
                pending_proposals: group.pending_proposals,
                pending_messages: group.pending_messages,
                pending_message_bytes: group.pending_message_bytes,
            })
            .collect::<Vec<_>>();
        top_groups.sort_by(|left, right| {
            right
                .pending_message_bytes
                .cmp(&left.pending_message_bytes)
                .then_with(|| right.pending_messages.cmp(&left.pending_messages))
                .then_with(|| right.pending_proposals.cmp(&left.pending_proposals))
                .then_with(|| {
                    left.identity
                        .raft_group_id
                        .cmp(&right.identity.raft_group_id)
                })
                .then_with(|| left.identity.replica_id.cmp(&right.identity.replica_id))
        });
        top_groups.truncate(max_top_groups.min(MAX_TOP_GROUPS));

        MultiRaftHostSummary {
            node_id: self.node_id,
            state: self.state,
            pending_message_count: self.pending_message_count,
            pending_message_bytes: self.pending_message_bytes,
            pending_persistence_groups: self.pending_persistence_groups,
            pending_persistence_records: self.pending_persistence_records,
            pending_persistence_bytes: self.pending_persistence_bytes,
            pending_proposal_count: self
                .groups
                .iter()
                .map(|group| group.pending_proposals)
                .sum(),
            group_count: self.groups.len(),
            leader_count: self
                .groups
                .iter()
                .filter(|group| group.role == Some(MultiRaftRole::Leader))
                .count(),
            candidate_count: self
                .groups
                .iter()
                .filter(|group| group.role == Some(MultiRaftRole::Candidate))
                .count(),
            quarantined_group_count: self
                .groups
                .iter()
                .filter(|group| group.quarantine_reason.is_some())
                .count(),
            top_groups,
        }
    }
}

pub type SharedMultiRaftHostStatus = Arc<RwLock<MultiRaftHostStatus>>;

/// Work performed by one hosted group operation.
///
/// The node host can enforce group and message fairness without knowing how a
/// group applies committed entries or transfers snapshots. Concrete group
/// adapters report those finer-grained counters so callers can observe the
/// same budget boundary across metadata and tablet groups.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct HostedGroupTurn {
    pub outbound: Vec<RaftMessageEnvelope>,
    /// ReadIndex results released after this Ready generation's committed
    /// entries have applied. These states authorize a future read only after
    /// the consumer independently verifies the local applied frontier.
    pub read_states: Vec<ReadState>,
    pub ready_generations: usize,
    pub apply_entries: usize,
    pub snapshot_bytes: usize,
    pub(crate) persistence: Option<HostedPersistenceBatch>,
}

/// Bounded persistence pressure reported by the node-local A-WAL service.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MultiRaftPersistenceStatus {
    pub pending_groups: usize,
    pub pending_records: usize,
    pub pending_bytes: usize,
}

/// One group's encoded Ready records awaiting the node-wide WAL boundary.
///
/// Payloads are converted to owned boxed slices at the service boundary. The
/// service never re-encodes or mutates these buffers; the corresponding group
/// keeps its prevalidated logical successor until the exact returned extent
/// range is committed.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct HostedPersistenceBatch {
    records: Box<[HostedPersistenceRecord]>,
    encoded_bytes: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct HostedPersistenceRecord {
    record_type: RecordType,
    payload: Box<[u8]>,
}

impl HostedPersistenceBatch {
    fn new(records: Vec<(RecordType, Vec<u8>)>) -> Self {
        let encoded_bytes = records.iter().fold(0_usize, |total, (_, payload)| {
            total.saturating_add(payload.len())
        });
        let records = records
            .into_iter()
            .map(|(record_type, payload)| HostedPersistenceRecord {
                record_type,
                payload: payload.into_boxed_slice(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            records,
            encoded_bytes,
        }
    }

    fn record_count(&self) -> usize {
        self.records.len()
    }

    fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }
}

/// Result of one bounded host turn.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct MultiRaftTurnResult {
    pub groups_serviced: usize,
    pub messages_processed: usize,
    pub ready_generations: usize,
    pub apply_entries: usize,
    pub snapshot_bytes: usize,
    pub outbound: Vec<RoutedRaftMessage>,
    /// Read states released by hosted groups after their local apply boundary.
    pub read_states: Vec<(RaftGroupId, ReadState)>,
}

#[derive(Debug, Default)]
struct RunnableGroupQueue {
    control: VecDeque<RaftGroupId>,
    bulk: VecDeque<RaftGroupId>,
    queued: BTreeSet<RaftGroupId>,
    control_queued: BTreeSet<RaftGroupId>,
}

impl RunnableGroupQueue {
    fn enqueue(&mut self, raft_group_id: RaftGroupId) {
        if self.queued.insert(raft_group_id) {
            self.bulk.push_back(raft_group_id);
        }
    }

    fn enqueue_control(&mut self, raft_group_id: RaftGroupId) {
        if self.control_queued.insert(raft_group_id) {
            self.bulk.retain(|queued| *queued != raft_group_id);
            self.control.push_back(raft_group_id);
            self.queued.insert(raft_group_id);
        }
    }

    fn pop(&mut self) -> Option<RaftGroupId> {
        let raft_group_id = self.control.pop_front().or_else(|| self.bulk.pop_front())?;
        let removed = self.queued.remove(&raft_group_id);
        debug_assert!(removed);
        self.control_queued.remove(&raft_group_id);
        Some(raft_group_id)
    }

    fn remove(&mut self, raft_group_id: RaftGroupId) -> bool {
        if !self.queued.remove(&raft_group_id) {
            return false;
        }

        self.control_queued.remove(&raft_group_id);
        self.control.retain(|queued| *queued != raft_group_id);
        self.bulk.retain(|queued| *queued != raft_group_id);
        true
    }

    fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }
}

#[derive(Debug, Default)]
struct GroupTimerScheduler {
    now: u64,
    deadlines: BTreeMap<u64, BTreeSet<RaftGroupId>>,
    scheduled: BTreeMap<RaftGroupId, ScheduledDeadline>,
}

#[derive(Debug, Clone, Copy)]
struct ScheduledDeadline {
    deadline: u64,
    scheduled_at: u64,
}

impl GroupTimerScheduler {
    fn schedule_after(&mut self, raft_group_id: RaftGroupId, delay: u64) {
        let deadline = self.now.saturating_add(delay);

        if self
            .scheduled
            .get(&raft_group_id)
            .is_some_and(|existing| existing.deadline <= deadline)
        {
            return;
        }

        self.remove(raft_group_id);
        self.deadlines
            .entry(deadline)
            .or_default()
            .insert(raft_group_id);
        self.scheduled.insert(
            raft_group_id,
            ScheduledDeadline {
                deadline,
                scheduled_at: self.now,
            },
        );
    }

    fn reschedule_after(&mut self, raft_group_id: RaftGroupId, delay: u64) {
        self.remove(raft_group_id);
        self.schedule_after(raft_group_id, delay);
    }

    fn advance(&mut self, ticks: u64) -> Vec<(RaftGroupId, u64)> {
        self.now = self.now.saturating_add(ticks);
        let due_deadlines: Vec<u64> = self
            .deadlines
            .range(..=self.now)
            .map(|(deadline, _)| *deadline)
            .collect();
        let mut due_groups = Vec::new();

        for deadline in due_deadlines {
            if let Some(groups) = self.deadlines.remove(&deadline) {
                for raft_group_id in groups {
                    let scheduled = self
                        .scheduled
                        .remove(&raft_group_id)
                        .expect("every timer-wheel entry has a scheduled deadline");
                    due_groups.push((
                        raft_group_id,
                        self.now.saturating_sub(scheduled.scheduled_at),
                    ));
                }
            }
        }

        due_groups
    }

    fn next_deadline(&self) -> Option<u64> {
        self.deadlines.keys().next().copied()
    }

    fn advance_clock(&mut self, ticks: u64) {
        self.now = self.now.saturating_add(ticks);
    }

    fn remove(&mut self, raft_group_id: RaftGroupId) {
        let Some(scheduled) = self.scheduled.remove(&raft_group_id) else {
            return;
        };

        if let Some(groups) = self.deadlines.get_mut(&scheduled.deadline) {
            groups.remove(&raft_group_id);
        }
        if self
            .deadlines
            .get(&scheduled.deadline)
            .is_some_and(BTreeSet::is_empty)
        {
            self.deadlines.remove(&scheduled.deadline);
        }
    }
}

#[derive(Debug, Default)]
struct PendingGroupMessages {
    control: VecDeque<PendingMessage>,
    bulk: VecDeque<PendingMessage>,
    message_count: usize,
    message_bytes: usize,
}

#[derive(Debug, Clone)]
struct PendingMessage {
    message: RoutedRaftMessage,
    wire_bytes: usize,
}

#[derive(Debug)]
struct PendingPersistenceGroup {
    raft_group_id: RaftGroupId,
    timer_due: bool,
    batch: HostedPersistenceBatch,
    /// Outbound envelopes are held until the group's Ready persistence has
    /// completed. This keeps custom group adapters subject to the same
    /// persistence-before-message invariant as the built-in adapter.
    outbound: Vec<RaftMessageEnvelope>,
    /// Read states are held with the Ready batch so a custom adapter cannot
    /// publish a read result before the batch's persistence/apply boundary.
    read_states: Vec<ReadState>,
}

/// Admission failure from the node-local persistence service.
#[derive(Debug)]
enum PersistenceAdmissionError {
    /// The request is valid but cannot fit in the bounded staging window yet.
    Capacity(PendingPersistenceGroup),
    /// A single request exceeds the configured service limit and cannot make
    /// progress without an operator-selected configuration change.
    RequestTooLarge {
        pending: PendingPersistenceGroup,
        record_count: usize,
        encoded_bytes: usize,
    },
}

/// The single host-owned service which stages prepared Ready generations and
/// submits their immutable records to the node-wide A-WAL in FIFO order.
///
/// This is intentionally one dedicated worker rather than one thread per
/// request. The current Ready contract does not allow a group to expose its
/// next generation until the previous one is acknowledged, so the useful
/// overlap is preparation by other groups while this service performs one
/// ordered append-and-sync operation outside the host scheduler thread.
struct PersistenceWork {
    groups: Vec<PendingPersistenceGroup>,
}

struct PersistenceCompletion {
    groups: Vec<PendingPersistenceGroup>,
    outcome: Result<BatchAppendResult, BatchAppendFailure>,
}

struct PersistenceService<W>
where
    W: RaftWal + Send + 'static,
{
    node_wal: NodeRaftWal<W>,
    max_pending_groups: usize,
    max_pending_records: usize,
    max_pending_bytes: usize,
    pending: VecDeque<PendingPersistenceGroup>,
    pending_group_ids: BTreeSet<RaftGroupId>,
    pending_records: usize,
    pending_bytes: usize,
    in_flight: bool,
    in_flight_groups: usize,
    in_flight_records: usize,
    in_flight_bytes: usize,
    work_tx: Option<mpsc::SyncSender<PersistenceWork>>,
    completion_rx: mpsc::Receiver<PersistenceCompletion>,
    host_thread: Arc<Mutex<Option<thread::Thread>>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl<W> PersistenceService<W>
where
    W: RaftWal + Send + 'static,
{
    fn new(node_wal: NodeRaftWal<W>, config: MultiRaftHostConfig) -> Self {
        let (work_tx, work_rx) = mpsc::sync_channel::<PersistenceWork>(1);
        let (completion_tx, completion_rx) = mpsc::channel();
        let host_thread = Arc::new(Mutex::new(None::<thread::Thread>));
        let worker_host_thread = Arc::clone(&host_thread);
        let worker_wal = node_wal.clone();
        let worker = thread::Builder::new()
            .name("ragnordb-raft-persistence".to_string())
            .spawn(move || {
                while let Ok(work) = work_rx.recv() {
                    let groups = work.groups;
                    let outcome = append_prepared_batch(&worker_wal, &groups);
                    if completion_tx
                        .send(PersistenceCompletion { groups, outcome })
                        .is_err()
                    {
                        break;
                    }
                    if let Some(host) = worker_host_thread
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .as_ref()
                        .cloned()
                    {
                        host.unpark();
                    }
                }
            })
            .expect("Raft persistence worker thread creation must succeed");

        Self {
            node_wal,
            max_pending_groups: config.max_pending_persistence_groups,
            max_pending_records: config.max_pending_persistence_records,
            max_pending_bytes: config.max_pending_persistence_bytes,
            pending: VecDeque::new(),
            pending_group_ids: BTreeSet::new(),
            pending_records: 0,
            pending_bytes: 0,
            in_flight: false,
            in_flight_groups: 0,
            in_flight_records: 0,
            in_flight_bytes: 0,
            work_tx: Some(work_tx),
            completion_rx,
            host_thread,
            worker: Some(worker),
        }
    }

    fn status(&self) -> MultiRaftPersistenceStatus {
        MultiRaftPersistenceStatus {
            pending_groups: self.pending.len() + self.in_flight_groups,
            pending_records: self.pending_records + self.in_flight_records,
            pending_bytes: self.pending_bytes + self.in_flight_bytes,
        }
    }

    fn try_submit(
        &mut self,
        pending: PendingPersistenceGroup,
    ) -> Result<(), PersistenceAdmissionError> {
        if self.pending_group_ids.contains(&pending.raft_group_id) {
            return Err(PersistenceAdmissionError::Capacity(pending));
        }

        let record_count = pending.batch.record_count();
        let encoded_bytes = pending.batch.encoded_bytes();
        if record_count > self.max_pending_records || encoded_bytes > self.max_pending_bytes {
            return Err(PersistenceAdmissionError::RequestTooLarge {
                pending,
                record_count,
                encoded_bytes,
            });
        }

        if self.pending.len() + self.in_flight_groups >= self.max_pending_groups
            || self
                .pending_records
                .saturating_add(self.in_flight_records)
                .saturating_add(record_count)
                > self.max_pending_records
            || self
                .pending_bytes
                .saturating_add(self.in_flight_bytes)
                .saturating_add(encoded_bytes)
                > self.max_pending_bytes
        {
            return Err(PersistenceAdmissionError::Capacity(pending));
        }

        self.pending_records = self.pending_records.saturating_add(record_count);
        self.pending_bytes = self.pending_bytes.saturating_add(encoded_bytes);
        self.pending_group_ids.insert(pending.raft_group_id);
        self.pending.push_back(pending);
        Ok(())
    }

    fn contains_group(&self, raft_group_id: RaftGroupId) -> bool {
        self.pending_group_ids.contains(&raft_group_id)
    }

    fn bind_host_thread(&self) {
        *self
            .host_thread
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(thread::current());
    }

    fn dispatch_if_idle(&mut self) {
        if self.in_flight || self.pending.is_empty() {
            return;
        }

        let groups = self.pending.drain(..).collect::<Vec<_>>();
        let record_count = groups
            .iter()
            .map(PendingPersistenceGroup::record_count)
            .sum::<usize>();
        let encoded_bytes = groups
            .iter()
            .map(PendingPersistenceGroup::encoded_bytes)
            .sum::<usize>();
        self.pending_records = 0;
        self.pending_bytes = 0;

        let work = PersistenceWork { groups };
        let group_count = work.groups.len();
        match self
            .work_tx
            .as_ref()
            .expect("Raft persistence worker sender must remain available")
            .try_send(work)
        {
            Ok(()) => {
                self.in_flight = true;
                self.in_flight_groups = group_count;
                self.in_flight_records = record_count;
                self.in_flight_bytes = encoded_bytes;
            }
            Err(mpsc::TrySendError::Full(work)) => {
                for group in work.groups.into_iter().rev() {
                    self.pending.push_front(group);
                }
                self.pending_records = record_count;
                self.pending_bytes = encoded_bytes;
            }
            Err(mpsc::TrySendError::Disconnected(work)) => {
                for group in work.groups.into_iter().rev() {
                    self.pending.push_front(group);
                }
                self.pending_records = record_count;
                self.pending_bytes = encoded_bytes;
                self.node_wal
                    .require_recovery("Raft persistence worker disconnected");
            }
        }
    }

    fn try_take_completion(&mut self) -> Option<PersistenceCompletion> {
        match self.completion_rx.try_recv() {
            Ok(completion) => {
                for pending in &completion.groups {
                    self.pending_group_ids.remove(&pending.raft_group_id);
                }
                self.in_flight = false;
                self.in_flight_groups = 0;
                self.in_flight_records = 0;
                self.in_flight_bytes = 0;
                Some(completion)
            }
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => {
                if self.in_flight {
                    self.node_wal
                        .require_recovery("Raft persistence worker stopped unexpectedly");
                    self.in_flight = false;
                    self.in_flight_groups = 0;
                    self.in_flight_records = 0;
                    self.in_flight_bytes = 0;
                }
                None
            }
        }
    }
}

impl<W> Drop for PersistenceService<W>
where
    W: RaftWal + Send + 'static,
{
    fn drop(&mut self) {
        self.work_tx.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl PendingPersistenceGroup {
    fn record_count(&self) -> usize {
        self.batch.record_count()
    }

    fn encoded_bytes(&self) -> usize {
        self.batch.encoded_bytes()
    }
}

fn append_prepared_batch<W: RaftWal>(
    node_wal: &NodeRaftWal<W>,
    groups: &[PendingPersistenceGroup],
) -> Result<BatchAppendResult, BatchAppendFailure> {
    let total_records = groups.iter().fold(0_usize, |total, pending| {
        total.saturating_add(pending.batch.record_count())
    });
    let records: Vec<_> = groups
        .iter()
        .flat_map(|pending| pending.batch.records.iter())
        // A-WAL remains the checksum and LSN authority. Passing these exact
        // borrowed payloads avoids a second encoding or checksum
        // interpretation between preparation and the durable boundary.
        .map(|record| (record.record_type, record.payload.as_ref()))
        .collect();

    let mut outcome = if records.is_empty() {
        Ok(BatchAppendResult {
            record_extents: Vec::new(),
            final_end_lsn: wal::lsn::Lsn::ZERO,
        })
    } else {
        node_wal.append_batch_and_sync(&records)
    };

    if let Ok(batch_result) = &outcome
        && (batch_result.record_extents.len() != total_records
            || (total_records > 0
                && batch_result
                    .record_extents
                    .last()
                    .is_none_or(|extent| extent.end_lsn != batch_result.final_end_lsn)))
    {
        node_wal.require_recovery("shared A-WAL returned an invalid cross-group batch frontier");
        outcome = Err(BatchAppendFailure::OutcomeUnknown {
            result: batch_result.clone(),
            source: wal::error::WalError::BrokenDurabilityContract,
        });
    }

    outcome
}

impl PendingGroupMessages {
    fn len(&self) -> usize {
        self.message_count
    }

    fn wire_bytes(&self) -> usize {
        self.message_bytes
    }

    fn is_empty(&self) -> bool {
        self.control.is_empty() && self.bulk.is_empty()
    }

    fn has_control(&self) -> bool {
        !self.control.is_empty()
    }

    fn pop(&mut self) -> Option<PendingMessage> {
        let message = self.control.pop_front().or_else(|| self.bulk.pop_front())?;
        self.message_count = self.message_count.saturating_sub(1);
        self.message_bytes = self.message_bytes.saturating_sub(message.wire_bytes);
        Some(message)
    }

    fn push_front(&mut self, message: PendingMessage) {
        let wire_bytes = message.wire_bytes;
        if is_control_message(&message.message.envelope) {
            self.control.push_front(message);
        } else {
            self.bulk.push_front(message);
        }
        self.message_count = self.message_count.saturating_add(1);
        self.message_bytes = self.message_bytes.saturating_add(wire_bytes);
    }

    fn push_back(&mut self, message: PendingMessage) {
        let wire_bytes = message.wire_bytes;
        if is_control_message(&message.message.envelope) {
            self.control.push_back(message);
        } else {
            self.bulk.push_back(message);
        }
        self.message_count = self.message_count.saturating_add(1);
        self.message_bytes = self.message_bytes.saturating_add(wire_bytes);
    }
}

pub(crate) fn is_control_message(message: &RaftMessageEnvelope) -> bool {
    matches!(
        message.msg,
        raft::message::Message::PreVote(_)
            | raft::message::Message::PreVoteResponse(_)
            | raft::message::Message::RequestVote(_)
            | raft::message::Message::RequestVoteResponse(_)
            | raft::message::Message::ReadIndex(_)
            | raft::message::Message::ReadIndexResponse(_)
            | raft::message::Message::TimeoutNow(_)
            | raft::message::Message::AppendEntriesResponse(_)
            | raft::message::Message::InstallSnapshot(_)
            | raft::message::Message::InstallSnapshotResponse(_)
    ) || matches!(
        &message.msg,
        raft::message::Message::AppendEntries(request) if request.entries.is_empty()
    )
}

fn group_budget_for_completion(budget: MultiRaftTurnBudget) -> MultiRaftTurnBudget {
    MultiRaftTurnBudget {
        max_groups: 1,
        max_messages: 0,
        max_ready_generations: budget.max_ready_generations,
        max_apply_entries: budget.max_apply_entries,
        max_apply_bytes: budget.max_apply_bytes,
        max_snapshot_bytes: budget.max_snapshot_bytes,
    }
}

/// Type-erased lifecycle boundary for one hosted Raft replica.
///
/// Direct operations drain the complete Ready lifecycle before returning.
/// Budgeted scheduler operations may instead prepare a Ready and return its
/// records to the host for cross-group WAL batching; the host then calls
/// [`HostedRaftGroup::complete_persistence`] before releasing dependent output.
/// This prevents another caller from interleaving work between `step()` and
/// persistence/apply.
pub trait HostedRaftGroup: Send {
    fn identity(&self) -> RaftReplicaIdentity;

    /// Return the adapter-owned portion of the node status. The host adds
    /// queue and quarantine information because those counters belong to the
    /// physical-node scheduler rather than to an individual Raft core.
    fn status(&self) -> MultiRaftGroupStatus {
        MultiRaftGroupStatus {
            identity: self.identity(),
            role: None,
            leader_replica_id: None,
            term: 0,
            commit_index: 0,
            last_log_index: 0,
            applied_index: 0,
            snapshot_index: 0,
            uncommitted_bytes: 0,
            replication_inflight_bytes: 0,
            pending_work: self.has_pending_work(),
            pending_proposals: 0,
            apply_backlog_entries: 0,
            apply_backlog_bytes: 0,
            apply_backlog_age_ms: 0,
            apply_backlog_generations: 0,
            pending_messages: 0,
            pending_message_bytes: 0,
            quarantine_reason: None,
            conf_state_version: None,
            joining: false,
            voters: Vec::new(),
            learners: Vec::new(),
            outgoing_voters: Vec::new(),
            replica_match_indices: Vec::new(),
            pending_conf_change_index: None,
            last_conf_change: None,
            last_removed_replica: None,
        }
    }

    /// Returns whether work from an earlier operation must be resumed before a
    /// new Raft mutation is admitted. Lightweight adapters may use the
    /// compatibility default because their operation is host-atomic.
    fn has_pending_work(&self) -> bool {
        false
    }

    /// Return the next logical timer delay for this group.
    ///
    /// The host owns the shared deadline scheduler, but the group owns the
    /// Raft role-specific interval. A leader normally returns its heartbeat
    /// interval, while a follower or candidate returns its current election
    /// timeout. `None` is reserved for a passive replica that should only be
    /// driven by inbound messages or explicit local work.
    fn next_timer_delay_ticks(&self) -> Option<u64> {
        None
    }

    /// Returns whether the only pending work is a compatibility-path
    /// ReadState that can be safely invalidated by processing an inbound
    /// control message first. A normal outstanding Ready must continue to
    /// block message admission because its persistence/apply ordering is
    /// still owned by the group.
    fn has_only_deferred_read_states(&self) -> bool {
        false
    }

    /// Enables ReadIndex only after the group has crossed its current-term
    /// persistence and apply boundary. Adapters which still expose their own
    /// read barrier retain the safe default and must opt into this contract
    /// explicitly.
    fn activate_read_index(&mut self, _term: Term) -> Result<(), HostedGroupError> {
        Err(HostedGroupError::Rejected(
            "hosted group does not expose Raft ReadIndex admission".to_string(),
        ))
    }

    /// Admits one opaque ReadIndex context. Completion remains asynchronous;
    /// the resulting [`ReadState`] is returned from a later fully completed
    /// [`HostedGroupTurn`].
    fn read_index(&mut self, _context: Vec<u8>) -> Result<(), HostedGroupError> {
        Err(HostedGroupError::Rejected(
            "hosted group does not expose Raft ReadIndex admission".to_string(),
        ))
    }

    fn tick_and_drain(&mut self, ticks: u64) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError>;

    fn step_and_drain(
        &mut self,
        message: RaftMessageEnvelope,
    ) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError>;

    fn propose_and_drain(
        &mut self,
        command: Vec<u8>,
        encoded_len: usize,
    ) -> Result<(LogIndex, Vec<RaftMessageEnvelope>), HostedGroupError>;

    /// Admits one typed membership transition through the same Ready and
    /// shared-WAL boundary as an application proposal.
    fn propose_conf_change(
        &mut self,
        _change: ConfChange,
    ) -> Result<(LogIndex, Vec<RaftMessageEnvelope>), HostedGroupError> {
        Err(HostedGroupError::Rejected(
            "hosted group does not expose Raft membership proposals".to_string(),
        ))
    }

    /// Requests leadership transfer without treating the control-plane action
    /// as an application or membership proposal.
    fn transfer_leadership(
        &mut self,
        _target: ReplicaId,
        _timeout_ticks: u64,
    ) -> Result<(LeadershipTransferStatus, Vec<RaftMessageEnvelope>), HostedGroupError> {
        Err(HostedGroupError::Rejected(
            "hosted group does not expose Raft leadership transfer".to_string(),
        ))
    }

    /// Executes one bounded group turn. Group adapters with finer-grained
    /// apply or snapshot work can override this method; the default preserves
    /// the existing host-group contract while the host still enforces
    /// cross-group fairness and inbound message limits.
    fn tick_and_drain_budgeted(
        &mut self,
        ticks: u64,
        _budget: MultiRaftTurnBudget,
    ) -> Result<HostedGroupTurn, HostedGroupError> {
        self.tick_and_drain(ticks).map(|outbound| HostedGroupTurn {
            outbound,
            ..HostedGroupTurn::default()
        })
    }

    fn step_and_drain_budgeted(
        &mut self,
        message: RaftMessageEnvelope,
        _budget: MultiRaftTurnBudget,
    ) -> Result<HostedGroupTurn, HostedGroupError> {
        self.step_and_drain(message)
            .map(|outbound| HostedGroupTurn {
                outbound,
                ..HostedGroupTurn::default()
            })
    }

    /// Prepare group work without publishing outbound messages that depend on
    /// its Ready persistence. The default keeps compatibility with lightweight
    /// host adapters that do not expose a two-phase persistence lifecycle.
    fn tick_and_prepare_budgeted(
        &mut self,
        ticks: u64,
        budget: MultiRaftTurnBudget,
    ) -> Result<HostedGroupTurn, HostedGroupError> {
        self.tick_and_drain_budgeted(ticks, budget)
    }

    fn step_and_prepare_budgeted(
        &mut self,
        message: RaftMessageEnvelope,
        budget: MultiRaftTurnBudget,
    ) -> Result<HostedGroupTurn, HostedGroupError> {
        self.step_and_drain_budgeted(message, budget)
    }

    /// Complete a group whose records were included in the shared WAL batch.
    /// The default has no group-owned persistence to complete.
    fn complete_persistence(
        &mut self,
        _outcome: Result<BatchAppendResult, BatchAppendFailure>,
        _budget: MultiRaftTurnBudget,
    ) -> Result<HostedGroupTurn, HostedGroupError> {
        Ok(HostedGroupTurn::default())
    }
}

/// Error reported by an individual hosted group.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum HostedGroupError {
    #[error("the shared Raft WAL requires recovery")]
    RecoveryRequired,

    /// The group crossed a correctness boundary and must not execute again
    /// until process restart/recovery.
    #[error("hosted Raft group failed: {0}")]
    Group(String),

    /// The operation was valid to attempt but could not complete now. The
    /// group remains healthy and may retry its pending Ready lifecycle later.
    #[error("hosted Raft group operation is retryable: {0}")]
    Retryable(String),

    /// The operation was rejected without damaging the group's runtime state,
    /// such as proposing to a follower.
    #[error("hosted Raft group rejected the operation: {0}")]
    Rejected(String),
}

/// Concrete adapter that drives the existing Ready runtime and its application
/// dependencies as one host-managed group.
pub struct ReadyLoopHostedGroup<W, LS, SS, SM, SF>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
    SM: RaftReadyStateMachine,
    SF: RaftSnapshotStore,
{
    ready_loop: RaftReadyLoop<W, LS, SS>,
    state_machine: SM,
    snapshot_store: SF,
    /// Read states already completed by a direct compatibility operation.
    /// Those operations predate [`HostedGroupTurn`] and cannot return read
    /// states themselves, so retain them until the bounded host turn can
    /// publish the group-tagged completion without dropping it.
    deferred_read_states: Vec<ReadState>,
}

/// Remove compatibility-path ReadStates which no longer belong to the exact
/// local leader term. Direct host operations can finish a Ready generation
/// before a later bounded host turn publishes the state; a leadership change
/// in that gap must therefore invalidate the adapter-owned queue as well as
/// the Raft core's pending ReadIndex state.
fn retain_current_leader_read_states(
    states: &mut Vec<ReadState>,
    local_id: raft::types::NodeId,
    leader_id: Option<raft::types::NodeId>,
    current_term: Term,
) {
    states.retain(|state| state.term == current_term && leader_id == Some(local_id));
}

impl<W, LS, SS, SM, SF> ReadyLoopHostedGroup<W, LS, SS, SM, SF>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
    SM: RaftReadyStateMachine,
    SF: RaftSnapshotStore,
{
    pub fn new(
        ready_loop: RaftReadyLoop<W, LS, SS>,
        state_machine: SM,
        snapshot_store: SF,
    ) -> Self {
        Self {
            ready_loop,
            state_machine,
            snapshot_store,
            deferred_read_states: Vec::new(),
        }
    }

    pub fn ready_loop(&self) -> &RaftReadyLoop<W, LS, SS> {
        &self.ready_loop
    }

    fn drain_ready(&mut self) -> Result<HostedGroupTurn, HostedGroupError> {
        let progress = self
            .ready_loop
            .persist_and_apply_next_ready_budgeted(
                &mut self.snapshot_store,
                &mut self.state_machine,
                MultiRaftTurnBudget::default(),
            )
            .map_err(classify_apply_error)?;

        Ok(HostedGroupTurn {
            outbound: progress
                .persistence_safe_messages
                .into_iter()
                .chain(progress.apply_dependent_messages)
                .collect(),
            read_states: progress.read_states,
            ready_generations: progress.ready_generations,
            apply_entries: progress.apply_entries,
            snapshot_bytes: progress.snapshot_bytes,
            persistence: None,
        })
    }

    fn defer_read_states(&mut self, turn: &mut HostedGroupTurn) {
        self.deferred_read_states.append(&mut turn.read_states);
    }

    fn drain_ready_budgeted(
        &mut self,
        budget: MultiRaftTurnBudget,
    ) -> Result<HostedGroupTurn, HostedGroupError> {
        let progress = self
            .ready_loop
            .persist_and_apply_next_ready_budgeted(
                &mut self.snapshot_store,
                &mut self.state_machine,
                budget,
            )
            .map_err(classify_apply_error)?;

        Ok(HostedGroupTurn {
            outbound: progress
                .persistence_safe_messages
                .into_iter()
                .chain(progress.apply_dependent_messages)
                .collect(),
            read_states: progress.read_states,
            ready_generations: progress.ready_generations,
            apply_entries: progress.apply_entries,
            snapshot_bytes: progress.snapshot_bytes,
            persistence: None,
        })
    }

    fn prepare_ready_budgeted(
        &mut self,
        budget: MultiRaftTurnBudget,
    ) -> Result<HostedGroupTurn, HostedGroupError> {
        if !self.deferred_read_states.is_empty() {
            let raft = self.ready_loop.raft();
            retain_current_leader_read_states(
                &mut self.deferred_read_states,
                raft.id(),
                raft.leader_id(),
                raft.hard_state().current_term,
            );
        }

        if !self.deferred_read_states.is_empty() {
            return Ok(HostedGroupTurn {
                read_states: std::mem::take(&mut self.deferred_read_states),
                ..HostedGroupTurn::default()
            });
        }

        if self.ready_loop.has_pending_persistence() {
            let progress = self
                .ready_loop
                .prepare_next_ready_for_batch(&mut self.snapshot_store, budget)
                .map_err(classify_apply_error)?;

            return Ok(HostedGroupTurn {
                outbound: Vec::new(),
                read_states: Vec::new(),
                ready_generations: progress.ready_generations,
                apply_entries: 0,
                snapshot_bytes: progress.snapshot_bytes,
                persistence: progress
                    .request
                    .map(|request| HostedPersistenceBatch::new(request.records)),
            });
        }

        if self.ready_loop.has_pending_apply() {
            if self.ready_loop.apply_backlog_full() {
                return self.drain_ready_budgeted(budget);
            }

            let progress = match self
                .ready_loop
                .prepare_next_ready_for_batch(&mut self.snapshot_store, budget)
            {
                Ok(progress) => progress,
                Err(ReadyApplyError::ApplyBacklogFull { .. }) => {
                    return self.drain_ready_budgeted(budget);
                }
                Err(error) => return Err(classify_apply_error(error)),
            };

            if progress.request.is_some() {
                return Ok(HostedGroupTurn {
                    outbound: Vec::new(),
                    read_states: Vec::new(),
                    ready_generations: progress.ready_generations,
                    apply_entries: 0,
                    snapshot_bytes: progress.snapshot_bytes,
                    persistence: progress
                        .request
                        .map(|request| HostedPersistenceBatch::new(request.records)),
                });
            }

            return self.drain_ready_budgeted(budget);
        }

        let progress = self
            .ready_loop
            .prepare_next_ready_for_batch(&mut self.snapshot_store, budget)
            .map_err(classify_apply_error)?;

        Ok(HostedGroupTurn {
            outbound: Vec::new(),
            read_states: Vec::new(),
            ready_generations: progress.ready_generations,
            apply_entries: 0,
            snapshot_bytes: progress.snapshot_bytes,
            persistence: progress
                .request
                .map(|request| HostedPersistenceBatch::new(request.records)),
        })
    }
}

impl<W, LS, SS, SM, SF> HostedRaftGroup for ReadyLoopHostedGroup<W, LS, SS, SM, SF>
where
    W: RaftWal + Send,
    LS: LogStore<Vec<u8>> + Send,
    SS: StableStore + Send,
    SM: RaftReadyStateMachine + Send,
    SF: RaftSnapshotStore + Send,
{
    fn identity(&self) -> RaftReplicaIdentity {
        self.ready_loop.persistence().log_view().identity()
    }

    fn status(&self) -> MultiRaftGroupStatus {
        let raft = self.ready_loop.raft();
        let identity = self.identity();
        let replication_inflight_bytes = raft
            .conf_state()
            .replication_targets()
            .into_iter()
            .filter_map(|replica_id| raft.progress(replica_id))
            .map(|progress| progress.inflight_bytes)
            .sum();
        let apply_backlog = self.ready_loop.apply_backlog_status();

        MultiRaftGroupStatus {
            identity,
            role: Some((*raft.role()).into()),
            leader_replica_id: raft.leader_id().map(ReplicaId::from_raft),
            term: raft.hard_state().current_term,
            commit_index: raft.commit_index(),
            last_log_index: raft.last_log_index(),
            applied_index: raft.last_applied(),
            snapshot_index: raft.first_log_index().saturating_sub(1),
            uncommitted_bytes: raft.uncommitted_bytes(),
            replication_inflight_bytes,
            pending_work: self.has_pending_work(),
            pending_proposals: 0,
            apply_backlog_entries: apply_backlog.entries,
            apply_backlog_bytes: apply_backlog.bytes,
            apply_backlog_age_ms: apply_backlog.age_ms,
            apply_backlog_generations: apply_backlog.generations,
            pending_messages: 0,
            pending_message_bytes: 0,
            quarantine_reason: None,
            conf_state_version: Some(raft.conf_state().version),
            joining: raft.is_joining(),
            voters: raft
                .conf_state()
                .voters
                .iter()
                .copied()
                .map(ReplicaId::from_raft)
                .collect(),
            learners: raft
                .conf_state()
                .learners
                .iter()
                .copied()
                .map(ReplicaId::from_raft)
                .collect(),
            outgoing_voters: raft
                .conf_state()
                .outgoing_voters
                .iter()
                .copied()
                .map(ReplicaId::from_raft)
                .collect(),
            replica_match_indices: raft
                .replication_match_indices()
                .into_iter()
                .map(|(replica_id, index)| (ReplicaId::from_raft(replica_id), index))
                .collect(),
            pending_conf_change_index: raft.pending_conf_change_index(),
            last_conf_change: raft.last_applied_conf_change(),
            last_removed_replica: raft.last_removed_replica().map(
                |(replica_id, index, term, version)| {
                    (ReplicaId::from_raft(replica_id), index, term, version)
                },
            ),
        }
    }

    fn has_pending_work(&self) -> bool {
        self.ready_loop.has_pending_work() || !self.deferred_read_states.is_empty()
    }

    fn next_timer_delay_ticks(&self) -> Option<u64> {
        let raft = self.ready_loop.raft();
        if raft.is_joining() {
            return None;
        }

        Some(match raft.role() {
            Role::Leader => 3,
            Role::Follower | Role::Candidate => raft.current_election_timeout().max(1),
        })
    }

    fn has_only_deferred_read_states(&self) -> bool {
        !self.ready_loop.has_pending_work() && !self.deferred_read_states.is_empty()
    }

    fn activate_read_index(&mut self, term: Term) -> Result<(), HostedGroupError> {
        if self.has_pending_work() {
            return Err(HostedGroupError::Retryable(
                "a previous Ready generation is still pending".to_string(),
            ));
        }

        self.ready_loop
            .activate_read_index(term)
            .map_err(classify_ready_error)
    }

    fn read_index(&mut self, context: Vec<u8>) -> Result<(), HostedGroupError> {
        if self.has_pending_work() {
            return Err(HostedGroupError::Retryable(
                "a previous Ready generation is still pending".to_string(),
            ));
        }

        self.ready_loop
            .read_index(context)
            .map_err(classify_ready_error)
    }

    fn propose_conf_change(
        &mut self,
        change: ConfChange,
    ) -> Result<(LogIndex, Vec<RaftMessageEnvelope>), HostedGroupError> {
        if self.ready_loop.has_pending_persistence() {
            return Err(HostedGroupError::Retryable(
                "a shared-WAL batch is still awaiting completion".to_string(),
            ));
        }

        let mut turn = self.drain_ready()?;
        self.defer_read_states(&mut turn);

        if self.ready_loop.has_pending_work() {
            return Err(HostedGroupError::Retryable(
                "a previous Ready generation is still being resumed".to_string(),
            ));
        }

        let index = self
            .ready_loop
            .propose_conf_change(change)
            .map_err(classify_ready_error)?;
        let mut after_proposal = self.drain_ready()?;
        self.defer_read_states(&mut after_proposal);
        turn.outbound.extend(after_proposal.outbound);

        Ok((index, turn.outbound))
    }

    fn transfer_leadership(
        &mut self,
        target: ReplicaId,
        timeout_ticks: u64,
    ) -> Result<(LeadershipTransferStatus, Vec<RaftMessageEnvelope>), HostedGroupError> {
        if self.ready_loop.has_pending_persistence() {
            return Err(HostedGroupError::Retryable(
                "a shared-WAL batch is still awaiting completion".to_string(),
            ));
        }

        let mut turn = self.drain_ready()?;
        self.defer_read_states(&mut turn);

        if self.ready_loop.has_pending_work() {
            return Err(HostedGroupError::Retryable(
                "a previous Ready generation is still being resumed".to_string(),
            ));
        }

        let target = target.to_raft().map_err(|reason| {
            HostedGroupError::Rejected(format!("invalid leadership-transfer target: {reason}"))
        })?;
        let status = self
            .ready_loop
            .transfer_leadership(target, timeout_ticks)
            .map_err(classify_ready_error)?;

        let mut after_transfer = self.drain_ready()?;
        self.defer_read_states(&mut after_transfer);
        turn.outbound.extend(after_transfer.outbound);

        Ok((status, turn.outbound))
    }

    fn tick_and_drain(&mut self, ticks: u64) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
        if self.ready_loop.has_pending_persistence() {
            return Err(HostedGroupError::Retryable(
                "a shared-WAL batch is still awaiting completion".to_string(),
            ));
        }

        // A previous retryable persistence operation may have left a Ready
        // generation pending. Finish it before mutating Raft again.
        let mut turn = self.drain_ready()?;
        self.defer_read_states(&mut turn);

        if turn.ready_generations > 0 || self.ready_loop.has_pending_work() {
            return Ok(turn.outbound);
        }

        self.ready_loop.tick(ticks).map_err(classify_ready_error)?;

        let mut after_tick = self.drain_ready()?;
        self.defer_read_states(&mut after_tick);
        turn.outbound.extend(after_tick.outbound);

        Ok(turn.outbound)
    }

    fn step_and_drain(
        &mut self,
        message: RaftMessageEnvelope,
    ) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
        if self.ready_loop.has_pending_persistence() {
            return Err(HostedGroupError::Retryable(
                "a shared-WAL batch is still awaiting completion".to_string(),
            ));
        }

        let mut turn = self.drain_ready()?;
        self.defer_read_states(&mut turn);

        if self.ready_loop.has_pending_work() {
            return Err(HostedGroupError::Retryable(
                "a previous Ready generation is still being resumed".to_string(),
            ));
        }

        self.ready_loop
            .step(message)
            .map_err(classify_ready_error)?;

        let mut after_step = self.drain_ready()?;
        self.defer_read_states(&mut after_step);
        turn.outbound.extend(after_step.outbound);

        Ok(turn.outbound)
    }

    fn propose_and_drain(
        &mut self,
        command: Vec<u8>,
        encoded_len: usize,
    ) -> Result<(LogIndex, Vec<RaftMessageEnvelope>), HostedGroupError> {
        if self.ready_loop.has_pending_persistence() {
            return Err(HostedGroupError::Retryable(
                "a shared-WAL batch is still awaiting completion".to_string(),
            ));
        }

        let mut turn = self.drain_ready()?;
        self.defer_read_states(&mut turn);

        if self.ready_loop.has_pending_work() {
            return Err(HostedGroupError::Retryable(
                "a previous Ready generation is still being resumed".to_string(),
            ));
        }

        let index = self
            .ready_loop
            .propose(command, encoded_len)
            .map_err(classify_ready_error)?;

        let mut after_proposal = self.drain_ready()?;
        self.defer_read_states(&mut after_proposal);
        turn.outbound.extend(after_proposal.outbound);

        Ok((index, turn.outbound))
    }

    fn tick_and_prepare_budgeted(
        &mut self,
        ticks: u64,
        budget: MultiRaftTurnBudget,
    ) -> Result<HostedGroupTurn, HostedGroupError> {
        let mut turn = self.prepare_ready_budgeted(budget)?;

        if !turn.read_states.is_empty()
            || turn.persistence.is_some()
            || turn.ready_generations > 0
            || self.ready_loop.has_pending_work()
        {
            // A deferred ReadState is valid only for the leadership term that
            // produced it. Do not advance the clock after selecting it: a
            // same-turn check-quorum transition could otherwise invalidate
            // the state after it has already been returned to the host.
            return Ok(turn);
        }

        self.ready_loop.tick(ticks).map_err(classify_ready_error)?;
        let after_tick = self.prepare_ready_budgeted(budget);
        match after_tick {
            Ok(after_tick) => {
                turn.outbound.extend(after_tick.outbound);
                turn.read_states.extend(after_tick.read_states);
                turn.ready_generations += after_tick.ready_generations;
                turn.apply_entries += after_tick.apply_entries;
                turn.snapshot_bytes += after_tick.snapshot_bytes;
                turn.persistence = after_tick.persistence;
                Ok(turn)
            }
            Err(error) => Err(error),
        }
    }

    fn step_and_prepare_budgeted(
        &mut self,
        message: RaftMessageEnvelope,
        budget: MultiRaftTurnBudget,
    ) -> Result<HostedGroupTurn, HostedGroupError> {
        // A direct compatibility operation may have completed a ReadIndex and
        // retained its state for publication by the next bounded host turn.
        // Process the queued Raft message before exposing that state: a higher
        // term or a new leader can invalidate it, and publishing it first
        // would make a same-turn stale-read result observable.
        if !self.ready_loop.has_pending_work() && !self.deferred_read_states.is_empty() {
            self.ready_loop
                .step(message)
                .map_err(classify_ready_error)?;
            return self.prepare_ready_budgeted(budget);
        }

        let mut turn = self.prepare_ready_budgeted(budget)?;

        if turn.persistence.is_some()
            || turn.ready_generations > 0
            || self.ready_loop.has_pending_work()
        {
            return Ok(turn);
        }

        self.ready_loop
            .step(message)
            .map_err(classify_ready_error)?;
        let after_step = self.prepare_ready_budgeted(budget)?;
        turn.outbound.extend(after_step.outbound);
        turn.read_states.extend(after_step.read_states);
        turn.ready_generations += after_step.ready_generations;
        turn.apply_entries += after_step.apply_entries;
        turn.snapshot_bytes += after_step.snapshot_bytes;
        turn.persistence = after_step.persistence;
        Ok(turn)
    }

    fn complete_persistence(
        &mut self,
        outcome: Result<BatchAppendResult, BatchAppendFailure>,
        budget: MultiRaftTurnBudget,
    ) -> Result<HostedGroupTurn, HostedGroupError> {
        let progress = self
            .ready_loop
            .complete_prepared_ready(outcome, &mut self.state_machine, budget)
            .map_err(classify_apply_error)?;

        Ok(HostedGroupTurn {
            outbound: progress
                .persistence_safe_messages
                .into_iter()
                .chain(progress.apply_dependent_messages)
                .collect(),
            read_states: progress.read_states,
            ready_generations: 0,
            apply_entries: progress.apply_entries,
            snapshot_bytes: progress.snapshot_bytes,
            persistence: None,
        })
    }
}

/// Convert one Ready-loop failure into the physical host failure domain.
///
/// Recovery-required variants are matched recursively because the Raft core
/// can report the same irreversible condition through tick, step, proposal,
/// snapshot-install, or Ready acknowledgement APIs.
pub fn classify_ready_error(error: ReadyLoopError) -> HostedGroupError {
    match error {
        ReadyLoopError::RecoveryRequired
        | ReadyLoopError::Tick(RaftError::RecoveryRequired)
        | ReadyLoopError::Step(StepError::RecoveryRequired)
        | ReadyLoopError::Proposal(ProposeError::RecoveryRequired)
        | ReadyLoopError::LeadershipTransfer(LeadershipTransferError::RecoveryRequired)
        | ReadyLoopError::ReadIndex(ReadIndexError::RecoveryRequired)
        | ReadyLoopError::SnapshotInstall(SnapshotInstallError::RecoveryRequired)
        | ReadyLoopError::Advance(AdvanceError::RecoveryRequired) => {
            HostedGroupError::RecoveryRequired
        }

        ReadyLoopError::PendingReady | ReadyLoopError::RetryablePersistence(_) => {
            HostedGroupError::Retryable(error.to_string())
        }

        ReadyLoopError::LeadershipTransfer(
            LeadershipTransferError::JointConsensusInProgress
            | LeadershipTransferError::ConfigurationChangePending
            | LeadershipTransferError::TransferInProgress { .. }
            | LeadershipTransferError::TargetProgressUnavailable(_)
            | LeadershipTransferError::TargetNotCaughtUp { .. },
        ) => HostedGroupError::Retryable(error.to_string()),

        ReadyLoopError::Proposal(_)
        | ReadyLoopError::LeadershipTransfer(_)
        | ReadyLoopError::ReadIndex(_)
        | ReadyLoopError::Step(_) => HostedGroupError::Rejected(error.to_string()),

        other => HostedGroupError::Group(other.to_string()),
    }
}

fn classify_apply_error(error: ReadyApplyError) -> HostedGroupError {
    match error {
        ReadyApplyError::Ready(error) => classify_ready_error(error),

        ReadyApplyError::ApplyBacklogFull { .. } => HostedGroupError::Retryable(error.to_string()),

        // Snapshot verification/restoration, application, or committed-entry
        // ordering errors occur after a durable consensus boundary and isolate
        // this replica until restart/recovery.
        other => HostedGroupError::Group(other.to_string()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostState {
    Registering,
    Active,
    RecoveryRequired,
}

/// Minimum physical-node multi-Raft host.
///
/// All recovered identities must be registered before [`Self::activate`]. The
/// caller must obtain each group's writer through `NodeRaftWal::group_writer_for`
/// before activation; activation seals that registration so retention always
/// considers every replica represented in the shared WAL.
pub struct MultiRaftHost<W>
where
    W: RaftWal + Send + 'static,
{
    node_id: NodeId,

    /// The one physical Raft persistence authority for this node.
    node_wal: NodeRaftWal<W>,

    /// Node-local admission limits for work already handed to the scheduler.
    config: MultiRaftHostConfig,

    state: HostState,

    groups: BTreeMap<RaftGroupId, Box<dyn HostedRaftGroup>>,

    /// Fair FIFO work queue. Membership is deduplicated so repeated wakeups
    /// cannot turn one group into an unbounded sequence of adjacent turns.
    runnable: RunnableGroupQueue,

    /// One logical timer wheel shared by all local groups. Timer expiration
    /// only makes a group runnable; it never bypasses the group-turn budget.
    timers: GroupTimerScheduler,

    /// Prevents a scheduled wall-clock tick from being advanced twice when
    /// the caller schedules due groups and then executes their bounded turn.
    timer_advance_pending: bool,

    /// Timer work is tracked separately from ordinary queued messages. This
    /// preserves the Raft clock event when a group also has inbound traffic;
    /// a message must not accidentally consume the only turn for an expired
    /// election or heartbeat deadline.
    timer_due: BTreeMap<RaftGroupId, u64>,

    /// Inbound messages remain owned by their tagged group until that group is
    /// serviced. Control messages are kept ahead of bulk append traffic within
    /// the same group, but no message is acknowledged by merely queueing it.
    pending_messages: BTreeMap<RaftGroupId, PendingGroupMessages>,

    pending_message_count: usize,
    pending_message_bytes: usize,

    /// Durable identities discovered by the one shared-WAL scan which have
    /// not yet been accounted for by startup reconstruction.
    pending_recovered: BTreeSet<RaftReplicaIdentity>,

    /// Identities for which this host issued an identity-bound writer.
    ///
    /// A hosted group cannot be registered without first obtaining its WAL
    /// handle from the same NodeRaftWal owned by this host.
    issued_writers: BTreeSet<RaftReplicaIdentity>,

    /// Durable lifecycle tombstones mirrored into the live host admission
    /// boundary. A tombstoned identity is fenced before group lookup so stale
    /// messages cannot turn an absent group into a new allocation.
    tombstoned: BTreeSet<RaftReplicaIdentity>,

    /// Permanently isolated groups for this process lifetime.
    ///
    /// Group-local corruption/apply/snapshot failures do not stop unrelated
    /// Raft groups. Shared-WAL uncertainty still moves the entire host into
    /// RecoveryRequired.
    quarantined: BTreeMap<RaftGroupId, String>,

    /// The single bounded preparation queue used to assemble the next
    /// cross-group A-WAL batch. It shares the node-wide WAL state with the
    /// registration handle above but is the only host path that appends the
    /// prepared cross-group batch.
    persistence: PersistenceService<W>,
}

impl<W> MultiRaftHost<W>
where
    W: RaftWal + Send + 'static,
{
    pub fn new(node_id: NodeId, node_wal: NodeRaftWal<W>) -> Self {
        Self::new_with_config(node_id, node_wal, MultiRaftHostConfig::default())
            .expect("MultiRaftHostConfig::default must be valid")
    }

    pub fn new_with_config(
        node_id: NodeId,
        node_wal: NodeRaftWal<W>,
        config: MultiRaftHostConfig,
    ) -> Result<Self, MultiRaftHostError> {
        config.validate()?;
        let persistence = PersistenceService::new(node_wal.clone(), config);

        Ok(Self {
            node_id,
            node_wal,
            config,
            state: HostState::Registering,
            groups: BTreeMap::new(),
            runnable: RunnableGroupQueue::default(),
            timers: GroupTimerScheduler::default(),
            timer_advance_pending: false,
            timer_due: BTreeMap::new(),
            pending_messages: BTreeMap::new(),
            pending_message_count: 0,
            pending_message_bytes: 0,
            pending_recovered: BTreeSet::new(),
            issued_writers: BTreeSet::new(),
            tombstoned: BTreeSet::new(),
            quarantined: BTreeMap::new(),
            persistence,
        })
    }

    pub fn from_recovered(
        node_id: NodeId,
        node_wal: NodeRaftWal<W>,
        recovered: &RecoveredRaftStorage,
    ) -> Self {
        Self::from_recovered_with_config(
            node_id,
            node_wal,
            recovered,
            MultiRaftHostConfig::default(),
        )
        .expect("MultiRaftHostConfig::default must be valid")
    }

    pub fn from_recovered_with_config(
        node_id: NodeId,
        node_wal: NodeRaftWal<W>,
        recovered: &RecoveredRaftStorage,
        config: MultiRaftHostConfig,
    ) -> Result<Self, MultiRaftHostError> {
        config.validate()?;
        let persistence = PersistenceService::new(node_wal.clone(), config);

        Ok(Self {
            node_id,
            node_wal,
            config,
            state: HostState::Registering,
            groups: BTreeMap::new(),
            runnable: RunnableGroupQueue::default(),
            timers: GroupTimerScheduler::default(),
            timer_advance_pending: false,
            timer_due: BTreeMap::new(),
            pending_messages: BTreeMap::new(),
            pending_message_count: 0,
            pending_message_bytes: 0,
            pending_recovered: recovered
                .replicas()
                .map(|(identity, _)| *identity)
                .collect(),
            issued_writers: BTreeSet::new(),
            tombstoned: BTreeSet::new(),
            quarantined: BTreeMap::new(),
            persistence,
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Clone the node-wide WAL authority.
    ///
    /// Clones share the same physical persistence state and recovery fence.
    pub fn node_wal(&self) -> NodeRaftWal<W> {
        self.node_wal.clone()
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    pub fn group_failure(&self, raft_group_id: RaftGroupId) -> Option<&str> {
        self.quarantined.get(&raft_group_id).map(String::as_str)
    }

    pub fn config(&self) -> MultiRaftHostConfig {
        self.config
    }

    /// Capture a point-in-time view of every local group and scheduler queue.
    ///
    /// This method is observational: it does not acquire a persistence handle,
    /// advance a Ready generation, or change runnable-queue membership. A
    /// caller may therefore publish the returned value to an admin endpoint
    /// without weakening the host's durability boundary.
    pub fn status(&self) -> MultiRaftHostStatus {
        let state = match self.state {
            HostState::Registering => MultiRaftHostState::Registering,
            HostState::Active => MultiRaftHostState::Active,
            HostState::RecoveryRequired => MultiRaftHostState::RecoveryRequired,
        };
        let persistence = self.persistence.status();

        let groups = self
            .groups
            .iter()
            .map(|(raft_group_id, group)| {
                let mut status = group.status();
                let pending = self.pending_messages.get(raft_group_id);
                status.pending_messages = pending.map_or(0, PendingGroupMessages::len);
                status.pending_message_bytes = pending.map_or(0, PendingGroupMessages::wire_bytes);
                status.quarantine_reason = self.quarantined.get(raft_group_id).cloned();
                status
            })
            .collect();

        MultiRaftHostStatus {
            node_id: self.node_id,
            state,
            pending_message_count: self.pending_message_count,
            pending_message_bytes: self.pending_message_bytes,
            pending_persistence_groups: persistence.pending_groups,
            pending_persistence_records: persistence.pending_records,
            pending_persistence_bytes: persistence.pending_bytes,
            groups,
        }
    }

    pub fn register_new_group(
        &mut self,
        group: Box<dyn HostedRaftGroup>,
    ) -> Result<(), MultiRaftHostError> {
        self.register_group(group, false)
    }

    pub fn register_recovered_group(
        &mut self,
        group: Box<dyn HostedRaftGroup>,
    ) -> Result<(), MultiRaftHostError> {
        self.register_group(group, true)
    }

    /// Records a recovered replica lifetime which must remain in shared-WAL
    /// retention accounting but has no active runtime after membership
    /// reconstruction (for example, a removed predecessor replica).
    ///
    /// The caller must still have registered this identity with
    /// `NodeRaftWal::group_writer_for` before activation. Keeping retired
    /// identities separate from the active group map avoids conflating a
    /// Raft-group ID with a replica lifetime.
    pub fn register_inactive_recovered_identity(
        &mut self,
        identity: RaftReplicaIdentity,
    ) -> Result<(), MultiRaftHostError> {
        self.ensure_registering()?;
        self.ensure_shared_wal_healthy()?;
        if !self.issued_writers.contains(&identity) {
            return Err(MultiRaftHostError::WalWriterNotIssued(identity));
        }
        if self.pending_recovered.remove(&identity) {
            Ok(())
        } else {
            Err(MultiRaftHostError::UnexpectedRecovered(identity))
        }
    }

    /// Register a durable tombstone discovered during startup recovery.
    ///
    /// The identity-bound writer must be issued first so the shared-WAL
    /// retention registry accounts for the lifetime until cleanup advances its
    /// floor. The operation is idempotent and consumes a matching recovered
    /// identity when one was present in the scan.
    pub fn register_tombstoned_identity(
        &mut self,
        identity: RaftReplicaIdentity,
    ) -> Result<(), MultiRaftHostError> {
        self.ensure_registering()?;
        self.ensure_shared_wal_healthy()?;
        if !self.issued_writers.contains(&identity) {
            return Err(MultiRaftHostError::WalWriterNotIssued(identity));
        }
        self.pending_recovered.remove(&identity);
        self.tombstoned.insert(identity);
        Ok(())
    }

    fn register_group(
        &mut self,
        group: Box<dyn HostedRaftGroup>,
        recovered: bool,
    ) -> Result<(), MultiRaftHostError> {
        self.ensure_registering()?;
        self.ensure_shared_wal_healthy()?;
        let identity = group.identity();
        if self.tombstoned.contains(&identity) {
            return Err(MultiRaftHostError::TombstonedReplica(identity));
        }
        if !self.issued_writers.contains(&identity) {
            return Err(MultiRaftHostError::WalWriterNotIssued(identity));
        }
        if self.groups.contains_key(&identity.raft_group_id) {
            return Err(MultiRaftHostError::DuplicateGroup(identity.raft_group_id));
        }
        if recovered {
            if !self.pending_recovered.remove(&identity) {
                return Err(MultiRaftHostError::UnexpectedRecovered(identity));
            }
        } else if self.pending_recovered.contains(&identity) {
            return Err(MultiRaftHostError::RecoveredIdentityRequiresRecovery(
                identity,
            ));
        }
        self.groups.insert(identity.raft_group_id, group);
        Ok(())
    }

    /// Issue the only valid persistence handle for one replica lifetime.
    ///
    /// The caller must use this handle to construct the corresponding hosted
    /// group. Registration without prior issuance is rejected.
    pub fn issue_group_writer(
        &mut self,
        identity: RaftReplicaIdentity,
    ) -> Result<NodeRaftWalHandle<W>, MultiRaftHostError> {
        self.ensure_registering()?;
        self.ensure_shared_wal_healthy()?;

        if self.tombstoned.contains(&identity) {
            return Err(MultiRaftHostError::TombstonedReplica(identity));
        }

        if !self.issued_writers.insert(identity) {
            return Err(MultiRaftHostError::WalWriterAlreadyIssued(identity));
        }

        self.node_wal
            .group_writer_for(identity)
            .map_err(MultiRaftHostError::WalRegistration)
    }

    /// Issue a writer for a metadata-created replica after the host is live.
    ///
    /// The host remains the sole owner of writer issuance. The new identity is
    /// inserted into shared-WAL retention with an empty floor before any
    /// tablet worker is started, preventing a concurrent prune from deleting
    /// the prefix that the worker will need to recover or publish.
    pub fn issue_group_writer_after_activation(
        &mut self,
        identity: RaftReplicaIdentity,
    ) -> Result<NodeRaftWalHandle<W>, MultiRaftHostError> {
        self.ensure_active()?;
        self.ensure_shared_wal_healthy()?;

        if self.tombstoned.contains(&identity) {
            return Err(MultiRaftHostError::TombstonedReplica(identity));
        }

        if !self.issued_writers.insert(identity) {
            return Err(MultiRaftHostError::WalWriterAlreadyIssued(identity));
        }

        match self.node_wal.group_writer_for_active(identity) {
            Ok(writer) => Ok(writer),
            Err(error) => {
                self.issued_writers.remove(&identity);
                Err(MultiRaftHostError::WalRegistration(error))
            }
        }
    }

    /// Register one fully constructed metadata tablet while the host is
    /// active and make it runnable on the next bounded scheduler turn.
    pub fn register_active_group(
        &mut self,
        group: Box<dyn HostedRaftGroup>,
    ) -> Result<(), MultiRaftHostError> {
        self.ensure_active()?;
        self.ensure_shared_wal_healthy()?;
        let identity = group.identity();
        if self.tombstoned.contains(&identity) {
            return Err(MultiRaftHostError::TombstonedReplica(identity));
        }
        if !self.issued_writers.contains(&identity) {
            return Err(MultiRaftHostError::WalWriterNotIssued(identity));
        }
        if self.groups.contains_key(&identity.raft_group_id) {
            return Err(MultiRaftHostError::DuplicateGroup(identity.raft_group_id));
        }
        let group_id = identity.raft_group_id;
        self.groups.insert(group_id, group);
        self.timers.schedule_after(group_id, 1);
        self.runnable.enqueue(group_id);
        Ok(())
    }

    /// Detach one active group and fence its replica lifetime permanently.
    ///
    /// Registry durability is owned by the server lifecycle manager and must
    /// be committed before this method is called. Removing the scheduler and
    /// pending-message state here makes the host safe to continue serving
    /// unrelated groups while delayed envelopes receive a typed tombstone
    /// error.
    pub fn tombstone_group(
        &mut self,
        identity: RaftReplicaIdentity,
    ) -> Result<(), MultiRaftHostError> {
        self.ensure_active()?;
        self.ensure_shared_wal_healthy()?;
        if self.tombstoned.contains(&identity) {
            return Ok(());
        }
        if !self.issued_writers.contains(&identity) {
            return Err(MultiRaftHostError::WalWriterNotIssued(identity));
        }

        if let Some(group) = self.groups.get(&identity.raft_group_id)
            && group.identity() != identity
        {
            return Err(MultiRaftHostError::ReplicaIdentityMismatch {
                raft_group_id: identity.raft_group_id,
                expected: group.identity(),
                received: identity,
            });
        }

        self.groups.remove(&identity.raft_group_id);
        self.runnable.remove(identity.raft_group_id);
        self.timers.remove(identity.raft_group_id);
        self.timer_due.remove(&identity.raft_group_id);
        self.remove_pending_group(identity.raft_group_id);
        self.quarantined.remove(&identity.raft_group_id);
        self.tombstoned.insert(identity);
        Ok(())
    }

    /// Seals local replica discovery and permits Ready processing.
    pub fn activate(&mut self) -> Result<(), MultiRaftHostError> {
        self.ensure_registering()?;
        self.ensure_shared_wal_healthy()?;
        if !self.pending_recovered.is_empty() {
            return Err(MultiRaftHostError::MissingRecovered(
                self.pending_recovered.iter().copied().collect(),
            ));
        }
        self.node_wal
            .seal_retention_registry()
            .map_err(MultiRaftHostError::RetentionRegistry)?;
        self.state = HostState::Active;
        Ok(())
    }

    /// Makes one registered group eligible for the next host turn.
    pub fn schedule_group_now(
        &mut self,
        raft_group_id: RaftGroupId,
    ) -> Result<(), MultiRaftHostError> {
        self.ensure_active()?;
        self.ensure_schedulable_group(raft_group_id)?;
        self.runnable.enqueue(raft_group_id);
        Ok(())
    }

    /// Schedules one registered group after a number of logical host ticks.
    ///
    /// The deadline is coalesced with an earlier deadline for the same group;
    /// a later wakeup must never postpone an already-due Raft timer.
    pub fn schedule_group_after(
        &mut self,
        raft_group_id: RaftGroupId,
        delay: u64,
    ) -> Result<(), MultiRaftHostError> {
        self.ensure_active()?;
        self.ensure_schedulable_group(raft_group_id)?;
        self.timers.schedule_after(raft_group_id, delay);
        Ok(())
    }

    /// Seed the event scheduler for groups admitted during startup. This is a
    /// one-time lifecycle action; explicit per-group deadlines remain
    /// authoritative and are never postponed by the startup seed.
    pub fn schedule_all_groups_after(&mut self, delay: u64) -> Result<(), MultiRaftHostError> {
        self.ensure_active()?;
        let group_ids = self.groups.keys().copied().collect::<Vec<_>>();
        for raft_group_id in group_ids {
            self.timers.schedule_after(raft_group_id, delay);
        }
        Ok(())
    }

    /// Advance the event scheduler and enqueue only groups whose logical Raft
    /// timer expired. The production host loop uses this boundary instead of
    /// enumerating every registered group on every wall-clock tick.
    pub fn schedule_due_ticks(&mut self, ticks: u64) -> Result<(), MultiRaftHostError> {
        self.ensure_active()?;
        for (raft_group_id, due_ticks) in self.timers.advance(ticks) {
            if self.groups.contains_key(&raft_group_id)
                && !self.quarantined.contains_key(&raft_group_id)
            {
                self.timer_due
                    .entry(raft_group_id)
                    .and_modify(|pending| *pending = pending.saturating_add(due_ticks))
                    .or_insert(due_ticks);
                self.runnable.enqueue_control(raft_group_id);
            }
        }
        self.timer_advance_pending = true;
        Ok(())
    }

    /// Enables the Raft core's ReadIndex path for one hosted group after the
    /// caller has applied that group's current-term activation entry.
    ///
    /// The host owns this admission boundary so a caller cannot bypass group
    /// quarantine, shared-WAL health, or the per-group Ready ordering. The
    /// operation does not itself create a read result; a later
    /// [`Self::run_turn`] delivers any completed [`ReadState`] values.
    pub fn activate_read_index(
        &mut self,
        raft_group_id: RaftGroupId,
        term: Term,
    ) -> Result<(), MultiRaftHostError> {
        self.ensure_active()?;
        self.ensure_schedulable_group(raft_group_id)?;

        let result = self
            .groups
            .get_mut(&raft_group_id)
            .expect("schedulable group must be registered")
            .activate_read_index(term);

        self.finish_group_admission(raft_group_id, result)
    }

    /// Admits one opaque quorum-confirmed read context for a hosted group.
    ///
    /// Admission is asynchronous. The group is scheduled immediately so its
    /// outbound ReadIndex messages and, after quorum confirmation, its
    /// persisted/applied Ready read states flow through the ordinary host
    /// turn. The caller correlates completion using the returned context in
    /// [`MultiRaftTurnResult::read_states`].
    pub fn read_index(
        &mut self,
        raft_group_id: RaftGroupId,
        context: Vec<u8>,
    ) -> Result<(), MultiRaftHostError> {
        self.ensure_active()?;
        self.ensure_schedulable_group(raft_group_id)?;

        let result = self
            .groups
            .get_mut(&raft_group_id)
            .expect("schedulable group must be registered")
            .read_index(context);

        self.finish_group_admission(raft_group_id, result)
    }

    fn account_pending_addition(&mut self, wire_bytes: usize) {
        self.pending_message_count = self.pending_message_count.saturating_add(1);
        self.pending_message_bytes = self.pending_message_bytes.saturating_add(wire_bytes);
    }

    fn account_pending_removal(&mut self, wire_bytes: usize) {
        self.pending_message_count = self.pending_message_count.saturating_sub(1);
        self.pending_message_bytes = self.pending_message_bytes.saturating_sub(wire_bytes);
    }

    fn account_pending_group_removal(&mut self, pending: &PendingGroupMessages) {
        self.pending_message_count = self.pending_message_count.saturating_sub(pending.len());
        self.pending_message_bytes = self
            .pending_message_bytes
            .saturating_sub(pending.wire_bytes());
    }

    fn remove_pending_group(&mut self, raft_group_id: RaftGroupId) {
        if let Some(pending) = self.pending_messages.remove(&raft_group_id) {
            self.account_pending_group_removal(&pending);
        }
    }

    /// Queues a tagged inbound message for bounded processing by its group.
    ///
    /// Validation happens at admission so an unknown group or wrong recipient
    /// cannot occupy scheduler capacity. Processing and Ready completion still
    /// happen only from [`Self::run_turn`].
    pub fn enqueue_message(
        &mut self,
        message: RoutedRaftMessage,
    ) -> Result<(), MultiRaftHostError> {
        self.ensure_active()?;
        let raft_group_id = message.raft_group_id;
        self.validate_message(&message)?;
        let control = is_control_message(&message.envelope);
        let wire_bytes = crate::transport::routed_message_wire_size(&message)
            .map_err(|error| MultiRaftHostError::InvalidMessage(error.to_string()))?;

        let (group_messages, group_bytes) = self
            .pending_messages
            .get(&raft_group_id)
            .map(|pending| (pending.len(), pending.wire_bytes()))
            .unwrap_or_default();
        if group_messages >= self.config.max_pending_group_messages
            || group_bytes.saturating_add(wire_bytes) > self.config.max_pending_group_message_bytes
        {
            return Err(MultiRaftHostError::PendingMessagesFull {
                raft_group_id,
                reason: "per-group pending-message limit reached".to_string(),
            });
        }
        if self.pending_message_count >= self.config.max_pending_messages
            || self.pending_message_bytes.saturating_add(wire_bytes)
                > self.config.max_pending_message_bytes
        {
            return Err(MultiRaftHostError::PendingMessagesFull {
                raft_group_id,
                reason: "node pending-message limit reached".to_string(),
            });
        }

        self.pending_messages
            .entry(raft_group_id)
            .or_default()
            .push_back(PendingMessage {
                message,
                wire_bytes,
            });
        self.account_pending_addition(wire_bytes);
        if control {
            self.runnable.enqueue_control(raft_group_id);
        } else {
            self.runnable.enqueue(raft_group_id);
        }
        Ok(())
    }

    /// Runs a bounded, fair host turn.
    ///
    /// Timer advancement is independent from work admission. A hot group can
    /// therefore remain runnable without preventing other queued groups from
    /// receiving a turn, and an inbound message remains queued when the
    /// message budget is exhausted. Group-local failures quarantine only that
    /// group; shared-WAL uncertainty fences the complete host.
    pub fn run_turn(
        &mut self,
        ticks: u64,
        budget: MultiRaftTurnBudget,
    ) -> Result<MultiRaftTurnResult, MultiRaftHostError> {
        self.ensure_active()?;
        self.persistence.bind_host_thread();

        if !self.timer_advance_pending {
            for (raft_group_id, due_ticks) in self.timers.advance(ticks) {
                if self.groups.contains_key(&raft_group_id)
                    && !self.quarantined.contains_key(&raft_group_id)
                {
                    self.timer_due
                        .entry(raft_group_id)
                        .and_modify(|pending| *pending = pending.saturating_add(due_ticks))
                        .or_insert(due_ticks);
                    self.runnable.enqueue_control(raft_group_id);
                }
            }
        }
        self.timer_advance_pending = false;

        let mut result = MultiRaftTurnResult::default();
        let mut persistence_error = None;

        while result.groups_serviced < budget.max_groups {
            let Some(raft_group_id) = self.runnable.pop() else {
                break;
            };

            if self.quarantined.contains_key(&raft_group_id) {
                self.timer_due.remove(&raft_group_id);
                self.remove_pending_group(raft_group_id);
                continue;
            }

            let timer_due_ticks = self.timer_due.remove(&raft_group_id);
            let timer_due = timer_due_ticks.is_some();
            let group_timer_ticks = timer_due_ticks.unwrap_or(0);

            let has_queued_message = self
                .pending_messages
                .get(&raft_group_id)
                .is_some_and(|pending| !pending.is_empty());
            let mut pending_message = if budget.max_messages == 0 {
                None
            } else {
                self.pending_messages
                    .get_mut(&raft_group_id)
                    .and_then(PendingGroupMessages::pop)
            };
            let had_message = pending_message.is_some();
            if let Some(message) = pending_message.as_ref() {
                self.account_pending_removal(message.wire_bytes);
            }

            if had_message && result.messages_processed >= budget.max_messages {
                let control = is_control_message(
                    &pending_message
                        .as_ref()
                        .expect("message was checked above")
                        .message
                        .envelope,
                );
                let message = pending_message.expect("message was checked above");
                let wire_bytes = message.wire_bytes;
                self.pending_messages
                    .get_mut(&raft_group_id)
                    .expect("popped message implies a group queue")
                    .push_front(message);
                self.account_pending_addition(wire_bytes);
                if timer_due {
                    self.timer_due
                        .entry(raft_group_id)
                        .and_modify(|pending| *pending = pending.saturating_add(group_timer_ticks))
                        .or_insert(group_timer_ticks);
                }
                if control {
                    self.runnable.enqueue_control(raft_group_id);
                } else {
                    self.runnable.enqueue(raft_group_id);
                }
                break;
            }

            let group_has_pending_work = self
                .groups
                .get(&raft_group_id)
                .is_some_and(|group| group.has_pending_work());
            let group_only_deferred_read_states = self
                .groups
                .get(&raft_group_id)
                .is_some_and(|group| group.has_only_deferred_read_states());

            if group_has_pending_work && self.persistence.contains_group(raft_group_id) {
                // A prepared Ready generation is already owned by the
                // persistence service. An inbound message or timer may have
                // made this group runnable again, but preparing the same
                // generation a second time would produce duplicate completion
                // and could fence a healthy node as if its Ready were lost.
                if let Some(message) = pending_message.take() {
                    let wire_bytes = message.wire_bytes;
                    self.pending_messages
                        .get_mut(&raft_group_id)
                        .expect("popped message implies a group queue")
                        .push_front(message);
                    self.account_pending_addition(wire_bytes);
                }
                if timer_due {
                    self.timer_due
                        .entry(raft_group_id)
                        .and_modify(|pending| *pending = pending.saturating_add(group_timer_ticks))
                        .or_insert(group_timer_ticks);
                }
                continue;
            }

            if has_queued_message && group_only_deferred_read_states && !had_message && !timer_due {
                // A zero message budget leaves the control message queued.
                // Do not publish a deferred ReadState while that message is
                // waiting: it may carry a newer term which invalidates the
                // state. Preserve the queue lane and give other groups a
                // chance to use this turn's non-message budget.
                let control = self
                    .pending_messages
                    .get(&raft_group_id)
                    .is_some_and(PendingGroupMessages::has_control);
                if control {
                    self.runnable.enqueue_control(raft_group_id);
                } else {
                    self.runnable.enqueue(raft_group_id);
                }
                result.groups_serviced += 1;
                continue;
            }

            let process_message = had_message
                && !timer_due
                && (!group_has_pending_work || group_only_deferred_read_states);
            let rearm_timer = timer_due || process_message;
            let retry_message = process_message.then(|| {
                pending_message
                    .as_ref()
                    .expect("a processed message must be present")
                    .clone()
            });

            if had_message && !process_message {
                let message = pending_message.take().expect("message was checked above");
                let wire_bytes = message.wire_bytes;
                self.pending_messages
                    .get_mut(&raft_group_id)
                    .expect("popped message implies a group queue")
                    .push_front(message);
                self.account_pending_addition(wire_bytes);
            }

            let group_budget = MultiRaftTurnBudget {
                max_groups: budget.max_groups.saturating_sub(result.groups_serviced),
                max_messages: budget
                    .max_messages
                    .saturating_sub(result.messages_processed),
                // These limits are per group. Keeping the same bounded slice
                // for every group lets the shared WAL batch independent Ready
                // generations without allowing one group to consume another's
                // apply or snapshot budget.
                max_ready_generations: budget.max_ready_generations,
                max_apply_entries: budget.max_apply_entries,
                max_apply_bytes: budget.max_apply_bytes,
                max_snapshot_bytes: budget.max_snapshot_bytes,
            };

            let group_result = {
                let group = self
                    .groups
                    .get_mut(&raft_group_id)
                    .expect("runnable group came from the active registry");

                match (process_message, pending_message.take()) {
                    (true, Some(message)) => {
                        group.step_and_prepare_budgeted(message.message.envelope, group_budget)
                    }
                    (false, None) | (false, Some(_)) => {
                        group.tick_and_prepare_budgeted(group_timer_ticks, group_budget)
                    }
                    (true, None) => unreachable!("message selection was checked above"),
                }
            };

            result.groups_serviced += 1;
            if process_message {
                result.messages_processed += 1;
            }

            match group_result {
                Ok(turn) => {
                    result.ready_generations += turn.ready_generations;
                    result.apply_entries += turn.apply_entries;
                    result.snapshot_bytes += turn.snapshot_bytes;

                    if let Some(batch) = turn.persistence {
                        let pending = PendingPersistenceGroup {
                            raft_group_id,
                            timer_due: rearm_timer,
                            batch,
                            outbound: turn.outbound,
                            read_states: turn.read_states,
                        };

                        match self.persistence.try_submit(pending) {
                            Ok(()) => {}
                            Err(PersistenceAdmissionError::Capacity(pending)) => {
                                // The concrete Ready adapter keeps all
                                // persistence-dependent output in the Ready
                                // itself until completion. A custom adapter
                                // must preserve that same contract.
                                self.ensure_persistence_output_invariant(&pending)?;
                                self.reschedule_after_turn(
                                    pending.raft_group_id,
                                    pending.timer_due,
                                );
                                break;
                            }
                            Err(PersistenceAdmissionError::RequestTooLarge {
                                pending,
                                record_count,
                                encoded_bytes,
                            }) => {
                                self.ensure_persistence_output_invariant(&pending)?;
                                self.reschedule_after_turn(
                                    pending.raft_group_id,
                                    pending.timer_due,
                                );
                                persistence_error =
                                    Some(MultiRaftHostError::PersistenceRequestTooLarge {
                                        raft_group_id: pending.raft_group_id,
                                        record_count,
                                        encoded_bytes,
                                        max_records: self.persistence.max_pending_records,
                                        max_bytes: self.persistence.max_pending_bytes,
                                    });
                                break;
                            }
                        }
                    } else {
                        result.read_states.extend(
                            turn.read_states
                                .into_iter()
                                .map(|state| (raft_group_id, state)),
                        );
                        result
                            .outbound
                            .extend(turn.outbound.into_iter().map(|envelope| RoutedRaftMessage {
                                raft_group_id,
                                envelope,
                            }));

                        self.ensure_shared_wal_healthy_for_turn()?;
                        self.reschedule_after_turn(raft_group_id, rearm_timer);
                    }
                }

                Err(HostedGroupError::RecoveryRequired) => {
                    self.state = HostState::RecoveryRequired;
                    return Err(MultiRaftHostError::RecoveryRequired);
                }

                Err(HostedGroupError::Group(reason)) => {
                    if self.node_wal.recovery_required() {
                        self.state = HostState::RecoveryRequired;
                        return Err(MultiRaftHostError::RecoveryRequired);
                    }
                    self.quarantined.insert(raft_group_id, reason);
                    self.remove_pending_group(raft_group_id);
                }

                Err(HostedGroupError::Retryable(reason)) => {
                    if self.node_wal.recovery_required() {
                        self.state = HostState::RecoveryRequired;
                        return Err(MultiRaftHostError::RecoveryRequired);
                    }
                    if let Some(message) = retry_message {
                        let control = is_control_message(&message.message.envelope);
                        let wire_bytes = message.wire_bytes;
                        self.pending_messages
                            .get_mut(&raft_group_id)
                            .expect("retrying message implies a group queue")
                            .push_front(message);
                        self.account_pending_addition(wire_bytes);
                        if control {
                            self.runnable.enqueue_control(raft_group_id);
                        } else {
                            self.runnable.enqueue(raft_group_id);
                        }
                    } else {
                        self.reschedule_after_turn(raft_group_id, rearm_timer);
                    }
                    let _ = reason;
                }

                Err(HostedGroupError::Rejected(reason)) => {
                    if self.node_wal.recovery_required() {
                        self.state = HostState::RecoveryRequired;
                        return Err(MultiRaftHostError::RecoveryRequired);
                    }
                    let _ = reason;
                    self.reschedule_after_turn(raft_group_id, rearm_timer);
                }
            }
        }

        if let Some(service_batch) = self.persistence.try_take_completion() {
            let shared_outcome = service_batch.outcome;
            let mut extent_offset = 0;
            let mut recovery_required = false;

            for pending in service_batch.groups {
                let raft_group_id = pending.raft_group_id;
                let record_count = pending.batch.record_count();
                let group_outcome = match &shared_outcome {
                    Ok(batch_result) => {
                        let extents = batch_result
                            .record_extents
                            .get(extent_offset..extent_offset + record_count)
                            .expect("cross-group WAL extent validation precedes splitting");
                        extent_offset += record_count;
                        Ok(BatchAppendResult {
                            record_extents: extents.to_vec(),
                            final_end_lsn: extents
                                .last()
                                .map(|extent| extent.end_lsn)
                                .unwrap_or(wal::lsn::Lsn::ZERO),
                        })
                    }
                    Err(error) => Err(error.clone()),
                };

                let rearm_timer = pending.timer_due;
                let completion = {
                    let group = self
                        .groups
                        .get_mut(&raft_group_id)
                        .expect("a pending persistence batch belongs to an active group");
                    group.complete_persistence(group_outcome, group_budget_for_completion(budget))
                };

                match completion {
                    Ok(turn) => {
                        result.apply_entries += turn.apply_entries;
                        result.snapshot_bytes += turn.snapshot_bytes;
                        result.read_states.extend(
                            pending
                                .read_states
                                .into_iter()
                                .chain(turn.read_states)
                                .map(|state| (raft_group_id, state)),
                        );
                        result.outbound.extend(
                            pending
                                .outbound
                                .into_iter()
                                .chain(turn.outbound)
                                .map(|envelope| RoutedRaftMessage {
                                    raft_group_id,
                                    envelope,
                                }),
                        );

                        if self.node_wal.recovery_required() {
                            recovery_required = true;
                        } else {
                            self.reschedule_after_turn(raft_group_id, rearm_timer);
                        }
                    }
                    Err(HostedGroupError::RecoveryRequired) => {
                        recovery_required = true;
                    }
                    Err(HostedGroupError::Group(reason)) => {
                        if self.node_wal.recovery_required() {
                            recovery_required = true;
                        } else {
                            self.quarantined.insert(raft_group_id, reason);
                            self.remove_pending_group(raft_group_id);
                        }
                    }
                    Err(HostedGroupError::Retryable(_reason)) => {
                        self.reschedule_after_turn(raft_group_id, rearm_timer);
                    }
                    Err(HostedGroupError::Rejected(_reason)) => {
                        self.reschedule_after_turn(raft_group_id, rearm_timer);
                    }
                }
            }

            if recovery_required || self.node_wal.recovery_required() {
                self.state = HostState::RecoveryRequired;
                return Err(MultiRaftHostError::RecoveryRequired);
            }
        }

        self.persistence.dispatch_if_idle();
        if self.node_wal.recovery_required() && !self.persistence.in_flight {
            self.state = HostState::RecoveryRequired;
            return Err(MultiRaftHostError::RecoveryRequired);
        }

        if let Some(error) = persistence_error {
            return Err(error);
        }

        Ok(result)
    }

    fn validate_message(&self, message: &RoutedRaftMessage) -> Result<(), MultiRaftHostError> {
        let raft_group_id = message.raft_group_id;
        let received = ReplicaId::from_raft(message.envelope.to);
        let tombstone_identity = RaftReplicaIdentity::new(raft_group_id, received)
            .map_err(|error| MultiRaftHostError::InvalidMessage(error.to_string()))?;
        if self.tombstoned.contains(&tombstone_identity) {
            return Err(MultiRaftHostError::TombstonedReplica(tombstone_identity));
        }

        if let Some(reason) = self.quarantined.get(&raft_group_id) {
            return Err(MultiRaftHostError::GroupQuarantined {
                raft_group_id,
                reason: reason.clone(),
            });
        }

        let identity = self
            .groups
            .get(&raft_group_id)
            .ok_or(MultiRaftHostError::UnknownGroup(raft_group_id))?
            .identity();
        let expected = identity
            .replica_id
            .to_raft()
            .expect("registered replica identity is validated");

        if message.envelope.to != expected {
            return Err(MultiRaftHostError::RecipientMismatch {
                raft_group_id,
                expected: identity.replica_id,
                received,
            });
        }

        Ok(())
    }

    fn ensure_persistence_output_invariant(
        &mut self,
        pending: &PendingPersistenceGroup,
    ) -> Result<(), MultiRaftHostError> {
        if pending.outbound.is_empty() && pending.read_states.is_empty() {
            return Ok(());
        }

        self.node_wal.require_recovery(
            "a hosted group exposed persistence-dependent output before A-WAL admission",
        );
        self.state = HostState::RecoveryRequired;
        Err(MultiRaftHostError::RecoveryRequired)
    }

    fn ensure_schedulable_group(
        &self,
        raft_group_id: RaftGroupId,
    ) -> Result<(), MultiRaftHostError> {
        if let Some(reason) = self.quarantined.get(&raft_group_id) {
            return Err(MultiRaftHostError::GroupQuarantined {
                raft_group_id,
                reason: reason.clone(),
            });
        }

        if !self.groups.contains_key(&raft_group_id) {
            return Err(MultiRaftHostError::UnknownGroup(raft_group_id));
        }

        Ok(())
    }

    fn reschedule_after_turn(&mut self, raft_group_id: RaftGroupId, rearm_timer: bool) {
        if rearm_timer {
            if let Some(delay) = self
                .groups
                .get(&raft_group_id)
                .and_then(|group| group.next_timer_delay_ticks())
            {
                self.timers.reschedule_after(raft_group_id, delay.max(1));
            } else {
                self.timers.remove(raft_group_id);
            }
        }

        let has_messages = self
            .pending_messages
            .get(&raft_group_id)
            .is_some_and(|messages| !messages.is_empty());
        let has_control_messages = self
            .pending_messages
            .get(&raft_group_id)
            .is_some_and(PendingGroupMessages::has_control);
        let has_pending_work = self
            .groups
            .get(&raft_group_id)
            .is_some_and(|group| group.has_pending_work());

        if has_messages || has_pending_work {
            if has_control_messages {
                self.runnable.enqueue_control(raft_group_id);
            } else {
                self.runnable.enqueue(raft_group_id);
            }
        }
    }

    /// Return the number of logical ticks until the earliest group deadline.
    ///
    /// A zero result means the deadline is already due and the caller should
    /// run the host immediately. The method intentionally exposes no group
    /// identity: placement and timer ownership remain inside the host.
    pub fn next_timer_delay_ticks(&self) -> Option<u64> {
        self.timers
            .next_deadline()
            .map(|deadline| deadline.saturating_sub(self.timers.now))
    }

    /// Whether the host has work that should be serviced without parking.
    pub fn has_runnable_work(&self) -> bool {
        !self.runnable.is_empty()
    }

    /// Delivers an inbound group-tagged Raft message through the legacy direct
    /// path. New host loops should use [`Self::enqueue_message`] and
    /// [`Self::run_turn`] so message and group budgets remain effective.
    pub fn route(
        &mut self,
        message: RoutedRaftMessage,
    ) -> Result<Vec<RoutedRaftMessage>, MultiRaftHostError> {
        self.ensure_active()?;
        let raft_group_id = message.raft_group_id;
        self.validate_message(&message)?;

        if self
            .groups
            .get(&raft_group_id)
            .is_some_and(|group| group.has_pending_work())
            || self
                .pending_messages
                .get(&raft_group_id)
                .is_some_and(|pending| !pending.is_empty())
        {
            return Err(MultiRaftHostError::GroupRetryable {
                raft_group_id,
                reason: "group already has queued work; use the bounded host turn".to_string(),
            });
        }

        let control = is_control_message(&message.envelope);
        self.enqueue_message(message)?;
        assert!(
            self.runnable.remove(raft_group_id),
            "a newly admitted route message must make its group runnable"
        );
        if control {
            self.runnable.enqueue_control(raft_group_id);
        } else {
            self.runnable.enqueue(raft_group_id);
        }
        self.run_turn(
            0,
            MultiRaftTurnBudget {
                max_groups: 1,
                max_messages: 1,
                max_ready_generations: 1,
                max_apply_entries: 128,
                max_apply_bytes: 4 * 1024 * 1024,
                max_snapshot_bytes: 4 * 1024 * 1024,
            },
        )
        .map(|turn| {
            turn.outbound
                .into_iter()
                .filter(|message| message.raft_group_id == raft_group_id)
                .collect()
        })
    }

    /// Tick every healthy local group once.
    ///
    /// A group-local failure quarantines only that group and iteration
    /// continues. Shared-WAL uncertainty aborts the complete host immediately.
    pub fn tick_all(&mut self, ticks: u64) -> Result<Vec<RoutedRaftMessage>, MultiRaftHostError> {
        self.ensure_active()?;

        let group_ids: Vec<_> = self.groups.keys().copied().collect();

        // This compatibility API deliberately retains the old "tick every
        // group" contract used by standalone host callers and tests. Advance
        // the shared clock without consuming sparse deadlines, then mark each
        // healthy group with the exact tick delta it must receive. The
        // production runtime uses `run_turn` directly and never takes this
        // all-groups path.
        self.timers.advance_clock(ticks);

        for raft_group_id in group_ids.iter().copied() {
            if !self.quarantined.contains_key(&raft_group_id) {
                self.timer_due
                    .entry(raft_group_id)
                    .and_modify(|pending| *pending = pending.saturating_add(ticks))
                    .or_insert(ticks);
                self.runnable.enqueue(raft_group_id);
            }
        }

        self.run_turn(
            0,
            MultiRaftTurnBudget {
                max_groups: group_ids.len(),
                max_messages: 0,
                ..MultiRaftTurnBudget::default()
            },
        )
        .map(|turn| turn.outbound)
    }

    pub fn propose(
        &mut self,
        raft_group_id: RaftGroupId,
        command: Vec<u8>,
        encoded_len: usize,
    ) -> Result<HostedProposal, MultiRaftHostError> {
        self.ensure_active()?;

        if encoded_len > self.config.max_proposal_bytes {
            return Err(MultiRaftHostError::ProposalTooLarge {
                raft_group_id,
                encoded_len,
                max_bytes: self.config.max_proposal_bytes,
            });
        }

        if let Some(reason) = self.quarantined.get(&raft_group_id) {
            return Err(MultiRaftHostError::GroupQuarantined {
                raft_group_id,
                reason: reason.clone(),
            });
        }

        let result = {
            let group = self
                .groups
                .get_mut(&raft_group_id)
                .ok_or(MultiRaftHostError::UnknownGroup(raft_group_id))?;

            group.propose_and_drain(command, encoded_len)
        };

        self.finish_group_proposal(raft_group_id, result)
    }

    /// Proposes one typed membership transition through the group-owned Ready
    /// lifecycle. Configuration entries are not exposed as SQL/tablet
    /// commands and become authoritative only after the same shared-WAL and
    /// applied-frontier acknowledgements used for all Raft entries.
    pub fn propose_conf_change(
        &mut self,
        raft_group_id: RaftGroupId,
        change: ConfChange,
    ) -> Result<HostedProposal, MultiRaftHostError> {
        self.ensure_active()?;

        const CONFIGURATION_ENTRY_BYTES: usize = 24;
        if CONFIGURATION_ENTRY_BYTES > self.config.max_proposal_bytes {
            return Err(MultiRaftHostError::ProposalTooLarge {
                raft_group_id,
                encoded_len: CONFIGURATION_ENTRY_BYTES,
                max_bytes: self.config.max_proposal_bytes,
            });
        }

        if let Some(reason) = self.quarantined.get(&raft_group_id) {
            return Err(MultiRaftHostError::GroupQuarantined {
                raft_group_id,
                reason: reason.clone(),
            });
        }

        let result = {
            let group = self
                .groups
                .get_mut(&raft_group_id)
                .ok_or(MultiRaftHostError::UnknownGroup(raft_group_id))?;

            group.propose_conf_change(change)
        };

        self.finish_group_proposal(raft_group_id, result)
    }

    /// Requests a bounded leadership transfer for one local Raft group.
    ///
    /// Transfer is deliberately kept separate from proposal admission: it
    /// does not allocate a log index or create a database record. The hosted
    /// group still completes any direct Ready work before returning, and the
    /// host retains the same recovery/quarantine/error-isolation policy used
    /// by other direct group operations.
    pub fn transfer_leadership(
        &mut self,
        raft_group_id: RaftGroupId,
        target: ReplicaId,
        timeout_ticks: u64,
    ) -> Result<HostedLeadershipTransfer, MultiRaftHostError> {
        self.ensure_active()?;

        if let Some(reason) = self.quarantined.get(&raft_group_id) {
            return Err(MultiRaftHostError::GroupQuarantined {
                raft_group_id,
                reason: reason.clone(),
            });
        }

        let result = {
            let group = self
                .groups
                .get_mut(&raft_group_id)
                .ok_or(MultiRaftHostError::UnknownGroup(raft_group_id))?;

            group.transfer_leadership(target, timeout_ticks)
        };

        let (status, messages) = match result {
            Ok(result) => result,

            Err(HostedGroupError::RecoveryRequired) => {
                self.state = HostState::RecoveryRequired;
                return Err(MultiRaftHostError::RecoveryRequired);
            }

            Err(HostedGroupError::Group(reason)) => {
                if self.node_wal.recovery_required() {
                    self.state = HostState::RecoveryRequired;
                    return Err(MultiRaftHostError::RecoveryRequired);
                }

                self.quarantined.insert(raft_group_id, reason.clone());
                return Err(MultiRaftHostError::Group {
                    raft_group_id,
                    reason,
                });
            }

            Err(HostedGroupError::Retryable(reason)) => {
                if self.node_wal.recovery_required() {
                    self.state = HostState::RecoveryRequired;
                    return Err(MultiRaftHostError::RecoveryRequired);
                }

                return Err(MultiRaftHostError::GroupRetryable {
                    raft_group_id,
                    reason,
                });
            }

            Err(HostedGroupError::Rejected(reason)) => {
                if self.node_wal.recovery_required() {
                    self.state = HostState::RecoveryRequired;
                    return Err(MultiRaftHostError::RecoveryRequired);
                }

                return Err(MultiRaftHostError::GroupRejected {
                    raft_group_id,
                    reason,
                });
            }
        };

        self.ensure_shared_wal_healthy()?;
        self.reschedule_after_turn(raft_group_id, false);

        Ok(HostedLeadershipTransfer {
            status,
            outbound: messages
                .into_iter()
                .map(|envelope| RoutedRaftMessage {
                    raft_group_id,
                    envelope,
                })
                .collect(),
        })
    }

    /// Propose one planner-produced membership action only if the group still
    /// exposes the exact ConfState version that was observed by reconciliation.
    /// The check is repeated immediately before Raft admission; a stale
    /// scheduler action therefore becomes a replan outcome instead of mutating
    /// a newer membership lifetime.
    pub fn propose_reconcile_action(
        &mut self,
        raft_group_id: RaftGroupId,
        action: MetadataReconcileAction,
    ) -> Result<HostedProposal, MultiRaftHostError> {
        self.ensure_active()?;

        let status = self
            .groups
            .get(&raft_group_id)
            .ok_or(MultiRaftHostError::UnknownGroup(raft_group_id))?
            .status();
        let observed_version =
            status
                .conf_state_version
                .ok_or_else(|| MultiRaftHostError::GroupRetryable {
                    raft_group_id,
                    reason: "membership ConfState is not published yet".to_string(),
                })?;
        if observed_version != action.expected_conf_state_version {
            return Err(MultiRaftHostError::StaleMembershipObservation {
                raft_group_id,
                expected: action.expected_conf_state_version,
                observed: observed_version,
            });
        }
        if let MetadataReconcileActionKind::RemoveReplica { replica_id } = action.kind
            && status.leader_replica_id == Some(replica_id)
        {
            return Err(MultiRaftHostError::LeaderTransferRequired {
                raft_group_id,
                replica_id,
            });
        }

        let voters = status
            .voters
            .iter()
            .map(|replica_id| {
                replica_id
                    .to_raft()
                    .map_err(|reason| MultiRaftHostError::InvalidMessage(reason.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let learners = status
            .learners
            .iter()
            .map(|replica_id| {
                replica_id
                    .to_raft()
                    .map_err(|reason| MultiRaftHostError::InvalidMessage(reason.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut observed = ConfState::new(observed_version, voters, learners)
            .map_err(|error| MultiRaftHostError::InvalidMessage(format!("{error:?}")))?;
        observed.outgoing_voters = status
            .outgoing_voters
            .iter()
            .map(|replica_id| {
                replica_id
                    .to_raft()
                    .map_err(|reason| MultiRaftHostError::InvalidMessage(reason.to_string()))
            })
            .collect::<Result<_, _>>()?;
        let change = conf_change_for_action(&action, &observed).map_err(|error| {
            MultiRaftHostError::StaleMembershipAction {
                raft_group_id,
                reason: error.to_string(),
            }
        })?;
        self.propose_conf_change(raft_group_id, change)
    }

    fn finish_group_proposal(
        &mut self,
        raft_group_id: RaftGroupId,
        result: Result<(LogIndex, Vec<RaftMessageEnvelope>), HostedGroupError>,
    ) -> Result<HostedProposal, MultiRaftHostError> {
        let (index, messages) = match result {
            Ok(result) => result,

            Err(HostedGroupError::RecoveryRequired) => {
                self.state = HostState::RecoveryRequired;
                return Err(MultiRaftHostError::RecoveryRequired);
            }

            Err(HostedGroupError::Group(reason)) => {
                if self.node_wal.recovery_required() {
                    self.state = HostState::RecoveryRequired;
                    return Err(MultiRaftHostError::RecoveryRequired);
                }

                self.quarantined.insert(raft_group_id, reason.clone());
                return Err(MultiRaftHostError::Group {
                    raft_group_id,
                    reason,
                });
            }

            Err(HostedGroupError::Retryable(reason)) => {
                if self.node_wal.recovery_required() {
                    self.state = HostState::RecoveryRequired;
                    return Err(MultiRaftHostError::RecoveryRequired);
                }

                return Err(MultiRaftHostError::GroupRetryable {
                    raft_group_id,
                    reason,
                });
            }

            Err(HostedGroupError::Rejected(reason)) => {
                if self.node_wal.recovery_required() {
                    self.state = HostState::RecoveryRequired;
                    return Err(MultiRaftHostError::RecoveryRequired);
                }

                return Err(MultiRaftHostError::GroupRejected {
                    raft_group_id,
                    reason,
                });
            }
        };

        self.ensure_shared_wal_healthy()?;
        self.reschedule_after_turn(raft_group_id, false);

        Ok(HostedProposal {
            index,
            outbound: messages
                .into_iter()
                .map(|envelope| RoutedRaftMessage {
                    raft_group_id,
                    envelope,
                })
                .collect(),
        })
    }

    fn finish_group_admission(
        &mut self,
        raft_group_id: RaftGroupId,
        result: Result<(), HostedGroupError>,
    ) -> Result<(), MultiRaftHostError> {
        match result {
            Ok(()) => {
                self.ensure_shared_wal_healthy()?;
                self.reschedule_after_turn(raft_group_id, false);
                Ok(())
            }

            Err(HostedGroupError::RecoveryRequired) => {
                self.state = HostState::RecoveryRequired;
                Err(MultiRaftHostError::RecoveryRequired)
            }

            Err(HostedGroupError::Group(reason)) => {
                if self.node_wal.recovery_required() {
                    self.state = HostState::RecoveryRequired;
                    return Err(MultiRaftHostError::RecoveryRequired);
                }

                self.quarantined.insert(raft_group_id, reason.clone());
                Err(MultiRaftHostError::Group {
                    raft_group_id,
                    reason,
                })
            }

            Err(HostedGroupError::Retryable(reason)) => {
                if self.node_wal.recovery_required() {
                    self.state = HostState::RecoveryRequired;
                    return Err(MultiRaftHostError::RecoveryRequired);
                }

                Err(MultiRaftHostError::GroupRetryable {
                    raft_group_id,
                    reason,
                })
            }

            Err(HostedGroupError::Rejected(reason)) => {
                if self.node_wal.recovery_required() {
                    self.state = HostState::RecoveryRequired;
                    return Err(MultiRaftHostError::RecoveryRequired);
                }

                Err(MultiRaftHostError::GroupRejected {
                    raft_group_id,
                    reason,
                })
            }
        }
    }

    fn ensure_registering(&self) -> Result<(), MultiRaftHostError> {
        match self.state {
            HostState::Registering => Ok(()),
            HostState::Active => Err(MultiRaftHostError::AlreadyActive),
            HostState::RecoveryRequired => Err(MultiRaftHostError::RecoveryRequired),
        }
    }

    fn ensure_active(&mut self) -> Result<(), MultiRaftHostError> {
        // An asynchronous WAL worker may have fenced the shared owner before
        // the host has consumed its completion. Keep this active turn alive
        // long enough to fan the exact outcome out to every prepared group;
        // otherwise an unknown batch would strand their Ready generations.
        if self.node_wal.recovery_required() && !self.persistence.in_flight {
            self.state = HostState::RecoveryRequired;
        }
        match self.state {
            HostState::Active => Ok(()),
            HostState::Registering => Err(MultiRaftHostError::NotActive),
            HostState::RecoveryRequired => Err(MultiRaftHostError::RecoveryRequired),
        }
    }

    fn ensure_shared_wal_healthy(&mut self) -> Result<(), MultiRaftHostError> {
        if self.node_wal.recovery_required() {
            self.state = HostState::RecoveryRequired;
            Err(MultiRaftHostError::RecoveryRequired)
        } else {
            Ok(())
        }
    }

    fn ensure_shared_wal_healthy_for_turn(&mut self) -> Result<(), MultiRaftHostError> {
        if self.node_wal.recovery_required() && !self.persistence.in_flight {
            self.state = HostState::RecoveryRequired;
            Err(MultiRaftHostError::RecoveryRequired)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MultiRaftHostError {
    #[error("invalid MultiRaft host configuration: {0}")]
    InvalidConfiguration(String),
    #[error("the host has not completed local replica registration")]
    NotActive,
    #[error("the host is already active")]
    AlreadyActive,
    #[error("the shared Raft WAL requires full node recovery")]
    RecoveryRequired,
    #[error("no local replica is registered for Raft group {0:?}")]
    UnknownGroup(RaftGroupId),
    #[error("replica lifetime {0:?} is permanently tombstoned")]
    TombstonedReplica(RaftReplicaIdentity),
    #[error("invalid Raft envelope: {0}")]
    InvalidMessage(String),
    #[error(
        "pending Raft messages for group {raft_group_id:?} exceeded the admission limit: {reason}"
    )]
    PendingMessagesFull {
        raft_group_id: RaftGroupId,
        reason: String,
    },
    #[error(
        "proposal for Raft group {raft_group_id:?} is {encoded_len} bytes, maximum is {max_bytes}"
    )]
    ProposalTooLarge {
        raft_group_id: RaftGroupId,
        encoded_len: usize,
        max_bytes: usize,
    },
    #[error(
        "prepared persistence request for group {raft_group_id:?} exceeds the service limit: {record_count} records and {encoded_bytes} bytes (maximum {max_records} records and {max_bytes} bytes)"
    )]
    PersistenceRequestTooLarge {
        raft_group_id: RaftGroupId,
        record_count: usize,
        encoded_bytes: usize,
        max_records: usize,
        max_bytes: usize,
    },
    #[error(
        "Raft envelope for group {raft_group_id:?} targets replica {received:?}, not local replica {expected:?}"
    )]
    RecipientMismatch {
        raft_group_id: RaftGroupId,
        expected: ReplicaId,
        received: ReplicaId,
    },
    #[error("Raft group {0:?} is already registered on this node")]
    DuplicateGroup(RaftGroupId),
    #[error(
        "Raft group {raft_group_id:?} is registered for {expected:?}, not requested lifetime {received:?}"
    )]
    ReplicaIdentityMismatch {
        raft_group_id: RaftGroupId,
        expected: RaftReplicaIdentity,
        received: RaftReplicaIdentity,
    },
    #[error("recovered identity {0:?} must use the recovered startup path")]
    RecoveredIdentityRequiresRecovery(RaftReplicaIdentity),
    #[error("identity {0:?} was not discovered by shared-WAL recovery")]
    UnexpectedRecovered(RaftReplicaIdentity),
    #[error("cannot activate before recovering local replica identities: {0:?}")]
    MissingRecovered(Vec<RaftReplicaIdentity>),
    #[error("could not seal shared-WAL retention registry: {0}")]
    RetentionRegistry(String),
    #[error("hosted Raft group {raft_group_id:?} failed: {reason}")]
    Group {
        raft_group_id: RaftGroupId,
        reason: String,
    },
    #[error("Raft group {raft_group_id:?} temporarily could not complete operation: {reason}")]
    GroupRetryable {
        raft_group_id: RaftGroupId,
        reason: String,
    },

    #[error("Raft group {raft_group_id:?} rejected operation without failing: {reason}")]
    GroupRejected {
        raft_group_id: RaftGroupId,
        reason: String,
    },
    #[error(
        "membership observation for Raft group {raft_group_id:?} is stale: expected {expected}, observed {observed}"
    )]
    StaleMembershipObservation {
        raft_group_id: RaftGroupId,
        expected: u64,
        observed: u64,
    },
    #[error(
        "membership action for Raft group {raft_group_id:?} requires leadership transfer before removing replica {replica_id:?}"
    )]
    LeaderTransferRequired {
        raft_group_id: RaftGroupId,
        replica_id: ReplicaId,
    },
    #[error("membership action for Raft group {raft_group_id:?} is stale or invalid: {reason}")]
    StaleMembershipAction {
        raft_group_id: RaftGroupId,
        reason: String,
    },
    #[error(
        "Raft WAL writer for replica lifetime {0:?} \
         was not issued by this MultiRaft host"
    )]
    WalWriterNotIssued(RaftReplicaIdentity),

    #[error(
        "Raft WAL writer for replica lifetime {0:?} \
         was already issued"
    )]
    WalWriterAlreadyIssued(RaftReplicaIdentity),

    #[error("could not register replica lifetime with shared Raft WAL: {0}")]
    WalRegistration(String),

    #[error("Raft group {raft_group_id:?} is quarantined: {reason}")]
    GroupQuarantined {
        raft_group_id: RaftGroupId,
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::{
        message::{InstallSnapshotRequest, Message, TimeoutNowRequest},
        types::{ReplicaId as RaftReplicaId, SnapshotMetadata},
    };
    use ragnordb_common::ids::ReplicaId;
    use std::sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    };
    use wal::{
        error::{BatchAppendFailure, WalError},
        lsn::Lsn,
        types::RecordType,
        wal::{AppendResult, BatchAppendResult},
    };

    struct TestWal;

    impl RaftWal for TestWal {
        fn append_batch_and_sync(
            &mut self,
            _: &[(RecordType, &[u8])],
        ) -> Result<BatchAppendResult, BatchAppendFailure> {
            unreachable!("host tests do not persist Ready generations")
        }
    }

    #[derive(Clone, Copy)]
    enum TickBehavior {
        Healthy,
        GroupFailure,
        RecoveryRequired,
    }

    struct TestGroup {
        identity: RaftReplicaIdentity,
        tick_behavior: TickBehavior,
        ticks: Arc<AtomicU64>,
        stepped: Arc<AtomicU64>,
        outbound: Vec<RaftMessageEnvelope>,
    }

    impl HostedRaftGroup for TestGroup {
        fn identity(&self) -> RaftReplicaIdentity {
            self.identity
        }

        fn next_timer_delay_ticks(&self) -> Option<u64> {
            Some(1)
        }

        fn tick_and_drain(&mut self, _: u64) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
            match self.tick_behavior {
                TickBehavior::Healthy => {
                    self.ticks.fetch_add(1, Ordering::SeqCst);
                    Ok(Vec::new())
                }
                TickBehavior::GroupFailure => Err(HostedGroupError::Group(
                    "injected group-local failure".to_string(),
                )),
                TickBehavior::RecoveryRequired => Err(HostedGroupError::RecoveryRequired),
            }
        }

        fn step_and_drain(
            &mut self,
            _: RaftMessageEnvelope,
        ) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
            self.stepped.fetch_add(1, Ordering::SeqCst);
            Ok(std::mem::take(&mut self.outbound))
        }

        fn propose_and_drain(
            &mut self,
            _: Vec<u8>,
            _: usize,
        ) -> Result<(LogIndex, Vec<RaftMessageEnvelope>), HostedGroupError> {
            Ok((0, std::mem::take(&mut self.outbound)))
        }

        fn propose_conf_change(
            &mut self,
            _: ConfChange,
        ) -> Result<(LogIndex, Vec<RaftMessageEnvelope>), HostedGroupError> {
            Ok((7, std::mem::take(&mut self.outbound)))
        }
    }

    #[derive(Clone)]
    struct BatchWal {
        state: Arc<Mutex<BatchWalState>>,
        outcome_unknown: bool,
    }

    struct BatchWalState {
        next_lsn: Lsn,
        append_calls: usize,
        record_types: Vec<RecordType>,
    }

    impl BatchWal {
        fn new(outcome_unknown: bool) -> (Self, Arc<Mutex<BatchWalState>>) {
            let state = Arc::new(Mutex::new(BatchWalState {
                next_lsn: Lsn::new(100),
                append_calls: 0,
                record_types: Vec::new(),
            }));
            (
                Self {
                    state: Arc::clone(&state),
                    outcome_unknown,
                },
                state,
            )
        }
    }

    impl RaftWal for BatchWal {
        fn append_batch_and_sync(
            &mut self,
            records: &[(RecordType, &[u8])],
        ) -> Result<BatchAppendResult, BatchAppendFailure> {
            let mut state = self.state.lock().unwrap();
            state.append_calls += 1;

            let mut extents = Vec::with_capacity(records.len());
            for (record_type, payload) in records {
                let start_lsn = state.next_lsn;
                let end_lsn = start_lsn
                    .checked_add_bytes(payload.len() as u64 + 32)
                    .unwrap();
                state.next_lsn = end_lsn;
                state.record_types.push(*record_type);
                extents.push(AppendResult { start_lsn, end_lsn });
            }

            let result = BatchAppendResult {
                final_end_lsn: extents
                    .last()
                    .map(|extent| extent.end_lsn)
                    .unwrap_or(Lsn::ZERO),
                record_extents: extents,
            };

            if self.outcome_unknown {
                Err(BatchAppendFailure::OutcomeUnknown {
                    result,
                    source: WalError::BrokenDurabilityContract,
                })
            } else {
                Ok(result)
            }
        }
    }

    #[derive(Clone)]
    struct BlockingWal {
        state: Arc<(Mutex<BlockingWalState>, Condvar)>,
    }

    struct BlockingWalState {
        next_lsn: Lsn,
        append_calls: usize,
        started: bool,
        release: bool,
    }

    impl BlockingWal {
        fn new() -> (Self, Arc<(Mutex<BlockingWalState>, Condvar)>) {
            let state = Arc::new((
                Mutex::new(BlockingWalState {
                    next_lsn: Lsn::new(100),
                    append_calls: 0,
                    started: false,
                    release: false,
                }),
                Condvar::new(),
            ));
            (
                Self {
                    state: Arc::clone(&state),
                },
                state,
            )
        }
    }

    impl RaftWal for BlockingWal {
        fn append_batch_and_sync(
            &mut self,
            records: &[(RecordType, &[u8])],
        ) -> Result<BatchAppendResult, BatchAppendFailure> {
            let (lock, wake) = &*self.state;
            let mut state = lock.lock().unwrap();
            state.append_calls += 1;
            state.started = true;
            wake.notify_all();
            while !state.release {
                state = wake.wait(state).unwrap();
            }

            let mut extents = Vec::with_capacity(records.len());
            for (_, payload) in records {
                let start_lsn = state.next_lsn;
                let end_lsn = start_lsn
                    .checked_add_bytes(payload.len() as u64 + 32)
                    .unwrap();
                state.next_lsn = end_lsn;
                extents.push(AppendResult { start_lsn, end_lsn });
            }

            Ok(BatchAppendResult {
                final_end_lsn: extents
                    .last()
                    .map(|extent| extent.end_lsn)
                    .unwrap_or(Lsn::ZERO),
                record_extents: extents,
            })
        }
    }

    struct PersistenceGroup {
        identity: RaftReplicaIdentity,
        records: Vec<(RecordType, Vec<u8>)>,
        pending: bool,
        completions: Arc<Mutex<Vec<PersistenceCompletion>>>,
        completed: Arc<AtomicUsize>,
    }

    type PersistenceCompletion = Result<(usize, Option<Lsn>), String>;

    impl HostedRaftGroup for PersistenceGroup {
        fn identity(&self) -> RaftReplicaIdentity {
            self.identity
        }

        fn has_pending_work(&self) -> bool {
            self.pending
        }

        fn tick_and_drain(&mut self, _: u64) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
            unreachable!("Slice 2 test groups use the two-phase budgeted path")
        }

        fn step_and_drain(
            &mut self,
            _: RaftMessageEnvelope,
        ) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
            unreachable!("Slice 2 test groups use the two-phase budgeted path")
        }

        fn propose_and_drain(
            &mut self,
            _: Vec<u8>,
            _: usize,
        ) -> Result<(LogIndex, Vec<RaftMessageEnvelope>), HostedGroupError> {
            unreachable!("Slice 2 test groups use the two-phase budgeted path")
        }

        fn tick_and_prepare_budgeted(
            &mut self,
            _: u64,
            _: MultiRaftTurnBudget,
        ) -> Result<HostedGroupTurn, HostedGroupError> {
            self.pending = true;
            Ok(HostedGroupTurn {
                ready_generations: 1,
                persistence: Some(HostedPersistenceBatch::new(self.records.clone())),
                ..HostedGroupTurn::default()
            })
        }

        fn step_and_prepare_budgeted(
            &mut self,
            _: RaftMessageEnvelope,
            budget: MultiRaftTurnBudget,
        ) -> Result<HostedGroupTurn, HostedGroupError> {
            self.tick_and_prepare_budgeted(0, budget)
        }

        fn complete_persistence(
            &mut self,
            outcome: Result<BatchAppendResult, BatchAppendFailure>,
            _: MultiRaftTurnBudget,
        ) -> Result<HostedGroupTurn, HostedGroupError> {
            let completion = match outcome {
                Ok(result) => {
                    self.pending = false;
                    self.completed.fetch_add(1, Ordering::SeqCst);
                    Ok((
                        result.record_extents.len(),
                        result.record_extents.first().map(|extent| extent.start_lsn),
                    ))
                }
                Err(error) => Err(error.to_string()),
            };
            self.completions.lock().unwrap().push(completion);
            Ok(HostedGroupTurn::default())
        }
    }

    fn identity(group: u64, replica: u64) -> RaftReplicaIdentity {
        RaftReplicaIdentity::new(RaftGroupId(group), ReplicaId(replica)).unwrap()
    }

    fn healthy_group(
        identity: RaftReplicaIdentity,
        outbound: Vec<RaftMessageEnvelope>,
    ) -> TestGroup {
        TestGroup {
            identity,
            tick_behavior: TickBehavior::Healthy,
            ticks: Arc::new(AtomicU64::new(0)),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound,
        }
    }

    #[test]
    fn route_demultiplexes_to_the_tagged_group_and_releases_only_its_messages() {
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let first_identity = identity(10, 10);
        let second_identity = identity(11, 11);
        let _first_writer = host.issue_group_writer(first_identity).unwrap();
        let _second_writer = host.issue_group_writer(second_identity).unwrap();
        let inbound = Envelope {
            from: RaftReplicaId::must(20),
            to: RaftReplicaId::must(11),
            msg: Message::PreVoteResponse(raft::message::PreVoteResponse {
                term: 0,
                vote_granted: true,
            }),
        };
        let outbound = Envelope {
            from: RaftReplicaId::must(11),
            to: RaftReplicaId::must(21),
            msg: Message::PreVoteResponse(raft::message::PreVoteResponse {
                term: 0,
                vote_granted: true,
            }),
        };
        host.register_new_group(Box::new(healthy_group(first_identity, Vec::new())))
            .unwrap();
        host.register_new_group(Box::new(healthy_group(
            second_identity,
            vec![outbound.clone()],
        )))
        .unwrap();
        host.activate().unwrap();
        assert_eq!(
            host.route(RoutedRaftMessage {
                raft_group_id: RaftGroupId(11),
                envelope: inbound
            })
            .unwrap(),
            vec![RoutedRaftMessage {
                raft_group_id: RaftGroupId(11),
                envelope: outbound
            }]
        );
    }

    #[test]
    fn active_host_can_register_a_new_group_after_retention_sealing() {
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let first = identity(10, 10);
        let second = identity(11, 11);
        let _writer = host.issue_group_writer(first).unwrap();
        host.register_new_group(Box::new(healthy_group(first, Vec::new())))
            .unwrap();
        host.activate().unwrap();

        let _late_writer = host.issue_group_writer_after_activation(second).unwrap();
        host.register_active_group(Box::new(healthy_group(second, Vec::new())))
            .unwrap();

        assert_eq!(host.group_count(), 2);
        assert!(
            host.status()
                .groups
                .iter()
                .any(|group| group.identity == second)
        );
    }

    #[test]
    fn quarantined_group_does_not_starve_other_groups() {
        let healthy_ticks = Arc::new(AtomicU64::new(0));
        let failing_identity = identity(10, 101);
        let healthy_identity = identity(20, 202);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let _failing_writer = host.issue_group_writer(failing_identity).unwrap();
        let _healthy_writer = host.issue_group_writer(healthy_identity).unwrap();

        host.register_new_group(Box::new(TestGroup {
            identity: failing_identity,
            tick_behavior: TickBehavior::GroupFailure,
            ticks: Arc::new(AtomicU64::new(0)),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.register_new_group(Box::new(TestGroup {
            identity: healthy_identity,
            tick_behavior: TickBehavior::Healthy,
            ticks: Arc::clone(&healthy_ticks),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.activate().unwrap();

        host.tick_all(1).unwrap();
        assert_eq!(healthy_ticks.load(Ordering::SeqCst), 1);
        assert!(host.group_failure(RaftGroupId(10)).is_some());

        host.tick_all(1).unwrap();
        assert_eq!(healthy_ticks.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn recovery_required_from_one_group_fences_whole_host() {
        let failing_identity = identity(10, 101);
        let healthy_identity = identity(20, 202);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let _failing_writer = host.issue_group_writer(failing_identity).unwrap();
        let _healthy_writer = host.issue_group_writer(healthy_identity).unwrap();

        host.register_new_group(Box::new(TestGroup {
            identity: failing_identity,
            tick_behavior: TickBehavior::RecoveryRequired,
            ticks: Arc::new(AtomicU64::new(0)),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.register_new_group(Box::new(healthy_group(healthy_identity, Vec::new())))
            .unwrap();
        host.activate().unwrap();

        assert_eq!(
            host.tick_all(1).unwrap_err(),
            MultiRaftHostError::RecoveryRequired
        );
        assert_eq!(
            host.propose(RaftGroupId(20), vec![1], 1).unwrap_err(),
            MultiRaftHostError::RecoveryRequired
        );
    }

    #[test]
    fn typed_membership_proposal_uses_the_hosted_group_boundary() {
        let group_identity = identity(10, 101);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let _writer = host.issue_group_writer(group_identity).unwrap();
        host.register_new_group(Box::new(healthy_group(group_identity, Vec::new())))
            .unwrap();
        host.activate().unwrap();

        let proposal = host
            .propose_conf_change(
                RaftGroupId(10),
                ConfChange {
                    expected_version: 1,
                    kind: raft::types::ConfChangeKind::AddLearner(RaftReplicaId::must(202)),
                },
            )
            .unwrap();

        assert_eq!(proposal.index, 7);
        assert!(proposal.outbound.is_empty());
    }

    #[test]
    fn external_shared_wal_fence_stops_host_via_durability_gate() {
        use ragnordb_common::durability::{DurabilityFailureKind, DurabilityGate};
        let gate = DurabilityGate::new();
        let node_wal = NodeRaftWal::with_durability_gate(TestWal, gate.clone());
        let mut host = MultiRaftHost::new(NodeId(7), node_wal);
        let first_identity = identity(10, 10);
        let second_identity = identity(20, 20);
        let _first_writer = host.issue_group_writer(first_identity).unwrap();
        let _second_writer = host.issue_group_writer(second_identity).unwrap();
        host.register_new_group(Box::new(healthy_group(first_identity, Vec::new())))
            .unwrap();
        host.register_new_group(Box::new(healthy_group(second_identity, Vec::new())))
            .unwrap();
        host.activate().unwrap();
        let _ = gate.require_recovery(
            DurabilityFailureKind::CatalogOutcomeUnknown,
            "injected catalog WAL uncertainty",
        );
        assert_eq!(
            host.tick_all(1).unwrap_err(),
            MultiRaftHostError::RecoveryRequired,
        );
    }

    #[test]
    fn nested_recovery_required_is_never_rejected_or_group() {
        use crate::runtime::ReadyLoopError;
        use raft::core::{
            node::{
                LeadershipTransferError, ProposeError, RaftError, SnapshotInstallError, StepError,
            },
            ready::AdvanceError,
        };

        let cases = vec![
            ReadyLoopError::RecoveryRequired,
            ReadyLoopError::Tick(RaftError::RecoveryRequired),
            ReadyLoopError::Step(StepError::RecoveryRequired),
            ReadyLoopError::Proposal(ProposeError::RecoveryRequired),
            ReadyLoopError::LeadershipTransfer(LeadershipTransferError::RecoveryRequired),
            ReadyLoopError::SnapshotInstall(SnapshotInstallError::RecoveryRequired),
            ReadyLoopError::Advance(AdvanceError::RecoveryRequired),
        ];
        for error in cases {
            assert_eq!(
                classify_ready_error(error),
                HostedGroupError::RecoveryRequired,
                "nested RecoveryRequired must be classified as RecoveryRequired"
            );
        }

        // Ensure ordinary rejections are not misclassified as recovery
        let rejected = ReadyLoopError::Proposal(ProposeError::NotLeader);
        assert!(matches!(
            classify_ready_error(rejected),
            HostedGroupError::Rejected(_)
        ));

        let retryable = ReadyLoopError::RetryablePersistence(
            crate::storage::persistence::RaftPersistenceError::NotStaged {
                recovery_required: false,
                reason: "injected retryable".to_string(),
            },
        );
        assert!(matches!(
            classify_ready_error(retryable),
            HostedGroupError::Retryable(_)
        ));

        // Realistic bug caught: a caught-up requirement that is temporarily
        // false must not quarantine a healthy group; the target may become
        // eligible after normal replication progress.
        let transfer_retryable =
            ReadyLoopError::LeadershipTransfer(LeadershipTransferError::TargetNotCaughtUp {
                target: raft::types::NodeId::must(2),
                match_index: 4,
                leader_last_index: 5,
            });
        assert!(matches!(
            classify_ready_error(transfer_retryable),
            HostedGroupError::Retryable(_)
        ));
    }

    /// Realistic bug caught: a targeted leadership-transfer request queued
    /// behind bulk replication traffic can expire before the target receives
    /// it, making a healthy handoff appear to have failed.
    #[test]
    fn timeout_now_uses_the_host_control_lane() {
        let message = Envelope {
            from: RaftReplicaId::must(1),
            to: RaftReplicaId::must(2),
            msg: Message::TimeoutNow(TimeoutNowRequest {
                term: 3,
                leader_id: RaftReplicaId::must(1),
                target_id: RaftReplicaId::must(2),
                transfer_id: 7,
                last_log_index: 12,
                last_log_term: 3,
            }),
        };

        assert!(is_control_message(&message));
    }

    #[test]
    fn runnable_groups_are_serviced_round_robin_with_a_group_budget() {
        let first_ticks = Arc::new(AtomicU64::new(0));
        let second_ticks = Arc::new(AtomicU64::new(0));
        let first_identity = identity(10, 101);
        let second_identity = identity(20, 202);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let _first_writer = host.issue_group_writer(first_identity).unwrap();
        let _second_writer = host.issue_group_writer(second_identity).unwrap();

        host.register_new_group(Box::new(TestGroup {
            identity: first_identity,
            tick_behavior: TickBehavior::Healthy,
            ticks: Arc::clone(&first_ticks),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.register_new_group(Box::new(TestGroup {
            identity: second_identity,
            tick_behavior: TickBehavior::Healthy,
            ticks: Arc::clone(&second_ticks),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.activate().unwrap();

        host.schedule_group_now(first_identity.raft_group_id)
            .unwrap();
        host.schedule_group_now(second_identity.raft_group_id)
            .unwrap();

        let budget = MultiRaftTurnBudget {
            max_groups: 1,
            max_messages: 0,
            max_ready_generations: 1,
            max_apply_entries: 1,
            max_apply_bytes: usize::MAX,
            max_snapshot_bytes: 1,
        };

        let first_turn = host.run_turn(1, budget).unwrap();
        let second_turn = host.run_turn(1, budget).unwrap();

        assert_eq!(first_turn.groups_serviced, 1);
        assert_eq!(second_turn.groups_serviced, 1);
        assert_eq!(
            first_ticks.load(Ordering::SeqCst) + second_ticks.load(Ordering::SeqCst),
            2
        );
        assert_eq!(first_ticks.load(Ordering::SeqCst), 1);
        assert_eq!(second_ticks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn one_shared_wal_sync_completes_every_prepared_group_with_its_extent_range() {
        let (wal, wal_state) = BatchWal::new(false);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(wal));
        let first_identity = identity(10, 101);
        let second_identity = identity(20, 202);
        let first_completions = Arc::new(Mutex::new(Vec::new()));
        let second_completions = Arc::new(Mutex::new(Vec::new()));
        let completed = Arc::new(AtomicUsize::new(0));

        let _first_writer = host.issue_group_writer(first_identity).unwrap();
        let _second_writer = host.issue_group_writer(second_identity).unwrap();
        host.register_new_group(Box::new(PersistenceGroup {
            identity: first_identity,
            records: vec![
                (RecordType::new(101), b"first-entry".to_vec()),
                (RecordType::new(102), b"first-hard-state".to_vec()),
            ],
            pending: false,
            completions: Arc::clone(&first_completions),
            completed: Arc::clone(&completed),
        }))
        .unwrap();
        host.register_new_group(Box::new(PersistenceGroup {
            identity: second_identity,
            records: vec![(RecordType::new(201), b"second-entry".to_vec())],
            pending: false,
            completions: Arc::clone(&second_completions),
            completed: Arc::clone(&completed),
        }))
        .unwrap();
        host.activate().unwrap();
        host.schedule_group_now(first_identity.raft_group_id)
            .unwrap();
        host.schedule_group_now(second_identity.raft_group_id)
            .unwrap();

        let turn = host
            .run_turn(
                0,
                MultiRaftTurnBudget {
                    max_groups: 2,
                    max_messages: 0,
                    max_ready_generations: 1,
                    max_apply_entries: 1,
                    max_apply_bytes: usize::MAX,
                    max_snapshot_bytes: usize::MAX,
                },
            )
            .unwrap();

        assert_eq!(turn.groups_serviced, 2);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while completed.load(Ordering::SeqCst) < 2 {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
            host.run_turn(
                0,
                MultiRaftTurnBudget {
                    max_groups: 2,
                    max_messages: 0,
                    max_ready_generations: 1,
                    max_apply_entries: 1,
                    max_apply_bytes: usize::MAX,
                    max_snapshot_bytes: usize::MAX,
                },
            )
            .unwrap();
        }

        let wal_state = wal_state.lock().unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 2);
        assert_eq!(wal_state.append_calls, 1);
        assert_eq!(
            wal_state.record_types,
            vec![
                RecordType::new(101),
                RecordType::new(102),
                RecordType::new(201)
            ]
        );
        assert_eq!(
            *first_completions.lock().unwrap(),
            vec![Ok((2, Some(Lsn::new(100))))]
        );
        assert_eq!(
            *second_completions.lock().unwrap(),
            vec![Ok((1, Some(Lsn::new(100 + 11 + 32 + 16 + 32))))]
        );
    }

    /// Realistic bug caught: performing the shared append-and-sync on the host
    /// scheduler thread prevents unrelated groups from receiving turns while
    /// storage is slow. The dedicated FIFO worker must allow an independent
    /// group to run before the blocked WAL operation is released.
    #[test]
    fn persistence_sync_does_not_block_unrelated_group_turns() {
        let (wal, wal_state) = BlockingWal::new();
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(wal));
        let first_identity = identity(10, 101);
        let second_identity = identity(20, 202);
        let healthy_identity = identity(30, 303);
        let first_completions = Arc::new(Mutex::new(Vec::new()));
        let second_completions = Arc::new(Mutex::new(Vec::new()));
        let completed = Arc::new(AtomicUsize::new(0));
        let healthy_ticks = Arc::new(AtomicU64::new(0));

        let _first_writer = host.issue_group_writer(first_identity).unwrap();
        let _second_writer = host.issue_group_writer(second_identity).unwrap();
        let _healthy_writer = host.issue_group_writer(healthy_identity).unwrap();
        host.register_new_group(Box::new(PersistenceGroup {
            identity: first_identity,
            records: vec![(RecordType::new(101), b"first-entry".to_vec())],
            pending: false,
            completions: Arc::clone(&first_completions),
            completed: Arc::clone(&completed),
        }))
        .unwrap();
        host.register_new_group(Box::new(PersistenceGroup {
            identity: second_identity,
            records: vec![(RecordType::new(201), b"second-entry".to_vec())],
            pending: false,
            completions: Arc::clone(&second_completions),
            completed: Arc::clone(&completed),
        }))
        .unwrap();
        host.register_new_group(Box::new(TestGroup {
            identity: healthy_identity,
            tick_behavior: TickBehavior::Healthy,
            ticks: Arc::clone(&healthy_ticks),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.activate().unwrap();
        host.schedule_group_now(first_identity.raft_group_id)
            .unwrap();
        host.schedule_group_now(second_identity.raft_group_id)
            .unwrap();
        host.schedule_group_now(healthy_identity.raft_group_id)
            .unwrap();

        let budget = MultiRaftTurnBudget {
            max_groups: 3,
            max_messages: 0,
            max_ready_generations: 1,
            max_apply_entries: 1,
            max_apply_bytes: usize::MAX,
            max_snapshot_bytes: usize::MAX,
        };
        let turn = host.run_turn(0, budget).unwrap();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while !wal_state.0.lock().unwrap().started {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert_eq!(turn.groups_serviced, 3);
        assert_eq!(healthy_ticks.load(Ordering::SeqCst), 1);
        assert_eq!(wal_state.0.lock().unwrap().append_calls, 1);

        {
            let mut state = wal_state.0.lock().unwrap();
            state.release = true;
            wal_state.1.notify_all();
        }

        let completion_deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while completed.load(Ordering::SeqCst) < 2 {
            assert!(std::time::Instant::now() < completion_deadline);
            std::thread::yield_now();
            host.run_turn(0, budget).unwrap();
        }

        assert_eq!(completed.load(Ordering::SeqCst), 2);
        assert_eq!(first_completions.lock().unwrap().len(), 1);
        assert_eq!(second_completions.lock().unwrap().len(), 1);
    }

    /// Realistic bug caught: a burst of independent Ready generations must not
    /// turn the host's cross-group WAL staging area into an unbounded queue.
    /// The deferred group's Ready remains unacknowledged and is completed by a
    /// later host turn, preserving both FIFO WAL order and per-group fencing.
    #[test]
    fn persistence_service_defers_ready_when_group_bound_is_full() {
        let (wal, wal_state) = BatchWal::new(false);
        let first_identity = identity(10, 101);
        let second_identity = identity(20, 202);
        let first_completions = Arc::new(Mutex::new(Vec::new()));
        let second_completions = Arc::new(Mutex::new(Vec::new()));
        let completed = Arc::new(AtomicUsize::new(0));
        let mut host = MultiRaftHost::new_with_config(
            NodeId(7),
            NodeRaftWal::new(wal),
            MultiRaftHostConfig {
                max_pending_persistence_groups: 1,
                max_pending_persistence_records: 3,
                max_pending_persistence_bytes: 128,
                ..MultiRaftHostConfig::default()
            },
        )
        .unwrap();

        let _first_writer = host.issue_group_writer(first_identity).unwrap();
        let _second_writer = host.issue_group_writer(second_identity).unwrap();
        host.register_new_group(Box::new(PersistenceGroup {
            identity: first_identity,
            records: vec![
                (RecordType::new(101), b"first-entry".to_vec()),
                (RecordType::new(102), b"first-hard-state".to_vec()),
            ],
            pending: false,
            completions: Arc::clone(&first_completions),
            completed: Arc::clone(&completed),
        }))
        .unwrap();
        host.register_new_group(Box::new(PersistenceGroup {
            identity: second_identity,
            records: vec![(RecordType::new(201), b"second-entry".to_vec())],
            pending: false,
            completions: Arc::clone(&second_completions),
            completed: Arc::clone(&completed),
        }))
        .unwrap();
        host.activate().unwrap();
        host.schedule_group_now(first_identity.raft_group_id)
            .unwrap();
        host.schedule_group_now(second_identity.raft_group_id)
            .unwrap();

        let budget = MultiRaftTurnBudget {
            max_groups: 2,
            max_messages: 0,
            max_ready_generations: 1,
            max_apply_entries: 1,
            max_apply_bytes: usize::MAX,
            max_snapshot_bytes: usize::MAX,
        };

        host.run_turn(0, budget).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while completed.load(Ordering::SeqCst) < 2 {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
            host.run_turn(0, budget).unwrap();
        }
        assert_eq!(wal_state.lock().unwrap().append_calls, 2);
        assert_eq!(completed.load(Ordering::SeqCst), 2);
        assert_eq!(second_completions.lock().unwrap().len(), 1);
    }

    /// Realistic bug caught: limiting only the number of prepared groups still
    /// permits a few large Ready generations to exhaust memory. Record and
    /// encoded-byte accounting must reject the next request before A-WAL is
    /// called, while retaining the accepted request's exact payloads.
    #[test]
    fn persistence_service_bounds_records_and_encoded_bytes() {
        let (wal, wal_state) = BatchWal::new(false);
        let first_identity = identity(10, 101);
        let second_identity = identity(20, 202);
        let mut service = PersistenceService::new(
            NodeRaftWal::new(wal),
            MultiRaftHostConfig {
                max_pending_persistence_groups: 2,
                max_pending_persistence_records: 2,
                max_pending_persistence_bytes: 27,
                ..MultiRaftHostConfig::default()
            },
        );

        service
            .try_submit(PendingPersistenceGroup {
                raft_group_id: first_identity.raft_group_id,
                timer_due: false,
                batch: HostedPersistenceBatch::new(vec![
                    (RecordType::new(101), b"first-entry".to_vec()),
                    (RecordType::new(102), b"first-hard-state".to_vec()),
                ]),
                outbound: Vec::new(),
                read_states: Vec::new(),
            })
            .unwrap();
        assert_eq!(
            service.status(),
            MultiRaftPersistenceStatus {
                pending_groups: 1,
                pending_records: 2,
                pending_bytes: 27,
            }
        );

        assert!(matches!(
            service.try_submit(PendingPersistenceGroup {
                raft_group_id: second_identity.raft_group_id,
                timer_due: false,
                batch: HostedPersistenceBatch::new(vec![(
                    RecordType::new(201),
                    b"second-entry".to_vec(),
                )]),
                outbound: Vec::new(),
                read_states: Vec::new(),
            }),
            Err(PersistenceAdmissionError::Capacity(_))
        ));
        assert_eq!(wal_state.lock().unwrap().append_calls, 0);

        service.bind_host_thread();
        service.dispatch_if_idle();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        let flushed = loop {
            if let Some(completion) = service.try_take_completion() {
                break completion;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        };
        assert_eq!(flushed.groups.len(), 1);
        assert_eq!(service.status(), MultiRaftPersistenceStatus::default());
        assert_eq!(wal_state.lock().unwrap().append_calls, 1);
    }

    #[test]
    fn unknown_cross_group_wal_outcome_is_fanned_out_before_host_fences() {
        let (wal, wal_state) = BatchWal::new(true);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(wal));
        let first_identity = identity(10, 101);
        let second_identity = identity(20, 202);
        let first_completions = Arc::new(Mutex::new(Vec::new()));
        let second_completions = Arc::new(Mutex::new(Vec::new()));
        let completed = Arc::new(AtomicUsize::new(0));

        let _first_writer = host.issue_group_writer(first_identity).unwrap();
        let _second_writer = host.issue_group_writer(second_identity).unwrap();
        host.register_new_group(Box::new(PersistenceGroup {
            identity: first_identity,
            records: vec![(RecordType::new(301), b"first".to_vec())],
            pending: false,
            completions: Arc::clone(&first_completions),
            completed: Arc::clone(&completed),
        }))
        .unwrap();
        host.register_new_group(Box::new(PersistenceGroup {
            identity: second_identity,
            records: vec![(RecordType::new(302), b"second".to_vec())],
            pending: false,
            completions: Arc::clone(&second_completions),
            completed: Arc::clone(&completed),
        }))
        .unwrap();
        host.activate().unwrap();
        host.schedule_group_now(first_identity.raft_group_id)
            .unwrap();
        host.schedule_group_now(second_identity.raft_group_id)
            .unwrap();

        let budget = MultiRaftTurnBudget {
            max_groups: 2,
            max_messages: 0,
            max_ready_generations: 1,
            max_apply_entries: 1,
            max_apply_bytes: usize::MAX,
            max_snapshot_bytes: usize::MAX,
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        let error = loop {
            match host.run_turn(0, budget) {
                Ok(_) => {
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::yield_now();
                }
                Err(error) => break error,
            }
        };
        assert_eq!(error, MultiRaftHostError::RecoveryRequired);

        assert_eq!(wal_state.lock().unwrap().append_calls, 1);
        assert_eq!(completed.load(Ordering::SeqCst), 0);
        assert_eq!(first_completions.lock().unwrap().len(), 1);
        assert_eq!(second_completions.lock().unwrap().len(), 1);
        assert!(first_completions.lock().unwrap()[0].is_err());
        assert!(second_completions.lock().unwrap()[0].is_err());
        assert_eq!(
            host.run_turn(0, MultiRaftTurnBudget::default()),
            Err(MultiRaftHostError::RecoveryRequired)
        );
    }

    #[test]
    fn scheduling_same_group_twice_does_not_duplicate_runnable_work() {
        let ticks = Arc::new(AtomicU64::new(0));
        let group_identity = identity(10, 101);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let _writer = host.issue_group_writer(group_identity).unwrap();
        host.register_new_group(Box::new(TestGroup {
            identity: group_identity,
            tick_behavior: TickBehavior::Healthy,
            ticks: Arc::clone(&ticks),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.activate().unwrap();

        host.schedule_group_now(group_identity.raft_group_id)
            .unwrap();
        host.schedule_group_now(group_identity.raft_group_id)
            .unwrap();

        let result = host
            .run_turn(
                1,
                MultiRaftTurnBudget {
                    max_groups: 2,
                    max_messages: 0,
                    max_ready_generations: 1,
                    max_apply_entries: 1,
                    max_apply_bytes: usize::MAX,
                    max_snapshot_bytes: 1,
                },
            )
            .unwrap();

        assert_eq!(result.groups_serviced, 1);
        assert_eq!(ticks.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn shared_timer_does_not_run_a_group_before_its_deadline() {
        let ticks = Arc::new(AtomicU64::new(0));
        let group_identity = identity(10, 101);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let _writer = host.issue_group_writer(group_identity).unwrap();
        host.register_new_group(Box::new(TestGroup {
            identity: group_identity,
            tick_behavior: TickBehavior::Healthy,
            ticks: Arc::clone(&ticks),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.activate().unwrap();
        host.schedule_group_after(group_identity.raft_group_id, 2)
            .unwrap();

        let budget = MultiRaftTurnBudget {
            max_groups: 1,
            max_messages: 0,
            max_ready_generations: 1,
            max_apply_entries: 1,
            max_apply_bytes: usize::MAX,
            max_snapshot_bytes: 1,
        };

        assert_eq!(host.run_turn(1, budget).unwrap().groups_serviced, 0);
        assert_eq!(ticks.load(Ordering::SeqCst), 0);
        assert_eq!(host.run_turn(1, budget).unwrap().groups_serviced, 1);
        assert_eq!(ticks.load(Ordering::SeqCst), 1);
    }

    #[test]
    /// Realistic bug caught: a sparse timer that is removed when it expires
    /// but never rearmed causes a healthy Raft group to stop sending
    /// heartbeats and eventually trigger avoidable elections.
    fn expired_group_timer_is_rearmed_after_each_turn() {
        let ticks = Arc::new(AtomicU64::new(0));
        let group_identity = identity(10, 101);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let _writer = host.issue_group_writer(group_identity).unwrap();
        host.register_new_group(Box::new(TestGroup {
            identity: group_identity,
            tick_behavior: TickBehavior::Healthy,
            ticks: Arc::clone(&ticks),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.activate().unwrap();
        host.schedule_group_after(group_identity.raft_group_id, 1)
            .unwrap();

        let budget = MultiRaftTurnBudget {
            max_groups: 1,
            max_messages: 0,
            ..MultiRaftTurnBudget::default()
        };

        assert_eq!(host.run_turn(1, budget).unwrap().groups_serviced, 1);
        assert_eq!(host.run_turn(1, budget).unwrap().groups_serviced, 1);
        assert_eq!(ticks.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn queued_messages_remain_bounded_and_are_not_dropped_between_turns() {
        let stepped = Arc::new(AtomicU64::new(0));
        let group_identity = identity(10, 101);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let _writer = host.issue_group_writer(group_identity).unwrap();
        host.register_new_group(Box::new(TestGroup {
            identity: group_identity,
            tick_behavior: TickBehavior::Healthy,
            ticks: Arc::new(AtomicU64::new(0)),
            stepped: Arc::clone(&stepped),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.activate().unwrap();

        for from in [20, 21] {
            host.enqueue_message(RoutedRaftMessage {
                raft_group_id: group_identity.raft_group_id,
                envelope: Envelope {
                    from: RaftReplicaId::must(from),
                    to: RaftReplicaId::must(group_identity.replica_id.0),
                    msg: Message::PreVoteResponse(raft::message::PreVoteResponse {
                        term: 0,
                        vote_granted: true,
                    }),
                },
            })
            .unwrap();
        }

        let budget = MultiRaftTurnBudget {
            max_groups: 2,
            max_messages: 1,
            ..MultiRaftTurnBudget::default()
        };

        assert_eq!(host.run_turn(0, budget).unwrap().messages_processed, 1);
        assert_eq!(stepped.load(Ordering::SeqCst), 1);
        assert_eq!(host.run_turn(0, budget).unwrap().messages_processed, 1);
        assert_eq!(stepped.load(Ordering::SeqCst), 2);
    }

    /// Realistic bug caught: a slow group can keep receiving transport wakeups
    /// faster than the shared scheduler services it, allowing an unbounded
    /// in-memory queue while node status reports no admission pressure.
    #[test]
    fn pending_message_admission_enforces_node_and_group_limits() {
        let group_identity = identity(10, 101);
        let mut host = MultiRaftHost::new_with_config(
            NodeId(7),
            NodeRaftWal::new(TestWal),
            MultiRaftHostConfig {
                max_pending_messages: 1,
                max_pending_group_messages: 1,
                ..MultiRaftHostConfig::default()
            },
        )
        .unwrap();
        let _writer = host.issue_group_writer(group_identity).unwrap();
        host.register_new_group(Box::new(healthy_group(group_identity, Vec::new())))
            .unwrap();
        host.activate().unwrap();

        let message = || RoutedRaftMessage {
            raft_group_id: group_identity.raft_group_id,
            envelope: Envelope {
                from: RaftReplicaId::must(20),
                to: RaftReplicaId::must(101),
                msg: Message::PreVoteResponse(raft::message::PreVoteResponse {
                    term: 0,
                    vote_granted: true,
                }),
            },
        };

        host.enqueue_message(message()).unwrap();
        assert!(matches!(
            host.enqueue_message(message()),
            Err(MultiRaftHostError::PendingMessagesFull { .. })
        ));
        assert_eq!(host.status().pending_message_count, 1);

        host.run_turn(0, MultiRaftTurnBudget::default()).unwrap();
        assert_eq!(host.status().pending_message_count, 0);
        assert_eq!(host.status().pending_message_bytes, 0);
    }

    #[test]
    fn control_message_promotes_its_group_ahead_of_bulk_work() {
        let bulk_stepped = Arc::new(AtomicU64::new(0));
        let control_stepped = Arc::new(AtomicU64::new(0));
        let bulk_identity = identity(10, 101);
        let control_identity = identity(20, 202);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let _bulk_writer = host.issue_group_writer(bulk_identity).unwrap();
        let _control_writer = host.issue_group_writer(control_identity).unwrap();

        host.register_new_group(Box::new(TestGroup {
            identity: bulk_identity,
            tick_behavior: TickBehavior::Healthy,
            ticks: Arc::new(AtomicU64::new(0)),
            stepped: Arc::clone(&bulk_stepped),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.register_new_group(Box::new(TestGroup {
            identity: control_identity,
            tick_behavior: TickBehavior::Healthy,
            ticks: Arc::new(AtomicU64::new(0)),
            stepped: Arc::clone(&control_stepped),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.activate().unwrap();

        host.enqueue_message(RoutedRaftMessage {
            raft_group_id: bulk_identity.raft_group_id,
            envelope: Envelope {
                from: RaftReplicaId::must(1),
                to: RaftReplicaId::must(101),
                msg: Message::AppendEntries(raft::message::AppendEntriesRequest {
                    term: 1,
                    leader_id: RaftReplicaId::must(1),
                    generation: 0,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![raft::entry::LogEntry::normal(1, 1, vec![1])],
                    leader_commit: 0,
                }),
            },
        })
        .unwrap();
        host.enqueue_message(RoutedRaftMessage {
            raft_group_id: control_identity.raft_group_id,
            envelope: Envelope {
                from: RaftReplicaId::must(1),
                to: RaftReplicaId::must(202),
                msg: Message::RequestVoteResponse(raft::message::RequestVoteResponse {
                    term: 1,
                    vote_granted: true,
                }),
            },
        })
        .unwrap();

        let turn = host
            .run_turn(
                0,
                MultiRaftTurnBudget {
                    max_groups: 1,
                    max_messages: 1,
                    ..MultiRaftTurnBudget::default()
                },
            )
            .unwrap();

        assert_eq!(turn.messages_processed, 1);
        assert_eq!(bulk_stepped.load(Ordering::SeqCst), 0);
        assert_eq!(control_stepped.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn snapshot_install_requests_use_the_control_lane() {
        let message = Envelope {
            from: RaftReplicaId::must(1),
            to: RaftReplicaId::must(101),
            msg: Message::InstallSnapshot(InstallSnapshotRequest::new(
                1,
                RaftReplicaId::must(1),
                SnapshotMetadata {
                    snapshot_id: 1,
                    last_included_index: 1,
                    last_included_term: 1,
                    conf_state: raft::types::ConfState::new(1, [RaftReplicaId::must(101)], [])
                        .unwrap(),
                    last_removed_replica: None,
                    size_bytes: 0,
                    checksum: [0; 32],
                },
            )),
        };

        assert!(is_control_message(&message));
    }

    #[test]
    fn status_lists_every_local_group_including_quarantined_groups() {
        let failing_identity = identity(10, 101);
        let healthy_identity = identity(20, 202);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let _failing_writer = host.issue_group_writer(failing_identity).unwrap();
        let _healthy_writer = host.issue_group_writer(healthy_identity).unwrap();

        host.register_new_group(Box::new(TestGroup {
            identity: failing_identity,
            tick_behavior: TickBehavior::GroupFailure,
            ticks: Arc::new(AtomicU64::new(0)),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.register_new_group(Box::new(TestGroup {
            identity: healthy_identity,
            tick_behavior: TickBehavior::Healthy,
            ticks: Arc::new(AtomicU64::new(0)),
            stepped: Arc::new(AtomicU64::new(0)),
            outbound: Vec::new(),
        }))
        .unwrap();
        host.activate().unwrap();
        host.tick_all(1).unwrap();

        let status = host.status();
        assert_eq!(status.node_id, NodeId(7));
        assert_eq!(status.state, MultiRaftHostState::Active);
        assert_eq!(
            status
                .groups
                .iter()
                .map(|group| group.identity.raft_group_id)
                .collect::<Vec<_>>(),
            vec![RaftGroupId(10), RaftGroupId(20)]
        );
        assert_eq!(
            status.groups[0].quarantine_reason.as_deref(),
            Some("injected group-local failure")
        );
        assert!(status.groups[1].quarantine_reason.is_none());
    }

    #[test]
    fn bounded_summary_caps_top_groups_and_includes_pending_proposals() {
        let mut status = MultiRaftHostStatus {
            node_id: NodeId(7),
            state: MultiRaftHostState::Active,
            pending_message_count: 528,
            pending_message_bytes: 5280,
            pending_persistence_groups: 0,
            pending_persistence_records: 0,
            pending_persistence_bytes: 0,
            groups: Vec::new(),
        };
        for index in 0..32 {
            status.groups.push(MultiRaftGroupStatus {
                identity: identity(index as u64 + 10, index as u64 + 101),
                role: None,
                leader_replica_id: None,
                term: 0,
                commit_index: 0,
                last_log_index: 0,
                applied_index: 0,
                snapshot_index: 0,
                uncommitted_bytes: 0,
                replication_inflight_bytes: 0,
                pending_work: false,
                pending_proposals: 0,
                apply_backlog_entries: 0,
                apply_backlog_bytes: 0,
                apply_backlog_age_ms: 0,
                apply_backlog_generations: 0,
                pending_messages: index + 1,
                pending_message_bytes: (index + 1) * 10,
                quarantine_reason: None,
                conf_state_version: None,
                joining: false,
                voters: Vec::new(),
                learners: Vec::new(),
                outgoing_voters: Vec::new(),
                replica_match_indices: Vec::new(),
                pending_conf_change_index: None,
                last_conf_change: None,
                last_removed_replica: None,
            });
        }

        let summary = status.bounded_summary(8);
        assert_eq!(summary.group_count, 32);
        assert_eq!(summary.pending_message_count, 528);
        assert_eq!(summary.top_groups.len(), 8);
        assert_eq!(summary.top_groups[0].pending_message_bytes, 320);
        assert_eq!(summary.top_groups[7].pending_message_bytes, 250);

        // This catches proposal-only load disappearing from the bounded view
        // when the same group has no queued network messages.
        for group in &mut status.groups {
            group.pending_messages = 0;
            group.pending_message_bytes = 0;
        }
        status.groups[0].pending_proposals = 2;

        let proposal_only_summary = status.bounded_summary(8);
        assert_eq!(proposal_only_summary.pending_proposal_count, 2);
        assert_eq!(proposal_only_summary.top_groups.len(), 1);
        assert_eq!(proposal_only_summary.top_groups[0].pending_proposals, 2);
    }

    /// Realistic bug caught: after local destruction, a delayed envelope must
    /// be rejected as a tombstoned lifetime instead of being treated as an
    /// unknown group that a later bootstrap could accidentally recreate.
    #[test]
    fn tombstoned_group_fences_delayed_messages_and_re_registration() {
        let group_identity = identity(10, 101);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let _writer = host.issue_group_writer(group_identity).unwrap();
        host.register_new_group(Box::new(healthy_group(group_identity, Vec::new())))
            .unwrap();
        host.activate().unwrap();

        host.tombstone_group(group_identity).unwrap();
        assert_eq!(host.group_count(), 0);

        let message = RoutedRaftMessage {
            raft_group_id: group_identity.raft_group_id,
            envelope: Envelope {
                from: RaftReplicaId::must(20),
                to: RaftReplicaId::must(group_identity.replica_id.0),
                msg: Message::PreVoteResponse(raft::message::PreVoteResponse {
                    term: 1,
                    vote_granted: true,
                }),
            },
        };
        assert_eq!(
            host.enqueue_message(message),
            Err(MultiRaftHostError::TombstonedReplica(group_identity))
        );
        assert!(matches!(
            host.issue_group_writer_after_activation(group_identity),
            Err(MultiRaftHostError::TombstonedReplica(identity)) if identity == group_identity
        ));
    }

    /// Realistic bug caught: restart recovery may have no active group object
    /// left, but it must still install the durable tombstone before activation
    /// so the first delayed envelope cannot enter an unknown-group path.
    #[test]
    fn startup_tombstone_is_admitted_without_a_group_runtime() {
        let group_identity = identity(10, 101);
        let mut host = MultiRaftHost::new(NodeId(7), NodeRaftWal::new(TestWal));
        let _writer = host.issue_group_writer(group_identity).unwrap();
        host.register_tombstoned_identity(group_identity).unwrap();
        host.activate().unwrap();

        let message = RoutedRaftMessage {
            raft_group_id: group_identity.raft_group_id,
            envelope: Envelope {
                from: RaftReplicaId::must(20),
                to: RaftReplicaId::must(group_identity.replica_id.0),
                msg: Message::PreVoteResponse(raft::message::PreVoteResponse {
                    term: 1,
                    vote_granted: true,
                }),
            },
        };
        assert!(matches!(
            host.enqueue_message(message),
            Err(MultiRaftHostError::TombstonedReplica(identity)) if identity == group_identity
        ));
    }

    #[test]
    /// Catches publication of a ReadState retained by the compatibility
    /// adapter after a higher-term message has removed local leadership.
    fn deferred_read_states_require_the_current_leader_term() {
        let local = RaftReplicaId::must(101);
        let former_term = ReadState {
            request_ctx: b"former-term".to_vec(),
            index: 10,
            term: 4,
        };
        let current_term = ReadState {
            request_ctx: b"current-term".to_vec(),
            index: 11,
            term: 5,
        };
        let mut states = vec![former_term, current_term.clone()];

        retain_current_leader_read_states(&mut states, local, Some(local), 5);
        assert_eq!(states, vec![current_term]);

        retain_current_leader_read_states(&mut states, local, Some(RaftReplicaId::must(202)), 5);
        assert!(states.is_empty());
    }
}
