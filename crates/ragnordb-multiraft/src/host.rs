//! Physical-node host for the independent Raft replicas assigned to one server.
//!
//! The host owns the cross-group admission boundary. It schedules bounded group
//! operations, keeps inbound work tagged by group, and preserves the per-group
//! Ready ordering delegated to each [`HostedRaftGroup`]. Prepared Ready records
//! are coalesced into one shared A-WAL sync when a scheduler turn has multiple
//! eligible groups. Shared A-WAL uncertainty still fences every local replica.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, RwLock},
};

use raft::{
    core::{
        node::{ProposeError, RaftError, SnapshotInstallError, StepError},
        ready::AdvanceError,
    },
    message::Envelope,
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{LogIndex, Role},
};
use ragnordb_common::ids::{NodeId, RaftGroupId, ReplicaId};

use crate::{
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
}

impl Default for MultiRaftHostConfig {
    fn default() -> Self {
        Self {
            max_pending_messages: 8 * 1024,
            max_pending_message_bytes: 64 * 1024 * 1024,
            max_pending_group_messages: 2 * 1024,
            max_pending_group_message_bytes: 16 * 1024 * 1024,
            max_proposal_bytes: 16 * 1024 * 1024,
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
    pub pending_messages: usize,
    pub pending_message_bytes: usize,
    pub quarantine_reason: Option<String>,
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
    pub groups: Vec<MultiRaftGroupStatus>,
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
    pub ready_generations: usize,
    pub apply_entries: usize,
    pub snapshot_bytes: usize,
    pub(crate) persistence: Option<HostedPersistenceBatch>,
}

/// One group's encoded Ready records awaiting the node-wide WAL boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HostedPersistenceBatch {
    pub(crate) records: Vec<(RecordType, Vec<u8>)>,
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
}

#[derive(Debug, Default)]
struct GroupTimerScheduler {
    now: u64,
    deadlines: BTreeMap<u64, BTreeSet<RaftGroupId>>,
    scheduled: BTreeMap<RaftGroupId, u64>,
}

impl GroupTimerScheduler {
    fn schedule_after(&mut self, raft_group_id: RaftGroupId, delay: u64) {
        let deadline = self.now.saturating_add(delay);

        if self
            .scheduled
            .get(&raft_group_id)
            .is_some_and(|existing| *existing <= deadline)
        {
            return;
        }

        self.remove(raft_group_id);
        self.deadlines
            .entry(deadline)
            .or_default()
            .insert(raft_group_id);
        self.scheduled.insert(raft_group_id, deadline);
    }

    fn advance(&mut self, ticks: u64) -> Vec<RaftGroupId> {
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
                    self.scheduled.remove(&raft_group_id);
                    due_groups.push(raft_group_id);
                }
            }
        }

        due_groups
    }

    fn remove(&mut self, raft_group_id: RaftGroupId) {
        let Some(deadline) = self.scheduled.remove(&raft_group_id) else {
            return;
        };

        if let Some(groups) = self.deadlines.get_mut(&deadline) {
            groups.remove(&raft_group_id);
            if groups.is_empty() {
                self.deadlines.remove(&deadline);
            }
        }
    }
}

#[derive(Debug, Default)]
struct PendingGroupMessages {
    control: VecDeque<RoutedRaftMessage>,
    bulk: VecDeque<RoutedRaftMessage>,
}

struct PendingPersistenceGroup {
    raft_group_id: RaftGroupId,
    batch: HostedPersistenceBatch,
    /// Outbound envelopes are held until the group's Ready persistence has
    /// completed. This keeps custom group adapters subject to the same
    /// persistence-before-message invariant as the built-in adapter.
    outbound: Vec<RaftMessageEnvelope>,
}

impl PendingGroupMessages {
    fn len(&self) -> usize {
        self.control.len() + self.bulk.len()
    }

    fn wire_bytes(&self) -> usize {
        self.control
            .iter()
            .chain(self.bulk.iter())
            .map(|message| crate::transport::routed_message_wire_size(message).unwrap_or(0))
            .sum()
    }

    fn is_empty(&self) -> bool {
        self.control.is_empty() && self.bulk.is_empty()
    }

    fn has_control(&self) -> bool {
        !self.control.is_empty()
    }

    fn pop(&mut self) -> Option<RoutedRaftMessage> {
        self.control.pop_front().or_else(|| self.bulk.pop_front())
    }

    fn push_front(&mut self, message: RoutedRaftMessage) {
        if is_control_message(&message.envelope) {
            self.control.push_front(message);
        } else {
            self.bulk.push_front(message);
        }
    }

    fn push_back(&mut self, message: RoutedRaftMessage) {
        if is_control_message(&message.envelope) {
            self.control.push_back(message);
        } else {
            self.bulk.push_back(message);
        }
    }
}

pub(crate) fn is_control_message(message: &RaftMessageEnvelope) -> bool {
    matches!(
        message.msg,
        raft::message::Message::PreVote(_)
            | raft::message::Message::PreVoteResponse(_)
            | raft::message::Message::RequestVote(_)
            | raft::message::Message::RequestVoteResponse(_)
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
            pending_messages: 0,
            pending_message_bytes: 0,
            quarantine_reason: None,
        }
    }

    /// Returns whether work from an earlier operation must be resumed before a
    /// new Raft mutation is admitted. Lightweight adapters may use the
    /// compatibility default because their operation is host-atomic.
    fn has_pending_work(&self) -> bool {
        false
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
                .ready
                .map(|ready| ready.messages)
                .unwrap_or_default(),
            ready_generations: progress.ready_generations,
            apply_entries: progress.apply_entries,
            snapshot_bytes: progress.snapshot_bytes,
            persistence: None,
        })
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
                .ready
                .map(|ready| ready.messages)
                .unwrap_or_default(),
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
        if self.ready_loop.has_pending_persistence() {
            let progress = self
                .ready_loop
                .prepare_next_ready_for_batch(&mut self.snapshot_store, budget)
                .map_err(classify_apply_error)?;

            return Ok(HostedGroupTurn {
                outbound: Vec::new(),
                ready_generations: progress.ready_generations,
                apply_entries: 0,
                snapshot_bytes: progress.snapshot_bytes,
                persistence: progress.request.map(|request| HostedPersistenceBatch {
                    records: request.records,
                }),
            });
        }

        if self.ready_loop.has_pending_apply() {
            return self.drain_ready_budgeted(budget);
        }

        let progress = self
            .ready_loop
            .prepare_next_ready_for_batch(&mut self.snapshot_store, budget)
            .map_err(classify_apply_error)?;

        Ok(HostedGroupTurn {
            outbound: Vec::new(),
            ready_generations: progress.ready_generations,
            apply_entries: 0,
            snapshot_bytes: progress.snapshot_bytes,
            persistence: progress.request.map(|request| HostedPersistenceBatch {
                records: request.records,
            }),
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
            pending_messages: 0,
            pending_message_bytes: 0,
            quarantine_reason: None,
        }
    }

    fn has_pending_work(&self) -> bool {
        self.ready_loop.has_pending_work()
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

        if turn.ready_generations > 0 || self.ready_loop.has_pending_work() {
            return Ok(turn.outbound);
        }

        self.ready_loop.tick(ticks).map_err(classify_ready_error)?;

        let after_tick = self.drain_ready()?;
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

        if self.ready_loop.has_pending_work() {
            return Err(HostedGroupError::Retryable(
                "a previous Ready generation is still being resumed".to_string(),
            ));
        }

        self.ready_loop
            .step(message)
            .map_err(classify_ready_error)?;

        let after_step = self.drain_ready()?;
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

        if self.ready_loop.has_pending_work() {
            return Err(HostedGroupError::Retryable(
                "a previous Ready generation is still being resumed".to_string(),
            ));
        }

        let index = self
            .ready_loop
            .propose(command, encoded_len)
            .map_err(classify_ready_error)?;

        let after_proposal = self.drain_ready()?;
        turn.outbound.extend(after_proposal.outbound);

        Ok((index, turn.outbound))
    }

    fn tick_and_prepare_budgeted(
        &mut self,
        ticks: u64,
        budget: MultiRaftTurnBudget,
    ) -> Result<HostedGroupTurn, HostedGroupError> {
        let mut turn = self.prepare_ready_budgeted(budget)?;

        if turn.persistence.is_some()
            || turn.ready_generations > 0
            || self.ready_loop.has_pending_work()
        {
            return Ok(turn);
        }

        self.ready_loop.tick(ticks).map_err(classify_ready_error)?;
        let after_tick = self.prepare_ready_budgeted(budget);
        match after_tick {
            Ok(after_tick) => {
                turn.outbound.extend(after_tick.outbound);
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
                .ready
                .map(|ready| ready.messages)
                .unwrap_or_default(),
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
        | ReadyLoopError::SnapshotInstall(SnapshotInstallError::RecoveryRequired)
        | ReadyLoopError::Advance(AdvanceError::RecoveryRequired) => {
            HostedGroupError::RecoveryRequired
        }

        ReadyLoopError::PendingReady | ReadyLoopError::RetryablePersistence(_) => {
            HostedGroupError::Retryable(error.to_string())
        }

        ReadyLoopError::Proposal(_) | ReadyLoopError::Step(_) => {
            HostedGroupError::Rejected(error.to_string())
        }

        other => HostedGroupError::Group(other.to_string()),
    }
}

fn classify_apply_error(error: ReadyApplyError) -> HostedGroupError {
    match error {
        ReadyApplyError::Ready(error) => classify_ready_error(error),

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
    W: RaftWal,
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

    /// Permanently isolated groups for this process lifetime.
    ///
    /// Group-local corruption/apply/snapshot failures do not stop unrelated
    /// Raft groups. Shared-WAL uncertainty still moves the entire host into
    /// RecoveryRequired.
    quarantined: BTreeMap<RaftGroupId, String>,
}

impl<W> MultiRaftHost<W>
where
    W: RaftWal,
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

        Ok(Self {
            node_id,
            node_wal,
            config,
            state: HostState::Registering,
            groups: BTreeMap::new(),
            runnable: RunnableGroupQueue::default(),
            timers: GroupTimerScheduler::default(),
            pending_messages: BTreeMap::new(),
            pending_message_count: 0,
            pending_message_bytes: 0,
            pending_recovered: BTreeSet::new(),
            issued_writers: BTreeSet::new(),
            quarantined: BTreeMap::new(),
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

        Ok(Self {
            node_id,
            node_wal,
            config,
            state: HostState::Registering,
            groups: BTreeMap::new(),
            runnable: RunnableGroupQueue::default(),
            timers: GroupTimerScheduler::default(),
            pending_messages: BTreeMap::new(),
            pending_message_count: 0,
            pending_message_bytes: 0,
            pending_recovered: recovered
                .replicas()
                .map(|(identity, _)| *identity)
                .collect(),
            issued_writers: BTreeSet::new(),
            quarantined: BTreeMap::new(),
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

    fn register_group(
        &mut self,
        group: Box<dyn HostedRaftGroup>,
        recovered: bool,
    ) -> Result<(), MultiRaftHostError> {
        self.ensure_registering()?;
        self.ensure_shared_wal_healthy()?;
        let identity = group.identity();
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

        if !self.issued_writers.insert(identity) {
            return Err(MultiRaftHostError::WalWriterAlreadyIssued(identity));
        }

        self.node_wal
            .group_writer_for(identity)
            .map_err(MultiRaftHostError::WalRegistration)
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

    fn message_wire_bytes(message: &RoutedRaftMessage) -> usize {
        crate::transport::routed_message_wire_size(message)
            .expect("a message admitted by the host must remain serializable")
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
            .push_back(message);
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

        for raft_group_id in self.timers.advance(ticks) {
            if self.groups.contains_key(&raft_group_id)
                && !self.quarantined.contains_key(&raft_group_id)
            {
                self.runnable.enqueue(raft_group_id);
            }
        }

        let mut result = MultiRaftTurnResult::default();
        let mut persistence_groups = Vec::new();

        while result.groups_serviced < budget.max_groups {
            let Some(raft_group_id) = self.runnable.pop() else {
                break;
            };

            if self.quarantined.contains_key(&raft_group_id) {
                self.remove_pending_group(raft_group_id);
                continue;
            }

            let mut pending_message = if budget.max_messages == 0 {
                None
            } else {
                self.pending_messages
                    .get_mut(&raft_group_id)
                    .and_then(PendingGroupMessages::pop)
            };
            let had_message = pending_message.is_some();
            if let Some(message) = pending_message.as_ref() {
                self.account_pending_removal(Self::message_wire_bytes(message));
            }

            if had_message && result.messages_processed >= budget.max_messages {
                let control = is_control_message(
                    &pending_message
                        .as_ref()
                        .expect("message was checked above")
                        .envelope,
                );
                let message = pending_message.expect("message was checked above");
                self.pending_messages
                    .get_mut(&raft_group_id)
                    .expect("popped message implies a group queue")
                    .push_front(message.clone());
                self.account_pending_addition(Self::message_wire_bytes(&message));
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
            let process_message = had_message && !group_has_pending_work;
            let retry_message = process_message.then(|| {
                pending_message
                    .as_ref()
                    .expect("a processed message must be present")
                    .clone()
            });

            if had_message && !process_message {
                let message = pending_message.take().expect("message was checked above");
                self.pending_messages
                    .get_mut(&raft_group_id)
                    .expect("popped message implies a group queue")
                    .push_front(message.clone());
                self.account_pending_addition(Self::message_wire_bytes(&message));
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
                        group.step_and_prepare_budgeted(message.envelope, group_budget)
                    }
                    (false, None) | (false, Some(_)) => {
                        group.tick_and_prepare_budgeted(ticks, group_budget)
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
                        persistence_groups.push(PendingPersistenceGroup {
                            raft_group_id,
                            batch,
                            outbound: turn.outbound,
                        });
                    } else {
                        result
                            .outbound
                            .extend(turn.outbound.into_iter().map(|envelope| RoutedRaftMessage {
                                raft_group_id,
                                envelope,
                            }));

                        self.ensure_shared_wal_healthy()?;
                        self.reschedule_after_turn(raft_group_id);
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
                        let control = is_control_message(&message.envelope);
                        self.pending_messages
                            .get_mut(&raft_group_id)
                            .expect("retrying message implies a group queue")
                            .push_front(message.clone());
                        self.account_pending_addition(Self::message_wire_bytes(&message));
                        if control {
                            self.runnable.enqueue_control(raft_group_id);
                        } else {
                            self.runnable.enqueue(raft_group_id);
                        }
                    } else {
                        self.reschedule_after_turn(raft_group_id);
                    }
                    let _ = reason;
                }

                Err(HostedGroupError::Rejected(reason)) => {
                    if self.node_wal.recovery_required() {
                        self.state = HostState::RecoveryRequired;
                        return Err(MultiRaftHostError::RecoveryRequired);
                    }
                    let _ = reason;
                    self.reschedule_after_turn(raft_group_id);
                }
            }
        }

        if !persistence_groups.is_empty() {
            let total_records = persistence_groups
                .iter()
                .map(|pending| pending.batch.records.len())
                .sum::<usize>();
            let records: Vec<_> = persistence_groups
                .iter()
                .flat_map(|pending| pending.batch.records.iter())
                .map(|(record_type, payload)| (*record_type, payload.as_slice()))
                .collect();

            let mut shared_outcome = if records.is_empty() {
                Ok(BatchAppendResult {
                    record_extents: Vec::new(),
                    final_end_lsn: wal::lsn::Lsn::ZERO,
                })
            } else {
                self.node_wal.append_batch_and_sync(&records)
            };

            if let Ok(batch_result) = &shared_outcome
                && (batch_result.record_extents.len() != total_records
                    || (total_records > 0
                        && batch_result
                            .record_extents
                            .last()
                            .is_none_or(|extent| extent.end_lsn != batch_result.final_end_lsn)))
            {
                self.node_wal.require_recovery(
                    "shared A-WAL returned an invalid cross-group batch frontier",
                );
                shared_outcome = Err(BatchAppendFailure::OutcomeUnknown {
                    result: batch_result.clone(),
                    source: wal::error::WalError::BrokenDurabilityContract,
                });
            }

            let mut extent_offset = 0;
            let mut recovery_required = false;

            for pending in persistence_groups {
                let record_count = pending.batch.records.len();
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

                let completion = {
                    let group = self
                        .groups
                        .get_mut(&pending.raft_group_id)
                        .expect("a pending persistence batch belongs to an active group");
                    group.complete_persistence(group_outcome, group_budget_for_completion(budget))
                };

                match completion {
                    Ok(turn) => {
                        result.apply_entries += turn.apply_entries;
                        result.snapshot_bytes += turn.snapshot_bytes;
                        result.outbound.extend(
                            pending
                                .outbound
                                .into_iter()
                                .chain(turn.outbound)
                                .map(|envelope| RoutedRaftMessage {
                                    raft_group_id: pending.raft_group_id,
                                    envelope,
                                }),
                        );

                        if self.node_wal.recovery_required() {
                            recovery_required = true;
                        } else {
                            self.reschedule_after_turn(pending.raft_group_id);
                        }
                    }
                    Err(HostedGroupError::RecoveryRequired) => {
                        recovery_required = true;
                    }
                    Err(HostedGroupError::Group(reason)) => {
                        if self.node_wal.recovery_required() {
                            recovery_required = true;
                        } else {
                            self.quarantined.insert(pending.raft_group_id, reason);
                            self.remove_pending_group(pending.raft_group_id);
                        }
                    }
                    Err(HostedGroupError::Retryable(_reason)) => {
                        self.reschedule_after_turn(pending.raft_group_id);
                    }
                    Err(HostedGroupError::Rejected(_reason)) => {
                        self.reschedule_after_turn(pending.raft_group_id);
                    }
                }
            }

            if recovery_required || self.node_wal.recovery_required() {
                self.state = HostState::RecoveryRequired;
                return Err(MultiRaftHostError::RecoveryRequired);
            }
        }

        Ok(result)
    }

    fn validate_message(&self, message: &RoutedRaftMessage) -> Result<(), MultiRaftHostError> {
        let raft_group_id = message.raft_group_id;

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
                received: ReplicaId::from_raft(message.envelope.to),
            });
        }

        Ok(())
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

    fn reschedule_after_turn(&mut self, raft_group_id: RaftGroupId) {
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

        for raft_group_id in group_ids.iter().copied() {
            if !self.quarantined.contains_key(&raft_group_id) {
                self.runnable.enqueue(raft_group_id);
            }
        }

        self.run_turn(
            ticks,
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
        self.reschedule_after_turn(raft_group_id);

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

    fn ensure_registering(&self) -> Result<(), MultiRaftHostError> {
        match self.state {
            HostState::Registering => Ok(()),
            HostState::Active => Err(MultiRaftHostError::AlreadyActive),
            HostState::RecoveryRequired => Err(MultiRaftHostError::RecoveryRequired),
        }
    }

    fn ensure_active(&mut self) -> Result<(), MultiRaftHostError> {
        if self.node_wal.recovery_required() {
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
        "Raft envelope for group {raft_group_id:?} targets replica {received:?}, not local replica {expected:?}"
    )]
    RecipientMismatch {
        raft_group_id: RaftGroupId,
        expected: ReplicaId,
        received: ReplicaId,
    },
    #[error("Raft group {0:?} is already registered on this node")]
    DuplicateGroup(RaftGroupId),
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
        message::{InstallSnapshotRequest, Message},
        types::{ReplicaId as RaftReplicaId, SnapshotMetadata},
    };
    use ragnordb_common::ids::ReplicaId;
    use std::sync::{
        Arc, Mutex,
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
                persistence: Some(HostedPersistenceBatch {
                    records: self.records.clone(),
                }),
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
            node::{ProposeError, RaftError, SnapshotInstallError, StepError},
            ready::AdvanceError,
        };

        let cases = vec![
            ReadyLoopError::RecoveryRequired,
            ReadyLoopError::Tick(RaftError::RecoveryRequired),
            ReadyLoopError::Step(StepError::RecoveryRequired),
            ReadyLoopError::Proposal(ProposeError::RecoveryRequired),
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

        let wal_state = wal_state.lock().unwrap();
        assert_eq!(turn.groups_serviced, 2);
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

        assert_eq!(
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
            .unwrap_err(),
            MultiRaftHostError::RecoveryRequired
        );

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
}
