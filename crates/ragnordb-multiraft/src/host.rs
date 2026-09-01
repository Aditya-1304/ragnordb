//! Physical-node host for the independent Raft replicas assigned to one server.
//!
//! The host owns the cross-group admission boundary. It schedules one group
//! operation at a time, keeps inbound work tagged by group, and preserves the
//! per-group Ready ordering delegated to each [`HostedRaftGroup`]. Shared A-WAL
//! uncertainty still fences every local replica.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use raft::{
    core::{
        node::{ProposeError, RaftError, SnapshotInstallError, StepError},
        ready::AdvanceError,
    },
    message::Envelope,
    traits::{log_store::LogStore, stable_store::StableStore},
    types::LogIndex,
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
    pub max_snapshot_bytes: usize,
}

impl Default for MultiRaftTurnBudget {
    fn default() -> Self {
        Self {
            max_groups: 64,
            max_messages: 256,
            max_ready_generations: 1,
            max_apply_entries: 128,
            max_snapshot_bytes: 4 * 1024 * 1024,
        }
    }
}

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

impl PendingGroupMessages {
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

fn is_control_message(message: &RaftMessageEnvelope) -> bool {
    matches!(
        message.msg,
        raft::message::Message::PreVote(_)
            | raft::message::Message::PreVoteResponse(_)
            | raft::message::Message::RequestVote(_)
            | raft::message::Message::RequestVoteResponse(_)
            | raft::message::Message::AppendEntriesResponse(_)
            | raft::message::Message::InstallSnapshotResponse(_)
    ) || matches!(
        &message.msg,
        raft::message::Message::AppendEntries(request) if request.entries.is_empty()
    )
}

/// Type-erased lifecycle boundary for one hosted Raft replica.
///
/// A single call performs the triggering Raft operation and drains the
/// resulting Ready lifecycle before returning. This prevents another caller
/// from interleaving work between `step()` and persistence/apply.
pub trait HostedRaftGroup: Send {
    fn identity(&self) -> RaftReplicaIdentity;

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
                MultiRaftTurnBudget {
                    max_groups: usize::MAX,
                    max_messages: usize::MAX,
                    max_ready_generations: 1,
                    max_apply_entries: usize::MAX,
                    max_snapshot_bytes: usize::MAX,
                },
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

    fn has_pending_work(&self) -> bool {
        self.ready_loop.has_pending_work()
    }

    fn tick_and_drain(&mut self, ticks: u64) -> Result<Vec<RaftMessageEnvelope>, HostedGroupError> {
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

    fn tick_and_drain_budgeted(
        &mut self,
        ticks: u64,
        budget: MultiRaftTurnBudget,
    ) -> Result<HostedGroupTurn, HostedGroupError> {
        let mut turn = self.drain_ready_budgeted(budget)?;

        if turn.ready_generations > 0 || self.ready_loop.has_pending_work() {
            return Ok(turn);
        }

        self.ready_loop.tick(ticks).map_err(classify_ready_error)?;
        let after_tick = self.drain_ready_budgeted(budget);
        match after_tick {
            Ok(after_tick) => {
                turn.outbound.extend(after_tick.outbound);
                turn.ready_generations += after_tick.ready_generations;
                turn.apply_entries += after_tick.apply_entries;
                turn.snapshot_bytes += after_tick.snapshot_bytes;
                Ok(turn)
            }
            Err(error) => Err(error),
        }
    }

    fn step_and_drain_budgeted(
        &mut self,
        message: RaftMessageEnvelope,
        budget: MultiRaftTurnBudget,
    ) -> Result<HostedGroupTurn, HostedGroupError> {
        let mut turn = self.drain_ready_budgeted(budget)?;

        if turn.ready_generations > 0 || self.ready_loop.has_pending_work() {
            return Ok(turn);
        }

        self.ready_loop
            .step(message)
            .map_err(classify_ready_error)?;
        let after_step = self.drain_ready_budgeted(budget)?;
        turn.outbound.extend(after_step.outbound);
        turn.ready_generations += after_step.ready_generations;
        turn.apply_entries += after_step.apply_entries;
        turn.snapshot_bytes += after_step.snapshot_bytes;
        Ok(turn)
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
        Self {
            node_id,
            node_wal,
            state: HostState::Registering,
            groups: BTreeMap::new(),
            runnable: RunnableGroupQueue::default(),
            timers: GroupTimerScheduler::default(),
            pending_messages: BTreeMap::new(),
            pending_recovered: BTreeSet::new(),
            issued_writers: BTreeSet::new(),
            quarantined: BTreeMap::new(),
        }
    }

    pub fn from_recovered(
        node_id: NodeId,
        node_wal: NodeRaftWal<W>,
        recovered: &RecoveredRaftStorage,
    ) -> Self {
        Self {
            node_id,
            node_wal,
            state: HostState::Registering,
            groups: BTreeMap::new(),
            runnable: RunnableGroupQueue::default(),
            timers: GroupTimerScheduler::default(),
            pending_messages: BTreeMap::new(),
            pending_recovered: recovered
                .replicas()
                .map(|(identity, _)| *identity)
                .collect(),
            issued_writers: BTreeSet::new(),
            quarantined: BTreeMap::new(),
        }
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

        self.pending_messages
            .entry(raft_group_id)
            .or_default()
            .push_back(message);
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

        while result.groups_serviced < budget.max_groups {
            let Some(raft_group_id) = self.runnable.pop() else {
                break;
            };

            if self.quarantined.contains_key(&raft_group_id) {
                self.pending_messages.remove(&raft_group_id);
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

            if had_message && result.messages_processed >= budget.max_messages {
                let control = is_control_message(
                    &pending_message
                        .as_ref()
                        .expect("message was checked above")
                        .envelope,
                );
                self.pending_messages
                    .get_mut(&raft_group_id)
                    .expect("popped message implies a group queue")
                    .push_front(pending_message.expect("message was checked above"));
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
                self.pending_messages
                    .get_mut(&raft_group_id)
                    .expect("popped message implies a group queue")
                    .push_front(pending_message.take().expect("message was checked above"));
            }

            let group_budget = MultiRaftTurnBudget {
                max_groups: budget.max_groups.saturating_sub(result.groups_serviced),
                max_messages: budget
                    .max_messages
                    .saturating_sub(result.messages_processed),
                max_ready_generations: budget
                    .max_ready_generations
                    .saturating_sub(result.ready_generations),
                max_apply_entries: budget
                    .max_apply_entries
                    .saturating_sub(result.apply_entries),
                max_snapshot_bytes: budget
                    .max_snapshot_bytes
                    .saturating_sub(result.snapshot_bytes),
            };

            let group_result = {
                let group = self
                    .groups
                    .get_mut(&raft_group_id)
                    .expect("runnable group came from the active registry");

                match (process_message, pending_message.take()) {
                    (true, Some(message)) => {
                        group.step_and_drain_budgeted(message.envelope, group_budget)
                    }
                    (false, None) | (false, Some(_)) => {
                        group.tick_and_drain_budgeted(ticks, group_budget)
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
                    result
                        .outbound
                        .extend(turn.outbound.into_iter().map(|envelope| RoutedRaftMessage {
                            raft_group_id,
                            envelope,
                        }));

                    self.ensure_shared_wal_healthy()?;
                    self.reschedule_after_turn(raft_group_id);
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
                    self.pending_messages.remove(&raft_group_id);
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
                            .push_front(message);
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

        let result = {
            let group = self
                .groups
                .get_mut(&raft_group_id)
                .expect("group was resolved above");

            group.step_and_drain(message.envelope)
        };

        let messages = match result {
            Ok(messages) => messages,

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

        Ok(messages
            .into_iter()
            .map(|envelope| RoutedRaftMessage {
                raft_group_id,
                envelope,
            })
            .collect())
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
    #[error("the host has not completed local replica registration")]
    NotActive,
    #[error("the host is already active")]
    AlreadyActive,
    #[error("the shared Raft WAL requires full node recovery")]
    RecoveryRequired,
    #[error("no local replica is registered for Raft group {0:?}")]
    UnknownGroup(RaftGroupId),
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
    use raft::{message::Message, types::ReplicaId as RaftReplicaId};
    use ragnordb_common::ids::ReplicaId;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };
    use wal::{error::BatchAppendFailure, types::RecordType, wal::BatchAppendResult};

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
}
