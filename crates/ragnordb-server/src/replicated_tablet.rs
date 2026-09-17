//! Production Ready owner for one replicated tablet Raft group.
//!
//! The physical MultiRaft scheduler owns cross-group fairness. This module
//! owns the tablet group's reactor boundary: SQL, snapshot, and Ready state
//! transitions remain serialized here while the scheduler interacts through
//! bounded, non-blocking mailboxes.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};

use raft::{
    core::{
        node::LeadershipTransferStatus,
        read_index::{ReadIndexError, ReadState},
        ready::Ready,
    },
    entry::EntryPayload,
    message::{Envelope, Message},
    traits::{log_store::LogStore, stable_store::StableStore},
    types::{ConfChange, ConfState, HardState, LogIndex, SnapshotMetadata},
};

use ragnordb_catalog::{CatalogLogExtent, CatalogLogRecord, DurableCatalogLog};
use ragnordb_common::{
    Error, Result,
    codec::WriteKind,
    command_codec::{
        CachedTabletCommandOutcome, MAX_TABLET_COMMAND_BATCH_BYTES,
        MAX_TABLET_COMMAND_BATCH_COMMANDS, NoopCommand, SingleShardCommitCommand, TabletCommand,
        TabletCommandBatchEnvelope, TabletCommandEnvelope, WriteEntry,
    },
    durability::DurabilityGate,
    encoding::{decode_row, encode_row},
    ids::{RaftGroupId, ReplicaId, RequestId, TableId, TabletId, TxnId},
    raft_bootstrap::RaftGroupBootstrap,
    rpc_codec::{
        TabletCommandRequest, TabletOutcomeQueryRequest, TabletReadRequest, TabletScanBatch,
        TabletScanRequest, TabletScanRow,
    },
};
use ragnordb_multiraft::{
    bootstrap::{FileBootstrapStore, load_durable_group_bootstrap},
    host::{
        HostedGroupError, HostedGroupTurn, HostedRaftGroup, MultiRaftGroupStatus, MultiRaftRole,
        MultiRaftTurnBudget, RaftMessageEnvelope, classify_ready_error,
    },
    proposal::{ProposalCompletion, ProposalRegistry, ProposalTicket},
    replica_startup::{
        bootstrap_joining_tablet_replica, bootstrap_tablet_replica, recover_joining_tablet_replica,
        recover_tablet_replica,
    },
    runtime::{AppliedRaftFrontier, RaftReadyLoop, ReadyLoopError, classify_ready_messages},
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
    tablet_apply::{
        CommittedTabletCommandDisposition, CommittedTabletCommandEntry, TabletCommandApplier,
    },
    transport::GroupRaftTransport,
};

use prost::Message as ProstMessage;
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
use tracing::warn;
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
const REACTOR_REGISTRATION_CAPACITY: usize = 256;
const REACTOR_TURN_GROUP_BUDGET: usize = 32;
const INTERNAL_BARRIER_CLIENT_NAMESPACE: u64 = 0x5241_474e_4f52_4442;
const READ_INDEX_CONTEXT_NAMESPACE: u64 = 0x5241_474e_4944_5801;
const READ_INDEX_FALLBACK_GRACE: Duration = Duration::from_millis(5);
/// Do not spend an additional wait interval on a command whose deadline is
/// already close. Batches are formed from work already queued in this owner
/// turn, so the normal path remains immediate at low load.
const TABLET_BATCH_DEADLINE_GRACE: Duration = Duration::from_millis(1);
/// Batch formation never waits for a future request. This bound additionally
/// limits time spent inspecting already-queued compatible requests in one
/// owner turn when the queue is continuously busy.
const TABLET_BATCH_MAX_DELAY: Duration = Duration::from_millis(1);
/// Bound the number of caller reply channels retained while one coalesced
/// ReadIndex request waits for quorum or its fallback barrier.
const MAX_PENDING_READ_BARRIER_WAITERS: usize = 1_024;

/// Identity that must remain consistent across the Raft Ready owner and the
/// tablet snapshot contract. Keeping these values together prevents a reactor
/// for a metadata-created tablet from accidentally publishing legacy group
/// identifiers in proposals, snapshots, or replicated SQL storage.
#[derive(Debug, Clone)]
struct TabletRuntimeIdentity {
    target: TabletSnapshotInstallTarget,
    /// Whether follower apply must materialize a derived SQL mirror. Metadata
    /// tablets are authoritative in their Raft state machine and are read via
    /// the gateway, so they deliberately set this to false.
    sql_mirror_enabled: bool,
}

impl TabletRuntimeIdentity {
    #[cfg(test)]
    fn new(target: TabletSnapshotInstallTarget) -> Self {
        Self {
            target,
            sql_mirror_enabled: true,
        }
    }

    fn with_sql_mirror(target: TabletSnapshotInstallTarget, sql_mirror_enabled: bool) -> Self {
        Self {
            target,
            sql_mirror_enabled,
        }
    }
}

type LocalWal = WalHandle<FsSegmentDirectory, ()>;
type Completion = ProposalCompletion<TabletCommandApplyOutcome, TabletCommandApplyError>;

/// Point-in-time routing state published without exposing the Ready owner.
#[derive(Debug, Clone, Default)]
pub struct ReplicatedTabletStatus {
    pub role: Option<MultiRaftRole>,
    /// Current Raft election timeout in logical ticks. The host scheduler uses
    /// this published value for follower and candidate deadlines so wall-clock
    /// parking never changes the Raft core's abstract timing contract.
    pub current_election_timeout_ticks: u64,
    pub leader_replica_id: Option<u64>,
    pub term: u64,
    pub commit_index: u64,
    pub last_log_index: u64,
    pub applied_index: u64,
    pub applied_term: u64,
    pub snapshot_index: u64,
    pub snapshot_term: u64,
    pub uncommitted_bytes: usize,
    pub replication_inflight_bytes: usize,
    pub apply_backlog_entries: usize,
    pub apply_backlog_bytes: usize,
    pub apply_backlog_age_ms: u64,
    pub apply_backlog_generations: usize,
    pub serving_leader: bool,
    pub runtime_error: Option<String>,
    /// Whether the local replica is present in the latest durable ConfState.
    /// `None` means no Ready-owned membership has been published yet, so
    /// lifecycle destruction must wait rather than infer removal from
    /// metadata placement alone.
    pub replica_in_conf_state: Option<bool>,
    pub conf_state_version: Option<u64>,
    pub joining: bool,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub outgoing_voters: Vec<u64>,
    pub replica_match_indices: Vec<(u64, u64)>,
    pub pending_conf_change_index: Option<u64>,
    pub last_conf_change: Option<(u64, u64)>,
    pub last_removed_replica: Option<(u64, u64, u64, u64)>,
    /// Reactor assignment published with the same status snapshot as the
    /// Raft frontier. A replacement lifetime must use a newer generation.
    pub reactor_id: usize,
    pub owner_generation: u64,
    /// True while an incoming or locally generated snapshot still owns a
    /// persistence boundary. Promotion must wait for this to clear.
    pub snapshot_install_pending: bool,
}

/// Completion payload published by a tablet reactor for a node-to-node RPC.
///
/// The request token, source node, and transport attempt remain owned by the
/// RPC dispatcher. The reactor publishes only the operation result after the
/// same validation, Raft ordering, and apply boundary used by local callers.
pub(crate) enum TabletRpcCompletion {
    Command(Result<TabletCommandApplyOutcome>),
    Read(Result<Option<Vec<u8>>>),
    Scan(Result<TabletScanBatch>),
    Outcome(Result<Option<CachedTabletCommandOutcome>>),
}

/// Non-blocking completion publication boundary between a tablet reactor and
/// the node RPC dispatcher. Implementations must not wait for network I/O or
/// a caller; the dispatcher owns response emission and transport backpressure.
pub(crate) trait TabletRpcCompletionSink: Send + Sync {
    fn publish(&self, token: u64, completion: TabletRpcCompletion);
}

enum HostRequest {
    Commit {
        commit: SingleNodeTxnCommit,
        reply: mpsc::SyncSender<Result<DurableWalExtent>>,
        deadline: Instant,
    },
    Catalog {
        update: CatalogLogRecord,
        reply: mpsc::SyncSender<Result<CatalogLogExtent>>,
        deadline: Instant,
    },
    Barrier {
        reply: ClientReply,
        deadline: Instant,
    },
    Command {
        request: TabletCommandRequest,
        reply: mpsc::SyncSender<Result<TabletCommandApplyOutcome>>,
        deadline: Instant,
    },
    ReadPoint {
        request: TabletReadRequest,
        reply: mpsc::SyncSender<Result<Option<Vec<u8>>>>,
        deadline: Instant,
    },
    Scan {
        request: TabletScanRequest,
        reply: mpsc::SyncSender<Result<TabletScanBatch>>,
        deadline: Instant,
    },
    OutcomeQuery {
        request: TabletOutcomeQueryRequest,
        reply: mpsc::SyncSender<Result<Option<CachedTabletCommandOutcome>>>,
        deadline: Instant,
    },
    RpcCommand {
        request: TabletCommandRequest,
        completion: Arc<dyn TabletRpcCompletionSink>,
        token: u64,
        deadline: Instant,
    },
    RpcReadPoint {
        request: TabletReadRequest,
        completion: Arc<dyn TabletRpcCompletionSink>,
        token: u64,
        deadline: Instant,
    },
    RpcScan {
        request: TabletScanRequest,
        completion: Arc<dyn TabletRpcCompletionSink>,
        token: u64,
        deadline: Instant,
    },
    RpcOutcomeQuery {
        request: TabletOutcomeQueryRequest,
        completion: Arc<dyn TabletRpcCompletionSink>,
        token: u64,
        deadline: Instant,
    },
}

struct PendingReadBarrierWaiter {
    reply: ClientReply,
    deadline: Instant,
}

struct PendingReadBarrier {
    context: Vec<u8>,
    term: u64,
    fallback_at: Instant,
    waiters: Vec<PendingReadBarrierWaiter>,
}

/// Node-level MultiRaft control messages.
///
/// Client SQL work continues to use `HostRequest`. Raft transport and ticking
/// enter through this separate host-owned channel.
enum RaftHostControlResult {
    Completed,
    Proposed(LogIndex),
    Transferred(LeadershipTransferStatus),
}

enum RaftHostControl {
    Tick {
        ticks: u64,
        reply: mpsc::SyncSender<std::result::Result<RaftHostControlResult, HostedGroupError>>,
    },

    Step {
        message: RaftMessageEnvelope,
        reply: mpsc::SyncSender<std::result::Result<RaftHostControlResult, HostedGroupError>>,
    },

    Propose {
        command: Vec<u8>,
        encoded_len: usize,
        reply: mpsc::SyncSender<std::result::Result<RaftHostControlResult, HostedGroupError>>,
    },

    ProposeConfChange {
        change: ConfChange,
        reply: mpsc::SyncSender<std::result::Result<RaftHostControlResult, HostedGroupError>>,
    },

    TransferLeadership {
        target: raft::types::NodeId,
        timeout_ticks: u64,
        reply: mpsc::SyncSender<std::result::Result<RaftHostControlResult, HostedGroupError>>,
    },
}

const REACTOR_MAILBOX_BYTE_CAPACITY: usize = 64 * 1024 * 1024;
const MAILBOX_ITEM_OVERHEAD: usize = 64;

/// A queue item provides the number of bytes reserved before it enters a
/// bounded mailbox. The size is carried by the item itself or derived from
/// already-known encoded lengths; the queue never scans its contents to
/// calculate occupancy.
trait MailboxSized {
    fn mailbox_bytes(&self) -> usize;
}

struct MailboxBudget {
    capacity: usize,
    used: AtomicUsize,
}

impl MailboxBudget {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            used: AtomicUsize::new(0),
        }
    }

    fn reserve(&self, bytes: usize) -> bool {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.capacity)
            })
            .is_ok()
    }

    fn release(&self, bytes: usize) {
        let _ = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                Some(used.saturating_sub(bytes))
            });
    }

    #[cfg(test)]
    fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}

/// Sender half of a bounded reactor mailbox.
///
/// The underlying synchronous channel bounds item count. `MailboxBudget`
/// independently bounds retained payload bytes, so a small number of large
/// requests cannot bypass the memory limit and a large number of tiny
/// requests cannot grow the queue past its count limit.
struct ByteBoundedSender<T: MailboxSized> {
    sender: SyncSender<MailboxEntry<T>>,
    budget: Arc<MailboxBudget>,
    wake: ReactorWake,
    identity: Option<RaftReplicaIdentity>,
    pending_items: Arc<AtomicUsize>,
}

struct MailboxEntry<T> {
    item: T,
    bytes: usize,
}

impl<T: MailboxSized> Clone for ByteBoundedSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            budget: self.budget.clone(),
            wake: self.wake.clone(),
            identity: self.identity,
            pending_items: self.pending_items.clone(),
        }
    }
}

impl<T: MailboxSized> ByteBoundedSender<T> {
    fn try_send(&self, item: T) -> std::result::Result<(), mpsc::TrySendError<T>> {
        let bytes = item.mailbox_bytes().max(MAILBOX_ITEM_OVERHEAD);
        if !self.budget.reserve(bytes) {
            return Err(mpsc::TrySendError::Full(item));
        }

        self.pending_items.fetch_add(1, Ordering::Release);
        match self.sender.try_send(MailboxEntry { item, bytes }) {
            Ok(()) => {
                if let Some(identity) = self.identity {
                    self.wake.wake_group(identity);
                } else {
                    self.wake.wake();
                }
                Ok(())
            }
            Err(mpsc::TrySendError::Full(entry)) => {
                self.pending_items.fetch_sub(1, Ordering::Release);
                self.budget.release(entry.bytes);
                Err(mpsc::TrySendError::Full(entry.item))
            }
            Err(mpsc::TrySendError::Disconnected(entry)) => {
                self.pending_items.fetch_sub(1, Ordering::Release);
                self.budget.release(entry.bytes);
                Err(mpsc::TrySendError::Disconnected(entry.item))
            }
        }
    }

    fn send(&self, item: T) -> std::result::Result<(), mpsc::SendError<T>> {
        let bytes = item.mailbox_bytes().max(MAILBOX_ITEM_OVERHEAD);
        if !self.budget.reserve(bytes) {
            return Err(mpsc::SendError(item));
        }

        self.pending_items.fetch_add(1, Ordering::Release);
        match self.sender.send(MailboxEntry { item, bytes }) {
            Ok(()) => {
                if let Some(identity) = self.identity {
                    self.wake.wake_group(identity);
                } else {
                    self.wake.wake();
                }
                Ok(())
            }
            Err(mpsc::SendError(entry)) => {
                self.pending_items.fetch_sub(1, Ordering::Release);
                self.budget.release(entry.bytes);
                Err(mpsc::SendError(entry.item))
            }
        }
    }
}

struct ByteBoundedReceiver<T: MailboxSized> {
    receiver: Receiver<MailboxEntry<T>>,
    budget: Arc<MailboxBudget>,
    pending_items: Arc<AtomicUsize>,
}

impl<T: MailboxSized> ByteBoundedReceiver<T> {
    fn try_recv(&self) -> std::result::Result<T, mpsc::TryRecvError> {
        match self.receiver.try_recv() {
            Ok(entry) => {
                self.pending_items.fetch_sub(1, Ordering::Release);
                self.budget.release(entry.bytes);
                Ok(entry.item)
            }
            Err(error) => Err(error),
        }
    }

    fn has_pending(&self) -> bool {
        self.pending_items.load(Ordering::Acquire) != 0
    }

    #[cfg(test)]
    fn recv_timeout(&self, timeout: Duration) -> std::result::Result<T, mpsc::RecvTimeoutError> {
        match self.receiver.recv_timeout(timeout) {
            Ok(entry) => {
                self.pending_items.fetch_sub(1, Ordering::Release);
                self.budget.release(entry.bytes);
                Ok(entry.item)
            }
            Err(error) => Err(error),
        }
    }
}

impl<T: MailboxSized> Drop for ByteBoundedReceiver<T> {
    fn drop(&mut self) {
        while let Ok(entry) = self.receiver.try_recv() {
            self.pending_items.fetch_sub(1, Ordering::Release);
            self.budget.release(entry.bytes);
        }
    }
}

struct ByteBoundedMailbox<T: MailboxSized> {
    _marker: std::marker::PhantomData<T>,
}

impl<T: MailboxSized> ByteBoundedMailbox<T> {
    #[cfg(test)]
    fn pair(
        capacity: usize,
        budget: Arc<MailboxBudget>,
        wake: ReactorWake,
    ) -> (ByteBoundedSender<T>, ByteBoundedReceiver<T>) {
        Self::pair_with_identity(capacity, budget, wake, None)
    }

    fn pair_for_group(
        capacity: usize,
        budget: Arc<MailboxBudget>,
        wake: ReactorWake,
        identity: RaftReplicaIdentity,
    ) -> (ByteBoundedSender<T>, ByteBoundedReceiver<T>) {
        Self::pair_with_identity(capacity, budget, wake, Some(identity))
    }

    fn pair_with_identity(
        capacity: usize,
        budget: Arc<MailboxBudget>,
        wake: ReactorWake,
        identity: Option<RaftReplicaIdentity>,
    ) -> (ByteBoundedSender<T>, ByteBoundedReceiver<T>) {
        let (sender, receiver) = mpsc::sync_channel(capacity);
        let pending_items = Arc::new(AtomicUsize::new(0));
        (
            ByteBoundedSender {
                sender,
                budget: budget.clone(),
                wake,
                identity,
                pending_items: pending_items.clone(),
            },
            ByteBoundedReceiver {
                receiver,
                budget,
                pending_items,
            },
        )
    }
}

impl MailboxSized for HostRequest {
    fn mailbox_bytes(&self) -> usize {
        let payload_bytes = match self {
            Self::Commit { commit, .. } => commit
                .encode()
                .map(|encoded| encoded.len())
                .unwrap_or_else(|_| std::mem::size_of_val(commit)),
            Self::Catalog { update, .. } => update.command.to_proto().encoded_len(),
            Self::Barrier { .. } => 0,
            Self::Command { request, .. } => request
                .to_proto()
                .map(|encoded| encoded.encoded_len())
                .unwrap_or_else(|_| std::mem::size_of_val(request)),
            Self::ReadPoint { request, .. } => request.to_proto().encoded_len(),
            Self::Scan { request, .. } => request.to_proto().encoded_len(),
            Self::OutcomeQuery { request, .. } => request.to_proto().encoded_len(),
            Self::RpcCommand { request, .. } => request
                .to_proto()
                .map(|encoded| encoded.encoded_len())
                .unwrap_or_else(|_| std::mem::size_of_val(request)),
            Self::RpcReadPoint { request, .. } => request.to_proto().encoded_len(),
            Self::RpcScan { request, .. } => request.to_proto().encoded_len(),
            Self::RpcOutcomeQuery { request, .. } => request.to_proto().encoded_len(),
        };
        std::mem::size_of_val(self).saturating_add(payload_bytes)
    }
}

fn raft_envelope_mailbox_bytes(message: &RaftMessageEnvelope) -> usize {
    let payload_bytes = match &message.msg {
        Message::AppendEntries(request) => {
            request.entries.iter().map(|entry| entry.encoded_len).sum()
        }
        Message::ReadIndex(request) => request.context.len(),
        Message::ReadIndexResponse(response) => response.context.len(),
        _ => 0,
    };
    std::mem::size_of_val(message).saturating_add(payload_bytes)
}

impl MailboxSized for RaftHostControl {
    fn mailbox_bytes(&self) -> usize {
        let payload_bytes = match self {
            Self::Tick { .. } => 0,
            Self::Step { message, .. } => raft_envelope_mailbox_bytes(message),
            Self::Propose {
                command,
                encoded_len,
                ..
            } => command.len().max(*encoded_len),
            Self::ProposeConfChange { change, .. } => std::mem::size_of_val(change),
            Self::TransferLeadership { .. } => 0,
        };
        std::mem::size_of_val(self).saturating_add(payload_bytes)
    }
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

        RaftHostControl::Propose { reply, .. }
        | RaftHostControl::ProposeConfChange { reply, .. }
        | RaftHostControl::TransferLeadership { reply, .. } => {
            let _ = reply.send(Err(HostedGroupError::Retryable(reason.to_string())));
        }
    }
}

/// Drain host requests which cannot legally mutate this Raft group while a
/// snapshot persistence retry owns its exact Ready/frontier.
///
/// Requests are answered instead of merely left queued so the synchronous
/// node-level host remains free to service unrelated Raft groups.
fn reject_snapshot_blocked_host_controls(
    host_control: &ByteBoundedReceiver<RaftHostControl>,
    reason: &str,
) {
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
    Commit(mpsc::SyncSender<Result<DurableWalExtent>>),
    Catalog(mpsc::SyncSender<Result<CatalogLogExtent>>),
    Barrier(mpsc::SyncSender<Result<()>>),
    Command(mpsc::SyncSender<Result<TabletCommandApplyOutcome>>),
    RpcCommand {
        token: u64,
        completion: Arc<dyn TabletRpcCompletionSink>,
        remote_commit: Option<SingleShardCommitCommand>,
    },
    RpcReadPoint {
        token: u64,
        completion: Arc<dyn TabletRpcCompletionSink>,
        request: TabletReadRequest,
        deadline: Instant,
    },
    RpcScan {
        token: u64,
        completion: Arc<dyn TabletRpcCompletionSink>,
        request: TabletScanRequest,
        deadline: Instant,
    },
}

struct PendingClient {
    ticket: ProposalTicket<TabletCommandApplyOutcome, TabletCommandApplyError>,
    reply: ClientReply,
}

/// One command prepared for admission but not yet assigned a Raft position.
/// The reply remains beside the envelope so a later batch can share a log
/// position without conflating per-request deadlines or RPC correlation.
struct PreparedCommandRequest {
    envelope: TabletCommandEnvelope,
    deadline: Instant,
    reply: ClientReply,
}

enum PendingTabletRequest {
    Raw(Box<HostRequest>),
    Prepared(Box<PreparedCommandRequest>),
}

impl PreparedCommandRequest {
    fn compatible_with(&self, first: &Self) -> bool {
        self.envelope.tablet_id == first.envelope.tablet_id
            && self.envelope.expected_epoch == first.envelope.expected_epoch
            && self.envelope.request_id.raft_group_id == first.envelope.request_id.raft_group_id
            && self.envelope.command.is_batchable()
    }
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

/// Stable ownership assigned to one `(RaftGroupId, ReplicaId)` lifetime.
///
/// A generation is consumed for every registration, including a replacement
/// lifetime after an earlier group is removed. This prevents a delayed
/// shutdown or completion from being interpreted as work for a newer owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReactorOwnership {
    pub reactor_id: usize,
    pub generation: u64,
}

#[derive(Clone)]
struct ReactorWake {
    thread: Arc<Mutex<Option<thread::Thread>>>,
    runnable: Arc<Mutex<BTreeSet<RaftReplicaIdentity>>>,
}

impl ReactorWake {
    fn new() -> Self {
        Self {
            thread: Arc::new(Mutex::new(None)),
            runnable: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    fn bind_current_thread(&self) {
        *self
            .thread
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(thread::current());
    }

    fn wake(&self) {
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

    fn wake_group(&self, identity: RaftReplicaIdentity) {
        self.runnable
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(identity);
        self.wake();
    }

    fn take_runnable(&self) -> Vec<RaftReplicaIdentity> {
        let mut runnable = self
            .runnable
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::mem::take(&mut *runnable).into_iter().collect()
    }
}

trait ReactorGroup: Send {
    fn identity(&self) -> RaftReplicaIdentity;
    fn ownership(&self) -> ReactorOwnership;
    fn is_shutdown(&self) -> bool;
    fn turn(&mut self) -> std::result::Result<bool, String>;
    fn fail(&mut self, reason: String);
}

struct ReactorRegistration {
    group: Box<dyn ReactorGroup>,
    reply: mpsc::SyncSender<std::result::Result<(), String>>,
}

enum ReactorCommand {
    Register(ReactorRegistration),
}

#[derive(Clone)]
struct ReactorAssignment {
    ownership: ReactorOwnership,
    command: SyncSender<ReactorCommand>,
    wake: ReactorWake,
    mailbox_budget: Arc<MailboxBudget>,
}

/// Fixed-size reactor set for all local replicated tablet state machines.
///
/// Registration is round-robin and intentionally has no stealing or live
/// migration path. A reactor owns every mutable field of its groups and runs
/// each group for a bounded turn before servicing the next group. The only
/// cross-thread data exposed by a group is its bounded request/control
/// mailbox, immutable identity, and published status snapshot.
pub(crate) struct FixedReactorSet {
    slots: Vec<ReactorAssignment>,
    next_reactor: AtomicUsize,
    next_generation: AtomicU64,
    shutdown: Arc<AtomicBool>,
    ownerships: Mutex<BTreeMap<RaftReplicaIdentity, ReactorOwnership>>,
    workers: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl FixedReactorSet {
    pub(crate) fn new(count: usize) -> Result<Arc<Self>> {
        if count == 0 {
            return Err(Error::Configuration(
                "reactor_count must be greater than zero".to_string(),
            ));
        }

        let shutdown = Arc::new(AtomicBool::new(false));
        let mailbox_budget = Arc::new(MailboxBudget::new(REACTOR_MAILBOX_BYTE_CAPACITY));
        let mut slots = Vec::with_capacity(count);
        let mut receivers = Vec::with_capacity(count);
        for reactor_id in 0..count {
            let (command, receiver) = mpsc::sync_channel(REACTOR_REGISTRATION_CAPACITY);
            let wake = ReactorWake::new();
            slots.push(ReactorAssignment {
                ownership: ReactorOwnership {
                    reactor_id,
                    generation: 0,
                },
                command,
                wake,
                mailbox_budget: mailbox_budget.clone(),
            });
            receivers.push(receiver);
        }

        let reactors = Arc::new(Self {
            slots,
            next_reactor: AtomicUsize::new(0),
            next_generation: AtomicU64::new(1),
            shutdown: Arc::clone(&shutdown),
            ownerships: Mutex::new(BTreeMap::new()),
            workers: Mutex::new(Vec::with_capacity(count)),
        });

        for (reactor_id, receiver) in receivers.into_iter().enumerate() {
            let wake = reactors.slots[reactor_id].wake.clone();
            let worker_shutdown = Arc::clone(&shutdown);
            let worker = thread::Builder::new()
                .name(format!("ragnordb-reactor-{reactor_id}"))
                .spawn(move || run_fixed_reactor(receiver, wake, worker_shutdown))
                .map_err(|source| {
                    Error::Configuration(format!("spawn reactor {reactor_id}: {source}"))
                })?;
            reactors
                .workers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(worker);
        }

        Ok(reactors)
    }

    fn assign(&self) -> Result<ReactorAssignment> {
        let reactor_id = self.next_reactor.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let slot = &self.slots[reactor_id];
        Ok(ReactorAssignment {
            ownership: ReactorOwnership {
                reactor_id,
                generation,
            },
            command: slot.command.clone(),
            wake: slot.wake.clone(),
            mailbox_budget: slot.mailbox_budget.clone(),
        })
    }

    fn register(&self, assignment: &ReactorAssignment, group: Box<dyn ReactorGroup>) -> Result<()> {
        if group.ownership() != assignment.ownership {
            return Err(Error::Configuration(
                "reactor group registration ownership does not match its assignment".to_string(),
            ));
        }
        let identity = group.identity();
        {
            let mut ownerships = self
                .ownerships
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if ownerships.insert(identity, assignment.ownership).is_some() {
                return Err(Error::Configuration(format!(
                    "reactor ownership already exists for {identity:?}"
                )));
            }
        }
        let (reply, response) = mpsc::sync_channel(1);
        let send_result = assignment
            .command
            .try_send(ReactorCommand::Register(ReactorRegistration {
                group,
                reply,
            }))
            .map_err(|error| {
                Error::Configuration(match error {
                    mpsc::TrySendError::Full(_) => "reactor registration queue is full".to_string(),
                    mpsc::TrySendError::Disconnected(_) => "reactor set has stopped".to_string(),
                })
            });
        if let Err(error) = send_result {
            self.ownerships
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&identity);
            return Err(error);
        }
        assignment.wake.wake();
        let result = response
            .recv()
            .map_err(|_| {
                Error::Configuration("reactor registration acknowledgement was lost".to_string())
            })?
            .map_err(Error::Configuration);
        if let Err(error) = result {
            self.ownerships
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&identity);
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn wake_all(&self) {
        let identities = self
            .ownerships
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(identity, ownership)| (*identity, ownership.reactor_id))
            .collect::<Vec<_>>();
        for (identity, reactor_id) in identities {
            self.slots[reactor_id].wake.wake_group(identity);
        }
    }
}

impl Drop for FixedReactorSet {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        for slot in &self.slots {
            slot.wake.wake();
        }
        let mut workers = self
            .workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for worker in workers.drain(..) {
            let _ = worker.join();
        }
    }
}

fn run_fixed_reactor(
    receiver: Receiver<ReactorCommand>,
    wake: ReactorWake,
    shutdown: Arc<AtomicBool>,
) {
    wake.bind_current_thread();
    let mut groups = BTreeMap::<RaftReplicaIdentity, Box<dyn ReactorGroup>>::new();
    let mut runnable = VecDeque::<RaftReplicaIdentity>::new();
    let mut queued = BTreeSet::<RaftReplicaIdentity>::new();

    while !shutdown.load(Ordering::Acquire) {
        for identity in wake.take_runnable() {
            if groups.contains_key(&identity) && queued.insert(identity) {
                runnable.push_back(identity);
            }
        }

        while let Ok(command) = receiver.try_recv() {
            match command {
                ReactorCommand::Register(registration) => {
                    let identity = registration.group.identity();
                    let result = if let std::collections::btree_map::Entry::Vacant(entry) =
                        groups.entry(identity)
                    {
                        entry.insert(registration.group);
                        if queued.insert(identity) {
                            runnable.push_back(identity);
                        }
                        Ok(())
                    } else {
                        Err(format!("reactor already owns group {identity:?}"))
                    };
                    let _ = registration.reply.send(result);
                }
            }
        }

        let mut serviced = 0;
        while serviced < REACTOR_TURN_GROUP_BUDGET {
            let Some(identity) = runnable.pop_front() else {
                break;
            };
            queued.remove(&identity);
            let Some(group) = groups.get_mut(&identity) else {
                continue;
            };
            if group.is_shutdown() {
                groups.remove(&identity);
                continue;
            }
            serviced += 1;
            match group.turn() {
                Ok(true) => {
                    if groups
                        .get(&identity)
                        .is_some_and(|group| !group.is_shutdown() && queued.insert(identity))
                    {
                        runnable.push_back(identity);
                    }
                }
                Ok(false) => {}
                Err(reason) => group.fail(reason),
            }
        }

        if serviced == 0 || groups.is_empty() {
            thread::park();
        } else {
            thread::yield_now();
        }
    }
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
    control: ByteBoundedSender<RaftHostControl>,
    status: Arc<RwLock<ReplicatedTabletStatus>>,
    pending: Option<mpsc::Receiver<std::result::Result<RaftHostControlResult, HostedGroupError>>>,
}

impl ReplicatedTabletGroupProxy {
    fn control_unavailable() -> HostedGroupError {
        HostedGroupError::Group("replicated tablet reactor has stopped".to_string())
    }

    fn next_timer_delay_ticks(&self) -> Option<u64> {
        let status = self
            .status
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if status.joining {
            return None;
        }

        let delay = match status.role {
            Some(MultiRaftRole::Leader) => HEARTBEAT_INTERVAL_TICKS,
            Some(MultiRaftRole::Follower | MultiRaftRole::Candidate) => {
                status.current_election_timeout_ticks.max(1)
            }
            None => 1,
        };
        Some(delay)
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
            mpsc::SyncSender<std::result::Result<RaftHostControlResult, HostedGroupError>>,
        ) -> RaftHostControl,
    ) -> std::result::Result<HostedGroupTurn, HostedGroupError> {
        if let Some(turn) = self.poll_pending()? {
            return Ok(turn);
        }

        let (reply, response) = mpsc::sync_channel(1);
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
        let (reply, response) = mpsc::sync_channel(1);
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
            RaftHostControl::ProposeConfChange { change, .. } => {
                RaftHostControl::ProposeConfChange { change, reply }
            }
            RaftHostControl::TransferLeadership {
                target,
                timeout_ticks,
                ..
            } => RaftHostControl::TransferLeadership {
                target,
                timeout_ticks,
                reply,
            },
        };

        self.control
            .try_send(control)
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => HostedGroupError::Retryable(
                    "replicated tablet host-control queue is full".to_string(),
                ),
                mpsc::TrySendError::Disconnected(_) => Self::control_unavailable(),
            })?;
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
            apply_backlog_entries: status.apply_backlog_entries,
            apply_backlog_bytes: status.apply_backlog_bytes,
            apply_backlog_age_ms: status.apply_backlog_age_ms,
            apply_backlog_generations: status.apply_backlog_generations,
            pending_work: self.has_pending_work(),
            pending_messages: 0,
            pending_message_bytes: 0,
            quarantine_reason: status.runtime_error,
            conf_state_version: status.conf_state_version,
            joining: status.joining,
            voters: status.voters.into_iter().map(ReplicaId).collect(),
            learners: status.learners.into_iter().map(ReplicaId).collect(),
            outgoing_voters: status.outgoing_voters.into_iter().map(ReplicaId).collect(),
            replica_match_indices: status
                .replica_match_indices
                .into_iter()
                .map(|(replica_id, index)| (ReplicaId(replica_id), index))
                .collect(),
            pending_conf_change_index: status.pending_conf_change_index,
            last_conf_change: status.last_conf_change,
            last_removed_replica: status.last_removed_replica.map(
                |(replica_id, index, term, version)| (ReplicaId(replica_id), index, term, version),
            ),
        }
    }

    fn has_pending_work(&self) -> bool {
        self.pending.is_some()
    }

    fn next_timer_delay_ticks(&self) -> Option<u64> {
        Self::next_timer_delay_ticks(self)
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
            reply: mpsc::sync_channel(1).0,
        })?;

        // The tablet reactor sends its Ready messages through its
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
            reply: mpsc::sync_channel(1).0,
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
            reply: mpsc::sync_channel(1).0,
        })?;
        let RaftHostControlResult::Proposed(index) = result else {
            return Err(HostedGroupError::Group(
                "tablet proposal control returned a non-proposal result".to_string(),
            ));
        };

        Ok((index, Vec::new()))
    }

    fn propose_conf_change(
        &mut self,
        change: ConfChange,
    ) -> std::result::Result<(LogIndex, Vec<RaftMessageEnvelope>), HostedGroupError> {
        if self.pending.is_some() {
            return Err(HostedGroupError::Retryable(
                "replicated tablet operation is still pending".to_string(),
            ));
        }
        let result = self.submit_direct(RaftHostControl::ProposeConfChange {
            change,
            reply: mpsc::sync_channel(1).0,
        })?;
        let RaftHostControlResult::Proposed(index) = result else {
            return Err(HostedGroupError::Group(
                "tablet membership proposal control returned a non-proposal result".to_string(),
            ));
        };

        Ok((index, Vec::new()))
    }

    fn transfer_leadership(
        &mut self,
        target: ReplicaId,
        timeout_ticks: u64,
    ) -> std::result::Result<(LeadershipTransferStatus, Vec<RaftMessageEnvelope>), HostedGroupError>
    {
        if self.pending.is_some() {
            return Err(HostedGroupError::Retryable(
                "replicated tablet operation is still pending".to_string(),
            ));
        }

        let target = target.to_raft().map_err(|reason| {
            HostedGroupError::Rejected(format!("invalid leadership-transfer target: {reason}"))
        })?;
        let result = self.submit_direct(RaftHostControl::TransferLeadership {
            target,
            timeout_ticks,
            reply: mpsc::sync_channel(1).0,
        })?;
        let RaftHostControlResult::Transferred(status) = result else {
            return Err(HostedGroupError::Group(
                "tablet leadership-transfer control returned a non-transfer result".to_string(),
            ));
        };

        // The worker owns transport publication. Returning an empty envelope
        // list prevents the physical host from publishing the same control
        // message a second time while still exposing the transfer status.
        Ok((status, Vec::new()))
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

/// Durable catalog-cache boundary owned by the Ready reactor.
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
    requests: ByteBoundedSender<HostRequest>,
    wake: ReactorWake,
    ownership: ReactorOwnership,
    status: Arc<RwLock<ReplicatedTabletStatus>>,
}

impl ReplicatedTabletHandle {
    /// Return the immutable owner assignment for this replica lifetime.
    /// Ownership is never changed in place; a future handoff must publish a
    /// new generation only after the old owner has quiesced and drained.
    pub fn reactor_ownership(&self) -> ReactorOwnership {
        self.ownership
    }

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

    pub(crate) fn status_shared(&self) -> Arc<RwLock<ReplicatedTabletStatus>> {
        self.status.clone()
    }

    /// Establish an applied current-term ordering point before a latest read.
    pub fn read_barrier(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::InvalidArgument("read barrier deadline overflowed".into()))?;
        self.read_barrier_until(deadline)
    }

    /// Establish an applied current-term ordering point without resetting the
    /// caller's deadline. ReadIndex admission and the fallback barrier share
    /// this exact deadline with the subsequent tablet read.
    pub(crate) fn read_barrier_until(&self, deadline: Instant) -> Result<()> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Error::ProposalUnavailable {
                reason: "read barrier deadline elapsed before admission".to_string(),
            });
        }
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .try_send(HostRequest::Barrier {
                reply: ClientReply::Barrier(reply),
                deadline,
            })
            .map_err(|error| Error::ProposalUnavailable {
                reason: match error {
                    mpsc::TrySendError::Full(_) => {
                        "read barrier admission queue is full before deadline".to_string()
                    }
                    mpsc::TrySendError::Disconnected(_) => {
                        "replicated tablet runtime has stopped".to_string()
                    }
                },
            })?;
        response
            .recv_timeout(remaining)
            .map_err(|_| Error::ProposalUnavailable {
                reason: "read barrier deadline elapsed before apply".to_string(),
            })?
    }

    /// Enqueue a node-to-node command without waiting for the proposal or
    /// apply result. The owning reactor publishes the result through the
    /// supplied token sink after the durable Raft/apply boundary is crossed.
    pub(crate) fn enqueue_rpc_command(
        &self,
        request: TabletCommandRequest,
        deadline: Instant,
        token: u64,
        completion: Arc<dyn TabletRpcCompletionSink>,
    ) -> Result<()> {
        if deadline <= Instant::now() {
            return Err(Error::ProposalUnavailable {
                reason: "tablet command deadline elapsed before admission".to_string(),
            });
        }
        self.requests
            .try_send(HostRequest::RpcCommand {
                request,
                completion,
                token,
                deadline,
            })
            .map_err(|error| Error::ProposalUnavailable {
                reason: match error {
                    mpsc::TrySendError::Full(_) => {
                        "tablet command admission queue is full".to_string()
                    }
                    mpsc::TrySendError::Disconnected(_) => {
                        "replicated tablet runtime has stopped".to_string()
                    }
                },
            })
    }

    /// Enqueue a point read without waiting on the reactor. Linearizability
    /// is preserved because the reactor performs the existing ReadIndex or
    /// fallback barrier before evaluating the read.
    pub(crate) fn enqueue_rpc_read_point(
        &self,
        request: TabletReadRequest,
        deadline: Instant,
        token: u64,
        completion: Arc<dyn TabletRpcCompletionSink>,
    ) -> Result<()> {
        if deadline <= Instant::now() {
            return Err(Error::ProposalUnavailable {
                reason: "tablet read deadline elapsed before admission".to_string(),
            });
        }
        self.requests
            .try_send(HostRequest::RpcReadPoint {
                request,
                completion,
                token,
                deadline,
            })
            .map_err(|error| Error::ProposalUnavailable {
                reason: match error {
                    mpsc::TrySendError::Full(_) => {
                        "tablet read admission queue is full".to_string()
                    }
                    mpsc::TrySendError::Disconnected(_) => {
                        "replicated tablet runtime has stopped".to_string()
                    }
                },
            })
    }

    /// Enqueue one bounded scan page without waiting on the reactor. The
    /// scan remains a single tablet-owner operation, so snapshot/epoch and
    /// MVCC checks cannot race a concurrent state-machine transition.
    pub(crate) fn enqueue_rpc_scan(
        &self,
        request: TabletScanRequest,
        deadline: Instant,
        token: u64,
        completion: Arc<dyn TabletRpcCompletionSink>,
    ) -> Result<()> {
        if deadline <= Instant::now() {
            return Err(Error::ProposalUnavailable {
                reason: "tablet scan deadline elapsed before admission".to_string(),
            });
        }
        self.requests
            .try_send(HostRequest::RpcScan {
                request,
                completion,
                token,
                deadline,
            })
            .map_err(|error| Error::ProposalUnavailable {
                reason: match error {
                    mpsc::TrySendError::Full(_) => {
                        "tablet scan admission queue is full".to_string()
                    }
                    mpsc::TrySendError::Disconnected(_) => {
                        "replicated tablet runtime has stopped".to_string()
                    }
                },
            })
    }

    /// Enqueue an outcome lookup without waiting on the reactor. This lookup
    /// is read-only and does not bypass the tablet owner's epoch or leader
    /// validation.
    pub(crate) fn enqueue_rpc_outcome_query(
        &self,
        request: TabletOutcomeQueryRequest,
        deadline: Instant,
        token: u64,
        completion: Arc<dyn TabletRpcCompletionSink>,
    ) -> Result<()> {
        if deadline <= Instant::now() {
            return Err(Error::ProposalUnavailable {
                reason: "tablet outcome query deadline elapsed before admission".to_string(),
            });
        }
        self.requests
            .try_send(HostRequest::RpcOutcomeQuery {
                request,
                completion,
                token,
                deadline,
            })
            .map_err(|error| Error::ProposalUnavailable {
                reason: match error {
                    mpsc::TrySendError::Full(_) => {
                        "tablet outcome query admission queue is full".to_string()
                    }
                    mpsc::TrySendError::Disconnected(_) => {
                        "replicated tablet runtime has stopped".to_string()
                    }
                },
            })
    }

    /// Submit one already-routed command to this tablet's Raft leader.
    ///
    /// The request identity is preserved byte-for-byte through proposal,
    /// commit, apply, and response. This is the retry safety boundary for a
    /// gateway: a transport timeout may cause the same request to be sent
    /// again, but the tablet state machine decides whether it is a fresh or
    /// deduplicated operation.
    pub fn submit_command(
        &self,
        request: TabletCommandRequest,
        timeout: Duration,
    ) -> Result<TabletCommandApplyOutcome> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::InvalidArgument("tablet request deadline overflowed".into()))?;
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(HostRequest::Command {
                request,
                reply,
                deadline,
            })
            .map_err(|_| Error::ProposalUnavailable {
                reason: "replicated tablet runtime has stopped".to_string(),
            })?;
        response
            .recv_timeout(timeout)
            .map_err(|_| Error::ProposalUnavailable {
                reason: "tablet command deadline elapsed before apply".to_string(),
            })?
    }

    /// Read one committed row at a caller-supplied MVCC timestamp.
    ///
    /// The Ready owner performs the read on the same thread that owns the
    /// mutable state machine. This avoids exposing a concurrent `Tablet` read
    /// view that could race snapshot installation or apply-frontier updates.
    pub fn read_point(
        &self,
        request: TabletReadRequest,
        timeout: Duration,
    ) -> Result<Option<Vec<u8>>> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::InvalidArgument("tablet read deadline overflowed".into()))?;
        self.read_point_until(request, deadline)
    }

    /// Execute a point read using an already-established absolute deadline.
    pub(crate) fn read_point_until(
        &self,
        request: TabletReadRequest,
        deadline: Instant,
    ) -> Result<Option<Vec<u8>>> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Error::ProposalUnavailable {
                reason: "tablet read deadline elapsed before execution".to_string(),
            });
        }
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .try_send(HostRequest::ReadPoint {
                request,
                reply,
                deadline,
            })
            .map_err(|error| Error::ProposalUnavailable {
                reason: match error {
                    mpsc::TrySendError::Full(_) => {
                        "tablet read admission queue is full before deadline".to_string()
                    }
                    mpsc::TrySendError::Disconnected(_) => {
                        "replicated tablet runtime has stopped".to_string()
                    }
                },
            })?;
        response
            .recv_timeout(remaining)
            .map_err(|_| Error::ProposalUnavailable {
                reason: "tablet read deadline elapsed before execution".to_string(),
            })?
    }

    /// Read one bounded, ordered page at a caller-supplied MVCC timestamp.
    ///
    /// Like point reads, the Ready owner executes this on the state-machine
    /// thread after the caller has crossed the current-term read barrier.
    pub fn scan_page(
        &self,
        request: TabletScanRequest,
        timeout: Duration,
    ) -> Result<TabletScanBatch> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::InvalidArgument("tablet scan deadline overflowed".into()))?;
        self.scan_page_until(request, deadline)
    }

    /// Execute one scan page using an already-established absolute deadline.
    pub(crate) fn scan_page_until(
        &self,
        request: TabletScanRequest,
        deadline: Instant,
    ) -> Result<TabletScanBatch> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Error::ProposalUnavailable {
                reason: "tablet scan deadline elapsed before execution".to_string(),
            });
        }
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .try_send(HostRequest::Scan {
                request,
                reply,
                deadline,
            })
            .map_err(|error| Error::ProposalUnavailable {
                reason: match error {
                    mpsc::TrySendError::Full(_) => {
                        "tablet scan admission queue is full before deadline".to_string()
                    }
                    mpsc::TrySendError::Disconnected(_) => {
                        "replicated tablet runtime has stopped".to_string()
                    }
                },
            })?;
        response
            .recv_timeout(remaining)
            .map_err(|_| Error::ProposalUnavailable {
                reason: "tablet scan deadline elapsed before execution".to_string(),
            })?
    }

    /// Query a retained V2 mutation outcome without proposing or executing a
    /// new command. Unknown outcomes remain unknown to the caller.
    pub fn query_original_outcome(
        &self,
        request: TabletOutcomeQueryRequest,
        timeout: Duration,
    ) -> Result<Option<CachedTabletCommandOutcome>> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::InvalidArgument("outcome query deadline overflowed".into()))?;
        let (reply, response) = mpsc::sync_channel(1);
        self.requests
            .send(HostRequest::OutcomeQuery {
                request,
                reply,
                deadline,
            })
            .map_err(|_| Error::ProposalUnavailable {
                reason: "replicated tablet runtime has stopped".to_string(),
            })?;
        response
            .recv_timeout(timeout)
            .map_err(|_| Error::ProposalUnavailable {
                reason: "tablet outcome query deadline elapsed".to_string(),
            })?
    }
}

impl DurableCommitLog for ReplicatedTabletHandle {
    fn append_single_node_commit(&self, commit: &SingleNodeTxnCommit) -> Result<DurableWalExtent> {
        let timeout = Duration::from_secs(30);
        let deadline = Instant::now() + timeout;
        let (reply, response) = mpsc::sync_channel(1);
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
        let (reply, response) = mpsc::sync_channel(1);
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
    host_control: ByteBoundedSender<RaftHostControl>,
    shutdown: Arc<AtomicBool>,
    _reactors: Arc<FixedReactorSet>,
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

    #[allow(clippy::needless_borrow, clippy::too_many_arguments)]
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
        reactors: Arc<FixedReactorSet>,
    ) -> Result<Self> {
        let cluster_id = config.cluster_id.clone().ok_or_else(|| {
            Error::Configuration("replicated tablet runtime requires cluster_id".to_string())
        })?;
        let target = TabletSnapshotInstallTarget {
            cluster_id,
            raft_group_id: TABLET_RAFT_GROUP_ID,
            tablet_id: TABLET_ID,
            table_id: TABLE_ID,
            tablet_epoch: TABLET_EPOCH,
        };
        Self::start_hosted_tablet_from_shared_recovery(
            config,
            wal,
            database,
            bootstrap,
            target,
            group_wal,
            transport,
            snapshot_store,
            snapshot_work,
            snapshot_endpoint,
            recovered,
            start_gate,
            reactors,
            true,
            None,
        )
    }

    /// Start a tablet reactor-owned group whose identity comes from metadata rather than
    /// the legacy Milestone 4 constants. The bootstrap and snapshot target
    /// are checked together so a Raft group cannot be paired with another
    /// tablet's state machine or snapshot files.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start_hosted_tablet_from_shared_recovery(
        config: &NodeConfig,
        wal: LocalWal,
        database: SharedLocalDatabase,
        bootstrap: RaftGroupBootstrap,
        target: TabletSnapshotInstallTarget,
        group_wal: NodeRaftWalHandle<LocalWal>,
        transport: GroupRaftTransport,
        snapshot_store: Arc<FileTabletSnapshotStore>,
        snapshot_work: SnapshotWorkController,
        snapshot_endpoint: GroupSnapshotEndpoint,
        recovered: &RecoveredRaftStorage,
        start_gate: Arc<AtomicBool>,
        reactors: Arc<FixedReactorSet>,
        install_sql_mirror: bool,
        provided_durability_gate: Option<DurabilityGate>,
    ) -> Result<Self> {
        let cluster_id = config.cluster_id.clone().ok_or_else(|| {
            Error::Configuration("replicated tablet runtime requires cluster_id".to_string())
        })?;

        if target.cluster_id != cluster_id {
            return Err(Error::RecoveryFailed {
                reason: format!(
                    "tablet snapshot target belongs to cluster {}, \
                     configured cluster is {}",
                    target.cluster_id, cluster_id,
                ),
            });
        }

        if bootstrap.cluster_id != cluster_id || bootstrap.raft_group_id != target.raft_group_id {
            return Err(Error::RecoveryFailed {
                reason: format!(
                    "tablet bootstrap identity ({}, {}) does not match target ({}, {})",
                    bootstrap.cluster_id,
                    bootstrap.raft_group_id.0,
                    target.cluster_id,
                    target.raft_group_id.0,
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

        let raft_identity = RaftReplicaIdentity::new(bootstrap.raft_group_id, local_replica_id)
            .map_err(|source| Error::Configuration(source.to_string()))?;
        let assignment = reactors.assign()?;

        let runtime_identity = TabletRuntimeIdentity::with_sql_mirror(target, install_sql_mirror);

        let mut bootstrap_store = FileBootstrapStore::open(config.data_dir.join("raft-bootstrap"))
            .map_err(|source| Error::RecoveryFailed {
                reason: source.to_string(),
            })?;

        let durable_bootstrap =
            load_durable_group_bootstrap(&bootstrap_store, runtime_identity.target.raft_group_id)
                .map_err(|source| Error::RecoveryFailed {
                reason: source.to_string(),
            })?;

        let (request_tx, request_rx) = ByteBoundedMailbox::pair_for_group(
            CHANNEL_CAPACITY,
            assignment.mailbox_budget.clone(),
            assignment.wake.clone(),
            raft_identity,
        );
        let (host_control_tx, host_control_rx) = ByteBoundedMailbox::pair_for_group(
            CHANNEL_CAPACITY,
            assignment.mailbox_budget.clone(),
            assignment.wake.clone(),
            raft_identity,
        );

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
            wake: assignment.wake.clone(),
            ownership: assignment.ownership,
            status: status.clone(),
        });

        let durability_gate = match provided_durability_gate {
            Some(gate) => gate,
            None => database
                .try_lock()
                .map_err(|_| {
                    Error::Configuration("database is busy during tablet startup".to_string())
                })?
                .durability_gate(),
        };

        let catalog_cache: Arc<dyn CatalogCacheWriter> = Arc::new(FencedCatalogCache {
            adapter: RagnorDbWalAdapter::new(wal.clone()),
            durability_gate,
        });

        let worker_shutdown = shutdown.clone();
        let owner: Box<dyn ReactorGroup> = if recovered.replica(raft_identity).is_some() {
            let durable_bootstrap = durable_bootstrap.ok_or_else(|| Error::RecoveryFailed {
                reason: format!(
                    "Raft WAL contains {:?} state but its durable bootstrap is missing",
                    raft_identity,
                ),
            })?;
            if durable_bootstrap != bootstrap {
                return Err(Error::RecoveryFailed {
                    reason: format!(
                        "resolved bootstrap for Raft group {} \
                         changed during startup",
                        runtime_identity.target.raft_group_id.0,
                    ),
                });
            }

            let replica = recovered.replica(raft_identity).expect("checked above");

            if install_sql_mirror {
                install_recovered_catalog(&database, replica)?;
            }

            let recovered_replica = recover_tablet_replica(
                durable_bootstrap,
                local_replica_id,
                group_wal,
                wal.durable_lsn(),
                replica,
                &snapshot_store,
                &runtime_identity.target,
                ELECTION_TIMEOUT_TICKS,
                HEARTBEAT_INTERVAL_TICKS,
            )
            .map_err(|source| Error::RecoveryFailed {
                reason: source.to_string(),
            })?;

            if install_sql_mirror {
                install_recovered_sql_mirror(
                    &database,
                    runtime_identity.target.table_id,
                    &recovered_replica.tablet,
                )?;
            }

            Box::new(ReadyOwner::new(
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
                runtime_identity.clone(),
                catalog_cache,
                snapshot_policy,
                assignment.ownership,
            )?)
        } else {
            let bootstrapped = bootstrap_tablet_replica(
                &mut bootstrap_store,
                &bootstrap,
                local_replica_id,
                group_wal,
                &runtime_identity.target,
                ELECTION_TIMEOUT_TICKS,
                HEARTBEAT_INTERVAL_TICKS,
            )
            .map_err(|source| Error::RecoveryFailed {
                reason: source.to_string(),
            })?;

            if install_sql_mirror {
                install_recovered_sql_mirror(
                    &database,
                    runtime_identity.target.table_id,
                    &bootstrapped.tablet,
                )?;
            }

            Box::new(ReadyOwner::new(
                bootstrapped.ready_loop,
                bootstrapped.tablet,
                bootstrapped.initial_ready,
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
                runtime_identity.clone(),
                catalog_cache,
                snapshot_policy,
                assignment.ownership,
            )?)
        };

        reactors.register(&assignment, owner)?;

        Ok(Self {
            handle,
            identity: raft_identity,
            host_control: host_control_tx,
            shutdown,
            _reactors: reactors,
        })
    }

    pub fn handle(&self) -> Arc<ReplicatedTabletHandle> {
        self.handle.clone()
    }

    /// Start a post-bootstrap replica from a committed membership witness.
    ///
    /// This path never opens or rewrites `RaftGroupBootstrap`: the witness is
    /// lifecycle authority for this new replica only. A fresh target starts in
    /// passive joining mode; a target with recovered WAL selects joining or
    /// ordinary restart from its recovered committed ConfState.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start_hosted_joining_tablet_from_shared_recovery(
        config: &NodeConfig,
        wal: LocalWal,
        database: SharedLocalDatabase,
        local_replica_id: ReplicaId,
        witness: ConfState,
        target: TabletSnapshotInstallTarget,
        group_wal: NodeRaftWalHandle<LocalWal>,
        transport: GroupRaftTransport,
        snapshot_store: Arc<FileTabletSnapshotStore>,
        snapshot_work: SnapshotWorkController,
        snapshot_endpoint: GroupSnapshotEndpoint,
        recovered: &RecoveredRaftStorage,
        start_gate: Arc<AtomicBool>,
        reactors: Arc<FixedReactorSet>,
        provided_durability_gate: Option<DurabilityGate>,
    ) -> Result<Self> {
        let cluster_id = config.cluster_id.clone().ok_or_else(|| {
            Error::Configuration("replicated tablet runtime requires cluster_id".to_string())
        })?;
        if target.cluster_id != cluster_id {
            return Err(Error::RecoveryFailed {
                reason: "dynamic tablet target belongs to another cluster".to_string(),
            });
        }
        witness.validate().map_err(|error| Error::RecoveryFailed {
            reason: format!("invalid dynamic membership witness: {error:?}"),
        })?;
        let identity = RaftReplicaIdentity::new(target.raft_group_id, local_replica_id)
            .map_err(|source| Error::Configuration(source.to_string()))?;
        let assignment = reactors.assign()?;
        if witness.contains(
            local_replica_id
                .to_raft()
                .map_err(|reason| Error::Configuration(reason.to_string()))?,
        ) {
            return Err(Error::RecoveryFailed {
                reason: format!(
                    "joining witness already contains local replica {}",
                    local_replica_id.0
                ),
            });
        }

        let (request_tx, request_rx) = ByteBoundedMailbox::pair_for_group(
            CHANNEL_CAPACITY,
            assignment.mailbox_budget.clone(),
            assignment.wake.clone(),
            identity,
        );
        let (host_control_tx, host_control_rx) = ByteBoundedMailbox::pair_for_group(
            CHANNEL_CAPACITY,
            assignment.mailbox_budget.clone(),
            assignment.wake.clone(),
            identity,
        );
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
            wake: assignment.wake.clone(),
            ownership: assignment.ownership,
            status: status.clone(),
        });
        let durability_gate = match provided_durability_gate {
            Some(gate) => gate,
            None => database
                .try_lock()
                .map_err(|_| Error::Configuration("database is busy during tablet startup".into()))?
                .durability_gate(),
        };
        let catalog_cache: Arc<dyn CatalogCacheWriter> = Arc::new(FencedCatalogCache {
            adapter: RagnorDbWalAdapter::new(wal.clone()),
            durability_gate,
        });
        let worker_shutdown = shutdown.clone();
        let runtime_identity = TabletRuntimeIdentity::with_sql_mirror(target, false);
        let owner: Box<dyn ReactorGroup> =
            if let Some(recovered_replica) = recovered.replica(identity) {
                let recovered = recover_joining_tablet_replica(
                    local_replica_id,
                    group_wal,
                    wal.durable_lsn(),
                    recovered_replica,
                    &snapshot_store,
                    &runtime_identity.target,
                    ELECTION_TIMEOUT_TICKS,
                    HEARTBEAT_INTERVAL_TICKS,
                )
                .map_err(|source| Error::RecoveryFailed {
                    reason: source.to_string(),
                })?;
                Box::new(ReadyOwner::new(
                    recovered.ready_loop,
                    recovered.tablet,
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
                    runtime_identity,
                    catalog_cache,
                    snapshot_policy,
                    assignment.ownership,
                )?)
            } else {
                let bootstrapped = bootstrap_joining_tablet_replica(
                    local_replica_id,
                    witness,
                    group_wal,
                    &runtime_identity.target,
                    ELECTION_TIMEOUT_TICKS,
                    HEARTBEAT_INTERVAL_TICKS,
                )
                .map_err(|source| Error::RecoveryFailed {
                    reason: source.to_string(),
                })?;
                Box::new(ReadyOwner::new(
                    bootstrapped.ready_loop,
                    bootstrapped.tablet,
                    bootstrapped.initial_ready,
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
                    runtime_identity,
                    catalog_cache,
                    snapshot_policy,
                    assignment.ownership,
                )?)
            };
        reactors.register(&assignment, owner)?;
        Ok(Self {
            handle,
            identity,
            host_control: host_control_tx,
            shutdown,
            _reactors: reactors,
        })
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
    table_id: TableId,
    tablet: &TabletCommandApplier,
) -> Result<()> {
    let storage = tablet.state_machine().tablet().storage().clone();
    let mut database = database.try_lock().map_err(|_| {
        Error::Configuration(
            "database owner is busy while replicated startup installs its recovered tablet"
                .to_string(),
        )
    })?;
    database.install_replicated_storage(table_id, storage)?;
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
        match TabletCommandEnvelope::decode(bytes) {
            Ok(envelope) => {
                if let TabletCommand::Catalog(command) = envelope.command {
                    database.apply_replicated_catalog(
                        &command,
                        ragnordb_common::ids::Timestamp(
                            (envelope.request_id.client_id >> 64) as u64,
                        ),
                    )?;
                }
            }
            Err(single_error) => {
                // Mutation batches never contain catalog entries, so they do
                // not contribute to the startup SQL-catalog projection. They
                // are replayed by the tablet state-machine recovery pass.
                TabletCommandBatchEnvelope::decode(bytes)
                    .map_err(|_| Error::CorruptData(single_error.to_string()))?;
            }
        }
    }
    Ok(())
}

impl Drop for ReplicatedTabletRuntime {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        self.handle.wake.wake_group(self.identity);
    }
}

struct ReadyOwner<W, LS, SS>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    ready_loop: RaftReadyLoop<W, LS, SS>,
    tablet: TabletCommandApplier,
    initial_ready: Option<Ready<Vec<u8>, Vec<u8>>>,
    transport: GroupRaftTransport,
    host_control: ByteBoundedReceiver<RaftHostControl>,
    requests: ByteBoundedReceiver<HostRequest>,
    database: SharedLocalDatabase,
    status: Arc<RwLock<ReplicatedTabletStatus>>,
    shutdown: Arc<AtomicBool>,
    start_gate: Arc<AtomicBool>,
    snapshot_store: Arc<FileTabletSnapshotStore>,
    snapshot_work: SnapshotWorkController,
    snapshot_endpoint: GroupSnapshotEndpoint,
    cluster_id: String,
    identity: TabletRuntimeIdentity,
    catalog_cache: Arc<dyn CatalogCacheWriter>,
    snapshot_policy: SnapshotPolicy,
    ownership: ReactorOwnership,
    registry: ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    clients: Vec<PendingClient>,
    pending_requests: VecDeque<PendingTabletRequest>,
    internal_barrier_allocator: InternalBarrierAllocator,
    was_leader: bool,
    leader_activation: Option<ragnordb_multiraft::proposal::ProposalPosition>,
    pending_read_barriers: Vec<PendingReadBarrier>,
    pending_read_states: Vec<ReadState>,
    next_read_index_context: u64,
    latest_snapshot: Option<TabletSnapshotImage>,
    last_snapshot_index: u64,
    last_snapshot_at: Instant,
    expected_snapshot_install: Option<SnapshotMetadata>,
    pending_snapshot_install: Option<PendingIncomingSnapshotInstall>,
    pending_local_snapshot: Option<PendingLocalSnapshotPublication>,
}

impl<W, LS, SS> ReadyOwner<W, LS, SS>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        ready_loop: RaftReadyLoop<W, LS, SS>,
        tablet: TabletCommandApplier,
        initial_ready: Option<Ready<Vec<u8>, Vec<u8>>>,
        transport: GroupRaftTransport,
        host_control: ByteBoundedReceiver<RaftHostControl>,
        requests: ByteBoundedReceiver<HostRequest>,
        database: SharedLocalDatabase,
        status: Arc<RwLock<ReplicatedTabletStatus>>,
        shutdown: Arc<AtomicBool>,
        start_gate: Arc<AtomicBool>,
        snapshot_store: Arc<FileTabletSnapshotStore>,
        snapshot_work: SnapshotWorkController,
        snapshot_endpoint: GroupSnapshotEndpoint,
        cluster_id: String,
        identity: TabletRuntimeIdentity,
        catalog_cache: Arc<dyn CatalogCacheWriter>,
        snapshot_policy: SnapshotPolicy,
        ownership: ReactorOwnership,
    ) -> Result<Self> {
        let latest_snapshot = ready_loop
            .persistence()
            .snapshot()
            .map(|pointer| snapshot_store.load_verified_by_name(&pointer.file_name))
            .transpose()
            .map_err(|error| Error::RecoveryFailed {
                reason: error.to_string(),
            })?;
        let last_snapshot_index = latest_snapshot
            .as_ref()
            .map(|image| image.metadata.last_included_index)
            .unwrap_or(0);

        Ok(Self {
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
            identity,
            catalog_cache,
            snapshot_policy,
            ownership,
            registry: ProposalRegistry::new(),
            clients: Vec::new(),
            pending_requests: VecDeque::new(),
            internal_barrier_allocator: InternalBarrierAllocator::default(),
            was_leader: false,
            leader_activation: None,
            pending_read_barriers: Vec::new(),
            pending_read_states: Vec::new(),
            next_read_index_context: 0,
            latest_snapshot,
            last_snapshot_index,
            last_snapshot_at: Instant::now(),
            expected_snapshot_install: None,
            pending_snapshot_install: None,
            pending_local_snapshot: None,
        })
    }

    fn fail_with(&mut self, reason: String) {
        self.shutdown.store(true, Ordering::Release);
        self.status
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .runtime_error = Some(reason);
    }

    fn turn(&mut self) -> std::result::Result<bool, String> {
        if !self.start_gate.load(Ordering::Acquire) {
            return Ok(false);
        }

        if let Some(ready) = self.initial_ready.take() {
            send_messages(
                &self.transport,
                &self.snapshot_endpoint,
                &self.latest_snapshot,
                ready.messages,
            );
        }

        self.run_turn()?;
        Ok(self.should_run_again())
    }

    fn should_run_again(&self) -> bool {
        self.host_control.has_pending()
            || self.requests.has_pending()
            || !self.pending_requests.is_empty()
            || self.initial_ready.is_some()
            || self.pending_snapshot_install.is_some()
            || self.pending_local_snapshot.is_some()
    }
}

impl<W, LS, SS> ReactorGroup for ReadyOwner<W, LS, SS>
where
    W: RaftWal + Send + 'static,
    LS: LogStore<Vec<u8>> + Send + 'static,
    SS: StableStore + Send + 'static,
{
    fn identity(&self) -> RaftReplicaIdentity {
        self.ready_loop.persistence().log_view().identity()
    }

    fn ownership(&self) -> ReactorOwnership {
        self.ownership
    }

    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    fn turn(&mut self) -> std::result::Result<bool, String> {
        ReadyOwner::turn(self)
    }

    fn fail(&mut self, reason: String) {
        self.fail_with(reason);
    }
}

impl<W, LS, SS> ReadyOwner<W, LS, SS>
where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    #[allow(clippy::needless_borrow, clippy::too_many_arguments)]
    fn run_turn(&mut self) -> std::result::Result<(), String> {
        let Self {
            ready_loop,
            tablet,
            transport,
            host_control,
            requests,
            database,
            status,
            shutdown,
            start_gate: _,
            snapshot_store,
            snapshot_work,
            snapshot_endpoint,
            cluster_id,
            identity,
            catalog_cache,
            snapshot_policy,
            ownership,
            registry,
            clients,
            pending_requests,
            internal_barrier_allocator,
            was_leader,
            leader_activation,
            pending_read_barriers,
            pending_read_states,
            next_read_index_context,
            latest_snapshot,
            last_snapshot_index,
            last_snapshot_at,
            expected_snapshot_install,
            pending_snapshot_install,
            pending_local_snapshot,
            initial_ready: _,
        } = self;

        // The destructuring above yields mutable references to the owner's fields.
        // Rebinding the references as mutable locals permits repeated reborrows while
        // preserving the single-reactor ownership boundary for the complete turn.
        let mut ready_loop = ready_loop;
        let mut tablet = tablet;
        let mut registry = registry;
        let mut clients = clients;
        let mut internal_barrier_allocator = internal_barrier_allocator;
        let mut leader_activation = leader_activation;
        let mut pending_read_barriers = pending_read_barriers;
        let mut pending_read_states = pending_read_states;
        let mut next_read_index_context = next_read_index_context;
        let mut latest_snapshot = latest_snapshot;
        let mut last_snapshot_index = last_snapshot_index;
        let mut last_snapshot_at = last_snapshot_at;
        let mut pending_local_snapshot = pending_local_snapshot;

        if shutdown.load(Ordering::Acquire) {
            return Ok(());
        }

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
                &identity,
                &mut pending_read_states,
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

                    return Ok(());
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
                                &identity,
                                &mut pending_read_states,
                            )? {
                                *expected_snapshot_install = Some(metadata);
                                *pending_snapshot_install = None;
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
                                &identity,
                                &mut pending_read_states,
                            )? {
                                *expected_snapshot_install = Some(metadata);
                                *pending_snapshot_install = None;
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
                                &identity,
                                &mut pending_read_states,
                            )? {
                                *expected_snapshot_install = Some(metadata);
                                *pending_snapshot_install = None;
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
                                &identity,
                                &mut pending_read_states,
                            )? {
                                *expected_snapshot_install = Some(metadata);
                                *pending_snapshot_install = None;
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
                                &identity,
                                &mut pending_read_states,
                            )? {
                                *expected_snapshot_install = Some(metadata);
                                *pending_snapshot_install = None;
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
                                &identity,
                                &mut pending_read_states,
                            )? {
                                *expected_snapshot_install = Some(metadata);
                                *pending_snapshot_install = None;
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

                    RaftHostControl::ProposeConfChange { change, reply } => {
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
                                &identity,
                                &mut pending_read_states,
                            )? {
                                *expected_snapshot_install = Some(metadata);
                                *pending_snapshot_install = None;
                            }
                            if had_pending_ready {
                                return Err(HostedGroupError::Retryable(
                                    "a previous Ready generation is still being resumed"
                                        .to_string(),
                                ));
                            }

                            // Membership entries are handled by the Raft core
                            // and Ready persistence boundary. They deliberately
                            // never pass through the tablet command decoder.
                            let index = ready_loop
                                .propose_conf_change(change)
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
                                &identity,
                                &mut pending_read_states,
                            )? {
                                *expected_snapshot_install = Some(metadata);
                                *pending_snapshot_install = None;
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

                    RaftHostControl::TransferLeadership {
                        target,
                        timeout_ticks,
                        reply,
                    } => {
                        let result: std::result::Result<
                            LeadershipTransferStatus,
                            HostedGroupError,
                        > = (|| {
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
                                &identity,
                                &mut pending_read_states,
                            )? {
                                *expected_snapshot_install = Some(metadata);
                                *pending_snapshot_install = None;
                            }
                            if had_pending_ready {
                                return Err(HostedGroupError::Retryable(
                                    "a previous Ready generation is still being resumed"
                                        .to_string(),
                                ));
                            }

                            // Leadership transfer is a control-plane action;
                            // it emits TimeoutNow through the normal Ready
                            // message path but creates no database entry.
                            let status = ready_loop
                                .transfer_leadership(target, timeout_ticks)
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
                                &identity,
                                &mut pending_read_states,
                            )? {
                                *expected_snapshot_install = Some(metadata);
                                *pending_snapshot_install = None;
                            }
                            Ok(status)
                        })();

                        let fatal_reason = fatal_host_control_reason(&result);
                        let _ = reply.send(result.map(RaftHostControlResult::Transferred));
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
                *pending_snapshot_install =
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
                    *pending_snapshot_install =
                        Some(PendingIncomingSnapshotInstall::Received { expected, received });
                    return Ok(());
                }
                Err(error) => return Err(error.to_string()),
            };
            let target = TabletSnapshotInstallTarget {
                cluster_id: identity.target.cluster_id.clone(),
                raft_group_id: identity.target.raft_group_id,
                tablet_id: identity.target.tablet_id,
                table_id: identity.target.table_id,
                tablet_epoch: identity.target.tablet_epoch,
            };
            match prepare_incoming_tablet_snapshot(
                snapshot_store.as_ref(),
                received.session,
                &target,
                install_permit,
            ) {
                Ok(prepared) => {
                    *pending_snapshot_install =
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
                            *pending_snapshot_install =
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
                                    *pending_snapshot_install =
                                        Some(PendingIncomingSnapshotInstall::BoundaryPending {
                                            expected,
                                            prepared,
                                        });
                                    return Ok(());
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
                            *pending_snapshot_install =
                                Some(PendingIncomingSnapshotInstall::BoundaryPending {
                                    expected,
                                    prepared,
                                });
                            return Ok(());
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
                    *tablet = TabletCommandApplier::new(installed.state_machine);
                    if identity.sql_mirror_enabled {
                        database
                            .blocking_lock()
                            .install_replicated_storage(
                                identity.target.table_id,
                                tablet.state_machine().tablet().storage().clone(),
                            )
                            .map_err(|e| e.to_string())?;
                    }
                    let classified_messages = classify_ready_messages(ready.messages);
                    send_messages(
                        &transport,
                        &snapshot_endpoint,
                        &latest_snapshot,
                        classified_messages.persistence_safe,
                    );
                    let mut frontier = AppliedRaftFrontier::new(
                        image.metadata.last_included_index,
                        image.metadata.last_included_term,
                    );
                    for entry in &ready.committed_entries {
                        frontier = AppliedRaftFrontier::new(entry.index, entry.term);
                        let EntryPayload::Normal(bytes) = &entry.payload else {
                            continue;
                        };
                        let dispositions = tablet
                            .apply_committed_entry(
                                ragnordb_multiraft::proposal::ProposalPosition {
                                    term: entry.term,
                                    index: entry.index,
                                },
                                bytes,
                            )
                            .map_err(|e| e.to_string())?;
                        snapshot_policy.note_applied(bytes.len());
                        publish_committed_entry(
                            bytes,
                            dispositions,
                            &mut registry,
                            &database,
                            catalog_cache.as_ref(),
                            &identity,
                        )
                        .map_err(|e| e.to_string())?;
                    }
                    ready_loop
                        .advance_applied_frontier(frontier)
                        .map_err(|e| e.to_string())?;
                    *latest_snapshot = Some(image.clone());
                    snapshot_policy.reset();
                    send_messages(
                        &transport,
                        &snapshot_endpoint,
                        &latest_snapshot,
                        classified_messages.apply_dependent,
                    );
                    release_replica_retention(&mut ready_loop).map_err(|e| e.to_string())?;
                    snapshot_store
                        .prune_older_snapshots(&installed.pointer)
                        .map_err(|e| e.to_string())?;
                    *expected_snapshot_install = None;
                    internal_barrier_allocator.clear();
                    *last_snapshot_index = latest_snapshot
                        .as_ref()
                        .map(|img| img.metadata.last_included_index)
                        .unwrap_or(*last_snapshot_index);
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
                            *pending_snapshot_install =
                                Some(PendingIncomingSnapshotInstall::ReadyPending {
                                    expected,
                                    prepared,
                                    image,
                                    raft_pointer,
                                });
                            return Ok(());
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
            return Ok(());
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
            &identity,
            &mut pending_read_states,
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
        let serving_leader = ready_loop.raft().leader_id() == Some(ready_loop.raft().id())
            && leader_activation.is_some_and(|activation| {
                activation.term == ready_loop.raft().hard_state().current_term
                    && ready_loop
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
            &identity,
            &mut pending_read_states,
        ) {
            Ok(Some(metadata)) => {
                *expected_snapshot_install = Some(metadata);
                *pending_snapshot_install = None;
                return Ok(());
            }
            Ok(None) => {}
            Err(HostedGroupError::Retryable(error)) | Err(HostedGroupError::Rejected(error)) => {
                tracing::debug!(
                    error = %error,
                    "pending Ready remains blocked before client admission"
                );
                return Ok(());
            }
            Err(error) => return Err(error.to_string()),
        }

        process_pending_read_states(
            &ready_loop,
            &tablet,
            serving_leader,
            &identity,
            &mut pending_read_states,
            &mut pending_read_barriers,
            Instant::now(),
        );
        fallback_pending_read_barriers(
            &mut ready_loop,
            &tablet,
            &mut registry,
            &mut clients,
            serving_leader,
            &mut internal_barrier_allocator,
            &identity,
            &mut pending_read_barriers,
            &mut pending_read_states,
        );

        let mut admitted_requests = 0;
        while admitted_requests < TABLET_REQUEST_BUDGET {
            let request = if let Some(request) = pending_requests.pop_front() {
                request
            } else {
                let Ok(request) = requests.try_recv() else {
                    break;
                };
                PendingTabletRequest::Raw(Box::new(request))
            };
            admitted_requests += 1;

            let Some(request) = prepare_pending_request(
                request,
                &tablet,
                serving_leader,
                ready_loop.raft().leader_id().map(|id| id.get()),
                ready_loop.raft().id().get(),
                &identity,
            ) else {
                continue;
            };

            match request {
                PendingTabletRequest::Prepared(first) => {
                    let mut batch = vec![*first];
                    let now = Instant::now();
                    let batch_started = now;
                    if batch[0].deadline > now + TABLET_BATCH_DEADLINE_GRACE {
                        while batch.len() < MAX_TABLET_COMMAND_BATCH_COMMANDS
                            && admitted_requests < TABLET_REQUEST_BUDGET
                            && batch_started.elapsed() < TABLET_BATCH_MAX_DELAY
                        {
                            let next = if let Some(request) = pending_requests.pop_front() {
                                Some(request)
                            } else {
                                requests
                                    .try_recv()
                                    .ok()
                                    .map(|request| PendingTabletRequest::Raw(Box::new(request)))
                            };
                            let Some(next) = next else {
                                break;
                            };
                            admitted_requests += 1;
                            let Some(next) = prepare_pending_request(
                                next,
                                &tablet,
                                serving_leader,
                                ready_loop.raft().leader_id().map(|id| id.get()),
                                ready_loop.raft().id().get(),
                                &identity,
                            ) else {
                                continue;
                            };

                            let PendingTabletRequest::Prepared(next) = next else {
                                pending_requests.push_front(next);
                                break;
                            };
                            if !next.compatible_with(&batch[0])
                                || next.deadline <= Instant::now() + TABLET_BATCH_DEADLINE_GRACE
                            {
                                pending_requests.push_front(PendingTabletRequest::Prepared(next));
                                break;
                            }

                            let mut candidate_envelopes = batch
                                .iter()
                                .map(|request| request.envelope.clone())
                                .collect::<Vec<_>>();
                            candidate_envelopes.push(next.envelope.clone());
                            let candidate_fits =
                                TabletCommandBatchEnvelope::new(candidate_envelopes)
                                    .and_then(|batch| batch.encode())
                                    .is_ok();
                            if !candidate_fits {
                                pending_requests.push_front(PendingTabletRequest::Prepared(next));
                                break;
                            }
                            batch.push(*next);
                        }
                    }

                    if batch.len() == 1 {
                        admit_prepared_command(
                            batch.pop().expect("single batch item exists"),
                            &mut ready_loop,
                            &mut registry,
                            &mut clients,
                            serving_leader,
                        );
                    } else {
                        admit_prepared_batch(batch, &mut ready_loop, &mut registry, &mut clients);
                    }
                }
                PendingTabletRequest::Raw(request) => match *request {
                    HostRequest::ReadPoint {
                        request,
                        reply,
                        deadline,
                    } => admit_read_request(
                        request,
                        &tablet,
                        serving_leader,
                        ready_loop.raft().leader_id().map(|id| id.get()),
                        &identity,
                        reply,
                        deadline,
                    ),
                    HostRequest::Scan {
                        request,
                        reply,
                        deadline,
                    } => admit_scan_request(
                        request,
                        &tablet,
                        serving_leader,
                        ready_loop.raft().leader_id().map(|id| id.get()),
                        &identity,
                        reply,
                        deadline,
                    ),
                    HostRequest::OutcomeQuery {
                        request,
                        reply,
                        deadline,
                    } => admit_outcome_query(
                        request,
                        &tablet,
                        serving_leader,
                        ready_loop.raft().leader_id().map(|id| id.get()),
                        reply,
                        deadline,
                    ),
                    HostRequest::RpcReadPoint {
                        request,
                        completion,
                        token,
                        deadline,
                    } => admit_read_barrier(
                        ClientReply::RpcReadPoint {
                            token,
                            completion,
                            request,
                            deadline,
                        },
                        deadline,
                        &mut ready_loop,
                        &tablet,
                        &mut registry,
                        &mut clients,
                        serving_leader,
                        &mut internal_barrier_allocator,
                        &identity,
                        &mut pending_read_barriers,
                        &mut next_read_index_context,
                    ),
                    HostRequest::RpcScan {
                        request,
                        completion,
                        token,
                        deadline,
                    } => admit_read_barrier(
                        ClientReply::RpcScan {
                            token,
                            completion,
                            request,
                            deadline,
                        },
                        deadline,
                        &mut ready_loop,
                        &tablet,
                        &mut registry,
                        &mut clients,
                        serving_leader,
                        &mut internal_barrier_allocator,
                        &identity,
                        &mut pending_read_barriers,
                        &mut next_read_index_context,
                    ),
                    HostRequest::RpcOutcomeQuery {
                        request,
                        completion,
                        token,
                        deadline,
                    } => admit_rpc_outcome_query(
                        request,
                        &tablet,
                        serving_leader,
                        ready_loop.raft().leader_id().map(|id| id.get()),
                        &identity,
                        completion,
                        token,
                        deadline,
                    ),
                    HostRequest::Barrier { reply, deadline } => admit_read_barrier(
                        reply,
                        deadline,
                        &mut ready_loop,
                        &tablet,
                        &mut registry,
                        &mut clients,
                        serving_leader,
                        &mut internal_barrier_allocator,
                        &identity,
                        &mut pending_read_barriers,
                        &mut next_read_index_context,
                    ),
                    request => admit_request(
                        request,
                        &mut ready_loop,
                        &tablet,
                        &mut registry,
                        &mut clients,
                        serving_leader,
                        &mut internal_barrier_allocator,
                        &identity,
                    ),
                },
            }
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
                &identity,
                &mut pending_read_states,
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

        process_pending_read_states(
            &ready_loop,
            &tablet,
            serving_leader,
            &identity,
            &mut pending_read_states,
            &mut pending_read_barriers,
            Instant::now(),
        );

        let now = Instant::now();
        registry.expire_deadlines(now);
        let is_leader = ready_loop.raft().leader_id() == Some(ready_loop.raft().id());
        if *was_leader && !is_leader {
            registry.mark_leadership_lost(ready_loop.raft().hard_state().current_term);
            *leader_activation = None;
            internal_barrier_allocator.clear();
            reject_pending_read_barriers(
                &mut pending_read_barriers,
                &mut pending_read_states,
                ready_loop.raft().leader_id().map(|id| id.get()),
            );
        }
        *was_leader = is_leader;
        let serving_leader = is_leader
            && leader_activation.is_some_and(|activation| {
                activation.term == ready_loop.raft().hard_state().current_term
                    && ready_loop
                        .applied_frontier()
                        .is_some_and(|frontier| frontier.index >= activation.index)
            });
        forward_completions(
            &mut clients,
            &tablet,
            &database,
            serving_leader,
            ready_loop.raft().leader_id().map(|id| id.get()),
            &identity,
        );
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
                &identity,
                &mut pending_read_states,
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
                .map(|image| {
                    (
                        image.metadata.last_included_index,
                        image.metadata.last_included_term,
                    )
                })
                .unwrap_or((0, 0)),
            pending_snapshot_install.is_some() || pending_local_snapshot.is_some(),
            *ownership,
            &status,
        );
        Ok(())
    }
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
        raft_group_id: RaftGroupId,
    ) -> std::result::Result<RequestId, TabletCommandApplyError> {
        if self.term != Some(term) {
            let client_id = internal_barrier_client_id(term);
            self.term = Some(term);
            self.next_sequence = Some(tablet.state_machine().next_sequence_for_client(client_id)?);
        }

        self.candidate_for_group(raft_group_id)
    }

    #[cfg(test)]
    fn candidate_for_active_term(&self) -> std::result::Result<RequestId, TabletCommandApplyError> {
        self.candidate_for_group(TABLET_RAFT_GROUP_ID)
    }

    fn candidate_for_group(
        &self,
        raft_group_id: RaftGroupId,
    ) -> std::result::Result<RequestId, TabletCommandApplyError> {
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
            raft_group_id,
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

fn admit_outcome_query(
    request: TabletOutcomeQueryRequest,
    tablet: &TabletCommandApplier,
    serving_leader: bool,
    leader_id: Option<u64>,
    reply: mpsc::SyncSender<Result<Option<CachedTabletCommandOutcome>>>,
    _deadline: Instant,
) {
    if !serving_leader {
        let _ = reply.send(Err(Error::NotLeader { leader_id }));
        return;
    }

    if request.tablet_id != tablet.state_machine().tablet().id()
        || request.tablet_epoch != tablet.state_machine().epoch()
    {
        let _ = reply.send(Err(Error::StaleTabletEpoch {
            current_epoch: tablet.state_machine().epoch(),
            expected_epoch: request.tablet_epoch,
        }));
        return;
    }

    let _ = reply.send(Ok(tablet
        .state_machine()
        .logical_command_outcome(&request.logical_command_id)
        .cloned()));
}

fn evaluate_rpc_outcome_query(
    request: TabletOutcomeQueryRequest,
    tablet: &TabletCommandApplier,
    serving_leader: bool,
    leader_id: Option<u64>,
    identity: &TabletRuntimeIdentity,
    deadline: Instant,
) -> Result<Option<CachedTabletCommandOutcome>> {
    if deadline <= Instant::now() {
        return Err(Error::ProposalUnavailable {
            reason: "tablet outcome query deadline elapsed before admission".to_string(),
        });
    }
    if !serving_leader {
        return Err(Error::NotLeader { leader_id });
    }
    if request.request_id.raft_group_id != identity.target.raft_group_id {
        return Err(Error::InvalidArgument(
            "tablet outcome query targets a different Raft group".to_string(),
        ));
    }
    if request.tablet_id != identity.target.tablet_id {
        return Err(Error::InvalidArgument(
            "tablet outcome query targets a different tablet".to_string(),
        ));
    }
    if request.tablet_epoch != tablet.state_machine().epoch() {
        return Err(Error::StaleTabletEpoch {
            current_epoch: tablet.state_machine().epoch(),
            expected_epoch: request.tablet_epoch,
        });
    }

    Ok(tablet
        .state_machine()
        .logical_command_outcome(&request.logical_command_id)
        .cloned())
}

#[allow(clippy::too_many_arguments)]
fn admit_rpc_outcome_query(
    request: TabletOutcomeQueryRequest,
    tablet: &TabletCommandApplier,
    serving_leader: bool,
    leader_id: Option<u64>,
    identity: &TabletRuntimeIdentity,
    completion: Arc<dyn TabletRpcCompletionSink>,
    token: u64,
    deadline: Instant,
) {
    completion.publish(
        token,
        TabletRpcCompletion::Outcome(evaluate_rpc_outcome_query(
            request,
            tablet,
            serving_leader,
            leader_id,
            identity,
            deadline,
        )),
    );
}

/// Convert only mutation requests into the prepared form used by the batch
/// collector. Ordering boundaries such as catalog updates, no-op barriers,
/// reads, and scans remain raw requests and therefore keep their existing
/// admission paths.
fn prepare_pending_request(
    request: PendingTabletRequest,
    tablet: &TabletCommandApplier,
    serving_leader: bool,
    leader_id: Option<u64>,
    local_id: u64,
    identity: &TabletRuntimeIdentity,
) -> Option<PendingTabletRequest> {
    let PendingTabletRequest::Raw(request) = request else {
        return Some(request);
    };

    match *request {
        HostRequest::Commit {
            commit,
            reply,
            deadline,
        } => {
            let reply = ClientReply::Commit(reply);
            let envelope = envelope_from_commit_for_identity(local_id, commit, identity);
            prepare_mutation_envelope(envelope, reply, deadline, tablet, serving_leader, leader_id)
                .map(|prepared| PendingTabletRequest::Prepared(Box::new(prepared)))
        }
        HostRequest::Command {
            request,
            reply,
            deadline,
        } => {
            if !request.command.is_batchable() {
                return Some(PendingTabletRequest::Raw(Box::new(HostRequest::Command {
                    request,
                    reply,
                    deadline,
                })));
            }
            let reply = ClientReply::Command(reply);
            prepare_mutation_request(request, reply, deadline, tablet, serving_leader, leader_id)
                .map(|prepared| PendingTabletRequest::Prepared(Box::new(prepared)))
        }
        HostRequest::RpcCommand {
            request,
            completion,
            token,
            deadline,
        } => {
            if !request.command.is_batchable() {
                return Some(PendingTabletRequest::Raw(Box::new(
                    HostRequest::RpcCommand {
                        request,
                        completion,
                        token,
                        deadline,
                    },
                )));
            }
            let remote_commit = match &request.command {
                TabletCommand::SingleShardCommit(command) => Some(command.clone()),
                _ => None,
            };
            let reply = ClientReply::RpcCommand {
                token,
                completion,
                remote_commit,
            };
            prepare_mutation_request(request, reply, deadline, tablet, serving_leader, leader_id)
                .map(|prepared| PendingTabletRequest::Prepared(Box::new(prepared)))
        }
        request => Some(PendingTabletRequest::Raw(Box::new(request))),
    }
}

fn prepare_mutation_request(
    request: TabletCommandRequest,
    reply: ClientReply,
    deadline: Instant,
    tablet: &TabletCommandApplier,
    serving_leader: bool,
    leader_id: Option<u64>,
) -> Option<PreparedCommandRequest> {
    let envelope = envelope_from_tablet_command_request(request)
        .map_err(|source| Error::InvalidArgument(source.to_string()));
    prepare_mutation_envelope(envelope, reply, deadline, tablet, serving_leader, leader_id)
}

fn prepare_mutation_envelope(
    envelope: Result<TabletCommandEnvelope>,
    reply: ClientReply,
    deadline: Instant,
    tablet: &TabletCommandApplier,
    serving_leader: bool,
    leader_id: Option<u64>,
) -> Option<PreparedCommandRequest> {
    if !serving_leader {
        send_client_error(reply, Error::NotLeader { leader_id });
        return None;
    }
    if deadline <= Instant::now() {
        send_client_error(
            reply,
            Error::ProposalUnavailable {
                reason: "tablet command deadline elapsed before admission".to_string(),
            },
        );
        return None;
    }

    let envelope = match envelope {
        Ok(envelope) => envelope,
        Err(source) => {
            send_client_error(reply, Error::InvalidArgument(source.to_string()));
            return None;
        }
    };
    if let Err(source) = tablet.state_machine().validate_proposal(&envelope) {
        send_client_error(reply, map_tablet_rejection(source));
        return None;
    }

    Some(PreparedCommandRequest {
        envelope,
        deadline,
        reply,
    })
}

fn envelope_from_tablet_command_request(
    request: TabletCommandRequest,
) -> std::result::Result<
    TabletCommandEnvelope,
    ragnordb_common::command_codec::TabletCommandEnvelopeError,
> {
    let TabletCommandRequest {
        request_id,
        logical_command_id,
        acknowledged_through,
        tablet_id,
        tablet_epoch,
        command,
    } = request;

    match logical_command_id {
        Some(logical_command_id) => TabletCommandEnvelope::new_with_logical_command_id_and_ack(
            request_id,
            logical_command_id,
            tablet_id,
            tablet_epoch,
            acknowledged_through,
            command,
        ),
        None => TabletCommandEnvelope::new(request_id, tablet_id, tablet_epoch, command),
    }
}

#[allow(clippy::too_many_arguments)]
fn admit_prepared_command<W, LS, SS>(
    prepared: PreparedCommandRequest,
    ready_loop: &mut RaftReadyLoop<W, LS, SS>,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    clients: &mut Vec<PendingClient>,
    serving_leader: bool,
) where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    let PreparedCommandRequest {
        envelope,
        deadline,
        reply,
    } = prepared;
    let leader_id = ready_loop.raft().leader_id().map(|id| id.get());
    if !serving_leader || ready_loop.raft().leader_id() != Some(ready_loop.raft().id()) {
        send_client_error(reply, Error::NotLeader { leader_id });
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

#[allow(clippy::too_many_arguments)]
fn admit_prepared_batch<W, LS, SS>(
    prepared: Vec<PreparedCommandRequest>,
    ready_loop: &mut RaftReadyLoop<W, LS, SS>,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    clients: &mut Vec<PendingClient>,
) where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    let mut prepared = prepared
        .into_iter()
        .filter_map(|request| {
            if request.deadline <= Instant::now() {
                send_client_error(
                    request.reply,
                    Error::ProposalUnavailable {
                        reason: "tablet command deadline elapsed before admission".to_string(),
                    },
                );
                return None;
            }
            if registry.is_pending(&request.envelope.request_id) {
                send_client_error(
                    request.reply,
                    Error::ProposalUnavailable {
                        reason: "an identical request is already awaiting tablet apply".to_string(),
                    },
                );
                None
            } else {
                Some(request)
            }
        })
        .collect::<Vec<_>>();

    if prepared.is_empty() {
        return;
    }
    if prepared.len() == 1 {
        admit_prepared_command(
            prepared.pop().expect("single prepared request exists"),
            ready_loop,
            registry,
            clients,
            true,
        );
        return;
    }

    let envelopes = prepared
        .iter()
        .map(|request| request.envelope.clone())
        .collect::<Vec<_>>();
    let batch = match TabletCommandBatchEnvelope::new(envelopes) {
        Ok(batch) => batch,
        Err(source) => {
            for request in prepared {
                send_client_error(request.reply, Error::InvalidArgument(source.to_string()));
            }
            return;
        }
    };
    let bytes = match batch.encode() {
        Ok(bytes) => bytes,
        Err(source) => {
            for request in prepared {
                send_client_error(request.reply, Error::InvalidArgument(source.to_string()));
            }
            return;
        }
    };
    if bytes.len() > MAX_TABLET_COMMAND_BATCH_BYTES {
        for request in prepared {
            send_client_error(
                request.reply,
                Error::InvalidArgument(
                    "tablet command batch exceeds the encoded byte limit".to_string(),
                ),
            );
        }
        return;
    }

    let index = match ready_loop.propose(bytes.clone(), bytes.len()) {
        Ok(index) => index,
        Err(source) => {
            for request in prepared {
                send_client_error(
                    request.reply,
                    Error::ProposalUnavailable {
                        reason: source.to_string(),
                    },
                );
            }
            return;
        }
    };
    let position = ragnordb_multiraft::proposal::ProposalPosition {
        term: ready_loop.raft().hard_state().current_term,
        index,
    };

    for request in prepared {
        let request_id = request.envelope.request_id.clone();
        match registry.register(request_id, position, request.deadline) {
            Ok(ticket) => clients.push(PendingClient {
                ticket,
                reply: request.reply,
            }),
            Err(source) => send_client_error(
                request.reply,
                Error::ProposalUnavailable {
                    reason: source.to_string(),
                },
            ),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn admit_request<W, LS, SS>(
    request: HostRequest,
    ready_loop: &mut RaftReadyLoop<W, LS, SS>,
    tablet: &TabletCommandApplier,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    clients: &mut Vec<PendingClient>,
    serving_leader: bool,
    internal_barrier_allocator: &mut InternalBarrierAllocator,
    identity: &TabletRuntimeIdentity,
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
        } => match envelope_from_commit_for_identity(local_id.get(), commit, identity) {
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
        } => match envelope_from_catalog_for_identity(local_id.get(), &update, identity) {
            Ok(envelope) => (envelope, deadline, ClientReply::Catalog(reply), None),
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        },
        HostRequest::Barrier { reply, deadline } => {
            let request_id = match internal_barrier_allocator.candidate(
                ready_loop.raft().hard_state().current_term,
                tablet,
                identity.target.raft_group_id,
            ) {
                Ok(request_id) => request_id,
                Err(source) => {
                    send_client_error(
                        reply,
                        Error::RecoveryRequired {
                            reason: source.to_string(),
                        },
                    );
                    return;
                }
            };
            let envelope = TabletCommandEnvelope::new(
                request_id.clone(),
                identity.target.tablet_id,
                identity.target.tablet_epoch,
                TabletCommand::Noop(NoopCommand),
            )
            .map_err(|source| Error::InvalidArgument(source.to_string()));
            match envelope {
                Ok(envelope) => (envelope, deadline, reply, Some(request_id.sequence)),
                Err(error) => {
                    send_client_error(reply, error);
                    return;
                }
            }
        }
        HostRequest::Command {
            request,
            reply,
            deadline,
        } => {
            let envelope = match request.logical_command_id {
                Some(logical_command_id) => {
                    TabletCommandEnvelope::new_with_logical_command_id_and_ack(
                        request.request_id,
                        logical_command_id,
                        request.tablet_id,
                        request.tablet_epoch,
                        request.acknowledged_through,
                        request.command,
                    )
                }
                None => TabletCommandEnvelope::new(
                    request.request_id,
                    request.tablet_id,
                    request.tablet_epoch,
                    request.command,
                ),
            };
            match envelope {
                Ok(envelope) => (envelope, deadline, ClientReply::Command(reply), None),
                Err(source) => {
                    let _ = reply.send(Err(Error::InvalidArgument(source.to_string())));
                    return;
                }
            }
        }
        HostRequest::RpcCommand {
            request,
            completion,
            token,
            deadline,
        } => {
            let remote_commit = match &request.command {
                TabletCommand::SingleShardCommit(command) => Some(command.clone()),
                _ => None,
            };
            let envelope = match request.logical_command_id {
                Some(logical_command_id) => {
                    TabletCommandEnvelope::new_with_logical_command_id_and_ack(
                        request.request_id,
                        logical_command_id,
                        request.tablet_id,
                        request.tablet_epoch,
                        request.acknowledged_through,
                        request.command,
                    )
                }
                None => TabletCommandEnvelope::new(
                    request.request_id,
                    request.tablet_id,
                    request.tablet_epoch,
                    request.command,
                ),
            };
            match envelope {
                Ok(envelope) => (
                    envelope,
                    deadline,
                    ClientReply::RpcCommand {
                        token,
                        completion,
                        remote_commit,
                    },
                    None,
                ),
                Err(source) => {
                    completion.publish(
                        token,
                        TabletRpcCompletion::Command(Err(Error::InvalidArgument(
                            source.to_string(),
                        ))),
                    );
                    return;
                }
            }
        }
        HostRequest::ReadPoint { reply, .. } => {
            let _ = reply.send(Err(Error::InvalidArgument(
                "tablet reads must use the read admission path".to_string(),
            )));
            return;
        }
        HostRequest::Scan { reply, .. } => {
            let _ = reply.send(Err(Error::InvalidArgument(
                "tablet scans must use the scan admission path".to_string(),
            )));
            return;
        }
        HostRequest::OutcomeQuery { reply, .. } => {
            let _ = reply.send(Err(Error::InvalidArgument(
                "tablet outcome queries must use the outcome admission path".to_string(),
            )));
            return;
        }
        HostRequest::RpcReadPoint { .. }
        | HostRequest::RpcScan { .. }
        | HostRequest::RpcOutcomeQuery { .. } => {
            unreachable!("RPC read requests use their dedicated admission paths")
        }
    };

    if let Err(source) = tablet.state_machine().validate_proposal(&envelope) {
        // Routing/generation failures are checked before proposal admission
        // and before deduplication. Preserve their typed meaning so a gateway
        // can refresh metadata instead of treating a stale route as malformed
        // SQL or replaying a mutation against the wrong tablet.
        send_client_error(reply, map_tablet_rejection(source));
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

fn read_index_context(group_id: RaftGroupId, term: u64, sequence: u64) -> Vec<u8> {
    let mut context = Vec::with_capacity(32);
    context.extend_from_slice(&READ_INDEX_CONTEXT_NAMESPACE.to_le_bytes());
    context.extend_from_slice(&group_id.0.to_le_bytes());
    context.extend_from_slice(&term.to_le_bytes());
    context.extend_from_slice(&sequence.to_le_bytes());
    context
}

#[allow(clippy::too_many_arguments)]
fn admit_read_barrier<W, LS, SS>(
    reply: ClientReply,
    deadline: Instant,
    ready_loop: &mut RaftReadyLoop<W, LS, SS>,
    tablet: &TabletCommandApplier,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    clients: &mut Vec<PendingClient>,
    serving_leader: bool,
    internal_barrier_allocator: &mut InternalBarrierAllocator,
    identity: &TabletRuntimeIdentity,
    pending_read_barriers: &mut Vec<PendingReadBarrier>,
    next_read_index_context: &mut u64,
) where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    if deadline <= Instant::now() {
        send_client_error(
            reply,
            Error::ProposalUnavailable {
                reason: "read barrier deadline elapsed before admission".to_string(),
            },
        );
        return;
    }

    let leader_id = ready_loop.raft().leader_id().map(|id| id.get());
    if !serving_leader {
        send_client_error(reply, Error::NotLeader { leader_id });
        return;
    }

    let term = ready_loop.raft().hard_state().current_term;
    let fallback_at = deadline
        .checked_sub(READ_INDEX_FALLBACK_GRACE)
        .unwrap_or(deadline);

    if pending_read_barrier_waiter_count(pending_read_barriers) >= MAX_PENDING_READ_BARRIER_WAITERS
    {
        send_client_error(
            reply,
            Error::ProposalUnavailable {
                reason: "latest-read admission limit reached while awaiting quorum".to_string(),
            },
        );
        return;
    }

    if let Some(pending) = pending_read_barriers
        .iter_mut()
        .find(|pending| pending.term == term)
    {
        pending.fallback_at = pending.fallback_at.min(fallback_at);
        pending
            .waiters
            .push(PendingReadBarrierWaiter { reply, deadline });
        return;
    }

    let sequence = *next_read_index_context;
    *next_read_index_context = match sequence.checked_add(1) {
        Some(next) => next,
        None => {
            send_client_error(
                reply,
                Error::RecoveryRequired {
                    reason: "ReadIndex context sequence exhausted".to_string(),
                },
            );
            return;
        }
    };
    let context = read_index_context(identity.target.raft_group_id, term, sequence);

    match ready_loop.read_index(context.clone()) {
        Ok(()) => pending_read_barriers.push(PendingReadBarrier {
            context,
            term,
            fallback_at,
            waiters: vec![PendingReadBarrierWaiter { reply, deadline }],
        }),
        Err(ReadyLoopError::ReadIndex(ReadIndexError::NotLeader)) => {
            send_client_error(reply, Error::NotLeader { leader_id });
        }
        Err(ReadyLoopError::ReadIndex(ReadIndexError::RecoveryRequired)) => {
            send_client_error(
                reply,
                Error::RecoveryRequired {
                    reason: "Raft ReadIndex requires recovery".to_string(),
                },
            );
        }
        Err(ReadyLoopError::ReadIndex(
            ReadIndexError::NotActivated { .. }
            | ReadIndexError::CurrentTermNotCommitted { .. }
            | ReadIndexError::CurrentTermNotApplied { .. }
            | ReadIndexError::ActivationTermMismatch { .. },
        ))
        | Err(ReadyLoopError::ReadIndex(ReadIndexError::RequestIdExhausted))
        | Err(ReadyLoopError::ReadIndex(
            ReadIndexError::PendingReadIndexLimitReached { .. }
            | ReadIndexError::PendingReadIndexBytesLimitReached { .. },
        ))
        | Err(ReadyLoopError::PendingReady) => {
            // ReadIndex is an optimization. If its activation boundary is not
            // available, preserve the proven Milestone-4 log-barrier path.
            admit_request(
                HostRequest::Barrier { reply, deadline },
                ready_loop,
                tablet,
                registry,
                clients,
                serving_leader,
                internal_barrier_allocator,
                identity,
            );
        }
        Err(error) => {
            send_client_error(
                reply,
                Error::ProposalUnavailable {
                    reason: error.to_string(),
                },
            );
        }
    }
}

fn pending_read_barrier_waiter_count(pending_read_barriers: &[PendingReadBarrier]) -> usize {
    pending_read_barriers
        .iter()
        .map(|pending| pending.waiters.len())
        .fold(0, usize::saturating_add)
}

fn complete_read_barrier_reply(
    reply: ClientReply,
    barrier_result: Result<()>,
    tablet: &TabletCommandApplier,
    serving_leader: bool,
    leader_replica_id: Option<u64>,
    identity: &TabletRuntimeIdentity,
    deadline: Instant,
) {
    match reply {
        ClientReply::Barrier(sender) => {
            let _ = sender.send(barrier_result);
        }
        ClientReply::RpcReadPoint {
            token,
            completion,
            request,
            ..
        } => {
            let result = barrier_result.and_then(|()| {
                evaluate_point_read(
                    request,
                    tablet,
                    serving_leader,
                    leader_replica_id,
                    identity,
                    deadline,
                )
            });
            completion.publish(token, TabletRpcCompletion::Read(result));
        }
        ClientReply::RpcScan {
            token,
            completion,
            request,
            ..
        } => {
            let result = barrier_result.and_then(|()| {
                evaluate_scan_request(
                    request,
                    tablet,
                    serving_leader,
                    leader_replica_id,
                    identity,
                    deadline,
                )
            });
            completion.publish(token, TabletRpcCompletion::Scan(result));
        }
        other => {
            if let Err(error) = barrier_result {
                send_client_error(other, error);
            } else {
                unreachable!("only read replies may wait on a read barrier")
            }
        }
    }
}

fn process_pending_read_states<W, LS, SS>(
    ready_loop: &RaftReadyLoop<W, LS, SS>,
    tablet: &TabletCommandApplier,
    serving_leader: bool,
    identity: &TabletRuntimeIdentity,
    read_states: &mut Vec<ReadState>,
    pending_read_barriers: &mut Vec<PendingReadBarrier>,
    now: Instant,
) where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    let local_id = ready_loop.raft().id();
    let leader_id = ready_loop.raft().leader_id();
    let term = ready_loop.raft().hard_state().current_term;
    let applied_index = ready_loop
        .applied_frontier()
        .map(|frontier| frontier.index)
        .unwrap_or(0);
    let states = std::mem::take(read_states);

    for state in states {
        let Some(position) = pending_read_barriers
            .iter()
            .position(|pending| pending.term == state.term && pending.context == state.request_ctx)
        else {
            // A late or duplicate ReadState is harmless once its waiter has
            // been completed, timed out, or invalidated by a term change.
            continue;
        };

        if leader_id != Some(local_id) || state.term != term || state.index == 0 {
            let pending = pending_read_barriers.remove(position);
            for waiter in pending.waiters {
                let PendingReadBarrierWaiter { reply, deadline } = waiter;
                complete_read_barrier_reply(
                    reply,
                    Err(Error::NotLeader {
                        leader_id: leader_id.map(|id| id.get()),
                    }),
                    tablet,
                    serving_leader,
                    leader_id.map(|id| id.get()),
                    identity,
                    deadline,
                );
            }
            continue;
        }

        if applied_index < state.index {
            read_states.push(state);
            continue;
        }

        let pending = pending_read_barriers.remove(position);
        for waiter in pending.waiters {
            let PendingReadBarrierWaiter { reply, deadline } = waiter;
            let result = if deadline <= now {
                Err(Error::ProposalUnavailable {
                    reason: "read barrier deadline elapsed before ReadIndex confirmation"
                        .to_string(),
                })
            } else {
                Ok(())
            };
            complete_read_barrier_reply(
                reply,
                result,
                tablet,
                serving_leader,
                leader_id.map(|id| id.get()),
                identity,
                deadline,
            );
        }
    }
}

fn reject_pending_read_barriers(
    pending_read_barriers: &mut Vec<PendingReadBarrier>,
    pending_read_states: &mut Vec<ReadState>,
    leader_id: Option<u64>,
) {
    pending_read_states.clear();
    for pending in pending_read_barriers.drain(..) {
        for waiter in pending.waiters {
            send_client_error(waiter.reply, Error::NotLeader { leader_id });
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn fallback_pending_read_barriers<W, LS, SS>(
    ready_loop: &mut RaftReadyLoop<W, LS, SS>,
    tablet: &TabletCommandApplier,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    clients: &mut Vec<PendingClient>,
    serving_leader: bool,
    internal_barrier_allocator: &mut InternalBarrierAllocator,
    identity: &TabletRuntimeIdentity,
    pending_read_barriers: &mut Vec<PendingReadBarrier>,
    pending_read_states: &mut Vec<ReadState>,
) where
    W: RaftWal,
    LS: LogStore<Vec<u8>>,
    SS: StableStore,
{
    let local_id = ready_loop.raft().id();
    let leader_id = ready_loop.raft().leader_id();
    let term = ready_loop.raft().hard_state().current_term;
    if leader_id != Some(local_id)
        || pending_read_barriers
            .iter()
            .any(|pending| pending.term != term)
    {
        reject_pending_read_barriers(
            pending_read_barriers,
            pending_read_states,
            leader_id.map(|id| id.get()),
        );
        return;
    }

    let now = Instant::now();
    let mut position = 0;
    while position < pending_read_barriers.len() {
        let pending = &mut pending_read_barriers[position];
        let mut active_waiters = Vec::with_capacity(pending.waiters.len());
        for waiter in pending.waiters.drain(..) {
            if waiter.deadline <= now {
                send_client_error(
                    waiter.reply,
                    Error::ProposalUnavailable {
                        reason: "read barrier deadline elapsed before ReadIndex confirmation"
                            .to_string(),
                    },
                );
            } else {
                active_waiters.push(waiter);
            }
        }
        pending.waiters = active_waiters;

        if pending.waiters.is_empty() {
            pending_read_barriers.remove(position);
            continue;
        }
        if pending.fallback_at > now {
            position += 1;
            continue;
        }

        let pending = pending_read_barriers.remove(position);
        for waiter in pending.waiters {
            admit_request(
                HostRequest::Barrier {
                    reply: waiter.reply,
                    deadline: waiter.deadline,
                },
                ready_loop,
                tablet,
                registry,
                clients,
                serving_leader,
                internal_barrier_allocator,
                identity,
            );
        }
    }
}

fn evaluate_point_read(
    request: TabletReadRequest,
    tablet: &TabletCommandApplier,
    serving_leader: bool,
    leader_replica_id: Option<u64>,
    identity: &TabletRuntimeIdentity,
    deadline: Instant,
) -> Result<Option<Vec<u8>>> {
    if deadline <= Instant::now() {
        return Err(Error::ProposalUnavailable {
            reason: "tablet read deadline elapsed before admission".to_string(),
        });
    }
    if !serving_leader {
        return Err(Error::NotLeader {
            leader_id: leader_replica_id,
        });
    }
    if request.request_id.raft_group_id != identity.target.raft_group_id {
        return Err(Error::InvalidArgument(
            "tablet read request targets a different Raft group".to_string(),
        ));
    }
    if request.tablet_id != identity.target.tablet_id {
        return Err(Error::InvalidArgument(
            "tablet read request targets a different tablet".to_string(),
        ));
    }
    if request.tablet_epoch != identity.target.tablet_epoch {
        return Err(Error::StaleTabletEpoch {
            current_epoch: identity.target.tablet_epoch,
            expected_epoch: request.tablet_epoch,
        });
    }
    if request.read_timestamp.0 == 0
        || request.request_id.client_id == 0
        || request.request_id.sequence == 0
    {
        return Err(Error::InvalidArgument(
            "tablet read request contains a reserved zero value".to_string(),
        ));
    }

    let row_key = request.row_key;
    let transaction_id = TxnId((request.request_id.client_id as u64).max(1));
    let transaction = ragnordb_txn::Transaction::new(transaction_id, request.read_timestamp)?;
    if row_key.table_id != identity.target.table_id {
        return Err(Error::InvalidArgument(
            "tablet read row key targets a different table".to_string(),
        ));
    }

    tablet
        .state_machine()
        .tablet()
        .get(&transaction, &row_key)
        .and_then(|row| row.map(|row| encode_row(&row)).transpose())
}

fn evaluate_scan_request(
    request: TabletScanRequest,
    tablet: &TabletCommandApplier,
    serving_leader: bool,
    leader_replica_id: Option<u64>,
    identity: &TabletRuntimeIdentity,
    deadline: Instant,
) -> Result<TabletScanBatch> {
    if deadline <= Instant::now() {
        return Err(Error::ProposalUnavailable {
            reason: "tablet scan deadline elapsed before admission".to_string(),
        });
    }
    if !serving_leader {
        return Err(Error::NotLeader {
            leader_id: leader_replica_id,
        });
    }
    request
        .validate()
        .map_err(|error| Error::InvalidArgument(error.to_string()))?;
    if request.request_id.raft_group_id != identity.target.raft_group_id {
        return Err(Error::InvalidArgument(
            "tablet scan request targets a different Raft group".to_string(),
        ));
    }
    if request.tablet_id != identity.target.tablet_id {
        return Err(Error::InvalidArgument(
            "tablet scan request targets a different tablet".to_string(),
        ));
    }
    if request.tablet_epoch != identity.target.tablet_epoch {
        return Err(Error::StaleTabletEpoch {
            current_epoch: identity.target.tablet_epoch,
            expected_epoch: request.tablet_epoch,
        });
    }

    let transaction_id = TxnId((request.request_id.client_id as u64).max(1));
    let transaction = ragnordb_txn::Transaction::new(transaction_id, request.read_timestamp)?;
    let table_id = identity.target.table_id;
    let start = request
        .start_key
        .as_ref()
        .map(|primary_key_bytes| ragnordb_common::ids::RowKey {
            table_id,
            primary_key_bytes: primary_key_bytes.clone(),
        });
    let end = request
        .end_key
        .as_ref()
        .map(|primary_key_bytes| ragnordb_common::ids::RowKey {
            table_id,
            primary_key_bytes: primary_key_bytes.clone(),
        });
    let resume_after =
        request
            .resume_after
            .as_ref()
            .map(|primary_key_bytes| ragnordb_common::ids::RowKey {
                table_id,
                primary_key_bytes: primary_key_bytes.clone(),
            });

    tablet
        .state_machine()
        .tablet()
        .scan_page(
            &transaction,
            start.as_ref(),
            end.as_ref(),
            resume_after.as_ref(),
            request.max_rows as usize,
            request.max_bytes as usize,
        )
        .and_then(|page| {
            let rows = page
                .rows
                .into_iter()
                .map(|(key, row)| {
                    Ok(TabletScanRow {
                        key: key.primary_key_bytes,
                        row: encode_row(&row)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let next_resume_after = page
                .has_more
                .then(|| rows.last().map(|row| row.key.clone()))
                .flatten();
            let batch = TabletScanBatch {
                rows,
                next_resume_after,
                exhausted: !page.has_more,
            };
            batch
                .validate_for(&request)
                .map_err(|error| Error::CorruptData(error.to_string()))?;
            Ok(batch)
        })
}

/// Execute a point read after the Ready owner has drained all committed work
/// that was already pending at the start of the host turn.
fn admit_read_request(
    request: TabletReadRequest,
    tablet: &TabletCommandApplier,
    serving_leader: bool,
    leader_replica_id: Option<u64>,
    identity: &TabletRuntimeIdentity,
    reply: mpsc::SyncSender<Result<Option<Vec<u8>>>>,
    deadline: Instant,
) {
    if deadline <= Instant::now() {
        let _ = reply.send(Err(Error::ProposalUnavailable {
            reason: "tablet read deadline elapsed before admission".to_string(),
        }));
        return;
    }
    if !serving_leader {
        let _ = reply.send(Err(Error::NotLeader {
            leader_id: leader_replica_id,
        }));
        return;
    }
    if request.request_id.raft_group_id != identity.target.raft_group_id {
        let _ = reply.send(Err(Error::InvalidArgument(
            "tablet read request targets a different Raft group".to_string(),
        )));
        return;
    }
    if request.tablet_id != identity.target.tablet_id {
        let _ = reply.send(Err(Error::InvalidArgument(
            "tablet read request targets a different tablet".to_string(),
        )));
        return;
    }
    if request.tablet_epoch != identity.target.tablet_epoch {
        let _ = reply.send(Err(Error::StaleTabletEpoch {
            current_epoch: identity.target.tablet_epoch,
            expected_epoch: request.tablet_epoch,
        }));
        return;
    }
    if request.read_timestamp.0 == 0
        || request.request_id.client_id == 0
        || request.request_id.sequence == 0
    {
        let _ = reply.send(Err(Error::InvalidArgument(
            "tablet read request contains a reserved zero value".to_string(),
        )));
        return;
    }

    let row_key = request.row_key;
    let transaction_id = TxnId((request.request_id.client_id as u64).max(1));
    let transaction = match ragnordb_txn::Transaction::new(transaction_id, request.read_timestamp) {
        Ok(transaction) => transaction,
        Err(error) => {
            let _ = reply.send(Err(error));
            return;
        }
    };
    if row_key.table_id != identity.target.table_id {
        let _ = reply.send(Err(Error::InvalidArgument(
            "tablet read row key targets a different table".to_string(),
        )));
        return;
    }

    let result = tablet
        .state_machine()
        .tablet()
        .get(&transaction, &row_key)
        .and_then(|row| row.map(|row| encode_row(&row)).transpose());
    let _ = reply.send(result);
}

/// Execute a bounded range page after the same Ready-owner admission checks
/// used by point reads. No scan request can bypass leader activation, target
/// identity, tablet epoch, or the caller's fixed MVCC timestamp.
fn admit_scan_request(
    request: TabletScanRequest,
    tablet: &TabletCommandApplier,
    serving_leader: bool,
    leader_replica_id: Option<u64>,
    identity: &TabletRuntimeIdentity,
    reply: mpsc::SyncSender<Result<TabletScanBatch>>,
    deadline: Instant,
) {
    if deadline <= Instant::now() {
        let _ = reply.send(Err(Error::ProposalUnavailable {
            reason: "tablet scan deadline elapsed before admission".to_string(),
        }));
        return;
    }
    if !serving_leader {
        let _ = reply.send(Err(Error::NotLeader {
            leader_id: leader_replica_id,
        }));
        return;
    }
    if let Err(error) = request.validate() {
        let _ = reply.send(Err(Error::InvalidArgument(error.to_string())));
        return;
    }
    if request.request_id.raft_group_id != identity.target.raft_group_id {
        let _ = reply.send(Err(Error::InvalidArgument(
            "tablet scan request targets a different Raft group".to_string(),
        )));
        return;
    }
    if request.tablet_id != identity.target.tablet_id {
        let _ = reply.send(Err(Error::InvalidArgument(
            "tablet scan request targets a different tablet".to_string(),
        )));
        return;
    }
    if request.tablet_epoch != identity.target.tablet_epoch {
        let _ = reply.send(Err(Error::StaleTabletEpoch {
            current_epoch: identity.target.tablet_epoch,
            expected_epoch: request.tablet_epoch,
        }));
        return;
    }

    let transaction_id = TxnId((request.request_id.client_id as u64).max(1));
    let transaction = match ragnordb_txn::Transaction::new(transaction_id, request.read_timestamp) {
        Ok(transaction) => transaction,
        Err(error) => {
            let _ = reply.send(Err(error));
            return;
        }
    };
    let table_id = identity.target.table_id;
    let start = request
        .start_key
        .as_ref()
        .map(|primary_key_bytes| ragnordb_common::ids::RowKey {
            table_id,
            primary_key_bytes: primary_key_bytes.clone(),
        });
    let end = request
        .end_key
        .as_ref()
        .map(|primary_key_bytes| ragnordb_common::ids::RowKey {
            table_id,
            primary_key_bytes: primary_key_bytes.clone(),
        });
    let resume_after =
        request
            .resume_after
            .as_ref()
            .map(|primary_key_bytes| ragnordb_common::ids::RowKey {
                table_id,
                primary_key_bytes: primary_key_bytes.clone(),
            });

    let result = tablet
        .state_machine()
        .tablet()
        .scan_page(
            &transaction,
            start.as_ref(),
            end.as_ref(),
            resume_after.as_ref(),
            request.max_rows as usize,
            request.max_bytes as usize,
        )
        .and_then(|page| {
            let rows = page
                .rows
                .into_iter()
                .map(|(key, row)| {
                    Ok(TabletScanRow {
                        key: key.primary_key_bytes,
                        row: encode_row(&row)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let next_resume_after = page
                .has_more
                .then(|| rows.last().map(|row| row.key.clone()))
                .flatten();
            let batch = TabletScanBatch {
                rows,
                next_resume_after,
                exhausted: !page.has_more,
            };
            batch
                .validate_for(&request)
                .map_err(|error| Error::CorruptData(error.to_string()))?;
            Ok(batch)
        });
    let _ = reply.send(result);
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
    identity: &TabletRuntimeIdentity,
    pending_read_states: &mut Vec<ReadState>,
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
    if let Some(position) = activation
        .as_ref()
        .copied()
        .filter(|position| position.term == term)
    {
        if ready_loop.raft().read_index_activation_term() == Some(term) {
            return Ok(());
        }
        if ready_loop
            .applied_frontier()
            .is_some_and(|frontier| frontier.index >= position.index)
        {
            ready_loop
                .activate_read_index(term)
                .map_err(classify_ready_error)?;
        }
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
        identity,
        pending_read_states,
    )?;

    let request_id = internal_barrier_allocator
        .candidate(term, tablet, identity.target.raft_group_id)
        .map_err(|error| HostedGroupError::Group(error.to_string()))?;
    let envelope = TabletCommandEnvelope::new(
        request_id.clone(),
        identity.target.tablet_id,
        identity.target.tablet_epoch,
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
        identity,
        pending_read_states,
    )?;
    if ready_loop
        .applied_frontier()
        .is_some_and(|frontier| frontier.index >= index)
    {
        ready_loop
            .activate_read_index(term)
            .map_err(classify_ready_error)?;
    }
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
    identity: &TabletRuntimeIdentity,
    pending_read_states: &mut Vec<ReadState>,
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
            identity,
            pending_read_states,
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
            .allocate_snapshot_id(
                identity.target.raft_group_id,
                local_replica_id,
                identity.target.tablet_id,
            )
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
        identity,
        pending_read_states,
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
    identity: &TabletRuntimeIdentity,
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
        raft_group_id: identity.target.raft_group_id,
        tablet_id: identity.target.tablet_id,
        table_id: identity.target.table_id,
        tablet_epoch: identity.target.tablet_epoch,
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
    let raft_identity = ready_loop.persistence().log_view().identity();
    let raft_pointer = raft_pointer_for_tablet(raft_identity, &durable.installed.pointer)
        .map_err(|error| error.to_string())?;
    let ready = ready_loop
        .persist_ready_after_snapshot_boundary(&raft_pointer)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "completed snapshot install produced no Ready generation".to_string())?;
    let classified_messages = classify_ready_messages(ready.messages);
    send_messages(
        transport,
        snapshot_endpoint,
        latest_snapshot,
        classified_messages.persistence_safe,
    );

    *tablet = TabletCommandApplier::new(durable.installed.state_machine);
    if identity.sql_mirror_enabled {
        database
            .blocking_lock()
            .install_replicated_storage(
                identity.target.table_id,
                tablet.state_machine().tablet().storage().clone(),
            )
            .map_err(|error| error.to_string())?;
    }

    let mut frontier = AppliedRaftFrontier::new(
        image.metadata.last_included_index,
        image.metadata.last_included_term,
    );
    for entry in &ready.committed_entries {
        frontier = AppliedRaftFrontier::new(entry.index, entry.term);
        let EntryPayload::Normal(bytes) = &entry.payload else {
            continue;
        };
        let dispositions = tablet
            .apply_committed_entry(
                ragnordb_multiraft::proposal::ProposalPosition {
                    term: entry.term,
                    index: entry.index,
                },
                bytes,
            )
            .map_err(|error| error.to_string())?;
        snapshot_policy.note_applied(bytes.len());
        publish_committed_entry(
            bytes,
            dispositions,
            registry,
            database,
            catalog_cache,
            identity,
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
        classified_messages.apply_dependent,
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

fn envelope_from_commit_for_identity(
    local_id: u64,
    commit: SingleNodeTxnCommit,
    identity: &TabletRuntimeIdentity,
) -> Result<TabletCommandEnvelope> {
    if commit.table_id != identity.target.table_id {
        return Err(Error::InvalidArgument(format!(
            "replicated tablet owns table {}, received table {}",
            identity.target.table_id.0, commit.table_id.0
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
            raft_group_id: identity.target.raft_group_id,
        },
        identity.target.tablet_id,
        identity.target.tablet_epoch,
        TabletCommand::SingleShardCommit(SingleShardCommitCommand {
            txn_id: commit.txn_id,
            start_timestamp: commit.start_timestamp,
            commit_timestamp: commit.commit_timestamp,
            writes,
        }),
    )
    .map_err(|source| Error::InvalidArgument(source.to_string()))
}

#[cfg(test)]
fn envelope_from_catalog(
    local_id: u64,
    update: &CatalogLogRecord,
) -> Result<TabletCommandEnvelope> {
    let identity = TabletRuntimeIdentity::new(TabletSnapshotInstallTarget {
        cluster_id: String::new(),
        raft_group_id: TABLET_RAFT_GROUP_ID,
        tablet_id: TABLET_ID,
        table_id: TABLE_ID,
        tablet_epoch: TABLET_EPOCH,
    });
    envelope_from_catalog_for_identity(local_id, update, &identity)
}

fn envelope_from_catalog_for_identity(
    local_id: u64,
    update: &CatalogLogRecord,
    identity: &TabletRuntimeIdentity,
) -> Result<TabletCommandEnvelope> {
    let namespace = local_id.rotate_left(17) ^ update.table_id.0;
    let client_id = (u128::from(update.update_timestamp.0) << 64) | u128::from(namespace);
    TabletCommandEnvelope::new(
        RequestId {
            client_id,
            sequence: 1,
            raft_group_id: identity.target.raft_group_id,
        },
        identity.target.tablet_id,
        identity.target.tablet_epoch,
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
fn publish_committed_entry(
    bytes: &[u8],
    entry: CommittedTabletCommandEntry,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    database: &SharedLocalDatabase,
    catalog_cache: &dyn CatalogCacheWriter,
    identity: &TabletRuntimeIdentity,
) -> std::result::Result<(), String> {
    match entry {
        CommittedTabletCommandEntry::Single(disposition) => {
            let envelope =
                TabletCommandEnvelope::decode(bytes).map_err(|error| error.to_string())?;
            let locally_proposed = registry.is_pending(&envelope.request_id);
            publish_committed_command(
                &envelope,
                locally_proposed,
                disposition,
                registry,
                database,
                catalog_cache,
                identity,
            )
        }
        CommittedTabletCommandEntry::Batch(dispositions) => {
            let batch =
                TabletCommandBatchEnvelope::decode(bytes).map_err(|error| error.to_string())?;
            if batch.commands.len() != dispositions.len() {
                return Err(format!(
                    "committed batch produced {} dispositions for {} commands",
                    dispositions.len(),
                    batch.commands.len()
                ));
            }
            for (envelope, disposition) in batch.commands.iter().zip(dispositions) {
                let locally_proposed = registry.is_pending(&envelope.request_id);
                publish_committed_command(
                    envelope,
                    locally_proposed,
                    disposition,
                    registry,
                    database,
                    catalog_cache,
                    identity,
                )?;
            }
            Ok(())
        }
    }
}

fn publish_committed_command(
    envelope: &TabletCommandEnvelope,
    locally_proposed: bool,
    disposition: CommittedTabletCommandDisposition,
    registry: &mut ProposalRegistry<TabletCommandApplyOutcome, TabletCommandApplyError>,
    database: &SharedLocalDatabase,
    catalog_cache: &dyn CatalogCacheWriter,
    identity: &TabletRuntimeIdentity,
) -> std::result::Result<(), String> {
    if matches!(disposition, CommittedTabletCommandDisposition::Applied(_)) {
        if let TabletCommand::SingleShardCommit(command) = &envelope.command
            && !locally_proposed
        {
            let mut database = database.blocking_lock();
            if identity.sql_mirror_enabled {
                database
                    .apply_replicated_commit(command)
                    .map_err(|error| error.to_string())?;
            } else {
                // A metadata tablet has no SQL mirror, but every follower
                // still needs the committed timestamp floor before it
                // serves a routed read or becomes leader.
                database
                    .observe_replicated_commit_high_water(command)
                    .map_err(|error| error.to_string())?;
            }
        }

        if let TabletCommand::Catalog(command) = &envelope.command {
            let update_timestamp =
                ragnordb_common::ids::Timestamp((envelope.request_id.client_id >> 64) as u64);
            catalog_cache
                .append_catalog_update(&CatalogLogRecord {
                    table_id: identity.target.table_id,
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
    identity: &TabletRuntimeIdentity,
    pending_read_states: &mut Vec<ReadState>,
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

    let classified_messages = classify_ready_messages(ready.messages);
    send_messages(
        transport,
        snapshot_endpoint,
        latest_snapshot,
        classified_messages.persistence_safe,
    );

    let mut frontier = None;
    for entry in &ready.committed_entries {
        frontier = Some(AppliedRaftFrontier::new(entry.index, entry.term));
        let EntryPayload::Normal(bytes) = &entry.payload else {
            continue;
        };
        let dispositions = tablet
            .apply_committed_entry(
                ragnordb_multiraft::proposal::ProposalPosition {
                    term: entry.term,
                    index: entry.index,
                },
                bytes,
            )
            .map_err(|error| HostedGroupError::Group(error.to_string()))?;
        snapshot_policy.note_applied(bytes.len());
        publish_committed_entry(
            bytes,
            dispositions,
            registry,
            database,
            catalog_cache,
            identity,
        )
        .map_err(HostedGroupError::Group)?;
    }
    if let Some(frontier) = frontier {
        ready_loop
            .advance_applied_frontier(frontier)
            .map_err(|error| HostedGroupError::Group(error.to_string()))?;
    }
    // Read states are emitted only after the Ready's committed entries have
    // applied and the applied frontier has advanced. They authorize a read
    // only; they never mutate or advance that frontier themselves.
    pending_read_states.extend(ready.read_states);
    let snapshot_install = ready.snapshot_install.clone();
    send_messages(
        transport,
        snapshot_endpoint,
        latest_snapshot,
        classified_messages.apply_dependent,
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

fn forward_completions(
    clients: &mut Vec<PendingClient>,
    tablet: &TabletCommandApplier,
    database: &SharedLocalDatabase,
    serving_leader: bool,
    leader_replica_id: Option<u64>,
    identity: &TabletRuntimeIdentity,
) {
    let mut pending = Vec::with_capacity(clients.len());
    for client in clients.drain(..) {
        match client.ticket.try_recv() {
            Ok(completion) => forward_completion(
                client.reply,
                completion,
                tablet,
                database,
                serving_leader,
                leader_replica_id,
                identity,
            ),
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

fn forward_completion(
    reply: ClientReply,
    completion: Completion,
    tablet: &TabletCommandApplier,
    database: &SharedLocalDatabase,
    serving_leader: bool,
    leader_replica_id: Option<u64>,
    identity: &TabletRuntimeIdentity,
) {
    match completion {
        ProposalCompletion::Applied {
            position, result, ..
        } => match reply {
            ClientReply::Commit(sender) => {
                let _ = sender.send(Ok(DurableWalExtent::from_raw(
                    position.index,
                    position.index.saturating_add(1),
                )));
            }
            ClientReply::Barrier(sender) => {
                let _ = sender.send(Ok(()));
            }
            ClientReply::Catalog(sender) => {
                let _ = sender.send(Ok(CatalogLogExtent {
                    start_lsn: position.index,
                    end_lsn: position.index.saturating_add(1),
                }));
            }
            ClientReply::Command(sender) => {
                let _ = sender.send(Ok(result));
            }
            ClientReply::RpcCommand {
                token,
                completion,
                remote_commit,
            } => {
                let result = match remote_commit {
                    Some(command) => database
                        .blocking_lock()
                        .observe_replicated_commit_high_water(&command)
                        .map(|()| result),
                    None => Ok(result),
                };
                completion.publish(token, TabletRpcCompletion::Command(result));
            }
            ClientReply::RpcReadPoint {
                token,
                completion,
                request,
                deadline,
            } => {
                completion.publish(
                    token,
                    TabletRpcCompletion::Read(evaluate_point_read(
                        request,
                        tablet,
                        serving_leader,
                        leader_replica_id,
                        identity,
                        deadline,
                    )),
                );
            }
            ClientReply::RpcScan {
                token,
                completion,
                request,
                deadline,
            } => {
                completion.publish(
                    token,
                    TabletRpcCompletion::Scan(evaluate_scan_request(
                        request,
                        tablet,
                        serving_leader,
                        leader_replica_id,
                        identity,
                        deadline,
                    )),
                );
            }
        },
        ProposalCompletion::Rejected { rejection, .. } => {
            send_client_error(reply, map_tablet_rejection(rejection));
        }
        ProposalCompletion::Retryable { failure, .. } => {
            send_client_error(
                reply,
                Error::ProposalUnavailable {
                    reason: format!("{failure:?}"),
                },
            );
        }
    }
}

fn map_tablet_rejection(rejection: TabletCommandApplyError) -> Error {
    match rejection {
        TabletCommandApplyError::WriteConflict { reason } => Error::WriteConflict(reason),
        TabletCommandApplyError::TabletEpochMismatch {
            current_epoch,
            expected_epoch,
        } => Error::StaleTabletEpoch {
            current_epoch,
            expected_epoch,
        },
        TabletCommandApplyError::RequestIdExpired {
            client_id,
            session_epoch,
            sequence,
            ..
        } => Error::RequestIdExpired {
            identity: format!("client={client_id:#034x}/epoch={session_epoch}/sequence={sequence}"),
        },
        other => Error::InvalidArgument(other.to_string()),
    }
}

fn reply_error(request: HostRequest, error: Error) {
    match request {
        HostRequest::Commit { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        HostRequest::Barrier { reply, .. } => {
            send_client_error(reply, error);
        }
        HostRequest::Catalog { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        HostRequest::Command { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        HostRequest::ReadPoint { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        HostRequest::Scan { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        HostRequest::OutcomeQuery { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        HostRequest::RpcCommand {
            completion, token, ..
        } => completion.publish(token, TabletRpcCompletion::Command(Err(error))),
        HostRequest::RpcReadPoint {
            completion, token, ..
        } => completion.publish(token, TabletRpcCompletion::Read(Err(error))),
        HostRequest::RpcScan {
            completion, token, ..
        } => completion.publish(token, TabletRpcCompletion::Scan(Err(error))),
        HostRequest::RpcOutcomeQuery {
            completion, token, ..
        } => completion.publish(token, TabletRpcCompletion::Outcome(Err(error))),
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
        ClientReply::Command(sender) => {
            let _ = sender.send(Err(error));
        }
        ClientReply::RpcCommand {
            token, completion, ..
        } => completion.publish(token, TabletRpcCompletion::Command(Err(error))),
        ClientReply::RpcReadPoint {
            token, completion, ..
        } => completion.publish(token, TabletRpcCompletion::Read(Err(error))),
        ClientReply::RpcScan {
            token, completion, ..
        } => completion.publish(token, TabletRpcCompletion::Scan(Err(error))),
    }
}

fn publish_status<W, LS, SS>(
    ready_loop: &RaftReadyLoop<W, LS, SS>,
    serving_leader: bool,
    snapshot: (u64, u64),
    snapshot_install_pending: bool,
    ownership: ReactorOwnership,
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
    published.current_election_timeout_ticks = ready_loop.raft().current_election_timeout();
    published.term = ready_loop.raft().hard_state().current_term;
    published.commit_index = ready_loop.raft().hard_state().commit;
    published.last_log_index = ready_loop.raft().last_log_index();
    published.serving_leader = serving_leader;
    published.snapshot_index = snapshot.0;
    published.snapshot_term = snapshot.1;
    if let Some(frontier) = ready_loop.applied_frontier() {
        published.applied_index = frontier.index;
        published.applied_term = frontier.term;
    } else {
        published.applied_index = 0;
        published.applied_term = 0;
    }
    published.uncommitted_bytes = ready_loop.raft().uncommitted_bytes();
    published.replication_inflight_bytes = ready_loop
        .raft()
        .conf_state()
        .replication_targets()
        .into_iter()
        .filter_map(|replica_id| ready_loop.raft().progress(replica_id))
        .map(|progress| progress.inflight_bytes)
        .sum();
    let apply_backlog = ready_loop.apply_backlog_status();
    published.apply_backlog_entries = apply_backlog.entries;
    published.apply_backlog_bytes = apply_backlog.bytes;
    published.apply_backlog_age_ms = apply_backlog.age_ms;
    published.apply_backlog_generations = apply_backlog.generations;
    let conf_state = ready_loop.raft().conf_state();
    published.conf_state_version = Some(conf_state.version);
    published.joining = ready_loop.raft().is_joining();
    published.voters = conf_state
        .voters
        .iter()
        .map(|replica_id| replica_id.get())
        .collect();
    published.learners = conf_state
        .learners
        .iter()
        .map(|replica_id| replica_id.get())
        .collect();
    published.outgoing_voters = conf_state
        .outgoing_voters
        .iter()
        .map(|replica_id| replica_id.get())
        .collect();
    published.replica_match_indices = ready_loop
        .raft()
        .replication_match_indices()
        .into_iter()
        .map(|(replica_id, index)| (replica_id.get(), index))
        .collect();
    published.pending_conf_change_index = ready_loop.raft().pending_conf_change_index();
    published.last_conf_change = ready_loop.raft().last_applied_conf_change();
    published.last_removed_replica = ready_loop
        .raft()
        .last_removed_replica()
        .map(|(replica_id, index, term, version)| (replica_id.get(), index, term, version));
    published.snapshot_install_pending = snapshot_install_pending;
    published.reactor_id = ownership.reactor_id;
    published.owner_generation = ownership.generation;
    let local_replica = ready_loop.raft().id();
    published.replica_in_conf_state = ready_loop
        .raft()
        .durable_conf_state()
        .map(|conf_state| conf_state.contains(local_replica));
}

#[cfg(test)]
mod tests {
    use super::*;
    use ragnordb_common::{
        catalog_codec::TableDefinition,
        command_codec::{CatalogCommand, CatalogOperation, CreateTableOperation},
        ids::{RowKey, Timestamp},
    };
    use ragnordb_multiraft::{proposal::ProposalPosition, tablet_apply::AppliedTabletCommand};
    use ragnordb_tablet::command::TabletCommandApplyResult;
    use std::collections::HashMap;
    use std::sync::Condvar;

    type OwnershipObservations = Arc<(
        Mutex<HashMap<RaftReplicaIdentity, (thread::ThreadId, bool)>>,
        Condvar,
    )>;

    struct OwnershipProbe {
        identity: RaftReplicaIdentity,
        ownership: ReactorOwnership,
        observations: OwnershipObservations,
    }

    impl ReactorGroup for OwnershipProbe {
        fn identity(&self) -> RaftReplicaIdentity {
            self.identity
        }

        fn ownership(&self) -> ReactorOwnership {
            self.ownership
        }

        fn is_shutdown(&self) -> bool {
            false
        }

        fn turn(&mut self) -> std::result::Result<bool, String> {
            let (owners, wake) = &*self.observations;
            let mut owners = owners
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let current_thread = thread::current().id();
            if let Some((owner_thread, changed)) = owners.get_mut(&self.identity) {
                if *owner_thread != current_thread {
                    *changed = true;
                }
            } else {
                owners.insert(self.identity, (current_thread, false));
            }
            wake.notify_all();
            Ok(false)
        }

        fn fail(&mut self, _reason: String) {}
    }

    #[test]
    /// Realistic bug caught: a fixed reactor implementation could accidentally
    /// spawn one execution thread per group or let one group execute on two
    /// owners concurrently after registration.
    fn fixed_reactors_keep_each_group_on_one_of_the_fixed_threads() {
        let reactors = FixedReactorSet::new(2).expect("the fixed reactor set must start");
        let group_count: usize = 10_000;
        let observations = Arc::new((
            Mutex::new(HashMap::<RaftReplicaIdentity, (thread::ThreadId, bool)>::new()),
            Condvar::new(),
        ));
        let mut generations = BTreeSet::new();

        for group_number in 0..group_count {
            let identity =
                RaftReplicaIdentity::new(RaftGroupId(100 + group_number as u64), ReplicaId(1))
                    .expect("the probe identity must be valid");
            let assignment = reactors.assign().expect("assignment must succeed");
            assert!(assignment.ownership.reactor_id < 2);
            assert!(generations.insert(assignment.ownership.generation));
            reactors
                .register(
                    &assignment,
                    Box::new(OwnershipProbe {
                        identity,
                        ownership: assignment.ownership,
                        observations: observations.clone(),
                    }),
                )
                .expect("registration must be acknowledged by the owner reactor");
        }

        let duplicate_assignment = reactors.assign().expect("assignment must succeed");
        let duplicate_identity = RaftReplicaIdentity::new(RaftGroupId(100), ReplicaId(1))
            .expect("the duplicate probe identity must be valid");
        assert!(
            reactors
                .register(
                    &duplicate_assignment,
                    Box::new(OwnershipProbe {
                        identity: duplicate_identity,
                        ownership: duplicate_assignment.ownership,
                        observations: observations.clone(),
                    }),
                )
                .is_err(),
            "one replica identity must not be registered on two reactors"
        );

        let deadline = Instant::now() + Duration::from_secs(2);
        let (owners, wake) = &*observations;
        let mut owners = owners
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while owners.len() < group_count && Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let (next, _) = wake
                .wait_timeout(owners, remaining)
                .expect("probe observation lock must not be poisoned");
            owners = next;
        }

        assert_eq!(owners.len(), group_count);
        assert_eq!(generations.len(), group_count);
        assert!(owners.values().all(|(_, changed)| !changed));
        let distinct_threads = owners
            .values()
            .map(|(thread_id, _)| thread_id)
            .collect::<std::collections::HashSet<_>>();
        assert!(
            distinct_threads.len() <= 2,
            "{} groups used {} execution threads",
            group_count,
            distinct_threads.len()
        );
        drop(owners);
        drop(reactors);
    }

    #[test]
    /// Realistic bug caught: byte reservations that are not released on pop
    /// permanently reject later work even though the bounded queue is empty.
    fn mailbox_byte_accounting_is_released_on_pop() {
        let wake = ReactorWake::new();
        let (sender, receiver) =
            ByteBoundedMailbox::pair(2, Arc::new(MailboxBudget::new(1_024)), wake);
        let (reply, _response) = mpsc::sync_channel(1);
        let item = HostRequest::Barrier {
            reply: ClientReply::Barrier(reply),
            deadline: Instant::now() + Duration::from_secs(1),
        };
        let item_bytes = item.mailbox_bytes().max(MAILBOX_ITEM_OVERHEAD);
        assert!(sender.try_send(item).is_ok());
        assert_eq!(sender.budget.used(), item_bytes);
        let _ = receiver.try_recv().expect("the queued item must be popped");
        assert_eq!(sender.budget.used(), 0);
    }

    fn snapshot_state_blocks_normal_work(
        incoming_phase: Option<IncomingSnapshotPhase>,
        local_snapshot_pending: bool,
    ) -> bool {
        local_snapshot_pending || incoming_phase == Some(IncomingSnapshotPhase::ReadyPending)
    }

    #[test]
    fn point_read_rejects_stale_tablet_generation_before_storage_access() {
        let identity = TabletRuntimeIdentity::new(TabletSnapshotInstallTarget {
            cluster_id: "cluster".to_string(),
            raft_group_id: RaftGroupId(9),
            tablet_id: TabletId(3),
            table_id: TableId(3),
            tablet_epoch: 7,
        });
        let tablet = ragnordb_tablet::Tablet::new(TabletId(3), TableId(3)).unwrap();
        let state_machine =
            ragnordb_tablet::command::TabletStateMachine::new(tablet, 7, RaftGroupId(9)).unwrap();
        let applier = TabletCommandApplier::new(state_machine);
        let request = TabletReadRequest {
            request_id: RequestId {
                client_id: 1,
                sequence: 1,
                raft_group_id: RaftGroupId(9),
            },
            logical_command_id: None,
            tablet_id: TabletId(3),
            tablet_epoch: 6,
            row_key: RowKey {
                table_id: TableId(3),
                primary_key_bytes: b"pk".to_vec(),
            },
            read_timestamp: Timestamp(10),
            deadline_remaining_ms: None,
        };
        let (sender, receiver) = mpsc::sync_channel(1);

        admit_read_request(
            request,
            &applier,
            true,
            Some(1),
            &identity,
            sender,
            Instant::now() + Duration::from_secs(1),
        );

        assert!(matches!(
            receiver.recv().unwrap(),
            Err(Error::StaleTabletEpoch {
                current_epoch: 7,
                expected_epoch: 6,
            })
        ));
    }

    #[test]
    /// Realistic bug caught: a saturated Ready-owner request queue could make
    /// a latest-read caller wait in `SyncSender::send` after its deadline had
    /// already elapsed.
    fn latest_read_admission_does_not_block_on_a_full_request_queue() {
        let wake = ReactorWake::new();
        let (request_tx, request_rx) =
            ByteBoundedMailbox::pair(1, Arc::new(MailboxBudget::new(1_024)), wake.clone());
        let (queued_reply, _queued_response) = mpsc::sync_channel(1);
        request_tx
            .send(HostRequest::Barrier {
                reply: ClientReply::Barrier(queued_reply),
                deadline: Instant::now() + Duration::from_secs(1),
            })
            .expect("the saturation fixture must fill the request queue");

        let handle = Arc::new(ReplicatedTabletHandle {
            requests: request_tx,
            wake,
            ownership: ReactorOwnership {
                reactor_id: 0,
                generation: 1,
            },
            status: Arc::new(RwLock::new(ReplicatedTabletStatus::default())),
        });
        let deadline = Instant::now() + Duration::from_millis(60);
        let (result_tx, result_rx) = mpsc::channel();
        let caller = thread::spawn(move || {
            result_tx
                .send(handle.read_barrier_until(deadline))
                .expect("the deadline result must be observable");
        });

        let early_result = result_rx.recv_timeout(Duration::from_millis(150)).ok();
        let completed_before_queue_release = early_result.is_some();
        if early_result.is_none() {
            // Release the fixture so the pre-fix blocking sender cannot leak
            // beyond this test after the assertion has been evaluated.
            let _ = request_rx
                .recv_timeout(Duration::from_millis(100))
                .expect("the saturated request must be releasable");
        }
        let result =
            early_result.or_else(|| result_rx.recv_timeout(Duration::from_millis(200)).ok());
        caller.join().expect("the read caller must exit");

        assert!(
            completed_before_queue_release,
            "latest-read admission must return by its deadline even when the request queue is full; result={result:?}"
        );
        assert!(matches!(
            result,
            Some(Err(Error::ProposalUnavailable { .. }))
        ));
    }

    #[test]
    /// Realistic bug caught: an already-expired latest read must not enqueue
    /// work behind a saturated request queue or wait for the Ready owner.
    fn expired_latest_read_admission_is_rejected_before_queue_access() {
        let wake = ReactorWake::new();
        let (request_tx, request_rx) =
            ByteBoundedMailbox::pair(1, Arc::new(MailboxBudget::new(1_024)), wake.clone());
        let (queued_reply, _queued_response) = mpsc::sync_channel(1);
        request_tx
            .send(HostRequest::Barrier {
                reply: ClientReply::Barrier(queued_reply),
                deadline: Instant::now() + Duration::from_secs(1),
            })
            .expect("the saturation fixture must fill the request queue");
        let handle = ReplicatedTabletHandle {
            requests: request_tx,
            wake,
            ownership: ReactorOwnership {
                reactor_id: 0,
                generation: 1,
            },
            status: Arc::new(RwLock::new(ReplicatedTabletStatus::default())),
        };
        let deadline = Instant::now();

        let barrier = handle.read_barrier_until(deadline);
        let point = handle.read_point_until(
            TabletReadRequest {
                request_id: RequestId {
                    client_id: 1,
                    sequence: 1,
                    raft_group_id: RaftGroupId(9),
                },
                logical_command_id: None,
                tablet_id: TabletId(3),
                tablet_epoch: 7,
                row_key: RowKey {
                    table_id: TableId(3),
                    primary_key_bytes: b"pk".to_vec(),
                },
                read_timestamp: Timestamp(10),
                deadline_remaining_ms: None,
            },
            deadline,
        );
        let scan = handle.scan_page_until(
            TabletScanRequest {
                request_id: RequestId {
                    client_id: 1,
                    sequence: 2,
                    raft_group_id: RaftGroupId(9),
                },
                tablet_id: TabletId(3),
                tablet_epoch: 7,
                start_key: Some(vec![0]),
                end_key: Some(vec![1]),
                resume_after: None,
                read_timestamp: Timestamp(10),
                max_rows: 1,
                max_bytes: 64,
                rpc_attempt_id: Some(1),
                deadline_remaining_ms: None,
            },
            deadline,
        );

        assert!(matches!(barrier, Err(Error::ProposalUnavailable { .. })));
        assert!(matches!(point, Err(Error::ProposalUnavailable { .. })));
        assert!(matches!(scan, Err(Error::ProposalUnavailable { .. })));
        assert!(matches!(
            request_rx.try_recv(),
            Ok(HostRequest::Barrier { .. })
        ));
        assert!(matches!(
            request_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
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

        let wake = ReactorWake::new();
        let (control_tx, control_rx) =
            ByteBoundedMailbox::pair(1, Arc::new(MailboxBudget::new(1_024)), wake);
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
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
                RaftHostControl::Step { .. }
                | RaftHostControl::Propose { .. }
                | RaftHostControl::ProposeConfChange { .. }
                | RaftHostControl::TransferLeadership { .. } => {
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
            &TabletRuntimeIdentity::new(TabletSnapshotInstallTarget {
                cluster_id: String::new(),
                raft_group_id: TABLET_RAFT_GROUP_ID,
                tablet_id: TABLET_ID,
                table_id: TABLE_ID,
                tablet_epoch: TABLET_EPOCH,
            }),
        )
        .expect_err("uncertain catalog persistence must stop Ready publication");

        assert!(error.contains("injected synchronization ambiguity"));
        assert_eq!(registry.pending_count(), 1);
        assert!(matches!(ticket.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }

    /// Realistic bug caught: a metadata-created tablet could still emit a
    /// legacy group/tablet identity in a catalog proposal, causing followers
    /// to reject an otherwise valid command as belonging to another tablet.
    #[test]
    fn metadata_tablet_proposal_uses_its_declared_identity() {
        let identity = TabletRuntimeIdentity::new(TabletSnapshotInstallTarget {
            cluster_id: "cluster-5-5".to_string(),
            raft_group_id: RaftGroupId(42),
            tablet_id: TabletId(9),
            table_id: TableId(77),
            tablet_epoch: 3,
        });
        let record = CatalogLogRecord {
            table_id: identity.target.table_id,
            update_timestamp: Timestamp(7),
            command: CatalogCommand {
                operation: CatalogOperation::CreateTable(CreateTableOperation {
                    table_def: TableDefinition {
                        table_id: identity.target.table_id.0,
                        name: "metadata_users".to_string(),
                        columns: Vec::new(),
                        primary_key_column_ids: Vec::new(),
                        schema_version: 1,
                        tablet_count: 1,
                    },
                }),
            },
        };

        let envelope = envelope_from_catalog_for_identity(1, &record, &identity)
            .expect("metadata tablet catalog envelope must encode");
        assert_eq!(
            envelope.request_id.raft_group_id,
            identity.target.raft_group_id
        );
        assert_eq!(envelope.tablet_id, identity.target.tablet_id);
        assert_eq!(envelope.expected_epoch, identity.target.tablet_epoch);
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
        let wake = ReactorWake::new();
        let (control_tx, control_rx) =
            ByteBoundedMailbox::pair(3, Arc::new(MailboxBudget::new(1_024)), wake);

        let (tick_reply_tx, tick_reply_rx) = mpsc::sync_channel(1);

        let (step_reply_tx, step_reply_rx) = mpsc::sync_channel(1);

        let (propose_reply_tx, propose_reply_rx) = mpsc::sync_channel(1);

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
                generation: 0,
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

    #[test]
    /// Catches unbounded reply-channel retention when a leader cannot obtain a
    /// ReadIndex quorum and many latest reads coalesce behind one context.
    fn coalesced_latest_read_waiters_have_a_hard_admission_bound() {
        let (reply, _receiver) = mpsc::sync_channel(1);
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut pending = vec![PendingReadBarrier {
            context: b"coalesced".to_vec(),
            term: 7,
            fallback_at: deadline,
            waiters: (0..MAX_PENDING_READ_BARRIER_WAITERS)
                .map(|_| PendingReadBarrierWaiter {
                    reply: ClientReply::Barrier(reply.clone()),
                    deadline,
                })
                .collect(),
        }];

        assert_eq!(
            pending_read_barrier_waiter_count(&pending),
            MAX_PENDING_READ_BARRIER_WAITERS
        );
        assert!(
            pending_read_barrier_waiter_count(&pending) >= MAX_PENDING_READ_BARRIER_WAITERS,
            "the admission path must reject another waiter before pushing it"
        );
        pending.clear();
        assert_eq!(pending_read_barrier_waiter_count(&pending), 0);
    }
}
