//! Tablet RPC gateway and node-local request dispatcher.
//!
//! The physical transport only authenticates the source node and multiplexes
//! frames. This module owns the next boundary: it validates the typed tablet
//! payload, resolves the group to a Ready-owner handle, and sends the result
//! back with the original request identity. Keeping this logic outside the
//! Raft host prevents network retries from bypassing proposal/apply ordering.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use prost::Message;
use ragnordb_catalog::{Catalog, MetadataApplyOutcome, MetadataRejection, MetadataState};
use ragnordb_common::{
    Error, Result,
    codec::TxnStatusRecord,
    command_codec::{CachedTabletCommandOutcome, TabletCommand},
    ids::{
        LogicalCommandId, NodeId, RaftGroupId, ReplicaId, RequestId, RowKey, TableId, TabletId,
        Timestamp, TxnId,
    },
    metadata_codec::DesiredReplicaRole,
    proto::{mvcc, rpc},
    rpc_codec::{
        MessageType, MetadataProposalRequest, MetadataRequest, MetadataResponse, ReplicaRoute,
        RpcFrame, TabletCommandRequest, TabletCommandResponse, TabletOutcomeQueryRequest,
        TabletPointInspectionRequest, TabletPointReadInspection, TabletReadRequest, TabletRoute,
        TabletRouteCache, TabletScanBatch, TabletScanRequest, TabletTransactionStatusRequest,
    },
};
use ragnordb_exec::{TabletGateway, TabletScanRoute};
use ragnordb_multiraft::meta::MetadataRuntimeHandle;
use ragnordb_multiraft::transport::{NodeRaftTransport, NodeRpcInbound};
use ragnordb_tablet::command::{TabletCommandApplyOutcome, TabletCommandApplyResult};

/// One bounded route page and the generation of its committed metadata view.
pub type TabletRoutePage = (u64, Vec<(TableId, TabletRoute)>, bool);
use ragnordb_tablet::{ScanSpan, TabletRouter};

use crate::bootstrap::METADATA_RAFT_GROUP_ID;
use crate::multiraft_runtime::{HostWake, MetadataHostRequest};
use crate::replicated_tablet::{ReplicatedTabletHandle, ReplicatedTabletStatus};
use crate::replicated_tablet::{TabletRpcCompletion, TabletRpcCompletionSink};

/// Group-qualified handles published by the lifecycle owner after a tablet
/// runtime has crossed its activation boundary.
pub type SharedTabletHandleRegistry =
    Arc<RwLock<BTreeMap<RaftGroupId, Arc<ReplicatedTabletHandle>>>>;

enum PendingResponseMessage {
    Tablet(TabletCommandResponse),
    Metadata(MetadataResponse),
    ReplicaJoin(rpc::ReplicaJoinResponse),
}

struct PendingResponse {
    target: NodeId,
    sender: mpsc::Sender<PendingResponseMessage>,
}

const MAX_PENDING_INBOUND_TABLET_RPCS: usize = 4_096;
const INBOUND_TABLET_RPC_POLL_INTERVAL: Duration = Duration::from_millis(1);
const RPC_DISPATCH_MESSAGE_BUDGET: usize = 256;
const MAX_PENDING_CONTROL_RPCS: usize = 512;
const CONTROL_RPC_DRAIN_BUDGET: usize = 256;
const CONTROL_RPC_DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

struct PendingTabletRpc {
    source: NodeId,
    group_id: RaftGroupId,
    attempt_id: Option<u64>,
    request_id: RequestId,
    status: Arc<RwLock<ReplicatedTabletStatus>>,
    deadline: Instant,
    completion: Option<TabletRpcCompletion>,
    kind: PendingTabletRpcKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingTabletRpcKind {
    Mutation,
    ReadOnly,
}

enum PendingControlRpc {
    ReplicaJoin {
        source: NodeId,
        group_id: RaftGroupId,
        attempt_id: u64,
        deadline: Instant,
        response: mpsc::Receiver<ReplicaJoinAdmissionResult>,
    },
    MetadataCommand {
        source: NodeId,
        group_id: RaftGroupId,
        attempt_id: u64,
        request_id: RequestId,
        deadline: Instant,
        response: mpsc::Receiver<Result<MetadataApplyOutcome>>,
    },
    MetadataConfChange {
        source: NodeId,
        group_id: RaftGroupId,
        attempt_id: u64,
        deadline: Instant,
        response: mpsc::Receiver<Result<()>>,
    },
}

impl PendingControlRpc {
    fn attempt_id(&self) -> u64 {
        match self {
            Self::ReplicaJoin { attempt_id, .. }
            | Self::MetadataCommand { attempt_id, .. }
            | Self::MetadataConfChange { attempt_id, .. } => *attempt_id,
        }
    }
}

#[derive(Clone)]
pub(crate) struct RpcState {
    pending: Arc<Mutex<BTreeMap<u64, PendingResponse>>>,
    next_attempt: Arc<AtomicU64>,
    inbound_tablet: Arc<Mutex<BTreeMap<u64, PendingTabletRpc>>>,
    next_tablet_token: Arc<AtomicU64>,
    pending_control: Arc<Mutex<Vec<PendingControlRpc>>>,
    dispatcher_wake: Arc<Mutex<Option<thread::Thread>>>,
}

#[derive(Clone)]
pub(crate) struct MetadataRpcClient {
    transport: NodeRaftTransport,
    rpc_state: RpcState,
}

#[derive(Clone)]
pub(crate) struct ReplicaJoinRpcClient {
    transport: NodeRaftTransport,
    rpc_state: RpcState,
}

pub(crate) struct ReplicaJoinAdmission {
    pub source_node_id: NodeId,
    pub request: rpc::ReplicaJoinRequest,
    pub reply: mpsc::Sender<ReplicaJoinAdmissionResult>,
}

#[derive(Debug)]
pub(crate) struct ReplicaJoinAdmissionResult {
    pub success: bool,
    pub error_message: String,
}

impl ReplicaJoinRpcClient {
    pub(crate) fn new(transport: NodeRaftTransport, rpc_state: RpcState) -> Self {
        Self {
            transport,
            rpc_state,
        }
    }

    /// Ask the target node to durably prepare a joining lifetime. The leader
    /// must receive a successful response before it can propose AddLearner;
    /// otherwise Raft may emit replication traffic to an unmaterialized route.
    pub(crate) fn prepare(
        &self,
        target: NodeId,
        mut request: rpc::ReplicaJoinRequest,
        timeout: Duration,
    ) -> Result<()> {
        let attempt_id = self.rpc_state.next_attempt_id()?;
        request.rpc_attempt_id = Some(attempt_id);
        let frame = RpcFrame {
            msg_type: MessageType::ReplicaJoinRequest,
            raft_group_id: ragnordb_common::ids::RaftGroupId::from_proto(
                request
                    .raft_group_id
                    .ok_or_else(|| Error::Configuration("join request has no group".into()))?,
            ),
            payload: request.encode_to_vec(),
        };
        let (sender, receiver) = mpsc::channel();
        self.rpc_state
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(attempt_id, PendingResponse { target, sender });
        if let Err(error) = self.transport.try_send_rpc(target, frame) {
            self.rpc_state
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&attempt_id);
            return Err(Error::ProposalUnavailable {
                reason: format!("replica join request could not be queued: {error}"),
            });
        }
        match receiver.recv_timeout(timeout) {
            Ok(PendingResponseMessage::ReplicaJoin(response)) if response.success => Ok(()),
            Ok(PendingResponseMessage::ReplicaJoin(response)) => Err(Error::ProposalUnavailable {
                reason: response.error_message,
            }),
            Ok(_) => Err(Error::CorruptData(
                "replica join waiter received an unrelated response".to_string(),
            )),
            Err(_) => {
                self.rpc_state
                    .pending
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&attempt_id);
                Err(Error::ProposalUnavailable {
                    reason: "replica join preparation deadline elapsed".to_string(),
                })
            }
        }
    }
}

impl MetadataRpcClient {
    pub(crate) fn new(transport: NodeRaftTransport, rpc_state: RpcState) -> Self {
        Self {
            transport,
            rpc_state,
        }
    }

    pub(crate) fn propose(
        &self,
        target: NodeId,
        raft_group_id: RaftGroupId,
        request: MetadataProposalRequest,
        timeout: Duration,
    ) -> Result<MetadataResponse> {
        let frame = RpcFrame {
            msg_type: MessageType::MetadataRequest,
            raft_group_id,
            payload: MetadataRequest::ProposeCommand(request)
                .to_proto()
                .encode_to_vec(),
        };
        let attempt_id = self.rpc_state.next_attempt_id()?;
        let payload = attach_rpc_attempt_id(frame.msg_type, &frame.payload, attempt_id)?;
        let (sender, receiver) = mpsc::channel();
        {
            let mut pending =
                self.rpc_state
                    .pending
                    .lock()
                    .map_err(|_| Error::ProposalUnavailable {
                        reason: "metadata RPC pending-response lock is poisoned".to_string(),
                    })?;
            pending.insert(attempt_id, PendingResponse { target, sender });
        }
        if let Err(error) = self.transport.try_send_rpc(
            target,
            RpcFrame {
                msg_type: frame.msg_type,
                raft_group_id: frame.raft_group_id,
                payload,
            },
        ) {
            self.rpc_state
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&attempt_id);
            return Err(Error::ProposalUnavailable {
                reason: format!("metadata RPC could not be queued: {error}"),
            });
        }

        match receiver.recv_timeout(timeout) {
            Ok(PendingResponseMessage::Metadata(response)) => Ok(response),
            Ok(PendingResponseMessage::Tablet(_)) => Err(Error::CorruptData(
                "metadata RPC waiter received a tablet response".to_string(),
            )),
            Ok(PendingResponseMessage::ReplicaJoin(_)) => Err(Error::CorruptData(
                "metadata RPC waiter received a replica-join response".to_string(),
            )),
            Err(_) => {
                self.rpc_state
                    .pending
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&attempt_id);
                Err(Error::ProposalUnavailable {
                    reason: "metadata RPC response deadline elapsed".to_string(),
                })
            }
        }
    }

    pub(crate) fn propose_conf_change(
        &self,
        target: NodeId,
        expected_conf_state_version: u64,
        replica_id: ReplicaId,
        timeout: Duration,
    ) -> Result<MetadataResponse> {
        let attempt_id = self.rpc_state.next_attempt_id()?;
        let request = rpc::MetadataConfChangeRequest {
            rpc_attempt_id: Some(attempt_id),
            expected_conf_state_version,
            replica_id: Some(replica_id.to_proto()),
            remove_replica: true,
        };
        let frame = RpcFrame {
            msg_type: MessageType::MetadataRequest,
            raft_group_id: METADATA_RAFT_GROUP_ID,
            payload: rpc::MetadataRequest {
                request: Some(rpc::metadata_request::Request::ProposeConfChange(request)),
            }
            .encode_to_vec(),
        };
        let (sender, receiver) = mpsc::channel();
        self.rpc_state
            .pending
            .lock()
            .map_err(|_| Error::ProposalUnavailable {
                reason: "metadata ConfChange pending-response lock is poisoned".to_string(),
            })?
            .insert(attempt_id, PendingResponse { target, sender });
        if let Err(error) = self.transport.try_send_rpc(
            target,
            RpcFrame {
                msg_type: frame.msg_type,
                raft_group_id: frame.raft_group_id,
                payload: frame.payload,
            },
        ) {
            self.rpc_state
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&attempt_id);
            return Err(Error::ProposalUnavailable {
                reason: format!("metadata ConfChange RPC could not be queued: {error}"),
            });
        }

        match receiver.recv_timeout(timeout) {
            Ok(PendingResponseMessage::Metadata(response)) => Ok(response),
            Ok(PendingResponseMessage::Tablet(_)) | Ok(PendingResponseMessage::ReplicaJoin(_)) => {
                Err(Error::CorruptData(
                    "metadata ConfChange waiter received an unrelated response".to_string(),
                ))
            }
            Err(_) => {
                self.rpc_state
                    .pending
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&attempt_id);
                Err(Error::ProposalUnavailable {
                    reason: "metadata ConfChange response deadline elapsed".to_string(),
                })
            }
        }
    }
}

impl RpcState {
    pub(crate) fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(BTreeMap::new())),
            next_attempt: Arc::new(AtomicU64::new(1)),
            inbound_tablet: Arc::new(Mutex::new(BTreeMap::new())),
            next_tablet_token: Arc::new(AtomicU64::new(1)),
            pending_control: Arc::new(Mutex::new(Vec::new())),
            dispatcher_wake: Arc::new(Mutex::new(None)),
        }
    }

    fn next_attempt_id(&self) -> Result<u64> {
        let attempt_id = self.next_attempt.fetch_add(1, Ordering::Relaxed);
        if attempt_id == 0 {
            return Err(Error::ProposalUnavailable {
                reason: "RPC attempt identity exhausted".to_string(),
            });
        }
        Ok(attempt_id)
    }

    fn bind_dispatcher(&self) {
        *self
            .dispatcher_wake
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(thread::current());
    }

    fn wake_dispatcher(&self) {
        if let Some(dispatcher) = self
            .dispatcher_wake
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .cloned()
        {
            dispatcher.unpark();
        }
    }

    fn reserve_tablet_rpc(&self, pending: PendingTabletRpc) -> Result<u64> {
        let token = self.next_tablet_token.fetch_add(1, Ordering::Relaxed);
        if token == 0 {
            return Err(Error::ProposalUnavailable {
                reason: "tablet RPC completion token identity exhausted".to_string(),
            });
        }
        let mut pending_requests = self
            .inbound_tablet
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending_requests.len() >= MAX_PENDING_INBOUND_TABLET_RPCS {
            return Err(Error::ProposalUnavailable {
                reason: "tablet RPC completion capacity is full".to_string(),
            });
        }
        pending_requests.insert(token, pending);
        Ok(token)
    }

    fn cancel_tablet_rpc(&self, token: u64) {
        self.inbound_tablet
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&token);
    }

    fn reserve_control_rpc(&self, pending: PendingControlRpc) -> Result<()> {
        let mut pending_requests = self
            .pending_control
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending_requests.len() >= MAX_PENDING_CONTROL_RPCS {
            return Err(Error::ProposalUnavailable {
                reason: "control-plane RPC completion capacity is full".to_string(),
            });
        }
        pending_requests.push(pending);
        Ok(())
    }

    fn cancel_control_rpc(&self, attempt_id: u64) {
        self.pending_control
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|pending| pending.attempt_id() != attempt_id);
    }

    fn take_ready_tablet_rpcs(&self, now: Instant) -> Vec<PendingTabletRpc> {
        let mut pending_requests = self
            .inbound_tablet
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let ready_tokens = pending_requests
            .iter()
            .filter(|(_, pending)| pending.completion.is_some() || pending.deadline <= now)
            .map(|(token, _)| *token)
            .take(256)
            .collect::<Vec<_>>();
        ready_tokens
            .into_iter()
            .filter_map(|token| pending_requests.remove(&token))
            .collect()
    }

    fn publish_tablet_completion(&self, token: u64, completion: TabletRpcCompletion) {
        let mut pending_requests = self
            .inbound_tablet
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(pending) = pending_requests.get_mut(&token)
            && pending.completion.is_none()
        {
            pending.completion = Some(completion);
            drop(pending_requests);
            self.wake_dispatcher();
        }
    }
}

impl TabletRpcCompletionSink for RpcState {
    fn publish(&self, token: u64, completion: TabletRpcCompletion) {
        self.publish_tablet_completion(token, completion);
    }
}

/// Client-side gateway for local and remote tablet operations.
#[derive(Clone)]
pub struct TabletRpcClient {
    transport: NodeRaftTransport,
    handles: SharedTabletHandleRegistry,
    rpc_state: RpcState,
    metadata: MetadataRuntimeHandle,
    /// One validated routing table is published per committed metadata
    /// generation. Request routing only clones this immutable view; it never
    /// rebuilds partition boundaries or placement routes on the hot path.
    routing_snapshot: Arc<RwLock<Arc<RoutingSnapshot>>>,
    route_cache: Arc<RwLock<TabletRouteCache>>,
}

#[derive(Debug, Clone)]
struct RoutingSnapshot {
    generation: u64,
    tables: BTreeMap<TableId, TableRoutingSnapshot>,
    /// Global ordered index lets background workers seek directly to a route
    /// cursor instead of rescanning every route on every bounded page.
    routes_by_tablet: BTreeMap<(TableId, TabletId), TabletRoute>,
}

#[derive(Debug, Clone)]
struct TableRoutingSnapshot {
    router: Arc<TabletRouter>,
    routes_by_tablet: BTreeMap<ragnordb_common::ids::TabletId, TabletRoute>,
}

impl TabletRpcClient {
    fn new(
        transport: NodeRaftTransport,
        handles: SharedTabletHandleRegistry,
        rpc_state: RpcState,
        metadata: MetadataRuntimeHandle,
    ) -> Self {
        Self {
            transport,
            handles,
            rpc_state,
            metadata,
            routing_snapshot: Arc::new(RwLock::new(Arc::new(RoutingSnapshot {
                generation: 0,
                tables: BTreeMap::new(),
                routes_by_tablet: BTreeMap::new(),
            }))),
            route_cache: Arc::new(RwLock::new(TabletRouteCache::new())),
        }
    }

    /// Load a complete routing snapshot, rebuilding it only after the
    /// metadata publication generation changes.
    ///
    /// The metadata state is immutable after publication. Building the
    /// derived view is therefore safe outside the snapshot lock; the short
    /// write section only swaps one Arc after all tables and routes have
    /// passed validation.
    fn routing_snapshot(&self) -> Result<Arc<RoutingSnapshot>> {
        let (generation, state) = self.metadata.state_snapshot_with_generation();
        {
            let current = self
                .routing_snapshot
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if current.generation == generation {
                return Ok(current.clone());
            }
        }

        let replacement = Arc::new(build_routing_snapshot(generation, &state)?);
        let mut current = self
            .routing_snapshot
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if current.generation < generation {
            *current = replacement;
        }
        Ok(current.clone())
    }

    fn route_with_leader_hint(&self, base_route: &TabletRoute) -> Result<TabletRoute> {
        let mut route = base_route.clone();
        let published_leader = self
            .handles
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&route.raft_group_id)
            .and_then(|handle| handle.status().leader_replica_id)
            .map(ReplicaId)
            .filter(|leader| {
                route
                    .replicas
                    .iter()
                    .any(|replica| replica.replica_id == *leader)
            });
        if let Some(leader) = published_leader {
            route.leader_replica_id = leader;
        }
        if let Some(cached_leader) = self.cached_leader_for(&route) {
            route.leader_replica_id = cached_leader;
        }
        route
            .validate()
            .map_err(|error| Error::CorruptData(error.to_string()))?;
        self.cache_authoritative_route(&route);
        Ok(route)
    }

    /// Resolve the current committed metadata route for one point key.
    ///
    /// Metadata placement is read-only here. The returned leader is only a
    /// hint; a tablet response may refresh it after a leadership transition.
    pub fn lookup_tablet_route(&self, table_id: TableId, key: &[u8]) -> Result<TabletRoute> {
        let snapshot = self.routing_snapshot()?;
        let table = snapshot.tables.get(&table_id).ok_or_else(|| {
            Error::SchemaMismatch(format!("metadata has no table {}", table_id.0))
        })?;
        let tablet_id = table.router.route_point(key)?;
        let route = table.routes_by_tablet.get(&tablet_id).ok_or_else(|| {
            Error::CorruptData("metadata route selected an unknown tablet".to_string())
        })?;
        self.route_with_leader_hint(route)
    }

    /// Resolve one logical span against a single committed metadata snapshot.
    /// The returned physical routes are deliberately paired with clipped
    /// logical fragments so a stale epoch can be re-resolved without treating
    /// the original tablet-ID list as progress.
    pub fn lookup_scan_routes(
        &self,
        table_id: TableId,
        span: &ScanSpan,
    ) -> Result<Vec<TabletScanRoute>> {
        span.validate()?;
        let snapshot = self.routing_snapshot()?;
        let table = snapshot.tables.get(&table_id).ok_or_else(|| {
            Error::SchemaMismatch(format!("metadata has no table {}", table_id.0))
        })?;
        table
            .router
            .route_scan_fragments(span)?
            .into_iter()
            .map(|fragment| {
                let route = table
                    .routes_by_tablet
                    .get(&fragment.tablet_id)
                    .ok_or_else(|| {
                        Error::CorruptData(
                            "metadata scan route selected an unknown tablet".to_string(),
                        )
                    })?;
                Ok(TabletScanRoute {
                    route: self.route_with_leader_hint(route)?,
                    span: fragment.span,
                })
            })
            .collect()
    }

    /// Return one route for every tablet in the current committed metadata
    /// view. Background lifecycle workers use this snapshot to schedule bounded
    /// per-tablet work without enumerating physical Raft groups independently.
    pub fn all_tablet_routes(&self) -> Result<Vec<(TableId, TabletRoute)>> {
        self.all_tablet_routes_with_generation()
            .map(|(_, routes)| routes)
    }

    pub fn all_tablet_routes_with_generation(&self) -> Result<(u64, Vec<(TableId, TabletRoute)>)> {
        let snapshot = self.routing_snapshot()?;
        let routes = snapshot
            .routes_by_tablet
            .iter()
            .map(|((table_id, _tablet_id), route)| {
                Ok((*table_id, self.route_with_leader_hint(route)?))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((snapshot.generation, routes))
    }

    /// Return one bounded, lexicographically ordered page of committed tablet
    /// routes. Cleaner scheduling uses this cursor instead of cloning every
    /// route into a per-pass work list.
    pub fn tablet_route_page_with_generation(
        &self,
        after: Option<(TableId, TabletId)>,
        limit: usize,
    ) -> Result<TabletRoutePage> {
        if limit == 0 {
            return Err(Error::InvalidArgument(
                "tablet route page limit must be non-zero".into(),
            ));
        }
        let snapshot = self.routing_snapshot()?;
        use std::ops::Bound::{Excluded, Unbounded};

        let candidates = match after {
            Some(cursor) => snapshot
                .routes_by_tablet
                .range((Excluded(cursor), Unbounded))
                .take(limit.saturating_add(1))
                .collect::<Vec<_>>(),
            None => snapshot
                .routes_by_tablet
                .iter()
                .take(limit.saturating_add(1))
                .collect::<Vec<_>>(),
        };
        let has_more = candidates.len() > limit;
        let routes = candidates
            .into_iter()
            .take(limit)
            .map(|((table_id, _tablet_id), route)| {
                Ok((*table_id, self.route_with_leader_hint(route)?))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((snapshot.generation, routes, has_more))
    }

    /// Return the local tablet lifecycle status when this node hosts the
    /// requested group. This is intentionally read-only: callers use the
    /// serving-leader bit to distinguish an elected Raft leader from a tablet
    /// that has completed its current-term activation boundary.
    pub fn tablet_status(&self, group_id: RaftGroupId) -> Option<ReplicatedTabletStatus> {
        self.handles
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&group_id)
            .map(|handle| handle.status())
    }

    /// Submit one command to a metadata-selected tablet route.
    pub fn submit_command(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        command: TabletCommand,
        timeout: Duration,
    ) -> Result<TabletCommandApplyOutcome> {
        self.submit_command_with_identity(route, request_id, None, command, timeout)
    }

    pub fn submit_command_with_identity(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        logical_command_id: Option<LogicalCommandId>,
        command: TabletCommand,
        timeout: Duration,
    ) -> Result<TabletCommandApplyOutcome> {
        self.submit_command_with_identity_and_ack(
            route,
            request_id,
            logical_command_id,
            None,
            command,
            timeout,
        )
    }

    pub fn submit_command_with_identity_and_ack(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        logical_command_id: Option<LogicalCommandId>,
        acknowledged_through: Option<u64>,
        command: TabletCommand,
        timeout: Duration,
    ) -> Result<TabletCommandApplyOutcome> {
        route
            .validate()
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        if request_id.raft_group_id != route.raft_group_id {
            return Err(Error::InvalidArgument(
                "tablet command request group does not match its route".to_string(),
            ));
        }
        let request = TabletCommandRequest {
            request_id,
            logical_command_id,
            acknowledged_through,
            tablet_id: route.tablet_id,
            tablet_epoch: route.tablet_epoch,
            command: command.clone(),
            deadline_remaining_ms: None,
        };
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::InvalidArgument("tablet retry deadline overflowed".into()))?;
        let mut current_route = route.clone();
        let mut last_error = None;
        let mut attempted_replicas = BTreeSet::new();

        for attempt in 0..MAX_TABLET_RETRY_ATTEMPTS {
            let (target_replica, target) =
                match next_unattempted_replica(&current_route, &attempted_replicas) {
                    Ok(target) => target,
                    Err(_) => {
                        last_error = Some(Error::LeaderUnknown);
                        continue;
                    }
                };
            attempted_replicas.insert((current_route.raft_group_id, target_replica));
            let attempt_timeout = retry_attempt_timeout(deadline, attempt);
            if attempt_timeout.is_zero() {
                break;
            }

            let mut attempt_request = TabletCommandRequest {
                request_id: request.request_id.clone(),
                logical_command_id: request.logical_command_id,
                acknowledged_through: request.acknowledged_through,
                tablet_id: current_route.tablet_id,
                tablet_epoch: current_route.tablet_epoch,
                command: request.command.clone(),
                deadline_remaining_ms: None,
            };
            // RequestId is retained for transport correlation and legacy
            // group-scoped deduplication. V2 logical identity is the durable
            // idempotency key, so a topology move may update this routing
            // field while preserving the same logical command.
            attempt_request.request_id.raft_group_id = current_route.raft_group_id;
            match self.submit_command_to(
                target,
                current_route.raft_group_id,
                attempt_request,
                attempt_timeout,
                deadline,
            ) {
                Ok(outcome) => {
                    self.record_successful_leader(&current_route, target_replica);
                    return Ok(outcome);
                }
                Err(error)
                    if is_retryable_tablet_error(&error)
                        && attempt + 1 < MAX_TABLET_RETRY_ATTEMPTS =>
                {
                    let route_refreshed = self.adjust_route_after_error(
                        &mut current_route,
                        target_replica,
                        &command,
                        logical_command_id.is_some(),
                        &error,
                    );
                    if route_refreshed {
                        // The rejecting leader may still be the correct leader
                        // after metadata returns the current epoch. Clear only
                        // attempts for the refreshed group so that leader can
                        // be retried without allowing a stale old-group
                        // target to be reused after a topology move.
                        attempted_replicas
                            .retain(|(group_id, _)| *group_id != current_route.raft_group_id);
                    }
                    last_error = Some(error);
                    let backoff = retry_backoff(
                        attempt,
                        deadline.saturating_duration_since(std::time::Instant::now()),
                    );
                    if !backoff.is_zero() {
                        thread::sleep(backoff);
                    }
                }
                Err(error) => return Err(error),
            }
        }

        Err(last_error.unwrap_or_else(|| Error::TabletUnavailable {
            reason: "tablet retry deadline elapsed".to_string(),
        }))
    }

    /// Query a retained logical mutation outcome without re-executing the
    /// command. Callers must resolve the outcome before retrying an unknown
    /// mutation; a missing outcome remains an explicit `None` result.
    pub fn query_original_outcome(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        logical_command_id: LogicalCommandId,
        timeout: Duration,
    ) -> Result<Option<CachedTabletCommandOutcome>> {
        route
            .validate()
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        if request_id.raft_group_id != route.raft_group_id {
            return Err(Error::InvalidArgument(
                "tablet outcome query request group does not match its route".to_string(),
            ));
        }

        let mut request = TabletOutcomeQueryRequest {
            request_id,
            logical_command_id,
            tablet_id: route.tablet_id,
            tablet_epoch: route.tablet_epoch,
            deadline_remaining_ms: None,
        };
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| {
                Error::InvalidArgument("tablet outcome retry deadline overflowed".into())
            })?;
        let mut current_route = route.clone();
        let mut attempted_replicas = BTreeSet::new();
        let mut last_error = None;

        for attempt in 0..MAX_TABLET_RETRY_ATTEMPTS {
            let (target_replica, target) =
                match next_unattempted_replica(&current_route, &attempted_replicas) {
                    Ok(target) => target,
                    Err(_) => {
                        last_error = Some(Error::LeaderUnknown);
                        continue;
                    }
                };
            attempted_replicas.insert((current_route.raft_group_id, target_replica));
            let attempt_timeout = retry_attempt_timeout(deadline, attempt);
            if attempt_timeout.is_zero() {
                break;
            }

            request.tablet_id = current_route.tablet_id;
            request.tablet_epoch = current_route.tablet_epoch;
            request.request_id.raft_group_id = current_route.raft_group_id;
            let result = if target == self.transport.local_node_id() {
                match self.local_handle(current_route.raft_group_id) {
                    Ok(handle) => handle
                        .query_original_outcome(request.clone(), attempt_timeout)
                        .and_then(|outcome| match outcome {
                            Some(outcome) => outcome
                                .encode_for_outcome_query()
                                .map(|result_data| Some((result_data, true)))
                                .map_err(|error| Error::CorruptData(error.to_string())),
                            None => Ok(Some((Vec::new(), false))),
                        }),
                    Err(error) => Err(error),
                }
            } else {
                request.deadline_remaining_ms = Some(forwarded_foreground_budget_millis(
                    deadline,
                    attempt_timeout,
                )?);

                self.send_remote(
                    target,
                    RpcFrame {
                        msg_type: MessageType::TabletOutcomeQueryRequest,
                        raft_group_id: current_route.raft_group_id,
                        payload: request.to_proto().encode_to_vec(),
                    },
                    request.request_id.clone(),
                    attempt_timeout,
                    false,
                )
                .and_then(|response| {
                    if !response.success {
                        return Err(response_error(response));
                    }
                    Ok(Some((response.result_data, response.found)))
                })
            };

            match result {
                Ok(Some((result_data, true))) => {
                    return CachedTabletCommandOutcome::decode_from_outcome_query(&result_data)
                        .map(Some)
                        .map_err(|error| Error::CorruptData(error.to_string()));
                }
                Ok(Some((_, false))) => return Ok(None),
                Ok(None) => return Ok(None),
                Err(error)
                    if is_retryable_tablet_error(&error)
                        && attempt + 1 < MAX_TABLET_RETRY_ATTEMPTS =>
                {
                    if let Error::StaleTabletEpoch { current_epoch, .. } = error
                        && current_epoch != 0
                    {
                        current_route.tablet_epoch = current_epoch;
                        request.tablet_epoch = current_epoch;
                    }
                    self.update_route_after_error(&mut current_route, target_replica, &error);
                    last_error = Some(error);
                    let backoff = retry_backoff(
                        attempt,
                        deadline.saturating_duration_since(std::time::Instant::now()),
                    );
                    if !backoff.is_zero() {
                        thread::sleep(backoff);
                    }
                }
                Err(error) => return Err(error),
            }
        }

        Err(last_error.unwrap_or_else(|| Error::TabletUnavailable {
            reason: "tablet outcome query retry deadline elapsed".to_string(),
        }))
    }

    /// Read one row from a metadata-selected tablet route.
    pub fn read_point(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        row_key: ragnordb_common::ids::RowKey,
        read_timestamp: ragnordb_common::ids::Timestamp,
        timeout: Duration,
    ) -> Result<Option<Vec<u8>>> {
        ensure_snapshot_not_collected(&self.metadata, read_timestamp)?;
        route
            .validate()
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        if request_id.raft_group_id != route.raft_group_id {
            return Err(Error::InvalidArgument(
                "tablet read request group does not match its route".to_string(),
            ));
        }
        let mut request = TabletReadRequest {
            request_id,
            logical_command_id: None,
            tablet_id: route.tablet_id,
            tablet_epoch: route.tablet_epoch,
            row_key,
            read_timestamp,
            deadline_remaining_ms: None,
        };
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| {
                Error::InvalidArgument("tablet read retry deadline overflowed".into())
            })?;
        let mut current_route = route.clone();
        let mut last_error = None;
        let mut attempted_replicas = BTreeSet::new();
        let mut attempt = 0_u32;

        while !deadline.saturating_duration_since(Instant::now()).is_zero() {
            let (target_replica, target) =
                match next_unattempted_replica(&current_route, &attempted_replicas) {
                    Ok(target) => target,
                    Err(_) => {
                        attempted_replicas.clear();
                        let backoff = retry_backoff(
                            attempt,
                            deadline.saturating_duration_since(Instant::now()),
                        );
                        if !backoff.is_zero() {
                            thread::sleep(backoff);
                        }
                        continue;
                    }
                };
            attempted_replicas.insert((current_route.raft_group_id, target_replica));
            let attempt_timeout = retry_attempt_timeout(
                deadline,
                attempt.min(MAX_TABLET_RETRY_ATTEMPTS.saturating_sub(1)),
            );
            if attempt_timeout.is_zero() {
                break;
            }

            request.tablet_id = current_route.tablet_id;
            request.tablet_epoch = current_route.tablet_epoch;
            request.request_id.raft_group_id = current_route.raft_group_id;
            match self.read_point_to(
                target,
                current_route.raft_group_id,
                request.clone(),
                attempt_timeout,
                deadline,
            ) {
                Ok(row) => {
                    self.record_successful_leader(&current_route, target_replica);
                    return Ok(row);
                }
                Err(error) => {
                    if !is_retryable_tablet_error(&error) {
                        return Err(error);
                    }
                    let route_refreshed =
                        if let Error::StaleTabletEpoch { current_epoch, .. } = &error {
                            if *current_epoch == 0 {
                                false
                            } else if let Ok(refreshed) = self.lookup_tablet_route(
                                request.row_key.table_id,
                                &request.row_key.primary_key_bytes,
                            ) {
                                current_route = refreshed;
                                true
                            } else {
                                current_route.tablet_epoch = *current_epoch;
                                false
                            }
                        } else {
                            false
                        };
                    if route_refreshed {
                        // A stale epoch can be returned by the current leader
                        // before the route's leader hint changes. Reusing the
                        // old group's attempted set would skip that leader on
                        // the refreshed route and turn a recoverable epoch
                        // change into a false LeaderUnknown result.
                        attempted_replicas
                            .retain(|(group_id, _)| *group_id != current_route.raft_group_id);
                    }
                    self.update_route_after_error(&mut current_route, target_replica, &error);
                    last_error = Some(error);
                    attempt = attempt.saturating_add(1);
                    let backoff = retry_backoff(
                        attempt,
                        deadline.saturating_duration_since(std::time::Instant::now()),
                    );
                    if !backoff.is_zero() {
                        thread::sleep(backoff);
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| Error::TabletUnavailable {
            reason: "tablet read retry deadline elapsed".to_string(),
        }))
    }

    /// Read the durable transaction decision from its designated primary
    /// tablet. The owner executes this through the same read barrier as point
    /// reads; absence remains `None` and is never upgraded to an abort result.
    pub fn transaction_status(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        txn_id: TxnId,
        timeout: Duration,
    ) -> Result<Option<TxnStatusRecord>> {
        route
            .validate()
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        if request_id.raft_group_id != route.raft_group_id {
            return Err(Error::InvalidArgument(
                "transaction status request group does not match its route".to_string(),
            ));
        }
        if txn_id.0 == 0 {
            return Err(Error::InvalidArgument(
                "transaction status request requires a non-zero transaction ID".to_string(),
            ));
        }
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            Error::InvalidArgument("transaction status deadline overflowed".into())
        })?;
        let mut current_route = route.clone();
        let mut request = TabletTransactionStatusRequest {
            request_id,
            tablet_id: route.tablet_id,
            tablet_epoch: route.tablet_epoch,
            txn_id,
            deadline_remaining_ms: None,
        };
        let mut attempted_replicas = BTreeSet::new();
        let mut last_error = None;
        let mut attempt = 0_u32;

        while !deadline.saturating_duration_since(Instant::now()).is_zero() {
            let (target_replica, target) =
                match next_unattempted_replica(&current_route, &attempted_replicas) {
                    Ok(target) => target,
                    Err(_) => {
                        attempted_replicas.clear();
                        let backoff = retry_backoff(
                            attempt,
                            deadline.saturating_duration_since(Instant::now()),
                        );
                        if !backoff.is_zero() {
                            thread::sleep(backoff);
                        }
                        continue;
                    }
                };
            attempted_replicas.insert((current_route.raft_group_id, target_replica));
            let attempt_timeout = retry_attempt_timeout(
                deadline,
                attempt.min(MAX_TABLET_RETRY_ATTEMPTS.saturating_sub(1)),
            );
            if attempt_timeout.is_zero() {
                break;
            }
            request.tablet_id = current_route.tablet_id;
            request.tablet_epoch = current_route.tablet_epoch;
            request.request_id.raft_group_id = current_route.raft_group_id;

            match self.transaction_status_to(
                target,
                current_route.raft_group_id,
                request.clone(),
                attempt_timeout,
                deadline,
            ) {
                Ok(status) => {
                    self.record_successful_leader(&current_route, target_replica);
                    return Ok(status);
                }
                Err(error) => {
                    if !is_retryable_tablet_error(&error) {
                        return Err(error);
                    }
                    if matches!(error, Error::StaleTabletEpoch { .. }) {
                        return Err(error);
                    }
                    self.update_route_after_error(&mut current_route, target_replica, &error);
                    last_error = Some(error);
                    attempt = attempt.saturating_add(1);
                    let backoff =
                        retry_backoff(attempt, deadline.saturating_duration_since(Instant::now()));
                    if !backoff.is_zero() {
                        thread::sleep(backoff);
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| Error::TabletUnavailable {
            reason: "transaction status read retry deadline elapsed".to_string(),
        }))
    }

    /// Inspect the snapshot-visible row and any conflicting intent together
    /// on the participant tablet. The result is read-only; terminal intent
    /// resolution must be submitted as a separate replicated command.
    pub fn inspect_point(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        row_key: RowKey,
        read_timestamp: Timestamp,
        timeout: Duration,
    ) -> Result<TabletPointReadInspection> {
        ensure_snapshot_not_collected(&self.metadata, read_timestamp)?;
        route
            .validate()
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        if request_id.raft_group_id != route.raft_group_id {
            return Err(Error::InvalidArgument(
                "point inspection request group does not match its route".to_string(),
            ));
        }
        let mut request = TabletPointInspectionRequest {
            request_id,
            tablet_id: route.tablet_id,
            tablet_epoch: route.tablet_epoch,
            row_key,
            read_timestamp,
            deadline_remaining_ms: None,
        };
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::InvalidArgument("point inspection deadline overflowed".into()))?;
        let mut current_route = route.clone();
        let mut attempted_replicas = BTreeSet::new();
        let mut last_error = None;
        let mut attempt = 0_u32;

        while !deadline.saturating_duration_since(Instant::now()).is_zero() {
            let (target_replica, target) =
                match next_unattempted_replica(&current_route, &attempted_replicas) {
                    Ok(target) => target,
                    Err(_) => {
                        attempted_replicas.clear();
                        let backoff = retry_backoff(
                            attempt,
                            deadline.saturating_duration_since(Instant::now()),
                        );
                        if !backoff.is_zero() {
                            thread::sleep(backoff);
                        }
                        continue;
                    }
                };
            attempted_replicas.insert((current_route.raft_group_id, target_replica));
            let attempt_timeout = retry_attempt_timeout(
                deadline,
                attempt.min(MAX_TABLET_RETRY_ATTEMPTS.saturating_sub(1)),
            );
            if attempt_timeout.is_zero() {
                break;
            }
            request.tablet_id = current_route.tablet_id;
            request.tablet_epoch = current_route.tablet_epoch;
            request.request_id.raft_group_id = current_route.raft_group_id;

            match self.point_inspection_to(
                target,
                current_route.raft_group_id,
                request.clone(),
                attempt_timeout,
                deadline,
            ) {
                Ok(inspection) => {
                    self.record_successful_leader(&current_route, target_replica);
                    return Ok(inspection);
                }
                Err(error) => {
                    if !is_retryable_tablet_error(&error) {
                        return Err(error);
                    }
                    let route_refreshed =
                        if let Error::StaleTabletEpoch { current_epoch, .. } = &error {
                            if *current_epoch == 0 {
                                false
                            } else if let Ok(refreshed) = self.lookup_tablet_route(
                                request.row_key.table_id,
                                &request.row_key.primary_key_bytes,
                            ) {
                                current_route = refreshed;
                                true
                            } else {
                                current_route.tablet_epoch = *current_epoch;
                                false
                            }
                        } else {
                            false
                        };
                    if route_refreshed {
                        attempted_replicas
                            .retain(|(group_id, _)| *group_id != current_route.raft_group_id);
                    }
                    self.update_route_after_error(&mut current_route, target_replica, &error);
                    last_error = Some(error);
                    attempt = attempt.saturating_add(1);
                    let backoff =
                        retry_backoff(attempt, deadline.saturating_duration_since(Instant::now()));
                    if !backoff.is_zero() {
                        thread::sleep(backoff);
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| Error::TabletUnavailable {
            reason: "point inspection retry deadline elapsed".to_string(),
        }))
    }

    /// Read one bounded scan page, retrying leader and transport failures on
    /// the same logical fragment. A stale epoch is returned to the executor so
    /// it can re-resolve the unfinished logical span; selecting one replacement
    /// tablet here would be incorrect after a split.
    #[allow(clippy::too_many_arguments)]
    pub fn scan_page(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        span: &ScanSpan,
        resume_after: Option<&[u8]>,
        read_timestamp: Timestamp,
        max_rows: u32,
        max_bytes: u32,
        timeout: Duration,
    ) -> Result<TabletScanBatch> {
        route
            .validate()
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        let request = TabletScanRequest {
            request_id,
            tablet_id: route.tablet_id,
            tablet_epoch: route.tablet_epoch,
            start_key: span.start_key.clone(),
            end_key: span.end_key.clone(),
            resume_after: resume_after.map(ToOwned::to_owned),
            read_timestamp,
            max_rows,
            max_bytes,
            rpc_attempt_id: None,
            deadline_remaining_ms: None,
            intent_only: false,
        };
        self.scan_request(route, request, timeout)
    }

    /// Enumerate a bounded page of tablet intents for the background cleaner.
    /// The distinct request mode returns lock metadata only and never reads or
    /// changes MVCC rows on behalf of the cleaner.
    #[allow(clippy::too_many_arguments)]
    pub fn scan_intents_page(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        span: &ScanSpan,
        resume_after: Option<&[u8]>,
        max_rows: u32,
        max_bytes: u32,
        timeout: Duration,
    ) -> Result<TabletScanBatch> {
        route
            .validate()
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        let request = TabletScanRequest {
            request_id,
            tablet_id: route.tablet_id,
            tablet_epoch: route.tablet_epoch,
            start_key: span.start_key.clone(),
            end_key: span.end_key.clone(),
            resume_after: resume_after.map(ToOwned::to_owned),
            read_timestamp: Timestamp(1),
            max_rows,
            max_bytes,
            rpc_attempt_id: None,
            deadline_remaining_ms: None,
            intent_only: true,
        };
        self.scan_request(route, request, timeout)
    }

    fn scan_request(
        &self,
        route: &TabletRoute,
        mut request: TabletScanRequest,
        timeout: Duration,
    ) -> Result<TabletScanBatch> {
        if !request.intent_only {
            ensure_snapshot_not_collected(&self.metadata, request.read_timestamp)?;
        }
        route
            .validate()
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        request
            .validate()
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| {
                Error::InvalidArgument("tablet scan retry deadline overflowed".into())
            })?;
        let mut current_route = route.clone();
        let mut last_error = None;
        let mut attempted_replicas = BTreeSet::new();

        for attempt in 0..MAX_TABLET_RETRY_ATTEMPTS {
            let (target_replica, target) =
                match next_unattempted_replica(&current_route, &attempted_replicas) {
                    Ok(target) => target,
                    Err(_) => {
                        last_error = Some(Error::LeaderUnknown);
                        continue;
                    }
                };
            attempted_replicas.insert((current_route.raft_group_id, target_replica));
            let attempt_timeout = retry_attempt_timeout(deadline, attempt);
            if attempt_timeout.is_zero() {
                break;
            }

            request.tablet_id = current_route.tablet_id;
            request.tablet_epoch = current_route.tablet_epoch;
            request.request_id.raft_group_id = current_route.raft_group_id;
            match self.scan_page_to(
                target,
                current_route.raft_group_id,
                request.clone(),
                attempt_timeout,
                deadline,
            ) {
                Ok(batch) => {
                    batch
                        .validate_for(&request)
                        .map_err(|error| Error::CorruptData(error.to_string()))?;
                    self.record_successful_leader(&current_route, target_replica);
                    return Ok(batch);
                }
                Err(error @ Error::StaleTabletEpoch { .. }) => return Err(error),
                Err(error)
                    if is_retryable_tablet_error(&error)
                        && attempt + 1 < MAX_TABLET_RETRY_ATTEMPTS =>
                {
                    self.update_route_after_error(&mut current_route, target_replica, &error);
                    last_error = Some(error);
                    let backoff = retry_backoff(
                        attempt,
                        deadline.saturating_duration_since(std::time::Instant::now()),
                    );
                    if !backoff.is_zero() {
                        thread::sleep(backoff);
                    }
                }
                Err(error) => return Err(error),
            }
        }

        Err(last_error.unwrap_or_else(|| Error::TabletUnavailable {
            reason: "tablet scan retry deadline elapsed".to_string(),
        }))
    }

    fn cached_leader_for(&self, route: &TabletRoute) -> Option<ReplicaId> {
        let cache = self
            .route_cache
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let cached = cache.get(route.tablet_id)?;
        (cached.raft_group_id == route.raft_group_id
            && cached.tablet_epoch == route.tablet_epoch
            && cached.replicas == route.replicas)
            .then(|| cache.leader_hint(route.tablet_id).ok().flatten())
            .flatten()
    }

    fn cache_authoritative_route(&self, route: &TabletRoute) {
        // Route publication is immutable on the request path. The read check
        // keeps the common route-cache hit off the write lock; only a new or
        // topology-changed identity enters the short publication section.
        {
            let cache = self
                .route_cache
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if cache
                .get(route.tablet_id)
                .is_some_and(|cached| same_route_identity(cached, route))
            {
                return;
            }
        }

        let mut cache = self
            .route_cache
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if cache
            .get(route.tablet_id)
            .is_some_and(|cached| same_route_identity(cached, route))
        {
            return;
        }
        let _ = cache.insert(route.clone());
    }

    fn record_successful_leader(&self, route: &TabletRoute, leader: ReplicaId) {
        let mut updated = route.clone();
        if !apply_leader_hint(&mut updated, leader) {
            return;
        }

        let mut cache = self
            .route_cache
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match cache.get(route.tablet_id).cloned() {
            Some(cached) if same_route_identity(&cached, route) => {
                let _ = cache.update_leader(route.tablet_id, leader);
            }
            Some(_) => {}
            None => {
                let _ = cache.insert(updated);
            }
        }
    }

    fn update_route_after_error(
        &self,
        route: &mut TabletRoute,
        rejected_leader: ReplicaId,
        error: &Error,
    ) {
        let mut cache = self
            .route_cache
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match error {
            Error::NotLeader {
                leader_id: Some(leader_id),
            } => {
                let leader = ReplicaId(*leader_id);
                if apply_leader_hint(route, leader) {
                    let _ = cache.update_leader_if_current(
                        route.tablet_id,
                        Some(rejected_leader),
                        Some(leader),
                    );
                } else {
                    let _ = cache.invalidate_leader(route.tablet_id, rejected_leader);
                }
            }
            Error::NotLeader { leader_id: None } | Error::LeaderUnknown => {
                let _ = cache.invalidate_leader(route.tablet_id, rejected_leader);
            }
            Error::ProposalUnavailable { .. } | Error::TabletUnavailable { .. } => {
                // A transport timeout can leave the destination's outcome
                // unknown, but it still proves that this gateway must not
                // keep selecting the failed replica as its cached leader.
                // Mutation callers resolve the original identity separately;
                // read callers can immediately use the remaining placement.
                let _ = cache.invalidate_leader(route.tablet_id, rejected_leader);
            }
            _ => {}
        }
    }

    fn adjust_route_after_error(
        &self,
        route: &mut TabletRoute,
        rejected_leader: ReplicaId,
        command: &TabletCommand,
        allow_topology_move: bool,
        error: &Error,
    ) -> bool {
        if let Error::StaleTabletEpoch { current_epoch, .. } = error
            && *current_epoch != 0
        {
            route.tablet_epoch = *current_epoch;
        }
        self.update_route_after_error(route, rejected_leader, error);
        if error_leader(error).is_some() {
            return false;
        }

        if matches!(error, Error::StaleTabletEpoch { .. })
            && let Some(refreshed) =
                self.refresh_route_for_command(route, command, allow_topology_move)
        {
            *route = refreshed;
            return true;
        }
        false
    }

    fn refresh_route_for_command(
        &self,
        route: &TabletRoute,
        command: &TabletCommand,
        allow_topology_move: bool,
    ) -> Option<TabletRoute> {
        let key = match command {
            TabletCommand::SingleShardCommit(command) => command.writes.first()?.key.as_slice(),
            TabletCommand::Prewrite(command) => command.primary_key.as_slice(),
            TabletCommand::Commit(command) => command.keys.first()?.as_slice(),
            TabletCommand::Rollback(command) => command.keys.first()?.as_slice(),
            TabletCommand::ResolveIntent(command) => command.keys.first()?.as_slice(),
            TabletCommand::PublishAbortedTransactionStatus(command) => {
                command.status_record.primary_key.as_slice()
            }
            TabletCommand::HeartbeatTransactionStatus(command) => {
                command.expected_status.primary_key.as_slice()
            }
            TabletCommand::ExpirePendingTransactionStatus(command) => {
                command.expected_status.primary_key.as_slice()
            }
            TabletCommand::Catalog(_) | TabletCommand::Noop(_) => return None,
        };
        let row_key = ragnordb_storage::key::decode_row_key(key).ok()?;
        self.lookup_tablet_route(row_key.table_id, &row_key.primary_key_bytes)
            .ok()
            .filter(|refreshed| {
                allow_topology_move || refreshed.raft_group_id == route.raft_group_id
            })
    }

    fn submit_command_to(
        &self,
        target: NodeId,
        group_id: RaftGroupId,
        mut request: TabletCommandRequest,
        timeout: Duration,
        deadline: Instant,
    ) -> Result<TabletCommandApplyOutcome> {
        if target == self.transport.local_node_id() {
            let handle = self.local_handle(group_id)?;
            let logical_command_id = request.logical_command_id;
            return match handle.submit_command(request, timeout) {
                Err(Error::ProposalUnavailable { reason }) if logical_command_id.is_some() => {
                    Err(Error::RequestOutcomeUnknown {
                        identity: format!("local tablet proposal outcome unknown: {reason}"),
                    })
                }
                result => result,
            };
        }

        request.deadline_remaining_ms =
            Some(forwarded_foreground_budget_millis(deadline, timeout)?);

        let request_id = request.request_id.clone();
        let response = self.send_remote(
            target,
            RpcFrame {
                msg_type: MessageType::TabletCommandRequest,
                raft_group_id: group_id,
                payload: request
                    .to_proto()
                    .map_err(|error| Error::InvalidArgument(error.to_string()))?
                    .encode_to_vec(),
            },
            request_id,
            timeout,
            true,
        )?;
        if !response.success {
            return Err(response_error(response));
        }
        decode_command_outcome(&response.result_data)
    }

    fn read_point_to(
        &self,
        target: NodeId,
        group_id: RaftGroupId,
        mut request: TabletReadRequest,
        timeout: Duration,
        deadline: Instant,
    ) -> Result<Option<Vec<u8>>> {
        if target == self.transport.local_node_id() {
            let handle = self.local_handle(group_id)?;
            handle.read_barrier_until(deadline)?;
            return handle.read_point_until(request, deadline);
        }

        request.deadline_remaining_ms =
            Some(forwarded_foreground_budget_millis(deadline, timeout)?);
        let request_id = request.request_id.clone();
        let response = self.send_remote(
            target,
            RpcFrame {
                msg_type: MessageType::TabletReadRequest,
                raft_group_id: group_id,
                payload: request.to_proto().encode_to_vec(),
            },
            request_id,
            timeout,
            false,
        )?;
        if !response.success {
            return Err(response_error(response));
        }
        Ok(response.found.then_some(response.result_data))
    }

    fn transaction_status_to(
        &self,
        target: NodeId,
        group_id: RaftGroupId,
        mut request: TabletTransactionStatusRequest,
        timeout: Duration,
        deadline: Instant,
    ) -> Result<Option<TxnStatusRecord>> {
        if target == self.transport.local_node_id() {
            let handle = self.local_handle(group_id)?;
            handle.read_barrier_until(deadline)?;
            return handle.transaction_status_until(request, deadline);
        }

        request.deadline_remaining_ms =
            Some(forwarded_foreground_budget_millis(deadline, timeout)?);
        let request_id = request.request_id.clone();
        let response = self.send_remote(
            target,
            RpcFrame {
                msg_type: MessageType::TabletTransactionStatusRequest,
                raft_group_id: group_id,
                payload: request.to_proto().encode_to_vec(),
            },
            request_id,
            timeout,
            false,
        )?;
        if !response.success {
            return Err(response_error(response));
        }
        if !response.found {
            if !response.result_data.is_empty() {
                return Err(Error::CorruptData(
                    "absent transaction status response contains a record".to_string(),
                ));
            }
            return Ok(None);
        }
        let status = mvcc::TxnStatusRecord::decode(response.result_data.as_slice())
            .map_err(|error| {
                Error::CorruptData(format!("invalid transaction status response: {error}"))
            })
            .and_then(|proto| {
                TxnStatusRecord::from_proto(proto)
                    .map_err(|error| Error::CorruptData(error.to_string()))
            })?;
        if status.txn_id != request.txn_id {
            return Err(Error::CorruptData(
                "transaction status response identifies a different transaction".to_string(),
            ));
        }
        Ok(Some(status))
    }

    fn point_inspection_to(
        &self,
        target: NodeId,
        group_id: RaftGroupId,
        mut request: TabletPointInspectionRequest,
        timeout: Duration,
        deadline: Instant,
    ) -> Result<TabletPointReadInspection> {
        if target == self.transport.local_node_id() {
            let handle = self.local_handle(group_id)?;
            handle.read_barrier_until(deadline)?;
            return handle.point_inspection_until(request, deadline);
        }

        request.deadline_remaining_ms =
            Some(forwarded_foreground_budget_millis(deadline, timeout)?);
        let request_id = request.request_id.clone();
        let response = self.send_remote(
            target,
            RpcFrame {
                msg_type: MessageType::TabletPointInspectionRequest,
                raft_group_id: group_id,
                payload: request.to_proto().encode_to_vec(),
            },
            request_id,
            timeout,
            false,
        )?;
        if !response.success {
            return Err(response_error(response));
        }
        if !response.found {
            return Err(Error::CorruptData(
                "point inspection response omitted its typed result".to_string(),
            ));
        }
        let proto = rpc::TabletPointInspection::decode(response.result_data.as_slice()).map_err(
            |error| Error::CorruptData(format!("invalid point inspection response: {error}")),
        )?;
        TabletPointReadInspection::from_proto(proto)
            .map_err(|error| Error::CorruptData(error.to_string()))
    }

    fn scan_page_to(
        &self,
        target: NodeId,
        group_id: RaftGroupId,
        mut request: TabletScanRequest,
        timeout: Duration,
        deadline: Instant,
    ) -> Result<TabletScanBatch> {
        if target == self.transport.local_node_id() {
            let handle = self.local_handle(group_id)?;
            handle.read_barrier_until(deadline)?;
            return handle.scan_page_until(request, deadline);
        }

        request.deadline_remaining_ms =
            Some(forwarded_foreground_budget_millis(deadline, timeout)?);
        let request_id = request.request_id.clone();
        let response = self.send_remote(
            target,
            RpcFrame {
                msg_type: MessageType::TabletScanRequest,
                raft_group_id: group_id,
                payload: request.to_proto().encode_to_vec(),
            },
            request_id,
            timeout,
            false,
        )?;
        if !response.success {
            return Err(response_error(response));
        }
        TabletScanBatch::from_proto(
            rpc::TabletScanBatch::decode(response.result_data.as_slice()).map_err(|error| {
                Error::CorruptData(format!("invalid tablet scan batch: {error}"))
            })?,
        )
        .map_err(|error| Error::CorruptData(error.to_string()))
    }

    fn local_handle(&self, group_id: RaftGroupId) -> Result<Arc<ReplicatedTabletHandle>> {
        self.handles
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&group_id)
            .cloned()
            .ok_or_else(|| Error::ProposalUnavailable {
                reason: format!("tablet Raft group {} is not hosted locally", group_id.0),
            })
    }

    fn send_remote(
        &self,
        target: NodeId,
        mut frame: RpcFrame,
        request_id: RequestId,
        timeout: Duration,
        outcome_uncertain_on_timeout: bool,
    ) -> Result<TabletCommandResponse> {
        let attempt_id = self.rpc_state.next_attempt_id()?;
        frame.payload = attach_rpc_attempt_id(frame.msg_type, &frame.payload, attempt_id)?;
        let (sender, receiver) = mpsc::channel();
        {
            let mut pending =
                self.rpc_state
                    .pending
                    .lock()
                    .map_err(|_| Error::ProposalUnavailable {
                        reason: "tablet RPC pending-response lock is poisoned".to_string(),
                    })?;
            if pending
                .insert(attempt_id, PendingResponse { target, sender })
                .is_some()
            {
                return Err(Error::ProposalUnavailable {
                    reason: "RPC attempt identity is already pending".to_string(),
                });
            }
        }

        if let Err(error) = self.transport.try_send_rpc(target, frame) {
            self.rpc_state
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&attempt_id);
            return Err(Error::ProposalUnavailable {
                reason: format!("tablet RPC could not be queued: {error}"),
            });
        }

        match receiver.recv_timeout(timeout) {
            Ok(PendingResponseMessage::Tablet(response)) => Ok(response),
            Ok(PendingResponseMessage::Metadata(_)) => Err(Error::CorruptData(
                "tablet RPC waiter received a metadata response".to_string(),
            )),
            Ok(PendingResponseMessage::ReplicaJoin(_)) => Err(Error::CorruptData(
                "tablet RPC waiter received a replica-join response".to_string(),
            )),
            Err(_) => {
                self.rpc_state
                    .pending
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&attempt_id);
                if outcome_uncertain_on_timeout {
                    Err(Error::RequestOutcomeUnknown {
                        identity: format!(
                            "client={:#034x}/group={}/sequence={}",
                            request_id.client_id, request_id.raft_group_id.0, request_id.sequence
                        ),
                    })
                } else {
                    Err(Error::ProposalUnavailable {
                        reason: "tablet RPC response deadline elapsed".to_string(),
                    })
                }
            }
        }
    }
}

fn build_routing_snapshot(generation: u64, state: &MetadataState) -> Result<RoutingSnapshot> {
    let mut tables = BTreeMap::new();
    let mut global_routes_by_tablet = BTreeMap::new();
    for schema in state.list_tables() {
        let table_id = schema.id;
        let descriptors = state.tablets_for_table(table_id);
        let router = TabletRouter::new(table_id, &descriptors).map_err(|error| {
            Error::CorruptData(format!(
                "metadata route for table {} is invalid: {error}",
                table_id.0
            ))
        })?;

        let mut routes_by_tablet = BTreeMap::new();
        for descriptor in descriptors {
            let placement = state
                .desired_placement(descriptor.tablet_id)
                .ok_or_else(|| {
                    Error::CorruptData(format!(
                        "tablet {} has no desired placement",
                        descriptor.tablet_id.0
                    ))
                })?;
            let replicas = placement
                .replicas
                .iter()
                .map(|replica| ReplicaRoute {
                    replica_id: replica.replica_id,
                    node_id: replica.node_id,
                })
                .collect::<Vec<_>>();
            let leader_replica_id = placement
                .replicas
                .iter()
                .find(|replica| replica.role == DesiredReplicaRole::Voter)
                .or_else(|| placement.replicas.first())
                .map(|replica| replica.replica_id)
                .ok_or_else(|| {
                    Error::CorruptData("tablet placement has no replicas".to_string())
                })?;
            let route = TabletRoute {
                raft_group_id: descriptor.raft_group_id,
                tablet_id: descriptor.tablet_id,
                tablet_epoch: descriptor.tablet_epoch,
                leader_replica_id,
                replicas: replicas.into(),
            };
            route
                .validate()
                .map_err(|error| Error::CorruptData(error.to_string()))?;
            if routes_by_tablet
                .insert(descriptor.tablet_id, route.clone())
                .is_some()
            {
                return Err(Error::CorruptData(format!(
                    "metadata contains duplicate tablet {} in table {}",
                    descriptor.tablet_id.0, table_id.0
                )));
            }
            if global_routes_by_tablet
                .insert((table_id, descriptor.tablet_id), route)
                .is_some()
            {
                return Err(Error::CorruptData(format!(
                    "metadata contains duplicate route for tablet {} in table {}",
                    descriptor.tablet_id.0, table_id.0
                )));
            }
        }

        tables.insert(
            table_id,
            TableRoutingSnapshot {
                router: Arc::new(router),
                routes_by_tablet,
            },
        );
    }

    Ok(RoutingSnapshot {
        generation,
        tables,
        routes_by_tablet: global_routes_by_tablet,
    })
}

impl TabletGateway for TabletRpcClient {
    fn lookup_tablet_route(&self, table_id: TableId, key: &[u8]) -> Result<TabletRoute> {
        TabletRpcClient::lookup_tablet_route(self, table_id, key)
    }

    fn read_point(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        row_key: RowKey,
        read_timestamp: Timestamp,
        timeout: Duration,
    ) -> Result<Option<Vec<u8>>> {
        TabletRpcClient::read_point(self, route, request_id, row_key, read_timestamp, timeout)
    }

    fn inspect_point(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        row_key: RowKey,
        read_timestamp: Timestamp,
        timeout: Duration,
    ) -> Result<TabletPointReadInspection> {
        TabletRpcClient::inspect_point(self, route, request_id, row_key, read_timestamp, timeout)
    }

    fn transaction_status(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        txn_id: TxnId,
        timeout: Duration,
    ) -> Result<Option<TxnStatusRecord>> {
        TabletRpcClient::transaction_status(self, route, request_id, txn_id, timeout)
    }

    fn lookup_scan_routes(
        &self,
        table_id: TableId,
        span: &ScanSpan,
    ) -> Result<Vec<TabletScanRoute>> {
        TabletRpcClient::lookup_scan_routes(self, table_id, span)
    }

    fn scan_page(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        span: &ScanSpan,
        resume_after: Option<&[u8]>,
        read_timestamp: Timestamp,
        max_rows: u32,
        max_bytes: u32,
        timeout: Duration,
    ) -> Result<TabletScanBatch> {
        TabletRpcClient::scan_page(
            self,
            route,
            request_id,
            span,
            resume_after,
            read_timestamp,
            max_rows,
            max_bytes,
            timeout,
        )
    }

    fn submit_command(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        command: TabletCommand,
        timeout: Duration,
    ) -> Result<TabletCommandApplyOutcome> {
        TabletRpcClient::submit_command(self, route, request_id, command, timeout)
    }

    fn submit_command_with_identity(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        logical_command_id: LogicalCommandId,
        command: TabletCommand,
        timeout: Duration,
    ) -> Result<TabletCommandApplyOutcome> {
        TabletRpcClient::submit_command_with_identity(
            self,
            route,
            request_id,
            Some(logical_command_id),
            command,
            timeout,
        )
    }

    fn submit_command_with_identity_and_ack(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        logical_command_id: LogicalCommandId,
        acknowledged_through: Option<u64>,
        command: TabletCommand,
        timeout: Duration,
    ) -> Result<TabletCommandApplyOutcome> {
        TabletRpcClient::submit_command_with_identity_and_ack(
            self,
            route,
            request_id,
            Some(logical_command_id),
            acknowledged_through,
            command,
            timeout,
        )
    }

    fn query_original_outcome(
        &self,
        route: &TabletRoute,
        request_id: RequestId,
        logical_command_id: LogicalCommandId,
        timeout: Duration,
    ) -> Result<Option<CachedTabletCommandOutcome>> {
        TabletRpcClient::query_original_outcome(
            self,
            route,
            request_id,
            logical_command_id,
            timeout,
        )
    }
}

/// Spawn the node-level dispatcher that services tablet request/response
/// frames. It is intentionally independent from the MultiRaft host loop: a
/// slow SQL request cannot consume the host's Raft turn budget, while the
/// bounded transport queue still provides admission backpressure.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_dispatcher(
    inbound: NodeRpcInbound,
    transport: NodeRaftTransport,
    rpc_state: RpcState,
    handles: SharedTabletHandleRegistry,
    metadata: MetadataRuntimeHandle,
    metadata_requests: mpsc::SyncSender<MetadataHostRequest>,
    join_requests: mpsc::SyncSender<ReplicaJoinAdmission>,
    host_wake: HostWake,
    shutdown: Arc<AtomicBool>,
) -> (TabletRpcClient, thread::JoinHandle<()>) {
    let dispatch_metadata = metadata.clone();
    let client = TabletRpcClient::new(
        transport.clone(),
        handles.clone(),
        rpc_state.clone(),
        metadata,
    );
    let worker = thread::Builder::new()
        .name("ragnordb-tablet-rpc".to_string())
        .spawn(move || {
            inbound.bind_current_thread();
            rpc_state.bind_dispatcher();
            while !shutdown.load(Ordering::Acquire) {
                let mut made_progress = false;
                drain_control_rpc_completions(&transport, &rpc_state);
                drain_tablet_rpc_completions(&transport, &rpc_state);

                for _ in 0..RPC_DISPATCH_MESSAGE_BUDGET {
                    let Ok(message) = inbound.try_recv() else {
                        break;
                    };
                    made_progress = true;
                    dispatch_message(
                        &transport,
                        &handles,
                        &rpc_state,
                        &metadata_requests,
                        &join_requests,
                        &host_wake,
                        &dispatch_metadata,
                        message.source_node_id,
                        message.frame,
                    );
                    drain_control_rpc_completions(&transport, &rpc_state);
                    drain_tablet_rpc_completions(&transport, &rpc_state);
                }

                if !made_progress {
                    // Both the physical RPC queue and the reactor completion
                    // registry are event sources. The transport queue has no
                    // shared select primitive, so the dispatcher parks for a
                    // short bounded interval; reactor completion publication
                    // unparks it immediately and avoids response HOL waiting.
                    thread::park_timeout(INBOUND_TABLET_RPC_POLL_INTERVAL);
                }
            }
            drain_control_rpc_completions(&transport, &rpc_state);
            drain_tablet_rpc_completions(&transport, &rpc_state);
        })
        .expect("tablet RPC dispatcher thread creation must succeed");
    (client, worker)
}

fn drain_control_rpc_completions(transport: &NodeRaftTransport, rpc_state: &RpcState) {
    let now = Instant::now();
    let mut pending = {
        let mut requests = rpc_state
            .pending_control
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        std::mem::take(&mut *requests)
    };
    let mut retained = Vec::with_capacity(pending.len());
    let mut drained = 0;

    for request in pending.drain(..) {
        if drained >= CONTROL_RPC_DRAIN_BUDGET {
            retained.push(request);
            continue;
        }

        match request {
            PendingControlRpc::ReplicaJoin {
                source,
                group_id,
                attempt_id,
                deadline,
                response,
            } => match response.try_recv() {
                Ok(result) => {
                    send_replica_join_response(
                        transport,
                        source,
                        group_id,
                        attempt_id,
                        result.success,
                        result.error_message,
                    );
                    drained += 1;
                }
                Err(mpsc::TryRecvError::Empty) if deadline > now => {
                    retained.push(PendingControlRpc::ReplicaJoin {
                        source,
                        group_id,
                        attempt_id,
                        deadline,
                        response,
                    });
                }
                Err(mpsc::TryRecvError::Empty) => {
                    send_replica_join_response(
                        transport,
                        source,
                        group_id,
                        attempt_id,
                        false,
                        "replica join admission deadline elapsed".to_string(),
                    );
                    drained += 1;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    send_replica_join_response(
                        transport,
                        source,
                        group_id,
                        attempt_id,
                        false,
                        "replica join admission owner disconnected".to_string(),
                    );
                    drained += 1;
                }
            },
            PendingControlRpc::MetadataCommand {
                source,
                group_id,
                attempt_id,
                request_id,
                deadline,
                response,
            } => match response.try_recv() {
                Ok(result) => {
                    send_metadata_response(
                        transport, source, group_id, attempt_id, request_id, result,
                    );
                    drained += 1;
                }
                Err(mpsc::TryRecvError::Empty) if deadline > now => {
                    retained.push(PendingControlRpc::MetadataCommand {
                        source,
                        group_id,
                        attempt_id,
                        request_id,
                        deadline,
                        response,
                    });
                }
                Err(mpsc::TryRecvError::Empty) => {
                    send_metadata_response(
                        transport,
                        source,
                        group_id,
                        attempt_id,
                        request_id,
                        Err(Error::ProposalUnavailable {
                            reason: "metadata proposal forwarding deadline elapsed".to_string(),
                        }),
                    );
                    drained += 1;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    send_metadata_response(
                        transport,
                        source,
                        group_id,
                        attempt_id,
                        request_id,
                        Err(Error::ProposalUnavailable {
                            reason: "metadata proposal owner disconnected".to_string(),
                        }),
                    );
                    drained += 1;
                }
            },
            PendingControlRpc::MetadataConfChange {
                source,
                group_id,
                attempt_id,
                deadline,
                response,
            } => match response.try_recv() {
                Ok(result) => {
                    send_metadata_conf_change_response(
                        transport, source, group_id, attempt_id, result,
                    );
                    drained += 1;
                }
                Err(mpsc::TryRecvError::Empty) if deadline > now => {
                    retained.push(PendingControlRpc::MetadataConfChange {
                        source,
                        group_id,
                        attempt_id,
                        deadline,
                        response,
                    });
                }
                Err(mpsc::TryRecvError::Empty) => {
                    send_metadata_conf_change_response(
                        transport,
                        source,
                        group_id,
                        attempt_id,
                        Err(Error::ProposalUnavailable {
                            reason: "metadata ConfChange forwarding deadline elapsed".to_string(),
                        }),
                    );
                    drained += 1;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    send_metadata_conf_change_response(
                        transport,
                        source,
                        group_id,
                        attempt_id,
                        Err(Error::ProposalUnavailable {
                            reason: "metadata ConfChange owner disconnected".to_string(),
                        }),
                    );
                    drained += 1;
                }
            },
        }
    }

    let mut requests = rpc_state
        .pending_control
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    requests.extend(retained);
}

fn drain_tablet_rpc_completions(transport: &NodeRaftTransport, rpc_state: &RpcState) {
    let now = Instant::now();
    for mut pending in rpc_state.take_ready_tablet_rpcs(now) {
        if pending.deadline <= now {
            let error = tablet_rpc_deadline_error(&pending);
            send_response(
                transport,
                pending.source,
                pending.group_id,
                pending.attempt_id,
                error_response(pending.request_id, error),
            );
            continue;
        }

        let Some(completion) = pending.completion.take() else {
            continue;
        };
        let response = tablet_rpc_response(&pending, completion);
        send_response(
            transport,
            pending.source,
            pending.group_id,
            pending.attempt_id,
            response,
        );
    }
}

fn tablet_rpc_deadline_error(pending: &PendingTabletRpc) -> Error {
    match pending.kind {
        PendingTabletRpcKind::Mutation => Error::RequestOutcomeUnknown {
            identity: format!(
                "client={:#034x}/group={}/sequence={}",
                pending.request_id.client_id,
                pending.request_id.raft_group_id.0,
                pending.request_id.sequence,
            ),
        },
        PendingTabletRpcKind::ReadOnly => Error::ProposalUnavailable {
            reason: "tablet RPC deadline elapsed before completion".to_string(),
        },
    }
}

fn tablet_rpc_response(
    pending: &PendingTabletRpc,
    completion: TabletRpcCompletion,
) -> TabletCommandResponse {
    let leader_replica_id = pending
        .status
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .leader_replica_id
        .map(ReplicaId);
    match completion {
        TabletRpcCompletion::Command(result) => match result {
            Ok(outcome) => TabletCommandResponse {
                request_id: pending.request_id.clone(),
                success: true,
                error_message: String::new(),
                error_code: String::new(),
                retryable: false,
                result_data: encode_command_outcome(outcome),
                found: false,
                leader_replica_id,
                current_tablet_epoch: None,
                expected_tablet_epoch: None,
            },
            Err(error) => error_response(pending.request_id.clone(), error),
        },
        TabletRpcCompletion::Read(result) => match result {
            Ok(row) => TabletCommandResponse {
                request_id: pending.request_id.clone(),
                success: true,
                error_message: String::new(),
                error_code: String::new(),
                retryable: false,
                found: row.is_some(),
                result_data: row.unwrap_or_default(),
                leader_replica_id,
                current_tablet_epoch: None,
                expected_tablet_epoch: None,
            },
            Err(error) => error_response(pending.request_id.clone(), error),
        },
        TabletRpcCompletion::TransactionStatus(result) => match result {
            Ok(Some(status)) => match status.to_proto() {
                Ok(proto) => TabletCommandResponse {
                    request_id: pending.request_id.clone(),
                    success: true,
                    error_message: String::new(),
                    error_code: String::new(),
                    retryable: false,
                    result_data: proto.encode_to_vec(),
                    found: true,
                    leader_replica_id,
                    current_tablet_epoch: None,
                    expected_tablet_epoch: None,
                },
                Err(error) => error_response(
                    pending.request_id.clone(),
                    Error::CorruptData(error.to_string()),
                ),
            },
            Ok(None) => TabletCommandResponse {
                request_id: pending.request_id.clone(),
                success: true,
                error_message: String::new(),
                error_code: String::new(),
                retryable: false,
                result_data: Vec::new(),
                found: false,
                leader_replica_id,
                current_tablet_epoch: None,
                expected_tablet_epoch: None,
            },
            Err(error) => error_response(pending.request_id.clone(), error),
        },
        TabletRpcCompletion::PointInspection(result) => match result {
            Ok(inspection) => match inspection.to_proto() {
                Ok(proto) => TabletCommandResponse {
                    request_id: pending.request_id.clone(),
                    success: true,
                    error_message: String::new(),
                    error_code: String::new(),
                    retryable: false,
                    result_data: proto.encode_to_vec(),
                    found: true,
                    leader_replica_id,
                    current_tablet_epoch: None,
                    expected_tablet_epoch: None,
                },
                Err(error) => error_response(
                    pending.request_id.clone(),
                    Error::CorruptData(error.to_string()),
                ),
            },
            Err(error) => error_response(pending.request_id.clone(), error),
        },
        TabletRpcCompletion::Scan(result) => match result {
            Ok(batch) => match batch.to_proto() {
                Ok(batch) => TabletCommandResponse {
                    request_id: pending.request_id.clone(),
                    success: true,
                    error_message: String::new(),
                    error_code: String::new(),
                    retryable: false,
                    found: !batch.rows.is_empty() || !batch.intents.is_empty(),
                    result_data: batch.encode_to_vec(),
                    leader_replica_id,
                    current_tablet_epoch: None,
                    expected_tablet_epoch: None,
                },
                Err(error) => error_response(
                    pending.request_id.clone(),
                    Error::CorruptData(format!("tablet scan response cannot be encoded: {error}")),
                ),
            },
            Err(error) => error_response(pending.request_id.clone(), error),
        },
        TabletRpcCompletion::Outcome(result) => match result {
            Ok(Some(outcome)) => match outcome.encode_for_outcome_query() {
                Ok(result_data) => TabletCommandResponse {
                    request_id: pending.request_id.clone(),
                    success: true,
                    error_message: String::new(),
                    error_code: String::new(),
                    retryable: false,
                    result_data,
                    found: true,
                    leader_replica_id,
                    current_tablet_epoch: None,
                    expected_tablet_epoch: None,
                },
                Err(error) => error_response(
                    pending.request_id.clone(),
                    Error::CorruptData(error.to_string()),
                ),
            },
            Ok(None) => TabletCommandResponse {
                request_id: pending.request_id.clone(),
                success: true,
                error_message: String::new(),
                error_code: String::new(),
                retryable: false,
                result_data: Vec::new(),
                found: false,
                leader_replica_id,
                current_tablet_epoch: None,
                expected_tablet_epoch: None,
            },
            Err(error) => error_response(pending.request_id.clone(), error),
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch_message(
    transport: &NodeRaftTransport,
    handles: &SharedTabletHandleRegistry,
    rpc_state: &RpcState,
    metadata_requests: &mpsc::SyncSender<MetadataHostRequest>,
    join_requests: &mpsc::SyncSender<ReplicaJoinAdmission>,
    host_wake: &HostWake,
    metadata: &MetadataRuntimeHandle,
    source: NodeId,
    frame: RpcFrame,
) {
    match frame.msg_type {
        MessageType::ReplicaJoinRequest => {
            let Ok(request) = rpc::ReplicaJoinRequest::decode(frame.payload.as_slice()) else {
                return;
            };
            let Some(attempt_id) = request.rpc_attempt_id else {
                return;
            };
            let (reply, response) = mpsc::channel();
            let group_id = frame.raft_group_id;
            if let Err(error) = rpc_state.reserve_control_rpc(PendingControlRpc::ReplicaJoin {
                source,
                group_id,
                attempt_id,
                deadline: Instant::now() + CONTROL_RPC_DEFAULT_TIMEOUT,
                response,
            }) {
                send_replica_join_response(
                    transport,
                    source,
                    group_id,
                    attempt_id,
                    false,
                    error.to_string(),
                );
                return;
            }

            // The lifecycle owner may wait on WAL/database ownership and must
            // not block this dispatcher: doing so would let one slow join
            // starve unrelated tablet and metadata RPCs. The admission queue
            // is bounded; a full queue is rejected synchronously above.
            if join_requests
                .try_send(ReplicaJoinAdmission {
                    source_node_id: source,
                    request,
                    reply,
                })
                .is_err()
            {
                rpc_state.cancel_control_rpc(attempt_id);
                send_replica_join_response(
                    transport,
                    source,
                    group_id,
                    attempt_id,
                    false,
                    "replica join lifecycle owner is unavailable".to_string(),
                );
                return;
            }
            host_wake.wake();
        }
        MessageType::TabletCommandRequest => {
            let Ok(proto) = rpc::TabletCommandRequest::decode(frame.payload.as_slice()) else {
                return;
            };
            let attempt_id = proto.rpc_attempt_id;
            let Ok(request) = TabletCommandRequest::from_proto(proto) else {
                return;
            };
            let request_id = request.request_id.clone();
            if request_id.raft_group_id != frame.raft_group_id {
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(
                        request_id,
                        Error::InvalidArgument(
                            "tablet command request group does not match its frame".to_string(),
                        ),
                    ),
                );
                return;
            }
            let deadline = match remote_foreground_deadline(request.deadline_remaining_ms) {
                Ok(deadline) => deadline,
                Err(error) => {
                    send_response(
                        transport,
                        source,
                        frame.raft_group_id,
                        attempt_id,
                        error_response(request_id, error),
                    );
                    return;
                }
            };
            let Some(handle) = handles
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&frame.raft_group_id)
                .cloned()
            else {
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(
                        request_id,
                        Error::ProposalUnavailable {
                            reason: "tablet Raft group is not hosted on this node".to_string(),
                        },
                    ),
                );
                return;
            };
            let token = match rpc_state.reserve_tablet_rpc(PendingTabletRpc {
                source,
                group_id: frame.raft_group_id,
                attempt_id,
                request_id: request_id.clone(),
                status: handle.status_shared(),
                deadline,
                completion: None,
                kind: PendingTabletRpcKind::Mutation,
            }) {
                Ok(token) => token,
                Err(error) => {
                    send_response(
                        transport,
                        source,
                        frame.raft_group_id,
                        attempt_id,
                        error_response(request_id, error),
                    );
                    return;
                }
            };
            let completion: Arc<dyn TabletRpcCompletionSink> = Arc::new(rpc_state.clone());
            if let Err(error) = handle.enqueue_rpc_command(request, deadline, token, completion) {
                rpc_state.cancel_tablet_rpc(token);
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(request_id, error),
                );
            }
        }
        MessageType::TabletReadRequest => {
            let Ok(proto) = rpc::TabletReadRequest::decode(frame.payload.as_slice()) else {
                return;
            };
            let attempt_id = proto.rpc_attempt_id;
            let Ok(request) = TabletReadRequest::from_proto(proto) else {
                return;
            };
            let request_id = request.request_id.clone();
            if request_id.raft_group_id != frame.raft_group_id {
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(
                        request_id,
                        Error::InvalidArgument(
                            "tablet read request group does not match its frame".to_string(),
                        ),
                    ),
                );
                return;
            }
            if let Err(error) = ensure_snapshot_not_collected(metadata, request.read_timestamp) {
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(request_id, error),
                );
                return;
            }
            let deadline = match remote_foreground_deadline(request.deadline_remaining_ms) {
                Ok(deadline) => deadline,
                Err(error) => {
                    send_response(
                        transport,
                        source,
                        frame.raft_group_id,
                        attempt_id,
                        error_response(request_id, error),
                    );
                    return;
                }
            };
            let Some(handle) = handles
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&frame.raft_group_id)
                .cloned()
            else {
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(
                        request_id,
                        Error::ProposalUnavailable {
                            reason: "tablet Raft group is not hosted on this node".to_string(),
                        },
                    ),
                );
                return;
            };
            let token = match rpc_state.reserve_tablet_rpc(PendingTabletRpc {
                source,
                group_id: frame.raft_group_id,
                attempt_id,
                request_id: request_id.clone(),
                status: handle.status_shared(),
                deadline,
                completion: None,
                kind: PendingTabletRpcKind::ReadOnly,
            }) {
                Ok(token) => token,
                Err(error) => {
                    send_response(
                        transport,
                        source,
                        frame.raft_group_id,
                        attempt_id,
                        error_response(request_id, error),
                    );
                    return;
                }
            };
            let completion: Arc<dyn TabletRpcCompletionSink> = Arc::new(rpc_state.clone());
            if let Err(error) = handle.enqueue_rpc_read_point(request, deadline, token, completion)
            {
                rpc_state.cancel_tablet_rpc(token);
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(request_id, error),
                );
            }
        }
        MessageType::TabletTransactionStatusRequest => {
            let Ok(proto) = rpc::TabletTransactionStatusRequest::decode(frame.payload.as_slice())
            else {
                return;
            };
            let attempt_id = proto.rpc_attempt_id;
            let Ok(request) = TabletTransactionStatusRequest::from_proto(proto) else {
                return;
            };
            let request_id = request.request_id.clone();
            if request_id.raft_group_id != frame.raft_group_id {
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(
                        request_id,
                        Error::InvalidArgument(
                            "transaction status request group does not match its frame".to_string(),
                        ),
                    ),
                );
                return;
            }
            let deadline = match remote_foreground_deadline(request.deadline_remaining_ms) {
                Ok(deadline) => deadline,
                Err(error) => {
                    send_response(
                        transport,
                        source,
                        frame.raft_group_id,
                        attempt_id,
                        error_response(request_id, error),
                    );
                    return;
                }
            };
            let Some(handle) = handles
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&frame.raft_group_id)
                .cloned()
            else {
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(
                        request_id,
                        Error::ProposalUnavailable {
                            reason: "tablet Raft group is not hosted on this node".to_string(),
                        },
                    ),
                );
                return;
            };
            let token = match rpc_state.reserve_tablet_rpc(PendingTabletRpc {
                source,
                group_id: frame.raft_group_id,
                attempt_id,
                request_id: request_id.clone(),
                status: handle.status_shared(),
                deadline,
                completion: None,
                kind: PendingTabletRpcKind::ReadOnly,
            }) {
                Ok(token) => token,
                Err(error) => {
                    send_response(
                        transport,
                        source,
                        frame.raft_group_id,
                        attempt_id,
                        error_response(request_id, error),
                    );
                    return;
                }
            };
            let completion: Arc<dyn TabletRpcCompletionSink> = Arc::new(rpc_state.clone());
            if let Err(error) =
                handle.enqueue_rpc_transaction_status(request, deadline, token, completion)
            {
                rpc_state.cancel_tablet_rpc(token);
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(request_id, error),
                );
            }
        }
        MessageType::TabletPointInspectionRequest => {
            let Ok(proto) = rpc::TabletPointInspectionRequest::decode(frame.payload.as_slice())
            else {
                return;
            };
            let attempt_id = proto.rpc_attempt_id;
            let Ok(request) = TabletPointInspectionRequest::from_proto(proto) else {
                return;
            };
            let request_id = request.request_id.clone();
            if request_id.raft_group_id != frame.raft_group_id {
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(
                        request_id,
                        Error::InvalidArgument(
                            "point inspection request group does not match its frame".to_string(),
                        ),
                    ),
                );
                return;
            }
            if let Err(error) = ensure_snapshot_not_collected(metadata, request.read_timestamp) {
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(request_id, error),
                );
                return;
            }
            let deadline = match remote_foreground_deadline(request.deadline_remaining_ms) {
                Ok(deadline) => deadline,
                Err(error) => {
                    send_response(
                        transport,
                        source,
                        frame.raft_group_id,
                        attempt_id,
                        error_response(request_id, error),
                    );
                    return;
                }
            };
            let Some(handle) = handles
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&frame.raft_group_id)
                .cloned()
            else {
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(
                        request_id,
                        Error::ProposalUnavailable {
                            reason: "tablet Raft group is not hosted on this node".to_string(),
                        },
                    ),
                );
                return;
            };
            let token = match rpc_state.reserve_tablet_rpc(PendingTabletRpc {
                source,
                group_id: frame.raft_group_id,
                attempt_id,
                request_id: request_id.clone(),
                status: handle.status_shared(),
                deadline,
                completion: None,
                kind: PendingTabletRpcKind::ReadOnly,
            }) {
                Ok(token) => token,
                Err(error) => {
                    send_response(
                        transport,
                        source,
                        frame.raft_group_id,
                        attempt_id,
                        error_response(request_id, error),
                    );
                    return;
                }
            };
            let completion: Arc<dyn TabletRpcCompletionSink> = Arc::new(rpc_state.clone());
            if let Err(error) =
                handle.enqueue_rpc_point_inspection(request, deadline, token, completion)
            {
                rpc_state.cancel_tablet_rpc(token);
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(request_id, error),
                );
            }
        }
        MessageType::TabletScanRequest => {
            let Ok(proto) = rpc::TabletScanRequest::decode(frame.payload.as_slice()) else {
                return;
            };
            let attempt_id = proto.rpc_attempt_id;
            let Ok(request) = TabletScanRequest::from_proto(proto) else {
                return;
            };
            let request_id = request.request_id.clone();
            if request_id.raft_group_id != frame.raft_group_id {
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(
                        request_id,
                        Error::InvalidArgument(
                            "tablet scan request group does not match its frame".to_string(),
                        ),
                    ),
                );
                return;
            }
            if !request.intent_only
                && let Err(error) = ensure_snapshot_not_collected(metadata, request.read_timestamp)
            {
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(request_id, error),
                );
                return;
            }
            let deadline = match remote_foreground_deadline(request.deadline_remaining_ms) {
                Ok(deadline) => deadline,
                Err(error) => {
                    send_response(
                        transport,
                        source,
                        frame.raft_group_id,
                        attempt_id,
                        error_response(request_id, error),
                    );
                    return;
                }
            };
            let Some(handle) = handles
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&frame.raft_group_id)
                .cloned()
            else {
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(
                        request_id,
                        Error::ProposalUnavailable {
                            reason: "tablet Raft group is not hosted on this node".to_string(),
                        },
                    ),
                );
                return;
            };
            let token = match rpc_state.reserve_tablet_rpc(PendingTabletRpc {
                source,
                group_id: frame.raft_group_id,
                attempt_id,
                request_id: request_id.clone(),
                status: handle.status_shared(),
                deadline,
                completion: None,
                kind: PendingTabletRpcKind::ReadOnly,
            }) {
                Ok(token) => token,
                Err(error) => {
                    send_response(
                        transport,
                        source,
                        frame.raft_group_id,
                        attempt_id,
                        error_response(request_id, error),
                    );
                    return;
                }
            };
            let completion: Arc<dyn TabletRpcCompletionSink> = Arc::new(rpc_state.clone());
            if let Err(error) = handle.enqueue_rpc_scan(request, deadline, token, completion) {
                rpc_state.cancel_tablet_rpc(token);
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(request_id, error),
                );
            }
        }
        MessageType::TabletOutcomeQueryRequest => {
            let Ok(proto) = rpc::TabletOutcomeQueryRequest::decode(frame.payload.as_slice()) else {
                return;
            };
            let attempt_id = proto.rpc_attempt_id;
            let Ok(request) = TabletOutcomeQueryRequest::from_proto(proto) else {
                return;
            };
            let request_id = request.request_id.clone();
            if request_id.raft_group_id != frame.raft_group_id {
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(
                        request_id,
                        Error::InvalidArgument(
                            "tablet outcome query group does not match its frame".to_string(),
                        ),
                    ),
                );
                return;
            }
            let deadline = match remote_foreground_deadline(request.deadline_remaining_ms) {
                Ok(deadline) => deadline,
                Err(error) => {
                    send_response(
                        transport,
                        source,
                        frame.raft_group_id,
                        attempt_id,
                        error_response(request_id, error),
                    );
                    return;
                }
            };
            let Some(handle) = handles
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&frame.raft_group_id)
                .cloned()
            else {
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(
                        request_id,
                        Error::ProposalUnavailable {
                            reason: "tablet Raft group is not hosted on this node".to_string(),
                        },
                    ),
                );
                return;
            };
            let token = match rpc_state.reserve_tablet_rpc(PendingTabletRpc {
                source,
                group_id: frame.raft_group_id,
                attempt_id,
                request_id: request_id.clone(),
                status: handle.status_shared(),
                deadline,
                completion: None,
                kind: PendingTabletRpcKind::ReadOnly,
            }) {
                Ok(token) => token,
                Err(error) => {
                    send_response(
                        transport,
                        source,
                        frame.raft_group_id,
                        attempt_id,
                        error_response(request_id, error),
                    );
                    return;
                }
            };
            let completion: Arc<dyn TabletRpcCompletionSink> = Arc::new(rpc_state.clone());
            if let Err(error) =
                handle.enqueue_rpc_outcome_query(request, deadline, token, completion)
            {
                rpc_state.cancel_tablet_rpc(token);
                send_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    error_response(request_id, error),
                );
            }
        }
        MessageType::TabletCommandResponse => {
            let Ok(proto) = rpc::TabletCommandResponse::decode(frame.payload.as_slice()) else {
                return;
            };
            let Some(attempt_id) = proto.rpc_attempt_id else {
                return;
            };
            let Ok(response) = TabletCommandResponse::from_proto(proto) else {
                return;
            };
            let mut pending_guard = rpc_state
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let pending_response = pending_guard
                .get(&attempt_id)
                .is_some_and(|pending| pending.target == source)
                .then(|| pending_guard.remove(&attempt_id))
                .flatten();
            if let Some(pending_response) = pending_response {
                let _ = pending_response
                    .sender
                    .send(PendingResponseMessage::Tablet(response));
            }
        }
        MessageType::MetadataRequest => {
            let Ok(proto) = rpc::MetadataRequest::decode(frame.payload.as_slice()) else {
                return;
            };
            if let Some(rpc::metadata_request::Request::ProposeConfChange(request)) =
                proto.request.as_ref()
            {
                let Some(attempt_id) = request.rpc_attempt_id else {
                    return;
                };
                let Some(replica_id) = request.replica_id.as_ref() else {
                    send_metadata_conf_change_response(
                        transport,
                        source,
                        frame.raft_group_id,
                        attempt_id,
                        Err(Error::InvalidArgument(
                            "metadata ConfChange request omitted replica_id".to_string(),
                        )),
                    );
                    return;
                };
                let replica_id = ReplicaId::from_proto(*replica_id);
                if !request.remove_replica
                    || request.expected_conf_state_version == 0
                    || replica_id.0 == 0
                {
                    send_metadata_conf_change_response(
                        transport,
                        source,
                        frame.raft_group_id,
                        attempt_id,
                        Err(Error::InvalidArgument(
                            "metadata ConfChange request is invalid".to_string(),
                        )),
                    );
                    return;
                }
                let group_id = frame.raft_group_id;
                let (reply, response) = mpsc::channel();
                if let Err(error) =
                    rpc_state.reserve_control_rpc(PendingControlRpc::MetadataConfChange {
                        source,
                        group_id,
                        attempt_id,
                        deadline: Instant::now() + CONTROL_RPC_DEFAULT_TIMEOUT,
                        response,
                    })
                {
                    send_metadata_conf_change_response(
                        transport,
                        source,
                        group_id,
                        attempt_id,
                        Err(error),
                    );
                    return;
                }
                if metadata_requests
                    .try_send(MetadataHostRequest::ConfChange {
                        expected_conf_state_version: request.expected_conf_state_version,
                        replica_id,
                        reply,
                    })
                    .is_err()
                {
                    rpc_state.cancel_control_rpc(attempt_id);
                    send_metadata_conf_change_response(
                        transport,
                        source,
                        group_id,
                        attempt_id,
                        Err(Error::ProposalUnavailable {
                            reason: "metadata ConfChange queue is full".to_string(),
                        }),
                    );
                    return;
                }
                host_wake.wake();
                return;
            }
            let Some(rpc::metadata_request::Request::ProposeCommand(request)) = proto.request
            else {
                return;
            };
            let Some(attempt_id) = request.rpc_attempt_id else {
                return;
            };
            let Some(request_id) = request.request_id else {
                return;
            };
            let Ok(request_id) = RequestId::from_proto(request_id) else {
                return;
            };
            let Ok(envelope) = ragnordb_common::metadata_codec::MetadataCommandEnvelope::decode(
                request.command_envelope.as_slice(),
            ) else {
                send_metadata_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    request_id,
                    Err(Error::InvalidArgument(
                        "metadata proposal envelope could not be decoded".to_string(),
                    )),
                );
                return;
            };
            let group_id = frame.raft_group_id;
            let (reply, response) = mpsc::channel();
            if let Err(error) = rpc_state.reserve_control_rpc(PendingControlRpc::MetadataCommand {
                source,
                group_id,
                attempt_id,
                request_id: request_id.clone(),
                deadline: Instant::now() + CONTROL_RPC_DEFAULT_TIMEOUT,
                response,
            }) {
                send_metadata_response(
                    transport,
                    source,
                    group_id,
                    attempt_id,
                    request_id,
                    Err(error),
                );
                return;
            }

            if metadata_requests
                .try_send(MetadataHostRequest::Command {
                    envelope: Box::new(envelope),
                    reply,
                    deadline: std::time::Instant::now() + Duration::from_secs(30),
                })
                .is_err()
            {
                rpc_state.cancel_control_rpc(attempt_id);
                send_metadata_response(
                    transport,
                    source,
                    frame.raft_group_id,
                    attempt_id,
                    request_id,
                    Err(Error::ProposalUnavailable {
                        reason: "metadata proposal queue is full".to_string(),
                    }),
                );
                return;
            }
            host_wake.wake();
        }
        MessageType::MetadataResponse => {
            let Ok(proto) = rpc::MetadataResponse::decode(frame.payload.as_slice()) else {
                return;
            };
            let attempt_id = match proto.response.as_ref() {
                Some(rpc::metadata_response::Response::ProposeCommand(response)) => {
                    response.rpc_attempt_id
                }
                Some(rpc::metadata_response::Response::ProposeConfChange(response)) => {
                    response.rpc_attempt_id
                }
                _ => None,
            };
            let Some(attempt_id) = attempt_id else {
                return;
            };
            let Ok(response) = MetadataResponse::from_proto(proto) else {
                return;
            };
            let mut pending_guard = rpc_state
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let pending_response = pending_guard
                .get(&attempt_id)
                .is_some_and(|pending| pending.target == source)
                .then(|| pending_guard.remove(&attempt_id))
                .flatten();
            if let Some(pending_response) = pending_response {
                let _ = pending_response
                    .sender
                    .send(PendingResponseMessage::Metadata(response));
            }
        }
        MessageType::ReplicaJoinResponse => {
            let Ok(response) = rpc::ReplicaJoinResponse::decode(frame.payload.as_slice()) else {
                return;
            };
            let Some(attempt_id) = response.rpc_attempt_id else {
                return;
            };
            let pending_response = rpc_state
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&attempt_id)
                .is_some_and(|pending| pending.target == source)
                .then(|| {
                    rpc_state
                        .pending
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .remove(&attempt_id)
                })
                .flatten();
            if let Some(pending_response) = pending_response {
                let _ = pending_response
                    .sender
                    .send(PendingResponseMessage::ReplicaJoin(response));
            }
        }
        MessageType::RaftConsensus => {}
    }
}

fn send_metadata_response(
    transport: &NodeRaftTransport,
    target: NodeId,
    group_id: RaftGroupId,
    attempt_id: u64,
    request_id: RequestId,
    result: Result<MetadataApplyOutcome>,
) {
    let (success, error_code, error_message, outcome, leader_replica_id) = match result {
        Ok(outcome) => (
            true,
            String::new(),
            String::new(),
            Some(metadata_outcome_to_wire(outcome)),
            None,
        ),
        Err(error) => (
            false,
            metadata_error_code(&error).to_string(),
            error.to_string(),
            None,
            match error {
                Error::NotLeader { leader_id } => leader_id,
                _ => None,
            },
        ),
    };
    let mut proto = MetadataResponse::ProposeCommand {
        request_id,
        success,
        error_code,
        error_message,
        outcome,
        leader_replica_id: leader_replica_id.map(ReplicaId),
    }
    .to_proto();
    if let Some(rpc::metadata_response::Response::ProposeCommand(response)) =
        proto.response.as_mut()
    {
        response.rpc_attempt_id = Some(attempt_id);
    }
    let _ = transport.try_send_rpc(
        target,
        RpcFrame {
            msg_type: MessageType::MetadataResponse,
            raft_group_id: group_id,
            payload: proto.encode_to_vec(),
        },
    );
}

fn send_metadata_conf_change_response(
    transport: &NodeRaftTransport,
    target: NodeId,
    group_id: RaftGroupId,
    attempt_id: u64,
    result: Result<()>,
) {
    let (success, error_code, error_message, leader_replica_id) = match result {
        Ok(()) => (true, String::new(), String::new(), None),
        Err(error) => (
            false,
            metadata_error_code(&error).to_string(),
            error.to_string(),
            match error {
                Error::NotLeader { leader_id } => leader_id.map(ReplicaId),
                _ => None,
            },
        ),
    };
    let mut proto = MetadataResponse::ProposeConfChange {
        success,
        error_code,
        error_message,
        leader_replica_id,
    }
    .to_proto();
    if let Some(rpc::metadata_response::Response::ProposeConfChange(response)) =
        proto.response.as_mut()
    {
        response.rpc_attempt_id = Some(attempt_id);
    }
    let _ = transport.try_send_rpc(
        target,
        RpcFrame {
            msg_type: MessageType::MetadataResponse,
            raft_group_id: group_id,
            payload: proto.encode_to_vec(),
        },
    );
}

fn metadata_outcome_to_wire(
    outcome: MetadataApplyOutcome,
) -> ragnordb_common::rpc_codec::MetadataProposalOutcome {
    match outcome {
        MetadataApplyOutcome::Applied => {
            ragnordb_common::rpc_codec::MetadataProposalOutcome::Applied
        }
        MetadataApplyOutcome::AlreadyApplied => {
            ragnordb_common::rpc_codec::MetadataProposalOutcome::AlreadyApplied
        }
        MetadataApplyOutcome::ClientRegistered { session_epoch, .. } => {
            ragnordb_common::rpc_codec::MetadataProposalOutcome::ClientRegistered { session_epoch }
        }
        MetadataApplyOutcome::ClientRenewed => {
            ragnordb_common::rpc_codec::MetadataProposalOutcome::ClientRenewed
        }
        MetadataApplyOutcome::TimestampsReserved {
            reserved_from,
            reserved_until,
        } => ragnordb_common::rpc_codec::MetadataProposalOutcome::TimestampsReserved {
            reserved_from,
            reserved_until,
        },
        MetadataApplyOutcome::TableCreated(created) => {
            ragnordb_common::rpc_codec::MetadataProposalOutcome::TableCreated {
                table_id: created.table_id,
                tablet_id: created.tablet_id,
                raft_group_id: created.raft_group_id,
            }
        }
        MetadataApplyOutcome::Rejected(rejection) => match rejection {
            MetadataRejection::TimestampReservationRegressed { current, received } => {
                ragnordb_common::rpc_codec::MetadataProposalOutcome::TimestampReservationRegressed {
                    current,
                    received,
                }
            }
            rejection => ragnordb_common::rpc_codec::MetadataProposalOutcome::Rejected {
                reason: rejection.to_string(),
            },
        },
    }
}

fn metadata_error_code(error: &Error) -> &'static str {
    match error {
        Error::NotLeader { .. } | Error::LeaderUnknown => "NOT_LEADER",
        Error::RecoveryRequired { .. } => "RECOVERY_REQUIRED",
        Error::ConstraintViolation(_) => "METADATA_REJECTED",
        Error::ProposalUnavailable { .. } => "METADATA_UNAVAILABLE",
        _ => "METADATA_ERROR",
    }
}

fn attach_rpc_attempt_id(
    msg_type: MessageType,
    payload: &[u8],
    attempt_id: u64,
) -> Result<Vec<u8>> {
    if attempt_id == 0 {
        return Err(Error::InvalidArgument(
            "RPC attempt identity must be non-zero".to_string(),
        ));
    }
    match msg_type {
        MessageType::TabletCommandRequest => {
            let mut proto = rpc::TabletCommandRequest::decode(payload).map_err(|error| {
                Error::InvalidArgument(format!("invalid tablet command: {error}"))
            })?;
            proto.rpc_attempt_id = Some(attempt_id);
            Ok(proto.encode_to_vec())
        }
        MessageType::TabletReadRequest => {
            let mut proto = rpc::TabletReadRequest::decode(payload)
                .map_err(|error| Error::InvalidArgument(format!("invalid tablet read: {error}")))?;
            proto.rpc_attempt_id = Some(attempt_id);
            Ok(proto.encode_to_vec())
        }
        MessageType::TabletTransactionStatusRequest => {
            let mut proto =
                rpc::TabletTransactionStatusRequest::decode(payload).map_err(|error| {
                    Error::InvalidArgument(format!("invalid transaction status request: {error}"))
                })?;
            proto.rpc_attempt_id = Some(attempt_id);
            Ok(proto.encode_to_vec())
        }
        MessageType::TabletPointInspectionRequest => {
            let mut proto =
                rpc::TabletPointInspectionRequest::decode(payload).map_err(|error| {
                    Error::InvalidArgument(format!("invalid point inspection request: {error}"))
                })?;
            proto.rpc_attempt_id = Some(attempt_id);
            Ok(proto.encode_to_vec())
        }
        MessageType::TabletScanRequest => {
            let mut proto = rpc::TabletScanRequest::decode(payload)
                .map_err(|error| Error::InvalidArgument(format!("invalid tablet scan: {error}")))?;
            proto.rpc_attempt_id = Some(attempt_id);
            Ok(proto.encode_to_vec())
        }
        MessageType::TabletOutcomeQueryRequest => {
            let mut proto = rpc::TabletOutcomeQueryRequest::decode(payload).map_err(|error| {
                Error::InvalidArgument(format!("invalid outcome query: {error}"))
            })?;
            proto.rpc_attempt_id = Some(attempt_id);
            Ok(proto.encode_to_vec())
        }
        MessageType::MetadataRequest => {
            let mut proto = rpc::MetadataRequest::decode(payload).map_err(|error| {
                Error::InvalidArgument(format!("invalid metadata request: {error}"))
            })?;
            match proto.request.as_mut() {
                Some(rpc::metadata_request::Request::ProposeCommand(request)) => {
                    request.rpc_attempt_id = Some(attempt_id);
                }
                Some(rpc::metadata_request::Request::ProposeConfChange(request)) => {
                    request.rpc_attempt_id = Some(attempt_id);
                }
                _ => {}
            }
            Ok(proto.encode_to_vec())
        }
        MessageType::ReplicaJoinRequest => {
            let mut proto = rpc::ReplicaJoinRequest::decode(payload).map_err(|error| {
                Error::InvalidArgument(format!("invalid replica join request: {error}"))
            })?;
            proto.rpc_attempt_id = Some(attempt_id);
            Ok(proto.encode_to_vec())
        }
        _ => Err(Error::InvalidArgument(
            "RPC attempts are only valid for request messages".to_string(),
        )),
    }
}

fn send_replica_join_response(
    transport: &NodeRaftTransport,
    target: NodeId,
    group_id: RaftGroupId,
    attempt_id: u64,
    success: bool,
    error_message: String,
) {
    let response = rpc::ReplicaJoinResponse {
        rpc_attempt_id: Some(attempt_id),
        success,
        error_message,
    };
    let _ = transport.try_send_rpc(
        target,
        RpcFrame {
            msg_type: MessageType::ReplicaJoinResponse,
            raft_group_id: group_id,
            payload: response.encode_to_vec(),
        },
    );
}

fn send_response(
    transport: &NodeRaftTransport,
    target: NodeId,
    group_id: RaftGroupId,
    attempt_id: Option<u64>,
    response: TabletCommandResponse,
) {
    let mut proto = response.to_proto();
    proto.rpc_attempt_id = attempt_id;
    let frame = RpcFrame {
        msg_type: MessageType::TabletCommandResponse,
        raft_group_id: group_id,
        payload: proto.encode_to_vec(),
    };
    let _ = transport.try_send_rpc(target, frame);
}

fn error_response(request_id: RequestId, error: Error) -> TabletCommandResponse {
    let (error_code, retryable, leader_replica_id) = match &error {
        Error::NotLeader {
            leader_id: Some(leader_id),
        } => ("NOT_LEADER", true, Some(ReplicaId(*leader_id))),
        Error::NotLeader { leader_id: None } | Error::LeaderUnknown => {
            ("LEADER_UNKNOWN", true, None)
        }
        Error::ProposalUnavailable { .. } | Error::TabletUnavailable { .. } => {
            ("TABLET_UNAVAILABLE", true, None)
        }
        Error::StaleTabletEpoch { .. } => ("STALE_TABLET_EPOCH", true, None),
        // An indeterminate result is not a permission to replay the mutation.
        // The caller must query the original logical command outcome first.
        Error::RequestOutcomeUnknown { .. } => ("REQUEST_OUTCOME_UNKNOWN", false, None),
        Error::RequestIdExpired { .. } => ("REQUEST_ID_EXPIRED", false, None),
        Error::ClientSessionExpired { .. } => ("CLIENT_SESSION_EXPIRED", false, None),
        Error::WriteConflict(_) => ("WRITE_CONFLICT", false, None),
        Error::SnapshotTooOld { .. } => ("SNAPSHOT_TOO_OLD", false, None),
        Error::RecoveryRequired { .. } => ("RECOVERY_REQUIRED", false, None),
        Error::InvalidArgument(_) => ("INVALID_ARGUMENT", false, None),
        _ => ("TABLET_ERROR", false, None),
    };
    TabletCommandResponse {
        request_id,
        success: false,
        error_message: error.to_string(),
        error_code: error_code.to_string(),
        retryable,
        result_data: Vec::new(),
        found: false,
        leader_replica_id,
        current_tablet_epoch: match &error {
            Error::StaleTabletEpoch { current_epoch, .. } => Some(*current_epoch),
            _ => None,
        },
        expected_tablet_epoch: match &error {
            Error::StaleTabletEpoch { expected_epoch, .. } => Some(*expected_epoch),
            _ => None,
        },
    }
}

fn response_error(response: TabletCommandResponse) -> Error {
    if response.error_code == "NOT_LEADER" {
        return Error::NotLeader {
            leader_id: response.leader_replica_id.map(|id| id.0),
        };
    }
    if response.error_code == "LEADER_UNKNOWN" {
        return Error::LeaderUnknown;
    }
    if response.error_code == "STALE_TABLET_EPOCH" {
        return Error::StaleTabletEpoch {
            current_epoch: response.current_tablet_epoch.unwrap_or_default(),
            expected_epoch: response.expected_tablet_epoch.unwrap_or_default(),
        };
    }
    if response.error_code == "REQUEST_OUTCOME_UNKNOWN" {
        return Error::RequestOutcomeUnknown {
            identity: response.error_message,
        };
    }
    if response.error_code == "REQUEST_ID_EXPIRED" {
        return Error::RequestIdExpired {
            identity: response.error_message,
        };
    }
    if response.error_code == "CLIENT_SESSION_EXPIRED" {
        return Error::ClientSessionExpired { session_epoch: 0 };
    }
    if response.error_code == "TABLET_UNAVAILABLE" {
        return Error::TabletUnavailable {
            reason: response.error_message,
        };
    }
    if response.retryable {
        return Error::ProposalUnavailable {
            reason: response.error_message,
        };
    }
    if response.error_code == "RECOVERY_REQUIRED" {
        return Error::RecoveryRequired {
            reason: response.error_message,
        };
    }
    if response.error_code == "WRITE_CONFLICT" {
        return Error::WriteConflict(response.error_message);
    }
    if response.error_code == "SNAPSHOT_TOO_OLD" {
        return Error::SnapshotTooOld {
            reason: response.error_message,
        };
    }
    Error::InvalidArgument(response.error_message)
}

fn error_leader(error: &Error) -> Option<ReplicaId> {
    match error {
        Error::NotLeader {
            leader_id: Some(leader_id),
        } => Some(ReplicaId(*leader_id)),
        _ => None,
    }
}

const MAX_TABLET_RETRY_ATTEMPTS: u32 = 3;
const TABLET_RETRY_BACKOFF_BASE_MS: u64 = 5;
const TABLET_RETRY_BACKOFF_MAX_MS: u64 = 100;
/// Budget reserved for transport queueing and forwarding overhead. A remote
/// tablet never receives the full sender-side attempt timeout, so the hop
/// cannot restart or extend that budget.
const FOREGROUND_FORWARD_SAFETY_MARGIN: Duration = Duration::from_millis(100);

/// Encode one conservative remaining budget for a remote node. The wire uses
/// a duration rather than a wall-clock timestamp: independent node clocks
/// cannot safely reconstruct a monotonic deadline. The sender's absolute
/// deadline remains authoritative locally, while the receiver gets only the
/// smaller of the remaining request budget and this attempt's transport
/// budget, minus a fixed forwarding safety margin.
fn forwarded_foreground_budget_millis(deadline: Instant, attempt_timeout: Duration) -> Result<u64> {
    let remaining = deadline
        .saturating_duration_since(Instant::now())
        .min(attempt_timeout);
    let safe_budget = remaining
        .checked_sub(FOREGROUND_FORWARD_SAFETY_MARGIN)
        .ok_or_else(|| Error::ProposalUnavailable {
            reason: "foreground RPC has no safe forwarding budget remaining".to_string(),
        })?;
    let millis = safe_budget.as_millis();
    if millis == 0 {
        return Err(Error::ProposalUnavailable {
            reason: "foreground RPC has no safe forwarding budget remaining".to_string(),
        });
    }
    u64::try_from(millis)
        .map_err(|_| Error::InvalidArgument("foreground RPC deadline overflowed".to_string()))
}

/// Reconstruct a receiver-local monotonic deadline from a conservative
/// duration supplied by the forwarding node.
///
/// Missing budgets fail closed. An upgraded receiver must never replace a
/// missing statement budget with an unrelated fixed timeout.
fn remote_foreground_deadline(deadline_remaining_ms: Option<u64>) -> Result<Instant> {
    let deadline_remaining_ms = deadline_remaining_ms.ok_or_else(|| {
        Error::InvalidArgument(
            "forwarded foreground RPC is missing its deadline budget".to_string(),
        )
    })?;

    if deadline_remaining_ms == 0 {
        return Err(Error::InvalidArgument(
            "forwarded foreground RPC deadline budget must be non-zero".to_string(),
        ));
    }

    Instant::now()
        .checked_add(Duration::from_millis(deadline_remaining_ms))
        .ok_or_else(|| {
            Error::InvalidArgument("forwarded foreground RPC deadline overflowed".to_string())
        })
}

fn ensure_snapshot_not_collected(
    metadata: &MetadataRuntimeHandle,
    read_timestamp: Timestamp,
) -> Result<()> {
    ensure_read_timestamp_not_collected(read_timestamp, metadata.state_snapshot().gc_safe_point())
}

fn ensure_read_timestamp_not_collected(
    read_timestamp: Timestamp,
    gc_safe_point: Timestamp,
) -> Result<()> {
    if read_timestamp < gc_safe_point {
        return Err(Error::SnapshotTooOld {
            reason: format!(
                "requested timestamp {} is below GC safe point {}",
                read_timestamp.0, gc_safe_point.0
            ),
        });
    }
    Ok(())
}

fn apply_leader_hint(route: &mut TabletRoute, leader: ReplicaId) -> bool {
    if route.node_for_replica(leader).is_none() {
        return false;
    }
    route.leader_replica_id = leader;
    true
}

fn same_route_identity(left: &TabletRoute, right: &TabletRoute) -> bool {
    left.raft_group_id == right.raft_group_id
        && left.tablet_id == right.tablet_id
        && left.tablet_epoch == right.tablet_epoch
        && left.replicas == right.replicas
}

fn retry_backoff(attempt: u32, remaining: Duration) -> Duration {
    let multiplier = 1_u64 << attempt.min(20);
    let delay_ms = TABLET_RETRY_BACKOFF_BASE_MS
        .saturating_mul(multiplier)
        .min(TABLET_RETRY_BACKOFF_MAX_MS);
    Duration::from_millis(delay_ms).min(remaining)
}

/// Reserve part of the caller's deadline for each remaining replica attempt.
///
/// A stopped peer can accept an outbound frame and then provide no response
/// until the transport timeout. Giving the first attempt the entire statement
/// deadline would prevent the gateway from trying a healthy replica, even
/// though the route still contains enough placement information to fail over.
fn retry_attempt_timeout(deadline: std::time::Instant, attempt: u32) -> Duration {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    let attempts_left = MAX_TABLET_RETRY_ATTEMPTS.saturating_sub(attempt).max(1);
    if attempts_left == 1 {
        remaining
    } else {
        remaining / attempts_left
    }
}

fn next_unattempted_replica(
    route: &TabletRoute,
    attempted: &BTreeSet<(RaftGroupId, ReplicaId)>,
) -> Result<(ReplicaId, NodeId)> {
    route
        .validate()
        .map_err(|error| Error::InvalidArgument(error.to_string()))?;
    let mut replicas = route.replicas.iter();
    let mut preferred = route
        .replicas
        .iter()
        .find(|replica| replica.replica_id == route.leader_replica_id)
        .into_iter()
        .chain(replicas.by_ref());
    preferred
        .find(|replica| !attempted.contains(&(route.raft_group_id, replica.replica_id)))
        .map(|replica| (replica.replica_id, replica.node_id))
        .ok_or(Error::LeaderUnknown)
}

fn is_retryable_tablet_error(error: &Error) -> bool {
    matches!(
        error,
        Error::NotLeader { .. }
            | Error::LeaderUnknown
            | Error::StaleTabletEpoch { .. }
            | Error::TabletUnavailable { .. }
            | Error::ProposalUnavailable { .. }
    )
}

fn encode_command_outcome(outcome: TabletCommandApplyOutcome) -> Vec<u8> {
    vec![
        command_result_code(outcome.result),
        u8::from(outcome.deduplicated),
    ]
}

fn decode_command_outcome(bytes: &[u8]) -> Result<TabletCommandApplyOutcome> {
    if bytes.len() != 2 {
        return Err(Error::CorruptData(
            "tablet command response has an invalid result payload".to_string(),
        ));
    }
    let result = match bytes[0] {
        0 => TabletCommandApplyResult::Noop,
        1 => TabletCommandApplyResult::SingleShardCommit,
        2 => TabletCommandApplyResult::Prewrite,
        3 => TabletCommandApplyResult::Commit,
        4 => TabletCommandApplyResult::Rollback,
        5 => TabletCommandApplyResult::ResolveIntent,
        6 => TabletCommandApplyResult::PublishAbortedTransactionStatus,
        7 => TabletCommandApplyResult::HeartbeatTransactionStatus,
        _ => {
            return Err(Error::CorruptData(
                "tablet command response contains an unknown result".to_string(),
            ));
        }
    };
    Ok(TabletCommandApplyOutcome {
        result,
        deduplicated: bytes[1] != 0,
    })
}

const fn command_result_code(result: TabletCommandApplyResult) -> u8 {
    match result {
        TabletCommandApplyResult::Noop => 0,
        TabletCommandApplyResult::SingleShardCommit => 1,
        TabletCommandApplyResult::Prewrite => 2,
        TabletCommandApplyResult::Commit => 3,
        TabletCommandApplyResult::Rollback => 4,
        TabletCommandApplyResult::ResolveIntent => 5,
        TabletCommandApplyResult::PublishAbortedTransactionStatus => 6,
        TabletCommandApplyResult::HeartbeatTransactionStatus => 7,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ragnordb_common::ids::TabletId;

    #[test]
    fn historical_reads_at_the_safe_point_pass_and_older_reads_fail_closed() {
        assert!(ensure_read_timestamp_not_collected(Timestamp(40), Timestamp(40)).is_ok());
        let error = ensure_read_timestamp_not_collected(Timestamp(39), Timestamp(40)).unwrap_err();
        assert!(matches!(
            &error,
            Error::SnapshotTooOld { reason } if reason.contains("below GC safe point")
        ));
        let response = error_response(
            RequestId {
                client_id: 9,
                sequence: 1,
                raft_group_id: RaftGroupId(7),
            },
            error,
        );
        assert_eq!(response.error_code, "SNAPSHOT_TOO_OLD");
        assert!(!response.retryable);
    }

    #[test]
    fn command_outcome_wire_roundtrip_preserves_deduplication() {
        let expected = TabletCommandApplyOutcome {
            result: TabletCommandApplyResult::SingleShardCommit,
            deduplicated: true,
        };

        assert_eq!(
            decode_command_outcome(&encode_command_outcome(expected)).unwrap(),
            expected
        );
    }

    #[test]
    fn not_leader_response_preserves_typed_leader_hint() {
        let request_id = RequestId {
            client_id: 9,
            sequence: 1,
            raft_group_id: RaftGroupId(7),
        };
        let response = error_response(
            request_id,
            Error::NotLeader {
                leader_id: Some(12),
            },
        );

        assert_eq!(response.error_code, "NOT_LEADER");
        assert!(response.retryable);
        assert_eq!(response.leader_replica_id, Some(ReplicaId(12)));
        assert!(matches!(
            response_error(response),
            Error::NotLeader {
                leader_id: Some(12)
            }
        ));
    }

    /// Realistic bug caught: a delayed response from a timed-out physical
    /// attempt was previously correlated only by RequestId and could complete
    /// a later retry waiter for the same logical mutation.
    #[test]
    fn delayed_attempt_cannot_complete_a_new_waiter() {
        let state = RpcState::new();
        let (old_sender, old_receiver) = mpsc::channel();
        let (new_sender, new_receiver) = mpsc::channel();
        let target = NodeId(2);

        state.pending.lock().unwrap().insert(
            41,
            PendingResponse {
                target,
                sender: old_sender,
            },
        );
        state.pending.lock().unwrap().remove(&41);
        state.pending.lock().unwrap().insert(
            42,
            PendingResponse {
                target,
                sender: new_sender,
            },
        );

        assert!(state.pending.lock().unwrap().get(&41).is_none());
        assert!(new_receiver.try_recv().is_err());
        drop(old_receiver);
        drop(new_receiver);
    }

    /// Realistic bug caught: a completion queue ordered by request admission
    /// would make a fast tablet wait behind an earlier slow tablet. Completion
    /// tokens must therefore be independently drainable and must not require
    /// the dispatcher to wait for the oldest request first.
    #[test]
    fn tablet_rpc_completion_tokens_are_drained_independently() {
        let state = RpcState::new();
        let status = Arc::new(RwLock::new(ReplicatedTabletStatus::default()));
        let first = state
            .reserve_tablet_rpc(PendingTabletRpc {
                source: NodeId(2),
                group_id: RaftGroupId(7),
                attempt_id: Some(41),
                request_id: RequestId {
                    client_id: 1,
                    sequence: 1,
                    raft_group_id: RaftGroupId(7),
                },
                status: status.clone(),
                deadline: Instant::now() + Duration::from_secs(1),
                completion: None,
                kind: PendingTabletRpcKind::ReadOnly,
            })
            .unwrap();
        let second = state
            .reserve_tablet_rpc(PendingTabletRpc {
                source: NodeId(2),
                group_id: RaftGroupId(8),
                attempt_id: Some(42),
                request_id: RequestId {
                    client_id: 2,
                    sequence: 1,
                    raft_group_id: RaftGroupId(8),
                },
                status,
                deadline: Instant::now() + Duration::from_secs(1),
                completion: None,
                kind: PendingTabletRpcKind::ReadOnly,
            })
            .unwrap();

        state.publish_tablet_completion(second, TabletRpcCompletion::Outcome(Ok(None)));
        let ready = state.take_ready_tablet_rpcs(Instant::now());

        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].request_id.client_id, 2);
        assert!(state.inbound_tablet.lock().unwrap().contains_key(&first));
    }

    /// Realistic bug caught: a timed-out inbound RPC could retain its token
    /// forever and later emit a stale response after the caller had cancelled
    /// the attempt. Cancellation removes the token, making late completion a
    /// harmless no-op.
    #[test]
    fn cancelled_tablet_rpc_ignores_late_completion() {
        let state = RpcState::new();
        let token = state
            .reserve_tablet_rpc(PendingTabletRpc {
                source: NodeId(2),
                group_id: RaftGroupId(7),
                attempt_id: Some(41),
                request_id: RequestId {
                    client_id: 1,
                    sequence: 1,
                    raft_group_id: RaftGroupId(7),
                },
                status: Arc::new(RwLock::new(ReplicatedTabletStatus::default())),
                deadline: Instant::now() + Duration::from_secs(1),
                completion: None,
                kind: PendingTabletRpcKind::ReadOnly,
            })
            .unwrap();

        state.cancel_tablet_rpc(token);
        state.publish_tablet_completion(token, TabletRpcCompletion::Outcome(Ok(None)));

        assert!(state.take_ready_tablet_rpcs(Instant::now()).is_empty());
    }

    /// Realistic bug caught: replacing completion threads with a vector is
    /// only safe when the vector itself remains bounded independently of the
    /// number of in-flight control requests.
    #[test]
    fn control_rpc_completion_capacity_is_bounded() {
        let state = RpcState::new();
        for attempt_id in 1..=MAX_PENDING_CONTROL_RPCS as u64 {
            let (_sender, response) = mpsc::channel();
            state
                .reserve_control_rpc(PendingControlRpc::ReplicaJoin {
                    source: NodeId(2),
                    group_id: RaftGroupId(7),
                    attempt_id,
                    deadline: Instant::now() + Duration::from_secs(1),
                    response,
                })
                .unwrap();
        }

        let (_sender, response) = mpsc::channel();
        assert!(matches!(
            state.reserve_control_rpc(PendingControlRpc::ReplicaJoin {
                source: NodeId(2),
                group_id: RaftGroupId(7),
                attempt_id: MAX_PENDING_CONTROL_RPCS as u64 + 1,
                deadline: Instant::now() + Duration::from_secs(1),
                response,
            }),
            Err(Error::ProposalUnavailable { .. })
        ));
        assert_eq!(
            state.pending_control.lock().unwrap().len(),
            MAX_PENDING_CONTROL_RPCS
        );
    }

    /// Realistic bug caught: timing out after mutation admission cannot be
    /// mapped to an ordinary retry because the command may already be durable
    /// or applied on the owning reactor.
    #[test]
    fn admitted_mutation_timeout_is_outcome_unknown_but_reads_remain_retryable() {
        let status = Arc::new(RwLock::new(ReplicatedTabletStatus::default()));
        let mutation = PendingTabletRpc {
            source: NodeId(2),
            group_id: RaftGroupId(7),
            attempt_id: Some(1),
            request_id: RequestId {
                client_id: 9,
                sequence: 4,
                raft_group_id: RaftGroupId(7),
            },
            status: status.clone(),
            deadline: Instant::now(),
            completion: None,
            kind: PendingTabletRpcKind::Mutation,
        };
        let read = PendingTabletRpc {
            source: NodeId(2),
            group_id: RaftGroupId(7),
            attempt_id: Some(1),
            request_id: RequestId {
                client_id: 9,
                sequence: 4,
                raft_group_id: RaftGroupId(7),
            },
            status,
            deadline: Instant::now(),
            completion: None,
            kind: PendingTabletRpcKind::ReadOnly,
        };

        assert!(matches!(
            tablet_rpc_deadline_error(&mutation),
            Error::RequestOutcomeUnknown { .. }
        ));
        assert!(matches!(
            tablet_rpc_deadline_error(&read),
            Error::ProposalUnavailable { .. }
        ));
    }

    #[test]
    fn retry_route_advances_through_each_replica_once() {
        let route = TabletRoute {
            raft_group_id: RaftGroupId(7),
            tablet_id: TabletId(8),
            tablet_epoch: 1,
            leader_replica_id: ReplicaId(1),
            replicas: vec![
                ReplicaRoute {
                    replica_id: ReplicaId(1),
                    node_id: NodeId(11),
                },
                ReplicaRoute {
                    replica_id: ReplicaId(2),
                    node_id: NodeId(12),
                },
                ReplicaRoute {
                    replica_id: ReplicaId(3),
                    node_id: NodeId(13),
                },
            ]
            .into(),
        };
        let mut attempted = BTreeSet::new();
        assert_eq!(
            next_unattempted_replica(&route, &attempted).unwrap(),
            (ReplicaId(1), NodeId(11))
        );
        attempted.insert((route.raft_group_id, ReplicaId(1)));
        assert_eq!(
            next_unattempted_replica(&route, &attempted).unwrap(),
            (ReplicaId(2), NodeId(12))
        );
        attempted.insert((route.raft_group_id, ReplicaId(2)));
        assert_eq!(
            next_unattempted_replica(&route, &attempted).unwrap(),
            (ReplicaId(3), NodeId(13))
        );
    }

    /// Realistic bug caught: linear or uncapped retry delays can create a
    /// retry storm and can sleep past the statement deadline instead of
    /// returning the bounded retry result to the caller.
    #[test]
    fn retry_backoff_is_exponential_and_deadline_bounded() {
        assert_eq!(
            retry_backoff(0, Duration::from_millis(100)),
            Duration::from_millis(5)
        );
        assert_eq!(
            retry_backoff(1, Duration::from_millis(100)),
            Duration::from_millis(10)
        );
        assert_eq!(
            retry_backoff(2, Duration::from_millis(100)),
            Duration::from_millis(20)
        );
        assert_eq!(
            retry_backoff(10, Duration::from_millis(100)),
            Duration::from_millis(100)
        );
        assert_eq!(
            retry_backoff(1, Duration::from_millis(3)),
            Duration::from_millis(3)
        );
    }

    #[test]
    /// Catches restarting the full statement timeout after a node-to-node
    /// forward instead of honoring the original absolute caller deadline.
    fn forwarded_foreground_budget_is_required_and_conservative() {
        assert!(matches!(
            remote_foreground_deadline(None),
            Err(Error::InvalidArgument(reason))
                if reason.contains("missing its deadline budget")
        ));

        assert!(matches!(
            remote_foreground_deadline(Some(0)),
            Err(Error::InvalidArgument(reason))
                if reason.contains("must be non-zero")
        ));

        let deadline = Instant::now() + Duration::from_millis(500);
        let encoded =
            forwarded_foreground_budget_millis(deadline, Duration::from_millis(500)).unwrap();
        assert!(encoded <= 400);
        let remaining = remote_foreground_deadline(Some(encoded)).unwrap();
        assert!(remaining > Instant::now());
        assert!(remaining <= Instant::now() + Duration::from_millis(400));

        let short = forwarded_foreground_budget_millis(deadline, Duration::from_millis(50));
        assert!(matches!(short, Err(Error::ProposalUnavailable { .. })));
    }

    /// Realistic bug caught: a stale or malicious leader hint can replace the
    /// route's leader with a replica outside the authoritative placement and
    /// make later retries target an invalid node.
    #[test]
    fn invalid_leader_hint_does_not_replace_route_leader() {
        let mut route = TabletRoute {
            raft_group_id: RaftGroupId(7),
            tablet_id: TabletId(8),
            tablet_epoch: 1,
            leader_replica_id: ReplicaId(1),
            replicas: vec![ReplicaRoute {
                replica_id: ReplicaId(1),
                node_id: NodeId(11),
            }]
            .into(),
        };

        assert!(!apply_leader_hint(&mut route, ReplicaId(99)));
        assert_eq!(route.leader_replica_id, ReplicaId(1));
        assert!(apply_leader_hint(&mut route, ReplicaId(1)));
    }

    /// Realistic bug caught:
    ///
    /// An unknown outcome is not safe to execute again automatically. The
    /// client must query the original logical command outcome first; marking
    /// this response retryable would permit a duplicate durable mutation.
    #[test]
    fn unknown_outcome_response_requires_outcome_query() {
        let request_id = RequestId {
            client_id: 9,
            sequence: 2,
            raft_group_id: RaftGroupId(7),
        };
        let response = error_response(
            request_id,
            Error::RequestOutcomeUnknown {
                identity: "client=9/session=3/sequence=2".to_string(),
            },
        );

        assert_eq!(response.error_code, "REQUEST_OUTCOME_UNKNOWN");
        assert!(!response.retryable);
        assert!(matches!(
            response_error(response),
            Error::RequestOutcomeUnknown { .. }
        ));
    }

    /// Realistic bug caught:
    ///
    /// The gateway retry loop must not convert an indeterminate mutation into
    /// an automatic resend. The caller needs the outcome-query path first.
    #[test]
    fn unknown_outcome_is_not_automatically_resubmitted() {
        assert!(!is_retryable_tablet_error(&Error::RequestOutcomeUnknown {
            identity: "client=9/session=3/sequence=2".to_string(),
        }));
    }
}
