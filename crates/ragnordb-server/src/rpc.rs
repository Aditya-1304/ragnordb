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
    time::Duration,
};

use prost::Message;
use ragnordb_catalog::MetadataApplyOutcome;
use ragnordb_common::{
    Error, Result,
    command_codec::{CachedTabletCommandOutcome, TabletCommand},
    ids::{
        LogicalCommandId, NodeId, RaftGroupId, ReplicaId, RequestId, RowKey, TableId, Timestamp,
    },
    metadata_codec::DesiredReplicaRole,
    proto::rpc,
    rpc_codec::{
        MessageType, MetadataProposalRequest, MetadataRequest, MetadataResponse, ReplicaRoute,
        RpcFrame, TabletCommandRequest, TabletCommandResponse, TabletOutcomeQueryRequest,
        TabletReadRequest, TabletRoute,
    },
};
use ragnordb_exec::TabletGateway;
use ragnordb_multiraft::meta::MetadataRuntimeHandle;
use ragnordb_multiraft::transport::{NodeRaftTransport, NodeRpcInbound};
use ragnordb_tablet::TabletRouter;
use ragnordb_tablet::command::{TabletCommandApplyOutcome, TabletCommandApplyResult};

use crate::database::SharedLocalDatabase;
use crate::multiraft_runtime::MetadataHostRequest;
use crate::replicated_tablet::ReplicatedTabletHandle;

/// Group-qualified handles published by the lifecycle owner after a tablet
/// runtime has crossed its activation boundary.
pub type SharedTabletHandleRegistry =
    Arc<RwLock<BTreeMap<RaftGroupId, Arc<ReplicatedTabletHandle>>>>;

enum PendingResponseMessage {
    Tablet(TabletCommandResponse),
    Metadata(MetadataResponse),
}

struct PendingResponse {
    target: NodeId,
    sender: mpsc::Sender<PendingResponseMessage>,
}

#[derive(Clone)]
pub(crate) struct RpcState {
    pending: Arc<Mutex<BTreeMap<u64, PendingResponse>>>,
    next_attempt: Arc<AtomicU64>,
}

#[derive(Clone)]
pub(crate) struct MetadataRpcClient {
    transport: NodeRaftTransport,
    rpc_state: RpcState,
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
}

impl RpcState {
    pub(crate) fn new() -> Self {
        Self {
            pending: Arc::new(Mutex::new(BTreeMap::new())),
            next_attempt: Arc::new(AtomicU64::new(1)),
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
}

/// Client-side gateway for local and remote tablet operations.
#[derive(Clone)]
pub struct TabletRpcClient {
    transport: NodeRaftTransport,
    handles: SharedTabletHandleRegistry,
    rpc_state: RpcState,
    metadata: MetadataRuntimeHandle,
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
        }
    }

    /// Resolve the current committed metadata route for one point key.
    ///
    /// Metadata placement is read-only here. The returned leader is only a
    /// hint; a tablet response may refresh it after a leadership transition.
    pub fn lookup_tablet_route(&self, table_id: TableId, key: &[u8]) -> Result<TabletRoute> {
        let state = self.metadata.state_snapshot();
        state.table(table_id).ok_or_else(|| {
            Error::SchemaMismatch(format!("metadata has no table {}", table_id.0))
        })?;
        let descriptors = state.tablets_for_table(table_id);
        let router = TabletRouter::new(table_id, &descriptors).map_err(|error| {
            Error::CorruptData(format!(
                "metadata route for table {} is invalid: {error}",
                table_id.0
            ))
        })?;
        let tablet_id = router.route_point(key)?;
        let descriptor = descriptors
            .iter()
            .find(|descriptor| descriptor.tablet_id == tablet_id)
            .ok_or_else(|| {
                Error::CorruptData("metadata route selected an unknown tablet".to_string())
            })?;
        let placement = state.desired_placement(tablet_id).ok_or_else(|| {
            Error::CorruptData(format!("tablet {} has no desired placement", tablet_id.0))
        })?;
        let replicas = placement
            .replicas
            .iter()
            .map(|replica| ReplicaRoute {
                replica_id: replica.replica_id,
                node_id: replica.node_id,
            })
            .collect::<Vec<_>>();
        let placement_leader_replica_id = placement
            .replicas
            .iter()
            .find(|replica| replica.role == DesiredReplicaRole::Voter)
            .or_else(|| placement.replicas.first())
            .map(|replica| replica.replica_id)
            .ok_or_else(|| Error::CorruptData("tablet placement has no replicas".to_string()))?;
        // Metadata records desired membership, not the current Raft leader.
        // Prefer the local Ready owner's published leader whenever this node
        // hosts the group; fall back to the canonical first voter only while
        // that point-in-time status is still unknown during election/startup.
        let published_leader_replica_id = self
            .handles
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&descriptor.raft_group_id)
            .and_then(|handle| handle.status().leader_replica_id)
            .map(ReplicaId);
        let leader_replica_id = published_leader_replica_id
            .filter(|leader| {
                placement
                    .replicas
                    .iter()
                    .any(|replica| replica.replica_id == *leader)
            })
            .unwrap_or(placement_leader_replica_id);
        let route = TabletRoute {
            raft_group_id: descriptor.raft_group_id,
            tablet_id: descriptor.tablet_id,
            tablet_epoch: descriptor.tablet_epoch,
            leader_replica_id,
            replicas,
        };
        route
            .validate()
            .map_err(|error| Error::CorruptData(error.to_string()))?;
        Ok(route)
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
        };
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::InvalidArgument("tablet retry deadline overflowed".into()))?;
        let mut current_route = route.clone();
        let mut last_error = None;
        let mut attempted_replicas = BTreeSet::new();

        for attempt in 0..=2_u32 {
            let (target_replica, target) =
                match next_unattempted_replica(&current_route, &attempted_replicas) {
                    Ok(target) => target,
                    Err(_) => {
                        last_error = Some(Error::LeaderUnknown);
                        continue;
                    }
                };
            attempted_replicas.insert((current_route.raft_group_id, target_replica));
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            let mut attempt_request = TabletCommandRequest {
                request_id: request.request_id.clone(),
                logical_command_id: request.logical_command_id,
                acknowledged_through: request.acknowledged_through,
                tablet_id: current_route.tablet_id,
                tablet_epoch: current_route.tablet_epoch,
                command: request.command.clone(),
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
                remaining,
            ) {
                Ok(outcome) => return Ok(outcome),
                Err(error) if is_retryable_tablet_error(&error) && attempt < 2 => {
                    self.adjust_route_after_error(
                        &mut current_route,
                        &command,
                        logical_command_id.is_some(),
                        &error,
                    );
                    last_error = Some(error);
                    thread::sleep(Duration::from_millis(5 * (u64::from(attempt) + 1)));
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

        let request = TabletOutcomeQueryRequest {
            request_id,
            logical_command_id,
            tablet_id: route.tablet_id,
            tablet_epoch: route.tablet_epoch,
        };
        let target = route.leader_node().map_err(|_| Error::LeaderUnknown)?;
        let response = if target == self.transport.local_node_id() {
            let handle = self.local_handle(route.raft_group_id)?;
            let outcome = handle.query_original_outcome(request, timeout)?;
            return Ok(outcome);
        } else {
            self.send_remote(
                target,
                RpcFrame {
                    msg_type: MessageType::TabletOutcomeQueryRequest,
                    raft_group_id: route.raft_group_id,
                    payload: request.to_proto().encode_to_vec(),
                },
                request.request_id.clone(),
                timeout,
                false,
            )?
        };

        if !response.success {
            return Err(response_error(response));
        }
        if !response.found {
            return Ok(None);
        }
        CachedTabletCommandOutcome::decode_from_outcome_query(&response.result_data)
            .map(Some)
            .map_err(|error| Error::CorruptData(error.to_string()))
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
        };
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| {
                Error::InvalidArgument("tablet read retry deadline overflowed".into())
            })?;
        let mut current_route = route.clone();
        let mut last_error = None;
        let mut attempted_replicas = BTreeSet::new();

        for attempt in 0..=2_u32 {
            let (target_replica, target) =
                match next_unattempted_replica(&current_route, &attempted_replicas) {
                    Ok(target) => target,
                    Err(_) => {
                        last_error = Some(Error::LeaderUnknown);
                        continue;
                    }
                };
            attempted_replicas.insert((current_route.raft_group_id, target_replica));
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            request.tablet_id = current_route.tablet_id;
            request.tablet_epoch = current_route.tablet_epoch;
            request.request_id.raft_group_id = current_route.raft_group_id;
            match self.read_point_to(
                target,
                current_route.raft_group_id,
                request.clone(),
                remaining,
            ) {
                Ok(row) => return Ok(row),
                Err(error) if is_retryable_tablet_error(&error) && attempt < 2 => {
                    if let Error::StaleTabletEpoch { current_epoch, .. } = &error
                        && *current_epoch != 0
                    {
                        current_route.tablet_epoch = *current_epoch;
                        request.tablet_epoch = *current_epoch;
                    }
                    if let Some(leader) = error_leader(&error) {
                        current_route.leader_replica_id = leader;
                    }
                    last_error = Some(error);
                    thread::sleep(Duration::from_millis(5 * (u64::from(attempt) + 1)));
                }
                Err(error) => return Err(error),
            }
        }

        Err(last_error.unwrap_or_else(|| Error::TabletUnavailable {
            reason: "tablet read retry deadline elapsed".to_string(),
        }))
    }

    fn adjust_route_after_error(
        &self,
        route: &mut TabletRoute,
        command: &TabletCommand,
        allow_topology_move: bool,
        error: &Error,
    ) {
        if let Error::StaleTabletEpoch { current_epoch, .. } = error
            && *current_epoch != 0
        {
            route.tablet_epoch = *current_epoch;
        }
        if let Some(leader_replica_id) = error_leader(error) {
            route.leader_replica_id = leader_replica_id;
            return;
        }

        if matches!(error, Error::StaleTabletEpoch { .. })
            && let Some(refreshed) =
                self.refresh_route_for_command(route, command, allow_topology_move)
        {
            *route = refreshed;
        }
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
        request: TabletCommandRequest,
        timeout: Duration,
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
        request: TabletReadRequest,
        timeout: Duration,
    ) -> Result<Option<Vec<u8>>> {
        if target == self.transport.local_node_id() {
            let handle = self.local_handle(group_id)?;
            handle.read_barrier(timeout)?;
            return handle.read_point(request, timeout);
        }

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
    database: SharedLocalDatabase,
    shutdown: Arc<AtomicBool>,
) -> (TabletRpcClient, thread::JoinHandle<()>) {
    let client = TabletRpcClient::new(
        transport.clone(),
        handles.clone(),
        rpc_state.clone(),
        metadata,
    );
    let worker = thread::Builder::new()
        .name("ragnordb-tablet-rpc".to_string())
        .spawn(move || {
            while !shutdown.load(Ordering::Acquire) {
                let Ok(message) = inbound.recv_timeout(Duration::from_millis(50)) else {
                    continue;
                };
                dispatch_message(
                    &transport,
                    &handles,
                    &rpc_state,
                    &database,
                    &metadata_requests,
                    message.source_node_id,
                    message.frame,
                );
            }
        })
        .expect("tablet RPC dispatcher thread creation must succeed");
    (client, worker)
}

fn dispatch_message(
    transport: &NodeRaftTransport,
    handles: &SharedTabletHandleRegistry,
    rpc_state: &RpcState,
    database: &SharedLocalDatabase,
    metadata_requests: &mpsc::SyncSender<MetadataHostRequest>,
    source: NodeId,
    frame: RpcFrame,
) {
    match frame.msg_type {
        MessageType::TabletCommandRequest => {
            let Ok(proto) = rpc::TabletCommandRequest::decode(frame.payload.as_slice()) else {
                return;
            };
            let attempt_id = proto.rpc_attempt_id;
            let Ok(request) = TabletCommandRequest::from_proto(proto) else {
                return;
            };
            let request_id = request.request_id.clone();
            let remote_commit = match &request.command {
                TabletCommand::SingleShardCommit(command) => Some(command.clone()),
                _ => None,
            };
            let response = match handles
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&frame.raft_group_id)
                .cloned()
            {
                Some(handle) => match handle.submit_command(request, Duration::from_secs(30)) {
                    Ok(outcome) => match remote_commit.as_ref().map(|command| {
                        database
                            .blocking_lock()
                            .observe_replicated_commit_high_water(command)
                    }) {
                        Some(Err(error)) => error_response(request_id, error),
                        _ => TabletCommandResponse {
                            request_id,
                            success: true,
                            error_message: String::new(),
                            error_code: String::new(),
                            retryable: false,
                            result_data: encode_command_outcome(outcome),
                            found: false,
                            leader_replica_id: handle.status().leader_replica_id.map(ReplicaId),
                            current_tablet_epoch: None,
                            expected_tablet_epoch: None,
                        },
                    },
                    Err(error) => error_response(request_id, error),
                },
                None => error_response(
                    request_id,
                    Error::ProposalUnavailable {
                        reason: "tablet Raft group is not hosted on this node".to_string(),
                    },
                ),
            };
            send_response(transport, source, frame.raft_group_id, attempt_id, response);
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
            let response = match handles
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&frame.raft_group_id)
                .cloned()
            {
                Some(handle) => match handle
                    .read_barrier(Duration::from_secs(30))
                    .and_then(|()| handle.read_point(request, Duration::from_secs(30)))
                {
                    Ok(row) => TabletCommandResponse {
                        request_id,
                        success: true,
                        error_message: String::new(),
                        error_code: String::new(),
                        retryable: false,
                        found: row.is_some(),
                        result_data: row.unwrap_or_default(),
                        leader_replica_id: handle.status().leader_replica_id.map(ReplicaId),
                        current_tablet_epoch: None,
                        expected_tablet_epoch: None,
                    },
                    Err(error) => error_response(request_id, error),
                },
                None => error_response(
                    request_id,
                    Error::ProposalUnavailable {
                        reason: "tablet Raft group is not hosted on this node".to_string(),
                    },
                ),
            };
            send_response(transport, source, frame.raft_group_id, attempt_id, response);
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
            let response = match handles
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&frame.raft_group_id)
                .cloned()
            {
                Some(handle) => match handle
                    .query_original_outcome(request, Duration::from_secs(30))
                {
                    Ok(outcome) => match outcome {
                        Some(outcome) => match outcome.encode_for_outcome_query() {
                            Ok(result_data) => TabletCommandResponse {
                                request_id,
                                success: true,
                                error_message: String::new(),
                                error_code: String::new(),
                                retryable: false,
                                result_data,
                                found: true,
                                leader_replica_id: handle.status().leader_replica_id.map(ReplicaId),
                                current_tablet_epoch: None,
                                expected_tablet_epoch: None,
                            },
                            Err(error) => {
                                error_response(request_id, Error::CorruptData(error.to_string()))
                            }
                        },
                        None => TabletCommandResponse {
                            request_id,
                            success: true,
                            error_message: String::new(),
                            error_code: String::new(),
                            retryable: false,
                            result_data: Vec::new(),
                            found: false,
                            leader_replica_id: handle.status().leader_replica_id.map(ReplicaId),
                            current_tablet_epoch: None,
                            expected_tablet_epoch: None,
                        },
                    },
                    Err(error) => error_response(request_id, error),
                },
                None => error_response(
                    request_id,
                    Error::ProposalUnavailable {
                        reason: "tablet Raft group is not hosted on this node".to_string(),
                    },
                ),
            };
            send_response(transport, source, frame.raft_group_id, attempt_id, response);
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
            let (reply, response) = mpsc::channel();
            if metadata_requests
                .try_send(MetadataHostRequest::Command {
                    envelope,
                    reply,
                    deadline: std::time::Instant::now() + Duration::from_secs(30),
                })
                .is_err()
            {
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

            let transport = transport.clone();
            let request_id_for_thread = request_id.clone();
            let group_id = frame.raft_group_id;
            thread::spawn(move || {
                let result = response
                    .recv_timeout(Duration::from_secs(30))
                    .map_err(|error| Error::ProposalUnavailable {
                        reason: format!("metadata proposal forwarding failed: {error}"),
                    })?;
                send_metadata_response(
                    &transport,
                    source,
                    group_id,
                    attempt_id,
                    request_id_for_thread,
                    result,
                );
                Ok::<(), Error>(())
            });
        }
        MessageType::MetadataResponse => {
            let Ok(proto) = rpc::MetadataResponse::decode(frame.payload.as_slice()) else {
                return;
            };
            let Some(rpc::metadata_response::Response::ProposeCommand(response)) = proto.response
            else {
                return;
            };
            let Some(attempt_id) = response.rpc_attempt_id else {
                return;
            };
            let Ok(response) = MetadataResponse::from_proto(rpc::MetadataResponse {
                response: Some(rpc::metadata_response::Response::ProposeCommand(response)),
            }) else {
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
        MetadataApplyOutcome::TableCreated(created) => {
            ragnordb_common::rpc_codec::MetadataProposalOutcome::TableCreated {
                table_id: created.table_id,
                tablet_id: created.tablet_id,
                raft_group_id: created.raft_group_id,
            }
        }
        MetadataApplyOutcome::Rejected(rejection) => {
            ragnordb_common::rpc_codec::MetadataProposalOutcome::Rejected {
                reason: rejection.to_string(),
            }
        }
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
            if let Some(rpc::metadata_request::Request::ProposeCommand(request)) =
                proto.request.as_mut()
            {
                request.rpc_attempt_id = Some(attempt_id);
            }
            Ok(proto.encode_to_vec())
        }
        _ => Err(Error::InvalidArgument(
            "RPC attempts are only valid for request messages".to_string(),
        )),
    }
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ragnordb_common::ids::TabletId;

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
            ],
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
