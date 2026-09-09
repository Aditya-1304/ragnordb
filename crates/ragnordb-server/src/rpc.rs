//! Tablet RPC gateway and node-local request dispatcher.
//!
//! The physical transport only authenticates the source node and multiplexes
//! frames. This module owns the next boundary: it validates the typed tablet
//! payload, resolves the group to a Ready-owner handle, and sends the result
//! back with the original request identity. Keeping this logic outside the
//! Raft host prevents network retries from bypassing proposal/apply ordering.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use prost::Message;
use ragnordb_common::{
    Error, Result,
    command_codec::TabletCommand,
    ids::{NodeId, RaftGroupId, ReplicaId, RequestId, RowKey, TableId, Timestamp},
    metadata_codec::DesiredReplicaRole,
    proto::rpc,
    rpc_codec::{
        MessageType, ReplicaRoute, RpcFrame, TabletCommandRequest, TabletCommandResponse,
        TabletReadRequest, TabletRoute,
    },
};
use ragnordb_exec::TabletGateway;
use ragnordb_multiraft::meta::MetadataRuntimeHandle;
use ragnordb_multiraft::transport::{NodeRaftTransport, NodeRpcInbound};
use ragnordb_tablet::TabletRouter;
use ragnordb_tablet::command::{TabletCommandApplyOutcome, TabletCommandApplyResult};

use crate::replicated_tablet::ReplicatedTabletHandle;

/// Group-qualified handles published by the lifecycle owner after a tablet
/// runtime has crossed its activation boundary.
pub type SharedTabletHandleRegistry =
    Arc<RwLock<BTreeMap<RaftGroupId, Arc<ReplicatedTabletHandle>>>>;

struct PendingResponse {
    target: NodeId,
    sender: mpsc::Sender<TabletCommandResponse>,
}

type PendingResponses = Arc<Mutex<BTreeMap<RequestId, PendingResponse>>>;

/// Client-side gateway for local and remote tablet operations.
#[derive(Clone)]
pub struct TabletRpcClient {
    transport: NodeRaftTransport,
    handles: SharedTabletHandleRegistry,
    pending: PendingResponses,
    metadata: MetadataRuntimeHandle,
}

impl TabletRpcClient {
    fn new(
        transport: NodeRaftTransport,
        handles: SharedTabletHandleRegistry,
        pending: PendingResponses,
        metadata: MetadataRuntimeHandle,
    ) -> Self {
        Self {
            transport,
            handles,
            pending,
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
        let leader_replica_id = placement
            .replicas
            .iter()
            .find(|replica| replica.role == DesiredReplicaRole::Voter)
            .or_else(|| placement.replicas.first())
            .map(|replica| replica.replica_id)
            .ok_or_else(|| Error::CorruptData("tablet placement has no replicas".to_string()))?;
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
            tablet_id: route.tablet_id,
            tablet_epoch: route.tablet_epoch,
            command,
        };
        let target = route
            .leader_node()
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        self.submit_command_to(target, route.raft_group_id, request, timeout)
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
        let request = TabletReadRequest {
            request_id,
            tablet_id: route.tablet_id,
            tablet_epoch: route.tablet_epoch,
            row_key,
            read_timestamp,
        };
        let target = route
            .leader_node()
            .map_err(|error| Error::InvalidArgument(error.to_string()))?;
        self.read_point_to(target, route.raft_group_id, request, timeout)
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
            return handle.submit_command(request, timeout);
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
        frame: RpcFrame,
        request_id: RequestId,
        timeout: Duration,
    ) -> Result<TabletCommandResponse> {
        let (sender, receiver) = mpsc::channel();
        {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| Error::ProposalUnavailable {
                    reason: "tablet RPC pending-response lock is poisoned".to_string(),
                })?;
            if pending
                .insert(request_id.clone(), PendingResponse { target, sender })
                .is_some()
            {
                return Err(Error::ProposalUnavailable {
                    reason: "tablet RPC request identity is already pending".to_string(),
                });
            }
        }

        if let Err(error) = self.transport.try_send_rpc(target, frame) {
            self.pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&request_id);
            return Err(Error::ProposalUnavailable {
                reason: format!("tablet RPC could not be queued: {error}"),
            });
        }

        receiver.recv_timeout(timeout).map_err(|_| {
            self.pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&request_id);
            Error::ProposalUnavailable {
                reason: "tablet RPC response deadline elapsed".to_string(),
            }
        })
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
}

/// Spawn the node-level dispatcher that services tablet request/response
/// frames. It is intentionally independent from the MultiRaft host loop: a
/// slow SQL request cannot consume the host's Raft turn budget, while the
/// bounded transport queue still provides admission backpressure.
pub(crate) fn spawn_dispatcher(
    inbound: NodeRpcInbound,
    transport: NodeRaftTransport,
    handles: SharedTabletHandleRegistry,
    metadata: MetadataRuntimeHandle,
    shutdown: Arc<AtomicBool>,
) -> (TabletRpcClient, thread::JoinHandle<()>) {
    let pending = Arc::new(Mutex::new(BTreeMap::new()));
    let client = TabletRpcClient::new(
        transport.clone(),
        handles.clone(),
        pending.clone(),
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
                    &pending,
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
    pending: &PendingResponses,
    source: NodeId,
    frame: RpcFrame,
) {
    match frame.msg_type {
        MessageType::TabletCommandRequest => {
            let Ok(proto) = rpc::TabletCommandRequest::decode(frame.payload.as_slice()) else {
                return;
            };
            let Ok(request) = TabletCommandRequest::from_proto(proto) else {
                return;
            };
            let request_id = request.request_id.clone();
            let response = match handles
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&frame.raft_group_id)
                .cloned()
            {
                Some(handle) => match handle.submit_command(request, Duration::from_secs(30)) {
                    Ok(outcome) => TabletCommandResponse {
                        request_id,
                        success: true,
                        error_message: String::new(),
                        error_code: String::new(),
                        retryable: false,
                        result_data: encode_command_outcome(outcome),
                        found: false,
                        leader_replica_id: handle.status().leader_replica_id.map(ReplicaId),
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
            send_response(transport, source, frame.raft_group_id, response);
        }
        MessageType::TabletReadRequest => {
            let Ok(proto) = rpc::TabletReadRequest::decode(frame.payload.as_slice()) else {
                return;
            };
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
            send_response(transport, source, frame.raft_group_id, response);
        }
        MessageType::TabletCommandResponse => {
            let Ok(proto) = rpc::TabletCommandResponse::decode(frame.payload.as_slice()) else {
                return;
            };
            let Ok(response) = TabletCommandResponse::from_proto(proto) else {
                return;
            };
            let request_id = response.request_id.clone();
            let mut pending_guard = pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let pending_response = pending_guard
                .get(&request_id)
                .is_some_and(|pending| pending.target == source)
                .then(|| pending_guard.remove(&request_id))
                .flatten();
            if let Some(pending_response) = pending_response {
                let _ = pending_response.sender.send(response);
            }
        }
        MessageType::RaftConsensus
        | MessageType::MetadataRequest
        | MessageType::MetadataResponse => {}
    }
}

fn send_response(
    transport: &NodeRaftTransport,
    target: NodeId,
    group_id: RaftGroupId,
    response: TabletCommandResponse,
) {
    let frame = RpcFrame {
        msg_type: MessageType::TabletCommandResponse,
        raft_group_id: group_id,
        payload: response.to_proto().encode_to_vec(),
    };
    let _ = transport.try_send_rpc(target, frame);
}

fn error_response(request_id: RequestId, error: Error) -> TabletCommandResponse {
    let (error_code, retryable, leader_replica_id) = match &error {
        Error::NotLeader { leader_id } => ("NOT_LEADER", true, leader_id.map(ReplicaId)),
        Error::ProposalUnavailable { .. } => ("PROPOSAL_UNAVAILABLE", true, None),
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
    }
}

fn response_error(response: TabletCommandResponse) -> Error {
    if response.error_code == "NOT_LEADER" {
        return Error::NotLeader {
            leader_id: response.leader_replica_id.map(|id| id.0),
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
}
