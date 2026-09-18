use axum::{
    Json, Router,
    extract::{State, rejection::JsonRejection},
    http::{StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use ragnordb_common::durability::{DurabilityGate, NodeDurabilityState};
use ragnordb_common::{
    Error,
    ids::{NodeId, RaftGroupId, ReplicaId},
    metadata_codec::{DesiredReplicaRole, NodeLifecycle},
};
use ragnordb_multiraft::meta::MetadataRuntimeHandle;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::build_info::BUILD_INFO;
use crate::database::SharedLocalDatabase;
use crate::metrics;
use crate::multiraft_runtime::MetadataProposalClient;
use crate::node_lifecycle::{
    NodeDrainBlocker, NodeDrainGroupStatus, NodeDrainStatus, compute_node_drain_status,
};
use crate::replicated_tablet::ReplicatedTabletHandle;
use ragnordb_multiraft::host::{MultiRaftHostStatus, SharedMultiRaftHostStatus};

/// Thread-safe error type returned by administrative server tasks.
///
/// Tokio may move spawned futures and their outputs between worker threads.
/// Therefore, errors returned from a spawned task must implement `Send`.
pub type AdminError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Metadata publication and proposal channels used by the local admin API.
///
/// The control client never mutates metadata directly: all lifecycle changes
/// still cross the metadata Raft proposal and committed-apply boundaries.
#[derive(Clone)]
pub struct NodeLifecycleAdminState {
    pub node_id: NodeId,
    pub metadata: MetadataRuntimeHandle,
    pub control: MetadataProposalClient,
}

pub struct AdminState {
    pub started_at: u64,
    pub connection_semaphore: Arc<Semaphore>,
    pub max_connections: u32,
    pub durability_gate: DurabilityGate,
    pub database: SharedLocalDatabase,
    pub replicated_tablet: Option<Arc<ReplicatedTabletHandle>>,
    pub multiraft_status: Option<SharedMultiRaftHostStatus>,
    pub node_lifecycle: Option<NodeLifecycleAdminState>,
}

pub async fn start_admin_server(
    addr: SocketAddr,
    state: Arc<AdminState>,
    shutdown: CancellationToken,
) -> Result<(), AdminError> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    serve_admin(listener, state, shutdown).await
}

pub async fn serve_admin(
    listener: tokio::net::TcpListener,
    state: Arc<AdminState>,
    shutdown: CancellationToken,
) -> Result<(), AdminError> {
    let addr = listener.local_addr()?;

    let app = Router::new()
        .route("/metrics", get(handle_metrics))
        .route("/status", get(handle_status))
        .route("/status/groups", get(handle_multiraft_groups_status))
        .route(
            "/node/lifecycle",
            get(handle_node_lifecycle_status).post(handle_node_lifecycle),
        )
        .with_state(state);

    info!(admin_addr = %addr, "admin HTTP server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
            info!("admin HTTP server shutting down");
        })
        .await?;

    Ok(())
}

/// Return metrics using the Prometheus text exposition content type.
async fn handle_metrics() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        metrics::render_metrics(),
    )
}

/// Return structured node status as JSON.
async fn handle_status(State(state): State<Arc<AdminState>>) -> Json<serde_json::Value> {
    let active = state.max_connections as usize - state.connection_semaphore.available_permits();
    // Health and leadership diagnostics must remain available while a SQL
    // request owns the serialized database state. Storage details are omitted
    // for that sample instead of blocking the complete status response.
    let storage = state
        .database
        .try_lock()
        .ok()
        .map(|database| database.status());

    let durability = match state.durability_gate.state() {
        NodeDurabilityState::Healthy => {
            metrics::gauge_set("ragnordb_node_recovery_required", 0.0);
            serde_json::json!({
                "state": "healthy",
                "recovery_required": false,
            })
        }

        NodeDurabilityState::RecoveryRequired(failure) => {
            metrics::gauge_set("ragnordb_node_recovery_required", 1.0);
            serde_json::json!({
                "state": failure.kind().as_str(),
                "recovery_required": true,
                "reason": failure.reason(),
            })
        }
    };
    let replication = state.replicated_tablet.as_ref().map(|runtime| {
        let status = runtime.status();
        serde_json::json!({
            "role": status.role.map(|role| role.as_str()),
            "leader_replica_id": status.leader_replica_id,
            "term": status.term,
            "commit_index": status.commit_index,
            "last_log_index": status.last_log_index,
            "applied_index": status.applied_index,
            "snapshot_index": status.snapshot_index,
            "uncommitted_bytes": status.uncommitted_bytes,
            "replication_inflight_bytes": status.replication_inflight_bytes,
            "is_leader": status.serving_leader,
            "runtime_error": status.runtime_error,
        })
    });
    let multiraft = state.multiraft_status.as_ref().map(|status_handle| {
        let status = status_handle
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let summary = status.bounded_summary(8);
        let top_groups = summary
            .top_groups
            .iter()
            .map(|group| {
                serde_json::json!({
                    "raft_group_id": group.identity.raft_group_id.0,
                    "replica_id": group.identity.replica_id.0,
                    "role": group.role.map(|role| role.as_str()),
                    "pending_messages": group.pending_messages,
                    "pending_message_bytes": group.pending_message_bytes,
                })
            })
            .collect::<Vec<_>>();

        serde_json::json!({
            "node_id": summary.node_id.0,
            "state": summary.state.as_str(),
            "pending_message_count": summary.pending_message_count,
            "pending_message_bytes": summary.pending_message_bytes,
            "pending_persistence_groups": summary.pending_persistence_groups,
            "pending_persistence_records": summary.pending_persistence_records,
            "pending_persistence_bytes": summary.pending_persistence_bytes,
            "group_count": summary.group_count,
            "leader_count": summary.leader_count,
            "candidate_count": summary.candidate_count,
            "quarantined_group_count": summary.quarantined_group_count,
            "top_groups": top_groups,
        })
    });
    let node_lifecycle = current_node_lifecycle_status(&state)
        .as_ref()
        .map(node_drain_status_json);

    Json(serde_json::json!({
        "build": {
            "version": BUILD_INFO.ragnordb_version,
            "target": BUILD_INFO.target,
            "built_at": BUILD_INFO.built_at,
            "rust_version": BUILD_INFO.rust_version,
            "features": BUILD_INFO.feature_flags,
        },
        "infra": {
            "raft": BUILD_INFO.raft_version,
            "raft_revision": BUILD_INFO.raft_revision,
            "wal": BUILD_INFO.wal_version,
            "wal_revision": BUILD_INFO.wal_revision,
            "bloom": BUILD_INFO.bloom_version,
            "bloom_revision": BUILD_INFO.bloom_revision,
        },
        "server": {
            "started_at": state.started_at,
            "max_connections": state.max_connections,
            "active_connections": active,
        },
        "durability": durability,
        "replication": replication,
        "multiraft": multiraft,
        "node_lifecycle": node_lifecycle,
        "storage": storage.map(|storage| serde_json::json!({
            "durable_lsn": storage.durable_lsn,
            "replay_frontier": storage.replay_frontier,
            "latest_checkpoint_id": storage.latest_checkpoint_id,
            "wal_retained_bytes": storage.wal_retained_bytes,
            "retention_pins_active": storage.retention_pins_active,
            "oldest_retention_pin_lsn": storage.oldest_retention_pin_lsn,
        })),
    }))
}

/// Return detailed per-group diagnostics only when explicitly requested.
///
/// The ordinary /status endpoint deliberately publishes only aggregate and
/// top-K queue pressure so its response size remains independent of tablet
/// count. Lifecycle and failover tooling uses this on-demand endpoint when it
/// needs full Raft progress vectors.
async fn handle_multiraft_groups_status(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    let Some(status_handle) = state.multiraft_status.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "ok": false,
                "error": "MultiRaft status is unavailable",
            })),
        );
    };
    let status = status_handle
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "multiraft": multiraft_detail_json(&status),
        })),
    )
}

fn multiraft_detail_json(status: &MultiRaftHostStatus) -> serde_json::Value {
    let groups = status
        .groups
        .iter()
        .map(|group| {
            serde_json::json!({
                "raft_group_id": group.identity.raft_group_id.0,
                "replica_id": group.identity.replica_id.0,
                "role": group.role.map(|role| role.as_str()),
                "leader_replica_id": group.leader_replica_id.map(|replica_id| replica_id.0),
                "term": group.term,
                "commit_index": group.commit_index,
                "last_log_index": group.last_log_index,
                "applied_index": group.applied_index,
                "snapshot_index": group.snapshot_index,
                "uncommitted_bytes": group.uncommitted_bytes,
                "replication_inflight_bytes": group.replication_inflight_bytes,
                "pending_work": group.pending_work,
                "apply_backlog_entries": group.apply_backlog_entries,
                "apply_backlog_bytes": group.apply_backlog_bytes,
                "apply_backlog_age_ms": group.apply_backlog_age_ms,
                "apply_backlog_generations": group.apply_backlog_generations,
                "pending_messages": group.pending_messages,
                "pending_message_bytes": group.pending_message_bytes,
                "quarantine_reason": group.quarantine_reason,
            })
        })
        .collect::<Vec<_>>();

    serde_json::json!({
        "node_id": status.node_id.0,
        "state": status.state.as_str(),
        "pending_message_count": status.pending_message_count,
        "pending_message_bytes": status.pending_message_bytes,
        "pending_persistence_groups": status.pending_persistence_groups,
        "pending_persistence_records": status.pending_persistence_records,
        "pending_persistence_bytes": status.pending_persistence_bytes,
        "groups": groups,
    })
}

async fn handle_node_lifecycle_status(State(state): State<Arc<AdminState>>) -> impl IntoResponse {
    match current_node_lifecycle_status(&state) {
        Some(status) => (StatusCode::OK, Json(node_drain_status_json(&status))),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "ok": false,
                "error": "metadata lifecycle status is unavailable",
            })),
        ),
    }
}

fn current_node_lifecycle_status(state: &AdminState) -> Option<NodeDrainStatus> {
    let node = state.node_lifecycle.as_ref()?;
    node_lifecycle_status_for(state, node.node_id)
}

fn node_lifecycle_status_for(state: &AdminState, node_id: NodeId) -> Option<NodeDrainStatus> {
    let node = state.node_lifecycle.as_ref()?;
    let metadata = node.metadata.state_snapshot();
    let host_status = state.multiraft_status.as_ref().map(|status_handle| {
        status_handle
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    });
    Some(compute_node_drain_status(
        &metadata,
        host_status.as_ref(),
        node_id,
    ))
}

#[derive(Debug, serde::Deserialize)]
struct SetNodeLifecycleRequest {
    node_id: u64,
    lifecycle: String,
}

async fn handle_node_lifecycle(
    State(state): State<Arc<AdminState>>,
    request: Result<Json<SetNodeLifecycleRequest>, JsonRejection>,
) -> impl IntoResponse {
    let Ok(Json(request)) = request else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": "request body must contain numeric node_id and lifecycle string",
            })),
        );
    };

    let Some(node) = state.node_lifecycle.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "ok": false,
                "error": "metadata lifecycle control is unavailable",
            })),
        );
    };

    let Some(lifecycle) = parse_node_lifecycle(&request.lifecycle) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": "lifecycle must be active, draining, decommissioning, decommissioned, or tombstoned",
            })),
        );
    };

    let node_id = NodeId(request.node_id);
    if node_id.0 == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": "node_id must be non-zero",
            })),
        );
    }
    if matches!(
        lifecycle,
        NodeLifecycle::Decommissioned | NodeLifecycle::Tombstoned
    ) {
        let preflight = node_lifecycle_status_for(&state, node_id).unwrap_or_else(|| {
            compute_node_drain_status(&node.metadata.state_snapshot(), None, node_id)
        });
        let ready = match lifecycle {
            NodeLifecycle::Decommissioned => preflight.ready_for_decommissioned,
            NodeLifecycle::Tombstoned => {
                preflight.lifecycle == Some(NodeLifecycle::Decommissioned)
                    && preflight.blockers.is_empty()
            }
            NodeLifecycle::Active | NodeLifecycle::Draining | NodeLifecycle::Decommissioning => {
                true
            }
        };
        if !ready {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "ok": false,
                    "node_id": node_id.0,
                    "lifecycle": node_lifecycle_name(lifecycle),
                    "error": "terminal lifecycle preflight is blocked",
                    "preflight": node_drain_status_json(&preflight),
                })),
            );
        }
    }

    let result = tokio::task::spawn_blocking(move || {
        node.control
            .set_node_lifecycle(node_id, lifecycle, Duration::from_secs(10))
    })
    .await;

    match result {
        Ok(Ok(outcome)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "ok": true,
                "node_id": node_id.0,
                "lifecycle": node_lifecycle_name(lifecycle),
                "outcome": metadata_outcome_name(&outcome),
            })),
        ),
        Ok(Err(error)) => {
            let status = if matches!(error, Error::ConstraintViolation(_)) {
                StatusCode::CONFLICT
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            (
                status,
                Json(serde_json::json!({
                    "ok": false,
                    "node_id": node_id.0,
                    "lifecycle": node_lifecycle_name(lifecycle),
                    "error": error.to_string(),
                })),
            )
        }
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "ok": false,
                "node_id": node_id.0,
                "error": format!("lifecycle proposal task failed: {error}"),
            })),
        ),
    }
}

fn parse_node_lifecycle(value: &str) -> Option<NodeLifecycle> {
    match value.trim().to_ascii_lowercase().as_str() {
        "active" => Some(NodeLifecycle::Active),
        "draining" => Some(NodeLifecycle::Draining),
        "decommissioning" => Some(NodeLifecycle::Decommissioning),
        "decommissioned" => Some(NodeLifecycle::Decommissioned),
        "tombstoned" => Some(NodeLifecycle::Tombstoned),
        _ => None,
    }
}

fn node_lifecycle_name(lifecycle: NodeLifecycle) -> &'static str {
    match lifecycle {
        NodeLifecycle::Active => "active",
        NodeLifecycle::Draining => "draining",
        NodeLifecycle::Decommissioning => "decommissioning",
        NodeLifecycle::Decommissioned => "decommissioned",
        NodeLifecycle::Tombstoned => "tombstoned",
    }
}

fn metadata_outcome_name(outcome: &ragnordb_catalog::MetadataApplyOutcome) -> &'static str {
    match outcome {
        ragnordb_catalog::MetadataApplyOutcome::Applied => "applied",
        ragnordb_catalog::MetadataApplyOutcome::AlreadyApplied => "already_applied",
        ragnordb_catalog::MetadataApplyOutcome::ClientRegistered { .. } => "client_registered",
        ragnordb_catalog::MetadataApplyOutcome::ClientRenewed => "client_renewed",
        ragnordb_catalog::MetadataApplyOutcome::TimestampsReserved { .. } => "timestamps_reserved",
        ragnordb_catalog::MetadataApplyOutcome::TableCreated(_) => "table_created",
        ragnordb_catalog::MetadataApplyOutcome::Rejected(_) => "rejected",
    }
}

fn node_drain_status_json(status: &NodeDrainStatus) -> serde_json::Value {
    serde_json::json!({
        "node_id": status.node_id.0,
        "lifecycle": status.lifecycle.map(node_lifecycle_name),
        "desired_replica_count": status.desired_replica_count,
        "hosted_group_count": status.hosted_group_count,
        "leader_group_count": status.leader_group_count,
        "ready_for_decommissioned": status.ready_for_decommissioned,
        "blockers": status.blockers.iter().map(node_drain_blocker_json).collect::<Vec<_>>(),
        "groups": status.groups.iter().map(node_drain_group_json).collect::<Vec<_>>(),
    })
}

fn node_drain_group_json(group: &NodeDrainGroupStatus) -> serde_json::Value {
    serde_json::json!({
        "raft_group_id": group.raft_group_id.0,
        "tablet_id": group.tablet_id.map(|tablet_id| tablet_id.0),
        "replica_id": group.replica_id.0,
        "desired_role": group.desired_role.map(desired_role_name),
        "local_group_present": group.local_group_present,
        "local_role": group.local_role.map(|role| role.as_str()),
        "local_leader": group.local_leader,
        "local_replica_in_committed_conf_state": group.local_replica_in_committed_conf_state,
        "replacement_candidates": group
            .replacement_candidates
            .iter()
            .map(|node_id| node_id.0)
            .collect::<Vec<_>>(),
        "blockers": group.blockers.iter().map(node_drain_blocker_json).collect::<Vec<_>>(),
    })
}

fn desired_role_name(role: DesiredReplicaRole) -> &'static str {
    match role {
        DesiredReplicaRole::Voter => "voter",
        DesiredReplicaRole::Learner => "learner",
    }
}

fn node_drain_blocker_json(blocker: &NodeDrainBlocker) -> serde_json::Value {
    match blocker {
        NodeDrainBlocker::NodeNotRegistered => serde_json::json!({
            "kind": "node_not_registered",
        }),
        NodeDrainBlocker::HostStatusUnavailable => serde_json::json!({
            "kind": "host_status_unavailable",
        }),
        NodeDrainBlocker::ReplicaStillDesired {
            raft_group_id,
            replica_id,
        } => replica_blocker_json("replica_still_desired", *raft_group_id, *replica_id),
        NodeDrainBlocker::ReplicaNotMaterialized {
            raft_group_id,
            replica_id,
        } => replica_blocker_json("replica_not_materialized", *raft_group_id, *replica_id),
        NodeDrainBlocker::LeaderTransferRequired {
            raft_group_id,
            replica_id,
        } => replica_blocker_json("leader_transfer_required", *raft_group_id, *replica_id),
        NodeDrainBlocker::PreferredLeaderOnDrainingNode { raft_group_id } => serde_json::json!({
            "kind": "preferred_leader_on_draining_node",
            "raft_group_id": raft_group_id.0,
        }),
        NodeDrainBlocker::ReplicaStillInCommittedConfState {
            raft_group_id,
            replica_id,
        } => replica_blocker_json(
            "replica_still_in_committed_conf_state",
            *raft_group_id,
            *replica_id,
        ),
        NodeDrainBlocker::JointConsensusInProgress { raft_group_id } => serde_json::json!({
            "kind": "joint_consensus_in_progress",
            "raft_group_id": raft_group_id.0,
        }),
        NodeDrainBlocker::NoEligibleReplacement {
            raft_group_id,
            replica_id,
        } => replica_blocker_json("no_eligible_replacement", *raft_group_id, *replica_id),
        NodeDrainBlocker::HostedReplicaNotInMetadata {
            raft_group_id,
            replica_id,
        } => replica_blocker_json(
            "hosted_replica_not_in_metadata",
            *raft_group_id,
            *replica_id,
        ),
    }
}

fn replica_blocker_json(
    kind: &'static str,
    raft_group_id: RaftGroupId,
    replica_id: ReplicaId,
) -> serde_json::Value {
    serde_json::json!({
        "kind": kind,
        "raft_group_id": raft_group_id.0,
        "replica_id": replica_id.0,
    })
}
