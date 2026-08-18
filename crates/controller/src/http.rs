use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::{
    ClusterMetadata, ControllerCommandResult, ControllerErrorBody, ControllerNode,
    ControllerRaftMetrics, ControllerRaftTypeConfig, MetadataCommand, NodeId,
};

const MAX_RAFT_HTTP_BODY: usize = 64 * 1024 * 1024;

pub(crate) fn router(node: Arc<ControllerNode>) -> Router {
    Router::new()
        // Stable external controller API.
        .route("/api/v1/controller/command", post(command))
        .route("/api/v1/controller/metadata", get(metadata))
        .route("/api/v1/controller/raft", get(raft_metrics))
        .route("/api/v1/controller/bootstrap", post(bootstrap))
        .route(
            "/api/v1/controller/trigger-election",
            post(trigger_election),
        )
        .route(
            "/api/v1/controller/trigger-snapshot",
            post(trigger_snapshot),
        )
        // OpenRaft's internal HTTP RPC transport.
        .route("/raft-vote", post(raft_vote))
        .route("/raft-append", post(raft_append))
        .route("/raft-snapshot", post(raft_snapshot))
        .layer(DefaultBodyLimit::max(MAX_RAFT_HTTP_BODY))
        .with_state(node)
}

async fn command(
    State(node): State<Arc<ControllerNode>>,
    Json(command): Json<MetadataCommand>,
) -> Json<ControllerCommandResult> {
    Json(node.write_metadata(command).await)
}

async fn metadata(
    State(node): State<Arc<ControllerNode>>,
) -> Result<Json<ClusterMetadata>, (StatusCode, Json<ControllerErrorBody>)> {
    node.local_metadata()
        .await
        .map(Json)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, Json(error)))
}

async fn raft_metrics(State(node): State<Arc<ControllerNode>>) -> Json<ControllerRaftMetrics> {
    Json(node.raft_metrics())
}

async fn bootstrap(
    State(node): State<Arc<ControllerNode>>,
) -> Json<Result<(), ControllerErrorBody>> {
    Json(node.bootstrap().await)
}

async fn trigger_election(
    State(node): State<Arc<ControllerNode>>,
) -> Json<Result<(), ControllerErrorBody>> {
    Json(
        node.raft()
            .trigger()
            .elect()
            .await
            .map_err(|error| ControllerErrorBody {
                code: "raft_trigger".to_owned(),
                message: error.to_string(),
                leader_id: node.raft_metrics().current_leader,
                retryable: true,
            }),
    )
}

async fn trigger_snapshot(
    State(node): State<Arc<ControllerNode>>,
) -> Json<Result<(), ControllerErrorBody>> {
    Json(
        node.raft()
            .trigger()
            .snapshot()
            .await
            .map_err(|error| ControllerErrorBody {
                code: "raft_trigger".to_owned(),
                message: error.to_string(),
                leader_id: node.raft_metrics().current_leader,
                retryable: true,
            }),
    )
}

async fn raft_vote(
    State(node): State<Arc<ControllerNode>>,
    Json(request): Json<VoteRequest<NodeId>>,
) -> Json<Result<VoteResponse<NodeId>, RaftError<NodeId>>> {
    Json(node.raft().vote(request).await)
}

async fn raft_append(
    State(node): State<Arc<ControllerNode>>,
    Json(request): Json<AppendEntriesRequest<ControllerRaftTypeConfig>>,
) -> Json<Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>> {
    Json(node.raft().append_entries(request).await)
}

async fn raft_snapshot(
    State(node): State<Arc<ControllerNode>>,
    Json(request): Json<InstallSnapshotRequest<ControllerRaftTypeConfig>>,
) -> Json<Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>> {
    Json(node.raft().install_snapshot(request).await)
}
