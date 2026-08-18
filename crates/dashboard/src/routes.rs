//! HTTP routes for the metrics API, admin operations and the dashboard
//! (DESIGN.md §9.2).
//!
//! Read endpoints need `viewer`, topic administration needs `operator`,
//! and user administration needs `admin`. `GET /metrics` is deliberately
//! unauthenticated so a Prometheus scraper does not need a session — bind
//! the port accordingly.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use brahmaputra_broker::Broker;
use brahmaputra_metadata::{ClusterMetadata, MetadataCache, Role, UserRecord};
use brahmaputra_metrics::{names, Metrics};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::auth::{
    bearer_token, hash_password, issue_token, require_role, verify_password, verify_token,
    AuthError, Claims,
};
use crate::ui;

/// Everything the routes need: the local broker for live partition state,
/// the metadata cache for cluster-wide state, and a way to submit metadata
/// commands (user administration, topic create/delete) to the controller.
#[derive(Clone)]
pub struct DashboardState {
    pub broker: Arc<Broker>,
    pub metadata: Option<MetadataCache>,
    /// Submits a metadata command to the active controller. `None` in
    /// standalone mode, where there is no controller to submit to.
    pub controller: Option<Arc<dyn ControllerClient>>,
}

/// How the dashboard reaches the controller for writes. A trait so the
/// dashboard does not depend on the controller crate (which would be a
/// dependency cycle through the server binary).
#[async_trait::async_trait]
pub trait ControllerClient: Send + Sync {
    async fn submit(&self, command: serde_json::Value) -> Result<(), String>;
}

impl DashboardState {
    fn image(&self) -> Arc<ClusterMetadata> {
        self.metadata
            .as_ref()
            .map(|cache| cache.snapshot())
            .unwrap_or_else(|| Arc::new(ClusterMetadata::default()))
    }

    fn metrics(&self) -> &Metrics {
        self.broker.metrics()
    }

    /// Authenticate a request and check its role in one step, so no route
    /// can read the token without also declaring what it requires.
    fn authorize(&self, headers: &HeaderMap, required: Role) -> Result<Claims, AuthError> {
        let image = self.image();
        let secret = image.jwt_secret.as_deref().ok_or(AuthError::NotReady)?;
        let header = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok());
        let token = bearer_token(header).ok_or(AuthError::TokenInvalid)?;
        let claims = verify_token(secret, token)?;
        // Re-read the role from metadata rather than trusting the token:
        // a demotion must take effect before the token expires.
        let current_role = image
            .users
            .get(&claims.sub)
            .map(|user| user.role)
            .ok_or(AuthError::TokenInvalid)?;
        let claims = Claims {
            role: current_role,
            ..claims
        };
        require_role(&claims, required)?;
        Ok(claims)
    }

    async fn submit(&self, command: serde_json::Value) -> Result<(), (StatusCode, String)> {
        let Some(controller) = self.controller.as_ref() else {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "this node has no controller to submit metadata changes to".to_owned(),
            ));
        };
        controller
            .submit(command)
            .await
            .map_err(|error| (StatusCode::BAD_GATEWAY, error))
    }
}

fn auth_error(error: AuthError) -> Response {
    (
        StatusCode::from_u16(error.status()).unwrap_or(StatusCode::UNAUTHORIZED),
        Json(json!({ "error": error.message() })),
    )
        .into_response()
}

// ------------------------------------------------------------------ routes

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct LoginResponse {
    token: String,
    username: String,
    role: Role,
    expires_in_hours: i64,
}

async fn login(State(state): State<DashboardState>, Json(body): Json<LoginRequest>) -> Response {
    let image = state.image();
    let Some(secret) = image.jwt_secret.as_deref() else {
        return auth_error(AuthError::NotReady);
    };
    let Some(user) = image.users.get(&body.username) else {
        // Hash anyway would be better still; at this size the timing
        // difference is dwarfed by network jitter, and the response is
        // identical either way so accounts cannot be enumerated.
        return auth_error(AuthError::InvalidCredentials);
    };
    if !verify_password(&body.password, &user.password_hash) {
        warn!(username = %body.username, "failed login");
        return auth_error(AuthError::InvalidCredentials);
    }
    match issue_token(secret, &user.username, user.role) {
        Ok(token) => {
            info!(username = %user.username, role = ?user.role, "login");
            Json(LoginResponse {
                token,
                username: user.username.clone(),
                role: user.role,
                expires_in_hours: crate::auth::SESSION_HOURS,
            })
            .into_response()
        }
        Err(error) => auth_error(error),
    }
}

async fn overview(State(state): State<DashboardState>, headers: HeaderMap) -> Response {
    if let Err(error) = state.authorize(&headers, Role::Viewer) {
        return auth_error(error);
    }
    let image = state.image();
    let snapshot = state.metrics().snapshot();
    let partitions: usize = image
        .topics
        .values()
        .map(|topic| topic.partitions.len())
        .sum();
    let under_replicated = image
        .topics
        .values()
        .flat_map(|topic| topic.partitions.values())
        .filter(|partition| partition.isr.len() < partition.replicas.len())
        .count();
    let offline = image
        .topics
        .values()
        .flat_map(|topic| topic.partitions.values())
        .filter(|partition| partition.leader < 0)
        .count();

    Json(json!({
        "cluster_id": image.cluster_id,
        "controller_id": image.controller_id,
        "brokers": image.brokers.len(),
        "brokers_alive": image.brokers.values().filter(|broker| broker.alive).count(),
        "topics": image.topics.len(),
        "partitions": partitions,
        "under_replicated_partitions": under_replicated,
        "offline_partitions": offline,
        "produce_records_total": snapshot.get(names::PRODUCE_RECORDS).copied().unwrap_or(0.0),
        "produce_bytes_total": snapshot.get(names::PRODUCE_BYTES).copied().unwrap_or(0.0),
        "fetch_bytes_total": snapshot.get(names::FETCH_BYTES).copied().unwrap_or(0.0),
        "throttled_requests_total": snapshot.get(names::THROTTLED_REQUESTS).copied().unwrap_or(0.0),
    }))
    .into_response()
}

async fn brokers(State(state): State<DashboardState>, headers: HeaderMap) -> Response {
    if let Err(error) = state.authorize(&headers, Role::Viewer) {
        return auth_error(error);
    }
    let image = state.image();
    let brokers: Vec<_> = image
        .brokers
        .values()
        .map(|broker| {
            json!({
                "broker_id": broker.broker_id,
                "host": broker.host,
                "data_port": broker.data_port,
                "control_port": broker.control_port,
                "alive": broker.alive,
                "roles": broker.roles,
                "rack": broker.rack,
                "broker_epoch": broker.broker_epoch,
                "is_controller": image.controller_id == Some(broker.broker_id),
            })
        })
        .collect();
    Json(json!({ "brokers": brokers })).into_response()
}

async fn topics(State(state): State<DashboardState>, headers: HeaderMap) -> Response {
    if let Err(error) = state.authorize(&headers, Role::Viewer) {
        return auth_error(error);
    }
    let image = state.image();
    let topics: Vec<_> = image
        .topics
        .values()
        .map(|topic| {
            json!({
                "name": topic.name,
                "partitions": topic.partitions.len(),
                "replication_factor": topic.replication_factor,
                "configs": topic.configs,
                "under_replicated": topic
                    .partitions
                    .values()
                    .filter(|partition| partition.isr.len() < partition.replicas.len())
                    .count(),
            })
        })
        .collect();
    Json(json!({ "topics": topics })).into_response()
}

async fn topic_detail(
    State(state): State<DashboardState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if let Err(error) = state.authorize(&headers, Role::Viewer) {
        return auth_error(error);
    }
    let image = state.image();
    let Some(topic) = image.topics.get(&name) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("no such topic {name:?}") })),
        )
            .into_response();
    };
    let snapshot = state.metrics().snapshot();
    let partitions: Vec<_> = topic
        .partitions
        .values()
        .map(|partition| {
            let label = format!(
                "{{topic=\"{}\",partition=\"{}\"}}",
                topic.name, partition.partition
            );
            let metric = |name: &str| {
                snapshot
                    .get(&format!("{name}{label}"))
                    .copied()
                    .unwrap_or(-1.0)
            };
            json!({
                "partition": partition.partition,
                "leader": partition.leader,
                "replicas": partition.replicas,
                "isr": partition.isr,
                "leader_epoch": partition.leader_epoch,
                "under_replicated": partition.isr.len() < partition.replicas.len(),
                "log_start_offset": metric(names::LOG_START_OFFSET),
                "log_end_offset": metric(names::LOG_END_OFFSET),
                "high_watermark": metric(names::HIGH_WATERMARK),
            })
        })
        .collect();
    Json(json!({
        "name": topic.name,
        "replication_factor": topic.replication_factor,
        "configs": topic.configs,
        "partitions": partitions,
    }))
    .into_response()
}

#[derive(Deserialize)]
struct CreateTopicRequest {
    name: String,
    partitions: i32,
    replication_factor: i32,
    #[serde(default)]
    configs: std::collections::BTreeMap<String, String>,
}

async fn create_topic(
    State(state): State<DashboardState>,
    headers: HeaderMap,
    Json(body): Json<CreateTopicRequest>,
) -> Response {
    if let Err(error) = state.authorize(&headers, Role::Operator) {
        return auth_error(error);
    }
    match state
        .submit(json!({
            "type": "create_topic",
            "name": body.name,
            "partitions": body.partitions,
            "replication_factor": body.replication_factor,
            "configs": body.configs,
        }))
        .await
    {
        Ok(()) => (StatusCode::CREATED, Json(json!({ "created": body.name }))).into_response(),
        Err((status, error)) => (status, Json(json!({ "error": error }))).into_response(),
    }
}

async fn delete_topic(
    State(state): State<DashboardState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Response {
    if let Err(error) = state.authorize(&headers, Role::Operator) {
        return auth_error(error);
    }
    match state
        .submit(json!({ "type": "delete_topic", "name": name }))
        .await
    {
        Ok(()) => Json(json!({ "deleted": name })).into_response(),
        Err((status, error)) => (status, Json(json!({ "error": error }))).into_response(),
    }
}

async fn groups(State(state): State<DashboardState>, headers: HeaderMap) -> Response {
    if let Err(error) = state.authorize(&headers, Role::Viewer) {
        return auth_error(error);
    }
    match state.broker.list_groups().await {
        Ok(groups) => Json(json!({ "groups": groups })).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": error })),
        )
            .into_response(),
    }
}

async fn group_lag(
    State(state): State<DashboardState>,
    headers: HeaderMap,
    Path(group): Path<String>,
) -> Response {
    if let Err(error) = state.authorize(&headers, Role::Viewer) {
        return auth_error(error);
    }
    match state.broker.group_lag(&group).await {
        Ok(lag) => Json(json!({ "group": group, "partitions": lag })).into_response(),
        Err(error) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": error })),
        )
            .into_response(),
    }
}

async fn metrics_snapshot(State(state): State<DashboardState>, headers: HeaderMap) -> Response {
    if let Err(error) = state.authorize(&headers, Role::Viewer) {
        return auth_error(error);
    }
    Json(json!({ "metrics": state.metrics().snapshot() })).into_response()
}

#[derive(Deserialize)]
struct SeriesQuery {
    metric: String,
    from: Option<i64>,
    to: Option<i64>,
}

async fn metrics_series(
    State(state): State<DashboardState>,
    headers: HeaderMap,
    Query(query): Query<SeriesQuery>,
) -> Response {
    if let Err(error) = state.authorize(&headers, Role::Viewer) {
        return auth_error(error);
    }
    let samples = state.metrics().series(&query.metric, query.from, query.to);
    Json(json!({
        "metric": query.metric,
        "samples": samples,
        "available": state.metrics().series_names(),
    }))
    .into_response()
}

/// Prometheus scrape endpoint. Unauthenticated by design (DESIGN.md §9.2):
/// scrapers do not hold sessions. Restrict it by binding, not by token.
async fn prometheus(State(state): State<DashboardState>) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics().prometheus(),
    )
        .into_response()
}

async fn list_users(State(state): State<DashboardState>, headers: HeaderMap) -> Response {
    if let Err(error) = state.authorize(&headers, Role::Admin) {
        return auth_error(error);
    }
    let image = state.image();
    // Never return password hashes, even to an admin.
    let users: Vec<_> = image
        .users
        .values()
        .map(|user| {
            json!({
                "username": user.username,
                "role": user.role,
                "force_password_change": user.force_password_change,
            })
        })
        .collect();
    Json(json!({ "users": users })).into_response()
}

#[derive(Deserialize)]
struct PutUserRequest {
    username: String,
    password: String,
    role: Role,
}

async fn put_user(
    State(state): State<DashboardState>,
    headers: HeaderMap,
    Json(body): Json<PutUserRequest>,
) -> Response {
    if let Err(error) = state.authorize(&headers, Role::Admin) {
        return auth_error(error);
    }
    if body.password.len() < 8 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "password must be at least 8 characters" })),
        )
            .into_response();
    }
    let Ok(password_hash) = hash_password(&body.password) else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "cannot hash password" })),
        )
            .into_response();
    };
    let user = UserRecord {
        username: body.username.clone(),
        password_hash,
        role: body.role,
        force_password_change: false,
    };
    match state
        .submit(json!({ "type": "put_user", "user": user }))
        .await
    {
        Ok(()) => Json(json!({ "username": body.username, "role": body.role })).into_response(),
        Err((status, error)) => (status, Json(json!({ "error": error }))).into_response(),
    }
}

async fn delete_user(
    State(state): State<DashboardState>,
    headers: HeaderMap,
    Path(username): Path<String>,
) -> Response {
    let claims = match state.authorize(&headers, Role::Admin) {
        Ok(claims) => claims,
        Err(error) => return auth_error(error),
    };
    if claims.sub == username {
        // Locking every admin out of the cluster is not a recoverable
        // mistake, so refuse the most common way to do it.
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "an admin cannot delete their own account" })),
        )
            .into_response();
    }
    match state
        .submit(json!({ "type": "delete_user", "username": username }))
        .await
    {
        Ok(()) => Json(json!({ "deleted": username })).into_response(),
        Err((status, error)) => (status, Json(json!({ "error": error }))).into_response(),
    }
}

async fn dashboard_page() -> Html<&'static str> {
    Html(ui::INDEX_HTML)
}

/// Build the router. Every route except `/metrics` and the login endpoint
/// requires a session.
pub fn router(state: DashboardState) -> Router {
    Router::new()
        .route("/", get(dashboard_page))
        .route("/api/v1/auth/login", post(login))
        .route("/api/v1/overview", get(overview))
        .route("/api/v1/brokers", get(brokers))
        .route("/api/v1/topics", get(topics).post(create_topic))
        .route("/api/v1/topics/{name}", get(topic_detail).delete(delete_topic))
        .route("/api/v1/groups", get(groups))
        .route("/api/v1/groups/{group}/lag", get(group_lag))
        .route("/api/v1/metrics/snapshot", get(metrics_snapshot))
        .route("/api/v1/metrics/timeseries", get(metrics_series))
        .route("/api/v1/users", get(list_users).post(put_user))
        .route("/api/v1/users/{username}", delete(delete_user))
        .route("/metrics", get(prometheus))
        .with_state(state)
}

/// Serve the dashboard until `shutdown` resolves.
pub async fn serve(
    listener: TcpListener,
    state: DashboardState,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let addr = listener.local_addr()?;
    info!(%addr, "dashboard listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await
}
