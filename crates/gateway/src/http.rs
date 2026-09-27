//! Operational endpoints, on their own listener so they stay reachable
//! (and private) whatever the WebSocket side is doing.
//!
//! - `/healthz`: the process is alive. Restart it if this fails.
//! - `/readyz`: the broker is reachable and the gateway is not draining.
//!   Take it out of the load balancer if this fails; do not restart it.
//! - `/metrics`: Prometheus text format.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;

use crate::gateway::SharedView;

pub async fn serve(listener: tokio::net::TcpListener, shared: Arc<SharedView>) {
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(shared);
    if let Err(error) = axum::serve(listener, app).await {
        tracing::error!(%error, "metrics listener failed");
    }
}

async fn readyz(State(shared): State<Arc<SharedView>>) -> impl IntoResponse {
    if shared.ready() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

async fn metrics(State(shared): State<Arc<SharedView>>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        shared.metrics().render(shared.ready()),
    )
}
