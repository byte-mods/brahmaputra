//! The gateway: authenticated WebSocket connections in, keyed records out.
//!
//! **Why it can hold a very large number of sockets.** Each connection is
//! one tokio task with a small read buffer and no per-connection broker
//! state: an idle socket costs a few kilobytes. All sockets share a small,
//! fixed pool of producers, so the broker sees `--producers` connections
//! per gateway instance, however many clients there are.
//!
//! **Why it cannot hurt the broker.** Messages from every socket are
//! batched per partition (`linger`, `batch-bytes`, compression), so the
//! broker's request rate follows partitions and time, not sockets. Memory
//! waiting on the broker is capped (`buffer-bytes`); when it is full a
//! publish fails fast with `OVERLOADED` instead of queueing, and each
//! connection has a bounded number of unacknowledged messages, past which
//! the gateway stops reading that socket and TCP slows the client down. A
//! slow cluster therefore produces pushback at the edge, never an
//! unbounded queue or a retry storm aimed at the broker. Broker produce
//! quotas keyed on `--client-id` cap the whole fleet from the broker side.
//!
//! **Why it scales without touching the broker.** Instances share nothing:
//! a connection's key picks its partition by the same `murmur2` every
//! Brahmaputra client uses, so any instance routes any user identically.
//! Add instances behind an L4 load balancer to add sockets.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use brahmaputra_client::{murmur2, ClientError, Consumer, Producer, ProducerConfig};
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::RecordHeader;
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::{Future, SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::WebSocketStream;
use tracing::{debug, info, warn};

use crate::auth::{Authenticator, Claims, SigningKey};
use crate::config::GatewayConfig;
use crate::metrics::Metrics;
use crate::protocol::{self, ErrorCode, Publish, ServerFrame, TopicPolicy, USER_HEADER};

/// Subprotocol a browser offers alongside `bearer.<token>`: browsers cannot
/// set an Authorization header on a WebSocket, and a handshake that offers
/// subprotocols must be answered with one of them.
pub const SUBPROTOCOL: &str = "brahmaputra.v1";
const MAX_SESSION_KEY_BYTES: usize = 1024;

struct Shared {
    config: GatewayConfig,
    auth: Authenticator,
    policy: TopicPolicy,
    producers: Vec<Arc<Producer>>,
    metrics: Metrics,
    broker_healthy: AtomicBool,
    shutting_down: AtomicBool,
    shutdown: watch::Sender<bool>,
    /// Signalled whenever a connection ends, so shutdown can wait for the
    /// last one without holding a handle per connection.
    connection_closed: Notify,
    ws_config: WebSocketConfig,
    tls: Option<tokio_rustls::TlsAcceptor>,
}

impl Shared {
    fn ready(&self) -> bool {
        self.broker_healthy.load(Relaxed) && !self.shutting_down.load(Relaxed)
    }
}

/// A started gateway. Dropping it does not stop it; call [`shutdown`].
///
/// [`shutdown`]: RunningGateway::shutdown
pub struct RunningGateway {
    pub ws_addr: SocketAddr,
    pub http_addr: SocketAddr,
    shared: Arc<Shared>,
    accept: JoinHandle<()>,
    http: JoinHandle<()>,
    health: JoinHandle<()>,
}

impl RunningGateway {
    pub fn metrics(&self) -> &Metrics {
        &self.shared.metrics
    }

    pub fn ready(&self) -> bool {
        self.shared.ready()
    }

    /// Graceful stop: refuse new upgrades and report not-ready at once (so
    /// a load balancer drains this instance), let every connection finish
    /// what it has in flight within the grace period, close them with
    /// 1001 "going away", then flush the producers.
    pub async fn shutdown(self) {
        let shared = &self.shared;
        shared.shutting_down.store(true, Relaxed);
        self.accept.abort();
        let _ = shared.shutdown.send(true);
        let deadline = tokio::time::Instant::now()
            + Duration::from_secs(shared.config.shutdown_grace_secs + 5);
        loop {
            let closed = shared.connection_closed.notified();
            if shared.metrics.connections_open.load(Relaxed) == 0 {
                break;
            }
            if tokio::time::timeout_at(deadline, closed).await.is_err() {
                warn!(
                    open = shared.metrics.connections_open.load(Relaxed),
                    "shutdown grace expired with connections still open"
                );
                break;
            }
        }
        for producer in &shared.producers {
            let _ = producer.flush().await;
        }
        self.health.abort();
        self.http.abort();
        info!("gateway stopped");
    }
}

pub async fn start(config: GatewayConfig) -> anyhow::Result<RunningGateway> {
    let keys = load_keys(&config)?;
    let auth = Authenticator::new(
        keys,
        config.jwt_issuer.clone(),
        config.jwt_audience.clone(),
        config.jwt_leeway_secs,
    );
    for pattern in &config.allowed_topics {
        if pattern.is_empty() {
            bail!("empty --allow-topic pattern");
        }
    }
    let policy = TopicPolicy::new(config.allowed_topics.clone());
    if let Some(topic) = &config.default_topic {
        protocol::validate_topic_name(topic).map_err(anyhow::Error::msg)?;
        if !policy.allows(topic) {
            bail!("--default-topic {topic} is not permitted by --allow-topic");
        }
    }
    if config.producers == 0 || config.max_inflight_per_connection == 0 {
        bail!("--producers and --max-inflight must be at least 1");
    }

    let broker = resolve_broker(&config).await?;
    let producer_config = ProducerConfig {
        client_id: config.client_id.clone(),
        batch_size: config.batch_bytes,
        linger_ms: config.linger_ms,
        compression: config.compression.into(),
        acks: config.acks.wire(),
        buffer_memory: config.buffer_bytes,
        max_block_ms: config.max_block_ms,
        delivery_timeout_ms: config.delivery_timeout_ms,
        ..ProducerConfig::default()
    };
    let mut producers = Vec::with_capacity(config.producers);
    for _ in 0..config.producers {
        producers.push(Arc::new(
            Producer::connect(broker, producer_config.clone())
                .await
                .with_context(|| format!("connecting a producer to {broker}"))?,
        ));
    }
    let health_consumer = Consumer::connect(broker, &format!("{}-health", config.client_id))
        .await
        .context("connecting the health checker")?;

    let ws_config = WebSocketConfig::default()
        .read_buffer_size(config.read_buffer_bytes.max(256))
        // Write each frame as it is produced: acks are small and latency
        // matters more than coalescing them.
        .write_buffer_size(0)
        // A client that never reads its acks cannot make us buffer without
        // limit: past this the write fails and the connection closes.
        .max_write_buffer_size(4 * 1024 * 1024)
        .max_message_size(Some(config.max_message_bytes))
        .max_frame_size(Some(config.max_message_bytes));

    let tls = match (&config.tls_cert, &config.tls_key) {
        (Some(cert), Some(key)) => Some(tls_acceptor(cert, key)?),
        (None, None) => None,
        _ => bail!("--tls-cert and --tls-key must be given together"),
    };

    let listener = bind(config.listen)?;
    let ws_addr = listener.local_addr()?;
    let http_listener = tokio::net::TcpListener::bind(config.http_listen)
        .await
        .with_context(|| format!("binding {}", config.http_listen))?;
    let http_addr = http_listener.local_addr()?;

    let (shutdown, _) = watch::channel(false);
    let shared = Arc::new(Shared {
        config,
        auth,
        policy,
        producers,
        metrics: Metrics::default(),
        broker_healthy: AtomicBool::new(true),
        shutting_down: AtomicBool::new(false),
        shutdown,
        connection_closed: Notify::new(),
        ws_config,
        tls,
    });

    let health = tokio::spawn(health_loop(shared.clone(), health_consumer));
    let http = tokio::spawn(crate::http::serve(http_listener, shared_view(&shared)));
    let accept = tokio::spawn(accept_loop(shared.clone(), listener));
    info!(%ws_addr, %http_addr, %broker, "gateway listening");
    Ok(RunningGateway {
        ws_addr,
        http_addr,
        shared,
        accept,
        http,
        health,
    })
}

/// What the HTTP endpoints may see: metrics and readiness, nothing else.
pub struct SharedView(Arc<Shared>);

impl SharedView {
    pub fn ready(&self) -> bool {
        self.0.ready()
    }
    pub fn metrics(&self) -> &Metrics {
        &self.0.metrics
    }
}

fn shared_view(shared: &Arc<Shared>) -> Arc<SharedView> {
    Arc::new(SharedView(shared.clone()))
}

fn load_keys(config: &GatewayConfig) -> anyhow::Result<Vec<SigningKey>> {
    let mut specs: Vec<String> = config.jwt_secrets.clone();
    if let Some(path) = &config.jwt_secret_file {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        specs.extend(
            text.lines()
                .map(str::trim)
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .map(str::to_owned),
        );
    }
    if specs.is_empty() {
        bail!("no signing secret: set --jwt-secret or --jwt-secret-file");
    }
    for spec in &specs {
        let secret = spec.split_once(':').map_or(spec.as_str(), |(_, s)| s);
        if secret.len() < 16 {
            bail!("JWT secrets must be at least 16 bytes");
        }
    }
    Ok(specs.iter().map(|s| SigningKey::parse(s)).collect())
}

async fn resolve_broker(config: &GatewayConfig) -> anyhow::Result<SocketAddr> {
    let mut last_error = None;
    for spec in &config.brokers {
        match tokio::net::lookup_host(spec.as_str()).await {
            Ok(addrs) => {
                for addr in addrs {
                    match Consumer::connect(addr, &config.client_id).await {
                        Ok(consumer) => match consumer.api_versions().await {
                            Ok(_) => return Ok(addr),
                            Err(e) => last_error = Some(anyhow::Error::from(e)),
                        },
                        Err(e) => last_error = Some(e.into()),
                    }
                }
            }
            Err(e) => last_error = Some(e.into()),
        }
    }
    Err(last_error
        .unwrap_or_else(|| anyhow::anyhow!("no brokers configured"))
        .context("no bootstrap broker reachable"))
}

/// Terminate TLS in the gateway itself (`wss://`). Terminating at the
/// load balancer instead is usually cheaper per socket; this is for
/// deployments without one.
fn tls_acceptor(
    cert: &std::path::Path,
    key: &std::path::Path,
) -> anyhow::Result<tokio_rustls::TlsAcceptor> {
    use std::io::BufReader;
    let chain = rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(cert).with_context(|| format!("opening {}", cert.display()))?,
    ))
    .collect::<Result<Vec<_>, _>>()
    .with_context(|| format!("reading certificates from {}", cert.display()))?;
    if chain.is_empty() {
        bail!("no certificates in {}", cert.display());
    }
    let key = rustls_pemfile::private_key(&mut BufReader::new(
        std::fs::File::open(key).with_context(|| format!("opening {}", key.display()))?,
    ))
    .with_context(|| format!("reading {}", key.display()))?
    .with_context(|| format!("no private key in {}", key.display()))?;
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .context("building the TLS configuration")?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

fn bind(addr: SocketAddr) -> anyhow::Result<tokio::net::TcpListener> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket
        .bind(addr)
        .with_context(|| format!("binding {addr}"))?;
    // A reconnect storm (a deploy, a network blip across a carrier) sends
    // many SYNs at once; a short backlog turns that into dropped handshakes.
    Ok(socket.listen(65_535)?)
}

async fn health_loop(shared: Arc<Shared>, consumer: Consumer) {
    let mut failures = 0u32;
    loop {
        let ok = tokio::time::timeout(Duration::from_secs(2), consumer.api_versions())
            .await
            .is_ok_and(|r| r.is_ok());
        failures = if ok { 0 } else { failures + 1 };
        // One missed probe is noise; three is a broker we should stop
        // accepting new clients for.
        let healthy = failures < 3;
        if healthy != shared.broker_healthy.swap(healthy, Relaxed) {
            if healthy {
                info!("broker reachable again; accepting connections");
            } else {
                warn!("broker unreachable; refusing new connections");
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn accept_loop(shared: Arc<Shared>, listener: tokio::net::TcpListener) {
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                shared.metrics.connections_open.fetch_add(1, Relaxed);
                let shared = shared.clone();
                tokio::spawn(async move {
                    let guard = OpenGuard(shared.clone());
                    serve(shared, stream, peer).await;
                    drop(guard);
                });
            }
            Err(error) => {
                // EMFILE/ENFILE: out of descriptors. Spinning on accept
                // would burn a core without freeing one; back off instead.
                warn!(%error, "accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

struct OpenGuard(Arc<Shared>);

impl Drop for OpenGuard {
    fn drop(&mut self) {
        self.0.metrics.connections_open.fetch_sub(1, Relaxed);
        self.0.connection_closed.notify_waiters();
    }
}

/// Everything decided at the handshake and fixed for the connection.
struct Session {
    user: String,
    topic: Option<String>,
    key: Bytes,
    /// A token's `topics` claim, when present.
    claim_policy: Option<TopicPolicy>,
    /// `?acks=errors` asks for error frames only.
    acks: bool,
}

impl Session {
    fn may_publish(&self, shared: &Shared, topic: &str) -> bool {
        shared.policy.allows(topic) && self.claim_policy.as_ref().is_none_or(|p| p.allows(topic))
    }
}

async fn serve(shared: Arc<Shared>, stream: TcpStream, peer: SocketAddr) {
    let _ = stream.set_nodelay(true);
    match shared.tls.clone() {
        None => upgrade(shared, stream, peer).await,
        Some(acceptor) => {
            // The TLS handshake shares the upgrade's deadline: a client
            // that stalls mid-handshake holds a socket and a task.
            let deadline = Duration::from_secs(shared.config.handshake_timeout_secs);
            match tokio::time::timeout(deadline, acceptor.accept(stream)).await {
                Ok(Ok(tls)) => upgrade(shared, tls, peer).await,
                Ok(Err(error)) => {
                    debug!(%peer, %error, "tls handshake failed");
                    shared
                        .metrics
                        .handshakes_rejected_other
                        .fetch_add(1, Relaxed);
                }
                Err(_) => {
                    shared
                        .metrics
                        .handshakes_rejected_other
                        .fetch_add(1, Relaxed);
                }
            }
        }
    }
}

// `ErrorResponse` is the handshake callback's error type, fixed by
// tungstenite; it is built once per refused upgrade, never per message.
#[allow(clippy::result_large_err)]
async fn upgrade<S>(shared: Arc<Shared>, stream: S, peer: SocketAddr)
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut session = None;
    let callback =
        |request: &Request, response: Response| match authenticate(&shared, request, response) {
            Ok((response, s)) => {
                session = Some(s);
                Ok(response)
            }
            Err(rejection) => Err(rejection),
        };
    let handshake =
        tokio_tungstenite::accept_hdr_async_with_config(stream, callback, Some(shared.ws_config));
    let ws = match tokio::time::timeout(
        Duration::from_secs(shared.config.handshake_timeout_secs),
        handshake,
    )
    .await
    {
        Ok(Ok(ws)) => ws,
        Ok(Err(error)) => {
            if session.is_none() {
                debug!(%peer, %error, "handshake failed");
            }
            return;
        }
        Err(_) => {
            shared
                .metrics
                .handshakes_rejected_other
                .fetch_add(1, Relaxed);
            return;
        }
    };
    let Some(session) = session else { return };
    shared.metrics.connections_total.fetch_add(1, Relaxed);
    debug!(%peer, user = %session.user, "connected");
    Connection::new(shared, session, ws).run().await;
}

fn reject(status: StatusCode, reason: &str) -> ErrorResponse {
    let mut response = ErrorResponse::new(Some(reason.to_owned()));
    *response.status_mut() = status;
    response
}

#[allow(clippy::result_large_err)]
fn authenticate(
    shared: &Shared,
    request: &Request,
    mut response: Response,
) -> Result<(Response, Session), ErrorResponse> {
    let m = &shared.metrics;
    let path = request.uri().path();
    if path != "/ws" && path != "/ws/" {
        m.handshakes_rejected_other.fetch_add(1, Relaxed);
        return Err(reject(StatusCode::NOT_FOUND, "connect to /ws"));
    }
    // Refuse before verifying anything: under a reconnect storm, spending
    // HMAC time on connections that will be refused anyway is waste.
    if !shared.ready() {
        m.handshakes_rejected_capacity.fetch_add(1, Relaxed);
        return Err(reject(StatusCode::SERVICE_UNAVAILABLE, "gateway not ready"));
    }
    if m.connections_open.load(Relaxed) > shared.config.max_connections as u64 {
        m.handshakes_rejected_capacity.fetch_add(1, Relaxed);
        return Err(reject(
            StatusCode::SERVICE_UNAVAILABLE,
            "gateway at capacity",
        ));
    }

    let query = protocol::parse_query(request.uri().query().unwrap_or(""));
    let mut answer_subprotocol = false;
    let token = if let Some(value) = request.headers().get("authorization") {
        let value = value.to_str().unwrap_or("");
        match value.split_once(' ') {
            Some((scheme, token)) if scheme.eq_ignore_ascii_case("bearer") => {
                Some(token.trim().to_owned())
            }
            _ => None,
        }
    } else if let Some(token) = query.get("access_token") {
        Some(token.clone())
    } else {
        let offered: Vec<&str> = request
            .headers()
            .get_all("sec-websocket-protocol")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .map(str::trim)
            .collect();
        let token = offered
            .iter()
            .find_map(|p| p.strip_prefix("bearer."))
            .map(str::to_owned);
        if token.is_some() {
            if !offered.contains(&SUBPROTOCOL) {
                m.handshakes_rejected_auth.fetch_add(1, Relaxed);
                return Err(reject(
                    StatusCode::BAD_REQUEST,
                    "offer the brahmaputra.v1 subprotocol alongside bearer.<token>",
                ));
            }
            answer_subprotocol = true;
        }
        token
    };
    let Some(token) = token else {
        m.handshakes_rejected_auth.fetch_add(1, Relaxed);
        return Err(reject(StatusCode::UNAUTHORIZED, "missing bearer token"));
    };
    let claims: Claims = shared.auth.verify(&token).map_err(|error| {
        m.handshakes_rejected_auth.fetch_add(1, Relaxed);
        reject(StatusCode::UNAUTHORIZED, &error.to_string())
    })?;
    let claim_policy = claims.topics.clone().map(TopicPolicy::new);

    let forbidden = |message: String| {
        m.handshakes_rejected_forbidden.fetch_add(1, Relaxed);
        reject(StatusCode::FORBIDDEN, &message)
    };
    let may = |topic: &str| {
        shared.policy.allows(topic) && claim_policy.as_ref().is_none_or(|p| p.allows(topic))
    };
    let topic = match query.get("topic") {
        Some(topic) => {
            protocol::validate_topic_name(topic).map_err(|e| {
                m.handshakes_rejected_other.fetch_add(1, Relaxed);
                reject(StatusCode::BAD_REQUEST, &e)
            })?;
            if !may(topic) {
                return Err(forbidden(format!("not permitted to publish to {topic}")));
            }
            Some(topic.clone())
        }
        // The configured default only applies if this token may use it.
        None => shared.config.default_topic.clone().filter(|t| may(t)),
    };
    let key = match query.get("key") {
        Some(key) if !key.is_empty() => {
            if key.len() > MAX_SESSION_KEY_BYTES {
                m.handshakes_rejected_other.fetch_add(1, Relaxed);
                return Err(reject(
                    StatusCode::BAD_REQUEST,
                    "key longer than 1024 bytes",
                ));
            }
            Bytes::from(key.clone())
        }
        _ => Bytes::from(claims.sub.clone()),
    };
    let acks = match query.get("acks").map(String::as_str) {
        None | Some("all") => true,
        Some("errors") => false,
        Some(other) => {
            m.handshakes_rejected_other.fetch_add(1, Relaxed);
            return Err(reject(
                StatusCode::BAD_REQUEST,
                &format!("acks must be all or errors, not {other}"),
            ));
        }
    };
    if answer_subprotocol {
        response.headers_mut().insert(
            "sec-websocket-protocol",
            HeaderValue::from_static(SUBPROTOCOL),
        );
    }
    Ok((
        response,
        Session {
            user: claims.sub,
            topic,
            key,
            claim_policy,
            acks,
        },
    ))
}

type ProduceFuture = Pin<Box<dyn Future<Output = Outcome> + Send>>;

struct Outcome {
    id: Option<u64>,
    result: Result<(String, i32, i64), Refusal>,
}

struct Refusal {
    code: ErrorCode,
    message: String,
    retryable: bool,
}

impl Refusal {
    fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Refusal {
            code,
            message: message.into(),
            retryable: code.retryable(),
        }
    }
}

/// A token bucket: `rate` per second sustained, `burst` at once.
struct RateLimit {
    rate: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl RateLimit {
    fn new(rate: f64, burst: f64) -> Self {
        RateLimit {
            rate,
            burst: burst.max(1.0),
            tokens: burst.max(1.0),
            last: Instant::now(),
        }
    }

    fn allow(&mut self) -> bool {
        if self.rate <= 0.0 {
            return true;
        }
        let now = Instant::now();
        let refill = now.duration_since(self.last).as_secs_f64() * self.rate;
        self.tokens = (self.tokens + refill).min(self.burst);
        self.last = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

struct Connection<S> {
    shared: Arc<Shared>,
    session: Session,
    ws: WebSocketStream<S>,
    inflight: FuturesUnordered<ProduceFuture>,
    limiter: RateLimit,
    binary_seq: u64,
    last_seen: Instant,
}

enum Step {
    Continue,
    Close(CloseCode, &'static str),
    Gone,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> Connection<S> {
    fn new(shared: Arc<Shared>, session: Session, ws: WebSocketStream<S>) -> Self {
        let limiter = RateLimit::new(
            shared.config.rate_limit_per_sec,
            shared.config.rate_limit_burst,
        );
        Connection {
            shared,
            session,
            ws,
            inflight: FuturesUnordered::new(),
            limiter,
            binary_seq: 0,
            last_seen: Instant::now(),
        }
    }

    async fn run(mut self) {
        let welcome = ServerFrame::Welcome {
            user: &self.session.user,
            topic: self.session.topic.as_deref(),
            key: std::str::from_utf8(&self.session.key).unwrap_or(""),
            max_message_bytes: self.shared.config.max_message_bytes,
            max_inflight: self.shared.config.max_inflight_per_connection,
        }
        .to_json();
        if !self.write(Message::text(welcome)).await {
            return;
        }
        let config = &self.shared.config;
        let ping_every = Duration::from_secs(config.ping_interval_secs.max(1));
        let idle_after = Duration::from_secs(config.idle_timeout_secs.max(1));
        let max_inflight = config.max_inflight_per_connection;
        let grace = Duration::from_secs(config.shutdown_grace_secs);
        let mut ping =
            tokio::time::interval_at(tokio::time::Instant::now() + ping_every, ping_every);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut shutdown = self.shared.shutdown.subscribe();
        let mut draining = *shutdown.borrow();
        let mut drain_deadline = tokio::time::Instant::now() + grace;

        let end = loop {
            if draining && self.inflight.is_empty() {
                break Step::Close(CloseCode::Away, "server shutting down");
            }
            let can_read = !draining && self.inflight.len() < max_inflight;
            let step = tokio::select! {
                biased;
                changed = shutdown.changed(), if !draining => {
                    if changed.is_err() || *shutdown.borrow() {
                        draining = true;
                        drain_deadline = tokio::time::Instant::now() + grace;
                    }
                    Step::Continue
                }
                Some(outcome) = self.inflight.next(), if !self.inflight.is_empty() => {
                    self.shared.metrics.inflight.fetch_sub(1, Relaxed);
                    self.report(outcome).await
                }
                frame = self.ws.next(), if can_read => self.on_frame(frame).await,
                _ = ping.tick() => {
                    if self.last_seen.elapsed() >= idle_after {
                        self.shared.metrics.closed_idle.fetch_add(1, Relaxed);
                        Step::Close(CloseCode::Normal, "idle timeout")
                    } else if self.last_seen.elapsed() >= ping_every
                        && !self.write(Message::Ping(Bytes::new())).await
                    {
                        Step::Gone
                    } else {
                        Step::Continue
                    }
                }
                _ = tokio::time::sleep_until(drain_deadline), if draining => {
                    Step::Close(CloseCode::Away, "server shutting down")
                }
            };
            match step {
                Step::Continue => {}
                other => break other,
            }
        };
        // Messages still in flight belong to a client that is leaving; let
        // them finish so their inflight accounting is released, without
        // writing acks nobody will read.
        let leftover = self.inflight.len() as u64;
        if leftover > 0 {
            let inflight = std::mem::take(&mut self.inflight);
            let shared = self.shared.clone();
            tokio::spawn(async move {
                inflight.for_each(|_| async {}).await;
                shared.metrics.inflight.fetch_sub(leftover, Relaxed);
            });
        }
        if let Step::Close(code, reason) = end {
            let frame = CloseFrame {
                code,
                reason: reason.into(),
            };
            let _ = tokio::time::timeout(Duration::from_secs(2), self.ws.close(Some(frame))).await;
        }
    }

    async fn on_frame(&mut self, frame: Option<Result<Message, WsError>>) -> Step {
        let message = match frame {
            None => return Step::Gone,
            Some(Ok(message)) => message,
            Some(Err(WsError::Capacity(_))) => {
                return Step::Close(CloseCode::Size, "message too big");
            }
            Some(Err(WsError::Protocol(_))) => {
                return Step::Close(CloseCode::Protocol, "protocol error");
            }
            Some(Err(_)) => return Step::Gone,
        };
        self.last_seen = Instant::now();
        match message {
            Message::Text(text) => {
                self.count_received(text.len());
                match protocol::parse_text(&text) {
                    Ok(publish) => self.publish(publish).await,
                    Err(message) => {
                        self.shared
                            .metrics
                            .rejected_bad_request
                            .fetch_add(1, Relaxed);
                        let id = serde_json::from_str::<serde_json::Value>(&text)
                            .ok()
                            .and_then(|v| v.get("id").and_then(|id| id.as_u64()));
                        self.refuse(id, Refusal::new(ErrorCode::BadRequest, message))
                            .await
                    }
                }
            }
            Message::Binary(value) => {
                self.count_received(value.len());
                self.binary_seq += 1;
                let publish = Publish {
                    id: Some(self.binary_seq),
                    topic: None,
                    key: None,
                    value: Some(value),
                    headers: Vec::new(),
                };
                self.publish(publish).await
            }
            Message::Close(_) => Step::Gone,
            // Pings are answered by the library; any frame proves liveness.
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => Step::Continue,
        }
    }

    fn count_received(&self, bytes: usize) {
        let m = &self.shared.metrics;
        m.messages_received.fetch_add(1, Relaxed);
        m.bytes_received.fetch_add(bytes as u64, Relaxed);
    }

    async fn publish(&mut self, publish: Publish) -> Step {
        let m = &self.shared.metrics;
        if !self.limiter.allow() {
            m.rejected_rate_limited.fetch_add(1, Relaxed);
            let refusal = Refusal::new(ErrorCode::RateLimited, "slow down");
            return self.refuse(publish.id, refusal).await;
        }
        let Some(topic) = publish.topic.or_else(|| self.session.topic.clone()) else {
            m.rejected_bad_request.fetch_add(1, Relaxed);
            let refusal = Refusal::new(
                ErrorCode::BadRequest,
                "no topic: name one in the message or connect with ?topic=",
            );
            return self.refuse(publish.id, refusal).await;
        };
        if !self.session.may_publish(&self.shared, &topic) {
            m.rejected_topic.fetch_add(1, Relaxed);
            let refusal = Refusal::new(
                ErrorCode::TopicNotAllowed,
                format!("not permitted to publish to {topic}"),
            );
            return self.refuse(publish.id, refusal).await;
        }
        let key = publish.key.unwrap_or_else(|| self.session.key.clone());
        let mut headers: Vec<RecordHeader> = publish
            .headers
            .into_iter()
            .map(|(key, value)| RecordHeader { key, value })
            .collect();
        if self.shared.config.user_header {
            headers.push(RecordHeader {
                key: USER_HEADER.to_owned(),
                value: Some(Bytes::from(self.session.user.clone())),
            });
        }
        // The key picks the producer as well as the partition, so every
        // record for a key leaves through one connection in send order.
        let producers = &self.shared.producers;
        let producer = producers[(murmur2(&key) as usize) % producers.len()].clone();
        let shared = self.shared.clone();
        let id = publish.id;
        let value = publish.value;
        m.inflight.fetch_add(1, Relaxed);
        self.inflight.push(Box::pin(async move {
            let started = Instant::now();
            let result = async {
                let partition = producer.partition_for(&topic, Some(&key)).await?;
                let offset = producer
                    .send_with_headers(&topic, Some(partition), Some(key), value, headers)
                    .await?;
                Ok::<_, ClientError>((partition, offset))
            }
            .await;
            let result = match result {
                Ok((partition, offset)) => {
                    shared.metrics.messages_produced.fetch_add(1, Relaxed);
                    shared
                        .metrics
                        .observe_produce(started.elapsed().as_micros() as u64);
                    Ok((topic, partition, offset))
                }
                Err(error) => {
                    let refusal = classify(&error);
                    if refusal.code == ErrorCode::Overloaded {
                        shared.metrics.rejected_overloaded.fetch_add(1, Relaxed);
                    } else {
                        shared.metrics.produce_errors.fetch_add(1, Relaxed);
                    }
                    Err(refusal)
                }
            };
            Outcome { id, result }
        }));
        Step::Continue
    }

    async fn report(&mut self, outcome: Outcome) -> Step {
        match outcome.result {
            Ok((topic, partition, offset)) => {
                let Some(id) = outcome.id else {
                    return Step::Continue;
                };
                if !self.session.acks {
                    return Step::Continue;
                }
                let ack = ServerFrame::Ack {
                    id,
                    topic: &topic,
                    partition,
                    offset,
                }
                .to_json();
                if self.write(Message::text(ack)).await {
                    Step::Continue
                } else {
                    Step::Gone
                }
            }
            Err(refusal) => self.refuse(outcome.id, refusal).await,
        }
    }

    async fn refuse(&mut self, id: Option<u64>, refusal: Refusal) -> Step {
        let frame = ServerFrame::Error {
            id,
            code: refusal.code.as_str(),
            message: &refusal.message,
            retryable: refusal.retryable,
        }
        .to_json();
        if self.write(Message::text(frame)).await {
            Step::Continue
        } else {
            Step::Gone
        }
    }

    /// Send one frame, or give up on a client that is not reading.
    async fn write(&mut self, message: Message) -> bool {
        let timeout = Duration::from_secs(self.shared.config.write_timeout_secs.max(1));
        match tokio::time::timeout(timeout, self.ws.send(message)).await {
            Ok(Ok(())) => true,
            Ok(Err(_)) => false,
            Err(_) => {
                self.shared.metrics.closed_slow_reader.fetch_add(1, Relaxed);
                false
            }
        }
    }
}

/// Turn a producer failure into what the client is told.
fn classify(error: &ClientError) -> Refusal {
    match error {
        // max.block.ms expired waiting for buffer space: the cluster is
        // slower than the edge. The client should back off and retry.
        ClientError::Timeout(message) => Refusal::new(
            ErrorCode::Overloaded,
            format!("broker backlog; retry later ({message})"),
        ),
        ClientError::Io(_) | ClientError::ConnectionClosed => Refusal {
            code: ErrorCode::BrokerError,
            message: error.to_string(),
            retryable: true,
        },
        ClientError::Server { code, message } => Refusal {
            code: ErrorCode::BrokerError,
            message: format!("broker error {code}: {message}"),
            // Negative codes are the client library's own transport
            // failures (a refused or dropped connection), which a later
            // attempt can outlive.
            retryable: *code < 0
                || matches!(
                    *code,
                    ec::UNKNOWN_TOPIC_OR_PARTITION
                        | ec::NOT_LEADER_OR_FOLLOWER
                        | ec::NOT_ENOUGH_REPLICAS
                        | ec::FENCED_LEADER_EPOCH
                        | ec::UNKNOWN_LEADER_EPOCH
                        | ec::FENCED_BROKER_EPOCH
                        | ec::INTERNAL
                        | ec::LOG_DIR_OFFLINE
                ),
        },
        other => Refusal {
            code: ErrorCode::BrokerError,
            message: other.to_string(),
            retryable: false,
        },
    }
}
