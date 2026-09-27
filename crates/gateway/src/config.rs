//! Gateway configuration. Every flag has a `GW_*` environment variable, so
//! a container can be configured without a command line.

use std::net::SocketAddr;
use std::path::PathBuf;

use brahmaputra_protocol::Compression;
use clap::{Args, ValueEnum};

#[derive(Args, Debug, Clone)]
pub struct GatewayConfig {
    /// WebSocket listener. Clients connect to ws://<this>/ws.
    #[arg(long, env = "GW_LISTEN", default_value = "0.0.0.0:8090")]
    pub listen: SocketAddr,

    /// Health, readiness and Prometheus metrics (/healthz, /readyz,
    /// /metrics). Keep it off the public load balancer.
    #[arg(long, env = "GW_HTTP_LISTEN", default_value = "0.0.0.0:8091")]
    pub http_listen: SocketAddr,

    /// Brahmaputra bootstrap brokers, host:port, comma-separated. The
    /// first reachable one is used to discover the rest of the cluster.
    #[arg(
        long = "broker",
        env = "GW_BROKERS",
        value_delimiter = ',',
        required = true
    )]
    pub brokers: Vec<String>,

    /// HS256 signing secret, `secret` or `kid:secret`. Repeat to accept
    /// several at once while rotating.
    #[arg(long = "jwt-secret", env = "GW_JWT_SECRETS", value_delimiter = ',')]
    pub jwt_secrets: Vec<String>,

    /// File with one `secret` or `kid:secret` per line (preferred over the
    /// flag: it keeps secrets out of `ps` and the environment).
    #[arg(long, env = "GW_JWT_SECRET_FILE")]
    pub jwt_secret_file: Option<PathBuf>,

    /// Required `iss` claim, if set.
    #[arg(long, env = "GW_JWT_ISSUER")]
    pub jwt_issuer: Option<String>,

    /// Required `aud` claim, if set.
    #[arg(long, env = "GW_JWT_AUDIENCE")]
    pub jwt_audience: Option<String>,

    /// Clock skew tolerated on `exp` and `nbf`.
    #[arg(long, env = "GW_JWT_LEEWAY_SECS", default_value_t = 30)]
    pub jwt_leeway_secs: i64,

    /// Topic a connection publishes to when a message names none (and
    /// the only topic binary frames can go to). A client may choose its
    /// own with `?topic=` if the allow-list permits it.
    #[arg(long, env = "GW_DEFAULT_TOPIC")]
    pub default_topic: Option<String>,

    /// Topic patterns clients may publish to: `name`, `prefix.*` or `*`.
    /// A token's `topics` claim can narrow this, never widen it. Topics
    /// starting with `__` (the broker's internal ones) are always refused.
    #[arg(
        long = "allow-topic",
        env = "GW_ALLOWED_TOPICS",
        value_delimiter = ',',
        default_value = "*"
    )]
    pub allowed_topics: Vec<String>,

    /// Stop accepting upgrades above this many open connections (the
    /// process also needs a file-descriptor limit above it).
    #[arg(long, env = "GW_MAX_CONNECTIONS", default_value_t = 1_000_000)]
    pub max_connections: usize,

    /// Largest WebSocket message accepted; bigger closes with 1009.
    #[arg(long, env = "GW_MAX_MESSAGE_BYTES", default_value_t = 1024 * 1024)]
    pub max_message_bytes: usize,

    /// Read buffer allocated per connection. The WebSocket library's
    /// default is 128 KiB, which at a million sockets is 128 GB of buffers
    /// for connections that are mostly idle; larger messages still work,
    /// the buffer grows for them and they take more reads.
    #[arg(long, env = "GW_READ_BUFFER_BYTES", default_value_t = 4096)]
    pub read_buffer_bytes: usize,

    /// Messages one connection may have awaiting a broker acknowledgement.
    /// At the limit the gateway stops reading that socket, so a client
    /// faster than the cluster is slowed by TCP rather than buffered.
    #[arg(long, env = "GW_MAX_INFLIGHT", default_value_t = 64)]
    pub max_inflight_per_connection: usize,

    /// Sustained messages per second per connection (0 disables).
    #[arg(long, env = "GW_RATE_LIMIT", default_value_t = 1000.0)]
    pub rate_limit_per_sec: f64,

    /// Burst allowance on top of the sustained rate.
    #[arg(long, env = "GW_RATE_BURST", default_value_t = 2000.0)]
    pub rate_limit_burst: f64,

    /// Close a connection that sent nothing (not even a pong) for this long.
    #[arg(long, env = "GW_IDLE_TIMEOUT_SECS", default_value_t = 120)]
    pub idle_timeout_secs: u64,

    /// Ping a quiet connection this often, which also keeps NAT and
    /// mobile-carrier mappings alive.
    #[arg(long, env = "GW_PING_INTERVAL_SECS", default_value_t = 30)]
    pub ping_interval_secs: u64,

    /// Deadline for a client to finish the upgrade after connecting.
    #[arg(long, env = "GW_HANDSHAKE_TIMEOUT_SECS", default_value_t = 10)]
    pub handshake_timeout_secs: u64,

    /// Deadline for one outgoing frame; a client that stops reading is
    /// disconnected rather than allowed to pin memory.
    #[arg(long, env = "GW_WRITE_TIMEOUT_SECS", default_value_t = 10)]
    pub write_timeout_secs: u64,

    /// On SIGTERM, how long in-flight messages get to be acknowledged
    /// before connections are closed anyway.
    #[arg(long, env = "GW_SHUTDOWN_GRACE_SECS", default_value_t = 25)]
    pub shutdown_grace_secs: u64,

    /// Broker connections (producers) this instance holds, however many
    /// sockets it serves. A key always uses the same one, so its order
    /// holds across the pool.
    #[arg(long, env = "GW_PRODUCERS", default_value_t = 2)]
    pub producers: usize,

    /// Client id the gateway presents to the broker. Broker-side produce
    /// quotas match on it, so one quota caps the whole gateway fleet.
    #[arg(long, env = "GW_CLIENT_ID", default_value = "ws-gateway")]
    pub client_id: String,

    #[arg(long, env = "GW_ACKS", value_enum, default_value_t = AcksArg::Leader)]
    pub acks: AcksArg,

    /// How long records wait to be batched (`linger.ms`). Batching is what
    /// turns a million sockets' messages into a few requests per partition.
    #[arg(long, env = "GW_LINGER_MS", default_value_t = 5)]
    pub linger_ms: u64,

    #[arg(long, env = "GW_BATCH_BYTES", default_value_t = 64 * 1024)]
    pub batch_bytes: usize,

    #[arg(long, env = "GW_COMPRESSION", value_enum, default_value_t = CompressionArg::Lz4)]
    pub compression: CompressionArg,

    /// Unacknowledged record bytes held per producer (`buffer.memory`).
    #[arg(long, env = "GW_BUFFER_BYTES", default_value_t = 128 * 1024 * 1024)]
    pub buffer_bytes: usize,

    /// How long a publish may wait for buffer space before the client is
    /// told OVERLOADED (`max.block.ms`).
    #[arg(long, env = "GW_MAX_BLOCK_MS", default_value_t = 2_000)]
    pub max_block_ms: u64,

    /// Ceiling on one record's delivery, retries included.
    #[arg(long, env = "GW_DELIVERY_TIMEOUT_MS", default_value_t = 30_000)]
    pub delivery_timeout_ms: u64,

    /// Add `x-gw-user` (the token subject) to every record. Clients may
    /// not set `x-gw-*` headers themselves.
    #[arg(long, env = "GW_USER_HEADER", default_value_t = true, action = clap::ArgAction::Set)]
    pub user_header: bool,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcksArg {
    /// Fire and forget: clients are acknowledged with offset -1.
    #[value(name = "0")]
    None,
    /// Acknowledged once the partition leader has appended.
    #[value(name = "1")]
    Leader,
    /// Acknowledged once every in-sync replica has.
    #[value(name = "all")]
    All,
}

impl AcksArg {
    pub fn wire(self) -> i32 {
        match self {
            AcksArg::None => 0,
            AcksArg::Leader => 1,
            AcksArg::All => -1,
        }
    }
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionArg {
    None,
    Gzip,
    Lz4,
    Zstd,
    Snappy,
}

impl From<CompressionArg> for Compression {
    fn from(c: CompressionArg) -> Self {
        match c {
            CompressionArg::None => Compression::None,
            CompressionArg::Gzip => Compression::Gzip,
            CompressionArg::Lz4 => Compression::Lz4,
            CompressionArg::Zstd => Compression::Zstd,
            CompressionArg::Snappy => Compression::Snappy,
        }
    }
}

impl GatewayConfig {
    /// Defaults for everything except where to listen, which broker to
    /// use and the signing secret. For tests and embedding.
    pub fn for_test(broker: SocketAddr, secret: &str) -> Self {
        use clap::Parser;
        #[derive(Parser)]
        struct Wrapper {
            #[command(flatten)]
            config: GatewayConfig,
        }
        let broker = broker.to_string();
        let mut config = Wrapper::parse_from([
            "gateway",
            "--listen",
            "127.0.0.1:0",
            "--http-listen",
            "127.0.0.1:0",
            "--broker",
            &broker,
            "--jwt-secret",
            secret,
        ])
        .config;
        config.linger_ms = 2;
        config
    }
}
