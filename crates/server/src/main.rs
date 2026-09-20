//! `brahmaputra-server`: standalone broker or combined broker/controller node.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod observability;

use anyhow::{anyhow, bail, Context, Result};
use brahmaputra_broker::{
    Broker, BrokerConfig, InternalListener, QuotaConfig, ReplicaManager, ReplicaManagerConfig,
    ReplicationHealthSnapshot, TlsIdentity,
};
use brahmaputra_client::Transport;
use brahmaputra_controller::{ControllerConfig, ControllerNode, NodeId};
use brahmaputra_metadata::{
    BrokerEpoch, ClusterMetadata, MetadataCache, MetadataCommand, MetadataEvent, NodeRole,
};
use brahmaputra_storage::LogConfig;
use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinSet};
use tokio::time::{interval, sleep_until, Instant, MissedTickBehavior};

const DEFAULT_CLUSTER_ID: &str = "brahmaputra";
const DEFAULT_CONTROL_PORT: u16 = 19_092;
const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 1_000;
const DEFAULT_SESSION_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_REPLICA_LAG_TIME_MAX_MS: u64 = 10_000;
/// Kafka's `leader.imbalance.check.interval.seconds` is 300; matched here.
/// Leadership handover costs a leader-epoch bump and a follower truncation
/// check, so doing it eagerly would trade a balance problem for a latency
/// one.
const DEFAULT_LEADER_REBALANCE_INTERVAL_MS: u64 = 300_000;
const METADATA_SYNC_INTERVAL: Duration = Duration::from_millis(50);
const REPLICATION_RECONCILE_INTERVAL: Duration = Duration::from_millis(100);
/// Internal consumer-group offsets topic (Blueprint 05 §1), auto-created in
/// cluster mode once the local node is registered.
const OFFSETS_TOPIC: &str = "__consumer_offsets";
const TRANSACTION_STATE_TOPIC: &str = "__transaction_state";
const DEFAULT_OFFSETS_TOPIC_PARTITIONS: i32 = 50;
/// How long a node that lost the internal-topic creation race waits for
/// its own Raft copy to show the topic the leader already has, before
/// treating the failure as real.
const INTERNAL_TOPIC_SETTLE: Duration = Duration::from_secs(15);

const DEFAULT_RETENTION_CHECK_INTERVAL_MS: u64 = 1_000;
const DEFAULT_HTTP_PORT: u16 = 8080;
/// How often partition gauges are refreshed and every metric sampled into
/// its ring (DESIGN.md §9.1).
const METRICS_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Parser)]
#[command(
    name = "brahmaputra-server",
    about = "Brahmaputra broker (standalone or combined broker/controller node)"
)]
struct Args {
    /// Host to bind and advertise to clients.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port for the data-plane TCP listener.
    #[arg(long, default_value_t = 9092)]
    port: u16,

    /// Data directory (one `<topic>-<partition>` log dir per partition, plus
    /// meta.toml). Repeat, or comma-separate, for one directory per disk.
    ///
    /// Several directories is JBOD: each partition lives on exactly one of
    /// them, a new partition is placed on whichever holds the fewest, and a
    /// directory that fails takes only its own partitions offline instead
    /// of the whole broker. That last part is the point — with one
    /// directory a disk failure has no partial mode.
    ///
    /// Give the broker the disks directly rather than a RAID array
    /// underneath: replication across brokers already provides the
    /// redundancy, and RAID would spend capacity and write throughput
    /// duplicating it.
    #[arg(long = "data-dir", default_value = "./data", value_delimiter = ',')]
    data_dirs: Vec<PathBuf>,

    /// Partition count for auto-created topics.
    #[arg(long, default_value_t = 1)]
    default_partitions: i32,

    /// Wait for more members before a new consumer group's first assignment.
    #[arg(long, default_value_t = 1000)]
    group_initial_rebalance_delay_ms: u64,

    /// Roll log segments at this many bytes.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    segment_bytes: u64,

    /// Delete sealed segments older than this many milliseconds.
    #[arg(long)]
    retention_ms: Option<u64>,

    /// Delete oldest sealed segments while the partition log exceeds this
    /// many bytes.
    #[arg(long)]
    retention_bytes: Option<u64>,

    /// How often partitions apply the retention policies above.
    #[arg(long, default_value_t = DEFAULT_RETENTION_CHECK_INTERVAL_MS)]
    retention_check_interval_ms: u64,

    /// Largest request or response frame this broker will handle
    /// (`socket.request.max.bytes`).
    ///
    /// It bounds a multi-partition fetch response as well as a produce
    /// request, so raising it costs memory per in-flight request and
    /// lowering it below the largest batch a producer sends makes those
    /// batches unproduceable.
    #[arg(long = "max-frame-bytes", default_value_t = 32 * 1024 * 1024)]
    max_frame_bytes: usize,

    /// Add a sparse-index entry at least every this many log bytes
    /// (`log.index.interval.bytes`).
    ///
    /// Smaller narrows the scan a lookup ends with and costs index file
    /// size; larger does the opposite. Kafka's default is the same 4 KiB.
    #[arg(long = "index-interval-bytes", default_value_t = 4096)]
    index_interval_bytes: u64,

    /// Largest record batch a producer may send, in bytes
    /// (`message.max.bytes`); `-1` leaves it bounded only by
    /// `--max-frame-bytes`.
    #[arg(long = "max-message-bytes", default_value_t = -1, allow_hyphen_values = true)]
    max_message_bytes: i64,

    /// Default `cleanup.policy` for topics that do not set one
    /// (`log.cleanup.policy`): `delete` drops whole aged segments,
    /// `compact` keeps the newest record per key.
    #[arg(long = "cleanup-policy", value_parser = ["delete", "compact"], default_value = "delete")]
    cleanup_policy: String,

    /// How long a tombstone is kept once compaction could remove it
    /// (`log.cleaner.delete.retention.ms`). This is the window a consumer
    /// has to observe a deletion before the record carrying it is gone.
    #[arg(long = "delete-retention-ms", default_value_t = 24 * 60 * 60 * 1000)]
    delete_retention_ms: u64,

    /// Fraction of a partition's cleanable log that must be dirty before
    /// compaction rewrites it (`log.cleaner.min.cleanable.ratio`).
    #[arg(long = "min-cleanable-dirty-ratio", default_value_t = 0.5)]
    min_cleanable_dirty_ratio: f64,

    /// How long a record is protected from being compacted away
    /// (`log.cleaner.min.compaction.lag.ms`).
    #[arg(long = "min-compaction-lag-ms", default_value_t = 0)]
    min_compaction_lag_ms: u64,

    /// How long a dirty record may wait before compaction runs regardless
    /// of the dirty ratio (`log.cleaner.max.compaction.lag.ms`); unset
    /// never forces a pass.
    #[arg(long = "max-compaction-lag-ms")]
    max_compaction_lag_ms: Option<u64>,

    /// fsync the active segment after this many records. Unset leaves
    /// durability to replication and the page cache (Kafka's default).
    #[arg(long)]
    flush_interval_messages: Option<u64>,

    /// fsync the active segment at least this often, in milliseconds.
    #[arg(long)]
    flush_interval_ms: Option<u64>,

    /// Per-client produce byte rate. Over-rate clients have their
    /// acknowledgements delayed, never rejected.
    #[arg(long)]
    quota_produce_bytes_per_sec: Option<u64>,

    /// Per-client fetch byte rate, throttled the same way.
    #[arg(long)]
    quota_fetch_bytes_per_sec: Option<u64>,

    /// Ceiling on bytes this broker serves to *followers* catching up.
    /// Without one, a rejoining broker fetches as fast as the leader can
    /// read and competes with client traffic for the same disk and NIC, so
    /// a single restart shows up as latency on every producer and consumer.
    #[arg(long)]
    quota_replication_bytes_per_sec: Option<u64>,

    /// Ceiling on how long one response may be delayed by a quota.
    #[arg(long, default_value_t = 30_000)]
    quota_max_throttle_ms: u64,

    /// Require every data-plane connection to authenticate, and authorize
    /// each request against the cluster ACLs. Off by default, matching a
    /// Kafka PLAINTEXT listener; production should enable it. Needs a
    /// cluster (the user store lives in the Raft metadata) and an
    /// encrypted transport, since credentials cross the wire in the clear.
    #[arg(long, default_value_t = false)]
    require_auth: bool,

    /// PEM certificate chain the broker presents on `tcp-tls` and `quic`.
    ///
    /// Without it the broker generates a self-signed certificate at
    /// startup: fine for development, but a client has no way to tell that
    /// certificate apart from any other, so it proves nothing about who it
    /// is talking to.
    #[arg(long = "tls-cert", requires = "tls_key")]
    tls_cert: Option<PathBuf>,

    /// PEM private key matching `--tls-cert`.
    #[arg(long = "tls-key", requires = "tls_cert")]
    tls_key: Option<PathBuf>,

    /// PEM CA bundle that client certificates must chain to.
    ///
    /// Setting it *requires* a client certificate on every encrypted
    /// connection, and binds that certificate's common name to the
    /// connection as its principal — authentication with no password
    /// crossing the wire, and ACLs enforceable against a client that never
    /// calls Authenticate.
    #[arg(long = "tls-client-ca")]
    tls_client_ca: Option<PathBuf>,

    /// Port for the metrics API and dashboard (DESIGN.md §9.2). 0
    /// disables it.
    #[arg(long, default_value_t = DEFAULT_HTTP_PORT)]
    http_port: u16,

    /// Initial admin password, used only on first boot when the cluster
    /// has no users yet. Prefer the BRAHMAPUTRA_ADMIN_PASSWORD env var:
    /// a command line is visible to every process on the machine.
    #[arg(long, env = "BRAHMAPUTRA_ADMIN_PASSWORD")]
    admin_password: Option<String>,

    /// Username for that first admin. Only used on first boot, alongside
    /// `--admin-password`.
    #[arg(long, env = "BRAHMAPUTRA_ADMIN_USER", default_value = "admin")]
    admin_user: String,

    /// Stable controller node ID. Supplying this enables combined cluster mode.
    #[arg(long)]
    node_id: Option<NodeId>,

    /// Stable cluster identity shared by every controller peer.
    #[arg(long, default_value = DEFAULT_CLUSTER_ID)]
    cluster_id: String,

    /// Port for controller HTTP and internal Raft RPCs in cluster mode.
    #[arg(long, default_value_t = DEFAULT_CONTROL_PORT)]
    control_port: u16,

    /// Fixed controller peer, repeated as `NODE_ID=HOST:CONTROL_PORT`.
    #[arg(long = "controller-peer", value_name = "NODE_ID=ADDRESS")]
    controller_peers: Vec<ControllerPeer>,

    /// Initialize the fixed controller quorum. Use on exactly one initial node.
    #[arg(long)]
    bootstrap: bool,

    /// Optional rack label published with this broker registration.
    #[arg(long)]
    rack: Option<String>,

    /// Broker heartbeat interval in milliseconds.
    #[arg(long, default_value_t = DEFAULT_HEARTBEAT_INTERVAL_MS)]
    heartbeat_interval_ms: u64,

    /// Fence brokers silent for longer than this many milliseconds.
    #[arg(long, default_value_t = DEFAULT_SESSION_TIMEOUT_MS)]
    session_timeout_ms: u64,

    /// Remove an assigned follower from ISR after it stops fetching for this long.
    #[arg(long, default_value_t = DEFAULT_REPLICA_LAG_TIME_MAX_MS)]
    replica_lag_time_max_ms: u64,

    /// How often the controller moves leadership back to each partition's
    /// preferred replica (`leader.imbalance.check.interval.seconds`); 0
    /// disables it.
    ///
    /// Without this, every broker restart leaves leadership permanently on
    /// whichever replica took over, so a cluster that has been operated for
    /// months ends up with its load wherever its outages happened to put it.
    #[arg(long, default_value_t = DEFAULT_LEADER_REBALANCE_INTERVAL_MS)]
    auto_leader_rebalance_interval_ms: u64,

    /// Partition count for the internal __consumer_offsets topic created at
    /// cluster startup.
    #[arg(long, default_value_t = DEFAULT_OFFSETS_TOPIC_PARTITIONS)]
    offsets_topic_partitions: i32,

    /// Replication factor for the internal `__consumer_offsets` and
    /// `__transaction_state` topics
    /// (`offsets.topic.replication.factor`). `0` derives it from the
    /// cluster size, capped at 3.
    ///
    /// Worth setting explicitly on a cluster that grows: the internal
    /// topics are created once, when the first nodes register, so a
    /// three-node cluster whose second and third nodes had not started yet
    /// would otherwise pin committed offsets to a single broker forever.
    #[arg(long = "offsets-topic-replication-factor", default_value_t = 0)]
    offsets_topic_replication_factor: i32,

    /// How long a group with no members keeps its committed offsets
    /// (`offsets.retention.ms`); 0 keeps them forever. The clock starts
    /// when the group empties, so a live consumer is never affected.
    #[arg(long = "offsets-retention-ms", default_value_t = 7 * 24 * 60 * 60 * 1000)]
    offsets_retention_ms: u64,

    /// Largest `transaction.timeout.ms` a producer may ask for
    /// (`transaction.max.timeout.ms`). A transaction that outlives its
    /// timeout is aborted by the coordinator and its producer fenced,
    /// which is what stops an abandoned transaction holding every
    /// `read_committed` consumer of its partitions still.
    #[arg(long = "transaction-max-timeout-ms", default_value_t = 15 * 60 * 1000)]
    transaction_max_timeout_ms: u64,

    /// How long an idle `transactional.id` is remembered before its
    /// coordinator state is deleted (`transactional.id.expiration.ms`).
    #[arg(long = "transactional-id-expiration-ms", default_value_t = 7 * 24 * 60 * 60 * 1000)]
    transactional_id_expiration_ms: u64,

    /// Host to publish in metadata, when it differs from `--host`.
    ///
    /// Kafka's `advertised.listeners`. A broker behind NAT, in a container
    /// with a bridged network, or on a Kubernetes pod IP binds one address
    /// and is reachable at another; without this it publishes the address
    /// it bound and every client is routed somewhere it cannot reach.
    #[arg(long = "advertised-host")]
    advertised_host: Option<String>,

    /// Port to publish in metadata, when it differs from the bound one.
    #[arg(long = "advertised-port")]
    advertised_port: Option<u16>,

    /// Bind a second data-plane listener for broker-to-broker traffic
    /// (Kafka's `inter.broker.listener.name`); `0` picks a port.
    ///
    /// Replication carries every record once per follower, so a cluster
    /// required to present TLS to its clients would otherwise encrypt the
    /// same bytes two or three more times to reach brokers on a private
    /// network it already trusts. With this set, clients use `--port` and
    /// its transport, and peers use this one and its.
    #[arg(long = "internal-port")]
    internal_port: Option<u16>,

    /// Whether the inter-broker listener speaks TLS. Off means plaintext,
    /// which is the point of having it.
    #[arg(long = "internal-tls", default_value_t = false)]
    internal_tls: bool,

    /// Data-plane transport: `tcp` (one multiplexed byte stream, plain),
    /// `tcp-tls` (the same, encrypted with TLS 1.3), or `quic` (TLS 1.3,
    /// one bidirectional stream per request, no head-of-line blocking
    /// between requests). Clients must use the same one.
    #[arg(long, default_value = "tcp")]
    transport: Transport,
}

/// One entry in the fixed controller map accepted by Clap.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ControllerPeer {
    node_id: NodeId,
    address: String,
}

impl FromStr for ControllerPeer {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let (node_id, address) = value
            .split_once('=')
            .ok_or_else(|| "expected NODE_ID=HOST:CONTROL_PORT".to_owned())?;
        let node_id = node_id
            .parse::<NodeId>()
            .map_err(|error| format!("invalid controller node ID {node_id:?}: {error}"))?;
        node_id_to_broker_id(node_id).map_err(|error| error.to_string())?;
        if address.is_empty() || address.chars().any(char::is_whitespace) {
            return Err(
                "controller address must be non-empty and contain no whitespace".to_owned(),
            );
        }
        Ok(Self {
            node_id,
            address: address.to_owned(),
        })
    }
}

#[derive(Clone, Debug)]
struct ClusterSettings {
    node_id: NodeId,
    broker_id: i32,
    cluster_id: String,
    control_port: u16,
    peers: BTreeMap<NodeId, String>,
    bootstrap: bool,
    rack: Option<String>,
    heartbeat_interval: Duration,
    session_timeout: Duration,
    replica_lag_time_max: Duration,
    /// `None` disables preferred-leader rebalancing entirely.
    auto_leader_rebalance_interval: Option<Duration>,
}

#[derive(Clone, Debug)]
struct BrokerRegistration {
    broker_id: i32,
    host: String,
    data_port: u16,
    control_port: u16,
    /// Where peers reach this broker, or 0 when that is `data_port`.
    internal_port: u16,
    rack: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "brahmaputra=info".into()),
        )
        .init();

    let args = Args::parse();
    if let Some(cluster) = cluster_settings(&args)? {
        return run_cluster(args, cluster).await;
    }
    run_standalone(args).await
}

/// Byte-rate limits from the CLI flags.
fn quota_config(args: &Args) -> QuotaConfig {
    QuotaConfig {
        produce_bytes_per_sec: args.quota_produce_bytes_per_sec,
        fetch_bytes_per_sec: args.quota_fetch_bytes_per_sec,
        replication_bytes_per_sec: args.quota_replication_bytes_per_sec,
        max_throttle: Some(Duration::from_millis(args.quota_max_throttle_ms)),
    }
}

async fn run_standalone(args: Args) -> Result<()> {
    let quota = quota_config(&args);
    let tls = tls_identity(&args);
    let host = args.host.clone();
    let config = BrokerConfig {
        host: args.host.clone(),
        port: args.port,
        data_dirs: args.data_dirs,
        default_partitions: args.default_partitions,
        group_initial_rebalance_delay: Duration::from_millis(args.group_initial_rebalance_delay_ms),
        log_config: LogConfig {
            segment_bytes: args.segment_bytes,
            retention_ms: args.retention_ms,
            retention_bytes: args.retention_bytes,
            flush_interval_messages: args.flush_interval_messages,
            flush_interval_ms: args.flush_interval_ms,
            compact: args.cleanup_policy == "compact",
            delete_retention_ms: args.delete_retention_ms,
            min_cleanable_dirty_ratio: args.min_cleanable_dirty_ratio,
            min_compaction_lag_ms: args.min_compaction_lag_ms,
            max_compaction_lag_ms: args.max_compaction_lag_ms,
            index_interval_bytes: args.index_interval_bytes.max(1),
            ..LogConfig::default()
        },
        retention_check_interval: Duration::from_millis(args.retention_check_interval_ms.max(1)),
        max_frame_bytes: args.max_frame_bytes.max(1 << 16),
        max_message_bytes: (args.max_message_bytes > 0).then_some(args.max_message_bytes as usize),
        offsets_retention: (args.offsets_retention_ms > 0)
            .then(|| Duration::from_millis(args.offsets_retention_ms)),
        transaction_max_timeout: Duration::from_millis(args.transaction_max_timeout_ms),
        transactional_id_expiration: Duration::from_millis(args.transactional_id_expiration_ms),
        internal_listener: args.internal_port.map(|port| InternalListener {
            port,
            tls: args.internal_tls,
        }),
        transport: args.transport,
        quota,
        require_auth: args.require_auth,
        tls,
        ..BrokerConfig::default()
    };

    let broker = Broker::bind(config)
        .await
        .context("failed to start broker")?;
    tracing::info!(addr = %broker.local_addr(), "brahmaputra broker up");
    let broker = Arc::new(broker);

    // Metrics and the dashboard run alongside the data plane. Standalone
    // mode has no controller, so the dashboard serves reads and refuses
    // metadata writes rather than pretending they went somewhere.
    observability::describe_metrics(broker.metrics());
    let (observability_shutdown, observability_rx) = watch::channel(false);
    let sampler = tokio::spawn(observability::run_metrics_sampler(
        Arc::clone(&broker),
        observability_rx.clone(),
        METRICS_SAMPLE_INTERVAL,
    ));
    if args.http_port != 0 {
        match observability::start_dashboard(
            &host,
            args.http_port,
            Arc::clone(&broker),
            None,
            None,
            observability_rx,
        )
        .await
        {
            Ok(addr) => tracing::info!(%addr, "dashboard and metrics available"),
            Err(error) => tracing::warn!(%error, "dashboard did not start"),
        }
    }

    broker
        .run(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    let _ = observability_shutdown.send(true);
    sampler.abort();
    Ok(())
}

async fn run_cluster(args: Args, cluster: ClusterSettings) -> Result<()> {
    let quota = quota_config(&args);
    let tls = tls_identity(&args);
    let host = args.host.clone();
    let controller_listener = TcpListener::bind((args.host.as_str(), cluster.control_port))
        .await
        .with_context(|| {
            format!(
                "failed to bind controller HTTP listener on {}:{}",
                args.host, cluster.control_port
            )
        })?;
    let control_addr = controller_listener
        .local_addr()
        .context("failed to read controller listener address")?;

    // Keep coalesced heartbeat checkpoints inside the configured lease
    // margin. At the defaults this remains one second; tight but valid
    // sub-second sessions checkpoint more often instead of being fenced by a
    // fixed one-second batching window.
    let heartbeat_checkpoint_interval = Duration::from_millis(
        args.session_timeout_ms
            .saturating_sub(args.heartbeat_interval_ms)
            .clamp(1, 1_000),
    );
    let controller_config = ControllerConfig::new(
        cluster.node_id,
        cluster.cluster_id.clone(),
        cluster.peers.clone(),
    )
    // The Raft log is broker-wide, not per-partition, so it lives in the
    // first data dir rather than being spread across them.
    .with_data_dir(args.data_dirs[0].join("controller"))
    .with_heartbeat_checkpoint_interval(heartbeat_checkpoint_interval);
    let controller = ControllerNode::new(controller_config)
        .await
        .context("failed to start controller")?;

    let metadata_cache = MetadataCache::new(ClusterMetadata::new(cluster.cluster_id.clone()));
    let broker = Arc::new(
        Broker::bind(BrokerConfig {
            broker_id: cluster.broker_id,
            host: args.host.clone(),
            port: args.port,
            data_dirs: args.data_dirs,
            default_partitions: args.default_partitions,
            group_initial_rebalance_delay: Duration::from_millis(
                args.group_initial_rebalance_delay_ms,
            ),
            log_config: LogConfig {
                segment_bytes: args.segment_bytes,
                retention_ms: args.retention_ms,
                retention_bytes: args.retention_bytes,
                flush_interval_messages: args.flush_interval_messages,
                flush_interval_ms: args.flush_interval_ms,
                compact: args.cleanup_policy == "compact",
                delete_retention_ms: args.delete_retention_ms,
                min_cleanable_dirty_ratio: args.min_cleanable_dirty_ratio,
                min_compaction_lag_ms: args.min_compaction_lag_ms,
                max_compaction_lag_ms: args.max_compaction_lag_ms,
                index_interval_bytes: args.index_interval_bytes.max(1),
                ..LogConfig::default()
            },
            retention_check_interval: Duration::from_millis(
                args.retention_check_interval_ms.max(1),
            ),
            max_frame_bytes: args.max_frame_bytes.max(1 << 16),
            max_message_bytes: (args.max_message_bytes > 0)
                .then_some(args.max_message_bytes as usize),
            offsets_retention: (args.offsets_retention_ms > 0)
                .then(|| Duration::from_millis(args.offsets_retention_ms)),
            transaction_max_timeout: Duration::from_millis(args.transaction_max_timeout_ms),
            transactional_id_expiration: Duration::from_millis(args.transactional_id_expiration_ms),
            internal_listener: args.internal_port.map(|port| InternalListener {
                port,
                tls: args.internal_tls,
            }),
            transport: args.transport,
            quota,
            require_auth: args.require_auth,
            tls,
            metadata_cache: Some(metadata_cache.clone()),
            replication_enabled: true,
            ..BrokerConfig::default()
        })
        .await
        .context("failed to start broker")?,
    );
    // What peers and clients are told, which is not necessarily what was
    // bound: a broker behind NAT or on a pod IP is reachable at an
    // address it never bound.
    let internal_transport = if args.internal_port.is_some() {
        if args.internal_tls {
            Transport::TcpTls
        } else {
            Transport::Tcp
        }
    } else {
        args.transport
    };
    let registration = BrokerRegistration {
        broker_id: cluster.broker_id,
        host: args.advertised_host.clone().unwrap_or(args.host),
        data_port: args
            .advertised_port
            .unwrap_or_else(|| broker.local_addr().port()),
        control_port: control_addr.port(),
        internal_port: broker.internal_addr().map_or(0, |addr| addr.port()),
        rack: cluster.rack.clone(),
    };

    tracing::info!(
        node_id = cluster.node_id,
        broker_id = cluster.broker_id,
        data_addr = %broker.local_addr(),
        control_addr = %control_addr,
        cluster_id = %cluster.cluster_id,
        "combined broker/controller node up"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (broker_epoch_tx, broker_epoch_rx) = watch::channel(0_u64);
    let mut components: JoinSet<Result<()>> = JoinSet::new();

    let controller_server = Arc::clone(&controller);
    let controller_shutdown = shutdown_rx.clone();
    components.spawn(async move {
        controller_server
            .serve(controller_listener, wait_for_shutdown(controller_shutdown))
            .await
            .context("controller HTTP/Raft service failed")
    });

    let broker_server = Arc::clone(&broker);
    let broker_shutdown = shutdown_rx.clone();
    components.spawn(async move {
        broker_server
            .run(wait_for_shutdown(broker_shutdown))
            .await
            .context("broker data-plane service failed")
    });

    // Metrics sampler and dashboard (DESIGN.md §9). Both observe the same
    // shutdown watch as every other component.
    observability::describe_metrics(broker.metrics());
    let sampler_broker = Arc::clone(&broker);
    let sampler_shutdown = shutdown_rx.clone();
    components.spawn(async move {
        observability::run_metrics_sampler(
            sampler_broker,
            sampler_shutdown,
            METRICS_SAMPLE_INTERVAL,
        )
        .await;
        Ok(())
    });
    if args.http_port != 0 {
        match observability::start_dashboard(
            &host,
            args.http_port,
            Arc::clone(&broker),
            Some(metadata_cache.clone()),
            Some(Arc::clone(&controller)),
            shutdown_rx.clone(),
        )
        .await
        {
            Ok(addr) => tracing::info!(%addr, "dashboard and metrics available"),
            Err(error) => tracing::warn!(%error, "dashboard did not start"),
        }
    }

    let lifecycle_controller = Arc::clone(&controller);
    let lifecycle_broker = Arc::clone(&broker);
    let lifecycle_settings = cluster.clone();
    let lifecycle_shutdown = shutdown_rx.clone();
    let lifecycle_epoch = broker_epoch_tx;
    components.spawn(async move {
        broker_lifecycle(
            lifecycle_controller,
            lifecycle_broker,
            lifecycle_settings,
            registration,
            lifecycle_epoch,
            lifecycle_shutdown,
        )
        .await
    });

    // Controller/expiry maintenance may wait on a lagging local Raft state
    // machine or a remote metadata write. Keep it independent from broker
    // lease renewal so neither operation can starve heartbeats.
    let maintenance_controller = Arc::clone(&controller);
    let maintenance_settings = cluster.clone();
    let maintenance_epoch = broker_epoch_rx.clone();
    let maintenance_shutdown = shutdown_rx.clone();
    components.spawn(async move {
        controller_maintenance(
            maintenance_controller,
            maintenance_settings,
            maintenance_epoch,
            maintenance_shutdown,
        )
        .await
    });

    let replica_manager = ReplicaManager::new(
        Arc::clone(&broker),
        broker_epoch_rx.clone(),
        ReplicaManagerConfig {
            // The inter-broker listener's transport when there is one:
            // replication has to speak whatever that socket speaks, not
            // whatever the client listener does.
            transport: internal_transport,
            ..ReplicaManagerConfig::default()
        },
    );
    let replica_shutdown = shutdown_rx.clone();
    components.spawn(async move {
        replica_manager
            .run(wait_for_shutdown(replica_shutdown))
            .await;
        Ok(())
    });

    // M4: create the internal group-coordinator topic once this node is a
    // registered broker. Concurrent creators race; losers tolerate the
    // already-exists outcome.
    let offsets_controller = Arc::clone(&controller);
    let offsets_cache = metadata_cache.clone();
    let offsets_epoch = broker_epoch_rx.clone();
    let offsets_shutdown = shutdown_rx.clone();
    let offsets_partitions = args.offsets_topic_partitions;
    let offsets_replication_factor = args.offsets_topic_replication_factor;
    components.spawn(async move {
        ensure_internal_topics(
            offsets_controller,
            offsets_cache,
            offsets_epoch,
            offsets_partitions,
            offsets_replication_factor,
            offsets_shutdown,
        )
        .await
    });

    // M6: the session secret and the first admin, created once per cluster.
    // Like the offsets topic, several nodes may race and all but one will
    // find the work already done.
    let admin_controller = Arc::clone(&controller);
    let admin_cache = metadata_cache.clone();
    let admin_epoch = broker_epoch_rx.clone();
    let admin_shutdown = shutdown_rx.clone();
    let admin_password = args.admin_password.clone();
    let admin_user = args.admin_user.clone();
    components.spawn(async move {
        ensure_admin_user(
            admin_controller,
            admin_cache,
            admin_epoch,
            admin_user,
            admin_password,
            admin_shutdown,
        )
        .await
    });

    let replication_broker = Arc::clone(&broker);
    let replication_controller = Arc::clone(&controller);
    let replication_cache = metadata_cache.clone();
    let replication_settings = cluster.clone();
    let replication_shutdown = shutdown_rx.clone();
    components.spawn(async move {
        replication_maintenance(
            replication_broker,
            replication_controller,
            replication_cache,
            replication_settings,
            broker_epoch_rx,
            replication_shutdown,
        )
        .await
    });

    let sync_controller = Arc::clone(&controller);
    components.spawn(async move {
        synchronize_metadata(sync_controller, metadata_cache, shutdown_rx).await
    });

    supervise_components(&shutdown_tx, &mut components).await
}

fn cluster_settings(args: &Args) -> Result<Option<ClusterSettings>> {
    let Some(node_id) = args.node_id else {
        let cluster_flags_present = !args.controller_peers.is_empty()
            || args.bootstrap
            || args.rack.is_some()
            || args.cluster_id != DEFAULT_CLUSTER_ID
            || args.control_port != DEFAULT_CONTROL_PORT
            || args.heartbeat_interval_ms != DEFAULT_HEARTBEAT_INTERVAL_MS
            || args.session_timeout_ms != DEFAULT_SESSION_TIMEOUT_MS
            || args.replica_lag_time_max_ms != DEFAULT_REPLICA_LAG_TIME_MAX_MS
            || args.offsets_topic_partitions != DEFAULT_OFFSETS_TOPIC_PARTITIONS;
        if cluster_flags_present {
            bail!("cluster-only flags require --node-id");
        }
        return Ok(None);
    };

    let broker_id = node_id_to_broker_id(node_id)?;
    if args.cluster_id.trim().is_empty() {
        bail!("--cluster-id must not be empty");
    }
    if args.control_port == 0 {
        bail!("--control-port must be fixed and non-zero in cluster mode");
    }
    if args.heartbeat_interval_ms == 0 {
        bail!("--heartbeat-interval-ms must be non-zero");
    }
    if args.session_timeout_ms <= args.heartbeat_interval_ms {
        bail!("--session-timeout-ms must be greater than --heartbeat-interval-ms");
    }
    if args.replica_lag_time_max_ms == 0 {
        bail!("--replica-lag-time-max-ms must be non-zero");
    }
    if args.offsets_topic_partitions < 1 {
        bail!("--offsets-topic-partitions must be at least 1");
    }
    if args.offsets_topic_replication_factor < 0 {
        bail!("--offsets-topic-replication-factor must be 0 (derive) or positive");
    }

    let mut peers = BTreeMap::new();
    for peer in &args.controller_peers {
        node_id_to_broker_id(peer.node_id)?;
        if peers.insert(peer.node_id, peer.address.clone()).is_some() {
            bail!("duplicate --controller-peer for node {}", peer.node_id);
        }
    }
    if peers.is_empty() {
        bail!("cluster mode requires at least one --controller-peer");
    }
    if !peers.contains_key(&node_id) {
        bail!("--controller-peer map does not contain local node {node_id}");
    }

    Ok(Some(ClusterSettings {
        node_id,
        broker_id,
        cluster_id: args.cluster_id.clone(),
        control_port: args.control_port,
        peers,
        bootstrap: args.bootstrap,
        rack: args.rack.clone(),
        heartbeat_interval: Duration::from_millis(args.heartbeat_interval_ms),
        session_timeout: Duration::from_millis(args.session_timeout_ms),
        replica_lag_time_max: Duration::from_millis(args.replica_lag_time_max_ms),
        auto_leader_rebalance_interval: (args.auto_leader_rebalance_interval_ms > 0)
            .then(|| Duration::from_millis(args.auto_leader_rebalance_interval_ms)),
    }))
}

/// The TLS material this broker should use, from the command line.
///
/// All three unset is the development default: a self-signed certificate
/// generated at startup, and nothing asked of the client. That keeps a
/// single-node broker startable with no files to create, which is the only
/// reason it is the default rather than the safe choice.
fn tls_identity(args: &Args) -> TlsIdentity {
    TlsIdentity {
        cert_path: args.tls_cert.clone(),
        key_path: args.tls_key.clone(),
        client_ca_path: args.tls_client_ca.clone(),
    }
}

fn node_id_to_broker_id(node_id: NodeId) -> Result<i32> {
    i32::try_from(node_id).map_err(|_| {
        anyhow!("controller node ID {node_id} cannot be represented as an i32 broker ID")
    })
}

async fn broker_lifecycle(
    controller: Arc<ControllerNode>,
    broker: Arc<Broker>,
    settings: ClusterSettings,
    registration: BrokerRegistration,
    broker_epoch: watch::Sender<BrokerEpoch>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    if settings.bootstrap {
        loop {
            let bootstrap = controller.bootstrap();
            tokio::select! {
                _ = wait_for_shutdown(shutdown.clone()) => return Ok(()),
                result = bootstrap => match result {
                    Ok(()) => {
                        tracing::info!(node_id = settings.node_id, "controller quorum initialized");
                        break;
                    }
                    Err(error) => tracing::warn!(%error, "controller bootstrap failed; retrying"),
                }
            }
            if wait_or_shutdown(settings.heartbeat_interval, &mut shutdown).await {
                return Ok(());
            }
        }
    }

    let mut heartbeat = interval(settings.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut epoch: Option<BrokerEpoch> = None;
    let mut expected_epoch: Option<BrokerEpoch> = None;
    let mut pending_registration = None;
    let mut last_renewal = None;
    let mut prefer_remote_heartbeat = false;

    loop {
        let lease_expiry = last_renewal.map(|renewed: Instant| renewed + settings.session_timeout);
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = async {
                match lease_expiry {
                    Some(deadline) => sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                let expired_epoch = epoch.take().unwrap_or(0);
                broker.suspend_broker_lease();
                broker_epoch.send_replace(0);
                expected_epoch = (expired_epoch != 0).then_some(expired_epoch);
                last_renewal = None;
                prefer_remote_heartbeat = false;
                tracing::warn!(
                    broker_id = registration.broker_id,
                    broker_epoch = expired_epoch,
                    timeout = ?settings.session_timeout,
                    "broker lease expired locally; data plane suspended until conditional re-registration"
                );
            }
            _ = heartbeat.tick() => {
                if broker.is_fenced() {
                    broker_epoch.send_replace(0);
                    bail!("broker {} was irreversibly fenced", registration.broker_id);
                }

                if let Some(current_epoch) = epoch {
                    let deadline = last_renewal
                        .expect("an active broker epoch always has a renewal time")
                        + settings.session_timeout;
                    let outcome = tokio::select! {
                        result = heartbeat_broker(
                            &controller,
                            registration.broker_id,
                            current_epoch,
                            prefer_remote_heartbeat,
                            &mut shutdown,
                        ) => Some(result),
                        _ = sleep_until(deadline) => {
                            None
                        }
                    };
                    let Some(outcome) = outcome else {
                        epoch = None;
                        expected_epoch = Some(current_epoch);
                        last_renewal = None;
                        prefer_remote_heartbeat = false;
                        broker.suspend_broker_lease();
                        broker_epoch.send_replace(0);
                        tracing::warn!(
                            broker_id = registration.broker_id,
                            broker_epoch = current_epoch,
                            timeout = ?settings.session_timeout,
                            "broker heartbeat exceeded its lease; data plane suspended until conditional re-registration"
                        );
                        continue;
                    };
                    match outcome {
                        Ok(HeartbeatOutcome::Reregister) => {
                            epoch = None;
                            expected_epoch = Some(current_epoch);
                            last_renewal = None;
                            prefer_remote_heartbeat = false;
                            broker.suspend_broker_lease();
                            broker_epoch.send_replace(0);
                            continue;
                        }
                        Ok(HeartbeatOutcome::Renewed) => {
                            last_renewal = Some(Instant::now());
                            if prefer_remote_heartbeat {
                                let visibility = tokio::time::timeout(
                                    Duration::from_millis(10),
                                    controller.local_metadata(),
                                )
                                .await;
                                if let Ok(Ok(image)) = visibility {
                                    prefer_remote_heartbeat = image
                                        .brokers
                                        .get(&registration.broker_id)
                                        .is_none_or(|registered| {
                                            registered.broker_epoch != current_epoch
                                                || !registered.alive
                                        });
                                }
                            }
                        }
                        Ok(HeartbeatOutcome::Retry) => {}
                        Ok(HeartbeatOutcome::Shutdown) => return Ok(()),
                        Err(error) => {
                            broker.fence();
                            broker_epoch.send_replace(0);
                            return Err(error);
                        }
                    }
                } else {
                    if pending_registration.is_none() {
                        pending_registration = Some(PendingRegistration::new(expected_epoch)?);
                    }
                    let pending = pending_registration.as_mut().unwrap();
                    if let (Some(expected), Some(identity)) =
                        (pending.expected_epoch, pending.registration_id.as_deref())
                    {
                        broker.prepare_broker_registration(expected, identity)?;
                    }
                    let registered = register_broker(
                        &controller,
                        &registration,
                        pending,
                        &mut shutdown,
                    )
                    .await?;
                    if let Some(registered_epoch) = registered {
                        broker.activate_broker_epoch(registered_epoch)?;
                        epoch = Some(registered_epoch);
                        expected_epoch = None;
                        pending_registration = None;
                        last_renewal = Some(Instant::now());
                        prefer_remote_heartbeat = true;
                    }
                }
                broker_epoch.send_replace(epoch.unwrap_or(0));
            }
        }
    }
}

async fn controller_maintenance(
    controller: Arc<ControllerNode>,
    settings: ClusterSettings,
    mut broker_epoch: watch::Receiver<BrokerEpoch>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut maintenance = interval(settings.heartbeat_interval);
    maintenance.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Leadership rebalancing runs on its own, much slower clock than
    // fencing does. Tracked as "when it was last considered" rather than as
    // a second timer so a node that only just became Raft leader does not
    // immediately reshuffle a cluster it has barely observed.
    let mut last_rebalance = Instant::now();
    // When this node became the Raft leader. Fencing decisions are made
    // against heartbeats that could only be written to a quorum, so a
    // leader younger than one session timeout has no evidence yet — every
    // timestamp it inherited predates its own election.
    let mut leader_since: Option<Instant> = None;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = maintenance.tick() => {
                if controller.raft_metrics().current_leader == Some(settings.node_id) {
                    leader_since.get_or_insert_with(Instant::now);
                } else {
                    leader_since = None;
                }
                if *broker_epoch.borrow_and_update() != 0 {
                    let rebalance_due = match settings.auto_leader_rebalance_interval {
                        Some(every) => last_rebalance.elapsed() >= every,
                        None => false,
                    };
                    if rebalance_due {
                        last_rebalance = Instant::now();
                    }
                    let fencing_open = leader_since
                        .is_some_and(|since| since.elapsed() >= settings.session_timeout);
                    run_leader_maintenance(
                        &controller,
                        &settings,
                        fencing_open,
                        rebalance_due,
                        &mut shutdown,
                    )
                    .await?;
                }
            }
        }
    }
}

/// Keep a conditional operation's identity and receipt across RPC/tick retries.
/// A receipt alone is not a lease: recovery always confirms a fresh heartbeat.
struct PendingRegistration {
    expected_epoch: Option<BrokerEpoch>,
    registration_id: Option<String>,
    acknowledged_epoch: Option<BrokerEpoch>,
}

impl PendingRegistration {
    fn new(expected_epoch: Option<BrokerEpoch>) -> Result<Self> {
        let registration_id = if expected_epoch.is_some() {
            let mut random = [0_u8; 32];
            getrandom::fill(&mut random)
                .map_err(|error| anyhow!("cannot generate registration identity: {error}"))?;
            Some(random.iter().map(|byte| format!("{byte:02x}")).collect())
        } else {
            None
        };
        Ok(Self {
            expected_epoch,
            registration_id,
            acknowledged_epoch: None,
        })
    }
}

async fn register_broker(
    controller: &ControllerNode,
    registration: &BrokerRegistration,
    pending: &mut PendingRegistration,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<BrokerEpoch>> {
    if pending.acknowledged_epoch.is_none() {
        pending.acknowledged_epoch =
            submit_registration(controller, registration, pending, shutdown).await?;
    }
    let Some(epoch) = pending.acknowledged_epoch else {
        return Ok(None);
    };
    match heartbeat_broker(controller, registration.broker_id, epoch, true, shutdown).await? {
        HeartbeatOutcome::Renewed => Ok(Some(epoch)),
        HeartbeatOutcome::Reregister => {
            *pending = PendingRegistration::new(Some(epoch))?;
            Ok(None)
        }
        HeartbeatOutcome::Retry | HeartbeatOutcome::Shutdown => Ok(None),
    }
}

async fn submit_registration(
    controller: &ControllerNode,
    registration: &BrokerRegistration,
    pending: &PendingRegistration,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<BrokerEpoch>> {
    let expected_epoch = pending.expected_epoch;
    let command = MetadataCommand::RegisterBroker {
        broker_id: registration.broker_id,
        host: registration.host.clone(),
        data_port: registration.data_port,
        control_port: registration.control_port,
        internal_port: registration.internal_port,
        expected_epoch,
        registration_id: pending.registration_id.clone(),
        roles: vec![NodeRole::Broker, NodeRole::Controller],
        rack: registration.rack.clone(),
        now_ms: unix_time_ms(),
    };
    // Stamped on the attempt that commits: this write may wait for a quorum
    // to exist at all, and a registration that lands looking older than the
    // session timeout is fenced by the next maintenance pass.
    let Some(result) = (tokio::select! {
        result = controller.write_metadata_stamped(command) => Some(result),
        _ = wait_for_shutdown(shutdown.clone()) => None,
    }) else {
        return Ok(None);
    };
    match result {
        Ok(MetadataEvent::BrokerRegistered {
            broker_id,
            broker_epoch,
        }) if broker_id == registration.broker_id => {
            tracing::info!(broker_id, broker_epoch, "broker registered with controller");
            Ok(Some(broker_epoch))
        }
        Ok(event) => Err(anyhow!("unexpected broker registration result: {event:?}")),
        Err(error) if error.retryable => {
            tracing::warn!(%error, "broker registration failed; retrying after next tick");
            Ok(None)
        }
        Err(error) => {
            if let Some(epoch) = expected_epoch {
                let image = controller.local_metadata().await?;
                if image
                    .brokers
                    .get(&registration.broker_id)
                    .is_none_or(|registered| registered.broker_epoch < epoch)
                {
                    // The process already received this epoch from a committed
                    // registration. An older local image cannot prove that a
                    // replacement superseded it. Stay suspended and retry the
                    // same conditional registration; never drop the epoch guard.
                    tracing::warn!(%error, broker_id = registration.broker_id, epoch,
                        "registration rejected against older local metadata; retrying");
                    return Ok(None);
                }
            }
            Err(anyhow!("broker registration was rejected: {error}"))
        }
    }
}

/// What one heartbeat attempt established about this broker's lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeartbeatOutcome {
    /// The controller acknowledged the epoch; the lease is renewed.
    Renewed,
    /// Nothing was established. Try again on the next tick, within the
    /// independently enforced lease deadline.
    Retry,
    /// The controller has fenced this epoch, but nothing newer holds this
    /// broker id. Being fenced for lateness is not proof of replacement:
    /// suspend and let a conditional registration decide.
    Reregister,
    /// The process is shutting down.
    Shutdown,
}

async fn heartbeat_broker(
    controller: &ControllerNode,
    broker_id: i32,
    broker_epoch: BrokerEpoch,
    prefer_remote_leader: bool,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<HeartbeatOutcome> {
    let command = MetadataCommand::Heartbeat {
        broker_id,
        broker_epoch,
        now_ms: unix_time_ms(),
    };
    let write = async {
        if prefer_remote_leader {
            controller.write_metadata_via_remote_leader(command).await
        } else {
            controller.write_metadata(command).await
        }
    };
    // Keep one heartbeat write in flight until it completes or the lifecycle's
    // absolute lease deadline fires. Short per-attempt cancellation is unsafe:
    // the Raft request can still commit after its waiter is dropped, and rapid
    // retries can flood the controller with ambiguous writes during recovery.
    let Some(result) = (tokio::select! {
        result = write => Some(result),
        _ = wait_for_shutdown(shutdown.clone()) => None,
    }) else {
        return Ok(HeartbeatOutcome::Shutdown);
    };
    match result {
        Ok(MetadataEvent::BrokerHeartbeat {
            broker_id: acknowledged_id,
            broker_epoch: acknowledged_epoch,
        }) if acknowledged_id == broker_id && acknowledged_epoch == broker_epoch => {
            Ok(HeartbeatOutcome::Renewed)
        }
        Ok(event) => Err(anyhow!("unexpected broker heartbeat result: {event:?}")),
        Err(error) if error.retryable => {
            tracing::warn!(%error, "broker heartbeat could not reach the active controller");
            Ok(HeartbeatOutcome::Retry)
        }
        Err(error) => {
            // Classify against the metadata rather than against the message
            // text: what matters is whether anything newer holds this broker
            // id, and only the image can say.
            let image = controller
                .local_metadata()
                .await
                .context("failed to inspect local metadata after heartbeat rejection")?;
            let registered = image.brokers.get(&broker_id);
            // A follower may still believe it is the leader immediately after
            // restart and pre-validate this heartbeat against its stale local
            // broker epoch. The registration itself already committed on the
            // active leader, so let the lifecycle retry until the local image
            // catches up or the independently enforced lease deadline expires.
            let locally_older =
                registered.is_none_or(|registered| registered.broker_epoch < broker_epoch);
            if locally_older {
                tracing::warn!(
                    %error,
                    broker_id,
                    broker_epoch,
                    "broker heartbeat was rejected by a stale local controller image; retrying"
                );
                return Ok(HeartbeatOutcome::Retry);
            }
            // The controller fenced this exact epoch and nothing newer has
            // claimed the id. That happens whenever an election outlasts the
            // session timeout: the writes that would have renewed the lease
            // had nowhere to commit. Re-registering conditionally is the
            // correct answer, and it is refused if a zombie is what this is.
            if registered.is_some_and(|registered| {
                registered.broker_epoch == broker_epoch && !registered.alive
            }) {
                tracing::warn!(
                    %error,
                    broker_id,
                    broker_epoch,
                    "broker was fenced by the controller; re-registering conditionally"
                );
                return Ok(HeartbeatOutcome::Reregister);
            }
            Err(anyhow!("broker heartbeat was rejected: {error}"))
        }
    }
}

async fn run_leader_maintenance(
    controller: &ControllerNode,
    settings: &ClusterSettings,
    fencing_open: bool,
    rebalance_leadership: bool,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let image = controller
        .local_metadata()
        .await
        .context("failed to read metadata for leader maintenance")?;
    let commands = leader_maintenance_commands(
        &image,
        settings.node_id,
        settings.broker_id,
        controller.raft_metrics().current_leader,
        FencingWindow {
            now_ms: unix_time_ms(),
            session_timeout_ms: duration_millis_i64(settings.session_timeout),
            open: fencing_open,
        },
        rebalance_leadership,
    );
    for command in commands {
        let Some(result) = write_or_shutdown(controller, command, shutdown).await else {
            return Ok(());
        };
        match result {
            Ok(event) => tracing::info!(?event, "controller leader maintenance applied"),
            Err(error) if error.retryable => {
                tracing::warn!(%error, "controller leadership changed during maintenance");
                return Ok(());
            }
            Err(error) => {
                // Another committed command can make an expiry/controller update
                // stale. Recompute from the next applied image instead of
                // terminating an otherwise healthy combined node.
                tracing::debug!(%error, "controller maintenance command became stale");
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct ReplicaLagState {
    missing_since: BTreeMap<(String, i32, i32, i32), i64>,
}

async fn replication_maintenance(
    broker: Arc<Broker>,
    controller: Arc<ControllerNode>,
    metadata_cache: MetadataCache,
    settings: ClusterSettings,
    broker_epoch: watch::Receiver<BrokerEpoch>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut refresh = interval(REPLICATION_RECONCILE_INTERVAL);
    refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut lag_state = ReplicaLagState::default();

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = refresh.tick() => {
                let image = metadata_cache.snapshot();
                let epoch = *broker_epoch.borrow();
                let local_registered = epoch != 0
                    && image.brokers.get(&settings.broker_id).is_some_and(|registered| {
                        registered.alive
                            && registered.broker_epoch == epoch
                            && registered.roles.contains(&NodeRole::Broker)
                    });
                if !local_registered {
                    lag_state.missing_since.clear();
                    continue;
                }

                let mut leader_offsets = BTreeMap::new();
                for topic in image.topics.values() {
                    for assignment in topic.partitions.values() {
                        if assignment.leader != settings.broker_id {
                            continue;
                        }
                        if let Err(error) = broker
                            .reconcile_leader_partition(&topic.name, assignment)
                            .await
                        {
                            tracing::debug!(
                                topic = %topic.name,
                                partition = assignment.partition,
                                %error,
                                "leader assignment changed during replication reconciliation"
                            );
                            continue;
                        }
                        match broker.partition(&topic.name, assignment.partition) {
                            Ok(handle) => match handle.offsets().await {
                                Ok((_, log_end, high_watermark)) => {
                                    leader_offsets.insert(
                                        (topic.name.clone(), assignment.partition),
                                        (log_end, high_watermark),
                                    );
                                }
                                Err(error) => tracing::warn!(
                                    topic = %topic.name,
                                    partition = assignment.partition,
                                    %error,
                                    "failed to read leader offsets for ISR reconciliation"
                                ),
                            },
                            Err(error) => tracing::debug!(
                                topic = %topic.name,
                                partition = assignment.partition,
                                %error,
                                "leader assignment changed before offset inspection"
                            ),
                        }
                    }
                }

                let health = broker.replication_health();
                let commands = replication_maintenance_commands(
                    ReplicationMaintenanceView {
                        image: &image,
                        broker_id: settings.broker_id,
                        broker_epoch: epoch,
                        health: &health,
                        leader_offsets: &leader_offsets,
                        now_ms: unix_time_ms(),
                        replica_lag_time_max_ms: duration_millis_i64(
                            settings.replica_lag_time_max,
                        ),
                    },
                    &mut lag_state,
                );
                drop(image);
                for command in commands {
                    // Completing a reassignment is a partition mutation but
                    // not an ISR mutation: the gate below exists to serialise
                    // an ISR change against Produce, and its preflight is
                    // written entirely in terms of ChangePartition. Routing
                    // this through it would only find a command it does not
                    // understand. It is safe to submit directly because the
                    // metadata state machine re-checks the ISR condition when
                    // it applies, so a completion that raced a shrinking ISR
                    // is rejected there rather than here.
                    if matches!(command, MetadataCommand::CompleteReassignment { .. }) {
                        match controller.write_metadata(command).await {
                            Ok(event) => {
                                tracing::info!(?event, "partition reassignment completed")
                            }
                            Err(error) => tracing::debug!(
                                ?error,
                                "reassignment completion deferred; will retry next tick"
                            ),
                        }
                        continue;
                    }
                    match apply_partition_change(
                        &broker,
                        &controller,
                        &metadata_cache,
                        command,
                        settings.replica_lag_time_max,
                        &mut shutdown,
                    )
                    .await?
                    {
                        Some(event) => {
                            tracing::info!(?event, "replica ISR reconciliation applied")
                        }
                        None => tracing::debug!(
                            "ISR reconciliation was stale or no longer safe to apply"
                        ),
                    }
                }
            }
        }
    }
}

/// Serialize an ISR mutation with Produce from the leader-assignment snapshot
/// through publication of the locally applied controller image. A retryable
/// controller result is ambiguous: keep the partition gate held and retry (or
/// wait for a definitive newer image) rather than exposing an old ISR after a
/// potentially committed expansion.
async fn apply_partition_change(
    broker: &Broker,
    controller: &ControllerNode,
    metadata_cache: &MetadataCache,
    command: MetadataCommand,
    replica_lag_time_max: Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<MetadataEvent>> {
    let (topic, partition, leader, mut desired_isr, expected_leader_epoch) = match &command {
        MetadataCommand::ChangePartition {
            topic,
            partition,
            leader,
            isr,
            expected_leader_epoch,
        } => (
            topic.clone(),
            *partition,
            *leader,
            isr.clone(),
            *expected_leader_epoch,
        ),
        _ => bail!("partition mutation gate received a non-partition command"),
    };
    desired_isr.sort_unstable();
    desired_isr.dedup();

    let _mutation_guard = tokio::select! {
        guard = broker.partition_mutation_guard(&topic, partition) => guard,
        _ = wait_for_shutdown(shutdown.clone()) => return Ok(None),
    };
    let mut known_event = None;
    let mut saw_ambiguous_result = false;
    let mut wait_only = false;

    loop {
        if broker.is_fenced() {
            bail!("broker fenced while serializing ISR mutation for {topic}-{partition}");
        }
        let image = controller
            .local_metadata()
            .await
            .context("failed to read locally applied ISR metadata")?;
        publish_if_newer(metadata_cache, image.clone());
        if let Err(error) = broker.validate_local_broker_epoch(&image) {
            if broker.is_fenced() {
                return Err(error.into());
            }
            // Lease suspension is recoverable. In particular, an ambiguous
            // ISR write may still commit while the lifecycle re-registers.
            // Keep the partition gate closed until a current image resolves
            // it; propagating this refusal would terminate the entire broker.
            if wait_or_shutdown(Duration::from_millis(25), shutdown).await {
                return Ok(None);
            }
            continue;
        }

        let Some(assignment) = image
            .topics
            .get(&topic)
            .and_then(|topic| topic.partitions.get(&partition))
        else {
            return Ok(None);
        };
        if assignment.leader_epoch > expected_leader_epoch {
            let desired_was_published =
                assignment.leader == leader && assignment.isr == desired_isr;
            return Ok(desired_was_published.then_some(known_event).flatten());
        }
        if assignment.leader_epoch == expected_leader_epoch
            && (assignment.leader != leader || assignment.leader != broker.config().broker_id)
        {
            return Ok(None);
        }

        // A locally lagging controller image cannot yet resolve an ambiguous
        // forwarded write. Keep the gate closed until it catches up.
        if assignment.leader_epoch < expected_leader_epoch || wait_only {
            if wait_or_shutdown(Duration::from_millis(25), shutdown).await {
                return Ok(None);
            }
            continue;
        }

        if !partition_change_is_fresh(
            broker,
            &image,
            &topic,
            assignment,
            &desired_isr,
            replica_lag_time_max,
        )
        .await?
        {
            return Ok(None);
        }

        let result = tokio::select! {
            result = controller.write_metadata(command.clone()) => result,
            _ = wait_for_shutdown(shutdown.clone()) => return Ok(None),
        };
        match result {
            Ok(event) => {
                known_event = Some(event);
                wait_only = true;
            }
            Err(error) if error.retryable => {
                saw_ambiguous_result = true;
                tracing::warn!(
                    %error,
                    topic,
                    partition,
                    "controller result is ambiguous; partition remains mutation-fenced"
                );
            }
            Err(error) if saw_ambiguous_result => {
                // A stale rejection after a retry may mean the earlier write
                // committed. Only the locally applied metadata image can
                // distinguish that from another concurrent partition change.
                tracing::warn!(
                    %error,
                    topic,
                    partition,
                    "awaiting authoritative metadata after ambiguous ISR mutation"
                );
                wait_only = true;
            }
            Err(error) => {
                tracing::debug!(%error, topic, partition, "ISR mutation was rejected");
                return Ok(None);
            }
        }
        if wait_or_shutdown(Duration::from_millis(25), shutdown).await {
            return Ok(None);
        }
    }
}

async fn partition_change_is_fresh(
    broker: &Broker,
    image: &ClusterMetadata,
    topic: &str,
    assignment: &brahmaputra_metadata::PartitionMetadata,
    desired_isr: &[i32],
    replica_lag_time_max: Duration,
) -> Result<bool> {
    let additions = desired_isr
        .iter()
        .copied()
        .filter(|broker_id| !assignment.isr.contains(broker_id))
        .collect::<Vec<_>>();
    if additions.is_empty() {
        return Ok(true);
    }

    let handle = broker.partition(topic, assignment.partition)?;
    let (_, log_end, _) = handle.offsets().await?;
    let health = broker.replication_health();
    let now_ms = unix_time_ms();
    let lag_max_ms = duration_millis_i64(replica_lag_time_max);

    Ok(additions.into_iter().all(|follower_id| {
        let follower = image.brokers.get(&follower_id);
        let observed = health.leader_followers.iter().find(|observed| {
            observed.topic == topic
                && observed.partition == assignment.partition
                && observed.follower_id == follower_id
        });
        follower.is_some_and(|follower| {
            follower.alive
                && follower.roles.contains(&NodeRole::Broker)
                && observed.is_some_and(|observed| {
                    observed.leader_epoch == assignment.leader_epoch
                        && observed.follower_broker_epoch == follower.broker_epoch
                        && observed.fetch_offset >= log_end
                        && now_ms.saturating_sub(observed.last_fetch_ms) <= lag_max_ms
                })
        })
    }))
}

struct ReplicationMaintenanceView<'a> {
    image: &'a ClusterMetadata,
    broker_id: i32,
    broker_epoch: BrokerEpoch,
    health: &'a ReplicationHealthSnapshot,
    leader_offsets: &'a BTreeMap<(String, i32), (i64, i64)>,
    now_ms: i64,
    replica_lag_time_max_ms: i64,
}

fn replication_maintenance_commands(
    view: ReplicationMaintenanceView<'_>,
    lag_state: &mut ReplicaLagState,
) -> Vec<MetadataCommand> {
    let ReplicationMaintenanceView {
        image,
        broker_id,
        broker_epoch,
        health,
        leader_offsets,
        now_ms,
        replica_lag_time_max_ms,
    } = view;
    let local_registered = broker_epoch != 0
        && image.brokers.get(&broker_id).is_some_and(|registered| {
            registered.alive
                && registered.broker_epoch == broker_epoch
                && registered.roles.contains(&NodeRole::Broker)
        });
    if !local_registered {
        lag_state.missing_since.clear();
        return Vec::new();
    }

    let progress = health
        .leader_followers
        .iter()
        .map(|follower| {
            (
                (
                    follower.topic.as_str(),
                    follower.partition,
                    follower.follower_id,
                ),
                follower,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut active_missing = BTreeSet::new();
    let mut commands = Vec::new();

    for topic in image.topics.values() {
        for assignment in topic.partitions.values() {
            if assignment.leader != broker_id {
                continue;
            }
            let Some((log_end, _)) =
                leader_offsets.get(&(topic.name.clone(), assignment.partition))
            else {
                continue;
            };
            let mut desired_isr = assignment.isr.clone();
            for follower_id in assignment
                .replicas
                .iter()
                .copied()
                .filter(|replica_id| *replica_id != broker_id)
            {
                let follower_alive = image.brokers.get(&follower_id).is_some_and(|registered| {
                    registered.alive && registered.roles.contains(&NodeRole::Broker)
                });
                let observed = progress
                    .get(&(topic.name.as_str(), assignment.partition, follower_id))
                    .copied()
                    .filter(|observed| {
                        observed.leader_epoch == assignment.leader_epoch
                            && image.brokers.get(&follower_id).is_some_and(|registered| {
                                registered.broker_epoch == observed.follower_broker_epoch
                            })
                    });
                if assignment.isr.contains(&follower_id) {
                    let last_fetch_ms = if let Some(observed) = observed {
                        observed.last_fetch_ms
                    } else {
                        let key = (
                            topic.name.clone(),
                            assignment.partition,
                            assignment.leader_epoch,
                            follower_id,
                        );
                        active_missing.insert(key.clone());
                        *lag_state.missing_since.entry(key).or_insert(now_ms)
                    };
                    if !follower_alive
                        || now_ms.saturating_sub(last_fetch_ms) > replica_lag_time_max_ms
                    {
                        desired_isr.retain(|member| *member != follower_id);
                    }
                } else if follower_alive
                    && observed.is_some_and(|observed| {
                        now_ms.saturating_sub(observed.last_fetch_ms) <= replica_lag_time_max_ms
                            && observed.fetch_offset >= *log_end
                    })
                {
                    desired_isr.push(follower_id);
                }
            }
            desired_isr.sort_unstable();
            desired_isr.dedup();
            if desired_isr != assignment.isr {
                commands.push(MetadataCommand::ChangePartition {
                    topic: topic.name.clone(),
                    partition: assignment.partition,
                    leader: assignment.leader,
                    isr: desired_isr.clone(),
                    expected_leader_epoch: assignment.leader_epoch,
                });
            }

            // Finish a reassignment whose targets have caught up.
            //
            // Only the leader can see this: the ISR is what proves the new
            // replicas actually hold the data, and until they do, dropping
            // the old ones would discard the only copies. Checked against
            // the ISR this tick is about to produce rather than the one in
            // the image, so completion does not wait an extra round for the
            // ChangePartition above to land.
            if let Some(target) = assignment.target_replicas.as_ref() {
                if target.iter().all(|broker| desired_isr.contains(broker)) {
                    commands.push(MetadataCommand::CompleteReassignment {
                        topic: topic.name.clone(),
                        partition: assignment.partition,
                    });
                }
            }
        }
    }
    lag_state
        .missing_since
        .retain(|key, _| active_missing.contains(key));
    commands
}

/// When a controller may fence brokers it has not heard from lately.
///
/// `open` is false for the first session timeout after this node becomes
/// the Raft leader. A broker renews its lease by writing to the quorum, so
/// an election is a window in which no broker *can* renew: the timestamps a
/// new leader inherits are all stale, and fencing on them fences the whole
/// cluster the moment it recovers — including the brokers that stayed up
/// the entire time. Every broker gets a full session timeout to check in
/// with the new leader first, which is the same grace Kafka's controller
/// gives after a failover. A broker that is genuinely gone is fenced one
/// session timeout later than it otherwise would have been.
#[derive(Debug, Clone, Copy)]
struct FencingWindow {
    now_ms: i64,
    session_timeout_ms: i64,
    open: bool,
}

fn leader_maintenance_commands(
    image: &ClusterMetadata,
    node_id: NodeId,
    broker_id: i32,
    current_leader: Option<NodeId>,
    fencing: FencingWindow,
    rebalance_leadership: bool,
) -> Vec<MetadataCommand> {
    if current_leader != Some(node_id) {
        return Vec::new();
    }
    let mut commands = Vec::new();
    let local_is_live_controller = image
        .brokers
        .get(&broker_id)
        .is_some_and(|broker| broker.alive && broker.roles.contains(&NodeRole::Controller));
    if image.controller_id != Some(broker_id) && local_is_live_controller {
        commands.push(MetadataCommand::SetController { broker_id });
    }
    if fencing.open {
        commands.extend(
            image
                .expired_brokers(fencing.now_ms, fencing.session_timeout_ms)
                .into_iter()
                .map(|(broker_id, broker_epoch)| MetadataCommand::ExpireBroker {
                    broker_id,
                    broker_epoch,
                    last_heartbeat_ms: image.brokers[&broker_id].last_heartbeat_ms,
                }),
        );
    }
    if rebalance_leadership {
        commands.extend(preferred_leader_commands(image));
    }
    commands
}

/// Move leadership back to each partition's preferred replica.
///
/// A partition fails over when its leader dies, and nothing brings
/// leadership back when that broker returns — so after a rolling restart
/// every partition is led by whichever broker happened to be next in line.
/// Left alone, load concentrates on the brokers that stayed up, which is
/// exactly the set already carrying the most work.
///
/// Only partitions whose preferred replica is *in the ISR* are moved. That
/// replica therefore holds every committed record, so the handover cannot
/// lose data — it costs one leader-epoch bump. Partitions mid-reassignment
/// are skipped: their replica set is deliberately a superset of where they
/// are heading, so `replicas[0]` is not yet meaningful.
fn preferred_leader_commands(image: &ClusterMetadata) -> Vec<MetadataCommand> {
    let mut commands = Vec::new();
    for topic in image.topics.values() {
        for assignment in topic.partitions.values() {
            if !assignment.is_leader_imbalanced() {
                continue;
            }
            let Some(preferred) = assignment.preferred_leader() else {
                continue;
            };
            // The ISR is only evidence of a caught-up *replica*; the broker
            // must also still be alive and serving as a broker, or the
            // handover would elect a node that cannot take the traffic.
            let usable = image
                .brokers
                .get(&preferred)
                .is_some_and(|broker| broker.alive && broker.roles.contains(&NodeRole::Broker));
            if !usable {
                continue;
            }
            commands.push(MetadataCommand::ChangePartition {
                topic: topic.name.clone(),
                partition: assignment.partition,
                leader: preferred,
                isr: assignment.isr.clone(),
                expected_leader_epoch: assignment.leader_epoch,
            });
        }
    }
    commands
}

#[cfg(test)]
fn local_registration_is_fenced(
    image: &ClusterMetadata,
    broker_id: i32,
    epoch: BrokerEpoch,
) -> bool {
    image.brokers.get(&broker_id).is_some_and(|broker| {
        broker.broker_epoch > epoch
            || (broker.broker_epoch == epoch
                && (!broker.alive || !broker.roles.contains(&NodeRole::Broker)))
    })
}

async fn write_or_shutdown(
    controller: &ControllerNode,
    command: MetadataCommand,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<brahmaputra_controller::ControllerCommandResult> {
    tokio::select! {
        result = controller.write_metadata(command) => Some(result),
        _ = wait_for_shutdown(shutdown.clone()) => None,
    }
}

/// Create the internal topics once the local node is a registered broker
/// (broker epoch != 0): `__consumer_offsets` for group state, and
/// `__transaction_state` for the transaction coordinator.
///
/// Every node attempts each create; all but one lose the race, which is
/// fine as long as the topics exist in the applied image afterwards.
async fn ensure_internal_topics(
    controller: Arc<ControllerNode>,
    cache: MetadataCache,
    mut broker_epoch: watch::Receiver<BrokerEpoch>,
    partitions: i32,
    replication_factor: i32,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        if *broker_epoch.borrow_and_update() != 0 {
            break;
        }
        tokio::select! {
            _ = wait_for_shutdown(shutdown.clone()) => return Ok(()),
            changed = broker_epoch.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
            }
        }
    }

    // The transaction state topic is deliberately smaller: it holds one
    // record per transactional id rather than one per group-partition, and
    // its partition count is what bounds how many coordinators a cluster
    // spreads that work across.
    for (name, partitions) in [
        (OFFSETS_TOPIC, partitions),
        (TRANSACTION_STATE_TOPIC, partitions.clamp(1, 50)),
    ] {
        create_internal_topic(
            &controller,
            &cache,
            name,
            partitions,
            replication_factor,
            &mut shutdown,
        )
        .await?;
    }
    // The topics are ensured; park until shutdown. Returning early would
    // make supervise_components tear down the whole node (any completed
    // component is treated as a stop signal).
    wait_for_shutdown(shutdown).await;
    Ok(())
}

/// Create one internal topic if the applied image does not already have it.
///
/// Losing the create race is not a failure: several nodes attempt it at
/// startup, and the only thing that matters is that the topic exists
/// afterwards.
async fn create_internal_topic(
    controller: &Arc<ControllerNode>,
    cache: &MetadataCache,
    name: &str,
    partitions: i32,
    replication_factor: i32,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let image = controller
        .local_metadata()
        .await
        .with_context(|| format!("failed to read metadata before creating {name}"))?;
    publish_if_newer(cache, image.clone());
    if image.topics.contains_key(name) {
        return Ok(());
    }
    let live = image.brokers.values().filter(|broker| broker.alive).count();
    // An explicit factor is honoured as given, but never above the number
    // of brokers that exist: a topic asking for more replicas than there
    // are brokers cannot be created at all, and failing to create the
    // offsets topic takes the whole cluster down with it.
    let replication_factor = if replication_factor > 0 {
        replication_factor.min(live.max(1) as i32)
    } else {
        (live as i32).clamp(1, 3)
    };
    let command = MetadataCommand::CreateTopic {
        name: name.to_owned(),
        partitions,
        replication_factor,
        configs: BTreeMap::new(),
    };
    let Some(result) = write_or_shutdown(controller, command, shutdown).await else {
        return Ok(());
    };
    match result {
        Ok(event) => {
            tracing::info!(?event, name, partitions, "internal topic created");
        }
        Err(error) => {
            // Losing the race is the expected outcome for all but one node,
            // and the loser learns it did by finding the topic present.
            //
            // But "present" has to be given time to arrive. The create was
            // refused by the *leader*, whose state this node's local Raft
            // copy trails by however long replication takes — so a single
            // look can miss a topic that certainly exists, and bailing on
            // that look takes the node down at startup for a race it was
            // designed to lose.
            let deadline = Instant::now() + INTERNAL_TOPIC_SETTLE;
            loop {
                let image = controller
                    .local_metadata()
                    .await
                    .with_context(|| format!("failed to re-read metadata after creating {name}"))?;
                let present = image.topics.contains_key(name);
                publish_if_newer(cache, image);
                if present {
                    break;
                }
                if Instant::now() >= deadline {
                    bail!("failed to create internal topic {name}: {error}")
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    Ok(())
}

async fn synchronize_metadata(
    controller: Arc<ControllerNode>,
    cache: MetadataCache,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut refresh = interval(METADATA_SYNC_INTERVAL);
    refresh.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = refresh.tick() => {
                let image = controller
                    .local_metadata()
                    .await
                    .context("failed to synchronize local controller metadata")?;
                publish_if_newer(&cache, image);
            }
        }
    }
}

fn publish_if_newer(cache: &MetadataCache, image: ClusterMetadata) {
    let current = cache.snapshot();
    if image.cluster_id == current.cluster_id && image.offset >= current.offset {
        drop(current);
        cache.replace(image);
    }
}

async fn supervise_components(
    shutdown: &watch::Sender<bool>,
    components: &mut JoinSet<Result<()>>,
) -> Result<()> {
    let mut failure = None;
    tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            if let Err(error) = signal {
                failure = Some(anyhow!(error).context("failed to listen for Ctrl-C"));
            }
        }
        completed = components.join_next() => {
            if let Some(result) = completed {
                record_component_result(result, &mut failure);
            }
        }
    }

    let _ = shutdown.send(true);
    while let Some(result) = components.join_next().await {
        record_component_result(result, &mut failure);
    }
    if let Some(error) = failure {
        Err(error)
    } else {
        Ok(())
    }
}

fn record_component_result(
    result: std::result::Result<Result<()>, JoinError>,
    failure: &mut Option<anyhow::Error>,
) {
    let error = match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error),
        Err(error) => Some(anyhow!(error).context("combined-node component task failed")),
    };
    if failure.is_none() {
        *failure = error;
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.changed().await;
}

async fn wait_or_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(duration) => false,
        _ = wait_for_shutdown(shutdown.clone()) => true,
    }
}

fn unix_time_ms() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    duration_millis_i64(elapsed)
}

fn duration_millis_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

/// Create the cluster session secret and first admin once this node is a
/// registered broker (DESIGN.md §9.4).
///
/// Waits for the same broker-epoch signal as the offsets topic, so it only
/// runs on a node whose registration has committed and therefore has a
/// controller to write through.
async fn ensure_admin_user(
    controller: Arc<ControllerNode>,
    metadata_cache: MetadataCache,
    mut broker_epoch: watch::Receiver<u64>,
    admin_user: String,
    admin_password: Option<String>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        if *shutdown.borrow_and_update() {
            return Ok(());
        }
        if *broker_epoch.borrow_and_update() > 0 {
            break;
        }
        tokio::select! {
            _ = shutdown.changed() => {}
            changed = broker_epoch.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
            }
        }
    }

    let image = metadata_cache.snapshot();
    if let Err(error) =
        observability::bootstrap_admin(&controller, &image, &admin_user, admin_password.as_deref())
            .await
    {
        // A losing racer sees "already exists"; a real failure is worth
        // surfacing but must not take the node down, since the data plane
        // is unaffected by the dashboard having no users yet.
        tracing::warn!(%error, "could not bootstrap the admin user");
    }
    // Park until shutdown, like ensure_offsets_topic: a component that
    // returns is read by supervise_components as a stop signal for the
    // whole node.
    wait_for_shutdown(shutdown).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use brahmaputra_metadata::{BrokerMetadata, PartitionMetadata, TopicMetadata};
    use std::collections::BTreeSet;

    fn parse(arguments: &[&str]) -> Args {
        Args::try_parse_from(arguments).unwrap()
    }

    #[tokio::test]
    async fn lease_recovery_retries_a_lost_receipt_and_confirms_liveness_before_activation() {
        let controller = ControllerNode::new(ControllerConfig::new(
            1,
            "registration-receipt",
            BTreeMap::from([(1, "127.0.0.1:9".into())]),
        ))
        .await
        .unwrap();
        controller.bootstrap().await.unwrap();
        controller
            .wait_for_leader(Duration::from_secs(5))
            .await
            .unwrap();
        let registration = BrokerRegistration {
            broker_id: 1,
            host: "127.0.0.1".into(),
            data_port: 0,
            control_port: 0,
            internal_port: 0,
            rack: None,
        };
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let mut initial = PendingRegistration::new(None).unwrap();
        assert_eq!(
            register_broker(&controller, &registration, &mut initial, &mut shutdown_rx)
                .await
                .unwrap(),
            Some(1)
        );
        let mut recovery = PendingRegistration::new(Some(1)).unwrap();
        // Commit, then discard the response as if the HTTP connection closed.
        assert_eq!(
            submit_registration(&controller, &registration, &recovery, &mut shutdown_rx)
                .await
                .unwrap(),
            Some(2)
        );
        assert_eq!(recovery.acknowledged_epoch, None);
        let identity = recovery.registration_id.clone();
        assert_eq!(
            register_broker(&controller, &registration, &mut recovery, &mut shutdown_rx)
                .await
                .unwrap(),
            Some(2)
        );
        assert_eq!(recovery.registration_id, identity);
        assert_eq!(
            controller.local_metadata().await.unwrap().brokers[&1].broker_epoch,
            2
        );

        controller
            .write_metadata(MetadataCommand::FenceBroker {
                broker_id: 1,
                broker_epoch: 2,
            })
            .await
            .unwrap();
        // Even a known receipt must not activate an expired/fenced incarnation.
        assert_eq!(
            register_broker(&controller, &registration, &mut recovery, &mut shutdown_rx)
                .await
                .unwrap(),
            None
        );
        assert_eq!(recovery.expected_epoch, Some(2));
        assert_ne!(recovery.registration_id, identity);
        assert!(!controller.local_metadata().await.unwrap().brokers[&1].alive);
        assert_eq!(
            register_broker(&controller, &registration, &mut recovery, &mut shutdown_rx)
                .await
                .unwrap(),
            Some(3)
        );

        let mut replacement = PendingRegistration::new(None).unwrap();
        assert_eq!(
            register_broker(
                &controller,
                &registration,
                &mut replacement,
                &mut shutdown_rx
            )
            .await
            .unwrap(),
            Some(4)
        );
        assert!(
            register_broker(&controller, &registration, &mut recovery, &mut shutdown_rx)
                .await
                .is_err()
        );
        assert_eq!(
            controller.local_metadata().await.unwrap().brokers[&1].broker_epoch,
            4
        );
        controller.shutdown_raft().await.unwrap();
    }

    #[tokio::test]
    async fn isr_mutation_waits_for_lease_recovery_but_rejects_replacement() {
        let controller = ControllerNode::new(ControllerConfig::new(
            1,
            "isr-lease",
            BTreeMap::from([(1, "127.0.0.1:9".into())]),
        ))
        .await
        .unwrap();
        controller.bootstrap().await.unwrap();
        controller
            .wait_for_leader(Duration::from_secs(5))
            .await
            .unwrap();
        let registration = MetadataCommand::RegisterBroker {
            broker_id: 1,
            host: "127.0.0.1".into(),
            data_port: 0,
            control_port: 0,
            internal_port: 0,
            expected_epoch: None,
            registration_id: None,
            roles: vec![NodeRole::Broker],
            rack: None,
            now_ms: unix_time_ms(),
        };
        controller
            .write_metadata(registration.clone())
            .await
            .unwrap();
        let cache = MetadataCache::new(controller.local_metadata().await.unwrap());
        let directory = tempfile::tempdir().unwrap();
        let broker = Broker::bind(BrokerConfig {
            broker_id: 1,
            broker_epoch: Some(1),
            port: 0,
            data_dirs: vec![directory.path().to_owned()],
            metadata_cache: Some(cache.clone()),
            ..BrokerConfig::default()
        })
        .await
        .unwrap();
        let command = MetadataCommand::ChangePartition {
            topic: "removed-during-recovery".into(),
            partition: 0,
            leader: 1,
            isr: vec![1],
            expected_leader_epoch: 0,
        };
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        broker.suspend_broker_lease();
        let mutation = apply_partition_change(
            &broker,
            &controller,
            &cache,
            command.clone(),
            Duration::from_secs(10),
            &mut shutdown_rx,
        );
        tokio::pin!(mutation);
        assert!(
            tokio::time::timeout(Duration::from_millis(75), &mut mutation)
                .await
                .is_err()
        );
        assert!(!broker.is_fenced());
        // Even during suspension, a competing Produce cannot pass an ISR
        // mutation whose commit status has not yet been resolved.
        assert!(tokio::time::timeout(
            Duration::from_millis(25),
            broker.partition_mutation_guard("removed-during-recovery", 0),
        )
        .await
        .is_err());
        broker.activate_broker_epoch(1).unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(2), &mut mutation)
            .await
            .unwrap()
            .unwrap()
            .is_none());

        // An actual replacement must still irreversibly fence this process.
        controller.write_metadata(registration).await.unwrap();
        let mut replacement_shutdown = shutdown_tx.subscribe();
        assert!(apply_partition_change(
            &broker,
            &controller,
            &cache,
            command,
            Duration::from_secs(10),
            &mut replacement_shutdown,
        )
        .await
        .is_err());
        assert!(broker.is_fenced());
        controller.shutdown_raft().await.unwrap();
    }

    #[test]
    fn original_invocation_remains_standalone() {
        let args = parse(&[
            "server",
            "--host",
            "127.0.0.1",
            "--port",
            "9092",
            "--data-dir",
            "./data",
            "--default-partitions",
            "3",
            "--segment-bytes",
            "1024",
            "--retention-ms",
            "5000",
        ]);
        assert!(cluster_settings(&args).unwrap().is_none());
    }

    #[test]
    fn parses_fixed_peer_map_and_cluster_timing() {
        let args = parse(&[
            "server",
            "--node-id",
            "2",
            "--cluster-id",
            "test-cluster",
            "--control-port",
            "19093",
            "--controller-peer",
            "1=127.0.0.1:19092",
            "--controller-peer",
            "2=http://127.0.0.1:19093",
            "--heartbeat-interval-ms",
            "100",
            "--session-timeout-ms",
            "500",
            "--replica-lag-time-max-ms",
            "750",
            "--rack",
            "rack-b",
        ]);
        let settings = cluster_settings(&args).unwrap().unwrap();
        assert_eq!((settings.node_id, settings.broker_id), (2, 2));
        assert_eq!(settings.peers.len(), 2);
        assert_eq!(settings.peers[&1], "127.0.0.1:19092");
        assert_eq!(settings.peers[&2], "http://127.0.0.1:19093");
        assert_eq!(settings.heartbeat_interval, Duration::from_millis(100));
        assert_eq!(settings.session_timeout, Duration::from_millis(500));
        assert_eq!(settings.replica_lag_time_max, Duration::from_millis(750));
        assert_eq!(settings.rack.as_deref(), Some("rack-b"));
    }

    #[test]
    fn rejects_duplicate_or_unrepresentable_node_ids() {
        let duplicate = parse(&[
            "server",
            "--node-id",
            "1",
            "--controller-peer",
            "1=127.0.0.1:19092",
            "--controller-peer",
            "1=127.0.0.1:29092",
        ]);
        assert!(cluster_settings(&duplicate)
            .unwrap_err()
            .to_string()
            .contains("duplicate"));

        let too_large = (i32::MAX as u64 + 1).to_string();
        let parse_error = Args::try_parse_from([
            "server",
            "--node-id",
            &too_large,
            "--controller-peer",
            &format!("{too_large}=127.0.0.1:19092"),
        ])
        .unwrap_err();
        assert!(parse_error.to_string().contains("i32 broker ID"));
    }

    /// A controller that has been leading for longer than one session
    /// timeout, which is when expiry decisions become evidence-based.
    fn open_window(now_ms: i64, session_timeout_ms: i64) -> FencingWindow {
        FencingWindow {
            now_ms,
            session_timeout_ms,
            open: true,
        }
    }

    #[test]
    fn a_new_controller_leader_fences_nobody_until_its_grace_has_passed() {
        // Every heartbeat is a quorum write, so an election is a window in
        // which no broker can renew. A leader that fenced on the timestamps
        // it inherits would fence the brokers that survived the election —
        // the whole cluster, at the moment it recovers.
        let mut image = ClusterMetadata::new("test");
        image.brokers.insert(1, broker(1, 7, 1_800));
        image.brokers.insert(2, broker(2, 9, 10));

        let commands = leader_maintenance_commands(
            &image,
            1,
            1,
            Some(1),
            FencingWindow {
                now_ms: 2_000,
                session_timeout_ms: 500,
                open: false,
            },
            false,
        );
        assert!(
            !commands
                .iter()
                .any(|command| matches!(command, MetadataCommand::ExpireBroker { .. })),
            "a leader inside its grace window must fence nobody"
        );
        assert!(
            commands
                .iter()
                .any(|command| matches!(command, MetadataCommand::SetController { .. })),
            "the rest of maintenance still runs"
        );

        // Once the grace has passed, the same image expires broker 2.
        let commands =
            leader_maintenance_commands(&image, 1, 1, Some(1), open_window(2_000, 500), false);
        assert!(commands
            .iter()
            .any(|command| matches!(command, MetadataCommand::ExpireBroker { broker_id: 2, .. })));
    }

    #[test]
    fn only_leader_generates_controller_and_expiry_commands() {
        let mut image = ClusterMetadata::new("test");
        image.brokers.insert(1, broker(1, 7, 1_800));
        image.brokers.insert(2, broker(2, 9, 10));

        assert!(
            leader_maintenance_commands(&image, 1, 1, Some(2), open_window(2_000, 500), false)
                .is_empty()
        );
        let commands =
            leader_maintenance_commands(&image, 1, 1, Some(1), open_window(2_000, 500), false);
        assert_eq!(commands.len(), 2);
        assert!(matches!(
            commands[0],
            MetadataCommand::SetController { broker_id: 1 }
        ));
        assert!(matches!(
            commands[1],
            MetadataCommand::ExpireBroker {
                broker_id: 2,
                broker_epoch: 9,
                last_heartbeat_ms: 10,
            }
        ));

        image
            .brokers
            .get_mut(&1)
            .unwrap()
            .roles
            .remove(&NodeRole::Controller);
        let commands =
            leader_maintenance_commands(&image, 1, 1, Some(1), open_window(2_000, 500), false);
        assert!(!commands
            .iter()
            .any(|command| matches!(command, MetadataCommand::SetController { .. })));
    }

    #[test]
    fn leadership_returns_to_the_preferred_replica_only_when_it_is_in_sync() {
        let mut image = ClusterMetadata::new("test");
        for id in 1..=3 {
            image.brokers.insert(id, broker(id, 1, 1_900));
        }
        // Broker 2 is preferred but broker 3 is leading: what a restart of
        // broker 2 leaves behind once it has caught up again.
        image.topics.insert(
            "orders".into(),
            TopicMetadata {
                name: "orders".into(),
                topic_epoch: 1,
                replication_factor: 3,
                partitions: BTreeMap::from([(
                    0,
                    PartitionMetadata {
                        partition: 0,
                        replicas: vec![2, 3, 1],
                        leader: 3,
                        isr: vec![1, 3],
                        leader_epoch: 4,
                        target_replicas: None,
                        last_isr: Vec::new(),
                    },
                )]),
                configs: BTreeMap::new(),
            },
        );

        // Still catching up: leadership must not move to a replica that
        // does not hold every committed record.
        let commands =
            leader_maintenance_commands(&image, 1, 1, Some(1), open_window(2_000, 500), true);
        assert!(!commands
            .iter()
            .any(|command| matches!(command, MetadataCommand::ChangePartition { .. })));

        let partition = image
            .topics
            .get_mut("orders")
            .unwrap()
            .partitions
            .get_mut(&0)
            .unwrap();
        partition.isr = vec![1, 2, 3];

        // Rejoined the ISR — but nothing moves until the rebalance is due.
        let commands =
            leader_maintenance_commands(&image, 1, 1, Some(1), open_window(2_000, 500), false);
        assert!(!commands
            .iter()
            .any(|command| matches!(command, MetadataCommand::ChangePartition { .. })));

        let commands =
            leader_maintenance_commands(&image, 1, 1, Some(1), open_window(2_000, 500), true);
        let change = commands
            .iter()
            .find_map(|command| match command {
                MetadataCommand::ChangePartition {
                    topic,
                    partition,
                    leader,
                    expected_leader_epoch,
                    ..
                } => Some((topic.clone(), *partition, *leader, *expected_leader_epoch)),
                _ => None,
            })
            .expect("preferred leader handover");
        assert_eq!(change, ("orders".to_owned(), 0, 2, 4));

        // A partition mid-reassignment holds the union of old and new
        // replicas, so its first replica is not yet a placement decision.
        image
            .topics
            .get_mut("orders")
            .unwrap()
            .partitions
            .get_mut(&0)
            .unwrap()
            .target_replicas = Some(vec![1, 3]);
        let commands =
            leader_maintenance_commands(&image, 1, 1, Some(1), open_window(2_000, 500), true);
        assert!(!commands
            .iter()
            .any(|command| matches!(command, MetadataCommand::ChangePartition { .. })));
    }

    #[test]
    fn registration_state_only_fences_the_current_lifecycle() {
        let mut image = ClusterMetadata::new("test");
        image.brokers.insert(3, broker(3, 4, 100));
        assert!(!local_registration_is_fenced(&image, 3, 4));
        assert!(local_registration_is_fenced(&image, 3, 3));
        image.brokers.get_mut(&3).unwrap().alive = false;
        assert!(local_registration_is_fenced(&image, 3, 4));

        // A forwarded registration may commit before this follower's local
        // metadata image catches up. A lower/missing epoch is not proof that
        // this already-active lifecycle has been superseded.
        assert!(!local_registration_is_fenced(&image, 3, 5));
        assert!(!local_registration_is_fenced(
            &ClusterMetadata::new("test"),
            3,
            5
        ));
    }

    #[test]
    fn isr_reconciliation_has_startup_grace_then_requires_hwm_catchup() {
        use brahmaputra_broker::LeaderFollowerHealth;
        use brahmaputra_metadata::{PartitionMetadata, TopicMetadata};

        let mut image = ClusterMetadata::new("test");
        image.brokers.insert(1, broker(1, 7, 1_000));
        image.brokers.insert(2, broker(2, 8, 1_000));
        image.brokers.insert(3, broker(3, 9, 1_000));
        image.topics.insert(
            "orders".into(),
            TopicMetadata {
                name: "orders".into(),
                topic_epoch: 1,
                replication_factor: 3,
                partitions: BTreeMap::from([(
                    0,
                    PartitionMetadata {
                        partition: 0,
                        leader: 1,
                        replicas: vec![1, 2, 3],
                        isr: vec![1, 2, 3],
                        leader_epoch: 4,
                        target_replicas: None,
                        last_isr: Vec::new(),
                    },
                )]),
                configs: BTreeMap::new(),
            },
        );
        let offsets = BTreeMap::from([(("orders".to_owned(), 0), (10, 8))]);
        let health = ReplicationHealthSnapshot {
            leader_followers: vec![LeaderFollowerHealth {
                topic: "orders".into(),
                partition: 0,
                leader_epoch: 4,
                follower_id: 2,
                follower_broker_epoch: 8,
                fetch_offset: 7,
                last_fetch_ms: 1_050,
                in_sync: true,
            }],
            follower_fetchers: Vec::new(),
        };
        let mut lag = ReplicaLagState::default();

        assert!(replication_maintenance_commands(
            ReplicationMaintenanceView {
                image: &image,
                broker_id: 1,
                broker_epoch: 7,
                health: &health,
                leader_offsets: &offsets,
                now_ms: 1_000,
                replica_lag_time_max_ms: 100,
            },
            &mut lag,
        )
        .is_empty());
        let shrink = replication_maintenance_commands(
            ReplicationMaintenanceView {
                image: &image,
                broker_id: 1,
                broker_epoch: 7,
                health: &health,
                leader_offsets: &offsets,
                now_ms: 1_101,
                replica_lag_time_max_ms: 100,
            },
            &mut lag,
        );
        assert!(matches!(
            shrink.as_slice(),
            [MetadataCommand::ChangePartition {
                leader: 1,
                isr,
                expected_leader_epoch: 4,
                ..
            }] if isr == &vec![1, 2]
        ));

        let assignment = image
            .topics
            .get_mut("orders")
            .unwrap()
            .partitions
            .get_mut(&0)
            .unwrap();
        assignment.isr = vec![1, 2];
        assignment.leader_epoch = 5;
        let mut caught_up = ReplicationHealthSnapshot {
            leader_followers: vec![
                LeaderFollowerHealth {
                    topic: "orders".into(),
                    partition: 0,
                    leader_epoch: 5,
                    follower_id: 2,
                    follower_broker_epoch: 8,
                    fetch_offset: 8,
                    last_fetch_ms: 1_110,
                    in_sync: true,
                },
                LeaderFollowerHealth {
                    topic: "orders".into(),
                    partition: 0,
                    leader_epoch: 5,
                    follower_id: 3,
                    follower_broker_epoch: 9,
                    fetch_offset: 8,
                    last_fetch_ms: 1_110,
                    in_sync: false,
                },
            ],
            follower_fetchers: Vec::new(),
        };
        assert!(replication_maintenance_commands(
            ReplicationMaintenanceView {
                image: &image,
                broker_id: 1,
                broker_epoch: 7,
                health: &caught_up,
                leader_offsets: &offsets,
                now_ms: 1_111,
                replica_lag_time_max_ms: 100,
            },
            &mut lag,
        )
        .is_empty());

        // Matching the HWM is insufficient while the leader has an
        // uncommitted suffix. Expansion is allowed only after the follower's
        // next fetch proves it has reached the fresh leader LEO.
        caught_up.leader_followers[1].fetch_offset = 10;
        let expand = replication_maintenance_commands(
            ReplicationMaintenanceView {
                image: &image,
                broker_id: 1,
                broker_epoch: 7,
                health: &caught_up,
                leader_offsets: &offsets,
                now_ms: 1_111,
                replica_lag_time_max_ms: 100,
            },
            &mut lag,
        );
        assert!(matches!(
            expand.as_slice(),
            [MetadataCommand::ChangePartition {
                leader: 1,
                isr,
                expected_leader_epoch: 5,
                ..
            }] if isr == &vec![1, 2, 3]
        ));
    }

    fn broker(broker_id: i32, broker_epoch: BrokerEpoch, last_heartbeat_ms: i64) -> BrokerMetadata {
        BrokerMetadata {
            broker_id,
            host: "127.0.0.1".into(),
            data_port: 9_092,
            control_port: 19_092,
            internal_port: 0,
            broker_epoch,
            registration_id: None,
            roles: BTreeSet::from([NodeRole::Broker, NodeRole::Controller]),
            rack: None,
            alive: true,
            last_heartbeat_ms,
        }
    }
}
