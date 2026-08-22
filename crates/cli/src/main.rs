//! `brahmaputra-cli`: manual verification harness for the data plane
//! (Blueprint 02 §7).

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use brahmaputra_client::{
    Admin, Assignor, AutoOffsetReset, Connection, Consumer, FetchedRecord, GroupAdmin,
    GroupConsumer, IsolationLevel, Producer, ProducerConfig, TlsSettings, TransactionalProducer,
    Transport, TransportConfig, DEFAULT_TRANSACTION_TIMEOUT_MS, EARLIEST,
    LATEST,
};




use brahmaputra_controller::{
    ClusterMetadata, ControllerCommandResult, MetadataCommand, MetadataEvent, QuotaEntity,
    QuotaLimits,
};
use brahmaputra_protocol::codec;
use brahmaputra_protocol::gen::{ProduceRequest, ProduceResponse};
use brahmaputra_protocol::producer::{InitProducerIdRequest, InitProducerIdResponse};
use brahmaputra_protocol::{ApiKey, Compression, Record, RecordBatch, RecordHeader};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use futures::{stream, StreamExt};

/// Keep enough sends active to fill several default-sized batches while
/// bounding memory for arbitrarily large `--count` inputs.
const BULK_SEND_CONCURRENCY: usize = 512;
const FOLLOW_IDLE_DELAY: Duration = Duration::from_millis(100);
/// Internal topic whose partition leaders coordinate consumer groups.
const OFFSETS_TOPIC: &str = "__consumer_offsets";

/// Data-plane transport and TLS material for this invocation. A CLI process
/// talks to one cluster over one transport with one identity, so it is set
/// once from the global flags rather than threaded through every command
/// function.
static TRANSPORT: std::sync::OnceLock<TransportConfig> = std::sync::OnceLock::new();

fn transport() -> TransportConfig {
    TRANSPORT.get().cloned().unwrap_or_default()
}

#[derive(Parser)]
#[command(name = "brahmaputra-cli", about = "Brahmaputra CLI")]
struct Cli {
    /// Broker address (host:port).
    #[arg(long, global = true, default_value = "127.0.0.1:9092")]
    broker: String,

    /// Controller HTTP base URL.
    #[arg(long, global = true, default_value = "http://127.0.0.1:19092")]
    controller: String,

    /// Data-plane transport; must match the broker's --transport.
    #[arg(long, global = true, default_value = "tcp")]
    transport: Transport,

    /// PEM CA bundle the broker's certificate must chain to. Without it any
    /// certificate is accepted, which encrypts the connection but proves
    /// nothing about who is on the other end of it.
    #[arg(long = "tls-ca", global = true)]
    tls_ca: Option<PathBuf>,

    /// PEM certificate chain to present to a broker started with
    /// `--tls-client-ca`. Its common name becomes this connection's
    /// principal, so no password crosses the wire at all.
    #[arg(long = "tls-cert", global = true, requires = "tls_key")]
    tls_cert: Option<PathBuf>,

    /// PEM private key matching `--tls-cert`.
    #[arg(long = "tls-key", global = true, requires = "tls_cert")]
    tls_key: Option<PathBuf>,

    /// Name to check the broker's certificate against. Brokers that
    /// generate their own use `brahmaputra`, which is the default.
    #[arg(long = "tls-server-name", global = true)]
    tls_server_name: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Produce record(s) to a topic.
    Produce {
        #[arg(long)]
        topic: String,
        #[arg(long)]
        partition: Option<i32>,
        #[arg(long)]
        key: Option<String>,
        /// One record with this value.
        #[arg(long, conflicts_with_all = ["file", "count"])]
        value: Option<String>,
        /// One record per line of this file.
        #[arg(long, conflicts_with_all = ["value", "count"])]
        file: Option<String>,
        /// Load-test mode: this many generated records...
        #[arg(long, requires = "value_size")]
        count: Option<u64>,
        /// ...of this many bytes each.
        #[arg(long = "value-size")]
        value_size: Option<usize>,
        /// Send null keys in --count mode instead of a generated per-record
        /// key, matching Kafka's producer-perf-test payload shape.
        #[arg(long = "no-key", requires = "count")]
        no_key: bool,
        /// Required acknowledgements: 0 (none), 1 (leader), or all/-1 (all ISR).
        #[arg(
            long,
            default_value = "1",
            value_parser = parse_acks,
            allow_hyphen_values = true
        )]
        acks: i32,
        /// Flush a partition buffer once it holds this many bytes (`batch.size`).
        #[arg(long = "batch-size", default_value_t = 16 * 1024)]
        batch_size: usize,
        /// Flush every non-empty buffer at least this often (`linger.ms`);
        /// 0 sends each record immediately.
        #[arg(long = "linger-ms", default_value_t = 5)]
        linger_ms: u64,
        /// Batch compression (`compression.type`).
        #[arg(
            long,
            value_parser = ["none", "lz4", "zstd", "snappy", "gzip"],
            default_value = "lz4"
        )]
        compression: String,
        /// Attach a record header, `key=value`. Repeatable; keys may repeat.
        #[arg(long = "header")]
        headers: Vec<String>,
        /// Records kept in flight by --file/--count mode. Batches fill by
        /// size only if this exceeds batch.size/record.size, otherwise every
        /// batch waits out linger.ms.
        #[arg(long = "in-flight", default_value_t = BULK_SEND_CONCURRENCY)]
        in_flight: usize,
        /// Unacknowledged requests allowed on the connection
        /// (`max.in.flight.requests.per.connection`).
        #[arg(long = "max-in-flight", default_value_t = 5)]
        max_in_flight: usize,
        /// With --count, also report acknowledgement-latency percentiles.
        /// Off by default: it adds a line of output, and the throughput
        /// line above it is parsed positionally by scripts.
        #[arg(long)]
        latency: bool,
        /// Offer records at this many per second instead of as fast as the
        /// broker accepts them (`kafka-producer-perf-test --throughput`).
        ///
        /// Latency measured at saturation is queue depth divided by
        /// throughput — arithmetic, not a property of the commit path. To
        /// measure what a caller actually waits for, offer load below
        /// saturation so the queue stays near empty.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        rate: Option<u64>,
        /// How long the broker may wait for the requested acknowledgements.
        #[arg(
            long = "timeout-ms",
            default_value_t = 30_000,
            value_parser = clap::value_parser!(i32).range(0..)
        )]
        timeout_ms: i32,
        /// Allocate producer identity/sequence metadata and safely retry an
        /// ambiguous transport failure with the exact same batch.
        #[arg(long, conflicts_with_all = ["producer_id", "producer_epoch", "base_sequence"])]
        idempotent: bool,
        /// Low-level verification mode: explicit magic-v2 producer id.
        #[arg(
            long,
            value_parser = clap::value_parser!(i64).range(0..),
            requires_all = ["producer_epoch", "base_sequence", "partition", "value"],
            conflicts_with_all = ["idempotent", "file", "count"]
        )]
        producer_id: Option<i64>,
        /// Explicit producer epoch (all explicit producer fields are required).
        #[arg(
            long,
            value_parser = clap::value_parser!(i16).range(0..),
            requires_all = ["producer_id", "base_sequence", "partition", "value"]
        )]
        producer_epoch: Option<i16>,
        /// Explicit per-partition base sequence.
        #[arg(
            long,
            value_parser = clap::value_parser!(i32).range(0..),
            requires_all = ["producer_id", "producer_epoch", "partition", "value"]
        )]
        base_sequence: Option<i32>,
    },
    /// Consume records from a topic.
    Consume {
        /// Topic(s) to read; comma-separated when consuming with --group.
        #[arg(long)]
        topic: String,
        /// Partition to read; default: all partitions of the topic.
        #[arg(long)]
        partition: Option<i32>,
        /// Start position.
        #[arg(long, value_parser = ["earliest", "latest"], conflicts_with = "offset", default_value = "earliest")]
        from: String,
        /// Explicit start offset (overrides --from).
        #[arg(long)]
        offset: Option<i64>,
        /// Stop after this many records.
        #[arg(long)]
        max: Option<u64>,
        /// Keep polling for new records (long-poll).
        #[arg(long)]
        follow: bool,
        /// What this consumer is allowed to see: `read_uncommitted`
        /// (the default, and what every non-transactional topic gives
        /// either way) or `read_committed`, which stops at the last stable
        /// offset and skips the records of aborted transactions.
        #[arg(
            long = "isolation-level",
            value_parser = ["read_uncommitted", "read_committed"],
            default_value = "read_uncommitted"
        )]
        isolation_level: String,
        /// Join this consumer group instead of consuming standalone.
        #[arg(long, conflicts_with_all = ["partition", "from", "offset"])]
        group: Option<String>,
        /// Auto-commit interval for group consumption (0 disables it).
        #[arg(
            long = "commit-interval-ms",
            default_value_t = 5000,
            requires = "group",
            value_parser = clap::value_parser!(u64)
        )]
        commit_interval_ms: u64,
        /// Partition assignor for group consumption.
        #[arg(
            long,
            value_parser = ["range", "roundrobin", "sticky", "cooperative-sticky"],
            default_value = "range",
            requires = "group"
        )]
        assignor: String,
        /// Where a group starts when a partition has no committed offset,
        /// or its committed offset has aged off the log (`auto.offset.reset`).
        #[arg(
            long = "auto-offset-reset",
            value_parser = ["earliest", "latest", "none"],
            default_value = "earliest",
            requires = "group"
        )]
        auto_offset_reset: String,
        /// Print only a throughput summary, not every record. Use for
        /// benchmarks, where per-record stdout dominates the measurement.
        #[arg(long)]
        quiet: bool,
        /// Also print each record's create timestamp. Off by default: the
        /// default output line is parsed positionally by scripts and
        /// pipelines, so widening it would break them silently.
        #[arg(long = "show-timestamp")]
        show_timestamp: bool,
    },
    /// Print broker/topic/partition metadata.
    Metadata {
        /// Restrict to this topic (auto-creates it, M1 behavior).
        #[arg(long)]
        topic: Option<String>,
    },
    /// Print the APIs and versions this broker speaks.
    ApiVersions,
    /// Print earliest/latest offsets for a topic's partitions.
    Offsets {
        #[arg(long)]
        topic: String,
        #[arg(long)]
        partition: Option<i32>,
        /// Also resolve the first offset at or after this unix-millisecond
        /// timestamp — where a consumer would start to replay from a point
        /// in time.
        #[arg(long)]
        timestamp: Option<i64>,
    },
    /// Create or delete topics through the controller.
    Topic {
        #[command(subcommand)]
        command: TopicCommand,
    },
    /// Set byte-rate limits for a user, a client id, or both.
    Quota {
        #[command(subcommand)]
        command: QuotaCommand,
    },
    /// Write records inside a transaction, then commit or abort them.
    ///
    /// Every `--send TOPIC:PARTITION=VALUE` is written immediately; whether
    /// it *counts* is decided at the end. A `read_committed` consumer sees
    /// all of them or none; the default `read_uncommitted` sees them either
    /// way, which is the point of the flag.
    Transaction {
        /// The `transactional.id` to claim. Claiming it fences any previous
        /// holder and resolves whatever that holder abandoned.
        #[arg(long = "id")]
        transactional_id: String,
        /// A record to write, as `topic:partition=value`. Repeatable.
        #[arg(long = "send", value_parser = parse_transactional_record)]
        sends: Vec<TransactionalRecord>,
        /// Abort instead of committing.
        #[arg(long)]
        abort: bool,
        /// Write the records and exit without ending the transaction, as a
        /// crashed producer would.
        ///
        /// The records stay in doubt and a `read_committed` consumer stops
        /// before them until something resolves it — which is what the next
        /// claim of this `--id` does. That recovery is the reason the
        /// coordinator owns the decision rather than the client.
        #[arg(long, conflicts_with = "abort")]
        abandon: bool,
        /// Commit these consumed offsets with the transaction, as
        /// `topic:partition=offset`. Repeatable; requires --group.
        #[arg(long = "offset", value_parser = parse_transactional_record, requires = "group")]
        offsets: Vec<TransactionalRecord>,
        /// Consumer group the `--offset` values belong to.
        #[arg(long)]
        group: Option<String>,
    },
    /// Describe the cluster: brokers, racks, and the current controller.
    DescribeCluster,
    /// Print the configuration in force on a topic or on a broker.
    DescribeConfigs {
        /// `topic` or `broker`.
        #[arg(long = "type", value_parser = ["topic", "broker"], default_value = "topic")]
        resource_type: String,
        /// Topic name. Ignored (and unnecessary) for `--type broker`.
        #[arg(long, default_value = "")]
        name: String,
        /// Show only these configs; repeatable. Default shows every one,
        /// including the ones left at their default.
        #[arg(long = "config")]
        config_names: Vec<String>,
    },
    /// Print per-partition disk usage, asked of every broker.
    DescribeLogDirs {
        /// Restrict to these topics; repeatable. Default covers all.
        #[arg(long = "topic")]
        topics: Vec<String>,
    },
    /// Delete every record below an offset, reclaiming its segments.
    ///
    /// The only way to reclaim space on a topic retention will not touch,
    /// and the only answer to "delete this data now" short of deleting the
    /// topic. The offset is clamped to what is committed.
    DeleteRecords {
        #[arg(long)]
        topic: String,
        /// Partition to trim; default trims every partition of the topic.
        #[arg(long)]
        partition: Option<i32>,
        /// Delete records below this offset; -1 deletes everything
        /// committed.
        #[arg(long, default_value_t = -1, allow_hyphen_values = true)]
        offset: i64,
    },
    /// Inspect consumer groups: membership, committed offsets, lag.
    Groups {
        #[command(subcommand)]
        command: GroupCommand,
    },
    /// Allocate or bump an idempotent producer identity.
    Producer {
        #[command(subcommand)]
        command: ProducerCommand,
    },
}

#[derive(Subcommand)]
enum GroupCommand {
    /// List every group the cluster coordinates.
    List {
        /// Only groups in these states (repeatable), e.g. --state Stable.
        #[arg(
            long = "state",
            value_parser = ["Empty", "PreparingRebalance", "AwaitingSync", "Stable", "Dead"]
        )]
        states: Vec<String>,
    },
    /// Show one group's members, assignment and committed offsets.
    Describe {
        #[arg(long)]
        group: String,
    },
    /// Show per-partition lag (log end offset − committed offset).
    Lag {
        #[arg(long)]
        group: String,
    },
}

#[derive(Subcommand)]
enum ProducerCommand {
    /// Allocate a new identity, or bump an existing id's current epoch.
    Init {
        #[arg(
            long,
            value_parser = clap::value_parser!(i64).range(0..),
            requires = "producer_epoch"
        )]
        producer_id: Option<i64>,
        #[arg(
            long,
            value_parser = clap::value_parser!(i16).range(0..),
            requires = "producer_id"
        )]
        producer_epoch: Option<i16>,
    },
}

#[derive(Subcommand)]
enum TopicCommand {
    /// Create a topic and assign its replicas.
    Create {
        #[arg(long)]
        name: String,
        #[arg(long, value_parser = clap::value_parser!(i32).range(1..))]
        partitions: i32,
        #[arg(long = "replication-factor", value_parser = clap::value_parser!(i32).range(1..))]
        replication_factor: i32,
        /// Topic configuration in KEY=VALUE form; repeat for multiple entries.
        #[arg(long = "config", value_parser = parse_topic_config)]
        configs: Vec<TopicConfig>,
    },
    /// Move a partition to a different set of brokers.
    ///
    /// The partition keeps every copy it already has until the new brokers
    /// have caught up, so durability never dips during the move.
    Reassign {
        #[arg(long)]
        name: String,
        #[arg(long, value_parser = clap::value_parser!(i32).range(0..))]
        partition: i32,
        /// Target broker ids, comma-separated (e.g. `--replicas 2,3,4`).
        #[arg(long, value_delimiter = ',', num_args = 1..)]
        replicas: Vec<i32>,
    },
    /// Delete a topic and its metadata.
    Delete {
        #[arg(long)]
        name: String,
    },
}

/// `quota` subcommands: byte-rate limits bound to a user, a client id, or
/// both.
#[derive(Subcommand)]
enum QuotaCommand {
    /// Set (or replace) the limits for one entity.
    ///
    /// Omitting both `--user` and `--client-id` writes the cluster-wide
    /// default, which every request falls back to when no more specific
    /// rule matches. Omitting a direction leaves it to the next-less
    /// specific rule; setting both directions to 0 removes the entity.
    Set {
        /// Authenticated principal this applies to; omit for any user.
        #[arg(long)]
        user: Option<String>,
        /// `client.id` this applies to; omit for any client.
        #[arg(long = "client-id")]
        client_id: Option<String>,
        /// Produce ceiling in bytes per second.
        #[arg(long = "produce-bytes-per-sec")]
        produce_bytes_per_sec: Option<u64>,
        /// Fetch ceiling in bytes per second.
        #[arg(long = "fetch-bytes-per-sec")]
        fetch_bytes_per_sec: Option<u64>,
    },
    /// Remove the limits bound to one entity.
    Delete {
        #[arg(long)]
        user: Option<String>,
        #[arg(long = "client-id")]
        client_id: Option<String>,
    },
    /// List every configured quota entity.
    List,
}

struct ProduceOptions {
    broker: SocketAddr,
    topic: String,
    partition: Option<i32>,
    key: Option<String>,
    value: Option<String>,
    file: Option<String>,
    count: Option<u64>,
    value_size: Option<usize>,
    no_key: bool,
    in_flight: usize,
    max_in_flight: usize,
    latency: bool,
    rate: Option<u64>,
    acks: i32,
    timeout_ms: i32,
    batch_size: usize,
    linger_ms: u64,
    compression: String,
    headers: Vec<String>,
    idempotent: bool,
    producer_id: Option<i64>,
    producer_epoch: Option<i16>,
    base_sequence: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TopicConfig {
    key: String,
    value: String,
}

fn parse_acks(value: &str) -> std::result::Result<i32, String> {
    match value {
        "0" => Ok(0),
        "1" => Ok(1),
        "all" | "-1" => Ok(-1),
        _ => Err("acks must be one of: 0, 1, all, -1".to_owned()),
    }
}

/// One `topic:partition=value` argument to `transaction`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TransactionalRecord {
    topic: String,
    partition: i32,
    value: String,
}

fn parse_transactional_record(value: &str) -> std::result::Result<TransactionalRecord, String> {
    let (target, payload) = value
        .split_once('=')
        .ok_or_else(|| "expected topic:partition=value".to_owned())?;
    let (topic, partition) = target
        .split_once(':')
        .ok_or_else(|| "expected topic:partition=value".to_owned())?;
    if topic.is_empty() {
        return Err("the topic must not be empty".to_owned());
    }
    let partition = partition
        .parse::<i32>()
        .map_err(|_| format!("{partition:?} is not a partition number"))?;
    if partition < 0 {
        return Err("the partition must not be negative".to_owned());
    }
    Ok(TransactionalRecord {
        topic: topic.to_owned(),
        partition,
        value: payload.to_owned(),
    })
}

fn parse_topic_config(value: &str) -> std::result::Result<TopicConfig, String> {
    let (key, config_value) = value
        .split_once('=')
        .ok_or_else(|| "topic config must use KEY=VALUE form".to_owned())?;
    if key.is_empty() {
        return Err("topic config key must not be empty".to_owned());
    }
    if config_value.is_empty() {
        return Err(format!("topic config {key:?} value must not be empty"));
    }
    if key.trim() != key || key.chars().any(char::is_whitespace) {
        return Err(format!(
            "topic config key {key:?} must not contain whitespace"
        ));
    }

    Ok(TopicConfig {
        key: key.to_owned(),
        value: config_value.to_owned(),
    })
}

fn collect_topic_configs(configs: Vec<TopicConfig>) -> Result<BTreeMap<String, String>> {
    let mut collected = BTreeMap::new();
    for config in configs {
        if collected.contains_key(&config.key) {
            anyhow::bail!("duplicate topic config {:?}", config.key);
        }
        collected.insert(config.key, config.value);
    }
    Ok(collected)
}

fn producer_config(
    acks: i32,
    timeout_ms: i32,
    idempotence: bool,
    batch_size: usize,
    linger_ms: u64,
    compression: Compression,
    max_in_flight: usize,
) -> ProducerConfig {
    ProducerConfig {
        acks,
        timeout_ms,
        idempotence,
        batch_size,
        linger_ms,
        compression,
        max_in_flight: max_in_flight.max(1),
        transport: transport(),
        ..ProducerConfig::default()
    }
}

fn parse_compression(value: &str) -> Result<Compression> {
    Compression::parse(value).ok_or_else(|| {
        anyhow::anyhow!("unknown compression {value:?} (none, lz4, zstd, snappy, gzip)")
    })
}

/// Parse a `--header key=value` argument.
///
/// Splitting on the *first* `=` only, because a header value is arbitrary
/// bytes and may well contain one.
fn parse_header(value: &str) -> Result<RecordHeader> {
    match value.split_once('=') {
        Some((key, value)) => Ok(RecordHeader::new(key, value.as_bytes().to_vec())),
        None => anyhow::bail!("header {value:?} is not in key=value form"),
    }
}

/// Drive a bounded set of futures concurrently, but observe their results in
/// input order. This preserves the CLI's former first-error ordering while
/// allowing the producer to batch records and exercise its in-flight window.
async fn drive_ordered<I, Fut, T, E, F>(
    futures: I,
    concurrency: usize,
    mut on_success: F,
) -> std::result::Result<usize, E>
where
    I: IntoIterator<Item = Fut>,
    Fut: Future<Output = std::result::Result<T, E>>,
    F: FnMut(T),
{
    let results = stream::iter(futures).buffered(concurrency.max(1));
    futures::pin_mut!(results);

    let mut completed = 0;
    while let Some(result) = results.next().await {
        on_success(result?);
        completed += 1;
    }
    Ok(completed)
}

/// Summarise acknowledgement waits as milliseconds, by nearest rank.
///
/// Nearest rank rather than interpolation so a reported percentile is a
/// wait some record actually experienced, and so the value cannot drift
/// with the sample size.
fn format_latency(waits: &mut [u32]) -> String {
    if waits.is_empty() {
        return "latency ms: no samples".to_owned();
    }
    waits.sort_unstable();
    let at = |quantile: f64| -> f64 {
        let rank = (quantile * waits.len() as f64).ceil() as usize;
        f64::from(waits[rank.clamp(1, waits.len()) - 1]) / 1000.0
    };
    let mean =
        waits.iter().map(|micros| f64::from(*micros)).sum::<f64>() / waits.len() as f64 / 1000.0;
    format!(
        "latency ms: avg={:.2} p50={:.2} p95={:.2} p99={:.2} p99.9={:.2} max={:.2} (n={})",
        mean,
        at(0.50),
        at(0.95),
        at(0.99),
        at(0.999),
        f64::from(*waits.last().expect("non-empty")) / 1000.0,
        waits.len()
    )
}

async fn resolve_broker(addr: &str) -> Result<SocketAddr> {
    if let Ok(parsed) = addr.parse::<SocketAddr>() {
        return Ok(parsed);
    }
    tokio::net::lookup_host(addr)
        .await
        .with_context(|| format!("cannot resolve broker address {addr:?}"))?
        .next()
        .with_context(|| format!("no address for broker {addr:?}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();

    run(Cli::parse()).await
}

async fn run(cli: Cli) -> Result<()> {
    let _ = TRANSPORT.set(TransportConfig::new(
        cli.transport,
        TlsSettings {
            ca_path: cli.tls_ca.clone(),
            cert_path: cli.tls_cert.clone(),
            key_path: cli.tls_key.clone(),
            server_name: cli.tls_server_name.clone(),
        },
    ));
    let broker = if matches!(&cli.command, Command::Topic { .. } | Command::Quota { .. }) {
        None
    } else {
        Some(resolve_broker(&cli.broker).await?)
    };

    match cli.command {
        Command::Produce {
            topic,
            partition,
            key,
            value,
            file,
            count,
            value_size,
            no_key,
            in_flight,
            max_in_flight,
            latency,
            rate,
            acks,
            batch_size,
            linger_ms,
            compression,
            headers,
            timeout_ms,
            idempotent,
            producer_id,
            producer_epoch,
            base_sequence,
        } => {
            produce(ProduceOptions {
                broker: broker.expect("data-plane commands resolve a broker"),
                topic,
                partition,
                key,
                value,
                file,
                count,
                value_size,
                no_key,
                in_flight,
                max_in_flight,
                latency,
                rate,
                acks,
                timeout_ms,
                batch_size,
                linger_ms,
                compression,
                headers,
                idempotent,
                producer_id,
                producer_epoch,
                base_sequence,
            })
            .await
        }
        Command::Consume {
            topic,
            partition,
            from,
            offset,
            max,
            follow,
            isolation_level,
            group,
            commit_interval_ms,
            assignor,
            auto_offset_reset,
            quiet,
            show_timestamp,
        } => {
            let broker = broker.expect("data-plane commands resolve a broker");
            match group {
                Some(group) => {
                    consume_group(
                        broker,
                        &topic,
                        group,
                        GroupConsumeOptions {
                            commit_interval_ms,
                            assignor: &assignor,
                            auto_offset_reset: &auto_offset_reset,
                            max,
                            follow,
                            quiet,
                        },
                    )
                    .await
                }
                None => {
                    consume(
                        broker,
                        topic,
                        ConsumeOptions {
                            partition,
                            from: &from,
                            offset,
                            max,
                            follow,
                            isolation_level: &isolation_level,
                            quiet,
                            show_timestamp,
                        },
                    )
                    .await
                }
            }
        }
        Command::Metadata { topic } => {
            metadata(broker.expect("data-plane commands resolve a broker"), topic).await
        }
        Command::ApiVersions => {
            api_versions(broker.expect("data-plane commands resolve a broker")).await
        }
        Command::Offsets {
            topic,
            partition,
            timestamp,
        } => {
            offsets(
                broker.expect("data-plane commands resolve a broker"),
                topic,
                partition,
                timestamp,
            )
            .await
        }
        Command::Topic { command } => topic_admin(&cli.controller, command).await,
        Command::Quota { command } => quota_admin(&cli.controller, command).await,
        Command::Transaction {
            transactional_id,
            sends,
            abort,
            abandon,
            offsets,
            group,
        } => {
            run_transaction(
                broker.expect("data-plane commands resolve a broker"),
                &transactional_id,
                &sends,
                abort,
                abandon,
                &offsets,
                group.as_deref(),
            )
            .await
        }
        Command::DescribeCluster => {
            describe_cluster(broker.expect("data-plane commands resolve a broker")).await
        }
        Command::DescribeConfigs {
            resource_type,
            name,
            config_names,
        } => {
            describe_configs(
                broker.expect("data-plane commands resolve a broker"),
                &resource_type,
                &name,
                &config_names,
            )
            .await
        }
        Command::DescribeLogDirs { topics } => {
            describe_log_dirs(
                broker.expect("data-plane commands resolve a broker"),
                &topics,
            )
            .await
        }
        Command::DeleteRecords {
            topic,
            partition,
            offset,
        } => {
            delete_records(
                broker.expect("data-plane commands resolve a broker"),
                &topic,
                partition,
                offset,
            )
            .await
        }
        Command::Groups { command } => {
            group_admin(
                broker.expect("data-plane commands resolve a broker"),
                command,
            )
            .await
        }
        Command::Producer { command } => {
            producer_admin(
                broker.expect("data-plane commands resolve a broker"),
                command,
            )
            .await
        }
    }
}

/// `groups` subcommands (Blueprint 05 §6): the manual verification surface
/// for consumer-group membership and lag.
async fn group_admin(broker: SocketAddr, command: GroupCommand) -> Result<()> {
    let admin = GroupAdmin::connect_with(transport(), broker, "brahmaputra-cli").await?;
    match command {
        GroupCommand::List { states } => {
            let report = admin.list_groups(&states).await?;
            if report.groups.is_empty() {
                println!("no consumer groups");
            }
            for group in &report.groups {
                println!(
                    "group {:?}: state={} generation={} members={} coordinator=broker {} ({}-{})",
                    group.group_id,
                    group.state,
                    group.generation,
                    group.member_count,
                    group.coordinator_broker,
                    OFFSETS_TOPIC,
                    group.coordinator_partition
                );
            }
            for (broker_id, error) in &report.unreachable {
                // Listing is a union over brokers; say which slice is missing
                // rather than reporting a partial list as complete.
                eprintln!("warning: broker {broker_id} did not answer ListGroups: {error}");
            }
            Ok(())
        }
        GroupCommand::Describe { group } => {
            let description = admin.describe_group(&group).await?;
            println!(
                "group {:?}: state={} generation={} leader={:?} coordinator={}-{}",
                description.group_id,
                description.state,
                description.generation,
                description.leader_member_id,
                OFFSETS_TOPIC,
                description.coordinator_partition
            );
            for member in &description.members {
                let assignment = member
                    .assignment
                    .iter()
                    .map(|(topic, partition)| format!("{topic}-{partition}"))
                    .collect::<Vec<_>>()
                    .join(",");
                println!(
                    "  member {}: subscribed={:?} assignment=[{}]",
                    member.member_id, member.subscription_topics, assignment
                );
            }
            for (topic, partition, offset) in &description.offsets {
                println!("  committed {topic}-{partition}: {offset}");
            }
            Ok(())
        }
        GroupCommand::Lag { group } => {
            let lags = admin.group_lag(&group).await?;
            if lags.is_empty() {
                println!("group {group:?} has no assigned or committed partitions");
            }
            let mut total = 0i64;
            for entry in &lags {
                let committed = entry
                    .committed_offset
                    .map_or_else(|| "-".to_owned(), |offset| offset.to_string());
                let lag = match entry.lag {
                    Some(lag) => {
                        total += lag;
                        lag.to_string()
                    }
                    None => "-".to_owned(),
                };
                println!(
                    "{}-{}: committed={} log_end={} lag={} owner={}",
                    entry.topic,
                    entry.partition,
                    committed,
                    entry.log_end_offset,
                    lag,
                    entry.member_id.as_deref().unwrap_or("-")
                );
            }
            println!("total lag: {total}");
            Ok(())
        }
    }
}

async fn producer_admin(broker: SocketAddr, command: ProducerCommand) -> Result<()> {
    let request = match command {
        ProducerCommand::Init {
            producer_id: None,
            producer_epoch: None,
        } => InitProducerIdRequest::allocate(),
        ProducerCommand::Init {
            producer_id: Some(producer_id),
            producer_epoch: Some(producer_epoch),
        } => InitProducerIdRequest {
            producer_id,
            producer_epoch,
            transactional_id: None,
            transaction_timeout_ms: 0,
        },
        ProducerCommand::Init { .. } => {
            unreachable!("clap requires producer id and epoch together")
        }
    };
    let connection =
        Connection::connect_with(transport(), broker, Some("brahmaputra-cli-init".into()), 1)
            .await?;
    let response = connection
        .request(ApiKey::InitProducerId, &request.encode())
        .await?;
    let response = InitProducerIdResponse::decode(&response)?;
    brahmaputra_client::ClientError::from_error_code(response.error_code)?;
    println!(
        "producer_id={} producer_epoch={}",
        response.producer_id, response.producer_epoch
    );
    Ok(())
}

async fn topic_admin(controller: &str, command: TopicCommand) -> Result<()> {
    let (request, success) = match command {
        TopicCommand::Create {
            name,
            partitions,
            replication_factor,
            configs,
        } => {
            let configs = collect_topic_configs(configs)?;
            let success = format!(
                "topic created name={name:?} partitions={partitions} replication_factor={replication_factor}"
            );
            (
                MetadataCommand::CreateTopic {
                    name,
                    partitions,
                    replication_factor,
                    configs,
                },
                success,
            )
        }
        TopicCommand::Reassign {
            name,
            partition,
            replicas,
        } => {
            if replicas.is_empty() {
                anyhow::bail!("--replicas needs at least one broker id");
            }
            let success = format!(
                "reassignment started topic={name:?} partition={partition} replicas={replicas:?} \
                 (the partition keeps its current replicas until the targets catch up)"
            );
            (
                MetadataCommand::ReassignPartition {
                    topic: name,
                    partition,
                    replicas,
                },
                success,
            )
        }
        TopicCommand::Delete { name } => {
            let success = format!("topic deleted name={name:?}");
            (MetadataCommand::DeleteTopic { name }, success)
        }
    };

    let event = submit_controller_command(controller, &request).await?;
    let expected_event = match &request {
        MetadataCommand::CreateTopic { name, .. } => {
            MetadataEvent::TopicCreated { name: name.clone() }
        }
        MetadataCommand::DeleteTopic { name } => MetadataEvent::TopicDeleted { name: name.clone() },
        MetadataCommand::ReassignPartition {
            topic,
            partition,
            replicas,
        } => MetadataEvent::PartitionReassigned {
            topic: topic.clone(),
            partition: *partition,
            replicas: {
                let mut sorted = replicas.clone();
                sorted.sort_unstable();
                sorted.dedup();
                sorted
            },
        },
        _ => unreachable!("topic admin builds only topic commands"),
    };
    if event != expected_event {
        anyhow::bail!("controller returned unexpected event for topic command: {event:?}");
    }

    println!("{success}");
    Ok(())
}

async fn run_transaction(
    broker: SocketAddr,
    transactional_id: &str,
    sends: &[TransactionalRecord],
    abort: bool,
    abandon: bool,
    offsets: &[TransactionalRecord],
    group: Option<&str>,
) -> Result<()> {
    let mut producer = TransactionalProducer::init_with(
        transport(),
        broker,
        transactional_id,
        DEFAULT_TRANSACTION_TIMEOUT_MS,
    )
    .await?;
    let (producer_id, producer_epoch) = producer.producer_identity();
    println!("producer id={producer_id} epoch={producer_epoch}");

    producer.begin()?;
    for record in sends {
        let offset = producer
            .send(
                &record.topic,
                record.partition,
                Record::new(record.value.clone().into_bytes()),
            )
            .await?;
        println!(
            "wrote {}-{} offset={offset} (in doubt until the transaction ends)",
            record.topic, record.partition
        );
    }

    if let Some(group) = group {
        let committed: Vec<(String, i32, i64)> = offsets
            .iter()
            .map(|entry| {
                let offset = entry
                    .value
                    .parse::<i64>()
                    .map_err(|_| anyhow::anyhow!("{:?} is not an offset", entry.value))?;
                Ok((entry.topic.clone(), entry.partition, offset))
            })
            .collect::<Result<_>>()?;
        if !committed.is_empty() {
            producer.send_offsets(group, &committed).await?;
            println!("staged {} offset(s) for group {group}", committed.len());
        }
    }

    if abandon {
        println!(
            "abandoned: the transaction is still open, so a read_committed \n             consumer stops before these records until it is resolved"
        );
        return Ok(());
    }
    if abort {
        producer.abort().await?;
        println!("aborted: a read_committed consumer will skip every record above");
    } else {
        producer.commit().await?;
        println!("committed: every record above is now visible to a read_committed consumer");
    }
    Ok(())
}

async fn describe_cluster(broker: SocketAddr) -> Result<()> {
    let admin = Admin::connect_with(transport(), broker, "brahmaputra-cli").await?;
    let cluster = admin.describe_cluster().await?;
    println!("cluster id: {}", cluster.cluster_id);
    println!(
        "controller: {}",
        if cluster.controller_id < 0 {
            "none".to_owned()
        } else {
            cluster.controller_id.to_string()
        }
    );
    println!("{:<8} {:<24} {:<10}", "BROKER", "ADDRESS", "RACK");
    for member in &cluster.brokers {
        println!(
            "{:<8} {:<24} {:<10}",
            member.broker_id,
            format!("{}:{}", member.host, member.port),
            if member.rack.is_empty() {
                "-"
            } else {
                &member.rack
            },
        );
    }
    println!("{} broker(s)", cluster.brokers.len());
    Ok(())
}

async fn describe_configs(
    broker: SocketAddr,
    resource_type: &str,
    name: &str,
    config_names: &[String],
) -> Result<()> {
    if resource_type == "topic" && name.is_empty() {
        anyhow::bail!("--name is required for --type topic");
    }
    let admin = Admin::connect_with(transport(), broker, "brahmaputra-cli").await?;
    let configs = admin
        .describe_configs(resource_type, name, config_names)
        .await?;
    if name.is_empty() {
        println!("{resource_type}");
    } else {
        println!("{resource_type} {name}");
    }
    println!("{:<32} {:<24} {:<10}", "CONFIG", "VALUE", "SOURCE");
    for config in &configs {
        println!(
            "{:<32} {:<24} {:<10}",
            config.name,
            config.value,
            if config.is_default { "default" } else { "set" },
        );
    }
    println!("{} config(s)", configs.len());
    Ok(())
}

async fn describe_log_dirs(broker: SocketAddr, topics: &[String]) -> Result<()> {
    let admin = Admin::connect_with(transport(), broker, "brahmaputra-cli").await?;
    let (dirs, unreachable) = admin.describe_log_dirs(topics).await?;
    let mut total: i64 = 0;
    for dir in &dirs {
        println!(
            "broker {} dir {} ({})",
            dir.broker_id,
            dir.log_dir,
            if dir.error_code != 0 {
                // The line an operator is actually looking for. A failed
                // disk with its capacity blanked out would read as an
                // empty one.
                format!(
                    "OFFLINE — this disk has failed{}",
                    if dir.offline_reason.is_empty() {
                        String::new()
                    } else {
                        format!(": {}", dir.offline_reason)
                    }
                )
            } else {
                format!(
                    "total={} usable={}",
                    describe_bytes(dir.total_bytes),
                    describe_bytes(dir.usable_bytes)
                )
            },
        );
        println!("  {:<28} {:>14} {:>10} {:>8}", "PARTITION", "SIZE", "LAG", "ROLE");
        for partition in &dir.partitions {
            total += partition.size_bytes;
            println!(
                "  {:<28} {:>14} {:>10} {:>8}",
                format!("{}-{}", partition.topic, partition.partition),
                partition.size_bytes,
                partition.offset_lag,
                if partition.is_leader {
                    "leader"
                } else {
                    "follower"
                },
            );
        }
    }
    for (broker_id, error) in &unreachable {
        println!("broker {broker_id}: unreachable ({error})");
    }
    println!("total {total} bytes across {} dir(s)", dirs.len());
    Ok(())
}

fn describe_bytes(bytes: i64) -> String {
    if bytes < 0 {
        "unknown".to_owned()
    } else {
        bytes.to_string()
    }
}

async fn delete_records(
    broker: SocketAddr,
    topic: &str,
    partition: Option<i32>,
    offset: i64,
) -> Result<()> {
    let admin = Admin::connect_with(transport(), broker, "brahmaputra-cli").await?;
    let partitions = match partition {
        Some(one) => vec![one],
        None => admin.partitions(topic).await?,
    };
    let targets: Vec<(String, i32, i64)> = partitions
        .into_iter()
        .map(|partition| (topic.to_owned(), partition, offset))
        .collect();
    for result in admin.delete_records(&targets).await? {
        println!(
            "{}-{}: records below {} deleted, log start now {}",
            result.topic, result.partition, result.low_watermark, result.low_watermark
        );
    }
    Ok(())
}

/// `quota` subcommands: read and write the byte-rate overrides the brokers
/// enforce.
///
/// Limits go through the controller rather than a broker flag so the whole
/// cluster agrees on one number — otherwise a client's real ceiling would
/// depend on which leader it happened to reach.
async fn quota_admin(controller: &str, command: QuotaCommand) -> Result<()> {
    let (request, success) = match command {
        QuotaCommand::Set {
            user,
            client_id,
            produce_bytes_per_sec,
            fetch_bytes_per_sec,
        } => {
            if produce_bytes_per_sec.is_none() && fetch_bytes_per_sec.is_none() {
                anyhow::bail!(
                    "set at least one of --produce-bytes-per-sec or --fetch-bytes-per-sec"
                );
            }
            let entity = QuotaEntity::new(user, client_id);
            let limits = QuotaLimits {
                // 0 is how an operator says "stop overriding this
                // direction"; storing it would instead mean "no bytes at
                // all", which is a stall, not a removal.
                produce_bytes_per_sec: produce_bytes_per_sec.filter(|rate| *rate > 0),
                fetch_bytes_per_sec: fetch_bytes_per_sec.filter(|rate| *rate > 0),
            };
            let success = if limits.is_empty() {
                format!("quota removed entity={}", entity.key())
            } else {
                format!(
                    "quota set entity={} produce={} fetch={}",
                    entity.key(),
                    describe_rate(limits.produce_bytes_per_sec),
                    describe_rate(limits.fetch_bytes_per_sec),
                )
            };
            (MetadataCommand::PutQuota { entity, limits }, success)
        }
        QuotaCommand::Delete { user, client_id } => {
            let key = QuotaEntity::new(user, client_id).key();
            let success = format!("quota removed entity={key}");
            (MetadataCommand::DeleteQuota { key }, success)
        }
        QuotaCommand::List => return list_quotas(controller).await,
    };

    let event = submit_controller_command(controller, &request).await?;
    if !matches!(event, MetadataEvent::QuotaChanged { .. }) {
        anyhow::bail!("controller returned unexpected event for quota command: {event:?}");
    }
    println!("{success}");
    Ok(())
}

fn describe_rate(rate: Option<u64>) -> String {
    rate.map_or_else(|| "default".to_owned(), |bytes| format!("{bytes}B/s"))
}

async fn list_quotas(controller: &str) -> Result<()> {
    let url = format!(
        "{}/api/v1/controller/metadata",
        controller.trim_end_matches('/')
    );
    let image = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .with_context(|| format!("cannot reach controller at {url}"))?
        .error_for_status()
        .context("controller rejected the metadata request")?
        .json::<ClusterMetadata>()
        .await
        .context("controller returned an invalid metadata image")?;

    if image.quotas.is_empty() {
        println!("no quota overrides configured; every client uses the broker defaults");
        return Ok(());
    }
    println!("{:<40} {:>16} {:>16}", "ENTITY", "PRODUCE", "FETCH");
    for (entity, limits) in image.quotas.values() {
        println!(
            "{:<40} {:>16} {:>16}",
            entity.key(),
            describe_rate(limits.produce_bytes_per_sec),
            describe_rate(limits.fetch_bytes_per_sec),
        );
    }
    Ok(())
}

async fn submit_controller_command(
    controller: &str,
    command: &MetadataCommand,
) -> Result<MetadataEvent> {
    let url = format!(
        "{}/api/v1/controller/command",
        controller.trim_end_matches('/')
    );
    let response = reqwest::Client::new()
        .post(&url)
        .json(command)
        .send()
        .await
        .with_context(|| format!("cannot reach controller at {url}"))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("controller returned HTTP {status}: {body}");
    }

    response
        .json::<ControllerCommandResult>()
        .await
        .context("controller returned an invalid command response")?
        .map_err(|error| {
            anyhow::anyhow!(
                "controller rejected command ({}): {}",
                error.code,
                error.message
            )
        })
}

async fn produce(options: ProduceOptions) -> Result<()> {
    let ProduceOptions {
        broker,
        topic,
        partition,
        key,
        value,
        file,
        count,
        value_size,
        no_key,
        in_flight,
        max_in_flight,
        latency,
        rate,
        acks,
        timeout_ms,
        batch_size,
        linger_ms,
        compression,
        headers,
        idempotent,
        producer_id,
        producer_epoch,
        base_sequence,
    } = options;
    if (idempotent || producer_id.is_some()) && acks == 0 {
        anyhow::bail!("idempotent produce requires acknowledgements (--acks 1 or all)");
    }
    if let (
        Some(producer_id),
        Some(producer_epoch),
        Some(base_sequence),
        Some(partition),
        Some(value),
    ) = (
        producer_id,
        producer_epoch,
        base_sequence,
        partition,
        value.as_ref(),
    ) {
        let offset = explicit_produce(
            broker,
            &topic,
            partition,
            key.as_deref(),
            value,
            acks,
            timeout_ms,
            producer_id,
            producer_epoch,
            base_sequence,
        )
        .await?;
        println!(
            "acked offset={offset} producer_id={producer_id} producer_epoch={producer_epoch} base_sequence={base_sequence}"
        );
        return Ok(());
    }
    let producer = Producer::connect(
        broker,
        producer_config(
            acks,
            timeout_ms,
            idempotent,
            batch_size,
            linger_ms,
            parse_compression(&compression)?,
            max_in_flight,
        ),
    )
    .await?;
    if let Some((producer_id, producer_epoch)) = producer.producer_identity() {
        println!("producer_id={producer_id} producer_epoch={producer_epoch}");
    }
    let key = key.map(Bytes::from);
    let headers = headers
        .iter()
        .map(|header| parse_header(header))
        .collect::<Result<Vec<_>>>()?;

    match (value, file, count) {
        (Some(value), _, _) => {
            let offset = producer
                .send_with_headers(&topic, partition, key, Bytes::from(value), headers)
                .await?;
            println!("acked offset={offset}");
        }
        (_, Some(path), _) => {
            let text =
                std::fs::read_to_string(&path).with_context(|| format!("cannot read {path:?}"))?;
            let sends = text.lines().map(|line| {
                let key = key.clone();
                let headers = headers.clone();
                let value = Bytes::from(line.to_owned());
                let producer = &producer;
                let topic = &topic;
                async move {
                    producer
                        .send_with_headers(topic, partition, key, value, headers)
                        .await
                }
            });
            let sent = drive_ordered(sends, in_flight.max(1), |_| {}).await?;
            producer.flush().await?;
            println!("produced {sent} records from {path:?}");
        }
        (_, _, Some(count)) => {
            let size = value_size.expect("clap requires --value-size with --count");
            let value = Bytes::from(vec![b'x'; size]);
            let started = Instant::now();
            let sends = (0..count).map(|i| {
                let producer = &producer;
                let topic = &topic;
                let value = value.clone();
                async move {
                    // Distinct per-record key keeps batches honest in count
                    // mode; --no-key sends null keys instead.
                    let key = (!no_key).then(|| Bytes::from(format!("load-{i}").into_bytes()));
                    // Hold each record until its slot in the offered rate.
                    // Pacing from a fixed origin rather than sleeping
                    // between sends keeps the offered rate honest: a send
                    // that runs long steals from the next interval instead
                    // of pushing the whole schedule back.
                    if let Some(rate) = rate {
                        let slot =
                            started + Duration::from_secs_f64(i as f64 / rate as f64);
                        tokio::time::sleep_until(tokio::time::Instant::from_std(slot)).await;
                    }
                    // Timed from the moment this record is admitted — the
                    // in-flight window has already let it through — to its
                    // acknowledgement. That is the same span
                    // `kafka-producer-perf-test` reports: it covers batching
                    // and `linger.ms`, which is exactly what `acks=all`
                    // callers wait on.
                    let at = Instant::now();
                    let offset = producer.send(topic, partition, key, value).await?;
                    Ok::<_, brahmaputra_client::ClientError>((offset, at.elapsed()))
                }
            });
            let mut waits: Vec<u32> = Vec::with_capacity(if latency { count as usize } else { 0 });
            let sent = drive_ordered(sends, in_flight.max(1), |(_, waited)| {
                if latency {
                    waits.push(waited.as_micros().min(u128::from(u32::MAX)) as u32);
                }
            })
            .await?;
            debug_assert_eq!(sent as u64, count);
            producer.flush().await?;
            let elapsed = started.elapsed().as_secs_f64();
            println!(
                "produced {count} records ({size} B each) in {elapsed:.2}s -> {:.0} msgs/sec",
                count as f64 / elapsed
            );
            if latency {
                println!("{}", format_latency(&mut waits));
            }
        }
        (None, None, None) => {
            anyhow::bail!("one of --value, --file, or --count/--value-size is required")
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn explicit_produce(
    broker: SocketAddr,
    topic: &str,
    partition: i32,
    key: Option<&str>,
    value: &str,
    acks: i32,
    timeout_ms: i32,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) -> Result<i64> {
    let record = Record {
        key: key.map(|key| Bytes::copy_from_slice(key.as_bytes())),
        value: Bytes::copy_from_slice(value.as_bytes()),
        timestamp_delta: 0,
        headers: Vec::new(),
    };
    // A fixed timestamp makes repeated CLI invocations with identical
    // explicit fields/content byte-identical, which is the intended manual
    // duplicate-replay harness.
    let batch = RecordBatch::new(0, 0, 0, vec![record])
        .with_compression(Compression::None)
        .with_producer(producer_id, producer_epoch, base_sequence);
    let request = ProduceRequest {
        topic: topic.to_owned(),
        partition,
        acks,
        timeout_ms,
        batches_length: 0,
    };
    let body = codec::encode_produce_request(&request, &[batch.encode()])?;
    let connection = Connection::connect_with(
        transport(),
        broker,
        Some("brahmaputra-cli-explicit".into()),
        1,
    )
    .await?;
    let response = connection.request(ApiKey::Produce, &body).await?;
    let response = ProduceResponse::decode(&response)
        .map_err(|error| brahmaputra_protocol::ProtocolError::Message(error.to_string()))?;
    brahmaputra_client::ClientError::from_error_code(response.error_code)?;
    Ok(response.base_offset)
}

/// Partitions to operate on: the explicit one, or all of the topic's.
async fn topic_partitions(
    consumer: &Consumer,
    topic: &str,
    partition: Option<i32>,
) -> Result<Vec<i32>> {
    if let Some(p) = partition {
        return Ok(vec![p]);
    }
    let meta = consumer.metadata(&[topic.to_owned()]).await?;
    let info = meta
        .topics
        .into_iter()
        .next()
        .with_context(|| format!("topic {topic:?} not in metadata"))?;
    Ok(info.partitions.iter().map(|p| p.partition).collect())
}

/// Where a standalone read starts and when it stops. Bundled because these
/// travel together everywhere and are meaningless apart.
struct ConsumeOptions<'a> {
    partition: Option<i32>,
    from: &'a str,
    offset: Option<i64>,
    max: Option<u64>,
    follow: bool,
    isolation_level: &'a str,
    quiet: bool,
    show_timestamp: bool,
}

async fn consume(broker: SocketAddr, topic: String, options: ConsumeOptions<'_>) -> Result<()> {
    let ConsumeOptions {
        partition,
        from,
        offset,
        max,
        follow,
        isolation_level,
        quiet,
        show_timestamp,
    } = options;
    let consumer = Consumer::connect_with(transport(), broker, "brahmaputra-cli")
        .await?
        .with_isolation_level(
            IsolationLevel::parse(isolation_level).unwrap_or_default(),
        );
    let started = Instant::now();
    let mut bytes = 0u64;
    let partitions = topic_partitions(&consumer, &topic, partition).await?;

    if max == Some(0) {
        return Ok(());
    }

    let mut printed = 0u64;
    let mut cursors = Vec::with_capacity(partitions.len());
    for p in partitions {
        let next = match offset {
            Some(o) => o,
            None => {
                let ts = if from == "latest" { LATEST } else { EARLIEST };
                consumer.list_offsets(&topic, p, ts).await?
            }
        };
        cursors.push((p, next));
    }

    if follow {
        // A long poll on one partition can otherwise prevent every later
        // partition from being serviced. Sweep all partitions without
        // blocking, sleeping only after a completely idle sweep.
        let max_wait_ms = if cursors.len() == 1 { 500 } else { 0 };
        loop {
            let mut made_progress = false;
            for (p, next) in &mut cursors {
                let records = consumer.fetch(&topic, *p, *next, max_wait_ms).await?;
                made_progress |= !records.is_empty();
                if emit_records(
                    *p,
                    next,
                    &mut printed,
                    max,
                    records,
                    quiet,
                    show_timestamp,
                    &mut bytes,
                ) {
                    report_consume_rate(quiet, printed, bytes, started);
                    return Ok(());
                }
            }
            if !made_progress {
                tokio::time::sleep(FOLLOW_IDLE_DELAY).await;
            }
        }
    }

    // Drain every partition with one request per broker per sweep rather
    // than one request per partition: at small record sizes the
    // per-request cost is what limits consume throughput.
    let mut cursors: Vec<(i32, i64)> = cursors;
    loop {
        let requests: Vec<(String, i32, i64)> = cursors
            .iter()
            .map(|(partition, next)| (topic.clone(), *partition, *next))
            .collect();
        let fetched = consumer.fetch_many_public(&requests, 500).await?;
        let mut made_progress = false;
        for (_, partition, records) in fetched {
            if records.is_empty() {
                continue;
            }
            made_progress = true;
            let Some(cursor) = cursors.iter_mut().find(|(p, _)| *p == partition) else {
                continue;
            };
            if emit_records(
                partition,
                &mut cursor.1,
                &mut printed,
                max,
                records,
                quiet,
                show_timestamp,
                &mut bytes,
            ) {
                report_consume_rate(quiet, printed, bytes, started);
                return Ok(());
            }
        }
        if !made_progress {
            break; // caught up
        }
    }
    report_consume_rate(quiet, printed, bytes, started);
    Ok(())
}

/// Throughput summary for `--quiet` runs (benchmark mode).
fn report_consume_rate(quiet: bool, records: u64, bytes: u64, started: Instant) {
    if !quiet {
        return;
    }
    let elapsed = started.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
    println!(
        "consumed {records} records ({bytes} B) in {elapsed:.2}s -> {:.0} msgs/sec, {:.2} MB/sec",
        records as f64 / elapsed,
        bytes as f64 / elapsed / (1024.0 * 1024.0)
    );
}

/// Group-coordinated consumption: join `group`, print records in the usual
/// format, and print the new assignment (one line) whenever it changes.
/// The group-specific half of the same thing.
struct GroupConsumeOptions<'a> {
    commit_interval_ms: u64,
    assignor: &'a str,
    auto_offset_reset: &'a str,
    max: Option<u64>,
    follow: bool,
    quiet: bool,
}

async fn consume_group(
    broker: SocketAddr,
    topic: &str,
    group: String,
    options: GroupConsumeOptions<'_>,
) -> Result<()> {
    let GroupConsumeOptions {
        commit_interval_ms,
        assignor,
        auto_offset_reset,
        max,
        follow,
        quiet,
    } = options;
    let topics: Vec<&str> = topic
        .split(',')
        .map(str::trim)
        .filter(|topic| !topic.is_empty())
        .collect();
    if topics.is_empty() {
        anyhow::bail!("--topic must name at least one topic");
    }
    let assignor = match assignor {
        "range" => Assignor::Range,
        "roundrobin" => Assignor::RoundRobin,
        "sticky" => Assignor::Sticky,
        "cooperative-sticky" => Assignor::CooperativeSticky,
        other => anyhow::bail!("unknown assignor {other:?}"),
    };
    let auto_offset_reset = match auto_offset_reset {
        "earliest" => AutoOffsetReset::Earliest,
        "latest" => AutoOffsetReset::Latest,
        "none" => AutoOffsetReset::None,
        other => anyhow::bail!("unknown auto-offset-reset {other:?}"),
    };
    let auto_commit = (commit_interval_ms > 0).then_some(Duration::from_millis(commit_interval_ms));
    let mut consumer = GroupConsumer::connect_with(transport(), broker, "brahmaputra-cli", &group)
        .await?
        .with_assignor(assignor)
        .with_auto_offset_reset(auto_offset_reset)
        .with_auto_commit(auto_commit);
    consumer.subscribe(&topics);

    if max == Some(0) {
        return Ok(());
    }

    let started = Instant::now();
    let mut bytes = 0u64;
    let mut printed = 0u64;
    let mut seen_version = consumer.assignment_version();
    loop {
        // Never let a poll hand back more than the remaining --max budget:
        // records returned count as consumed, so over-fetching here would
        // commit past what this run printed.
        if let Some(max) = max {
            let remaining = usize::try_from(max - printed).unwrap_or(usize::MAX);
            consumer.set_max_poll_records(remaining);
        }
        let records = consumer.poll(Duration::from_millis(500)).await?;
        if consumer.assignment_version() != seen_version && !quiet {
            seen_version = consumer.assignment_version();
            print_assignment(consumer.assignment());
        }
        let idle = records.is_empty();
        for record in records {
            bytes += (record.value.len() + record.key.as_ref().map_or(0, |key| key.len())) as u64;
            if !quiet {
                let key = record
                    .key
                    .map(|key| String::from_utf8_lossy(&key).into_owned())
                    .unwrap_or_else(|| "-".into());
                println!(
                    "partition={} offset={} key={} value={}",
                    record.partition,
                    record.offset,
                    key,
                    String::from_utf8_lossy(&record.value)
                );
            }
            printed += 1;
            if max.is_some_and(|max| printed >= max) {
                consumer.commit_sync().await?;
                report_consume_rate(quiet, printed, bytes, started);
                return Ok(());
            }
        }
        if !follow && idle {
            // Caught up: without --follow or an unmet --max, stop here.
            consumer.commit_sync().await?;
            report_consume_rate(quiet, printed, bytes, started);
            return Ok(());
        }
    }
}

/// Render the assignment as `assignment: topic-a=[0,1] topic-b=[2]`.
fn print_assignment(assignment: &[(String, i32)]) {
    let mut by_topic: BTreeMap<&str, Vec<i32>> = BTreeMap::new();
    for (topic, partition) in assignment {
        by_topic.entry(topic.as_str()).or_default().push(*partition);
    }
    let rendered = by_topic
        .iter()
        .map(|(topic, partitions)| {
            let partitions = partitions
                .iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join(",");
            format!("{topic}=[{partitions}]")
        })
        .collect::<Vec<_>>()
        .join(" ");
    println!(
        "assignment:{}",
        if rendered.is_empty() {
            rendered
        } else {
            format!(" {rendered}")
        }
    );
}

fn emit_records(
    partition: i32,
    next: &mut i64,
    printed: &mut u64,
    max: Option<u64>,
    records: Vec<FetchedRecord>,
    quiet: bool,
    show_timestamp: bool,
    bytes: &mut u64,
) -> bool {
    for record in records {
        *bytes += (record.value.len() + record.key.as_ref().map_or(0, |key| key.len())) as u64;
        if !quiet {
            let key = record
                .key
                .map(|key| String::from_utf8_lossy(&key).into_owned())
                .unwrap_or_else(|| "-".into());
            // Headers are printed only when present, so the common output
            // stays exactly as narrow as it was.
            let headers = if record.headers.is_empty() {
                String::new()
            } else {
                let rendered: Vec<String> = record
                    .headers
                    .iter()
                    .map(|header| {
                        let value = header
                            .value
                            .as_ref()
                            .map(|value| String::from_utf8_lossy(value).into_owned())
                            .unwrap_or_else(|| "null".into());
                        format!("{}={}", header.key, value)
                    })
                    .collect();
                format!(" headers=[{}]", rendered.join(","))
            };
            // Timestamps are opt-in, and headers only appear when a record
            // actually has one. The default line is a contract: the
            // verification scripts parse it positionally, and so does
            // anything a user has piped it into. Widening it by default
            // breaks every one of those silently.
            let timestamp = if show_timestamp {
                format!(" timestamp={}", record.timestamp)
            } else {
                String::new()
            };
            println!(
                "partition={partition} offset={}{timestamp} key={key} value={}{headers}",
                record.offset,
                String::from_utf8_lossy(&record.value)
            );
        }
        *printed += 1;
        *next = record.offset + 1;
        if max.is_some_and(|max| *printed >= max) {
            return true;
        }
    }
    false
}

/// What the broker speaks — the first call a client makes when broker and
/// client versions may differ (rolling upgrade).
async fn api_versions(broker: SocketAddr) -> Result<()> {
    let consumer = Consumer::connect_with(transport(), broker, "brahmaputra-cli").await?;
    let versions = consumer.api_versions().await?;
    println!("broker version: {}", versions.broker_version);
    println!("client wire version: {}", brahmaputra_protocol::API_VERSION);
    for (api_key, min, max) in &versions.api_versions {
        let name = ApiKey::from_i16(*api_key as i16)
            .map(|key| format!("{key:?}"))
            .unwrap_or_else(|_| format!("unknown({api_key})"));
        let usable = if *min <= brahmaputra_protocol::API_VERSION
            && brahmaputra_protocol::API_VERSION <= *max
        {
            "ok"
        } else {
            "INCOMPATIBLE"
        };
        println!("  api {api_key:>2} {name:<22} versions {min}..={max}  {usable}");
    }
    Ok(())
}

async fn metadata(broker: SocketAddr, topic: Option<String>) -> Result<()> {
    let consumer = Consumer::connect_with(transport(), broker, "brahmaputra-cli").await?;
    let topics = topic.into_iter().collect::<Vec<_>>();
    let meta = consumer.metadata(&topics).await?;

    println!("controller: {}", meta.controller_id);
    for b in &meta.brokers {
        println!("broker {}: {}:{}", b.broker_id, b.host, b.port);
    }
    for t in &meta.topics {
        println!("topic {:?} (error_code={}):", t.name, t.error_code);
        for p in &t.partitions {
            println!(
                "  partition {}: leader={} replicas={:?} isr={:?} leader_epoch={}",
                p.partition, p.leader, p.replicas, p.isr, p.leader_epoch
            );
        }
    }
    Ok(())
}

async fn offsets(
    broker: SocketAddr,
    topic: String,
    partition: Option<i32>,
    timestamp: Option<i64>,
) -> Result<()> {
    let consumer = Consumer::connect_with(transport(), broker, "brahmaputra-cli").await?;
    for p in topic_partitions(&consumer, &topic, partition).await? {
        let earliest = consumer.list_offsets(&topic, p, EARLIEST).await?;
        let latest = consumer.list_offsets(&topic, p, LATEST).await?;
        let mut line = format!("{topic}-{p}: earliest={earliest} latest={latest}");
        if let Some(target) = timestamp {
            let at = consumer.list_offsets(&topic, p, target).await?;
            line.push_str(&format!(" at({target})={at}"));
        }
        println!("{line}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn latency_percentiles_use_nearest_rank() {
        // 1 ms .. 100 ms, one sample each, so every percentile has an
        // unambiguous answer that interpolation would get wrong.
        let mut waits: Vec<u32> = (1..=100).map(|ms| ms * 1000).collect();
        let line = super::format_latency(&mut waits);
        assert!(line.contains("p50=50.00"), "{line}");
        assert!(line.contains("p95=95.00"), "{line}");
        assert!(line.contains("p99=99.00"), "{line}");
        assert!(line.contains("p99.9=100.00"), "{line}");
        assert!(line.contains("max=100.00"), "{line}");
        assert!(line.contains("avg=50.50"), "{line}");
        assert!(line.contains("(n=100)"), "{line}");
    }

    #[test]
    fn latency_percentiles_handle_one_and_none() {
        assert!(super::format_latency(&mut []).contains("no samples"));
        let line = super::format_latency(&mut [2_500]);
        assert!(line.contains("p50=2.50") && line.contains("p99.9=2.50"), "{line}");
    }

    use brahmaputra_broker::{Broker, BrokerConfig};
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    use super::*;

    #[test]
    fn parses_topic_create_with_controller_override() {
        let cli = Cli::try_parse_from([
            "brahmaputra-cli",
            "--controller",
            "http://controller.example:29092",
            "topic",
            "create",
            "--name",
            "orders",
            "--partitions",
            "6",
            "--replication-factor",
            "3",
        ])
        .unwrap();

        assert_eq!(cli.controller, "http://controller.example:29092");
        match cli.command {
            Command::Topic {
                command:
                    TopicCommand::Create {
                        name,
                        partitions,
                        replication_factor,
                        configs,
                    },
            } => {
                assert_eq!(name, "orders");
                assert_eq!(partitions, 6);
                assert_eq!(replication_factor, 3);
                assert!(configs.is_empty());
            }
            _ => panic!("expected topic create command"),
        }
    }

    #[test]
    fn parses_topic_delete_and_rejects_non_positive_create_counts() {
        let cli = Cli::try_parse_from(["brahmaputra-cli", "topic", "delete", "--name", "obsolete"])
            .unwrap();
        assert_eq!(cli.controller, "http://127.0.0.1:19092");
        assert!(matches!(
            cli.command,
            Command::Topic {
                command: TopicCommand::Delete { name }
            } if name == "obsolete"
        ));

        let invalid = Cli::try_parse_from([
            "brahmaputra-cli",
            "topic",
            "create",
            "--name",
            "orders",
            "--partitions",
            "0",
            "--replication-factor",
            "1",
        ]);
        assert!(invalid.is_err());
    }

    #[test]
    fn parses_all_supported_ack_modes_and_timeout() {
        for (raw_acks, expected) in [("0", 0), ("1", 1), ("all", -1), ("-1", -1)] {
            let cli = Cli::try_parse_from([
                "brahmaputra-cli",
                "produce",
                "--topic",
                "orders",
                "--value",
                "hello",
                "--acks",
                raw_acks,
                "--timeout-ms",
                "4321",
            ])
            .unwrap();
            assert!(matches!(
                cli.command,
                Command::Produce {
                    acks,
                    timeout_ms: 4321,
                    ..
                } if acks == expected
            ));
        }

        let defaults = Cli::try_parse_from([
            "brahmaputra-cli",
            "produce",
            "--topic",
            "orders",
            "--value",
            "hello",
        ])
        .unwrap();
        assert!(matches!(
            defaults.command,
            Command::Produce {
                acks: 1,
                timeout_ms: 30_000,
                ..
            }
        ));

        let config = producer_config(-1, 4321, false, 65_536, 20, Compression::None, 12);
        assert_eq!(config.acks, -1);
        assert_eq!(config.timeout_ms, 4321);
        assert_eq!(config.batch_size, 65_536);
        assert_eq!(config.linger_ms, 20);
        assert_eq!(config.compression, Compression::None);
        assert_eq!(config.max_in_flight, 12);
    }

    #[test]
    fn rejects_unsupported_ack_modes_and_negative_timeout() {
        for invalid_acks in ["2", "leader", "-2", "ALL"] {
            let result = Cli::try_parse_from([
                "brahmaputra-cli",
                "produce",
                "--topic",
                "orders",
                "--value",
                "hello",
                "--acks",
                invalid_acks,
            ]);
            let error = result
                .err()
                .expect("invalid ack mode should fail")
                .to_string();
            assert!(
                error.contains("acks must be one of: 0, 1, all, -1"),
                "unexpected error for {invalid_acks:?}: {error}"
            );
        }

        assert!(Cli::try_parse_from([
            "brahmaputra-cli",
            "produce",
            "--topic",
            "orders",
            "--value",
            "hello",
            "--timeout-ms",
            "-1",
        ])
        .is_err());
    }

    #[test]
    fn parses_producer_init_and_requires_bump_identity_as_a_pair() {
        let allocate = Cli::try_parse_from(["brahmaputra-cli", "producer", "init"]).unwrap();
        assert!(matches!(
            allocate.command,
            Command::Producer {
                command: ProducerCommand::Init {
                    producer_id: None,
                    producer_epoch: None
                }
            }
        ));

        let bump = Cli::try_parse_from([
            "brahmaputra-cli",
            "producer",
            "init",
            "--producer-id",
            "42",
            "--producer-epoch",
            "7",
        ])
        .unwrap();
        assert!(matches!(
            bump.command,
            Command::Producer {
                command: ProducerCommand::Init {
                    producer_id: Some(42),
                    producer_epoch: Some(7)
                }
            }
        ));
        assert!(Cli::try_parse_from([
            "brahmaputra-cli",
            "producer",
            "init",
            "--producer-id",
            "42"
        ])
        .is_err());
    }

    #[test]
    fn explicit_producer_fields_are_all_or_none_and_require_single_partition_value() {
        let explicit = Cli::try_parse_from([
            "brahmaputra-cli",
            "produce",
            "--topic",
            "orders",
            "--partition",
            "2",
            "--value",
            "one",
            "--producer-id",
            "44",
            "--producer-epoch",
            "3",
            "--base-sequence",
            "9",
        ])
        .unwrap();
        assert!(matches!(
            explicit.command,
            Command::Produce {
                partition: Some(2),
                producer_id: Some(44),
                producer_epoch: Some(3),
                base_sequence: Some(9),
                idempotent: false,
                ..
            }
        ));

        for incomplete in [
            vec!["--producer-id", "44"],
            vec!["--producer-id", "44", "--producer-epoch", "3"],
        ] {
            let mut args = vec![
                "brahmaputra-cli",
                "produce",
                "--topic",
                "orders",
                "--partition",
                "0",
                "--value",
                "one",
            ];
            args.extend(incomplete);
            assert!(Cli::try_parse_from(args).is_err());
        }
        assert!(Cli::try_parse_from([
            "brahmaputra-cli",
            "produce",
            "--topic",
            "orders",
            "--value",
            "one",
            "--producer-id",
            "44",
            "--producer-epoch",
            "3",
            "--base-sequence",
            "0"
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "brahmaputra-cli",
            "produce",
            "--topic",
            "orders",
            "--partition",
            "0",
            "--value",
            "one",
            "--idempotent",
            "--producer-id",
            "44",
            "--producer-epoch",
            "3",
            "--base-sequence",
            "0"
        ])
        .is_err());
    }

    #[test]
    fn parses_consume_group_options() {
        let cli = Cli::try_parse_from([
            "brahmaputra-cli",
            "consume",
            "--topic",
            "orders,payments",
            "--group",
            "shoppers",
            "--commit-interval-ms",
            "1000",
            "--assignor",
            "roundrobin",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Consume {
                ref topic,
                group: Some(ref group),
                commit_interval_ms: 1000,
                ref assignor,
                ..
            } if topic == "orders,payments" && group == "shoppers" && assignor == "roundrobin"
        ));

        let defaults = Cli::try_parse_from([
            "brahmaputra-cli",
            "consume",
            "--topic",
            "orders",
            "--group",
            "shoppers",
        ])
        .unwrap();
        assert!(matches!(
            defaults.command,
            Command::Consume {
                group: Some(_),
                commit_interval_ms: 5000,
                ref assignor,
                ..
            } if assignor == "range"
        ));
    }

    #[test]
    fn consume_group_conflicts_with_manual_positioning_and_validates_assignor() {
        for conflicting in [
            vec!["--partition", "0"],
            vec!["--offset", "3"],
            vec!["--from", "latest"],
        ] {
            let mut args = vec![
                "brahmaputra-cli",
                "consume",
                "--topic",
                "orders",
                "--group",
                "shoppers",
            ];
            args.extend(conflicting.clone());
            assert!(
                Cli::try_parse_from(args).is_err(),
                "--group should conflict with {conflicting:?}"
            );
        }

        // --assignor / --commit-interval-ms require --group.
        for extra in [
            vec!["--assignor", "roundrobin"],
            vec!["--commit-interval-ms", "1000"],
        ] {
            let mut args = vec!["brahmaputra-cli", "consume", "--topic", "orders"];
            args.extend(extra.clone());
            assert!(
                Cli::try_parse_from(args).is_err(),
                "{extra:?} should require --group"
            );
        }

        // Every assignor the group consumer implements must be reachable
        // from the CLI, or the flag silently lags the library.
        for assignor in ["range", "roundrobin", "sticky", "cooperative-sticky"] {
            assert!(
                Cli::try_parse_from([
                    "brahmaputra-cli",
                    "consume",
                    "--topic",
                    "orders",
                    "--group",
                    "shoppers",
                    "--assignor",
                    assignor,
                ])
                .is_ok(),
                "{assignor} should be accepted"
            );
        }

        assert!(Cli::try_parse_from([
            "brahmaputra-cli",
            "consume",
            "--topic",
            "orders",
            "--group",
            "shoppers",
            "--assignor",
            "no-such-assignor",
        ])
        .is_err());
    }

    #[tokio::test]
    async fn idempotent_acks_zero_is_rejected_before_network_io() {
        let cli = Cli::try_parse_from([
            "brahmaputra-cli",
            "--broker",
            "127.0.0.1:1",
            "produce",
            "--topic",
            "orders",
            "--partition",
            "0",
            "--value",
            "one",
            "--idempotent",
            "--acks",
            "0",
        ])
        .unwrap();
        assert_eq!(
            run(cli).await.unwrap_err().to_string(),
            "idempotent produce requires acknowledgements (--acks 1 or all)"
        );
    }

    #[test]
    fn topic_configs_are_repeatable_and_duplicates_are_rejected_in_argument_order() {
        let cli = Cli::try_parse_from([
            "brahmaputra-cli",
            "topic",
            "create",
            "--name",
            "orders",
            "--partitions",
            "3",
            "--replication-factor",
            "3",
            "--config",
            "min.insync.replicas=2",
            "--config",
            "cleanup.policy=compact",
        ])
        .unwrap();
        let Command::Topic {
            command: TopicCommand::Create { configs, .. },
        } = cli.command
        else {
            panic!("expected topic create command");
        };
        assert_eq!(
            collect_topic_configs(configs).unwrap(),
            BTreeMap::from([
                ("cleanup.policy".to_owned(), "compact".to_owned()),
                ("min.insync.replicas".to_owned(), "2".to_owned()),
            ])
        );

        let error = collect_topic_configs(vec![
            parse_topic_config("min.insync.replicas=2").unwrap(),
            parse_topic_config("cleanup.policy=compact").unwrap(),
            parse_topic_config("min.insync.replicas=3").unwrap(),
        ])
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "duplicate topic config \"min.insync.replicas\""
        );
    }

    #[test]
    fn rejects_malformed_topic_configs() {
        for invalid_config in [
            "min.insync.replicas",
            "=2",
            "min.insync.replicas=",
            "min.insync replicas=2",
        ] {
            let result = Cli::try_parse_from([
                "brahmaputra-cli",
                "topic",
                "create",
                "--name",
                "orders",
                "--partitions",
                "3",
                "--replication-factor",
                "3",
                "--config",
                invalid_config,
            ]);
            assert!(
                result.is_err(),
                "accepted malformed config {invalid_config:?}"
            );
        }
    }

    #[tokio::test]
    async fn duplicate_topic_configs_fail_before_contacting_controller() {
        let cli = Cli::try_parse_from([
            "brahmaputra-cli",
            "--controller",
            "http://127.0.0.1:1",
            "topic",
            "create",
            "--name",
            "orders",
            "--partitions",
            "3",
            "--replication-factor",
            "3",
            "--config",
            "min.insync.replicas=2",
            "--config",
            "min.insync.replicas=3",
        ])
        .unwrap();

        let error = run(cli).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "duplicate topic config \"min.insync.replicas\""
        );
    }

    async fn mock_controller(
        expected_command: MetadataCommand,
        response: ControllerCommandResult,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 1024];
            let (header_end, content_length) = loop {
                let read = socket.read(&mut chunk).await.unwrap();
                assert!(
                    read > 0,
                    "controller client closed before sending a request"
                );
                request.extend_from_slice(&chunk[..read]);
                if let Some(header_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                {
                    let headers = str::from_utf8(&request[..header_end]).unwrap();
                    assert!(
                        headers.starts_with("POST /api/v1/controller/command HTTP/1.1\r\n"),
                        "unexpected request line: {headers}"
                    );
                    let content_length = headers
                        .lines()
                        .filter_map(|line| line.split_once(':'))
                        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                        .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                        .expect("request should include content-length");
                    break (header_end + 4, content_length);
                }
            };
            while request.len() < header_end + content_length {
                let read = socket.read(&mut chunk).await.unwrap();
                assert!(
                    read > 0,
                    "controller client closed before sending its JSON body"
                );
                request.extend_from_slice(&chunk[..read]);
            }

            let actual_command = serde_json::from_slice::<MetadataCommand>(
                &request[header_end..header_end + content_length],
            )
            .unwrap();
            assert_eq!(actual_command, expected_command);

            let body = serde_json::to_vec(&response).unwrap();
            let headers = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(headers.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
        });
        (format!("http://{address}"), task)
    }

    #[tokio::test]
    async fn topic_create_posts_to_controller_without_resolving_broker() {
        let expected_command = MetadataCommand::CreateTopic {
            name: "orders".to_owned(),
            partitions: 6,
            replication_factor: 3,
            configs: BTreeMap::from([
                ("cleanup.policy".to_owned(), "compact".to_owned()),
                ("min.insync.replicas".to_owned(), "2".to_owned()),
            ]),
        };
        let (controller, server) = mock_controller(
            expected_command,
            Ok(MetadataEvent::TopicCreated {
                name: "orders".to_owned(),
            }),
        )
        .await;
        let cli = Cli::try_parse_from([
            "brahmaputra-cli",
            "--broker",
            "this broker address is deliberately invalid",
            "--controller",
            &controller,
            "topic",
            "create",
            "--name",
            "orders",
            "--partitions",
            "6",
            "--replication-factor",
            "3",
            "--config",
            "min.insync.replicas=2",
            "--config",
            "cleanup.policy=compact",
        ])
        .unwrap();

        run(cli).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn topic_delete_posts_to_controller() {
        let (controller, server) = mock_controller(
            MetadataCommand::DeleteTopic {
                name: "obsolete".to_owned(),
            },
            Ok(MetadataEvent::TopicDeleted {
                name: "obsolete".to_owned(),
            }),
        )
        .await;
        let cli = Cli::try_parse_from([
            "brahmaputra-cli",
            "--controller",
            &controller,
            "topic",
            "delete",
            "--name",
            "obsolete",
        ])
        .unwrap();

        run(cli).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ordered_driver_bounds_concurrency_and_reports_in_input_order() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let futures = (0..8).map(|value| {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            async move {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis((8 - value) * 2)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok::<_, &'static str>(value)
            }
        });

        let mut seen = Vec::new();
        let completed = drive_ordered(futures, 3, |value| seen.push(value))
            .await
            .unwrap();

        assert_eq!(completed, 8);
        assert_eq!(seen, (0..8).collect::<Vec<_>>());
        assert_eq!(peak.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn ordered_driver_returns_the_first_input_error() {
        let futures = (0..3).map(|value| async move {
            if value == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Err("first")
            } else if value == 1 {
                Err("later")
            } else {
                Ok(value)
            }
        });

        let error = drive_ordered(futures, 3, |_| {}).await.unwrap_err();
        assert_eq!(error, "first");
    }

    #[tokio::test]
    async fn produce_cli_uses_all_acks_against_a_real_broker() {
        let dir = tempdir().unwrap();
        let broker = Broker::bind(BrokerConfig {
            port: 0,
            data_dirs: vec![dir.path().to_path_buf()],
            ..BrokerConfig::default()
        })
        .await
        .unwrap();
        let addr = broker.local_addr();
        let broker_address = addr.to_string();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(Arc::new(broker).run(async move {
            let _ = shutdown_rx.await;
        }));

        let cli = Cli::try_parse_from([
            "brahmaputra-cli",
            "--broker",
            &broker_address,
            "produce",
            "--topic",
            "acks-all",
            "--partition",
            "0",
            "--value",
            "replicated-value",
            "--acks",
            "all",
            "--timeout-ms",
            "2000",
        ])
        .unwrap();
        run(cli).await.unwrap();

        let consumer = Consumer::connect(addr, "cli-acks-test").await.unwrap();
        let records = consumer.fetch("acks-all", 0, 0, 500).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[0].value, Bytes::from_static(b"replicated-value"));

        let _ = shutdown_tx.send(());
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("broker shutdown timed out")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn producer_cli_harness_allocates_and_replays_explicit_sequence() {
        let dir = tempdir().unwrap();
        let broker = Broker::bind(BrokerConfig {
            port: 0,
            data_dirs: vec![dir.path().to_path_buf()],
            ..BrokerConfig::default()
        })
        .await
        .unwrap();
        let addr = broker.local_addr();
        let broker_address = addr.to_string();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(Arc::new(broker).run(async move {
            let _ = shutdown_rx.await;
        }));

        run(Cli::try_parse_from([
            "brahmaputra-cli",
            "--broker",
            &broker_address,
            "producer",
            "init",
        ])
        .unwrap())
        .await
        .unwrap();
        let explicit = || {
            Cli::try_parse_from([
                "brahmaputra-cli",
                "--broker",
                &broker_address,
                "produce",
                "--topic",
                "cli-idempotent",
                "--partition",
                "0",
                "--value",
                "one",
                "--producer-id",
                "1",
                "--producer-epoch",
                "0",
                "--base-sequence",
                "0",
            ])
            .unwrap()
        };
        run(explicit()).await.unwrap();
        run(explicit()).await.unwrap();
        let consumer = Consumer::connect(addr, "cli-idempotent-check")
            .await
            .unwrap();
        let records = consumer.fetch("cli-idempotent", 0, 0, 100).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value, Bytes::from_static(b"one"));

        let _ = shutdown_tx.send(());
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn follow_services_a_later_partition() {
        let dir = tempdir().unwrap();
        let broker = Broker::bind(BrokerConfig {
            port: 0,
            data_dirs: vec![dir.path().to_path_buf()],
            default_partitions: 2,
            ..BrokerConfig::default()
        })
        .await
        .unwrap();
        let addr = broker.local_addr();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(Arc::new(broker).run(async move {
            let _ = shutdown_rx.await;
        }));

        let producer = Producer::connect(
            addr,
            ProducerConfig {
                linger_ms: 0,
                ..ProducerConfig::default()
            },
        )
        .await
        .unwrap();
        producer
            .send(
                "follow-fairness",
                Some(1),
                None,
                Bytes::from_static(b"partition-one"),
            )
            .await
            .unwrap();
        drop(producer);

        tokio::time::timeout(
            Duration::from_secs(2),
            consume(
                addr,
                "follow-fairness".into(),
                ConsumeOptions {
                    partition: None,
                    from: "earliest",
                    offset: None,
                    max: Some(1),
                    follow: true,
                    isolation_level: "read_uncommitted",
                    quiet: false,
                    show_timestamp: false,
                },
            ),
        )
        .await
        .expect("follow should not remain blocked on empty partition zero")
        .unwrap();

        let _ = shutdown_tx.send(());
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("broker shutdown timed out")
            .unwrap()
            .unwrap();
    }
}
