//! The broker: TCP listener, per-connection tasks, and the partition-actor
//! supervisor (Blueprint 02 §2).

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use brahmaputra_client::Transport;
use brahmaputra_metrics::{names, MetricKey, Metrics};
use brahmaputra_metadata::{
    BrokerEpoch, ClusterMetadata, MetadataCache, NodeRole, PartitionMetadata,
};
use brahmaputra_protocol::{decode_payload, encode_payload, FrameHeader};
use brahmaputra_storage::{Log, LogConfig};
use bytes::Bytes;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex, OwnedMutexGuard, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, info, warn};

use crate::actor::{self, PartitionHandle};
use crate::error::BrokerError;
use crate::group::GroupCoordinator;
use crate::handlers;
use crate::producer_id::ProducerIdManager;
use crate::quic::QuicListener;
use crate::quota::{QuotaConfig, QuotaKind, QuotaManager};
use tokio_rustls::TlsAcceptor;
use crate::replication::{ReplicationHealthSnapshot, ReplicationTracker};
use crate::state::{partition_dir, BrokerState};

const TASK_DRAIN_GRACE: Duration = Duration::from_millis(250);
/// Requests one connection may have in flight at once. Generous enough to
/// keep a batching client busy, bounded so one client cannot spawn
/// unbounded work.
const MAX_IN_FLIGHT_PER_CONNECTION: usize = 64;

#[derive(Default)]
struct BrokerLifecycle {
    closing: bool,
    actor_tasks: Vec<JoinHandle<()>>,
}

/// Broker configuration.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Stable broker identity used by cluster partition assignments.
    /// The M1 single-broker default remains broker 0.
    pub broker_id: i32,
    /// Controller-issued epoch for an already registered embedded broker.
    /// The combined server leaves this unset and activates the epoch only
    /// after its RegisterBroker command commits.
    pub broker_epoch: Option<BrokerEpoch>,
    /// Host to bind and advertise in Metadata responses.
    pub host: String,
    /// Port to bind; 0 picks an ephemeral port (tests).
    pub port: u16,
    /// Data directory holding one `<topic>-<partition>` log dir per
    /// partition plus `meta.toml`.
    pub data_dir: PathBuf,
    /// Partition count for auto-created topics.
    pub default_partitions: i32,
    /// Storage config applied to every partition log.
    pub log_config: LogConfig,
    /// Bounded command-channel capacity per partition actor.
    pub channel_capacity: usize,
    /// Largest accepted frame (post-length-prefix bytes).
    pub max_frame_bytes: usize,
    /// How often partition actors apply enabled time/size retention policies.
    pub retention_check_interval: Duration,
    /// Shared controller-materialized metadata image. `None` preserves the
    /// standalone M1 topic map and implicit topic creation behavior.
    pub metadata_cache: Option<MetadataCache>,
    /// Opt in to M3 replicated-commit semantics. This must be enabled only
    /// when the combined server also starts a [`crate::ReplicaManager`].
    /// Metadata-only M2 clusters retain their prior auto-commit behavior.
    pub replication_enabled: bool,
    /// Data-plane transport. Clients must be configured to match: a broker
    /// listens on exactly one.
    pub transport: Transport,
    /// Per-client byte-rate limits. Disabled by default.
    pub quota: QuotaConfig,
    /// Shared metric registry. The broker records into it; the dashboard
    /// and Prometheus endpoint read from it.
    pub metrics: Metrics,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        BrokerConfig {
            broker_id: 0,
            broker_epoch: None,
            host: "127.0.0.1".into(),
            port: 9092,
            data_dir: PathBuf::from("./data"),
            default_partitions: 1,
            log_config: LogConfig::default(),
            channel_capacity: 1024,
            max_frame_bytes: 32 * 1024 * 1024,
            retention_check_interval: Duration::from_secs(1),
            metadata_cache: None,
            replication_enabled: false,
            transport: Transport::default(),
            quota: QuotaConfig::default(),
            metrics: Metrics::default(),
        }
    }
}

/// The bound listener for the configured transport.
enum BoundListener {
    Tcp(TcpListener),
    TcpTls(TcpListener, TlsAcceptor),
    Quic(QuicListener),
}

/// TLS 1.3 acceptor over a freshly generated self-signed certificate.
fn tls_acceptor() -> Result<TlsAcceptor, BrokerError> {
    let (cert_der, key_der) = crate::quic::self_signed_identity()?;
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der.into())
        .map_err(|error| BrokerError::Meta(format!("cannot build tls config: {error}")))?;
    config.alpn_protocols = vec![crate::quic::ALPN.to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// A bound broker: load state, (re)open logs, spawn partition actors.
/// Call [`Broker::run`] to serve.
pub struct Broker {
    config: BrokerConfig,
    state: BrokerState,
    handles: DashMap<(String, i32), PartitionHandle>,
    partition_mutations: DashMap<(String, i32), Arc<AsyncMutex<()>>>,
    lifecycle: Mutex<BrokerLifecycle>,
    replication: ReplicationTracker,
    producer_ids: ProducerIdManager,
    groups: GroupCoordinator,
    quotas: QuotaManager,
    listener: Mutex<Option<BoundListener>>,
    shutdown_tx: watch::Sender<bool>,
    local_broker_epoch: AtomicU64,
    addr: SocketAddr,
}

impl Broker {
    /// Bind the listener and spawn actors for every partition known from
    /// `meta.toml`.
    pub async fn bind(config: BrokerConfig) -> Result<Broker, BrokerError> {
        if config.replication_enabled && config.metadata_cache.is_none() {
            return Err(BrokerError::Meta(
                "replication_enabled requires a metadata cache".into(),
            ));
        }
        let state = BrokerState::load(&config.data_dir, config.default_partitions)?;
        let producer_ids = ProducerIdManager::open(&config.data_dir, config.broker_id)
            .map_err(|error| BrokerError::Meta(error.to_string()))?;
        // One transport at a time: a broker either speaks TCP or QUIC, and
        // clients must be configured to match (see `Transport`).
        let (listener, addr) = match config.transport {
            Transport::Tcp => {
                let listener = TcpListener::bind((config.host.as_str(), config.port)).await?;
                let addr = listener.local_addr()?;
                (BoundListener::Tcp(listener), addr)
            }
            Transport::TcpTls => {
                let listener = TcpListener::bind((config.host.as_str(), config.port)).await?;
                let addr = listener.local_addr()?;
                (BoundListener::TcpTls(listener, tls_acceptor()?), addr)
            }
            Transport::Quic => {
                let bind = resolve_bind_addr(&config.host, config.port).await?;
                let listener = QuicListener::bind(bind, config.max_frame_bytes)?;
                let addr = listener.local_addr();
                (BoundListener::Quic(listener), addr)
            }
        };
        let (shutdown_tx, _) = watch::channel(false);
        let initial_broker_epoch = config.broker_epoch.unwrap_or(0);
        let config_quota = config.quota;
        let broker = Broker {
            config,
            state,
            handles: DashMap::new(),
            partition_mutations: DashMap::new(),
            lifecycle: Mutex::new(BrokerLifecycle::default()),
            replication: ReplicationTracker::default(),
            producer_ids,
            groups: GroupCoordinator::default(),
            quotas: QuotaManager::new(config_quota),
            listener: Mutex::new(Some(listener)),
            shutdown_tx,
            local_broker_epoch: AtomicU64::new(initial_broker_epoch),
            addr,
        };
        let local_partitions = if let Some(cache) = broker.metadata_cache() {
            let image = cache.snapshot();
            image
                .topics
                .values()
                .flat_map(|topic| {
                    topic
                        .partitions
                        .values()
                        .filter(|partition| partition.replicas.contains(&broker.config.broker_id))
                        .map(|partition| (topic.name.clone(), partition.partition))
                })
                .collect::<Vec<_>>()
        } else {
            broker
                .state
                .topics()
                .into_iter()
                .flat_map(|(topic, partitions)| {
                    (0..partitions).map(move |partition| (topic.clone(), partition))
                })
                .collect()
        };
        for (topic, partition) in local_partitions {
            broker.open_partition(&topic, partition)?;
        }
        info!(
            addr = %broker.addr,
            local_partitions = broker.handles.len(),
            "broker bound"
        );
        Ok(broker)
    }

    /// The address actually bound (after port 0 resolution). This is what
    /// Metadata responses advertise.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn config(&self) -> &BrokerConfig {
        &self.config
    }

    pub fn state(&self) -> &BrokerState {
        &self.state
    }

    /// Irreversibly fence this broker incarnation and request whole-node
    /// shutdown. This is synchronous so epoch-validation paths can call it
    /// immediately; [`Broker::run`] observes the signal, closes all tracked
    /// connections, and then drains partition actors before returning.
    pub fn fence(&self) {
        self.lifecycle.lock().expect("broker lifecycle").closing = true;
        self.shutdown_tx.send_replace(true);
        // If `run` has not claimed the listener yet, close it here. Otherwise
        // the broker-owned shutdown signal wakes the active accept loop.
        drop(self.listener.lock().expect("broker listener").take());
    }

    /// Whether this broker incarnation has entered irreversible shutdown.
    pub fn is_fenced(&self) -> bool {
        *self.shutdown_tx.borrow()
    }

    /// Activate the controller-issued epoch for this process exactly once.
    /// A different epoch means another incarnation has superseded this one;
    /// this process fences itself instead of participating in epoch ping-pong.
    pub fn activate_broker_epoch(&self, broker_epoch: BrokerEpoch) -> Result<(), BrokerError> {
        if broker_epoch == 0 {
            return Err(BrokerError::Meta(
                "controller broker epochs must be non-zero".into(),
            ));
        }
        if self.is_fenced() {
            return Err(BrokerError::FencedBrokerEpoch {
                broker_id: self.config.broker_id,
                requested: self.local_broker_epoch.load(Ordering::Acquire),
                current: broker_epoch,
            });
        }
        match self.local_broker_epoch.compare_exchange(
            0,
            broker_epoch,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(current) if current == broker_epoch => Ok(()),
            Err(current) => {
                self.fence();
                Err(BrokerError::FencedBrokerEpoch {
                    broker_id: self.config.broker_id,
                    requested: current,
                    current: broker_epoch,
                })
            }
        }
    }

    pub fn local_broker_epoch(&self) -> BrokerEpoch {
        self.local_broker_epoch.load(Ordering::Acquire)
    }

    /// Validate the exact local broker incarnation against one immutable
    /// controller image. Higher epochs and an explicit same-epoch controller
    /// fence irreversibly stop this process. A missing/lower image merely
    /// rejects traffic while local metadata catches up.
    pub fn validate_local_broker_epoch(&self, image: &ClusterMetadata) -> Result<(), BrokerError> {
        if self.metadata_cache().is_none() {
            return Ok(());
        }
        let requested = self.local_broker_epoch();
        let registered = image.brokers.get(&self.config.broker_id);
        let current = registered.map_or(0, |broker| broker.broker_epoch);
        let eligible = !self.is_fenced()
            && requested != 0
            && registered.is_some_and(|broker| {
                broker.broker_epoch == requested
                    && broker.alive
                    && broker.roles.contains(&NodeRole::Broker)
            });
        if eligible {
            return Ok(());
        }

        if requested != 0
            && registered.is_some_and(|broker| {
                broker.broker_epoch > requested
                    || (broker.broker_epoch == requested
                        && (!broker.alive || !broker.roles.contains(&NodeRole::Broker)))
            })
        {
            self.fence();
        }
        Err(BrokerError::FencedBrokerEpoch {
            broker_id: self.config.broker_id,
            requested,
            current,
        })
    }

    /// Validate against the broker's currently published controller image.
    pub fn validate_local_broker_lease(&self) -> Result<(), BrokerError> {
        match self.metadata_cache() {
            Some(cache) => self.validate_local_broker_epoch(&cache.snapshot()),
            None => Ok(()),
        }
    }

    /// Per-partition gate shared by Produce and controller ISR mutations.
    pub async fn partition_mutation_guard(
        &self,
        topic: &str,
        partition: i32,
    ) -> OwnedMutexGuard<()> {
        let gate = self
            .partition_mutations
            .entry((topic.to_owned(), partition))
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone();
        gate.lock_owned().await
    }

    /// Shared cluster metadata cache, when this broker is in M2 cluster mode.
    pub fn metadata_cache(&self) -> Option<&MetadataCache> {
        self.config.metadata_cache.as_ref()
    }

    pub fn replication_health(&self) -> ReplicationHealthSnapshot {
        if let Some(cache) = self.metadata_cache() {
            let image = cache.snapshot();
            self.replication
                .reconcile_metadata(&image, self.config.broker_id);
        }
        self.replication.snapshot()
    }

    pub(crate) fn replication_tracker(&self) -> &ReplicationTracker {
        &self.replication
    }

    pub(crate) fn producer_ids(&self) -> &ProducerIdManager {
        &self.producer_ids
    }

    /// Consumer-group coordinator (M4), sharded by `__consumer_offsets`
    /// partition.
    pub(crate) fn groups(&self) -> &GroupCoordinator {
        &self.groups
    }

    /// The shared metric registry (DESIGN.md §9.1).
    pub fn metrics(&self) -> &Metrics {
        &self.config.metrics
    }

    /// Groups this broker coordinates, for the dashboard's group view.
    pub async fn list_groups(self: &Arc<Self>) -> Result<Vec<serde_json::Value>, String> {
        self.groups
            .list(self, &[])
            .await
            .map(|groups| {
                groups
                    .into_iter()
                    .map(|group| {
                        serde_json::json!({
                            "group_id": group.group_id,
                            "state": group.state,
                            "generation": group.generation,
                            "member_count": group.member_count,
                            "coordinator_partition": group.coordinator_partition,
                        })
                    })
                    .collect()
            })
            .map_err(|error| error.to_string())
    }

    /// Per-partition committed offsets for one group, as the dashboard's
    /// lag view needs them. Lag itself is derived against the log end
    /// offsets this broker leads.
    pub async fn group_lag(self: &Arc<Self>, group_id: &str) -> Result<Vec<serde_json::Value>, String> {
        let shard = crate::handlers::coordinator_shard_for(self, group_id)
            .await
            .map_err(|error| error.to_string())?;
        let described = self
            .groups
            .describe(&shard, group_id)
            .map_err(|error| error.to_string())?;
        let mut out = Vec::with_capacity(described.offsets.len());
        for entry in described.offsets {
            let log_end = match self.partition(&entry.topic, entry.partition) {
                Ok(handle) => handle.offsets().await.map(|(_, _, hwm)| hwm).unwrap_or(-1),
                Err(_) => -1,
            };
            out.push(serde_json::json!({
                "topic": entry.topic,
                "partition": entry.partition,
                "committed_offset": entry.offset,
                "log_end_offset": log_end,
                "lag": if log_end >= 0 && entry.offset >= 0 { (log_end - entry.offset).max(0) } else { -1 },
            }));
        }
        Ok(out)
    }

    /// Refresh the per-partition gauges the dashboard charts. Called on a
    /// timer rather than on every append: these are point-in-time values,
    /// and sampling them costs one actor round trip per local partition.
    pub async fn sample_partition_metrics(&self) {
        let metrics = self.metrics();
        let mut leader_partitions = 0_i64;
        let mut under_replicated = 0_i64;
        let image = self.metadata_cache().map(|cache| cache.snapshot());

        let handles: Vec<((String, i32), PartitionHandle)> = self
            .handles
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();
        for ((topic, partition), handle) in handles {
            let Ok((start, end, high_watermark)) = handle.offsets().await else {
                continue;
            };
            let partition_label = partition.to_string();
            let labels = [("topic", topic.as_str()), ("partition", partition_label.as_str())];
            metrics.set_gauge(MetricKey::with(names::LOG_START_OFFSET, &labels), start);
            metrics.set_gauge(MetricKey::with(names::LOG_END_OFFSET, &labels), end);
            metrics.set_gauge(MetricKey::with(names::HIGH_WATERMARK, &labels), high_watermark);

            if let Some(assignment) = image
                .as_ref()
                .and_then(|image| image.topics.get(&topic))
                .and_then(|topic| topic.partitions.get(&partition))
            {
                metrics.set_gauge(
                    MetricKey::with(names::ISR_SIZE, &labels),
                    assignment.isr.len() as i64,
                );
                if assignment.leader == self.config.broker_id {
                    leader_partitions += 1;
                    if assignment.isr.len() < assignment.replicas.len() {
                        under_replicated += 1;
                    }
                }
            }
        }
        metrics.gauge(names::LEADER_PARTITIONS, leader_partitions);
        metrics.gauge(names::UNDER_REPLICATED, under_replicated);
    }

    /// Charge `bytes` to a client's quota and wait out any throttle it
    /// owes, before the caller sends its response.
    ///
    /// Delaying the response rather than rejecting the request is what
    /// keeps a quota lossless: the work is already done and durable, the
    /// client simply learns about it later (Kafka's model).
    pub(crate) async fn throttle(
        &self,
        client_id: Option<&str>,
        kind: QuotaKind,
        bytes: u64,
    ) -> Duration {
        if !self.quotas.is_enabled() {
            return Duration::ZERO;
        }
        let delay = self.quotas.throttle_for(client_id, kind, bytes);
        if !delay.is_zero() {
            debug!(client = client_id.unwrap_or("<anonymous>"), ?delay, ?kind, "throttling client");
            tokio::time::sleep(delay).await;
        }
        delay
    }

    /// A receiver for the broker-wide shutdown/fence signal; background
    /// tasks (like the group session-expiry sweeper) stop on it.
    pub(crate) fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    /// Apply a controller-published local leader assignment to the data
    /// plane before serving traffic. This records the exact election epoch
    /// boundary and recomputes HWM from the current ISR. In particular, a
    /// promoted sole-ISR follower exposes its already replicated log even
    /// if it never received the old leader's final HWM response.
    pub async fn reconcile_leader_partition(
        &self,
        topic: &str,
        assignment: &PartitionMetadata,
    ) -> Result<(), BrokerError> {
        if !self.config.replication_enabled {
            return Err(BrokerError::Meta(
                "leader reconciliation requires replication_enabled".into(),
            ));
        }
        if assignment.leader != self.config.broker_id {
            return Err(BrokerError::NotLeaderOrFollower {
                topic: topic.to_owned(),
                partition: assignment.partition,
                broker_id: self.config.broker_id,
                leader: assignment.leader,
            });
        }
        if let Some(cache) = self.metadata_cache() {
            let image = cache.snapshot();
            self.replication
                .reconcile_metadata(&image, self.config.broker_id);
        }
        let handle = self.partition(topic, assignment.partition)?;
        handle.record_leader_epoch(assignment.leader_epoch).await?;
        self.replication
            .advance_leader_high_watermark(topic, assignment, &handle)
            .await?;
        Ok(())
    }

    /// Handle to an existing partition; errors if the topic is unknown or
    /// the partition out of range. In cluster mode this is a client-facing
    /// leader-only lookup (fetch / list-offsets path).
    pub fn partition(&self, topic: &str, partition: i32) -> Result<PartitionHandle, BrokerError> {
        if self.metadata_cache().is_some() {
            return self.cluster_partition(topic, partition, true);
        }
        match self.state.partitions(topic) {
            Some(n) if partition >= 0 && partition < n => self.open_partition(topic, partition),
            _ => Err(BrokerError::UnknownTopicOrPartition {
                topic: topic.to_owned(),
                partition,
            }),
        }
    }

    /// Like [`Broker::partition`], but auto-creates the topic with the
    /// default partition count on first sight (produce / metadata path, M1).
    pub fn partition_auto_create(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<PartitionHandle, BrokerError> {
        if self.metadata_cache().is_some() {
            return self.cluster_partition(topic, partition, true);
        }
        let n = self.state.ensure_topic(topic)?;
        if partition < 0 || partition >= n {
            return Err(BrokerError::UnknownTopicOrPartition {
                topic: topic.to_owned(),
                partition,
            });
        }
        self.open_partition(topic, partition)
    }

    /// Open a locally assigned leader or follower replica.
    ///
    /// This is the cluster-internal boundary reserved for follower
    /// replication. Client produce, fetch, and list-offsets use the
    /// leader-only methods above. In M1 there is only one local replica, so
    /// this is equivalent to [`Broker::partition`].
    pub fn replica_partition(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<PartitionHandle, BrokerError> {
        if self.metadata_cache().is_some() {
            self.cluster_partition(topic, partition, false)
        } else {
            self.partition(topic, partition)
        }
    }

    fn cluster_partition(
        &self,
        topic: &str,
        partition: i32,
        require_leader: bool,
    ) -> Result<PartitionHandle, BrokerError> {
        let image = self
            .metadata_cache()
            .expect("cluster partition lookup requires a metadata cache")
            .snapshot();
        self.validate_local_broker_epoch(&image)?;
        let metadata = image
            .topics
            .get(topic)
            .and_then(|topic| topic.partitions.get(&partition))
            .ok_or_else(|| BrokerError::UnknownTopicOrPartition {
                topic: topic.to_owned(),
                partition,
            })?;
        let is_replica = metadata.replicas.contains(&self.config.broker_id);
        if !is_replica || (require_leader && metadata.leader != self.config.broker_id) {
            return Err(BrokerError::NotLeaderOrFollower {
                topic: topic.to_owned(),
                partition,
                broker_id: self.config.broker_id,
                leader: metadata.leader,
            });
        }
        self.open_partition(topic, partition)
    }

    /// Open the partition's log and spawn its actor, or return the existing
    /// handle. The lifecycle lock closes the actor-spawn gate atomically with
    /// shutdown; the `DashMap` entry lock makes concurrent spawns for the
    /// same partition impossible.
    fn open_partition(&self, topic: &str, partition: i32) -> Result<PartitionHandle, BrokerError> {
        let mut lifecycle = self.lifecycle.lock().expect("broker lifecycle");
        if lifecycle.closing {
            return Err(BrokerError::ActorUnavailable(
                "broker is shutting down".into(),
            ));
        }
        let key = (topic.to_owned(), partition);
        match self.handles.entry(key) {
            Entry::Occupied(e) => Ok(e.get().clone()),
            Entry::Vacant(e) => {
                let dir = partition_dir(&self.config.data_dir, topic, partition);
                actor::complete_pending_replica_reset(&dir)?;
                let mut log_config = self.config.log_config.clone();
                if topic == crate::group::OFFSETS_TOPIC {
                    // A committed consumer offset that disappears on restart
                    // is a correctness break, not a lost optimisation, and
                    // commits arrive far too slowly for an eager checkpoint
                    // to cost anything. User topics keep the periodic one.
                    log_config.hwm_checkpoint_interval_ms = 0;
                }
                let mut log = Log::open(&dir, log_config)?;
                if !self.config.replication_enabled {
                    // A standalone leader owns the only replica, so every
                    // complete batch recovered from disk is committed. A
                    // hard kill can persist the log append before its HWM
                    // checkpoint; promote that durable tail on M1 reopen.
                    // Cluster replicas must retain the controller/replication
                    // supplied HWM and therefore deliberately skip this.
                    let recovered_end = log.log_end_offset();
                    if log.high_watermark() < recovered_end {
                        log.set_high_watermark(recovered_end)?;
                    }
                }
                // The same periodic tick drives retention and the time-based
                // flush policy, so either one being configured starts it.
                let retention_enabled = self.config.log_config.retention_ms.is_some()
                    || self.config.log_config.retention_bytes.is_some()
                    || self.config.log_config.flush_interval_ms.is_some();
                let replicated_commit =
                    self.config.metadata_cache.is_some() && self.config.replication_enabled;
                let (handle, task) = match (replicated_commit, retention_enabled) {
                    (true, true) => actor::spawn_cluster_with_retention(
                        log,
                        self.config.channel_capacity,
                        Some(self.config.retention_check_interval),
                        dir.clone(),
                        self.config.log_config.clone(),
                    ),
                    (true, false) => actor::spawn_cluster(
                        log,
                        self.config.channel_capacity,
                        dir.clone(),
                        self.config.log_config.clone(),
                    ),
                    (false, true) => actor::spawn_with_retention(
                        log,
                        self.config.channel_capacity,
                        Some(self.config.retention_check_interval),
                    ),
                    (false, false) => actor::spawn(log, self.config.channel_capacity),
                };
                lifecycle.actor_tasks.push(task);
                debug!(topic, partition, "partition actor spawned");
                Ok(e.insert(handle).clone())
            }
        }
    }

    /// Serve until `shutdown` resolves or [`Broker::fence`] is called, then
    /// stop gracefully: cancel and await every tracked connection first,
    /// then drop actor senders and await the partition tasks.
    /// Serve the QUIC endpoint until shutdown. The per-stream request model
    /// lives in [`crate::quic`]; everything else — the group sweeper, the
    /// fence watch, actor drain — is identical to the TCP path.
    async fn run_quic(
        self: Arc<Self>,
        listener: QuicListener,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), BrokerError> {
        let mut shutdown = std::pin::pin!(shutdown);
        let mut broker_shutdown = self.shutdown_tx.subscribe();
        info!(addr = %self.addr, "broker serving (quic)");

        let sweeper_broker = Arc::clone(&self);
        let sweeper = tokio::spawn(async move {
            crate::group::run_expiry_sweeper(sweeper_broker).await;
        });
        let serving = listener.serve(Arc::clone(&self), self.config.max_frame_bytes);
        let mut serving = std::pin::pin!(serving);

        tokio::select! {
            biased;
            _ = &mut shutdown => info!("shutdown signal received"),
            _ = wait_for_shutdown(&mut broker_shutdown) => info!("broker fence received"),
            _ = &mut serving => info!("quic endpoint closed"),
        }
        let _ = self.shutdown_tx.send(true);
        sweeper.abort();
        self.graceful_shutdown_actors().await;
        Ok(())
    }

    pub async fn run(
        self: Arc<Self>,
        shutdown: impl Future<Output = ()>,
    ) -> Result<(), BrokerError> {
        let listener = self.listener.lock().expect("broker listener").take();
        let Some(listener) = listener else {
            if self.is_fenced() {
                self.graceful_shutdown_actors().await;
                return Ok(());
            }
            return Err(BrokerError::Meta(
                "broker listener is already running or closed".into(),
            ));
        };
        let (listener, tls) = match listener {
            BoundListener::Tcp(listener) => (listener, None),
            BoundListener::TcpTls(listener, acceptor) => (listener, Some(acceptor)),
            BoundListener::Quic(quic) => return self.run_quic(quic, shutdown).await,
        };
        let mut shutdown = std::pin::pin!(shutdown);
        let mut broker_shutdown = self.shutdown_tx.subscribe();
        let mut connection_tasks = JoinSet::new();
        info!(addr = %self.addr, "broker serving");
        // Group session-expiry sweeper (M4): works in standalone and cluster
        // mode; it observes the broker shutdown watch like a connection task.
        {
            let sweeper_broker = Arc::clone(&self);
            connection_tasks.spawn(async move {
                crate::group::run_expiry_sweeper(sweeper_broker).await;
            });
        }
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    info!("shutdown signal received");
                    break;
                }
                _ = wait_for_shutdown(&mut broker_shutdown) => {
                    info!("broker fence received");
                    break;
                }
                joined = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                    log_task_result(joined, "connection");
                }
                accepted = listener.accept() => match accepted {
                    Ok((socket, peer)) => {
                        debug!(%peer, "connection accepted");
                        // Responses are small and latency-critical; without
                        // this, Nagle holds a response until the previous
                        // segment is acknowledged, which pairs with the
                        // peer's delayed ACK to add tens of milliseconds per
                        // request on anything but loopback. The client sets
                        // it on its half already.
                        if let Err(error) = socket.set_nodelay(true) {
                            warn!(%peer, %error, "cannot disable Nagle on accepted socket");
                        }
                        let broker = Arc::clone(&self);
                        let mut connection_shutdown = self.shutdown_tx.subscribe();
                        let tls = tls.clone();
                        connection_tasks.spawn(async move {
                            // The TLS handshake happens inside the
                            // connection task, so a slow or hostile peer
                            // stalls only itself, never the accept loop.
                            let stream: Box<dyn BrokerStream> = match tls {
                                None => Box::new(socket),
                                Some(acceptor) => match acceptor.accept(socket).await {
                                    Ok(stream) => Box::new(stream),
                                    Err(error) => {
                                        debug!(%peer, %error, "tls handshake failed");
                                        return;
                                    }
                                },
                            };
                            tokio::select! {
                                biased;
                                _ = wait_for_shutdown(&mut connection_shutdown) => {
                                    debug!(%peer, "connection cancelled for broker shutdown");
                                }
                                result = handle_connection(broker, stream, peer) => {
                                    if let Err(e) = result {
                                        debug!(%peer, error = %e, "connection closed");
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => warn!(error = %e, "accept failed"),
                },
            }
        }
        // Close the listening socket and fence partition lookup/spawn before
        // cancelling connections. A handler that races the signal can no
        // longer create an actor after the actor-task snapshot is taken.
        drop(listener);
        self.fence();
        drain_connection_tasks(&mut connection_tasks).await;
        self.graceful_shutdown_actors().await;
        info!("broker stopped");
        Ok(())
    }

    async fn graceful_shutdown_actors(&self) {
        self.handles.clear();
        let tasks = {
            let mut lifecycle = self.lifecycle.lock().expect("broker lifecycle");
            std::mem::take(&mut lifecycle.actor_tasks)
        };
        if tasks.is_empty() {
            return;
        }

        let abort_handles = tasks
            .iter()
            .map(JoinHandle::abort_handle)
            .collect::<Vec<_>>();
        let mut tracked = JoinSet::new();
        for task in tasks {
            tracked.spawn(async move {
                if let Err(error) = task.await {
                    if !error.is_cancelled() {
                        warn!(%error, "partition actor task failed");
                    }
                }
            });
        }

        if tokio::time::timeout(TASK_DRAIN_GRACE, drain_tasks(&mut tracked))
            .await
            .is_err()
        {
            warn!(
                remaining = tracked.len(),
                "partition actors exceeded shutdown grace; cancelling"
            );
            for task in abort_handles {
                task.abort();
            }
            drain_tasks(&mut tracked).await;
        }
    }
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

async fn drain_connection_tasks(tasks: &mut JoinSet<()>) {
    if tokio::time::timeout(TASK_DRAIN_GRACE, drain_tasks(tasks))
        .await
        .is_err()
    {
        warn!(
            remaining = tasks.len(),
            "connections exceeded shutdown grace; aborting"
        );
        tasks.abort_all();
        drain_tasks(tasks).await;
    }
}

async fn drain_tasks(tasks: &mut JoinSet<()>) {
    while let Some(joined) = tasks.join_next().await {
        log_task_result(Some(joined), "broker");
    }
}

fn log_task_result(joined: Option<Result<(), tokio::task::JoinError>>, kind: &str) {
    if let Some(Err(error)) = joined {
        if !error.is_cancelled() {
            warn!(%error, task_kind = kind, "broker task failed");
        }
    }
}

/// One task per connection: length-delimited frames in, dispatched to
/// handlers, responses framed back. Requests are processed in arrival
/// order per connection, so responses go out in order too.
/// A plain or TLS-wrapped client socket; the framing above is identical.
pub(crate) trait BrokerStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> BrokerStream for T {}

/// Serve one client connection.
///
/// Requests are dispatched concurrently rather than one at a time.
/// Correlation ids already let responses return out of order, and each
/// partition is a single-writer actor, so concurrency here cannot reorder
/// a partition's appends — it only stops one slow request (a long poll, an
/// `acks=all` wait) from blocking every other request on the same socket.
/// The client's in-flight window is meaningless without this.
async fn handle_connection(
    broker: Arc<Broker>,
    socket: Box<dyn BrokerStream>,
    peer: SocketAddr,
) -> Result<(), BrokerError> {
    let codec = LengthDelimitedCodec::builder()
        .big_endian()
        .length_field_length(4)
        .max_frame_length(broker.config.max_frame_bytes)
        .new_codec();
    let framed = Framed::new(socket, codec);
    let (mut sink, mut stream) = framed.split();

    // Responses funnel through one writer task; the channel is bounded so a
    // client that stops reading applies backpressure instead of growing the
    // broker's memory without limit.
    let (responses_tx, mut responses_rx) = mpsc::channel::<Bytes>(broker.config.channel_capacity);
    let writer = tokio::spawn(async move {
        while let Some(payload) = responses_rx.recv().await {
            if sink.send(payload).await.is_err() {
                break;
            }
        }
    });

    // Bound how many requests one connection may have in flight, so a
    // single client cannot spawn unbounded work on the broker.
    let in_flight = Arc::new(Semaphore::new(MAX_IN_FLIGHT_PER_CONNECTION));
    let mut requests = JoinSet::new();

    while let Some(frame) = stream.next().await {
        let mut payload = frame?.freeze();
        let header = match decode_payload(&mut payload) {
            Ok(header) => header,
            Err(e) => {
                warn!(%peer, error = %e, "bad frame header, closing connection");
                break;
            }
        };
        trace_request(&header, payload.len());

        let Ok(permit) = Arc::clone(&in_flight).acquire_owned().await else {
            break;
        };
        let broker = Arc::clone(&broker);
        let responses = responses_tx.clone();
        requests.spawn(async move {
            let _permit = permit;
            if let Some(body) = handlers::dispatch(&broker, &header, payload).await {
                let response_header = FrameHeader {
                    api_key: header.api_key,
                    api_version: header.api_version,
                    correlation_id: header.correlation_id,
                    client_id: None,
                };
                let _ = responses.send(encode_payload(&response_header, &body)).await;
            }
        });

        // Reap finished requests so the set does not grow with the
        // connection's lifetime.
        while let Some(joined) = requests.try_join_next() {
            if let Err(error) = joined {
                debug!(%peer, %error, "request task failed");
            }
        }
    }

    // Drain in-flight requests before closing: an accepted append must be
    // answered, which is the same graceful-shutdown promise the actors make.
    while let Some(joined) = requests.join_next().await {
        if let Err(error) = joined {
            debug!(%peer, %error, "request task failed");
        }
    }
    drop(responses_tx);
    let _ = writer.await;
    Ok(())
}


fn trace_request(header: &FrameHeader, body_len: usize) {
    debug!(
        api = ?header.api_key,
        version = header.api_version,
        correlation_id = header.correlation_id,
        client_id = ?header.client_id,
        body_len,
        "request"
    );
}

/// Resolve `host:port` to a bindable address. QUIC binds a UDP socket, so
/// unlike `TcpListener::bind` there is no host-string overload to lean on.
async fn resolve_bind_addr(host: &str, port: u16) -> Result<SocketAddr, BrokerError> {
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    tokio::net::lookup_host((host, port))
        .await?
        .next()
        .ok_or_else(|| BrokerError::Meta(format!("host {host:?} resolved to no addresses")))
}
