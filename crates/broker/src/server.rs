//! The broker: TCP listener, per-connection tasks, and the partition-actor
//! supervisor (Blueprint 02 §2).

use std::future::Future;
use std::io::IoSlice;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use brahmaputra_client::Transport;
use brahmaputra_metadata::{
    BrokerEpoch, ClusterMetadata, MetadataCache, NodeRole, PartitionMetadata,
};
use brahmaputra_metrics::{names, MetricKey, Metrics};
use brahmaputra_protocol::{decode_payload, encode_frame_prefix, FrameHeader};
use brahmaputra_storage::{Log, LogConfig, LogRegion};
use bytes::Bytes;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, Mutex as AsyncMutex, OwnedMutexGuard, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tracing::{debug, info, warn};

use crate::actor::{self, PartitionHandle};
use crate::error::BrokerError;
use crate::group::GroupCoordinator;
use crate::transaction::TransactionCoordinator;
use crate::handlers;
use crate::producer_id::ProducerIdManager;
use crate::quic::QuicListener;
use crate::quota::{QuotaConfig, QuotaKind, QuotaManager};
use crate::replication::{ReplicationHealthSnapshot, ReplicationTracker};
use crate::logdirs::LogDirs;
use crate::state::BrokerState;
use crate::tls::TlsIdentity;
use tokio_rustls::TlsAcceptor;

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
    /// Directories holding partition logs, one per disk.
    ///
    /// Several means JBOD: each partition lives on exactly one of them, a
    /// new one is placed on whichever holds the fewest, and a directory
    /// that fails takes only its own partitions offline instead of the
    /// whole broker.
    pub data_dirs: Vec<PathBuf>,
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
    /// How long a group with no members keeps its committed offsets
    /// (`offsets.retention.ms`). `None` keeps them forever.
    ///
    /// The clock starts when the group empties, not when each offset was
    /// committed: a live group is still using its offsets however old they
    /// are, and expiring under it would silently rewind the consumer.
    pub offsets_retention: Option<Duration>,
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
    /// Refuse any request from a connection that has not authenticated, and
    /// authorize every request against the cluster ACLs. Off by default,
    /// matching Kafka PLAINTEXT listeners; production must turn it on.
    pub require_auth: bool,
    /// Which certificate the broker presents, and whose client certificates
    /// it accepts. Default generates a self-signed identity at startup and
    /// asks the client for nothing, which is the development behaviour.
    pub tls: TlsIdentity,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        BrokerConfig {
            broker_id: 0,
            broker_epoch: None,
            host: "127.0.0.1".into(),
            port: 9092,
            data_dirs: vec![PathBuf::from("./data")],
            default_partitions: 1,
            log_config: LogConfig::default(),
            channel_capacity: 1024,
            max_frame_bytes: 32 * 1024 * 1024,
            retention_check_interval: Duration::from_secs(1),
            // Kafka's default: 7 days.
            offsets_retention: Some(Duration::from_secs(7 * 24 * 60 * 60)),
            metadata_cache: None,
            replication_enabled: false,
            transport: Transport::default(),
            quota: QuotaConfig::default(),
            metrics: Metrics::default(),
            require_auth: false,
            tls: TlsIdentity::default(),
        }
    }
}

/// The bound listener for the configured transport.
enum BoundListener {
    Tcp(TcpListener),
    TcpTls(TcpListener, TlsAcceptor),
    Quic(QuicListener),
}

/// TLS 1.3 acceptor for the configured identity.
///
/// With no `--tls-cert` this still generates a self-signed certificate at
/// startup, so a development broker needs no files; with one, it presents
/// what the operator gave it, and with `--tls-client-ca` it also demands a
/// certificate from the client.
/// Whether a storage error means the disk is gone rather than the data
/// being wrong.
///
/// The distinction matters: a corrupt batch is this partition's problem and
/// taking eleven other partitions offline for it would be an outage caused
/// by the recovery. A directory that cannot be read at all is the disk's
/// problem, and every partition on it is already affected.
fn is_disk_failure(error: &brahmaputra_storage::StorageError) -> bool {
    matches!(error, brahmaputra_storage::StorageError::Io(_))
}

fn tls_acceptor(identity: &TlsIdentity) -> Result<TlsAcceptor, BrokerError> {
    Ok(TlsAcceptor::from(Arc::new(crate::tls::server_config(
        identity,
    )?)))
}

/// A bound broker: load state, (re)open logs, spawn partition actors.
/// Call [`Broker::run`] to serve.
pub struct Broker {
    config: BrokerConfig,
    state: BrokerState,
    handles: DashMap<(String, i32), PartitionHandle>,
    /// The log configuration each open partition was last told to use, so
    /// the maintenance tick can tell a real change from a no-op.
    applied_topic_configs: DashMap<(String, i32), LogConfig>,
    partition_mutations: DashMap<(String, i32), Arc<AsyncMutex<()>>>,
    lifecycle: Mutex<BrokerLifecycle>,
    replication: ReplicationTracker,
    producer_ids: ProducerIdManager,
    groups: GroupCoordinator,
    log_dirs: Arc<LogDirs>,
    transactions: TransactionCoordinator,
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
        let log_dirs = Arc::new(LogDirs::open(&config.data_dirs)?);
        log_dirs.log_layout();
        // Broker-wide state — the topic map and the producer-id journal —
        // is not per-partition, so it lives in the first directory rather
        // than being spread across them.
        let state = BrokerState::load(log_dirs.primary(), config.default_partitions)?;
        let producer_ids = ProducerIdManager::open(log_dirs.primary(), config.broker_id)
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
                (BoundListener::TcpTls(listener, tls_acceptor(&config.tls)?), addr)
            }
            Transport::Quic => {
                let bind = resolve_bind_addr(&config.host, config.port).await?;
                let listener = QuicListener::bind(bind, config.max_frame_bytes, &config.tls)?;
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
            applied_topic_configs: DashMap::new(),
            partition_mutations: DashMap::new(),
            lifecycle: Mutex::new(BrokerLifecycle::default()),
            replication: ReplicationTracker::default(),
            producer_ids,
            groups: GroupCoordinator::default(),
            log_dirs,
            transactions: TransactionCoordinator::default(),
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
            match broker.open_partition(&topic, partition) {
                Ok(_) => {}
                // A partition on a disk that is already broken must not stop
                // the broker from starting. Refusing to start because one
                // disk of twelve is bad would give up exactly the isolation
                // several directories exist to provide — the other eleven
                // disks are fine and their partitions are serveable.
                //
                // The partition is left unopened, so every request for it is
                // refused as unavailable, and in a cluster the controller
                // elects around it as it would for any replica that stopped
                // fetching.
                Err(error @ BrokerError::LogDirOffline { .. }) => {
                    warn!(%topic, partition, %error, "skipping a partition on an offline log dir");
                }
                Err(error) => return Err(error),
            }
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

    /// Delete local data for partitions this broker no longer replicates.
    ///
    /// Reassignment moves a partition by adding the new brokers and then
    /// dropping the old ones from `replicas`. Without this the dropped
    /// broker keeps every byte forever, so a cluster can be rebalanced but
    /// never reclaims disk — which is half the reason to rebalance.
    ///
    /// Deleting data is the one operation that cannot be undone, so the
    /// conditions are deliberately narrow. Every one of these must hold:
    ///
    /// * the image knows this broker, and lists it as alive — a partial or
    ///   pre-registration image is not evidence of anything;
    /// * the image contains the topic, and the topic contains the
    ///   partition — "the topic is missing" is indistinguishable from "the
    ///   image has not caught up", and deleting on that reading would lose
    ///   data on any lagging follower;
    /// * `replicas` genuinely omits this broker, with a reassignment
    ///   settled rather than in flight.
    ///
    /// A partition still being reassigned is skipped: `replicas` is the
    /// union during a move, so a broker that appears absent is a broker
    /// the controller has not finished with.
    /// Push topic configuration changes into partitions that are already
    /// open.
    ///
    /// Resolving a topic's config only when a partition is opened made a
    /// live change silently inert: an operator who shortens `retention.ms`
    /// sees the new value echoed back by the dashboard, watches nothing
    /// happen, and cannot tell whether the setting is wrong or merely not
    /// in effect until the next restart. Recomputing here and sending the
    /// result to the actor closes that gap.
    ///
    /// Cheap enough to run on the maintenance tick: it compares the
    /// resolved config against what the partition already holds and sends
    /// nothing when they match, which is the overwhelmingly common case.
    pub(crate) async fn apply_topic_config_changes(&self, image: &ClusterMetadata) {
        let open: Vec<((String, i32), PartitionHandle)> = self
            .handles
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        for ((topic_name, partition), handle) in open {
            let configs = image.topics.get(&topic_name).map(|topic| &topic.configs);
            let resolved =
                crate::state::log_config_for_topic(&self.config.log_config, &topic_name, configs);
            let previous = self
                .applied_topic_configs
                .insert((topic_name.clone(), partition), resolved.clone());
            if previous.as_ref() == Some(&resolved) {
                continue;
            }
            if let Err(error) = handle.reconfigure(resolved).await {
                tracing::warn!(
                    %error,
                    topic = %topic_name,
                    partition,
                    "could not push a topic configuration change to a live partition"
                );
            } else if previous.is_some() {
                tracing::info!(
                    topic = %topic_name,
                    partition,
                    "applied a topic configuration change to a running partition"
                );
            }
        }
    }

    pub(crate) fn drain_unowned_partitions(&self, image: &ClusterMetadata) {
        let local_id = self.config.broker_id;
        let registered = image
            .brokers
            .get(&local_id)
            .is_some_and(|broker| broker.alive);
        if !registered {
            return;
        }

        let owned: Vec<(String, i32)> = self
            .handles
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for (topic_name, partition) in owned {
            // The offsets topic is coordinator state, not a reassignable
            // user partition; leave it alone.
            if topic_name == crate::group::OFFSETS_TOPIC {
                continue;
            }
            let Some(topic) = image.topics.get(&topic_name) else {
                continue; // topic unknown to this image: not evidence
            };
            let Some(assignment) = topic.partitions.get(&partition) else {
                continue;
            };
            if assignment.is_reassigning() || assignment.replicas.contains(&local_id) {
                continue;
            }

            // Close the actor before touching the directory: deleting files
            // out from under a running log would surface as corruption
            // rather than as a clean removal.
            let Some((_, handle)) = self.handles.remove(&(topic_name.clone(), partition)) else {
                continue;
            };
            drop(handle);
            let Some(dir) = self.log_dirs.existing(&topic_name, partition) else {
                continue;
            };
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => tracing::info!(
                    topic = %topic_name,
                    partition,
                    "dropped local data for a partition this broker no longer replicates"
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => tracing::warn!(
                    %error,
                    topic = %topic_name,
                    partition,
                    "could not remove drained partition data"
                ),
            }
            // Release the placement too. If this partition is ever assigned
            // back to this broker it should be placed afresh — most likely
            // on a different disk, since the one it used to be on now holds
            // one fewer partition than it did.
            self.log_dirs.forget(&topic_name, partition);
            self.applied_topic_configs
                .remove(&(topic_name.clone(), partition));
        }
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

    /// Transaction coordinator, sharded by `__transaction_state` partition.
    pub(crate) fn transactions(&self) -> &TransactionCoordinator {
        &self.transactions
    }

    /// The broker's log directories and the partition placement over them.
    pub(crate) fn log_dirs(&self) -> &Arc<LogDirs> {
        &self.log_dirs
    }

    /// Stop serving these partitions locally.
    ///
    /// Called when the disk under them fails. Dropping the handle closes
    /// the actor, which is what stops anything else touching a disk that
    /// is returning errors; every later request resolves to a directory
    /// that is now offline and is refused with `LOG_DIR_OFFLINE`.
    ///
    /// Deliberately does **not** delete anything or tell the controller. In
    /// a cluster the replica simply stops fetching, and the ISR machinery
    /// that already handles a broker going quiet handles this too — the
    /// difference being that only these partitions go quiet rather than
    /// every partition on the broker.
    pub(crate) async fn close_partitions(&self, partitions: &[(String, i32)]) {
        for (topic, partition) in partitions {
            if let Some((_, handle)) = self.handles.remove(&(topic.clone(), *partition)) {
                drop(handle);
                warn!(
                    %topic,
                    partition,
                    "partition closed: the log directory holding it went offline"
                );
            }
            self.applied_topic_configs
                .remove(&(topic.clone(), *partition));
        }
        self.metrics()
            .gauge(names::OFFLINE_LOG_DIRS, self.offline_log_dirs() as i64);
    }

    /// How many configured log directories have failed.
    pub(crate) fn offline_log_dirs(&self) -> usize {
        self.log_dirs
            .describe()
            .iter()
            .filter(|dir| !dir.online)
            .count()
    }

    /// Attribute an IO failure to the disk it happened on and take that
    /// disk offline.
    ///
    /// Synchronous, because it is called from paths that cannot await: the
    /// directory is marked immediately so nothing else opens a partition on
    /// it, and the health watcher closes the actors on its next tick.
    /// Marking is what makes the difference; closing is cleanup.
    pub(crate) fn note_log_dir_failure(&self, path: &std::path::Path, reason: &str) {
        let Some(dir) = self.log_dirs.owning_dir(path) else {
            return;
        };
        if !self.log_dirs.mark_offline(&dir, reason).is_empty() {
            self.metrics()
                .gauge(names::OFFLINE_LOG_DIRS, self.offline_log_dirs() as i64);
        }
    }

    /// The data-plane address of another broker, from cluster metadata.
    ///
    /// `None` in standalone mode, or for a broker whose registration is not
    /// in the current image — a caller that cannot reach a peer has to
    /// treat that as a failure rather than as a reason to act locally.
    pub(crate) fn broker_address(&self, broker_id: i32) -> Option<SocketAddr> {
        let image = self.metadata_cache()?.snapshot();
        let registered = image.brokers.get(&broker_id)?;
        format!("{}:{}", registered.host, registered.data_port)
            .parse()
            .ok()
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
    pub async fn group_lag(
        self: &Arc<Self>,
        group_id: &str,
    ) -> Result<Vec<serde_json::Value>, String> {
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
            let labels = [
                ("topic", topic.as_str()),
                ("partition", partition_label.as_str()),
            ];
            metrics.set_gauge(MetricKey::with(names::LOG_START_OFFSET, &labels), start);
            metrics.set_gauge(MetricKey::with(names::LOG_END_OFFSET, &labels), end);
            metrics.set_gauge(
                MetricKey::with(names::HIGH_WATERMARK, &labels),
                high_watermark,
            );

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
        principal: Option<&str>,
        client_id: Option<&str>,
        kind: QuotaKind,
        bytes: u64,
    ) -> Duration {
        let override_rate = self.quota_override(principal, client_id, kind);
        if override_rate.is_none() && !self.quotas.is_enabled() {
            return Duration::ZERO;
        }
        let delay = self
            .quotas
            .throttle_for(principal, client_id, kind, bytes, override_rate);
        if !delay.is_zero() {
            debug!(
                user = principal.unwrap_or("<anonymous>"),
                client = client_id.unwrap_or("<anonymous>"),
                ?delay,
                ?kind,
                "throttling client"
            );
            tokio::time::sleep(delay).await;
        }
        delay
    }

    /// The rate a configured quota entity imposes on this request, if any.
    ///
    /// Replication is deliberately excluded: it is charged against the
    /// cluster's own catch-up traffic, not against a tenant, so a rule
    /// written about a user has nothing to say about it.
    fn quota_override(
        &self,
        principal: Option<&str>,
        client_id: Option<&str>,
        kind: QuotaKind,
    ) -> Option<u64> {
        if matches!(kind, QuotaKind::Replication) {
            return None;
        }
        let cache = self.metadata_cache()?;
        let image = cache.snapshot();
        // Overwhelmingly the common case, and worth not paying for.
        if image.quotas.is_empty() {
            return None;
        }
        let limits = image.quota_for(principal, client_id)?;
        match kind {
            QuotaKind::Produce => limits.produce_bytes_per_sec,
            QuotaKind::Fetch => limits.fetch_bytes_per_sec,
            QuotaKind::Replication => None,
        }
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

    /// A handle to a partition this broker has *already opened*, or `None`.
    ///
    /// Reporting on disk usage must not itself allocate disk: going through
    /// [`Broker::partition`] would create the log directory for a partition
    /// this broker happens not to host yet, so a monitoring sweep would
    /// leave a trail of empty partitions behind it.
    pub fn hosted_partition(&self, topic: &str, partition: i32) -> Option<PartitionHandle> {
        self.handles
            .get(&(topic.to_owned(), partition))
            .map(|entry| entry.clone())
    }

    /// Whether this broker is the leader for `partition` right now.
    ///
    /// Standalone brokers lead everything they host — there is nobody else
    /// to lead it.
    pub fn leads_partition(&self, topic: &str, partition: i32) -> bool {
        match self.metadata_cache() {
            Some(cache) => cache
                .snapshot()
                .topics
                .get(topic)
                .and_then(|topic| topic.partitions.get(&partition))
                .is_some_and(|assignment| assignment.leader == self.config.broker_id),
            None => true,
        }
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
                let dir = self.log_dirs.resolve(topic, partition)?;
                actor::complete_pending_replica_reset(&dir)?;
                // Broker defaults, overridden by whatever this topic sets.
                // Reading them here is what makes `retention.ms` on a topic
                // mean something rather than being stored and ignored.
                let image = self.config.metadata_cache.as_ref().map(|c| c.snapshot());
                let topic_configs = image
                    .as_ref()
                    .and_then(|image| image.topics.get(topic))
                    .map(|meta| &meta.configs);
                let log_config = crate::state::log_config_for_topic(
                    &self.config.log_config,
                    topic,
                    topic_configs,
                );
                // Opening a log is the first thing that touches the disk,
                // so an IO failure here is the earliest evidence it is
                // gone — earlier than the health probe, which runs on a
                // timer. Recording it takes the directory offline for
                // every other partition on it too, rather than letting
                // each one discover the same dead disk separately.
                let mut log = match Log::open(&dir, log_config.clone()) {
                    Ok(log) => log,
                    Err(error) => {
                        if is_disk_failure(&error) {
                            self.note_log_dir_failure(&dir, &error.to_string());
                        }
                        return Err(error.into());
                    }
                };
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
                // One maintenance tick drives retention, timed flushes, segment
                // rolls, compaction, and the periodic high-watermark checkpoint.
                // User topics need the tick even when all optional retention
                // and flush policies are disabled, otherwise the default
                // five-second HWM checkpoint would never reach disk.
                let maintenance_enabled = log_config.retention_ms.is_some()
                    || log_config.retention_bytes.is_some()
                    || log_config.flush_interval_ms.is_some()
                    || log_config.segment_ms.is_some()
                    || log_config.compact
                    || log_config.hwm_checkpoint_interval_ms > 0;
                let replicated_commit =
                    self.config.metadata_cache.is_some() && self.config.replication_enabled;
                let (handle, task) = match (replicated_commit, maintenance_enabled) {
                    (true, true) => actor::spawn_cluster_with_retention(
                        log,
                        self.config.channel_capacity,
                        Some(self.config.retention_check_interval),
                        dir.clone(),
                        log_config.clone(),
                    ),
                    (true, false) => actor::spawn_cluster(
                        log,
                        self.config.channel_capacity,
                        dir.clone(),
                        log_config.clone(),
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
        let watcher_broker = Arc::clone(&self);
        let watcher = tokio::spawn(async move {
            crate::logdirs::run_health_watcher(watcher_broker).await;
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
        watcher.abort();
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
        // Disk health: a directory that fails takes only its own partitions
        // offline. Probed on a timer as well as noticed on IO errors,
        // because a disk that dies under an idle topic would otherwise not
        // be discovered until something next tried to use it.
        {
            let watcher_broker = Arc::clone(&self);
            connection_tasks.spawn(async move {
                crate::logdirs::run_health_watcher(watcher_broker).await;
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
                            match tls {
                                // Plaintext keeps the socket concrete, so a
                                // fetch can be answered with `sendfile`.
                                None => {
                                    let (reader, writer) = socket.into_split();
                                    tokio::select! {
                                        biased;
                                        _ = wait_for_shutdown(&mut connection_shutdown) => {
                                            debug!(%peer, "connection cancelled for broker shutdown");
                                        }
                                        result = handle_connection(
                                            broker,
                                            reader,
                                            ResponseSink::Plain(writer),
                                            peer,
                                            // Plaintext carries no certificate
                                            // and therefore no identity.
                                            None,
                                        ) => {
                                            if let Err(error) = result {
                                                debug!(%peer, %error, "connection closed");
                                            }
                                        }
                                    }
                                }
                                Some(acceptor) => {
                                    let (stream, principal): (Box<dyn BrokerStream>, Option<String>) = match acceptor.accept(socket).await {
                                        Ok(stream) => {
                                            // rustls has already verified the
                                            // chain against the configured CA
                                            // by this point; with no client CA
                                            // configured there is no peer
                                            // certificate and the connection
                                            // stays anonymous.
                                            let principal = stream
                                                .get_ref()
                                                .1
                                                .peer_certificates()
                                                .and_then(|chain| chain.first())
                                                .and_then(crate::tls::common_name);
                                            (Box::new(stream), principal)
                                        }
                                        Err(error) => {
                                            debug!(%peer, %error, "tls handshake failed");
                                            return;
                                        }
                                    };
                                    let (reader, writer) = tokio::io::split(stream);
                                    tokio::select! {
                                        biased;
                                        _ = wait_for_shutdown(&mut connection_shutdown) => {
                                            debug!(%peer, "connection cancelled for broker shutdown");
                                        }
                                        result = handle_connection(
                                            broker,
                                            reader,
                                            ResponseSink::Encrypted(writer),
                                            peer,
                                            principal,
                                        ) => {
                                            if let Err(error) = result {
                                                debug!(%peer, %error, "connection closed");
                                            }
                                        }
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
pub(crate) trait BrokerStream:
    tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send
{
}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> BrokerStream for T {}

/// One response as it will be written: buffers first, then any file ranges.
struct Response {
    chunks: Vec<Bytes>,
    regions: Vec<LogRegion>,
}

/// The write half of a connection.
///
/// This is an enum rather than a trait object because the plaintext case
/// has to stay concrete: answering a fetch with `sendfile` needs the
/// socket's file descriptor, and a boxed `AsyncWrite` does not have one.
/// `OwnedWriteHalf` hands it back through `AsRef<TcpStream>`.
enum ResponseSink {
    Plain(tokio::net::tcp::OwnedWriteHalf),
    Encrypted(tokio::io::WriteHalf<Box<dyn BrokerStream>>),
}

impl ResponseSink {
    async fn write(&mut self, response: &Response) -> std::io::Result<()> {
        match self {
            ResponseSink::Plain(sink) => {
                write_chunks(sink, &response.chunks).await?;
                if response.regions.is_empty() {
                    return Ok(());
                }
                // `sendfile` writes to the socket itself, so anything still
                // buffered here has to go out first or it would arrive
                // after the payload it describes.
                sink.flush().await?;
                for region in &response.regions {
                    send_region(sink, region).await?;
                }
                Ok(())
            }
            ResponseSink::Encrypted(sink) => {
                write_chunks(sink, &response.chunks).await?;
                // Regions are only produced for the plaintext path, so this
                // is unreachable in practice; reading them keeps it correct
                // rather than relying on that.
                for region in &response.regions {
                    send_region_buffered(sink, region).await?;
                }
                Ok(())
            }
        }
    }
}

/// Send a range of a log segment straight from the page cache to the
/// socket, so the payload is never read into this process at all.
///
/// This is the point of the whole region path: a fetch response is mostly
/// record batches that nothing needs to look at, and the kernel can move
/// them without our help. Only plaintext TCP can do this — TLS and QUIC
/// must see the bytes to encrypt them, which is exactly where Kafka draws
/// the same line.
#[cfg(target_os = "linux")]
async fn send_region(
    sink: &tokio::net::tcp::OwnedWriteHalf,
    region: &LogRegion,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;

    let socket: &tokio::net::TcpStream = sink.as_ref();
    let mut offset = region.position as libc::off_t;
    let end = offset + region.len as libc::off_t;
    while offset < end {
        socket.writable().await?;
        let remaining = (end - offset) as usize;
        let attempt = socket.try_io(tokio::io::Interest::WRITABLE, || {
            // SAFETY: both descriptors are owned and stay open across the
            // call — the socket by `sink`, the segment by the `Arc<File>`
            // the region carries — and the kernel advances `offset` by
            // whatever it consumed.
            let sent = unsafe {
                libc::sendfile(
                    socket.as_raw_fd(),
                    region.file.as_raw_fd(),
                    &mut offset,
                    remaining,
                )
            };
            if sent < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(sent)
        });
        match attempt {
            Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
            Ok(_) => {}
            // The socket was not ready after all; wait again rather than
            // treating it as a failure.
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Where `sendfile` does not exist, read the range and write it. Correct
/// everywhere, just not free.
#[cfg(not(target_os = "linux"))]
async fn send_region(
    sink: &mut tokio::net::tcp::OwnedWriteHalf,
    region: &LogRegion,
) -> std::io::Result<()> {
    send_region_buffered(sink, region).await
}

async fn send_region_buffered<W>(writer: &mut W, region: &LogRegion) -> std::io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use std::io::{Read, Seek, SeekFrom};

    let mut file: &std::fs::File = &region.file;
    let mut buffer = vec![0u8; region.len];
    file.seek(SeekFrom::Start(region.position))?;
    file.read_exact(&mut buffer)?;
    writer.write_all(&buffer).await
}

/// Write a response's buffers, in one `writev` where the transport
/// supports it.
///
/// A fetch response is a small struct followed by record batches.
/// Concatenating them to satisfy a framing codec would copy every byte
/// served an extra time, which at megabyte records is the dominant cost of
/// serving a read. `write_vectored` takes the pieces as they are; partial
/// writes resume from wherever the kernel stopped.
async fn write_chunks<W>(writer: &mut W, chunks: &[Bytes]) -> std::io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut index = 0;
    let mut consumed = 0;
    while index < chunks.len() {
        let slices: Vec<IoSlice<'_>> = std::iter::once(IoSlice::new(&chunks[index][consumed..]))
            .chain(chunks[index + 1..].iter().map(|chunk| IoSlice::new(chunk)))
            .collect();
        let written = writer.write_vectored(&slices).await?;
        if written == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::WriteZero));
        }
        let mut remaining = written;
        while remaining > 0 && index < chunks.len() {
            let available = chunks[index].len() - consumed;
            if remaining >= available {
                remaining -= available;
                index += 1;
                consumed = 0;
            } else {
                consumed += remaining;
                remaining = 0;
            }
        }
    }
    Ok(())
}
/// Serve one client connection.
///
/// Requests are dispatched concurrently rather than one at a time.
/// Correlation ids already let responses return out of order, and each
/// partition is a single-writer actor, so concurrency here cannot reorder
/// a partition's appends — it only stops one slow request (a long poll, an
/// `acks=all` wait) from blocking every other request on the same socket.
/// The client's in-flight window is meaningless without this.
async fn handle_connection<R>(
    broker: Arc<Broker>,
    reader: R,
    mut sink: ResponseSink,
    peer: SocketAddr,
    // Principal proven by a verified client certificate, if the listener
    // demanded one. Binding it here rather than waiting for an
    // `Authenticate` call is the point of mTLS: the identity was settled
    // during the handshake and nothing the client sends can change it.
    peer_principal: Option<String>,
) -> Result<(), BrokerError>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let codec = LengthDelimitedCodec::builder()
        .big_endian()
        .length_field_length(4)
        .max_frame_length(broker.config.max_frame_bytes)
        .new_codec();
    // Requests still come through the framing codec; responses do not, so
    // that a fetch can be written from its own buffers — or, on the
    // plaintext path, straight out of the page cache.
    let mut stream = FramedRead::new(reader, codec);

    // Responses funnel through one writer task; the channel is bounded so a
    // client that stops reading applies backpressure instead of growing the
    // broker's memory without limit.
    let (responses_tx, mut responses_rx) =
        mpsc::channel::<Response>(broker.config.channel_capacity);
    let writer = tokio::spawn(async move {
        while let Some(response) = responses_rx.recv().await {
            if sink.write(&response).await.is_err() {
                break;
            }
        }
    });

    // Bound how many requests one connection may have in flight, so a
    // single client cannot spawn unbounded work on the broker.
    let in_flight = Arc::new(Semaphore::new(MAX_IN_FLIGHT_PER_CONNECTION));
    // One identity per connection, shared by its concurrent requests. A
    // certificate the CA signed is a stronger claim than a password sent
    // afterwards, so when the handshake produced one the connection starts
    // authenticated rather than anonymous.
    let session = Arc::new(match peer_principal {
        Some(principal) => {
            debug!(%peer, %principal, "connection authenticated by client certificate");
            handlers::ConnectionSession::authenticated(principal)
        }
        None => handlers::ConnectionSession::new(),
    });
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
        let session = Arc::clone(&session);
        requests.spawn(async move {
            let _permit = permit;
            if let Some(body) = handlers::dispatch(&broker, &header, payload, &session).await {
                let response_header = FrameHeader {
                    api_key: header.api_key,
                    api_version: header.api_version,
                    correlation_id: header.correlation_id,
                    client_id: None,
                };
                let mut chunks = Vec::with_capacity(body.chunks().len() + 1);
                chunks.push(encode_frame_prefix(&response_header, body.len()));
                chunks.extend_from_slice(body.chunks());
                let _ = responses
                    .send(Response {
                        chunks,
                        regions: body.regions().to_vec(),
                    })
                    .await;
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

impl Broker {
    /// The largest batch this topic accepts, if it sets `max.message.bytes`.
    ///
    /// `None` means only the frame limit applies, which is the broker-wide
    /// ceiling every request is already bounded by.
    pub(crate) fn max_message_bytes(&self, topic: &str) -> Option<usize> {
        let image = self.config.metadata_cache.as_ref()?.snapshot();
        let configs = &image.topics.get(topic)?.configs;
        configs
            .get("max.message.bytes")?
            .parse::<usize>()
            .ok()
            .filter(|limit| *limit > 0)
    }
}

#[cfg(test)]
mod live_topic_config_tests {
    use super::*;
    use brahmaputra_metadata::MetadataCommand;
    use std::collections::BTreeMap;

    /// A topic config change has to reach a *running* partition.
    ///
    /// Resolving the config only when a partition is opened made a live
    /// change silently inert: an operator shortens `retention.ms`, sees the
    /// new value echoed back, watches nothing happen, and cannot tell
    /// whether the setting is wrong or merely not in effect until the next
    /// restart.
    #[tokio::test]
    async fn a_topic_config_change_reaches_a_running_partition() {
        let dir = tempfile::tempdir().unwrap();

        let mut image = ClusterMetadata::default();
        image
            .apply(MetadataCommand::RegisterBroker {
                broker_id: 0,
                host: "127.0.0.1".into(),
                data_port: 9092,
                control_port: 19092,
                roles: vec![NodeRole::Broker],
                rack: None,
                now_ms: 1,
            })
            .expect("register");
        image
            .apply(MetadataCommand::CreateTopic {
                name: "cfg".into(),
                partitions: 1,
                replication_factor: 1,
                configs: BTreeMap::new(),
            })
            .expect("create");
        let cache = MetadataCache::new(image.clone());

        let broker = Broker::bind(BrokerConfig {
            port: 0,
            data_dirs: vec![dir.path().to_path_buf()],
            default_partitions: 1,
            metadata_cache: Some(cache.clone()),
            ..BrokerConfig::default()
        })
        .await
        .expect("bind");

        // Open the partition, which resolves its config once, at open time.
        let handle = broker.open_partition("cfg", 0).expect("open partition");
        handle.offsets().await.expect("partition actor is running");

        // Change the topic's retention, exactly as an operator would.
        image
            .apply(MetadataCommand::SetTopicConfig {
                name: "cfg".into(),
                configs: BTreeMap::from([("retention.ms".to_string(), "60000".to_string())]),
            })
            .expect("set config");
        cache.replace(image.clone());

        broker.apply_topic_config_changes(&cache.snapshot()).await;

        let applied = broker
            .applied_topic_configs
            .get(&("cfg".to_string(), 0))
            .map(|entry| entry.value().clone())
            .expect("the partition should have been told a config");
        assert_eq!(
            applied.retention_ms,
            Some(60_000),
            "the running partition never received the changed retention"
        );

        // A second sweep with nothing changed must be a no-op rather than
        // re-sending the same config on every tick.
        broker.apply_topic_config_changes(&cache.snapshot()).await;
        let again = broker
            .applied_topic_configs
            .get(&("cfg".to_string(), 0))
            .map(|entry| entry.value().clone())
            .expect("still tracked");
        assert_eq!(applied, again);
    }
}
