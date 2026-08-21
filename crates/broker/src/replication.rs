//! Autonomous follower replication and leader-side progress tracking.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use brahmaputra_client::{ReplicaClient, Transport};
use brahmaputra_metadata::{ClusterMetadata, NodeRole, PartitionMetadata};
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::replica::{OffsetsForLeaderEpochRequest, ReplicaFetchRequest};
use dashmap::DashMap;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::warn;

use crate::{Broker, PartitionHandle};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ReplicaPartition {
    pub topic: String,
    pub partition: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderFollowerHealth {
    pub topic: String,
    pub partition: i32,
    pub leader_epoch: i32,
    pub follower_id: i32,
    pub follower_broker_epoch: u64,
    pub fetch_offset: i64,
    pub last_fetch_ms: i64,
    pub in_sync: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetcherState {
    Connecting,
    Reconciling,
    Fetching,
    Backoff,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowerFetcherHealth {
    pub topic: String,
    pub partition: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub local_log_end: i64,
    pub high_watermark: i64,
    pub last_fetch_ms: i64,
    pub state: FetcherState,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicationHealthSnapshot {
    /// Progress reported to this broker by followers of its leader replicas.
    pub leader_followers: Vec<LeaderFollowerHealth>,
    /// Fetch loops running locally for partitions led by another broker.
    pub follower_fetchers: Vec<FollowerFetcherHealth>,
}

#[derive(Default)]
pub(crate) struct ReplicationTracker {
    leader_followers: DashMap<(String, i32, i32), LeaderFollowerHealth>,
    follower_fetchers: DashMap<(String, i32), FollowerFetcherHealth>,
}

impl ReplicationTracker {
    pub(crate) async fn observe_leader_fetch(
        &self,
        topic: &str,
        assignment: &PartitionMetadata,
        follower_id: i32,
        follower_broker_epoch: u64,
        fetch_offset: i64,
        handle: &PartitionHandle,
    ) -> Result<(), brahmaputra_storage::StorageError> {
        let (_, log_end, _) = handle.offsets().await?;
        let observed_offset = fetch_offset.min(log_end);
        self.leader_followers
            .retain(|(entry_topic, entry_partition, _), progress| {
                entry_topic != topic
                    || *entry_partition != assignment.partition
                    || progress.leader_epoch == assignment.leader_epoch
            });
        self.leader_followers.insert(
            (topic.to_owned(), assignment.partition, follower_id),
            LeaderFollowerHealth {
                topic: topic.to_owned(),
                partition: assignment.partition,
                leader_epoch: assignment.leader_epoch,
                follower_id,
                follower_broker_epoch,
                fetch_offset: observed_offset,
                last_fetch_ms: now_ms(),
                in_sync: assignment.isr.contains(&follower_id),
            },
        );

        self.advance_leader_high_watermark(topic, assignment, handle)
            .await
    }

    pub(crate) async fn advance_leader_high_watermark(
        &self,
        topic: &str,
        assignment: &PartitionMetadata,
        handle: &PartitionHandle,
    ) -> Result<(), brahmaputra_storage::StorageError> {
        let (_, log_end, high_watermark) = handle.offsets().await?;
        // Missing progress for a current ISR member is conservatively the
        // already committed HWM. Thus membership changes never regress HWM
        // and cannot advance it until every ISR member has fetched farther.
        let mut candidate = log_end;
        for replica_id in &assignment.isr {
            if *replica_id == assignment.leader {
                continue;
            }
            let progress = self
                .leader_followers
                .get(&(topic.to_owned(), assignment.partition, *replica_id))
                .filter(|progress| progress.leader_epoch == assignment.leader_epoch)
                .map_or(high_watermark, |progress| progress.fetch_offset);
            candidate = candidate.min(progress);
        }
        if candidate > high_watermark {
            handle.update_high_watermark(candidate).await?;
        }
        Ok(())
    }

    fn set_fetcher(&self, health: FollowerFetcherHealth) {
        self.follower_fetchers
            .insert((health.topic.clone(), health.partition), health);
    }

    fn update_fetcher(
        &self,
        spec: &WorkerSpec,
        state: FetcherState,
        offsets: Option<(i64, i64)>,
        fetched: bool,
        last_error: Option<String>,
    ) {
        let key = (spec.key.topic.clone(), spec.key.partition);
        let previous = self.follower_fetchers.get(&key).map(|value| value.clone());
        let (local_log_end, high_watermark) = offsets.unwrap_or_else(|| {
            previous
                .as_ref()
                .map_or((0, 0), |value| (value.local_log_end, value.high_watermark))
        });
        let last_fetch_ms = if fetched {
            now_ms()
        } else {
            previous.as_ref().map_or(0, |value| value.last_fetch_ms)
        };
        self.set_fetcher(FollowerFetcherHealth {
            topic: spec.key.topic.clone(),
            partition: spec.key.partition,
            leader_id: spec.leader_id,
            leader_epoch: spec.leader_epoch,
            local_log_end,
            high_watermark,
            last_fetch_ms,
            state,
            last_error,
        });
    }

    fn remove_fetcher(&self, key: &ReplicaPartition) {
        self.follower_fetchers
            .remove(&(key.topic.clone(), key.partition));
    }

    pub(crate) fn reconcile_metadata(&self, image: &ClusterMetadata, local_id: i32) {
        let local_is_broker = image
            .brokers
            .get(&local_id)
            .is_some_and(|broker| broker.alive && broker.roles.contains(&NodeRole::Broker));
        self.leader_followers
            .retain(|(topic, partition, follower_id), progress| {
                let Some(assignment) = image
                    .topics
                    .get(topic)
                    .and_then(|metadata| metadata.partitions.get(partition))
                else {
                    return false;
                };
                let keep = local_is_broker
                    && assignment.leader == local_id
                    && assignment.leader_epoch == progress.leader_epoch
                    && assignment.replicas.contains(follower_id);
                let current_broker_epoch = image
                    .brokers
                    .get(follower_id)
                    .filter(|broker| broker.alive && broker.roles.contains(&NodeRole::Broker))
                    .map(|broker| broker.broker_epoch);
                let keep = keep && current_broker_epoch == Some(progress.follower_broker_epoch);
                if keep {
                    progress.in_sync = assignment.isr.contains(follower_id);
                }
                keep
            });
    }

    pub(crate) fn snapshot(&self) -> ReplicationHealthSnapshot {
        let mut leader_followers = self
            .leader_followers
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        leader_followers.sort_by(|left, right| {
            (&left.topic, left.partition, left.follower_id).cmp(&(
                &right.topic,
                right.partition,
                right.follower_id,
            ))
        });
        let mut follower_fetchers = self
            .follower_fetchers
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        follower_fetchers.sort_by(|left, right| {
            (&left.topic, left.partition).cmp(&(&right.topic, right.partition))
        });
        ReplicationHealthSnapshot {
            leader_followers,
            follower_fetchers,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaManagerConfig {
    pub metadata_poll_interval: Duration,
    pub idle_fetch_interval: Duration,
    pub retry_backoff: Duration,
    pub max_fetch_bytes: i32,
    pub max_in_flight: usize,
    /// Transport for follower fetches; matches the broker's listener.
    pub transport: Transport,
}

impl Default for ReplicaManagerConfig {
    fn default() -> Self {
        Self {
            metadata_poll_interval: Duration::from_millis(200),
            idle_fetch_interval: Duration::from_millis(50),
            retry_backoff: Duration::from_millis(200),
            max_fetch_bytes: 4 * 1024 * 1024,
            max_in_flight: 4,
            transport: Transport::default(),
        }
    }
}

/// Supervises one cancellable fetch loop per locally assigned follower.
pub struct ReplicaManager {
    broker: Arc<Broker>,
    broker_epoch: watch::Receiver<u64>,
    config: ReplicaManagerConfig,
}

impl ReplicaManager {
    pub fn new(
        broker: Arc<Broker>,
        broker_epoch: watch::Receiver<u64>,
        config: ReplicaManagerConfig,
    ) -> Self {
        Self {
            broker,
            broker_epoch,
            config,
        }
    }

    /// Poll controller-materialized metadata until shutdown, starting,
    /// replacing, and cancelling follower workers as assignments change.
    pub async fn run(self, shutdown: impl Future<Output = ()>) {
        let mut shutdown = std::pin::pin!(shutdown);
        let mut workers: BTreeMap<ReplicaPartition, Worker> = BTreeMap::new();
        let interval = self
            .config
            .metadata_poll_interval
            .max(Duration::from_millis(1));
        let mut tick = tokio::time::interval_at(Instant::now(), interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                _ = tick.tick() => {
                    self.reconcile_workers(&mut workers).await;
                    // A partition dropped from this broker's replica set by a
                    // completed reassignment still has its data here; free it,
                    // or a drained broker never gives its disk back.
                    if let Some(cache) = self.broker.metadata_cache() {
                        self.broker.drain_unowned_partitions(&cache.snapshot());
                    }
                }
            }
        }
        for (_, worker) in workers {
            stop_worker(worker).await;
        }
        self.broker.replication_tracker().follower_fetchers.clear();
    }

    async fn reconcile_workers(&self, workers: &mut BTreeMap<ReplicaPartition, Worker>) {
        let desired = self.desired_workers();
        let obsolete = workers
            .iter()
            .filter(|(key, worker)| {
                worker.task.is_finished() || desired.get(*key) != Some(&worker.spec)
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in obsolete {
            if let Some(worker) = workers.remove(&key) {
                stop_worker(worker).await;
            }
            self.broker.replication_tracker().remove_fetcher(&key);
        }
        for (key, spec) in desired {
            if workers.contains_key(&key) {
                continue;
            }
            let (cancel, cancel_rx) = watch::channel(false);
            self.broker.replication_tracker().update_fetcher(
                &spec,
                FetcherState::Connecting,
                None,
                false,
                None,
            );
            let broker = Arc::clone(&self.broker);
            let config = self.config.clone();
            let running_spec = spec.clone();
            let task = tokio::spawn(async move {
                follower_worker(broker, running_spec, config, cancel_rx).await;
            });
            workers.insert(key, Worker { spec, cancel, task });
        }
    }

    fn desired_workers(&self) -> BTreeMap<ReplicaPartition, WorkerSpec> {
        if !self.broker.config().replication_enabled {
            return BTreeMap::new();
        }
        let Some(cache) = self.broker.metadata_cache() else {
            return BTreeMap::new();
        };
        let image = cache.snapshot();
        let local_id = self.broker.config().broker_id;
        let broker_epoch = *self.broker_epoch.borrow();
        self.broker
            .replication_tracker()
            .reconcile_metadata(&image, local_id);
        let local_registered = broker_epoch != 0
            && image.brokers.get(&local_id).is_some_and(|broker| {
                broker.alive
                    && broker.broker_epoch == broker_epoch
                    && broker.roles.contains(&NodeRole::Broker)
            });
        if !local_registered {
            return BTreeMap::new();
        }
        let mut desired = BTreeMap::new();
        for topic in image.topics.values() {
            for partition in topic.partitions.values() {
                if partition.leader == local_id || !partition.replicas.contains(&local_id) {
                    continue;
                }
                let Some(leader) = image
                    .brokers
                    .get(&partition.leader)
                    .filter(|broker| broker.alive && broker.roles.contains(&NodeRole::Broker))
                else {
                    continue;
                };
                let key = ReplicaPartition {
                    topic: topic.name.clone(),
                    partition: partition.partition,
                };
                desired.insert(
                    key.clone(),
                    WorkerSpec {
                        key,
                        leader_id: leader.broker_id,
                        leader_epoch: partition.leader_epoch,
                        leader_host: leader.host.clone(),
                        leader_port: leader.data_port,
                        follower_id: local_id,
                        follower_broker_epoch: broker_epoch,
                    },
                );
            }
        }
        desired
    }
}

struct Worker {
    spec: WorkerSpec,
    cancel: watch::Sender<bool>,
    task: JoinHandle<()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkerSpec {
    key: ReplicaPartition,
    leader_id: i32,
    leader_epoch: i32,
    leader_host: String,
    leader_port: u16,
    follower_id: i32,
    follower_broker_epoch: u64,
}

async fn stop_worker(worker: Worker) {
    let _ = worker.cancel.send(true);
    let _ = worker.task.await;
}

async fn follower_worker(
    broker: Arc<Broker>,
    spec: WorkerSpec,
    config: ReplicaManagerConfig,
    mut cancel: watch::Receiver<bool>,
) {
    loop {
        if *cancel.borrow() {
            return;
        }
        broker.replication_tracker().update_fetcher(
            &spec,
            FetcherState::Connecting,
            None,
            false,
            None,
        );
        let address = match cancellable(&mut cancel, resolve(&spec)).await {
            Some(Ok(address)) => address,
            Some(Err(error)) => {
                backoff(&broker, &spec, &config, &mut cancel, error).await;
                continue;
            }
            None => return,
        };
        let client = match cancellable(
            &mut cancel,
            ReplicaClient::connect_with(
                config.transport,
                address,
                format!("replica-{}", spec.follower_id),
                config.max_in_flight,
            ),
        )
        .await
        {
            Some(Ok(client)) => client,
            Some(Err(error)) => {
                backoff(&broker, &spec, &config, &mut cancel, error.to_string()).await;
                continue;
            }
            None => return,
        };
        let handle = match broker.replica_partition(&spec.key.topic, spec.key.partition) {
            Ok(handle) => handle,
            Err(error) => {
                backoff(&broker, &spec, &config, &mut cancel, error.to_string()).await;
                continue;
            }
        };
        broker.replication_tracker().update_fetcher(
            &spec,
            FetcherState::Reconciling,
            None,
            false,
            None,
        );
        match reconcile_log(&client, &handle, &spec, &mut cancel).await {
            Reconcile::Ready => {}
            Reconcile::Cancelled | Reconcile::MetadataStale => return,
            Reconcile::Retry(error) => {
                backoff(&broker, &spec, &config, &mut cancel, error).await;
                continue;
            }
        }

        loop {
            if *cancel.borrow() {
                return;
            }
            let (_, fetch_offset, high_watermark) = match handle.offsets().await {
                Ok(offsets) => offsets,
                Err(error) => {
                    backoff(&broker, &spec, &config, &mut cancel, error.to_string()).await;
                    break;
                }
            };
            broker.replication_tracker().update_fetcher(
                &spec,
                FetcherState::Fetching,
                Some((fetch_offset, high_watermark)),
                false,
                None,
            );
            let request = ReplicaFetchRequest {
                topic: spec.key.topic.clone(),
                partition: spec.key.partition,
                follower_id: spec.follower_id,
                follower_broker_epoch: spec.follower_broker_epoch,
                leader_epoch: spec.leader_epoch,
                fetch_offset,
                max_bytes: config.max_fetch_bytes.max(1),
            };
            let result = match cancellable(&mut cancel, client.fetch_raw(&request)).await {
                Some(Ok(result)) => result,
                Some(Err(error)) => {
                    backoff(&broker, &spec, &config, &mut cancel, error.to_string()).await;
                    break;
                }
                None => return,
            };
            match result.response.error_code {
                ec::NONE => {}
                ec::OFFSET_OUT_OF_RANGE => {
                    let leader_start = result.response.log_start_offset;
                    let leader_end = result.response.log_end_offset;
                    let (_, local_end, local_hwm) =
                        handle.offsets().await.unwrap_or((0, fetch_offset, 0));
                    if leader_start >= 0
                        && leader_start <= leader_end
                        && local_end < leader_start
                        && local_hwm <= local_end
                    {
                        match handle.reset_to_offset(leader_start).await {
                            Ok(()) => {
                                broker.replication_tracker().update_fetcher(
                                    &spec,
                                    FetcherState::Reconciling,
                                    Some((leader_start, leader_start)),
                                    false,
                                    None,
                                );
                                continue;
                            }
                            Err(error) => {
                                backoff(&broker, &spec, &config, &mut cancel, error.to_string())
                                    .await;
                                break;
                            }
                        }
                    }
                    backoff(
                        &broker,
                        &spec,
                        &config,
                        &mut cancel,
                        format!(
                            "cannot rebase follower [{local_hwm}, {local_end}) to leader range [{leader_start}, {leader_end})"
                        ),
                    )
                    .await;
                    break;
                }
                ec::FENCED_BROKER_EPOCH
                | ec::FENCED_LEADER_EPOCH
                | ec::UNKNOWN_LEADER_EPOCH
                | ec::NOT_LEADER_OR_FOLLOWER => return,
                code => {
                    backoff(
                        &broker,
                        &spec,
                        &config,
                        &mut cancel,
                        format!("replica fetch failed with error code {code}"),
                    )
                    .await;
                    break;
                }
            }
            let had_batches = !result.batches.is_empty();
            let mut append_failed = None;
            for batch in result.batches {
                if let Err(error) = handle.append_replica_batch(batch).await {
                    append_failed = Some(error.to_string());
                    break;
                }
            }
            if let Some(error) = append_failed {
                backoff(&broker, &spec, &config, &mut cancel, error).await;
                break;
            }
            let (_, local_end, local_hwm) = match handle.offsets().await {
                Ok(offsets) => offsets,
                Err(error) => {
                    backoff(&broker, &spec, &config, &mut cancel, error.to_string()).await;
                    break;
                }
            };
            let leader_hwm = result.response.high_watermark.min(local_end);
            if leader_hwm > local_hwm {
                if let Err(error) = handle.update_high_watermark(leader_hwm).await {
                    backoff(&broker, &spec, &config, &mut cancel, error.to_string()).await;
                    break;
                }
            }
            let (_, local_end, local_hwm) = handle.offsets().await.unwrap_or((0, 0, 0));
            broker.replication_tracker().update_fetcher(
                &spec,
                FetcherState::Fetching,
                Some((local_end, local_hwm)),
                true,
                None,
            );
            if !had_batches
                && cancellable(&mut cancel, tokio::time::sleep(config.idle_fetch_interval))
                    .await
                    .is_none()
            {
                return;
            }
        }
    }
}

enum Reconcile {
    Ready,
    Retry(String),
    MetadataStale,
    Cancelled,
}

async fn reconcile_log(
    client: &ReplicaClient,
    handle: &PartitionHandle,
    spec: &WorkerSpec,
    cancel: &mut watch::Receiver<bool>,
) -> Reconcile {
    let entries = match handle.leader_epoch_entries().await {
        Ok(entries) => entries,
        Err(error) => return Reconcile::Retry(error.to_string()),
    };
    let (_, local_end, high_watermark) = match handle.offsets().await {
        Ok(offsets) => offsets,
        Err(error) => return Reconcile::Retry(error.to_string()),
    };
    let mut common_end = None;
    for entry in entries.iter().rev() {
        let request = OffsetsForLeaderEpochRequest {
            topic: spec.key.topic.clone(),
            partition: spec.key.partition,
            follower_id: spec.follower_id,
            follower_broker_epoch: spec.follower_broker_epoch,
            leader_epoch: spec.leader_epoch,
            query_leader_epoch: entry.epoch,
        };
        let response = match cancellable(cancel, client.offsets_for_leader_epoch(&request)).await {
            Some(Ok(response)) => response,
            Some(Err(error)) => return Reconcile::Retry(error.to_string()),
            None => return Reconcile::Cancelled,
        };
        match response.error_code {
            ec::NONE => {
                common_end = Some(response.end_offset.min(local_end));
                break;
            }
            ec::UNKNOWN_LEADER_EPOCH => continue,
            ec::FENCED_BROKER_EPOCH | ec::FENCED_LEADER_EPOCH | ec::NOT_LEADER_OR_FOLLOWER => {
                return Reconcile::MetadataStale
            }
            code => {
                return Reconcile::Retry(format!(
                    "offsets-for-leader-epoch failed with error code {code}"
                ));
            }
        }
    }

    // With no shared checkpoint, retain committed data and discard only an
    // unverifiable uncommitted suffix. A controller must resolve any true
    // committed-history conflict.
    let target = common_end.unwrap_or(high_watermark);
    if target < high_watermark {
        return Reconcile::Retry(format!(
            "leader common prefix {target} is below follower HWM {high_watermark}"
        ));
    }
    if target < local_end {
        if let Err(error) = handle.truncate_to(target).await {
            return Reconcile::Retry(error.to_string());
        }
    }
    Reconcile::Ready
}

async fn resolve(spec: &WorkerSpec) -> Result<SocketAddr, String> {
    tokio::net::lookup_host((spec.leader_host.as_str(), spec.leader_port))
        .await
        .map_err(|error| error.to_string())?
        .next()
        .ok_or_else(|| "leader address resolved to no socket".to_owned())
}

async fn backoff(
    broker: &Broker,
    spec: &WorkerSpec,
    config: &ReplicaManagerConfig,
    cancel: &mut watch::Receiver<bool>,
    error: String,
) {
    warn!(
        topic = %spec.key.topic,
        partition = spec.key.partition,
        leader = spec.leader_id,
        %error,
        "replica fetcher backing off"
    );
    broker.replication_tracker().update_fetcher(
        spec,
        FetcherState::Backoff,
        None,
        false,
        Some(error),
    );
    let _ = cancellable(cancel, tokio::time::sleep(config.retry_backoff)).await;
}

async fn cancellable<T>(
    cancel: &mut watch::Receiver<bool>,
    future: impl Future<Output = T>,
) -> Option<T> {
    if *cancel.borrow() {
        return None;
    }
    tokio::select! {
        result = future => Some(result),
        changed = cancel.changed() => {
            let _ = changed;
            None
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}
