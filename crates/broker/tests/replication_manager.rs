use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use brahmaputra_broker::{Broker, BrokerConfig, ReplicaManager, ReplicaManagerConfig};
use brahmaputra_client::Connection;
use brahmaputra_metadata::{
    BrokerMetadata, ClusterMetadata, MetadataCache, NodeRole, PartitionMetadata, TopicMetadata,
};
use brahmaputra_protocol::codec;
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{ProduceRequest, ProduceResponse};
use brahmaputra_protocol::{ApiKey, Record, RecordBatch};
use brahmaputra_storage::{Log, LogConfig};
use tempfile::TempDir;
use tokio::sync::watch;
use tokio::task::JoinHandle;

const TOPIC: &str = "replication-test";

fn broker_metadata(broker_id: i32) -> BrokerMetadata {
    BrokerMetadata {
        broker_id,
        host: "127.0.0.1".into(),
        data_port: 0,
        control_port: 0,
        broker_epoch: broker_epoch(broker_id),
        roles: BTreeSet::from([NodeRole::Broker]),
        rack: None,
        alive: true,
        last_heartbeat_ms: 1_000,
    }
}

fn broker_epoch(broker_id: i32) -> u64 {
    100 + broker_id as u64
}

fn cluster_image(
    broker_ids: &[i32],
    replicas: Vec<i32>,
    isr: Vec<i32>,
    leader: i32,
    leader_epoch: i32,
    configs: BTreeMap<String, String>,
) -> ClusterMetadata {
    ClusterMetadata {
        cluster_id: "replication-manager-test".into(),
        offset: 1,
        controller_id: Some(leader),
        brokers: broker_ids
            .iter()
            .map(|broker_id| (*broker_id, broker_metadata(*broker_id)))
            .collect(),
        topics: BTreeMap::from([(
            TOPIC.into(),
            TopicMetadata {
                name: TOPIC.into(),
                replication_factor: replicas.len() as i32,
                partitions: BTreeMap::from([(
                    0,
                    PartitionMetadata {
                        partition: 0,
                        replicas,
                        leader,
                        isr,
                        leader_epoch,
                        target_replicas: None,
                    },
                )]),
                configs,
            },
        )]),
        users: BTreeMap::new(),
        acls: Default::default(),
        quotas: Default::default(),
        jwt_secret: None,
    }
}

struct ClusterHarness {
    _temp: TempDir,
    cache: MetadataCache,
    brokers: BTreeMap<i32, Arc<Broker>>,
    shutdown: watch::Sender<bool>,
    broker_tasks: Vec<JoinHandle<()>>,
    manager_tasks: Vec<JoinHandle<()>>,
    epoch_senders: BTreeMap<i32, watch::Sender<u64>>,
}

impl ClusterHarness {
    async fn start(temp: TempDir, image: ClusterMetadata) -> Self {
        let cache = MetadataCache::new(image);
        let broker_ids = cache.snapshot().brokers.keys().copied().collect::<Vec<_>>();
        let mut brokers = BTreeMap::new();
        for broker_id in broker_ids {
            let broker = Arc::new(
                Broker::bind(BrokerConfig {
                    broker_id,
                    broker_epoch: Some(broker_epoch(broker_id)),
                    port: 0,
                    data_dirs: vec![temp.path().join(format!("broker-{broker_id}"))],
                    metadata_cache: Some(cache.clone()),
                    replication_enabled: true,
                    ..BrokerConfig::default()
                })
                .await
                .unwrap(),
            );
            brokers.insert(broker_id, broker);
        }

        let mut advertised = cache.snapshot().as_ref().clone();
        for (broker_id, broker) in &brokers {
            advertised.brokers.get_mut(broker_id).unwrap().data_port = broker.local_addr().port();
        }
        advertised.offset += 1;
        cache.replace(advertised);

        let (shutdown, _) = watch::channel(false);
        let mut broker_tasks = Vec::new();
        for broker in brokers.values() {
            let serving = Arc::clone(broker);
            let shutdown_rx = shutdown.subscribe();
            broker_tasks.push(tokio::spawn(async move {
                serving.run(shutdown_signal(shutdown_rx)).await.unwrap();
            }));
        }
        Self {
            _temp: temp,
            cache,
            brokers,
            shutdown,
            broker_tasks,
            manager_tasks: Vec::new(),
            epoch_senders: BTreeMap::new(),
        }
    }

    fn broker(&self, broker_id: i32) -> Arc<Broker> {
        Arc::clone(&self.brokers[&broker_id])
    }

    fn start_manager(&mut self, broker_id: i32, initial_epoch: u64) {
        let (epoch_tx, epoch_rx) = watch::channel(initial_epoch);
        let manager = ReplicaManager::new(
            self.broker(broker_id),
            epoch_rx,
            ReplicaManagerConfig {
                metadata_poll_interval: Duration::from_millis(10),
                idle_fetch_interval: Duration::from_millis(5),
                retry_backoff: Duration::from_millis(10),
                max_fetch_bytes: 1024 * 1024,
                max_in_flight: 2,
                ..ReplicaManagerConfig::default()
            },
        );
        let shutdown_rx = self.shutdown.subscribe();
        self.manager_tasks.push(tokio::spawn(async move {
            manager.run(shutdown_signal(shutdown_rx)).await;
        }));
        self.epoch_senders.insert(broker_id, epoch_tx);
    }

    async fn stop(self) {
        let _ = self.shutdown.send(true);
        for task in self.manager_tasks {
            task.await.unwrap();
        }
        for task in self.broker_tasks {
            task.await.unwrap();
        }
    }
}

async fn shutdown_signal(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
}

async fn produce(
    address: std::net::SocketAddr,
    acks: i32,
    timeout_ms: i32,
    value: &'static str,
) -> ProduceResponse {
    let connection = Connection::connect(address, Some(format!("produce-{value}")), 4)
        .await
        .unwrap();
    let request = ProduceRequest {
        topic: TOPIC.into(),
        partition: 0,
        acks,
        timeout_ms,
        ..Default::default()
    };
    let batch = RecordBatch::new(0, 0, 1_000, vec![Record::new(value)]).encode();
    let body = codec::encode_produce_request(&request, &[batch]).unwrap();
    let response = connection.request(ApiKey::Produce, &body).await.unwrap();
    ProduceResponse::decode(&response).unwrap()
}

async fn produce_batch(address: std::net::SocketAddr, batch: RecordBatch) -> ProduceResponse {
    let connection = Connection::connect(address, Some("idempotent-promotion".into()), 2)
        .await
        .unwrap();
    let request = ProduceRequest {
        topic: TOPIC.into(),
        partition: 0,
        acks: 1,
        timeout_ms: 5_000,
        ..Default::default()
    };
    let body = codec::encode_produce_request(&request, &[batch.encode()]).unwrap();
    let response = connection.request(ApiKey::Produce, &body).await.unwrap();
    ProduceResponse::decode(&response).unwrap()
}

async fn wait_for_offsets(broker: &Broker, log_end: i64, high_watermark: i64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let handle = broker.replica_partition(TOPIC, 0).unwrap();
            let (_, actual_end, actual_hwm) = handle.offsets().await.unwrap();
            if (actual_end, actual_hwm) == (log_end, high_watermark) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replica offsets did not converge");
}

async fn raw_batches(broker: &Broker) -> Vec<bytes::Bytes> {
    broker
        .replica_partition(TOPIC, 0)
        .unwrap()
        .read_uncommitted(0, usize::MAX)
        .await
        .unwrap()
        .batches
}

#[tokio::test]
async fn rf3_replication_preserves_bytes_gates_hwm_and_completes_acks_all() {
    let temp = tempfile::tempdir().unwrap();
    let image = cluster_image(
        &[1, 2, 3],
        vec![1, 2, 3],
        vec![1, 2, 3],
        1,
        7,
        BTreeMap::new(),
    );
    let mut cluster = ClusterHarness::start(temp, image).await;
    let leader = cluster.broker(1);

    let first = produce(leader.local_addr(), 1, 1_000, "acks-one").await;
    assert_eq!(first.error_code, ec::NONE);
    let leader_handle = leader.partition(TOPIC, 0).unwrap();
    assert_eq!(leader_handle.offsets().await.unwrap(), (0, 1, 0));
    assert!(leader_handle
        .read(0, usize::MAX)
        .await
        .unwrap()
        .batches
        .is_empty());

    let address = leader.local_addr();
    let pending_all = tokio::spawn(async move { produce(address, -1, 5_000, "acks-all").await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!pending_all.is_finished(), "acks=all must wait below HWM");
    assert_eq!(leader_handle.offsets().await.unwrap(), (0, 2, 0));

    cluster.start_manager(2, broker_epoch(2));
    cluster.start_manager(3, broker_epoch(3));
    let response = tokio::time::timeout(Duration::from_secs(5), pending_all)
        .await
        .expect("acks=all did not complete")
        .unwrap();
    assert_eq!(response.error_code, ec::NONE);
    assert_eq!(response.base_offset, 1);

    for broker_id in [1, 2, 3] {
        wait_for_offsets(&cluster.brokers[&broker_id], 2, 2).await;
    }
    let leader_bytes = raw_batches(&leader).await;
    assert_eq!(raw_batches(&cluster.brokers[&2]).await, leader_bytes);
    assert_eq!(raw_batches(&cluster.brokers[&3]).await, leader_bytes);
    let health = leader.replication_health();
    assert_eq!(health.leader_followers.len(), 2);
    assert!(health
        .leader_followers
        .iter()
        .all(|progress| progress.fetch_offset == 2 && progress.in_sync));

    drop(leader_handle);
    drop(leader);
    cluster.stop().await;
}

#[tokio::test]
async fn acks_all_rejects_below_default_min_isr_without_appending() {
    let temp = tempfile::tempdir().unwrap();
    let image = cluster_image(&[1, 2, 3], vec![1, 2, 3], vec![1], 1, 2, BTreeMap::new());
    let cluster = ClusterHarness::start(temp, image).await;
    let leader = cluster.broker(1);
    let invalid = produce(leader.local_addr(), 2, 1_000, "invalid-acks").await;
    assert_eq!(invalid.error_code, ec::INVALID_REQUEST);
    let response = produce(leader.local_addr(), -1, 1_000, "must-reject").await;
    assert_eq!(response.error_code, ec::NOT_ENOUGH_REPLICAS);
    assert_eq!(response.base_offset, -1);
    let handle = leader.partition(TOPIC, 0).unwrap();
    assert_eq!(handle.offsets().await.unwrap(), (0, 0, 0));

    drop(handle);
    drop(leader);
    cluster.stop().await;
}

#[tokio::test]
async fn rf1_cluster_advances_hwm_from_local_leo_without_a_fetcher() {
    let temp = tempfile::tempdir().unwrap();
    let image = cluster_image(&[1], vec![1], vec![1], 1, 4, BTreeMap::new());
    let cluster = ClusterHarness::start(temp, image).await;
    let leader = cluster.broker(1);
    let response = produce(leader.local_addr(), -1, 1_000, "rf-one").await;
    assert_eq!(response.error_code, ec::NONE);
    assert_eq!(response.base_offset, 0);
    let handle = leader.partition(TOPIC, 0).unwrap();
    assert_eq!(handle.offsets().await.unwrap(), (0, 1, 1));
    assert_eq!(handle.read(0, usize::MAX).await.unwrap().batches.len(), 1);

    drop(handle);
    drop(leader);
    cluster.stop().await;
}

#[tokio::test]
async fn promoted_follower_deduplicates_from_replicated_magic_v2_log() {
    let temp = tempfile::tempdir().unwrap();
    let image = cluster_image(&[1, 2], vec![1, 2], vec![1, 2], 1, 4, BTreeMap::new());
    let mut cluster = ClusterHarness::start(temp, image).await;
    cluster.start_manager(2, broker_epoch(2));

    let original = RecordBatch::new(0, 0, 1_000, vec![Record::new("once")]).with_producer(77, 0, 0);
    let response = produce_batch(cluster.brokers[&1].local_addr(), original.clone()).await;
    assert_eq!((response.error_code, response.base_offset), (ec::NONE, 0));
    wait_for_offsets(&cluster.brokers[&1], 1, 1).await;
    wait_for_offsets(&cluster.brokers[&2], 1, 1).await;
    assert_eq!(
        raw_batches(&cluster.brokers[&1]).await,
        raw_batches(&cluster.brokers[&2]).await
    );

    let mut promoted = cluster.cache.snapshot().as_ref().clone();
    let assignment = promoted
        .topics
        .get_mut(TOPIC)
        .unwrap()
        .partitions
        .get_mut(&0)
        .unwrap();
    assignment.leader = 2;
    assignment.isr = vec![2];
    assignment.leader_epoch += 1;
    promoted.offset += 1;
    cluster.cache.replace(promoted);
    let assignment = cluster.cache.snapshot().topics[TOPIC].partitions[&0].clone();
    cluster.brokers[&2]
        .reconcile_leader_partition(TOPIC, &assignment)
        .await
        .unwrap();

    let duplicate = produce_batch(cluster.brokers[&2].local_addr(), original).await;
    assert_eq!((duplicate.error_code, duplicate.base_offset), (ec::NONE, 0));
    assert_eq!(
        cluster.brokers[&2]
            .partition(TOPIC, 0)
            .unwrap()
            .offsets()
            .await
            .unwrap(),
        (0, 1, 1)
    );
    let next =
        RecordBatch::new(0, 0, 2_000, vec![Record::new("after-promotion")]).with_producer(77, 0, 1);
    let response = produce_batch(cluster.brokers[&2].local_addr(), next).await;
    assert_eq!((response.error_code, response.base_offset), (ec::NONE, 1));
    wait_for_offsets(&cluster.brokers[&2], 2, 2).await;
    cluster.stop().await;
}

#[tokio::test]
async fn promoted_sole_isr_reconciles_stale_hwm_before_new_traffic() {
    let temp = tempfile::tempdir().unwrap();
    let log_dir = temp.path().join("broker-2").join(format!("{TOPIC}-0"));
    let mut log = Log::open(log_dir, LogConfig::default()).unwrap();
    log.record_leader_epoch(1).unwrap();
    log.append_replica_batch(
        RecordBatch::new(0, 1, 1_000, vec![Record::new("committed-old")]).encode(),
    )
    .unwrap();
    log.record_leader_epoch(2).unwrap();
    log.append_replica_batch(
        RecordBatch::new(1, 2, 2_000, vec![Record::new("fetched-before-failover")]).encode(),
    )
    .unwrap();
    log.set_high_watermark(1).unwrap();
    drop(log);

    let image = cluster_image(&[2], vec![2], vec![2], 2, 3, BTreeMap::new());
    let cluster = ClusterHarness::start(temp, image).await;
    let promoted = cluster.broker(2);
    let assignment = cluster.cache.snapshot().topics[TOPIC].partitions[&0].clone();
    promoted
        .reconcile_leader_partition(TOPIC, &assignment)
        .await
        .unwrap();

    let handle = promoted.partition(TOPIC, 0).unwrap();
    assert_eq!(handle.offsets().await.unwrap(), (0, 2, 2));
    assert_eq!(handle.read(0, usize::MAX).await.unwrap().batches.len(), 2);
    let entries = handle.leader_epoch_entries().await.unwrap();
    assert_eq!(
        entries
            .last()
            .map(|entry| (entry.epoch, entry.start_offset)),
        Some((3, 2))
    );

    drop(handle);
    drop(promoted);
    cluster.stop().await;
}

fn seed_divergent_logs(temp: &TempDir) -> (Vec<bytes::Bytes>, Vec<bytes::Bytes>) {
    let leader_dir = temp.path().join("broker-1").join(format!("{TOPIC}-0"));
    let follower_dir = temp.path().join("broker-2").join(format!("{TOPIC}-0"));
    let shared = RecordBatch::new(0, 1, 1_000, vec![Record::new("shared")]).encode();
    let leader_new = RecordBatch::new(1, 2, 2_000, vec![Record::new("leader-new")]).encode();
    let follower_old = RecordBatch::new(1, 1, 2_000, vec![Record::new("divergent-old")]).encode();

    let mut leader = Log::open(leader_dir, LogConfig::default()).unwrap();
    leader.record_leader_epoch(1).unwrap();
    leader.append_replica_batch(shared.clone()).unwrap();
    leader.record_leader_epoch(2).unwrap();
    leader.append_replica_batch(leader_new.clone()).unwrap();
    leader.set_high_watermark(1).unwrap();

    let mut follower = Log::open(follower_dir, LogConfig::default()).unwrap();
    follower.record_leader_epoch(1).unwrap();
    follower.append_replica_batch(shared.clone()).unwrap();
    follower.append_replica_batch(follower_old.clone()).unwrap();
    follower.set_high_watermark(1).unwrap();
    (vec![shared, leader_new], vec![follower_old])
}

fn seed_leader_with_retained_prefix(temp: &TempDir) -> (i64, i64, Vec<bytes::Bytes>) {
    let leader_dir = temp.path().join("broker-1").join(format!("{TOPIC}-0"));
    let follower_dir = temp.path().join("broker-2").join(format!("{TOPIC}-0"));
    let mut leader = Log::open(
        leader_dir,
        LogConfig {
            segment_bytes: 1,
            retention_bytes: Some(300),
            ..LogConfig::default()
        },
    )
    .unwrap();
    leader.record_leader_epoch(1).unwrap();
    for offset in 0..12 {
        leader
            .append_replica_batch(
                RecordBatch::new(
                    offset,
                    1,
                    1_000 + offset,
                    vec![Record::new(format!("retained-{offset}"))],
                )
                .encode(),
            )
            .unwrap();
    }
    leader.set_high_watermark(12).unwrap();
    assert!(leader.apply_retention().unwrap() > 0);
    let start = leader.log_start_offset();
    let end = leader.log_end_offset();
    assert!(
        start > 1,
        "test requires leader retention ahead of follower"
    );
    let retained = leader.read(start, usize::MAX).unwrap();
    drop(leader);

    let mut follower = Log::open(follower_dir, LogConfig::default()).unwrap();
    follower.record_leader_epoch(1).unwrap();
    follower
        .append_replica_batch(RecordBatch::new(0, 1, 1_000, vec![Record::new("obsolete")]).encode())
        .unwrap();
    follower.set_high_watermark(1).unwrap();
    (start, end, retained)
}

#[tokio::test]
async fn stale_follower_rebases_to_leader_retained_start_and_resumes_fetching() {
    let temp = tempfile::tempdir().unwrap();
    let (leader_start, leader_end, retained) = seed_leader_with_retained_prefix(&temp);
    let image = cluster_image(&[1, 2], vec![1, 2], vec![1], 1, 1, BTreeMap::new());
    let mut cluster = ClusterHarness::start(temp, image).await;
    cluster.start_manager(2, broker_epoch(2));

    wait_for_offsets(&cluster.brokers[&2], leader_end, leader_end).await;
    let follower = cluster.brokers[&2].replica_partition(TOPIC, 0).unwrap();
    assert_eq!(
        follower.offsets().await.unwrap(),
        (leader_start, leader_end, leader_end)
    );
    assert_eq!(
        follower
            .read_uncommitted(leader_start, usize::MAX)
            .await
            .unwrap()
            .batches,
        retained
    );
    let entries = follower.leader_epoch_entries().await.unwrap();
    assert_eq!(
        entries
            .first()
            .map(|entry| (entry.epoch, entry.start_offset)),
        Some((1, leader_start))
    );

    drop(follower);
    cluster.stop().await;
}

#[tokio::test]
async fn follower_truncates_divergence_tracks_batch_epoch_and_cancels_on_role_change() {
    let temp = tempfile::tempdir().unwrap();
    let (leader_bytes, divergent) = seed_divergent_logs(&temp);
    let image = cluster_image(&[1, 2], vec![1, 2], vec![1], 1, 2, BTreeMap::new());
    let mut cluster = ClusterHarness::start(temp, image).await;
    cluster.start_manager(2, 0);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(cluster.brokers[&2]
        .replication_health()
        .follower_fetchers
        .is_empty());
    cluster.epoch_senders[&2].send(broker_epoch(2)).unwrap();

    wait_for_offsets(&cluster.brokers[&2], 2, 2).await;
    let follower_bytes = raw_batches(&cluster.brokers[&2]).await;
    assert_eq!(follower_bytes, leader_bytes);
    assert!(!follower_bytes.contains(&divergent[0]));
    let follower_handle = cluster.brokers[&2].replica_partition(TOPIC, 0).unwrap();
    let entries = follower_handle.leader_epoch_entries().await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!((entries[0].epoch, entries[0].start_offset), (1, 0));
    assert_eq!((entries[1].epoch, entries[1].start_offset), (2, 1));

    let mut role_changed = cluster.cache.snapshot().as_ref().clone();
    role_changed
        .brokers
        .get_mut(&2)
        .unwrap()
        .roles
        .remove(&NodeRole::Broker);
    role_changed.offset += 1;
    cluster.cache.replace(role_changed);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if cluster.brokers[&2]
                .replication_health()
                .follower_fetchers
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("follower worker was not cancelled after broker role removal");

    drop(follower_handle);
    cluster.stop().await;
}
