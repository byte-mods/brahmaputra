use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use brahmaputra_broker::{Broker, BrokerConfig};
use brahmaputra_client::ReplicaClient;
use brahmaputra_metadata::{
    BrokerMetadata, ClusterMetadata, NodeRole, PartitionMetadata, TopicMetadata,
};
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::replica::{OffsetsForLeaderEpochRequest, ReplicaFetchRequest};
use brahmaputra_protocol::{Record, RecordBatch};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const TOPIC: &str = "replicated-orders";
const LEADER_ID: i32 = 1;
const FOLLOWER_ID: i32 = 2;
const FOLLOWER_EPOCH: u64 = 22;
const LEADER_EPOCH: i32 = 3;

struct RunningBroker {
    broker: Arc<Broker>,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

fn broker_metadata(broker_id: i32, broker_epoch: u64) -> BrokerMetadata {
    BrokerMetadata {
        broker_id,
        host: "127.0.0.1".into(),
        data_port: 0,
        control_port: 0,
        broker_epoch,
        roles: BTreeSet::from([NodeRole::Broker]),
        rack: None,
        alive: true,
        last_heartbeat_ms: 1_000,
    }
}

fn cluster_image() -> ClusterMetadata {
    ClusterMetadata {
        cluster_id: "replica-rpc-test".into(),
        offset: 8,
        controller_id: Some(LEADER_ID),
        brokers: BTreeMap::from([
            (LEADER_ID, broker_metadata(LEADER_ID, 11)),
            (FOLLOWER_ID, broker_metadata(FOLLOWER_ID, FOLLOWER_EPOCH)),
        ]),
        topics: BTreeMap::from([(
            TOPIC.into(),
            TopicMetadata {
                name: TOPIC.into(),
                replication_factor: 2,
                partitions: BTreeMap::from([(
                    0,
                    PartitionMetadata {
                        partition: 0,
                        replicas: vec![LEADER_ID, FOLLOWER_ID],
                        leader: LEADER_ID,
                        isr: vec![LEADER_ID, FOLLOWER_ID],
                        leader_epoch: LEADER_EPOCH,
                        target_replicas: None,
                    },
                )]),
                configs: BTreeMap::new(),
            },
        )]),
        users: BTreeMap::new(),
        acls: Default::default(),
        quotas: Default::default(),
        jwt_secret: None,
    }
}

async fn start_broker(data_dir: &std::path::Path) -> RunningBroker {
    let broker = Arc::new(
        Broker::bind(BrokerConfig {
            broker_id: LEADER_ID,
            broker_epoch: Some(11),
            port: 0,
            data_dirs: vec![data_dir.to_owned()],
            metadata_cache: Some(brahmaputra_metadata::MetadataCache::new(cluster_image())),
            ..BrokerConfig::default()
        })
        .await
        .unwrap(),
    );
    let (shutdown, shutdown_rx) = oneshot::channel();
    let serving = Arc::clone(&broker);
    let task = tokio::spawn(async move {
        serving
            .run(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    RunningBroker {
        broker,
        shutdown,
        task,
    }
}

async fn stop_broker(running: RunningBroker) {
    let _ = running.shutdown.send(());
    running.task.await.unwrap();
}

fn fetch_request() -> ReplicaFetchRequest {
    ReplicaFetchRequest {
        topic: TOPIC.into(),
        partition: 0,
        follower_id: FOLLOWER_ID,
        follower_broker_epoch: FOLLOWER_EPOCH,
        leader_epoch: LEADER_EPOCH,
        fetch_offset: 0,
        max_bytes: 1024 * 1024,
    }
}

#[tokio::test]
async fn live_replica_fetch_preserves_bytes_and_epoch_lookup_finds_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let running = start_broker(dir.path()).await;
    let handle = running.broker.partition(TOPIC, 0).unwrap();

    handle.record_leader_epoch(2).await.unwrap();
    handle
        .append(RecordBatch::new(
            0,
            2,
            1_000,
            vec![Record::new("first"), Record::new("second")],
        ))
        .await
        .unwrap();
    handle.record_leader_epoch(LEADER_EPOCH).await.unwrap();
    handle
        .append(RecordBatch::new(
            0,
            LEADER_EPOCH,
            2_000,
            vec![Record::with_key("key", "third", 0)],
        ))
        .await
        .unwrap();
    let expected = handle
        .read_uncommitted(0, usize::MAX)
        .await
        .unwrap()
        .batches;

    let client = ReplicaClient::connect(running.broker.local_addr(), "follower-2", 4)
        .await
        .unwrap();
    let fetched = client.fetch_raw(&fetch_request()).await.unwrap();
    assert_eq!(fetched.response.error_code, ec::NONE);
    assert_eq!(fetched.response.leader_epoch, LEADER_EPOCH);
    assert_eq!(fetched.response.high_watermark, 3);
    assert_eq!(fetched.response.log_start_offset, 0);
    assert_eq!(fetched.response.log_end_offset, 3);
    assert_eq!(fetched.batches, expected, "wire bytes must equal log bytes");

    let offset = client
        .offsets_for_leader_epoch(&OffsetsForLeaderEpochRequest {
            topic: TOPIC.into(),
            partition: 0,
            follower_id: FOLLOWER_ID,
            follower_broker_epoch: FOLLOWER_EPOCH,
            leader_epoch: LEADER_EPOCH,
            query_leader_epoch: 2,
        })
        .await
        .unwrap();
    assert_eq!(offset.error_code, ec::NONE);
    assert_eq!(offset.leader_epoch, LEADER_EPOCH);
    assert_eq!(offset.end_offset, 2);

    drop(handle);
    stop_broker(running).await;
}

#[tokio::test]
async fn live_replica_rpc_rejects_stale_broker_and_leader_epochs() {
    let dir = tempfile::tempdir().unwrap();
    let running = start_broker(dir.path()).await;
    let client = ReplicaClient::connect(running.broker.local_addr(), "zombie-follower", 4)
        .await
        .unwrap();

    let mut stale_broker = fetch_request();
    stale_broker.follower_broker_epoch = FOLLOWER_EPOCH - 1;
    let response = client.fetch_raw(&stale_broker).await.unwrap();
    assert_eq!(response.response.error_code, ec::FENCED_BROKER_EPOCH);
    assert_eq!(response.response.leader_epoch, LEADER_EPOCH);
    assert!(response.batches.is_empty());

    let mut stale_leader = fetch_request();
    stale_leader.leader_epoch = LEADER_EPOCH - 1;
    let response = client.fetch_raw(&stale_leader).await.unwrap();
    assert_eq!(response.response.error_code, ec::FENCED_LEADER_EPOCH);
    assert_eq!(response.response.leader_epoch, LEADER_EPOCH);
    assert!(response.batches.is_empty());

    let mut future_leader = fetch_request();
    future_leader.leader_epoch = LEADER_EPOCH + 1;
    let response = client.fetch_raw(&future_leader).await.unwrap();
    assert_eq!(response.response.error_code, ec::UNKNOWN_LEADER_EPOCH);
    assert_eq!(response.response.leader_epoch, LEADER_EPOCH);
    assert!(response.batches.is_empty());

    stop_broker(running).await;
}
