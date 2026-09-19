use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use brahmaputra_broker::{Broker, BrokerConfig, BrokerError, BrokerState};
use brahmaputra_client::Consumer;
use brahmaputra_metadata::{ClusterMetadata, MetadataCache, MetadataCommand, NodeRole};
use brahmaputra_protocol::codec;
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{FetchRequest, MetadataResponse, ProduceRequest, ProduceResponse};
use brahmaputra_protocol::{
    decode_payload, encode_frame, ApiKey, FrameHeader, Record, RecordBatch,
};
use brahmaputra_storage::{Log, LogConfig};
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

struct RunningBroker {
    broker: Arc<Broker>,
    addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

async fn start_broker(
    broker_id: i32,
    data_dir: &Path,
    metadata_cache: MetadataCache,
) -> RunningBroker {
    let broker_epoch = metadata_cache
        .snapshot()
        .brokers
        .get(&broker_id)
        .map(|broker| broker.broker_epoch);
    let broker = Arc::new(
        Broker::bind(BrokerConfig {
            broker_id,
            broker_epoch,
            port: 0,
            data_dirs: vec![data_dir.to_owned()],
            metadata_cache: Some(metadata_cache),
            ..BrokerConfig::default()
        })
        .await
        .unwrap(),
    );
    let addr = broker.local_addr();
    let serving = Arc::clone(&broker);
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        serving
            .run(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    RunningBroker {
        broker,
        addr,
        shutdown,
        task,
    }
}

async fn stop_broker(running: RunningBroker) {
    let _ = running.shutdown.send(());
    running.task.await.unwrap();
}

#[tokio::test]
async fn producers_retry_a_suspended_broker_lease_without_duplicate_appends() {
    use brahmaputra_client::{Producer, ProducerConfig};
    use std::time::Duration;
    for linger_ms in [0, 5] {
        let dir = tempfile::tempdir().unwrap();
        let mut image = ClusterMetadata::new("lease-retry");
        image
            .apply(MetadataCommand::RegisterBroker {
                broker_id: 1,
                host: "127.0.0.1".into(),
                data_port: 0,
                control_port: 0,
                internal_port: 0,
                expected_epoch: None,
                roles: vec![NodeRole::Broker],
                rack: None,
                now_ms: 1_000,
            })
            .unwrap();
        image
            .apply(MetadataCommand::CreateTopic {
                name: "lease".into(),
                partitions: 1,
                replication_factor: 1,
                configs: BTreeMap::new(),
            })
            .unwrap();
        let cache = MetadataCache::new(image.clone());
        let running = start_broker(1, dir.path(), cache.clone()).await;
        image.brokers.get_mut(&1).unwrap().data_port = running.addr.port();
        cache.replace(image);
        let producer = Producer::connect(
            running.addr,
            ProducerConfig {
                linger_ms,
                retries: 100,
                retry_backoff_ms: 10,
                ..ProducerConfig::default()
            },
        )
        .await
        .unwrap();
        producer
            .send("lease", Some(0), None, Bytes::from_static(b"before"))
            .await
            .unwrap();
        running.broker.suspend_broker_lease();
        let fresh_consumer = Consumer::connect(running.addr, "suspended-seed-metadata")
            .await
            .unwrap();
        let routing = fresh_consumer.metadata(&["lease".into()]).await.unwrap();
        assert_eq!(
            routing.topics.len(),
            1,
            "a suspended seed must still provide read-only cluster routes"
        );
        assert_eq!(routing.topics[0].error_code, ec::NONE);
        assert_eq!(routing.topics[0].partitions[0].leader, 1);
        let broker = running.broker.clone();
        let recovery = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            broker
                .activate_broker_epoch(broker.local_broker_epoch())
                .unwrap();
        });
        let offset = tokio::time::timeout(
            Duration::from_secs(5),
            producer.send("lease", Some(0), None, Bytes::from_static(b"after")),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(offset, 1);
        recovery.await.unwrap();
        let consumer = Consumer::connect(running.addr, "check-lease")
            .await
            .unwrap();
        let records = consumer.fetch("lease", 0, 0, 0).await.unwrap();
        assert_eq!(records.len(), 2, "neither send may be lost or duplicated");
        stop_broker(running).await;
    }
}

#[tokio::test]
async fn new_group_waits_for_an_unroutable_coordinator_to_recover() {
    use brahmaputra_client::{AutoOffsetReset, GroupConsumer, Producer, ProducerConfig};
    use std::time::Duration;
    let directory = tempfile::tempdir().unwrap();
    let mut image = ClusterMetadata::new("coordinator-recovery");
    image
        .apply(MetadataCommand::RegisterBroker {
            broker_id: 1,
            host: "127.0.0.1".into(),
            data_port: 0,
            control_port: 0,
            internal_port: 0,
            expected_epoch: None,
            roles: vec![NodeRole::Broker],
            rack: None,
            now_ms: 1_000,
        })
        .unwrap();
    for name in ["records", "__consumer_offsets"] {
        image
            .apply(MetadataCommand::CreateTopic {
                name: name.into(),
                partitions: 1,
                replication_factor: 1,
                configs: BTreeMap::new(),
            })
            .unwrap();
    }
    let cache = MetadataCache::new(image.clone());
    let running = start_broker(1, directory.path(), cache.clone()).await;
    image.brokers.get_mut(&1).unwrap().data_port = running.addr.port();
    cache.replace(image.clone());
    let producer = Producer::connect(running.addr, ProducerConfig::default())
        .await
        .unwrap();
    producer
        .send("records", Some(0), None, Bytes::from_static(b"survives"))
        .await
        .unwrap();
    let mut unavailable = image.clone();
    unavailable
        .topics
        .get_mut("__consumer_offsets")
        .unwrap()
        .partitions
        .get_mut(&0)
        .unwrap()
        .leader = -1;
    cache.replace(unavailable);
    let mut consumer = GroupConsumer::connect(running.addr, "new-reader", "new-group")
        .await
        .unwrap()
        .with_auto_offset_reset(AutoOffsetReset::Earliest);
    consumer.subscribe(&["records"]);
    let recovery = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        cache.replace(image);
    });
    let records = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let records = consumer.poll(Duration::from_millis(100)).await.unwrap();
            if !records.is_empty() {
                break records;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].value.as_deref(), Some(b"survives".as_slice()));
    consumer.commit_sync().await.unwrap();
    consumer.close().await.unwrap();
    recovery.await.unwrap();
    stop_broker(running).await;
}

fn cluster_image() -> ClusterMetadata {
    let mut image = ClusterMetadata::new("cluster-test");
    for (broker_id, host, data_port) in [(1, "broker-one", 19_091), (2, "broker-two", 19_092)] {
        image
            .apply(MetadataCommand::RegisterBroker {
                broker_id,
                host: host.into(),
                data_port,
                control_port: data_port + 10_000,
                internal_port: 0,
                expected_epoch: None,
                roles: vec![NodeRole::Broker, NodeRole::Controller],
                rack: None,
                now_ms: 1_000,
            })
            .unwrap();
    }
    image
        .apply(MetadataCommand::SetController { broker_id: 2 })
        .unwrap();
    image
        .apply(MetadataCommand::CreateTopic {
            name: "orders".into(),
            partitions: 2,
            replication_factor: 2,
            configs: BTreeMap::new(),
        })
        .unwrap();
    image
        .apply(MetadataCommand::ChangePartition {
            topic: "orders".into(),
            partition: 0,
            leader: 2,
            isr: vec![1, 2],
            expected_leader_epoch: 0,
        })
        .unwrap();
    image
}

#[derive(Debug, PartialEq, Eq)]
struct WireMetadataImage {
    brokers: Vec<(i32, String, i32)>,
    controller_id: i32,
    topics: Vec<WireTopicImage>,
}

#[derive(Debug, PartialEq, Eq)]
struct WireTopicImage {
    name: String,
    error_code: i32,
    partitions: Vec<WirePartitionImage>,
}

#[derive(Debug, PartialEq, Eq)]
struct WirePartitionImage {
    partition: i32,
    leader: i32,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    leader_epoch: i32,
}

fn wire_image(response: MetadataResponse) -> WireMetadataImage {
    WireMetadataImage {
        brokers: response
            .brokers
            .into_iter()
            .map(|broker| (broker.broker_id, broker.host, broker.port))
            .collect(),
        controller_id: response.controller_id,
        topics: response
            .topics
            .into_iter()
            .map(|topic| WireTopicImage {
                name: topic.name,
                error_code: topic.error_code,
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| WirePartitionImage {
                        partition: partition.partition,
                        leader: partition.leader,
                        replicas: partition.replicas,
                        isr: partition.isr,
                        leader_epoch: partition.leader_epoch,
                    })
                    .collect(),
            })
            .collect(),
    }
}

async fn raw_request(addr: SocketAddr, api_key: ApiKey, body: &[u8]) -> Bytes {
    let mut socket = TcpStream::connect(addr).await.unwrap();
    let header = FrameHeader::new(api_key, 73, Some("cluster-test".into()));
    socket
        .write_all(&encode_frame(&header, body))
        .await
        .unwrap();

    let mut length = [0_u8; 4];
    socket.read_exact(&mut length).await.unwrap();
    let length = i32::from_be_bytes(length);
    assert!(length >= 0);
    let mut payload = vec![0_u8; length as usize];
    socket.read_exact(&mut payload).await.unwrap();
    let mut payload = Bytes::from(payload);
    let response_header = decode_payload(&mut payload).unwrap();
    assert_eq!(response_header.api_key, api_key);
    assert_eq!(response_header.correlation_id, 73);
    payload
}

#[tokio::test]
async fn every_broker_serves_the_same_complete_cluster_image() {
    let temp = tempfile::tempdir().unwrap();
    let image = cluster_image();
    let broker_one = start_broker(
        1,
        &temp.path().join("broker-one"),
        MetadataCache::new(image.clone()),
    )
    .await;
    let broker_two = start_broker(
        2,
        &temp.path().join("broker-two"),
        MetadataCache::new(image),
    )
    .await;

    let client_one = Consumer::connect(broker_one.addr, "metadata-one")
        .await
        .unwrap();
    let client_two = Consumer::connect(broker_two.addr, "metadata-two")
        .await
        .unwrap();
    let from_one = wire_image(client_one.metadata(&[]).await.unwrap());
    let from_two = wire_image(client_two.metadata(&[]).await.unwrap());
    assert_eq!(from_one, from_two);
    assert_eq!(
        from_one,
        WireMetadataImage {
            brokers: vec![
                (1, "broker-one".into(), 19_091),
                (2, "broker-two".into(), 19_092),
            ],
            controller_id: 2,
            topics: vec![WireTopicImage {
                name: "orders".into(),
                error_code: ec::NONE,
                partitions: vec![
                    WirePartitionImage {
                        partition: 0,
                        leader: 2,
                        replicas: vec![1, 2],
                        isr: vec![1, 2],
                        leader_epoch: 1,
                    },
                    WirePartitionImage {
                        partition: 1,
                        leader: 2,
                        replicas: vec![2, 1],
                        isr: vec![2, 1],
                        leader_epoch: 0,
                    },
                ],
            }],
        }
    );

    let missing = client_one
        .metadata(&["implicit-topic".into()])
        .await
        .unwrap();
    assert_eq!(missing.topics.len(), 1);
    assert_eq!(missing.topics[0].error_code, ec::UNKNOWN_TOPIC_OR_PARTITION);
    assert!(matches!(
        broker_one.broker.partition_auto_create("implicit-topic", 0),
        Err(BrokerError::UnknownTopicOrPartition { .. })
    ));
    assert!(!broker_one
        .broker
        .metadata_cache()
        .unwrap()
        .snapshot()
        .topics
        .contains_key("implicit-topic"));

    stop_broker(broker_one).await;
    stop_broker(broker_two).await;
}

#[tokio::test]
async fn client_io_rejects_a_follower_but_replica_io_can_open_its_log() {
    let temp = tempfile::tempdir().unwrap();
    let mut image = cluster_image();
    let follower = start_broker(
        1,
        &temp.path().join("follower"),
        MetadataCache::new(image.clone()),
    )
    .await;

    let produce = ProduceRequest {
        topic: "orders".into(),
        partition: 0,
        acks: 1,
        timeout_ms: 1_000,
        ..Default::default()
    };
    let batch = RecordBatch::new(0, 0, 1_000, vec![Record::new("must-not-append")]);
    let body = codec::encode_produce_request(&produce, &[batch.encode()]).unwrap();
    let response = raw_request(follower.addr, ApiKey::Produce, &body).await;
    let response = ProduceResponse::decode(&response).unwrap();
    assert_eq!(response.error_code, ec::NOT_LEADER_OR_FOLLOWER);
    assert_eq!(response.base_offset, -1);

    let fetch = FetchRequest {
        topic: "orders".into(),
        partition: 0,
        fetch_offset: 0,
        max_bytes: 1_024,
        max_wait_ms: 0,
        min_bytes: 1,
        isolation_level: 0,
        rack: String::new(),
    };
    let response = raw_request(follower.addr, ApiKey::Fetch, &fetch.encode().unwrap()).await;
    let (response, batches) = codec::decode_fetch_response(response).unwrap();
    assert_eq!(response.error_code, ec::NOT_LEADER_OR_FOLLOWER);
    assert!(batches.is_empty());

    let replica = follower
        .broker
        .replica_partition("orders", 0)
        .expect("assigned follower log remains available for replication");
    let (_, log_end, _) = replica.offsets().await.unwrap();
    assert_eq!(log_end, 0, "rejected client produce must not append");

    image
        .apply(MetadataCommand::RegisterBroker {
            broker_id: 3,
            host: "broker-three".into(),
            data_port: 19_093,
            control_port: 29_093,
            internal_port: 0,
            expected_epoch: None,
            roles: vec![NodeRole::Broker],
            rack: None,
            now_ms: 1_000,
        })
        .unwrap();
    let unassigned = Broker::bind(BrokerConfig {
        broker_id: 3,
        broker_epoch: Some(1),
        port: 0,
        data_dirs: vec![temp.path().join("unassigned")],
        metadata_cache: Some(MetadataCache::new(image)),
        ..BrokerConfig::default()
    })
    .await
    .unwrap();
    assert!(matches!(
        unassigned.replica_partition("orders", 0),
        Err(BrokerError::NotLeaderOrFollower {
            broker_id: 3,
            leader: 2,
            ..
        })
    ));

    drop(replica);
    stop_broker(follower).await;
}

#[tokio::test]
async fn default_config_keeps_m1_implicit_topic_creation() {
    let temp = tempfile::tempdir().unwrap();
    let broker = Broker::bind(BrokerConfig {
        port: 0,
        data_dirs: vec![temp.path().to_owned()],
        default_partitions: 3,
        ..BrokerConfig::default()
    })
    .await
    .unwrap();

    assert_eq!(broker.config().broker_id, 0);
    assert!(broker.metadata_cache().is_none());
    broker.partition_auto_create("m1-topic", 2).unwrap();
    assert_eq!(broker.state().partitions("m1-topic"), Some(3));
}

#[tokio::test]
async fn standalone_recovery_promotes_a_stale_hwm_but_cluster_recovery_does_not() {
    let temp = tempfile::tempdir().unwrap();
    let standalone_dir = temp.path().join("standalone");
    BrokerState::load(&standalone_dir, 1)
        .unwrap()
        .ensure_topic("recovered")
        .unwrap();
    let mut stale_log =
        Log::open(standalone_dir.join("recovered-0"), LogConfig::default()).unwrap();
    stale_log
        .append(RecordBatch::new(
            0,
            0,
            1_000,
            vec![Record::new("durable-a"), Record::new("durable-b")],
        ))
        .unwrap();
    assert_eq!(stale_log.log_end_offset(), 2);
    assert_eq!(stale_log.high_watermark(), 0);
    drop(stale_log);

    let standalone = Broker::bind(BrokerConfig {
        port: 0,
        data_dirs: vec![standalone_dir],
        ..BrokerConfig::default()
    })
    .await
    .unwrap();
    let recovered = standalone.partition("recovered", 0).unwrap();
    assert_eq!(recovered.offsets().await.unwrap(), (0, 2, 2));
    let outcome = recovered.read(0, usize::MAX).await.unwrap();
    assert_eq!(outcome.high_watermark, 2);
    assert_eq!(outcome.batches.len(), 1, "durable tail must be visible");
    drop(recovered);

    let cluster_dir = temp.path().join("cluster");
    let mut cluster_log = Log::open(cluster_dir.join("orders-0"), LogConfig::default()).unwrap();
    cluster_log
        .append(RecordBatch::new(
            0,
            0,
            2_000,
            vec![Record::new("uncommitted-follower-tail")],
        ))
        .unwrap();
    assert_eq!(cluster_log.log_end_offset(), 1);
    assert_eq!(cluster_log.high_watermark(), 0);
    drop(cluster_log);

    let cluster = Broker::bind(BrokerConfig {
        broker_id: 1,
        broker_epoch: Some(1),
        port: 0,
        data_dirs: vec![cluster_dir],
        metadata_cache: Some(MetadataCache::new(cluster_image())),
        replication_enabled: true,
        ..BrokerConfig::default()
    })
    .await
    .unwrap();
    let replica = cluster.replica_partition("orders", 0).unwrap();
    assert_eq!(replica.offsets().await.unwrap(), (0, 1, 0));
    let outcome = replica.read(0, usize::MAX).await.unwrap();
    assert!(
        outcome.batches.is_empty(),
        "cluster HWM must remain authoritative"
    );
}
