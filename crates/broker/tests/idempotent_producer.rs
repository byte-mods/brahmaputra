//! Real-TCP idempotent producer verification, including a fault proxy that
//! loses the first Produce response after the broker has appended it.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use brahmaputra_broker::{Broker, BrokerConfig};
use brahmaputra_client::{Connection, Consumer, Producer, ProducerConfig, LATEST};
use brahmaputra_metadata::{
    BrokerMetadata, ClusterMetadata, MetadataCache, NodeRole, PartitionMetadata, TopicMetadata,
};
use brahmaputra_protocol::codec;
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{ProduceRequest, ProduceResponse};
use brahmaputra_protocol::producer::{InitProducerIdRequest, InitProducerIdResponse};
use brahmaputra_protocol::{ApiKey, Compression, Record, RecordBatch};
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

const TOPIC: &str = "idempotent-orders";

struct RunningBroker {
    broker: Arc<Broker>,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

async fn start_broker(config: BrokerConfig) -> RunningBroker {
    let broker = Arc::new(Broker::bind(config).await.unwrap());
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

async fn init_producer(
    address: std::net::SocketAddr,
    request: InitProducerIdRequest,
) -> InitProducerIdResponse {
    let connection = Connection::connect(address, Some("init-test".into()), 1)
        .await
        .unwrap();
    let response = connection
        .request(ApiKey::InitProducerId, &request.encode())
        .await
        .unwrap();
    InitProducerIdResponse::decode(&response).unwrap()
}

async fn produce_raw(address: std::net::SocketAddr, batch: RecordBatch) -> ProduceResponse {
    let connection = Connection::connect(address, Some("raw-idempotent-test".into()), 1)
        .await
        .unwrap();
    let request = ProduceRequest {
        topic: TOPIC.into(),
        partition: 0,
        acks: 1,
        timeout_ms: 5_000,
        batches_length: 0,
    };
    let body = codec::encode_produce_request(&request, &[batch.encode()]).unwrap();
    let response = connection.request(ApiKey::Produce, &body).await.unwrap();
    ProduceResponse::decode(&response).unwrap()
}

fn batch(producer_id: i64, epoch: i16, sequence: i32, value: &str) -> RecordBatch {
    RecordBatch::new(0, 0, 1_000, vec![Record::new(value.as_bytes().to_vec())]).with_producer(
        producer_id,
        epoch,
        sequence,
    )
}

#[tokio::test]
async fn live_tcp_init_dedup_restart_fencing_and_ordering() {
    let temp = tempfile::tempdir().unwrap();
    let config = || BrokerConfig {
        port: 0,
        data_dir: temp.path().to_owned(),
        ..BrokerConfig::default()
    };

    let running = start_broker(config()).await;
    let allocated = init_producer(
        running.broker.local_addr(),
        InitProducerIdRequest::allocate(),
    )
    .await;
    assert_eq!(allocated.error_code, ec::NONE);
    assert!(allocated.producer_id >= 0);
    assert_eq!(allocated.producer_epoch, 0);

    let original = batch(allocated.producer_id, 0, 0, "once");
    let first = produce_raw(running.broker.local_addr(), original.clone()).await;
    assert_eq!((first.error_code, first.base_offset), (ec::NONE, 0));
    let duplicate = produce_raw(running.broker.local_addr(), original).await;
    assert_eq!((duplicate.error_code, duplicate.base_offset), (ec::NONE, 0));
    let conflict = produce_raw(
        running.broker.local_addr(),
        batch(allocated.producer_id, 0, 0, "different-content"),
    )
    .await;
    assert_eq!(conflict.error_code, ec::OUT_OF_ORDER_SEQUENCE);
    let gap = produce_raw(
        running.broker.local_addr(),
        batch(allocated.producer_id, 0, 2, "gap"),
    )
    .await;
    assert_eq!(gap.error_code, ec::OUT_OF_ORDER_SEQUENCE);

    let bumped = init_producer(
        running.broker.local_addr(),
        InitProducerIdRequest {
            producer_id: allocated.producer_id,
            producer_epoch: 0,
        },
    )
    .await;
    assert_eq!(
        (bumped.error_code, bumped.producer_id, bumped.producer_epoch),
        (ec::NONE, allocated.producer_id, 1)
    );
    // Epoch fencing is made durable per partition when the newer epoch's
    // sequence-zero batch lands; Init alone is not a cluster-wide fence.
    let new_epoch = batch(allocated.producer_id, 1, 0, "new-epoch");
    let response = produce_raw(running.broker.local_addr(), new_epoch.clone()).await;
    assert_eq!((response.error_code, response.base_offset), (ec::NONE, 1));
    let stale = produce_raw(
        running.broker.local_addr(),
        batch(allocated.producer_id, 0, 1, "zombie"),
    )
    .await;
    assert_eq!(stale.error_code, ec::FENCED_PRODUCER_EPOCH);
    assert_eq!(
        running
            .broker
            .partition(TOPIC, 0)
            .unwrap()
            .offsets()
            .await
            .unwrap(),
        (0, 2, 2)
    );
    let address = running.broker.local_addr();
    stop_broker(running).await;

    let running = start_broker(config()).await;
    assert_ne!(running.broker.local_addr(), address);
    let replay_after_restart = produce_raw(running.broker.local_addr(), new_epoch).await;
    assert_eq!(
        (
            replay_after_restart.error_code,
            replay_after_restart.base_offset
        ),
        (ec::NONE, 1)
    );
    assert_eq!(
        running
            .broker
            .partition(TOPIC, 0)
            .unwrap()
            .offsets()
            .await
            .unwrap(),
        (0, 2, 2)
    );
    let stale_bump = init_producer(
        running.broker.local_addr(),
        InitProducerIdRequest {
            producer_id: allocated.producer_id,
            producer_epoch: 0,
        },
    )
    .await;
    assert_eq!(stale_bump.error_code, ec::FENCED_PRODUCER_EPOCH);
    assert_eq!(stale_bump.producer_epoch, 1);
    let epoch_two = init_producer(
        running.broker.local_addr(),
        InitProducerIdRequest {
            producer_id: allocated.producer_id,
            producer_epoch: 1,
        },
    )
    .await;
    assert_eq!(
        (epoch_two.error_code, epoch_two.producer_epoch),
        (ec::NONE, 2)
    );
    stop_broker(running).await;
}

fn proxy_cluster_image(proxy_port: u16) -> ClusterMetadata {
    ClusterMetadata {
        cluster_id: "idempotent-proxy".into(),
        offset: 1,
        controller_id: Some(1),
        brokers: BTreeMap::from([(
            1,
            BrokerMetadata {
                broker_id: 1,
                host: "127.0.0.1".into(),
                data_port: proxy_port,
                control_port: 0,
                broker_epoch: 1,
                roles: BTreeSet::from([NodeRole::Broker]),
                rack: None,
                alive: true,
                last_heartbeat_ms: 0,
            },
        )]),
        topics: BTreeMap::from([(
            TOPIC.into(),
            TopicMetadata {
                name: TOPIC.into(),
                replication_factor: 1,
                partitions: BTreeMap::from([(
                    0,
                    PartitionMetadata {
                        partition: 0,
                        replicas: vec![1],
                        leader: 1,
                        isr: vec![1],
                        leader_epoch: 0,
                    },
                )]),
                configs: BTreeMap::new(),
            },
        )]),
        users: BTreeMap::new(),
        acls: Default::default(),
        jwt_secret: None,
    }
}

async fn run_fault_proxy(
    listener: TcpListener,
    upstream: std::net::SocketAddr,
    dropped: Arc<AtomicBool>,
    mut shutdown: watch::Receiver<bool>,
) {
    while !*shutdown.borrow() {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let Ok((client, _)) = accepted else { break };
                let dropped = Arc::clone(&dropped);
                tokio::spawn(async move {
                    let _ = proxy_connection(client, upstream, dropped).await;
                });
            }
        }
    }
}

async fn proxy_connection(
    mut client: TcpStream,
    upstream: std::net::SocketAddr,
    dropped: Arc<AtomicBool>,
) -> io::Result<()> {
    let mut broker = TcpStream::connect(upstream).await?;
    loop {
        let request = match read_frame(&mut client).await {
            Ok(frame) => frame,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        };
        let api_key = i16::from_be_bytes([request[4], request[5]]);
        broker.write_all(&request).await?;
        let response = read_frame(&mut broker).await?;
        if api_key == ApiKey::Produce as i16 && !dropped.swap(true, Ordering::SeqCst) {
            // The upstream response has been fully read, proving the broker
            // appended before both sockets are closed without forwarding it.
            return Ok(());
        }
        client.write_all(&response).await?;
    }
}

async fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).await?;
    let length = i32::from_be_bytes(length);
    if !(0..=32 * 1024 * 1024).contains(&length) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid frame length",
        ));
    }
    let mut frame = Vec::with_capacity(4 + length as usize);
    frame.extend_from_slice(&length.to_be_bytes());
    frame.resize(4 + length as usize, 0);
    stream.read_exact(&mut frame[4..]).await?;
    Ok(frame)
}

#[tokio::test]
async fn idempotent_client_retries_lost_response_without_duplicate_append() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let temp = tempfile::tempdir().unwrap();
        let proxy_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let proxy_address = proxy_listener.local_addr().unwrap();
        let cache = MetadataCache::new(proxy_cluster_image(proxy_address.port()));
        let running = start_broker(BrokerConfig {
            broker_id: 1,
            broker_epoch: Some(1),
            port: 0,
            data_dir: temp.path().to_owned(),
            metadata_cache: Some(cache),
            ..BrokerConfig::default()
        })
        .await;
        let dropped = Arc::new(AtomicBool::new(false));
        let (proxy_shutdown, proxy_shutdown_rx) = watch::channel(false);
        let proxy_task = tokio::spawn(run_fault_proxy(
            proxy_listener,
            running.broker.local_addr(),
            Arc::clone(&dropped),
            proxy_shutdown_rx,
        ));

        let producer = Producer::connect(
            proxy_address,
            ProducerConfig {
                client_id: "loss-retry".into(),
                linger_ms: 0,
                compression: Compression::None,
                idempotence: true,
                ..ProducerConfig::default()
            },
        )
        .await
        .unwrap();
        assert!(producer.producer_identity().is_some());
        let offset = producer
            .send(TOPIC, Some(0), None, Bytes::from_static(b"exactly-once"))
            .await
            .unwrap();
        assert_eq!(offset, 0);
        assert!(dropped.load(Ordering::SeqCst));

        let consumer = Consumer::connect(proxy_address, "loss-check")
            .await
            .unwrap();
        let records = consumer.fetch(TOPIC, 0, 0, 100).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, 0);
        assert_eq!(records[0].2, Bytes::from_static(b"exactly-once"));
        assert_eq!(consumer.list_offsets(TOPIC, 0, LATEST).await.unwrap(), 1);

        let _ = proxy_shutdown.send(true);
        proxy_task.await.unwrap();
        stop_broker(running).await;
    })
    .await
    .expect("idempotent loss/retry test timed out");
}
