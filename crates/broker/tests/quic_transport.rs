//! QUIC transport end-to-end: the same broker, the same wire format, the
//! same client APIs — only the transport differs.
//!
//! These tests exist to pin the property that matters: a transport swap must
//! not change *behaviour*. Produce/fetch, offsets, and consumer groups are
//! exercised over QUIC and compared against what TCP produces, so a QUIC
//! regression cannot hide behind "well, it's a different transport".

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use brahmaputra_broker::{Broker, BrokerConfig};
use brahmaputra_client::{
    Consumer, GroupAdmin, GroupConsumer, Producer, ProducerConfig, Transport, EARLIEST, LATEST,
};
use bytes::Bytes;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const TOPIC: &str = "quic-e2e";
const PARTITIONS: i32 = 4;

struct RunningBroker {
    addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

async fn start_broker(data_dir: &Path, transport: Transport) -> RunningBroker {
    let broker = Broker::bind(BrokerConfig {
        port: 0,
        data_dirs: vec![data_dir.to_path_buf()],
        default_partitions: PARTITIONS,
        transport,
        ..BrokerConfig::default()
    })
    .await
    .expect("bind broker");
    let addr = broker.local_addr();
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        Arc::new(broker)
            .run(async {
                let _ = stopped.await;
            })
            .await
            .expect("broker run");
    });
    RunningBroker {
        addr,
        shutdown,
        task,
    }
}

async fn stop_broker(broker: RunningBroker) {
    let _ = broker.shutdown.send(());
    broker.task.await.expect("broker task");
}

async fn produce(addr: SocketAddr, transport: Transport, count: u64) -> Vec<i64> {
    let producer = Producer::connect(
        addr,
        ProducerConfig {
            linger_ms: 0,
            transport: transport.into(),
            ..ProducerConfig::default()
        },
    )
    .await
    .expect("connect producer");
    let mut offsets = Vec::with_capacity(count as usize);
    for i in 0..count {
        offsets.push(
            producer
                .send(TOPIC, None, None, Bytes::from(format!("rec-{i}")))
                .await
                .expect("send record"),
        );
    }
    producer.flush().await.expect("flush");
    offsets
}

/// Every record on every partition, as `(partition, offset, value)`.
async fn drain(addr: SocketAddr, transport: Transport) -> Vec<(i32, i64, String)> {
    let consumer = Consumer::connect_with(transport, addr, "quic-drain")
        .await
        .expect("connect consumer");
    let mut out = Vec::new();
    for partition in 0..PARTITIONS {
        let mut next = consumer
            .list_offsets(TOPIC, partition, EARLIEST)
            .await
            .expect("earliest");
        loop {
            let records = consumer
                .fetch(TOPIC, partition, next, 200)
                .await
                .expect("fetch");
            if records.is_empty() {
                break;
            }
            for record in records {
                out.push((
                    partition,
                    record.offset,
                    String::from_utf8(record.value.to_vec()).expect("utf8 value"),
                ));
                next = record.offset + 1;
            }
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn quic_produce_and_fetch_match_tcp_byte_for_byte() {
    let tcp_dir = TempDir::new().unwrap();
    let quic_dir = TempDir::new().unwrap();

    let tcp = start_broker(tcp_dir.path(), Transport::Tcp).await;
    produce(tcp.addr, Transport::Tcp, 200).await;
    let over_tcp = drain(tcp.addr, Transport::Tcp).await;
    stop_broker(tcp).await;

    let quic = start_broker(quic_dir.path(), Transport::Quic).await;
    produce(quic.addr, Transport::Quic, 200).await;
    let over_quic = drain(quic.addr, Transport::Quic).await;

    assert_eq!(over_quic.len(), 200, "every record is readable over QUIC");
    assert_eq!(
        over_quic, over_tcp,
        "QUIC and TCP deliver identical records, partitions and offsets"
    );

    // Offsets agree too: no record silently lost or duplicated.
    let consumer = Consumer::connect_with(Transport::Quic, quic.addr, "quic-offsets")
        .await
        .unwrap();
    let mut total = 0;
    for partition in 0..PARTITIONS {
        total += consumer
            .list_offsets(TOPIC, partition, LATEST)
            .await
            .unwrap();
    }
    assert_eq!(total, 200, "log end offsets sum to the produced count");

    stop_broker(quic).await;
}

#[tokio::test]
async fn quic_carries_consumer_groups_end_to_end() {
    let dir = TempDir::new().unwrap();
    let broker = start_broker(dir.path(), Transport::Quic).await;
    produce(broker.addr, Transport::Quic, 120).await;

    let mut consumer = GroupConsumer::connect_with(
        Transport::Quic,
        broker.addr,
        "quic-group-consumer",
        "quic-group",
    )
    .await
    .expect("connect group consumer")
    .with_session_timeout(6_000)
    .with_rebalance_timeout(2_500)
    .with_auto_commit(None);
    consumer.subscribe(&[TOPIC]);

    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while seen.len() < 120 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out after {} records over QUIC",
            seen.len()
        );
        seen.extend(
            consumer
                .poll(Duration::from_millis(200))
                .await
                .expect("poll")
                .into_iter()
                .map(|record| (record.partition, record.offset)),
        );
    }
    consumer.commit_sync().await.expect("commit");

    // Group membership and committed offsets are visible over QUIC too.
    let admin = GroupAdmin::connect_with(Transport::Quic, broker.addr, "quic-admin")
        .await
        .expect("connect admin");
    let described = admin
        .describe_group("quic-group")
        .await
        .expect("describe group");
    assert_eq!(described.state, "Stable");
    assert_eq!(described.members.len(), 1);

    let committed: BTreeMap<i32, i64> = described
        .offsets
        .iter()
        .map(|(_, partition, offset)| (*partition, *offset))
        .collect();
    assert_eq!(
        committed.values().sum::<i64>(),
        120,
        "committed positions cover every consumed record"
    );

    drop(consumer);
    stop_broker(broker).await;
}

#[tokio::test]
async fn quic_rejects_a_tcp_client_instead_of_hanging() {
    let dir = TempDir::new().unwrap();
    let broker = start_broker(dir.path(), Transport::Quic).await;

    // A TCP client against a QUIC broker must fail fast: the port is UDP
    // only, so there is nothing to accept the connection.
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        Consumer::connect_with(Transport::Tcp, broker.addr, "wrong-transport"),
    )
    .await
    .expect("connect attempt should not hang");
    assert!(
        result.is_err(),
        "a TCP client must not appear to connect to a QUIC broker"
    );

    stop_broker(broker).await;
}

#[tokio::test]
async fn tls_over_tcp_carries_the_same_records_and_encrypts_them() {
    let dir = TempDir::new().unwrap();
    let broker = start_broker(dir.path(), Transport::TcpTls).await;
    produce(broker.addr, Transport::TcpTls, 150).await;
    let over_tls = drain(broker.addr, Transport::TcpTls).await;
    assert_eq!(over_tls.len(), 150, "every record readable over TLS");

    // A plaintext client must not be able to talk to a TLS listener: the
    // handshake bytes are not a valid frame, so the connection fails rather
    // than silently downgrading.
    let plaintext = tokio::time::timeout(Duration::from_secs(5), async {
        let consumer = Consumer::connect_with(Transport::Tcp, broker.addr, "plaintext").await?;
        consumer.list_offsets(TOPIC, 0, EARLIEST).await
    })
    .await
    .expect("attempt should not hang");
    assert!(
        plaintext.is_err(),
        "a plaintext client must not be served by a TLS listener"
    );

    stop_broker(broker).await;
}

#[tokio::test]
async fn every_transport_reports_the_same_api_versions() {
    let mut seen = Vec::new();
    for transport in [Transport::Tcp, Transport::TcpTls, Transport::Quic] {
        let dir = TempDir::new().unwrap();
        let broker = start_broker(dir.path(), transport).await;
        let consumer = Consumer::connect_with(transport, broker.addr, "versions")
            .await
            .expect("connect");
        let versions = consumer.api_versions().await.expect("api versions");
        assert!(
            versions.supports(
                brahmaputra_protocol::ApiKey::Produce,
                brahmaputra_protocol::API_VERSION,
            ),
            "{transport} broker must accept this client's Produce version"
        );
        seen.push(versions.api_versions);
        stop_broker(broker).await;
    }
    assert_eq!(seen[0], seen[1], "tcp and tcp-tls advertise the same APIs");
    assert_eq!(seen[1], seen[2], "tcp-tls and quic advertise the same APIs");
}
