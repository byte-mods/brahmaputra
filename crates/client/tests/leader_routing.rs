use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use brahmaputra_client::{Consumer, Producer, ProducerConfig, LATEST};
use brahmaputra_protocol::codec;
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{
    BrokerInfo, FetchRequest, FetchResponse, ListOffsetsRequest, ListOffsetsResponse,
    MetadataRequest, MetadataResponse, PartitionInfo, ProduceResponse, TopicInfo,
};
use brahmaputra_protocol::{
    decode_payload, encode_payload, ApiKey, FrameHeader, Record, RecordBatch,
};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const TOPIC: &str = "routed-topic";

#[derive(Default)]
struct RequestCounts {
    produce: AtomicUsize,
    fetch: AtomicUsize,
    list_offsets: AtomicUsize,
    not_leader: AtomicUsize,
}

struct ClusterState {
    leader: AtomicI32,
    brokers: Vec<(i32, SocketAddr)>,
    records: Mutex<HashMap<i32, Vec<Record>>>,
    counts: HashMap<i32, RequestCounts>,
}

impl ClusterState {
    fn count(&self, broker_id: i32) -> &RequestCounts {
        self.counts.get(&broker_id).expect("known test broker")
    }
}

async fn bind_brokers() -> (TcpListener, TcpListener) {
    let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
    (first, second)
}

fn serve(listener: TcpListener, broker_id: i32, state: Arc<ClusterState>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                serve_connection(socket, broker_id, state).await;
            });
        }
    })
}

async fn serve_connection(socket: TcpStream, broker_id: i32, state: Arc<ClusterState>) {
    let codec = LengthDelimitedCodec::builder()
        .big_endian()
        .length_field_length(4)
        .new_codec();
    let mut framed = Framed::new(socket, codec);
    while let Some(frame) = framed.next().await {
        let mut body = frame.unwrap().freeze();
        let header = decode_payload(&mut body).unwrap();
        let response = dispatch(broker_id, &state, header.api_key, body);
        let response_header = FrameHeader {
            api_key: header.api_key,
            api_version: header.api_version,
            correlation_id: header.correlation_id,
            client_id: None,
        };
        framed
            .send(encode_payload(&response_header, &response))
            .await
            .unwrap();
    }
}

fn dispatch(broker_id: i32, state: &ClusterState, api_key: ApiKey, body: Bytes) -> Bytes {
    match api_key {
        ApiKey::Metadata => metadata(state, body),
        ApiKey::Produce => produce(broker_id, state, body),
        ApiKey::Fetch => fetch(broker_id, state, body),
        ApiKey::ListOffsets => list_offsets(broker_id, state, body),
        other => panic!("unexpected test API: {other:?}"),
    }
}

fn metadata(state: &ClusterState, body: Bytes) -> Bytes {
    let request = MetadataRequest::decode(&body).unwrap();
    assert!(request.topics.is_empty() || request.topics == [TOPIC]);
    let leader = state.leader.load(Ordering::SeqCst);
    Bytes::from(
        MetadataResponse {
            brokers: state
                .brokers
                .iter()
                .map(|(broker_id, address)| BrokerInfo {
                    broker_id: *broker_id,
                    host: address.ip().to_string(),
                    port: i32::from(address.port()),
                })
                .collect(),
            controller_id: 1,
            topics: vec![TopicInfo {
                name: TOPIC.into(),
                error_code: ec::NONE,
                partitions: vec![PartitionInfo {
                    partition: 0,
                    leader,
                    replicas: vec![1, 2],
                    isr: vec![1, 2],
                    leader_epoch: if leader == 2 { 1 } else { 2 },
                }],
            }],
        }
        .encode()
        .unwrap(),
    )
}

fn produce(broker_id: i32, state: &ClusterState, body: Bytes) -> Bytes {
    state
        .count(broker_id)
        .produce
        .fetch_add(1, Ordering::SeqCst);
    let (request, batches) = codec::decode_produce_request(body).unwrap();
    assert_eq!(request.topic, TOPIC);
    assert_eq!(request.partition, 0);
    if state.leader.load(Ordering::SeqCst) != broker_id {
        state
            .count(broker_id)
            .not_leader
            .fetch_add(1, Ordering::SeqCst);
        return Bytes::from(
            ProduceResponse {
                topic: TOPIC.into(),
                partition: 0,
                error_code: ec::NOT_LEADER_OR_FOLLOWER,
                base_offset: -1,
                ..Default::default()
            }
            .encode()
            .unwrap(),
        );
    }

    let mut records = state.records.lock().unwrap();
    let log = records.entry(broker_id).or_default();
    let base_offset = log.len() as i64;
    for raw in batches {
        let mut raw = raw;
        let batch = RecordBatch::decode(&mut raw).unwrap();
        log.extend(batch.iter().map(|(_, record)| record.clone()));
    }
    Bytes::from(
        ProduceResponse {
            topic: TOPIC.into(),
            partition: 0,
            error_code: ec::NONE,
            base_offset,
            ..Default::default()
        }
        .encode()
        .unwrap(),
    )
}

fn fetch(broker_id: i32, state: &ClusterState, body: Bytes) -> Bytes {
    state.count(broker_id).fetch.fetch_add(1, Ordering::SeqCst);
    let request = FetchRequest::decode(&body).unwrap();
    if state.leader.load(Ordering::SeqCst) != broker_id {
        state
            .count(broker_id)
            .not_leader
            .fetch_add(1, Ordering::SeqCst);
        return codec::encode_fetch_response(
            &FetchResponse {
                topic: TOPIC.into(),
                partition: 0,
                error_code: ec::NOT_LEADER_OR_FOLLOWER,
                high_watermark: -1,
                last_stable_offset: -1,
                ..Default::default()
            },
            &[],
        )
        .unwrap();
    }

    let records = state.records.lock().unwrap();
    let log = records.get(&broker_id).cloned().unwrap_or_default();
    let start = usize::try_from(request.fetch_offset.max(0))
        .unwrap()
        .min(log.len());
    let batches = if start == log.len() {
        Vec::new()
    } else {
        vec![RecordBatch::new(start as i64, 0, 1_000, log[start..].to_vec()).encode()]
    };
    codec::encode_fetch_response(
        &FetchResponse {
            topic: TOPIC.into(),
            partition: 0,
            error_code: ec::NONE,
            high_watermark: log.len() as i64,
            last_stable_offset: log.len() as i64,
            ..Default::default()
        },
        &batches,
    )
    .unwrap()
}

fn list_offsets(broker_id: i32, state: &ClusterState, body: Bytes) -> Bytes {
    state
        .count(broker_id)
        .list_offsets
        .fetch_add(1, Ordering::SeqCst);
    let request = ListOffsetsRequest::decode(&body).unwrap();
    let leader = state.leader.load(Ordering::SeqCst);
    let (error_code, offset) = if leader == broker_id {
        let records = state.records.lock().unwrap();
        let log_end = records.get(&broker_id).map_or(0, Vec::len) as i64;
        let offset = if request.timestamp == LATEST {
            log_end
        } else {
            0
        };
        (ec::NONE, offset)
    } else {
        state
            .count(broker_id)
            .not_leader
            .fetch_add(1, Ordering::SeqCst);
        (ec::NOT_LEADER_OR_FOLLOWER, -1)
    };
    Bytes::from(
        ListOffsetsResponse {
            topic: TOPIC.into(),
            partition: 0,
            error_code,
            offset,
            timestamp: request.timestamp,
        }
        .encode()
        .unwrap(),
    )
}

fn producer_config() -> ProducerConfig {
    ProducerConfig {
        linger_ms: 0,
        compression: brahmaputra_protocol::Compression::None,
        ..ProducerConfig::default()
    }
}

#[tokio::test]
async fn follower_seed_routes_all_client_io_and_refreshes_once_after_leader_change() {
    let (listener_one, listener_two) = bind_brokers().await;
    let address_one = listener_one.local_addr().unwrap();
    let address_two = listener_two.local_addr().unwrap();
    let state = Arc::new(ClusterState {
        leader: AtomicI32::new(2),
        brokers: vec![(1, address_one), (2, address_two)],
        records: Mutex::new(HashMap::new()),
        counts: HashMap::from([(1, RequestCounts::default()), (2, RequestCounts::default())]),
    });
    let server_one = serve(listener_one, 1, Arc::clone(&state));
    let server_two = serve(listener_two, 2, Arc::clone(&state));

    // Broker 1 is a follower. Both clients learn broker 2 from Metadata and
    // route their partition operations there without changing connect APIs.
    let producer = Producer::connect(address_one, producer_config())
        .await
        .unwrap();
    let consumer = Consumer::connect(address_one, "routed-consumer")
        .await
        .unwrap();
    assert_eq!(
        producer
            .send(TOPIC, Some(0), None, Bytes::from_static(b"leader-two"))
            .await
            .unwrap(),
        0
    );
    assert_eq!(consumer.list_offsets(TOPIC, 0, LATEST).await.unwrap(), 1);
    let fetched = consumer.fetch(TOPIC, 0, 0, 0).await.unwrap();
    assert_eq!(fetched[0].2, Bytes::from_static(b"leader-two"));
    assert_eq!(state.count(1).produce.load(Ordering::SeqCst), 0);
    assert_eq!(state.count(1).fetch.load(Ordering::SeqCst), 0);
    assert_eq!(state.count(1).list_offsets.load(Ordering::SeqCst), 0);

    // Both routers currently cache broker 2. Moving leadership makes broker
    // 2 explicitly reject their next operations; each client refreshes once
    // and safely replays against broker 1.
    state.leader.store(1, Ordering::SeqCst);
    assert_eq!(
        producer
            .send(TOPIC, Some(0), None, Bytes::from_static(b"leader-one"))
            .await
            .unwrap(),
        0
    );
    assert_eq!(consumer.list_offsets(TOPIC, 0, LATEST).await.unwrap(), 1);
    let fetched = consumer.fetch(TOPIC, 0, 0, 0).await.unwrap();
    assert_eq!(fetched[0].2, Bytes::from_static(b"leader-one"));
    assert_eq!(state.count(2).not_leader.load(Ordering::SeqCst), 2);
    assert_eq!(state.count(2).produce.load(Ordering::SeqCst), 2);
    assert_eq!(state.count(2).fetch.load(Ordering::SeqCst), 1);
    assert_eq!(state.count(2).list_offsets.load(Ordering::SeqCst), 2);
    assert_eq!(state.count(1).produce.load(Ordering::SeqCst), 1);
    assert_eq!(state.count(1).fetch.load(Ordering::SeqCst), 1);
    assert_eq!(state.count(1).list_offsets.load(Ordering::SeqCst), 1);

    server_one.abort();
    server_two.abort();
}
