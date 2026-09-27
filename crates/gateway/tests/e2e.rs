//! End to end: a real broker, a real gateway, real WebSocket clients, and
//! a consumer reading back what landed. Every test asserts what arrived in
//! the log, not just what the gateway said.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use brahmaputra_broker::{Broker, BrokerConfig};
use brahmaputra_client::{murmur2, Consumer, FetchedRecord};
use brahmaputra_metrics::Metrics as BrokerMetrics;
use brahmaputra_ws_gateway::auth::{sign_hs256, Claims};
use brahmaputra_ws_gateway::{GatewayConfig, RunningGateway};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const SECRET: &str = "test-secret-at-least-16-bytes";
const PARTITIONS: i32 = 4;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Env {
    broker_addr: std::net::SocketAddr,
    broker_metrics: BrokerMetrics,
    broker_stop: Option<oneshot::Sender<()>>,
    broker_task: Option<JoinHandle<()>>,
    gateway: Option<RunningGateway>,
    _dir: tempfile::TempDir,
}

impl Env {
    async fn start(tweak: impl FnOnce(&mut GatewayConfig)) -> Env {
        let dir = tempfile::tempdir().unwrap();
        let broker_metrics = BrokerMetrics::default();
        let config = BrokerConfig {
            port: 0,
            data_dirs: vec![dir.path().to_path_buf()],
            default_partitions: PARTITIONS,
            metrics: broker_metrics.clone(),
            ..BrokerConfig::default()
        };
        let broker = Broker::bind(config).await.expect("bind broker");
        let broker_addr = broker.local_addr();
        let (stop, stopped) = oneshot::channel();
        let broker_task = tokio::spawn(async move {
            Arc::new(broker)
                .run(async {
                    let _ = stopped.await;
                })
                .await
                .expect("broker run");
        });
        let mut config = GatewayConfig::for_test(broker_addr, SECRET);
        tweak(&mut config);
        let gateway = brahmaputra_ws_gateway::start(config)
            .await
            .expect("gateway");
        Env {
            broker_addr,
            broker_metrics,
            broker_stop: Some(stop),
            broker_task: Some(broker_task),
            gateway: Some(gateway),
            _dir: dir,
        }
    }

    fn gw(&self) -> &RunningGateway {
        self.gateway.as_ref().unwrap()
    }

    fn url(&self, query: &str) -> String {
        format!("ws://{}/ws?{query}", self.gw().ws_addr)
    }

    async fn stop_broker(&mut self) {
        if let Some(stop) = self.broker_stop.take() {
            let _ = stop.send(());
            let _ = self.broker_task.take().unwrap().await;
        }
    }

    async fn finish(mut self) {
        if let Some(gateway) = self.gateway.take() {
            gateway.shutdown().await;
        }
        self.stop_broker().await;
    }

    async fn read_topic(&self, topic: &str) -> BTreeMap<i32, Vec<FetchedRecord>> {
        let consumer = Consumer::connect(self.broker_addr, "e2e-reader")
            .await
            .unwrap();
        let mut out = BTreeMap::new();
        for partition in 0..PARTITIONS {
            let mut records = Vec::new();
            let mut offset = 0;
            loop {
                let batch = consumer
                    .fetch(topic, partition, offset, 1 << 20)
                    .await
                    .unwrap();
                if batch.is_empty() {
                    break;
                }
                offset = batch.last().unwrap().offset + 1;
                records.extend(batch);
            }
            out.insert(partition, records);
        }
        out
    }
}

fn token(sub: &str, topics: Option<&[&str]>, ttl: i64) -> String {
    let now = brahmaputra_ws_gateway::auth::now_secs();
    let claims = Claims {
        sub: sub.into(),
        exp: now + ttl,
        nbf: None,
        iat: Some(now),
        iss: None,
        aud: None,
        topics: topics.map(|t| t.iter().map(|s| s.to_string()).collect()),
        subscribe: None,
    };
    sign_hs256(&claims, None, SECRET.as_bytes())
}

async fn connect(url: &str, bearer: Option<&str>) -> Result<Ws, WsError> {
    let mut request = url.into_client_request().unwrap();
    if let Some(bearer) = bearer {
        request.headers_mut().insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {bearer}")).unwrap(),
        );
    }
    tokio_tungstenite::connect_async(request)
        .await
        .map(|(ws, _)| ws)
}

fn status_of(result: Result<Ws, WsError>) -> u16 {
    match result {
        Err(WsError::Http(response)) => response.status().as_u16(),
        Ok(_) => 101,
        Err(other) => panic!("expected an HTTP rejection, got {other}"),
    }
}

/// Next JSON frame from the gateway, skipping pings.
async fn next_json(ws: &mut Ws) -> Value {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .expect("frame within 10s")
            .expect("stream open")
            .expect("frame ok");
        match message {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("unexpected frame {other:?}"),
        }
    }
}

async fn welcome(ws: &mut Ws) -> Value {
    let frame = next_json(ws).await;
    assert_eq!(frame["type"], "welcome", "{frame}");
    frame
}

fn partition_for(key: &[u8]) -> i32 {
    ((murmur2(key) & 0x7fff_ffff) % PARTITIONS as u32) as i32
}

fn header<'a>(record: &'a FetchedRecord, name: &str) -> Option<&'a [u8]> {
    record
        .headers
        .iter()
        .find(|h| h.key == name)
        .and_then(|h| h.value.as_deref())
}

#[tokio::test]
async fn handshakes_without_valid_credentials_are_refused() {
    let env = Env::start(|_| {}).await;
    let url = env.url("topic=orders");
    assert_eq!(status_of(connect(&url, None).await), 401, "no token");
    let forged = sign_hs256(
        &Claims {
            sub: "u".into(),
            exp: brahmaputra_ws_gateway::auth::now_secs() + 60,
            nbf: None,
            iat: None,
            iss: None,
            aud: None,
            topics: None,
            subscribe: None,
        },
        None,
        b"some-other-secret-entirely",
    );
    assert_eq!(
        status_of(connect(&url, Some(&forged)).await),
        401,
        "wrong key"
    );
    assert_eq!(
        status_of(connect(&url, Some(&token("u", None, -3600))).await),
        401,
        "expired"
    );
    assert_eq!(
        status_of(
            connect(
                &format!("ws://{}/other", env.gw().ws_addr),
                Some(&token("u", None, 60))
            )
            .await
        ),
        404
    );
    // The token may only publish to orders.*.
    let narrow = token("u", Some(&["orders.*"]), 60);
    assert_eq!(
        status_of(connect(&env.url("topic=payments"), Some(&narrow)).await),
        403
    );
    assert_eq!(
        status_of(
            connect(
                &env.url("topic=__consumer_offsets"),
                Some(&token("u", None, 60))
            )
            .await
        ),
        403,
        "broker-internal topics are never writable"
    );
    assert_eq!(
        status_of(connect(&env.url("topic=orders.eu"), Some(&narrow)).await),
        101
    );
    let via_query = env.url(&format!("topic=orders.eu&access_token={narrow}"));
    assert_eq!(
        status_of(connect(&via_query, None).await),
        101,
        "query-string token"
    );

    // Browsers: token in a subprotocol, which must be answered with ours.
    let mut request = env.url("topic=orders").into_client_request().unwrap();
    request.headers_mut().insert(
        "sec-websocket-protocol",
        HeaderValue::from_str(&format!("brahmaputra.v1, bearer.{}", token("u", None, 60))).unwrap(),
    );
    let (_, response) = tokio_tungstenite::connect_async(request).await.unwrap();
    assert_eq!(
        response.headers().get("sec-websocket-protocol").unwrap(),
        "brahmaputra.v1"
    );
    let mut request = env.url("topic=orders").into_client_request().unwrap();
    request.headers_mut().insert(
        "sec-websocket-protocol",
        HeaderValue::from_str(&format!("bearer.{}", token("u", None, 60))).unwrap(),
    );
    assert!(tokio_tungstenite::connect_async(request).await.is_err());

    let m = env.gw().metrics();
    assert!(
        m.handshakes_rejected_auth
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 4
    );
    env.finish().await;
}

#[tokio::test]
async fn keys_choose_partitions_and_records_carry_the_authenticated_user() {
    let env = Env::start(|_| {}).await;
    let topic = "e2e-keyed";
    let mut expected: Vec<(String, String, Option<String>, i32, i64)> = Vec::new();
    for user in ["alice", "bob", "carol"] {
        let mut ws = connect(
            &env.url(&format!("topic={topic}")),
            Some(&token(user, None, 60)),
        )
        .await
        .unwrap();
        let hello = welcome(&mut ws).await;
        assert_eq!(hello["user"], user);
        assert_eq!(hello["key"], user, "the user is the default key");
        for i in 0..30u64 {
            // A third explicitly keyed per order, the rest keyed by user.
            let key = (i % 3 == 0).then(|| format!("order-{}", i % 6));
            let mut publish =
                json!({"id": i, "value": format!("{user}-{i}"), "headers": {"n": i.to_string()}});
            if let Some(key) = &key {
                publish["key"] = json!(key);
            }
            ws.send(Message::text(publish.to_string())).await.unwrap();
        }
        let mut acks = HashMap::new();
        while acks.len() < 30 {
            let frame = next_json(&mut ws).await;
            assert_eq!(frame["type"], "ack", "{frame}");
            acks.insert(frame["id"].as_u64().unwrap(), frame);
        }
        for i in 0..30u64 {
            let key = if i % 3 == 0 {
                format!("order-{}", i % 6)
            } else {
                user.to_owned()
            };
            let ack = &acks[&i];
            assert_eq!(ack["topic"], topic);
            let partition = ack["partition"].as_i64().unwrap() as i32;
            assert_eq!(
                partition,
                partition_for(key.as_bytes()),
                "key {key} routed by murmur2"
            );
            expected.push((
                key,
                format!("{user}-{i}"),
                Some(i.to_string()),
                partition,
                ack["offset"].as_i64().unwrap(),
            ));
        }
        let _ = ws.close(None).await;
    }

    let log = env.read_topic(topic).await;
    assert_eq!(log.values().map(Vec::len).sum::<usize>(), expected.len());
    let mut last_offset_per_key: HashMap<String, i64> = HashMap::new();
    for (key, value, n, partition, offset) in &expected {
        let record = log[partition]
            .iter()
            .find(|r| r.offset == *offset)
            .unwrap_or_else(|| panic!("acked {partition}@{offset} is in the log"));
        assert_eq!(record.key.as_deref(), Some(key.as_bytes()));
        assert_eq!(record.value.as_deref(), Some(value.as_bytes()));
        assert_eq!(header(record, "n"), n.as_deref().map(str::as_bytes));
        let user = value.split('-').next().unwrap();
        assert_eq!(header(record, "x-gw-user"), Some(user.as_bytes()));
        // Within one connection a key's records keep send order.
        let _ = last_offset_per_key;
    }
    // Per key, per user: offsets increase in the order messages were sent.
    for user in ["alice", "bob", "carol"] {
        last_offset_per_key.clear();
        for (key, value, _, _, offset) in expected
            .iter()
            .filter(|e| e.1.starts_with(&format!("{user}-")))
        {
            let k = format!("{user}/{key}");
            if let Some(prev) = last_offset_per_key.insert(k, *offset) {
                assert!(*offset > prev, "{value}: offset {offset} after {prev}");
            }
        }
    }
    env.finish().await;
}

#[tokio::test]
async fn binary_frames_stream_to_the_connection_topic_under_its_key() {
    let env = Env::start(|_| {}).await;
    let mut ws = connect(
        &env.url("topic=e2e-telemetry&key=device-9"),
        Some(&token("fleet", None, 60)),
    )
    .await
    .unwrap();
    welcome(&mut ws).await;
    let payloads: Vec<Vec<u8>> = (0..50u8).map(|i| vec![i, 0, 255, i]).collect();
    for p in &payloads {
        ws.send(Message::binary(p.clone())).await.unwrap();
    }
    let mut ids = Vec::new();
    for _ in 0..50 {
        let frame = next_json(&mut ws).await;
        assert_eq!(frame["type"], "ack");
        assert_eq!(
            frame["partition"].as_i64().unwrap() as i32,
            partition_for(b"device-9")
        );
        ids.push(frame["id"].as_u64().unwrap());
    }
    ids.sort();
    assert_eq!(
        ids,
        (1..=50).collect::<Vec<_>>(),
        "binary frames are numbered from 1"
    );
    let log = env.read_topic("e2e-telemetry").await;
    let records = &log[&partition_for(b"device-9")];
    let values: Vec<Vec<u8>> = records
        .iter()
        .map(|r| r.value.clone().unwrap().to_vec())
        .collect();
    assert_eq!(values, payloads, "byte-identical, in order, one partition");
    assert!(records
        .iter()
        .all(|r| r.key.as_deref() == Some(&b"device-9"[..])));
    env.finish().await;
}

#[tokio::test]
async fn bad_publishes_get_errors_and_the_connection_survives() {
    let env = Env::start(|c| c.max_message_bytes = 4096).await;
    let narrow = token("u", Some(&["orders.*"]), 60);
    let mut ws = connect(&env.url("topic=orders.eu"), Some(&narrow))
        .await
        .unwrap();
    welcome(&mut ws).await;

    ws.send(Message::text(
        json!({"id": 1, "topic": "payments", "value": "x"}).to_string(),
    ))
    .await
    .unwrap();
    let e = next_json(&mut ws).await;
    assert_eq!(
        (e["type"].as_str(), e["code"].as_str(), e["id"].as_u64()),
        (Some("error"), Some("TOPIC_NOT_ALLOWED"), Some(1))
    );
    assert_eq!(e["retryable"], false);

    ws.send(Message::text(
        json!({"id": 2, "value": "x", "headers": {"x-gw-user": "admin"}}).to_string(),
    ))
    .await
    .unwrap();
    let e = next_json(&mut ws).await;
    assert_eq!(
        (e["code"].as_str(), e["id"].as_u64()),
        (Some("BAD_REQUEST"), Some(2))
    );

    ws.send(Message::text("{not json")).await.unwrap();
    assert_eq!(next_json(&mut ws).await["code"], "BAD_REQUEST");

    ws.send(Message::text(json!({"id": 3, "value": "fine"}).to_string()))
        .await
        .unwrap();
    let ack = next_json(&mut ws).await;
    assert_eq!(
        (ack["type"].as_str(), ack["id"].as_u64()),
        (Some("ack"), Some(3))
    );

    // A tombstone is a record with a null value, not an error.
    ws.send(Message::text(
        json!({"id": 4, "key": "k", "value": null}).to_string(),
    ))
    .await
    .unwrap();
    let ack = next_json(&mut ws).await;
    assert_eq!(ack["type"], "ack");
    let log = env.read_topic("orders.eu").await;
    let tomb = log[&(ack["partition"].as_i64().unwrap() as i32)]
        .iter()
        .find(|r| r.offset == ack["offset"].as_i64().unwrap())
        .unwrap();
    assert!(tomb.value.is_none());

    // Too big for --max-message-bytes: closed with 1009.
    ws.send(Message::binary(vec![0u8; 8192])).await.unwrap();
    let close = loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap()
        {
            Some(Ok(Message::Close(frame))) => break frame,
            Some(Ok(_)) => continue,
            other => panic!("expected a close frame, got {other:?}"),
        }
    };
    assert_eq!(close.unwrap().code, CloseCode::Size);
    env.finish().await;
}

#[tokio::test]
async fn per_connection_rate_limit_refuses_the_excess() {
    let env = Env::start(|c| {
        c.rate_limit_per_sec = 5.0;
        c.rate_limit_burst = 5.0;
    })
    .await;
    let mut ws = connect(
        &env.url("topic=e2e-rate"),
        Some(&token("spammer", None, 60)),
    )
    .await
    .unwrap();
    welcome(&mut ws).await;
    for i in 0..20u64 {
        ws.send(Message::text(json!({"id": i, "value": "x"}).to_string()))
            .await
            .unwrap();
    }
    let (mut acked, mut limited) = (0, 0);
    for _ in 0..20 {
        let frame = next_json(&mut ws).await;
        match (frame["type"].as_str(), frame["code"].as_str()) {
            (Some("ack"), _) => acked += 1,
            (Some("error"), Some("RATE_LIMITED")) => {
                assert_eq!(frame["retryable"], true);
                limited += 1
            }
            _ => panic!("{frame}"),
        }
    }
    assert!(
        (5..=7).contains(&acked),
        "burst of 5 (plus refill) got through: {acked}"
    );
    assert_eq!(acked + limited, 20);
    let stored: usize = env
        .read_topic("e2e-rate")
        .await
        .values()
        .map(Vec::len)
        .sum();
    assert_eq!(stored, acked, "refused messages never reach the broker");
    env.finish().await;
}

#[tokio::test]
async fn connections_beyond_capacity_are_turned_away() {
    let env = Env::start(|c| c.max_connections = 2).await;
    let t = token("u", None, 60);
    let a = connect(&env.url("topic=t"), Some(&t)).await.unwrap();
    let b = connect(&env.url("topic=t"), Some(&t)).await.unwrap();
    // The third socket is itself counted, so "more than two" is refused.
    assert_eq!(status_of(connect(&env.url("topic=t"), Some(&t)).await), 503);
    drop((a, b));
    env.finish().await;
}

#[tokio::test]
async fn idle_connections_are_closed() {
    let env = Env::start(|c| {
        c.idle_timeout_secs = 1;
        c.ping_interval_secs = 1;
    })
    .await;
    let mut ws = connect(&env.url("topic=t"), Some(&token("sleepy", None, 60)))
        .await
        .unwrap();
    welcome(&mut ws).await;
    // Not reading means not answering pings: to the gateway, this client
    // is gone.
    tokio::time::sleep(Duration::from_millis(3500)).await;
    let mut closed = false;
    while let Ok(Some(frame)) = tokio::time::timeout(Duration::from_secs(3), ws.next()).await {
        match frame {
            Ok(Message::Close(_)) | Err(_) => {
                closed = true;
                break;
            }
            Ok(_) => {}
        }
    }
    assert!(
        closed || ws.next().await.is_none(),
        "gateway closed the idle socket"
    );
    assert!(
        env.gw()
            .metrics()
            .closed_idle
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 1
    );
    env.finish().await;
}

/// Many sockets, one broker footprint: the broker sees the gateway's small
/// producer pool, not the clients, and batched requests, not one per
/// message.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_thousand_sockets_cost_the_broker_a_few_connections_and_batched_requests() {
    let env = Env::start(|c| c.linger_ms = 10).await;
    let broker_port = env.broker_addr.port();
    let before = broker_connections(broker_port);
    const CLIENTS: usize = 1000;
    const EACH: u64 = 10;
    let url = env.url("topic=e2e-fanin");
    let mut clients = Vec::new();
    for c in 0..CLIENTS {
        let url = url.clone();
        clients.push(tokio::spawn(async move {
            let mut ws = connect(&url, Some(&token(&format!("user-{c}"), None, 60)))
                .await
                .unwrap();
            welcome(&mut ws).await;
            for i in 0..EACH {
                ws.send(Message::text(
                    json!({"id": i, "value": format!("{c}:{i}")}).to_string(),
                ))
                .await
                .unwrap();
            }
            let mut acked = 0;
            while acked < EACH {
                let frame = next_json(&mut ws).await;
                assert_eq!(frame["type"], "ack", "{frame}");
                acked += 1;
            }
            ws
        }));
    }
    let mut sockets = Vec::new();
    for c in clients {
        sockets.push(c.await.unwrap());
    }
    let open = env
        .gw()
        .metrics()
        .connections_open
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(open as usize, CLIENTS);
    let during = broker_connections(broker_port);
    let records = CLIENTS as u64 * EACH;
    let produce_requests = env.broker_metrics.snapshot()["brahmaputra_produce_requests_total"];
    println!(
        "{CLIENTS} sockets, {records} records: broker connections {before} -> {during}, \
         produce requests {produce_requests}"
    );
    assert!(
        during <= before.max(2) + 1,
        "broker connections stay at the gateway's pool ({during}), not one per socket"
    );
    assert!(
        (produce_requests as u64) * 10 <= records,
        "{produce_requests} produce requests for {records} records: batching must cut requests at least tenfold"
    );
    let stored: usize = env
        .read_topic("e2e-fanin")
        .await
        .values()
        .map(Vec::len)
        .sum();
    assert_eq!(stored as u64, records);
    for mut ws in sockets {
        let _ = ws.close(None).await;
    }
    env.finish().await;
}

/// Established connections to the broker's port, from /proc/net/tcp.
fn broker_connections(port: u16) -> usize {
    let Ok(table) = std::fs::read_to_string("/proc/net/tcp") else {
        return 0;
    };
    table
        .lines()
        .skip(1)
        .filter(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            // local_address is HEX_IP:HEX_PORT; state 01 is ESTABLISHED.
            fields.len() > 3 && fields[1].ends_with(&format!(":{port:04X}")) && fields[3] == "01"
        })
        .count()
}

/// Messages the gateway has read but the broker has not yet acknowledged
/// are finished, not dropped, when the gateway is stopped. (Frames still
/// unread in the socket when shutdown starts are not: the client sees 1001
/// and resends whatever it holds no ack for.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_acknowledges_in_flight_messages_then_closes_1001() {
    let mut env = Env::start(|c| {
        // Long enough that every message is provably still in flight when
        // shutdown begins.
        c.linger_ms = 500;
        c.shutdown_grace_secs = 10;
    })
    .await;
    let mut ws = connect(&env.url("topic=e2e-drain"), Some(&token("u", None, 60)))
        .await
        .unwrap();
    welcome(&mut ws).await;
    for i in 0..40u64 {
        ws.send(Message::text(
            json!({"id": i, "value": i.to_string()}).to_string(),
        ))
        .await
        .unwrap();
    }
    let reader = tokio::spawn(async move {
        let mut acks = 0;
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(t))) => {
                    let v: Value = serde_json::from_str(&t).unwrap();
                    if v["type"] == "ack" {
                        acks += 1;
                    }
                }
                Some(Ok(Message::Close(frame))) => return (acks, frame.map(|f| f.code)),
                Some(Ok(_)) => {}
                other => panic!("expected close, got {other:?}"),
            }
        }
    });
    let relaxed = std::sync::atomic::Ordering::Relaxed;
    while env.gw().metrics().messages_received.load(relaxed) < 40 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let gateway = env.gateway.take().unwrap();
    assert!(gateway.ready());
    assert_eq!(
        gateway.metrics().messages_produced.load(relaxed),
        0,
        "nothing acknowledged yet"
    );
    gateway.shutdown().await;
    let (acks, code) = reader.await.unwrap();
    assert_eq!(
        acks, 40,
        "every in-flight message acknowledged before close"
    );
    assert_eq!(code, Some(CloseCode::Away));
    let stored: usize = env
        .read_topic("e2e-drain")
        .await
        .values()
        .map(Vec::len)
        .sum();
    assert_eq!(stored, 40);
    env.stop_broker().await;
}

#[tokio::test]
async fn an_unreachable_broker_makes_the_gateway_unready_instead_of_queueing() {
    let mut env = Env::start(|c| {
        c.max_block_ms = 500;
        c.delivery_timeout_ms = 1500;
    })
    .await;
    let mut ws = connect(&env.url("topic=e2e-outage"), Some(&token("u", None, 60)))
        .await
        .unwrap();
    welcome(&mut ws).await;
    ws.send(Message::text(
        json!({"id": 1, "value": "before"}).to_string(),
    ))
    .await
    .unwrap();
    assert_eq!(next_json(&mut ws).await["type"], "ack");

    env.stop_broker().await;
    ws.send(Message::text(
        json!({"id": 2, "value": "during"}).to_string(),
    ))
    .await
    .unwrap();
    let frame = next_json(&mut ws).await;
    assert_eq!(
        frame["type"], "error",
        "a publish fails fast rather than hanging: {frame}"
    );
    assert_eq!(frame["retryable"], true, "{frame}");

    // After three failed probes the gateway reports not-ready and refuses
    // new sockets, so a load balancer sends clients elsewhere.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while env.gw().ready() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "gateway never went unready"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let readyz = http_get(env.gw().http_addr, "/readyz").await;
    assert!(readyz.starts_with("HTTP/1.1 503"), "{readyz}");
    assert_eq!(
        status_of(connect(&env.url("topic=e2e-outage"), Some(&token("u", None, 60))).await),
        503
    );
    let metrics = http_get(env.gw().http_addr, "/metrics").await;
    assert!(metrics.contains("ws_ready 0"), "{metrics}");
    let gateway = env.gateway.take().unwrap();
    gateway.shutdown().await;
}

async fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
    let mut out = String::new();
    stream.read_to_string(&mut out).await.unwrap();
    out
}

#[tokio::test]
async fn tls_listener_serves_wss_and_refuses_plaintext() {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, certified.cert.pem()).unwrap();
    std::fs::write(&key_path, certified.key_pair.serialize_pem()).unwrap();
    let env = Env::start(|c| {
        c.tls_cert = Some(cert_path.clone());
        c.tls_key = Some(key_path.clone());
    })
    .await;

    let mut roots = rustls::RootCertStore::empty();
    roots.add(certified.cert.der().clone()).unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client));
    let tcp = TcpStream::connect(env.gw().ws_addr).await.unwrap();
    let tls = connector
        .connect("localhost".try_into().unwrap(), tcp)
        .await
        .expect("TLS handshake with the gateway");
    let url = format!(
        "wss://localhost:{}/ws?topic=e2e-tls",
        env.gw().ws_addr.port()
    );
    let mut request = url.into_client_request().unwrap();
    request.headers_mut().insert(
        "authorization",
        HeaderValue::from_str(&format!("Bearer {}", token("secure", None, 60))).unwrap(),
    );
    let (mut ws, _) = tokio_tungstenite::client_async(request, tls).await.unwrap();
    let hello = next_json_any(&mut ws).await;
    assert_eq!(hello["type"], "welcome");
    ws.send(Message::text(
        json!({"id": 1, "value": "over tls"}).to_string(),
    ))
    .await
    .unwrap();
    let ack = next_json_any(&mut ws).await;
    assert_eq!(ack["type"], "ack", "{ack}");
    let log = env.read_topic("e2e-tls").await;
    assert_eq!(log.values().map(Vec::len).sum::<usize>(), 1);

    // A plaintext upgrade against the TLS listener goes nowhere.
    assert!(
        connect(&env.url("topic=e2e-tls"), Some(&token("u", None, 60)))
            .await
            .is_err()
    );
    env.finish().await;
}

async fn next_json_any<S>(ws: &mut WebSocketStream<S>) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        match tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .expect("frame within 10s")
            .expect("open")
            .expect("frame")
        {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            _ => continue,
        }
    }
}

// ---------------------------------------------------------------------------
// Subscriptions: broker topics fanned out to sockets (e.g. a price feed).
// ---------------------------------------------------------------------------

/// A token with explicit publish (`topics`) and `subscribe` claims.
fn token_with(sub: &str, topics: Option<&[&str]>, subscribe: Option<&[&str]>) -> String {
    let now = brahmaputra_ws_gateway::auth::now_secs();
    let list = |t: &[&str]| t.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let claims = Claims {
        sub: sub.into(),
        exp: now + 300,
        nbf: None,
        iat: Some(now),
        iss: None,
        aud: None,
        topics: topics.map(list),
        subscribe: subscribe.map(list),
    };
    sign_hs256(&claims, None, SECRET.as_bytes())
}

async fn send_json(ws: &mut Ws, frame: Value) {
    ws.send(Message::text(frame.to_string())).await.unwrap();
}

/// Subscribe and return the confirmation plus the snapshot records.
async fn subscribe(ws: &mut Ws, frame: Value) -> (Value, Vec<Value>) {
    send_json(ws, frame).await;
    let confirmed = next_json(ws).await;
    assert_eq!(confirmed["type"], "subscribed", "{confirmed}");
    let n = confirmed["snapshot"].as_u64().unwrap();
    let mut snapshot = Vec::new();
    for _ in 0..n {
        let record = next_json(ws).await;
        assert_eq!(record["type"], "record", "{record}");
        snapshot.push(record);
    }
    (confirmed, snapshot)
}

async fn next_record(ws: &mut Ws) -> Value {
    let frame = next_json(ws).await;
    assert_eq!(frame["type"], "record", "{frame}");
    frame
}

/// No frame (other than pings) arrives within `wait`.
async fn assert_quiet(ws: &mut Ws, wait: Duration) {
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Err(_) => return,
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => continue,
            Ok(other) => panic!("expected silence, got {other:?}"),
        }
    }
}

async fn broker_producer(env: &Env) -> brahmaputra_client::Producer {
    brahmaputra_client::Producer::connect(
        env.broker_addr,
        brahmaputra_client::ProducerConfig {
            linger_ms: 0,
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

async fn price(
    producer: &brahmaputra_client::Producer,
    topic: &str,
    symbol: &str,
    value: &str,
) -> (i32, i64) {
    let partition = partition_for(symbol.as_bytes());
    let offset = producer
        .send(
            topic,
            Some(partition),
            Some(bytes::Bytes::from(symbol.to_owned())),
            bytes::Bytes::from(value.to_owned()),
        )
        .await
        .unwrap();
    (partition, offset)
}

/// The full loop: a trading back end writes prices to Brahmaputra, a UI
/// publishes through the gateway, and subscribed UIs receive both, with a
/// snapshot of the latest price per symbol first and key filters applied.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribers_get_a_snapshot_then_the_live_feed_filtered_by_key() {
    let env = Env::start(|c| c.allowed_subscribe = vec!["prices.*".into()]).await;
    let topic = "prices.us";
    let producer = broker_producer(&env).await;
    // History from before anyone subscribed: the latest per key is the
    // snapshot; AAPL's first price is superseded.
    price(&producer, topic, "AAPL", "99.00").await;
    let aapl = price(&producer, topic, "AAPL", "100.00").await;
    let msft = price(&producer, topic, "MSFT", "200.00").await;

    // A viewer: may subscribe to prices, may publish nowhere.
    let viewer_token = token_with("viewer-1", Some(&[]), Some(&["prices.*"]));
    let mut all = connect(&env.url(""), Some(&viewer_token)).await.unwrap();
    let hello = welcome(&mut all).await;
    assert_eq!(hello["subscribe"], true);
    let (confirmed, snapshot) = subscribe(
        &mut all,
        json!({"op":"subscribe","id":1,"topic":topic,"snapshot":true}),
    )
    .await;
    assert_eq!(confirmed["id"], 1);
    assert_eq!(confirmed["topic"], topic);
    let snap: HashMap<String, (String, i64, i64)> = snapshot
        .iter()
        .map(|r| {
            (
                r["key"].as_str().unwrap().to_owned(),
                (
                    r["value"].as_str().unwrap().to_owned(),
                    r["partition"].as_i64().unwrap(),
                    r["offset"].as_i64().unwrap(),
                ),
            )
        })
        .collect();
    assert_eq!(snap.len(), 2, "latest per key: {snapshot:?}");
    assert_eq!(snap["AAPL"], ("100.00".into(), aapl.0 as i64, aapl.1));
    assert_eq!(snap["MSFT"], ("200.00".into(), msft.0 as i64, msft.1));

    // A second viewer watching only AAPL, without a snapshot.
    let mut one = connect(&env.url(""), Some(&viewer_token)).await.unwrap();
    welcome(&mut one).await;
    let (_, snapshot) = subscribe(
        &mut one,
        json!({"op":"subscribe","topic":topic,"keys":["AAPL"]}),
    )
    .await;
    assert!(snapshot.is_empty());

    // Live ticks.
    let ticks = [("AAPL", "101.00"), ("MSFT", "201.00"), ("GOOG", "300.00")];
    let mut sent = Vec::new();
    for (symbol, value) in ticks {
        sent.push((symbol, value, price(&producer, topic, symbol, value).await));
    }
    let mut seen = HashMap::new();
    for _ in 0..ticks.len() {
        let r = next_record(&mut all).await;
        seen.insert(r["key"].as_str().unwrap().to_owned(), r);
    }
    for (symbol, value, (partition, offset)) in &sent {
        let r = &seen[*symbol];
        assert_eq!(r["value"], *value);
        assert_eq!(r["partition"], *partition);
        assert_eq!(r["offset"], *offset);
        assert!(r["timestamp"].as_i64().unwrap() > 0);
    }
    let r = next_record(&mut one).await;
    assert_eq!(
        (r["key"].as_str(), r["value"].as_str()),
        (Some("AAPL"), Some("101.00"))
    );
    assert_quiet(&mut one, Duration::from_millis(600)).await;

    // The viewer's token forbids publishing.
    send_json(
        &mut all,
        json!({"id":9,"topic":topic,"key":"AAPL","value":"1"}),
    )
    .await;
    let refused = next_json(&mut all).await;
    assert_eq!(refused["code"], "TOPIC_NOT_ALLOWED", "{refused}");

    // A UI that may publish: its message goes UI -> gateway -> broker ->
    // gateway -> every subscribed UI, stamped with the publisher's identity.
    let trader = token_with("trader-7", Some(&["prices.*"]), None);
    let mut publisher = connect(&env.url(&format!("topic={topic}")), Some(&trader))
        .await
        .unwrap();
    welcome(&mut publisher).await;
    send_json(
        &mut publisher,
        json!({"id":1,"key":"AAPL","value":"102.50"}),
    )
    .await;
    let ack = next_json(&mut publisher).await;
    assert_eq!(ack["type"], "ack", "{ack}");
    for ws in [&mut all, &mut one] {
        let r = next_record(ws).await;
        assert_eq!(r["value"], "102.50");
        assert_eq!(r["offset"], ack["offset"]);
        assert_eq!(r["partition"], ack["partition"]);
        assert_eq!(r["headers"]["x-gw-user"], "trader-7");
    }

    // Tombstones travel as null and leave the snapshot.
    producer
        .send_tombstone(
            topic,
            Some(partition_for(b"MSFT")),
            bytes::Bytes::from_static(b"MSFT"),
        )
        .await
        .unwrap();
    let r = next_record(&mut all).await;
    assert_eq!(r["key"], "MSFT");
    assert!(r["value"].is_null());
    let mut late = connect(&env.url(""), Some(&viewer_token)).await.unwrap();
    welcome(&mut late).await;
    let (_, snapshot) = subscribe(
        &mut late,
        json!({"op":"subscribe","topic":topic,"snapshot":true}),
    )
    .await;
    let keys: Vec<&str> = snapshot
        .iter()
        .map(|r| r["key"].as_str().unwrap())
        .collect();
    assert!(
        !keys.contains(&"MSFT"),
        "deleted key left the snapshot: {keys:?}"
    );
    assert!(keys.contains(&"AAPL") && keys.contains(&"GOOG"), "{keys:?}");
    let aapl_now = snapshot.iter().find(|r| r["key"] == "AAPL").unwrap();
    assert_eq!(aapl_now["value"], "102.50");

    // Unsubscribing stops delivery.
    send_json(&mut one, json!({"op":"unsubscribe","id":5,"topic":topic})).await;
    let bye = next_json(&mut one).await;
    assert_eq!(
        (bye["type"].as_str(), bye["id"].as_u64()),
        (Some("unsubscribed"), Some(5))
    );
    price(&producer, topic, "AAPL", "103.00").await;
    let r = next_record(&mut all).await;
    assert_eq!(r["value"], "103.00");
    assert_quiet(&mut one, Duration::from_millis(600)).await;

    assert_eq!(env.gw().feed_count(), 1, "one feed for the topic");
    for mut ws in [all, one, late, publisher] {
        let _ = ws.close(None).await;
    }
    env.finish().await;
}

#[tokio::test]
async fn subscriptions_are_authorized_by_the_gateway_and_the_token() {
    // Subscriptions are opt-in: a gateway without --allow-subscribe offers none.
    let env = Env::start(|_| {}).await;
    let mut ws = connect(&env.url(""), Some(&token("u", None, 60)))
        .await
        .unwrap();
    assert_eq!(welcome(&mut ws).await["subscribe"], false);
    send_json(
        &mut ws,
        json!({"op":"subscribe","id":1,"topic":"prices.us"}),
    )
    .await;
    let refused = next_json(&mut ws).await;
    assert_eq!(
        (refused["code"].as_str(), refused["id"].as_u64()),
        (Some("TOPIC_NOT_ALLOWED"), Some(1))
    );
    let _ = ws.close(None).await;
    env.finish().await;

    let env = Env::start(|c| {
        c.allowed_subscribe = vec!["prices.*".into(), "news".into()];
        c.max_subscriptions = 2;
        c.max_subscribe_keys = 3;
    })
    .await;
    // No subscribe claim: whatever the gateway allows.
    let mut ws = connect(&env.url(""), Some(&token("u", None, 60)))
        .await
        .unwrap();
    assert_eq!(welcome(&mut ws).await["subscribe"], true);
    for (topic, allowed) in [
        ("prices.eu", true),
        ("orders", false),
        ("__consumer_offsets", false),
    ] {
        send_json(&mut ws, json!({"op":"subscribe","id":2,"topic":topic})).await;
        let reply = next_json(&mut ws).await;
        if allowed {
            assert_eq!(reply["type"], "subscribed", "{topic}: {reply}");
        } else {
            assert_eq!(reply["code"], "TOPIC_NOT_ALLOWED", "{topic}: {reply}");
        }
    }
    send_json(
        &mut ws,
        json!({"op":"subscribe","id":3,"topic":"news","keys":["a","b","c","d"]}),
    )
    .await;
    assert_eq!(
        next_json(&mut ws).await["code"],
        "BAD_REQUEST",
        "too many keys"
    );
    send_json(
        &mut ws,
        json!({"op":"subscribe","id":3,"topic":"news","keys":[]}),
    )
    .await;
    assert_eq!(
        next_json(&mut ws).await["code"],
        "BAD_REQUEST",
        "empty key list"
    );
    send_json(&mut ws, json!({"op":"subscribe","id":4,"topic":"news"})).await;
    assert_eq!(next_json(&mut ws).await["type"], "subscribed");
    send_json(
        &mut ws,
        json!({"op":"subscribe","id":5,"topic":"prices.us"}),
    )
    .await;
    let reply = next_json(&mut ws).await;
    assert_eq!(reply["code"], "TOO_MANY_SUBSCRIPTIONS", "{reply}");
    // Re-subscribing to a held topic replaces its filter and is not a new one.
    send_json(
        &mut ws,
        json!({"op":"subscribe","id":6,"topic":"news","keys":["a"]}),
    )
    .await;
    assert_eq!(next_json(&mut ws).await["type"], "subscribed");
    send_json(&mut ws, json!({"op":"subscribe","topic":"x","value":"v"})).await;
    assert_eq!(
        next_json(&mut ws).await["code"],
        "BAD_REQUEST",
        "record fields on a subscribe"
    );
    let _ = ws.close(None).await;

    // A subscribe claim narrows the gateway's list.
    let narrow = token_with("u2", None, Some(&["prices.eu"]));
    let mut ws = connect(&env.url(""), Some(&narrow)).await.unwrap();
    welcome(&mut ws).await;
    send_json(&mut ws, json!({"op":"subscribe","topic":"prices.eu"})).await;
    assert_eq!(next_json(&mut ws).await["type"], "subscribed");
    send_json(&mut ws, json!({"op":"subscribe","topic":"prices.us"})).await;
    assert_eq!(next_json(&mut ws).await["code"], "TOPIC_NOT_ALLOWED");
    send_json(&mut ws, json!({"op":"subscribe","topic":"news"})).await;
    assert_eq!(next_json(&mut ws).await["code"], "TOPIC_NOT_ALLOWED");
    let _ = ws.close(None).await;

    // An empty claim rules out every topic.
    let none = token_with("u3", None, Some(&[]));
    let mut ws = connect(&env.url(""), Some(&none)).await.unwrap();
    assert_eq!(welcome(&mut ws).await["subscribe"], false);
    let _ = ws.close(None).await;

    let rejected = env
        .gw()
        .metrics()
        .rejected_subscribe
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(rejected >= 7, "{rejected}");
    env.finish().await;
}

/// Many sockets on one topic cost one feed (one broker connection) and one
/// fetch stream; every subscriber gets every record, in order per
/// partition. The feed stops once the last subscriber leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_feed_serves_every_subscriber_and_stops_when_they_leave() {
    let env = Env::start(|c| {
        c.allowed_subscribe = vec!["*".into()];
        c.feed_idle_secs = 1;
    })
    .await;
    let broker_port = env.broker_addr.port();
    let topic = "ticks";
    const SUBSCRIBERS: usize = 300;
    const TICKS: usize = 200;
    let url = env.url("");
    let mut sockets = Vec::new();
    for s in 0..SUBSCRIBERS {
        let mut ws = connect(&url, Some(&token(&format!("viewer-{s}"), None, 60)))
            .await
            .unwrap();
        welcome(&mut ws).await;
        subscribe(&mut ws, json!({"op":"subscribe","topic":topic})).await;
        sockets.push(ws);
    }
    // Let the feed find the topic's end before the first tick, so every
    // tick counts as live.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = broker_connections(broker_port);
    let producer = broker_producer(&env).await;
    let symbols = [
        "AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA", "IBM",
    ];
    for i in 0..TICKS {
        let symbol = symbols[i % symbols.len()];
        price(&producer, topic, symbol, &format!("{i}")).await;
    }
    let started = std::time::Instant::now();
    let mut readers = Vec::new();
    for mut ws in sockets {
        readers.push(tokio::spawn(async move {
            let mut last: HashMap<i64, i64> = HashMap::new();
            for _ in 0..TICKS {
                let r = next_record(&mut ws).await;
                let partition = r["partition"].as_i64().unwrap();
                let offset = r["offset"].as_i64().unwrap();
                if let Some(prev) = last.insert(partition, offset) {
                    assert!(offset > prev, "per-partition order");
                }
            }
            ws
        }));
    }
    let mut sockets = Vec::new();
    for r in readers {
        sockets.push(r.await.unwrap());
    }
    let delivered = env
        .gw()
        .metrics()
        .records_delivered
        .load(std::sync::atomic::Ordering::Relaxed);
    println!(
        "{SUBSCRIBERS} subscribers x {TICKS} ticks delivered in {:?}; broker connections {before}",
        started.elapsed()
    );
    assert_eq!(delivered as usize, SUBSCRIBERS * TICKS);
    assert_eq!(env.gw().feed_count(), 1);
    assert_eq!(
        env.gw()
            .metrics()
            .feed_records
            .load(std::sync::atomic::Ordering::Relaxed) as usize,
        TICKS,
        "each record read from the broker once"
    );
    // Gateway pool (2 producers + health) + one feed; not one per socket.
    assert!(before <= 6, "broker connections: {before}");

    for mut ws in sockets {
        let _ = ws.close(None).await;
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while env.gw().feed_count() > 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(env.gw().feed_count(), 0, "idle feed stopped");
    let active = env
        .gw()
        .metrics()
        .subscriptions_active
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(active, 0);
    env.finish().await;
}

/// A subscriber that stops reading does not hold the feed back: once it
/// is further behind than the feed buffer it is told how many records it
/// skipped, and received + skipped accounts for every record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_subscriber_skips_ahead_and_is_told_how_far() {
    let env = Env::start(|c| {
        c.allowed_subscribe = vec!["*".into()];
        c.feed_buffer = 16;
        c.write_timeout_secs = 60;
    })
    .await;
    let topic = "fast-feed";
    let mut slow = connect(&env.url(""), Some(&token("slow", None, 60)))
        .await
        .unwrap();
    welcome(&mut slow).await;
    subscribe(&mut slow, json!({"op":"subscribe","topic":topic})).await;
    let mut fast = connect(&env.url(""), Some(&token("fast", None, 60)))
        .await
        .unwrap();
    welcome(&mut fast).await;
    subscribe(&mut fast, json!({"op":"subscribe","topic":topic})).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Enough data to fill both TCP buffers between the gateway and the
    // stalled client, so the gateway's writes to it block.
    const RECORDS: usize = 4000;
    let value = "x".repeat(8 * 1024);
    let producer = broker_producer(&env).await;
    // The fast subscriber may lag too under a burst this size against a
    // 16-record buffer; what matters is that it is never held back.
    let fast_reader = tokio::spawn(async move {
        let mut seen = 0u64;
        while seen < RECORDS as u64 {
            let frame = next_json(&mut fast).await;
            seen += match frame["type"].as_str() {
                Some("record") => 1,
                Some("lagged") => frame["skipped"].as_u64().unwrap(),
                _ => panic!("{frame}"),
            };
        }
        assert_eq!(seen, RECORDS as u64);
        fast
    });
    for i in 0..RECORDS {
        producer
            .send(
                topic,
                Some((i % PARTITIONS as usize) as i32),
                None,
                bytes::Bytes::from(value.clone()),
            )
            .await
            .unwrap();
    }
    let fast = tokio::time::timeout(Duration::from_secs(60), fast_reader)
        .await
        .expect("the fast subscriber is not held back")
        .unwrap();

    let (mut received, mut skipped) = (0u64, 0u64);
    while received + skipped < RECORDS as u64 {
        let frame = next_json(&mut slow).await;
        match frame["type"].as_str() {
            Some("record") => received += 1,
            Some("lagged") => {
                assert_eq!(frame["topic"], topic);
                skipped += frame["skipped"].as_u64().unwrap();
            }
            other => panic!("unexpected {other:?}: {frame}"),
        }
    }
    println!("slow subscriber: {received} received, {skipped} skipped");
    assert!(skipped > 0, "the slow subscriber lagged");
    assert_eq!(received + skipped, RECORDS as u64);
    assert!(
        env.gw()
            .metrics()
            .records_lagged
            .load(std::sync::atomic::Ordering::Relaxed)
            >= skipped
    );
    for mut ws in [slow, fast] {
        let _ = ws.close(None).await;
    }
    env.finish().await;
}
