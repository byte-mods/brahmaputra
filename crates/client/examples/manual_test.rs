//! End-to-end check of the Rust client against a live broker, ported from
//! the Go driver's `cmd/manualtest` so the drivers are held to the same
//! checks.
//!
//! ```text
//! brahmaputra-server --data-dir ./data --default-partitions 4
//! cargo run --release -p brahmaputra-client --example manual_test -- 127.0.0.1 9092
//! ```
//!
//! Every check asserts a property of the system, not that a function ran:
//! records come back byte-identical, keys pin partitions, headers survive,
//! offsets are contiguous, a group splits partitions and resumes from its
//! commit. Prints "N passed, M failed" and exits non-zero on any failure.

use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use brahmaputra_client::{
    murmur2, Assignor, AutoOffsetReset, ClientError, ConsumedRecord, Consumer, GroupAdmin,
    GroupConsumer, Producer, ProducerConfig, EARLIEST, LATEST,
};
use brahmaputra_protocol::{Compression, RecordHeader};
use bytes::Bytes;
use futures::future::join_all;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

static PASSED: AtomicUsize = AtomicUsize::new(0);
static FAILED: AtomicUsize = AtomicUsize::new(0);
static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn check(name: &str, ok: bool, detail: impl std::fmt::Display) {
    if ok {
        PASSED.fetch_add(1, Ordering::Relaxed);
        println!("  ok   {name}");
    } else {
        FAILED.fetch_add(1, Ordering::Relaxed);
        let detail = detail.to_string();
        if detail.is_empty() {
            println!("  FAIL {name}");
        } else {
            println!("  FAIL {name}: {detail}");
        }
    }
}

fn section(title: &str) {
    println!("\n{title}");
}

fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() % 1_000_000_000)
        .unwrap_or(0);
    format!(
        "{prefix}-{nanos}-{}",
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

/// Unwrap or abort the run: a setup step failing means the checks after it
/// would only report noise.
fn must<T, E: std::fmt::Display>(result: Result<T, E>, what: &str) -> T {
    match result {
        Ok(value) => value,
        Err(error) => {
            println!("  FATAL {what}: {error}");
            std::process::exit(2);
        }
    }
}

fn config(linger_ms: u64) -> ProducerConfig {
    ProducerConfig {
        client_id: "rust-manualtest".into(),
        linger_ms,
        compression: Compression::None,
        ..ProducerConfig::default()
    }
}

fn b(text: &str) -> Bytes {
    Bytes::copy_from_slice(text.as_bytes())
}

async fn producer(addr: SocketAddr, linger_ms: u64) -> Producer {
    must(
        Producer::connect(addr, config(linger_ms)).await,
        "producer connect",
    )
}

async fn consumer(addr: SocketAddr) -> Consumer {
    must(
        Consumer::connect(addr, "rust-manualtest").await,
        "consumer connect",
    )
}

/// Everything in one partition from offset 0, following the log to its end.
async fn fetch_all(
    consumer: &Consumer,
    topic: &str,
    partition: i32,
    want: usize,
) -> Vec<brahmaputra_client::FetchedRecord> {
    let mut out = Vec::new();
    let mut offset = 0;
    while out.len() < want {
        let batch = must(consumer.fetch(topic, partition, offset, 500).await, "fetch");
        let Some(last) = batch.last() else { break };
        offset = last.offset + 1;
        out.extend(batch);
    }
    out
}

async fn partitions_of(consumer: &Consumer, topic: &str) -> Vec<i32> {
    let metadata = must(consumer.metadata(&[topic.to_owned()]).await, "metadata");
    let mut partitions: Vec<i32> = metadata
        .topics
        .iter()
        .filter(|entry| entry.name == topic)
        .flat_map(|entry| entry.partitions.iter().map(|info| info.partition))
        .collect();
    partitions.sort_unstable();
    partitions
}

fn partition_for_key(key: &[u8], partitions: &[i32]) -> i32 {
    partitions[(murmur2(key) & 0x7fff_ffff) as usize % partitions.len()]
}

async fn group(addr: SocketAddr, group_id: &str) -> GroupConsumer {
    must(
        GroupConsumer::connect(addr, &unique("rust-member"), group_id).await,
        "group connect",
    )
    .with_auto_commit(None)
}

async fn poll_until(
    consumer: &mut GroupConsumer,
    want: usize,
    within: Duration,
) -> Result<Vec<ConsumedRecord>, ClientError> {
    let mut seen = Vec::new();
    let deadline = Instant::now() + within;
    while seen.len() < want && Instant::now() < deadline {
        seen.extend(consumer.poll(Duration::from_millis(300)).await?);
    }
    Ok(seen)
}

async fn committed_total(addr: SocketAddr, group_id: &str) -> i64 {
    let admin = must(
        GroupAdmin::connect(addr, "rust-manualtest-admin").await,
        "admin",
    );
    let description = must(admin.describe_group(group_id).await, "describe group");
    description
        .offsets
        .iter()
        .map(|(_, _, offset)| (*offset).max(0))
        .sum()
}

/// A TCP forwarder that can sever every live connection, which is how a
/// broker restart or an idle timeout looks to a client.
struct Proxy {
    addr: SocketAddr,
    live: Arc<Mutex<Vec<JoinHandle<()>>>>,
    accept: JoinHandle<()>,
}

impl Proxy {
    async fn start(target: SocketAddr) -> Proxy {
        let listener = must(TcpListener::bind("127.0.0.1:0").await, "proxy bind");
        let addr = must(listener.local_addr(), "proxy addr");
        let live: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::default();
        let tracked = Arc::clone(&live);
        let accept = tokio::spawn(async move {
            while let Ok((mut client, _)) = listener.accept().await {
                let Ok(mut upstream) = TcpStream::connect(target).await else {
                    continue;
                };
                let pipe = tokio::spawn(async move {
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
                });
                tracked.lock().expect("proxy").push(pipe);
            }
        });
        Proxy { addr, live, accept }
    }

    async fn drop_all(&self) {
        for pipe in self.live.lock().expect("proxy").drain(..) {
            pipe.abort();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    async fn close(self) {
        self.accept.abort();
        self.drop_all().await;
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let host = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1".into());
    let port = args.get(2).cloned().unwrap_or_else(|| "9092".into());
    let addr: SocketAddr = must(
        tokio::net::lookup_host(format!("{host}:{port}"))
            .await
            .map_err(|error| error.to_string())
            .and_then(|mut addrs| addrs.next().ok_or_else(|| "no address".to_owned())),
        "resolve broker",
    );

    section("connection and metadata");
    {
        let consumer = consumer(addr).await;
        let versions = consumer.api_versions().await;
        check(
            "ApiVersions answers",
            versions
                .as_ref()
                .is_ok_and(|versions| !versions.api_versions.is_empty()),
            versions
                .as_ref()
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default(),
        );
        let broker_version = versions.map(|v| v.broker_version).unwrap_or_default();
        check(
            "broker reports a version",
            !broker_version.is_empty(),
            &broker_version,
        );
        let metadata = must(consumer.metadata(&[]).await, "metadata");
        check(
            "metadata lists brokers",
            !metadata.brokers.is_empty(),
            format!("{} brokers", metadata.brokers.len()),
        );
    }

    section("produce and consume round trip");
    let topic = unique("rust-roundtrip");
    let payloads: Vec<Bytes> = (0..50).map(|i| b(&format!("record-{i}"))).collect();
    {
        let producer = producer(addr, 0).await;
        for payload in &payloads {
            must(
                producer.send(&topic, Some(0), None, payload.clone()).await,
                "send",
            );
        }
        must(producer.flush().await, "flush");
    }
    {
        let consumer = consumer(addr).await;
        let got = fetch_all(&consumer, &topic, 0, payloads.len()).await;
        check(
            "every record comes back",
            got.len() == payloads.len(),
            format!("got {}", got.len()),
        );
        let identical = got.len() == payloads.len()
            && got.iter().enumerate().all(|(i, record)| {
                record.value.as_ref() == Some(&payloads[i]) && record.offset == i as i64
            });
        check(
            "values byte-identical and offsets contiguous",
            identical,
            "",
        );
    }

    section("compression codecs");
    for (name, codec) in [
        ("none", Compression::None),
        ("gzip", Compression::Gzip),
        ("lz4", Compression::Lz4),
    ] {
        let codec_topic = unique(&format!("rust-{name}"));
        let body = "the same line over and over. ".repeat(40);
        let producer = must(
            Producer::connect(
                addr,
                ProducerConfig {
                    compression: codec,
                    ..config(0)
                },
            )
            .await,
            "producer connect",
        );
        for i in 0..20 {
            must(
                producer
                    .send(&codec_topic, Some(0), None, b(&format!("{body}{}", i % 10)))
                    .await,
                "send",
            );
        }
        let consumer = consumer(addr).await;
        let got = fetch_all(&consumer, &codec_topic, 0, 20).await;
        check(
            &format!("{name}: round trips"),
            got.len() == 20
                && got[0]
                    .value
                    .as_ref()
                    .is_some_and(|value| value.starts_with(body.as_bytes())),
            format!("got {} records", got.len()),
        );
    }

    section("keys, partitioning and ordering");
    {
        let key_topic = unique("rust-keys");
        let producer = producer(addr, 0).await;
        for i in 0..30 {
            must(
                producer
                    .send(&key_topic, None, Some(b("user-7")), b(&format!("v{i}")))
                    .await,
                "send",
            );
        }
        let consumer = consumer(addr).await;
        let partitions = partitions_of(&consumer, &key_topic).await;
        let target = partition_for_key(b"user-7", &partitions);
        let on_target = fetch_all(&consumer, &key_topic, target, 30).await;
        check(
            "a key pins every record to one partition",
            on_target.len() == 30,
            format!("partition {target} holds {} of 30", on_target.len()),
        );
        let ordered = on_target.len() == 30
            && on_target
                .iter()
                .enumerate()
                .all(|(i, record)| record.value.as_ref() == Some(&b(&format!("v{i}"))));
        check("per-key order is preserved", ordered, "");
        let mut strays = 0;
        for partition in partitions.iter().filter(|p| **p != target) {
            strays += must(
                consumer.fetch(&key_topic, *partition, 0, 200).await,
                "fetch",
            )
            .len();
        }
        check(
            "no keyed record landed elsewhere",
            strays == 0,
            format!("{strays} strays"),
        );
    }

    section("murmur2 agrees with the broker's partitioner");
    check(
        "murmur2(\"\") is stable",
        murmur2(b"") == 275_646_681,
        murmur2(b""),
    );
    check(
        "murmur2 is deterministic",
        murmur2(b"user-7") == murmur2(b"user-7"),
        "",
    );
    check(
        "different keys hash differently",
        murmur2(b"user-7") != murmur2(b"user-8"),
        "",
    );

    section("record headers and timestamps");
    {
        let header_topic = unique("rust-headers");
        let before = now_ms() - 1000;
        let producer = producer(addr, 0).await;
        must(
            producer
                .send_with_headers(
                    &header_topic,
                    Some(0),
                    None,
                    b("annotated"),
                    vec![
                        RecordHeader::new("trace-id", b("abc-123")),
                        RecordHeader::new("content-type", b("application/json")),
                        RecordHeader {
                            key: "tombstone-reason".into(),
                            value: None,
                        },
                    ],
                )
                .await,
            "send",
        );
        must(
            producer
                .send(&header_topic, Some(0), None, b("plain"))
                .await,
            "send",
        );
        let after = now_ms() + 1000;
        let consumer = consumer(addr).await;
        let got = fetch_all(&consumer, &header_topic, 0, 2).await;
        check(
            "both records arrive",
            got.len() == 2,
            format!("got {}", got.len()),
        );
        if got.len() == 2 {
            let (annotated, plain) = (&got[0], &got[1]);
            check(
                "headers survive the round trip",
                annotated.headers.len() == 3,
                format!("{} headers", annotated.headers.len()),
            );
            check(
                "header values are exact",
                annotated
                    .headers
                    .iter()
                    .find(|header| header.key == "trace-id")
                    .and_then(|header| header.value.as_ref())
                    == Some(&b("abc-123")),
                "",
            );
            check(
                "a null header value stays null",
                annotated.headers.len() == 3 && annotated.headers[2].value.is_none(),
                "",
            );
            check(
                "a record with no headers gains none from its batch",
                plain.headers.is_empty(),
                format!("{} headers", plain.headers.len()),
            );
            check(
                "timestamps are real wall-clock values",
                got.iter()
                    .all(|record| record.timestamp >= before && record.timestamp <= after),
                format!(
                    "{},{} outside {before}..{after}",
                    got[0].timestamp, got[1].timestamp
                ),
            );
        }
    }

    section("tombstones");
    {
        let tomb_topic = unique("rust-tombstones");
        let producer = producer(addr, 0).await;
        must(
            producer
                .send(&tomb_topic, Some(0), Some(b("k1")), b("set"))
                .await,
            "send",
        );
        must(
            producer
                .send(&tomb_topic, Some(0), Some(b("k2")), Bytes::new())
                .await,
            "send",
        );
        // A null value is a deletion, and must stay distinguishable from
        // the empty value above all the way through the round trip.
        must(
            producer.send_tombstone(&tomb_topic, Some(0), b("k3")).await,
            "send",
        );
        let consumer = consumer(addr).await;
        let got = fetch_all(&consumer, &tomb_topic, 0, 3).await;
        check(
            "all three records arrive",
            got.len() == 3,
            format!("got {}", got.len()),
        );
        if got.len() == 3 {
            check(
                "an ordinary value round-trips",
                got[0].value == Some(b("set")),
                "",
            );
            check(
                "an empty value is empty, not null",
                got[1].value.as_ref().is_some_and(Bytes::is_empty),
                format!("{:?}", got[1].value),
            );
            check(
                "a tombstone arrives as a null value",
                got[2].value.is_none(),
                format!("{:?}", got[2].value),
            );
        }
    }

    section("offsets");
    {
        let consumer = consumer(addr).await;
        let earliest = must(
            consumer.list_offsets(&topic, 0, EARLIEST).await,
            "list offsets",
        );
        let latest = must(
            consumer.list_offsets(&topic, 0, LATEST).await,
            "list offsets",
        );
        check("earliest is 0 on a fresh topic", earliest == 0, earliest);
        check("latest equals the record count", latest == 50, latest);
    }

    section("acks");
    for acks in [0, 1, -1] {
        let acks_topic = unique(&format!("rust-acks{acks}"));
        let producer = must(
            Producer::connect(addr, ProducerConfig { acks, ..config(0) }).await,
            "producer connect",
        );
        must(
            producer
                .send(&acks_topic, Some(0), None, b("durable"))
                .await,
            "send",
        );
        must(producer.flush().await, "flush");
        tokio::time::sleep(Duration::from_millis(400)).await;
        let consumer = consumer(addr).await;
        let got = must(consumer.fetch(&acks_topic, 0, 0, 500).await, "fetch");
        check(
            &format!("acks={acks} stores the record"),
            got.len() == 1,
            format!("got {}", got.len()),
        );
    }

    section("consumer group: assignment, commit, resume");
    {
        let group_topic = unique("rust-group");
        let group_id = unique("rust-billing");
        let producer = producer(addr, 0).await;
        for i in 0..40 {
            must(
                producer
                    .send(&group_topic, None, None, b(&format!("g{i}")))
                    .await,
                "send",
            );
        }
        let mut consumer = group(addr, &group_id).await;
        consumer.subscribe(&[&group_topic]);
        let seen = must(
            poll_until(&mut consumer, 40, Duration::from_secs(30)).await,
            "poll",
        );
        check(
            "the group consumes every record",
            seen.len() == 40,
            format!("got {}", seen.len()),
        );
        let distinct: HashSet<(i32, i64)> = seen
            .iter()
            .map(|record| (record.partition, record.offset))
            .collect();
        check(
            "no record is delivered twice",
            distinct.len() == seen.len(),
            "",
        );
        must(consumer.commit_sync().await, "commit");
        let total = committed_total(addr, &group_id).await;
        check("commit records a position", total == 40, total);
        must(consumer.close().await, "close");

        // A second consumer in the same group must resume, not replay.
        let mut rejoined = group(addr, &group_id).await;
        rejoined.subscribe(&[&group_topic]);
        let mut replayed = 0;
        let until = Instant::now() + Duration::from_secs(5);
        while Instant::now() < until {
            replayed += rejoined
                .poll(Duration::from_millis(300))
                .await
                .map(|records| records.len())
                .unwrap_or(0);
        }
        check(
            "a rejoining group resumes from its commit",
            replayed == 0,
            format!("replayed {replayed} records it had already committed"),
        );
        must(rejoined.close().await, "close");
    }

    section("auto.offset.reset");
    {
        let reset_topic = unique("rust-reset");
        let producer = producer(addr, 0).await;
        for i in 0..10 {
            must(
                producer
                    .send(&reset_topic, None, None, b(&format!("r{i}")))
                    .await,
                "send",
            );
        }
        let mut latest = group(addr, &unique("rust-latest"))
            .await
            .with_auto_offset_reset(AutoOffsetReset::Latest);
        latest.subscribe(&[&reset_topic]);
        let mut skipped = 0;
        let until = Instant::now() + Duration::from_secs(4);
        while Instant::now() < until {
            skipped += latest
                .poll(Duration::from_millis(300))
                .await
                .map(|records| records.len())
                .unwrap_or(0);
        }
        check(
            "latest skips records produced before the group existed",
            skipped == 0,
            format!("saw {skipped}"),
        );
        must(latest.close().await, "close");

        let mut strict = group(addr, &unique("rust-none"))
            .await
            .with_auto_offset_reset(AutoOffsetReset::None);
        strict.subscribe(&[&reset_topic]);
        let mut raised = false;
        let until = Instant::now() + Duration::from_secs(5);
        while Instant::now() < until && !raised {
            if let Err(error) = strict.poll(Duration::from_millis(300)).await {
                raised = matches!(error, ClientError::NoOffsetForPartition { .. });
            }
        }
        check("none refuses to guess a position", raised, "");
        let _ = strict.close().await;
    }

    section("assignors");
    for (name, assignor) in [
        ("range", Assignor::Range),
        ("roundrobin", Assignor::RoundRobin),
        ("sticky", Assignor::Sticky),
        ("cooperative-sticky", Assignor::CooperativeSticky),
    ] {
        let assignor_topic = unique(&format!("rust-{name}"));
        let producer = producer(addr, 0).await;
        for i in 0..20 {
            must(
                producer
                    .send(&assignor_topic, None, None, b(&format!("a{i}")))
                    .await,
                "send",
            );
        }
        let mut consumer = group(addr, &unique(&format!("rust-grp-{name}")))
            .await
            .with_assignor(assignor);
        consumer.subscribe(&[&assignor_topic]);
        let collected = poll_until(&mut consumer, 20, Duration::from_secs(20))
            .await
            .unwrap_or_default();
        check(
            &format!("{name}: consumes every record"),
            collected.len() == 20,
            format!("got {}", collected.len()),
        );
        must(consumer.close().await, "close");
    }

    section("bounded client buffer");
    {
        let buffer_topic = unique("rust-buffer");
        let producer = Arc::new(must(
            Producer::connect(
                addr,
                ProducerConfig {
                    linger_ms: 10_000, // never flush on time during this check
                    buffer_memory: 2048,
                    max_block_ms: 300,
                    ..config(0)
                },
            )
            .await,
            "producer connect",
        ));
        // `send` awaits its offset, so filling the buffer takes many sends
        // in flight at once.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut tasks = Vec::new();
        for _ in 0..40 {
            let producer = Arc::clone(&producer);
            let topic = buffer_topic.clone();
            let tx = tx.clone();
            tasks.push(tokio::spawn(async move {
                let result = producer
                    .send(&topic, Some(0), None, Bytes::from(vec![b'x'; 256]))
                    .await;
                let _ = tx.send(result);
            }));
        }
        let mut blocked = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while !blocked {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(Err(error))) => blocked = error.to_string().contains("buffer full"),
                Ok(Some(Ok(_))) => {}
                _ => break,
            }
        }
        check("a full buffer blocks and then reports", blocked, "");
        for task in tasks {
            task.abort();
        }
    }

    section("wire edge cases");
    {
        let edge_topic = unique("rust-edge");
        let producer = producer(addr, 0).await;
        let large: Bytes = (0..(1usize << 20)).map(|i| (i * 7) as u8).collect();
        let unicode_key = b("ключ-✓-🔑");
        let unicode_value = b("значение — 数据 — 🚀");
        must(
            producer
                .send(&edge_topic, Some(0), None, large.clone())
                .await,
            "send large",
        );
        must(
            producer
                .send_with_headers(
                    &edge_topic,
                    Some(0),
                    Some(unicode_key.clone()),
                    unicode_value.clone(),
                    vec![RecordHeader::new("ünïcødé-🏷", b("✓"))],
                )
                .await,
            "send unicode",
        );
        // An empty key and an empty header value are values, not nulls.
        must(
            producer
                .send_with_headers(
                    &edge_topic,
                    Some(0),
                    Some(Bytes::new()),
                    b("empty-key"),
                    vec![
                        RecordHeader::new("empty", Bytes::new()),
                        RecordHeader {
                            key: "null".into(),
                            value: None,
                        },
                    ],
                )
                .await,
            "send empty",
        );
        must(
            producer
                .send(&edge_topic, Some(0), None, b("null-key"))
                .await,
            "send",
        );
        let consumer = consumer(addr).await;
        let got = fetch_all(&consumer, &edge_topic, 0, 4).await;
        check(
            "edge records all arrive",
            got.len() == 4,
            format!("got {}", got.len()),
        );
        if got.len() == 4 {
            check(
                "a 1 MiB value round-trips byte-identical",
                got[0].value.as_ref() == Some(&large),
                format!("{:?} bytes", got[0].value.as_ref().map(Bytes::len)),
            );
            check(
                "unicode key, value and header key round-trip",
                got[1].key.as_ref() == Some(&unicode_key)
                    && got[1].value.as_ref() == Some(&unicode_value)
                    && got[1].headers.len() == 1
                    && got[1].headers[0].key == "ünïcødé-🏷",
                "",
            );
            check(
                "an empty key stays empty, not null",
                got[2].key.as_ref().is_some_and(Bytes::is_empty),
                format!("{:?}", got[2].key),
            );
            check(
                "an empty header value stays empty, not null",
                got[2].headers.len() == 2
                    && got[2].headers[0]
                        .value
                        .as_ref()
                        .is_some_and(Bytes::is_empty)
                    && got[2].headers[1].value.is_none(),
                format!("{:?}", got[2].headers),
            );
            check(
                "a null key stays null",
                got[3].key.is_none(),
                format!("{:?}", got[3].key),
            );
        }
    }

    section("ordering under linger flushes");
    {
        let order_topic = unique("rust-order");
        let producer = must(
            Producer::connect(
                addr,
                ProducerConfig {
                    batch_size: 256,
                    ..config(1)
                },
            )
            .await,
            "producer connect",
        );
        const TOTAL: usize = 5000;
        let results = join_all(
            (0..TOTAL).map(|i| producer.send(&order_topic, Some(0), None, b(&i.to_string()))),
        )
        .await;
        let send_errors = results.iter().filter(|result| result.is_err()).count();
        // Offsets are handed back per record, so they can be checked
        // against send order directly as well as through a fetch.
        let offsets_in_order = results
            .windows(2)
            .all(|pair| matches!((&pair[0], &pair[1]), (Ok(a), Ok(b)) if a < b));
        let consumer = consumer(addr).await;
        let values: Vec<usize> = fetch_all(&consumer, &order_topic, 0, TOTAL)
            .await
            .iter()
            .filter_map(|record| {
                std::str::from_utf8(record.value.as_ref()?)
                    .ok()?
                    .parse()
                    .ok()
            })
            .collect();
        let inversions = values.windows(2).filter(|pair| pair[1] < pair[0]).count();
        check(
            "every record of a partition arrives",
            values.len() == TOTAL && send_errors == 0,
            format!("got {} ({send_errors} send errors)", values.len()),
        );
        check(
            "a partition's records keep send order",
            inversions == 0 && offsets_in_order,
            format!("{inversions} inversions"),
        );
    }

    section("send failures are reported");
    {
        let producer = producer(addr, 20).await;
        // Partition 999 does not exist; the send waits for its batch and
        // must come back with the failure rather than an offset.
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            producer.send(&unique("rust-bgfail"), Some(999), None, b("lost")),
        )
        .await;
        check(
            "a send to a partition that does not exist fails",
            matches!(result, Ok(Err(_))),
            format!("{result:?}"),
        );
    }

    section("connection failures");
    {
        // A connection the broker drops is redialled, not kept forever.
        let proxy = Proxy::start(addr).await;
        let drop_topic = unique("rust-drop");
        let producer = producer(proxy.addr, 0).await;
        must(
            producer.send(&drop_topic, Some(0), None, b("before")).await,
            "send",
        );
        proxy.drop_all().await;
        let mut recovered = Err("not attempted".to_owned());
        for _ in 0..3 {
            recovered = producer
                .send(&drop_topic, Some(0), None, b("after"))
                .await
                .map_err(|error| error.to_string());
            if recovered.is_ok() {
                break;
            }
        }
        check(
            "a producer recovers after its connection drops",
            recovered.is_ok(),
            format!("{recovered:?}"),
        );
        let consumer = consumer(proxy.addr).await;
        must(consumer.fetch(&drop_topic, 0, 0, 100).await, "fetch");
        proxy.drop_all().await;
        let mut fetched = Err("not attempted".to_owned());
        for _ in 0..3 {
            fetched = consumer
                .fetch(&drop_topic, 0, 0, 100)
                .await
                .map_err(|error| error.to_string());
            if fetched.is_ok() {
                break;
            }
        }
        check(
            "a consumer recovers after its connection drops",
            fetched.as_ref().is_ok_and(|records| !records.is_empty()),
            format!("{:?}", fetched.map(|records| records.len())),
        );
        proxy.close().await;
    }

    section("consumer group: max.poll.interval and rejoin");
    {
        let slow_topic = unique("rust-slow");
        let producer = producer(addr, 0).await;
        for i in 0..10 {
            must(
                producer
                    .send(&slow_topic, None, None, b(&format!("s{i}")))
                    .await,
                "send",
            );
        }
        let mut consumer = group(addr, &unique("rust-slow-grp"))
            .await
            .with_max_poll_interval_ms(1500);
        consumer.subscribe(&[&slow_topic]);
        let first = poll_until(&mut consumer, 10, Duration::from_secs(15))
            .await
            .map(|records| records.len());
        let first_commit = consumer.commit_sync().await;
        // Stall past max.poll.interval.ms: the member leaves the group.
        tokio::time::sleep(Duration::from_millis(2500)).await;
        for i in 10..20 {
            must(
                producer
                    .send(&slow_topic, None, None, b(&format!("s{i}")))
                    .await,
                "send",
            );
        }
        let second = poll_until(&mut consumer, 10, Duration::from_secs(15))
            .await
            .map(|records| records.len());
        check(
            "a member that stalled rejoins on its next poll",
            matches!(first, Ok(10)) && first_commit.is_ok() && matches!(second, Ok(10)),
            format!("first={first:?} commit={first_commit:?} second={second:?}"),
        );
        let _ = consumer.close().await;
    }

    section("consumer group: time inside poll does not count against max.poll.interval");
    {
        let join_topic = unique("rust-inpoll");
        let producer = Arc::new(producer(addr, 0).await);
        // Creates the topic, so the group has partitions to assign.
        must(
            producer.send(&join_topic, Some(0), None, b("seed")).await,
            "send",
        );
        let group_id = unique("rust-inpoll-grp");
        let mut consumer = group(addr, &group_id)
            .await
            .with_auto_offset_reset(AutoOffsetReset::Latest)
            // Far shorter than the poll below, which spends ~1s joining (the
            // broker's initial rebalance delay) and then waits for data.
            .with_max_poll_interval_ms(600);
        consumer.subscribe(&[&join_topic]);
        let background = {
            let producer = Arc::clone(&producer);
            let topic = join_topic.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
                for i in 0..10 {
                    let _ = producer.send(&topic, None, None, b(&format!("j{i}"))).await;
                }
            })
        };
        let got = consumer.poll(Duration::from_secs(4)).await;
        // Committed straight away, before another poll could quietly
        // rejoin: this fails if the member left the group mid-poll.
        let commit = consumer.commit_sync().await;
        let committed = committed_total(addr, &group_id).await;
        check(
            "a member is still in its group after a long poll",
            got.as_ref().is_ok_and(|records| !records.is_empty())
                && commit.is_ok()
                && committed > 0,
            format!(
                "got={:?} commit={commit:?} committed={committed}",
                got.as_ref().map(|records| records.len())
            ),
        );
        let _ = background.await;
        let _ = consumer.close().await;
    }

    section("consumer group: an explicit commit that the coordinator refuses is reported");
    {
        let fence_topic = unique("rust-fence");
        let group_id = unique("rust-fence-grp");
        let producer = producer(addr, 0).await;
        for i in 0..20 {
            must(
                producer
                    .send(&fence_topic, None, None, b(&format!("f{i}")))
                    .await,
                "send",
            );
        }
        let mut first = group(addr, &group_id).await;
        first.subscribe(&[&fence_topic]);
        let consumed = poll_until(&mut first, 20, Duration::from_secs(15))
            .await
            .map(|records| records.len());
        // A second member joining bumps the generation; the first member's
        // next commit is fenced until it rejoins.
        let second_task = {
            let group_id = group_id.clone();
            let topic = fence_topic.clone();
            tokio::spawn(async move {
                let mut second = group(addr, &group_id).await;
                second.subscribe(&[&topic]);
                let until = Instant::now() + Duration::from_secs(6);
                while Instant::now() < until {
                    let _ = second.poll(Duration::from_millis(200)).await;
                }
                let _ = second.close().await;
            })
        };
        tokio::time::sleep(Duration::from_millis(500)).await;
        let fenced = first.commit_sync().await;
        check(
            "commit_sync reports a commit the coordinator fenced",
            matches!(consumed, Ok(20))
                && matches!(
                    fenced,
                    Err(ClientError::Server {
                        code: 13 | 14 | 16,
                        ..
                    })
                ),
            format!("consumed={consumed:?} commit={fenced:?}"),
        );
        // The member rejoins on its next poll and commits normally again.
        let mut rejoined = Ok(());
        let until = Instant::now() + Duration::from_secs(10);
        while Instant::now() < until {
            let _ = first.poll(Duration::from_millis(200)).await;
            rejoined = first.commit_sync().await;
            if rejoined.is_ok() {
                break;
            }
        }
        check(
            "after rejoining, commits succeed again",
            rejoined.is_ok(),
            format!("{rejoined:?}"),
        );
        let _ = second_task.await;
        let _ = first.close().await;
    }

    section("consumer group: re-subscribing keeps the member");
    {
        let sub_topic = unique("rust-resub");
        let producer = producer(addr, 0).await;
        for i in 0..8 {
            must(
                producer
                    .send(&sub_topic, None, None, b(&format!("u{i}")))
                    .await,
                "send",
            );
        }
        let mut consumer = group(addr, &unique("rust-resub-grp")).await;
        consumer.subscribe(&[&sub_topic]);
        let first = poll_until(&mut consumer, 8, Duration::from_secs(15))
            .await
            .map(|records| records.len());
        let member = consumer.member_id();
        consumer.subscribe(&[&sub_topic]);
        let started = Instant::now();
        let again = consumer.poll(Duration::from_millis(100)).await;
        let rejoin_took = started.elapsed();
        let partitions: BTreeMap<i32, ()> = consumer
            .assignment()
            .iter()
            .map(|(_, partition)| (*partition, ()))
            .collect();
        check(
            "a re-subscribed member keeps its id and every partition",
            matches!(first, Ok(8))
                && again.is_ok()
                && consumer.member_id() == member
                && partitions.len() == 4
                && rejoin_took < Duration::from_millis(2500),
            format!(
                "first={first:?} member {member} -> {}, {} partitions, rejoin {rejoin_took:?}",
                consumer.member_id(),
                partitions.len()
            ),
        );
        let _ = consumer.close().await;
    }

    let passed = PASSED.load(Ordering::Relaxed);
    let failed = FAILED.load(Ordering::Relaxed);
    println!("\n{passed} passed, {failed} failed");
    std::process::exit(if failed > 0 { 1 } else { 0 });
}
