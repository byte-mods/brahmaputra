//! End-to-end M1 verification (Blueprint 02, Verification section):
//! real broker on an ephemeral port + tempdir, two independent producers
//! issuing requests concurrently, 1000 exact records across 3 partitions,
//! visible on-disk logs, and continued contiguous offsets after restart.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use brahmaputra_broker::{Broker, BrokerConfig};
use brahmaputra_client::{Consumer, FetchedRecord, Producer, ProducerConfig};
use brahmaputra_protocol::Compression;
use bytes::Bytes;
use tokio::sync::{oneshot, Barrier};
use tokio::task::JoinHandle;

const TOPIC: &str = "it-topic";
const PARTITIONS: i32 = 3;
const RECORDS_PER_PRODUCER: u64 = 500;
const CONTINUATION_RECORDS: u64 = 10;

#[tokio::test]
async fn incremental_fetch_applies_a_changed_byte_budget_at_the_same_offset() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;
    let producer = Producer::connect(
        broker.addr,
        ProducerConfig {
            linger_ms: 0,
            compression: Compression::None,
            ..ProducerConfig::default()
        },
    )
    .await
    .unwrap();
    for _ in 0..4 {
        producer
            .send(TOPIC, Some(0), None, Bytes::from(vec![b'x'; 70_000]))
            .await
            .unwrap();
    }
    let consumer = Consumer::connect(broker.addr, "budget-reader")
        .await
        .unwrap()
        .with_max_bytes(64 * 1024);
    let requests = [(TOPIC.to_owned(), 0, 0)];
    let first = consumer.fetch_many_public(&requests, 0).await.unwrap();
    assert_eq!(first[0].2.len(), 1);
    let consumer = consumer.with_max_bytes(300_000);
    let next = consumer.fetch_many_public(&requests, 0).await.unwrap();
    assert_eq!(
        next[0].2.len(),
        4,
        "an unchanged offset must not hide a changed budget"
    );
    stop_broker(broker).await;
}

#[tokio::test]
async fn multi_fetch_reports_partition_errors_instead_of_empty_success() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;
    let producer = Producer::connect(broker.addr, ProducerConfig::default())
        .await
        .unwrap();
    producer
        .send(TOPIC, Some(0), None, Bytes::from_static(b"record"))
        .await
        .unwrap();
    let consumer = Consumer::connect(broker.addr, "error-reader")
        .await
        .unwrap();
    let error = consumer
        .fetch_many_public(&[(TOPIC.to_owned(), 0, -1)], 0)
        .await
        .unwrap_err();
    assert!(
        matches!(error, brahmaputra_client::ClientError::Server { code, .. }
        if code == brahmaputra_protocol::error_code::OFFSET_OUT_OF_RANGE)
    );
    stop_broker(broker).await;
}

type ExpectedPartition = BTreeMap<i64, (Option<Bytes>, Bytes)>;
type ExpectedRecords = HashMap<i32, ExpectedPartition>;

struct RunningBroker {
    addr: std::net::SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

async fn start_broker(data_dir: &std::path::Path) -> RunningBroker {
    let config = BrokerConfig {
        port: 0, // ephemeral
        data_dirs: vec![data_dir.to_path_buf()],
        default_partitions: PARTITIONS,
        ..BrokerConfig::default()
    };
    let broker = Broker::bind(config).await.expect("bind broker");
    let addr = broker.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        Arc::new(broker)
            .run(async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("broker run");
    });
    RunningBroker {
        addr,
        shutdown: shutdown_tx,
        task,
    }
}

async fn stop_broker(broker: RunningBroker) {
    let _ = broker.shutdown.send(());
    broker.task.await.expect("broker task");
}

async fn read_all(consumer: &Consumer, partition: i32) -> Vec<FetchedRecord> {
    let mut out = Vec::new();
    let mut offset = 0;
    loop {
        let records = consumer
            .fetch(TOPIC, partition, offset, 200)
            .await
            .expect("fetch");
        if records.is_empty() {
            break;
        }
        offset = records.last().unwrap().offset + 1;
        out.extend(records);
    }
    out
}

fn assert_partition(records: &[FetchedRecord], partition: i32, expected: &ExpectedPartition) {
    assert_eq!(
        records.len(),
        expected.len(),
        "partition {partition} record count"
    );
    for (i, record) in records.iter().enumerate() {
        let offset = &record.offset;
        assert_eq!(
            *offset, i as i64,
            "partition {partition}: contiguous offsets"
        );
        let (want_key, want_value) = &expected[offset];
        assert_eq!(
            &record.key, want_key,
            "partition {partition} offset {offset}"
        );
        assert_eq!(
            record.value.as_ref(),
            Some(want_value),
            "partition {partition} offset {offset}"
        );
    }
}

fn assert_non_empty_log_files(data_dir: &std::path::Path) {
    for partition in 0..PARTITIONS {
        let partition_dir = data_dir.join(format!("{TOPIC}-{partition}"));
        let logs = std::fs::read_dir(&partition_dir)
            .unwrap_or_else(|error| panic!("read {}: {error}", partition_dir.display()))
            .map(|entry| entry.expect("partition directory entry").path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "log"))
            .collect::<Vec<_>>();
        assert!(
            !logs.is_empty(),
            "partition {partition} must expose a .log file while the broker is running"
        );
        assert!(
            logs.iter()
                .any(|path| std::fs::metadata(path).expect("log metadata").len() > 0),
            "partition {partition} must expose a non-empty .log file"
        );
    }
}

async fn concurrent_producer(
    producer_id: &'static str,
    producer: Producer,
    round_barrier: Arc<Barrier>,
) -> Vec<(i32, i64, Option<Bytes>, Bytes)> {
    let mut acknowledged = Vec::with_capacity(RECORDS_PER_PRODUCER as usize);
    for sequence in 0..RECORDS_PER_PRODUCER {
        // Neither producer can begin the next request until both have reached
        // this round, so every pair is issued concurrently over independent
        // TCP connections rather than merely being awaited in sequence.
        round_barrier.wait().await;
        let partition = (sequence % PARTITIONS as u64) as i32;
        let key = Some(Bytes::from(format!("{producer_id}-key-{sequence}")));
        let value = Bytes::from(format!("{producer_id}-value-{sequence}"));
        let offset = producer
            .send(TOPIC, Some(partition), key.clone(), value.clone())
            .await
            .unwrap_or_else(|error| panic!("{producer_id} send {sequence}: {error}"));
        acknowledged.push((partition, offset, key, value));
    }
    producer.flush().await.expect("producer flush");
    acknowledged
}

fn collect_expected(
    acknowledged: impl IntoIterator<Item = (i32, i64, Option<Bytes>, Bytes)>,
) -> ExpectedRecords {
    let mut expected = (0..PARTITIONS)
        .map(|partition| (partition, ExpectedPartition::new()))
        .collect::<ExpectedRecords>();
    let mut total = 0usize;
    for (partition, offset, key, value) in acknowledged {
        let old = expected
            .get_mut(&partition)
            .expect("known partition")
            .insert(offset, (key, value));
        assert!(
            old.is_none(),
            "duplicate acknowledgement for {partition}@{offset}"
        );
        total += 1;
    }
    assert_eq!(total, (RECORDS_PER_PRODUCER * 2) as usize);
    for (partition, records) in &expected {
        assert!(
            !records.is_empty(),
            "both producers should distribute records to partition {partition}"
        );
        assert_eq!(
            records.keys().copied().collect::<Vec<_>>(),
            (0..records.len() as i64).collect::<Vec<_>>(),
            "acknowledged offsets for partition {partition}"
        );
    }
    expected
}

#[tokio::test]
async fn produce_consume_restart_durability() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let dir = tempfile::tempdir().unwrap();

        // --- Boot 1: two producers concurrently write 1000 exact records. ---
        let broker = start_broker(dir.path()).await;
        let config = |client_id: &str| ProducerConfig {
            client_id: client_id.to_owned(),
            linger_ms: 0,
            compression: Compression::Lz4,
            ..ProducerConfig::default()
        };
        let producer_a = Producer::connect(broker.addr, config("integration-producer-a"))
            .await
            .expect("producer A connect");
        let producer_b = Producer::connect(broker.addr, config("integration-producer-b"))
            .await
            .expect("producer B connect");
        let round_barrier = Arc::new(Barrier::new(2));
        let task_a = tokio::spawn(concurrent_producer(
            "producer-a",
            producer_a,
            Arc::clone(&round_barrier),
        ));
        let task_b = tokio::spawn(concurrent_producer(
            "producer-b",
            producer_b,
            Arc::clone(&round_barrier),
        ));
        let (acknowledged_a, acknowledged_b) = tokio::join!(task_a, task_b);
        let expected = collect_expected(
            acknowledged_a
                .expect("producer A task")
                .into_iter()
                .chain(acknowledged_b.expect("producer B task")),
        );

        // Appends must be externally visible as real, non-empty log files
        // before the broker is stopped.
        assert_non_empty_log_files(dir.path());

        // --- Consume everything back from earliest; exact content. ---
        let consumer = Consumer::connect(broker.addr, "it-consumer")
            .await
            .expect("consumer connect");
        for partition in 0..PARTITIONS {
            let records = read_all(&consumer, partition).await;
            assert_partition(&records, partition, &expected[&partition]);
        }
        // Offsets API agrees with what we produced.
        for partition in 0..PARTITIONS {
            let latest = consumer
                .list_offsets(TOPIC, partition, brahmaputra_client::LATEST)
                .await
                .unwrap();
            assert_eq!(latest, expected[&partition].len() as i64);
        }
        drop(consumer);
        stop_broker(broker).await;

        // --- Boot 2: exact durable prefix, then continued offsets/content. ---
        let broker = start_broker(dir.path()).await;
        let consumer = Consumer::connect(broker.addr, "it-consumer-2")
            .await
            .expect("consumer reconnect");
        for partition in 0..PARTITIONS {
            let records = read_all(&consumer, partition).await;
            assert_partition(&records, partition, &expected[&partition]);
        }
        // Metadata survived the restart too (meta.toml).
        let meta = consumer.metadata(&[]).await.unwrap();
        let topic = meta.topics.iter().find(|t| t.name == TOPIC).unwrap();
        assert_eq!(topic.partitions.len(), PARTITIONS as usize);

        let continuation = Producer::connect(
            broker.addr,
            ProducerConfig {
                client_id: "integration-continuation".into(),
                linger_ms: 0,
                ..ProducerConfig::default()
            },
        )
        .await
        .expect("continuation producer connect");
        let mut expected = expected;
        for partition in 0..PARTITIONS {
            let first_offset = expected[&partition].len() as i64;
            for sequence in 0..CONTINUATION_RECORDS {
                let key = Some(Bytes::from(format!("continued-{partition}-key-{sequence}")));
                let value = Bytes::from(format!("continued-{partition}-value-{sequence}"));
                let offset = continuation
                    .send(TOPIC, Some(partition), key.clone(), value.clone())
                    .await
                    .expect("continuation send");
                assert_eq!(
                    offset,
                    first_offset + sequence as i64,
                    "partition {partition} offset must continue after restart"
                );
                expected
                    .get_mut(&partition)
                    .unwrap()
                    .insert(offset, (key, value));
            }
        }
        continuation.flush().await.expect("continuation flush");
        for partition in 0..PARTITIONS {
            let records = read_all(&consumer, partition).await;
            assert_partition(&records, partition, &expected[&partition]);
            let latest = consumer
                .list_offsets(TOPIC, partition, brahmaputra_client::LATEST)
                .await
                .expect("latest offset after continuation");
            assert_eq!(latest, expected[&partition].len() as i64);
        }
        stop_broker(broker).await;
    })
    .await
    .expect("test timed out");
}

/// A batched fetch must never build a response larger than the frame the
/// client will accept.
///
/// Each partition's read stops only *after* the batch that crosses its
/// allowance, so every partition can overshoot by most of a batch. With
/// megabyte batches across several partitions those overshoots add up, and
/// the response used to exceed `max_frame_bytes` — at which point the
/// client's length-delimited decoder rejects the frame and drops the
/// connection, which surfaced as an intermittent "connection closed" while
/// consuming large records. The response budget has to be enforced across
/// the whole response, not only per partition.
#[tokio::test]
async fn large_records_across_partitions_stay_inside_the_frame_limit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let running = start_broker(dir.path()).await;

    // Records big enough that a handful of them approach the frame limit.
    const RECORD_BYTES: usize = 1024 * 1024;
    // Enough records per partition, packed into one batch, that a single
    // batch dwarfs the per-partition allowance — which is what makes the
    // per-partition overshoot add up past the frame limit.
    const PER_PARTITION: usize = 12;

    let producer = Producer::connect(
        running.addr,
        ProducerConfig {
            acks: 1,
            batch_size: 24 * 1024 * 1024,
            linger_ms: 50,
            compression: Compression::None,
            ..ProducerConfig::default()
        },
    )
    .await
    .expect("connect producer");

    // Send concurrently so the producer packs many records into one batch.
    // That is what makes this test bite: single-record batches never
    // overshoot enough to matter, while multi-megabyte batches do.
    let producer = Arc::new(producer);
    let mut sends = Vec::new();
    for partition in 0..PARTITIONS {
        for index in 0..PER_PARTITION {
            let producer = Arc::clone(&producer);
            sends.push(tokio::spawn(async move {
                let mut value = vec![b'x'; RECORD_BYTES];
                value[0] = index as u8;
                producer
                    .send("bigrec", Some(partition), None, Bytes::from(value))
                    .await
                    .expect("send large record");
            }));
        }
    }
    for send in sends {
        send.await.expect("send task");
    }
    producer.flush().await.expect("flush");
    drop(producer);

    // Read every partition back. The consumer asks for all of them in one
    // request, which is precisely the shape that used to overflow.
    let consumer = Consumer::connect(running.addr, "big-reader")
        .await
        .expect("connect consumer");
    // Ask for every partition in one request: that is the shape whose
    // per-partition overshoots add up, and the single-partition path never
    // reproduces it.
    let mut positions: Vec<i64> = vec![0; PARTITIONS as usize];
    let mut seen = vec![0usize; PARTITIONS as usize];
    while seen.iter().sum::<usize>() < PARTITIONS as usize * PER_PARTITION {
        let requests: Vec<(String, i32, i64)> = (0..PARTITIONS)
            .filter(|partition| seen[*partition as usize] < PER_PARTITION)
            .map(|partition| {
                (
                    "bigrec".to_string(),
                    partition,
                    positions[partition as usize],
                )
            })
            .collect();
        let fetched = consumer
            .fetch_many_public(&requests, 500)
            .await
            .expect("batched fetch must not tear the connection down");
        let mut progressed = false;
        for (_, partition, records) in fetched {
            for record in records {
                assert_eq!(record.value.as_ref().map_or(0, |v| v.len()), RECORD_BYTES);
                positions[partition as usize] = record.offset + 1;
                seen[partition as usize] += 1;
                progressed = true;
            }
        }
        assert!(
            progressed,
            "batched fetch stalled at {positions:?} with {seen:?} read"
        );
    }

    stop_broker(running).await;
}

/// Headers and per-record timestamps must survive the whole path: producer
/// buffer, batch encode, broker append, disk, fetch, decode. Unit tests
/// cover the codec; this covers everything between it and a consumer.
#[tokio::test]
async fn headers_and_timestamps_survive_a_real_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_broker(dir.path()).await;

    let producer = Producer::connect(
        broker.addr,
        ProducerConfig {
            linger_ms: 0,
            ..ProducerConfig::default()
        },
    )
    .await
    .expect("connect producer");

    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    producer
        .send_with_headers(
            TOPIC,
            Some(0),
            Some(Bytes::from_static(b"k")),
            Bytes::from_static(b"with-headers"),
            vec![
                brahmaputra_protocol::RecordHeader::new("trace-id", b"abc-123".to_vec()),
                brahmaputra_protocol::RecordHeader::new(
                    "content-type",
                    b"application/json".to_vec(),
                ),
            ],
        )
        .await
        .expect("send");
    // A record with no headers shares the batch; the batch-level headers
    // bit must not invent headers for it.
    producer
        .send(TOPIC, Some(0), None, Bytes::from_static(b"no-headers"))
        .await
        .expect("send");
    producer.flush().await.expect("flush");

    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let consumer = Consumer::connect(broker.addr, "header-reader")
        .await
        .expect("connect consumer");
    let records = consumer.fetch(TOPIC, 0, 0, 500).await.expect("fetch");
    assert_eq!(records.len(), 2);

    let first = &records[0];
    assert_eq!(first.value, Some(Bytes::from_static(b"with-headers")));
    assert_eq!(first.headers.len(), 2, "both headers survived");
    assert_eq!(first.headers[0].key, "trace-id");
    assert_eq!(
        first.headers[0].value.as_deref(),
        Some(&b"abc-123"[..]),
        "header values are bytes, unchanged"
    );
    assert_eq!(first.headers[1].key, "content-type");

    let second = &records[1];
    assert_eq!(second.value, Some(Bytes::from_static(b"no-headers")));
    assert!(
        second.headers.is_empty(),
        "a record with no headers must not gain any from its batch"
    );

    // Timestamps are real wall-clock values bracketed by the send, not
    // zero and not the fetch time.
    for record in &records {
        assert!(
            record.timestamp >= before && record.timestamp <= after,
            "timestamp {} outside the send window {before}..={after}",
            record.timestamp
        );
    }

    stop_broker(broker).await;
}
