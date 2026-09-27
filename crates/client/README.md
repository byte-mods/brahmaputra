# brahmaputra-client (Rust)

The async Rust client (Tokio): a batching producer, a partition consumer, a
group consumer, admin and transactional clients. It is the reference the
other drivers are ported from, and the broker, CLI and WebSocket gateway use
it themselves.

## Build

```toml
[dependencies]
brahmaputra-client = { path = "crates/client" }   # inside this workspace
tokio = { version = "1", features = ["full"] }
bytes = "1"
```

```bash
cargo build -p brahmaputra-client
```

Every codec (`none`, `gzip`, `lz4`, `zstd`, `snappy`) is built in through
`brahmaputra-protocol`; there is nothing to register.

Verified end to end against a live broker: **85/85 checks**
(`examples/manual_test.rs`, a port of the Go driver's suite plus a section
per configuration area that shows each setting changing behaviour —
including retries against a fault-injecting proxy, generation fencing and
static membership).

## Produce

```rust
use brahmaputra_client::{Producer, ProducerConfig};
use brahmaputra_protocol::{Compression, RecordHeader};
use bytes::Bytes;

let producer = Producer::connect(
    "127.0.0.1:9092".parse()?,
    ProducerConfig {
        acks: -1,                        // 0, 1, or -1 (all)
        linger_ms: 5,
        compression: Compression::Gzip,
        ..ProducerConfig::default()
    },
)
.await?;

// Every send resolves with its record's offset once its batch is
// acknowledged (-1 with acks=0). Spawn sends concurrently to batch them.
// Keyed: murmur2(key) % partitions; no key and no partition: round-robin.
let offset = producer
    .send("orders", None, Some(Bytes::from("user-7")), Bytes::from(r#"{"id":1}"#))
    .await?;

// Explicit partition, headers (a header value may be None) and timestamp.
producer
    .send_with_timestamp(
        "orders",
        Some(3),
        None,
        Bytes::from(r#"{"id":2}"#),
        vec![RecordHeader::new("trace-id", Bytes::from("abc-123"))],
        1_700_000_000_000,
    )
    .await?;

// A tombstone: a null value, distinct from an empty one.
producer.send_tombstone("orders", None, Bytes::from("user-7")).await?;

producer.flush().await?;   // send everything buffered now
producer.close().await?;   // flush, stop the linger ticker, release
```

## Consume one partition

```rust
use brahmaputra_client::{Consumer, EARLIEST, LATEST};

let consumer = Consumer::connect("127.0.0.1:9092".parse()?, "reader")
    .await?
    .with_max_bytes(1 << 20)              // fetch.max.bytes
    .with_fetch_min_bytes(1)              // fetch.min.bytes
    .with_fetch_max_wait_ms(500)          // fetch.max.wait.ms
    .with_request_timeout(Some(std::time::Duration::from_secs(30)));

for record in consumer.fetch("orders", 0, 0, 500).await? {
    println!("{} {:?} {:?} {}", record.offset, record.key, record.value, record.timestamp);
}
let (records, high_watermark) = consumer.fetch_verbose("orders", 0, 0, 500).await?;
let start = consumer.list_offsets("orders", 0, EARLIEST).await?;
let end = consumer.list_offsets("orders", 0, LATEST).await?;
let at = consumer.list_offsets("orders", 0, 1_700_000_000_000).await?; // by unix-ms time
let metadata = consumer.metadata(&["orders".to_owned()]).await?;       // partitions, leaders
```

## Consume as a group

```rust
use brahmaputra_client::{Assignor, AutoOffsetReset, GroupConsumer};
use std::time::Duration;

let mut group = GroupConsumer::connect("127.0.0.1:9092".parse()?, "worker-3", "billing")
    .await?
    .with_assignor(Assignor::Sticky)
    .with_auto_offset_reset(AutoOffsetReset::Earliest)
    .with_auto_commit(None)                    // commit explicitly
    .with_group_instance_id("worker-3");       // static membership
group.subscribe(&["orders", "refunds"]);
loop {
    for record in group.poll(Duration::from_millis(500)).await? {
        handle(&record);
    }
    group.commit_sync().await?;                // at-least-once: after processing
}
// group.close().await? commits, then sends LeaveGroup.
```

`member_id()`, `generation()` and `assignment()` report the member's state.
Heartbeats and auto-commit run on background tasks; a member the
coordinator no longer knows rejoins as a new member, and one that stalls
past `max.poll.interval.ms` leaves and rejoins on its next `poll`.

## Configuration

| `ProducerConfig` field | Kafka | Default |
|---|---|---|
| `acks` | `acks` | 1 (`0`, `1`, `-1`) |
| `batch_size` | `batch.size` | 16384 |
| `linger_ms` | `linger.ms` | 5 (0 sends each record at once) |
| `compression` | `compression.type` | `Compression::Lz4` |
| `timeout_ms` | `request.timeout.ms` | 30000 (broker-side ack wait) |
| `retries` / `retry_backoff_ms` | `retries` / `retry.backoff.ms` | 5 / 100 (retriable errors only) |
| `delivery_timeout_ms` | `delivery.timeout.ms` | 120000 |
| `buffer_memory` / `max_block_ms` | `buffer.memory` / `max.block.ms` | 32 MiB / 60000 |
| `max_in_flight` | `max.in.flight.requests.per.connection` | 5 |
| `idempotence` | `enable.idempotence` | false |
| `batch_partitions` | | true (one request per broker) |

`Producer::set_request_timeout(Some(d))` bounds each request/response round
trip client-side (default: unbounded); a request that exceeds it fails with
`ClientError::Timeout` and the connection is redialled on next use.

| `Consumer` builder | Kafka | Default |
|---|---|---|
| `with_max_bytes` | `fetch.max.bytes` | 8 MiB |
| `with_fetch_min_bytes` | `fetch.min.bytes` | 1 |
| `with_fetch_max_wait_ms` | `fetch.max.wait.ms` | 500 |
| `with_isolation_level` | `isolation.level` | read uncommitted |
| `with_rack` | `client.rack` | none |
| `with_request_timeout` | client-side round-trip deadline | none |

| `GroupConsumer` builder | Kafka | Default |
|---|---|---|
| `with_session_timeout` | `session.timeout.ms` | 10000 |
| `with_heartbeat_interval` | `heartbeat.interval.ms` | session timeout / 3 |
| `with_rebalance_timeout` | `rebalance.timeout.ms` | 3000 |
| `with_max_poll_interval_ms` | `max.poll.interval.ms` | 300000 |
| `with_auto_commit` | `enable.auto.commit` + `auto.commit.interval.ms` | `Some(5 s)` |
| `with_auto_offset_reset` | `auto.offset.reset` | `Earliest` (`Latest`, `None`) |
| `with_assignor` | `partition.assignment.strategy` | `Range` (`RoundRobin`, `Sticky`) |
| `with_group_instance_id` | `group.instance.id` | dynamic member |
| `with_max_poll_records` | `max.poll.records` | 500 |
| `with_max_bytes`, `with_isolation_level`, `with_rack`, `with_request_timeout` | as the consumer | |

## Tests

```bash
cargo test -p brahmaputra-client          # unit tests (assignors, codecs, routing)
brahmaputra-server --data-dir ./data --default-partitions 4
cargo run --release -p brahmaputra-client --example manual_test -- 127.0.0.1 9092
```

The end-to-end run prints `85 passed, 0 failed` and exits non-zero on any
failure.
