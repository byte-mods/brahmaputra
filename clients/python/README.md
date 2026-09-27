# Brahmaputra client for Python

Requires Python 3.9+. Pure standard library: `none` and `gzip`
compression are built in, and lz4, zstd and snappy are opt-in.

```bash
pip install ./clients/python              # core, no dependencies
pip install "./clients/python[zstd]"      # plus an optional codec (lz4, zstd, snappy, all)
```

Verified end to end against a live broker: **81/81 checks**
(`./test.sh 127.0.0.1 9092`). The suite mirrors the Go driver's
`cmd/manualtest` section for section and check for check, and the wire
encodings (frames, BitPacker bodies, record batches, CRC32C, murmur2) and
the three assignors were cross-checked byte for byte against the Go driver.
Beyond the Go suite's 54 checks it shows every setting below changing
behaviour (linger, batch size, timestamps, retries against a
fault-injecting proxy, fetch limits, heartbeats, static membership, leave
on close, generation fencing, a registered codec).

## Produce

```python
from brahmaputra import Producer, ProducerConfig, RecordHeader

with Producer("127.0.0.1", 9092, ProducerConfig(
    acks=1,                 # 0, 1, or -1 / "all"
    linger_ms=5,
    compression_type="gzip",
)) as producer:
    # Keyed: murmur2(key) % partitions, so records sharing a key keep order.
    producer.send(
        "orders",
        b'{"id":1}',
        key=b"user-7",
        headers=[RecordHeader("trace-id", b"abc-123"), RecordHeader("note", None)],
    )

    # Explicit partition, bypassing the partitioner.
    producer.send("orders", b'{"id":2}', partition=3)

    # A tombstone: value=None deletes the key on a compacted topic. It is
    # distinct from b"", which is an ordinary record with an empty value.
    producer.send("orders", None, key=b"user-7")

    # An explicit record timestamp (unix ms) instead of the wall clock.
    producer.send("orders", b'{"id":4}', timestamp_ms=1_700_000_000_000)

    # Or wait for one record's offset. A full round trip — correct, and slow.
    # Records already buffered for that partition go out first.
    offset = producer.send_and_wait("orders", b'{"id":3}')

    producer.flush()      # close() also flushes
```

`send` buffers per partition and returns immediately; `flush` sends and
waits for acknowledgement. When `buffer_memory` bytes are buffered, `send`
blocks for up to `max_block_ms` and then raises `BrahmaputraError`
("producer buffer full"). A failed background (linger) flush is raised
from the next `flush` (or `close`). Each partition has at most one batch in
flight, so linger-driven and batch-full flushes never reorder a partition.

## Connections

Each request has a round-trip deadline (`round_trip_timeout_s` in every
config, default 120 s, generous enough for long-polls and rebalances;
`BrokerRouter.set_request_timeout` / `Connection.set_request_timeout`
change it later). A timeout,
I/O error or correlation mismatch closes the connection and marks it
`broken` — it is never reused, since the byte stream is at an unknown
position — and raises `BrokerConnectionError`. The router redials a broken
connection on its next use, so after a broker restart the call that hit
the dead socket fails and the next one succeeds.

## Consume one partition

```python
from brahmaputra import Consumer, ConsumerConfig, EARLIEST, LATEST

with Consumer("127.0.0.1", 9092, ConsumerConfig(fetch_max_wait_ms=500)) as consumer:
    for record in consumer.fetch("orders", 0, 0):
        print(record.offset, record.key, record.value, record.timestamp, record.headers)

    records, high_watermark = consumer.fetch_verbose("orders", 0, 0)
    start = consumer.list_offsets("orders", 0, EARLIEST)
    end = consumer.list_offsets("orders", 0, LATEST)
    at = consumer.list_offsets("orders", 0, 1_700_000_000_000)   # by unix-ms timestamp
```

## Consume as a group

```python
from brahmaputra import Assignor, AutoOffsetReset, GroupConfig, GroupConsumer

with GroupConsumer("127.0.0.1", 9092, "billing", GroupConfig(
    assignor=Assignor.STICKY,
    auto_offset_reset=AutoOffsetReset.EARLIEST,
    auto_commit_interval_ms=0,      # commit explicitly
    group_instance_id="worker-3",   # static membership
)) as consumer:
    consumer.subscribe(["orders"])
    while True:
        for record in consumer.poll(500):     # timeout in ms
            handle(record.value)
        # At-least-once: commit after processing, never before.
        consumer.commit()
```

Closing commits and then sends `LeaveGroup`, so its partitions move
immediately rather than after a session timeout. With
`auto_offset_reset="none"` and no committed offset, `poll` raises
`NoOffsetForPartition`. `max_poll_interval_ms` bounds the time *between*
polls: time spent inside `poll` (including a slow join) never counts, and
a member that did stall past it leaves the group and rejoins on its next
`poll`. A member the coordinator no longer knows (`UNKNOWN_MEMBER_ID`)
rejoins as a new member.

## Configuration

`ProducerConfig`

| Field | Kafka name | Default |
|---|---|---|
| `acks` | `acks` | `1` (`0`, `1`, `-1`/`"all"`) |
| `batch_size` | `batch.size` | 16384 |
| `linger_ms` | `linger.ms` | 5 (0 sends each record at once) |
| `compression_type` | `compression.type` | `"none"` |
| `request_timeout_ms` | `request.timeout.ms` | 30000 |
| `retries` | `retries` | 5 (retriable broker errors only) |
| `retry_backoff_ms` | `retry.backoff.ms` | 100 |
| `delivery_timeout_ms` | `delivery.timeout.ms` | 120000 |
| `buffer_memory` | `buffer.memory` | 33554432 |
| `max_block_ms` | `max.block.ms` | 60000 |
| `client_id` | `client.id` | `"brahmaputra-python"` |
| `socket_timeout_s` | connect timeout | 30.0 s |
| `round_trip_timeout_s` | client-side request deadline | 120.0 s (`None` disables) |

Per record: `key` (murmur2 partitioning; `None` round-robins), `partition`
(explicit), `headers` (a `RecordHeader` value may be `None`), `value=None`
(tombstone) and `timestamp_ms` (default: now).

`ConsumerConfig`

| Field | Kafka name | Default |
|---|---|---|
| `fetch_max_bytes` | `fetch.max.bytes` | 8388608 |
| `fetch_min_bytes` | `fetch.min.bytes` | 1 |
| `fetch_max_wait_ms` | `fetch.max.wait.ms` | 500 |
| `max_poll_records` | `max.poll.records` | 500 |
| `isolation_level` | `isolation.level` | `READ_UNCOMMITTED` |
| `rack` | `client.rack` | `""` |
| `socket_timeout_s` / `round_trip_timeout_s` | | 30.0 / 120.0 s |

`GroupConfig`

| Field | Kafka name | Default |
|---|---|---|
| `session_timeout_ms` | `session.timeout.ms` | 10000 |
| `heartbeat_interval_ms` | `heartbeat.interval.ms` | 0 (session timeout / 3) |
| `rebalance_timeout_ms` | `rebalance.timeout.ms` | 3000 |
| `max_poll_interval_ms` | `max.poll.interval.ms` | 300000 |
| `auto_commit_interval_ms` | `auto.commit.interval.ms` | 5000 (0 disables) |
| `auto_offset_reset` | `auto.offset.reset` | `"earliest"` (`"latest"`, `"none"`) |
| `assignor` | `partition.assignment.strategy` | `"range"` (`"roundrobin"`, `"sticky"`) |
| `group_instance_id` | `group.instance.id` | `""` (dynamic member) |
| `max_poll_records`, `fetch_max_bytes` | | 500, 8388608 |
| `socket_timeout_s` / `round_trip_timeout_s` | | 30.0 / 120.0 s |

`member_id`, `generation` and `assignment` report the member's current
state; `committed()` reads the group's committed offsets.

## Compression

`none` and `gzip` are built in. For the others either install the
optional package (`pip install lz4`, `zstandard` or `python-snappy`, or the
matching extra), which is imported only when that codec is used, or
register your own implementation, which takes precedence:

```python
import zstandard
from brahmaputra import register_codec

register_codec(
    "zstd",
    lambda payload: zstandard.ZstdCompressor(level=3).compress(payload),
    lambda payload: zstandard.ZstdDecompressor().decompressobj().decompress(payload),
)
```

If you register lz4, note that the broker expects a little-endian `uint32`
of the uncompressed length followed by a raw LZ4 **block** — not the LZ4
frame format. Snappy is raw (unframed) snappy.

## Running the end-to-end test

Start a broker, then from any directory:

```bash
brahmaputra-server --data-dir ./data --default-partitions 4
./clients/python/test.sh 127.0.0.1 9092
# equivalently: python3 clients/python/test_manual.py 127.0.0.1 9092
```

It prints `81 passed, 0 failed` and exits non-zero on any failure.
