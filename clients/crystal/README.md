# Brahmaputra client for Crystal

A native Crystal driver for Brahmaputra's wire protocol: producer,
partition consumer and consumer groups. Standard library only — no shard
dependencies (`TCPSocket`, `Compress::Gzip`, fibers, `Channel`, `Mutex`).

Verified end to end against a live broker: **54/54 checks**
(`./test.sh HOST PORT`).

## Install

Crystal 1.11 or newer. Add to `shard.yml`:

```yaml
dependencies:
  brahmaputra:
    path: ../brahmaputra/clients/crystal   # or a git source
```

```crystal
require "brahmaputra"
```

## Produce

```crystal
config = Brahmaputra::ProducerConfig.new do |c|
  c.acks = 1                  # 0, 1 or -1 (all)
  c.linger_ms = 5
  c.compression_type = "gzip"
end
producer = Brahmaputra::Producer.new("127.0.0.1:9092", config)

# Keyed: murmur2(key) % partitions, so records sharing a key keep order.
producer.send("orders", %({"id":1}), "user-7",
  [Brahmaputra::Header.new("trace-id", "abc-123")])

# Explicit partition; a nil value is a tombstone, distinct from "".
producer.send_to("orders", 0, nil, "user-7")

# Or wait for one record's offset. A full round trip — correct, and slow.
offset = producer.send_sync("orders", %({"id":2}))

producer.flush   # also raises the failure of any earlier background flush
producer.close
```

Keys, values and header values take `String`, `Bytes` or `nil`; `nil` is
null and an empty string or slice is empty, and both survive the round trip
as such.

## Consume one partition

```crystal
consumer = Brahmaputra::Consumer.new("127.0.0.1:9092")
records = consumer.fetch("orders", 0, 0_i64, 500)
records.each do |record|
  puts "#{record.offset} #{record.key_string} #{record.value_string}"
end

records, high_watermark = consumer.fetch_verbose("orders", 0, 0_i64)
latest = consumer.list_offsets("orders", 0, Brahmaputra::LATEST)
at_time = consumer.list_offsets("orders", 0, Brahmaputra.now_ms - 60_000)
consumer.close
```

## Consume as a group

```crystal
config = Brahmaputra::GroupConfig.new do |c|
  c.partition_assignment_strategy = "sticky"  # range | roundrobin | sticky
  c.auto_offset_reset = "earliest"            # earliest | latest | none
  c.auto_commit_interval_ms = 0               # commit explicitly
  c.group_instance_id = "worker-3"            # static membership
end
group = Brahmaputra::GroupConsumer.new("127.0.0.1:9092", "billing", config)
group.subscribe(["orders"])

begin
  loop do
    group.poll(500.milliseconds).each { |record| handle(record.value) }
    # At-least-once: commit after processing, never before.
    group.commit
  end
ensure
  group.close   # commits, then LeaveGroup so partitions move at once
end
```

A background fiber heartbeats every `session.timeout.ms / 3` and enforces
`max.poll.interval.ms`: a member whose application goes longer than that
between polls leaves the group and rejoins on its next `poll`. Time spent
*inside* `poll` (joining, long-polling) never counts against it.

## Configuration

`ProducerConfig`

| Property | Kafka name | Default |
|---|---|---|
| `acks` | `acks` | `1` |
| `batch_size` | `batch.size` | `16384` |
| `linger_ms` | `linger.ms` | `5` |
| `compression_type` | `compression.type` | `"none"` |
| `request_timeout_ms` | `request.timeout.ms` (broker-side ack wait) | `30000` |
| `retries` | `retries` | `5` |
| `retry_backoff_ms` | `retry.backoff.ms` | `100` |
| `delivery_timeout_ms` | `delivery.timeout.ms` | `120000` |
| `buffer_memory` | `buffer.memory` | `33554432` |
| `max_block_ms` | `max.block.ms` | `60000` |
| `connect_timeout_ms` | `socket.connection.setup.timeout.ms` | `30000` |
| `socket_timeout_ms` | client-side round-trip bound, `0` disables | `120000` |
| `client_id` | `client.id` | `"brahmaputra-crystal"` |

`ConsumerConfig`: `fetch_max_bytes` (8 MiB), `fetch_min_bytes` (1),
`fetch_max_wait_ms` (500), `max_poll_records` (500), `isolation_level`,
`client_rack`, `connect_timeout_ms`, `socket_timeout_ms`, `client_id`.

`GroupConfig`: `session_timeout_ms` (10000), `rebalance_timeout_ms` (3000),
`max_poll_interval_ms` (300000), `auto_commit_interval_ms` (5000; 0
disables), `auto_offset_reset` (`"earliest"`), `partition_assignment_strategy`
(`"range"`), `group_instance_id` (`""`), `max_poll_records`,
`fetch_max_bytes`, `fetch_min_bytes`, `connect_timeout_ms`,
`socket_timeout_ms`, `client_id`.

## Errors

Everything raised derives from `Brahmaputra::Error`:
`ServerError` (with `#code`), `ConnectionError` / `RequestTimeoutError`,
`DecodeError`, `BufferFullError`, `NoOffsetForPartitionError`, `ConfigError`.

Every request has a round-trip timeout. After a timeout, I/O error or
correlation-id mismatch the connection is closed and `broken?` returns
true; the router redials — the seed connection included — on next use.

## Compression

`none` and `gzip` are built in. The rest are opt-in:

```crystal
Brahmaputra.register_codec(Brahmaputra::Compression::Zstd,
  ->(payload : Bytes) { MyZstd.compress(payload) },
  ->(payload : Bytes) { MyZstd.decompress(payload) })
```

If you register lz4, the broker expects a little-endian `u32` of the
uncompressed length followed by a raw LZ4 **block** — not the LZ4 frame
format.

## Running the end-to-end suite

```bash
brahmaputra-server --data-dir ./data --default-partitions 4
clients/crystal/test.sh 127.0.0.1 9092
```

`test.sh` checks `crystal tool format`, builds `test/manual_test.cr` with
`--release` into `bin/`, and runs it; it prints `54 passed, 0 failed` and
exits non-zero on any failure. Without the build step:
`crystal run test/manual_test.cr -- 127.0.0.1 9092`.
