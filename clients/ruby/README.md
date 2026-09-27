# Brahmaputra client for Ruby

Pure Ruby, standard library only (`socket`, `zlib`, `monitor`). Ruby ≥ 3.0;
developed on 3.3.

```ruby
# Gemfile
gem "brahmaputra", path: "clients/ruby"
```

or build and install the gem:

```bash
cd clients/ruby && gem build brahmaputra.gemspec && gem install brahmaputra-0.1.0.gem
```

Verified end to end against a live broker: **87/87 checks**. The first 54
are the Go suite's (`clients/go/cmd/manualtest`): wire edge cases (1 MiB
values, empty vs null keys and header values, unicode), per-partition
ordering under linger flushes, background flush failures, unresponsive and
dropped connections, and `max.poll.interval.ms` behaviour. The other 33
cover the rest of the client contract: synchronous send, explicit partition
and timestamp, round-robin, `batch.size`/`linger.ms`/close, retries,
backoff, `request.timeout.ms` and `delivery.timeout.ms` (through a
fault-injecting proxy), codec registration, fetch limits, high watermark,
offsets by timestamp, metadata, `max.poll.records`, bounds-checked decoding,
auto commit, several topics, heartbeats, generation fencing, rejoin,
LeaveGroup on close, static membership and the sticky assignor. Run them
with `./test.sh HOST PORT`.

## Produce

```ruby
require "brahmaputra"

producer = Brahmaputra::Producer.new(
  "bootstrap.servers" => "127.0.0.1:9092",
  "acks"              => "all",
  "linger.ms"         => 5,
  "compression.type"  => "gzip",
)

# Keyed: murmur2(key) % partitions, so records sharing a key keep order.
# send only buffers. It returns a DeliveryFuture right away.
future = producer.send("orders", '{"id":1}', key: "user-7",
                       headers: { "trace-id" => "abc-123", "reason" => nil })

future.on_complete { |metadata, error| warn error.message if error }
metadata = future.value          # blocks; RecordMetadata(topic, partition, offset, timestamp)

producer.send("orders", "raw", partition: 2, timestamp: 1_700_000_000_000)
producer.send("orders", nil, key: "user-7")   # tombstone: nil value, not ""

# A full round trip per record: correct, and slow.
offset = producer.send_sync("orders", '{"id":2}').offset

producer.flush    # raises the first delivery error, if any
producer.close    # flushes, stops the sender thread, closes sockets
```

A background **sender thread** drains each partition's buffer when it
reaches `batch.size`, once its oldest record has waited `linger.ms`, or when
`flush`/`send_sync` asks. One batch per partition is in flight at a time,
so order within a partition is preserved. `send` blocks for up to
`max.block.ms` while `buffer.memory` is full and then raises
`Brahmaputra::BufferFullError`.

## Consume one partition

```ruby
consumer = Brahmaputra::Consumer.new("bootstrap.servers" => "127.0.0.1:9092")

consumer.fetch("orders", 0, 0, 500).each do |record|
  puts "#{record.offset} #{record.key} #{record.value} #{record.header('trace-id')}"
end

result = consumer.fetch_verbose("orders", 0, 42)   # .records, .high_watermark
first  = consumer.list_offsets("orders", 0, :earliest)     # or Brahmaputra::EARLIEST
last   = consumer.list_offsets("orders", 0, :latest)       # or Brahmaputra::LATEST
at     = consumer.list_offsets("orders", 0, 1_700_000_000_000)  # first offset at/after ts
consumer.close
```

## Consume as a group

```ruby
consumer = Brahmaputra::GroupConsumer.new(
  "bootstrap.servers"             => "127.0.0.1:9092",
  "group.id"                      => "billing",
  "partition.assignment.strategy" => "sticky",
  "auto.offset.reset"             => "earliest",
  "enable.auto.commit"            => false,   # commit explicitly
  "group.instance.id"             => "worker-3", # static membership
)
consumer.subscribe(["orders"])

begin
  loop do
    consumer.poll(500).each { |record| handle(record.value) }
    consumer.commit   # at-least-once: commit after processing, never before
  end
ensure
  consumer.close      # commits, then sends LeaveGroup so partitions move at once
end
```

A **heartbeat thread** keeps the membership alive every
`heartbeat.interval.ms` (default `session.timeout.ms / 3`). It flags a
rebalance for the next `poll` to rejoin. If `poll` is not called for
`max.poll.interval.ms`, it leaves the group. `poll`, `commit` and `close`
are meant for one thread, as with Kafka's consumer.

Snake_case symbols work everywhere a dotted name does:
`Producer.new(bootstrap_servers: "…", linger_ms: 0)`. An unknown key raises
`ArgumentError`, so a typo fails loudly instead of being silently ignored.

## Configuration

Common: `bootstrap.servers` (comma-separated `host:port`), `client.id`,
`request.timeout.ms` (30000), `socket.connection.setup.timeout.ms` (30000).

| Producer | Default | |
|---|---|---|
| `acks` | `1` | `0`, `1`, `-1`/`"all"` |
| `batch.size` | 16384 | bytes per partition batch |
| `linger.ms` | 5 | Kafka's default is 0; `0` sends immediately |
| `compression.type` | `none` | `none`, `gzip` built in; others via `register_codec` |
| `retries` / `retry.backoff.ms` | 5 / 100 | broker errors returned before the append (not leader, not enough replicas, ...) and failures to reach the leader; a request lost on the wire is not resent, since it may have been appended |
| `request.timeout.ms` | 30000 | sent to the broker as its ack wait, and bounds each round trip |
| `delivery.timeout.ms` | 120000 | caps buffered time plus every attempt |
| `buffer.memory` / `max.block.ms` | 32 MiB / 60000 | bounded client buffer |

| Consumer | Default |
|---|---|
| `fetch.min.bytes` / `fetch.max.bytes` | 1 / 8 MiB |
| `fetch.max.wait.ms` | 500 |
| `max.poll.records` | 500 |
| `isolation.level` | `read_uncommitted` |
| `client.rack` | `""` |

| Group consumer (also takes the consumer keys) | Default |
|---|---|
| `group.id` | required |
| `session.timeout.ms` / `heartbeat.interval.ms` | 10000 / session÷3 |
| `rebalance.timeout.ms` | 3000 |
| `max.poll.interval.ms` | 300000 |
| `enable.auto.commit` / `auto.commit.interval.ms` | true / 5000 (`0` disables) |
| `auto.offset.reset` | `earliest` (`latest`, `none` → `NoOffsetForPartitionError`) |
| `partition.assignment.strategy` | `range` (`roundrobin`, `sticky`) |
| `group.instance.id` | `""` (dynamic member) |

## Compression

`none` and `gzip` (stdlib `zlib`) are built in. The others are opt-in, so the gem has no dependencies:

```ruby
Brahmaputra.register_codec("zstd",
  compress:   ->(bytes) { Zstd.compress(bytes) },
  decompress: ->(bytes) { Zstd.decompress(bytes) })
```

If you register lz4, the broker expects a little-endian `uint32` of the
uncompressed length followed by a raw LZ4 **block**. That is not the LZ4
frame format.

## Errors

Everything raised derives from `Brahmaputra::Error`: `ServerError` (with
`#code`, see `Brahmaputra::ErrorCode`), `ConnectionError`, `TimeoutError`,
`BufferFullError`, `NoOffsetForPartitionError`, `ProtocolError`.

## Running the end-to-end suite

```bash
brahmaputra-server --data-dir ./data --default-partitions 4
cd clients/ruby && ruby -Ilib test/manual_test.rb 127.0.0.1 9092
# or, from any directory:
clients/ruby/test.sh 127.0.0.1 9092
```
