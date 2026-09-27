# Brahmaputra client for Go

```bash
go get github.com/byte-mods/brahmaputra/clients/go
```

Standard library only; Go 1.21+. Verified end to end against a live broker:
**80/80 checks** (`./test.sh HOST PORT`, which vets, runs the unit tests in
`brahmaputra/`, and runs `go run ./cmd/manualtest HOST:PORT` under the race
detector when cgo is available). The suite shows every setting below
changing behaviour, including retries against a fault-injecting proxy,
generation fencing, static membership and a registered codec.

## Produce

```go
config := brahmaputra.DefaultProducerConfig()
config.Acks = 1
config.LingerMs = 5
config.Compression = "gzip"

producer, err := brahmaputra.NewProducer("127.0.0.1:9092", config)
if err != nil {
    return err
}
defer producer.Close()

// Keyed: murmur2(key) % partitions, so records sharing a key keep order.
err = producer.Send("orders", []byte(`{"id":1}`), []byte("user-7"),
    brahmaputra.RecordHeader{Key: "trace-id", Value: []byte("abc-123")})

// Explicit partition; explicit record timestamp (unix ms); a nil value is
// a tombstone, distinct from []byte{}; a header value may be nil.
err = producer.SendTo("orders", 3, []byte(`{"id":2}`), nil)
err = producer.SendAt("orders", []byte(`{"id":3}`), []byte("user-7"), 1_700_000_000_000)
err = producer.SendTo("orders", 3, nil, []byte("user-7"))

// Or wait for one record's offset. A full round trip — correct, and slow.
// SendToSync takes an explicit partition and timestamp; records already
// buffered for that partition go first.
offset, err := producer.SendSync("orders", []byte(`{"id":4}`), nil)

err = producer.Flush()
```

## Consume one partition

```go
consumer, err := brahmaputra.NewConsumer("127.0.0.1:9092",
    brahmaputra.DefaultConsumerConfig())
defer consumer.Close()

records, err := consumer.Fetch("orders", 0, 0, 500)
for _, record := range records {
    log.Printf("%d %s %s", record.Offset, record.Key, record.Value)
}

end, err := consumer.ListOffsets("orders", 0, brahmaputra.Latest)
at, err := consumer.ListOffsets("orders", 0, 1_700_000_000_000) // first offset at/after a unix-ms time
records, highWatermark, err := consumer.FetchVerbose("orders", 0, 0, 500)
metadata, err := consumer.Router().Metadata([]string{"orders"}, true) // partitions, leaders
```

## Consume as a group

```go
config := brahmaputra.DefaultGroupConfig()
config.Assignor = brahmaputra.AssignorSticky
config.AutoOffsetReset = brahmaputra.AutoOffsetResetEarliest
config.AutoCommitIntervalMs = 0          // commit explicitly
config.GroupInstanceID = "worker-3"      // static membership

consumer, err := brahmaputra.NewGroupConsumer("127.0.0.1:9092", "billing", config)
consumer.Subscribe([]string{"orders"})
defer consumer.Close()   // commits, then leaves so partitions move at once

for {
    records, err := consumer.Poll(500 * time.Millisecond)
    if err != nil {
        return err
    }
    for _, record := range records {
        handle(record.Value)
    }
    // At-least-once: commit after processing, never before.
    if err := consumer.Commit(); err != nil {
        return err
    }
}
```

`MemberID()`, `Generation()` and `Assignment()` report the member's state;
`Committed(nil)` reads the group's committed offsets. A member the
coordinator no longer knows (`UNKNOWN_MEMBER_ID`) rejoins as a new member,
and a member that stalls past `MaxPollIntervalMs` leaves and rejoins on its
next `Poll`.

## Configuration

Start from `DefaultProducerConfig()`, `DefaultConsumerConfig()` or
`DefaultGroupConfig()`; fields are Kafka's settings in Go spelling.

| ProducerConfig | Kafka | Default |
|---|---|---|
| `Acks` | `acks` | 1 (`0`, `1`, `-1` = all) |
| `BatchSize` | `batch.size` | 16384 |
| `LingerMs` | `linger.ms` | 5 (0 sends each record at once) |
| `Compression` | `compression.type` | `"none"` (`gzip`; `lz4`/`zstd`/`snappy` once registered) |
| `RequestTimeoutMs` | `request.timeout.ms` | 30000 (broker-side ack wait) |
| `Retries` / `RetryBackoffMs` | `retries` / `retry.backoff.ms` | 5 / 100 (retriable errors only) |
| `DeliveryTimeoutMs` | `delivery.timeout.ms` | 120000 |
| `BufferMemory` / `MaxBlockMs` | `buffer.memory` / `max.block.ms` | 32 MiB / 60000 |
| `DialTimeout` / `SocketTimeout` | connect / client-side round-trip deadline | 30 s / 2 min |

| ConsumerConfig | Kafka | Default |
|---|---|---|
| `FetchMaxBytes` / `FetchMinBytes` / `FetchMaxWaitMs` | `fetch.*` | 8 MiB / 1 / 500 |
| `MaxPollRecords` | `max.poll.records` | 500 |
| `IsolationLevel` / `Rack` | `isolation.level` / `client.rack` | uncommitted / `""` |
| `DialTimeout` / `SocketTimeout` | | 30 s / 2 min |

| GroupConfig | Kafka | Default |
|---|---|---|
| `SessionTimeoutMs` | `session.timeout.ms` | 10000 |
| `HeartbeatIntervalMs` | `heartbeat.interval.ms` | 0 (session timeout / 3) |
| `RebalanceTimeoutMs` | `rebalance.timeout.ms` | 3000 |
| `MaxPollIntervalMs` | `max.poll.interval.ms` | 300000 (time inside `Poll` never counts) |
| `AutoCommitIntervalMs` | `auto.commit.interval.ms` | 5000 (0 disables auto-commit) |
| `AutoOffsetReset` | `auto.offset.reset` | `earliest` (`latest`, `none` returns `ErrNoOffsetForPartition`) |
| `Assignor` | `partition.assignment.strategy` | `range` (`roundrobin`, `sticky`) |
| `GroupInstanceID` | `group.instance.id` | `""` (dynamic member) |
| `MaxPollRecords` / `FetchMaxBytes` / `SocketTimeout` | | 500 / 8 MiB / 2 min |

A connection that fails or exceeds its round-trip deadline is closed and
redialled on next use, the seed included (`Router.SetRequestTimeout`,
`Conn.SetRequestTimeout`).

## Compression

`none` and `gzip` are built in. The rest are opt-in, so this package pulls
in no dependencies of its own:

```go
brahmaputra.RegisterCodec(
    brahmaputra.CompressionZstd,
    func(payload []byte) ([]byte, error) { return encoder.EncodeAll(payload, nil), nil },
    func(payload []byte) ([]byte, error) { return decoder.DecodeAll(payload, nil) },
)
```

If you register lz4, note that the broker expects a little-endian `uint32`
of the uncompressed length followed by a raw LZ4 **block** — not the LZ4
frame format, which a frame-format library would silently produce instead.

## Running the tests

```bash
brahmaputra-server --data-dir ./data --default-partitions 4
./test.sh 127.0.0.1 9092
```

It prints `80 passed, 0 failed` and exits non-zero on any failure.
