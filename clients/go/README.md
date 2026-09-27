# Brahmaputra client for Go

```bash
go get github.com/byte-mods/brahmaputra/clients/go
```

Verified end to end against a live broker: **54/54 checks**
(`./test.sh HOST PORT`, which vets and runs `go run ./cmd/manualtest HOST:PORT`
under the race detector when cgo is available).

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

// Or wait for one record's offset. A full round trip — correct, and slow.
offset, err := producer.SendSync("orders", []byte(`{"id":2}`), nil)

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
