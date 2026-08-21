# Brahmaputra client for Java

Requires Java 8+. No dependencies.

> **Not yet compiled or executed.** No JDK was available on the machine
> this driver was written on. It follows the same design as the Go and
> Node drivers, which are verified 34/34 against a live broker — but that
> is not the same as being verified itself, and unlike the other three
> this one has not even been through a compiler. Build it and run the
> suite before trusting it.

```bash
javac -d out $(find src -name "*.java")
java -cp out io.brahmaputra.ManualTest 127.0.0.1 9092
```

## Produce

```java
Client.ProducerConfig config = new Client.ProducerConfig();
config.acks = 1;
config.lingerMs = 5;
config.compressionType = "gzip";

try (Client.Producer producer = new Client.Producer("127.0.0.1", 9092, config)) {
    // Keyed: murmur2(key) % partitions, so records sharing a key keep order.
    producer.send(
            "orders",
            "{\"id\":1}".getBytes(UTF_8),
            "user-7".getBytes(UTF_8),
            List.of(new Protocol.RecordHeader("trace-id", "abc-123".getBytes(UTF_8))));
    producer.flush();

    // Or wait for one record's offset. A full round trip — correct, and slow.
    long offset = producer.sendSync("orders", "{\"id\":2}".getBytes(UTF_8), null, null);
}
```

## Consume one partition

```java
try (Client.Consumer consumer =
        new Client.Consumer("127.0.0.1", 9092, new Client.ConsumerConfig())) {
    for (Client.ConsumedRecord record : consumer.fetch("orders", 0, 0, 500)) {
        System.out.println(record.offset + " " + new String(record.value, UTF_8));
    }
    long end = consumer.listOffsets("orders", 0, Client.LATEST);
}
```

## Consume as a group

```java
GroupConsumer.GroupConfig config = new GroupConsumer.GroupConfig();
config.assignor = GroupConsumer.Assignor.STICKY;
config.autoOffsetReset = GroupConsumer.AutoOffsetReset.EARLIEST;
config.autoCommitIntervalMs = 0;      // commit explicitly
config.groupInstanceId = "worker-3";  // static membership

try (GroupConsumer consumer =
        new GroupConsumer("127.0.0.1", 9092, "billing", config)) {
    consumer.subscribe(List.of("orders"));
    while (true) {
        for (Client.ConsumedRecord record : consumer.poll(500)) {
            handle(record.value);
        }
        // At-least-once: commit after processing, never before.
        consumer.commit();
    }
}
```

Closing commits and then leaves the group, so its partitions move
immediately rather than after a session timeout.

## Compression

`none` and `gzip` are built in. Register anything else yourself, so this
library stays dependency-free:

```java
Protocol.registerCodec(Protocol.Compression.ZSTD, new Protocol.Codec() {
    @Override public byte[] compress(byte[] payload) { ... }
    @Override public byte[] decompress(byte[] payload) { ... }
});
```

If you register lz4, note that the broker expects a little-endian `uint32`
of the uncompressed length followed by a raw LZ4 **block** — not the LZ4
frame format, which a frame-format library would silently produce instead.
