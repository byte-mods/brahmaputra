# Brahmaputra client for C++

C++17, POSIX sockets, `std::thread`. No dependencies beyond the standard
library, except zlib for the built-in gzip codec, and that is optional.

This was tested end to end against a live broker and passed all **38/38 checks**
(`./test.sh 127.0.0.1 9092`). With `-Wall -Wextra -Wpedantic` it builds without
warnings on g++ 13 and clang 18, and it runs clean under ThreadSanitizer.

## Build

```bash
cmake -S . -B build                      # -DBRAHMAPUTRA_WITH_GZIP=OFF to drop zlib
cmake --build build -j
```

This produces the static library `build/libbrahmaputra.a` (target
`brahmaputra`, alias `brahmaputra::brahmaputra`) and the e2e suite
`build/manual_test`. Headers are under `include/brahmaputra/`. Include
`<brahmaputra/brahmaputra.hpp>` to get all of them.

From another CMake project:

```cmake
add_subdirectory(path/to/clients/cpp)
target_link_libraries(my_app PRIVATE brahmaputra::brahmaputra)
```

| CMake option | Default | |
|---|---|---|
| `BRAHMAPUTRA_WITH_GZIP` | `ON` | Build the gzip codec in. Needs zlib (`zlib1g-dev`). |
| `BRAHMAPUTRA_BUILD_TESTS` | `ON` | Build `manual_test`. |

Errors are exceptions. Everything derives from `brahmaputra::Error`. The
subclasses are `ServerError` (with `code()`), `NetworkError`,
`BufferFullError` and `NoOffsetForPartition`.

## Produce

```cpp
#include <brahmaputra/brahmaputra.hpp>
namespace bp = brahmaputra;

bp::ProducerConfig config;
config.acks = 1;
config.lingerMs = 5;
config.compressionType = "gzip";
// or: auto config = bp::ProducerConfig::fromProperties({{"acks", "all"}, {"linger.ms", "5"}});

bp::Producer producer("127.0.0.1:9092", config);   // bootstrap.servers, comma-separated OK

// Keyed: murmur2(key) % partitions, so records sharing a key keep order.
producer.send("orders", bp::toBytes(R"({"id":1})"), bp::toBytes("user-7"),
              {{"trace-id", bp::toBytes("abc-123")}, {"retry-of", std::nullopt}});

// Explicit partition; a tombstone (null value) is distinct from an empty value.
producer.sendTo("orders", 0, std::nullopt, bp::toBytes("user-7"));

// Full control, including the timestamp. sendSync waits for the offset:
// one round trip per record, which is correct and slow.
bp::ProducerRecord record;
record.topic = "orders";
record.value = bp::toBytes(R"({"id":2})");
record.timestampMs = bp::nowMillis();
std::int64_t offset = producer.sendSync(record);

producer.flush();   // also rethrows any error from a background linger flush
producer.close();   // flushes, stops the linger thread (the destructor does too)
```

## Consume one partition

```cpp
bp::Consumer consumer("127.0.0.1:9092");

for (const auto& r : consumer.fetch("orders", 0, /*offset*/ 0, /*maxWaitMs*/ 500)) {
    // r.key / r.value are std::optional<Bytes>; r.value == nullopt is a tombstone.
    std::printf("%lld %s\n", (long long)r.offset, r.value ? bp::toString(*r.value).c_str() : "<null>");
}

auto withHw = consumer.fetchWithWatermark("orders", 0, 0, 500);   // .highWatermark
std::int64_t end   = consumer.listOffsets("orders", 0, bp::kLatest);
std::int64_t start = consumer.listOffsets("orders", 0, bp::kEarliest);
std::int64_t atTs  = consumer.listOffsets("orders", 0, bp::nowMillis() - 3600'000);
```

## Consume as a group

```cpp
bp::GroupConfig config;
config.partitionAssignmentStrategy = bp::assignor::Sticky;
config.autoOffsetReset = bp::offset_reset::Earliest;
config.enableAutoCommit = false;          // commit explicitly
config.groupInstanceId = "worker-3";      // static membership

bp::GroupConsumer consumer("127.0.0.1:9092", "billing", config);
consumer.subscribe({"orders"});

for (;;) {
    for (const auto& record : consumer.poll(std::chrono::milliseconds(500))) {
        handle(record);
    }
    consumer.commit();   // at-least-once: after processing, never before
}
consumer.close();        // commits, then LeaveGroup so partitions move at once
```

A background thread heartbeats every `session.timeout.ms / 3`. If the
application goes `max.poll.interval.ms` without calling `poll()`, the thread
sends LeaveGroup, and the next `poll()` rejoins. Time spent inside `poll()`
does not count toward that interval. A commit from a generation the group
has moved past fails with `ILLEGAL_GENERATION` (generation fencing), and the
next poll rejoins.

## Configuration

Each config struct has a `fromProperties(std::map<string,string>)` that takes
Kafka's property names. Unknown keys throw. `bootstrap.servers` and
`group.id` are accepted and ignored, because they are constructor arguments.

**Producer** (`ProducerConfig`)

| Property | Field | Default |
|---|---|---|
| `client.id` | `clientId` | `brahmaputra-cpp` |
| `acks` (`0`, `1`, `-1`/`all`) | `acks` | `1` |
| `batch.size` | `batchSize` | 16384 |
| `linger.ms` | `lingerMs` | 5 (Kafka: 0) |
| `compression.type` | `compressionType` | `none` |
| `request.timeout.ms` | `requestTimeoutMs` | 30000 |
| `retries` | `retries` | 5 |
| `retry.backoff.ms` | `retryBackoffMs` | 100 |
| `delivery.timeout.ms` | `deliveryTimeoutMs` | 120000 |
| `buffer.memory` | `bufferMemory` | 32 MiB |
| `max.block.ms` | `maxBlockMs` | 60000 |
| `socket.connection.setup.timeout.ms` | `connectTimeoutMs` | 30000 |

**Consumer** (`ConsumerConfig`)

| Property | Field | Default |
|---|---|---|
| `client.id` | `clientId` | `brahmaputra-cpp` |
| `fetch.max.bytes` | `fetchMaxBytes` | 8 MiB |
| `fetch.min.bytes` | `fetchMinBytes` | 1 |
| `fetch.max.wait.ms` | `fetchMaxWaitMs` | 500 |
| `client.rack` | `clientRack` | empty |
| `isolation.level` | `isolationLevel` | `read_uncommitted` |
| `max.poll.records` | `maxPollRecords` | 500 |
| `request.timeout.ms` | `requestTimeoutMs` | 30000 |

**Group** (`GroupConfig`): all the consumer settings above, plus these:

| Property | Field | Default |
|---|---|---|
| `session.timeout.ms` | `sessionTimeoutMs` | 10000 (Kafka: 45000) |
| `rebalance.timeout.ms` | `rebalanceTimeoutMs` | 3000 |
| `max.poll.interval.ms` | `maxPollIntervalMs` | 300000 |
| `enable.auto.commit` | `enableAutoCommit` | `true` |
| `auto.commit.interval.ms` | `autoCommitIntervalMs` | 5000 (≤0 disables) |
| `auto.offset.reset` | `autoOffsetReset` | `earliest` (`latest`, `none`) |
| `partition.assignment.strategy` | `partitionAssignmentStrategy` | `range` (`roundrobin`, `sticky`) |
| `group.instance.id` | `groupInstanceId` | empty (dynamic member) |

## Compression

`none` is always available. `gzip` is built in when compiled with
`BRAHMAPUTRA_WITH_GZIP=ON` (the default). Register any other codec yourself,
so this library adds no dependency you did not ask for:

```cpp
bp::registerCodec(bp::Compression::Zstd,
    [](const bp::Bytes& in) { return my_zstd_compress(in); },
    [](const bp::Bytes& in) { return my_zstd_decompress(in); });
```

If you register lz4, note that the broker expects a little-endian `uint32` of
the uncompressed length followed by a raw LZ4 **block**. That is not the LZ4
frame format, which a frame-format library would produce instead.

## Running the end-to-end test

Start a broker, then:

```bash
./test.sh 127.0.0.1 9092        # builds into ./build and runs build/manual_test
```

The suite is a port of `clients/go/cmd/manualtest` with the same sections and
the same checks. It prints `38 passed, 0 failed` and exits non-zero on any
failure.

## Not implemented

- The `Authenticate` API (SCRAM or PLAIN). The broker refuses credentials on
  a plaintext listener, and this driver has no TLS, as with the other drivers.
- Idempotent or transactional production. `ProduceMulti` and `FetchMulti`
  are not used either: requests go one partition at a time, as in the Go
  driver.
