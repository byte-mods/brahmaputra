# Brahmaputra client for C

C11 + POSIX (pthreads, BSD sockets). No dependencies beyond libc, plus
zlib when gzip is wanted (on by default, one compile flag to drop it).

Verified end to end against a live broker: **54/54 checks**
(`./test.sh 127.0.0.1 9092`), also clean under
`-fsanitize=address,undefined` (with leak detection) and
`-fsanitize=thread`.

## Build

```bash
make                    # build/libbrahmaputra.a and build/manual_test
make WITH_ZLIB=0        # no zlib; gzip only if you register a codec
make SANITIZE=1         # ASan + UBSan build in build-asan/
```

or with CMake:

```bash
cmake -S . -B build-cmake -DBRP_WITH_ZLIB=ON && cmake --build build-cmake
```

Link with `-Iinclude build/libbrahmaputra.a -pthread -lz`. Everything
compiles clean under `-Wall -Wextra -Werror -pedantic` with gcc and clang.

## Conventions

- Handles are opaque: `brp_client_t`, `brp_producer_t`, `brp_consumer_t`,
  `brp_group_consumer_t`. Each `*_new()` returns a `brp_err_t` and writes
  the handle through an out-parameter.
- Every config struct has an `*_init()` that fills in defaults. Strings in
  a config are copied, so the config can go once the handle exists.
- Arrays handed to you are yours: `brp_records_free()`,
  `brp_metadata_free()`, `brp_offsets_free()`, or `brp_free()` for flat
  arrays and strings.
- `BRP_OK` is 0. Negative codes are client-side (`BRP_ERR_BUFFER_FULL`,
  `BRP_ERR_NO_OFFSET`, ...); positive codes are the broker's own
  (`BRP_ERR_NOT_LEADER_OR_FOLLOWER`, ...). `brp_last_error()` gives the
  detail for the last failure on the calling thread.
- `NULL` with length 0 is **null**; a non-`NULL` pointer with length 0 is
  **empty**. That holds for keys, values and header values, and a null
  value is a tombstone.
- `brp_client_t` and `brp_producer_t` are thread-safe. Consumers are
  single-threaded, like Kafka's: one per thread.

## Produce

```c
#include "brahmaputra.h"

brp_producer_config_t config;
brp_producer_config_init(&config);
config.acks = 1;
config.linger_ms = 5;
config.compression_type = "gzip";

brp_producer_t *producer;
if (brp_producer_new("127.0.0.1:9092", &config, &producer) != BRP_OK) {
    fprintf(stderr, "%s\n", brp_last_error());
    return 1;
}

brp_header_t headers[] = {{"trace-id", (const uint8_t *)"abc-123", 7}};
brp_message_t msg;
brp_message_init(&msg);              /* partition = BRP_PARTITION_ANY */
msg.topic = "orders";
msg.key = "user-7";                  /* murmur2(key) % partitions */
msg.key_len = 6;
msg.value = "{\"id\":1}";
msg.value_len = 8;
msg.headers = headers;
msg.header_count = 1;
brp_producer_send(producer, &msg);   /* buffered */

int64_t offset;                      /* or one record, one round trip */
brp_producer_send_sync(producer, &msg, &offset);

brp_producer_flush(producer);
brp_producer_close(producer);        /* flushes, then frees */
```

A tombstone is `msg.value = NULL`. An explicit partition is
`msg.partition = 3`. `msg.timestamp_ms` overrides the record time.

## Consume one partition

```c
brp_consumer_t *consumer;
brp_consumer_new("127.0.0.1:9092", NULL, &consumer);   /* NULL = defaults */

brp_record_t *records;
size_t count;
int64_t high_watermark;
if (brp_consumer_fetch(consumer, "orders", 0, 0, 500,
                       &records, &count, &high_watermark) == BRP_OK) {
    for (size_t i = 0; i < count; i++)
        printf("%lld %.*s\n", (long long)records[i].offset,
               (int)records[i].value_len, (const char *)records[i].value);
    brp_records_free(records, count);
}

int64_t end;
brp_consumer_list_offsets(consumer, "orders", 0, BRP_OFFSET_LATEST, &end);
brp_consumer_close(consumer);
```

## Consume as a group

```c
brp_group_config_t config;
brp_group_config_init(&config);
config.assignor = "sticky";
config.auto_offset_reset = "earliest";
config.auto_commit_interval_ms = 0;        /* commit explicitly */
config.group_instance_id = "worker-3";     /* static membership */

brp_group_consumer_t *group;
brp_group_consumer_new("127.0.0.1:9092", "billing", &config, &group);
const char *topics[] = {"orders"};
brp_group_consumer_subscribe(group, topics, 1);

for (;;) {
    brp_record_t *records;
    size_t count;
    brp_err_t err = brp_group_consumer_poll(group, 500, &records, &count);
    if (err != BRP_OK) {
        fprintf(stderr, "%s\n", brp_last_error());
        break;
    }
    for (size_t i = 0; i < count; i++) handle(&records[i]);
    brp_records_free(records, count);
    /* At-least-once: commit after processing, never before. */
    brp_group_consumer_commit(group);
}
brp_group_consumer_close(group);   /* commits, then leaves so partitions move at once */
```

## Configuration reference

Producer (`brp_producer_config_t`):

| Field | Kafka name | Default |
|---|---|---|
| `client_id` | `client.id` | `brahmaputra-c` |
| `acks` | `acks` | `1` (`0`, `1`, `-1` = all) |
| `batch_size` | `batch.size` | 16384 |
| `linger_ms` | `linger.ms` | 5 (0 sends each record immediately) |
| `compression_type` | `compression.type` | `none` |
| `request_timeout_ms` | `request.timeout.ms` | 30000 |
| `retries` | `retries` | 5 (retriable broker errors only) |
| `retry_backoff_ms` | `retry.backoff.ms` | 100 |
| `delivery_timeout_ms` | `delivery.timeout.ms` | 120000 |
| `buffer_memory` | `buffer.memory` | 32 MiB |
| `max_block_ms` | `max.block.ms` | 60000 |
| `socket_connection_setup_timeout_ms` | `socket.connection.setup.timeout.ms` | 30000 |

Consumer (`brp_consumer_config_t`):

| Field | Kafka name | Default |
|---|---|---|
| `fetch_max_bytes` | `fetch.max.bytes` | 8 MiB |
| `fetch_min_bytes` | `fetch.min.bytes` | 1 |
| `fetch_max_wait_ms` | `fetch.max.wait.ms` | 500 |
| `isolation_level` | `isolation.level` | `BRP_READ_UNCOMMITTED` |
| `client_rack` | `client.rack` | `""` |
| `max_poll_records` | `max.poll.records` | 500 |

Group (`brp_group_config_t`):

| Field | Kafka name | Default |
|---|---|---|
| `session_timeout_ms` | `session.timeout.ms` | 10000 |
| `rebalance_timeout_ms` | `rebalance.timeout.ms` | 3000 |
| `max_poll_interval_ms` | `max.poll.interval.ms` | 300000 |
| `auto_commit_interval_ms` | `auto.commit.interval.ms` | 5000 (0 = off) |
| `auto_offset_reset` | `auto.offset.reset` | `earliest` (`latest`, `none`) |
| `assignor` | `partition.assignment.strategy` | `range` (`roundrobin`, `sticky`) |
| `group_instance_id` | `group.instance.id` | `NULL` (dynamic member) |
| `max_poll_records` | `max.poll.records` | 500 |
| `fetch_max_bytes` | `fetch.max.bytes` | 8 MiB |

## Compression

`none` is always built in, and so is `gzip` when built with zlib
(`BRP_WITH_ZLIB`, the default). The rest are opt-in, so this library pulls
in no compression dependencies of its own:

```c
static int zstd_c(const uint8_t *in, size_t n, uint8_t **out, size_t *out_n, void *opaque) {
    size_t cap = ZSTD_compressBound(n);
    *out = malloc(cap);                       /* freed by the library */
    *out_n = ZSTD_compress(*out, cap, in, n, 3);
    return ZSTD_isError(*out_n) ? -1 : 0;
}
/* ... zstd_d likewise ... */
brp_register_codec(BRP_COMPRESSION_ZSTD, zstd_c, zstd_d, NULL);
```

A codec's output must be `malloc()`-allocated; the library releases it
with `free()`. If you register lz4, note that the broker expects a
little-endian `uint32` of the uncompressed length followed by a raw LZ4
**block**, not the LZ4 frame format.

## End-to-end test

Start a broker, then:

```bash
./test.sh 127.0.0.1 9092               # builds and runs build/manual_test
SANITIZE=1 ./test.sh 127.0.0.1 9092    # same suite under ASan + UBSan
```

It ports the Go suite section for section and prints `N passed, 0 failed`.
It exits 0 on success, 1 if any check fails, and 2 on a fatal setup error.

## Not implemented

- Authentication (`Authenticate`, SCRAM/PLAIN). The broker refuses
  credentials on a plaintext listener, and this driver has no TLS yet.
- TLS and QUIC transports, transactions, and the idempotent producer.
  The other drivers lack these too.
