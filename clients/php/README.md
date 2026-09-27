# Brahmaputra client for PHP

```bash
composer require brahmaputra/client
```

Pure PHP 8.2+ (tested on 8.4), speaking the wire protocol directly over
`stream_socket_client`. It needs only `ext-zlib` (for gzip) and uses
`ext-hash`'s native `crc32c` when present, falling back to a table
implementation otherwise. No Composer? Require the bundled autoloader:

```php
require '/path/to/clients/php/autoload.php';
```

Verified end to end against a live broker: **81/81 checks**
(`./test.sh 127.0.0.1 9092`, a port of the Go suite with the same
sections and checks, plus a section per configuration area showing each
setting change behaviour).

## No threads: what that changes

PHP has no background threads, so this client has no sender thread and no
heartbeat thread. Everything happens inside your calls, the way
librdkafka-less PHP clients work:

- **Producer.** Records are batched in-process per partition. A batch is
  sent by `send()` when it reaches `batch.size` (or at once when
  `linger.ms` is 0), by `send()`/`poll()`/`flush()` once its oldest record
  has waited `linger.ms`, and unconditionally by `flush()`/`close()`. Call
  `$producer->poll(0)` from a long-running loop so a lingering batch does
  not wait for the next `send()`, and always `flush()` or `close()` before
  the script ends (the destructor flushes as a last resort and warns if it
  cannot).
- **Producer errors.** A batch the call itself had to send (its record
  filled the batch, `linger.ms` is 0, `sendSync()`, `flush()`) throws from
  that call. A batch sent only because its linger expired while you called
  `send()`/`poll()` for something else is a *background* flush: its failure
  is held and thrown by the next `flush()` or `close()`, never dropped and
  never thrown from the unrelated call. With `delivery.report.callback`
  set, every outcome goes to the callback instead.
- **Ordering.** A partition has one open batch and batches are sent
  synchronously, so at most one batch per partition is ever in flight and
  records keep send order across the send/poll/flush/sendSync paths
  (`sendSync()` first sends the partition's open batch).
- **Broken connections.** A socket error or request timeout closes the
  connection (`Connection::isBroken()`); it is never reused, and the next
  request redials. Producers retry a dropped connection within `retries`;
  consumers resend an idempotent read once.
- **`buffer.memory` / `max.block.ms`.** When the buffer is full, `send()`
  blocks while sending any batch whose linger falls due; if that frees
  nothing within `max.block.ms` it throws `BufferFullException`.
- **Consumer groups.** `poll()` heartbeats every `heartbeat.interval.ms`
  (default `session.timeout.ms / 3`) while it waits, and `commit()`
  heartbeats too. Processing between two polls must therefore stay under
  `session.timeout.ms`; for longer work call `$consumer->heartbeat()` from
  your loop. `max.poll.interval.ms` bounds only the time *between* polls
  (it is stamped when `poll()` is entered and again when it returns, so a
  slow join inside `poll()` never counts). It is enforced at the next
  `poll()` (or `heartbeat()`): if it was exceeded the member leaves, drops
  its uncommitted positions and rejoins, as Java's heartbeat thread would
  have. `UNKNOWN_MEMBER_ID` on join, sync or heartbeat clears the member id
  and rejoins.
- Run one `GroupConsumer` per process. Two members in one PHP process
  cannot both answer a rebalance at once, because each blocks the other.

## Produce

```php
use Brahmaputra\Producer;
use Brahmaputra\RecordHeader;

$producer = new Producer([
    'bootstrap.servers' => '127.0.0.1:9092',
    'acks' => 'all',
    'linger.ms' => 5,
    'compression.type' => 'gzip',
]);

// Keyed: murmur2(key) % partitions, so records sharing a key keep order.
$producer->send('orders', '{"id":1}', 'user-7', [new RecordHeader('trace-id', 'abc-123')]);

// Explicit partition, explicit timestamp, a tombstone (null value):
$producer->send('orders', '{"id":2}', null, [], partition: 3);
$producer->send('orders', '{"id":3}', 'user-7', timestampMs: 1_700_000_000_000);
$producer->send('users', null, 'user-7');          // null = delete; '' is an empty value

// Or wait for one record's offset. A full round trip: correct, and slow.
$offset = $producer->sendSync('orders', '{"id":4}', 'user-9');

$producer->poll(0);   // send lingering batches from a worker loop
$producer->flush();   // send everything and wait for acks
$producer->close();
```

## Consume one partition

```php
use Brahmaputra\Consumer;
use Brahmaputra\Offset;

$consumer = new Consumer(['bootstrap.servers' => '127.0.0.1:9092']);

foreach ($consumer->fetch('orders', 0, 0, 500) as $record) {
    printf("%d %s %s\n", $record->offset, $record->key, $record->value);
}

$result = $consumer->fetchVerbose('orders', 0, 0);   // ->records, ->highWatermark
$start  = $consumer->listOffsets('orders', 0, Offset::EARLIEST);
$end    = $consumer->listOffsets('orders', 0, Offset::LATEST);
$atTime = $consumer->listOffsets('orders', 0, 1_700_000_000_000); // first offset at/after ts
$consumer->close();
```

## Consume as a group

```php
use Brahmaputra\GroupConsumer;

$consumer = new GroupConsumer([
    'bootstrap.servers' => '127.0.0.1:9092',
    'group.id' => 'billing',
    'partition.assignment.strategy' => 'sticky',
    'auto.offset.reset' => 'earliest',
    'enable.auto.commit' => false,        // commit explicitly
    'group.instance.id' => 'worker-3',    // static membership
]);
$consumer->subscribe(['orders']);

try {
    while (true) {
        foreach ($consumer->poll(500) as $record) {
            handle($record->value);
        }
        // At-least-once: commit after processing, never before.
        $consumer->commit();
    }
} finally {
    $consumer->close();   // commits, then LeaveGroup so partitions move at once
}
```

`$consumer->committed()` returns `TopicPartition` objects carrying the
committed `->offset`; `assignment()`, `memberId()` and `generation()`
expose the current membership.

## Compression

`none` and `gzip` are built in (gzip in the RFC 1952 container, i.e.
`gzencode`/`gzdecode`, matching the broker's flate2 `GzEncoder`). Others
are opt-in:

```php
use Brahmaputra\Protocol\Compression;

Compression::register(Compression::ZSTD, 'zstd_compress', 'zstd_uncompress'); // ext-zstd
```

If you register lz4, the broker expects a little-endian `uint32` of the
uncompressed length followed by a raw LZ4 **block**, not the LZ4 frame
format.

## Configuration

Kafka's names, passed as a flat array. Unknown keys are rejected.

**Producer**

| Key | Default | |
|---|---|---|
| `bootstrap.servers` | required | `host:port[,host:port]` |
| `client.id` | `brahmaputra-php` | |
| `acks` | `1` | `0`, `1`, `-1`/`all` |
| `batch.size` | `16384` | bytes per partition batch before it is sent |
| `linger.ms` | `5` | Kafka defaults to 0 |
| `compression.type` | `none` | `none`, `gzip`, or a registered codec |
| `request.timeout.ms` | `30000` | broker-side ack wait, and the client-side round-trip deadline |
| `retries` | `5` | retriable broker errors and connection failures |
| `retry.backoff.ms` | `100` | |
| `delivery.timeout.ms` | `120000` | caps a batch from its oldest record's send() to its last retry |
| `buffer.memory` | `33554432` | unsent bytes held client-side |
| `max.block.ms` | `60000` | how long send() blocks on a full buffer |
| `socket.connection.setup.timeout.ms` | `10000` | |
| `delivery.report.callback` | `null` | `fn(DeliveryReport $r)`; failures go here instead of being thrown |

**Consumer**

| Key | Default |
|---|---|
| `bootstrap.servers` | required |
| `fetch.max.bytes` | `8388608` |
| `fetch.min.bytes` | `1` |
| `fetch.max.wait.ms` | `500` |
| `max.poll.records` | `500` |
| `isolation.level` | `read_uncommitted` (`read_committed`) |
| `client.rack` | `''` |
| `request.timeout.ms` | `30000` (round-trip deadline; a fetch adds its wait) |
| `socket.connection.setup.timeout.ms` | `10000` |

**Group consumer**: all consumer keys, plus

| Key | Default | |
|---|---|---|
| `group.id` | required | |
| `session.timeout.ms` | `10000` | Kafka defaults to 45000 |
| `heartbeat.interval.ms` | `0` | 0 means `session.timeout.ms / 3` |
| `rebalance.timeout.ms` | `3000` | |
| `max.poll.interval.ms` | `300000` | |
| `enable.auto.commit` | `true` | commits from inside poll() |
| `auto.commit.interval.ms` | `5000` | |
| `auto.offset.reset` | `earliest` | `earliest`, `latest`, `none` (throws `NoOffsetForPartitionException`) |
| `partition.assignment.strategy` | `range` | `range`, `roundrobin`, `sticky` |
| `group.instance.id` | `''` | static membership |

## Errors

Everything extends `Brahmaputra\Exception\BrahmaputraException`:
`ServerException` (broker error code in `->errorCode`), `ConnectionException`
/ `TimeoutException`, `ProtocolException`, `BufferFullException`,
`NoOffsetForPartitionException`.

## Notes on 64-bit PHP

Offsets and timestamps are native `int` (64-bit signed). Zigzag varints,
CRC32C and Kafka's `murmur2` are implemented with explicit masking and
logical shifts, so `murmur2('') === 275646681` and batches carry the same
CRC the broker computes.

## Running the tests

Start a broker, then:

```bash
./test.sh 127.0.0.1 9092            # lints src/, then runs the e2e suite
# or directly:
php test_manual.php 127.0.0.1 9092
```

It prints one line per check and ends with `81 passed, 0 failed`; the exit
status is non-zero on any failure. The suite (not the driver) needs the
`pcntl` and `posix` extensions of the PHP CLI: the connection-failure
section runs a small TCP proxy in a forked child, the retry section runs a
fault-injecting proxy (it answers Produce with a chosen error code) in a
forked child, and the long-poll and generation-fencing group sections run
the second party in a forked child while the parent polls.

## Not implemented

TLS/QUIC transports, transactions and the idempotent producer, as for the
other drivers (see `../README.md`). Authentication (`Connection::authenticate`,
SCRAM-SHA-256) is present but only usable once TLS lands.
