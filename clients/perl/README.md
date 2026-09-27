# Brahmaputra client for Perl

```bash
cd clients/perl
perl Makefile.PL && make && make test && make install
# or, without installing:
perl -I/path/to/clients/perl/lib your_program.pl
```

Pure Perl 5 (tested on 5.38), speaking the wire protocol directly over
TCP using **core modules only**: `IO::Socket::IP`, `IO::Compress::Gzip` /
`IO::Uncompress::Gunzip`, `Time::HiRes`, `Digest::SHA`, `MIME::Base64`,
`List::Util`, `Scalar::Util`. No CPAN, no XS, no ithreads.

Verified end to end against a live broker: **87/87 checks**
(`./test.sh 127.0.0.1 9092`: the Go suite's 54 checks plus 33 covering
the rest of the client contract, including retries and timeouts through a
fault-injecting proxy). Every module compiles clean under
`use strict; use warnings` (`perl -wc`).

## Single-threaded: what that changes

Perl ithreads are an optional build feature and heavy where present, so
this client has no sender thread and no heartbeat thread. Everything
happens inside your calls, the same design as the PHP driver:

- **Producer.** Records are batched in-process per partition. A batch is
  sent by `send()` when it reaches `batch.size` (or at once when
  `linger.ms` is 0), by `send()`/`poll()`/`flush()` once its oldest record
  has waited `linger.ms`, and unconditionally by `flush()`/`close()`. Call
  `$producer->poll(0)` from a long-running loop so a lingering batch does
  not wait for the next `send()`, and `flush()` or `close()` before the
  program ends. As a last resort, producers still open at exit are flushed
  from an `END` block (before global destruction), and one that goes out
  of scope is flushed by `DESTROY`; either warns if it cannot deliver.
- **Producer errors.** A batch the call itself had to send (its record
  filled the batch, `linger.ms` is 0, `send_sync()`, `flush()`) dies from
  that call. A batch sent only because its linger expired while you called
  `send()`/`poll()` for something else is a *background* flush: its failure
  is held and thrown by the next `flush()` or `close()`, never dropped and
  never thrown from the unrelated call. With `delivery.report.callback`
  set, every outcome goes to the callback instead.
- **Ordering.** A partition has one open batch and batches are sent
  synchronously, so at most one batch per partition is ever in flight and
  records keep send order across the send/poll/flush/send_sync paths
  (`send_sync()` first sends the partition's open batch).
- **Timeouts and broken connections.** Sockets are non-blocking and every
  wait is a `select()` against a deadline, so each round trip is bounded
  (120 s by default on a bare `Brahmaputra::Connection`,
  `request.timeout.ms` + 5 s on client requests). A timeout, socket error
  or correlation-id mismatch closes the connection and marks it
  `$conn->broken`; it is never reused, and the router redials on next use
  (the bootstrap connection included). A producer retries, within
  `retries`, a batch that never left because the leader could not be
  reached; one whose connection failed after the request was written is
  not resent (the broker may already have appended it), so that error
  reaches the caller. Consumers resend an idempotent read once.
- **`buffer.memory` / `max.block.ms`.** When the buffer is full, `send()`
  blocks while sending any batch whose linger falls due; if that frees
  nothing within `max.block.ms` it dies with
  `Brahmaputra::Error::BufferFull`.
- **Consumer groups.** `poll()` heartbeats every `heartbeat.interval.ms`
  (default `session.timeout.ms / 3`) while it waits, and `commit()`
  heartbeats too. Processing between two polls must therefore stay under
  `session.timeout.ms`; for longer work call `$consumer->heartbeat` from
  your loop. `max.poll.interval.ms` bounds only the time *between* polls:
  it is stamped when `poll()` is entered and again when it returns, and is
  never enforced while inside `poll()`, so a slow join never counts. It is
  enforced at the next `poll()` (or `heartbeat()`): if it was exceeded the
  member leaves, drops its uncommitted positions and rejoins, as Java's
  heartbeat thread would have. `UNKNOWN_MEMBER_ID` on join, sync or
  heartbeat clears the member id and rejoins as a new member.
- Run one `GroupConsumer` per process: two members in one single-threaded
  process cannot both answer a rebalance at once.
- **fork.** A client created before `fork` belongs to the parent: a child
  never flushes, commits or leaves the group on its behalf at exit.
  Create new clients in the child.

## Produce

```perl
use Brahmaputra;

my $producer = Brahmaputra::Producer->new({
    'bootstrap.servers' => '127.0.0.1:9092',
    'acks'              => 'all',
    'linger.ms'         => 5,
    'compression.type'  => 'gzip',
});

# Keyed: murmur2(key) % partitions, so records sharing a key keep order.
$producer->send(topic => 'orders', value => '{"id":1}', key => 'user-7',
    headers => [['trace-id', 'abc-123'], ['retry', undef]]);   # undef header value = null

# Explicit partition, explicit timestamp, a tombstone (undef value):
$producer->send(topic => 'orders', value => '{"id":2}', partition => 3);
$producer->send(topic => 'orders', value => '{"id":3}', key => 'user-7', timestamp => 1_700_000_000_000);
$producer->send(topic => 'users', key => 'user-7', value => undef);   # delete; '' is an empty value

# Or wait for one record's offset. A full round trip: correct, and slow.
my $offset = $producer->send_sync(topic => 'orders', value => '{"id":4}', key => 'user-9');

$producer->poll(0);   # send lingering batches from a worker loop
$producer->flush;     # send everything and wait for acks
$producer->close;
```

Keys, values and headers are byte strings. A character string (one with
the UTF-8 flag on) is encoded as UTF-8 on the way out; what comes back is
always bytes.

## Consume one partition

```perl
use Brahmaputra;
use Brahmaputra::Consumer qw(EARLIEST LATEST);

my $consumer = Brahmaputra::Consumer->new({ 'bootstrap.servers' => '127.0.0.1:9092' });

for my $record ($consumer->fetch('orders', 0, 0, 500)) {   # topic, partition, offset, max wait ms
    printf "%d %s %s\n", $record->offset, $record->key // '(null)', $record->value // '(tombstone)';
    my $trace = $record->header('trace-id');
}

my $result = $consumer->fetch_verbose('orders', 0, 0);    # { records => [...], high_watermark => N }
my $start  = $consumer->list_offsets('orders', 0, EARLIEST);
my $end    = $consumer->list_offsets('orders', 0, LATEST);  # also ->high_watermark('orders', 0)
my $atTime = $consumer->list_offsets('orders', 0, 1_700_000_000_000);  # first offset at/after ts
$consumer->close;
```

## Consume as a group

```perl
my $consumer = Brahmaputra::GroupConsumer->new({
    'bootstrap.servers'             => '127.0.0.1:9092',
    'group.id'                      => 'billing',
    'partition.assignment.strategy' => 'sticky',
    'auto.offset.reset'             => 'earliest',
    'enable.auto.commit'            => 0,            # commit explicitly
    'group.instance.id'             => 'worker-3',   # static membership
});
$consumer->subscribe('orders');

while (1) {
    for my $record ($consumer->poll(500)) {
        handle($record->value);
    }
    # At-least-once: commit after processing, never before.
    $consumer->commit;
}
$consumer->close;   # commits, then LeaveGroup so partitions move at once
```

`$consumer->committed` returns `Brahmaputra::TopicPartition` objects
carrying the committed `->offset`; `assignment`, `member_id` and
`generation` expose the current membership.

## Compression

`none` and `gzip` are built in (gzip in the RFC 1952 container via core
`IO::Compress::Gzip`, matching the broker's flate2 `GzEncoder`). Others
are opt-in:

```perl
use Compress::Zstd ();   # from CPAN, if you want it
Brahmaputra::Compression::register(Brahmaputra::Compression::ZSTD,
    sub { Compress::Zstd::compress($_[0]) },
    sub { Compress::Zstd::decompress($_[0]) });
```

If you register lz4, the broker expects a little-endian `uint32` of the
uncompressed length followed by a raw LZ4 **block**, not the LZ4 frame
format.

## Configuration

Kafka's names, passed as a hash reference. Unknown keys are rejected.

**Producer**

| Key | Default | |
|---|---|---|
| `bootstrap.servers` | required | `host:port[,host:port]` |
| `client.id` | `brahmaputra-perl` | |
| `acks` | `1` | `0`, `1`, `-1`/`all` |
| `batch.size` | `16384` | bytes per partition batch before it is sent |
| `linger.ms` | `5` | Kafka defaults to 0 |
| `compression.type` | `none` | `none`, `gzip`, or a registered codec (an unregistered one croaks in `new`) |
| `request.timeout.ms` | `30000` | broker-side ack wait; the socket waits 5 s longer |
| `retries` | `5` | broker errors returned before the append, and failures to reach the leader |
| `retry.backoff.ms` | `100` | |
| `delivery.timeout.ms` | `120000` | caps a batch from its oldest record's `send()` to its last retry |
| `buffer.memory` | `33554432` | unsent bytes held client-side |
| `max.block.ms` | `60000` | how long `send()` blocks on a full buffer |
| `socket.connection.setup.timeout.ms` | `10000` | |
| `delivery.report.callback` | `undef` | `sub { my ($report) = @_ }` (a `Brahmaputra::DeliveryReport`); failures go here instead of dying |

**Consumer**

| Key | Default |
|---|---|
| `bootstrap.servers` | required |
| `client.id` | `brahmaputra-perl` |
| `fetch.max.bytes` | `8388608` |
| `fetch.min.bytes` | `1` |
| `fetch.max.wait.ms` | `500` |
| `max.poll.records` | `500` |
| `isolation.level` | `read_uncommitted` (or `read_committed`) |
| `client.rack` | `''` |
| `request.timeout.ms` | `30000` |
| `socket.connection.setup.timeout.ms` | `10000` |

**Group consumer**: all consumer keys, plus

| Key | Default | |
|---|---|---|
| `group.id` | required | |
| `session.timeout.ms` | `10000` | Kafka defaults to 45000 |
| `heartbeat.interval.ms` | `0` | 0 means `session.timeout.ms / 3` |
| `rebalance.timeout.ms` | `3000` | |
| `max.poll.interval.ms` | `300000` | |
| `enable.auto.commit` | `1` | commits from inside `poll()` |
| `auto.commit.interval.ms` | `5000` | |
| `auto.offset.reset` | `earliest` | `earliest`, `latest`, `none` (dies with `Brahmaputra::Error::NoOffset`) |
| `partition.assignment.strategy` | `range` | `range`, `roundrobin`, `sticky` |
| `group.instance.id` | `''` | static membership |

## Errors

Everything dies with an object that stringifies to its message, under
`Brahmaputra::Error`: `::Server` (broker error code in `->code`),
`::Connection` and its subclass `::Timeout`, `::Protocol`, `::BufferFull`,
`::NoOffset`. Invalid configuration croaks with a plain message.

```perl
eval { $producer->flush; 1 } or do {
    my $error = $@;
    if (ref $error && $error->isa('Brahmaputra::Error::Server')) { ... $error->code ... }
};
```

## Notes on 64-bit integers

Offsets and timestamps are native 64-bit integers (Perl must be built
with 64-bit IVs, the default on every 64-bit platform). Perl's bit
operators work on unsigned values, which is exactly what zigzag encoding
needs, so `zigzag64` is `($v << 1) ^ ($v < 0 ? ~0 : 0)` and decoding
converts back with arithmetic that cannot overflow; int64 min and max
round-trip (`t/01-unit.t`). CRC32C and Kafka's `murmur2` keep every
intermediate masked to 32 bits, so `murmur2('') == 275646681`.

## Running the tests

Start a broker, then:

```bash
./test.sh 127.0.0.1 9092        # compile-checks everything, runs t/01-unit.t, then the e2e suite
# or directly:
perl t/manual_test.pl 127.0.0.1 9092
prove t/                         # offline unit checks only (also: make test)
```

The e2e suite prints one line per check and ends with `87 passed, 0 failed`;
the exit status is non-zero on any failure. It needs `fork` (it is not run
on Windows): the connection-failure and retry sections run small TCP
proxies in forked children (one of them answers Produce with injected error
codes), and the long-poll group section produces from a forked child while
the parent polls.

## Not implemented

TLS/QUIC transports, transactions and the idempotent producer, as for the
other drivers (see `../README.md`). Authentication
(`$connection->authenticate`, SCRAM-SHA-256 via core `Digest::SHA`) is
present but only usable once TLS lands.
