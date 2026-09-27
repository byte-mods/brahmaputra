# Client drivers

Native clients for Brahmaputra in twelve languages. The Rust client is the
[`brahmaputra-client`](../crates/client) crate; every other driver speaks
the wire protocol directly rather than wrapping it, so there is no FFI, no
sidecar and no shared native library to ship.

| Language | Directory | Build | End-to-end suite, live broker |
|---|---|---|---|
| Rust | [../crates/client](../crates/client) | `cargo`, clippy clean | ✅ **56/56** (`examples/manual_test.rs`) |
| Go | [go/](go) | `go vet` clean, suite runs under `-race` | ✅ **54/54** |
| Node.js | [nodejs/](nodejs) | Node 22, no dependencies | ✅ **57/57** |
| Python | [python/](python) | Python ≥ 3.9, no dependencies | ✅ **54/54** |
| Java | [java/](java) | Java 17, `javac -Xlint:all -Werror`; Maven `pom.xml` | ✅ **54/54** |
| C# / .NET | [dotnet/](dotnet) | .NET 8, warnings as errors, no NuGet packages | ✅ **54/54** |
| C++ | [cpp/](cpp) | C++17, CMake, `-Wall -Wextra -Wpedantic` clean, TSan clean | ✅ **54/54** |
| C | [c/](c) | C11, Make or CMake, `-Werror` clean, ASan/UBSan/TSan clean | ✅ **54/54** |
| PHP | [php/](php) | PHP 8, Composer package with a no-Composer autoloader | ✅ **54/54** |
| Ruby | [ruby/](ruby) | Ruby 3, gem, stdlib only | ✅ **54/54** |
| Erlang | [erlang/](erlang) | OTP 25+, rebar3 layout, `erlc -Werror` | ✅ **54/54** |
| Elixir | [elixir/](elixir) | Elixir 1.14+, `mix compile --warnings-as-errors`, no deps | ✅ **54/54** |

Every suite is a port of the Go suite
([go/cmd/manualtest](go/cmd/manualtest/main.go)) with the same sections
and checks, so the numbers are comparable; Node and Rust carry a few
extra checks of their own. CI runs every suite against a live broker on
each push (the `clients` job).

## What every driver implements

The configuration names mirror Kafka's, because the point of a client
library is that someone who knows Kafka does not have to learn a new
vocabulary to use this one.

**Producer** — `acks` (0/1/all), `batch.size`, `linger.ms`,
`compression.type`, `request.timeout.ms`, `retries`, `retry.backoff.ms`,
`delivery.timeout.ms`, `buffer.memory`, `max.block.ms`, keyed partitioning
via Kafka's `murmur2`, explicit-partition sends, record headers, and
per-record timestamps.

**Consumer** — `fetch.max.bytes`, `fetch.min.bytes`, `fetch.max.wait.ms`,
`max.poll.records`, offset lookup by `earliest`/`latest`/timestamp, and
the partition high watermark.

**Consumer groups** — join/sync/heartbeat with generation fencing,
`range`, `roundrobin` and `sticky` assignment, `auto.offset.reset`
(`earliest`/`latest`/`none`), `auto.commit.interval.ms`,
`session.timeout.ms`, `max.poll.interval.ms`, `group.instance.id` for
static membership, and an explicit `LeaveGroup` on close.

## The three encodings

Most of the work in each driver is keeping three encodings apart, because
they share one connection and none of them agrees with the others.

| | Encoding |
|---|---|
| **Frame header** | Fixed big-endian: `int32` length prefix, `int16` api key, `int16` api version, `int32` correlation id, `int16`-prefixed client id. |
| **Request/response body** | BitPacker. Every integer is a **zigzag varint**; every string and array is a varint count then its contents; the whole body is prefixed with the schema version string `"1.0.0"`. |
| **Record batch** | Neither. Fixed big-endian header fields, then **plain** (non-zigzag) varints inside each record. |

The record batch is different on purpose: the broker stamps `base_offset`
and `leader_epoch` into it in place — both sit before the CRC, so it stays
valid — and validates the CRC without decoding the records. The bytes a
producer sends are the bytes on disk, which is why a driver that encodes a
batch wrongly corrupts the log rather than merely failing a request.

Two details that are easy to get wrong and fail silently:

- **CRC32C, not CRC32.** Record batches use the Castagnoli polynomial.
  Every standard library's `crc32` is the wrong one.
- **`murmur2` must be Kafka's.** A Python producer and a Rust producer
  writing the same key have to land on the same partition, so each driver
  transcribes the algorithm rather than importing "a murmur2". All of them
  agree that `murmur2("") == 275646681`, which the test suites assert.

## Compression is deliberately not uniform

Each driver carries the codecs its standard library already has and makes
the rest opt-in, so an application that does not want an lz4 or zstd
dependency does not acquire one by using this client.

| Driver | Built in | Opt-in |
|---|---|---|
| Go | `none`, `gzip` | `lz4`, `zstd`, `snappy` via `RegisterCodec` |
| Node.js | `none`, `gzip`, `zstd` (Node ≥ 22.15) | `lz4`, `snappy` via `registerCodec` |
| Python | `none`, `gzip` | `lz4`, `zstd`, `snappy` via `pip install brahmaputra[all]`, or `register_codec` |
| Java | `none`, `gzip` | `lz4`, `zstd`, `snappy` via `Protocol.registerCodec` |
| .NET | `none`, `gzip` | others via `Codecs.Register` |
| C++ | `none`, `gzip` (zlib, CMake option) | others via `registerCodec` |
| C | `none`, `gzip` (zlib, build flag) | others via `brp_register_codec` |
| PHP | `none`, `gzip` (ext-zlib) | others via `Compression::register` |
| Ruby | `none`, `gzip` | others via `Brahmaputra.register_codec` |
| Erlang | `none`, `gzip` (`zlib`) | others via `brahmaputra_protocol:register_codec/3` |
| Elixir | `none`, `gzip` (`:zlib`) | others via `Brahmaputra.register_codec/3` |

If you register lz4 yourself, note that the broker uses
`lz4_flex::compress_prepend_size`: a little-endian `u32` of the
uncompressed length followed by a **raw LZ4 block**. That is not the LZ4
frame format, so a frame-format library will produce batches the broker
cannot read.

## Running the test suites

One command starts a private broker and runs every driver whose toolchain
is installed, then prints a summary. A driver without its toolchain shows
as SKIP, never as a pass:

```bash
clients/run-e2e.sh                  # all twelve
clients/run-e2e.sh go python c      # just these
BROKER_ADDR=127.0.0.1:9092 clients/run-e2e.sh   # against a broker you run
```

Each driver also runs on its own against any broker:

```bash
brahmaputra-server --data-dir ./data --default-partitions 4
clients/<driver>/test.sh 127.0.0.1 9092
cargo run --release -p brahmaputra-client --example manual_test -- 127.0.0.1 9092
```

Each suite asserts properties of the system rather than that a function
ran:
- Records come back byte-identical with contiguous offsets.
- A key pins every record to one partition and keeps its order there.
- Headers, null header values and empty-but-not-null keys and values
  survive, and a tombstone stays distinct from an empty value.
- Timestamps are real wall-clock values.
- Linger and batch-full flushes never reorder a partition, and a failed
  background flush is reported rather than dropped.
- A timed-out connection is never reused, and a dropped one is redialled.
- A group splits partitions, and a rejoining member resumes from its
  commit instead of replaying.
- A member that exceeds `max.poll.interval.ms` leaves and rejoins, while
  time spent inside `poll` does not count against it.
- `auto.offset.reset=none` refuses to guess.
- A full client buffer blocks and then reports, rather than growing
  without limit.

## What the drivers other than Rust do not do

- **No Kafka wire compatibility.** These speak Brahmaputra's protocol.
  Existing Kafka clients do not work against this broker, and these
  drivers do not work against Kafka.
- **No TLS or QUIC.** They speak plaintext TCP. The Rust client supports
  `--transport tcp-tls` and `--transport quic`; the others do not yet.
  Several implement `Authenticate` (SCRAM-SHA-256), but the broker refuses
  credentials on a plaintext listener, so it is unusable until TLS lands.
- **No idempotent or transactional producer.** The broker and the Rust
  client support both. The other drivers do not allocate a producer id, so
  retrying an ambiguous send, such as one whose connection dropped
  mid-request, can duplicate a record.
- **Commits during a rebalance can be refused.** The broker bumps a
  group's generation as soon as a rebalance starts, so a commit sent with
  the old generation is rejected before the member hears of the
  rebalance. Records processed since the last commit are then delivered
  again to the partition's next owner. This is at-least-once delivery, as
  documented; fixing it needs a broker change.
