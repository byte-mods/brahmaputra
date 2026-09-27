# Client drivers

Clients for Brahmaputra in twenty-four languages. The Rust client is the
[`brahmaputra-client`](../crates/client) crate. Eighteen more speak the
wire protocol natively: no FFI, no sidecar, no shared native library to
ship. Kotlin, Scala and F# are idiomatic libraries over the verified Java
and .NET drivers, the way Kafka's JVM users share one engine. TypeScript
ships as type declarations for the Node.js driver.

| Language | Directory | Build | End-to-end suite, live broker |
|---|---|---|---|
| Rust | [../crates/client](../crates/client) | `cargo`, clippy clean | ✅ **85/85** (`examples/manual_test.rs`) |
| Go | [go/](go) | `go vet` clean, suite runs under `-race` | ✅ **80/80** |
| Node.js | [nodejs/](nodejs) | Node 22, no dependencies | ✅ **83/83** |
| TypeScript | [typescript/](typescript) | typings in `nodejs/src/index.d.ts`, strict `tsc` | ✅ **83/83** |
| Python | [python/](python) | Python ≥ 3.9, no dependencies | ✅ **81/81** |
| Java | [java/](java) | Java 17, `javac -Xlint:all -Werror`; Maven `pom.xml` | ✅ **88/88** |
| Kotlin | [kotlin/](kotlin) | over the Java driver; coroutines, DSL, Flow | ✅ **88/88** |
| Scala | [scala/](scala) | over the Java driver; Scala 3, Try/Future | ✅ **88/88** |
| C# / .NET | [dotnet/](dotnet) | .NET 8, warnings as errors, no NuGet packages | ✅ **88/88** |
| F# | [fsharp/](fsharp) | over the .NET driver; Result/Async/Task | ✅ **88/88** |
| C++ | [cpp/](cpp) | C++17, CMake, `-Wall -Wextra -Wpedantic` clean, TSan clean | ✅ **87/87** |
| C | [c/](c) | C11, Make or CMake, `-Werror` clean, ASan/UBSan/TSan clean | ✅ **88/88** |
| D | [d/](d) | LDC, `-w`; dub.json | ✅ **87/87** |
| PHP | [php/](php) | PHP 8, Composer package with a no-Composer autoloader | ✅ **81/81** |
| Ruby | [ruby/](ruby) | Ruby 3, gem, stdlib only | ✅ **87/87** |
| Perl | [perl/](perl) | Perl 5.38, core modules only | ✅ **87/87** |
| Lua | [lua/](lua) | Lua 5.4, LuaSocket (+ lua-zlib for gzip) | ✅ **87/87** |
| Erlang | [erlang/](erlang) | OTP 25+, rebar3 layout, `erlc -Werror` | ✅ **87/87** |
| Elixir | [elixir/](elixir) | Elixir 1.14+, `mix compile --warnings-as-errors`, no deps | ✅ **85/85** |
| Haskell | [haskell/](haskell) | GHC 9.4, `-Wall -Werror`, boot packages + network/zlib | ✅ **85/85** |
| OCaml | [ocaml/](ocaml) | OCaml 4.14, camlzip; dune/opam files shipped | ✅ **85/85** |
| Crystal | [crystal/](crystal) | Crystal 1.11 shard, stdlib only | ✅ **85/85** |
| Nim | [nim/](nim) | Nim 1.6, stdlib + system zlib | ✅ **85/85** |
| Dart | [dart/](dart) | Dart 3, `dart analyze` clean, no packages | ✅ **86/86** |

Every suite starts from the same 54-check port of the Go suite
([go/cmd/manualtest](go/cmd/manualtest/main.go)). Each then adds the
feature-audit checks: settings, retries through a fault-injecting proxy,
fetch limits, group behaviour and decoder bounds. That puts every client
at 80–88 checks against a live broker. The
**[feature matrix](../docs/client-feature-matrix.md)** shows every
producer, consumer and group feature, language by language. CI runs every
suite on each push (the `clients` job).

Single-threaded runtimes (PHP, Perl, Lua) batch inside
`send`/`poll`/`flush` and heartbeat from `poll`, documented in each README.
The rest use background threads, tasks or processes for linger and
heartbeats.

For mobile and browser clients, which should not hold broker
connections, see the [WebSocket gateway](../crates/gateway).

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
| D | `none`, `gzip` (`std.zlib`) | others via `registerCodec` |
| Perl | `none`, `gzip` (core IO::Compress) | others via `Brahmaputra::Compression::register` |
| Lua | `none`, `gzip` (with lua-zlib) | others via `register_codec` |
| Haskell | `none`, `gzip` (`zlib` package) | others via `registerCodec` |
| OCaml | `none`, `gzip` (camlzip) | others via the codec registry |
| Crystal | `none`, `gzip` (`Compress::Gzip`) | others via `Brahmaputra.register_codec` |
| Nim | `none`, `gzip` (system zlib) | others via the codec registry |
| Dart | `none`, `gzip` (`dart:io`) | others via `registerCodec` |
| Kotlin, Scala, F#, TypeScript | as the Java / .NET / Node driver underneath | |

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
clients/run-e2e.sh                  # all twenty-four
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
