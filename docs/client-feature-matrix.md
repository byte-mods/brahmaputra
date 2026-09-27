# Client feature matrix

Every Brahmaputra client library, checked against one feature contract.
Each ✅ is backed by an implementation (file:line) **and** a check in that
language's live end-to-end suite, which runs against a real broker. The
suites are what CI's `clients` job runs, and what `clients/run-e2e.sh`
runs locally.

The matrix was produced by auditing all 24 clients feature by feature
against the Go reference. Gaps were then fixed and a test added for every
feature that had none (see [what the audit changed](#what-the-audit-changed)).

## Live suite results

| Language | Checks | Language | Checks | Language | Checks | Language | Checks |
|---|---|---|---|---|---|---|---|
| Rust | **85/85** | Go | **80/80** | Node.js | **83/83** | TypeScript | **83/83** |
| Python | **81/81** | PHP | **81/81** | Java | **88/88** | Kotlin | **88/88** |
| Scala | **88/88** | C#/.NET | **88/88** | F# | **88/88** | C | **88/88** |
| C++ | **87/87** | D | **87/87** | Ruby | **87/87** | Perl | **87/87** |
| Lua | **87/87** | Erlang | **87/87** | Elixir | **85/85** | Haskell | **85/85** |
| OCaml | **85/85** | Crystal | **85/85** | Nim | **85/85** | Dart | **86/86** |

## Producer, consumer and group features

| Feature | Rust | Go | Node.js | TypeScript | Python | PHP | Java | Kotlin | Scala | C#/.NET | F# | C |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **Producer** | | | | | | | | | | | | |
| P1 acks 0 / 1 / all | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P2 batch.size | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P3 linger.ms | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P4 compression: none + gzip built in, codec hook | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P5 request.timeout.ms | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P6 retries + retry.backoff.ms (retriable errors only) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P7 delivery.timeout.ms | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅<sup>b</sup> | ✅<sup>b</sup> | ✅<sup>b</sup> | ✅ | ✅ | ✅<sup>b</sup> |
| P8 buffer.memory + max.block.ms | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P9 murmur2 keyed partitioning | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P10 round-robin for null keys | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P11 explicit partition | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P12 headers, incl. null values | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P13 per-record timestamp | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P14 tombstone (null) vs empty value | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P15 flush / close; background errors surfaced | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P16 synchronous send returning offset | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P17 one batch in flight per partition | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Consumer** | | | | | | | | | | | | |
| C1 fetch topic/partition/offset | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| C2 fetch.max.bytes / min.bytes / max.wait.ms | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| C3 max.poll.records | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| C4 offsets: earliest / latest / by timestamp | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| C5 high watermark | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| C6 decodes compressed batches, headers, null vs empty | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| C7 metadata + leader routing | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Consumer groups** | | | | | | | | | | | | |
| G1 join / sync / heartbeat, generation fencing | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G2 range / roundrobin / sticky | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G3 auto.offset.reset earliest / latest / none | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G4 auto commit + interval | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G5 manual commit | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G6 session.timeout.ms / heartbeat.interval.ms | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️<sup>a</sup> | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G7 max.poll.interval.ms (not enforced inside poll) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G8 group.instance.id (static membership) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G9 LeaveGroup on close; rejoin after UNKNOWN_MEMBER_ID | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G10 multi-topic subscribe | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Connection** | | | | | | | | | | | | |
| N1 settable round-trip timeout; broken connections redialled | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| N2 bounds-checked decoding | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

| Feature | C++ | D | Ruby | Perl | Lua | Erlang | Elixir | Haskell | OCaml | Crystal | Nim | Dart |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **Producer** | | | | | | | | | | | | |
| P1 acks 0 / 1 / all | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P2 batch.size | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P3 linger.ms | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P4 compression: none + gzip built in, codec hook | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P5 request.timeout.ms | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P6 retries + retry.backoff.ms (retriable errors only) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P7 delivery.timeout.ms | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P8 buffer.memory + max.block.ms | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P9 murmur2 keyed partitioning | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P10 round-robin for null keys | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P11 explicit partition | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P12 headers, incl. null values | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P13 per-record timestamp | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P14 tombstone (null) vs empty value | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P15 flush / close; background errors surfaced | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P16 synchronous send returning offset | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| P17 one batch in flight per partition | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Consumer** | | | | | | | | | | | | |
| C1 fetch topic/partition/offset | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| C2 fetch.max.bytes / min.bytes / max.wait.ms | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| C3 max.poll.records | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| C4 offsets: earliest / latest / by timestamp | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| C5 high watermark | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| C6 decodes compressed batches, headers, null vs empty | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| C7 metadata + leader routing | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Consumer groups** | | | | | | | | | | | | |
| G1 join / sync / heartbeat, generation fencing | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G2 range / roundrobin / sticky | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G3 auto.offset.reset earliest / latest / none | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G4 auto commit + interval | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G5 manual commit | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G6 session.timeout.ms / heartbeat.interval.ms | ✅ | ✅ | ✅ | ⚠️<sup>a</sup> | ⚠️<sup>a</sup> | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G7 max.poll.interval.ms (not enforced inside poll) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G8 group.instance.id (static membership) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G9 LeaveGroup on close; rejoin after UNKNOWN_MEMBER_ID | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| G10 multi-topic subscribe | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Connection** | | | | | | | | | | | | |
| N1 settable round-trip timeout; broken connections redialled | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| N2 bounds-checked decoding | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

<sup>a</sup> PHP, Perl and Lua are single-threaded by design, so there is no
background heartbeat thread. Heartbeats are sent from inside `poll`,
`commit` and an explicit `heartbeat()`, at `heartbeat.interval.ms`. An
application that stays out of `poll` longer than `session.timeout.ms`
must call `heartbeat()` itself.

<sup>b</sup> As in the Go reference, `delivery.timeout.ms` is measured from a
batch's first send attempt. Time spent buffered is bounded separately by
`linger.ms` and `max.block.ms`. .NET measures from `Send`, as Kafka does.

Kotlin, Scala and F# are libraries over the Java and .NET drivers. A
feature counts for them only when their own API exposes it, not when it is
reachable only through `.underlying`. TypeScript's column is the Node
driver's features as its type declarations expose them, exercised by a
strict TypeScript port of the Node suite.

## What the audit changed

Implementations added where the audit found a gap:

- **Per-record timestamps** (P13): Rust, Go, Node/TypeScript, Python,
  Java/Kotlin/Scala, D, Crystal, Nim, Dart.
- **`heartbeat.interval.ms`** (G6): Go, Node/TypeScript, Python,
  Java/Kotlin/Scala, .NET/F#, C, C++, D, Erlang, Elixir, Haskell, OCaml,
  Crystal, Nim, Dart.
- **Settable round-trip timeouts on the router** (N1): Rust (default
  unbounded, so the broker, CLI and gateway are unchanged), Go, Node,
  Python.
- **Synchronous sends no longer overtake buffered records** for the same
  partition (P16/P17): Go, Node, Python, Java, C, D, Erlang. Partitioned
  sync send added to Java, D, Crystal, Nim.
- **`max.poll.records` on plain fetches** (C3): Java/Kotlin/Scala,
  .NET/F#, C.
- **Rejoin after a fenced commit** (G9): Java, .NET, C, Erlang.
- **Retry only retriable errors** (P6): Ruby, Perl and Lua resent a batch
  after a connection failure mid-request, which could write it twice.
- **Unregistered codecs refused at construction** (P4): D, Perl, Erlang.
- **`enable.auto.commit`** (G4): D, Erlang.
- **Rust `Producer::close`** (P15).
- **Decoder bounds** (N2): Node, Ruby.
- **A crash** (N1): Dart exited on an unhandled error when every redial
  was refused.

Tests added to every suite: batch size and linger, sync send offsets,
explicit partition and timestamp, round-robin, a registered codec through
the broker, and acks and request timeouts as seen on the wire. Retries,
backoff, non-retriable errors and the delivery timeout run against a
fault-injecting proxy. Also covered: fetch limits, offsets by timestamp,
high watermark, metadata leaders, multi-topic groups, auto commit,
heartbeats, session-timeout eviction, static membership, LeaveGroup,
generation fencing, two-member rebalances and malformed batch lengths.

## Broker behaviour the audit surfaced

Two broker behaviours the suites work around. Neither is changed in this
release:

- `ListOffsets` by timestamp resolves to a whole batch. A timestamp inside
  a multi-record batch returns that batch's base offset, not the first
  record at or after it.
- `JoinGroup` accepts an unknown non-empty member id instead of answering
  `UNKNOWN_MEMBER_ID`, and reuses member ids such as `member-0` once a
  group empties.
