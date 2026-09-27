# Brahmaputra client for OCaml

OCaml ≥ 4.14 with `unix`, `threads.posix` and camlzip (for gzip). No other
dependencies. The library is `brahmaputra`, with the modules `Protocol`,
`Connection`, `Router`, `Producer`, `Consumer`, `Group` and `Assignor`.

Verified end to end against a live broker: **54/54 checks**, the same checks
as the Go suite (`clients/go/cmd/manualtest`). Run them with
`./test.sh HOST PORT`.

## Build

With dune (and opam):

```bash
opam install camlzip        # or: apt install libzip-ocaml-dev
dune build                  # library and build/default/test/manual_test.exe
```

Without dune or opam, `ocamlfind` alone is enough. The Makefile compiles
with the default warning set and treats any warning as an error:

```bash
apt install ocaml-nox ocaml-findlib libzip-ocaml-dev
make                        # build/brahmaputra.cmxa, build/manual_test.exe
make byte                   # build/byte/brahmaputra.cma
```

Link your program against it with
`ocamlfind ocamlopt -thread -package unix,threads.posix,zip -linkpkg -I build build/brahmaputra.cmxa app.ml`.

## A note on integer types

OCaml's native `int` is 63 bits wide. So every value that goes on the wire
is an `Int32.t` or `Int64.t`: partitions and `acks` are `int32`, offsets
and timestamps are `int64`. The varint zigzag, CRC32C and murmur2 are
computed in those types too, so that they wrap where the broker's do.
Sizes and durations that never leave the process, such as `batch_size` or
`linger_ms`, are plain `int`.

## Produce

```ocaml
open Brahmaputra

let producer =
  Producer.create
    ~config:{ Producer.default_config with acks = -1l; linger_ms = 5; compression_type = "gzip" }
    "127.0.0.1:9092"

(* Keyed: murmur2(key) mod partitions, so records sharing a key keep order. *)
let () =
  Producer.send producer "orders" ~key:"user-7"
    ~headers:[ Protocol.Header.make "trace-id" "abc-123"; Protocol.Header.null "reason" ]
    (Some {|{"id":1}|});
  Producer.send producer "orders" ~partition:2l ~timestamp:1_700_000_000_000L (Some "raw");
  Producer.send producer "orders" ~key:"user-7" None;    (* tombstone: None, not Some "" *)

  (* A full round trip per record: correct, and slow. *)
  let offset = Producer.send_sync producer "orders" (Some {|{"id":2}|}) in
  Printf.printf "stored at %Ld\n" offset;

  Producer.flush producer;   (* raises the first delivery error, including a failed linger flush *)
  Producer.close producer    (* flushes, stops the linger thread, closes sockets *)
```

`send` only buffers the record. A partition's buffer goes out when it
reaches `batch_size`, or when the linger thread runs every `linger_ms`, or
when `flush` is called. A partition has one batch in flight at a time, so
order within a partition is preserved. When `buffer_memory` is full, `send`
blocks for up to `max_block_ms` and then raises `Protocol.Buffer_full`.

## Consume one partition

```ocaml
let consumer = Consumer.create "127.0.0.1:9092"

let () =
  let records, high_watermark = Consumer.fetch_verbose consumer "orders" 0l 0L in
  List.iter
    (fun (r : Consumer.record) ->
      Printf.printf "%Ld %s %s\n" r.offset
        (Option.value r.key ~default:"<null>")
        (Option.value r.value ~default:"<tombstone>"))
    records;
  ignore high_watermark;
  let _end = Consumer.list_offsets consumer "orders" 0l Protocol.latest in
  let _at = Consumer.list_offsets consumer "orders" 0l 1_700_000_000_000L in
  Consumer.close consumer
```

## Consume as a group

```ocaml
let group =
  Group.create
    ~config:
      { Group.default_config with
        assignor = `Sticky;
        auto_offset_reset = `Earliest;
        auto_commit_interval_ms = 0;          (* commit explicitly *)
        group_instance_id = "worker-3" }      (* static membership *)
    "127.0.0.1:9092" "billing"

let () =
  Group.subscribe group [ "orders" ];
  Fun.protect ~finally:(fun () -> Group.close group) (* commits, then leaves *)
    (fun () ->
      while true do
        let records = Group.poll group ~timeout_ms:500 in
        List.iter handle records;
        (* At-least-once: commit after processing, never before. *)
        Group.commit group
      done)
```

A background heartbeat thread keeps the membership alive. It also enforces
`max_poll_interval_ms`: a member that goes that long between polls leaves
the group, and it rejoins on its next `poll`. Time spent inside `poll`
never counts against the interval, because the poll timestamp is stamped
on entry and again on exit. When `auto_offset_reset` is `` `None `` and there is
no committed offset, `poll` raises `Protocol.No_offset_for_partition`.

## Errors

| Exception | Meaning |
|---|---|
| `Protocol.Server_error { code; context }` | The broker returned a non-zero error code. `Protocol.error_name code` gives its name. |
| `Protocol.Connection_error msg` | I/O failure, round-trip timeout, or correlation mismatch. The connection is closed and `Connection.broken` becomes true. The router redials on the next use, including the seed connection. |
| `Protocol.Decode_error msg` | A malformed response or record batch, such as a truncated one, a bad length, a CRC mismatch or a schema mismatch. |
| `Protocol.Buffer_full msg` | `buffer_memory` stayed full for `max_block_ms`. |
| `Protocol.No_offset_for_partition { topic; partition }` | `auto_offset_reset` is `` `None `` and there is nothing to resume from. |

## Configuration

**`Producer.config`** (`Producer.default_config`)

| Field | Kafka name | Default |
|---|---|---|
| `acks : int32` | `acks` | `1l` (`0l`, `1l`, `-1l` = all) |
| `batch_size` | `batch.size` | 16384 bytes |
| `linger_ms` | `linger.ms` | 5 (0 sends every record at once) |
| `compression_type` | `compression.type` | `"none"` (`"gzip"` built in) |
| `request_timeout_ms : int32` | `request.timeout.ms` | 30000 (how long the broker waits for acks) |
| `retries` | `retries` | 5 (retriable errors only) |
| `retry_backoff_ms` | `retry.backoff.ms` | 100 |
| `delivery_timeout_ms` | `delivery.timeout.ms` | 120000 |
| `buffer_memory` | `buffer.memory` | 32 MiB |
| `max_block_ms` | `max.block.ms` | 60000 |
| `client_id`, `dial_timeout_ms`, `socket_timeout_ms` | | `"brahmaputra-ocaml"`, 30000, 120000 (client-side round-trip bound) |

**`Consumer.config`**: `fetch_max_bytes` (8 MiB), `fetch_min_bytes` (1),
`fetch_max_wait_ms` (500), `max_poll_records` (500), `rack`
(`client.rack`), `isolation_level` (`Protocol.read_uncommitted`),
`client_id`, `dial_timeout_ms`, `socket_timeout_ms`.

**`Group.config`**: `session_timeout_ms` (10000l), `rebalance_timeout_ms`
(3000l), `max_poll_interval_ms` (300000), `auto_commit_interval_ms` (5000,
where 0 disables auto commit), `auto_offset_reset` (`` `Earliest `` /
`` `Latest `` / `` `None ``), `assignor` (`` `Range `` / `` `Roundrobin `` /
`` `Sticky ``), `group_instance_id` (`""` for a dynamic member),
`max_poll_records`, `fetch_max_bytes`, `client_id`, `dial_timeout_ms`,
`socket_timeout_ms`.

A connection's round-trip timeout defaults to 120 s. It can be changed per
client with `socket_timeout_ms`, or per connection with
`Connection.set_request_timeout_ms`.

## Compression

`none` and `gzip` are built in. gzip uses camlzip's deflate with gzip
framing written by this library. The other codecs are opt-in, so this
library pulls in no dependencies for them:

```ocaml
Protocol.register_codec `Zstd ~compress:Zstd.compress ~decompress:Zstd.decompress
```

If you register lz4, note that the broker expects a little-endian `uint32`
of the uncompressed length followed by a raw LZ4 **block**. That is not the
LZ4 frame format, which a frame-format library would silently produce
instead.

## Test

```bash
brahmaputra-server --data-dir ./data --default-partitions 4
clients/ocaml/test.sh 127.0.0.1 9092     # builds, then runs build/manual_test.exe
```

The script works from any directory and exits non-zero on any failed check.

## Not implemented

These match the other non-Rust drivers: no TLS or QUIC, no SASL
`Authenticate`, and no idempotent or transactional producer.
