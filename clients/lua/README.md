# Brahmaputra client for Lua

```bash
luarocks install brahmaputra              # from brahmaputra-0.1.0-1.rockspec
# or, with no LuaRocks: the system packages plus a LUA_PATH entry
sudo apt install lua5.4 lua-socket lua-zlib
export LUA_PATH="/path/to/clients/lua/?.lua;;"
```

Pure Lua 5.4, speaking the wire protocol directly over LuaSocket. It needs
only **LuaSocket**; **lua-zlib** is optional and enables the gzip codec.
Nothing is compiled: CRC32C, Kafka's murmur2, zigzag varints and the record
batch codec are written in Lua on its native 64-bit integers.

Verified end to end against a live broker: **87/87 checks**
(`./test.sh HOST PORT`: the Go suite's 54 checks plus 33 covering the rest
of the client contract, including retries and timeouts through a
fault-injecting proxy).

## No threads: what that changes

Lua has no threads, so this client has no sender thread and no heartbeat
thread. Everything happens inside your calls (the same design as the PHP
driver):

- **Producer.** Records are batched in-process per partition. A batch is
  sent by `send()` when it reaches `batch.size` (or at once when
  `linger.ms` is 0), by `send()`/`poll()`/`flush()` once its oldest record
  has waited `linger.ms`, and unconditionally by `flush()`/`close()`. Call
  `producer:poll(0)` from a long-running loop so a lingering batch does not
  wait for the next `send()`, and always `flush()` or `close()` before the
  program ends (a `__gc` finalizer flushes as a last resort and warns on
  stderr if it cannot).
- **Producer errors.** A batch the call itself had to send (its record
  filled the batch, `linger.ms` is 0, `sendSync()`, `flush()`) raises from
  that call. A batch sent only because its linger expired while you called
  `send()`/`poll()` for something else is a *background* flush: its failure
  is held and raised by the next `flush()` or `close()`, never dropped and
  never raised from the unrelated call. `close()` releases its connections
  even when it raises. With `delivery.report.callback` set, every outcome
  goes to the callback instead.
- **Ordering.** A partition has one open batch and batches are sent
  synchronously, so at most one batch per partition is ever in flight and
  records keep send order across the send/poll/flush/sendSync paths
  (`sendSync()` first sends the partition's open batch).
- **Broken connections.** Every round trip has a timeout
  (`Connection.DEFAULT_REQUEST_TIMEOUT_MS` = 120 s for a bare connection;
  clients use `request.timeout.ms` plus the request's own wait). A socket
  error, timeout or correlation-id mismatch closes the connection and sets
  `conn.broken`; it is never reused, and the router redials on next use,
  the seed connection included. An idle connection the broker closed is
  noticed before it is used. A producer retries, within `retries`, a batch
  that never left because the leader could not be reached; one whose
  connection failed after the request was written is not resent (the
  broker may already have appended it), so that error reaches the caller.
  Consumers resend an idempotent read once.
- **`buffer.memory` / `max.block.ms`.** When the buffer is full, `send()`
  blocks while sending any batch whose linger falls due; if that frees
  nothing within `max.block.ms` it raises `BufferFullError`.
- **Consumer groups.** `poll()` heartbeats every `heartbeat.interval.ms`
  (default `session.timeout.ms / 3`) while it waits, and `commit()`
  heartbeats too. Processing between two polls must therefore stay under
  `session.timeout.ms`; for longer work call `consumer:heartbeat()` from
  your loop. `max.poll.interval.ms` bounds only the time *between* polls: it
  is stamped when `poll()` is entered and again when it returns and never
  enforced while inside `poll()`, so a slow join never counts. It is
  enforced at the next `poll()` (or `heartbeat()`): if it was exceeded the
  member leaves, drops its uncommitted positions and rejoins, as Java's
  heartbeat thread would have. `UNKNOWN_MEMBER_ID` on join, sync or
  heartbeat clears the member id and rejoins as a new member.
- Run one `GroupConsumer` per process (or coroutine scheduler). Two members
  in one Lua process cannot both answer a rebalance at once, because each
  blocks the other.

## Produce

```lua
local brahmaputra = require("brahmaputra")
local header = brahmaputra.header

local producer = brahmaputra.Producer.new({
  ["bootstrap.servers"] = "127.0.0.1:9092",
  ["acks"] = "all",
  ["linger.ms"] = 5,
  ["compression.type"] = "gzip",
})

-- send(topic, value, key, opts). Keyed: murmur2(key) % partitions, so
-- records sharing a key keep order.
producer:send("orders", '{"id":1}', "user-7", { headers = { header("trace-id", "abc-123") } })

-- Explicit partition, explicit timestamp, a tombstone (nil value):
producer:send("orders", '{"id":2}', nil, { partition = 3 })
producer:send("orders", '{"id":3}', "user-7", { timestamp = 1700000000000 })
producer:send("users", nil, "user-7")     -- nil = delete; "" is an empty value
producer:send("orders", "x", nil, { headers = { header("reason", nil) } })  -- nil header value

-- Or wait for one record's offset. A full round trip: correct, and slow.
local offset = producer:sendSync("orders", '{"id":4}', "user-9")

producer:poll(0)   -- send lingering batches from a worker loop
producer:flush()   -- send everything and wait for acks
producer:close()
```

## Consume one partition

```lua
local consumer = brahmaputra.Consumer.new({ ["bootstrap.servers"] = "127.0.0.1:9092" })

for _, record in ipairs(consumer:fetch("orders", 0, 0, 500)) do
  -- record: topic, partition, offset, key, value, timestamp, headers
  print(record.offset, record.key, record.value)
end

local records, highWatermark = consumer:fetchVerbose("orders", 0, 0)
local startOffset = consumer:listOffsets("orders", 0, brahmaputra.EARLIEST)
local endOffset   = consumer:listOffsets("orders", 0, brahmaputra.LATEST)
local atTime      = consumer:listOffsets("orders", 0, 1700000000000) -- first offset at/after ts
local hwm         = consumer:highWatermark("orders", 0)
consumer:close()
```

## Consume as a group

```lua
local consumer = brahmaputra.GroupConsumer.new({
  ["bootstrap.servers"] = "127.0.0.1:9092",
  ["group.id"] = "billing",
  ["partition.assignment.strategy"] = "sticky",
  ["auto.offset.reset"] = "earliest",
  ["enable.auto.commit"] = false,        -- commit explicitly
  ["group.instance.id"] = "worker-3",    -- static membership
})
consumer:subscribe({ "orders" })

local ok, err = pcall(function()
  while true do
    for _, record in ipairs(consumer:poll(500)) do
      handle(record.value)
    end
    -- At-least-once: commit after processing, never before.
    consumer:commit()
  end
end)
consumer:close()   -- commits, then LeaveGroup so partitions move at once
if not ok then error(err, 0) end
```

`consumer:committed()` returns `{topic=, partition=, offset=}` entries;
`assignment()`, `memberId()` and `generation()` expose the current
membership.

## Nil versus empty

Lua's `nil` is the wire's null. A `nil` value is a tombstone and comes back
as `nil`; `""` comes back as `""`. The same holds for keys and header
values: `header("k", nil)` and `header("k", "")` are different headers.
Consumed records keep `key`/`value` as fields, so a `nil` never punches a
hole in a list.

## Compression

`none` is always there; `gzip` is built in when lua-zlib is installed (RFC
1952 gzip container, matching the broker's flate2 `GzEncoder`). Without
lua-zlib, `["compression.type"] = "gzip"` raises `ConfigError` at
construction and fetching a gzip batch raises. Others are opt-in:

```lua
brahmaputra.registerCodec("zstd", zstd.compress, zstd.decompress)
```

If you register lz4, the broker expects a little-endian `uint32` of the
uncompressed length followed by a raw LZ4 **block**, not the LZ4 frame
format.

## Configuration

Kafka's names, as a flat table of dotted keys. Unknown keys are rejected.
Numbers may be floats with integral values (`1e3`); they are converted to
integers.

**Producer**

| Key | Default | |
|---|---|---|
| `bootstrap.servers` | required | `host:port[,host:port]` |
| `client.id` | `brahmaputra-lua` | |
| `acks` | `1` | `0`, `1`, `-1`/`"all"` |
| `batch.size` | `16384` | bytes per partition batch before it is sent |
| `linger.ms` | `5` | Kafka defaults to 0 |
| `compression.type` | `none` | `none`, `gzip`, or a registered codec |
| `request.timeout.ms` | `30000` | broker-side ack wait; socket bound is this + 5 s |
| `retries` | `5` | broker errors returned before the append, and failures to reach the leader |
| `retry.backoff.ms` | `100` | |
| `delivery.timeout.ms` | `120000` | caps a batch from its oldest record's send() to its last retry |
| `buffer.memory` | `33554432` | unsent bytes held client-side |
| `max.block.ms` | `60000` | how long send() blocks on a full buffer |
| `socket.connection.setup.timeout.ms` | `10000` | |
| `delivery.report.callback` | `nil` | `function(report)`; `report` has `topic`, `partition`, `baseOffset`, `recordCount`, `error`; failures go here instead of being raised |

**Consumer**

| Key | Default |
|---|---|
| `bootstrap.servers` | required |
| `client.id` | `brahmaputra-lua` |
| `fetch.max.bytes` | `8388608` |
| `fetch.min.bytes` | `1` |
| `fetch.max.wait.ms` | `500` |
| `max.poll.records` | `500` |
| `isolation.level` | `read_uncommitted` (`read_committed`) |
| `client.rack` | `""` |
| `request.timeout.ms` | `30000` (a fetch adds its wait) |
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
| `auto.offset.reset` | `earliest` | `earliest`, `latest`, `none` (raises `NoOffsetForPartitionError`) |
| `partition.assignment.strategy` | `range` | `range`, `roundrobin`, `sticky` |
| `group.instance.id` | `""` | static membership |

## Errors

Failures are raised with `error()` as tables carrying `kind` and `message`
(and `code` for broker errors); `tostring(err)` reads well. Test them with
`brahmaputra.errors.is(err, kind)`, which follows the hierarchy:

```lua
local ok, err = pcall(producer.flush, producer)
if not ok and brahmaputra.errors.is(err, "ServerError") then
  print(err.code, brahmaputra.protocol.errorName(err.code))
end
```

`BrahmaputraError` is the root; under it `ServerError` (`err.code`),
`ConnectionError` (and `TimeoutError` under that), `ProtocolError`,
`BufferFullError`, `NoOffsetForPartitionError` and `ConfigError`.

## Notes on Lua 5.4 integers

Offsets and timestamps are Lua integers (64-bit signed; `math.type` is
`"integer"`, never `"float"`). `>>` is a logical shift in Lua, so zigzag
spells the arithmetic shift as `-(v >> 63)`; unsigned comparisons use
`math.ult`, so varints with the top bit set (including `math.mininteger`)
round-trip. Negative or oversized lengths anywhere in a response (a record
batch's `batch_length`, a record, a header count) are decode errors, not
allocations.

## Running the tests

Start a broker, then:

```bash
./test.sh 127.0.0.1 9092     # compile-checks every file, then runs the e2e suite
# or directly:
LUA_PATH="$PWD/?.lua;;" lua5.4 test/manual_test.lua 127.0.0.1 9092
```

It prints one line per check and ends with `87 passed, 0 failed`; the exit
status is non-zero on any failure. `test.sh` sets `LUA_PATH` itself, so it
works from any directory. The suite needs lua-zlib (it checks gzip). With no
threads or fork in Lua, the connection-failure section runs its TCP proxy
as a child process (`test/proxy.lua`), the retry section runs a frame-aware
proxy that answers Produce with injected error codes (`test/fault_proxy.lua`),
and the long-poll group section produces from another
(`test/late_producer.lua`).

## Not implemented

TLS/QUIC transports, SASL authentication, transactions and the idempotent
producer, as for the other drivers (see `../README.md`).
