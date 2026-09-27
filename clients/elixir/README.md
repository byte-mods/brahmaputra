# Brahmaputra client for Elixir

A native driver: pure Elixir on OTP, no Hex dependencies (gzip comes from
`:zlib`, CRC32C and murmur2 are transcribed). Requires Elixir ≥ 1.14.

```elixir
# mix.exs
defp deps do
  [{:brahmaputra, path: "../brahmaputra/clients/elixir"}]
end
```

```bash
mix compile --warnings-as-errors
```

Verified end to end against a live broker: **54/54 checks**
(`./test.sh 127.0.0.1 9092`).

## Processes

| Module | What it is |
|---|---|
| `Brahmaputra.Connection` | GenServer owning one TCP socket; one request in flight, a request timeout (default 120 s, `set_request_timeout/2`), and marked broken — never reused — after any I/O error or timeout |
| `Brahmaputra.Router` | GenServer caching metadata and one connection per broker; routes by partition leader and redials a broken connection on next use |
| `Brahmaputra.Producer` | GenServer batching per partition; linger via `Process.send_after` |
| `Brahmaputra.Consumer` | Plain struct around a router; functions run in the caller |
| `Brahmaputra.GroupConsumer` | GenServer; heartbeat timer, join/sync, commits, LeaveGroup on close |

Every call returns `:ok` / `{:ok, value}` or `{:error, exception}`, where
the exception is a `Brahmaputra.ServerError` (a broker error code), a
`Brahmaputra.NoOffsetForPartitionError`, or a `Brahmaputra.Error`.

## Produce

```elixir
alias Brahmaputra.Producer

{:ok, producer} =
  Producer.start_link("127.0.0.1", 9092,
    acks: 1,
    linger_ms: 5,
    compression_type: "gzip"
  )

# Keyed: murmur2(key) % partitions, so records sharing a key keep order.
:ok =
  Producer.send(producer, "orders", ~s({"id":1}),
    key: "user-7",
    headers: [{"trace-id", "abc-123"}, {"retry-of", nil}]
  )

# Explicit partition, explicit timestamp, and a tombstone (nil value,
# which is distinct from "").
:ok = Producer.send(producer, "orders", nil, key: "user-7", partition: 2,
                    timestamp: System.system_time(:millisecond))

# Or wait for one record's offset. A full round trip — correct, and slow.
{:ok, offset} = Producer.send_sync(producer, "orders", ~s({"id":2}))

:ok = Producer.flush(producer)
:ok = Producer.close(producer)
```

## Consume one partition

```elixir
alias Brahmaputra.Consumer

{:ok, consumer} = Consumer.connect("127.0.0.1", 9092, fetch_max_wait_ms: 500)

{:ok, records} = Consumer.fetch(consumer, "orders", 0, 0, 500)
for r <- records, do: IO.puts("#{r.offset} #{inspect(r.key)} #{inspect(r.value)}")

{:ok, records, high_watermark} = Consumer.fetch_verbose(consumer, "orders", 0, 0)
{:ok, end_offset} = Consumer.list_offsets(consumer, "orders", 0, :latest)
{:ok, start} = Consumer.list_offsets(consumer, "orders", 0, :earliest)
{:ok, at_time} = Consumer.list_offsets(consumer, "orders", 0, 1_700_000_000_000)

Consumer.close(consumer)
```

## Consume as a group

```elixir
alias Brahmaputra.GroupConsumer

{:ok, group} =
  GroupConsumer.start_link("127.0.0.1", 9092, "billing",
    partition_assignment_strategy: :sticky,
    auto_offset_reset: :earliest,
    enable_auto_commit: false,         # commit explicitly
    group_instance_id: "worker-3"      # static membership
  )

:ok = GroupConsumer.subscribe(group, ["orders"])

loop = fn loop ->
  {:ok, records} = GroupConsumer.poll(group, 500)
  Enum.each(records, &handle/1)
  # At-least-once: commit after processing, never before.
  :ok = GroupConsumer.commit(group)
  loop.(loop)
end

# GroupConsumer.close(group) commits, then leaves so partitions move at once.
```

## Configuration

Names are Kafka's, as atoms.

**Producer** (`Brahmaputra.Producer.start_link/3`)

| Option | Default | Meaning |
|---|---|---|
| `client_id` | `"brahmaputra-elixir"` | sent in every frame header |
| `acks` | `1` | `0` fire-and-forget, `1` leader, `-1` / `:all` every in-sync replica |
| `batch_size` | `16384` | flush a partition once it holds this many bytes |
| `linger_ms` | `5` | flush every non-empty buffer this often; `0` sends immediately |
| `compression_type` | `"none"` | `none`, `gzip`, or a registered `lz4`/`zstd`/`snappy` |
| `request_timeout_ms` | `30000` | broker-side wait for acknowledgements |
| `retries` | `5` | resends after a retriable (pre-append) error |
| `retry_backoff_ms` | `100` | wait between retries |
| `delivery_timeout_ms` | `120000` | caps a send, first attempt to last retry |
| `buffer_memory` | `33554432` | cap on unflushed bytes held client-side |
| `max_block_ms` | `60000` | how long `send` blocks on a full buffer before failing |
| `connect_timeout` | `30000` | TCP connect timeout |

**Consumer** (`Brahmaputra.Consumer.connect/3`)

| Option | Default | Meaning |
|---|---|---|
| `fetch_max_bytes` | `8388608` | caps one response |
| `fetch_min_bytes` | `1` | return early once this many bytes are ready |
| `fetch_max_wait_ms` | `500` | long-poll ceiling when caught up |
| `max_poll_records` | `500` | records per group poll |
| `isolation_level` | `:read_uncommitted` | or `:read_committed` |
| `client_rack` | `""` | this consumer's failure domain |

**Group consumer** (`Brahmaputra.GroupConsumer.start_link/4`) — all consumer
options, plus:

| Option | Default | Meaning |
|---|---|---|
| `session_timeout_ms` | `10000` | coordinator evicts a member silent this long |
| `rebalance_timeout_ms` | `3000` | how long the coordinator waits for rejoins |
| `max_poll_interval_ms` | `300000` | longest gap between polls before this member leaves |
| `enable_auto_commit` | `true` | commit delivered positions from `poll` |
| `auto_commit_interval_ms` | `5000` | `0` also disables auto-commit |
| `auto_offset_reset` | `:earliest` | `:earliest`, `:latest`, or `:none` (returns `NoOffsetForPartitionError`) |
| `partition_assignment_strategy` | `:range` | `:range`, `:roundrobin`, `:sticky` |
| `group_instance_id` | `nil` | stable identity for static membership (KIP-345) |

## Compression

`none` and `gzip` are built in. The rest are opt-in, so this package pulls
in no dependencies of its own:

```elixir
Brahmaputra.register_codec(:zstd, &:ezstd.compress/1, &:ezstd.decompress/1)
```

If you register lz4, note that the broker expects a little-endian `uint32`
of the uncompressed length followed by a raw LZ4 **block** — not the LZ4
frame format, which a frame-format library would silently produce instead.

## Running the end-to-end suite

Start a broker, then:

```bash
./test.sh 127.0.0.1 9092
# equivalently, from this directory:
mix run e2e/manual_test.exs 127.0.0.1 9092
```

It prints the same sections and checks as the Go suite and ends with
`54 passed, 0 failed`; it exits 1 on a failed check and 2 on a fatal error.
