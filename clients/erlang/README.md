# Brahmaputra client for Erlang/OTP

A standard OTP library application (`brahmaputra`) with no dependencies
beyond `kernel` and `stdlib`. Needs OTP 25 or newer.

```erlang
%% rebar.config
{deps, [{brahmaputra, {git, "https://github.com/byte-mods/brahmaputra.git",
                       {branch, "main"}}}]}.
```

(The application lives in `clients/erlang`; point rebar3 at that
subdirectory, or copy it into your `_checkouts/`.)

Without rebar3, build with plain `erlc`:

```bash
make            # compiles src/ into ebin/, warning-free under -Werror
```

Verified end to end against a live broker: **38/38 checks**
(`./test.sh 127.0.0.1 9092`).

## Design

| Process | Module | Role |
|---|---|---|
| one `gen_server` per broker connection | `brahmaputra_conn` | owns the socket (`{packet, 4}` is the frame's int32 length prefix); pipelines requests by correlation id |
| one per client | `brahmaputra_router` | metadata cache, leader routing, redials dropped connections |
| producer `gen_server` | `brahmaputra_producer` | per-partition batches, `linger_ms` via `erlang:send_after/3`, bounded buffer that parks senders |
| group `gen_server` | `brahmaputra_group` | join/sync/heartbeat; heartbeats on a timer that keeps firing during `poll/2` |
| (plain module) | `brahmaputra_consumer` | partition fetches and offset lookups over a router |
| (plain module) | `brahmaputra_protocol` | BitPacker, frames, record batches, CRC32C, murmur2, codecs |

Records are maps. A missing key, value or header value is `undefined`,
which is distinct from `<<>>`: a record whose value is `undefined` is a
tombstone.

## Produce

```erlang
{ok, P} = brahmaputra_producer:start_link("127.0.0.1:9092",
              #{acks => 1, linger_ms => 5, compression => gzip}),

%% Keyed: murmur2(key) rem partitions, so records sharing a key keep order.
ok = brahmaputra_producer:send(P, <<"orders">>, <<"{\"id\":1}">>,
         #{key => <<"user-7">>,
           headers => [{<<"trace-id">>, <<"abc-123">>}, {<<"reason">>, undefined}]}),

%% Explicit partition, bypassing the partitioner; a tombstone.
ok = brahmaputra_producer:send_to(P, <<"orders">>, 0, undefined, #{key => <<"user-7">>}),

%% Or wait for one record's offset. A full round trip — correct, and slow.
{ok, Offset} = brahmaputra_producer:send_sync(P, <<"orders">>, <<"{\"id\":2}">>),

ok = brahmaputra_producer:flush(P),
ok = brahmaputra_producer:close(P).   % flushes, then stops
```

`send` returns once the record is buffered (or, with `linger_ms => 0` or
a full batch, once its batch is acknowledged). `flush/1` also reports any
failure of a background linger flush since the last call.

## Consume one partition

```erlang
{ok, C} = brahmaputra_consumer:new("127.0.0.1:9092", #{}),
{ok, Records} = brahmaputra_consumer:fetch(C, <<"orders">>, 0, 0, 500),
[io:format("~b ~p ~p~n", [O, K, V])
 || #{offset := O, key := K, value := V} <- Records],

{ok, Records2, HighWatermark} = brahmaputra_consumer:fetch_verbose(C, <<"orders">>, 0, 0, 500),
{ok, End} = brahmaputra_consumer:list_offsets(C, <<"orders">>, 0, latest),
{ok, AtTime} = brahmaputra_consumer:list_offsets(C, <<"orders">>, 0, 1700000000000),
brahmaputra_consumer:close(C).
```

## Consume as a group

```erlang
{ok, G} = brahmaputra_group:start_link("127.0.0.1:9092", <<"billing">>,
              #{assignor => sticky,
                auto_offset_reset => earliest,
                auto_commit_interval_ms => 0,        % commit explicitly
                group_instance_id => <<"worker-3">>}), % static membership
ok = brahmaputra_group:subscribe(G, [<<"orders">>]),

Loop = fun Loop() ->
           {ok, Records} = brahmaputra_group:poll(G, 500),
           [handle(V) || #{value := V} <- Records],
           %% At-least-once: commit after processing, never before.
           ok = brahmaputra_group:commit(G),
           Loop()
       end,
...
ok = brahmaputra_group:close(G).   % commits, then LeaveGroup so partitions move at once
```

With `auto_offset_reset => none`, `poll/2` returns
`{error, {no_offset_for_partition, Topic, Partition}}` instead of guessing.

## Configuration

Keys are Kafka's names with dots turned into underscores.

### Producer (`brahmaputra_producer:default_config/0`)

| Key | Default | Meaning |
|---|---|---|
| `client_id` | `<<"brahmaputra-erlang">>` | sent in every frame header |
| `acks` | `1` | `0` fire-and-forget, `1` leader, `-1`/`all` every in-sync replica |
| `batch_size` | `16384` | flush a partition buffer at this many bytes |
| `linger_ms` | `5` | flush this long after a buffer's first record; `0` sends immediately |
| `compression` | `none` | `none`, `gzip`; `lz4`, `zstd`, `snappy` once registered |
| `request_timeout_ms` | `30000` | broker-side wait for acknowledgements |
| `retries` | `5` | resends of errors returned before the append (never duplicates) |
| `retry_backoff_ms` | `100` | wait between retries |
| `delivery_timeout_ms` | `120000` | caps first attempt through last retry |
| `buffer_memory` | `33554432` | unflushed bytes held client-side |
| `max_block_ms` | `60000` | how long `send` blocks on a full buffer before `{error, {buffer_full, _}}` |
| `connect_timeout_ms` | `30000` | TCP connect timeout |

### Consumer (`brahmaputra_consumer:default_config/0`)

| Key | Default | Meaning |
|---|---|---|
| `fetch_max_bytes` | `8388608` | caps one response |
| `fetch_min_bytes` | `1` | return early once this much is ready |
| `fetch_max_wait_ms` | `500` | long-poll ceiling when caught up |
| `max_poll_records` | `500` | records one group `poll` returns |
| `isolation_level` | `read_uncommitted` | or `read_committed` |
| `client_rack` | `<<>>` | this consumer's rack, for follower fetching |
| `request_timeout_ms`, `connect_timeout_ms` | `30000` | |

### Group (`brahmaputra_group:default_config/0`, plus every consumer key)

| Key | Default | Meaning |
|---|---|---|
| `session_timeout_ms` | `10000` | coordinator evicts a member silent this long |
| `rebalance_timeout_ms` | `3000` | how long the coordinator waits for rejoins |
| `max_poll_interval_ms` | `300000` | longest gap between polls before this member leaves |
| `auto_commit_interval_ms` | `5000` | `0` disables auto-commit |
| `auto_offset_reset` | `earliest` | `earliest`, `latest` or `none` |
| `assignor` | `range` | `range`, `roundrobin` or `sticky` |
| `group_instance_id` | `<<>>` | static membership (KIP-345); empty for dynamic |

## Compression

`none` and `gzip` (via `zlib`) are built in. The rest are opt-in, so this
application pulls in no NIFs of its own:

```erlang
brahmaputra_protocol:register_codec(zstd,
    fun(Payload) -> ezstd:compress(Payload) end,
    fun(Payload) -> ezstd:decompress(Payload) end).
```

If you register lz4, note that the broker expects a little-endian `uint32`
of the uncompressed length followed by a raw LZ4 **block** — not the LZ4
frame format.

## End-to-end test

Start a broker, then from any directory:

```bash
clients/erlang/test.sh 127.0.0.1 9092
```

which is equivalent to

```bash
cd clients/erlang
make all test
erl -noshell -pa ebin -pa test -eval 'brahmaputra_manual_test:main(["127.0.0.1", "9092"])'
```

It prints the same sections and checks as the Go and Node.js suites, ends
with `N passed, 0 failed`, and halts non-zero on any failure.

## Not implemented

Same as the other drivers: no TLS/QUIC, no transactions, no idempotent
producer. The `Authenticate` API (SCRAM-SHA-256 / PLAIN) that the Go driver
carries is not wired up here, since the broker refuses credentials on the
plaintext listener this driver speaks.
