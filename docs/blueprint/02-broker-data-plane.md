# Blueprint 02 — Broker Data Plane, Wire Protocol & Client Flows

> Status: written from DESIGN.md §6/§8 ahead of the M1b implementation;
> reconciled against the actual `broker`/`client`/`cli` crates at the end of
> M1 (see the Verification section).

This document explains how a produce or fetch request travels through the
system, end to end, in a single-broker (M1) deployment.

---

## 1. Two protocols, two ports

| Plane | Transport | Used for |
|---|---|---|
| Control | gRPC (`tonic`/`prost`), from M2 | admin, controller↔broker RPCs, heartbeats, group coordination |
| Data | custom binary over TCP | Produce, Fetch, ListOffsets, Metadata |

The data plane is hand-rolled because every framing/copy cost is on the
multi-GB/s hot path. Every frame:

```
length:i32  api_key:i16  api_version:i16  correlation_id:i32  client_id:string  body
```

- `api_version` is negotiated per API → independent evolution, rolling
  upgrades (a v1 client and v2 broker interoperate on the min version).
- `correlation_id` lets a client pipeline multiple in-flight requests on one
  connection and match responses.
- M1 APIs: `Produce (0)`, `Fetch (1)`, `ListOffsets (2)`, `Metadata (3)`.

## 2. Connection handling

```
TcpListener
   └─ per-connection tokio task
        ├─ LengthDelimitedCodec decodes frames (zero-copy into Bytes)
        ├─ dispatch by api_key → handler
        └─ responses written back in request order per connection
```

Handlers are deliberately thin: they validate the frame, then send a message
to the **partition actor** that owns the target log, and await a `oneshot`
for the result. The connection task never touches files itself.

## 3. Partition actors (single-writer principle)

One tokio task owns each partition's `storage::Log`:

```
PartitionActor {
    log: storage::Log,            // the only handle; no locks anywhere
    inbox: mpsc::Receiver<Cmd>,   // bounded → backpressure
    pending_acks: Vec<oneshot>,   // produce requests awaiting ack policy
}
```

- `Cmd::Append(batch, acks, reply)` — stamp base offset, append, update
  LEO; reply immediately for `acks=1`; hold in `pending_acks` for `acks=all`
  (completed when the high watermark covers the batch — from M3 onward;
  in M1 `acks=all` degrades to `acks=1` since ISR = {self}).
- `Cmd::Read(offset, max_bytes, reply)` — read raw batch bytes (see
  Blueprint 01 §4) and reply.
- Because the actor is the only writer, append ordering *is* the log order —
  no coordination needed between concurrent producers.

Bounded `mpsc` inboxes are the backpressure mechanism: when disk or page
cache writeback stalls, the inbox fills, `try_send` starts failing, and
connection tasks stop reading new frames off the socket — TCP flow control
then throttles the client naturally.

## 4. Produce flow (M1)

```
producer client                       broker
     │  Produce(topic, part, acks, [batch bytes])  │
     │ ──────────────────────────────────────────▶ │
     │            connection task decodes frame     │
     │            Cmd::Append → partition actor     │
     │            actor: stamp offset, append, LEO  │
     │ ◀────────── base_offset, error_code ──────── │
```

- The client batches records per partition in memory (`batch.size`,
  `linger.ms`), compresses (LZ4 default), and sends one Produce request per
  ready batch.
- `max.in.flight=5` bounds unacknowledged requests per connection; with
  idempotence (M3) this also preserves per-partition order on retries.

## 5. Fetch flow (M1)

```
consumer client                       broker
     │  Fetch(topic, part, offset, max_bytes, max_wait_ms) │
     │ ──────────────────────────────────────────▶ │
     │      actor: if data ≥ offset → return now    │
     │      else hold until data arrives or         │
     │      max_wait_ms elapses (long poll)         │
     │ ◀────────── raw batch bytes, HW ──────────── │
```

- Long polling keeps fetch efficient at low and high traffic without a push
  mechanism: the consumer drives its own pace and can replay any offset.
- The high watermark is returned with every fetch so consumers never read
  un-replicated data (matters from M3; in M1 HW == LEO).

## 6. ListOffsets & Metadata

- `ListOffsets(topic, part, timestamp | earliest | latest)` → resolves via
  segment base offsets and the `.timeindex`.
- `Metadata([topics])` → served from the broker's metadata cache (in M1: a
  static map built from config; from M2: materialized from the Raft metadata
  log). Clients use it to route directly to partition leaders.

## 7. CLI

`brahmaputra-cli` wraps the client crate:

```
brahmaputra-cli topic create --name orders --partitions 3
brahmaputra-cli produce --topic orders --key k1 --value "hello"
brahmaputra-cli consume --topic orders --from earliest --max 10
brahmaputra-cli consume --topic orders --group billing --follow
brahmaputra-cli offsets --topic orders
brahmaputra-cli groups list
brahmaputra-cli groups describe --group billing
brahmaputra-cli groups lag --group billing
```

These commands are the **manual verification harness** for every milestone:
each phase's live checks are expressed as CLI invocations against running
brokers.

---

## Verification (M1)

- [x] Start one broker; `produce` 1000 records; `consume --from earliest`
      returns exactly those 1000, in order, with contiguous offsets.
- [x] Restart the broker; consume again — identical output (durability).
- [x] Concurrent producers (2 CLI processes) → no interleaved/corrupt
      batches; offsets contiguous per partition.
