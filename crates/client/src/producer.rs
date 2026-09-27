//! Batching producer (Blueprint 02 §4).
//!
//! Records accumulate in a per-(topic, partition) buffer until it reaches
//! `batch_size` bytes or `linger_ms` have passed, then go out as ONE
//! Produce request carrying ONE `RecordBatch` (optionally LZ4-compressed).
//! A background ticker implements the linger flush. Every `send` awaits
//! its record's offset: batch base offset + index within the batch.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

use brahmaputra_protocol::codec;
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{
    ProduceMultiPartition, ProduceMultiResponse, ProduceRequest, ProduceResponse,
};
use brahmaputra_protocol::producer::{InitProducerIdRequest, InitProducerIdResponse};
use brahmaputra_protocol::{ApiKey, Compression, Record, RecordBatch, RecordHeader};
use bytes::Bytes;
use futures::future::join_all;
use tokio::sync::{oneshot, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use tracing::{debug, trace};

use crate::error::ClientError;
use crate::router::BrokerRouter;
use crate::transport::TransportConfig;

type TopicPartition = (String, i32);
type PartitionSendLock = Arc<AsyncMutex<()>>;

/// Producer configuration.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub client_id: String,
    /// Flush a partition buffer once it holds roughly this many bytes of
    /// record payload.
    pub batch_size: usize,
    /// Flush every non-empty buffer at least this often. 0 = flush every
    /// record immediately (sync send).
    pub linger_ms: u64,
    /// Compression applied to each flushed batch.
    pub compression: Compression,
    /// Max unacknowledged requests on the connection.
    pub max_in_flight: usize,
    /// 1 = ack after leader append (default); -1 behaves the same on a
    /// single broker (M1); 0 = fire and forget, `send` returns -1.
    pub acks: i32,
    pub timeout_ms: i32,
    /// Enable magic-v2 producer identity/sequence tracking and safe replay of
    /// an ambiguous send. Requires acknowledgements and max_in_flight <= 5.
    pub idempotence: bool,
    /// Data-plane transport; must match the broker's.
    pub transport: TransportConfig,
    /// Send partitions that share a broker in one request (api_key 15).
    /// On by default: it is the difference between paying the per-request
    /// cost once and paying it per partition.
    pub batch_partitions: bool,
    /// How many times to retry a send the broker refused with a *retriable*
    /// error (`retries`): a stale leader, an ISR momentarily too small, a
    /// suspended broker lease, or a coordinator still loading. Some errors
    /// can occur after append; enable idempotence to deduplicate retries.
    ///
    /// Non-retriable errors are returned immediately: a malformed request
    /// or a failed authorization fails identically however often it is
    /// sent, so retrying only delays the report.
    pub retries: u32,
    /// Wait between retries (`retry.backoff.ms`). A tight retry loop
    /// against a recovering broker is indistinguishable from an attack on
    /// it, and slows the recovery it is waiting for.
    pub retry_backoff_ms: u64,
    /// Ceiling on the whole send, first attempt through last retry
    /// (`delivery.timeout.ms`). This bounds worst-case latency, which
    /// `retries` alone does not: N retries that each take `timeout_ms` is
    /// an unbounded wait in practice.
    pub delivery_timeout_ms: u64,
    /// Ceiling on unflushed record bytes held client-side
    /// (`buffer.memory`). Once reached, `send` waits rather than
    /// allocating: a producer faster than its broker must be slowed down,
    /// not allowed to consume the whole heap holding records nobody has
    /// acknowledged.
    pub buffer_memory: usize,
    /// How long `send` may block on a full buffer before failing
    /// (`max.block.ms`). Bounding it means a wedged broker surfaces as a
    /// visible error rather than an application that quietly stopped.
    pub max_block_ms: u64,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        ProducerConfig {
            client_id: "brahmaputra-client".into(),
            batch_size: 16 * 1024,
            linger_ms: 5,
            compression: Compression::Lz4,
            max_in_flight: 5,
            acks: 1,
            timeout_ms: 30_000,
            idempotence: false,
            transport: TransportConfig::default(),
            batch_partitions: true,
            retries: 5,
            retry_backoff_ms: 100,
            delivery_timeout_ms: 120_000,
            buffer_memory: 32 * 1024 * 1024,
            max_block_ms: 60_000,
        }
    }
}

type BufferedRecord = (Record, i64, oneshot::Sender<Result<i64, ClientError>>);

struct Buffer {
    /// Each record is buffered with the wall-clock time it was *sent*, not
    /// the time its batch happens to flush. Those differ by up to
    /// `linger.ms`, and it is the send time that a consumer filtering by
    /// timestamp is asking about.
    records: Vec<BufferedRecord>,
    size: usize,
}

impl Buffer {
    fn new() -> Self {
        Buffer {
            records: Vec::new(),
            size: 0,
        }
    }
}

/// Bounds unflushed record bytes held client-side (`buffer.memory` /
/// `max.block.ms`).
///
/// Separate from the producer because it depends on nothing else: no
/// routing, no connection, no partition map. Keeping it that way is what
/// lets the blocking behaviour be tested directly rather than inferred
/// from a live broker's timing.
struct BufferBudget {
    /// Total unflushed record bytes across every partition buffer. Kept as
    /// a counter rather than summed on demand: every `send` consults it,
    /// and walking every partition per record would cost more than the
    /// work it guards.
    used: AtomicUsize,
    /// Woken whenever a flush frees space, so blocked senders proceed as
    /// soon as there is room instead of polling for it.
    available: tokio::sync::Notify,
    limit: usize,
    max_block: Duration,
}

impl BufferBudget {
    fn new(limit: usize, max_block_ms: u64) -> Self {
        BufferBudget {
            used: AtomicUsize::new(0),
            available: tokio::sync::Notify::new(),
            limit,
            max_block: Duration::from_millis(max_block_ms.max(1)),
        }
    }
    /// Wait until `bytes` more may be buffered, then account for them.
    ///
    /// This is what makes `buffer.memory` real. Without it a producer that
    /// outruns its broker buffers without limit and dies holding records
    /// nobody has acknowledged — the failure mode where the data is lost
    /// *and* there is no error to point at. Blocking the caller instead
    /// pushes back on the source, which is the only place the pressure can
    /// actually be relieved.
    ///
    /// A single record larger than the whole budget is admitted rather than
    /// deadlocking forever on a condition that can never hold; refusing
    /// oversized records is the broker's job, via `max.message.bytes`.
    async fn reserve(&self, bytes: usize) -> Result<(), ClientError> {
        if self.limit == 0 || bytes >= self.limit {
            self.used.fetch_add(bytes, Ordering::AcqRel);
            return Ok(());
        }
        let deadline = Instant::now() + self.max_block;
        loop {
            // Register for the wakeup *before* re-reading the counter, or a
            // flush landing between the read and the wait is missed and
            // this sender sleeps to the deadline for no reason.
            let notified = self.available.notified();
            let current = self.used.load(Ordering::Acquire);
            if current + bytes <= self.limit {
                // Racing senders can both pass this check; the overshoot is
                // bounded by one record each and self-corrects on the next
                // flush, which beats holding a lock across an await.
                self.used.fetch_add(bytes, Ordering::AcqRel);
                return Ok(());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ClientError::Configuration(format!(
                    "producer buffer full: {current} of {} bytes unflushed after \
                     max.block.ms={}",
                    self.limit,
                    self.max_block.as_millis()
                )));
            }
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }

    /// Release space a flush has taken ownership of, waking any waiters.
    /// Take space back for records being returned to a buffer. Unlike
    /// `reserve` this never waits: the records already passed admission
    /// once, and blocking a flush on the budget it is trying to refill
    /// would deadlock.
    fn reclaim(&self, bytes: usize) {
        if bytes != 0 {
            self.used.fetch_add(bytes, Ordering::AcqRel);
        }
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        // Saturating: the reservation overshoot above means the counter can
        // briefly exceed what a single flush accounts for, and wrapping a
        // usize here would wedge every future send permanently.
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            })
            .ok();
        self.available.notify_waiters();
    }
}

struct Inner {
    router: BrokerRouter,
    config: ProducerConfig,
    buffers: Mutex<HashMap<TopicPartition, Buffer>>,
    /// Bounds unflushed bytes so a producer outrunning its broker is
    /// slowed down rather than allowed to buffer without limit.
    budget: BufferBudget,
    rr_counter: AtomicUsize,
    session: Option<ProducerSession>,
    sequences: Mutex<HashMap<TopicPartition, SequenceState>>,
    partition_sends: Mutex<HashMap<TopicPartition, PartitionSendLock>>,
}

#[derive(Debug, Clone, Copy)]
struct ProducerSession {
    producer_id: i64,
    producer_epoch: i16,
}

#[derive(Debug, Default)]
struct SequenceState {
    next_sequence: i32,
    poisoned: bool,
}

/// A batching producer over one multiplexed connection.
pub struct Producer {
    inner: Arc<Inner>,
    ticker: Option<JoinHandle<()>>,
}

impl Producer {
    pub async fn connect(
        addr: SocketAddr,
        config: ProducerConfig,
    ) -> Result<Producer, ClientError> {
        if config.max_in_flight == 0 {
            return Err(ClientError::Configuration(
                "max_in_flight must be at least one".into(),
            ));
        }
        if config.idempotence && config.acks == 0 {
            return Err(ClientError::Configuration(
                "idempotence requires acks=1 or acks=all".into(),
            ));
        }
        if config.idempotence && config.max_in_flight > 5 {
            return Err(ClientError::Configuration(
                "idempotence supports at most five in-flight requests".into(),
            ));
        }
        let router = BrokerRouter::connect_with(
            config.transport.clone(),
            addr,
            Some(config.client_id.clone()),
            config.max_in_flight,
        )
        .await?;
        let session = if config.idempotence {
            let response = router
                .request_seed(
                    ApiKey::InitProducerId,
                    &InitProducerIdRequest::allocate().encode(),
                )
                .await?;
            let response = InitProducerIdResponse::decode(&response)?;
            ClientError::from_error_code(response.error_code)?;
            if response.producer_id < 0 || response.producer_epoch < 0 {
                return Err(ClientError::Idempotence(
                    "broker returned an invalid producer identity".into(),
                ));
            }
            Some(ProducerSession {
                producer_id: response.producer_id,
                producer_epoch: response.producer_epoch,
            })
        } else {
            None
        };
        let inner = Arc::new(Inner {
            router,
            budget: BufferBudget::new(config.buffer_memory, config.max_block_ms),
            config,
            buffers: Mutex::new(HashMap::new()),
            rr_counter: AtomicUsize::new(0),
            session,
            sequences: Mutex::new(HashMap::new()),
            partition_sends: Mutex::new(HashMap::new()),
        });

        // Linger ticker: periodically flush every non-empty buffer.
        let ticker = if inner.config.linger_ms > 0 {
            let weak = Arc::downgrade(&inner);
            let interval = Duration::from_millis(inner.config.linger_ms);
            Some(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    let Some(inner) = weak.upgrade() else { break };
                    inner.flush_all().await;
                }
            }))
        } else {
            None
        };

        Ok(Producer { inner, ticker })
    }

    /// Send one record; returns its offset (batch base + position), or -1
    /// with `acks=0`. When `partition` is `None`, a keyed record goes to
    /// `murmur2(key) % partitions` — so records sharing a key share a
    /// partition and therefore keep their relative order — and a keyless
    /// record goes round-robin. This is Kafka's default partitioner.
    pub async fn send(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<Bytes>,
        value: impl Into<Option<Bytes>>,
    ) -> Result<i64, ClientError> {
        self.send_with_headers(topic, partition, key, value, Vec::new())
            .await
    }

    /// Delete `key` on a compacted topic.
    ///
    /// A tombstone is an ordinary record with a null value: it is appended,
    /// replicated and delivered like any other, and it is compaction that
    /// gives it its meaning — the key and every earlier record for it stop
    /// existing once the tombstone itself ages out of `delete.retention.ms`.
    pub async fn send_tombstone(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Bytes,
    ) -> Result<i64, ClientError> {
        self.send_with_headers(topic, partition, Some(key), None, Vec::new())
            .await
    }

    /// As [`send`](Self::send), with headers attached to the record.
    ///
    /// Headers travel beside the payload rather than inside it, which is
    /// what lets a consumer route or filter on them without deserialising
    /// a value it may not have the schema for.
    pub async fn send_with_headers(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<Bytes>,
        value: impl Into<Option<Bytes>>,
        headers: Vec<RecordHeader>,
    ) -> Result<i64, ClientError> {
        let value = value.into();
        let partition = match (partition, key.as_ref()) {
            (Some(p), _) => p,
            (None, Some(key)) => self.key_partition(topic, key).await?,
            (None, None) => self.round_robin_partition(topic).await?,
        };
        let record = Record {
            key,
            value,
            // Rebased against the batch's max_timestamp at flush; the
            // absolute time travels beside the record until then.
            timestamp_delta: 0,
            headers,
        };
        let approx_size = approx_record_size(&record);
        let created_ms = now_ms();
        // Admission control before the record enters a buffer: past this
        // point the producer owns it and the caller cannot take it back, so
        // the waiting has to happen here.
        self.inner.budget.reserve(approx_size).await?;

        let (tx, rx) = oneshot::channel();
        let full = {
            let mut buffers = self.inner.buffers.lock().expect("buffers");
            let buffer = buffers
                .entry((topic.to_owned(), partition))
                .or_insert_with(Buffer::new);
            buffer.size += approx_size;
            buffer.records.push((record, created_ms, tx));
            buffer.size >= self.inner.config.batch_size
        };
        if self.inner.config.linger_ms == 0 {
            // Sync-send semantics: this record must go out now, so wait for
            // the partition's turn rather than leaving it to the ticker.
            self.inner.flush_partition(topic, partition).await;
        } else if full {
            // A batch is ready. If another caller is already flushing this
            // partition, do NOT queue behind it: that flusher drains the
            // buffer again before it finishes, so it will carry these
            // records too. Queueing instead turns every concurrent sender
            // into its own near-empty batch, and each one costs a full
            // round trip. The linger ticker is the backstop.
            self.inner.try_flush_partition(topic, partition).await;
        }
        rx.await.map_err(|_| ClientError::ConnectionClosed)?
    }

    /// Flush every buffer now; await before relying on delivery.
    pub async fn flush(&self) -> Result<(), ClientError> {
        self.inner.flush_all().await;
        Ok(())
    }

    /// Allocated identity when idempotence is enabled.
    pub fn producer_identity(&self) -> Option<(i64, i16)> {
        self.inner
            .session
            .map(|session| (session.producer_id, session.producer_epoch))
    }

    /// The partition [`send`](Self::send) would pick with `partition: None`:
    /// `murmur2(key) % partitions` for a key, the next round-robin slot
    /// without one. Lets a caller that must report where a record went (a
    /// gateway acknowledging its own clients) choose once and pass the
    /// result as an explicit partition, rather than re-deriving it.
    pub async fn partition_for(&self, topic: &str, key: Option<&[u8]>) -> Result<i32, ClientError> {
        match key {
            Some(key) => self.key_partition(topic, key).await,
            None => self.round_robin_partition(topic).await,
        }
    }

    async fn round_robin_partition(&self, topic: &str) -> Result<i32, ClientError> {
        let partitions = self.inner.router.partitions(topic).await?;
        let index = self.inner.rr_counter.fetch_add(1, Ordering::Relaxed) % partitions.len();
        Ok(partitions[index])
    }

    async fn key_partition(&self, topic: &str, key: &[u8]) -> Result<i32, ClientError> {
        let partitions = self.inner.router.partitions(topic).await?;
        let index = (murmur2(key) & 0x7fff_ffff) as usize % partitions.len();
        Ok(partitions[index])
    }
}

/// Kafka's `murmur2` (the 32-bit variant its default partitioner uses), so
/// a key lands on the same partition here as it would there — and on the
/// same partition as every other Brahmaputra driver puts it.
/// `murmur2(b"") == 275646681`.
pub fn murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let length = data.len();
    let mut h: u32 = SEED ^ (length as u32);
    let chunks = length / 4;

    for i in 0..chunks {
        let offset = i * 4;
        let mut k = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    let tail = chunks * 4;
    match length - tail {
        3 => {
            h ^= u32::from(data[tail + 2]) << 16;
            h ^= u32::from(data[tail + 1]) << 8;
            h ^= u32::from(data[tail]);
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= u32::from(data[tail + 1]) << 8;
            h ^= u32::from(data[tail]);
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= u32::from(data[tail]);
            h = h.wrapping_mul(M);
        }
        _ => {}
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

impl Drop for Producer {
    fn drop(&mut self) {
        if let Some(ticker) = self.ticker.take() {
            ticker.abort();
        }
    }
}

impl Inner {
    /// Flush every non-empty buffer, batching partitions that share a
    /// broker into one request.
    ///
    /// This is where the per-request cost gets amortised: six partitions on
    /// one broker cost one round trip, not six. Partitions are still
    /// serialised per partition (the send lock) so ordering within a
    /// partition is unchanged — only the framing is shared.
    async fn flush_all(&self) {
        let keys: Vec<(String, i32)> = self
            .buffers
            .lock()
            .expect("buffers")
            .keys()
            .cloned()
            .collect();
        if keys.is_empty() {
            return;
        }
        if !self.config.batch_partitions || self.session.is_some() {
            // Idempotent sends carry per-partition sequences that the
            // batched path deliberately does not implement, so they keep
            // the one-request-per-partition route.
            join_all(
                keys.iter()
                    .map(|(topic, partition)| self.flush_partition(topic, *partition)),
            )
            .await;
            return;
        }

        let (grouped, unroutable) = self.router.group_by_leader(&keys).await;
        // A partition whose leader is unknown falls back to the single
        // path, which produces the routing error the caller needs to see.
        join_all(
            unroutable
                .iter()
                .map(|(topic, partition)| self.flush_partition(topic, *partition)),
        )
        .await;
        // One request per broker would leave a single request in flight per
        // connection, because a batched flush holds its partitions' send
        // locks until the response lands. Splitting a broker's partitions
        // into `max_in_flight` fixed shards keeps that ordering guarantee —
        // a partition always travels in the same shard, so it still has at
        // most one request outstanding — while letting the shards overlap.
        join_all(
            grouped
                .into_iter()
                .flat_map(|(address, partitions)| {
                    shard_partitions(partitions, self.config.max_in_flight)
                        .into_iter()
                        .map(move |shard| (address, shard))
                })
                .map(|(address, partitions)| self.flush_broker(address, partitions)),
        )
        .await;
    }

    /// Flush one broker's worth of partitions in a single request.
    async fn flush_broker(&self, address: SocketAddr, partitions: Vec<(String, i32)>) {
        // Hold every partition's send lock for the duration: the batched
        // request is that partition's next append, and a concurrent flush
        // would reorder it.
        let mut guards = Vec::with_capacity(partitions.len());
        for (topic, partition) in &partitions {
            guards.push(self.send_lock(topic, *partition).lock_owned().await);
        }

        // Each partition's records stay with their waiters until the broker
        // has answered for them: a retriable answer puts them back in the
        // buffer rather than failing the caller, which is what the
        // unbatched path's retry budget already does for a single batch.
        struct Unit {
            topic: String,
            partition: i32,
            records: Vec<BufferedRecord>,
        }
        let mut payload: Vec<(ProduceMultiPartition, Vec<Bytes>)> = Vec::new();
        let mut units: Vec<Unit> = Vec::new();
        let mut taken_buffers = Vec::with_capacity(partitions.len());
        {
            let mut buffers = self.buffers.lock().expect("buffers");
            for (topic, partition) in &partitions {
                let Some(buffer) = buffers.get_mut(&(topic.clone(), *partition)) else {
                    continue;
                };
                if buffer.records.is_empty() {
                    continue;
                }
                let taken = std::mem::replace(buffer, Buffer::new());
                taken_buffers.push((topic.clone(), *partition, taken));
            }
        }
        // Encoding and compression can be expensive. The per-partition send
        // guards still preserve ordering, but other partitions can enqueue
        // records while these detached buffers are being encoded.
        for (topic, partition, taken) in taken_buffers {
            // The records belong to this flush now, so the buffer space
            // they occupied is free for new sends.
            self.budget.release(taken.size);
            let timestamped: Vec<(Record, i64)> = taken
                .records
                .iter()
                .map(|(record, created_ms, _)| (record.clone(), *created_ms))
                .collect();
            let batch = RecordBatch::from_timestamped(0, 0, timestamped, now_ms())
                .with_compression(self.config.compression);
            payload.push((
                ProduceMultiPartition {
                    topic: topic.clone(),
                    partition,
                    batches_length: 0,
                },
                vec![batch.encode()],
            ));
            units.push(Unit {
                topic,
                partition,
                records: taken.records,
            });
        }
        if payload.is_empty() {
            return;
        }

        let fail_units = |units: Vec<Unit>, error: ClientError| {
            for unit in units {
                for (_, _, sender) in unit.records {
                    let _ = sender.send(Err(clone_error(&error)));
                }
            }
        };

        let body =
            match codec::encode_produce_multi(self.config.acks, self.config.timeout_ms, &payload) {
                Ok(body) => body,
                Err(error) => {
                    fail_units(units, ClientError::Protocol(error));
                    return;
                }
            };

        if self.config.acks == 0 {
            let _ = self
                .router
                .send_address(address, ApiKey::ProduceMulti, &body)
                .await;
            for unit in units {
                for (_, _, sender) in unit.records {
                    let _ = sender.send(Ok(-1));
                }
            }
            drop(guards);
            return;
        }

        let response = match self
            .router
            .request_address(address, ApiKey::ProduceMulti, &body)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                // The broker may be gone or leadership may have moved; the
                // send itself is unacknowledged either way. Refresh the
                // route and let the next tick try again, until each
                // record's delivery timeout says otherwise.
                for unit in units {
                    self.requeue_or_fail(unit.topic, unit.partition, unit.records, &error)
                        .await;
                }
                drop(guards);
                return;
            }
        };
        let decoded = match ProduceMultiResponse::decode(&response) {
            Ok(decoded) => decoded,
            Err(error) => {
                fail_units(units, ClientError::Io(error));
                drop(guards);
                return;
            }
        };

        // Results come back in request order, so they line up with the
        // units collected above.
        for (index, unit) in units.into_iter().enumerate() {
            let result = decoded.results.get(index);
            match result {
                Some(result) if result.error_code == ec::NONE => {
                    for (position, (_, _, sender)) in unit.records.into_iter().enumerate() {
                        let offset = if result.base_offset < 0 {
                            -1
                        } else {
                            result.base_offset + position as i64
                        };
                        let _ = sender.send(Ok(offset));
                    }
                }
                Some(result) => {
                    let error = ClientError::from_error_code(result.error_code)
                        .err()
                        .unwrap_or(ClientError::ConnectionClosed);
                    if is_retriable_error_code(result.error_code) {
                        self.requeue_or_fail(unit.topic, unit.partition, unit.records, &error)
                            .await;
                    } else {
                        for (_, _, sender) in unit.records {
                            let _ = sender.send(Err(clone_error(&error)));
                        }
                    }
                }
                None => {
                    self.requeue_or_fail(
                        unit.topic,
                        unit.partition,
                        unit.records,
                        &ClientError::ConnectionClosed,
                    )
                    .await;
                }
            }
        }
        drop(guards);
    }

    /// Put a partition's records back at the head of its buffer after a
    /// retriable failure, failing only those whose delivery timeout has
    /// passed. The route is refreshed first, because a stale leader is the
    /// usual reason, and the retry backoff is paid here — while this
    /// partition's send lock is still held, so nothing reorders around it.
    async fn requeue_or_fail(
        &self,
        topic: String,
        partition: i32,
        records: Vec<BufferedRecord>,
        error: &ClientError,
    ) {
        let _ = self.router.refresh_topic(&topic).await;
        let now = now_ms();
        let timeout = i64::try_from(self.config.delivery_timeout_ms).unwrap_or(i64::MAX);
        let mut kept: Vec<BufferedRecord> = Vec::with_capacity(records.len());
        let mut kept_size = 0;
        for (record, created_ms, sender) in records {
            if now.saturating_sub(created_ms) < timeout {
                kept_size += approx_record_size(&record);
                kept.push((record, created_ms, sender));
            } else {
                let _ = sender.send(Err(clone_error(error)));
            }
        }
        if kept.is_empty() {
            return;
        }
        debug!(
            %topic,
            partition,
            requeued = kept.len(),
            %error,
            "retriable batched produce error; requeued for the next flush"
        );
        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
        self.budget.reclaim(kept_size);
        let mut buffers = self.buffers.lock().expect("buffers");
        let buffer = buffers
            .entry((topic, partition))
            .or_insert_with(Buffer::new);
        // Ahead of anything sent since: these records were accepted first.
        kept.append(&mut buffer.records);
        buffer.records = kept;
        buffer.size += kept_size;
    }

    fn send_lock(&self, topic: &str, partition: i32) -> PartitionSendLock {
        let mut locks = self.partition_sends.lock().expect("partition send locks");
        locks
            .entry((topic.to_owned(), partition))
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }

    /// Flush this partition, waiting for any in-progress flush first.
    ///
    /// Even though the connection can multiplex several requests, a single
    /// partition is sent strictly in sequence. Other partitions may use the
    /// configured bounded max_in_flight concurrently.
    async fn flush_partition(&self, topic: &str, partition: i32) {
        let send_lock = self.send_lock(topic, partition);
        let _send_guard = send_lock.lock().await;
        self.drain_partition(topic, partition).await;
    }

    /// Flush only if no one else is already flushing this partition. The
    /// holder drains until the buffer is empty, so skipping here loses
    /// nothing: our records go out in the holder's next batch.
    async fn try_flush_partition(&self, topic: &str, partition: i32) {
        let send_lock = self.send_lock(topic, partition);
        let Ok(_send_guard) = send_lock.try_lock() else {
            return;
        };
        self.drain_partition(topic, partition).await;
    }

    /// Send batches back to back until the partition buffer is empty. The
    /// caller holds the partition's send lock for the whole drain.
    async fn drain_partition(&self, topic: &str, partition: i32) {
        while self.send_one_batch(topic, partition).await {}
    }

    /// Send one batch; `false` means the buffer was empty and nothing went
    /// out.
    async fn send_one_batch(&self, topic: &str, partition: i32) -> bool {
        let buffer = {
            let mut buffers = self.buffers.lock().expect("buffers");
            match buffers.get_mut(&(topic.to_owned(), partition)) {
                Some(b) if !b.records.is_empty() => std::mem::replace(b, Buffer::new()),
                _ => return false,
            }
        };
        self.budget.release(buffer.size);
        let count = buffer.records.len();
        let mut records = Vec::with_capacity(count);
        let mut waiters = Vec::with_capacity(count);
        for (record, created_ms, waiter) in buffer.records {
            records.push((record, created_ms));
            waiters.push(waiter);
        }
        let result = self.produce(topic, partition, records).await;
        match result {
            Ok(base) => {
                trace!(topic, partition, base, count, "batch acked");
                for (i, waiter) in waiters.into_iter().enumerate() {
                    let offset = if base < 0 { -1 } else { base + i as i64 };
                    let _ = waiter.send(Ok(offset));
                }
            }
            Err(e) => {
                debug!(topic, partition, error = %e, "batch failed");
                for waiter in waiters {
                    let _ = waiter.send(Err(ClientError::Server {
                        code: match &e {
                            ClientError::Server { code, .. } => *code,
                            _ => -1,
                        },
                        message: e.to_string(),
                    }));
                }
            }
        }
        true
    }

    /// One Produce request carrying one RecordBatch. Returns the batch's
    /// base offset (-1 for acks=0).
    async fn produce(
        &self,
        topic: &str,
        partition: i32,
        records: Vec<(Record, i64)>,
    ) -> Result<i64, ClientError> {
        let record_count = i32::try_from(records.len()).map_err(|_| {
            ClientError::Idempotence("record count exceeds producer sequence space".into())
        })?;
        let sequence = if let Some(session) = self.session {
            let mut sequences = self.sequences.lock().expect("producer sequences");
            let state = sequences.entry((topic.to_owned(), partition)).or_default();
            if state.poisoned {
                return Err(ClientError::Idempotence(format!(
                    "partition {topic}-{partition} has an unresolved or fatal prior send"
                )));
            }
            let next = state
                .next_sequence
                .checked_add(record_count)
                .ok_or_else(|| {
                    ClientError::Idempotence("producer sequence space exhausted".into())
                })?;
            Some((session, state.next_sequence, next))
        } else {
            None
        };
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let mut batch = RecordBatch::from_timestamped(0, 0, records, now_ms)
            .with_compression(self.config.compression);
        if let Some((session, base_sequence, _)) = sequence {
            batch = batch.with_producer(session.producer_id, session.producer_epoch, base_sequence);
        }
        let req = ProduceRequest {
            topic: topic.to_owned(),
            partition,
            acks: self.config.acks,
            timeout_ms: self.config.timeout_ms,
            batches_length: 0, // filled in by encode_produce_request
        };
        let body = codec::encode_produce_request(&req, &[batch.encode()])?;
        if self.config.acks == 0 {
            // Fire and forget: the broker sends no response.
            self.router
                .send_partition(topic, partition, ApiKey::Produce, &body)
                .await?;
            return Ok(-1);
        }

        let resp = match self
            .produce_with_bounded_retries(topic, partition, &body, sequence.is_some())
            .await
        {
            Ok(response) => response,
            Err(error) => {
                if sequence.is_some() && is_ambiguous_transport(&error) {
                    self.poison_partition(topic, partition);
                }
                return Err(error);
            }
        };
        if resp.error_code != ec::NONE {
            if sequence.is_some()
                && matches!(
                    resp.error_code,
                    ec::FENCED_PRODUCER_EPOCH
                        | ec::OUT_OF_ORDER_SEQUENCE
                        | ec::NOT_ENOUGH_REPLICAS
                        | ec::INTERNAL
                )
            {
                self.poison_partition(topic, partition);
            }
            ClientError::from_error_code(resp.error_code)?;
        }
        if let Some((_, base_sequence, next_sequence)) = sequence {
            let mut sequences = self.sequences.lock().expect("producer sequences");
            let state = sequences
                .get_mut(&(topic.to_owned(), partition))
                .expect("sequence state exists");
            if state.next_sequence != base_sequence || state.poisoned {
                return Err(ClientError::Idempotence(
                    "partition sequence state changed while a send was in flight".into(),
                ));
            }
            state.next_sequence = next_sequence;
        }
        Ok(resp.base_offset)
    }

    /// Send one already-encoded batch, retrying while the broker's answer
    /// says the record was not stored and the budget allows.
    ///
    /// Two budgets, because they bound different things: `retries` caps how
    /// many times we ask, and `delivery_timeout_ms` caps how long the
    /// caller waits in total. Without the deadline, N retries of a request
    /// that each take `timeout_ms` is an unbounded wait; without the count,
    /// a fast-failing broker gets hammered.
    async fn produce_with_bounded_retries(
        &self,
        topic: &str,
        partition: i32,
        body: &[u8],
        idempotent: bool,
    ) -> Result<ProduceResponse, ClientError> {
        let deadline =
            Instant::now() + Duration::from_millis(self.config.delivery_timeout_ms.max(1));
        let mut retried_ambiguous = false;
        let mut attempts_left = self.config.retries;
        loop {
            let attempt = self.produce_once(topic, partition, body).await;
            // A retriable condition can arrive two ways: as a code in a
            // response the broker sent, or as a client-side error raised
            // while routing — the topic is missing from *this client's*
            // metadata, which is the same staleness seen one step earlier.
            let routing_code = match &attempt {
                Err(ClientError::Server { code, .. }) if is_retriable_error_code(*code) => {
                    Some(*code)
                }
                _ => None,
            };
            match attempt {
                Err(error) if routing_code.is_some() => {
                    let code = routing_code.expect("checked");
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if attempts_left == 0 || remaining.is_zero() {
                        return Err(error);
                    }
                    attempts_left -= 1;
                    let _ = self.router.refresh_topic(topic).await;
                    debug!(
                        topic,
                        partition, code, attempts_left, "retriable routing error; backing off"
                    );
                    let backoff = Duration::from_millis(self.config.retry_backoff_ms);
                    tokio::time::sleep(backoff.min(remaining)).await;
                }
                Ok(response) if is_retriable_error_code(response.error_code) => {
                    let code = response.error_code;
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if attempts_left == 0 || remaining.is_zero() {
                        // Out of budget: return the broker's own answer
                        // rather than inventing a client-side error, so the
                        // caller sees why it actually failed.
                        return Ok(response);
                    }
                    attempts_left -= 1;
                    // A stale route is the most common retriable cause, and
                    // resending to the same broker would just repeat it.
                    if matches!(
                        code,
                        ec::UNKNOWN_TOPIC_OR_PARTITION
                            | ec::NOT_LEADER_OR_FOLLOWER
                            | ec::FENCED_BROKER_EPOCH
                            | ec::FENCED_LEADER_EPOCH
                            | ec::UNKNOWN_LEADER_EPOCH
                    ) {
                        let _ = self.router.refresh_topic(topic).await;
                    }
                    debug!(
                        topic,
                        partition, code, attempts_left, "retriable produce error; backing off"
                    );
                    let backoff = Duration::from_millis(self.config.retry_backoff_ms);
                    tokio::time::sleep(backoff.min(remaining)).await;
                }
                Ok(response) => return Ok(response),
                Err(error)
                    if idempotent && is_ambiguous_transport(&error) && !retried_ambiguous =>
                {
                    // The same magic-v2 bytes (including sequence and
                    // timestamp) may safely be replayed exactly once.
                    retried_ambiguous = true;
                    let _ = self.router.refresh_topic(topic).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn poison_partition(&self, topic: &str, partition: i32) {
        self.sequences
            .lock()
            .expect("producer sequences")
            .entry((topic.to_owned(), partition))
            .or_default()
            .poisoned = true;
    }

    async fn produce_once(
        &self,
        topic: &str,
        partition: i32,
        body: &[u8],
    ) -> Result<ProduceResponse, ClientError> {
        let response = self
            .router
            .request_partition(topic, partition, ApiKey::Produce, body)
            .await?;
        ProduceResponse::decode(&response).map_err(|error| {
            ClientError::Protocol(brahmaputra_protocol::ProtocolError::Message(
                error.to_string(),
            ))
        })
    }
}

/// Whether a broker error describes a condition that can recover.
///
/// These refusals often happen before append, but lease/leadership changes
/// and replication timeouts can also happen afterwards. Non-idempotent
/// retries are at-least-once; producer identity and sequence provide dedup.
///
/// - `UNKNOWN_TOPIC_OR_PARTITION`: the broker has not seen the topic *yet*.
///   Creation is a controller write that reaches brokers asynchronously, so
///   a send that follows a `CreateTopic` closely enough races the metadata,
///   and leadership moves produce the same answer briefly. A topic that
///   truly does not exist still fails — after the retry budget, with the
///   broker's own code — which is what Kafka does with this code too.
/// - `NOT_LEADER_OR_FOLLOWER` / `FENCED_LEADER_EPOCH` /
///   `UNKNOWN_LEADER_EPOCH`: the request reached a broker that does not
///   lead the partition, or lost leadership during append. Refresh routing.
/// - `FENCED_BROKER_EPOCH`: the broker cannot currently serve its lease.
///   Refresh routing and allow a replacement or a resumed lease to serve
///   the retry. A lease can expire during an append as well as before it.
/// - `NOT_ENOUGH_REPLICAS`: `acks=all` was refused because the ISR is
///   below `min.insync.replicas`, or the replication wait timed out after
///   append. It can recover when a follower catches up.
/// - `COORDINATOR_LOAD_IN_PROGRESS`: the coordinator is replaying its log
///   and is not ready to answer yet.
/// - `INTERNAL`: the broker failed the request rather than completing it.
///
/// Everything else is returned to the caller as-is. `INVALID_REQUEST` and
/// `AUTHORIZATION_FAILED` will fail identically on every attempt, so
/// retrying them only delays the report; the idempotence codes
/// (`FENCED_PRODUCER_EPOCH`, `OUT_OF_ORDER_SEQUENCE`) mean the producer's
/// sequence state is already broken and a blind retry would make it worse.
fn is_retriable_error_code(code: i32) -> bool {
    matches!(
        code,
        ec::UNKNOWN_TOPIC_OR_PARTITION
            | ec::NOT_LEADER_OR_FOLLOWER
            | ec::FENCED_BROKER_EPOCH
            | ec::FENCED_LEADER_EPOCH
            | ec::UNKNOWN_LEADER_EPOCH
            | ec::NOT_ENOUGH_REPLICAS
            | ec::COORDINATOR_LOAD_IN_PROGRESS
            | ec::INTERNAL
    )
}
fn is_ambiguous_transport(error: &ClientError) -> bool {
    matches!(error, ClientError::Io(_) | ClientError::ConnectionClosed)
}

#[cfg(test)]
mod tests {
    use super::murmur2;

    /// The partitioner must be a pure function of the key (per-key
    /// ordering), must cover the tail-length branches of murmur2, and must
    /// spread distinct keys. Frozen digests catch accidental changes to the
    /// `Utils.murmur2` pins the digests; comparing against a live Kafka is
    /// part of the benchmark harness.
    #[test]
    fn murmur2_partitioner_is_stable_and_spreads_keys() {
        let partition_of =
            |key: &[u8], partitions: usize| (murmur2(key) & 0x7fff_ffff) as usize % partitions;

        // Deterministic: the same key always maps to the same partition.
        for key in [&b""[..], b"a", b"ab", b"abc", b"abcd", b"orders-42"] {
            assert_eq!(partition_of(key, 6), partition_of(key, 6));
        }

        // Digests over every tail-length branch (len % 4 = 0..3), taken from
        // an independent transcription of Kafka's `Utils.murmur2`.
        assert_eq!(murmur2(b""), 0x106e_08d9);
        assert_eq!(murmur2(b"a"), 0xa2d0_b27c);
        assert_eq!(murmur2(b"ab"), 0x12d8_262a);
        assert_eq!(murmur2(b"abc"), 0x1c94_221b);
        assert_eq!(murmur2(b"abcd"), 0xb11a_b5f4);
        assert_eq!(murmur2(b"orders-42"), 0x1c91_3191);

        // Distinct keys are not funnelled into one partition.
        let spread: std::collections::BTreeSet<usize> = (0..64)
            .map(|i| partition_of(format!("k{i}").as_bytes(), 8))
            .collect();
        assert!(spread.len() > 4, "murmur2 spreads keys across partitions");
    }
}

/// `ClientError` is not `Clone` (it wraps `io::Error`), but every waiter of
/// a failed batch needs the same failure. Reconstructing preserves the
/// error code, which is what callers actually branch on.
fn clone_error(error: &ClientError) -> ClientError {
    match error {
        ClientError::Server { code, message } => ClientError::Server {
            code: *code,
            message: message.clone(),
        },
        ClientError::ConnectionClosed => ClientError::ConnectionClosed,
        other => ClientError::Server {
            code: -1,
            message: other.to_string(),
        },
    }
}

/// What a record costs against `buffer.memory`: its bytes plus a small
/// fixed overhead for the bookkeeping around it.
fn approx_record_size(record: &Record) -> usize {
    record.value_len()
        + record.key.as_ref().map_or(0, |k| k.len())
        + record
            .headers
            .iter()
            .map(|h| h.key.len() + h.value.as_ref().map_or(0, |v| v.len()) + 4)
            .sum::<usize>()
        + 16
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

/// Split one broker's partitions into at most `shards` groups, assigning a
/// partition to a group by hash so it always lands in the same one.
fn shard_partitions(partitions: Vec<(String, i32)>, shards: usize) -> Vec<Vec<(String, i32)>> {
    let shards = shards.max(1).min(partitions.len().max(1));
    if shards == 1 {
        return vec![partitions];
    }
    let mut groups: Vec<Vec<(String, i32)>> = vec![Vec::new(); shards];
    for (topic, partition) in partitions {
        let mut hash = 2166136261_u32;
        for byte in topic.as_bytes() {
            hash = (hash ^ u32::from(*byte)).wrapping_mul(16777619);
        }
        for byte in partition.to_le_bytes() {
            hash = (hash ^ u32::from(byte)).wrapping_mul(16777619);
        }
        groups[hash as usize % shards].push((topic, partition));
    }
    groups.retain(|group| !group.is_empty());
    groups
}

#[cfg(test)]
mod shard_tests {
    use super::shard_partitions;

    #[test]
    fn a_partition_always_lands_in_the_same_shard() {
        let partitions: Vec<(String, i32)> =
            (0..12).map(|index| ("orders".to_string(), index)).collect();
        let first = shard_partitions(partitions.clone(), 4);
        let second = shard_partitions(partitions.clone(), 4);
        assert_eq!(first, second);
        // Every partition is placed exactly once.
        let mut placed: Vec<(String, i32)> = first.into_iter().flatten().collect();
        placed.sort();
        let mut expected = partitions;
        expected.sort();
        assert_eq!(placed, expected);
    }

    #[test]
    fn fewer_partitions_than_shards_yields_no_empty_groups() {
        let groups = shard_partitions(vec![("t".to_string(), 0), ("t".to_string(), 1)], 8);
        assert!(groups.iter().all(|group| !group.is_empty()));
        assert_eq!(groups.iter().map(Vec::len).sum::<usize>(), 2);
    }
}

#[cfg(test)]
mod retry_classification_tests {
    use super::is_retriable_error_code;
    use brahmaputra_protocol::error_code as ec;

    /// Transient broker lifecycle and replication failures remain eligible
    /// for retry within the configured budget.
    #[test]
    fn transient_broker_errors_are_retried() {
        for code in [
            // Metadata staleness, not a verdict: the topic may exist and
            // this broker may simply not have been told yet.
            ec::UNKNOWN_TOPIC_OR_PARTITION,
            ec::NOT_LEADER_OR_FOLLOWER,
            ec::FENCED_BROKER_EPOCH,
            ec::FENCED_LEADER_EPOCH,
            ec::UNKNOWN_LEADER_EPOCH,
            ec::NOT_ENOUGH_REPLICAS,
            ec::COORDINATOR_LOAD_IN_PROGRESS,
            ec::INTERNAL,
        ] {
            assert!(is_retriable_error_code(code), "code {code} must be retried");
        }
    }

    /// Retrying these is either pointless or actively harmful.
    #[test]
    fn permanent_and_idempotence_errors_are_never_retried() {
        for code in [
            ec::INVALID_REQUEST,
            ec::UNSUPPORTED_VERSION,
            ec::AUTHORIZATION_FAILED,
            ec::SASL_AUTHENTICATION_FAILED,
            ec::OFFSET_OUT_OF_RANGE,
            // These two mean the producer's sequence state is already
            // broken; a blind retry makes it worse, not better.
            ec::FENCED_PRODUCER_EPOCH,
            ec::OUT_OF_ORDER_SEQUENCE,
        ] {
            assert!(
                !is_retriable_error_code(code),
                "code {code} must not be retried"
            );
        }
    }

    /// Success is not an error; retrying it would resend an acknowledged
    /// record. Worth asserting because the check is easy to invert.
    #[test]
    fn success_is_not_retriable() {
        assert!(!is_retriable_error_code(ec::NONE));
    }
}

#[cfg(test)]
mod buffer_budget_tests {
    use super::*;

    #[tokio::test]
    async fn reservations_below_the_limit_are_admitted_immediately() {
        let budget = BufferBudget::new(1000, 50);
        budget.reserve(400).await.unwrap();
        budget.reserve(400).await.unwrap();
        assert_eq!(budget.used.load(Ordering::Acquire), 800);
    }

    /// The whole point: a producer that outruns its broker is slowed down
    /// and then told, rather than buffering without limit until it dies.
    #[tokio::test]
    async fn a_full_buffer_blocks_and_then_fails_within_max_block_ms() {
        let budget = BufferBudget::new(1000, 100);
        budget.reserve(900).await.unwrap();

        let started = std::time::Instant::now();
        let error = budget.reserve(200).await.unwrap_err();
        assert!(
            started.elapsed() >= Duration::from_millis(90),
            "must actually wait for space before giving up"
        );
        assert!(
            error.to_string().contains("buffer full"),
            "the error should name the cause, got: {error}"
        );
    }

    /// A flush frees space, and a sender waiting on it proceeds rather than
    /// sitting out the whole timeout.
    #[tokio::test]
    async fn releasing_space_wakes_a_blocked_sender() {
        let budget = Arc::new(BufferBudget::new(1000, 5_000));
        budget.reserve(900).await.unwrap();

        let waiter = {
            let budget = Arc::clone(&budget);
            tokio::spawn(async move { budget.reserve(200).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        budget.release(900);

        let result = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("a woken sender must not wait out max.block.ms")
            .expect("task");
        assert!(result.is_ok(), "space was freed, so the send must proceed");
    }

    /// A record bigger than the entire budget is admitted rather than
    /// deadlocking on a condition that can never become true. Refusing
    /// oversized records is the broker's job (`max.message.bytes`).
    #[tokio::test]
    async fn a_record_larger_than_the_budget_does_not_deadlock() {
        let budget = BufferBudget::new(100, 5_000);
        tokio::time::timeout(Duration::from_secs(1), budget.reserve(5_000))
            .await
            .expect("must not block forever")
            .expect("an oversized single record is admitted");
    }

    /// Releasing more than was reserved must not wrap the counter — a
    /// wrapped usize here would wedge every later send permanently.
    #[tokio::test]
    async fn over_release_saturates_instead_of_wrapping() {
        let budget = BufferBudget::new(1000, 50);
        budget.reserve(100).await.unwrap();
        budget.release(100_000);
        assert_eq!(budget.used.load(Ordering::Acquire), 0);
        budget.reserve(900).await.unwrap();
    }

    /// A zero limit means "unbounded", which must not accidentally mean
    /// "block everything".
    #[tokio::test]
    async fn a_zero_limit_disables_the_bound() {
        let budget = BufferBudget::new(0, 50);
        tokio::time::timeout(Duration::from_millis(500), budget.reserve(1 << 30))
            .await
            .expect("an unbounded budget must never block")
            .expect("admitted");
    }
}
