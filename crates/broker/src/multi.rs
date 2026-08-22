//! Multi-partition Produce and Fetch (api_keys 15 and 16).
//!
//! A client that talks to one broker about six partitions sends one
//! request instead of six. At small record sizes the per-request cost is
//! what dominates throughput, so this is the difference between paying it
//! once and paying it per partition — the single largest gap against
//! Kafka, which has batched these from the start.
//!
//! Failures are reported per partition: one unknown partition does not
//! fail the other five. That is what a caller expects from a batched API,
//! and it is what lets a client keep making progress while one partition's
//! leadership moves.

use std::time::Duration;

/// Room left for the frame header and the response struct on top of the
/// record batches, so a full response still fits inside `max_frame_bytes`.
const FRAME_HEADROOM: usize = 1 << 20;

use brahmaputra_metrics::names;
use brahmaputra_protocol::codec;
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{
    FetchMultiRequest, FetchMultiResult, ProduceMultiResponse, ProduceMultiResult,
};
use brahmaputra_protocol::{validate_batch_header, ApiKey, IsolationLevel};
use bytes::Bytes;
use tracing::warn;

use crate::error::BrokerError;
use crate::handlers::{
    code_of, encode_error_for, now_ms, wait_for_high_watermark, ClientIdentity, ResponseBody,
};
use crate::quota::QuotaKind;
use crate::server::Broker;
use brahmaputra_client::Transport;
use brahmaputra_storage::LogRegion;

pub(crate) async fn produce_multi(
    broker: &Broker,
    body: Bytes,
    client: ClientIdentity<'_>,
) -> Option<Bytes> {
    let (request, per_partition) = match codec::decode_produce_multi(body) {
        Ok(decoded) => decoded,
        Err(error) => {
            warn!(%error, "undecodable ProduceMulti request");
            return Some(encode_error_for(ApiKey::ProduceMulti, ec::INVALID_REQUEST));
        }
    };
    let acks = request.acks;
    if !matches!(acks, -1..=1) {
        return Some(encode_error_for(ApiKey::ProduceMulti, ec::INVALID_REQUEST));
    }

    // Partitions are independent single-writer actors, so their appends
    // run concurrently. Doing them in sequence would trade N round trips
    // for one round trip that takes N times as long, which is no trade at
    // all — batching has to shorten the request, not just merge it.
    let outcomes =
        futures::future::join_all(request.partitions.iter().zip(per_partition.iter()).map(
            |(descriptor, batches)| {
                produce_one_partition(
                    broker,
                    &descriptor.topic,
                    descriptor.partition,
                    batches,
                    acks,
                    request.timeout_ms,
                )
            },
        ))
        .await;

    let mut results = Vec::with_capacity(request.partitions.len());
    let mut total_bytes = 0_u64;
    let mut total_records = 0_u64;
    for ((descriptor, batches), outcome) in request
        .partitions
        .iter()
        .zip(per_partition.iter())
        .zip(outcomes)
    {
        total_bytes += batches.iter().map(|batch| batch.len() as u64).sum::<u64>();
        total_records += outcome.records;
        results.push(ProduceMultiResult {
            topic: descriptor.topic.clone(),
            partition: descriptor.partition,
            error_code: outcome.error_code,
            base_offset: outcome.base_offset,
            log_append_time_ms: now_ms(),
        });
    }

    let metrics = broker.metrics();
    metrics.count(names::PRODUCE_REQUESTS, 1);
    metrics.count(names::PRODUCE_RECORDS, total_records);
    metrics.count(names::PRODUCE_BYTES, total_bytes);
    let throttle = broker
        .throttle(client.principal, client.client_id, QuotaKind::Produce, total_bytes)
        .await;
    if !throttle.is_zero() {
        metrics.count(names::THROTTLED_REQUESTS, 1);
        metrics.count(names::THROTTLE_MS, throttle.as_millis() as u64);
    }

    if acks == 0 {
        return None;
    }
    Some(Bytes::from(
        ProduceMultiResponse { results }
            .encode()
            .unwrap_or_default(),
    ))
}

struct PartitionAppend {
    error_code: i32,
    base_offset: i64,
    records: u64,
}

/// Append one partition's batches with the same validation, epoch stamping,
/// replication and acks semantics as the single-partition path.
async fn produce_one_partition(
    broker: &Broker,
    topic: &str,
    partition: i32,
    batches: &[Bytes],
    acks: i32,
    timeout_ms: i32,
) -> PartitionAppend {
    let failed = |error_code| PartitionAppend {
        error_code,
        base_offset: -1,
        records: 0,
    };
    if batches.is_empty() {
        return failed(ec::NONE);
    }

    for raw in batches {
        match validate_batch_header(raw) {
            Ok(header) => {
                // The idempotent producer needs decoded batches for its
                // dedup window, so those keep using the single-partition
                // route rather than being half-supported here.
                if header.producer.is_some() {
                    return failed(ec::INVALID_REQUEST);
                }
            }
            Err(error) => {
                warn!(%error, topic, partition, "corrupt batch in ProduceMulti");
                return failed(ec::INVALID_REQUEST);
            }
        }
    }

    let replicated_commit =
        broker.config().replication_enabled && broker.metadata_cache().is_some();
    let mutation_guard = if replicated_commit {
        Some(broker.partition_mutation_guard(topic, partition).await)
    } else {
        None
    };

    let cluster_assignment = if replicated_commit {
        let image = broker
            .metadata_cache()
            .expect("replicated commit requires metadata")
            .snapshot();
        if let Err(error) = broker.validate_local_broker_epoch(&image) {
            return failed(code_of(&error));
        }
        broker
            .replication_tracker()
            .reconcile_metadata(&image, broker.config().broker_id);
        let Some(topic_metadata) = image.topics.get(topic) else {
            return failed(ec::UNKNOWN_TOPIC_OR_PARTITION);
        };
        let Some(assignment) = topic_metadata.partitions.get(&partition) else {
            return failed(ec::UNKNOWN_TOPIC_OR_PARTITION);
        };
        if acks == -1 {
            let default_min_isr = usize::try_from(topic_metadata.replication_factor)
                .unwrap_or(1)
                .clamp(1, 2);
            let min_isr = match topic_metadata.configs.get("min.insync.replicas") {
                Some(value) => match value.parse::<usize>() {
                    Ok(value) if value > 0 => value,
                    _ => return failed(ec::INVALID_REQUEST),
                },
                None => default_min_isr,
            };
            if assignment.isr.len() < min_isr {
                return failed(ec::NOT_ENOUGH_REPLICAS);
            }
        }
        Some(assignment.clone())
    } else {
        None
    };
    let leader_epoch = cluster_assignment
        .as_ref()
        .map_or(0, |assignment| assignment.leader_epoch);

    let handle = match broker.partition_auto_create(topic, partition) {
        Ok(handle) => handle,
        Err(error) => return failed(code_of(&error)),
    };
    if replicated_commit {
        if let Err(error) = handle.record_leader_epoch(leader_epoch).await {
            return failed(code_of(&BrokerError::Storage(error)));
        }
    }

    let mut watermark = (acks == -1 && replicated_commit).then(|| {
        let mut watermark = handle.watermark_watch();
        watermark.borrow_and_update();
        watermark
    });

    let mut first_base = -1;
    let mut required_high_watermark = -1;
    let mut records = 0_u64;
    for raw in batches {
        match handle
            .append_producer_batch(raw.clone(), leader_epoch)
            .await
        {
            Ok((base, next)) => {
                if first_base < 0 {
                    first_base = base;
                }
                records += (next - base) as u64;
                required_high_watermark = next;
            }
            Err(error) => return failed(code_of(&BrokerError::Storage(error))),
        }
    }

    if let Some(assignment) = cluster_assignment.as_ref() {
        let latest = broker
            .metadata_cache()
            .expect("replicated commit requires metadata")
            .snapshot();
        if let Err(error) = broker.validate_local_broker_epoch(&latest) {
            return failed(code_of(&error));
        }
        let still_current = latest
            .topics
            .get(topic)
            .and_then(|topic| topic.partitions.get(&partition))
            .is_some_and(|current| current == assignment);
        if !still_current {
            return failed(ec::NOT_LEADER_OR_FOLLOWER);
        }
        if let Err(error) = broker
            .replication_tracker()
            .advance_leader_high_watermark(topic, assignment, &handle)
            .await
        {
            return failed(code_of(&BrokerError::Storage(error)));
        }
    }
    drop(mutation_guard);

    if let Some(watermark) = watermark.as_mut() {
        if !wait_for_high_watermark(
            watermark,
            required_high_watermark,
            Duration::from_millis(timeout_ms.max(0) as u64),
        )
        .await
        {
            return failed(ec::NOT_ENOUGH_REPLICAS);
        }
    }

    PartitionAppend {
        error_code: ec::NONE,
        base_offset: first_base,
        records,
    }
}

/// Whether a fetch can be answered with file ranges rather than buffers.
///
/// Only plaintext TCP qualifies: TLS and QUIC have to see the bytes to
/// encrypt them, so there is nothing to save. Kafka draws the line in the
/// same place — enabling SSL disables its `sendfile` path too.
///
/// The platform is deliberately not part of this test. Where `sendfile`
/// exists the ranges go straight from the page cache to the socket; where
/// it does not, the writer reads them just as the old path did. Keeping
/// the decision platform-independent means the selection logic is the same
/// code everywhere, and so is exercised by the tests everywhere.
fn zero_copy_fetch_available(broker: &Broker) -> bool {
    broker.config().transport == Transport::Tcp
}

/// Read every requested partition as file ranges instead of buffers.
///
/// Returns `None` when nothing was ready or any partition reported an
/// error, leaving those cases to the buffered path, which already knows
/// how to long-poll and how to report per-partition failures. The happy
/// path — which is the one that moves the bytes — never touches them.
async fn fetch_multi_zero_copy(
    broker: &Broker,
    request: &FetchMultiRequest,
) -> Option<(Bytes, Vec<LogRegion>, u64)> {
    let count = request.partitions.len().max(1);
    let budget = broker
        .config()
        .max_frame_bytes
        .saturating_sub(FRAME_HEADROOM);
    let per_partition = (budget / count).max(64 * 1024);

    let reads = futures::future::join_all(request.partitions.iter().map(|descriptor| async move {
        let handle = broker
            .partition(&descriptor.topic, descriptor.partition)
            .ok()?;
        let allowance = (descriptor.max_bytes.max(0) as usize).min(per_partition);
        let outcome = handle
            .read_regions(descriptor.fetch_offset, allowance)
            .await
            .ok()?;
        Some((descriptor, outcome))
    }))
    .await;

    let mut header_inputs: Vec<(FetchMultiResult, usize)> = Vec::with_capacity(reads.len());
    let mut regions: Vec<LogRegion> = Vec::new();
    let mut served = 0_u64;
    let mut remaining = budget;
    for read in reads {
        // Any failure at all hands the whole request to the buffered path,
        // which reports it per partition with the right error code.
        let (descriptor, outcome) = read?;
        let mut kept = 0usize;
        for region in outcome.regions {
            if region.len > remaining && !(served == 0 && regions.is_empty()) {
                break;
            }
            remaining = remaining.saturating_sub(region.len);
            served += region.len as u64;
            kept += region.len;
            regions.push(region);
        }
        header_inputs.push((
            FetchMultiResult {
                topic: descriptor.topic.clone(),
                partition: descriptor.partition,
                error_code: ec::NONE,
                high_watermark: outcome.high_watermark,
                last_stable_offset: outcome.high_watermark,
                batches_length: 0,
            },
            kept,
        ));
    }
    if served == 0 {
        return None;
    }
    let header = codec::encode_fetch_multi_header(&header_inputs).ok()?;
    Some((header, regions, served))
}

/// Count a served fetch and apply the client's quota.
async fn record_fetch(broker: &Broker, client: ClientIdentity<'_>, served: u64) {
    let metrics = broker.metrics();
    metrics.count(names::FETCH_REQUESTS, 1);
    metrics.count(names::FETCH_BYTES, served);
    let throttle = broker.throttle(client.principal, client.client_id, QuotaKind::Fetch, served).await;
    if !throttle.is_zero() {
        metrics.count(names::THROTTLED_REQUESTS, 1);
        metrics.count(names::THROTTLE_MS, throttle.as_millis() as u64);
    }
}

pub(crate) async fn fetch_multi(
    broker: &Broker,
    body: Bytes,
    client: ClientIdentity<'_>,
) -> ResponseBody {
    let request = match FetchMultiRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable FetchMulti request");
            return encode_error_for(ApiKey::FetchMulti, ec::INVALID_REQUEST).into();
        }
    };

    let isolation = IsolationLevel::from_wire(request.isolation_level);
    // The fast path: hand the socket file ranges and let the kernel move
    // the bytes. Anything unusual — an idle partition, an error — falls
    // through to the buffered path below, which handles both.
    //
    // A committed read cannot take it: deciding which batches to withhold
    // means looking at their headers, and the whole point of handing the
    // kernel a file range is that nobody looks at the bytes. Filtering is
    // still per batch, so this costs a copy, not a decompression.
    if isolation == IsolationLevel::ReadUncommitted && zero_copy_fetch_available(broker) {
        if let Some((header, regions, served)) = fetch_multi_zero_copy(broker, &request).await {
            record_fetch(broker, client, served).await;
            return ResponseBody::with_regions(header, regions);
        }
    }

    let mut results: Vec<(FetchMultiResult, Vec<Bytes>)> =
        Vec::with_capacity(request.partitions.len());
    let mut served = read_all_partitions(broker, &request, &mut results).await;

    // Long poll once for the whole request rather than once per partition:
    // a client asking about six idle partitions should wait one interval,
    // not six in sequence.
    //
    // An error is not "no data yet", though, and must never be held back by
    // the poll. A consumer whose committed offset has fallen off the log
    // depends on `OFFSET_OUT_OF_RANGE` coming straight back so it can reset
    // and re-fetch; delaying it by `max_wait_ms` can burn the consumer's
    // whole poll deadline and return nothing, which reads as an empty topic
    // rather than a seek.
    let failed = results
        .iter()
        .any(|(result, _)| result.error_code != ec::NONE);
    if served == 0 && !failed && request.max_wait_ms > 0 && !request.partitions.is_empty() {
        served = long_poll(broker, &request, &mut results).await;
    }

    record_fetch(broker, client, served).await;

    codec::encode_fetch_multi_response_chunks(&results)
        .unwrap_or_default()
        .into()
}

/// Read every requested partition once. Returns the total bytes gathered.
async fn read_all_partitions(
    broker: &Broker,
    request: &FetchMultiRequest,
    results: &mut Vec<(FetchMultiResult, Vec<Bytes>)>,
) -> u64 {
    let isolation = IsolationLevel::from_wire(request.isolation_level);
    // Divide the budget up front so the partitions can be read
    // concurrently: one round trip that takes as long as the slowest
    // partition, not as long as all of them added together.
    let count = request.partitions.len().max(1);
    let budget = broker
        .config()
        .max_frame_bytes
        .saturating_sub(FRAME_HEADROOM);
    let per_partition = (budget / count).max(64 * 1024);

    let reads = futures::future::join_all(request.partitions.iter().map(|descriptor| async move {
        let mut result = FetchMultiResult {
            topic: descriptor.topic.clone(),
            partition: descriptor.partition,
            error_code: ec::NONE,
            high_watermark: -1,
            last_stable_offset: -1,
            batches_length: 0,
        };
        let handle = match broker.partition(&descriptor.topic, descriptor.partition) {
            Ok(handle) => handle,
            Err(error) => {
                result.error_code = code_of(&error);
                return (result, Vec::new());
            }
        };
        let allowance = (descriptor.max_bytes.max(0) as usize).min(per_partition);
        match handle.read_at(descriptor.fetch_offset, allowance, isolation).await {
            Ok(outcome) => {
                result.high_watermark = outcome.high_watermark;
                result.last_stable_offset = outcome.high_watermark;
                (result, outcome.batches)
            }
            Err(error) => {
                result.error_code = code_of(&BrokerError::Storage(error));
                (result, Vec::new())
            }
        }
    }))
    .await;

    // Now enforce the budget across the whole response, which the
    // per-partition split alone cannot do: a read stops only *after* the
    // batch that crosses its allowance, so each partition may overshoot by
    // most of a batch, and with large batches those overshoots add up. Left
    // unchecked the frame exceeds `max_frame_bytes`, the client's decoder
    // rejects it, and the connection dies — which is exactly what a
    // megabyte-record consumer used to hit, intermittently, depending on
    // how much happened to be available.
    //
    // Trimming from the tail is safe because a fetch is allowed to return
    // less than was asked for; the client simply asks again from where it
    // got to. The first partition always keeps at least one batch, or a
    // consumer whose batches are larger than the budget could never make
    // progress.
    let mut served = 0_u64;
    let mut remaining = budget;
    for (result, batches) in reads {
        let mut kept = Vec::with_capacity(batches.len());
        for batch in batches {
            let size = batch.len();
            if size > remaining && !(served == 0 && kept.is_empty()) {
                break;
            }
            remaining = remaining.saturating_sub(size);
            served += size as u64;
            kept.push(batch);
        }
        results.push((result, kept));
    }
    served
}

/// Wait until any requested partition has data or the deadline passes, then
/// re-read. Polling rather than waking per partition keeps this simple; the
/// wait is bounded by `max_wait_ms` either way.
async fn long_poll(
    broker: &Broker,
    request: &FetchMultiRequest,
    results: &mut [(FetchMultiResult, Vec<Bytes>)],
) -> u64 {
    let isolation = IsolationLevel::from_wire(request.isolation_level);
    let deadline = tokio::time::Instant::now() + Duration::from_millis(request.max_wait_ms as u64);
    let poll_interval = Duration::from_millis(5);
    while tokio::time::Instant::now() < deadline {
        tokio::time::sleep(
            poll_interval.min(deadline.saturating_duration_since(tokio::time::Instant::now())),
        )
        .await;
        let mut served = 0_u64;
        for (index, descriptor) in request.partitions.iter().enumerate() {
            let Ok(handle) = broker.partition(&descriptor.topic, descriptor.partition) else {
                continue;
            };
            let Ok(outcome) = handle
                .read_at(
                    descriptor.fetch_offset,
                    descriptor.max_bytes.max(0) as usize,
                    isolation,
                )
                .await
            else {
                continue;
            };
            if outcome.batches.is_empty() {
                continue;
            }
            served += outcome
                .batches
                .iter()
                .map(|batch| batch.len() as u64)
                .sum::<u64>();
            results[index].0.error_code = ec::NONE;
            results[index].0.high_watermark = outcome.high_watermark;
            results[index].0.last_stable_offset = outcome.high_watermark;
            results[index].1 = outcome.batches;
        }
        if served > 0 {
            return served;
        }
    }
    0
}
