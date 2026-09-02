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
    FetchMultiPartition, FetchMultiRequest, FetchMultiResult, ProduceMultiResponse,
    ProduceMultiResult,
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
        .throttle(
            client.principal,
            client.client_id,
            QuotaKind::Produce,
            total_bytes,
        )
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
    session: codec::FetchSessionInfo,
) -> Option<(Bytes, Vec<LogRegion>, u64)> {
    let count = request.partitions.len().max(1);
    let budget = broker
        .config()
        .max_frame_bytes
        .saturating_sub(FRAME_HEADROOM);
    let per_partition = (budget / count).max(64 * 1024);

    let reads = futures::future::join_all(request.partitions.iter().map(|descriptor| async move {
        // The rack-aware resolution, so a consumer redirected to a
        // follower still gets the zero-copy path rather than quietly
        // falling back to the buffered one on every fetch.
        let handle = crate::handlers::consumer_partition(
            broker,
            &descriptor.topic,
            descriptor.partition,
            &request.rack,
        )
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
                preferred_read_replica: crate::handlers::preferred_read_replica(
                    broker,
                    &descriptor.topic,
                    descriptor.partition,
                    &request.rack,
                ),
            },
            kept,
        ));
    }
    if served == 0 {
        return None;
    }
    let header = codec::encode_fetch_multi_header(session, &header_inputs).ok()?;
    Some((header, regions, served))
}

/// Count a served fetch and apply the client's quota.
async fn record_fetch(broker: &Broker, client: ClientIdentity<'_>, served: u64) {
    let metrics = broker.metrics();
    metrics.count(names::FETCH_REQUESTS, 1);
    metrics.count(names::FETCH_BYTES, served);
    let throttle = broker
        .throttle(client.principal, client.client_id, QuotaKind::Fetch, served)
        .await;
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

    // Resolve the incremental fetch session (KIP-227) before anything
    // reads a partition: what the client sent is a *delta*, and every path
    // below wants the full set the session stands for.
    let (session, request) = match resolve_session(broker, request) {
        Ok(resolved) => resolved,
        Err(session) => {
            // The session is unknown or out of step. Answering with no
            // results and the error at request level is what tells the
            // client to start again in full — a per-partition error would
            // send it looking for a partition problem it does not have.
            return codec::encode_fetch_multi_response_chunks(session, &[])
                .unwrap_or_default()
                .into();
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
        if let Some((header, regions, served)) =
            fetch_multi_zero_copy(broker, &request, session).await
        {
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

    codec::encode_fetch_multi_response_chunks(session, &results)
        .unwrap_or_default()
        .into()
}

/// Expand an incremental fetch into the full set of partitions it means.
///
/// `Ok` carries the session to answer with and a request whose partition
/// list is complete; `Err` carries the session error to answer with when
/// the client named a session this broker does not have.
fn resolve_session(
    broker: &Broker,
    request: FetchMultiRequest,
) -> Result<(codec::FetchSessionInfo, FetchMultiRequest), codec::FetchSessionInfo> {
    let updates: Vec<(String, i32, i64, i32)> = request
        .partitions
        .iter()
        .map(|descriptor| {
            (
                descriptor.topic.clone(),
                descriptor.partition,
                descriptor.fetch_offset,
                descriptor.max_bytes,
            )
        })
        .collect();
    let forgotten: Vec<(String, i32)> = request
        .forgotten
        .iter()
        .map(|partition| (partition.topic.clone(), partition.partition))
        .collect();

    match broker.fetch_sessions().resolve(
        request.session_id,
        request.session_epoch,
        &updates,
        &forgotten,
    ) {
        crate::fetchsession::SessionOutcome::None => {
            Ok((codec::FetchSessionInfo::default(), request))
        }
        crate::fetchsession::SessionOutcome::Resolved {
            session_id,
            session_epoch,
            partitions,
        } => {
            let mut request = request;
            request.partitions = partitions
                .into_iter()
                .map(
                    |(topic, partition, fetch_offset, max_bytes)| FetchMultiPartition {
                        topic,
                        partition,
                        fetch_offset,
                        max_bytes,
                    },
                )
                .collect();
            Ok((
                codec::FetchSessionInfo {
                    session_id,
                    session_epoch,
                    error_code: ec::NONE,
                },
                request,
            ))
        }
        crate::fetchsession::SessionOutcome::Invalid(session_id) => Err(codec::FetchSessionInfo {
            session_id,
            session_epoch: 0,
            error_code: ec::FETCH_SESSION_NOT_FOUND,
        }),
    }
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
            preferred_read_replica: crate::handlers::preferred_read_replica(
                broker,
                &descriptor.topic,
                descriptor.partition,
                &request.rack,
            ),
        };
        let handle = match crate::handlers::consumer_partition(
            broker,
            &descriptor.topic,
            descriptor.partition,
            &request.rack,
        ) {
            Ok(handle) => handle,
            Err(error) => {
                result.error_code = code_of(&error);
                return (result, Vec::new());
            }
        };
        let allowance = (descriptor.max_bytes.max(0) as usize).min(per_partition);
        match handle
            .read_at(descriptor.fetch_offset, allowance, isolation)
            .await
        {
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
/// re-read everything through the same budgeted path as the first read.
///
/// The wait is on each partition's high-watermark watch rather than a
/// timer: a thousand idle partitions cost nothing until one of them moves.
/// Re-reading through `read_all_partitions` matters as much as the wait —
/// it is what applies the per-partition allowance and the frame budget, so
/// a burst arriving during the poll cannot produce a response larger than
/// the client's decoder accepts.
async fn long_poll(
    broker: &Broker,
    request: &FetchMultiRequest,
    results: &mut Vec<(FetchMultiResult, Vec<Bytes>)>,
) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(request.max_wait_ms as u64);
    // The rack-aware resolution, so a consumer redirected to a follower
    // waits on that follower's watermark instead of never waking.
    let mut watches: Vec<(usize, tokio::sync::watch::Receiver<i64>, i64)> = request
        .partitions
        .iter()
        .enumerate()
        .filter_map(|(index, descriptor)| {
            let handle = crate::handlers::consumer_partition(
                broker,
                &descriptor.topic,
                descriptor.partition,
                &request.rack,
            )
            .ok()?;
            let watch = handle.watermark_watch();
            Some((index, watch, descriptor.fetch_offset))
        })
        .collect();
    if watches.is_empty() {
        return 0;
    }
    loop {
        // Anything already past its fetch offset was written between the
        // first read and now; serve immediately rather than sleeping.
        let ready = watches
            .iter()
            .any(|(_, watch, fetch_offset)| *watch.borrow() > *fetch_offset);
        if !ready {
            let wakeups = watches
                .iter_mut()
                .map(|(_, watch, _)| Box::pin(watch.changed()));
            let woke =
                tokio::time::timeout_at(deadline, futures::future::select_all(wakeups)).await;
            match woke {
                Ok((Ok(()), _, _)) => {}
                // A closed watch means the partition actor went away;
                // stop waiting and let the re-read report it.
                Ok((Err(_), _, _)) => {}
                Err(_) => return 0,
            }
        }
        let mut reread = Vec::with_capacity(results.len());
        let served = read_all_partitions(broker, request, &mut reread).await;
        if served > 0 {
            *results = reread;
            return served;
        }
        if tokio::time::Instant::now() >= deadline {
            return 0;
        }
    }
}
