//! Cluster and topic introspection, plus explicit record deletion
//! (api_keys 19-22).
//!
//! Everything here answers a question an operator has at three in the
//! morning and that the data plane could not previously answer at all:
//! what cluster am I talking to, what is this topic actually configured
//! to do, which partition filled the disk, and how do I get rid of data
//! without deleting the topic.
//!
//! These are deliberately data-plane APIs rather than controller HTTP.
//! A client that can produce and consume can also ask what it is producing
//! into; making it reach a separate admin port to find out is how
//! monitoring ends up not being written.

use bytes::Bytes;
use tracing::warn;

use brahmaputra_metadata::NodeRole;
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{
    AlterConfigsRequest, AlterConfigsResponse, AlterLogDirResult, AlterReplicaLogDirsRequest,
    AlterReplicaLogDirsResponse, ConfigEntry, DeleteRecordsRequest, DeleteRecordsResponse,
    DeleteRecordsResult, DescribeClusterBroker, DescribeClusterRequest, DescribeClusterResponse,
    DescribeConfigsRequest, DescribeConfigsResponse, DescribeLogDirsRequest,
    DescribeLogDirsResponse, DescribeProducersRequest, DescribeProducersResponse,
    DescribeTransactionsRequest, DescribeTransactionsResponse, ListTransactionsRequest,
    ListTransactionsResponse, LogDirInfo, LogDirPartition, ProducerState,
};
use brahmaputra_protocol::ApiKey;

use crate::error::BrokerError;
use crate::handlers::{code_of, codec_bytes, encode_error_for};
use crate::server::Broker;

/// `-1` in a `DeleteRecords` request: trim everything that is committed.
const DELETE_TO_HIGH_WATERMARK: i64 = -1;

// ---------- DescribeCluster (api_key 19) ----------

pub(crate) fn describe_cluster(broker: &Broker, body: Bytes) -> Bytes {
    if DescribeClusterRequest::decode(&body).is_err() {
        warn!("undecodable describe-cluster request");
        return encode_error_for(ApiKey::DescribeCluster, ec::INVALID_REQUEST);
    }

    let response = match broker.metadata_cache() {
        Some(cache) => {
            let image = cache.snapshot();
            DescribeClusterResponse {
                error_code: ec::NONE,
                cluster_id: image.cluster_id.clone(),
                controller_id: image.controller_id.unwrap_or(-1),
                brokers: image
                    .brokers
                    .values()
                    // Fenced brokers stay in the image so their epochs
                    // remain comparable; reporting them as cluster members
                    // would tell an operator the cluster is healthier than
                    // it is.
                    .filter(|member| member.alive && member.roles.contains(&NodeRole::Broker))
                    .map(|member| DescribeClusterBroker {
                        broker_id: member.broker_id,
                        host: member.host.clone(),
                        port: i32::from(member.data_port),
                        rack: member.rack.clone().unwrap_or_default(),
                    })
                    .collect(),
            }
        }
        // Standalone: one broker, its own cluster, no controller.
        None => {
            let addr = broker.local_addr();
            DescribeClusterResponse {
                error_code: ec::NONE,
                cluster_id: "standalone".to_owned(),
                controller_id: -1,
                brokers: vec![DescribeClusterBroker {
                    broker_id: broker.config().broker_id,
                    host: addr.ip().to_string(),
                    port: i32::from(addr.port()),
                    rack: String::new(),
                }],
            }
        }
    };
    codec_bytes(response.encode())
}

// ---------- DescribeConfigs (api_key 20) ----------

/// Every topic-level config the broker actually reads, paired with the
/// broker-wide default it falls back to.
///
/// Listing them explicitly rather than echoing whatever happens to be set
/// is the point: an operator needs to see the configs that exist and are
/// unset just as much as the ones that were changed, because those are the
/// ones a broker restart with different flags will silently move.
fn topic_config_defaults(broker: &Broker) -> Vec<(&'static str, String)> {
    let log = &broker.config().log_config;
    vec![
        ("segment.bytes", log.segment_bytes.to_string()),
        (
            "segment.ms",
            log.segment_ms
                .map_or_else(|| "-1".to_owned(), |v| v.to_string()),
        ),
        (
            "retention.ms",
            log.retention_ms
                .map_or_else(|| "-1".to_owned(), |v| v.to_string()),
        ),
        (
            "retention.bytes",
            log.retention_bytes
                .map_or_else(|| "-1".to_owned(), |v| v.to_string()),
        ),
        (
            "cleanup.policy",
            if log.compact { "compact" } else { "delete" }.to_owned(),
        ),
        ("delete.retention.ms", log.delete_retention_ms.to_string()),
        (
            "min.cleanable.dirty.ratio",
            log.min_cleanable_dirty_ratio.to_string(),
        ),
        (
            "min.compaction.lag.ms",
            log.min_compaction_lag_ms.to_string(),
        ),
        (
            "max.compaction.lag.ms",
            log.max_compaction_lag_ms
                .map_or_else(|| "-1".to_owned(), |v| v.to_string()),
        ),
        (
            "max.message.bytes",
            broker
                .config()
                .max_message_bytes
                .map_or_else(|| "-1".to_owned(), |limit| limit.to_string()),
        ),
        (
            "flush.messages",
            log.flush_interval_messages
                .map_or_else(|| "-1".to_owned(), |v| v.to_string()),
        ),
        (
            "flush.ms",
            log.flush_interval_ms
                .map_or_else(|| "-1".to_owned(), |v| v.to_string()),
        ),
        ("index.interval.bytes", log.index_interval_bytes.to_string()),
        (
            "message.timestamp.type",
            if log.log_append_time {
                "LogAppendTime"
            } else {
                "CreateTime"
            }
            .to_owned(),
        ),
        // Reported as Kafka's `producer` value, which is exactly what this
        // broker does: it stores the codec the producer chose, byte for
        // byte. Converting a batch to a topic-wide codec would mean
        // decompressing and recompressing every one of them, which is the
        // cost the zero-copy fetch path and byte-identical replication
        // exist to avoid. `compression.type` on a topic is enforced by
        // *refusing* a batch in another codec, never by rewriting it.
        ("compression.type", "producer".to_owned()),
        ("min.insync.replicas", "1".to_owned()),
    ]
}

/// Broker-level configuration, as an operator would set it on the command
/// line.
fn broker_configs(broker: &Broker) -> Vec<(String, String)> {
    let config = broker.config();
    let log = &config.log_config;
    vec![
        ("broker.id".to_owned(), config.broker_id.to_string()),
        (
            "group.initial.rebalance.delay.ms".to_owned(),
            config.group_initial_rebalance_delay.as_millis().to_string(),
        ),
        ("host".to_owned(), config.host.clone()),
        ("port".to_owned(), config.port.to_string()),
        (
            "log.dir".to_owned(),
            broker
                .log_dirs()
                .paths()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(","),
        ),
        (
            "num.partitions".to_owned(),
            config.default_partitions.to_string(),
        ),
        (
            "log.segment.bytes".to_owned(),
            log.segment_bytes.to_string(),
        ),
        (
            "socket.request.max.bytes".to_owned(),
            config.max_frame_bytes.to_string(),
        ),
        (
            "log.retention.check.interval.ms".to_owned(),
            config.retention_check_interval.as_millis().to_string(),
        ),
        (
            "offsets.retention.ms".to_owned(),
            config
                .offsets_retention
                .map_or_else(|| "-1".to_owned(), |d| d.as_millis().to_string()),
        ),
        ("transport".to_owned(), format!("{:?}", config.transport)),
        ("require.auth".to_owned(), config.require_auth.to_string()),
        (
            "replication.enabled".to_owned(),
            config.replication_enabled.to_string(),
        ),
    ]
}

pub(crate) fn describe_configs(broker: &Broker, body: Bytes) -> Bytes {
    let request = match DescribeConfigsRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable describe-configs request");
            return encode_error_for(ApiKey::DescribeConfigs, ec::INVALID_REQUEST);
        }
    };
    let respond = |error_code, configs: Vec<ConfigEntry>| {
        codec_bytes(
            DescribeConfigsResponse {
                error_code,
                resource_type: request.resource_type.clone(),
                resource_name: request.resource_name.clone(),
                configs,
            }
            .encode(),
        )
    };

    let mut entries = if request.resource_type.eq_ignore_ascii_case("broker") {
        broker_configs(broker)
            .into_iter()
            .map(|(name, value)| ConfigEntry {
                name,
                value,
                is_default: true,
            })
            .collect()
    } else if request.resource_type.eq_ignore_ascii_case("topic") {
        // A topic's overrides live in cluster metadata. A standalone broker
        // has no such metadata and creates topics on first sight, so there
        // is nothing there to be unknown and nothing to override: every
        // value is the broker default.
        let overrides = match broker.metadata_cache() {
            Some(cache) => match cache.snapshot().topics.get(&request.resource_name) {
                Some(topic) => topic.configs.clone(),
                None => return respond(ec::UNKNOWN_TOPIC_OR_PARTITION, Vec::new()),
            },
            None => Default::default(),
        };
        topic_config_defaults(broker)
            .into_iter()
            .map(|(name, default)| match overrides.get(name) {
                Some(set) => ConfigEntry {
                    name: name.to_owned(),
                    value: set.clone(),
                    is_default: false,
                },
                None => ConfigEntry {
                    name: name.to_owned(),
                    value: default,
                    is_default: true,
                },
            })
            .collect::<Vec<_>>()
    } else {
        return respond(ec::INVALID_REQUEST, Vec::new());
    };

    if !request.config_names.is_empty() {
        entries.retain(|entry| request.config_names.contains(&entry.name));
    }
    respond(ec::NONE, entries)
}

// ---------- DescribeLogDirs (api_key 21) ----------

pub(crate) async fn describe_log_dirs(broker: &Broker, body: Bytes) -> Bytes {
    let request = match DescribeLogDirsRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable describe-log-dirs request");
            return encode_error_for(ApiKey::DescribeLogDirs, ec::INVALID_REQUEST);
        }
    };

    let wanted: Vec<String> = if request.topics.is_empty() {
        broker
            .state()
            .topics()
            .into_iter()
            .map(|(name, _)| name)
            .collect()
    } else {
        request.topics
    };

    let wanted: std::collections::BTreeSet<String> = wanted.into_iter().collect();

    // One entry per configured directory, which is what the response has
    // always been shaped for — until now there was only ever one to put in
    // it. Each partition is reported under the disk it is actually on,
    // because "which topic filled *which* disk" is the question a
    // multi-disk broker makes it possible to ask.
    let mut log_dirs = Vec::new();
    for described in broker.log_dirs().describe() {
        let mut partitions = Vec::new();
        for (topic, partition) in described.partitions {
            if !wanted.contains(&topic) {
                continue;
            }
            // Only partitions this broker has actually opened: asking how
            // big a partition is must not create it, and on an offline
            // directory there is nothing to ask.
            let Some(handle) = broker.hosted_partition(&topic, partition) else {
                continue;
            };
            let Ok(usage) = handle.usage().await else {
                continue;
            };
            partitions.push(LogDirPartition {
                topic: topic.clone(),
                partition,
                size_bytes: usage.size_bytes as i64,
                offset_lag: (usage.log_end_offset - usage.high_watermark).max(0),
                is_leader: broker.leads_partition(&topic, partition),
            });
        }

        let (total_bytes, usable_bytes) = if described.online {
            filesystem_capacity(&described.path)
        } else {
            // A disk that is gone has no capacity to report, and guessing
            // zero would read as "full" to anything watching.
            (-1, -1)
        };
        log_dirs.push(LogDirInfo {
            // A failed directory is reported *as failed* rather than
            // omitted: an operator needs to see the disk that died, not a
            // response that quietly no longer mentions it.
            error_code: if described.online {
                ec::NONE
            } else {
                ec::LOG_DIR_OFFLINE
            },
            log_dir: described.path.display().to_string(),
            offline_reason: described.offline_reason.unwrap_or_default(),
            total_bytes,
            usable_bytes,
            partitions,
        });
    }

    codec_bytes(
        DescribeLogDirsResponse {
            error_code: ec::NONE,
            log_dirs,
        }
        .encode(),
    )
}

/// Total and free bytes of the filesystem holding the data directory.
///
/// `(-1, -1)` when the platform cannot be asked. Reporting an unknown as a
/// sentinel rather than as zero matters: zero free bytes is a page, and a
/// monitoring system cannot tell a genuine "disk full" from "we could not
/// look" if both are spelled the same way.
#[cfg(unix)]
fn filesystem_capacity(path: &std::path::Path) -> (i64, i64) {
    use std::os::unix::ffi::OsStrExt;
    let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return (-1, -1);
    };
    // SAFETY: `statvfs` writes into a caller-provided struct and reads a
    // NUL-terminated path; both are satisfied here.
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) } != 0 {
        return (-1, -1);
    }
    let frag = stat.f_frsize as u64;
    (
        (stat.f_blocks as u64).saturating_mul(frag) as i64,
        (stat.f_bavail as u64).saturating_mul(frag) as i64,
    )
}

#[cfg(not(unix))]
fn filesystem_capacity(_path: &std::path::Path) -> (i64, i64) {
    (-1, -1)
}

// ---------- DeleteRecords (api_key 22) ----------

pub(crate) async fn delete_records(broker: &Broker, body: Bytes) -> Bytes {
    let request = match DeleteRecordsRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable delete-records request");
            return encode_error_for(ApiKey::DeleteRecords, ec::INVALID_REQUEST);
        }
    };

    let mut results = Vec::with_capacity(request.partitions.len());
    for target in request.partitions {
        let handle = match broker.partition(&target.topic, target.partition) {
            Ok(handle) => handle,
            Err(error) => {
                results.push(DeleteRecordsResult {
                    topic: target.topic,
                    partition: target.partition,
                    error_code: code_of(&error),
                    low_watermark: -1,
                });
                continue;
            }
        };
        // Deleting is a write to the log, and only the leader may write:
        // letting a follower trim independently would leave replicas
        // disagreeing about where the log starts.
        if !broker.leads_partition(&target.topic, target.partition) {
            results.push(DeleteRecordsResult {
                topic: target.topic,
                partition: target.partition,
                error_code: ec::NOT_LEADER_OR_FOLLOWER,
                low_watermark: -1,
            });
            continue;
        }

        let offset = if target.offset == DELETE_TO_HIGH_WATERMARK {
            match handle.offsets().await {
                Ok((_, _, high_watermark)) => high_watermark,
                Err(error) => {
                    results.push(DeleteRecordsResult {
                        topic: target.topic,
                        partition: target.partition,
                        error_code: code_of(&BrokerError::Storage(error)),
                        low_watermark: -1,
                    });
                    continue;
                }
            }
        } else {
            target.offset
        };

        match handle.delete_records_before(offset).await {
            Ok(low_watermark) => results.push(DeleteRecordsResult {
                topic: target.topic,
                partition: target.partition,
                error_code: ec::NONE,
                low_watermark,
            }),
            Err(error) => results.push(DeleteRecordsResult {
                topic: target.topic,
                partition: target.partition,
                error_code: code_of(&BrokerError::Storage(error)),
                low_watermark: -1,
            }),
        }
    }

    codec_bytes(DeleteRecordsResponse { results }.encode())
}

// ---------- AlterConfigs (api_key 28) ----------

/// Topic configuration keys the broker actually acts on.
///
/// A rejected name is the point: `retention.ms` set as `retention.msec`
/// would otherwise be accepted, stored, echoed back by `DescribeConfigs`,
/// and silently ignored by the log — which looks exactly like a broker
/// that does not honour retention.
const KNOWN_TOPIC_CONFIGS: &[&str] = &[
    "retention.ms",
    "retention.bytes",
    "segment.bytes",
    "segment.ms",
    "cleanup.policy",
    "delete.retention.ms",
    "min.cleanable.dirty.ratio",
    "min.compaction.lag.ms",
    "max.compaction.lag.ms",
    "flush.messages",
    "flush.ms",
    "max.message.bytes",
    "min.insync.replicas",
    "message.timestamp.type",
    "compression.type",
];

/// Change a topic's configuration over the data plane.
///
/// The broker does not own topic configuration — the controller does, and
/// that is what makes a change durable and ordered against every other
/// metadata change. So this forwards rather than applies, and a success
/// means the controller committed it, not that this broker wrote it down.
pub(crate) async fn alter_configs(broker: &Broker, body: Bytes) -> Bytes {
    let request = match AlterConfigsRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable alter-configs request");
            return encode_error_for(ApiKey::AlterConfigs, ec::INVALID_REQUEST);
        }
    };

    let respond = |error_code, message: String| {
        codec_bytes(
            AlterConfigsResponse {
                error_code,
                resource_type: request.resource_type.clone(),
                resource_name: request.resource_name.clone(),
                error_message: message,
            }
            .encode(),
        )
    };

    if !request.resource_type.eq_ignore_ascii_case("topic") {
        return respond(
            ec::INVALID_REQUEST,
            format!(
                "only topic configuration can be altered, not {:?}",
                request.resource_type
            ),
        );
    }

    let unknown: Vec<&str> = request
        .configs
        .iter()
        .map(|entry| entry.name.as_str())
        .filter(|name| !KNOWN_TOPIC_CONFIGS.contains(name))
        .collect();
    if !unknown.is_empty() {
        return respond(
            ec::INVALID_CONFIG,
            format!(
                "unknown topic configuration: {} (known: {})",
                unknown.join(", "),
                KNOWN_TOPIC_CONFIGS.join(", ")
            ),
        );
    }

    let Some(cache) = broker.metadata_cache() else {
        return respond(
            ec::INVALID_REQUEST,
            "a standalone broker has no topic configuration to alter; its settings come from its command line"
                .to_owned(),
        );
    };
    let image = cache.snapshot();
    let Some(topic) = image.topics.get(&request.resource_name) else {
        return respond(
            ec::UNKNOWN_TOPIC_OR_PARTITION,
            format!("no topic named {:?}", request.resource_name),
        );
    };

    // Incremental merges into what is there; the other form replaces it.
    // That is Kafka's distinction and it matters: replacing is how a config
    // is removed, and merging is how one is changed without resending the
    // rest and clobbering a concurrent change.
    let mut configs = if request.incremental {
        topic.configs.clone()
    } else {
        std::collections::BTreeMap::new()
    };
    for entry in &request.configs {
        if request.incremental && entry.value.is_empty() {
            configs.remove(&entry.name);
        } else {
            configs.insert(entry.name.clone(), entry.value.clone());
        }
    }

    let command = brahmaputra_metadata::MetadataCommand::SetTopicConfig {
        name: request.resource_name.clone(),
        configs,
    };
    match submit_to_controller(&image, &command).await {
        Ok(()) => respond(ec::NONE, String::new()),
        Err(error) => respond(ec::CONTROLLER_NOT_AVAILABLE, error),
    }
}

/// Send a metadata command to whichever node is currently the controller.
async fn submit_to_controller(
    image: &brahmaputra_metadata::ClusterMetadata,
    command: &brahmaputra_metadata::MetadataCommand,
) -> Result<(), String> {
    let controller_id = image
        .controller_id
        .ok_or_else(|| "the cluster has no controller right now".to_owned())?;
    let member = image
        .brokers
        .get(&controller_id)
        .ok_or_else(|| format!("controller {controller_id} is not in the metadata"))?;
    let url = format!(
        "http://{}:{}/api/v1/controller/command",
        member.host, member.control_port
    );
    let response = reqwest::Client::new()
        .post(&url)
        .json(command)
        .send()
        .await
        .map_err(|error| format!("cannot reach the controller at {url}: {error}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("controller returned HTTP {status}: {body}"));
    }
    // The controller answers with a Result-shaped body, so a rejected
    // command arrives as HTTP 200 carrying an error. Reading the status
    // alone would report that as success.
    let text = response
        .text()
        .await
        .map_err(|error| format!("controller returned an unreadable response: {error}"))?;
    if text.contains("\"Err\"") {
        return Err(format!("controller rejected the change: {text}"));
    }
    Ok(())
}

// ---------- DescribeProducers (api_key 29) ----------

/// Who has written to a partition, and what they have left open.
pub(crate) async fn describe_producers(broker: &Broker, body: Bytes) -> Bytes {
    let request = match DescribeProducersRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable describe-producers request");
            return encode_error_for(ApiKey::DescribeProducers, ec::INVALID_REQUEST);
        }
    };

    let respond = |error_code, state: Option<crate::actor::PartitionProducers>| {
        let state = state.unwrap_or_default();
        codec_bytes(
            DescribeProducersResponse {
                error_code,
                topic: request.topic.clone(),
                partition: request.partition,
                last_stable_offset: state.last_stable_offset,
                high_watermark: state.high_watermark,
                producers: state
                    .producers
                    .into_iter()
                    .map(|producer| ProducerState {
                        producer_id: producer.producer_id,
                        producer_epoch: i32::from(producer.producer_epoch),
                        last_sequence: producer.last_sequence,
                        // The producer state table remembers sequences, not
                        // wall-clock times; the timestamps that would answer
                        // this live in the batches themselves.
                        last_timestamp_ms: -1,
                        current_txn_start_offset: producer.current_txn_start_offset,
                    })
                    .collect(),
            }
            .encode(),
        )
    };

    let handle = match broker.partition(&request.topic, request.partition) {
        Ok(handle) => handle,
        Err(error) => return respond(code_of(&error), None),
    };
    match handle.producers().await {
        Ok(state) => respond(ec::NONE, Some(state)),
        Err(error) => respond(code_of(&BrokerError::Storage(error)), None),
    }
}

// ---------- ListTransactions / DescribeTransactions (api_keys 30, 31) ----------

/// Every transactional id this broker coordinates.
pub(crate) async fn list_transactions(broker: &Broker, body: Bytes) -> Bytes {
    let request = match ListTransactionsRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable list-transactions request");
            return encode_error_for(ApiKey::ListTransactions, ec::INVALID_REQUEST);
        }
    };

    let wanted: Vec<String> = request
        .states
        .iter()
        .map(|state| state.to_ascii_lowercase())
        .collect();
    let transactions = crate::transaction::list_local_transactions(broker)
        .await
        .into_iter()
        .filter(|listing| wanted.is_empty() || wanted.contains(&listing.state.to_ascii_lowercase()))
        .collect();

    codec_bytes(
        ListTransactionsResponse {
            error_code: ec::NONE,
            transactions,
        }
        .encode(),
    )
}

/// One transaction in full, including the partitions it has announced.
pub(crate) async fn describe_transactions(broker: &Broker, body: Bytes) -> Bytes {
    let request = match DescribeTransactionsRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable describe-transactions request");
            return encode_error_for(ApiKey::DescribeTransactions, ec::INVALID_REQUEST);
        }
    };

    match crate::transaction::describe_local_transaction(broker, &request.transactional_id).await {
        Ok(Some(response)) => codec_bytes(response.encode()),
        // Not found is not an error about the id: this broker may simply
        // not coordinate it. The client routes to the coordinator the same
        // way it routes a group, so a wrong broker is a client bug worth
        // naming rather than an empty answer that looks like "no such
        // transaction".
        Ok(None) => codec_bytes(
            DescribeTransactionsResponse {
                error_code: ec::UNKNOWN_TOPIC_OR_PARTITION,
                transactional_id: request.transactional_id,
                ..Default::default()
            }
            .encode(),
        ),
        Err(error) => codec_bytes(
            DescribeTransactionsResponse {
                error_code: code_of(&error),
                transactional_id: request.transactional_id,
                ..Default::default()
            }
            .encode(),
        ),
    }
}

// ---------- AlterReplicaLogDirs (api_key 32) ----------

/// Move partitions between the disks of the broker that answers.
///
/// The partition is closed for the duration of the copy. That is the honest
/// cost of moving bytes that are being appended to: Kafka builds a second
/// copy alongside, lets it catch up, and swaps — which avoids the pause but
/// needs both copies to fit and a whole state machine to manage the
/// catch-up. Here the pause is visible and bounded by the partition's size,
/// and an operator who cannot afford it moves a follower or hands
/// leadership away first.
pub(crate) async fn alter_replica_log_dirs(broker: &Broker, body: Bytes) -> Bytes {
    let request = match AlterReplicaLogDirsRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable alter-replica-log-dirs request");
            return encode_error_for(ApiKey::AlterReplicaLogDirs, ec::INVALID_REQUEST);
        }
    };

    let mut results = Vec::with_capacity(request.partitions.len());
    for target in request.partitions {
        let outcome =
            move_partition(broker, &target.topic, target.partition, &target.log_dir).await;
        results.push(match outcome {
            Ok((log_dir, bytes_moved)) => AlterLogDirResult {
                topic: target.topic,
                partition: target.partition,
                error_code: ec::NONE,
                log_dir,
                bytes_moved,
            },
            Err(error) => {
                warn!(topic = %target.topic, partition = target.partition, %error, "log dir move failed");
                AlterLogDirResult {
                    topic: target.topic,
                    partition: target.partition,
                    error_code: code_of(&error),
                    log_dir: String::new(),
                    bytes_moved: 0,
                }
            }
        });
    }

    codec_bytes(AlterReplicaLogDirsResponse { results }.encode())
}

async fn move_partition(
    broker: &Broker,
    topic: &str,
    partition: i32,
    destination: &str,
) -> Result<(String, i64), BrokerError> {
    let destination = std::path::PathBuf::from(destination);
    // The destination has to be a directory this broker was actually given.
    // Copying into an arbitrary path would put data somewhere nothing
    // scans on restart, which is a partition that vanishes at the next
    // reboot.
    let known = broker
        .log_dirs()
        .paths()
        .any(|path| path == destination.as_path());
    if !known {
        return Err(BrokerError::Meta(format!(
            "{} is not one of this broker's log directories",
            destination.display()
        )));
    }
    if !broker.log_dirs().is_online(&destination) {
        return Err(BrokerError::LogDirOffline {
            topic: topic.to_owned(),
            partition,
            dir: destination.display().to_string(),
        });
    }

    let Some(current) = broker.log_dirs().existing(topic, partition) else {
        return Err(BrokerError::UnknownTopicOrPartition {
            topic: topic.to_owned(),
            partition,
        });
    };
    let target = crate::logdirs::partition_path(&destination, topic, partition);
    if current == target {
        // Already there. A no-op rather than an error: an operator
        // levelling a broker's disks should be able to name every
        // partition and let the ones already in place stay.
        let bytes = directory_size(&current);
        return Ok((destination.display().to_string(), bytes));
    }

    // Stop writing before copying. A copy taken while appends continue is
    // a copy of no particular moment, and the difference would be silently
    // lost records rather than a visible failure.
    broker
        .close_partitions(&[(topic.to_owned(), partition)])
        .await;

    let copied = copy_directory(&current, &target).map_err(|error| {
        BrokerError::Meta(format!(
            "copying {} to {}: {error}",
            current.display(),
            target.display()
        ))
    })?;

    // Only now is the move real. Placement first, then the old copy: a
    // crash between the two leaves a stale directory that nothing points
    // at, which the next startup scan reconciles. The other order would
    // leave a partition with no copy at all.
    broker
        .log_dirs()
        .reassign(topic, partition, &destination)
        .map_err(BrokerError::Meta)?;
    if let Err(error) = std::fs::remove_dir_all(&current) {
        warn!(
            path = %current.display(),
            %error,
            "moved partition's old directory could not be removed"
        );
    }

    // Reopen from its new home, so the partition is serving again before
    // this request is answered.
    broker.partition(topic, partition)?;
    Ok((destination.display().to_string(), copied))
}

fn directory_size(path: &std::path::Path) -> i64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .flatten()
        .filter_map(|entry| entry.metadata().ok())
        .filter(|metadata| metadata.is_file())
        .map(|metadata| metadata.len() as i64)
        .sum()
}

/// Copy a partition directory, returning the bytes written.
///
/// Flat by construction — a partition directory holds segment files and
/// checkpoints, never subdirectories — so this does not recurse, and a
/// subdirectory appearing would be a bug worth noticing rather than
/// something to copy silently.
fn copy_directory(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<i64> {
    std::fs::create_dir_all(to)?;
    let mut copied = 0i64;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        if !entry.metadata()?.is_file() {
            continue;
        }
        copied += std::fs::copy(entry.path(), to.join(entry.file_name()))? as i64;
    }
    Ok(copied)
}
