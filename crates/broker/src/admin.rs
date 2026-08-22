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
    ConfigEntry, DeleteRecordsRequest, DeleteRecordsResponse, DeleteRecordsResult,
    DescribeClusterBroker, DescribeClusterRequest, DescribeClusterResponse, DescribeConfigsRequest,
    DescribeConfigsResponse, DescribeLogDirsRequest, DescribeLogDirsResponse, LogDirInfo,
    LogDirPartition,
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
            log.segment_ms.map_or_else(|| "-1".to_owned(), |v| v.to_string()),
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
        ("max.message.bytes", "-1".to_owned()),
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
        ("log.segment.bytes".to_owned(), log.segment_bytes.to_string()),
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
