//! Cluster and topic introspection, and explicit record deletion.
//!
//! The data-plane counterparts to what an operator would otherwise have to
//! read off the controller's HTTP API or off the brokers' disks by hand:
//! who is in the cluster, what a topic is configured to do, which
//! partitions are using the disk, and how to delete records without
//! deleting the topic that holds them.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use brahmaputra_protocol::gen::{
    DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsResponse, DescribeClusterRequest,
    DescribeClusterResponse, DescribeConfigsRequest, DescribeConfigsResponse,
    DescribeLogDirsRequest, DescribeLogDirsResponse,
};
use brahmaputra_protocol::ApiKey;

use crate::error::ClientError;
use crate::group_consumer::msg_err;
use crate::router::BrokerRouter;
use crate::transport::{Transport, TransportConfig};

/// One broker in a cluster description.
#[derive(Debug, Clone)]
pub struct ClusterBroker {
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
    /// Empty when the broker was started without `--rack`.
    pub rack: String,
}

/// What a broker says the cluster is.
#[derive(Debug, Clone)]
pub struct ClusterDescription {
    pub cluster_id: String,
    /// `-1` when no controller is elected, or in standalone mode.
    pub controller_id: i32,
    pub brokers: Vec<ClusterBroker>,
}

/// One configuration entry in force on a resource.
#[derive(Debug, Clone)]
pub struct ResourceConfig {
    pub name: String,
    pub value: String,
    /// `true` when the resource never set this and inherits the default.
    /// That is the difference that decides whether changing a broker flag
    /// will move it.
    pub is_default: bool,
}

/// Disk usage of one partition on one broker.
#[derive(Debug, Clone)]
pub struct LogDirPartitionUsage {
    pub topic: String,
    pub partition: i32,
    pub size_bytes: i64,
    /// Records held locally that are not yet known-committed.
    pub offset_lag: i64,
    pub is_leader: bool,
}

/// One data directory as its broker reports it.
#[derive(Debug, Clone)]
pub struct LogDirUsage {
    pub broker_id: i32,
    pub log_dir: String,
    /// Non-zero when the disk under this directory has failed. Its
    /// partitions are unavailable on that broker and have failed over.
    pub error_code: i32,
    /// Why the disk failed, empty when it is healthy.
    pub offline_reason: String,
    /// `-1` where the platform could not be asked. Deliberately not zero:
    /// a full disk and an unanswerable question must not look alike.
    pub total_bytes: i64,
    pub usable_bytes: i64,
    pub partitions: Vec<LogDirPartitionUsage>,
}

/// Outcome of deleting records from one partition.
#[derive(Debug, Clone)]
pub struct DeletedRecords {
    pub topic: String,
    pub partition: i32,
    /// The log start offset after the delete.
    pub low_watermark: i64,
}

/// Administrative reads and record deletion over the data plane.
pub struct Admin {
    router: BrokerRouter,
}

impl Admin {
    pub async fn connect(addr: SocketAddr, client_id: &str) -> Result<Admin, ClientError> {
        Admin::connect_with(Transport::default(), addr, client_id).await
    }

    /// Connect over an explicit transport (must match the broker's).
    pub async fn connect_with(
        transport: impl Into<TransportConfig>,
        addr: SocketAddr,
        client_id: &str,
    ) -> Result<Admin, ClientError> {
        let router =
            BrokerRouter::connect_with(transport, addr, Some(client_id.to_owned()), 5).await?;
        Ok(Admin { router })
    }

    /// Who is in the cluster, and which node is the controller.
    pub async fn describe_cluster(&self) -> Result<ClusterDescription, ClientError> {
        let body = DescribeClusterRequest {
            include_cluster_authorized_operations: false,
        }
        .encode()
        .map_err(msg_err)?;
        let bytes = self
            .router
            .request_seed(ApiKey::DescribeCluster, &body)
            .await?;
        let response = DescribeClusterResponse::decode(&bytes).map_err(msg_err)?;
        ClientError::from_error_code(response.error_code)?;
        Ok(ClusterDescription {
            cluster_id: response.cluster_id,
            controller_id: response.controller_id,
            brokers: response
                .brokers
                .into_iter()
                .map(|broker| ClusterBroker {
                    broker_id: broker.broker_id,
                    host: broker.host,
                    port: broker.port,
                    rack: broker.rack,
                })
                .collect(),
        })
    }

    /// The configuration in force on a topic or on the broker answering.
    ///
    /// `resource_type` is `"topic"` or `"broker"`. An empty `config_names`
    /// asks for everything, which is the useful default: the configs that
    /// are *not* set are exactly the ones a restart can move.
    pub async fn describe_configs(
        &self,
        resource_type: &str,
        resource_name: &str,
        config_names: &[String],
    ) -> Result<Vec<ResourceConfig>, ClientError> {
        let body = DescribeConfigsRequest {
            resource_type: resource_type.to_owned(),
            resource_name: resource_name.to_owned(),
            config_names: config_names.to_vec(),
        }
        .encode()
        .map_err(msg_err)?;
        let bytes = self
            .router
            .request_seed(ApiKey::DescribeConfigs, &body)
            .await?;
        let response = DescribeConfigsResponse::decode(&bytes).map_err(msg_err)?;
        ClientError::from_error_code(response.error_code)?;
        Ok(response
            .configs
            .into_iter()
            .map(|entry| ResourceConfig {
                name: entry.name,
                value: entry.value,
                is_default: entry.is_default,
            })
            .collect())
    }

    /// Disk usage per partition, asked of every broker in the cluster.
    ///
    /// A fan-out because a data directory is a property of one broker: the
    /// question "which topic filled the disk" has a different answer on
    /// each of them, and only the union tells an operator where to look.
    /// Brokers that cannot be reached are returned separately rather than
    /// failing the sweep — a broker being down is often the reason for
    /// asking.
    pub async fn describe_log_dirs(
        &self,
        topics: &[String],
    ) -> Result<(Vec<LogDirUsage>, Vec<(i32, String)>), ClientError> {
        let body = DescribeLogDirsRequest {
            topics: topics.to_vec(),
        }
        .encode()
        .map_err(msg_err)?;
        let responses = self
            .router
            .request_every_broker(ApiKey::DescribeLogDirs, &body)
            .await?;

        let mut dirs = Vec::new();
        let mut unreachable = Vec::new();
        for (broker_id, response) in responses {
            let bytes = match response {
                Ok(bytes) => bytes,
                Err(error) => {
                    unreachable.push((broker_id, error.to_string()));
                    continue;
                }
            };
            let decoded = match DescribeLogDirsResponse::decode(&bytes) {
                Ok(decoded) => decoded,
                Err(error) => {
                    unreachable.push((broker_id, error.to_string()));
                    continue;
                }
            };
            if let Err(error) = ClientError::from_error_code(decoded.error_code) {
                unreachable.push((broker_id, error.to_string()));
                continue;
            }
            for dir in decoded.log_dirs {
                dirs.push(LogDirUsage {
                    broker_id,
                    log_dir: dir.log_dir,
                    error_code: dir.error_code,
                    offline_reason: dir.offline_reason,
                    total_bytes: dir.total_bytes,
                    usable_bytes: dir.usable_bytes,
                    partitions: dir
                        .partitions
                        .into_iter()
                        .map(|partition| LogDirPartitionUsage {
                            topic: partition.topic,
                            partition: partition.partition,
                            size_bytes: partition.size_bytes,
                            offset_lag: partition.offset_lag,
                            is_leader: partition.is_leader,
                        })
                        .collect(),
                });
            }
        }
        Ok((dirs, unreachable))
    }

    /// Delete every record below `offset` on each partition given.
    ///
    /// `-1` means "everything committed". Each request goes to the
    /// partition's leader — a follower cannot trim its own log without the
    /// replicas disagreeing about where the log starts — so partitions are
    /// grouped by leader rather than sent as one request.
    pub async fn delete_records(
        &self,
        targets: &[(String, i32, i64)],
    ) -> Result<Vec<DeletedRecords>, ClientError> {
        let mut by_partition: BTreeMap<(String, i32), i64> = BTreeMap::new();
        for (topic, partition, offset) in targets {
            by_partition.insert((topic.clone(), *partition), *offset);
        }

        let mut deleted = Vec::new();
        for ((topic, partition), offset) in by_partition {
            let body = DeleteRecordsRequest {
                timeout_ms: 30_000,
                partitions: vec![DeleteRecordsPartition {
                    topic: topic.clone(),
                    partition,
                    offset,
                }],
            }
            .encode()
            .map_err(msg_err)?;
            let bytes = self
                .router
                .request_partition(&topic, partition, ApiKey::DeleteRecords, &body)
                .await?;
            let response = DeleteRecordsResponse::decode(&bytes).map_err(msg_err)?;
            for result in response.results {
                ClientError::from_error_code(result.error_code)?;
                deleted.push(DeletedRecords {
                    topic: result.topic,
                    partition: result.partition,
                    low_watermark: result.low_watermark,
                });
            }
        }
        Ok(deleted)
    }

    /// Partition ids of `topic`, for callers that want to sweep them all.
    pub async fn partitions(&self, topic: &str) -> Result<Vec<i32>, ClientError> {
        self.router.partitions(topic).await
    }
}
