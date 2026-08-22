//! Consumer-group observability (M4, Blueprint 05 §6): list the groups a
//! cluster coordinates, describe one group's membership and committed
//! offsets, and derive per-partition lag.
//!
//! Listing is a fan-out: every broker answers for the `__consumer_offsets`
//! partitions it leads, so the cluster-wide list is the union of the
//! per-broker answers. Describing a group goes to that group's coordinator
//! alone. Lag is `log_end_offset - committed_offset`, computed here against
//! ListOffsets on each partition's leader — the coordinator holds committed
//! offsets but not other topics' log ends.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use brahmaputra_protocol::gen::{
    DescribeGroupRequest, DescribeGroupResponse, ListGroupsRequest, ListGroupsResponse,
};
use brahmaputra_protocol::ApiKey;

use crate::consumer::{Consumer, LATEST};
use crate::error::ClientError;
use crate::group_consumer::{msg_err, GroupCoordinator};
use crate::router::BrokerRouter;
use crate::transport::{Transport, TransportConfig};

/// One group in a cluster-wide listing.
#[derive(Debug, Clone)]
pub struct GroupListing {
    pub group_id: String,
    pub state: String,
    pub generation: i32,
    pub member_count: i32,
    /// `__consumer_offsets` partition coordinating the group.
    pub coordinator_partition: i32,
    /// Broker leading that coordinator partition.
    pub coordinator_broker: i32,
}

/// Result of a listing sweep: what was found, and which brokers could not
/// be reached (their groups are missing from `groups`).
#[derive(Debug, Clone, Default)]
pub struct GroupListingReport {
    pub groups: Vec<GroupListing>,
    pub unreachable: Vec<(i32, String)>,
}

/// One member of a described group.
#[derive(Debug, Clone)]
pub struct GroupMember {
    pub member_id: String,
    pub subscription_topics: Vec<String>,
    pub assignment: Vec<(String, i32)>,
}

/// A group as its coordinator sees it.
#[derive(Debug, Clone)]
pub struct GroupDescription {
    pub group_id: String,
    pub state: String,
    pub generation: i32,
    pub leader_member_id: String,
    pub coordinator_partition: i32,
    pub members: Vec<GroupMember>,
    /// Committed offsets, sorted by (topic, partition).
    pub offsets: Vec<(String, i32, i64)>,
}

/// Lag for one topic-partition of a group.
#[derive(Debug, Clone)]
pub struct PartitionLag {
    pub topic: String,
    pub partition: i32,
    /// `None` when the group has never committed this partition.
    pub committed_offset: Option<i64>,
    pub log_end_offset: i64,
    /// `log_end_offset - committed_offset`; `None` without a commit.
    pub lag: Option<i64>,
    /// Member currently owning the partition, if any.
    pub member_id: Option<String>,
}

/// Read-only view over consumer-group state.
pub struct GroupAdmin {
    router: BrokerRouter,
    consumer: Consumer,
}

impl GroupAdmin {
    pub async fn connect(addr: SocketAddr, client_id: &str) -> Result<GroupAdmin, ClientError> {
        GroupAdmin::connect_with(Transport::default(), addr, client_id).await
    }

    /// Connect over an explicit transport (must match the broker's).
    pub async fn connect_with(
        transport: impl Into<TransportConfig>,
        addr: SocketAddr,
        client_id: &str,
    ) -> Result<GroupAdmin, ClientError> {
        let router =
            BrokerRouter::connect_with(transport, addr, Some(client_id.to_owned()), 5).await?;
        let consumer = Consumer::from_router(router.clone(), 1);
        Ok(GroupAdmin { router, consumer })
    }

    /// Every group the cluster coordinates, sorted by group id. `states`
    /// filters by group state (empty = all).
    pub async fn list_groups(&self, states: &[String]) -> Result<GroupListingReport, ClientError> {
        let request = ListGroupsRequest {
            states: states.to_vec(),
        };
        let body = request.encode().map_err(msg_err)?;
        let responses = self
            .router
            .request_every_broker(ApiKey::ListGroups, &body)
            .await?;

        let mut report = GroupListingReport::default();
        for (broker_id, response) in responses {
            let bytes = match response {
                Ok(bytes) => bytes,
                Err(error) => {
                    report.unreachable.push((broker_id, error.to_string()));
                    continue;
                }
            };
            let listed = match ListGroupsResponse::decode(&bytes) {
                Ok(listed) => listed,
                Err(error) => {
                    report.unreachable.push((broker_id, error.to_string()));
                    continue;
                }
            };
            if let Err(error) = ClientError::from_error_code(listed.error_code) {
                report.unreachable.push((broker_id, error.to_string()));
                continue;
            }
            for group in listed.groups {
                report.groups.push(GroupListing {
                    group_id: group.group_id,
                    state: group.state,
                    generation: group.generation,
                    member_count: group.member_count,
                    coordinator_partition: group.coordinator_partition,
                    coordinator_broker: broker_id,
                });
            }
        }
        report.groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        Ok(report)
    }

    /// Describe one group at its coordinator.
    pub async fn describe_group(&self, group_id: &str) -> Result<GroupDescription, ClientError> {
        let coordinator = GroupCoordinator {
            router: self.router.clone(),
            group_id: group_id.to_owned(),
        };
        let request = DescribeGroupRequest {
            group_id: group_id.to_owned(),
        };
        let body = request.encode().map_err(msg_err)?;
        let response = coordinator
            .request(
                ApiKey::DescribeGroup,
                &body,
                |bytes| DescribeGroupResponse::decode(bytes).map_err(msg_err),
                |response| response.error_code,
            )
            .await?;
        Ok(GroupDescription {
            group_id: response.group_id,
            state: response.state,
            generation: response.generation,
            leader_member_id: response.leader_member_id,
            coordinator_partition: response.coordinator_partition,
            members: response
                .members
                .into_iter()
                .map(|member| GroupMember {
                    member_id: member.member_id,
                    subscription_topics: member.subscription_topics,
                    assignment: member
                        .assignment
                        .into_iter()
                        .map(|assigned| (assigned.topic, assigned.partition))
                        .collect(),
                })
                .collect(),
            offsets: response
                .offsets
                .into_iter()
                .map(|entry| (entry.topic, entry.partition, entry.offset))
                .collect(),
        })
    }

    /// Per-partition lag for a group: `log_end_offset - committed_offset`
    /// over the union of committed and currently assigned partitions.
    pub async fn group_lag(&self, group_id: &str) -> Result<Vec<PartitionLag>, ClientError> {
        let description = self.describe_group(group_id).await?;

        let mut owners: BTreeMap<(String, i32), String> = BTreeMap::new();
        for member in &description.members {
            for (topic, partition) in &member.assignment {
                owners.insert((topic.clone(), *partition), member.member_id.clone());
            }
        }
        let mut committed: BTreeMap<(String, i32), i64> = BTreeMap::new();
        for (topic, partition, offset) in &description.offsets {
            if *offset >= 0 {
                committed.insert((topic.clone(), *partition), *offset);
            }
        }

        let mut partitions: Vec<(String, i32)> = owners.keys().cloned().collect();
        partitions.extend(committed.keys().cloned());
        partitions.sort();
        partitions.dedup();

        let mut lags = Vec::with_capacity(partitions.len());
        for (topic, partition) in partitions {
            let log_end_offset = self
                .consumer
                .list_offsets(&topic, partition, LATEST)
                .await?;
            let committed_offset = committed.get(&(topic.clone(), partition)).copied();
            lags.push(PartitionLag {
                lag: committed_offset.map(|offset| (log_end_offset - offset).max(0)),
                member_id: owners.get(&(topic.clone(), partition)).cloned(),
                topic,
                partition,
                committed_offset,
                log_end_offset,
            });
        }
        Ok(lags)
    }
}
