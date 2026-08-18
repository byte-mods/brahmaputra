//! Pull consumer with long polling (Blueprint 02 §5).

use std::net::SocketAddr;

use brahmaputra_protocol::codec;
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{
    ApiVersionsRequest, ApiVersionsResponse, FetchMultiPartition, FetchMultiRequest, FetchRequest,
    FetchResponse, ListOffsetsRequest,
};
use brahmaputra_protocol::{ApiKey, ProtocolError, RecordBatch};
use bytes::Bytes;

use crate::error::ClientError;
use crate::router::{message_error, BrokerRouter};
use crate::transport::Transport;

/// `timestamp` sentinel for [`Consumer::list_offsets`]: earliest offset.
pub const EARLIEST: i64 = -2;
/// `timestamp` sentinel for [`Consumer::list_offsets`]: latest offset (log end).
pub const LATEST: i64 = -1;

/// A pull consumer over one multiplexed connection.
pub struct Consumer {
    router: BrokerRouter,
    max_bytes: i32,
    min_bytes: i32,
    max_wait_ms: i32,
}

impl Consumer {
    pub async fn connect(addr: SocketAddr, client_id: &str) -> Result<Consumer, ClientError> {
        Consumer::connect_with(Transport::default(), addr, client_id).await
    }

    /// Connect over an explicit transport (must match the broker's).
    pub async fn connect_with(
        transport: Transport,
        addr: SocketAddr,
        client_id: &str,
    ) -> Result<Consumer, ClientError> {
        let router =
            BrokerRouter::connect_with(transport, addr, Some(client_id.to_owned()), 5).await?;
        Ok(Consumer {
            router,
            max_bytes: 8 * 1024 * 1024,
            min_bytes: 1,
            max_wait_ms: 500,
        })
    }

    /// Cap on response batch bytes per fetch.
    pub fn with_max_bytes(mut self, max_bytes: i32) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// Minimum bytes the broker gathers before answering a fetch
    /// (`fetch.min.bytes`, default 1).
    pub fn with_fetch_min_bytes(mut self, min_bytes: i32) -> Self {
        self.min_bytes = min_bytes;
        self
    }

    /// Server-side cap on how long a fetch waits for data
    /// (`fetch.max.wait.ms`, default 500); per-call `max_wait_ms` is
    /// clamped to it.
    pub fn with_fetch_max_wait_ms(mut self, max_wait_ms: i32) -> Self {
        self.max_wait_ms = max_wait_ms;
        self
    }

    /// Wrap an existing router (used by the group consumer to share one
    /// connection pool and metadata cache with its coordinator requests).
    pub(crate) fn from_router(router: BrokerRouter, max_bytes: i32) -> Consumer {
        Consumer {
            router,
            max_bytes,
            min_bytes: 1,
            max_wait_ms: 500,
        }
    }

    /// Fetch records starting at `offset`, waiting up to `max_wait_ms` for
    /// data when the partition is caught up. Returns
    /// `(offset, key, value)` triples; offsets are contiguous per partition.
    pub async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_wait_ms: i32,
    ) -> Result<Vec<(i64, Option<Bytes>, Bytes)>, ClientError> {
        Ok(self
            .fetch_verbose(topic, partition, offset, max_wait_ms)
            .await?
            .0)
    }

    /// Like [`Consumer::fetch`] but also returns the partition's high
    /// watermark (never-fetchable data is strictly below it).
    pub async fn fetch_verbose(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_wait_ms: i32,
    ) -> Result<(Vec<(i64, Option<Bytes>, Bytes)>, i64), ClientError> {
        let req = FetchRequest {
            topic: topic.to_owned(),
            partition,
            fetch_offset: offset,
            max_bytes: self.max_bytes,
            max_wait_ms: max_wait_ms.min(self.max_wait_ms),
            min_bytes: self.min_bytes,
        };
        let body = Bytes::from(req.encode().map_err(msg_err)?);
        let (mut resp, mut raw_batches) = self.fetch_once(topic, partition, &body).await?;
        if resp.error_code == ec::NOT_LEADER_OR_FOLLOWER {
            self.router.refresh_topic(topic).await?;
            (resp, raw_batches) = self.fetch_once(topic, partition, &body).await?;
        }
        ClientError::from_error_code(resp.error_code)?;

        let mut records = Vec::new();
        for raw in raw_batches {
            let mut buf = raw;
            let batch = RecordBatch::decode(&mut buf)?;
            for (record_offset, record) in batch.iter() {
                if record_offset >= offset {
                    records.push((record_offset, record.key.clone(), record.value.clone()));
                }
            }
        }
        Ok((records, resp.high_watermark))
    }

    /// Resolve `timestamp` ([`EARLIEST`], [`LATEST`] or unix ms) to an offset.
    pub async fn list_offsets(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<i64, ClientError> {
        let req = ListOffsetsRequest {
            topic: topic.to_owned(),
            partition,
            timestamp,
        };
        let body = Bytes::from(req.encode().map_err(msg_err)?);
        let mut resp = self.list_offsets_once(topic, partition, &body).await?;
        if resp.error_code == ec::NOT_LEADER_OR_FOLLOWER {
            self.router.refresh_topic(topic).await?;
            resp = self.list_offsets_once(topic, partition, &body).await?;
        }
        ClientError::from_error_code(resp.error_code)?;
        Ok(resp.offset)
    }

    /// Cluster + topic metadata (empty `topics` = all topics).
    pub async fn metadata(
        &self,
        topics: &[String],
    ) -> Result<brahmaputra_protocol::gen::MetadataResponse, ClientError> {
        self.router.metadata(topics).await
    }

    /// Ask the broker which APIs and versions it speaks.
    ///
    /// This is the one call that works across a version mismatch, so it is
    /// what a client uses to decide whether it can talk to a broker at all
    /// before sending anything that could be rejected.
    pub async fn api_versions(&self) -> Result<BrokerApiVersions, ClientError> {
        let request = ApiVersionsRequest {
            client_software_name: "brahmaputra-client".to_owned(),
            client_software_version: env!("CARGO_PKG_VERSION").to_owned(),
        };
        let body = request.encode().map_err(message_error)?;
        let response = self.router.request_seed(ApiKey::ApiVersions, &body).await?;
        let response = ApiVersionsResponse::decode(&response).map_err(message_error)?;
        ClientError::from_error_code(response.error_code)?;
        let mut api_versions: Vec<(i32, i16, i16)> = response
            .api_versions
            .into_iter()
            .map(|range| {
                (
                    range.api_key,
                    range.min_version as i16,
                    range.max_version as i16,
                )
            })
            .collect();
        api_versions.sort_by_key(|(api_key, _, _)| *api_key);
        Ok(BrokerApiVersions {
            broker_version: response.broker_version,
            api_versions,
        })
    }

    async fn fetch_once(
        &self,
        topic: &str,
        partition: i32,
        body: &[u8],
    ) -> Result<(FetchResponse, Vec<Bytes>), ClientError> {
        let response = self
            .router
            .request_partition(topic, partition, ApiKey::Fetch, body)
            .await?;
        Ok(codec::decode_fetch_response(response)?)
    }

    async fn list_offsets_once(
        &self,
        topic: &str,
        partition: i32,
        body: &[u8],
    ) -> Result<brahmaputra_protocol::gen::ListOffsetsResponse, ClientError> {
        let response = self
            .router
            .request_partition(topic, partition, ApiKey::ListOffsets, body)
            .await?;
        brahmaputra_protocol::gen::ListOffsetsResponse::decode(&response).map_err(msg_err)
    }
}

fn msg_err(e: std::io::Error) -> ClientError {
    ClientError::Protocol(ProtocolError::Message(e.to_string()))
}

/// What a broker says it speaks (`ApiVersions`, api_key 14).
#[derive(Debug, Clone)]
pub struct BrokerApiVersions {
    pub broker_version: String,
    /// `(api_key, min_version, max_version)`, sorted by api key.
    pub api_versions: Vec<(i32, i16, i16)>,
}

impl BrokerApiVersions {
    /// The version range this broker accepts for `api_key`, if it speaks it
    /// at all.
    pub fn range_for(&self, api_key: ApiKey) -> Option<(i16, i16)> {
        self.api_versions
            .iter()
            .find(|(key, _, _)| *key == api_key as i32)
            .map(|(_, min, max)| (*min, *max))
    }

    /// Whether this client's wire version is inside the broker's range for
    /// every API it means to use — the check that makes a rolling upgrade
    /// safe rather than hopeful.
    pub fn supports(&self, api_key: ApiKey, version: i16) -> bool {
        self.range_for(api_key)
            .is_some_and(|(min, max)| version >= min && version <= max)
    }
}

impl Consumer {
    /// Fetch several partitions in one request per broker (api_key 16).
    ///
    /// The group consumer polls its whole assignment this way: six
    /// partitions on one broker become one round trip instead of six, which
    /// is what makes per-poll latency independent of partition count.
    /// Returns one entry per requested partition, in the order given, so a
    /// caller can zip results back onto its positions.
    pub async fn fetch_many_public(
        &self,
        requests: &[(String, i32, i64)],
        max_wait_ms: i32,
    ) -> Result<Vec<(String, i32, Vec<(i64, Option<Bytes>, Bytes)>)>, ClientError> {
        Ok(self
            .fetch_many(requests, max_wait_ms)
            .await?
            .into_iter()
            .map(|fetched| (fetched.topic, fetched.partition, fetched.records))
            .collect())
    }

    pub(crate) async fn fetch_many(
        &self,
        requests: &[(String, i32, i64)],
        max_wait_ms: i32,
    ) -> Result<Vec<PartitionFetch>, ClientError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<(String, i32)> = requests
            .iter()
            .map(|(topic, partition, _)| (topic.clone(), *partition))
            .collect();
        let (grouped, unroutable) = self.router.group_by_leader(&keys).await;

        let mut out: Vec<PartitionFetch> = requests
            .iter()
            .map(|(topic, partition, _)| PartitionFetch {
                topic: topic.clone(),
                partition: *partition,
                records: Vec::new(),
                high_watermark: -1,
                error_code: ec::NONE,
            })
            .collect();
        // A partition with no known leader is reported as such rather than
        // silently returning empty, which would look like "caught up".
        for (topic, partition) in &unroutable {
            if let Some(slot) = out
                .iter_mut()
                .find(|slot| slot.topic == *topic && slot.partition == *partition)
            {
                slot.error_code = ec::NOT_LEADER_OR_FOLLOWER;
            }
        }

        for (address, partitions) in grouped {
            let per_partition_bytes =
                (self.max_bytes / partitions.len().max(1) as i32).max(64 * 1024);
            let descriptors: Vec<FetchMultiPartition> = partitions
                .iter()
                .map(|(topic, partition)| {
                    let offset = requests
                        .iter()
                        .find(|(request_topic, request_partition, _)| {
                            request_topic == topic && request_partition == partition
                        })
                        .map(|(_, _, offset)| *offset)
                        .unwrap_or(0);
                    FetchMultiPartition {
                        topic: topic.clone(),
                        partition: *partition,
                        fetch_offset: offset,
                        // Split the byte allowance across the partitions in
                        // this request: the broker caps the response as a
                        // whole, and asking for the full amount per
                        // partition would just be trimmed server-side.
                        max_bytes: per_partition_bytes,
                    }
                })
                .collect();
            let request = FetchMultiRequest {
                max_wait_ms: max_wait_ms.min(self.max_wait_ms),
                min_bytes: self.min_bytes,
                partitions: descriptors,
            };
            let body = request.encode().map_err(message_error)?;
            let response = self
                .router
                .request_address(address, ApiKey::FetchMulti, &body)
                .await?;
            let (decoded, batches) =
                codec::decode_fetch_multi_response(response).map_err(ClientError::Protocol)?;

            for (result, raw_batches) in decoded.results.iter().zip(batches.into_iter()) {
                let Some(slot) = out
                    .iter_mut()
                    .find(|slot| slot.topic == result.topic && slot.partition == result.partition)
                else {
                    continue;
                };
                slot.high_watermark = result.high_watermark;
                slot.error_code = result.error_code;
                if result.error_code != ec::NONE {
                    continue;
                }
                let fetch_offset = requests
                    .iter()
                    .find(|(topic, partition, _)| {
                        topic == &result.topic && *partition == result.partition
                    })
                    .map(|(_, _, offset)| *offset)
                    .unwrap_or(0);
                for raw in raw_batches {
                    let mut buffer = raw;
                    let batch = RecordBatch::decode(&mut buffer)?;
                    let base = batch.base_offset;
                    for (index, record) in batch.records.into_iter().enumerate() {
                        let offset = base + index as i64;
                        // A batch can start before the requested offset;
                        // skip what the caller has already seen.
                        if offset < fetch_offset {
                            continue;
                        }
                        slot.records.push((offset, record.key, record.value));
                    }
                }
            }
        }
        Ok(out)
    }
}

impl Consumer {
    /// Refresh one topic's routes after a stale-leader response.
    pub(crate) async fn refresh_topic(&self, topic: &str) -> Result<(), ClientError> {
        self.router.refresh_topic(topic).await
    }
}

/// One partition's slice of a multi-partition fetch.
#[derive(Debug)]
pub(crate) struct PartitionFetch {
    pub topic: String,
    pub partition: i32,
    pub records: Vec<(i64, Option<Bytes>, Bytes)>,
    pub high_watermark: i64,
    pub error_code: i32,
}
