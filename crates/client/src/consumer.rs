//! Pull consumer with long polling (Blueprint 02 §5).

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use brahmaputra_protocol::codec;
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{
    ApiVersionsRequest, ApiVersionsResponse, FetchMultiPartition, FetchMultiRequest,
    FetchMultiResponse, FetchRequest, FetchResponse, ForgottenPartition, ListOffsetsRequest,
};
use brahmaputra_protocol::{ApiKey, IsolationLevel, ProtocolError, RecordBatch, RecordHeader};
use bytes::Bytes;

use crate::error::ClientError;
use crate::router::{message_error, BrokerRouter};
use crate::transport::{Transport, TransportConfig};

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
    /// What a fetch is allowed to see. `ReadCommitted` bounds it at the
    /// last stable offset and skips aborted records.
    isolation_level: IsolationLevel,
    /// This consumer's own failure domain (`client.rack`), empty when it
    /// has none.
    rack: String,
    /// Per partition, the broker the leader last told this consumer to read
    /// from instead of itself (KIP-392).
    ///
    /// Held here rather than in the router because it is a property of this
    /// consumer's location, not of the cluster: two consumers in different
    /// racks reading the same partition are correctly sent to different
    /// replicas, and a router shared between them must not average that
    /// into one answer.
    preferred_replicas: Arc<Mutex<HashMap<(String, i32), i32>>>,
    /// Per broker, the incremental fetch session this consumer holds with
    /// it (KIP-227).
    sessions: Arc<Mutex<HashMap<SocketAddr, FetchSessionState>>>,
}

impl Consumer {
    pub async fn connect(addr: SocketAddr, client_id: &str) -> Result<Consumer, ClientError> {
        Consumer::connect_with(Transport::default(), addr, client_id).await
    }

    /// Read only what has been committed.
    ///
    /// On a topic nobody writes transactionally to this changes nothing —
    /// the last stable offset and the high watermark are the same place —
    /// which is why it is opt-in rather than the default.
    pub fn with_isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = isolation_level;
        self
    }

    /// Connect over an explicit transport (must match the broker's).
    pub async fn connect_with(
        transport: impl Into<TransportConfig>,
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
            isolation_level: IsolationLevel::default(),
            rack: String::new(),
            preferred_replicas: Arc::new(Mutex::new(HashMap::new())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Where this consumer is running (`client.rack`).
    ///
    /// Setting it lets the leader name a replica in the same rack to read
    /// from instead of itself, which is the difference between every fetch
    /// crossing an availability-zone boundary and none of them doing so.
    /// The replica named is always one the leader considers in sync, so the
    /// cost is bounded staleness — a follower's high watermark trails the
    /// leader's — and never a gap.
    pub fn with_rack(mut self, rack: impl Into<String>) -> Self {
        self.rack = rack.into();
        self
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
            isolation_level: IsolationLevel::default(),
            rack: String::new(),
            preferred_replicas: Arc::new(Mutex::new(HashMap::new())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Fetch records starting at `offset`, waiting up to `max_wait_ms` for
    /// data when the partition is caught up. Offsets are contiguous per
    /// partition.
    pub async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_wait_ms: i32,
    ) -> Result<Vec<FetchedRecord>, ClientError> {
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
    ) -> Result<(Vec<FetchedRecord>, i64), ClientError> {
        let req = FetchRequest {
            topic: topic.to_owned(),
            partition,
            fetch_offset: offset,
            max_bytes: self.max_bytes,
            max_wait_ms: max_wait_ms.min(self.max_wait_ms),
            min_bytes: self.min_bytes,
            isolation_level: self.isolation_level.to_wire(),
            rack: self.rack.clone(),
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
            // A control batch is a transaction marker, not data. It occupies
            // an offset — which is why a transactional topic's offsets are
            // not contiguous — but no application ever sees it, at either
            // isolation level.
            if batch.control {
                continue;
            }
            for (record_offset, record) in batch.iter() {
                if record_offset >= offset {
                    records.push(FetchedRecord {
                        offset: record_offset,
                        key: record.key.clone(),
                        value: record.value.clone(),
                        timestamp: record.timestamp(batch.max_timestamp),
                        headers: record.headers.clone(),
                    });
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
        let preferred = self.preferred_replica(topic, partition);
        let response = match preferred {
            Some(broker_id) => {
                self.router
                    .request_broker_or_leader(topic, partition, broker_id, ApiKey::Fetch, body)
                    .await?
            }
            None => {
                self.router
                    .request_partition(topic, partition, ApiKey::Fetch, body)
                    .await?
            }
        };
        let (response, batches) = codec::decode_fetch_response(response)?;
        self.note_preferred_replica(
            topic,
            partition,
            response.error_code,
            response.preferred_read_replica,
        );
        Ok((response, batches))
    }

    fn preferred_replica(&self, topic: &str, partition: i32) -> Option<i32> {
        self.preferred_replicas
            .lock()
            .expect("preferred replicas")
            .get(&(topic.to_owned(), partition))
            .copied()
    }

    /// Remember, or forget, where to read this partition from.
    ///
    /// Forgetting on any error is the important half: a follower that has
    /// fallen out of the ISR, been restarted, or lost the partition answers
    /// with an error rather than data, and a consumer that kept asking it
    /// would stall indefinitely while the leader was serving perfectly
    /// well.
    fn note_preferred_replica(&self, topic: &str, partition: i32, error_code: i32, preferred: i32) {
        let mut replicas = self.preferred_replicas.lock().expect("preferred replicas");
        let key = (topic.to_owned(), partition);
        if error_code != ec::NONE || preferred < 0 {
            replicas.remove(&key);
        } else {
            replicas.insert(key, preferred);
        }
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
    ) -> Result<Vec<(String, i32, Vec<FetchedRecord>)>, ClientError> {
        self.fetch_many(requests, max_wait_ms)
            .await?
            .into_iter()
            .map(|fetched| {
                ClientError::from_error_code(fetched.error_code)?;
                Ok((fetched.topic, fetched.partition, fetched.records))
            })
            .collect()
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
        let overrides = self
            .preferred_replicas
            .lock()
            .expect("preferred replicas")
            .clone();
        let (grouped, unroutable) = self.router.group_by_target(&keys, &overrides).await;

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
            // Incremental fetch (KIP-227): send only what changed since the
            // last fetch to this broker, and let it remember the rest.
            let (session_id, session_epoch, sent_descriptors, forgotten) =
                self.session_request(address, &descriptors);
            let request = FetchMultiRequest {
                max_wait_ms: max_wait_ms.min(self.max_wait_ms),
                min_bytes: self.min_bytes,
                isolation_level: self.isolation_level.to_wire(),
                rack: self.rack.clone(),
                session_id,
                session_epoch,
                partitions: sent_descriptors,
                forgotten,
            };
            let body = request.encode().map_err(message_error)?;
            let response = self
                .router
                .request_address(address, ApiKey::FetchMulti, &body)
                .await?;
            let (mut decoded, mut batches) =
                codec::decode_fetch_multi_response(response).map_err(ClientError::Protocol)?;

            // A session the broker no longer has — evicted, or lost with a
            // restart — costs one round trip, never a stalled consumer:
            // forget it and ask again in full.
            if decoded.error_code == ec::FETCH_SESSION_NOT_FOUND {
                self.forget_session(address);
                let retry = FetchMultiRequest {
                    max_wait_ms: max_wait_ms.min(self.max_wait_ms),
                    min_bytes: self.min_bytes,
                    isolation_level: self.isolation_level.to_wire(),
                    rack: self.rack.clone(),
                    session_id: NEW_SESSION_ID,
                    session_epoch: INITIAL_SESSION_EPOCH,
                    partitions: descriptors.clone(),
                    forgotten: Vec::new(),
                };
                let body = retry.encode().map_err(message_error)?;
                let response = self
                    .router
                    .request_address(address, ApiKey::FetchMulti, &body)
                    .await?;
                (decoded, batches) =
                    codec::decode_fetch_multi_response(response).map_err(ClientError::Protocol)?;
            }
            ClientError::from_error_code(decoded.error_code)?;
            self.note_session(address, &decoded, &descriptors);

            for (result, raw_batches) in decoded.results.iter().zip(batches) {
                let Some(slot) = out
                    .iter_mut()
                    .find(|slot| slot.topic == result.topic && slot.partition == result.partition)
                else {
                    continue;
                };
                slot.high_watermark = result.high_watermark;
                slot.error_code = result.error_code;
                self.note_preferred_replica(
                    &result.topic,
                    result.partition,
                    result.error_code,
                    result.preferred_read_replica,
                );
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
                    // Transaction markers occupy offsets but are not data;
                    // no application sees one, at either isolation level.
                    if batch.control {
                        continue;
                    }
                    let base = batch.base_offset;
                    let max_timestamp = batch.max_timestamp;
                    for (index, record) in batch.records.into_iter().enumerate() {
                        let offset = base + index as i64;
                        // A batch can start before the requested offset;
                        // skip what the caller has already seen.
                        if offset < fetch_offset {
                            continue;
                        }
                        slot.records.push(FetchedRecord {
                            offset,
                            timestamp: record.timestamp(max_timestamp),
                            key: record.key,
                            value: record.value,
                            headers: record.headers,
                        });
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

/// One record as it comes off the wire, with the batch context already
/// resolved away — the caller gets an absolute offset and an absolute
/// timestamp and never has to know a batch was involved.
#[derive(Debug, Clone)]
pub struct FetchedRecord {
    pub offset: i64,
    pub key: Option<Bytes>,
    /// `None` is a tombstone — the producer deleted this key. Delivered
    /// rather than hidden, because on a compacted topic the deletion *is*
    /// the event a consumer needs to see.
    pub value: Option<Bytes>,
    pub timestamp: i64,
    pub headers: Vec<RecordHeader>,
}

/// One partition's slice of a multi-partition fetch.
#[derive(Debug)]
pub(crate) struct PartitionFetch {
    pub topic: String,
    pub partition: i32,
    pub records: Vec<FetchedRecord>,
    pub high_watermark: i64,
    pub error_code: i32,
}

/// `session_id` a client sends to ask for a session it does not have yet.
pub(crate) const NEW_SESSION_ID: i32 = -1;
/// `session_epoch` that opens a session.
pub(crate) const INITIAL_SESSION_EPOCH: i32 = 0;

/// What this consumer last told one broker, so the next fetch can send only
/// the difference.
#[derive(Default)]
struct FetchSessionState {
    session_id: i32,
    session_epoch: i32,
    /// Fetch offset and byte budget per partition, as last sent to this broker.
    sent: HashMap<(String, i32), (i64, i32)>,
}

impl Consumer {
    /// Turn a full descriptor list into an incremental request.
    ///
    /// Returns the session identity to send, the descriptors that actually
    /// have to travel, and the partitions this consumer has stopped
    /// holding. Without a session, everything travels — which is exactly
    /// what the first fetch to a broker does.
    fn session_request(
        &self,
        address: SocketAddr,
        descriptors: &[FetchMultiPartition],
    ) -> (i32, i32, Vec<FetchMultiPartition>, Vec<ForgottenPartition>) {
        let sessions = self.sessions.lock().expect("fetch sessions");
        let Some(state) = sessions.get(&address) else {
            return (
                NEW_SESSION_ID,
                INITIAL_SESSION_EPOCH,
                descriptors.to_vec(),
                Vec::new(),
            );
        };
        let changed: Vec<FetchMultiPartition> = descriptors
            .iter()
            .filter(|descriptor| {
                state
                    .sent
                    .get(&(descriptor.topic.clone(), descriptor.partition))
                    .is_none_or(|previous| {
                        *previous != (descriptor.fetch_offset, descriptor.max_bytes)
                    })
            })
            .cloned()
            .collect();
        let held: HashSet<(String, i32)> = descriptors
            .iter()
            .map(|descriptor| (descriptor.topic.clone(), descriptor.partition))
            .collect();
        let forgotten: Vec<ForgottenPartition> = state
            .sent
            .keys()
            .filter(|key| !held.contains(*key))
            .map(|(topic, partition)| ForgottenPartition {
                topic: topic.clone(),
                partition: *partition,
            })
            .collect();
        (state.session_id, state.session_epoch, changed, forgotten)
    }

    /// Remember the session the broker answered with, and what this
    /// consumer has now told it.
    fn note_session(
        &self,
        address: SocketAddr,
        response: &FetchMultiResponse,
        descriptors: &[FetchMultiPartition],
    ) {
        let mut sessions = self.sessions.lock().expect("fetch sessions");
        if response.session_id == 0 || response.error_code != ec::NONE {
            sessions.remove(&address);
            return;
        }
        let state = sessions.entry(address).or_default();
        state.session_id = response.session_id;
        state.session_epoch = response.session_epoch;
        // What the broker now believes, which is what this consumer just
        // sent — including the partitions it did not resend, whose offsets
        // are unchanged by definition.
        let held: HashSet<(String, i32)> = descriptors
            .iter()
            .map(|descriptor| (descriptor.topic.clone(), descriptor.partition))
            .collect();
        state.sent.retain(|key, _| held.contains(key));
        for descriptor in descriptors {
            state.sent.insert(
                (descriptor.topic.clone(), descriptor.partition),
                (descriptor.fetch_offset, descriptor.max_bytes),
            );
        }
    }

    /// Drop a session, so the next fetch to this broker is a full one.
    fn forget_session(&self, address: SocketAddr) {
        self.sessions
            .lock()
            .expect("fetch sessions")
            .remove(&address);
    }
}
