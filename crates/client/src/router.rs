//! Metadata-backed broker connection pool and partition-leader routing.
//!
//! A router starts with one seed address, learns every broker endpoint from
//! Metadata, and lazily opens one multiplexed [`Connection`] per address.
//! Transport failures evict that pooled connection but are never replayed:
//! in particular, a Produce request may have reached the broker before its
//! connection failed. Callers may retry once only after decoding an explicit
//! `NOT_LEADER_OR_FOLLOWER` response, which proves that broker did not append.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{MetadataRequest, MetadataResponse};
use brahmaputra_protocol::{ApiKey, ProtocolError};
use bytes::Bytes;
use tokio::net::lookup_host;
use tokio::sync::Mutex as AsyncMutex;

use crate::{ClientError, Connection, Transport};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BrokerEndpoint {
    host: String,
    port: u16,
}

impl BrokerEndpoint {
    fn from_wire(host: String, port: i32) -> Result<Self, ClientError> {
        let port = u16::try_from(port).map_err(|_| {
            ClientError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("broker {host:?} advertised invalid port {port}"),
            ))
        })?;
        if port == 0 {
            return Err(ClientError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("broker {host:?} advertised port 0"),
            )));
        }
        Ok(Self { host, port })
    }

    async fn resolve(&self) -> Result<SocketAddr, ClientError> {
        if let Ok(ip) = self.host.parse::<IpAddr>() {
            return Ok(SocketAddr::new(ip, self.port));
        }
        lookup_host((self.host.as_str(), self.port))
            .await?
            .next()
            .ok_or_else(|| {
                ClientError::Io(io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    format!("broker host {:?} resolved to no addresses", self.host),
                ))
            })
    }
}

#[derive(Default)]
struct TopicRoute {
    partitions: BTreeMap<i32, i32>,
}

#[derive(Default)]
struct RoutingTable {
    brokers: HashMap<i32, BrokerEndpoint>,
    topics: HashMap<String, TopicRoute>,
}

impl RoutingTable {
    fn update(&mut self, metadata: &MetadataResponse) -> Result<(), ClientError> {
        let mut brokers = HashMap::with_capacity(metadata.brokers.len());
        for broker in &metadata.brokers {
            brokers.insert(
                broker.broker_id,
                BrokerEndpoint::from_wire(broker.host.clone(), broker.port)?,
            );
        }
        self.brokers = brokers;

        for topic in &metadata.topics {
            if topic.error_code != ec::NONE {
                self.topics.remove(&topic.name);
                continue;
            }
            self.topics.insert(
                topic.name.clone(),
                TopicRoute {
                    partitions: topic
                        .partitions
                        .iter()
                        .map(|partition| (partition.partition, partition.leader))
                        .collect(),
                },
            );
        }
        Ok(())
    }
}

#[derive(Clone)]
struct PooledConnection {
    address: SocketAddr,
    generation: u64,
    connection: Connection,
}

struct Inner {
    seed: SocketAddr,
    client_id: Option<String>,
    max_in_flight: usize,
    transport: Transport,
    next_generation: AtomicU64,
    connections: Mutex<HashMap<SocketAddr, PooledConnection>>,
    routes: Mutex<RoutingTable>,
    refresh: AsyncMutex<()>,
}

/// Cheaply cloned Metadata cache plus lazy broker connection pool.
#[derive(Clone)]
pub(crate) struct BrokerRouter {
    inner: Arc<Inner>,
}

impl BrokerRouter {
    /// Connect over an explicit transport; every later connection this
    /// router opens to any broker uses the same one.
    pub(crate) async fn connect_with(
        transport: Transport,
        seed: SocketAddr,
        client_id: Option<String>,
        max_in_flight: usize,
    ) -> Result<Self, ClientError> {
        let connection =
            Connection::connect_with(transport, seed, client_id.clone(), max_in_flight).await?;
        let pooled = PooledConnection {
            address: seed,
            generation: 0,
            connection,
        };
        Ok(Self {
            inner: Arc::new(Inner {
                seed,
                client_id,
                max_in_flight,
                transport,
                next_generation: AtomicU64::new(1),
                connections: Mutex::new(HashMap::from([(seed, pooled)])),
                routes: Mutex::new(RoutingTable::default()),
                refresh: AsyncMutex::new(()),
            }),
        })
    }

    /// Fetch Metadata from a live seed/cached broker and publish its routes.
    pub(crate) async fn metadata(
        &self,
        topics: &[String],
    ) -> Result<MetadataResponse, ClientError> {
        let _refresh = self.inner.refresh.lock().await;
        let response = self.fetch_metadata(topics).await?;
        self.inner
            .routes
            .lock()
            .expect("routes")
            .update(&response)?;
        Ok(response)
    }

    /// Force one topic's route to be refreshed after an explicit stale-leader
    /// response. This method itself never replays the original operation.
    pub(crate) async fn refresh_topic(&self, topic: &str) -> Result<(), ClientError> {
        let topics = [topic.to_owned()];
        let response = self.metadata(&topics).await?;
        let topic = response
            .topics
            .iter()
            .find(|candidate| candidate.name == topics[0])
            .ok_or_else(|| unknown_partition(&topics[0], -1))?;
        ClientError::from_error_code(topic.error_code)
    }

    /// Return known partition ids, fetching the topic route on first use.
    pub(crate) async fn partitions(&self, topic: &str) -> Result<Vec<i32>, ClientError> {
        if let Some(partitions) = self.cached_partitions(topic) {
            return Ok(partitions);
        }
        let topics = [topic.to_owned()];
        let response = self.metadata(&topics).await?;
        validate_topic_response(&response, topic)?;
        self.cached_partitions(topic)
            .ok_or_else(|| unknown_partition(topic, -1))
    }

    /// Send an acks=0 operation exactly once to the cached partition leader.
    pub(crate) async fn send_partition(
        &self,
        topic: &str,
        partition: i32,
        api_key: ApiKey,
        body: &[u8],
    ) -> Result<(), ClientError> {
        let pooled = self.partition_connection(topic, partition).await?;
        match pooled.connection.send(api_key, body).await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.invalidate(&pooled);
                Err(error)
            }
        }
    }

    /// Issue one request to the cached partition leader. A connection failure
    /// is returned after evicting the connection and is deliberately not
    /// retried because the request's outcome may be ambiguous.
    pub(crate) async fn request_partition(
        &self,
        topic: &str,
        partition: i32,
        api_key: ApiKey,
        body: &[u8],
    ) -> Result<Bytes, ClientError> {
        let pooled = self.partition_connection(topic, partition).await?;
        match pooled.connection.request(api_key, body).await {
            Ok(response) => Ok(response),
            Err(error) => {
                self.invalidate(&pooled);
                Err(error)
            }
        }
    }

    /// Issue one request to every known broker, in broker-id order, after
    /// refreshing metadata. Used by cluster-wide reads such as ListGroups,
    /// where each broker answers only for what it owns; per-broker failures
    /// are returned rather than aborting the sweep.
    pub(crate) async fn request_every_broker(
        &self,
        api_key: ApiKey,
        body: &[u8],
    ) -> Result<Vec<(i32, Result<Bytes, ClientError>)>, ClientError> {
        self.metadata(&[]).await?;
        let endpoints: BTreeMap<i32, BrokerEndpoint> = self
            .inner
            .routes
            .lock()
            .expect("routes")
            .brokers
            .iter()
            .map(|(broker_id, endpoint)| (*broker_id, endpoint.clone()))
            .collect();

        let mut responses = Vec::with_capacity(endpoints.len());
        for (broker_id, endpoint) in endpoints {
            let address = match endpoint.resolve().await {
                Ok(address) => address,
                Err(error) => {
                    responses.push((broker_id, Err(error)));
                    continue;
                }
            };
            let pooled = match self.connection(address).await {
                Ok(pooled) => pooled,
                Err(error) => {
                    responses.push((broker_id, Err(error)));
                    continue;
                }
            };
            let response = match pooled.connection.request(api_key, body).await {
                Ok(response) => Ok(response),
                Err(error) => {
                    self.invalidate(&pooled);
                    Err(error)
                }
            };
            responses.push((broker_id, response));
        }
        Ok(responses)
    }

    /// Issue one non-partitioned request to the original seed. The request is
    /// never replayed: callers such as InitProducerId must treat a lost
    /// response as ambiguous rather than accidentally allocating twice.
    pub(crate) async fn request_seed(
        &self,
        api_key: ApiKey,
        body: &[u8],
    ) -> Result<Bytes, ClientError> {
        let pooled = self.connection(self.inner.seed).await?;
        match pooled.connection.request(api_key, body).await {
            Ok(response) => Ok(response),
            Err(error) => {
                self.invalidate(&pooled);
                Err(error)
            }
        }
    }

    async fn partition_connection(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<PooledConnection, ClientError> {
        let endpoint = match self.cached_endpoint(topic, partition) {
            Some(endpoint) => endpoint,
            None => {
                let topics = [topic.to_owned()];
                let response = self.metadata(&topics).await?;
                validate_topic_response(&response, topic)?;
                self.cached_endpoint(topic, partition)
                    .ok_or_else(|| unknown_partition(topic, partition))?
            }
        };
        let address = endpoint.resolve().await?;
        self.connection(address).await
    }

    fn cached_endpoint(&self, topic: &str, partition: i32) -> Option<BrokerEndpoint> {
        let routes = self.inner.routes.lock().expect("routes");
        let broker_id = routes.topics.get(topic)?.partitions.get(&partition)?;
        routes.brokers.get(broker_id).cloned()
    }

    fn cached_partitions(&self, topic: &str) -> Option<Vec<i32>> {
        self.inner
            .routes
            .lock()
            .expect("routes")
            .topics
            .get(topic)
            .map(|route| route.partitions.keys().copied().collect::<Vec<_>>())
            .filter(|partitions| !partitions.is_empty())
    }

    async fn fetch_metadata(&self, topics: &[String]) -> Result<MetadataResponse, ClientError> {
        let request = MetadataRequest {
            topics: topics.to_vec(),
        };
        let body = request.encode().map_err(message_error)?;
        let candidates = self.metadata_candidates().await;
        let mut last_error = None;

        for address in candidates {
            let pooled = match self.connection(address).await {
                Ok(pooled) => pooled,
                Err(error) => {
                    last_error = Some(error);
                    continue;
                }
            };
            match pooled.connection.request(ApiKey::Metadata, &body).await {
                Ok(response) => {
                    return MetadataResponse::decode(&response).map_err(message_error);
                }
                Err(error) => {
                    self.invalidate(&pooled);
                    last_error = Some(error);
                }
            }
        }

        Err(last_error.unwrap_or(ClientError::ConnectionClosed))
    }

    async fn metadata_candidates(&self) -> Vec<SocketAddr> {
        let endpoints: Vec<_> = self
            .inner
            .routes
            .lock()
            .expect("routes")
            .brokers
            .values()
            .cloned()
            .collect();
        let mut seen = HashSet::from([self.inner.seed]);
        let mut candidates = vec![self.inner.seed];
        for endpoint in endpoints {
            if let Ok(address) = endpoint.resolve().await {
                if seen.insert(address) {
                    candidates.push(address);
                }
            }
        }
        candidates
    }

    async fn connection(&self, address: SocketAddr) -> Result<PooledConnection, ClientError> {
        if let Some(connection) = self
            .inner
            .connections
            .lock()
            .expect("connections")
            .get(&address)
            .cloned()
        {
            return Ok(connection);
        }

        let connection = Connection::connect_with(
            self.inner.transport,
            address,
            self.inner.client_id.clone(),
            self.inner.max_in_flight,
        )
        .await?;
        let candidate = PooledConnection {
            address,
            generation: self.inner.next_generation.fetch_add(1, Ordering::Relaxed),
            connection,
        };
        let mut connections = self.inner.connections.lock().expect("connections");
        Ok(connections
            .entry(address)
            .or_insert_with(|| candidate)
            .clone())
    }

    fn invalidate(&self, failed: &PooledConnection) {
        let mut connections = self.inner.connections.lock().expect("connections");
        if connections
            .get(&failed.address)
            .is_some_and(|current| current.generation == failed.generation)
        {
            connections.remove(&failed.address);
        }
    }
}

fn validate_topic_response(metadata: &MetadataResponse, topic: &str) -> Result<(), ClientError> {
    let topic = metadata
        .topics
        .iter()
        .find(|candidate| candidate.name == topic)
        .ok_or_else(|| unknown_partition(topic, -1))?;
    ClientError::from_error_code(topic.error_code)
}

fn unknown_partition(topic: &str, partition: i32) -> ClientError {
    ClientError::Server {
        code: ec::UNKNOWN_TOPIC_OR_PARTITION,
        message: if partition < 0 {
            format!("unknown topic {topic:?}")
        } else {
            format!("unknown topic-partition {topic:?}-{partition}")
        },
    }
}

pub(crate) fn message_error(error: io::Error) -> ClientError {
    ClientError::Protocol(ProtocolError::Message(error.to_string()))
}

impl BrokerRouter {
    /// Group topic-partitions by the broker that leads them.
    ///
    /// This is what turns N per-partition requests into one request per
    /// broker: the caller batches everything that shares a destination.
    /// Partitions with no known leader are returned separately so the
    /// caller can fail them explicitly instead of silently dropping them.
    pub(crate) async fn group_by_leader(
        &self,
        partitions: &[(String, i32)],
    ) -> (Vec<(SocketAddr, Vec<(String, i32)>)>, Vec<(String, i32)>) {
        let mut unknown_topics: HashSet<String> = HashSet::new();
        for (topic, partition) in partitions {
            if self.cached_endpoint(topic, *partition).is_none() {
                unknown_topics.insert(topic.clone());
            }
        }
        if !unknown_topics.is_empty() {
            let topics: Vec<String> = unknown_topics.into_iter().collect();
            let _ = self.metadata(&topics).await;
        }

        let mut grouped: BTreeMap<SocketAddr, Vec<(String, i32)>> = BTreeMap::new();
        let mut unroutable = Vec::new();
        for (topic, partition) in partitions {
            let Some(endpoint) = self.cached_endpoint(topic, *partition) else {
                unroutable.push((topic.clone(), *partition));
                continue;
            };
            match endpoint.resolve().await {
                Ok(address) => grouped
                    .entry(address)
                    .or_default()
                    .push((topic.clone(), *partition)),
                Err(_) => unroutable.push((topic.clone(), *partition)),
            }
        }
        (grouped.into_iter().collect(), unroutable)
    }

    /// Issue one request to a specific broker address, evicting the pooled
    /// connection on transport failure (never replaying it).
    pub(crate) async fn request_address(
        &self,
        address: SocketAddr,
        api_key: ApiKey,
        body: &[u8],
    ) -> Result<Bytes, ClientError> {
        let pooled = self.connection(address).await?;
        match pooled.connection.request(api_key, body).await {
            Ok(response) => Ok(response),
            Err(error) => {
                self.invalidate(&pooled);
                Err(error)
            }
        }
    }

    /// Fire-and-forget to a specific broker address (`acks=0`).
    pub(crate) async fn send_address(
        &self,
        address: SocketAddr,
        api_key: ApiKey,
        body: &[u8],
    ) -> Result<(), ClientError> {
        let pooled = self.connection(address).await?;
        match pooled.connection.send(api_key, body).await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.invalidate(&pooled);
                Err(error)
            }
        }
    }
}
