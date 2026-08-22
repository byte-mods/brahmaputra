//! Brahmaputra async client: producer, consumer, metadata helpers.
//!
//! One multiplexed TCP connection per client: requests carry a correlation
//! id and responses are matched out of order, with a bounded number of
//! in-flight requests (DESIGN.md §8 backpressure). The producer batches
//! records per partition (`batch.size` / `linger.ms`), like Kafka's.

mod admin;
mod connection;
mod consumer;
mod error;
mod group_admin;
mod group_consumer;
mod producer;
mod quic;
mod replica;
mod router;
mod tls;
mod transactional;
mod transport;

pub use admin::{
    Admin, ClusterBroker, ClusterDescription, DeletedRecords, LogDirPartitionUsage, LogDirUsage,
    ResourceConfig,
};
pub use connection::TcpConnection;
pub use consumer::{BrokerApiVersions, Consumer, FetchedRecord, EARLIEST, LATEST};
pub use error::ClientError;
pub use group_admin::{
    GroupAdmin, GroupDescription, GroupListing, GroupListingReport, GroupMember, PartitionLag,
};
pub use group_consumer::{Assignor, AutoOffsetReset, ConsumedRecord, GroupConsumer};
pub use producer::{Producer, ProducerConfig};
pub use quic::tune_for_datacenter as tune_quic_transport;
pub use replica::{ReplicaClient, ReplicaFetchResult};
pub use tls::TlsSettings;
pub use transactional::{TransactionalProducer, DEFAULT_TRANSACTION_TIMEOUT_MS};
pub use transport::{Connection, Credentials, Transport, TransportConfig};

pub use brahmaputra_protocol::gen::{BrokerInfo, MetadataResponse, PartitionInfo, TopicInfo};
pub use brahmaputra_protocol::IsolationLevel;
