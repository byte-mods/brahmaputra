//! Brahmaputra async client: producer, consumer, metadata helpers.
//!
//! One multiplexed TCP connection per client: requests carry a correlation
//! id and responses are matched out of order, with a bounded number of
//! in-flight requests (DESIGN.md §8 backpressure). The producer batches
//! records per partition (`batch.size` / `linger.ms`), like Kafka's.

mod connection;
mod consumer;
mod error;
mod group_admin;
mod group_consumer;
mod producer;
mod quic;
mod replica;
mod router;
mod transport;

pub use connection::TcpConnection;
pub use quic::tune_for_datacenter as tune_quic_transport;
pub use transport::{Connection, Transport};
pub use consumer::{BrokerApiVersions, Consumer, EARLIEST, LATEST};
pub use error::ClientError;
pub use group_admin::{
    GroupAdmin, GroupDescription, GroupListing, GroupListingReport, GroupMember, PartitionLag,
};
pub use group_consumer::{Assignor, ConsumedRecord, GroupConsumer};
pub use producer::{Producer, ProducerConfig};
pub use replica::{ReplicaClient, ReplicaFetchResult};

pub use brahmaputra_protocol::gen::{BrokerInfo, MetadataResponse, PartitionInfo, TopicInfo};
