use std::io;

use brahmaputra_protocol::ProtocolError;
use brahmaputra_storage::StorageError;
use thiserror::Error;

/// Errors produced by the broker.
#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    /// The requested topic or partition does not exist and could not be
    /// auto-created.
    #[error("unknown topic or partition: {topic}-{partition}")]
    UnknownTopicOrPartition { topic: String, partition: i32 },

    /// Cluster metadata says this broker cannot serve the requested local
    /// operation. Client produce/fetch require the leader; internal replica
    /// access also requires this broker to be in the replica assignment.
    #[error(
        "broker {broker_id} is not the leader or an eligible follower for {topic}-{partition}; current leader is {leader}"
    )]
    NotLeaderOrFollower {
        topic: String,
        partition: i32,
        broker_id: i32,
        leader: i32,
    },

    /// A restarted broker incarnation attempted to use an old epoch.
    #[error("broker {broker_id} epoch {requested} is fenced; current epoch is {current}")]
    FencedBrokerEpoch {
        broker_id: i32,
        requested: u64,
        current: u64,
    },

    /// A follower is talking to a leader assignment it has already missed.
    #[error("leader epoch {requested} is fenced; current epoch is {current}")]
    FencedLeaderEpoch { requested: i32, current: i32 },

    /// A follower requested an epoch the leader does not know yet.
    #[error("leader epoch {requested} is unknown; current epoch is {current}")]
    UnknownLeaderEpoch { requested: i32, current: i32 },

    /// The ISR is too small to satisfy an all-replica operation.
    #[error("not enough in-sync replicas: required {required}, available {available}")]
    NotEnoughReplicas { required: usize, available: usize },

    /// Topic name is empty or contains characters outside `[a-zA-Z0-9._-]`.
    #[error("invalid topic name: {0:?}")]
    InvalidTopic(String),

    /// The partition actor is gone (broker shutting down or actor crashed).
    #[error("partition actor unavailable: {0}")]
    ActorUnavailable(String),

    /// This broker does not lead the group's `__consumer_offsets` partition.
    #[error("broker is not the coordinator for group {group_id:?}")]
    NotCoordinator { group_id: String },

    /// The member is unknown to the group coordinator (evicted or never
    /// joined); it must rejoin.
    #[error("unknown member {member_id:?} in group {group_id:?}")]
    UnknownMemberId { group_id: String, member_id: String },

    /// The request carries a generation older than the group's current one.
    #[error("illegal generation {requested} for group {group_id:?}; current is {current}")]
    IllegalGeneration {
        group_id: String,
        requested: i32,
        current: i32,
    },

    /// The group is rebalancing; heartbeats and syncs must wait for the new
    /// generation.
    #[error("group {group_id:?} is rebalancing")]
    RebalanceInProgress { group_id: String },

    /// The coordinator shard is still replaying its offsets log.
    #[error("coordinator for __consumer_offsets partition {partition} is loading")]
    CoordinatorLoadInProgress { partition: i32 },

    #[error("invalid meta file: {0}")]
    Meta(String),
}
