//! Brahmaputra broker: the single-broker data plane (M1).
//!
//! One TCP listener, one connection task per client, and one **partition
//! actor** per (topic, partition) that owns its `storage::Log` outright —
//! the single-writer principle of DESIGN.md §8: no locks on the append
//! path, the actor's task is the only place a log is touched. Handlers are
//! thin: they validate frames, forward commands to the owning actor over a
//! bounded channel (backpressure), and await a oneshot reply.
//!
//! Topic metadata lives in a small in-memory map persisted to `meta.toml`
//! in the data dir (the Raft controller replaces this from M2 on). Topics
//! are auto-created on first Produce/Metadata request.
//!
//! Async lives here; `storage` and `protocol` stay pure/sync.

mod admin;
mod actor;
mod error;
mod group;
mod handlers;
mod logdirs;
mod multi;
mod producer_id;
mod quic;
mod quota;
mod replication;
mod server;
mod state;
mod tls;
mod transaction;

pub use actor::{PartitionHandle, ReadOutcome};
pub use error::BrokerError;
pub use quota::{QuotaConfig, QuotaKind};
pub use replication::{
    FetcherState, FollowerFetcherHealth, LeaderFollowerHealth, ReplicaManager,
    ReplicaManagerConfig, ReplicaPartition, ReplicationHealthSnapshot,
};
pub use server::{Broker, BrokerConfig};
pub use tls::TlsIdentity;
pub use state::BrokerState;
