//! Deterministic cluster-metadata state machine.
//!
//! The controller replicates [`MetadataCommand`] values through its Raft
//! log. Applying the same ordered commands always produces the same
//! [`ClusterMetadata`] image, which brokers can consume as snapshots or
//! offset-addressed deltas.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type BrokerId = i32;
pub type BrokerEpoch = u64;
pub type MetadataOffset = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    Broker,
    Controller,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Viewer,
    Operator,
    Admin,
}

impl Role {
    pub fn permits(self, required: Role) -> bool {
        self >= required
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRecord {
    pub username: String,
    pub password_hash: String,
    pub role: Role,
    #[serde(default)]
    pub force_password_change: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerMetadata {
    pub broker_id: BrokerId,
    pub host: String,
    pub data_port: u16,
    pub control_port: u16,
    pub broker_epoch: BrokerEpoch,
    pub roles: BTreeSet<NodeRole>,
    pub rack: Option<String>,
    pub alive: bool,
    pub last_heartbeat_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub partition: i32,
    pub replicas: Vec<BrokerId>,
    pub leader: BrokerId,
    pub isr: Vec<BrokerId>,
    pub leader_epoch: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub name: String,
    pub replication_factor: i32,
    pub partitions: BTreeMap<i32, PartitionMetadata>,
    #[serde(default)]
    pub configs: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterMetadata {
    pub cluster_id: String,
    pub offset: MetadataOffset,
    pub controller_id: Option<BrokerId>,
    #[serde(default)]
    pub brokers: BTreeMap<BrokerId, BrokerMetadata>,
    #[serde(default)]
    pub topics: BTreeMap<String, TopicMetadata>,
    #[serde(default)]
    pub users: BTreeMap<String, UserRecord>,
    pub jwt_secret: Option<String>,
}

/// Cheaply cloneable, thread-safe view of the latest cluster metadata image.
///
/// Readers take a short-lived read lock only long enough to clone the current
/// [`Arc`]. They can then inspect that immutable snapshot without holding a
/// lock while a controller subscriber atomically publishes a newer image.
#[derive(Debug, Clone)]
pub struct MetadataCache {
    image: Arc<RwLock<Arc<ClusterMetadata>>>,
}

impl MetadataCache {
    /// Create a cache containing `image`.
    pub fn new(image: ClusterMetadata) -> Self {
        Self {
            image: Arc::new(RwLock::new(Arc::new(image))),
        }
    }

    /// Return the current immutable image.
    pub fn snapshot(&self) -> Arc<ClusterMetadata> {
        Arc::clone(
            &self
                .image
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Atomically publish a complete replacement image.
    pub fn replace(&self, image: ClusterMetadata) {
        *self
            .image
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(image);
    }

    /// Apply one metadata-log command and atomically publish the new image.
    ///
    /// A failed command leaves the cached image unchanged.
    pub fn apply(&self, command: MetadataCommand) -> Result<MetadataEvent, MetadataError> {
        let mut current = self
            .image
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut next = current.as_ref().clone();
        let event = next.apply(command)?;
        *current = Arc::new(next);
        Ok(event)
    }

    /// Offset of the current cached image.
    pub fn offset(&self) -> MetadataOffset {
        self.snapshot().offset
    }
}

impl Default for MetadataCache {
    fn default() -> Self {
        Self::new(ClusterMetadata::default())
    }
}

impl From<ClusterMetadata> for MetadataCache {
    fn from(image: ClusterMetadata) -> Self {
        Self::new(image)
    }
}

impl ClusterMetadata {
    pub fn new(cluster_id: impl Into<String>) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            offset: 0,
            controller_id: None,
            brokers: BTreeMap::new(),
            topics: BTreeMap::new(),
            users: BTreeMap::new(),
            jwt_secret: None,
        }
    }

    pub fn live_broker_ids(&self) -> Vec<BrokerId> {
        self.brokers
            .values()
            .filter(|broker| broker.alive && broker.roles.contains(&NodeRole::Broker))
            .map(|broker| broker.broker_id)
            .collect()
    }

    pub fn next_broker_epoch(&self, broker_id: BrokerId) -> BrokerEpoch {
        self.brokers
            .get(&broker_id)
            .map_or(1, |broker| broker.broker_epoch.saturating_add(1))
    }

    pub fn apply(&mut self, command: MetadataCommand) -> Result<MetadataEvent, MetadataError> {
        let event = match command {
            MetadataCommand::SetController { broker_id } => {
                let broker = self
                    .brokers
                    .get(&broker_id)
                    .ok_or(MetadataError::UnknownBroker(broker_id))?;
                if !broker.alive || !broker.roles.contains(&NodeRole::Controller) {
                    return Err(MetadataError::InvalidController(broker_id));
                }
                self.controller_id = Some(broker_id);
                MetadataEvent::ControllerChanged { broker_id }
            }
            MetadataCommand::RegisterBroker {
                broker_id,
                host,
                data_port,
                control_port,
                roles,
                rack,
                now_ms,
            } => {
                let broker_epoch = self.next_broker_epoch(broker_id);
                let roles = roles.into_iter().collect();
                self.brokers.insert(
                    broker_id,
                    BrokerMetadata {
                        broker_id,
                        host,
                        data_port,
                        control_port,
                        broker_epoch,
                        roles,
                        rack,
                        alive: true,
                        last_heartbeat_ms: now_ms,
                    },
                );
                MetadataEvent::BrokerRegistered {
                    broker_id,
                    broker_epoch,
                }
            }
            MetadataCommand::Heartbeat {
                broker_id,
                broker_epoch,
                now_ms,
            } => {
                let broker = self
                    .brokers
                    .get_mut(&broker_id)
                    .ok_or(MetadataError::UnknownBroker(broker_id))?;
                if broker.broker_epoch != broker_epoch {
                    return Err(MetadataError::StaleBrokerEpoch {
                        broker_id,
                        expected: broker.broker_epoch,
                        actual: broker_epoch,
                    });
                }
                if !broker.alive {
                    return Err(MetadataError::BrokerFenced {
                        broker_id,
                        broker_epoch,
                    });
                }
                broker.alive = true;
                broker.last_heartbeat_ms = now_ms;
                MetadataEvent::BrokerHeartbeat {
                    broker_id,
                    broker_epoch,
                }
            }
            MetadataCommand::FenceBroker {
                broker_id,
                broker_epoch,
            } => {
                let broker = self
                    .brokers
                    .get_mut(&broker_id)
                    .ok_or(MetadataError::UnknownBroker(broker_id))?;
                if broker.broker_epoch != broker_epoch {
                    return Err(MetadataError::StaleBrokerEpoch {
                        broker_id,
                        expected: broker.broker_epoch,
                        actual: broker_epoch,
                    });
                }
                if !broker.alive {
                    return Err(MetadataError::BrokerFenced {
                        broker_id,
                        broker_epoch,
                    });
                }
                broker.alive = false;
                if self.controller_id == Some(broker_id) {
                    self.controller_id = None;
                }
                self.remove_from_isr_and_elect(broker_id);
                MetadataEvent::BrokerFenced {
                    broker_id,
                    broker_epoch,
                }
            }
            MetadataCommand::CreateTopic {
                name,
                partitions,
                replication_factor,
                configs,
            } => {
                validate_topic_name(&name)?;
                if self.topics.contains_key(&name) {
                    return Err(MetadataError::TopicExists(name));
                }
                if partitions < 1 {
                    return Err(MetadataError::InvalidPartitionCount(partitions));
                }
                let live = self.live_broker_ids();
                if replication_factor < 1 || replication_factor as usize > live.len() {
                    return Err(MetadataError::InvalidReplicationFactor {
                        requested: replication_factor,
                        live_brokers: live.len(),
                    });
                }
                let mut placed = BTreeMap::new();
                for partition in 0..partitions {
                    let start = partition as usize % live.len();
                    let replicas: Vec<_> = (0..replication_factor as usize)
                        .map(|index| live[(start + index) % live.len()])
                        .collect();
                    placed.insert(
                        partition,
                        PartitionMetadata {
                            partition,
                            leader: replicas[0],
                            isr: replicas.clone(),
                            replicas,
                            leader_epoch: 0,
                        },
                    );
                }
                self.topics.insert(
                    name.clone(),
                    TopicMetadata {
                        name: name.clone(),
                        replication_factor,
                        partitions: placed,
                        configs,
                    },
                );
                MetadataEvent::TopicCreated { name }
            }
            MetadataCommand::DeleteTopic { name } => {
                self.topics
                    .remove(&name)
                    .ok_or_else(|| MetadataError::UnknownTopic(name.clone()))?;
                MetadataEvent::TopicDeleted { name }
            }
            MetadataCommand::ChangePartition {
                topic,
                partition,
                leader,
                mut isr,
                expected_leader_epoch,
            } => {
                let alive_brokers: BTreeSet<_> = self.live_broker_ids().into_iter().collect();
                let partition_metadata = self.partition_mut(&topic, partition)?;
                if partition_metadata.leader_epoch != expected_leader_epoch {
                    return Err(MetadataError::StaleLeaderEpoch {
                        topic,
                        partition,
                        expected: partition_metadata.leader_epoch,
                        actual: expected_leader_epoch,
                    });
                }
                if !partition_metadata.replicas.contains(&leader)
                    || !isr.contains(&leader)
                    || !alive_brokers.contains(&leader)
                    || (leader != partition_metadata.leader
                        && !partition_metadata.isr.contains(&leader))
                {
                    return Err(MetadataError::InvalidLeader(leader));
                }
                let original_isr_len = isr.len();
                isr.sort_unstable();
                isr.dedup();
                if isr.iter().any(|broker_id| {
                    !partition_metadata.replicas.contains(broker_id)
                        || !alive_brokers.contains(broker_id)
                }) || isr.len() != original_isr_len
                {
                    return Err(MetadataError::InvalidIsr);
                }
                partition_metadata.leader = leader;
                partition_metadata.isr = isr;
                partition_metadata.leader_epoch += 1;
                MetadataEvent::PartitionChanged {
                    topic,
                    partition,
                    leader_epoch: partition_metadata.leader_epoch,
                }
            }
            MetadataCommand::PutUser { user } => {
                validate_username(&user.username)?;
                let username = user.username.clone();
                self.users.insert(username.clone(), user);
                MetadataEvent::UserChanged { username }
            }
            MetadataCommand::DeleteUser { username } => {
                self.users
                    .remove(&username)
                    .ok_or_else(|| MetadataError::UnknownUser(username.clone()))?;
                MetadataEvent::UserDeleted { username }
            }
            MetadataCommand::SetJwtSecret { secret } => {
                if secret.len() < 32 {
                    return Err(MetadataError::WeakJwtSecret);
                }
                self.jwt_secret = Some(secret);
                MetadataEvent::JwtSecretChanged
            }
        };
        self.offset = self.offset.saturating_add(1);
        Ok(event)
    }

    pub fn expired_brokers(
        &self,
        now_ms: i64,
        session_timeout_ms: i64,
    ) -> Vec<(BrokerId, BrokerEpoch)> {
        self.brokers
            .values()
            .filter(|broker| {
                broker.alive
                    && broker.roles.contains(&NodeRole::Broker)
                    && now_ms.saturating_sub(broker.last_heartbeat_ms) > session_timeout_ms
            })
            .map(|broker| (broker.broker_id, broker.broker_epoch))
            .collect()
    }

    fn partition_mut(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<&mut PartitionMetadata, MetadataError> {
        self.topics
            .get_mut(topic)
            .ok_or_else(|| MetadataError::UnknownTopic(topic.to_owned()))?
            .partitions
            .get_mut(&partition)
            .ok_or_else(|| MetadataError::UnknownPartition {
                topic: topic.to_owned(),
                partition,
            })
    }

    fn remove_from_isr_and_elect(&mut self, broker_id: BrokerId) {
        let alive: BTreeSet<_> = self.live_broker_ids().into_iter().collect();
        for topic in self.topics.values_mut() {
            for partition in topic.partitions.values_mut() {
                partition
                    .isr
                    .retain(|id| *id != broker_id && alive.contains(id));
                if partition.leader == broker_id || !partition.isr.contains(&partition.leader) {
                    partition.leader = partition.isr.first().copied().unwrap_or(-1);
                    partition.leader_epoch += 1;
                }
            }
        }
    }
}

impl Default for ClusterMetadata {
    fn default() -> Self {
        Self::new("brahmaputra")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MetadataCommand {
    SetController {
        broker_id: BrokerId,
    },
    RegisterBroker {
        broker_id: BrokerId,
        host: String,
        data_port: u16,
        control_port: u16,
        roles: Vec<NodeRole>,
        rack: Option<String>,
        now_ms: i64,
    },
    Heartbeat {
        broker_id: BrokerId,
        broker_epoch: BrokerEpoch,
        now_ms: i64,
    },
    FenceBroker {
        broker_id: BrokerId,
        broker_epoch: BrokerEpoch,
    },
    CreateTopic {
        name: String,
        partitions: i32,
        replication_factor: i32,
        #[serde(default)]
        configs: BTreeMap<String, String>,
    },
    DeleteTopic {
        name: String,
    },
    ChangePartition {
        topic: String,
        partition: i32,
        leader: BrokerId,
        isr: Vec<BrokerId>,
        expected_leader_epoch: i32,
    },
    PutUser {
        user: UserRecord,
    },
    DeleteUser {
        username: String,
    },
    SetJwtSecret {
        secret: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MetadataEvent {
    ControllerChanged {
        broker_id: BrokerId,
    },
    BrokerRegistered {
        broker_id: BrokerId,
        broker_epoch: BrokerEpoch,
    },
    BrokerHeartbeat {
        broker_id: BrokerId,
        broker_epoch: BrokerEpoch,
    },
    BrokerFenced {
        broker_id: BrokerId,
        broker_epoch: BrokerEpoch,
    },
    TopicCreated {
        name: String,
    },
    TopicDeleted {
        name: String,
    },
    PartitionChanged {
        topic: String,
        partition: i32,
        leader_epoch: i32,
    },
    UserChanged {
        username: String,
    },
    UserDeleted {
        username: String,
    },
    JwtSecretChanged,
}

#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataError {
    #[error("invalid topic name {0:?}")]
    InvalidTopic(String),
    #[error("topic already exists: {0}")]
    TopicExists(String),
    #[error("unknown topic: {0}")]
    UnknownTopic(String),
    #[error("unknown partition: {topic}-{partition}")]
    UnknownPartition { topic: String, partition: i32 },
    #[error("partition count must be positive, got {0}")]
    InvalidPartitionCount(i32),
    #[error("replication factor {requested} is invalid with {live_brokers} live brokers")]
    InvalidReplicationFactor { requested: i32, live_brokers: usize },
    #[error("unknown broker: {0}")]
    UnknownBroker(BrokerId),
    #[error("broker {0} is not an eligible live controller")]
    InvalidController(BrokerId),
    #[error("stale broker epoch for {broker_id}: expected {expected}, got {actual}")]
    StaleBrokerEpoch {
        broker_id: BrokerId,
        expected: BrokerEpoch,
        actual: BrokerEpoch,
    },
    #[error("broker {broker_id} epoch {broker_epoch} is fenced; re-register before heartbeating")]
    BrokerFenced {
        broker_id: BrokerId,
        broker_epoch: BrokerEpoch,
    },
    #[error("stale leader epoch for {topic}-{partition}: expected {expected}, got {actual}")]
    StaleLeaderEpoch {
        topic: String,
        partition: i32,
        expected: i32,
        actual: i32,
    },
    #[error("broker {0} cannot lead this partition")]
    InvalidLeader(BrokerId),
    #[error("ISR contains a broker that is not a replica")]
    InvalidIsr,
    #[error("invalid username {0:?}")]
    InvalidUsername(String),
    #[error("unknown user: {0}")]
    UnknownUser(String),
    #[error("JWT secret must be at least 32 bytes")]
    WeakJwtSecret,
}

pub fn validate_topic_name(name: &str) -> Result<(), MetadataError> {
    let valid = !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        });
    if valid {
        Ok(())
    } else {
        Err(MetadataError::InvalidTopic(name.to_owned()))
    }
}

pub fn validate_username(username: &str) -> Result<(), MetadataError> {
    let valid = !username.is_empty()
        && username.len() <= 128
        && username.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        });
    if valid {
        Ok(())
    } else {
        Err(MetadataError::InvalidUsername(username.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn register(state: &mut ClusterMetadata, broker_id: BrokerId, now_ms: i64) -> BrokerEpoch {
        let event = state
            .apply(MetadataCommand::RegisterBroker {
                broker_id,
                host: "127.0.0.1".into(),
                data_port: 9_092 + broker_id as u16,
                control_port: 19_092 + broker_id as u16,
                roles: vec![NodeRole::Broker, NodeRole::Controller],
                rack: None,
                now_ms,
            })
            .unwrap();
        match event {
            MetadataEvent::BrokerRegistered { broker_epoch, .. } => broker_epoch,
            other => panic!("unexpected event: {other:?}"),
        }
    }

    fn create_topic(state: &mut ClusterMetadata, name: &str) {
        state
            .apply(MetadataCommand::CreateTopic {
                name: name.into(),
                partitions: 6,
                replication_factor: 3,
                configs: BTreeMap::new(),
            })
            .unwrap();
    }

    #[test]
    fn placement_is_deterministic_and_balanced() {
        let mut state = ClusterMetadata::new("cluster-a");
        for broker_id in 1..=3 {
            register(&mut state, broker_id, 1_000);
        }
        create_topic(&mut state, "orders");
        let topic = &state.topics["orders"];
        assert_eq!(topic.partitions.len(), 6);
        assert_eq!(topic.partitions[&0].replicas, vec![1, 2, 3]);
        assert_eq!(topic.partitions[&1].replicas, vec![2, 3, 1]);
        assert_eq!(topic.partitions[&2].replicas, vec![3, 1, 2]);
        assert_eq!(topic.partitions[&3].replicas, vec![1, 2, 3]);
    }

    #[test]
    fn broker_epoch_fences_zombies_and_rejoin_increments_epoch() {
        let mut state = ClusterMetadata::default();
        let first = register(&mut state, 7, 10);
        let second = register(&mut state, 7, 20);
        assert_eq!((first, second), (1, 2));
        let error = state
            .apply(MetadataCommand::Heartbeat {
                broker_id: 7,
                broker_epoch: first,
                now_ms: 30,
            })
            .unwrap_err();
        assert!(matches!(error, MetadataError::StaleBrokerEpoch { .. }));
        state
            .apply(MetadataCommand::Heartbeat {
                broker_id: 7,
                broker_epoch: second,
                now_ms: 30,
            })
            .unwrap();

        state
            .apply(MetadataCommand::FenceBroker {
                broker_id: 7,
                broker_epoch: second,
            })
            .unwrap();
        let error = state
            .apply(MetadataCommand::Heartbeat {
                broker_id: 7,
                broker_epoch: second,
                now_ms: 40,
            })
            .unwrap_err();
        assert!(matches!(error, MetadataError::BrokerFenced { .. }));

        let error = state
            .apply(MetadataCommand::FenceBroker {
                broker_id: 7,
                broker_epoch: second,
            })
            .unwrap_err();
        assert!(matches!(error, MetadataError::BrokerFenced { .. }));

        let third = register(&mut state, 7, 50);
        assert_eq!(third, 3);
    }

    #[test]
    fn controller_must_be_live_and_have_the_controller_role() {
        let mut state = ClusterMetadata::default();
        state
            .apply(MetadataCommand::RegisterBroker {
                broker_id: 1,
                host: "127.0.0.1".to_owned(),
                data_port: 9_092,
                control_port: 19_092,
                roles: vec![NodeRole::Broker],
                rack: None,
                now_ms: 0,
            })
            .unwrap();
        assert_eq!(
            state
                .apply(MetadataCommand::SetController { broker_id: 1 })
                .unwrap_err(),
            MetadataError::InvalidController(1)
        );

        let epoch = register(&mut state, 2, 0);
        state
            .apply(MetadataCommand::SetController { broker_id: 2 })
            .unwrap();
        assert_eq!(state.controller_id, Some(2));
        state
            .apply(MetadataCommand::FenceBroker {
                broker_id: 2,
                broker_epoch: epoch,
            })
            .unwrap();
        assert_eq!(state.controller_id, None);
        assert_eq!(
            state
                .apply(MetadataCommand::SetController { broker_id: 2 })
                .unwrap_err(),
            MetadataError::InvalidController(2)
        );
    }

    #[test]
    fn fencing_elects_from_isr_and_registration_waits_for_replica_catchup() {
        let mut state = ClusterMetadata::default();
        let mut epochs = BTreeMap::new();
        for broker_id in 1..=3 {
            epochs.insert(broker_id, register(&mut state, broker_id, 0));
        }
        create_topic(&mut state, "orders");
        assert_eq!(state.topics["orders"].partitions[&0].leader, 1);

        state
            .apply(MetadataCommand::FenceBroker {
                broker_id: 1,
                broker_epoch: epochs[&1],
            })
            .unwrap();
        let partition = &state.topics["orders"].partitions[&0];
        assert_eq!(partition.leader, 2);
        assert_eq!(partition.isr, vec![2, 3]);
        assert_eq!(partition.leader_epoch, 1);

        let new_epoch = register(&mut state, 1, 100);
        assert_eq!(new_epoch, 2);
        // Registration proves only that the process is alive. Re-entering
        // ISR is a separate, replicated ChangePartition after the leader has
        // observed this replica catch up through the committed prefix.
        assert_eq!(state.topics["orders"].partitions[&0].isr, vec![2, 3]);
        state
            .apply(MetadataCommand::ChangePartition {
                topic: "orders".into(),
                partition: 0,
                leader: 2,
                isr: vec![1, 2, 3],
                expected_leader_epoch: 1,
            })
            .unwrap();
        assert_eq!(state.topics["orders"].partitions[&0].isr, vec![1, 2, 3]);
    }

    #[test]
    fn stale_leader_epoch_is_rejected() {
        let mut state = ClusterMetadata::default();
        for broker_id in 1..=3 {
            register(&mut state, broker_id, 0);
        }
        create_topic(&mut state, "events");
        state
            .apply(MetadataCommand::ChangePartition {
                topic: "events".into(),
                partition: 0,
                leader: 2,
                isr: vec![1, 2, 3],
                expected_leader_epoch: 0,
            })
            .unwrap();
        let error = state
            .apply(MetadataCommand::ChangePartition {
                topic: "events".into(),
                partition: 0,
                leader: 3,
                isr: vec![1, 2, 3],
                expected_leader_epoch: 0,
            })
            .unwrap_err();
        assert!(matches!(
            error,
            MetadataError::StaleLeaderEpoch {
                expected: 1,
                actual: 0,
                ..
            }
        ));
    }

    #[test]
    fn partition_changes_reject_unclean_leaders_and_duplicate_isr_members() {
        let mut state = ClusterMetadata::default();
        for broker_id in 1..=3 {
            register(&mut state, broker_id, 0);
        }
        create_topic(&mut state, "events");

        state
            .apply(MetadataCommand::ChangePartition {
                topic: "events".into(),
                partition: 0,
                leader: 1,
                isr: vec![1, 3],
                expected_leader_epoch: 0,
            })
            .unwrap();
        assert_eq!(state.topics["events"].partitions[&0].isr, vec![1, 3]);

        assert_eq!(
            state
                .apply(MetadataCommand::ChangePartition {
                    topic: "events".into(),
                    partition: 0,
                    leader: 2,
                    isr: vec![1, 2, 3],
                    expected_leader_epoch: 1,
                })
                .unwrap_err(),
            MetadataError::InvalidLeader(2)
        );
        assert_eq!(
            state
                .apply(MetadataCommand::ChangePartition {
                    topic: "events".into(),
                    partition: 0,
                    leader: 1,
                    isr: vec![1, 1, 3],
                    expected_leader_epoch: 1,
                })
                .unwrap_err(),
            MetadataError::InvalidIsr
        );
    }

    #[test]
    fn session_expiry_is_explicit_and_serializable() {
        let mut state = ClusterMetadata::new("cluster-json");
        let epoch = register(&mut state, 1, 100);
        assert!(state.expired_brokers(1_099, 1_000).is_empty());
        assert_eq!(state.expired_brokers(1_101, 1_000), vec![(1, epoch)]);
        let json = serde_json::to_string(&state).unwrap();
        let restored: ClusterMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, state);
    }

    #[test]
    fn role_hierarchy_matches_rbac_contract() {
        assert!(Role::Admin.permits(Role::Operator));
        assert!(Role::Operator.permits(Role::Viewer));
        assert!(!Role::Viewer.permits(Role::Operator));
    }

    #[test]
    fn cache_clones_share_atomic_immutable_images() {
        let cache = MetadataCache::new(ClusterMetadata::new("cluster-a"));
        let old_snapshot = cache.snapshot();
        let publisher = cache.clone();

        std::thread::spawn(move || {
            let mut replacement = ClusterMetadata::new("cluster-b");
            replacement.offset = 41;
            publisher.replace(replacement);
        })
        .join()
        .unwrap();

        assert_eq!(old_snapshot.cluster_id, "cluster-a");
        assert_eq!(old_snapshot.offset, 0);
        let current = cache.snapshot();
        assert_eq!(current.cluster_id, "cluster-b");
        assert_eq!(current.offset, 41);
    }

    #[test]
    fn cache_apply_publishes_only_successful_commands() {
        let cache = MetadataCache::default();
        cache
            .apply(MetadataCommand::RegisterBroker {
                broker_id: 1,
                host: "127.0.0.1".into(),
                data_port: 9092,
                control_port: 19092,
                roles: vec![NodeRole::Broker],
                rack: None,
                now_ms: 100,
            })
            .unwrap();
        assert_eq!(cache.offset(), 1);
        assert!(cache.snapshot().brokers.contains_key(&1));

        let error = cache
            .apply(MetadataCommand::SetController { broker_id: 99 })
            .unwrap_err();
        assert_eq!(error, MetadataError::UnknownBroker(99));
        assert_eq!(cache.offset(), 1);
        assert_eq!(cache.snapshot().controller_id, None);
    }
}
