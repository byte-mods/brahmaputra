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

/// What an ACL rule governs.
///
/// Deliberately narrower than Kafka's resource taxonomy: topics and groups
/// are what a client actually touches on this data plane, and `Cluster`
/// covers the operations that are not scoped to either.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceType {
    Topic,
    Group,
    Cluster,
}

/// What a principal may do to a resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AclOperation {
    /// Consume from a topic, or use a consumer group.
    Read,
    /// Produce to a topic.
    Write,
    /// See that a resource exists, and read its offsets and metadata.
    Describe,
    /// Any of the above.
    All,
}

impl AclOperation {
    /// Whether a rule granting `self` covers a request needing `wanted`.
    pub fn covers(self, wanted: AclOperation) -> bool {
        self == AclOperation::All || self == wanted
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AclPermission {
    Allow,
    Deny,
}

/// One access-control rule.
///
/// `principal` and `resource_name` accept `*` as "any". Evaluation is
/// deny-overrides-allow, and the default with no matching rule is denial —
/// so adding authentication cannot silently widen access.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AclRule {
    pub principal: String,
    pub resource_type: ResourceType,
    pub resource_name: String,
    pub operation: AclOperation,
    pub permission: AclPermission,
}

impl AclRule {
    /// A stable identity for the rule, so adding the same rule twice does
    /// not accumulate duplicates in the metadata log.
    pub fn key(&self) -> String {
        format!(
            "{}|{:?}|{}|{:?}|{:?}",
            self.principal, self.resource_type, self.resource_name, self.operation, self.permission
        )
    }

    fn matches(
        &self,
        principal: &str,
        resource_type: ResourceType,
        resource_name: &str,
        operation: AclOperation,
    ) -> bool {
        (self.principal == "*" || self.principal == principal)
            && self.resource_type == resource_type
            && (self.resource_name == "*" || self.resource_name == resource_name)
            && self.operation.covers(operation)
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

/// Who a byte-rate limit applies to.
///
/// A single broker-wide rate is the wrong shape for a shared cluster: the
/// tenant filling the disk and the tenant reading one topic an hour get the
/// same ceiling, so it has to be set for the worst case and is then too
/// loose for everyone. Kafka names the entity a quota binds to; this is the
/// same set, with `None` meaning "any".
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub struct QuotaEntity {
    /// Authenticated principal, or `None` for a rule that ignores identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// `client.id` the request announced, or `None` to ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
}

impl QuotaEntity {
    pub fn new(user: Option<String>, client_id: Option<String>) -> Self {
        QuotaEntity { user, client_id }
    }

    /// A stable identity, so re-setting the same entity replaces its limits
    /// instead of accumulating duplicates in the metadata log.
    pub fn key(&self) -> String {
        format!(
            "user={}|client={}",
            self.user.as_deref().unwrap_or("*"),
            self.client_id.as_deref().unwrap_or("*")
        )
    }

    /// How specific this rule is: 2 names both, 1 names one, 0 is the
    /// cluster-wide default. Higher wins.
    fn specificity(&self) -> u8 {
        u8::from(self.user.is_some()) + u8::from(self.client_id.is_some())
    }

    fn matches(&self, user: Option<&str>, client_id: Option<&str>) -> bool {
        let user_ok = match &self.user {
            Some(name) => user == Some(name.as_str()),
            None => true,
        };
        let client_ok = match &self.client_id {
            Some(name) => client_id == Some(name.as_str()),
            None => true,
        };
        user_ok && client_ok
    }
}

/// Byte-rate ceilings for one entity. `None` leaves that direction to
/// whatever the next-less-specific rule — ultimately the broker default —
/// says.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct QuotaLimits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub produce_bytes_per_sec: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch_bytes_per_sec: Option<u64>,
}

impl QuotaLimits {
    pub fn is_empty(&self) -> bool {
        self.produce_bytes_per_sec.is_none() && self.fetch_bytes_per_sec.is_none()
    }
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
    /// Where this partition is being moved to, while a reassignment is in
    /// flight. `None` when the replica set is settled.
    ///
    /// A reassignment cannot simply overwrite `replicas`: the new brokers
    /// hold none of the data yet, so switching to them would drop the
    /// partition's durability to zero for as long as the catch-up takes.
    /// Instead `replicas` becomes the *union* of old and new — every
    /// existing copy is kept while the new ones catch up — and this field
    /// records where it is heading. The controller narrows `replicas` to
    /// the target only once the target replicas are all in the ISR.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_replicas: Option<Vec<BrokerId>>,
}

impl PartitionMetadata {
    /// Whether a reassignment is in flight for this partition.
    pub fn is_reassigning(&self) -> bool {
        self.target_replicas.is_some()
    }

    /// The replica that *should* lead this partition.
    ///
    /// Placement spreads leadership across brokers and racks by rotating
    /// which replica it puts first, so `replicas[0]` is the choice that
    /// keeps the cluster balanced. Every other broker in the list is a
    /// fallback for when that one is unavailable.
    pub fn preferred_leader(&self) -> Option<BrokerId> {
        self.replicas.first().copied()
    }

    /// Whether leadership has drifted off the preferred replica while that
    /// replica is still a fully caught-up member of the ISR.
    ///
    /// This is the state a broker restart leaves behind: the partition
    /// failed over, the original broker came back and rejoined the ISR, and
    /// nothing moved leadership back. It is safe to correct precisely
    /// because the preferred replica is in the ISR — it holds every
    /// committed record, so the handover loses nothing.
    pub fn is_leader_imbalanced(&self) -> bool {
        match self.preferred_leader() {
            Some(preferred) => {
                preferred != self.leader && self.isr.contains(&preferred) && !self.is_reassigning()
            }
            None => false,
        }
    }
}

/// Pick a leader from the ISR, preferring the replica placement chose.
///
/// Taking `isr.first()` instead — the ISR is sorted, so that is the
/// lowest-numbered surviving broker — piles leadership onto low broker ids
/// every time a cluster loses and regains a node. Walking `replicas` in
/// order keeps failover landing where placement intended, and falls back to
/// any ISR member when the preferred replicas are all gone.
fn elect_from_isr(partition: &PartitionMetadata) -> BrokerId {
    partition
        .replicas
        .iter()
        .copied()
        .find(|replica| partition.isr.contains(replica))
        .or_else(|| partition.isr.first().copied())
        .unwrap_or(-1)
}

/// Choose `count` brokers for one partition, spreading across racks first.
///
/// Replicas that share a rack share a failure domain, so RF=3 inside one
/// rack survives no more than RF=1 does — the partition is lost when that
/// rack goes. Kafka calls this rack-aware assignment; the effect is that
/// `--rack` stops being a label nobody reads and starts being the thing
/// that makes a replication factor mean what an operator thinks it means.
///
/// Brokers are ordered by taking one from each rack in turn, so
/// consecutive picks land in different racks for as long as there are
/// racks left. Racks are visited in a rotation that shifts with
/// `partition`, which keeps leadership from piling onto whichever rack
/// happens to sort first.
///
/// With no racks configured this degrades to the previous round-robin,
/// which is what an unlabelled cluster should get.
pub fn place_replicas(
    brokers: &[BrokerId],
    racks: &BTreeMap<BrokerId, Option<String>>,
    partition: i32,
    count: usize,
) -> Vec<BrokerId> {
    if brokers.is_empty() || count == 0 {
        return Vec::new();
    }

    // Group by rack, preserving broker order inside each. An unlabelled
    // broker is its own rack: assuming two unlabelled brokers are in
    // different failure domains is the safe reading, because assuming they
    // share one would refuse placements that are probably fine, while the
    // reverse only matches today's behaviour.
    let mut by_rack: BTreeMap<String, Vec<BrokerId>> = BTreeMap::new();
    for broker in brokers {
        let rack = racks
            .get(broker)
            .and_then(|rack| rack.clone())
            .unwrap_or_else(|| format!("\u{0}unlabelled-{broker}"));
        by_rack.entry(rack).or_default().push(*broker);
    }

    let mut lanes: Vec<Vec<BrokerId>> = by_rack.into_values().collect();
    // Rotate which rack leads, per partition, so leadership spreads.
    let rotation = (partition.max(0) as usize) % lanes.len();
    lanes.rotate_left(rotation);

    // Interleave: one from each rack, then the next from each, and so on.
    let mut ordered = Vec::with_capacity(brokers.len());
    let deepest = lanes.iter().map(Vec::len).max().unwrap_or(0);
    for depth in 0..deepest {
        for lane in &lanes {
            if let Some(broker) = lane.get(depth) {
                ordered.push(*broker);
            }
        }
    }
    ordered.truncate(count.min(ordered.len()));
    ordered
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
    /// Access-control rules, keyed by [`AclRule::key`] so re-adding a rule
    /// replaces it rather than duplicating it.
    #[serde(default)]
    pub acls: BTreeMap<String, AclRule>,
    /// Byte-rate overrides, keyed by [`QuotaEntity::key`]. Empty means every
    /// client is held to the broker-wide default.
    #[serde(default)]
    pub quotas: BTreeMap<String, (QuotaEntity, QuotaLimits)>,
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
            acls: BTreeMap::new(),
            quotas: BTreeMap::new(),
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

    /// Every registered broker's rack label, for rack-aware placement.
    pub fn broker_racks(&self) -> BTreeMap<BrokerId, Option<String>> {
        self.brokers
            .values()
            .map(|broker| (broker.broker_id, broker.rack.clone()))
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
                let racks = self.broker_racks();
                let mut placed = BTreeMap::new();
                for partition in 0..partitions {
                    let replicas =
                        place_replicas(&live, &racks, partition, replication_factor as usize);
                    placed.insert(
                        partition,
                        PartitionMetadata {
                            partition,
                            leader: replicas[0],
                            isr: replicas.clone(),
                            replicas,
                            leader_epoch: 0,
                            target_replicas: None,
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
            MetadataCommand::AddPartitions { name, count } => {
                let live = self.live_broker_ids();
                let racks = self.broker_racks();
                if live.is_empty() {
                    return Err(MetadataError::InvalidReplicationFactor {
                        requested: 1,
                        live_brokers: 0,
                    });
                }
                let Some(topic) = self.topics.get_mut(&name) else {
                    return Err(MetadataError::UnknownTopic(name));
                };
                let current = topic.partitions.len() as i32;
                // Partitions only ever increase. Removing one would strand
                // the records already written to it, and a keyed producer
                // would silently start routing its keys elsewhere.
                if count <= current {
                    return Err(MetadataError::InvalidPartitionCount(count));
                }
                let replication_factor = topic
                    .partitions
                    .values()
                    .next()
                    .map(|partition| partition.replicas.len())
                    .unwrap_or(1)
                    .min(live.len())
                    .max(1);
                for partition in current..count {
                    let replicas = place_replicas(&live, &racks, partition, replication_factor);
                    topic.partitions.insert(
                        partition,
                        PartitionMetadata {
                            partition,
                            leader: replicas[0],
                            isr: replicas.clone(),
                            replicas,
                            leader_epoch: 0,
                            target_replicas: None,
                        },
                    );
                }
                MetadataEvent::TopicCreated { name }
            }
            MetadataCommand::ReassignPartition {
                topic,
                partition,
                mut replicas,
            } => {
                let alive: BTreeSet<_> = self.live_broker_ids().into_iter().collect();
                replicas.sort_unstable();
                replicas.dedup();
                if replicas.is_empty() {
                    return Err(MetadataError::InvalidIsr);
                }
                // Every target must be a live broker. Placing a replica on a
                // broker that is not there means a partition that can never
                // reach its replication factor, and the reassignment would
                // hang rather than fail.
                if let Some(missing) = replicas.iter().find(|id| !alive.contains(id)) {
                    return Err(MetadataError::UnknownBroker(*missing));
                }
                let current = self.partition_mut(&topic, partition)?;
                if current.is_reassigning() {
                    return Err(MetadataError::ReassignmentInProgress { topic, partition });
                }
                if current.replicas == replicas {
                    // Already where it was asked to go. Treated as done
                    // rather than as an error so a retried or idempotent
                    // request is not a failure.
                    return Ok(MetadataEvent::PartitionReassigned {
                        topic,
                        partition,
                        replicas,
                    });
                }
                // The union, not the target: the target brokers hold no data
                // yet, so narrowing now would leave the partition with fewer
                // usable copies than it started with for the whole catch-up.
                let mut union = current.replicas.clone();
                for broker in &replicas {
                    if !union.contains(broker) {
                        union.push(*broker);
                    }
                }
                current.replicas = union;
                current.target_replicas = Some(replicas.clone());
                MetadataEvent::PartitionReassigned {
                    topic,
                    partition,
                    replicas,
                }
            }
            MetadataCommand::CompleteReassignment { topic, partition } => {
                let alive: BTreeSet<_> = self.live_broker_ids().into_iter().collect();
                let current = self.partition_mut(&topic, partition)?;
                let Some(target) = current.target_replicas.clone() else {
                    return Err(MetadataError::NoReassignmentInProgress { topic, partition });
                };
                // The condition that makes dropping the old replicas safe:
                // every target replica is caught up and in the ISR, so the
                // data exists on all of them already.
                if !target.iter().all(|broker| current.isr.contains(broker)) {
                    return Err(MetadataError::ReassignmentNotCaughtUp { topic, partition });
                }
                current.replicas = target.clone();
                current.isr.retain(|broker| target.contains(broker));
                current.target_replicas = None;
                // The leader may be one of the replicas being dropped. Move
                // it to a target replica before the old set disappears,
                // rather than leaving the partition pointing at a broker
                // that no longer holds it.
                if !target.contains(&current.leader) {
                    let Some(next) = current
                        .isr
                        .iter()
                        .copied()
                        .find(|broker| alive.contains(broker))
                    else {
                        return Err(MetadataError::ReassignmentNotCaughtUp { topic, partition });
                    };
                    current.leader = next;
                }
                current.leader_epoch += 1;
                MetadataEvent::PartitionReassigned {
                    topic,
                    partition,
                    replicas: target,
                }
            }
            MetadataCommand::SetTopicConfig { name, configs } => {
                let Some(topic) = self.topics.get_mut(&name) else {
                    return Err(MetadataError::UnknownTopic(name));
                };
                // Merge rather than replace: an operator changing one key
                // from a UI should not silently clear the others.
                for (key, value) in configs {
                    topic.configs.insert(key, value);
                }
                MetadataEvent::TopicCreated { name }
            }
            MetadataCommand::PutAcl { rule } => {
                let key = rule.key();
                self.acls.insert(key.clone(), rule);
                MetadataEvent::AclChanged { key }
            }
            MetadataCommand::DeleteAcl { key } => {
                self.acls.remove(&key);
                MetadataEvent::AclChanged { key }
            }
            MetadataCommand::PutQuota { entity, limits } => {
                let key = entity.key();
                if limits.is_empty() {
                    // Setting every direction back to "unspecified" is how an
                    // operator removes an override, so treat it as a delete
                    // rather than storing a rule that says nothing.
                    self.quotas.remove(&key);
                } else {
                    self.quotas.insert(key.clone(), (entity, limits));
                }
                MetadataEvent::QuotaChanged { key }
            }
            MetadataCommand::DeleteQuota { key } => {
                self.quotas.remove(&key);
                MetadataEvent::QuotaChanged { key }
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
                    partition.leader = elect_from_isr(partition);
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
    AddPartitions {
        name: String,
        count: i32,
    },
    /// Move a partition to a different set of brokers.
    ///
    /// This is what makes a cluster reshapeable: without it a partition
    /// lives on whichever brokers it was created on, forever, so a cluster
    /// can be neither grown, shrunk, nor rebalanced.
    ///
    /// Applying it does *not* switch `replicas` to the target. The target
    /// brokers hold none of the data yet, so switching would drop the
    /// partition's durability to nothing for the length of the catch-up.
    /// `replicas` becomes the union of current and target — every existing
    /// copy survives — and the controller narrows it to the target once
    /// the target replicas are caught up and in the ISR.
    ReassignPartition {
        topic: String,
        partition: i32,
        replicas: Vec<BrokerId>,
    },
    /// Finish a reassignment whose target replicas have all caught up.
    ///
    /// Issued by the controller, not by an operator: only the controller
    /// can see that the ISR now covers the target, which is the condition
    /// that makes dropping the old replicas safe.
    CompleteReassignment {
        topic: String,
        partition: i32,
    },
    SetTopicConfig {
        name: String,
        configs: BTreeMap<String, String>,
    },
    PutAcl {
        rule: AclRule,
    },
    DeleteAcl {
        key: String,
    },
    /// Bind byte-rate limits to a user, a client id, or both.
    ///
    /// Limits live in cluster metadata rather than in broker flags so they
    /// can be changed on a running cluster and so every broker enforces the
    /// same number — a per-broker flag would mean a client's real ceiling
    /// depended on which leader it happened to be talking to.
    PutQuota {
        entity: QuotaEntity,
        limits: QuotaLimits,
    },
    DeleteQuota {
        key: String,
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
    PartitionReassigned {
        topic: String,
        partition: i32,
        replicas: Vec<BrokerId>,
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
    AclChanged {
        key: String,
    },
    QuotaChanged {
        key: String,
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
    #[error("a reassignment is already in flight for {topic}-{partition}")]
    ReassignmentInProgress { topic: String, partition: i32 },
    #[error("no reassignment is in flight for {topic}-{partition}")]
    NoReassignmentInProgress { topic: String, partition: i32 },
    #[error("the target replicas for {topic}-{partition} have not caught up yet")]
    ReassignmentNotCaughtUp { topic: String, partition: i32 },
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
    fn failover_follows_replica_order_not_broker_number() {
        let mut state = ClusterMetadata::default();
        let mut epochs = BTreeMap::new();
        for broker_id in 1..=3 {
            epochs.insert(broker_id, register(&mut state, broker_id, 0));
        }
        create_topic(&mut state, "orders");
        // Placement put broker 3 ahead of broker 1 for this partition, so
        // that is where leadership should land — taking the lowest ISR
        // member instead would pile every failover onto broker 1.
        let partition = state
            .topics
            .get_mut("orders")
            .unwrap()
            .partitions
            .get_mut(&0)
            .unwrap();
        partition.replicas = vec![2, 3, 1];
        partition.leader = 2;
        partition.isr = vec![1, 2, 3];

        state
            .apply(MetadataCommand::FenceBroker {
                broker_id: 2,
                broker_epoch: epochs[&2],
            })
            .unwrap();
        let partition = &state.topics["orders"].partitions[&0];
        assert_eq!(partition.leader, 3);
        assert_eq!(partition.isr, vec![1, 3]);
        assert_eq!(partition.preferred_leader(), Some(2));
        // Still out of the ISR, so there is nothing to rebalance back to
        // yet: an imbalance only becomes correctable once broker 2 returns
        // and catches up.
        assert!(!partition.is_leader_imbalanced());
        register(&mut state, 2, 100);
        state
            .apply(MetadataCommand::ChangePartition {
                topic: "orders".into(),
                partition: 0,
                leader: 3,
                isr: vec![1, 2, 3],
                expected_leader_epoch: 1,
            })
            .unwrap();
        assert!(state.topics["orders"].partitions[&0].is_leader_imbalanced());
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

impl ClusterMetadata {
    /// Whether `principal` may perform `operation` on a resource.
    ///
    /// Deny rules win over allow rules, and the default is denial. An
    /// `Admin` is exempt: someone who can already rewrite the ACLs gains
    /// nothing from being blocked by them, and it keeps a cluster
    /// recoverable after a bad rule.
    pub fn is_authorized(
        &self,
        principal: &str,
        resource_type: ResourceType,
        resource_name: &str,
        operation: AclOperation,
    ) -> bool {
        if self
            .users
            .get(principal)
            .is_some_and(|user| user.role == Role::Admin)
        {
            return true;
        }
        let matching = || {
            self.acls
                .values()
                .filter(|rule| rule.matches(principal, resource_type, resource_name, operation))
        };
        if matching().any(|rule| rule.permission == AclPermission::Deny) {
            return false;
        }
        matching().any(|rule| rule.permission == AclPermission::Allow)
    }

    /// The byte-rate limits that apply to one request, or `None` where no
    /// override matches and the broker default should stand.
    ///
    /// The most specific matching rule wins outright, as in Kafka: a
    /// `(user, client-id)` rule beats a rule naming only one of them, which
    /// beats the catch-all. Directions resolve independently, so a rule can
    /// cap a tenant's writes and leave its reads to the default. Ties are
    /// impossible — two rules of equal specificity that both match would
    /// have to name the same user and client, and that is one key.
    pub fn quota_for(&self, user: Option<&str>, client_id: Option<&str>) -> Option<QuotaLimits> {
        let mut produce = None;
        let mut fetch = None;
        let mut produce_rank = None;
        let mut fetch_rank = None;
        for (entity, limits) in self.quotas.values() {
            if !entity.matches(user, client_id) {
                continue;
            }
            let rank = entity.specificity();
            if limits.produce_bytes_per_sec.is_some()
                && produce_rank.is_none_or(|best| rank > best)
            {
                produce = limits.produce_bytes_per_sec;
                produce_rank = Some(rank);
            }
            if limits.fetch_bytes_per_sec.is_some() && fetch_rank.is_none_or(|best| rank > best) {
                fetch = limits.fetch_bytes_per_sec;
                fetch_rank = Some(rank);
            }
        }
        (produce.is_some() || fetch.is_some()).then_some(QuotaLimits {
            produce_bytes_per_sec: produce,
            fetch_bytes_per_sec: fetch,
        })
    }
}

#[cfg(test)]
mod quota_tests {
    use super::*;

    fn put(state: &mut ClusterMetadata, user: Option<&str>, client: Option<&str>, produce: Option<u64>, fetch: Option<u64>) {
        state
            .apply(MetadataCommand::PutQuota {
                entity: QuotaEntity::new(user.map(str::to_owned), client.map(str::to_owned)),
                limits: QuotaLimits {
                    produce_bytes_per_sec: produce,
                    fetch_bytes_per_sec: fetch,
                },
            })
            .unwrap();
    }

    #[test]
    fn no_override_leaves_the_broker_default_in_charge() {
        let state = ClusterMetadata::default();
        assert_eq!(state.quota_for(Some("alice"), Some("app")), None);
    }

    #[test]
    fn the_most_specific_matching_rule_wins_per_direction() {
        let mut state = ClusterMetadata::default();
        put(&mut state, None, None, Some(1_000), Some(1_000));
        put(&mut state, Some("alice"), None, Some(5_000), None);
        put(&mut state, Some("alice"), Some("batch"), Some(50_000), None);

        // Catch-all only.
        let bob = state.quota_for(Some("bob"), Some("app")).unwrap();
        assert_eq!(bob.produce_bytes_per_sec, Some(1_000));
        assert_eq!(bob.fetch_bytes_per_sec, Some(1_000));

        // Alice's own rule raises her writes; her reads fall back to the
        // catch-all, because directions resolve independently.
        let alice = state.quota_for(Some("alice"), Some("app")).unwrap();
        assert_eq!(alice.produce_bytes_per_sec, Some(5_000));
        assert_eq!(alice.fetch_bytes_per_sec, Some(1_000));

        // Her batch job is named exactly, so it beats the rule naming only
        // her.
        let batch = state.quota_for(Some("alice"), Some("batch")).unwrap();
        assert_eq!(batch.produce_bytes_per_sec, Some(50_000));
        assert_eq!(batch.fetch_bytes_per_sec, Some(1_000));
    }

    #[test]
    fn a_client_id_rule_applies_across_users() {
        let mut state = ClusterMetadata::default();
        put(&mut state, None, Some("scanner"), None, Some(100));
        assert_eq!(
            state
                .quota_for(Some("alice"), Some("scanner"))
                .unwrap()
                .fetch_bytes_per_sec,
            Some(100)
        );
        assert_eq!(
            state
                .quota_for(None, Some("scanner"))
                .unwrap()
                .fetch_bytes_per_sec,
            Some(100)
        );
        assert_eq!(state.quota_for(Some("alice"), Some("other")), None);
    }

    #[test]
    fn resetting_every_direction_removes_the_override() {
        let mut state = ClusterMetadata::default();
        put(&mut state, Some("alice"), None, Some(5_000), None);
        assert!(state.quota_for(Some("alice"), None).is_some());

        put(&mut state, Some("alice"), None, None, None);
        assert!(state.quotas.is_empty());
        assert_eq!(state.quota_for(Some("alice"), None), None);
    }

    #[test]
    fn re_setting_an_entity_replaces_rather_than_accumulates() {
        let mut state = ClusterMetadata::default();
        put(&mut state, Some("alice"), Some("app"), Some(1), None);
        put(&mut state, Some("alice"), Some("app"), Some(2), None);
        assert_eq!(state.quotas.len(), 1);
        assert_eq!(
            state
                .quota_for(Some("alice"), Some("app"))
                .unwrap()
                .produce_bytes_per_sec,
            Some(2)
        );

        let key = QuotaEntity::new(Some("alice".into()), Some("app".into())).key();
        state
            .apply(MetadataCommand::DeleteQuota { key })
            .unwrap();
        assert!(state.quotas.is_empty());
    }
}

#[cfg(test)]
mod acl_tests {
    use super::*;

    fn cluster() -> ClusterMetadata {
        ClusterMetadata::new("test")
    }

    fn rule(
        principal: &str,
        resource_name: &str,
        operation: AclOperation,
        permission: AclPermission,
    ) -> AclRule {
        AclRule {
            principal: principal.to_string(),
            resource_type: ResourceType::Topic,
            resource_name: resource_name.to_string(),
            operation,
            permission,
        }
    }

    fn put(cluster: &mut ClusterMetadata, rule: AclRule) {
        cluster.acls.insert(rule.key(), rule);
    }

    /// The default has to be denial. Adding authentication must never be
    /// able to widen access by accident.
    #[test]
    fn no_rule_means_denied() {
        let cluster = cluster();
        assert!(!cluster.is_authorized("alice", ResourceType::Topic, "orders", AclOperation::Read));
    }

    #[test]
    fn an_allow_rule_grants_exactly_its_operation() {
        let mut cluster = cluster();
        put(
            &mut cluster,
            rule("alice", "orders", AclOperation::Read, AclPermission::Allow),
        );
        assert!(cluster.is_authorized("alice", ResourceType::Topic, "orders", AclOperation::Read));
        assert!(!cluster.is_authorized(
            "alice",
            ResourceType::Topic,
            "orders",
            AclOperation::Write
        ));
        assert!(!cluster.is_authorized("alice", ResourceType::Topic, "other", AclOperation::Read));
        assert!(!cluster.is_authorized("bob", ResourceType::Topic, "orders", AclOperation::Read));
    }

    #[test]
    fn deny_beats_allow_however_the_rules_were_added() {
        let mut cluster = cluster();
        put(
            &mut cluster,
            rule("*", "*", AclOperation::All, AclPermission::Allow),
        );
        put(
            &mut cluster,
            rule(
                "mallory",
                "secrets",
                AclOperation::Read,
                AclPermission::Deny,
            ),
        );
        assert!(cluster.is_authorized(
            "mallory",
            ResourceType::Topic,
            "orders",
            AclOperation::Read
        ));
        assert!(!cluster.is_authorized(
            "mallory",
            ResourceType::Topic,
            "secrets",
            AclOperation::Read
        ));
    }

    #[test]
    fn all_covers_every_operation_and_wildcards_match_any_name() {
        let mut cluster = cluster();
        put(
            &mut cluster,
            rule("service", "*", AclOperation::All, AclPermission::Allow),
        );
        for operation in [
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Describe,
        ] {
            assert!(cluster.is_authorized("service", ResourceType::Topic, "anything", operation));
        }
        // Still scoped by resource type.
        assert!(!cluster.is_authorized("service", ResourceType::Group, "g", AclOperation::Read));
    }

    /// An admin must not be able to lock themselves out with a bad rule.
    #[test]
    fn an_admin_is_exempt_from_deny_rules() {
        let mut cluster = cluster();
        cluster.users.insert(
            "root".to_string(),
            UserRecord {
                username: "root".to_string(),
                password_hash: "x".to_string(),
                role: Role::Admin,
                force_password_change: false,
            },
        );
        put(
            &mut cluster,
            rule("*", "*", AclOperation::All, AclPermission::Deny),
        );
        assert!(cluster.is_authorized("root", ResourceType::Topic, "orders", AclOperation::Write));
    }

    /// Re-adding an identical rule replaces it rather than accumulating.
    #[test]
    fn rules_are_idempotent() {
        let mut cluster = cluster();
        let same = rule("alice", "orders", AclOperation::Read, AclPermission::Allow);
        put(&mut cluster, same.clone());
        put(&mut cluster, same);
        assert_eq!(cluster.acls.len(), 1);
    }
}

/// Password hashing for the cluster user store.
///
/// This lives beside [`UserRecord`] rather than in the dashboard because
/// the data plane authenticates against the same users, and a broker must
/// not have to depend on the HTTP layer to check a password.
pub mod password {
    use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
    use argon2::Argon2;

    /// Hash a password for storage. Each call salts randomly, so the same
    /// password never produces the same hash twice.
    pub fn hash(password: &str) -> Result<String, &'static str> {
        let salt = SaltString::generate(&mut rand_core::OsRng);
        Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map(|hash| hash.to_string())
            .map_err(|_| "cannot hash password")
    }

    /// Verify a password against a stored hash. A wrong password and an
    /// unparseable hash fail identically, so neither is distinguishable to
    /// a caller probing for valid accounts.
    pub fn verify(password: &str, stored_hash: &str) -> bool {
        let Ok(parsed) = PasswordHash::new(stored_hash) else {
            return false;
        };
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn a_hash_verifies_only_against_its_own_password() {
            let stored = hash("correct horse").expect("hash");
            assert!(verify("correct horse", &stored));
            assert!(!verify("wrong horse", &stored));
        }

        #[test]
        fn the_same_password_hashes_differently_each_time() {
            assert_ne!(hash("repeat").unwrap(), hash("repeat").unwrap());
        }

        #[test]
        fn a_corrupt_hash_fails_rather_than_panicking() {
            assert!(!verify("anything", "not-a-hash"));
        }
    }
}

#[cfg(test)]
mod partition_and_config_tests {
    use super::*;

    fn cluster_with_topic(partitions: i32, replication_factor: i32) -> ClusterMetadata {
        let mut image = ClusterMetadata::new("ui-test");
        for broker_id in 1..=3 {
            image
                .apply(MetadataCommand::RegisterBroker {
                    broker_id,
                    host: format!("host-{broker_id}"),
                    data_port: 9092,
                    control_port: 19092,
                    roles: vec![NodeRole::Broker],
                    rack: None,
                    now_ms: 1_000,
                })
                .expect("register");
        }
        image
            .apply(MetadataCommand::CreateTopic {
                name: "orders".into(),
                partitions,
                replication_factor,
                configs: BTreeMap::new(),
            })
            .expect("create");
        image
    }

    #[test]
    fn adding_partitions_keeps_the_existing_ones_untouched() {
        let mut image = cluster_with_topic(3, 2);
        let before: Vec<_> = image.topics["orders"]
            .partitions
            .iter()
            .map(|(id, meta)| (*id, meta.replicas.clone()))
            .collect();

        image
            .apply(MetadataCommand::AddPartitions {
                name: "orders".into(),
                count: 6,
            })
            .expect("add partitions");

        let topic = &image.topics["orders"];
        assert_eq!(topic.partitions.len(), 6);
        // The partitions that already held data must not be reassigned:
        // moving them would strand records and re-route keys.
        for (id, replicas) in before {
            assert_eq!(
                topic.partitions[&id].replicas, replicas,
                "partition {id} was reassigned"
            );
        }
        // New ones get the same replication factor as the old.
        for id in 3..6 {
            assert_eq!(topic.partitions[&id].replicas.len(), 2);
        }
    }

    #[test]
    fn partitions_cannot_be_reduced_or_left_unchanged() {
        let mut image = cluster_with_topic(4, 1);
        for requested in [4, 3, 0] {
            assert!(
                image
                    .apply(MetadataCommand::AddPartitions {
                        name: "orders".into(),
                        count: requested,
                    })
                    .is_err(),
                "count {requested} must be refused"
            );
        }
        assert_eq!(image.topics["orders"].partitions.len(), 4);
    }

    #[test]
    fn adding_partitions_to_an_unknown_topic_is_an_error() {
        let mut image = cluster_with_topic(1, 1);
        assert!(image
            .apply(MetadataCommand::AddPartitions {
                name: "nope".into(),
                count: 2,
            })
            .is_err());
    }

    #[test]
    fn setting_a_config_merges_rather_than_replaces() {
        let mut image = cluster_with_topic(1, 1);
        image
            .apply(MetadataCommand::SetTopicConfig {
                name: "orders".into(),
                configs: BTreeMap::from([
                    ("retention.ms".to_string(), "60000".to_string()),
                    ("min.insync.replicas".to_string(), "2".to_string()),
                ]),
            })
            .expect("set config");
        image
            .apply(MetadataCommand::SetTopicConfig {
                name: "orders".into(),
                configs: BTreeMap::from([("retention.ms".to_string(), "120000".to_string())]),
            })
            .expect("update one key");

        let configs = &image.topics["orders"].configs;
        assert_eq!(configs["retention.ms"], "120000", "the changed key updates");
        assert_eq!(
            configs["min.insync.replicas"], "2",
            "an untouched key must survive"
        );
    }
}

#[cfg(test)]
mod placement_and_reassignment_tests {
    use super::*;

    fn racks(pairs: &[(BrokerId, Option<&str>)]) -> BTreeMap<BrokerId, Option<String>> {
        pairs
            .iter()
            .map(|(id, rack)| (*id, rack.map(str::to_string)))
            .collect()
    }

    /// The point of rack awareness: RF=3 spread across one rack survives
    /// exactly as much as RF=1 does, because the rack is the failure
    /// domain. Every replica must land in a different rack while there are
    /// racks to use.
    #[test]
    fn replicas_spread_across_racks_before_repeating_one() {
        let brokers = vec![1, 2, 3, 4, 5, 6];
        let racks = racks(&[
            (1, Some("a")),
            (2, Some("a")),
            (3, Some("b")),
            (4, Some("b")),
            (5, Some("c")),
            (6, Some("c")),
        ]);
        for partition in 0..6 {
            let placed = place_replicas(&brokers, &racks, partition, 3);
            assert_eq!(placed.len(), 3);
            let chosen: BTreeSet<&str> = placed
                .iter()
                .map(|broker| racks[broker].as_deref().unwrap())
                .collect();
            assert_eq!(
                chosen.len(),
                3,
                "partition {partition} put two replicas in one rack: {placed:?}"
            );
        }
    }

    /// More replicas than racks has to reuse racks, but must still use all
    /// of them rather than piling into one.
    #[test]
    fn more_replicas_than_racks_still_uses_every_rack() {
        let brokers = vec![1, 2, 3, 4];
        let racks = racks(&[
            (1, Some("a")),
            (2, Some("b")),
            (3, Some("a")),
            (4, Some("b")),
        ]);
        let placed = place_replicas(&brokers, &racks, 0, 4);
        assert_eq!(placed.len(), 4);
        let chosen: BTreeSet<&str> = placed
            .iter()
            .map(|broker| racks[broker].as_deref().unwrap())
            .collect();
        assert_eq!(chosen.len(), 2, "both racks must be used");
    }

    /// Leadership is the first replica, so if every partition led from the
    /// same rack, that rack would carry all the write traffic.
    #[test]
    fn leadership_rotates_across_racks() {
        let brokers = vec![1, 2, 3];
        let racks = racks(&[(1, Some("a")), (2, Some("b")), (3, Some("c"))]);
        let leaders: BTreeSet<BrokerId> = (0..3)
            .map(|partition| place_replicas(&brokers, &racks, partition, 3)[0])
            .collect();
        assert_eq!(leaders.len(), 3, "every rack should lead some partition");
    }

    /// An unlabelled cluster must keep working, and must not decide that
    /// every broker shares one failure domain.
    #[test]
    fn unlabelled_brokers_are_treated_as_distinct() {
        let brokers = vec![1, 2, 3];
        let racks = racks(&[(1, None), (2, None), (3, None)]);
        let placed = place_replicas(&brokers, &racks, 0, 3);
        assert_eq!(placed.len(), 3);
        let unique: BTreeSet<BrokerId> = placed.iter().copied().collect();
        assert_eq!(unique.len(), 3, "no broker may appear twice");
    }

    fn cluster_with(brokers: &[(BrokerId, &str)]) -> ClusterMetadata {
        let mut image = ClusterMetadata::default();
        for (id, rack) in brokers {
            image
                .apply(MetadataCommand::RegisterBroker {
                    broker_id: *id,
                    host: "127.0.0.1".into(),
                    data_port: 9092 + *id as u16,
                    control_port: 19092 + *id as u16,
                    roles: vec![NodeRole::Broker, NodeRole::Controller],
                    rack: Some((*rack).to_string()),
                    now_ms: 1,
                })
                .expect("register");
        }
        image
    }

    /// A reassignment must never shrink the set of brokers holding data.
    /// Until the targets catch up, `replicas` is the union — every existing
    /// copy survives the whole move.
    #[test]
    fn a_reassignment_keeps_every_existing_replica_until_the_targets_catch_up() {
        let mut image = cluster_with(&[(1, "a"), (2, "b"), (3, "c"), (4, "a"), (5, "b")]);
        image
            .apply(MetadataCommand::CreateTopic {
                name: "orders".into(),
                partitions: 1,
                replication_factor: 3,
                configs: BTreeMap::new(),
            })
            .expect("create");

        let before = image.topics["orders"].partitions[&0].replicas.clone();
        assert_eq!(before.len(), 3);

        image
            .apply(MetadataCommand::ReassignPartition {
                topic: "orders".into(),
                partition: 0,
                replicas: vec![4, 5, 1],
            })
            .expect("reassign");

        let during = &image.topics["orders"].partitions[&0];
        assert!(during.is_reassigning());
        for broker in &before {
            assert!(
                during.replicas.contains(broker),
                "existing replica {broker} was dropped mid-move: {:?}",
                during.replicas
            );
        }
        for broker in [4, 5, 1] {
            assert!(during.replicas.contains(&broker), "target {broker} missing");
        }
        assert_eq!(during.target_replicas.as_deref(), Some(&[1, 4, 5][..]));
    }

    /// Completing before the targets are in the ISR would discard the only
    /// copies of the data, so it must be refused.
    #[test]
    fn completing_before_the_targets_are_in_sync_is_refused() {
        let mut image = cluster_with(&[(1, "a"), (2, "b"), (3, "c"), (4, "a")]);
        image
            .apply(MetadataCommand::CreateTopic {
                name: "orders".into(),
                partitions: 1,
                replication_factor: 3,
                configs: BTreeMap::new(),
            })
            .expect("create");
        image
            .apply(MetadataCommand::ReassignPartition {
                topic: "orders".into(),
                partition: 0,
                replicas: vec![2, 3, 4],
            })
            .expect("reassign");

        let error = image
            .apply(MetadataCommand::CompleteReassignment {
                topic: "orders".into(),
                partition: 0,
            })
            .expect_err("must refuse while broker 4 is not in the ISR");
        assert!(
            matches!(error, MetadataError::ReassignmentNotCaughtUp { .. }),
            "expected not-caught-up, got {error:?}"
        );
        assert!(image.topics["orders"].partitions[&0].is_reassigning());
    }

    /// Once the targets are in the ISR the old replicas can go, and the
    /// leader moves with them if it was one of the departing brokers.
    #[test]
    fn completion_narrows_the_replicas_and_moves_the_leader() {
        let mut image = cluster_with(&[(1, "a"), (2, "b"), (3, "c"), (4, "a"), (5, "b")]);
        image
            .apply(MetadataCommand::CreateTopic {
                name: "orders".into(),
                partitions: 1,
                replication_factor: 3,
                configs: BTreeMap::new(),
            })
            .expect("create");
        let original_leader = image.topics["orders"].partitions[&0].leader;

        // Move somewhere that excludes the current leader.
        let target: Vec<BrokerId> = [1, 2, 3, 4, 5]
            .into_iter()
            .filter(|broker| *broker != original_leader)
            .take(3)
            .collect();
        image
            .apply(MetadataCommand::ReassignPartition {
                topic: "orders".into(),
                partition: 0,
                replicas: target.clone(),
            })
            .expect("reassign");

        // Simulate the targets catching up: the controller would issue this
        // once it observed their fetch progress.
        let union = image.topics["orders"].partitions[&0].replicas.clone();
        let epoch = image.topics["orders"].partitions[&0].leader_epoch;
        image
            .apply(MetadataCommand::ChangePartition {
                topic: "orders".into(),
                partition: 0,
                leader: original_leader,
                isr: union,
                expected_leader_epoch: epoch,
            })
            .expect("isr grows to the union");

        image
            .apply(MetadataCommand::CompleteReassignment {
                topic: "orders".into(),
                partition: 0,
            })
            .expect("complete");

        let after = &image.topics["orders"].partitions[&0];
        let mut sorted_target = target.clone();
        sorted_target.sort_unstable();
        assert_eq!(after.replicas, sorted_target, "replicas narrowed to target");
        assert!(!after.is_reassigning());
        assert!(
            sorted_target.contains(&after.leader),
            "leader {} is not in the target set {sorted_target:?}",
            after.leader
        );
        assert!(
            after
                .isr
                .iter()
                .all(|broker| sorted_target.contains(broker)),
            "the ISR still lists departed replicas: {:?}",
            after.isr
        );
    }

    /// Targeting a broker that is not in the cluster would produce a
    /// partition that can never reach its replication factor.
    #[test]
    fn reassigning_onto_an_unknown_broker_is_refused() {
        let mut image = cluster_with(&[(1, "a"), (2, "b"), (3, "c")]);
        image
            .apply(MetadataCommand::CreateTopic {
                name: "orders".into(),
                partitions: 1,
                replication_factor: 3,
                configs: BTreeMap::new(),
            })
            .expect("create");
        let error = image
            .apply(MetadataCommand::ReassignPartition {
                topic: "orders".into(),
                partition: 0,
                replicas: vec![1, 2, 99],
            })
            .expect_err("broker 99 does not exist");
        assert!(matches!(error, MetadataError::UnknownBroker(99)));
    }

    /// Two overlapping reassignments would race over the same replica set.
    #[test]
    fn a_second_reassignment_is_refused_while_one_is_in_flight() {
        let mut image = cluster_with(&[(1, "a"), (2, "b"), (3, "c"), (4, "a")]);
        image
            .apply(MetadataCommand::CreateTopic {
                name: "orders".into(),
                partitions: 1,
                replication_factor: 3,
                configs: BTreeMap::new(),
            })
            .expect("create");
        image
            .apply(MetadataCommand::ReassignPartition {
                topic: "orders".into(),
                partition: 0,
                replicas: vec![1, 2, 4],
            })
            .expect("first");
        let error = image
            .apply(MetadataCommand::ReassignPartition {
                topic: "orders".into(),
                partition: 0,
                replicas: vec![2, 3, 4],
            })
            .expect_err("second must be refused");
        assert!(matches!(
            error,
            MetadataError::ReassignmentInProgress { .. }
        ));
    }

    /// Asking for the placement a partition already has is a no-op, not a
    /// failure: a retried or idempotent request must not error.
    #[test]
    fn reassigning_to_the_current_placement_is_a_no_op() {
        let mut image = cluster_with(&[(1, "a"), (2, "b"), (3, "c")]);
        image
            .apply(MetadataCommand::CreateTopic {
                name: "orders".into(),
                partitions: 1,
                replication_factor: 3,
                configs: BTreeMap::new(),
            })
            .expect("create");
        let current = image.topics["orders"].partitions[&0].replicas.clone();
        image
            .apply(MetadataCommand::ReassignPartition {
                topic: "orders".into(),
                partition: 0,
                replicas: current.clone(),
            })
            .expect("no-op reassignment");
        let after = &image.topics["orders"].partitions[&0];
        assert!(!after.is_reassigning(), "nothing should be in flight");
        assert_eq!(after.replicas, current);
    }
}
