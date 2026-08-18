//! Group-coordinated consumer (M4, Blueprint consumer groups).
//!
//! A `GroupConsumer` joins a consumer group through the broker-side group
//! coordinator, receives a partition assignment from SyncGroup, heartbeats
//! in the background, and commits consumed positions to the internal
//! `__consumer_offsets` topic. The coordinator for a group is the leader of
//! `__consumer_offsets` partition `crc32c(group_id) % partitions`.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{
    AssignedPartition, GroupMemberInfo, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest,
    JoinGroupResponse, MemberAssignment, OffsetCommitEntry, OffsetCommitRequest,
    OffsetCommitResponse, OffsetFetchEntry, OffsetFetchRequest, OffsetFetchResponse,
    SyncGroupRequest, SyncGroupResponse,
};
use brahmaputra_protocol::{ApiKey, ProtocolError};
use bytes::Bytes;
use tokio::task::JoinHandle;

use crate::consumer::{Consumer, EARLIEST};
use crate::error::ClientError;
use crate::router::BrokerRouter;
use crate::transport::Transport;

/// Internal topic whose partition leaders act as group coordinators.
const OFFSETS_TOPIC: &str = "__consumer_offsets";
/// Retries per coordinator request after a coordinator move/load.
const COORDINATOR_ATTEMPTS: usize = 4;
/// Join+sync rounds before giving up on a stabilizing group.
const JOIN_ATTEMPTS: usize = 4;
/// Idle delay between poll sweeps when no records arrive.
const POLL_IDLE_DELAY: Duration = Duration::from_millis(100);
/// `max.poll.records` default, matching Kafka's.
const DEFAULT_MAX_POLL_RECORDS: usize = 500;

type TopicPartition = (String, i32);
type Positions = Arc<Mutex<BTreeMap<TopicPartition, i64>>>;

/// Partition assignment strategy used by the group leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Assignor {
    /// Contiguous partition ranges per topic across members sorted by id.
    Range,
    /// Partitions dealt one at a time across members sorted by id.
    RoundRobin,
}

impl Assignor {
    /// Compute each member's assignment. `members` is `(member_id,
    /// subscribed_topics)`; `topic_partitions` maps each subscribed topic to
    /// its sorted partition ids. Every member appears in the result, possibly
    /// with an empty assignment.
    fn assign(
        &self,
        members: &[(String, Vec<String>)],
        topic_partitions: &BTreeMap<String, Vec<i32>>,
    ) -> BTreeMap<String, Vec<TopicPartition>> {
        match self {
            Assignor::Range => range_assign(members, topic_partitions),
            Assignor::RoundRobin => roundrobin_assign(members, topic_partitions),
        }
    }
}

fn empty_assignment(members: &[(String, Vec<String>)]) -> BTreeMap<String, Vec<TopicPartition>> {
    members
        .iter()
        .map(|(member_id, _)| (member_id.clone(), Vec::new()))
        .collect()
}

/// Per topic, subscribed members (sorted by id) take contiguous partition
/// ranges; the first `partitions % members` members take one extra.
fn range_assign(
    members: &[(String, Vec<String>)],
    topic_partitions: &BTreeMap<String, Vec<i32>>,
) -> BTreeMap<String, Vec<TopicPartition>> {
    let mut assignment = empty_assignment(members);
    for (topic, partitions) in topic_partitions {
        let mut subscribers: Vec<&str> = members
            .iter()
            .filter(|(_, topics)| topics.iter().any(|t| t == topic))
            .map(|(member_id, _)| member_id.as_str())
            .collect();
        subscribers.sort_unstable();
        if subscribers.is_empty() {
            continue;
        }
        let base = partitions.len() / subscribers.len();
        let extra = partitions.len() % subscribers.len();
        let mut cursor = 0;
        for (index, member_id) in subscribers.iter().enumerate() {
            let count = base + usize::from(index < extra);
            for partition in &partitions[cursor..cursor + count] {
                assignment
                    .get_mut(*member_id)
                    .expect("member present")
                    .push((topic.clone(), *partition));
            }
            cursor += count;
        }
    }
    assignment
}

/// The union of all subscribed topic-partitions (sorted by topic, then
/// partition) is dealt around the circle of members sorted by id, skipping
/// members not subscribed to a partition's topic.
fn roundrobin_assign(
    members: &[(String, Vec<String>)],
    topic_partitions: &BTreeMap<String, Vec<i32>>,
) -> BTreeMap<String, Vec<TopicPartition>> {
    let mut assignment = empty_assignment(members);
    let mut circle: Vec<&(String, Vec<String>)> = members.iter().collect();
    circle.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    if circle.is_empty() {
        return assignment;
    }
    let mut next = 0usize;
    for (topic, partitions) in topic_partitions {
        for partition in partitions {
            let start = next;
            loop {
                let member = circle[next % circle.len()];
                next += 1;
                if member.1.iter().any(|t| t == topic) {
                    assignment
                        .get_mut(&member.0)
                        .expect("member present")
                        .push((topic.clone(), *partition));
                    break;
                }
                if next - start >= circle.len() {
                    break; // no member subscribes to this topic
                }
            }
        }
    }
    assignment
}

/// One consumed record with its topic-partition and offset.
#[derive(Debug)]
pub struct ConsumedRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Bytes>,
    pub value: Bytes,
}

#[derive(Debug, Default)]
struct Membership {
    member_id: String,
    generation: i32,
    joined: bool,
}

/// Cloneable handle that routes group requests to the group's coordinator.
#[derive(Clone)]
pub(crate) struct GroupCoordinator {
    pub(crate) router: BrokerRouter,
    pub(crate) group_id: String,
}

impl GroupCoordinator {
    /// Coordinator partition: `crc32c(group_id) % partitions(__consumer_offsets)`.
    pub(crate) async fn partition(&self) -> Result<i32, ClientError> {
        let partitions = self.router.partitions(OFFSETS_TOPIC).await?;
        let count = u32::try_from(partitions.len())
            .map_err(|_| ClientError::Configuration("too many offsets partitions".into()))?;
        Ok((crc32c::crc32c(self.group_id.as_bytes()) % count) as i32)
    }

    /// Send one group request to the coordinator, following NOT_COORDINATOR /
    /// NOT_LEADER_OR_FOLLOWER moves (after a metadata refresh) and waiting out
    /// COORDINATOR_LOAD_IN_PROGRESS. Any other non-zero code becomes a typed
    /// `ClientError` for the caller to interpret.
    pub(crate) async fn request<T>(
        &self,
        api_key: ApiKey,
        body: &[u8],
        decode: impl Fn(&[u8]) -> Result<T, ClientError>,
        error_code: impl Fn(&T) -> i32,
    ) -> Result<T, ClientError> {
        for attempt in 0..COORDINATOR_ATTEMPTS {
            let partition = self.partition().await?;
            let response = self
                .router
                .request_partition(OFFSETS_TOPIC, partition, api_key, body)
                .await?;
            let parsed = decode(&response)?;
            match error_code(&parsed) {
                ec::NONE => return Ok(parsed),
                ec::NOT_COORDINATOR | ec::NOT_LEADER_OR_FOLLOWER
                    if attempt + 1 < COORDINATOR_ATTEMPTS =>
                {
                    let _ = self.router.refresh_topic(OFFSETS_TOPIC).await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                ec::COORDINATOR_LOAD_IN_PROGRESS if attempt + 1 < COORDINATOR_ATTEMPTS => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                other => ClientError::from_error_code(other)?,
            }
        }
        unreachable!("the last attempt returns or fails")
    }

    async fn join(
        &self,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        member_id: &str,
        subscription_topics: &[String],
    ) -> Result<JoinGroupResponse, ClientError> {
        let request = JoinGroupRequest {
            group_id: self.group_id.clone(),
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id: member_id.to_owned(),
            subscription_topics: subscription_topics.to_vec(),
        };
        let body = request.encode().map_err(msg_err)?;
        self.request(
            ApiKey::JoinGroup,
            &body,
            |bytes| JoinGroupResponse::decode(bytes).map_err(msg_err),
            |response| response.error_code,
        )
        .await
    }

    async fn sync(
        &self,
        generation: i32,
        member_id: &str,
        assignments: Vec<MemberAssignment>,
    ) -> Result<SyncGroupResponse, ClientError> {
        let request = SyncGroupRequest {
            group_id: self.group_id.clone(),
            generation,
            member_id: member_id.to_owned(),
            assignments,
        };
        let body = request.encode().map_err(msg_err)?;
        self.request(
            ApiKey::SyncGroup,
            &body,
            |bytes| SyncGroupResponse::decode(bytes).map_err(msg_err),
            |response| response.error_code,
        )
        .await
    }

    async fn heartbeat(
        &self,
        generation: i32,
        member_id: &str,
    ) -> Result<HeartbeatResponse, ClientError> {
        let request = HeartbeatRequest {
            group_id: self.group_id.clone(),
            generation,
            member_id: member_id.to_owned(),
        };
        let body = request.encode().map_err(msg_err)?;
        self.request(
            ApiKey::Heartbeat,
            &body,
            |bytes| HeartbeatResponse::decode(bytes).map_err(msg_err),
            |response| response.error_code,
        )
        .await
    }

    async fn commit(
        &self,
        generation: i32,
        member_id: &str,
        offsets: Vec<OffsetCommitEntry>,
    ) -> Result<OffsetCommitResponse, ClientError> {
        let request = OffsetCommitRequest {
            group_id: self.group_id.clone(),
            generation,
            member_id: member_id.to_owned(),
            offsets,
        };
        let body = request.encode().map_err(msg_err)?;
        self.request(
            ApiKey::OffsetCommit,
            &body,
            |bytes| OffsetCommitResponse::decode(bytes).map_err(msg_err),
            |response| response.error_code,
        )
        .await
    }

    async fn fetch_offsets(
        &self,
        partitions: Vec<AssignedPartition>,
    ) -> Result<Vec<OffsetFetchEntry>, ClientError> {
        let request = OffsetFetchRequest {
            group_id: self.group_id.clone(),
            partitions,
        };
        let body = request.encode().map_err(msg_err)?;
        let response = self
            .request(
                ApiKey::OffsetFetch,
                &body,
                |bytes| OffsetFetchResponse::decode(bytes).map_err(msg_err),
                |response| response.error_code,
            )
            .await?;
        Ok(response.offsets)
    }
}

/// Commit current positions with the current membership. Membership errors
/// mean a rebalance is needed: flag it and let the next poll rejoin and
/// recommit instead of failing the caller.
async fn commit_snapshot(
    coordinator: &GroupCoordinator,
    membership: &Arc<Mutex<Membership>>,
    positions: &Positions,
    rejoin: &AtomicBool,
) -> Result<(), ClientError> {
    let (generation, member_id, joined) = {
        let membership = membership.lock().expect("membership");
        (
            membership.generation,
            membership.member_id.clone(),
            membership.joined,
        )
    };
    if !joined {
        return Ok(());
    }
    let offsets: Vec<OffsetCommitEntry> = {
        let positions = positions.lock().expect("positions");
        positions
            .iter()
            .map(|((topic, partition), offset)| OffsetCommitEntry {
                topic: topic.clone(),
                partition: *partition,
                offset: *offset,
            })
            .collect()
    };
    if offsets.is_empty() {
        return Ok(());
    }
    match coordinator.commit(generation, &member_id, offsets).await {
        Ok(_) => Ok(()),
        Err(ClientError::Server {
            code: ec::UNKNOWN_MEMBER_ID | ec::ILLEGAL_GENERATION | ec::REBALANCE_IN_PROGRESS,
            ..
        }) => {
            rejoin.store(true, Ordering::Relaxed);
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// A consumer-group consumer: joins a group, polls its assigned partitions,
/// heartbeats and commits offsets in the background.
pub struct GroupConsumer {
    coordinator: GroupCoordinator,
    consumer: Consumer,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    assignor: Assignor,
    auto_commit: Option<Duration>,
    max_poll_records: usize,
    subscribed: Vec<String>,
    membership: Arc<Mutex<Membership>>,
    /// Next offset to *deliver* per partition — what gets committed. Only
    /// advances over records handed to the caller.
    positions: Positions,
    /// Next offset to *fetch* per partition. Runs ahead of `positions` by
    /// exactly the records sitting in `buffered`.
    fetch_positions: BTreeMap<TopicPartition, i64>,
    /// Records fetched but not yet returned by [`GroupConsumer::poll`].
    buffered: VecDeque<ConsumedRecord>,
    rejoin: Arc<AtomicBool>,
    assignment: Vec<TopicPartition>,
    assignment_version: Arc<AtomicU64>,
    heartbeat_task: Option<JoinHandle<()>>,
    auto_commit_task: Option<JoinHandle<()>>,
}

impl GroupConsumer {
    pub async fn connect(
        addr: SocketAddr,
        client_id: &str,
        group_id: &str,
    ) -> Result<GroupConsumer, ClientError> {
        GroupConsumer::connect_with(Transport::default(), addr, client_id, group_id).await
    }

    /// Connect over an explicit transport (must match the broker's).
    pub async fn connect_with(
        transport: Transport,
        addr: SocketAddr,
        client_id: &str,
        group_id: &str,
    ) -> Result<GroupConsumer, ClientError> {
        let router =
            BrokerRouter::connect_with(transport, addr, Some(client_id.to_owned()), 5).await?;
        let consumer = Consumer::from_router(router.clone(), 8 * 1024 * 1024);
        let group = GroupConsumer {
            coordinator: GroupCoordinator {
                router,
                group_id: group_id.to_owned(),
            },
            consumer,
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 3_000,
            assignor: Assignor::Range,
            auto_commit: Some(Duration::from_secs(5)),
            max_poll_records: DEFAULT_MAX_POLL_RECORDS,
            subscribed: Vec::new(),
            membership: Arc::new(Mutex::new(Membership::default())),
            positions: Arc::new(Mutex::new(BTreeMap::new())),
            fetch_positions: BTreeMap::new(),
            buffered: VecDeque::new(),
            rejoin: Arc::new(AtomicBool::new(false)),
            assignment: Vec::new(),
            assignment_version: Arc::new(AtomicU64::new(0)),
            heartbeat_task: None,
            auto_commit_task: None,
        };
        Ok(group)
    }

    /// Broker-side session timeout; heartbeats go out every timeout/3.
    pub fn with_session_timeout(mut self, session_timeout_ms: i32) -> Self {
        self.session_timeout_ms = session_timeout_ms;
        self
    }

    /// Maximum time the coordinator waits for members to rejoin.
    pub fn with_rebalance_timeout(mut self, rebalance_timeout_ms: i32) -> Self {
        self.rebalance_timeout_ms = rebalance_timeout_ms;
        self
    }

    /// Assignment strategy this member uses when it leads a rebalance.
    pub fn with_assignor(mut self, assignor: Assignor) -> Self {
        self.assignor = assignor;
        self
    }

    /// Background commit interval; `None` disables auto-commit (default 5s).
    pub fn with_auto_commit(mut self, interval: Option<Duration>) -> Self {
        self.auto_commit = interval;
        self
    }

    /// Maximum records one [`GroupConsumer::poll`] returns
    /// (`max.poll.records`, default 500). Records fetched beyond the cap
    /// stay buffered and are returned by later polls — the consumed
    /// position, and therefore every commit, only ever covers records the
    /// caller has actually received.
    pub fn with_max_poll_records(mut self, max_poll_records: usize) -> Self {
        self.set_max_poll_records(max_poll_records);
        self
    }

    /// Adjust `max.poll.records` between polls — a bounded reader shrinks it
    /// to its remaining budget so the final poll cannot hand back (and
    /// therefore consume) more records than the caller will process.
    pub fn set_max_poll_records(&mut self, max_poll_records: usize) {
        self.max_poll_records = max_poll_records.max(1);
    }

    /// Cap on response batch bytes per fetch.
    pub fn with_max_bytes(mut self, max_bytes: i32) -> Self {
        self.consumer = Consumer::from_router(self.coordinator.router.clone(), max_bytes);
        self
    }

    /// Set the subscription; the next [`GroupConsumer::poll`] (re)joins the
    /// group with it.
    pub fn subscribe(&mut self, topics: &[&str]) {
        self.subscribed = topics.iter().map(|topic| (*topic).to_owned()).collect();
        {
            let mut membership = self.membership.lock().expect("membership");
            membership.member_id = String::new();
            membership.joined = false;
        }
        self.rejoin.store(true, Ordering::Relaxed);
    }

    /// Current partition assignment, sorted by (topic, partition).
    pub fn assignment(&self) -> &[(String, i32)] {
        &self.assignment
    }

    /// Member id assigned by the coordinator (empty before the first join).
    pub fn member_id(&self) -> String {
        self.membership
            .lock()
            .expect("membership")
            .member_id
            .clone()
    }

    /// Current group generation (0 before the first join).
    pub fn generation(&self) -> i32 {
        self.membership.lock().expect("membership").generation
    }

    /// Bumped every time [`GroupConsumer::assignment`] changes.
    pub fn assignment_version(&self) -> u64 {
        self.assignment_version.load(Ordering::Relaxed)
    }

    /// Fetch records from all assigned partitions, waiting up to
    /// `max_wait` when caught up. Rejoins the group first when a heartbeat
    /// or commit signalled a rebalance. Returns at most `max.poll.records`;
    /// anything fetched beyond that stays buffered for the next poll and is
    /// *not* counted as consumed.
    pub async fn poll(&mut self, max_wait: Duration) -> Result<Vec<ConsumedRecord>, ClientError> {
        if self.subscribed.is_empty() {
            return Err(ClientError::Configuration(
                "subscribe to at least one topic before polling".into(),
            ));
        }
        if !self.is_joined() || self.rejoin.load(Ordering::Relaxed) {
            tracing::debug!(member = %self.membership.lock().expect("membership").member_id, "poll triggers (re)join");
            self.join().await?;
        }

        let deadline = Instant::now() + max_wait;
        loop {
            if !self.buffered.is_empty() {
                return Ok(self.take_buffered());
            }
            // One request per broker for the whole assignment, instead of
            // one per partition: per-poll latency stops scaling with the
            // number of partitions this member owns.
            let requests: Vec<(String, i32, i64)> = self
                .assignment
                .iter()
                .map(|(topic, partition)| {
                    let position = self
                        .fetch_positions
                        .get(&(topic.clone(), *partition))
                        .copied()
                        .unwrap_or(0);
                    (topic.clone(), *partition, position)
                })
                .collect();
            let wait_ms = i32::try_from(
                deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis()
                    .min(500),
            )
            .unwrap_or(i32::MAX);

            for fetched in self.consumer.fetch_many(&requests, wait_ms).await? {
                match fetched.error_code {
                    ec::NONE => {
                        for (offset, key, value) in fetched.records {
                            self.fetch_positions
                                .insert((fetched.topic.clone(), fetched.partition), offset + 1);
                            self.buffered.push_back(ConsumedRecord {
                                topic: fetched.topic.clone(),
                                partition: fetched.partition,
                                offset,
                                key,
                                value,
                            });
                        }
                    }
                    ec::OFFSET_OUT_OF_RANGE => {
                        // The committed offset fell off the log: restart at
                        // earliest, dropping anything buffered for it.
                        let earliest = self
                            .consumer
                            .list_offsets(&fetched.topic, fetched.partition, EARLIEST)
                            .await?;
                        self.buffered.retain(|record| {
                            record.topic != fetched.topic || record.partition != fetched.partition
                        });
                        self.fetch_positions
                            .insert((fetched.topic.clone(), fetched.partition), earliest);
                        self.positions
                            .lock()
                            .expect("positions")
                            .insert((fetched.topic, fetched.partition), earliest);
                    }
                    ec::NOT_LEADER_OR_FOLLOWER => {
                        // Leadership moved mid-poll; refresh routes and let
                        // the next sweep pick up the new leader.
                        let _ = self.consumer.refresh_topic(&fetched.topic).await;
                    }
                    code => ClientError::from_error_code(code)?,
                }
            }
            if !self.buffered.is_empty() {
                return Ok(self.take_buffered());
            }
            if Instant::now() >= deadline {
                return Ok(Vec::new());
            }
            tokio::time::sleep(POLL_IDLE_DELAY).await;
        }
    }

    /// Hand out up to `max.poll.records` buffered records, advancing the
    /// consumed position over exactly those records.
    fn take_buffered(&mut self) -> Vec<ConsumedRecord> {
        let count = self.buffered.len().min(self.max_poll_records);
        let records: Vec<ConsumedRecord> = self.buffered.drain(..count).collect();
        let mut positions = self.positions.lock().expect("positions");
        for record in &records {
            positions.insert((record.topic.clone(), record.partition), record.offset + 1);
        }
        records
    }

    /// Commit current positions for the assigned partitions now.
    pub async fn commit_sync(&self) -> Result<(), ClientError> {
        commit_snapshot(
            &self.coordinator,
            &self.membership,
            &self.positions,
            &self.rejoin,
        )
        .await
    }

    /// Commit current positions, then stop the background tasks (also
    /// aborted on drop).
    pub async fn close(self) -> Result<(), ClientError> {
        self.commit_sync().await
    }

    fn is_joined(&self) -> bool {
        self.membership.lock().expect("membership").joined
    }

    /// Join + sync until the group hands this member an assignment.
    async fn join(&mut self) -> Result<(), ClientError> {
        for _ in 0..JOIN_ATTEMPTS {
            let member_id = self
                .membership
                .lock()
                .expect("membership")
                .member_id
                .clone();
            let join = match self
                .coordinator
                .join(
                    self.session_timeout_ms,
                    self.rebalance_timeout_ms,
                    &member_id,
                    &self.subscribed,
                )
                .await
            {
                Ok(response) => response,
                Err(ClientError::Server { code, .. }) if code == ec::UNKNOWN_MEMBER_ID => {
                    // Stale member id: rejoin as a brand-new member.
                    self.membership.lock().expect("membership").member_id = String::new();
                    continue;
                }
                Err(error) => return Err(error),
            };
            {
                let mut membership = self.membership.lock().expect("membership");
                membership.member_id = join.member_id.clone();
                membership.generation = join.generation;
            }
            tracing::debug!(member = %join.member_id, gen = join.generation, leader = %join.leader_member_id, "joined group");

            let assignments = if join.member_id == join.leader_member_id {
                self.leader_assignments(&join.members).await?
            } else {
                Vec::new()
            };
            let sync = match self
                .coordinator
                .sync(join.generation, &join.member_id, assignments)
                .await
            {
                Ok(response) => response,
                Err(ClientError::Server { code, .. })
                    if code == ec::REBALANCE_IN_PROGRESS
                        || code == ec::UNKNOWN_MEMBER_ID
                        || code == ec::ILLEGAL_GENERATION =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };

            let assignment: Vec<TopicPartition> = sync
                .assignment
                .iter()
                .map(|assigned| (assigned.topic.clone(), assigned.partition))
                .collect();
            tracing::debug!(member = %join.member_id, gen = join.generation, ?assignment, "synced assignment");
            self.install_assignment(assignment).await?;
            self.membership.lock().expect("membership").joined = true;
            self.rejoin.store(false, Ordering::Relaxed);
            self.spawn_heartbeat();
            self.spawn_auto_commit();
            return Ok(());
        }
        Err(ClientError::Configuration(format!(
            "consumer group failed to stabilize after {JOIN_ATTEMPTS} join attempts"
        )))
    }

    /// Leader side of SyncGroup: assign the union of subscribed
    /// topic-partitions over the member list with the configured assignor.
    async fn leader_assignments(
        &self,
        members: &[GroupMemberInfo],
    ) -> Result<Vec<MemberAssignment>, ClientError> {
        let mut topic_partitions = BTreeMap::new();
        for topic in members
            .iter()
            .flat_map(|member| member.subscription_topics.iter())
        {
            if let std::collections::btree_map::Entry::Vacant(entry) =
                topic_partitions.entry(topic.clone())
            {
                entry.insert(self.coordinator.router.partitions(topic).await?);
            }
        }
        let member_list: Vec<(String, Vec<String>)> = members
            .iter()
            .map(|member| (member.member_id.clone(), member.subscription_topics.clone()))
            .collect();
        Ok(self
            .assignor
            .assign(&member_list, &topic_partitions)
            .into_iter()
            .map(|(member_id, partitions)| MemberAssignment {
                member_id,
                partitions: partitions
                    .into_iter()
                    .map(|(topic, partition)| AssignedPartition { topic, partition })
                    .collect(),
            })
            .collect())
    }

    /// Adopt a new assignment: drop revoked positions, seed new partitions
    /// from the group's committed offset (or the partition's earliest).
    async fn install_assignment(
        &mut self,
        assignment: Vec<TopicPartition>,
    ) -> Result<(), ClientError> {
        let mut assignment = assignment;
        assignment.sort();

        let needed: Vec<TopicPartition> = {
            let positions = self.positions.lock().expect("positions");
            assignment
                .iter()
                .filter(|tp| !positions.contains_key(*tp))
                .cloned()
                .collect()
        };
        let committed: HashMap<TopicPartition, i64> = if needed.is_empty() {
            HashMap::new()
        } else {
            let partitions = needed
                .iter()
                .map(|(topic, partition)| AssignedPartition {
                    topic: topic.clone(),
                    partition: *partition,
                })
                .collect();
            self.coordinator
                .fetch_offsets(partitions)
                .await?
                .into_iter()
                .map(|entry| ((entry.topic, entry.partition), entry.offset))
                .collect()
        };
        let mut seeded = Vec::with_capacity(needed.len());
        for (topic, partition) in needed {
            let offset = match committed.get(&(topic.clone(), partition)) {
                Some(&offset) if offset >= 0 => offset,
                _ => {
                    self.consumer
                        .list_offsets(&topic, partition, EARLIEST)
                        .await?
                }
            };
            seeded.push(((topic, partition), offset));
        }

        {
            let mut positions = self.positions.lock().expect("positions");
            positions.retain(|tp, _| assignment.binary_search(tp).is_ok());
            for (tp, offset) in seeded {
                positions.insert(tp, offset);
            }
            // Buffered records sit ahead of the consumed position and were
            // never delivered, so a new assignment simply drops them: fetch
            // resumes from the consumed position on partitions still owned.
            self.buffered.clear();
            self.fetch_positions = positions.clone();
        }
        if self.assignment != assignment {
            self.assignment = assignment;
            self.assignment_version.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn spawn_heartbeat(&mut self) {
        if self.heartbeat_task.is_some() {
            return;
        }
        let coordinator = self.coordinator.clone();
        let membership = Arc::clone(&self.membership);
        let rejoin = Arc::clone(&self.rejoin);
        let interval = Duration::from_millis(
            u64::try_from(self.session_timeout_ms / 3)
                .unwrap_or(1)
                .max(1),
        );
        self.heartbeat_task = Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let (generation, member_id, joined) = {
                    let membership = membership.lock().expect("membership");
                    (
                        membership.generation,
                        membership.member_id.clone(),
                        membership.joined,
                    )
                };
                if !joined {
                    continue;
                }
                match coordinator.heartbeat(generation, &member_id).await {
                    Ok(_) => {}
                    Err(ClientError::Server {
                        code:
                            ec::REBALANCE_IN_PROGRESS | ec::UNKNOWN_MEMBER_ID | ec::ILLEGAL_GENERATION,
                        ..
                    }) => {
                        tracing::debug!(%member_id, generation, "heartbeat demands rejoin");
                        rejoin.store(true, Ordering::Relaxed);
                    }
                    Err(_) => {} // transient failure: retry next tick
                }
            }
        }));
    }

    fn spawn_auto_commit(&mut self) {
        if self.auto_commit_task.is_some() {
            return;
        }
        let Some(interval) = self.auto_commit else {
            return;
        };
        let coordinator = self.coordinator.clone();
        let membership = Arc::clone(&self.membership);
        let positions = Arc::clone(&self.positions);
        let rejoin = Arc::clone(&self.rejoin);
        self.auto_commit_task = Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(error) =
                    commit_snapshot(&coordinator, &membership, &positions, &rejoin).await
                {
                    tracing::debug!(%error, "auto-commit failed");
                }
            }
        }));
    }
}

impl Drop for GroupConsumer {
    fn drop(&mut self) {
        if let Some(task) = self.heartbeat_task.take() {
            task.abort();
        }
        if let Some(task) = self.auto_commit_task.take() {
            task.abort();
        }
    }
}

pub(crate) fn msg_err(e: std::io::Error) -> ClientError {
    ClientError::Protocol(ProtocolError::Message(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members(list: &[(&str, &[&str])]) -> Vec<(String, Vec<String>)> {
        list.iter()
            .map(|(id, topics)| {
                (
                    (*id).to_owned(),
                    topics.iter().map(|t| (*t).to_owned()).collect(),
                )
            })
            .collect()
    }

    fn topics(list: &[(&str, i32)]) -> BTreeMap<String, Vec<i32>> {
        list.iter()
            .map(|(name, count)| ((*name).to_owned(), (0..*count).collect()))
            .collect()
    }

    #[test]
    fn range_distributes_contiguous_ranges_per_topic() {
        let members = members(&[("a", &["t"]), ("b", &["t"])]);
        let topics = topics(&[("t", 5)]);
        let assignment = Assignor::Range.assign(&members, &topics);
        assert_eq!(
            assignment["a"],
            vec![
                ("t".to_owned(), 0),
                ("t".to_owned(), 1),
                ("t".to_owned(), 2)
            ]
        );
        assert_eq!(
            assignment["b"],
            vec![("t".to_owned(), 3), ("t".to_owned(), 4)]
        );
    }

    #[test]
    fn range_handles_uneven_and_multi_topic_subscriptions() {
        let members = members(&[("a", &["t1", "t2"]), ("b", &["t1"])]);
        let topics = topics(&[("t1", 3), ("t2", 2)]);
        let assignment = Assignor::Range.assign(&members, &topics);
        assert_eq!(
            assignment["a"],
            vec![
                ("t1".to_owned(), 0),
                ("t1".to_owned(), 1),
                ("t2".to_owned(), 0),
                ("t2".to_owned(), 1)
            ]
        );
        assert_eq!(assignment["b"], vec![("t1".to_owned(), 2)]);
    }

    #[test]
    fn roundrobin_deals_partitions_across_members() {
        let members = members(&[("a", &["t"]), ("b", &["t"])]);
        let topics = topics(&[("t", 5)]);
        let assignment = Assignor::RoundRobin.assign(&members, &topics);
        assert_eq!(
            assignment["a"],
            vec![
                ("t".to_owned(), 0),
                ("t".to_owned(), 2),
                ("t".to_owned(), 4)
            ]
        );
        assert_eq!(
            assignment["b"],
            vec![("t".to_owned(), 1), ("t".to_owned(), 3)]
        );
    }

    #[test]
    fn roundrobin_skips_members_not_subscribed_to_a_topic() {
        let members = members(&[("a", &["t1"]), ("b", &["t1", "t2"])]);
        let topics = topics(&[("t1", 2), ("t2", 2)]);
        let assignment = Assignor::RoundRobin.assign(&members, &topics);
        assert_eq!(assignment["a"], vec![("t1".to_owned(), 0)]);
        assert_eq!(
            assignment["b"],
            vec![
                ("t1".to_owned(), 1),
                ("t2".to_owned(), 0),
                ("t2".to_owned(), 1)
            ]
        );
    }

    #[test]
    fn single_member_gets_everything() {
        for assignor in [Assignor::Range, Assignor::RoundRobin] {
            let members = members(&[("only", &["t"])]);
            let topics = topics(&[("t", 4)]);
            let assignment = assignor.assign(&members, &topics);
            assert_eq!(
                assignment["only"],
                (0..4).map(|p| ("t".to_owned(), p)).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn member_leaving_reassigns_its_partitions() {
        for assignor in [Assignor::Range, Assignor::RoundRobin] {
            let three = members(&[("a", &["t"]), ("b", &["t"]), ("c", &["t"])]);
            let topics = topics(&[("t", 3)]);
            let before = assignor.assign(&three, &topics);
            assert_eq!(before["b"], vec![("t".to_owned(), 1)]);

            let two = members(&[("a", &["t"]), ("c", &["t"])]);
            let after = assignor.assign(&two, &topics);
            let mut assigned: Vec<i32> = after
                .values()
                .flat_map(|tps| tps.iter().map(|(_, p)| *p))
                .collect();
            assigned.sort_unstable();
            assert_eq!(assigned, vec![0, 1, 2], "all partitions stay assigned");
        }
    }

    #[test]
    fn every_member_appears_even_with_no_partitions() {
        let members = members(&[("a", &["t"]), ("b", &["other"])]);
        let topics = topics(&[("t", 1)]);
        for assignor in [Assignor::Range, Assignor::RoundRobin] {
            let assignment = assignor.assign(&members, &topics);
            assert_eq!(assignment["a"], vec![("t".to_owned(), 0)]);
            assert!(assignment["b"].is_empty());
        }
    }
}
