//! M4 consumer-group coordinator integration tests (Blueprint 05).

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use brahmaputra_broker::{Broker, BrokerConfig};
use brahmaputra_client::Connection;
use brahmaputra_metadata::{
    BrokerMetadata, ClusterMetadata, MetadataCache, NodeRole, PartitionMetadata, TopicMetadata,
};
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{
    AssignedPartition, DescribeGroupRequest, DescribeGroupResponse, HeartbeatRequest,
    HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, ListGroupsRequest, ListGroupsResponse,
    MemberAssignment, OffsetCommitEntry, OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchResponse, SyncGroupRequest, SyncGroupResponse,
};
use brahmaputra_protocol::ApiKey;
use futures::FutureExt;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const OFFSETS_TOPIC: &str = "__consumer_offsets";

struct RunningBroker {
    addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

async fn run_broker(broker: Broker) -> RunningBroker {
    let broker = Arc::new(broker);
    let addr = broker.local_addr();
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        broker
            .run(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    RunningBroker {
        addr,
        shutdown,
        task,
    }
}

async fn stop_broker(running: RunningBroker) {
    let _ = running.shutdown.send(());
    running.task.await.unwrap();
}

async fn start_standalone(data_dir: &Path, default_partitions: i32) -> RunningBroker {
    let broker = Broker::bind(BrokerConfig {
        port: 0,
        data_dir: data_dir.to_owned(),
        default_partitions,
        ..BrokerConfig::default()
    })
    .await
    .unwrap();
    run_broker(broker).await
}

async fn start_cluster_broker(
    broker_id: i32,
    data_dir: &Path,
    cache: MetadataCache,
) -> RunningBroker {
    let broker_epoch = cache
        .snapshot()
        .brokers
        .get(&broker_id)
        .map(|broker| broker.broker_epoch);
    let broker = Broker::bind(BrokerConfig {
        broker_id,
        broker_epoch,
        port: 0,
        data_dir: data_dir.to_owned(),
        metadata_cache: Some(cache),
        replication_enabled: true,
        ..BrokerConfig::default()
    })
    .await
    .unwrap();
    run_broker(broker).await
}

fn broker_metadata(broker_id: i32) -> BrokerMetadata {
    BrokerMetadata {
        broker_id,
        host: "127.0.0.1".into(),
        data_port: 0,
        control_port: 0,
        broker_epoch: 100 + broker_id as u64,
        roles: BTreeSet::from([NodeRole::Broker]),
        rack: None,
        alive: true,
        last_heartbeat_ms: 1_000,
    }
}

/// Two brokers sharing the offsets topic; partition 0 led by broker 1,
/// partition 1 led by broker 2, each with a leader-only ISR so leader
/// appends commit without live followers.
fn cluster_image() -> ClusterMetadata {
    let partition = |partition: i32, leader: i32| PartitionMetadata {
        partition,
        replicas: vec![1, 2],
        leader,
        isr: vec![leader],
        leader_epoch: 1,
    };
    ClusterMetadata {
        cluster_id: "group-coordinator-test".into(),
        offset: 1,
        controller_id: Some(1),
        brokers: BTreeMap::from([(1, broker_metadata(1)), (2, broker_metadata(2))]),
        topics: BTreeMap::from([(
            OFFSETS_TOPIC.into(),
            TopicMetadata {
                name: OFFSETS_TOPIC.into(),
                replication_factor: 1,
                partitions: BTreeMap::from([(0, partition(0, 1)), (1, partition(1, 2))]),
                configs: BTreeMap::new(),
            },
        )]),
        users: BTreeMap::new(),
        acls: Default::default(),
        jwt_secret: None,
    }
}

async fn connect(addr: SocketAddr, client_id: &str) -> Connection {
    Connection::connect(addr, Some(client_id.into()), 16)
        .await
        .unwrap()
}

async fn join(
    conn: &Connection,
    group: &str,
    member_id: &str,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
) -> JoinGroupResponse {
    let request = JoinGroupRequest {
        group_id: group.into(),
        session_timeout_ms,
        rebalance_timeout_ms,
        member_id: member_id.into(),
        subscription_topics: vec!["events".into()],
    };
    let body = request.encode().unwrap();
    let response = conn.request(ApiKey::JoinGroup, &body).await.unwrap();
    JoinGroupResponse::decode(&response).unwrap()
}

async fn sync(
    conn: &Connection,
    group: &str,
    generation: i32,
    member_id: &str,
    assignments: Vec<MemberAssignment>,
) -> SyncGroupResponse {
    let request = SyncGroupRequest {
        group_id: group.into(),
        generation,
        member_id: member_id.into(),
        assignments,
    };
    let body = request.encode().unwrap();
    let response = conn.request(ApiKey::SyncGroup, &body).await.unwrap();
    SyncGroupResponse::decode(&response).unwrap()
}

async fn heartbeat(conn: &Connection, group: &str, generation: i32, member_id: &str) -> i32 {
    let request = HeartbeatRequest {
        group_id: group.into(),
        generation,
        member_id: member_id.into(),
    };
    let body = request.encode().unwrap();
    let response = conn.request(ApiKey::Heartbeat, &body).await.unwrap();
    HeartbeatResponse::decode(&response).unwrap().error_code
}

async fn commit(
    conn: &Connection,
    group: &str,
    generation: i32,
    member_id: &str,
    offsets: Vec<OffsetCommitEntry>,
) -> i32 {
    let request = OffsetCommitRequest {
        group_id: group.into(),
        generation,
        member_id: member_id.into(),
        offsets,
    };
    let body = request.encode().unwrap();
    let response = conn.request(ApiKey::OffsetCommit, &body).await.unwrap();
    OffsetCommitResponse::decode(&response).unwrap().error_code
}

async fn fetch_offsets(
    conn: &Connection,
    group: &str,
    partitions: Vec<AssignedPartition>,
) -> OffsetFetchResponse {
    let request = OffsetFetchRequest {
        group_id: group.into(),
        partitions,
    };
    let body = request.encode().unwrap();
    let response = conn.request(ApiKey::OffsetFetch, &body).await.unwrap();
    OffsetFetchResponse::decode(&response).unwrap()
}

async fn list_groups(conn: &Connection, states: &[&str]) -> ListGroupsResponse {
    let request = ListGroupsRequest {
        states: states.iter().map(|state| (*state).to_owned()).collect(),
    };
    let body = request.encode().unwrap();
    let response = conn.request(ApiKey::ListGroups, &body).await.unwrap();
    ListGroupsResponse::decode(&response).unwrap()
}

async fn describe_group(conn: &Connection, group: &str) -> DescribeGroupResponse {
    let request = DescribeGroupRequest {
        group_id: group.into(),
    };
    let body = request.encode().unwrap();
    let response = conn.request(ApiKey::DescribeGroup, &body).await.unwrap();
    DescribeGroupResponse::decode(&response).unwrap()
}

fn entry(topic: &str, partition: i32, offset: i64) -> OffsetCommitEntry {
    OffsetCommitEntry {
        topic: topic.into(),
        partition,
        offset,
    }
}

fn assigned(topic: &str, partition: i32) -> AssignedPartition {
    AssignedPartition {
        topic: topic.into(),
        partition,
    }
}

fn assigned_tuples(assignment: &[AssignedPartition]) -> Vec<(String, i32)> {
    assignment
        .iter()
        .map(|p| (p.topic.clone(), p.partition))
        .collect()
}

fn offset_tuples(
    entries: &[brahmaputra_protocol::gen::OffsetFetchEntry],
) -> Vec<(String, i32, i64)> {
    entries
        .iter()
        .map(|e| (e.topic.clone(), e.partition, e.offset))
        .collect()
}

/// Poll `f` until it returns true or the budget runs out.
async fn eventually<F, Fut>(mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if f().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition never became true"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn join_sync_rebalance_and_fencing() {
    let temp = TempDir::new().unwrap();
    let broker = start_standalone(&temp.path().join("broker"), 4).await;
    let conn = connect(broker.addr, "groups").await;

    // First member joins an Empty group: immediate completion, gen 1, self
    // as leader with the full member list.
    let first = join(&conn, "g1", "", 10_000, 500).await;
    assert_eq!(first.error_code, ec::NONE);
    assert_eq!(first.generation, 1);
    assert_eq!(first.member_id, "member-0");
    assert_eq!(first.leader_member_id, "member-0");
    assert_eq!(first.members.len(), 1);

    let leader = sync(
        &conn,
        "g1",
        1,
        "member-0",
        vec![MemberAssignment {
            member_id: "member-0".into(),
            partitions: vec![assigned("events", 0)],
        }],
    )
    .await;
    assert_eq!(leader.error_code, ec::NONE);
    assert_eq!(
        assigned_tuples(&leader.assignment),
        vec![("events".to_owned(), 0)]
    );
    assert_eq!(heartbeat(&conn, "g1", 1, "member-0").await, ec::NONE);

    // A second member joining a Stable group triggers a rebalance.
    let second_conn = connect(broker.addr, "groups-two").await;
    let second_join = tokio::spawn(async move { join(&second_conn, "g1", "", 10_000, 500).await });
    // The group bumps to generation 2, so member-0's stale-generation
    // heartbeat is fenced until it rejoins.
    eventually(|| {
        heartbeat(&conn, "g1", 1, "member-0")
            .then(|code| async move { code == ec::ILLEGAL_GENERATION })
    })
    .await;
    // The awaited member rejoins, completing the rebalance at generation 2.
    let rejoin = join(&conn, "g1", "member-0", 10_000, 500).await;
    assert_eq!(rejoin.error_code, ec::NONE);
    assert_eq!(rejoin.generation, 2);
    let second = second_join.await.unwrap();
    assert_eq!(second.error_code, ec::NONE);
    assert_eq!(second.generation, 2);
    assert_eq!(second.member_id, "member-1");
    assert_eq!(second.leader_member_id, "member-0");
    assert!(second.members.is_empty(), "only the leader gets members");

    // Stale-generation heartbeat is fenced; a current-generation heartbeat
    // while the group awaits the leader's assignment is fine — AwaitingSync
    // is a normal transient state and only PreparingRebalance fences.
    assert_eq!(
        heartbeat(&conn, "g1", 1, "member-0").await,
        ec::ILLEGAL_GENERATION
    );
    assert_eq!(heartbeat(&conn, "g1", 2, "member-1").await, ec::NONE);

    // The leader distributes assignments; the follower learns its own.
    let leader_sync = sync(
        &conn,
        "g1",
        2,
        "member-0",
        vec![
            MemberAssignment {
                member_id: "member-0".into(),
                partitions: vec![assigned("events", 0)],
            },
            MemberAssignment {
                member_id: "member-1".into(),
                partitions: vec![assigned("events", 1)],
            },
        ],
    )
    .await;
    assert_eq!(leader_sync.error_code, ec::NONE);
    assert_eq!(
        assigned_tuples(&leader_sync.assignment),
        vec![("events".to_owned(), 0)]
    );
    let follower_sync = sync(&conn, "g1", 2, "member-1", vec![]).await;
    assert_eq!(follower_sync.error_code, ec::NONE);
    assert_eq!(
        assigned_tuples(&follower_sync.assignment),
        vec![("events".to_owned(), 1)]
    );
    assert_eq!(heartbeat(&conn, "g1", 2, "member-0").await, ec::NONE);
    assert_eq!(heartbeat(&conn, "g1", 2, "member-1").await, ec::NONE);

    // Fencing: unknown member and stale generation.
    assert_eq!(
        heartbeat(&conn, "g1", 2, "member-999").await,
        ec::UNKNOWN_MEMBER_ID
    );
    assert_eq!(
        commit(&conn, "g1", 1, "member-0", vec![entry("events", 0, 5)]).await,
        ec::ILLEGAL_GENERATION
    );
    assert_eq!(
        commit(&conn, "g1", 2, "member-1", vec![entry("events", 1, 9)]).await,
        ec::NONE
    );

    stop_broker(broker).await;
}

#[tokio::test]
async fn session_expiry_evicts_member_and_rebalances() {
    let temp = TempDir::new().unwrap();
    let broker = start_standalone(&temp.path().join("broker"), 4).await;
    let conn = connect(broker.addr, "expiry").await;

    // member-0 has a long session; member-1 a short one (300ms).
    let first = join(&conn, "g2", "", 30_000, 500).await;
    assert_eq!(first.error_code, ec::NONE);
    let second_conn = connect(broker.addr, "expiry-two").await;
    let second_join = tokio::spawn(async move { join(&second_conn, "g2", "", 300, 500).await });
    // Wait for the rebalance to actually start (member-0's generation-1
    // heartbeat gets fenced) so the rejoin below joins generation 2 rather
    // than starting a rebalance of its own.
    eventually(|| {
        heartbeat(&conn, "g2", 1, "member-0")
            .then(|code| async move { code == ec::ILLEGAL_GENERATION })
    })
    .await;
    let rejoin = join(&conn, "g2", "member-0", 30_000, 500).await;
    assert_eq!(rejoin.generation, 2);
    let second = second_join.await.unwrap();
    assert_eq!(second.generation, 2);
    let leader_sync = sync(
        &conn,
        "g2",
        2,
        "member-0",
        vec![
            MemberAssignment {
                member_id: "member-0".into(),
                partitions: vec![assigned("events", 0)],
            },
            MemberAssignment {
                member_id: "member-1".into(),
                partitions: vec![],
            },
        ],
    )
    .await;
    assert_eq!(leader_sync.error_code, ec::NONE);
    assert_eq!(
        sync(&conn, "g2", 2, "member-1", vec![]).await.error_code,
        ec::NONE
    );
    assert_eq!(heartbeat(&conn, "g2", 2, "member-1").await, ec::NONE);

    // Stop member-1's heartbeats: the sweeper (100ms tick) evicts it and the
    // group rebalances into generation 3, fencing member-0's generation-2
    // heartbeat.
    eventually(|| {
        heartbeat(&conn, "g2", 2, "member-0")
            .then(|code| async move { code == ec::ILLEGAL_GENERATION })
    })
    .await;
    assert_eq!(
        heartbeat(&conn, "g2", 2, "member-1").await,
        ec::UNKNOWN_MEMBER_ID
    );

    // member-0 rejoins into generation 3 and the group stabilizes.
    let rejoin = join(&conn, "g2", "member-0", 30_000, 500).await;
    assert_eq!(rejoin.error_code, ec::NONE);
    assert_eq!(rejoin.generation, 3);
    let synced = sync(
        &conn,
        "g2",
        3,
        "member-0",
        vec![MemberAssignment {
            member_id: "member-0".into(),
            partitions: vec![assigned("events", 0)],
        }],
    )
    .await;
    assert_eq!(synced.error_code, ec::NONE);
    assert_eq!(heartbeat(&conn, "g2", 3, "member-0").await, ec::NONE);

    stop_broker(broker).await;
}

#[tokio::test]
async fn committed_offsets_and_group_survive_broker_restart() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("broker");

    let broker = start_standalone(&data_dir, 4).await;
    let conn = connect(broker.addr, "reload").await;
    let joined = join(&conn, "g3", "", 30_000, 500).await;
    assert_eq!(joined.error_code, ec::NONE);
    let synced = sync(
        &conn,
        "g3",
        1,
        "member-0",
        vec![MemberAssignment {
            member_id: "member-0".into(),
            partitions: vec![assigned("events", 0)],
        }],
    )
    .await;
    assert_eq!(synced.error_code, ec::NONE);
    assert_eq!(
        commit(
            &conn,
            "g3",
            1,
            "member-0",
            vec![entry("events", 0, 42), entry("events", 1, 7)],
        )
        .await,
        ec::NONE
    );
    // A standalone commit outside any membership is also accepted.
    assert_eq!(
        commit(&conn, "g3", -1, "", vec![entry("events", 2, 3)]).await,
        ec::NONE
    );
    let fetched = fetch_offsets(&conn, "g3", vec![]).await;
    assert_eq!(fetched.error_code, ec::NONE);
    assert_eq!(fetched.offsets.len(), 3);
    let missing = fetch_offsets(&conn, "g3", vec![assigned("events", 3)]).await;
    assert_eq!(missing.offsets[0].offset, -1);
    stop_broker(broker).await;

    // Restart on the same data dir: the shard replays the offsets log.
    let broker = start_standalone(&data_dir, 4).await;
    let conn = connect(broker.addr, "reload").await;
    let fetched = fetch_offsets(&conn, "g3", vec![]).await;
    assert_eq!(fetched.error_code, ec::NONE);
    assert_eq!(
        offset_tuples(&fetched.offsets),
        vec![
            ("events".to_owned(), 0, 42),
            ("events".to_owned(), 1, 7),
            ("events".to_owned(), 2, 3),
        ]
    );
    // Group metadata replayed too: the member heartbeats with generation 1.
    assert_eq!(heartbeat(&conn, "g3", 1, "member-0").await, ec::NONE);
    stop_broker(broker).await;
}

#[tokio::test]
async fn cluster_coordinator_serves_groups_and_fences_non_leaders() {
    let temp = TempDir::new().unwrap();
    let cache = MetadataCache::new(cluster_image());
    let broker_one = start_cluster_broker(1, &temp.path().join("broker-1"), cache.clone()).await;
    let broker_two = start_cluster_broker(2, &temp.path().join("broker-2"), cache.clone()).await;

    // Find the coordinator for the group by probing OffsetFetch.
    let conn_one = connect(broker_one.addr, "probe-one").await;
    let probe = fetch_offsets(&conn_one, "cg", vec![]).await;
    let (coordinator_id, coordinator, other) = if probe.error_code == ec::NONE {
        (1, broker_one, broker_two)
    } else {
        assert_eq!(probe.error_code, ec::NOT_COORDINATOR);
        (2, broker_two, broker_one)
    };

    // The non-leader of the offsets partition is not the coordinator.
    let other_conn = connect(other.addr, "probe-other").await;
    assert_eq!(
        fetch_offsets(&other_conn, "cg", vec![]).await.error_code,
        ec::NOT_COORDINATOR
    );
    let rejected = JoinGroupRequest {
        group_id: "cg".into(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 500,
        member_id: String::new(),
        subscription_topics: vec!["events".into()],
    };
    let body = rejected.encode().unwrap();
    let response = other_conn.request(ApiKey::JoinGroup, &body).await.unwrap();
    assert_eq!(
        JoinGroupResponse::decode(&response).unwrap().error_code,
        ec::NOT_COORDINATOR
    );

    // Full membership + commit flow against the coordinator.
    let conn = connect(coordinator.addr, "cluster-groups").await;
    let joined = join(&conn, "cg", "", 30_000, 500).await;
    assert_eq!(joined.error_code, ec::NONE);
    assert_eq!(joined.generation, 1);
    let synced = sync(
        &conn,
        "cg",
        1,
        "member-0",
        vec![MemberAssignment {
            member_id: "member-0".into(),
            partitions: vec![assigned("events", 0)],
        }],
    )
    .await;
    assert_eq!(synced.error_code, ec::NONE);
    assert_eq!(heartbeat(&conn, "cg", 1, "member-0").await, ec::NONE);
    assert_eq!(
        commit(&conn, "cg", 1, "member-0", vec![entry("events", 0, 11)]).await,
        ec::NONE
    );
    let fetched = fetch_offsets(&conn, "cg", vec![]).await;
    assert_eq!(fetched.offsets.len(), 1);
    assert_eq!(fetched.offsets[0].offset, 11);
    drop(conn);
    stop_broker(coordinator).await;

    // Coordinator restart on the same data dir: offsets and group metadata
    // are rebuilt by replaying the local `__consumer_offsets` partition.
    let coordinator = start_cluster_broker(
        coordinator_id,
        &temp.path().join(format!("broker-{coordinator_id}")),
        cache.clone(),
    )
    .await;
    let conn = connect(coordinator.addr, "cluster-groups").await;
    let fetched = fetch_offsets(&conn, "cg", vec![]).await;
    assert_eq!(fetched.error_code, ec::NONE);
    assert_eq!(fetched.offsets.len(), 1);
    assert_eq!(fetched.offsets[0].offset, 11);
    assert_eq!(heartbeat(&conn, "cg", 1, "member-0").await, ec::NONE);

    stop_broker(coordinator).await;
    stop_broker(other).await;
}

#[tokio::test]
async fn list_and_describe_expose_membership_offsets_and_survive_restart() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("broker");
    let broker = start_standalone(&data_dir, 4).await;
    let conn = connect(broker.addr, "observability").await;

    let joined = join(&conn, "g5", "", 30_000, 500).await;
    assert_eq!(joined.error_code, ec::NONE);
    let synced = sync(
        &conn,
        "g5",
        1,
        "member-0",
        vec![MemberAssignment {
            member_id: "member-0".into(),
            partitions: vec![assigned("events", 0), assigned("events", 1)],
        }],
    )
    .await;
    assert_eq!(synced.error_code, ec::NONE);
    assert_eq!(
        commit(&conn, "g5", 1, "member-0", vec![entry("events", 0, 17)]).await,
        ec::NONE
    );

    let listed = list_groups(&conn, &[]).await;
    assert_eq!(listed.error_code, ec::NONE);
    let group = listed
        .groups
        .iter()
        .find(|group| group.group_id == "g5")
        .expect("g5 listed");
    assert_eq!(group.state, "Stable");
    assert_eq!(group.generation, 1);
    assert_eq!(group.member_count, 1);
    // Coordinator partition is crc32c(group) % partitions, as the client
    // computes it independently.
    assert_eq!(
        group.coordinator_partition,
        (crc32c::crc32c(b"g5") % 4) as i32
    );

    // State filter.
    assert!(list_groups(&conn, &["Stable"])
        .await
        .groups
        .iter()
        .any(|group| group.group_id == "g5"));
    assert!(list_groups(&conn, &["PreparingRebalance"])
        .await
        .groups
        .is_empty());

    let described = describe_group(&conn, "g5").await;
    assert_eq!(described.error_code, ec::NONE);
    assert_eq!(described.state, "Stable");
    assert_eq!(described.leader_member_id, "member-0");
    assert_eq!(described.members.len(), 1);
    assert_eq!(described.members[0].member_id, "member-0");
    assert_eq!(described.members[0].subscription_topics, vec!["events"]);
    assert_eq!(
        assigned_tuples(&described.members[0].assignment),
        vec![("events".to_owned(), 0), ("events".to_owned(), 1)]
    );
    assert_eq!(
        offset_tuples(&described.offsets),
        vec![("events".to_owned(), 0, 17)]
    );

    // An unknown group is not a coordinator error: the coordinator is right,
    // the group simply does not exist.
    assert_eq!(
        describe_group(&conn, "never-existed").await.error_code,
        ec::UNKNOWN_MEMBER_ID
    );

    stop_broker(broker).await;

    // After a restart nothing has touched the coordinator shards yet:
    // ListGroups itself must load them from the offsets log.
    let broker = start_standalone(&data_dir, 4).await;
    let conn = connect(broker.addr, "observability").await;
    let listed = list_groups(&conn, &[]).await;
    assert_eq!(listed.error_code, ec::NONE);
    let group = listed
        .groups
        .iter()
        .find(|group| group.group_id == "g5")
        .expect("g5 relisted after restart");
    assert_eq!(group.state, "Stable");
    assert_eq!(group.member_count, 1);
    let described = describe_group(&conn, "g5").await;
    assert_eq!(
        offset_tuples(&described.offsets),
        vec![("events".to_owned(), 0, 17)]
    );

    stop_broker(broker).await;
}

#[tokio::test]
async fn list_groups_reports_only_locally_coordinated_groups() {
    let temp = TempDir::new().unwrap();
    let cache = MetadataCache::new(cluster_image());
    let broker_one = start_cluster_broker(1, &temp.path().join("broker-1"), cache.clone()).await;
    let broker_two = start_cluster_broker(2, &temp.path().join("broker-2"), cache.clone()).await;

    // Two groups whose crc32c lands them on different offsets partitions,
    // hence different coordinator brokers.
    let conn_one = connect(broker_one.addr, "list-one").await;
    let conn_two = connect(broker_two.addr, "list-two").await;
    for group in ["cg-a", "cg-b"] {
        let coordinator = if fetch_offsets(&conn_one, group, vec![]).await.error_code == ec::NONE {
            &conn_one
        } else {
            &conn_two
        };
        assert_eq!(
            join(coordinator, group, "", 30_000, 500).await.error_code,
            ec::NONE
        );
        assert_eq!(
            sync(
                coordinator,
                group,
                1,
                "member-0",
                vec![MemberAssignment {
                    member_id: "member-0".into(),
                    partitions: vec![assigned("events", 0)],
                }],
            )
            .await
            .error_code,
            ec::NONE
        );
    }

    let from_one = list_groups(&conn_one, &[]).await;
    let from_two = list_groups(&conn_two, &[]).await;
    assert_eq!(from_one.error_code, ec::NONE);
    assert_eq!(from_two.error_code, ec::NONE);
    let ids = |response: &ListGroupsResponse| {
        response
            .groups
            .iter()
            .map(|group| group.group_id.clone())
            .collect::<BTreeSet<_>>()
    };
    // Disjoint per broker, and together they cover both groups: a
    // cluster-wide listing is exactly the union.
    assert!(ids(&from_one).is_disjoint(&ids(&from_two)));
    let union: BTreeSet<String> = ids(&from_one).union(&ids(&from_two)).cloned().collect();
    assert_eq!(
        union,
        BTreeSet::from(["cg-a".to_owned(), "cg-b".to_owned()])
    );

    stop_broker(broker_one).await;
    stop_broker(broker_two).await;
}
