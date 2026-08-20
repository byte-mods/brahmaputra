use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use brahmaputra_controller::{
    ClusterMetadata, ControllerCommandResult, ControllerConfig, ControllerErrorBody,
    ControllerNode, ControllerRaftMetrics, MetadataCommand, MetadataEvent, NodeId,
};
use brahmaputra_metadata::{NodeRole, Role, UserRecord};
use reqwest::Client;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Instant};

const TEST_TIMEOUT: Duration = Duration::from_secs(15);
const CHILD_MODE_ENV: &str = "BRAHMAPUTRA_CONTROLLER_TEST_CHILD";
const CHILD_NODE_ID_ENV: &str = "BRAHMAPUTRA_CONTROLLER_TEST_NODE_ID";
const CHILD_CLUSTER_ID_ENV: &str = "BRAHMAPUTRA_CONTROLLER_TEST_CLUSTER_ID";
const CHILD_PEERS_ENV: &str = "BRAHMAPUTRA_CONTROLLER_TEST_PEERS";
const CHILD_DATA_DIR_ENV: &str = "BRAHMAPUTRA_CONTROLLER_TEST_DATA_DIR";
const CHILD_SHUTDOWN_ENV: &str = "BRAHMAPUTRA_CONTROLLER_TEST_SHUTDOWN";

struct RunningNode {
    id: NodeId,
    address: String,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), ControllerErrorBody>>,
    _node: Arc<ControllerNode>,
}

impl RunningNode {
    async fn stop(self) -> Result<()> {
        let _ = self.shutdown.send(());
        timeout(Duration::from_secs(5), self.task)
            .await
            .context("controller server did not stop")?
            .context("controller server task panicked")?
            .map_err(anyhow::Error::from)
    }
}

#[derive(Clone, Debug)]
struct ProcessSpec {
    id: NodeId,
    cluster_id: String,
    address: String,
    peers: BTreeMap<NodeId, String>,
    data_dir: PathBuf,
    shutdown_file: PathBuf,
}

struct ProcessNode {
    spec: ProcessSpec,
    child: Option<Child>,
}

impl ProcessNode {
    fn start(spec: ProcessSpec) -> Result<Self> {
        let child = spawn_controller_process(&spec)?;
        Ok(Self {
            spec,
            child: Some(child),
        })
    }

    fn address(&self) -> &str {
        &self.spec.address
    }

    fn graceful_stop(&mut self) -> Result<()> {
        fs::write(&self.spec.shutdown_file, b"shutdown")?;
        self.wait_for_exit(Duration::from_secs(10), "graceful shutdown")
    }

    fn hard_stop(&mut self) -> Result<()> {
        if let Some(child) = self.child.as_mut() {
            child
                .kill()
                .context("failed to terminate controller child")?;
        }
        self.wait_for_exit(Duration::from_secs(5), "hard shutdown")
    }

    fn restart(&mut self) -> Result<()> {
        if self.child.is_some() {
            return Err(anyhow!("controller {} is still running", self.spec.id));
        }
        match fs::remove_file(&self.spec.shutdown_file) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        self.child = Some(spawn_controller_process(&self.spec)?);
        Ok(())
    }

    fn wait_for_exit(&mut self, wait: Duration, operation: &str) -> Result<()> {
        let deadline = std::time::Instant::now() + wait;
        loop {
            let Some(child) = self.child.as_mut() else {
                return Ok(());
            };
            if let Some(status) = child.try_wait()? {
                self.child = None;
                if status.success() || operation == "hard shutdown" {
                    return Ok(());
                }
                return Err(anyhow!(
                    "controller {} failed during {operation} with {status}; log: {}",
                    self.spec.id,
                    child_log(&self.spec)
                ));
            }
            if std::time::Instant::now() >= deadline {
                return Err(anyhow!(
                    "controller {} timed out during {operation}; log: {}",
                    self.spec.id,
                    child_log(&self.spec)
                ));
            }
            thread::sleep(Duration::from_millis(25));
        }
    }
}

impl Drop for ProcessNode {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn spawn_controller_process(spec: &ProcessSpec) -> Result<Child> {
    fs::create_dir_all(&spec.data_dir)?;
    let log_path = spec.data_dir.join("process-test.log");
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let stderr = stdout.try_clone()?;
    Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "controller_process_helper",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_MODE_ENV, "1")
        .env(CHILD_NODE_ID_ENV, spec.id.to_string())
        .env(CHILD_CLUSTER_ID_ENV, &spec.cluster_id)
        .env(CHILD_PEERS_ENV, serde_json::to_string(&spec.peers)?)
        .env(CHILD_DATA_DIR_ENV, &spec.data_dir)
        .env(CHILD_SHUTDOWN_ENV, &spec.shutdown_file)
        .env("RUST_BACKTRACE", "1")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .context("failed to spawn controller test process")
}

fn child_log(spec: &ProcessSpec) -> String {
    fs::read_to_string(spec.data_dir.join("process-test.log"))
        .unwrap_or_else(|error| format!("<unavailable: {error}>"))
}

/// Subprocess entrypoint used by the destructive restart test. Normal test
/// runs return immediately; selected child invocations stay alive until a
/// sentinel appears or the parent kills the process.
#[test]
fn controller_process_helper() {
    if std::env::var_os(CHILD_MODE_ENV).is_none() {
        return;
    }
    run_controller_process_helper().expect("controller subprocess failed");
}

fn run_controller_process_helper() -> Result<()> {
    let node_id: NodeId = std::env::var(CHILD_NODE_ID_ENV)?.parse()?;
    let cluster_id = std::env::var(CHILD_CLUSTER_ID_ENV)?;
    let peers: BTreeMap<NodeId, String> = serde_json::from_str(&std::env::var(CHILD_PEERS_ENV)?)?;
    let data_dir = PathBuf::from(std::env::var_os(CHILD_DATA_DIR_ENV).context("missing data dir")?);
    let shutdown_file =
        PathBuf::from(std::env::var_os(CHILD_SHUTDOWN_ENV).context("missing shutdown path")?);
    let bind_address = peers[&node_id]
        .strip_prefix("http://")
        .context("test peer must use http://")?
        .to_owned();

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?
        .block_on(async move {
            let listener = TcpListener::bind(&bind_address).await?;
            let mut config =
                ControllerConfig::new(node_id, cluster_id, peers).with_data_dir(data_dir);
            config.raft.heartbeat_interval = 40;
            config.raft.election_timeout_min = 180;
            config.raft.election_timeout_max = 360;
            config.raft_rpc_timeout = Duration::from_millis(500);
            config.command_timeout = Duration::from_secs(12);
            let node = ControllerNode::new(config).await?;
            node.serve(listener, async move {
                loop {
                    if shutdown_file.exists() {
                        break;
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .map_err(anyhow::Error::from)
        })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn metadata_converges_and_writes_continue_after_leader_shutdown() -> Result<()> {
    let mut listeners = Vec::new();
    let mut peers = BTreeMap::new();
    for id in 1..=3 {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        peers.insert(id, format!("http://{}", listener.local_addr()?));
        listeners.push((id, listener));
    }

    let mut running = BTreeMap::new();
    for (id, listener) in listeners {
        let mut config = ControllerConfig::new(id, "http-integration", peers.clone());
        config.raft.heartbeat_interval = 40;
        config.raft.election_timeout_min = 180;
        config.raft.election_timeout_max = 360;
        config.raft_rpc_timeout = Duration::from_millis(500);
        config.command_timeout = Duration::from_secs(8);

        let node = ControllerNode::new(config).await?;
        let (shutdown, shutdown_rx) = oneshot::channel();
        let serving_node = node.clone();
        let task = tokio::spawn(async move {
            serving_node
                .serve(listener, async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        running.insert(
            id,
            RunningNode {
                id,
                address: peers[&id].clone(),
                shutdown,
                task,
                _node: node,
            },
        );
    }

    let client = Client::builder().timeout(TEST_TIMEOUT).build()?;
    wait_until_http_ready(&client, &running[&1].address).await?;

    // Exercise the management endpoint, rather than calling bootstrap in-process.
    let initialized: Result<(), ControllerErrorBody> = client
        .post(format!(
            "{}/api/v1/controller/bootstrap",
            running[&1].address
        ))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    initialized?;

    let first_leader = wait_for_shared_leader(&client, running.values()).await?;
    let follower = running
        .values()
        .find(|node| node.id != first_leader)
        .context("three-node cluster must have a follower")?;

    // Send the first application write to a follower. Its public handler must
    // discover and forward to the elected leader over HTTP.
    let event = post_command(
        &client,
        &follower.address,
        MetadataCommand::RegisterBroker {
            broker_id: 10,
            host: "127.0.0.1".to_owned(),
            data_port: 9_092,
            control_port: 19_092,
            roles: vec![NodeRole::Broker, NodeRole::Controller],
            rack: Some("rack-a".to_owned()),
            now_ms: 1_000,
        },
    )
    .await?;
    assert_eq!(
        event,
        MetadataEvent::BrokerRegistered {
            broker_id: 10,
            broker_epoch: 1,
        }
    );
    wait_for_metadata(&client, running.values(), 1, |metadata| {
        metadata.brokers.contains_key(&10)
    })
    .await?;

    // Kill the actual leader and issue a write immediately, while its old
    // leadership may still be cached. The receiving node has to retry through
    // the election instead of requiring the client to locate the new leader.
    let dead_leader = running
        .remove(&first_leader)
        .context("elected leader must be a running node")?;
    dead_leader.stop().await?;

    let survivor_address = running
        .values()
        .next()
        .context("two nodes must survive")?
        .address
        .clone();
    let event = post_command(
        &client,
        &survivor_address,
        MetadataCommand::SetJwtSecret {
            secret: "0123456789abcdef0123456789abcdef".to_owned(),
        },
    )
    .await?;
    assert_eq!(event, MetadataEvent::JwtSecretChanged);

    let second_leader = wait_for_shared_leader(&client, running.values()).await?;
    assert_ne!(second_leader, first_leader);
    wait_for_metadata(&client, running.values(), 2, |metadata| {
        metadata.jwt_secret.is_some()
    })
    .await?;

    // Make one more post-failover write explicitly through the remaining
    // follower, proving the forwarding path still works in the reduced quorum.
    let second_follower = running
        .values()
        .find(|node| node.id != second_leader)
        .context("two-node quorum must have one follower")?;
    let event = post_command(
        &client,
        &second_follower.address,
        MetadataCommand::PutUser {
            user: UserRecord {
                username: "operator".to_owned(),
                password_hash: "test-only-hash".to_owned(),
                role: Role::Operator,
                force_password_change: true,
            },
        },
    )
    .await?;
    assert_eq!(
        event,
        MetadataEvent::UserChanged {
            username: "operator".to_owned(),
        }
    );
    wait_for_metadata(&client, running.values(), 3, |metadata| {
        metadata.users.contains_key("operator")
    })
    .await?;

    for (_, node) in running {
        node.stop().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_follower_and_full_cluster_restarts_retain_raft_state() -> Result<()> {
    let root = tempfile::tempdir()?;
    let cluster_id = "durable-process-integration";
    let mut peers = BTreeMap::new();
    for id in 1..=3 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        peers.insert(id, format!("http://{}", listener.local_addr()?));
    }

    let specs: Vec<_> = peers
        .iter()
        .map(|(&id, address)| {
            let data_dir = root.path().join(format!("node-{id}"));
            ProcessSpec {
                id,
                cluster_id: cluster_id.to_owned(),
                address: address.clone(),
                peers: peers.clone(),
                shutdown_file: data_dir.join("shutdown.sentinel"),
                data_dir,
            }
        })
        .collect();
    let mut nodes: Vec<_> = specs
        .into_iter()
        .map(ProcessNode::start)
        .collect::<Result<_>>()?;

    let client = Client::builder().timeout(Duration::from_secs(20)).build()?;
    wait_for_process_http_ready(&client, &nodes).await?;
    let initialized: Result<(), ControllerErrorBody> = client
        .post(format!(
            "{}/api/v1/controller/bootstrap",
            nodes[0].address()
        ))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    initialized?;

    let first_leader = wait_for_process_leader(&client, &nodes).await?;
    let command_address = nodes
        .iter()
        .find(|node| node.spec.id != first_leader)
        .context("three-node cluster needs a follower")?
        .address()
        .to_owned();
    post_command(
        &client,
        &command_address,
        MetadataCommand::RegisterBroker {
            broker_id: 10,
            host: "127.0.0.1".to_owned(),
            data_port: 9_092,
            control_port: 19_092,
            roles: vec![NodeRole::Broker, NodeRole::Controller],
            rack: Some("rack-a".to_owned()),
            now_ms: 1_000,
        },
    )
    .await?;

    // Exceed the exact live failure's ~274 entries before restarting a
    // follower from its original store.
    for sequence in 1..=300_i64 {
        post_command(
            &client,
            &command_address,
            MetadataCommand::Heartbeat {
                broker_id: 10,
                broker_epoch: 1,
                now_ms: 1_000 + sequence * 1_000,
            },
        )
        .await?;
    }
    wait_for_process_metadata(&client, &nodes, 301).await?;

    let follower_index = nodes
        .iter()
        .position(|node| node.spec.id != first_leader)
        .context("three-node cluster needs a follower")?;
    nodes[follower_index].hard_stop()?;

    for sequence in 301..=340_i64 {
        let survivor = nodes
            .iter()
            .find(|node| node.child.is_some())
            .context("two controllers should survive")?;
        post_command(
            &client,
            survivor.address(),
            MetadataCommand::Heartbeat {
                broker_id: 10,
                broker_epoch: 1,
                now_ms: 1_000 + sequence * 1_000,
            },
        )
        .await?;
    }
    nodes[follower_index].restart()?;
    wait_for_process_http_ready(&client, &nodes).await?;
    wait_for_process_metadata(&client, &nodes, 341).await?;

    // A full graceful restart must elect directly from persisted membership,
    // without bootstrap, and retain the exact metadata image.
    for node in &mut nodes {
        node.graceful_stop()?;
    }
    for node in &mut nodes {
        node.restart()?;
    }
    wait_for_process_http_ready(&client, &nodes).await?;
    wait_for_process_leader(&client, &nodes).await?;
    wait_for_process_metadata(&client, &nodes, 341).await?;
    post_command(
        &client,
        nodes[1].address(),
        MetadataCommand::SetJwtSecret {
            secret: "0123456789abcdef0123456789abcdef".to_owned(),
        },
    )
    .await?;
    wait_for_process_metadata(&client, &nodes, 342).await?;

    // Kill every OS process, then reopen all three redb stores. This is the
    // hard-restart proof that vote/log/commit/state survived process death.
    for node in &mut nodes {
        node.hard_stop()?;
    }
    for node in &mut nodes {
        node.restart()?;
    }
    wait_for_process_http_ready(&client, &nodes).await?;
    wait_for_process_leader(&client, &nodes).await?;
    wait_for_process_metadata(&client, &nodes, 342).await?;
    post_command(
        &client,
        nodes[2].address(),
        MetadataCommand::PutUser {
            user: UserRecord {
                username: "restart-proof".to_owned(),
                password_hash: "test-only-hash".to_owned(),
                role: Role::Admin,
                force_password_change: false,
            },
        },
    )
    .await?;
    wait_for_process_metadata(&client, &nodes, 343).await?;

    for node in &mut nodes {
        node.graceful_stop()?;
    }
    Ok(())
}

async fn wait_for_process_http_ready(client: &Client, nodes: &[ProcessNode]) -> Result<()> {
    for node in nodes {
        wait_until_http_ready(client, node.address())
            .await
            .with_context(|| {
                format!(
                    "controller {} did not start; log: {}",
                    node.spec.id,
                    child_log(&node.spec)
                )
            })?;
    }
    Ok(())
}

async fn wait_for_process_leader(client: &Client, nodes: &[ProcessNode]) -> Result<NodeId> {
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        let mut observed = Vec::new();
        for node in nodes.iter().filter(|node| node.child.is_some()) {
            let response = client
                .get(format!("{}/api/v1/controller/raft", node.address()))
                .send()
                .await;
            match response {
                Ok(response) if response.status().is_success() => {
                    observed.push(
                        response
                            .json::<ControllerRaftMetrics>()
                            .await?
                            .current_leader,
                    );
                }
                _ => observed.push(None),
            }
        }
        if let Some(Some(leader)) = observed.first().copied() {
            if observed.iter().all(|seen| *seen == Some(leader)) {
                return Ok(leader);
            }
        }
        if Instant::now() >= deadline {
            return Err(anyhow!("controller processes did not agree on a leader"));
        }
        sleep(Duration::from_millis(40)).await;
    }
}

async fn wait_for_process_metadata(
    client: &Client,
    nodes: &[ProcessNode],
    expected_offset: u64,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let mut images = Vec::new();
        let mut all_replied = true;
        for node in nodes.iter().filter(|node| node.child.is_some()) {
            match client
                .get(format!("{}/api/v1/controller/metadata", node.address()))
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    images.push(response.json::<ClusterMetadata>().await?);
                }
                _ => all_replied = false,
            }
        }
        if all_replied
            && !images.is_empty()
            && images
                .iter()
                .all(|metadata| metadata.offset == expected_offset && metadata == &images[0])
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "process metadata did not converge at offset {expected_offset}: {images:?}"
            ));
        }
        sleep(Duration::from_millis(40)).await;
    }
}

async fn wait_until_http_ready(client: &Client, address: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if client
            .get(format!("{address}/api/v1/controller/raft"))
            .send()
            .await
            .is_ok()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow!("controller HTTP endpoint did not become ready"));
        }
        sleep(Duration::from_millis(25)).await;
    }
}

async fn post_command(
    client: &Client,
    address: &str,
    command: MetadataCommand,
) -> Result<MetadataEvent> {
    let result: ControllerCommandResult = timeout(
        TEST_TIMEOUT,
        client
            .post(format!("{address}/api/v1/controller/command"))
            .json(&command)
            .send(),
    )
    .await
    .context("metadata command timed out")??
    .error_for_status()?
    .json()
    .await?;
    result.map_err(anyhow::Error::from)
}

async fn wait_for_shared_leader<'a>(
    client: &Client,
    nodes: impl IntoIterator<Item = &'a RunningNode> + Clone,
) -> Result<NodeId> {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let active_ids: Vec<_> = nodes.clone().into_iter().map(|node| node.id).collect();
        let mut observed = Vec::new();
        let mut all_replied = true;
        for node in nodes.clone() {
            match client
                .get(format!("{}/api/v1/controller/raft", node.address))
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    let metrics: ControllerRaftMetrics = response.json().await?;
                    observed.push(metrics.current_leader);
                }
                _ => all_replied = false,
            }
        }

        if all_replied {
            if let Some(Some(leader)) = observed.first().copied() {
                if observed.iter().all(|seen| *seen == Some(leader)) && active_ids.contains(&leader)
                {
                    return Ok(leader);
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(anyhow!("active controllers did not agree on a leader"));
        }
        sleep(Duration::from_millis(40)).await;
    }
}

async fn wait_for_metadata<'a, F>(
    client: &Client,
    nodes: impl IntoIterator<Item = &'a RunningNode> + Clone,
    expected_offset: u64,
    predicate: F,
) -> Result<()>
where
    F: Fn(&ClusterMetadata) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let mut images = Vec::new();
        let mut all_replied = true;
        for node in nodes.clone() {
            match client
                .get(format!("{}/api/v1/controller/metadata", node.address))
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    images.push(response.json::<ClusterMetadata>().await?);
                }
                _ => all_replied = false,
            }
        }

        if all_replied
            && !images.is_empty()
            && images.iter().all(|metadata| {
                metadata.offset == expected_offset && predicate(metadata) && metadata == &images[0]
            })
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "metadata did not converge at offset {expected_offset}: {images:?}"
            ));
        }
        sleep(Duration::from_millis(40)).await;
    }
}
