//! HTTP-connected OpenRaft controller quorum for Brahmaputra metadata.
//!
//! Every vote, log mutation, commit index, metadata state-machine update, and
//! snapshot is committed to an embedded database before OpenRaft observes the
//! storage operation as complete. Controller nodes can therefore restart from
//! the same data directory without reverting their Raft log.

mod http;
mod network;
mod store;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{ClientWriteError, InitializeError, RaftError};
use openraft::storage::Adaptor;
use openraft::{Config as RaftConfig, RaftMetrics};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::time::Instant;

pub use brahmaputra_metadata::{ClusterMetadata, MetadataCommand, MetadataError, MetadataEvent};
pub use network::HttpNetworkFactory;
pub use store::TypeConfig as ControllerRaftTypeConfig;
use store::{ClientRequest, DurableStore};

pub type NodeId = u64;
pub type ControllerRaft = openraft::Raft<ControllerRaftTypeConfig>;
pub type ControllerRaftMetrics = RaftMetrics<NodeId, ()>;
pub type ControllerCommandResult = Result<MetadataEvent, ControllerErrorBody>;

const METADATA_STATUS_KEY: &str = "__brahmaputra_cluster_metadata__";
const WRITE_RETRY_INTERVAL: Duration = Duration::from_millis(40);

/// Static configuration for one controller process.
///
/// Every member must use the same `cluster_id` and `peers` map. Peer values may
/// be either `host:port` or an `http://`/`https://` base URL.
#[derive(Clone, Debug)]
pub struct ControllerConfig {
    pub node_id: NodeId,
    pub cluster_id: String,
    pub peers: BTreeMap<NodeId, String>,
    pub raft: RaftConfig,
    pub raft_rpc_timeout: Duration,
    pub command_timeout: Duration,
    /// Persistent controller directory. `None` creates an isolated temporary
    /// on-disk store, which is convenient for tests but intentionally cannot be
    /// rediscovered by a later process.
    pub data_dir: Option<PathBuf>,
}

impl ControllerConfig {
    pub fn new(
        node_id: NodeId,
        cluster_id: impl Into<String>,
        peers: BTreeMap<NodeId, String>,
    ) -> Self {
        let cluster_id = cluster_id.into();
        let raft = RaftConfig {
            cluster_name: cluster_id.clone(),
            ..RaftConfig::default()
        };

        Self {
            node_id,
            cluster_id,
            peers: peers
                .into_iter()
                .map(|(id, address)| (id, normalize_base_url(address)))
                .collect(),
            raft,
            raft_rpc_timeout: Duration::from_secs(2),
            command_timeout: Duration::from_secs(10),
            data_dir: None,
        }
    }

    /// Select a restart-stable controller data directory.
    pub fn with_data_dir(mut self, data_dir: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(data_dir.into());
        self
    }
}

/// Stable JSON error returned by controller command and admin endpoints.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerErrorBody {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leader_id: Option<NodeId>,
    pub retryable: bool,
}

impl ControllerErrorBody {
    fn new(code: &str, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.to_owned(),
            message: message.into(),
            leader_id: None,
            retryable,
        }
    }

    fn with_leader(mut self, leader_id: Option<NodeId>) -> Self {
        self.leader_id = leader_id;
        self
    }
}

impl fmt::Display for ControllerErrorBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ControllerErrorBody {}

/// One OpenRaft controller plus its HTTP transport and local metadata view.
pub struct ControllerNode {
    config: ControllerConfig,
    raft: ControllerRaft,
    store: Arc<DurableStore>,
    data_dir: PathBuf,
    _ephemeral_data_dir: Option<tempfile::TempDir>,
    write_mutex: Mutex<()>,
    forwarding_client: reqwest::Client,
}

impl fmt::Debug for ControllerNode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControllerNode")
            .field("node_id", &self.config.node_id)
            .field("cluster_id", &self.config.cluster_id)
            .field("peers", &self.config.peers)
            .field("data_dir", &self.data_dir)
            .finish_non_exhaustive()
    }
}

impl ControllerNode {
    /// Create the Raft task and durable store. HTTP serving is started
    /// separately with [`ControllerNode::serve`].
    pub async fn new(mut config: ControllerConfig) -> Result<Arc<Self>, ControllerErrorBody> {
        validate_controller_config(&config)?;
        config.raft.cluster_name = config.cluster_id.clone();
        let raft_config = config.raft.clone().validate().map_err(|error| {
            ControllerErrorBody::new("invalid_config", error.to_string(), false)
        })?;

        let peers = Arc::new(config.peers.clone());
        let network = HttpNetworkFactory::new(peers, config.raft_rpc_timeout)
            .map_err(|error| ControllerErrorBody::new("http_client", error.to_string(), false))?;
        let forwarding_client = reqwest::Client::builder()
            .timeout(config.raft_rpc_timeout)
            .build()
            .map_err(|error| ControllerErrorBody::new("http_client", error.to_string(), false))?;

        let ephemeral_data_dir = if config.data_dir.is_none() {
            Some(
                tempfile::Builder::new()
                    .prefix("brahmaputra-controller-")
                    .tempdir()
                    .map_err(|error| {
                        ControllerErrorBody::new("controller_data_dir", error.to_string(), false)
                    })?,
            )
        } else {
            None
        };
        let data_dir = config
            .data_dir
            .clone()
            .or_else(|| {
                ephemeral_data_dir
                    .as_ref()
                    .map(|directory| directory.path().to_path_buf())
            })
            .expect("either configured or temporary controller data directory must exist");
        let store = DurableStore::open(&data_dir, &config.cluster_id, config.node_id)
            .await
            .map_err(|error| {
                ControllerErrorBody::new("controller_store", error.to_string(), false)
            })?;
        let (log_store, state_machine) =
            Adaptor::<ControllerRaftTypeConfig, Arc<DurableStore>>::new(store.clone());
        let raft = ControllerRaft::new(
            config.node_id,
            Arc::new(raft_config),
            network,
            log_store,
            state_machine,
        )
        .await
        .map_err(|error| ControllerErrorBody::new("raft_start", error.to_string(), false))?;

        Ok(Arc::new(Self {
            config,
            raft,
            store,
            data_dir,
            _ephemeral_data_dir: ephemeral_data_dir,
            write_mutex: Mutex::new(()),
            forwarding_client,
        }))
    }

    pub fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    pub fn config(&self) -> &ControllerConfig {
        &self.config
    }

    /// Actual controller directory, including the generated temporary path
    /// used when [`ControllerConfig::data_dir`] is `None`.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Embedded database path, primarily useful for diagnostics.
    pub fn database_path(&self) -> &Path {
        self.store.database_path()
    }

    pub fn raft(&self) -> &ControllerRaft {
        &self.raft
    }

    /// Initialize the pristine quorum with the complete fixed peer membership.
    /// Calling this method again after initialization is harmless.
    pub async fn bootstrap(&self) -> Result<(), ControllerErrorBody> {
        if self
            .raft
            .is_initialized()
            .await
            .map_err(|error| ControllerErrorBody::new("raft", error.to_string(), true))?
        {
            return Ok(());
        }

        let members: BTreeSet<_> = self.config.peers.keys().copied().collect();
        match self.raft.initialize(members).await {
            Ok(()) | Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
            Err(error) => Err(ControllerErrorBody::new(
                "bootstrap_failed",
                error.to_string(),
                true,
            )),
        }
    }

    /// Read this node's locally applied metadata image.
    ///
    /// This is intentionally a local read: callers can compare offsets across
    /// nodes, and the integration test uses it to prove convergence.
    pub async fn local_metadata(&self) -> Result<ClusterMetadata, ControllerErrorBody> {
        let state_machine = self.store.state_machine().await;
        match state_machine.client_status.get(METADATA_STATUS_KEY) {
            Some(json) => serde_json::from_str(json).map_err(|error| {
                ControllerErrorBody::new("corrupt_metadata", error.to_string(), false)
            }),
            None => Ok(ClusterMetadata::new(self.config.cluster_id.clone())),
        }
    }

    /// Return the latest local OpenRaft metrics snapshot.
    pub fn raft_metrics(&self) -> ControllerRaftMetrics {
        let metrics = self.raft.metrics();
        let snapshot = metrics.borrow().clone();
        snapshot
    }

    /// Wait until this node observes an elected leader.
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<NodeId, ControllerErrorBody> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(leader_id) = self.raft_metrics().current_leader {
                return Ok(leader_id);
            }
            if Instant::now() >= deadline {
                return Err(ControllerErrorBody::new(
                    "leader_unavailable",
                    "timed out waiting for a Raft leader",
                    true,
                ));
            }
            tokio::time::sleep(WRITE_RETRY_INTERVAL).await;
        }
    }

    /// Apply a metadata command through Raft.
    ///
    /// The method works on every node. Followers forward to their currently
    /// observed leader, and both forwarding and direct writes retry across a
    /// leader election until `command_timeout` expires.
    pub async fn write_metadata(&self, command: MetadataCommand) -> ControllerCommandResult {
        let _guard = self.write_mutex.lock().await;
        let deadline = Instant::now() + self.config.command_timeout;
        let mut leader_hint = self.raft_metrics().current_leader;
        let mut last_error: ControllerErrorBody;

        loop {
            if let Some(leader_id) = leader_hint {
                if leader_id != self.config.node_id {
                    match self.forward_command(leader_id, &command).await {
                        Ok(event) => return Ok(event),
                        Err(error) if !error.retryable => return Err(error),
                        Err(error) => {
                            leader_hint = error.leader_id;
                            last_error = error;
                        }
                    }
                } else {
                    match self.write_on_local_leader(command.clone()).await {
                        Ok(event) => return Ok(event),
                        Err(LocalWriteError::Rejected(error)) => return Err(error),
                        Err(LocalWriteError::Retry { leader_id, error }) => {
                            leader_hint = leader_id;
                            last_error = error;
                        }
                    }
                }
            } else {
                match self.write_on_local_leader(command.clone()).await {
                    Ok(event) => return Ok(event),
                    Err(LocalWriteError::Rejected(error)) => return Err(error),
                    Err(LocalWriteError::Retry { leader_id, error }) => {
                        leader_hint = leader_id;
                        last_error = error;
                    }
                }
            }

            if Instant::now() >= deadline {
                last_error.leader_id = last_error.leader_id.or(leader_hint);
                return Err(last_error);
            }

            tokio::time::sleep(WRITE_RETRY_INTERVAL).await;
            leader_hint = self.raft_metrics().current_leader.or(leader_hint);
        }
    }

    async fn write_on_local_leader(
        &self,
        command: MetadataCommand,
    ) -> Result<MetadataEvent, LocalWriteError> {
        let mut next_metadata = self
            .local_metadata()
            .await
            .map_err(LocalWriteError::Rejected)?;
        let event = next_metadata.apply(command).map_err(|error| {
            LocalWriteError::Rejected(ControllerErrorBody::new(
                "metadata_rejected",
                error.to_string(),
                false,
            ))
        })?;
        let status = serde_json::to_string(&next_metadata).map_err(|error| {
            LocalWriteError::Rejected(ControllerErrorBody::new(
                "metadata_encoding",
                error.to_string(),
                false,
            ))
        })?;
        let request = ClientRequest {
            client: METADATA_STATUS_KEY.to_owned(),
            serial: next_metadata.offset,
            status,
        };

        match self.raft.client_write(request).await {
            Ok(_) => Ok(event),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => {
                let error = ControllerErrorBody::new(
                    "not_leader",
                    "local node is not the writable Raft leader",
                    true,
                )
                .with_leader(forward.leader_id);
                Err(LocalWriteError::Retry {
                    leader_id: forward.leader_id,
                    error,
                })
            }
            Err(error) => {
                let leader_id = self.raft_metrics().current_leader;
                Err(LocalWriteError::Retry {
                    leader_id,
                    error: ControllerErrorBody::new("raft_write", error.to_string(), true)
                        .with_leader(leader_id),
                })
            }
        }
    }

    async fn forward_command(
        &self,
        leader_id: NodeId,
        command: &MetadataCommand,
    ) -> ControllerCommandResult {
        let Some(address) = self.config.peers.get(&leader_id) else {
            return Err(ControllerErrorBody::new(
                "unknown_leader",
                format!("leader {leader_id} is absent from the fixed peer map"),
                true,
            ));
        };
        let url = format!(
            "{}/api/v1/controller/command",
            address.trim_end_matches('/')
        );
        let response = self
            .forwarding_client
            .post(url)
            .json(command)
            .send()
            .await
            .map_err(|error| {
                ControllerErrorBody::new("leader_unreachable", error.to_string(), true)
                    .with_leader(Some(leader_id))
            })?;

        if response.status() != StatusCode::OK {
            return Err(ControllerErrorBody::new(
                "leader_http_error",
                format!("leader {leader_id} returned HTTP {}", response.status()),
                true,
            )
            .with_leader(Some(leader_id)));
        }

        response
            .json::<ControllerCommandResult>()
            .await
            .map_err(|error| {
                ControllerErrorBody::new("leader_response", error.to_string(), true)
                    .with_leader(Some(leader_id))
            })?
    }

    /// Build the Axum router, useful for embedding or HTTP-level tests.
    pub fn router(self: &Arc<Self>) -> axum::Router {
        http::router(self.clone())
    }

    /// Serve all public and Raft routes on an already-bound listener. When the
    /// supplied shutdown future resolves, HTTP drains and the Raft task stops.
    pub async fn serve<F>(
        self: Arc<Self>,
        listener: TcpListener,
        shutdown: F,
    ) -> Result<(), ControllerErrorBody>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let serve_result = axum::serve(listener, self.router())
            .with_graceful_shutdown(shutdown)
            .await;
        let raft_result = self.raft.shutdown().await;

        serve_result
            .map_err(|error| ControllerErrorBody::new("http_server", error.to_string(), false))?;
        raft_result
            .map_err(|error| ControllerErrorBody::new("raft_shutdown", error.to_string(), false))?;
        Ok(())
    }

    /// Stop only the Raft task. Normally callers should resolve the shutdown
    /// future passed to [`ControllerNode::serve`] instead.
    pub async fn shutdown_raft(&self) -> Result<(), ControllerErrorBody> {
        self.raft
            .shutdown()
            .await
            .map_err(|error| ControllerErrorBody::new("raft_shutdown", error.to_string(), false))
    }
}

enum LocalWriteError {
    Retry {
        leader_id: Option<NodeId>,
        error: ControllerErrorBody,
    },
    Rejected(ControllerErrorBody),
}

fn normalize_base_url(address: String) -> String {
    let address = address.trim_end_matches('/');
    if address.starts_with("http://") || address.starts_with("https://") {
        address.to_owned()
    } else {
        format!("http://{address}")
    }
}

fn validate_controller_config(config: &ControllerConfig) -> Result<(), ControllerErrorBody> {
    if config.cluster_id.trim().is_empty() {
        return Err(ControllerErrorBody::new(
            "invalid_config",
            "cluster_id must not be empty",
            false,
        ));
    }
    if !config.peers.contains_key(&config.node_id) {
        return Err(ControllerErrorBody::new(
            "invalid_config",
            format!("node {} is absent from the fixed peer map", config.node_id),
            false,
        ));
    }
    if config
        .peers
        .values()
        .any(|address| address.trim().is_empty())
    {
        return Err(ControllerErrorBody::new(
            "invalid_config",
            "peer addresses must not be empty",
            false,
        ));
    }
    if config.command_timeout.is_zero() || config.raft_rpc_timeout.is_zero() {
        return Err(ControllerErrorBody::new(
            "invalid_config",
            "controller timeouts must be non-zero",
            false,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_normalizes_http_addresses() {
        let config = ControllerConfig::new(
            1,
            "cluster-a",
            BTreeMap::from([
                (1, "127.0.0.1:9001".to_owned()),
                (2, "https://controller.example:9002/".to_owned()),
            ]),
        );
        assert_eq!(config.peers[&1], "http://127.0.0.1:9001");
        assert_eq!(config.peers[&2], "https://controller.example:9002");
    }
}
