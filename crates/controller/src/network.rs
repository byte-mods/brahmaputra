use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{ControllerRaftTypeConfig, NodeId};

/// HTTP network factory used by OpenRaft.
///
/// The controller type config fixes `Node = ()`, so addresses cannot live in Raft's
/// membership node records. The complete, fixed peer map intentionally stays
/// here and is shared by every per-peer connection.
#[derive(Clone, Debug)]
pub struct HttpNetworkFactory {
    peers: Arc<BTreeMap<NodeId, String>>,
    client: reqwest::Client,
}

impl HttpNetworkFactory {
    pub(crate) fn new(
        peers: Arc<BTreeMap<NodeId, String>>,
        request_timeout: Duration,
    ) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()?;
        Ok(Self { peers, client })
    }

    /// Return the configured HTTP address for a Raft peer.
    pub fn peer_address(&self, node_id: NodeId) -> Option<&str> {
        self.peers.get(&node_id).map(String::as_str)
    }
}

impl RaftNetworkFactory<ControllerRaftTypeConfig> for HttpNetworkFactory {
    type Network = HttpNetworkConnection;

    async fn new_client(&mut self, target: NodeId, _node: &()) -> Self::Network {
        HttpNetworkConnection {
            peers: self.peers.clone(),
            client: self.client.clone(),
            target,
        }
    }
}

pub struct HttpNetworkConnection {
    peers: Arc<BTreeMap<NodeId, String>>,
    client: reqwest::Client,
    target: NodeId,
}

impl HttpNetworkConnection {
    // `RPCError` is openraft's error type for every network call; its size
    // is not ours to choose.
    #[allow(clippy::result_large_err)]
    async fn send_rpc<Req, Resp, Err>(
        &self,
        path: &str,
        request: &Req,
    ) -> Result<Resp, RPCError<NodeId, (), Err>>
    where
        Req: Serialize + ?Sized,
        Resp: DeserializeOwned,
        Err: std::error::Error + DeserializeOwned,
    {
        let Some(address) = self.peers.get(&self.target) else {
            let error = std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no HTTP address configured for Raft peer {}", self.target),
            );
            return Err(RPCError::Unreachable(Unreachable::new(&error)));
        };

        let url = format!(
            "{}/{}",
            address.trim_end_matches('/'),
            path.trim_start_matches('/')
        );
        let response = self
            .client
            .post(url)
            .json(request)
            .send()
            .await
            .map_err(classify_reqwest_error)?
            .error_for_status()
            .map_err(classify_reqwest_error)?;

        let result: Result<Resp, Err> = response
            .json()
            .await
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;

        result.map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }
}

fn classify_reqwest_error<N, E>(error: reqwest::Error) -> RPCError<NodeId, N, E>
where
    N: openraft::Node,
    E: std::error::Error,
{
    if error.is_connect() || error.is_timeout() {
        RPCError::Unreachable(Unreachable::new(&error))
    } else {
        RPCError::Network(NetworkError::new(&error))
    }
}

impl RaftNetwork<ControllerRaftTypeConfig> for HttpNetworkConnection {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<ControllerRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, (), RaftError<NodeId>>> {
        self.send_rpc("/raft-append", &request).await
    }

    async fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest<ControllerRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, (), RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.send_rpc("/raft-snapshot", &request).await
    }

    async fn vote(
        &mut self,
        request: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, (), RaftError<NodeId>>> {
        self.send_rpc("/raft-vote", &request).await
    }
}
