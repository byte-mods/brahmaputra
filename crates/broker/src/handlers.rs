//! Request handlers (Blueprint 02 §4–6). Deliberately thin: decode the
//! body, forward to the owning partition actor, encode the response.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use brahmaputra_client::Transport;
use brahmaputra_protocol::codec;
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse, AlterConfigsRequest, ApiVersionRange, ApiVersionsRequest,
    ApiVersionsResponse, AuthenticateRequest, AuthenticateResponse, BrokerInfo,
    DeleteRecordsRequest, DescribeConfigsRequest, DescribeGroupRequest, DescribeGroupResponse,
    DescribeProducersRequest, EndTxnRequest, EndTxnResponse, FetchMultiRequest, FetchRequest,
    FetchResponse, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse,
    LeaveGroupRequest, LeaveGroupResponse, ListGroupsRequest, ListGroupsResponse,
    ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse,
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    PartitionInfo, ProduceResponse, SyncGroupRequest, SyncGroupResponse, TopicInfo,
    TxnMarkerResult, TxnOffsetCommitRequest, TxnOffsetCommitResponse, WriteTxnMarkersRequest,
    WriteTxnMarkersResponse,
};
use brahmaputra_protocol::producer::{InitProducerIdRequest, InitProducerIdResponse};
use brahmaputra_protocol::replica::{
    encode_replica_fetch_response, OffsetsForLeaderEpochRequest, OffsetsForLeaderEpochResponse,
    ReplicaFetchRequest, ReplicaFetchResponse,
};
use brahmaputra_protocol::{
    validate_batch_header, ApiKey, FrameHeader, IsolationLevel, RecordBatch, API_VERSION,
};
use brahmaputra_storage::StorageError;
use bytes::Bytes;
use tracing::warn;

use crate::actor::{PartitionHandle, ProducerAppendError, ReadOutcome};
use crate::error::BrokerError;
use crate::group::{coordinator_partition, CoordinatorShard, OFFSETS_TOPIC};
use crate::producer_id::ProducerIdError;
use crate::quota::QuotaKind;
use crate::server::Broker;
use brahmaputra_metadata::{scram, AclOperation, ResourceType};
use brahmaputra_metrics::{names, MetricKey};

/// Sentinels for `ListOffsetsRequest.timestamp` (Kafka convention).
const TIMESTAMP_LATEST: i64 = -1;
const TIMESTAMP_EARLIEST: i64 = -2;

/// How long a leader holds a caught-up follower's fetch before answering it
/// empty. Long enough that an idle partition costs two round trips a second
/// rather than twenty; short enough that a follower still re-reads metadata
/// and notices a leadership change promptly. `ReplicaFetchRequest` carries
/// no client-chosen wait, so this is the leader's own policy and needs no
/// wire-format change.
const REPLICA_FETCH_MAX_WAIT_MS: u64 = 500;

/// Who a connection is acting as.
///
/// A connection starts anonymous. `Authenticate` binds a principal to it,
/// and every later request on that connection is authorized as that
/// principal. Requests are dispatched concurrently, so the identity lives
/// behind a lock — set once, read often.
#[derive(Debug, Default)]
pub struct ConnectionSession {
    principal: std::sync::RwLock<Option<String>>,
    /// A SCRAM exchange in progress on this connection.
    ///
    /// Per connection, and never shared, because that is what binds the two
    /// halves of the exchange together: a client-final message can only
    /// answer the server-first message sent on the same socket, so a proof
    /// captured elsewhere has nothing to attach itself to.
    scram: std::sync::Mutex<Option<ScramExchange>>,
}

/// What the server remembers between the two steps of a SCRAM exchange.
#[derive(Debug, Clone)]
struct ScramExchange {
    username: String,
    /// Client nonce + server nonce, which the client must echo back.
    nonce: String,
    /// `client-first-bare,server-first,client-final-without-proof`, the
    /// string both sides sign. Built as the exchange proceeds so neither
    /// side can change what was said earlier.
    auth_message_prefix: String,
}

impl ConnectionSession {
    pub fn new() -> Self {
        Self::default()
    }

    /// A connection that arrived already identified — today, by a client
    /// certificate the broker's CA signed during the TLS handshake.
    pub fn authenticated(principal: String) -> Self {
        ConnectionSession {
            principal: std::sync::RwLock::new(Some(principal)),
            scram: std::sync::Mutex::new(None),
        }
    }

    pub fn principal(&self) -> Option<String> {
        self.principal
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn set_principal(&self, principal: String) {
        *self
            .principal
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(principal);
    }
}

/// Who a request is charged to.
///
/// Quotas need both halves: the authenticated principal is the tenant, and
/// `client.id` distinguishes that tenant's individual applications. Kept
/// together in one value so every request-charging path takes the same
/// identity rather than each one deciding which half it can be bothered to
/// pass along.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ClientIdentity<'a> {
    pub principal: Option<&'a str>,
    pub client_id: Option<&'a str>,
}

impl<'a> ClientIdentity<'a> {
    pub fn new(principal: Option<&'a str>, client_id: Option<&'a str>) -> Self {
        ClientIdentity {
            principal,
            client_id,
        }
    }
}

/// Authorize `operation` on a resource for whoever this connection is.
///
/// With authentication disabled this is a no-op, which is what keeps a
/// single-node development broker usable. With it enabled the default is
/// denial: an unauthenticated connection is refused, and an authenticated
/// one still needs a matching ACL.
pub(crate) fn authorize(
    broker: &Broker,
    session: &ConnectionSession,
    resource_type: ResourceType,
    resource_name: &str,
    operation: AclOperation,
) -> Result<(), i32> {
    if !broker.config().require_auth {
        return Ok(());
    }
    let Some(principal) = session.principal() else {
        return Err(ec::SASL_AUTHENTICATION_FAILED);
    };
    let Some(cache) = broker.metadata_cache() else {
        // Authentication was demanded but there is no user store to check
        // against: refuse rather than fall open.
        return Err(ec::AUTHORIZATION_FAILED);
    };
    if cache
        .snapshot()
        .is_authorized(&principal, resource_type, resource_name, operation)
    {
        Ok(())
    } else {
        Err(ec::AUTHORIZATION_FAILED)
    }
}

/// What a request needs permission to do, derived from its body.
///
/// Authorization is decided in one place rather than inside each handler:
/// a security check that is scattered is a security check that is one day
/// forgotten. The cost is decoding the small request struct here as well
/// as in the handler — never the record batches, which are the bulk.
fn required_access(api_key: ApiKey, body: &Bytes) -> Vec<(ResourceType, String, AclOperation)> {
    match api_key {
        ApiKey::Produce => codec::decode_produce_request(body.clone())
            .map(|(request, _)| vec![(ResourceType::Topic, request.topic, AclOperation::Write)])
            .unwrap_or_default(),
        ApiKey::ProduceMulti => codec::decode_produce_multi(body.clone())
            .map(|(request, _)| {
                request
                    .partitions
                    .into_iter()
                    .map(|partition| (ResourceType::Topic, partition.topic, AclOperation::Write))
                    .collect()
            })
            .unwrap_or_default(),
        ApiKey::Fetch => FetchRequest::decode(body)
            .map(|request| vec![(ResourceType::Topic, request.topic, AclOperation::Read)])
            .unwrap_or_default(),
        ApiKey::FetchMulti => FetchMultiRequest::decode(body)
            .map(|request| {
                request
                    .partitions
                    .into_iter()
                    .map(|partition| (ResourceType::Topic, partition.topic, AclOperation::Read))
                    .collect()
            })
            .unwrap_or_default(),
        ApiKey::ListOffsets => ListOffsetsRequest::decode(body)
            .map(|request| vec![(ResourceType::Topic, request.topic, AclOperation::Describe)])
            .unwrap_or_default(),
        ApiKey::JoinGroup => JoinGroupRequest::decode(body)
            .map(|request| vec![(ResourceType::Group, request.group_id, AclOperation::Read)])
            .unwrap_or_default(),
        ApiKey::SyncGroup => SyncGroupRequest::decode(body)
            .map(|request| vec![(ResourceType::Group, request.group_id, AclOperation::Read)])
            .unwrap_or_default(),
        ApiKey::Heartbeat => HeartbeatRequest::decode(body)
            .map(|request| vec![(ResourceType::Group, request.group_id, AclOperation::Read)])
            .unwrap_or_default(),
        ApiKey::LeaveGroup => LeaveGroupRequest::decode(body)
            .map(|request| vec![(ResourceType::Group, request.group_id, AclOperation::Read)])
            .unwrap_or_default(),
        ApiKey::OffsetCommit => OffsetCommitRequest::decode(body)
            .map(|request| vec![(ResourceType::Group, request.group_id, AclOperation::Read)])
            .unwrap_or_default(),
        ApiKey::OffsetFetch => OffsetFetchRequest::decode(body)
            .map(|request| {
                vec![(
                    ResourceType::Group,
                    request.group_id,
                    AclOperation::Describe,
                )]
            })
            .unwrap_or_default(),
        ApiKey::DescribeGroup => DescribeGroupRequest::decode(body)
            .map(|request| {
                vec![(
                    ResourceType::Group,
                    request.group_id,
                    AclOperation::Describe,
                )]
            })
            .unwrap_or_default(),
        // Cluster-wide reads, and the inter-broker replication APIs. The
        // replication APIs serve raw log bytes above the high watermark, so
        // leaving them open would hand out every topic to anyone who can
        // speak the protocol.
        ApiKey::ReplicaFetch | ApiKey::OffsetsForLeaderEpoch => {
            vec![(
                ResourceType::Cluster,
                "cluster".to_string(),
                AclOperation::Read,
            )]
        }
        ApiKey::ListGroups => vec![(
            ResourceType::Cluster,
            "cluster".to_string(),
            AclOperation::Describe,
        )],
        ApiKey::Metadata => vec![(
            ResourceType::Cluster,
            "cluster".to_string(),
            AclOperation::Describe,
        )],
        ApiKey::InitProducerId => vec![(
            ResourceType::Cluster,
            "cluster".to_string(),
            AclOperation::Write,
        )],
        // Transactions are the producer's own session; the id it names is
        // its identity, not a resource anyone else can be granted. What it
        // may *write* is checked when it writes, so a transaction API needs
        // only the right to produce at all.
        ApiKey::AddPartitionsToTxn | ApiKey::AddOffsetsToTxn | ApiKey::EndTxn => {
            vec![(
                ResourceType::Cluster,
                "cluster".to_string(),
                AclOperation::Write,
            )]
        }
        ApiKey::TxnOffsetCommit => TxnOffsetCommitRequest::decode(body)
            .map(|request| vec![(ResourceType::Group, request.group_id, AclOperation::Read)])
            .unwrap_or_default(),
        // Cluster-internal, like the replication APIs: a marker rewrites
        // what a committed reader can see on any partition, so leaving it
        // open would let anyone who can reach the port resolve — or
        // fabricate — another producer's transaction.
        ApiKey::WriteTxnMarkers => vec![(
            ResourceType::Cluster,
            "cluster".to_string(),
            AclOperation::Write,
        )],
        // Cluster introspection. `DescribeLogDirs` names every topic on the
        // broker and how big it is, which is a topic listing by another
        // route, so it is gated at the cluster level rather than per topic.
        ApiKey::DescribeCluster | ApiKey::DescribeLogDirs => vec![(
            ResourceType::Cluster,
            "cluster".to_string(),
            AclOperation::Describe,
        )],
        ApiKey::DescribeConfigs => DescribeConfigsRequest::decode(body)
            .map(|request| {
                // A broker's configuration is cluster-wide information; a
                // topic's belongs to whoever may describe that topic.
                if request.resource_type.eq_ignore_ascii_case("topic") {
                    vec![(
                        ResourceType::Topic,
                        request.resource_name,
                        AclOperation::Describe,
                    )]
                } else {
                    vec![(
                        ResourceType::Cluster,
                        "cluster".to_string(),
                        AclOperation::Describe,
                    )]
                }
            })
            .unwrap_or_default(),
        // Destructive, so it needs write permission on every topic it
        // touches — Describe would let a reader delete what it can see.
        ApiKey::DeleteRecords => DeleteRecordsRequest::decode(body)
            .map(|request| {
                request
                    .partitions
                    .into_iter()
                    .map(|partition| (ResourceType::Topic, partition.topic, AclOperation::Write))
                    .collect()
            })
            .unwrap_or_default(),
        // Changing configuration is not describing it: `retention.ms` set
        // to zero deletes a topic's data as surely as deleting the topic
        // would, so this needs write permission on the resource.
        ApiKey::AlterConfigs => AlterConfigsRequest::decode(body)
            .map(|request| {
                if request.resource_type.eq_ignore_ascii_case("topic") {
                    vec![(
                        ResourceType::Topic,
                        request.resource_name,
                        AclOperation::Write,
                    )]
                } else {
                    vec![(
                        ResourceType::Cluster,
                        "cluster".to_string(),
                        AclOperation::Write,
                    )]
                }
            })
            .unwrap_or_default(),
        // Producer state on a partition is information about that
        // partition, so it is gated exactly as describing the topic is.
        ApiKey::DescribeProducers => DescribeProducersRequest::decode(body)
            .map(|request| vec![(ResourceType::Topic, request.topic, AclOperation::Describe)])
            .unwrap_or_default(),
        // A transaction listing names every transactional id on the
        // broker, which is cluster-wide information however narrow the
        // filter.
        ApiKey::ListTransactions | ApiKey::DescribeTransactions => vec![(
            ResourceType::Cluster,
            "cluster".to_string(),
            AclOperation::Describe,
        )],
        // Moving data between disks is an operation on the broker, not on
        // any one topic: it takes partitions offline while it runs.
        ApiKey::AlterReplicaLogDirs => vec![(
            ResourceType::Cluster,
            "cluster".to_string(),
            AclOperation::Write,
        )],
        // Answered before authentication: a client has to be able to
        // discover versions and to authenticate at all.
        ApiKey::ApiVersions | ApiKey::Authenticate => Vec::new(),
    }
}

/// One step of a SCRAM-SHA-256 exchange (RFC 5802).
///
/// Two round trips, and the connection remembers what happened in between:
///
/// ```text
/// client -> n,,n=alice,r=CLIENTNONCE
/// server -> r=CLIENTNONCESERVERNONCE,s=SALT,i=4096
/// client -> c=biws,r=CLIENTNONCESERVERNONCE,p=PROOF
/// server -> v=SERVERSIGNATURE
/// ```
///
/// The password appears nowhere. The client proves it can compute a key
/// derived from the password over a message *both* sides contributed
/// randomness to, which is what makes a captured exchange useless: replayed
/// against a new connection it answers a challenge nobody asked.
fn scram_step(
    broker: &Broker,
    request: &AuthenticateRequest,
    session: &ConnectionSession,
) -> Bytes {
    let respond = |error_code, principal: &str, role: &str, payload: String, done: bool| {
        Bytes::from(
            AuthenticateResponse {
                error_code,
                principal: principal.to_owned(),
                role: role.to_owned(),
                payload,
                done,
            }
            .encode()
            .unwrap_or_default(),
        )
    };
    let failed = || respond(ec::SASL_AUTHENTICATION_FAILED, "", "", String::new(), true);

    let Some(cache) = broker.metadata_cache() else {
        // Standalone has no user store to authenticate against.
        return failed();
    };
    let image = cache.snapshot();

    let in_progress = session
        .scram
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();

    let Some(exchange) = in_progress else {
        // Step one: the client-first message.
        let message = &request.payload;
        // The GS2 header is the part before the second comma. Channel
        // binding is not offered, so anything other than "n" or "y" is a
        // client asking for something this broker cannot honour.
        let Some(bare) = message
            .split_once(",,")
            .map(|(_, bare)| bare)
            .filter(|_| message.starts_with("n,") || message.starts_with("y,"))
        else {
            return failed();
        };
        let (Some(username), Some(client_nonce)) =
            (scram::field(bare, "n"), scram::field(bare, "r"))
        else {
            return failed();
        };
        // A user that does not exist, and one without a SCRAM credential,
        // are answered exactly as a wrong password is — including with a
        // well-formed challenge, so an attacker cannot enumerate accounts
        // by watching which usernames get one.
        let credential = image
            .users
            .get(username)
            .and_then(|user| user.scram.clone());
        let server_nonce = scram::random_nonce();
        let nonce = format!("{client_nonce}{server_nonce}");
        let (salt, iterations) = match &credential {
            Some(credential) => (credential.salt.clone(), credential.iterations),
            None => (
                // A decoy of the right shape. The exchange proceeds and
                // fails at the proof, which is where a wrong password
                // fails too.
                base64_of(scram::random_salt()),
                scram::DEFAULT_ITERATIONS,
            ),
        };
        let server_first = format!("r={nonce},s={salt},i={iterations}");
        *session
            .scram
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ScramExchange {
            username: username.to_owned(),
            nonce,
            auth_message_prefix: format!("{bare},{server_first}"),
        });
        return respond(ec::NONE, "", "", server_first, false);
    };

    // Step two: the client-final message.
    let message = &request.payload;
    let (Some(nonce), Some(proof)) = (scram::field(message, "r"), scram::field(message, "p"))
    else {
        return failed();
    };
    if nonce != exchange.nonce {
        // The client answered a different challenge than the one this
        // connection issued.
        return failed();
    }
    let without_proof = match message.rsplit_once(",p=") {
        Some((head, _)) => head,
        None => return failed(),
    };
    let auth_message = format!("{},{}", exchange.auth_message_prefix, without_proof);

    let Some(user) = image.users.get(&exchange.username) else {
        return failed();
    };
    let Some(credential) = user.scram.as_ref() else {
        return failed();
    };
    if !credential.verify_proof(&auth_message, proof) {
        warn!(username = %exchange.username, "failed SCRAM authentication");
        return failed();
    }

    // The server proves itself in the same breath, so the client knows it
    // is not talking to something that merely collected a proof.
    let server_final = format!("v={}", credential.server_signature(&auth_message));
    session.set_principal(user.username.clone());
    *session
        .scram
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    respond(
        ec::NONE,
        &user.username,
        &format!("{:?}", user.role).to_lowercase(),
        server_final,
        true,
    )
}

fn base64_of(bytes: Vec<u8>) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}
async fn authenticate(broker: &Broker, body: Bytes, session: &ConnectionSession) -> Bytes {
    let respond = |error_code, principal: &str, role: &str| {
        Bytes::from(
            AuthenticateResponse {
                error_code,
                principal: principal.to_owned(),
                role: role.to_owned(),
                payload: String::new(),
                done: true,
            }
            .encode()
            .unwrap_or_default(),
        )
    };
    let request = match AuthenticateRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable Authenticate request");
            return respond(ec::INVALID_REQUEST, "", "");
        }
    };

    if request.mechanism.eq_ignore_ascii_case(scram::MECHANISM) {
        return scram_step(broker, &request, session);
    }
    if !request.mechanism.is_empty() && !request.mechanism.eq_ignore_ascii_case("PLAIN") {
        warn!(mechanism = %request.mechanism, "unsupported SASL mechanism");
        return respond(ec::UNSUPPORTED_SASL_MECHANISM, "", "");
    }

    // A password in the clear is only meaningful under encryption. Refusing
    // it on a plaintext listener stops a deployment from believing it has
    // authentication when it is handing credentials to the network.
    //
    // SCRAM is the answer for a plaintext listener, and it is checked above
    // this line for exactly that reason: it never sends the password, so
    // the objection does not apply to it.
    if broker.config().transport == Transport::Tcp {
        warn!("PLAIN authentication refused on a plaintext listener");
        return respond(ec::SASL_AUTHENTICATION_FAILED, "", "");
    }
    let Some(cache) = broker.metadata_cache() else {
        return respond(ec::SASL_AUTHENTICATION_FAILED, "", "");
    };
    let image = cache.snapshot();
    let Some(user) = image.users.get(&request.username) else {
        // Same response as a wrong password: do not reveal which accounts
        // exist.
        return respond(ec::SASL_AUTHENTICATION_FAILED, "", "");
    };
    if !brahmaputra_metadata::password::verify(&request.password, &user.password_hash) {
        return respond(ec::SASL_AUTHENTICATION_FAILED, "", "");
    }
    session.set_principal(user.username.clone());
    respond(
        ec::NONE,
        &user.username,
        &format!("{:?}", user.role).to_lowercase(),
    )
}

/// A response as the buffers it will be written from.
///
/// Most responses are one small struct and stay a single buffer. A Fetch
/// response is a small struct followed by record batches that came
/// straight out of the page cache and that nothing needs to modify —
/// concatenating those into one buffer, copying that into a frame, and
/// copying that into a socket buffer copies every byte served three times
/// before the kernel sees it. Keeping the pieces apart lets one `writev`
/// take them as they are.
pub struct ResponseBody {
    chunks: Vec<Bytes>,
    /// File ranges written after the chunks, straight from the page cache.
    /// Only ever set on the plaintext path, where nothing has to look at
    /// the bytes on the way out.
    regions: Vec<brahmaputra_storage::LogRegion>,
}

impl ResponseBody {
    pub fn chunks(&self) -> &[Bytes] {
        &self.chunks
    }

    pub fn regions(&self) -> &[brahmaputra_storage::LogRegion] {
        &self.regions
    }

    /// Total body length, which the frame prefix has to declare before any
    /// of it is written.
    pub fn len(&self) -> usize {
        self.chunks.iter().map(Bytes::len).sum::<usize>()
            + self.regions.iter().map(|region| region.len).sum::<usize>()
    }

    /// A response whose trailing batches are file ranges rather than
    /// buffers.
    pub fn with_regions(header: Bytes, regions: Vec<brahmaputra_storage::LogRegion>) -> Self {
        ResponseBody {
            chunks: vec![header],
            regions,
        }
    }
}

impl From<Bytes> for ResponseBody {
    fn from(body: Bytes) -> Self {
        ResponseBody {
            chunks: vec![body],
            regions: Vec::new(),
        }
    }
}

impl From<Vec<Bytes>> for ResponseBody {
    fn from(chunks: Vec<Bytes>) -> Self {
        ResponseBody {
            chunks,
            regions: Vec::new(),
        }
    }
}

/// Dispatch one decoded frame to its handler. `None` means "no response"
/// (only `Produce` with `acks=0`).
pub async fn dispatch(
    broker: &Broker,
    header: &FrameHeader,
    body: Bytes,
    session: &ConnectionSession,
) -> Option<ResponseBody> {
    // ApiVersions answers at any requested version on purpose: it is how a
    // client discovers what this broker speaks, so refusing it for a version
    // mismatch would make version negotiation impossible — the exact
    // situation a rolling upgrade has to survive.
    if header.api_key == ApiKey::ApiVersions {
        return Some(api_versions(broker, body).await.into());
    }
    // Authenticating is how a connection stops being anonymous, so it
    // cannot itself require a principal.
    if header.api_key == ApiKey::Authenticate {
        return Some(authenticate(broker, body, session).await.into());
    }
    if header.api_version != API_VERSION {
        return Some(encode_error_for(header.api_key, ec::UNSUPPORTED_VERSION).into());
    }
    // Everything past this point is authorized. With authentication off
    // this costs one branch; with it on, an unauthenticated or unpermitted
    // request never reaches a handler.
    if broker.config().require_auth {
        for (resource_type, name, operation) in required_access(header.api_key, &body) {
            if let Err(error_code) = authorize(broker, session, resource_type, &name, operation) {
                return Some(encode_error_for(header.api_key, error_code).into());
            }
        }
    }
    let client_id = header.client_id.as_deref();
    let principal = session.principal();
    let client = ClientIdentity::new(principal.as_deref(), client_id);
    broker.metrics().increment(
        MetricKey::with(names::REQUESTS, &[("api", api_name(header.api_key))]),
        1,
    );
    match header.api_key {
        ApiKey::Produce => produce(broker, body, client).await.map(ResponseBody::from),
        // The fetch paths return their pieces rather than one buffer, so
        // the record batches reach the socket without being copied again.
        ApiKey::Fetch => Some(fetch(broker, body, client).await.into()),
        ApiKey::FetchMulti => Some(crate::multi::fetch_multi(broker, body, client).await),
        ApiKey::ReplicaFetch => Some(replica_fetch(broker, body).await.into()),
        ApiKey::ListOffsets => Some(list_offsets(broker, body).await.into()),
        ApiKey::Metadata => Some(metadata(broker, body).into()),
        ApiKey::OffsetsForLeaderEpoch => Some(offsets_for_leader_epoch(broker, body).await.into()),
        ApiKey::InitProducerId => Some(init_producer_id(broker, body).await.into()),
        ApiKey::JoinGroup => Some(join_group(broker, body).await.into()),
        ApiKey::SyncGroup => Some(sync_group(broker, body).await.into()),
        ApiKey::Heartbeat => Some(heartbeat(broker, body).await.into()),
        ApiKey::LeaveGroup => Some(leave_group(broker, body).await.into()),
        ApiKey::OffsetCommit => Some(offset_commit(broker, body).await.into()),
        ApiKey::OffsetFetch => Some(offset_fetch(broker, body).await.into()),
        ApiKey::ListGroups => Some(list_groups(broker, body).await.into()),
        ApiKey::DescribeGroup => Some(describe_group(broker, body).await.into()),
        ApiKey::ApiVersions => Some(api_versions(broker, body).await.into()),
        ApiKey::Authenticate => Some(authenticate(broker, body, session).await.into()),
        ApiKey::ProduceMulti => crate::multi::produce_multi(broker, body, client)
            .await
            .map(ResponseBody::from),
        ApiKey::AddPartitionsToTxn => Some(add_partitions_to_txn(broker, body).await.into()),
        ApiKey::AddOffsetsToTxn => Some(add_offsets_to_txn(broker, body).await.into()),
        ApiKey::EndTxn => Some(end_txn(broker, body).await.into()),
        ApiKey::TxnOffsetCommit => Some(txn_offset_commit(broker, body).await.into()),
        ApiKey::WriteTxnMarkers => Some(write_txn_markers(broker, body).await.into()),
        ApiKey::DescribeCluster => Some(crate::admin::describe_cluster(broker, body).into()),
        ApiKey::DescribeConfigs => Some(crate::admin::describe_configs(broker, body).into()),
        ApiKey::DescribeLogDirs => Some(crate::admin::describe_log_dirs(broker, body).await.into()),
        ApiKey::DeleteRecords => Some(crate::admin::delete_records(broker, body).await.into()),
        ApiKey::AlterConfigs => Some(crate::admin::alter_configs(broker, body).await.into()),
        ApiKey::DescribeProducers => {
            Some(crate::admin::describe_producers(broker, body).await.into())
        }
        ApiKey::ListTransactions => {
            Some(crate::admin::list_transactions(broker, body).await.into())
        }
        ApiKey::DescribeTransactions => Some(
            crate::admin::describe_transactions(broker, body)
                .await
                .into(),
        ),
        ApiKey::AlterReplicaLogDirs => Some(
            crate::admin::alter_replica_log_dirs(broker, body)
                .await
                .into(),
        ),
    }
}

/// Build the matching response struct with just an error code set, for
/// requests we could not even decode.
pub(crate) fn encode_error_for(api_key: ApiKey, error_code: i32) -> Bytes {
    let bytes = match api_key {
        ApiKey::Produce => ProduceResponse {
            error_code,
            ..Default::default()
        }
        .encode(),
        ApiKey::Fetch => FetchResponse {
            error_code,
            ..Default::default()
        }
        .encode(),
        ApiKey::ListOffsets => ListOffsetsResponse {
            error_code,
            ..Default::default()
        }
        .encode(),
        ApiKey::Metadata => MetadataResponse {
            // No error_code field on MetadataResponse itself; report an
            // empty cluster (topics carry their own error codes).
            ..Default::default()
        }
        .encode(),
        ApiKey::ReplicaFetch => {
            return encode_replica_fetch_response(
                &ReplicaFetchResponse {
                    topic: String::new(),
                    partition: -1,
                    error_code,
                    leader_epoch: -1,
                    high_watermark: -1,
                    log_start_offset: -1,
                    log_end_offset: -1,
                    batches_length: 0,
                },
                &[],
            )
            .unwrap_or_default();
        }
        ApiKey::OffsetsForLeaderEpoch => {
            return OffsetsForLeaderEpochResponse {
                topic: String::new(),
                partition: -1,
                error_code,
                leader_epoch: -1,
                end_offset: -1,
            }
            .encode()
            .unwrap_or_default();
        }
        ApiKey::InitProducerId => {
            return InitProducerIdResponse {
                error_code,
                producer_id: -1,
                producer_epoch: -1,
            }
            .encode();
        }
        ApiKey::JoinGroup => JoinGroupResponse {
            error_code,
            generation: -1,
            ..Default::default()
        }
        .encode(),
        ApiKey::SyncGroup => SyncGroupResponse {
            error_code,
            ..Default::default()
        }
        .encode(),
        ApiKey::Heartbeat => HeartbeatResponse { error_code }.encode(),
        ApiKey::LeaveGroup => LeaveGroupResponse { error_code }.encode(),
        ApiKey::OffsetCommit => OffsetCommitResponse { error_code }.encode(),
        ApiKey::OffsetFetch => OffsetFetchResponse {
            error_code,
            ..Default::default()
        }
        .encode(),
        ApiKey::ListGroups => ListGroupsResponse {
            error_code,
            ..Default::default()
        }
        .encode(),
        ApiKey::DescribeGroup => DescribeGroupResponse {
            error_code,
            generation: -1,
            coordinator_partition: -1,
            ..Default::default()
        }
        .encode(),
        ApiKey::Authenticate => AuthenticateResponse {
            error_code,
            ..Default::default()
        }
        .encode(),
        ApiKey::ApiVersions => ApiVersionsResponse {
            error_code,
            ..Default::default()
        }
        .encode(),
        // Batched APIs report failures per partition; a request-level error
        // means the request itself was unusable, so the result list is
        // empty rather than carrying a partition that was never attempted.
        ApiKey::ProduceMulti => brahmaputra_protocol::gen::ProduceMultiResponse::default().encode(),
        ApiKey::FetchMulti => brahmaputra_protocol::gen::FetchMultiResponse::default().encode(),
        ApiKey::DeleteRecords => {
            brahmaputra_protocol::gen::DeleteRecordsResponse::default().encode()
        }
        ApiKey::DescribeCluster => brahmaputra_protocol::gen::DescribeClusterResponse {
            error_code,
            controller_id: -1,
            ..Default::default()
        }
        .encode(),
        ApiKey::DescribeConfigs => brahmaputra_protocol::gen::DescribeConfigsResponse {
            error_code,
            ..Default::default()
        }
        .encode(),
        ApiKey::DescribeLogDirs => brahmaputra_protocol::gen::DescribeLogDirsResponse {
            error_code,
            ..Default::default()
        }
        .encode(),
        ApiKey::AddPartitionsToTxn => {
            brahmaputra_protocol::gen::AddPartitionsToTxnResponse { error_code }.encode()
        }
        ApiKey::AddOffsetsToTxn => {
            brahmaputra_protocol::gen::AddOffsetsToTxnResponse { error_code }.encode()
        }
        ApiKey::EndTxn => brahmaputra_protocol::gen::EndTxnResponse { error_code }.encode(),
        ApiKey::TxnOffsetCommit => {
            brahmaputra_protocol::gen::TxnOffsetCommitResponse { error_code }.encode()
        }
        // Per-partition results; a request-level failure names none of them.
        ApiKey::WriteTxnMarkers => {
            brahmaputra_protocol::gen::WriteTxnMarkersResponse::default().encode()
        }
        ApiKey::AlterConfigs => brahmaputra_protocol::gen::AlterConfigsResponse {
            error_code,
            ..Default::default()
        }
        .encode(),
        ApiKey::DescribeProducers => brahmaputra_protocol::gen::DescribeProducersResponse {
            error_code,
            ..Default::default()
        }
        .encode(),
        ApiKey::ListTransactions => brahmaputra_protocol::gen::ListTransactionsResponse {
            error_code,
            ..Default::default()
        }
        .encode(),
        ApiKey::DescribeTransactions => brahmaputra_protocol::gen::DescribeTransactionsResponse {
            error_code,
            ..Default::default()
        }
        .encode(),
        // Per-partition results; a request-level failure names none.
        ApiKey::AlterReplicaLogDirs => {
            brahmaputra_protocol::gen::AlterReplicaLogDirsResponse::default().encode()
        }
    };
    Bytes::from(bytes.unwrap_or_default())
}

// ---------- InitProducerId (api_key 6) ----------

async fn init_producer_id(broker: &Broker, body: Bytes) -> Bytes {
    if let Err(error) = broker.validate_local_broker_lease() {
        return encode_error_for(ApiKey::InitProducerId, code_of(&error));
    }
    let request = match InitProducerIdRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable InitProducerId request");
            return InitProducerIdResponse {
                error_code: ec::INVALID_REQUEST,
                producer_id: -1,
                producer_epoch: -1,
            }
            .encode();
        }
    };

    // A transactional id makes this a coordinator operation rather than a
    // bare identity allocation: the coordinator fences whatever instance
    // held the id before and resolves anything that instance abandoned.
    if let Some(transactional_id) = request.transactional_id.as_deref() {
        // Clamped, because the timeout is what bounds how long an abandoned
        // transaction can hold every committed reader of the partitions it
        // touched — that is the broker's problem, not the client's to
        // decide without limit.
        return match crate::transaction::init_transactional_producer(
            broker,
            transactional_id,
            crate::transaction::clamp_transaction_timeout(broker, request.transaction_timeout_ms),
        )
        .await
        {
            Ok((producer_id, producer_epoch)) => InitProducerIdResponse {
                error_code: ec::NONE,
                producer_id,
                producer_epoch,
            }
            .encode(),
            Err(error) => InitProducerIdResponse {
                error_code: code_of(&error),
                producer_id: -1,
                producer_epoch: -1,
            }
            .encode(),
        };
    }

    let result = if request == InitProducerIdRequest::allocate() {
        broker.producer_ids().allocate()
    } else if request.producer_id >= 0 && request.producer_epoch >= 0 {
        broker
            .producer_ids()
            .bump(request.producer_id, request.producer_epoch)
    } else {
        return InitProducerIdResponse {
            error_code: ec::INVALID_REQUEST,
            producer_id: request.producer_id,
            producer_epoch: request.producer_epoch,
        }
        .encode();
    };
    match result {
        Ok((producer_id, producer_epoch)) => InitProducerIdResponse {
            error_code: ec::NONE,
            producer_id,
            producer_epoch,
        }
        .encode(),
        Err(ProducerIdError::Fenced { current, .. }) => InitProducerIdResponse {
            error_code: ec::FENCED_PRODUCER_EPOCH,
            producer_id: request.producer_id,
            producer_epoch: current,
        }
        .encode(),
        Err(ProducerIdError::Unknown(_) | ProducerIdError::EpochExhausted(_)) => {
            InitProducerIdResponse {
                error_code: ec::INVALID_REQUEST,
                producer_id: request.producer_id,
                producer_epoch: -1,
            }
            .encode()
        }
        Err(ProducerIdError::Io(error)) => {
            warn!(%error, "producer identity journal update failed");
            InitProducerIdResponse {
                error_code: ec::INTERNAL,
                producer_id: -1,
                producer_epoch: -1,
            }
            .encode()
        }
        Err(ProducerIdError::IdExhausted) => InitProducerIdResponse {
            error_code: ec::INTERNAL,
            producer_id: -1,
            producer_epoch: -1,
        }
        .encode(),
    }
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// BrokerError → wire error code.
pub(crate) fn code_of(err: &BrokerError) -> i32 {
    match err {
        BrokerError::UnknownTopicOrPartition { .. } | BrokerError::InvalidTopic(_) => {
            ec::UNKNOWN_TOPIC_OR_PARTITION
        }
        BrokerError::NotLeaderOrFollower { .. } => ec::NOT_LEADER_OR_FOLLOWER,
        BrokerError::FencedBrokerEpoch { .. } => ec::FENCED_BROKER_EPOCH,
        BrokerError::FencedLeaderEpoch { .. } => ec::FENCED_LEADER_EPOCH,
        BrokerError::UnknownLeaderEpoch { .. } => ec::UNKNOWN_LEADER_EPOCH,
        BrokerError::NotEnoughReplicas { .. } => ec::NOT_ENOUGH_REPLICAS,
        BrokerError::Storage(StorageError::OffsetOutOfRange { .. }) => ec::OFFSET_OUT_OF_RANGE,
        BrokerError::NotCoordinator { .. } => ec::NOT_COORDINATOR,
        BrokerError::UnknownMemberId { .. } => ec::UNKNOWN_MEMBER_ID,
        BrokerError::IllegalGeneration { .. } => ec::ILLEGAL_GENERATION,
        BrokerError::RebalanceInProgress { .. } => ec::REBALANCE_IN_PROGRESS,
        BrokerError::CoordinatorLoadInProgress { .. } => ec::COORDINATOR_LOAD_IN_PROGRESS,
        BrokerError::LogDirOffline { .. } => ec::LOG_DIR_OFFLINE,
        _ => ec::INTERNAL,
    }
}

fn producer_code_of(err: &ProducerAppendError) -> i32 {
    match err {
        ProducerAppendError::InvalidMetadata => ec::INVALID_REQUEST,
        ProducerAppendError::FencedEpoch { .. } => ec::FENCED_PRODUCER_EPOCH,
        ProducerAppendError::OutOfOrderSequence { .. } => ec::OUT_OF_ORDER_SEQUENCE,
        ProducerAppendError::Storage(StorageError::OffsetOutOfRange { .. }) => {
            ec::OFFSET_OUT_OF_RANGE
        }
        ProducerAppendError::Storage(_) => ec::INTERNAL,
    }
}

// ---------- ReplicaFetch (api_key 4, cluster-internal) ----------

async fn replica_fetch(broker: &Broker, body: Bytes) -> Bytes {
    let request = match ReplicaFetchRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable replica-fetch request");
            return encode_error_for(ApiKey::ReplicaFetch, ec::INVALID_REQUEST);
        }
    };
    let current_epoch = current_leader_epoch(broker, &request.topic, request.partition);
    let respond = |error_code, outcome: Option<&ReadOutcome>| {
        let response = ReplicaFetchResponse {
            topic: request.topic.clone(),
            partition: request.partition,
            error_code,
            leader_epoch: current_epoch,
            high_watermark: outcome.map_or(-1, |outcome| outcome.high_watermark),
            log_start_offset: outcome.map_or(-1, |outcome| outcome.log_start_offset),
            log_end_offset: outcome.map_or(-1, |outcome| outcome.log_end_offset),
            batches_length: 0,
        };
        let batches = outcome.map_or(&[][..], |outcome| outcome.batches.as_slice());
        encode_replica_fetch_response(&response, batches).unwrap_or_default()
    };

    if request.fetch_offset < 0 || request.max_bytes <= 0 {
        return respond(ec::INVALID_REQUEST, None);
    }
    let validation = match validate_replica_request(
        broker,
        &request.topic,
        request.partition,
        request.follower_id,
        request.follower_broker_epoch,
        request.leader_epoch,
    ) {
        Ok(validation) => validation,
        Err(error) => return respond(code_of(&error), None),
    };
    if let Err(error) = validation
        .handle
        .record_leader_epoch(validation.leader_epoch)
        .await
    {
        return respond(code_of(&BrokerError::Storage(error)), None);
    }
    if broker.config().replication_enabled {
        if let Err(error) = broker
            .replication_tracker()
            .observe_leader_fetch(
                &request.topic,
                &validation.assignment,
                request.follower_id,
                request.follower_broker_epoch,
                request.fetch_offset,
                &validation.handle,
            )
            .await
        {
            return respond(code_of(&BrokerError::Storage(error)), None);
        }
    }
    // Long-poll a caught-up follower rather than answering it empty.
    //
    // Without this the follower's only way to notice a new append is to ask
    // again, and its loop sleeps between empty answers — so under
    // `acks=all` every producer waits out that sleep before its record can
    // commit, and the whole cluster runs at the polling interval instead of
    // at the speed of the log. Holding the request here costs one task on a
    // connection that belongs to this one partition's fetcher, and the
    // client fetch path already blocks the same way.
    //
    // The wait is on the *append* watch, not the watermark: under
    // `acks=all` the watermark cannot advance until this follower fetches,
    // so waiting on the watermark would be waiting on ourselves.
    let mut appends = validation.handle.append_watch();
    let mut read = validation
        .handle
        .read_uncommitted(request.fetch_offset, request.max_bytes as usize)
        .await;
    if matches!(&read, Ok(outcome) if outcome.batches.is_empty()) {
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(REPLICA_FETCH_MAX_WAIT_MS);
        while let Ok(Ok(())) = tokio::time::timeout_at(deadline, appends.changed()).await {
            read = validation
                .handle
                .read_uncommitted(request.fetch_offset, request.max_bytes as usize)
                .await;
            match &read {
                Ok(outcome) if outcome.batches.is_empty() => continue,
                _ => break,
            }
        }
        // Leadership can move while the request is parked here. Re-check
        // before answering: a broker demoted during the wait would
        // otherwise serve batches under an epoch it no longer owns, and the
        // follower would accept them as committed history. Validation
        // demands an exact leader-epoch match, so if it still passes the
        // epoch captured above is still the right one to report.
        if let Err(error) = validate_replica_request(
            broker,
            &request.topic,
            request.partition,
            request.follower_id,
            request.follower_broker_epoch,
            request.leader_epoch,
        ) {
            return respond(code_of(&error), None);
        }
    }
    match read {
        Ok(outcome) => {
            // Charge the bytes against the replication budget before
            // answering. A rejoining broker otherwise fetches as fast as
            // this leader can read, competing with client traffic for the
            // same disk and NIC, so one restart surfaces as latency for
            // every producer and consumer on this broker.
            //
            // Delaying the response rather than truncating it keeps
            // catch-up correct: the follower still receives every byte it
            // asked for, just no faster than the ceiling allows.
            let served: u64 = outcome.batches.iter().map(|batch| batch.len() as u64).sum();
            if served > 0 {
                broker
                    .throttle(
                        None,
                        Some(&format!("replica-{}", request.follower_id)),
                        crate::quota::QuotaKind::Replication,
                        served,
                    )
                    .await;
            }
            respond(ec::NONE, Some(&outcome))
        }
        Err(error @ StorageError::OffsetOutOfRange { .. }) => {
            match validation.handle.offsets().await {
                Ok((log_start_offset, log_end_offset, high_watermark)) => {
                    let offsets = ReadOutcome {
                        batches: Vec::new(),
                        high_watermark,
                        log_start_offset,
                        log_end_offset,
                    };
                    respond(code_of(&BrokerError::Storage(error)), Some(&offsets))
                }
                Err(offset_error) => respond(code_of(&BrokerError::Storage(offset_error)), None),
            }
        }
        Err(error) => respond(code_of(&BrokerError::Storage(error)), None),
    }
}

// ---------- OffsetsForLeaderEpoch (api_key 5, cluster-internal) ----------

async fn offsets_for_leader_epoch(broker: &Broker, body: Bytes) -> Bytes {
    let request = match OffsetsForLeaderEpochRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable offsets-for-leader-epoch request");
            return encode_error_for(ApiKey::OffsetsForLeaderEpoch, ec::INVALID_REQUEST);
        }
    };
    let current_epoch = current_leader_epoch(broker, &request.topic, request.partition);
    let respond = |error_code, end_offset| {
        OffsetsForLeaderEpochResponse {
            topic: request.topic.clone(),
            partition: request.partition,
            error_code,
            leader_epoch: current_epoch,
            end_offset,
        }
        .encode()
        .unwrap_or_default()
    };

    if request.query_leader_epoch < 0 {
        return respond(ec::INVALID_REQUEST, -1);
    }
    let validation = match validate_replica_request(
        broker,
        &request.topic,
        request.partition,
        request.follower_id,
        request.follower_broker_epoch,
        request.leader_epoch,
    ) {
        Ok(validation) => validation,
        Err(error) => return respond(code_of(&error), -1),
    };
    if request.query_leader_epoch > validation.leader_epoch {
        return respond(ec::UNKNOWN_LEADER_EPOCH, -1);
    }
    if let Err(error) = validation
        .handle
        .record_leader_epoch(validation.leader_epoch)
        .await
    {
        return respond(code_of(&BrokerError::Storage(error)), -1);
    }
    match validation
        .handle
        .end_offset_for_leader_epoch(request.query_leader_epoch)
        .await
    {
        Ok(Some(end_offset)) => respond(ec::NONE, end_offset),
        Ok(None) => respond(ec::UNKNOWN_LEADER_EPOCH, -1),
        Err(error) => respond(code_of(&BrokerError::Storage(error)), -1),
    }
}

struct ReplicaValidation {
    handle: PartitionHandle,
    leader_epoch: i32,
    assignment: brahmaputra_metadata::PartitionMetadata,
}

/// Validate every controller fence before opening the local leader log.
/// Error ordering is intentional: an obsolete broker incarnation is fenced
/// even when it also carries stale partition metadata.
fn validate_replica_request(
    broker: &Broker,
    topic: &str,
    partition: i32,
    follower_id: i32,
    follower_broker_epoch: u64,
    leader_epoch: i32,
) -> Result<ReplicaValidation, BrokerError> {
    let image = broker
        .metadata_cache()
        .ok_or_else(|| BrokerError::NotLeaderOrFollower {
            topic: topic.to_owned(),
            partition,
            broker_id: broker.config().broker_id,
            leader: broker.config().broker_id,
        })?
        .snapshot();
    broker.validate_local_broker_epoch(&image)?;
    let assignment = image
        .topics
        .get(topic)
        .and_then(|topic| topic.partitions.get(&partition))
        .ok_or_else(|| BrokerError::UnknownTopicOrPartition {
            topic: topic.to_owned(),
            partition,
        })?;
    if assignment.leader != broker.config().broker_id {
        return Err(BrokerError::NotLeaderOrFollower {
            topic: topic.to_owned(),
            partition,
            broker_id: broker.config().broker_id,
            leader: assignment.leader,
        });
    }
    let local_is_broker = image
        .brokers
        .get(&broker.config().broker_id)
        .is_some_and(|local| {
            local.alive
                && local
                    .roles
                    .contains(&brahmaputra_metadata::NodeRole::Broker)
        });
    if !local_is_broker {
        return Err(BrokerError::NotLeaderOrFollower {
            topic: topic.to_owned(),
            partition,
            broker_id: broker.config().broker_id,
            leader: assignment.leader,
        });
    }
    if follower_id == assignment.leader || !assignment.replicas.contains(&follower_id) {
        return Err(BrokerError::NotLeaderOrFollower {
            topic: topic.to_owned(),
            partition,
            broker_id: follower_id,
            leader: assignment.leader,
        });
    }
    let follower = image
        .brokers
        .get(&follower_id)
        .filter(|follower| {
            follower.alive
                && follower
                    .roles
                    .contains(&brahmaputra_metadata::NodeRole::Broker)
        })
        .ok_or_else(|| BrokerError::NotLeaderOrFollower {
            topic: topic.to_owned(),
            partition,
            broker_id: follower_id,
            leader: assignment.leader,
        })?;
    if follower.broker_epoch != follower_broker_epoch {
        return Err(BrokerError::FencedBrokerEpoch {
            broker_id: follower_id,
            requested: follower_broker_epoch,
            current: follower.broker_epoch,
        });
    }
    if leader_epoch < assignment.leader_epoch {
        return Err(BrokerError::FencedLeaderEpoch {
            requested: leader_epoch,
            current: assignment.leader_epoch,
        });
    }
    if leader_epoch > assignment.leader_epoch {
        return Err(BrokerError::UnknownLeaderEpoch {
            requested: leader_epoch,
            current: assignment.leader_epoch,
        });
    }
    Ok(ReplicaValidation {
        handle: broker.partition(topic, partition)?,
        leader_epoch: assignment.leader_epoch,
        assignment: assignment.clone(),
    })
}

fn current_leader_epoch(broker: &Broker, topic: &str, partition: i32) -> i32 {
    broker
        .metadata_cache()
        .and_then(|cache| {
            let image = cache.snapshot();
            image
                .topics
                .get(topic)
                .and_then(|topic| topic.partitions.get(&partition))
                .map(|partition| partition.leader_epoch)
        })
        .unwrap_or(-1)
}

// ---------- Produce (api_key 0) ----------

async fn produce(broker: &Broker, body: Bytes, client: ClientIdentity<'_>) -> Option<Bytes> {
    let (req, raw_batches) = match codec::decode_produce_request(body) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "undecodable produce request");
            return Some(encode_error_for(ApiKey::Produce, ec::INVALID_REQUEST));
        }
    };
    let acks = req.acks;
    let respond = |error_code, base_offset| {
        let resp = ProduceResponse {
            topic: req.topic.clone(),
            partition: req.partition,
            error_code,
            base_offset,
            log_append_time_ms: now_ms(),
        };
        codec_bytes(resp.encode())
    };

    if !matches!(acks, -1..=1) {
        return Some(respond(ec::INVALID_REQUEST, -1));
    }

    // Validate framing and CRC from the header alone. Everything the
    // non-idempotent path needs — record count, producer metadata, magic —
    // is in that header, so batches reach the log as the exact bytes the
    // producer sent (DESIGN.md §4.1). Only the idempotent path decodes,
    // because its deduplication compares decoded batches.
    let mut headers = Vec::with_capacity(raw_batches.len());
    for raw in &raw_batches {
        match validate_batch_header(raw) {
            Ok(header) => headers.push(header),
            Err(e) => {
                warn!(error = %e, "produce request carried a corrupt batch");
                if acks == 0 {
                    return None;
                }
                return Some(respond(ec::INVALID_REQUEST, -1));
            }
        }
    }
    if headers.is_empty() {
        if acks == 0 {
            return None;
        }
        return Some(respond(ec::INVALID_REQUEST, -1));
    }
    // `max.message.bytes`, per topic. Refusing here means an oversized
    // record is rejected with an error the producer can act on, rather
    // than being written and then breaking every consumer that has a
    // smaller fetch budget than the record.
    if let Some(limit) = broker.max_message_bytes(&req.topic) {
        if let Some(oversized) = raw_batches.iter().find(|raw| raw.len() > limit) {
            warn!(
                topic = %req.topic,
                size = oversized.len(),
                limit,
                "rejecting a batch larger than max.message.bytes"
            );
            if acks == 0 {
                return None;
            }
            return Some(respond(ec::INVALID_REQUEST, -1));
        }
    }
    // `compression.type`, per topic — enforced by refusing a batch in the
    // wrong codec rather than by converting it.
    //
    // Converting would mean decompressing and recompressing every batch on
    // the way in, which is exactly the cost that byte-identical storage and
    // the zero-copy fetch path exist to avoid. Refusing gives the operator
    // the same guarantee — every batch on this topic is in the named codec
    // — and tells the producer which one to use, which is more useful than
    // silently rewriting its data.
    if let Some(required) = broker.required_compression(&req.topic) {
        if let Some(wrong) = headers.iter().find(|header| header.compression != required) {
            warn!(
                topic = %req.topic,
                required = required.name(),
                actual = wrong.compression.name(),
                "rejecting a batch that is not in the topic's compression.type"
            );
            if acks == 0 {
                return None;
            }
            return Some(respond(ec::INVALID_REQUEST, -1));
        }
    }
    let idempotent = headers[0].producer.is_some();
    if headers
        .iter()
        .any(|header| header.producer.is_some() != idempotent)
        || (idempotent && acks == 0)
    {
        return Some(respond(ec::INVALID_REQUEST, -1));
    }
    let mut batches = Vec::new();
    if idempotent {
        for raw in &raw_batches {
            let mut buf = raw.clone();
            match RecordBatch::decode(&mut buf) {
                Ok(batch) => batches.push(batch),
                Err(e) => {
                    warn!(error = %e, "produce request carried a corrupt batch");
                    return Some(respond(ec::INVALID_REQUEST, -1));
                }
            }
        }
    }
    let replicated_commit =
        broker.config().replication_enabled && broker.metadata_cache().is_some();
    // Serialize the assignment snapshot, append, and immediate HWM advance
    // with ISR mutations. In particular, an expansion cannot publish a new
    // ISR based on a pre-append follower offset.
    let mutation_guard = if replicated_commit {
        Some(
            broker
                .partition_mutation_guard(&req.topic, req.partition)
                .await,
        )
    } else {
        None
    };
    let cluster_assignment = if replicated_commit {
        let image = broker
            .metadata_cache()
            .expect("replicated commit requires metadata")
            .snapshot();
        if let Err(error) = broker.validate_local_broker_epoch(&image) {
            if acks == 0 {
                return None;
            }
            return Some(respond(code_of(&error), -1));
        }
        broker
            .replication_tracker()
            .reconcile_metadata(&image, broker.config().broker_id);
        let Some(topic) = image.topics.get(&req.topic) else {
            if acks == 0 {
                return None;
            }
            return Some(respond(ec::UNKNOWN_TOPIC_OR_PARTITION, -1));
        };
        let Some(assignment) = topic.partitions.get(&req.partition) else {
            if acks == 0 {
                return None;
            }
            return Some(respond(ec::UNKNOWN_TOPIC_OR_PARTITION, -1));
        };
        if acks == -1 {
            let default_min_isr = usize::try_from(topic.replication_factor)
                .unwrap_or(1)
                .clamp(1, 2);
            let min_isr = match topic.configs.get("min.insync.replicas") {
                Some(value) => match value.parse::<usize>() {
                    Ok(value) if value > 0 => value,
                    _ => return Some(respond(ec::INVALID_REQUEST, -1)),
                },
                None => default_min_isr,
            };
            if assignment.isr.len() < min_isr {
                return Some(respond(ec::NOT_ENOUGH_REPLICAS, -1));
            }
        }
        Some(assignment.clone())
    } else {
        None
    };
    let leader_epoch = cluster_assignment
        .as_ref()
        .map_or(0, |assignment| assignment.leader_epoch);

    if replicated_commit && idempotent {
        for batch in &mut batches {
            batch.leader_epoch = leader_epoch;
        }
    }

    let handle = match broker.partition_auto_create(&req.topic, req.partition) {
        Ok(h) => h,
        Err(e) => {
            if acks == 0 {
                return None;
            }
            return Some(respond(code_of(&e), -1));
        }
    };

    if replicated_commit {
        if let Err(error) = handle.record_leader_epoch(leader_epoch).await {
            if acks == 0 {
                return None;
            }
            return Some(respond(code_of(&BrokerError::Storage(error)), -1));
        }
    }

    // Subscribe before appending so an immediately completed replication
    // quorum cannot race the acks=all waiter.
    let mut watermark = (acks == -1 && replicated_commit).then(|| {
        let mut watermark = handle.watermark_watch();
        watermark.borrow_and_update();
        watermark
    });

    // Append sequentially; the response carries the first batch's base
    // offset (batches are contiguous by the single-writer principle).
    let mut first_base = -1;
    let mut required_high_watermark = -1;
    if idempotent {
        for (i, batch) in batches.into_iter().enumerate() {
            match handle.append_idempotent(batch).await {
                Ok(outcome) => {
                    if i == 0 {
                        first_base = outcome.base_offset;
                    }
                    required_high_watermark = outcome.next_offset;
                }
                Err(error) => return Some(respond(producer_code_of(&error), -1)),
            }
        }
    } else {
        for (i, raw) in raw_batches.iter().enumerate() {
            match handle
                .append_producer_batch(raw.clone(), leader_epoch)
                .await
            {
                Ok((base, next)) => {
                    if i == 0 {
                        first_base = base;
                    }
                    required_high_watermark = next;
                }
                Err(e) => {
                    let err = BrokerError::Storage(e);
                    if acks == 0 {
                        return None;
                    }
                    return Some(respond(code_of(&err), -1));
                }
            }
        }
    }
    if let Some(assignment) = cluster_assignment.as_ref() {
        let latest = broker
            .metadata_cache()
            .expect("replicated commit requires metadata")
            .snapshot();
        if let Err(error) = broker.validate_local_broker_epoch(&latest) {
            if acks == 0 {
                return None;
            }
            return Some(respond(code_of(&error), -1));
        }
        let still_current = latest
            .topics
            .get(&req.topic)
            .and_then(|topic| topic.partitions.get(&req.partition))
            .is_some_and(|current| current == assignment);
        if !still_current {
            if acks == 0 {
                return None;
            }
            return Some(respond(ec::NOT_LEADER_OR_FOLLOWER, -1));
        }
        if let Err(error) = broker
            .replication_tracker()
            .advance_leader_high_watermark(&req.topic, assignment, &handle)
            .await
        {
            if acks == 0 {
                return None;
            }
            return Some(respond(code_of(&BrokerError::Storage(error)), -1));
        }
    }
    drop(mutation_guard);
    // The records are appended and (for acks=all) committed by this point,
    // so throttling here delays the acknowledgement without ever putting a
    // write at risk.
    let appended_bytes: u64 = raw_batches.iter().map(|batch| batch.len() as u64).sum();
    let records_appended: u64 = headers
        .iter()
        .map(|header| header.last_offset_delta as u64 + 1)
        .sum();
    let metrics = broker.metrics();
    metrics.count(names::PRODUCE_REQUESTS, 1);
    metrics.count(names::PRODUCE_RECORDS, records_appended);
    metrics.count(names::PRODUCE_BYTES, appended_bytes);
    let throttle = broker
        .throttle(
            client.principal,
            client.client_id,
            QuotaKind::Produce,
            appended_bytes,
        )
        .await;
    if !throttle.is_zero() {
        metrics.count(names::THROTTLED_REQUESTS, 1);
        metrics.count(names::THROTTLE_MS, throttle.as_millis() as u64);
    }
    // acks: 1 = leader append; -1 (all) = full ISR — in M1 the ISR is just
    // this broker, so both are acknowledged by the local append above
    // (replication refines acks=all in M3). acks=0: no response at all.
    if acks == 0 {
        None
    } else if let Some(watermark) = watermark.as_mut() {
        if wait_for_high_watermark(
            watermark,
            required_high_watermark,
            Duration::from_millis(req.timeout_ms.max(0) as u64),
        )
        .await
        {
            Some(respond(ec::NONE, first_base))
        } else {
            Some(respond(ec::NOT_ENOUGH_REPLICAS, -1))
        }
    } else {
        Some(respond(ec::NONE, first_base))
    }
}

pub(crate) async fn wait_for_high_watermark(
    watermark: &mut tokio::sync::watch::Receiver<i64>,
    required: i64,
    timeout: Duration,
) -> bool {
    if *watermark.borrow_and_update() >= required {
        return true;
    }
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match tokio::time::timeout_at(deadline, watermark.changed()).await {
            Ok(Ok(())) if *watermark.borrow_and_update() >= required => return true,
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => return false,
        }
    }
}

// ---------- Fetch (api_key 1) ----------

/// Which broker this consumer should read the partition from.
///
/// `-1` means "keep reading here", which is the answer for a consumer with
/// no rack, a cluster with no rack information, and a consumer that is
/// already in the leader's rack — the last of which matters, because
/// redirecting it to a follower in the same rack would trade a fresher read
/// for nothing.
///
/// Only in-sync replicas are ever named. A follower outside the ISR is by
/// definition behind by an unbounded amount, and pointing a consumer at one
/// converts a replication problem into a consumer that has silently stopped
/// making progress.
pub(crate) fn preferred_read_replica(
    broker: &Broker,
    topic: &str,
    partition: i32,
    rack: &str,
) -> i32 {
    if rack.is_empty() {
        return -1;
    }
    let Some(cache) = broker.metadata_cache() else {
        return -1;
    };
    preferred_read_replica_in(&cache.snapshot(), topic, partition, rack)
}

/// The rack decision itself, over a metadata image.
pub(crate) fn preferred_read_replica_in(
    image: &brahmaputra_metadata::ClusterMetadata,
    topic: &str,
    partition: i32,
    rack: &str,
) -> i32 {
    if rack.is_empty() {
        return -1;
    }
    let Some(assignment) = image
        .topics
        .get(topic)
        .and_then(|topic| topic.partitions.get(&partition))
    else {
        return -1;
    };
    let rack_of = |broker_id: &i32| {
        image
            .brokers
            .get(broker_id)
            .and_then(|member| member.rack.clone())
    };
    if rack_of(&assignment.leader).as_deref() == Some(rack) {
        return -1;
    }
    assignment
        .isr
        .iter()
        .filter(|broker_id| **broker_id != assignment.leader)
        .find(|broker_id| {
            rack_of(broker_id).as_deref() == Some(rack)
                && image
                    .brokers
                    .get(broker_id)
                    .is_some_and(|member| member.alive)
        })
        .copied()
        .unwrap_or(-1)
}

/// Resolve a partition for a consumer fetch.
///
/// A consumer that named a rack may be here because the leader sent it, so
/// a follower must answer rather than refusing. It answers only up to its
/// *own* high watermark, which the log already enforces — a follower cannot
/// serve what it has not replicated — so the worst case is a consumer that
/// is briefly behind, never one that sees a record the cluster has not
/// committed.
pub(crate) fn consumer_partition(
    broker: &Broker,
    topic: &str,
    partition: i32,
    rack: &str,
) -> Result<crate::actor::PartitionHandle, BrokerError> {
    if rack.is_empty() {
        return broker.partition(topic, partition);
    }
    broker.replica_partition_for_read(topic, partition)
}

async fn fetch(broker: &Broker, body: Bytes, client: ClientIdentity<'_>) -> Vec<Bytes> {
    let req = match FetchRequest::decode(&body) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "undecodable fetch request");
            return vec![encode_error_for(ApiKey::Fetch, ec::INVALID_REQUEST)];
        }
    };
    let isolation = IsolationLevel::from_wire(req.isolation_level);
    // Which replica this consumer should be reading from. Computed before
    // the read so that even a fetch that returns nothing carries the
    // redirect: a caught-up consumer must not have to wait for a record to
    // arrive before it stops crossing an availability zone.
    let preferred = preferred_read_replica(broker, &req.topic, req.partition, &req.rack);
    // A committed read is already bounded at the last stable offset, and
    // the actor reports that offset in place of the high watermark — so the
    // two fields agree about the ceiling that actually applied.
    let respond = |error_code, hw: i64, batches: &[Bytes]| {
        let resp = FetchResponse {
            topic: req.topic.clone(),
            partition: req.partition,
            error_code,
            high_watermark: hw,
            last_stable_offset: hw,
            batches_length: 0, // filled in by encode_fetch_response
            preferred_read_replica: preferred,
        };
        codec::encode_fetch_response_chunks(&resp, batches).unwrap_or_default()
    };

    let handle = match consumer_partition(broker, &req.topic, req.partition, &req.rack) {
        Ok(h) => h,
        Err(e) => return respond(code_of(&e), -1, &[]),
    };
    let max_bytes = req.max_bytes.max(1) as usize;
    let min_bytes = req.min_bytes.max(1) as usize;

    // Subscribe before the first read. If an append lands between the read
    // and `changed()`, the receiver retains that notification and the long
    // poll re-reads immediately instead of sleeping until its deadline.
    let mut watch = handle.watermark_watch();
    watch.borrow_and_update();
    let outcome = match handle.read_at(req.fetch_offset, max_bytes, isolation).await {
        Ok(o) => o,
        Err(e) => return respond(code_of(&BrokerError::Storage(e)), -1, &[]),
    };

    // Long poll (Blueprint 02 §5): if fewer than `min_bytes` are ready,
    // wait for the watermark to move (i.e. a new append) up to max_wait_ms.
    let outcome = match long_poll_until_min_bytes(
        &handle,
        &mut watch,
        req.fetch_offset,
        max_bytes,
        min_bytes,
        req.max_wait_ms,
        isolation,
        outcome,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(e) => return respond(code_of(&BrokerError::Storage(e)), -1, &[]),
    };

    // Charge the bytes actually served, then hold the response for as long
    // as the client owes. The read already happened, so a quota costs this
    // client latency and nothing else.
    let served: u64 = outcome.batches.iter().map(|batch| batch.len() as u64).sum();
    let metrics = broker.metrics();
    metrics.count(names::FETCH_REQUESTS, 1);
    metrics.count(names::FETCH_BYTES, served);
    let throttle = broker
        .throttle(client.principal, client.client_id, QuotaKind::Fetch, served)
        .await;
    if !throttle.is_zero() {
        metrics.count(names::THROTTLED_REQUESTS, 1);
        metrics.count(names::THROTTLE_MS, throttle.as_millis() as u64);
    }
    respond(ec::NONE, outcome.high_watermark, &outcome.batches)
}

#[allow(clippy::too_many_arguments)]
/// Eight parameters because a long poll is genuinely a function of all of
/// them: what to read, where to wait, how much is enough, how long to wait,
/// what the reader may see, and what was already read. Bundling them into a
/// struct would move the list rather than shorten it.
async fn long_poll_until_min_bytes(
    handle: &PartitionHandle,
    watch: &mut tokio::sync::watch::Receiver<i64>,
    fetch_offset: i64,
    max_bytes: usize,
    min_bytes: usize,
    max_wait_ms: i32,
    isolation: IsolationLevel,
    mut outcome: ReadOutcome,
) -> Result<ReadOutcome, StorageError> {
    let mut ready: usize = outcome.batches.iter().map(|b| b.len()).sum();
    if ready < min_bytes && max_wait_ms > 0 {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(max_wait_ms as u64);
        while let Ok(Ok(())) = tokio::time::timeout_at(deadline, watch.changed()).await {
            outcome = handle.read_at(fetch_offset, max_bytes, isolation).await?;
            ready = outcome.batches.iter().map(|b| b.len()).sum();
            if ready >= min_bytes {
                break;
            }
        }
    }
    Ok(outcome)
}

// ---------- ListOffsets (api_key 2) ----------

async fn list_offsets(broker: &Broker, body: Bytes) -> Bytes {
    let req = match ListOffsetsRequest::decode(&body) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "undecodable list-offsets request");
            return encode_error_for(ApiKey::ListOffsets, ec::INVALID_REQUEST);
        }
    };
    let respond = |error_code, offset, timestamp| {
        let resp = ListOffsetsResponse {
            topic: req.topic.clone(),
            partition: req.partition,
            error_code,
            offset,
            timestamp,
        };
        codec_bytes(resp.encode())
    };

    let handle = match broker.partition(&req.topic, req.partition) {
        Ok(h) => h,
        Err(e) => return respond(code_of(&e), -1, -1),
    };
    let (start, end, high_watermark) = match handle.offsets().await {
        Ok(o) => o,
        Err(e) => return respond(code_of(&BrokerError::Storage(e)), -1, -1),
    };

    match req.timestamp {
        TIMESTAMP_EARLIEST => respond(ec::NONE, start, -1),
        // Clients see the high watermark, never the leader's uncommitted
        // tail: "latest" must be an offset a consumer can actually reach
        // (Blueprint 04 invariant I2), and it is what consumer lag is
        // measured against. Replication uses ReplicaFetch, not this API.
        TIMESTAMP_LATEST => respond(ec::NONE, high_watermark, -1),
        target => {
            // The time index says which segment can hold the answer and
            // roughly where inside it, so the scan below starts near the
            // target instead of at the log start. It bounds *where to
            // look*; the scan still decides the answer, so the offset
            // returned is the same one a full walk would have found.
            let scan_start = match handle.scan_start_for_timestamp(target).await {
                Ok(Some(offset)) => offset.max(start),
                // No record in the log reaches the target timestamp.
                Ok(None) => return respond(ec::NONE, end, -1),
                Err(e) => return respond(code_of(&BrokerError::Storage(e)), -1, -1),
            };
            let mut pos = scan_start;
            while pos < end {
                match handle.read(pos, 1024 * 1024).await {
                    Ok(outcome) => {
                        if outcome.batches.is_empty() {
                            break;
                        }
                        let mut advanced = false;
                        for batch in &outcome.batches {
                            let Ok(header) = validate_batch_header(batch) else {
                                break;
                            };
                            if header.max_timestamp >= target {
                                return respond(ec::NONE, header.base_offset, header.max_timestamp);
                            }
                            let next = header.base_offset + header.last_offset_delta as i64 + 1;
                            advanced = true;
                            pos = pos.max(next);
                        }
                        if !advanced {
                            break;
                        }
                    }
                    Err(e) => return respond(code_of(&BrokerError::Storage(e)), -1, -1),
                }
            }
            // No batch reached the target timestamp: follow Kafka and
            // return the log end.
            respond(ec::NONE, end, -1)
        }
    }
}

// ---------- Transactions (api_keys 23-27) ----------

/// Decode a request, or answer with `INVALID_REQUEST`.
macro_rules! decode_or_reject {
    ($type:ty, $body:expr, $key:expr) => {
        match <$type>::decode(&$body) {
            Ok(request) => request,
            Err(error) => {
                warn!(%error, api = api_name($key), "undecodable transaction request");
                return encode_error_for($key, ec::INVALID_REQUEST);
            }
        }
    };
}

async fn add_partitions_to_txn(broker: &Broker, body: Bytes) -> Bytes {
    let request = decode_or_reject!(AddPartitionsToTxnRequest, body, ApiKey::AddPartitionsToTxn);
    let partitions = request
        .partitions
        .into_iter()
        .map(|partition| (partition.topic, partition.partition))
        .collect();
    let error_code = crate::transaction::add_partitions(
        broker,
        &request.transactional_id,
        request.producer_id,
        request.producer_epoch as i16,
        partitions,
    )
    .await
    .err()
    .unwrap_or(ec::NONE);
    codec_bytes(AddPartitionsToTxnResponse { error_code }.encode())
}

async fn add_offsets_to_txn(broker: &Broker, body: Bytes) -> Bytes {
    let request = decode_or_reject!(AddOffsetsToTxnRequest, body, ApiKey::AddOffsetsToTxn);
    let error_code = crate::transaction::add_offsets(
        broker,
        &request.transactional_id,
        request.producer_id,
        request.producer_epoch as i16,
        &request.group_id,
    )
    .await
    .err()
    .unwrap_or(ec::NONE);
    codec_bytes(AddOffsetsToTxnResponse { error_code }.encode())
}

async fn end_txn(broker: &Broker, body: Bytes) -> Bytes {
    let request = decode_or_reject!(EndTxnRequest, body, ApiKey::EndTxn);
    let error_code = crate::transaction::end_transaction(
        broker,
        &request.transactional_id,
        request.producer_id,
        request.producer_epoch as i16,
        request.committed,
    )
    .await
    .err()
    .unwrap_or(ec::NONE);
    codec_bytes(EndTxnResponse { error_code }.encode())
}

/// Commit consumed offsets as part of a transaction.
///
/// The offsets go to the *group* coordinator, because that is where offsets
/// live — but they are written as transactional records, so they become
/// visible to a `read_committed` reader of `__consumer_offsets` only when
/// the transaction's marker reaches that partition. That is what makes a
/// read-process-write pipeline atomic: the offsets advance if and only if
/// the output records do.
async fn txn_offset_commit(broker: &Broker, body: Bytes) -> Bytes {
    let request = decode_or_reject!(TxnOffsetCommitRequest, body, ApiKey::TxnOffsetCommit);
    let error_code = crate::group::commit_transactional_offsets(
        broker,
        &request.group_id,
        request.producer_id,
        request.producer_epoch as i16,
        request.offsets,
    )
    .await
    .err()
    .unwrap_or(ec::NONE);
    codec_bytes(TxnOffsetCommitResponse { error_code }.encode())
}

/// Cluster-internal: append markers to partitions this broker leads.
async fn write_txn_markers(broker: &Broker, body: Bytes) -> Bytes {
    let request = decode_or_reject!(WriteTxnMarkersRequest, body, ApiKey::WriteTxnMarkers);
    let mut results = Vec::new();
    for marker in request.markers {
        for partition in marker.partitions {
            let outcome = crate::transaction::append_marker(
                broker,
                &partition.topic,
                partition.partition,
                marker.producer_id,
                marker.producer_epoch as i16,
                marker.committed,
            )
            .await;
            results.push(TxnMarkerResult {
                topic: partition.topic,
                partition: partition.partition,
                error_code: outcome.err().map_or(ec::NONE, |error| code_of(&error)),
            });
        }
    }
    codec_bytes(WriteTxnMarkersResponse { results }.encode())
}

// ---------- Metadata (api_key 3) ----------

fn metadata(broker: &Broker, body: Bytes) -> Bytes {
    if let Err(error) = broker.validate_local_broker_lease() {
        return encode_error_for(ApiKey::Metadata, code_of(&error));
    }
    let req = match MetadataRequest::decode(&body) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "undecodable metadata request");
            return encode_error_for(ApiKey::Metadata, ec::INVALID_REQUEST);
        }
    };
    if let Some(cache) = broker.metadata_cache() {
        let image = cache.snapshot();
        let brokers = image
            .brokers
            .values()
            .map(|broker| BrokerInfo {
                broker_id: broker.broker_id,
                host: broker.host.clone(),
                port: i32::from(broker.data_port),
                // Empty when the broker was started without --rack, which
                // is what tells a client there is no locality to exploit.
                rack: broker.rack.clone().unwrap_or_default(),
            })
            .collect();
        let names: Vec<&str> = if req.topics.is_empty() {
            image.topics.keys().map(String::as_str).collect()
        } else {
            req.topics.iter().map(String::as_str).collect()
        };
        let topics = names
            .into_iter()
            .map(|name| match image.topics.get(name) {
                Some(topic) => TopicInfo {
                    name: topic.name.clone(),
                    error_code: ec::NONE,
                    partitions: topic
                        .partitions
                        .values()
                        .map(|partition| PartitionInfo {
                            partition: partition.partition,
                            leader: partition.leader,
                            replicas: partition.replicas.clone(),
                            isr: partition.isr.clone(),
                            leader_epoch: partition.leader_epoch,
                        })
                        .collect(),
                },
                None => TopicInfo {
                    name: name.to_owned(),
                    error_code: ec::UNKNOWN_TOPIC_OR_PARTITION,
                    partitions: Vec::new(),
                },
            })
            .collect();
        return codec_bytes(
            MetadataResponse {
                error_code: ec::NONE,
                brokers,
                controller_id: image.controller_id.unwrap_or(-1),
                topics,
            }
            .encode(),
        );
    }

    // Empty = all topics; named topics are auto-created on first sight (M1).
    let names: Vec<String> = if req.topics.is_empty() {
        broker
            .state()
            .topics()
            .into_iter()
            .map(|(n, _)| n)
            .collect()
    } else {
        req.topics
    };

    let addr = broker.local_addr();
    let broker_id = broker.config().broker_id;
    let mut topics = Vec::with_capacity(names.len());
    for name in names {
        match broker.state().ensure_topic(&name) {
            Ok(partitions) => {
                let partitions = (0..partitions)
                    .map(|p| PartitionInfo {
                        partition: p,
                        leader: broker_id,
                        replicas: vec![broker_id],
                        isr: vec![broker_id],
                        leader_epoch: 0,
                    })
                    .collect();
                topics.push(TopicInfo {
                    name,
                    error_code: ec::NONE,
                    partitions,
                });
            }
            Err(_) => topics.push(TopicInfo {
                name,
                error_code: ec::UNKNOWN_TOPIC_OR_PARTITION,
                partitions: vec![],
            }),
        }
    }

    let resp = MetadataResponse {
        error_code: ec::NONE,
        brokers: vec![BrokerInfo {
            broker_id,
            host: addr.ip().to_string(),
            port: addr.port() as i32,
            // A standalone broker is the only replica there is, so there is
            // nothing a rack could route to.
            rack: String::new(),
        }],
        controller_id: broker_id,
        topics,
    };
    codec_bytes(resp.encode())
}

pub(crate) fn codec_bytes(result: std::io::Result<Vec<u8>>) -> Bytes {
    Bytes::from(result.unwrap_or_default())
}

// ---------- ApiVersions (api_key 14) ----------

/// Report every API this broker speaks and the version range it accepts.
///
/// A client calls this first, at whatever version it happens to speak, and
/// uses the answer to decide what to send next. That is what allows a
/// cluster to run mixed broker versions during a rolling upgrade: a newer
/// client discovers an older broker's ceiling instead of failing against
/// it.
async fn api_versions(broker: &Broker, body: Bytes) -> Bytes {
    // The request body is advisory (client name/version, for logs); an
    // undecodable one must not stop a client from learning our versions.
    if let Ok(request) = ApiVersionsRequest::decode(&body) {
        if !request.client_software_name.is_empty() {
            tracing::debug!(
                client = %request.client_software_name,
                version = %request.client_software_version,
                "api versions requested"
            );
        }
    }
    let supported: Vec<ApiVersionRange> = SUPPORTED_APIS
        .iter()
        .map(|api_key| ApiVersionRange {
            api_key: *api_key as i32,
            min_version: API_VERSION as i32,
            max_version: API_VERSION as i32,
        })
        .collect();
    let throttle = broker
        .throttle(None, None, crate::quota::QuotaKind::Fetch, 0)
        .await;
    codec_bytes(
        ApiVersionsResponse {
            error_code: ec::NONE,
            api_versions: supported,
            broker_version: env!("CARGO_PKG_VERSION").to_owned(),
            throttle_time_ms: throttle.as_millis() as i32,
        }
        .encode(),
    )
}

/// Every API key the broker dispatches, in wire order.
const SUPPORTED_APIS: &[ApiKey] = &[
    ApiKey::Produce,
    ApiKey::Fetch,
    ApiKey::ListOffsets,
    ApiKey::Metadata,
    ApiKey::ReplicaFetch,
    ApiKey::OffsetsForLeaderEpoch,
    ApiKey::InitProducerId,
    ApiKey::JoinGroup,
    ApiKey::SyncGroup,
    ApiKey::Heartbeat,
    ApiKey::LeaveGroup,
    ApiKey::OffsetCommit,
    ApiKey::OffsetFetch,
    ApiKey::ListGroups,
    ApiKey::DescribeGroup,
    ApiKey::ApiVersions,
    // The multi-partition forms and Authenticate were dispatched but never
    // advertised, so a client that trusted ApiVersions to describe the
    // broker could not discover the very APIs it is supposed to prefer.
    ApiKey::ProduceMulti,
    ApiKey::FetchMulti,
    ApiKey::Authenticate,
    ApiKey::DescribeCluster,
    ApiKey::DescribeConfigs,
    ApiKey::DescribeLogDirs,
    ApiKey::DeleteRecords,
    ApiKey::AddPartitionsToTxn,
    ApiKey::AddOffsetsToTxn,
    ApiKey::EndTxn,
    ApiKey::TxnOffsetCommit,
    ApiKey::WriteTxnMarkers,
    ApiKey::AlterConfigs,
    ApiKey::DescribeProducers,
    ApiKey::ListTransactions,
    ApiKey::DescribeTransactions,
    ApiKey::AlterReplicaLogDirs,
];

// ---------- Consumer groups (api_keys 7-11, Blueprint 05) ----------

/// Resolve the coordinator shard for `group_id`: pick the
/// `__consumer_offsets` partition, validate this broker leads it (cluster
/// mode) or auto-create it (standalone), then return the loaded shard.
pub(crate) async fn coordinator_shard_for(
    broker: &Broker,
    group_id: &str,
) -> Result<Arc<CoordinatorShard>, BrokerError> {
    coordinator_shard(broker, group_id).await
}

async fn coordinator_shard(
    broker: &Broker,
    group_id: &str,
) -> Result<Arc<CoordinatorShard>, BrokerError> {
    let (partition, leader_epoch, handle) = if let Some(cache) = broker.metadata_cache() {
        let image = cache.snapshot();
        broker.validate_local_broker_epoch(&image)?;
        let not_coordinator = || BrokerError::NotCoordinator {
            group_id: group_id.to_owned(),
        };
        let Some(topic) = image.topics.get(OFFSETS_TOPIC) else {
            return Err(not_coordinator());
        };
        let partition = coordinator_partition(group_id, topic.partitions.len() as i32);
        let Some(assignment) = topic.partitions.get(&partition) else {
            return Err(not_coordinator());
        };
        if assignment.leader != broker.config().broker_id {
            return Err(not_coordinator());
        }
        let handle = broker
            .partition(OFFSETS_TOPIC, partition)
            .map_err(|error| match error {
                BrokerError::NotLeaderOrFollower { .. } => not_coordinator(),
                other => other,
            })?;
        (partition, assignment.leader_epoch, handle)
    } else {
        let partition_count = broker.state().ensure_topic(OFFSETS_TOPIC)?;
        let partition = coordinator_partition(group_id, partition_count);
        let handle = broker.partition_auto_create(OFFSETS_TOPIC, partition)?;
        (partition, 0, handle)
    };
    let shard = broker.groups().shard(partition, handle, leader_epoch);
    broker.groups().ensure_loaded(&shard).await?;
    Ok(shard)
}

async fn join_group(broker: &Broker, body: Bytes) -> Bytes {
    let request = match JoinGroupRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable JoinGroup request");
            return encode_error_for(ApiKey::JoinGroup, ec::INVALID_REQUEST);
        }
    };
    let result = match coordinator_shard(broker, &request.group_id).await {
        Ok(shard) => broker.groups().join(broker, &shard, request).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(response) => codec_bytes(response.encode()),
        Err(error) => encode_error_for(ApiKey::JoinGroup, code_of(&error)),
    }
}

async fn sync_group(broker: &Broker, body: Bytes) -> Bytes {
    let request = match SyncGroupRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable SyncGroup request");
            return encode_error_for(ApiKey::SyncGroup, ec::INVALID_REQUEST);
        }
    };
    let result = match coordinator_shard(broker, &request.group_id).await {
        Ok(shard) => broker.groups().sync(broker, &shard, request).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(response) => codec_bytes(response.encode()),
        Err(error) => encode_error_for(ApiKey::SyncGroup, code_of(&error)),
    }
}

async fn heartbeat(broker: &Broker, body: Bytes) -> Bytes {
    let request = match HeartbeatRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable Heartbeat request");
            return encode_error_for(ApiKey::Heartbeat, ec::INVALID_REQUEST);
        }
    };
    let result = match coordinator_shard(broker, &request.group_id).await {
        Ok(shard) => broker.groups().heartbeat(&shard, request),
        Err(error) => Err(error),
    };
    let error_code = match &result {
        Ok(()) => ec::NONE,
        Err(error) => code_of(error),
    };
    codec_bytes(HeartbeatResponse { error_code }.encode())
}

async fn leave_group(broker: &Broker, body: Bytes) -> Bytes {
    let request = match LeaveGroupRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable LeaveGroup request");
            return encode_error_for(ApiKey::LeaveGroup, ec::INVALID_REQUEST);
        }
    };
    let result = match coordinator_shard(broker, &request.group_id).await {
        Ok(shard) => broker.groups().leave(broker, &shard, request).await,
        Err(error) => Err(error),
    };
    let error_code = match &result {
        Ok(()) => ec::NONE,
        Err(error) => code_of(error),
    };
    codec_bytes(LeaveGroupResponse { error_code }.encode())
}
async fn offset_commit(broker: &Broker, body: Bytes) -> Bytes {
    let request = match OffsetCommitRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable OffsetCommit request");
            return encode_error_for(ApiKey::OffsetCommit, ec::INVALID_REQUEST);
        }
    };
    let result = match coordinator_shard(broker, &request.group_id).await {
        Ok(shard) => broker.groups().commit(broker, &shard, request).await,
        Err(error) => Err(error),
    };
    let error_code = match &result {
        Ok(()) => ec::NONE,
        Err(error) => code_of(error),
    };
    codec_bytes(OffsetCommitResponse { error_code }.encode())
}

async fn offset_fetch(broker: &Broker, body: Bytes) -> Bytes {
    let request = match OffsetFetchRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable OffsetFetch request");
            return encode_error_for(ApiKey::OffsetFetch, ec::INVALID_REQUEST);
        }
    };
    match coordinator_shard(broker, &request.group_id).await {
        Ok(shard) => codec_bytes(broker.groups().fetch_offsets(&shard, request).encode()),
        Err(error) => encode_error_for(ApiKey::OffsetFetch, code_of(&error)),
    }
}

/// ListGroups: this broker's own coordinator partitions only, so a
/// cluster-wide listing is the union over brokers (Blueprint 05 §6).
async fn list_groups(broker: &Broker, body: Bytes) -> Bytes {
    let request = match ListGroupsRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable ListGroups request");
            return encode_error_for(ApiKey::ListGroups, ec::INVALID_REQUEST);
        }
    };
    match broker.groups().list(broker, &request.states).await {
        Ok(groups) => codec_bytes(
            ListGroupsResponse {
                error_code: ec::NONE,
                groups,
            }
            .encode(),
        ),
        Err(error) => encode_error_for(ApiKey::ListGroups, code_of(&error)),
    }
}

/// DescribeGroup: membership and committed offsets from the group's own
/// coordinator; lag is derived by the caller against each partition's log
/// end offset.
async fn describe_group(broker: &Broker, body: Bytes) -> Bytes {
    let request = match DescribeGroupRequest::decode(&body) {
        Ok(request) => request,
        Err(error) => {
            warn!(%error, "undecodable DescribeGroup request");
            return encode_error_for(ApiKey::DescribeGroup, ec::INVALID_REQUEST);
        }
    };
    let result = match coordinator_shard(broker, &request.group_id).await {
        Ok(shard) => broker.groups().describe(&shard, &request.group_id),
        Err(error) => Err(error),
    };
    match result {
        Ok(response) => codec_bytes(response.encode()),
        Err(error) => encode_error_for(ApiKey::DescribeGroup, code_of(&error)),
    }
}

/// Stable metric label for an API key.
fn api_name(api_key: ApiKey) -> &'static str {
    match api_key {
        ApiKey::Produce => "produce",
        ApiKey::Fetch => "fetch",
        ApiKey::ListOffsets => "list_offsets",
        ApiKey::Metadata => "metadata",
        ApiKey::ReplicaFetch => "replica_fetch",
        ApiKey::OffsetsForLeaderEpoch => "offsets_for_leader_epoch",
        ApiKey::InitProducerId => "init_producer_id",
        ApiKey::JoinGroup => "join_group",
        ApiKey::SyncGroup => "sync_group",
        ApiKey::Heartbeat => "heartbeat",
        ApiKey::LeaveGroup => "leave_group",
        ApiKey::OffsetCommit => "offset_commit",
        ApiKey::OffsetFetch => "offset_fetch",
        ApiKey::ListGroups => "list_groups",
        ApiKey::DescribeGroup => "describe_group",
        ApiKey::ApiVersions => "api_versions",
        ApiKey::ProduceMulti => "produce_multi",
        ApiKey::FetchMulti => "fetch_multi",
        ApiKey::Authenticate => "authenticate",
        ApiKey::DescribeCluster => "describe_cluster",
        ApiKey::DescribeConfigs => "describe_configs",
        ApiKey::DescribeLogDirs => "describe_log_dirs",
        ApiKey::DeleteRecords => "delete_records",
        ApiKey::AddPartitionsToTxn => "add_partitions_to_txn",
        ApiKey::AddOffsetsToTxn => "add_offsets_to_txn",
        ApiKey::EndTxn => "end_txn",
        ApiKey::TxnOffsetCommit => "txn_offset_commit",
        ApiKey::WriteTxnMarkers => "write_txn_markers",
        ApiKey::AlterConfigs => "alter_configs",
        ApiKey::DescribeProducers => "describe_producers",
        ApiKey::ListTransactions => "list_transactions",
        ApiKey::DescribeTransactions => "describe_transactions",
        ApiKey::AlterReplicaLogDirs => "alter_replica_log_dirs",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor;
    use brahmaputra_protocol::Record;
    use brahmaputra_storage::{Log, LogConfig};

    mod rack_aware_reads {
        use super::*;
        use brahmaputra_metadata::{
            BrokerMetadata, ClusterMetadata, NodeRole, PartitionMetadata, TopicMetadata,
        };
        use std::collections::{BTreeMap, BTreeSet};

        /// Three brokers, one per rack, with a partition whose leader is on
        /// broker 1 and whose ISR is what each test sets.
        fn image(isr: Vec<i32>) -> ClusterMetadata {
            let mut image = ClusterMetadata::default();
            for (broker_id, rack) in [(1, "east"), (2, "west"), (3, "north")] {
                image.brokers.insert(
                    broker_id,
                    BrokerMetadata {
                        broker_id,
                        host: "127.0.0.1".into(),
                        data_port: 9092 + broker_id as u16,
                        control_port: 19092 + broker_id as u16,
                        internal_port: 0,
                        broker_epoch: 1,
                        roles: BTreeSet::from([NodeRole::Broker]),
                        rack: Some(rack.to_owned()),
                        alive: true,
                        last_heartbeat_ms: 0,
                    },
                );
            }
            image.topics.insert(
                "orders".into(),
                TopicMetadata {
                    name: "orders".into(),
                    replication_factor: 3,
                    partitions: BTreeMap::from([(
                        0,
                        PartitionMetadata {
                            partition: 0,
                            replicas: vec![1, 2, 3],
                            leader: 1,
                            isr,
                            leader_epoch: 1,
                            target_replicas: None,
                        },
                    )]),
                    configs: BTreeMap::new(),
                },
            );
            image
        }

        /// A consumer that names no rack is not being redirected anywhere.
        #[test]
        fn no_rack_means_read_from_the_leader() {
            assert_eq!(
                preferred_read_replica_in(&image(vec![1, 2, 3]), "orders", 0, ""),
                -1
            );
        }

        /// A consumer already in the leader's rack must stay there:
        /// redirecting it to a follower alongside would trade a fresher
        /// read for nothing at all.
        #[test]
        fn a_consumer_in_the_leaders_rack_is_not_redirected() {
            assert_eq!(
                preferred_read_replica_in(&image(vec![1, 2, 3]), "orders", 0, "east"),
                -1
            );
        }

        /// The point of the feature.
        #[test]
        fn a_consumer_is_sent_to_an_in_sync_replica_in_its_own_rack() {
            assert_eq!(
                preferred_read_replica_in(&image(vec![1, 2, 3]), "orders", 0, "west"),
                2
            );
        }

        /// A replica out of the ISR is behind by an unbounded amount.
        /// Sending a consumer there turns a replication problem into a
        /// consumer that has silently stopped making progress.
        #[test]
        fn a_replica_outside_the_isr_is_never_named() {
            assert_eq!(
                preferred_read_replica_in(&image(vec![1, 3]), "orders", 0, "west"),
                -1
            );
        }

        /// A rack nothing is in falls back to the leader rather than to
        /// whichever replica happened to be listed first.
        #[test]
        fn an_unknown_rack_falls_back_to_the_leader() {
            assert_eq!(
                preferred_read_replica_in(&image(vec![1, 2, 3]), "orders", 0, "south"),
                -1
            );
        }
    }

    #[tokio::test]
    async fn unread_watermark_notification_wakes_long_poll_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let (handle, task) = actor::spawn(log, 16);

        let mut watch = handle.watermark_watch();
        watch.borrow_and_update();
        let stale_outcome = handle.read(0, usize::MAX).await.unwrap();
        assert!(stale_outcome.batches.is_empty());

        handle
            .append(RecordBatch::new(
                0,
                0,
                1_000,
                vec![Record::new(b"arrived-during-read".as_slice())],
            ))
            .await
            .unwrap();

        let outcome = tokio::time::timeout(
            Duration::from_millis(100),
            long_poll_until_min_bytes(
                &handle,
                &mut watch,
                0,
                usize::MAX,
                1,
                2_000,
                IsolationLevel::ReadUncommitted,
                stale_outcome,
            ),
        )
        .await
        .expect("an unread watermark update should wake without waiting for the deadline")
        .unwrap();
        assert_eq!(outcome.batches.len(), 1);

        drop(handle);
        task.await.unwrap();
    }
}
