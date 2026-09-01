//! Data-plane authentication and authorization.
//!
//! These are the tests that decide whether the security control is real:
//! that an unauthenticated connection is refused, that a wrong password is
//! refused, that an authenticated principal is still bound by its ACLs, and
//! that a deny rule beats an allow. A security check nobody proves is
//! rejecting traffic is a security check that does not exist.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use brahmaputra_broker::{Broker, BrokerConfig};
use brahmaputra_client::{Connection, Credentials, SaslMechanism, Transport};
use brahmaputra_metadata::{
    AclOperation, AclPermission, AclRule, ClusterMetadata, MetadataCache, MetadataCommand,
    NodeRole, ResourceType, Role, UserRecord,
};
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{FetchRequest, ProduceRequest};
use brahmaputra_protocol::{codec, ApiKey, Record, RecordBatch};
use bytes::Bytes;
use tokio::sync::oneshot;

struct RunningBroker {
    addr: std::net::SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

/// A one-broker cluster with authentication required.
///
/// TLS rather than plaintext: the broker refuses to accept a password on a
/// plaintext listener, which is itself asserted below.
async fn start_secured_broker(data_dir: &Path, cache: MetadataCache) -> RunningBroker {
    // The broker must carry the epoch its registration committed, or every
    // request is fenced before authorization is even reached.
    let broker_epoch = cache
        .snapshot()
        .brokers
        .get(&1)
        .map(|broker| broker.broker_epoch);
    let broker = Arc::new(
        Broker::bind(BrokerConfig {
            broker_id: 1,
            broker_epoch,
            port: 0,
            data_dirs: vec![data_dir.to_owned()],
            default_partitions: 1,
            transport: Transport::TcpTls,
            require_auth: true,
            metadata_cache: Some(cache),
            ..BrokerConfig::default()
        })
        .await
        .expect("bind broker"),
    );
    let addr = broker.local_addr();
    let serving = Arc::clone(&broker);
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        serving
            .run(async {
                let _ = stopped.await;
            })
            .await
            .expect("broker run");
    });
    RunningBroker {
        addr,
        shutdown,
        task,
    }
}

async fn stop(running: RunningBroker) {
    let _ = running.shutdown.send(());
    let _ = running.task.await;
}

fn image_with_users() -> ClusterMetadata {
    let mut image = ClusterMetadata::new("auth-test");
    image
        .apply(MetadataCommand::RegisterBroker {
            broker_id: 1,
            host: "127.0.0.1".into(),
            data_port: 19_099,
            control_port: 29_099,
            internal_port: 0,
            expected_epoch: None,
            roles: vec![NodeRole::Broker, NodeRole::Controller],
            rack: None,
            now_ms: 1_000,
        })
        .expect("register broker");
    image
        .apply(MetadataCommand::SetController { broker_id: 1 })
        .expect("set controller");
    image
        .apply(MetadataCommand::CreateTopic {
            name: "orders".into(),
            partitions: 1,
            replication_factor: 1,
            configs: BTreeMap::new(),
        })
        .expect("create topic");

    for (username, role) in [("writer", Role::Operator), ("reader", Role::Viewer)] {
        image
            .apply(MetadataCommand::PutUser {
                // Both credentials, as every real creation path produces.
                user: UserRecord::new(username, "correct horse", role, false).expect("user"),
            })
            .expect("put user");
    }

    // writer may produce to orders; reader may only consume it.
    for (principal, operation) in [
        ("writer", AclOperation::Write),
        ("reader", AclOperation::Read),
    ] {
        image
            .apply(MetadataCommand::PutAcl {
                rule: AclRule {
                    principal: principal.into(),
                    resource_type: ResourceType::Topic,
                    resource_name: "orders".into(),
                    operation,
                    permission: AclPermission::Allow,
                },
            })
            .expect("put acl");
    }
    image
}

fn produce_body(topic: &str) -> Vec<u8> {
    let batch = RecordBatch::new(0, 0, 1, vec![Record::new(b"hello".to_vec())]);
    codec::encode_produce_request(
        &ProduceRequest {
            topic: topic.into(),
            partition: 0,
            acks: 1,
            timeout_ms: 5_000,
            batches_length: 0,
        },
        &[batch.encode()],
    )
    .expect("encode produce")
    .to_vec()
}

fn fetch_body(topic: &str) -> Vec<u8> {
    FetchRequest {
        topic: topic.into(),
        partition: 0,
        fetch_offset: 0,
        max_bytes: 1 << 20,
        max_wait_ms: 100,
        min_bytes: 1,
        isolation_level: 0,
        rack: String::new(),
    }
    .encode()
    .expect("encode fetch")
}

/// Decode just the error code, which every response carries first after
/// the topic/partition for produce and fetch alike.
fn produce_error(body: &Bytes) -> i32 {
    brahmaputra_protocol::gen::ProduceResponse::decode(body)
        .map(|response| response.error_code)
        .unwrap_or(i32::MIN)
}

async fn connect(addr: std::net::SocketAddr) -> Connection {
    Connection::connect_with(Transport::TcpTls, addr, Some("test".into()), 5)
        .await
        .expect("connect")
}

#[tokio::test]
async fn an_unauthenticated_connection_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = MetadataCache::new(image_with_users());
    let running = start_secured_broker(dir.path(), cache).await;

    let connection = connect(running.addr).await;
    let response = connection
        .request(ApiKey::Produce, &produce_body("orders"))
        .await
        .expect("request completes");
    assert_eq!(
        produce_error(&response),
        ec::SASL_AUTHENTICATION_FAILED,
        "an anonymous connection must not be able to produce"
    );

    stop(running).await;
}

#[tokio::test]
async fn a_wrong_password_is_refused_and_leaves_the_connection_anonymous() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = MetadataCache::new(image_with_users());
    let running = start_secured_broker(dir.path(), cache).await;

    let connection = connect(running.addr).await;
    let outcome = connection
        .authenticate(&Credentials {
            username: "writer".into(),
            password: "wrong".into(),
            mechanism: SaslMechanism::Plain,
        })
        .await;
    assert!(outcome.is_err(), "a wrong password must not authenticate");

    // And the connection is still anonymous, not half-authenticated.
    let response = connection
        .request(ApiKey::Produce, &produce_body("orders"))
        .await
        .expect("request completes");
    assert_eq!(produce_error(&response), ec::SASL_AUTHENTICATION_FAILED);

    stop(running).await;
}

#[tokio::test]
async fn an_unknown_user_fails_exactly_like_a_wrong_password() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = MetadataCache::new(image_with_users());
    let running = start_secured_broker(dir.path(), cache).await;

    let connection = connect(running.addr).await;
    let unknown = connection
        .authenticate(&Credentials {
            username: "nobody".into(),
            password: "correct horse".into(),
            mechanism: SaslMechanism::Plain,
        })
        .await;
    let wrong = connection
        .authenticate(&Credentials {
            username: "writer".into(),
            password: "wrong".into(),
            mechanism: SaslMechanism::Plain,
        })
        .await;
    // Identical failures: probing must not reveal which accounts exist.
    assert_eq!(
        format!("{:?}", unknown.err()),
        format!("{:?}", wrong.err()),
        "an unknown user and a wrong password must be indistinguishable"
    );

    stop(running).await;
}

#[tokio::test]
async fn an_authenticated_principal_is_still_bound_by_its_acls() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = MetadataCache::new(image_with_users());
    let running = start_secured_broker(dir.path(), cache).await;

    // `reader` may consume orders but has no write permission.
    let connection = connect(running.addr).await;
    let principal = connection
        .authenticate(&Credentials {
            username: "reader".into(),
            password: "correct horse".into(),
            mechanism: SaslMechanism::Plain,
        })
        .await
        .expect("reader authenticates");
    assert_eq!(principal, "reader");

    let denied = connection
        .request(ApiKey::Produce, &produce_body("orders"))
        .await
        .expect("request completes");
    assert_eq!(
        produce_error(&denied),
        ec::AUTHORIZATION_FAILED,
        "authenticating must not by itself grant write access"
    );

    // The same principal's permitted operation goes through.
    let allowed = connection
        .request(ApiKey::Fetch, &fetch_body("orders"))
        .await
        .expect("request completes");
    let fetched = brahmaputra_protocol::gen::FetchResponse::decode(&allowed).expect("decode fetch");
    assert_eq!(fetched.error_code, ec::NONE, "reader may consume orders");

    stop(running).await;
}

#[tokio::test]
async fn permission_is_scoped_to_the_named_resource() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = MetadataCache::new(image_with_users());
    let running = start_secured_broker(dir.path(), cache).await;

    let connection = connect(running.addr).await;
    connection
        .authenticate(&Credentials {
            username: "writer".into(),
            password: "correct horse".into(),
            mechanism: SaslMechanism::Plain,
        })
        .await
        .expect("writer authenticates");

    // Permitted on `orders`...
    let allowed = connection
        .request(ApiKey::Produce, &produce_body("orders"))
        .await
        .expect("request completes");
    assert_eq!(produce_error(&allowed), ec::NONE);

    // ...and nowhere else, even though the topic would be auto-created.
    let denied = connection
        .request(ApiKey::Produce, &produce_body("secrets"))
        .await
        .expect("request completes");
    assert_eq!(
        produce_error(&denied),
        ec::AUTHORIZATION_FAILED,
        "a rule naming one topic must not grant another"
    );

    stop(running).await;
}

#[tokio::test]
async fn a_deny_rule_overrides_a_wildcard_allow() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut image = image_with_users();
    image
        .apply(MetadataCommand::PutAcl {
            rule: AclRule {
                principal: "*".into(),
                resource_type: ResourceType::Topic,
                resource_name: "*".into(),
                operation: AclOperation::All,
                permission: AclPermission::Allow,
            },
        })
        .expect("wildcard allow");
    image
        .apply(MetadataCommand::PutAcl {
            rule: AclRule {
                principal: "reader".into(),
                resource_type: ResourceType::Topic,
                resource_name: "secrets".into(),
                operation: AclOperation::Read,
                permission: AclPermission::Deny,
            },
        })
        .expect("targeted deny");
    let running = start_secured_broker(dir.path(), MetadataCache::new(image)).await;

    let connection = connect(running.addr).await;
    connection
        .authenticate(&Credentials {
            username: "reader".into(),
            password: "correct horse".into(),
            mechanism: SaslMechanism::Plain,
        })
        .await
        .expect("reader authenticates");

    let denied = connection
        .request(ApiKey::Fetch, &fetch_body("secrets"))
        .await
        .expect("request completes");
    let response = brahmaputra_protocol::gen::FetchResponse::decode(&denied).expect("decode fetch");
    assert_eq!(
        response.error_code,
        ec::AUTHORIZATION_FAILED,
        "a deny rule must beat a wildcard allow"
    );

    stop(running).await;
}

/// SCRAM's whole reason to exist: authentication that is meaningful on a
/// listener that is not encrypted.
///
/// PLAIN is refused there — a password in the clear is not authentication,
/// it is a password on the network — so before this a plaintext deployment
/// had no way to authenticate at all short of client certificates.
#[tokio::test]
async fn scram_authenticates_over_a_plaintext_listener_and_plain_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let cache = MetadataCache::new(image_with_users());
    let broker_epoch = cache.snapshot().brokers.get(&1).map(|b| b.broker_epoch);
    let broker = Arc::new(
        Broker::bind(BrokerConfig {
            broker_id: 1,
            broker_epoch,
            port: 0,
            data_dirs: vec![dir.path().to_owned()],
            default_partitions: 1,
            // Plaintext, deliberately.
            transport: Transport::Tcp,
            require_auth: true,
            metadata_cache: Some(cache),
            ..BrokerConfig::default()
        })
        .await
        .expect("bind broker"),
    );
    let addr = broker.local_addr();
    let serving = Arc::clone(&broker);
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        serving
            .run(async {
                let _ = stopped.await;
            })
            .await
            .expect("broker run");
    });
    let running = RunningBroker {
        addr,
        shutdown,
        task,
    };

    let connection = Connection::connect(running.addr, Some("scram-test".into()), 4)
        .await
        .expect("connect");
    let principal = connection
        .authenticate(&Credentials::new("writer", "correct horse"))
        .await
        .expect("SCRAM authenticates without sending the password");
    assert_eq!(principal, "writer");

    // The same credentials under PLAIN are refused here, which is the
    // distinction: the objection is to sending the password, not to the
    // password being wrong.
    let plain = Connection::connect(running.addr, Some("plain-test".into()), 4)
        .await
        .expect("connect");
    assert!(
        plain
            .authenticate(&Credentials {
                username: "writer".into(),
                password: "correct horse".into(),
                mechanism: SaslMechanism::Plain,
            })
            .await
            .is_err(),
        "PLAIN must stay refused on a plaintext listener"
    );

    // And a wrong password fails under SCRAM too — the proof is what is
    // checked, not merely that the exchange completed.
    let wrong = Connection::connect(running.addr, Some("scram-wrong".into()), 4)
        .await
        .expect("connect");
    assert!(
        wrong
            .authenticate(&Credentials::new("writer", "wrong horse"))
            .await
            .is_err(),
        "a wrong password must not authenticate"
    );

    // As does a user that has no SCRAM credential, without revealing which
    // case it was.
    let unknown = Connection::connect(running.addr, Some("scram-unknown".into()), 4)
        .await
        .expect("connect");
    assert!(
        unknown
            .authenticate(&Credentials::new("nobody", "correct horse"))
            .await
            .is_err(),
        "an unknown user must not authenticate"
    );

    stop(running).await;
}
