//! Incremental fetch sessions (KIP-227), end to end over a real socket.
//!
//! The claim under test is narrow and easy to get wrong: a fetch that names
//! *no* partitions must still return data, because the session remembers
//! what the consumer asked for last time. If that were broken the failure
//! would be silent — a consumer that appears caught up on a topic that is
//! not.

use std::sync::Arc;

use brahmaputra_broker::{Broker, BrokerConfig};
use brahmaputra_client::{Connection, Producer, ProducerConfig};
use brahmaputra_protocol::codec;
use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{FetchMultiPartition, FetchMultiRequest, FetchMultiResponse};
use brahmaputra_protocol::ApiKey;
use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const TOPIC: &str = "session-orders";

struct RunningBroker {
    addr: std::net::SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

async fn start_broker(data_dir: &std::path::Path) -> RunningBroker {
    let broker = Broker::bind(BrokerConfig {
        port: 0,
        data_dirs: vec![data_dir.to_path_buf()],
        default_partitions: 2,
        ..BrokerConfig::default()
    })
    .await
    .expect("bind broker");
    let addr = broker.local_addr();
    let (shutdown, shutdown_rx) = oneshot::channel();
    let serving = Arc::new(broker);
    let task = tokio::spawn(async move {
        serving
            .run(async {
                let _ = shutdown_rx.await;
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

async fn fetch_multi(
    connection: &Connection,
    session_id: i32,
    session_epoch: i32,
    partitions: Vec<FetchMultiPartition>,
) -> FetchMultiResponse {
    let request = FetchMultiRequest {
        max_wait_ms: 200,
        min_bytes: 1,
        isolation_level: 0,
        rack: String::new(),
        session_id,
        session_epoch,
        partitions,
        forgotten: Vec::new(),
    };
    let body = request.encode().expect("encode");
    let response = connection
        .request(ApiKey::FetchMulti, &body)
        .await
        .expect("fetch");
    codec::decode_fetch_multi_response(response)
        .expect("decode")
        .0
}

fn descriptor(partition: i32, offset: i64) -> FetchMultiPartition {
    FetchMultiPartition {
        topic: TOPIC.to_owned(),
        partition,
        fetch_offset: offset,
        max_bytes: 1 << 20,
    }
}

fn records_in(response: &FetchMultiResponse, partition: i32) -> i64 {
    response
        .results
        .iter()
        .find(|result| result.partition == partition)
        .map(|result| result.batches_length)
        .unwrap_or(0)
}

#[tokio::test]
async fn a_session_fetches_partitions_the_request_never_mentions() {
    let temp = tempfile::tempdir().unwrap();
    let running = start_broker(temp.path()).await;

    let producer = Producer::connect(running.addr, ProducerConfig::default())
        .await
        .unwrap();
    for partition in 0..2 {
        for index in 0..5 {
            producer
                .send(
                    TOPIC,
                    Some(partition),
                    None,
                    Bytes::from(format!("p{partition}-{index}")),
                )
                .await
                .unwrap();
        }
    }
    producer.flush().await.unwrap();

    let connection = Connection::connect(running.addr, Some("session-test".into()), 1)
        .await
        .unwrap();

    // Opening a session sends everything and is answered with an id.
    let opened = fetch_multi(&connection, -1, 0, vec![descriptor(0, 0), descriptor(1, 0)]).await;
    assert_eq!(opened.error_code, ec::NONE);
    assert!(opened.session_id > 0, "a session must be established");
    assert_eq!(opened.session_epoch, 1);
    assert_eq!(opened.results.len(), 2);
    assert!(records_in(&opened, 0) > 0 && records_in(&opened, 1) > 0);

    // The claim: a request naming nothing still fetches both partitions.
    let incremental =
        fetch_multi(&connection, opened.session_id, opened.session_epoch, vec![]).await;
    assert_eq!(incremental.error_code, ec::NONE);
    assert_eq!(incremental.session_epoch, 2, "the epoch advances");
    assert_eq!(
        incremental.results.len(),
        2,
        "both partitions are still fetched: {:?}",
        incremental
            .results
            .iter()
            .map(|result| result.partition)
            .collect::<Vec<_>>()
    );
    assert!(records_in(&incremental, 0) > 0 && records_in(&incremental, 1) > 0);

    // An offset that moved is sent, and only that one.
    let advanced = fetch_multi(
        &connection,
        incremental.session_id,
        incremental.session_epoch,
        vec![descriptor(0, 5)],
    )
    .await;
    assert_eq!(advanced.error_code, ec::NONE);
    assert_eq!(advanced.results.len(), 2);
    assert_eq!(
        records_in(&advanced, 0),
        0,
        "the partition read to its end returns nothing"
    );
    assert!(
        records_in(&advanced, 1) > 0,
        "and the one that did not move is still served from where it was"
    );

    let _ = running.shutdown.send(());
    running.task.await.unwrap();
}

/// A session the broker does not have must fail loudly at request level.
///
/// Silently treating it as a full fetch would be worse than an error: the
/// request names only the partitions that changed, so it would look like a
/// consumer that had dropped every other partition from its assignment.
#[tokio::test]
async fn an_unknown_session_is_refused_rather_than_guessed_at() {
    let temp = tempfile::tempdir().unwrap();
    let running = start_broker(temp.path()).await;

    let producer = Producer::connect(running.addr, ProducerConfig::default())
        .await
        .unwrap();
    producer
        .send(TOPIC, Some(0), None, Bytes::from_static(b"one"))
        .await
        .unwrap();
    producer.flush().await.unwrap();

    let connection = Connection::connect(running.addr, Some("session-test".into()), 1)
        .await
        .unwrap();
    let response = fetch_multi(&connection, 9999, 3, vec![descriptor(0, 0)]).await;
    assert_eq!(response.error_code, ec::FETCH_SESSION_NOT_FOUND);
    assert!(response.results.is_empty());

    // And a full fetch still works immediately afterwards, which is the
    // recovery a client performs.
    let recovered = fetch_multi(&connection, -1, 0, vec![descriptor(0, 0)]).await;
    assert_eq!(recovered.error_code, ec::NONE);
    assert!(records_in(&recovered, 0) > 0);

    let _ = running.shutdown.send(());
    running.task.await.unwrap();
}
