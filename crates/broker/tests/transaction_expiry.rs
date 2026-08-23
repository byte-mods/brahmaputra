//! What happens to a transaction whose producer never comes back.
//!
//! A transaction that is never ended holds the last stable offset of every
//! partition it wrote to, and a `read_committed` consumer may not look past
//! it. That is correct while the producer might still commit — and a
//! permanent outage for every consumer of those partitions once it cannot,
//! which is what `transaction.timeout.ms` exists to bound.

use std::sync::Arc;
use std::time::Duration;

use brahmaputra_broker::{Broker, BrokerConfig};
use brahmaputra_client::Transport;
use brahmaputra_client::{Consumer, TransactionalProducer};
use brahmaputra_protocol::{IsolationLevel, Record};
use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const TOPIC: &str = "txn-expiry-orders";

struct RunningBroker {
    broker: Arc<Broker>,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

async fn start_broker(config: BrokerConfig) -> RunningBroker {
    let broker = Arc::new(Broker::bind(config).await.unwrap());
    let (shutdown, shutdown_rx) = oneshot::channel();
    let serving = Arc::clone(&broker);
    let task = tokio::spawn(async move {
        serving
            .run(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    RunningBroker {
        broker,
        shutdown,
        task,
    }
}

async fn stop_broker(running: RunningBroker) {
    let _ = running.shutdown.send(());
    running.task.await.unwrap();
}

/// Offsets a `read_committed` consumer can actually see.
async fn committed_values(address: std::net::SocketAddr) -> Vec<String> {
    let consumer = Consumer::connect(address, "committed-reader")
        .await
        .unwrap()
        .with_isolation_level(IsolationLevel::ReadCommitted);
    let records = consumer.fetch(TOPIC, 0, 0, 1_000).await.unwrap();
    records
        .into_iter()
        .map(|record| String::from_utf8_lossy(&record.value.unwrap_or_default()).into_owned())
        .collect()
}

/// An abandoned transaction must not hold committed readers forever.
///
/// The producer here never commits, never aborts, and never reconnects —
/// the shape of a process that was killed, scaled down, or redeployed under
/// a different `transactional.id`. Before the coordinator policed timeouts,
/// nothing in the system would ever resolve it.
#[tokio::test]
async fn an_abandoned_transaction_is_aborted_once_its_timeout_passes() {
    let temp = tempfile::tempdir().unwrap();
    let running = start_broker(BrokerConfig {
        port: 0,
        data_dirs: vec![temp.path().to_owned()],
        // Short enough to observe, long enough that a slow test machine
        // cannot trip it before the assertions below run.
        transaction_max_timeout: Duration::from_secs(3),
        ..BrokerConfig::default()
    })
    .await;
    let address = running.broker.local_addr();

    // A committed record first, so the assertions can tell "nothing is
    // visible" apart from "the topic is empty".
    let mut producer = TransactionalProducer::init_with(Transport::Tcp, address, "etl", 2_000)
        .await
        .unwrap();
    producer.begin().unwrap();
    producer
        .send(TOPIC, 0, Record::new(Bytes::from_static(b"committed")))
        .await
        .unwrap();
    producer.commit().await.unwrap();

    // Now one that is simply abandoned.
    producer.begin().unwrap();
    producer
        .send(TOPIC, 0, Record::new(Bytes::from_static(b"abandoned")))
        .await
        .unwrap();
    drop(producer);

    // While it is open, the committed reader is held at its first record —
    // it cannot see the abandoned record, and it also cannot see anything
    // after it, because nothing after it is decided yet.
    assert_eq!(
        committed_values(address).await,
        vec!["committed".to_owned()],
        "an open transaction must hold a committed reader at the LSO"
    );

    // A second producer commits perfectly well — and its records are still
    // invisible, because the abandoned transaction sits in front of them.
    // This is the failure being fixed: one dead producer stalls every
    // committed reader of the partition, however healthy everything else
    // is.
    let mut later = TransactionalProducer::init_with(Transport::Tcp, address, "etl-2", 60_000)
        .await
        .unwrap();
    later.begin().unwrap();
    later
        .send(TOPIC, 0, Record::new(Bytes::from_static(b"after")))
        .await
        .unwrap();
    later.commit().await.unwrap();
    assert_eq!(
        committed_values(address).await,
        vec!["committed".to_owned()],
        "a committed transaction behind an open one stays invisible"
    );

    // Past the timeout the coordinator aborts the abandoned transaction
    // without anyone asking, and everything behind it is released.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut visible = Vec::new();
    while std::time::Instant::now() < deadline {
        visible = committed_values(address).await;
        if visible.len() > 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        visible.contains(&"after".to_owned()),
        "the coordinator must abort a transaction that outlived its timeout, \
         releasing the committed records behind it: {visible:?}"
    );
    assert!(
        !visible.contains(&"abandoned".to_owned()),
        "the aborted transaction's records must never be delivered: {visible:?}"
    );

    stop_broker(running).await;
}

/// The producer of a timed-out transaction must not be able to carry on
/// writing into it.
///
/// If it could, its later records would belong to a transaction that has
/// already been marked aborted — in doubt with no marker ever coming, which
/// is the same permanent stall the timeout was meant to prevent.
#[tokio::test]
async fn a_producer_whose_transaction_timed_out_is_fenced() {
    let temp = tempfile::tempdir().unwrap();
    let running = start_broker(BrokerConfig {
        port: 0,
        data_dirs: vec![temp.path().to_owned()],
        transaction_max_timeout: Duration::from_secs(2),
        ..BrokerConfig::default()
    })
    .await;
    let address = running.broker.local_addr();

    let mut producer = TransactionalProducer::init_with(Transport::Tcp, address, "slow", 1_000)
        .await
        .unwrap();
    producer.begin().unwrap();
    producer
        .send(TOPIC, 0, Record::new(Bytes::from_static(b"first")))
        .await
        .unwrap();

    // Long enough for the coordinator to have timed it out and fenced this
    // instance.
    tokio::time::sleep(Duration::from_secs(4)).await;

    let outcome = producer.commit().await;
    assert!(
        outcome.is_err(),
        "committing a transaction the coordinator already aborted must fail"
    );

    stop_broker(running).await;
}
