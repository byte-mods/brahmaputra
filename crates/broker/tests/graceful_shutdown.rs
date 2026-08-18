use std::sync::Arc;
use std::time::Duration;

use brahmaputra_broker::{Broker, BrokerConfig, BrokerError};
use brahmaputra_client::{Consumer, Producer, ProducerConfig};
use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;

const TOPIC: &str = "shutdown-durability";
const SHUTDOWN_LIMIT: Duration = Duration::from_secs(1);

struct RunningBroker {
    broker: Arc<Broker>,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

async fn start_broker(data_dir: &std::path::Path) -> RunningBroker {
    let broker = Arc::new(
        Broker::bind(BrokerConfig {
            port: 0,
            data_dir: data_dir.to_owned(),
            ..BrokerConfig::default()
        })
        .await
        .expect("bind broker"),
    );
    let (shutdown, shutdown_rx) = oneshot::channel();
    let serving = Arc::clone(&broker);
    let task = tokio::spawn(async move {
        serving
            .run(async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("run broker");
    });
    RunningBroker {
        broker,
        shutdown,
        task,
    }
}

async fn stop_broker(running: RunningBroker) -> Arc<Broker> {
    let RunningBroker {
        broker,
        shutdown,
        task,
    } = running;
    let started = Instant::now();
    let _ = shutdown.send(());
    tokio::time::timeout(SHUTDOWN_LIMIT, task)
        .await
        .expect("broker shutdown must not wait on clients")
        .expect("broker task");
    assert!(
        started.elapsed() < SHUTDOWN_LIMIT,
        "broker shutdown took {:?}",
        started.elapsed()
    );
    broker
}

#[tokio::test]
async fn idle_connection_does_not_delay_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let running = start_broker(dir.path()).await;
    let consumer = Consumer::connect(running.broker.local_addr(), "idle-client")
        .await
        .expect("connect idle client");
    consumer
        .metadata(&[])
        .await
        .expect("prove the connection was accepted");

    let RunningBroker {
        broker,
        shutdown,
        task,
    } = running;
    let started = Instant::now();
    broker.fence();
    assert!(broker.is_fenced());
    tokio::time::timeout(SHUTDOWN_LIMIT, task)
        .await
        .expect("fence must stop the broker with an idle client")
        .expect("broker task");
    assert!(started.elapsed() < SHUTDOWN_LIMIT);
    drop(shutdown);
    drop(consumer);

    let reconnect = tokio::net::TcpStream::connect(broker.local_addr()).await;
    assert!(
        reconnect.is_err(),
        "the listener must be closed when run returns"
    );
}

#[tokio::test]
async fn very_long_fetch_is_cancelled_and_acknowledged_produce_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let running = start_broker(dir.path()).await;
    let producer = Producer::connect(
        running.broker.local_addr(),
        ProducerConfig {
            linger_ms: 0,
            ..ProducerConfig::default()
        },
    )
    .await
    .expect("connect producer");
    let offset = producer
        .send(TOPIC, Some(0), None, Bytes::from_static(b"durable"))
        .await
        .expect("acknowledged produce");
    assert_eq!(offset, 0);

    let consumer = Consumer::connect(running.broker.local_addr(), "long-fetch-client")
        .await
        .expect("connect consumer");
    let fetch = tokio::spawn(async move { consumer.fetch(TOPIC, 0, 1, i32::MAX).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!fetch.is_finished(), "fetch must be waiting at shutdown");

    stop_broker(running).await;
    assert!(
        tokio::time::timeout(SHUTDOWN_LIMIT, fetch)
            .await
            .expect("cancelled fetch task must finish")
            .expect("fetch task")
            .is_err(),
        "the cancelled Fetch must observe connection closure"
    );
    drop(producer);

    let reopened = start_broker(dir.path()).await;
    let consumer = Consumer::connect(reopened.broker.local_addr(), "restart-reader")
        .await
        .expect("connect restart reader");
    let records = consumer
        .fetch(TOPIC, 0, 0, 0)
        .await
        .expect("read acknowledged record after restart");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].0, 0);
    assert_eq!(records[0].2, Bytes::from_static(b"durable"));
    drop(consumer);
    stop_broker(reopened).await;
}

#[tokio::test]
async fn shutdown_gate_prevents_partition_actor_resurrection() {
    let dir = tempfile::tempdir().unwrap();
    let running = start_broker(dir.path()).await;
    let broker = Arc::clone(&running.broker);
    drop(
        broker
            .partition_auto_create("before-shutdown", 0)
            .expect("pre-shutdown actor"),
    );

    let RunningBroker {
        broker: _,
        shutdown,
        task,
    } = running;
    let _ = shutdown.send(());

    let deadline = Instant::now() + SHUTDOWN_LIMIT;
    loop {
        match broker.partition_auto_create("shutdown-race", 0) {
            Ok(handle) => drop(handle),
            Err(BrokerError::ActorUnavailable(_)) => break,
            Err(error) => panic!("unexpected partition error: {error}"),
        }
        assert!(
            Instant::now() < deadline,
            "shutdown gate was never published"
        );
        tokio::task::yield_now().await;
    }

    tokio::time::timeout(SHUTDOWN_LIMIT, task)
        .await
        .expect("broker shutdown")
        .expect("broker task");
    for topic in ["after-shutdown-a", "after-shutdown-b"] {
        assert!(matches!(
            broker.partition_auto_create(topic, 0),
            Err(BrokerError::ActorUnavailable(_))
        ));
        assert!(
            !dir.path().join(format!("{topic}-0")).exists(),
            "a post-shutdown actor created its log directory"
        );
    }
}
