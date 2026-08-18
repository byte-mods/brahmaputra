//! The client example printed in the README, compiled so it cannot go stale.
use brahmaputra_client::{Consumer, GroupConsumer, Producer, ProducerConfig, EARLIEST};
use bytes::Bytes;
use std::time::Duration;

#[allow(dead_code)]
async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let producer = Producer::connect(
        "127.0.0.1:9092".parse()?,
        ProducerConfig {
            acks: 1,
            batch_size: 64 * 1024,
            linger_ms: 5,
            ..ProducerConfig::default()
        },
    )
    .await?;
    let _offset = producer
        .send(
            "orders",
            None,
            Some(Bytes::from("user-7")),
            Bytes::from(r#"{"id":1}"#),
        )
        .await?;
    producer.flush().await?;

    let consumer = Consumer::connect("127.0.0.1:9092".parse()?, "reader").await?;
    let _records = consumer.fetch("orders", 0, EARLIEST, 500).await?;

    let mut group =
        GroupConsumer::connect("127.0.0.1:9092".parse()?, "reader-1", "billing").await?;
    group.subscribe(&["orders"]);
    loop {
        for record in group.poll(Duration::from_millis(500)).await? {
            let _ = &record.value;
        }
        group.commit_sync().await?;
    }
}

fn main() {}
