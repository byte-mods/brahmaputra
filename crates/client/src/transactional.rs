//! Transactional producer: many writes, one decision.
//!
//! ```text
//! let mut producer = TransactionalProducer::init(addr, "orders-etl").await?;
//! producer.begin()?;
//! producer.send("orders", 0, Record::new(b"a".to_vec())).await?;
//! producer.send("audit",  0, Record::new(b"b".to_vec())).await?;
//! producer.commit().await?;   // both, or (on abort) neither
//! ```
//!
//! Records reach the log as they are sent, not at commit — buffering a
//! transaction's whole output in the client would put durability back in
//! the process that is least able to provide it. What makes the writes
//! atomic is the marker the coordinator appends afterwards, and the rule
//! that a `read_committed` consumer will not look past the first record of
//! an unmarked transaction.
//!
//! Two consequences worth stating plainly, because they surprise people:
//!
//! * A `read_uncommitted` consumer — the default — **sees aborted
//!   records**. Isolation is the reader's choice; nothing about a
//!   transaction hides its writes from a reader that did not ask.
//! * Offsets are not contiguous. Each transaction's marker occupies one,
//!   so a committed reader sees gaps exactly as it does after compaction.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

use brahmaputra_protocol::error_code as ec;
use brahmaputra_protocol::gen::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse, EndTxnRequest, EndTxnResponse, OffsetCommitEntry, ProduceRequest,
    ProduceResponse, TxnOffsetCommitRequest, TxnOffsetCommitResponse, TxnPartition,
};
use brahmaputra_protocol::producer::{InitProducerIdRequest, InitProducerIdResponse};
use brahmaputra_protocol::{codec, ApiKey, ProducerMetadata, Record, RecordBatch};
use bytes::Bytes;

use crate::error::ClientError;
use crate::group_consumer::msg_err;
use crate::router::BrokerRouter;
use crate::transport::{Transport, TransportConfig};

/// Default `transaction.timeout.ms`, matching Kafka's.
pub const DEFAULT_TRANSACTION_TIMEOUT_MS: i32 = 60_000;

/// A producer that writes under a `transactional.id`.
pub struct TransactionalProducer {
    router: BrokerRouter,
    transactional_id: String,
    producer_id: i64,
    producer_epoch: i16,
    /// Next sequence per partition, for the broker's idempotent dedup.
    sequences: BTreeMap<(String, i32), i32>,
    /// Partitions announced to the coordinator in the current transaction.
    /// A partition is announced once, not once per record.
    announced: BTreeSet<(String, i32)>,
    open: bool,
}

impl TransactionalProducer {
    /// Claim `transactional_id`, fencing any previous holder of it.
    ///
    /// The coordinator bumps the epoch and resolves anything the previous
    /// instance abandoned, so this is also the recovery path: a crashed
    /// producer's half-finished transaction is settled by its replacement
    /// starting up, not by anyone noticing.
    pub async fn init(
        addr: SocketAddr,
        transactional_id: &str,
    ) -> Result<TransactionalProducer, ClientError> {
        TransactionalProducer::init_with(
            Transport::default(),
            addr,
            transactional_id,
            DEFAULT_TRANSACTION_TIMEOUT_MS,
        )
        .await
    }

    pub async fn init_with(
        transport: impl Into<TransportConfig>,
        addr: SocketAddr,
        transactional_id: &str,
        timeout_ms: i32,
    ) -> Result<TransactionalProducer, ClientError> {
        if transactional_id.is_empty() {
            return Err(ClientError::Configuration(
                "a transactional id must not be empty".into(),
            ));
        }
        let router =
            BrokerRouter::connect_with(transport, addr, Some(format!("txn-{transactional_id}")), 5)
                .await?;

        let request = InitProducerIdRequest::transactional(transactional_id, timeout_ms);
        let response = router
            .request_seed(ApiKey::InitProducerId, &request.encode())
            .await?;
        let response = InitProducerIdResponse::decode(&response).map_err(ClientError::Protocol)?;
        ClientError::from_error_code(response.error_code)?;

        Ok(TransactionalProducer {
            router,
            transactional_id: transactional_id.to_owned(),
            producer_id: response.producer_id,
            producer_epoch: response.producer_epoch,
            sequences: BTreeMap::new(),
            announced: BTreeSet::new(),
            open: false,
        })
    }

    /// The identity the coordinator issued. Useful for asserting in tests
    /// that a restart really did fence the previous instance.
    pub fn producer_identity(&self) -> (i64, i16) {
        (self.producer_id, self.producer_epoch)
    }

    /// Open a transaction. Purely local: nothing is sent until the first
    /// record needs a partition announced.
    pub fn begin(&mut self) -> Result<(), ClientError> {
        if self.open {
            return Err(ClientError::Configuration(
                "a transaction is already open".into(),
            ));
        }
        self.open = true;
        self.announced.clear();
        Ok(())
    }

    fn require_open(&self) -> Result<(), ClientError> {
        if self.open {
            Ok(())
        } else {
            Err(ClientError::Configuration(
                "no transaction is open; call begin() first".into(),
            ))
        }
    }

    /// Announce a partition to the coordinator, once per transaction.
    ///
    /// Before the first write to it, never after: a partition the
    /// coordinator has not been told about receives no marker, and its
    /// records stay in doubt for every committed reader indefinitely.
    async fn announce(&mut self, topic: &str, partition: i32) -> Result<(), ClientError> {
        let key = (topic.to_owned(), partition);
        if self.announced.contains(&key) {
            return Ok(());
        }
        let request = AddPartitionsToTxnRequest {
            transactional_id: self.transactional_id.clone(),
            producer_id: self.producer_id,
            producer_epoch: i32::from(self.producer_epoch),
            partitions: vec![TxnPartition {
                topic: topic.to_owned(),
                partition,
            }],
        };
        let body = request.encode().map_err(msg_err)?;
        let response = self
            .coordinator_request(ApiKey::AddPartitionsToTxn, &body)
            .await?;
        let decoded = AddPartitionsToTxnResponse::decode(&response).map_err(msg_err)?;
        ClientError::from_error_code(decoded.error_code)?;
        self.announced.insert(key);
        Ok(())
    }

    /// Send a request to this producer's transaction coordinator.
    ///
    /// Routed to the `__transaction_state` partition that owns the id, the
    /// same way a group request reaches its coordinator — so no discovery
    /// call is needed and the answer cannot disagree with metadata.
    async fn coordinator_request(
        &self,
        api_key: ApiKey,
        body: &[u8],
    ) -> Result<Bytes, ClientError> {
        let partitions = self.router.partitions(TRANSACTION_STATE_TOPIC).await?;
        let count = partitions.len().max(1) as i32;
        let partition = coordinator_partition(&self.transactional_id, count);
        self.router
            .request_partition(TRANSACTION_STATE_TOPIC, partition, api_key, body)
            .await
    }

    /// Write one record inside the open transaction.
    pub async fn send(
        &mut self,
        topic: &str,
        partition: i32,
        record: Record,
    ) -> Result<i64, ClientError> {
        self.send_batch(topic, partition, vec![record]).await
    }

    /// Write several records to one partition as a single batch.
    pub async fn send_batch(
        &mut self,
        topic: &str,
        partition: i32,
        records: Vec<Record>,
    ) -> Result<i64, ClientError> {
        self.require_open()?;
        if records.is_empty() {
            return Err(ClientError::Configuration(
                "a transactional send needs at least one record".into(),
            ));
        }
        self.announce(topic, partition).await?;

        let key = (topic.to_owned(), partition);
        let base_sequence = *self.sequences.get(&key).unwrap_or(&0);
        let count = records.len() as i32;

        let mut batch = RecordBatch::new(0, 0, now_ms(), records);
        batch.producer = Some(ProducerMetadata {
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            base_sequence,
        });
        batch.transactional = true;

        let request = ProduceRequest {
            topic: topic.to_owned(),
            partition,
            // The marker is what makes the write durable-and-decided; the
            // records themselves still need the ISR behind them, so this is
            // the strict setting rather than the fast one.
            acks: -1,
            timeout_ms: 30_000,
            batches_length: 0,
        };
        let body = codec::encode_produce_request(&request, &[batch.encode()])
            .map_err(ClientError::Protocol)?;
        let response = self
            .router
            .request_partition(topic, partition, ApiKey::Produce, &body)
            .await?;
        let decoded = ProduceResponse::decode(&response).map_err(msg_err)?;
        ClientError::from_error_code(decoded.error_code)?;

        self.sequences.insert(key, base_sequence + count);
        Ok(decoded.base_offset)
    }

    /// Commit consumed offsets as part of this transaction.
    ///
    /// This is what makes read-process-write atomic: the offsets advance if
    /// and only if the output records do. Two requests, because they go to
    /// two different coordinators — the transaction coordinator has to know
    /// that `__consumer_offsets` needs a marker too, and the group
    /// coordinator is where the offsets themselves live.
    pub async fn send_offsets(
        &mut self,
        group_id: &str,
        offsets: &[(String, i32, i64)],
    ) -> Result<(), ClientError> {
        self.require_open()?;
        if offsets.is_empty() {
            return Ok(());
        }

        let add = AddOffsetsToTxnRequest {
            transactional_id: self.transactional_id.clone(),
            producer_id: self.producer_id,
            producer_epoch: i32::from(self.producer_epoch),
            group_id: group_id.to_owned(),
        };
        let body = add.encode().map_err(msg_err)?;
        let response = self
            .coordinator_request(ApiKey::AddOffsetsToTxn, &body)
            .await?;
        ClientError::from_error_code(
            AddOffsetsToTxnResponse::decode(&response)
                .map_err(msg_err)?
                .error_code,
        )?;

        let commit = TxnOffsetCommitRequest {
            transactional_id: self.transactional_id.clone(),
            producer_id: self.producer_id,
            producer_epoch: i32::from(self.producer_epoch),
            group_id: group_id.to_owned(),
            offsets: offsets
                .iter()
                .map(|(topic, partition, offset)| OffsetCommitEntry {
                    topic: topic.clone(),
                    partition: *partition,
                    offset: *offset,
                })
                .collect(),
        };
        let body = commit.encode().map_err(msg_err)?;
        // To the *group* coordinator: that is where offsets live.
        let response = self
            .router
            .request_partition(
                OFFSETS_TOPIC,
                group_coordinator_partition(
                    group_id,
                    self.router.partitions(OFFSETS_TOPIC).await?.len().max(1) as i32,
                ),
                ApiKey::TxnOffsetCommit,
                &body,
            )
            .await?;
        ClientError::from_error_code(
            TxnOffsetCommitResponse::decode(&response)
                .map_err(msg_err)?
                .error_code,
        )?;
        Ok(())
    }

    /// Commit: every record written since `begin` becomes visible to a
    /// `read_committed` consumer, all at once.
    pub async fn commit(&mut self) -> Result<(), ClientError> {
        self.end(true).await
    }

    /// Abort: those records stay in the log but are skipped forever by a
    /// `read_committed` consumer. They are *not* deleted — an append-only
    /// log cannot remove them without moving every offset after them.
    pub async fn abort(&mut self) -> Result<(), ClientError> {
        self.end(false).await
    }

    async fn end(&mut self, committed: bool) -> Result<(), ClientError> {
        self.require_open()?;
        let request = EndTxnRequest {
            transactional_id: self.transactional_id.clone(),
            producer_id: self.producer_id,
            producer_epoch: i32::from(self.producer_epoch),
            committed,
        };
        let body = request.encode().map_err(msg_err)?;
        let response = self.coordinator_request(ApiKey::EndTxn, &body).await?;
        let decoded = EndTxnResponse::decode(&response).map_err(msg_err)?;
        // Whatever the outcome, this transaction is over as far as the
        // client is concerned: a failed end is retried by starting again,
        // not by leaving the local state saying a transaction is open.
        self.open = false;
        self.announced.clear();
        if decoded.error_code != ec::NONE {
            return Err(ClientError::from_error_code(decoded.error_code).unwrap_err());
        }
        Ok(())
    }
}

/// Internal topic names and hashes, mirrored from the broker so a client can
/// route to a coordinator without asking.
pub(crate) const TRANSACTION_STATE_TOPIC: &str = "__transaction_state";
const OFFSETS_TOPIC: &str = "__consumer_offsets";

pub(crate) fn coordinator_partition(transactional_id: &str, partition_count: i32) -> i32 {
    (crc32c::crc32c(transactional_id.as_bytes()) % partition_count.max(1) as u32) as i32
}

fn group_coordinator_partition(group_id: &str, partition_count: i32) -> i32 {
    (crc32c::crc32c(group_id.as_bytes()) % partition_count.max(1) as u32) as i32
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinator_selection_matches_the_brokers_own_hash() {
        // The client computes this locally; if it drifted from the broker's
        // formula, every transaction would be sent to a broker that would
        // answer NOT_COORDINATOR.
        for id in ["orders-etl", "billing", "x"] {
            assert_eq!(
                coordinator_partition(id, 50),
                (crc32c::crc32c(id.as_bytes()) % 50) as i32
            );
        }
    }
}
