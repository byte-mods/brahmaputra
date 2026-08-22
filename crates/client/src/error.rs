use std::io;

use brahmaputra_protocol::ProtocolError;
use thiserror::Error;

/// Errors produced by the client.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    /// The connection's reader/writer task died; all in-flight requests fail.
    #[error("connection closed")]
    ConnectionClosed,

    /// A local timeout expired (e.g. `max_block_ms` while waiting for an
    /// in-flight slot), like Kafka's `TimeoutException`.
    #[error("timeout: {0}")]
    Timeout(String),

    #[error("invalid producer configuration: {0}")]
    Configuration(String),

    #[error("idempotent producer cannot continue: {0}")]
    Idempotence(String),

    /// `auto.offset.reset=none` and the partition has no committed offset.
    #[error("no committed offset for {topic}-{partition} and auto.offset.reset=none")]
    NoOffsetForPartition { topic: String, partition: i32 },

    /// The broker answered with a non-zero `error_code`.
    #[error("server error {code}: {message}")]
    Server { code: i32, message: String },
}

impl ClientError {
    /// Map a wire `error_code` (0 = none) to a typed error.
    pub fn from_error_code(code: i32) -> Result<(), ClientError> {
        use brahmaputra_protocol::error_code as ec;
        match code {
            ec::NONE => Ok(()),
            ec::UNKNOWN_TOPIC_OR_PARTITION => Err(ClientError::Server {
                code,
                message: "unknown topic or partition".into(),
            }),
            ec::OFFSET_OUT_OF_RANGE => Err(ClientError::Server {
                code,
                message: "offset out of range".into(),
            }),
            ec::INVALID_REQUEST => Err(ClientError::Server {
                code,
                message: "invalid request".into(),
            }),
            ec::UNSUPPORTED_VERSION => Err(ClientError::Server {
                code,
                message: "unsupported api version".into(),
            }),
            ec::NOT_LEADER_OR_FOLLOWER => Err(ClientError::Server {
                code,
                message: "not leader or follower".into(),
            }),
            ec::FENCED_BROKER_EPOCH => Err(ClientError::Server {
                code,
                message: "fenced broker epoch".into(),
            }),
            ec::FENCED_LEADER_EPOCH => Err(ClientError::Server {
                code,
                message: "fenced leader epoch".into(),
            }),
            ec::UNKNOWN_LEADER_EPOCH => Err(ClientError::Server {
                code,
                message: "unknown leader epoch".into(),
            }),
            ec::NOT_ENOUGH_REPLICAS => Err(ClientError::Server {
                code,
                message: "not enough in-sync replicas".into(),
            }),
            ec::FENCED_PRODUCER_EPOCH => Err(ClientError::Server {
                code,
                message: "fenced producer epoch".into(),
            }),
            ec::OUT_OF_ORDER_SEQUENCE => Err(ClientError::Server {
                code,
                message: "out of order producer sequence".into(),
            }),
            ec::UNKNOWN_MEMBER_ID => Err(ClientError::Server {
                code,
                message: "unknown member id".into(),
            }),
            ec::REBALANCE_IN_PROGRESS => Err(ClientError::Server {
                code,
                message: "rebalance in progress".into(),
            }),
            ec::NOT_COORDINATOR => Err(ClientError::Server {
                code,
                message: "not the group coordinator".into(),
            }),
            ec::ILLEGAL_GENERATION => Err(ClientError::Server {
                code,
                message: "illegal generation".into(),
            }),
            ec::COORDINATOR_LOAD_IN_PROGRESS => Err(ClientError::Server {
                code,
                message: "coordinator load in progress".into(),
            }),
            ec::INVALID_TXN_STATE => Err(ClientError::Server {
                code,
                message: "invalid transaction state".into(),
            }),
            ec::INVALID_PRODUCER_ID_MAPPING => Err(ClientError::Server {
                code,
                message: "unknown transactional id for this producer".into(),
            }),
            ec::CONCURRENT_TRANSACTIONS => Err(ClientError::Server {
                code,
                message: "concurrent transaction; retry".into(),
            }),
            // Not "unknown partition": the partition exists and is assigned
            // to that broker, but the disk under it has failed. A client
            // that read this as a missing topic would conclude the topic
            // had been deleted.
            ec::LOG_DIR_OFFLINE => Err(ClientError::Server {
                code,
                message: "the broker's log directory for this partition is offline".into(),
            }),
            other => Err(ClientError::Server {
                code: other,
                message: "internal broker error".into(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ClientError;
    use brahmaputra_protocol::error_code as ec;

    #[test]
    fn maps_not_leader_or_follower_to_a_specific_message() {
        let error = ClientError::from_error_code(ec::NOT_LEADER_OR_FOLLOWER)
            .expect_err("non-zero broker error must fail");

        match error {
            ClientError::Server { code, message } => {
                assert_eq!(code, ec::NOT_LEADER_OR_FOLLOWER);
                assert_eq!(message, "not leader or follower");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn maps_replication_fences_to_stable_messages() {
        let cases = [
            (ec::FENCED_BROKER_EPOCH, "fenced broker epoch"),
            (ec::FENCED_LEADER_EPOCH, "fenced leader epoch"),
            (ec::UNKNOWN_LEADER_EPOCH, "unknown leader epoch"),
            (ec::NOT_ENOUGH_REPLICAS, "not enough in-sync replicas"),
            (ec::FENCED_PRODUCER_EPOCH, "fenced producer epoch"),
            (ec::OUT_OF_ORDER_SEQUENCE, "out of order producer sequence"),
        ];
        for (code, expected) in cases {
            match ClientError::from_error_code(code).unwrap_err() {
                ClientError::Server {
                    code: actual,
                    message,
                } => {
                    assert_eq!(actual, code);
                    assert_eq!(message, expected);
                }
                other => panic!("unexpected error: {other}"),
            }
        }
    }

    #[test]
    fn maps_transaction_and_storage_errors_to_stable_messages() {
        let cases = [
            (ec::INVALID_TXN_STATE, "invalid transaction state"),
            (
                ec::INVALID_PRODUCER_ID_MAPPING,
                "unknown transactional id for this producer",
            ),
            (ec::CONCURRENT_TRANSACTIONS, "concurrent transaction; retry"),
            (
                ec::LOG_DIR_OFFLINE,
                "the broker's log directory for this partition is offline",
            ),
        ];
        for (code, expected) in cases {
            match ClientError::from_error_code(code).unwrap_err() {
                ClientError::Server {
                    code: actual,
                    message,
                } => {
                    assert_eq!(actual, code);
                    assert_eq!(
                        message, expected,
                        "code {code} must not fall through to the generic message"
                    );
                }
                other => panic!("unexpected error: {other}"),
            }
        }
    }

    #[test]
    fn maps_group_coordinator_errors_to_stable_messages() {
        let cases = [
            (ec::UNKNOWN_MEMBER_ID, "unknown member id"),
            (ec::REBALANCE_IN_PROGRESS, "rebalance in progress"),
            (ec::NOT_COORDINATOR, "not the group coordinator"),
            (ec::ILLEGAL_GENERATION, "illegal generation"),
            (
                ec::COORDINATOR_LOAD_IN_PROGRESS,
                "coordinator load in progress",
            ),
        ];
        for (code, expected) in cases {
            match ClientError::from_error_code(code).unwrap_err() {
                ClientError::Server {
                    code: actual,
                    message,
                } => {
                    assert_eq!(actual, code);
                    assert_eq!(message, expected);
                }
                other => panic!("unexpected error: {other}"),
            }
        }
    }
}
