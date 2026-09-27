"""Brahmaputra client for Python.

    from brahmaputra import Producer, ProducerConfig, GroupConsumer

    with Producer("127.0.0.1", 9092) as producer:
        producer.send("orders", b'{"id":1}', key=b"user-7")
        producer.flush()

    with GroupConsumer("127.0.0.1", 9092, "billing") as consumer:
        consumer.subscribe(["orders"])
        while True:
            for record in consumer.poll(500):
                handle(record.value)
            consumer.commit()   # at-least-once: commit after processing
"""

from .protocol import (
    API_VERSION,
    READ_COMMITTED,
    READ_UNCOMMITTED,
    ApiKey,
    BrahmaputraError,
    Compression,
    ErrorCode,
    NoOffsetForPartition,
    ProtocolError,
    Record,
    RecordHeader,
    ServerError,
    crc32c,
    murmur2,
    partition_for_key,
    register_codec,
)
from .client import (
    EARLIEST,
    LATEST,
    BrokerInfo,
    BrokerRouter,
    ClusterMetadata,
    ConsumedRecord,
    Connection,
    Consumer,
    ConsumerConfig,
    PartitionInfo,
    Producer,
    ProducerConfig,
    TopicInfo,
)
from .group import (
    Assignor,
    AutoOffsetReset,
    GroupConfig,
    GroupConsumer,
)

__version__ = "0.1.0"

__all__ = [
    "API_VERSION",
    "READ_COMMITTED",
    "READ_UNCOMMITTED",
    "ApiKey",
    "Assignor",
    "AutoOffsetReset",
    "BrahmaputraError",
    "BrokerInfo",
    "BrokerRouter",
    "ClusterMetadata",
    "Compression",
    "Connection",
    "ConsumedRecord",
    "Consumer",
    "ConsumerConfig",
    "EARLIEST",
    "ErrorCode",
    "GroupConfig",
    "GroupConsumer",
    "LATEST",
    "NoOffsetForPartition",
    "PartitionInfo",
    "Producer",
    "ProducerConfig",
    "ProtocolError",
    "Record",
    "RecordHeader",
    "ServerError",
    "TopicInfo",
    "__version__",
    "crc32c",
    "murmur2",
    "partition_for_key",
    "register_codec",
]
