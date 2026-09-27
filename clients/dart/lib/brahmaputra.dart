/// Native Dart client for the Brahmaputra log broker.
///
/// Speaks Brahmaputra's own wire protocol over `dart:io` sockets: a
/// batching [Producer], a partition [Consumer], and a group-coordinated
/// [GroupConsumer]. No dependencies beyond the Dart SDK.
library;

export 'src/connection.dart'
    show
        ApiVersionRange,
        BrokerInfo,
        ClusterMetadata,
        Connection,
        PartitionInfo,
        Router,
        TopicInfo,
        defaultRequestTimeout;
export 'src/consumer.dart'
    show
        ConsumedRecord,
        Consumer,
        ConsumerConfig,
        FetchResult,
        TopicPartition,
        earliest,
        latest;
export 'src/group.dart'
    show
        Assignor,
        AutoOffsetReset,
        GroupConfig,
        GroupConsumer,
        rangeAssign,
        roundRobinAssign,
        stickyAssign;
export 'src/producer.dart' show Producer, ProducerConfig;
export 'src/protocol.dart'
    show
        ApiKey,
        BrahmaputraException,
        Codec,
        Compression,
        ErrorCode,
        NoOffsetForPartitionException,
        ProtocolException,
        Record,
        RecordBatch,
        RecordHeader,
        RequestTimeoutException,
        ServerException,
        crc32c,
        decodeRecordBatch,
        encodeRecordBatch,
        murmur2,
        partitionForKey,
        readCommitted,
        readUncommitted,
        registerCodec;
