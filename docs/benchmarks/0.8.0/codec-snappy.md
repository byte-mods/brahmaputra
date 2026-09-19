# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 500000 records of 256 B, acks=1, batch.size=65536,
linger.ms=10, compression=snappy. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `false` on both producers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 443655.723159 | 279991 | 0.63x |
| Produce (MB/sec) | 108.31 | 68.36 | 0.63x |
| Consume (msgs/sec) | 322164.9485 | 366155 | 1.14x |
| Consume (MB/sec) | 78.6536 | 89.39 | 1.14x |

## Resource cost for the same stream

Broker-container CPU and memory sampled once a second for the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 155.8 / 200.2 | 137.1 / 137.1 |
| Produce memory MiB (avg / peak) | 448 / 451 | 26 / 26 |
| Consume CPU % (avg / peak) | 172.6 / 172.6 | 14.5 / 14.5 |
| Consume memory MiB (avg / peak) | 513 / 513 | 207 / 207 |
| Log directory bytes after produce | 281101986 | 6676877 |
| Bytes on disk per record | 562.2 | 13.4 |
| Msgs/sec per CPU% (produce) | 2848 | 2042 |

Raw tool output and per-second samples: `bench/results/raw/`.
