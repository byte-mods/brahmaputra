# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 500000 records of 256 B, acks=0, batch.size=65536,
linger.ms=10, compression=none. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `false` on both producers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 448028.673835 | 335593 | 0.75x |
| Produce (MB/sec) | 109.38 | 81.93 | 0.75x |
| Consume (msgs/sec) | 396825.3968 | 378231 | 0.95x |
| Consume (MB/sec) | 96.8812 | 92.34 | 0.95x |

## Resource cost for the same stream

Broker-container CPU and memory sampled once a second for the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 120.7 / 174.0 | 115.2 / 115.2 |
| Produce memory MiB (avg / peak) | 442 / 455 | 15 / 15 |
| Consume CPU % (avg / peak) | 205.1 / 205.1 | n/a / n/a |
| Consume memory MiB (avg / peak) | 534 / 534 | n/a / n/a |
| Log directory bytes after produce | 410949063 | 136469747 |
| Bytes on disk per record | 821.9 | 272.9 |
| Msgs/sec per CPU% (produce) | 3712 | 2913 |

Raw tool output and per-second samples: `bench/results/raw/`.
