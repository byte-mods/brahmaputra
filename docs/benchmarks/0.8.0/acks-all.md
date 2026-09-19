# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 500000 records of 256 B, acks=all, batch.size=65536,
linger.ms=10, compression=none. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `false` on both producers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 413223.140496 | 308815 | 0.75x |
| Produce (MB/sec) | 100.88 | 75.39 | 0.75x |
| Consume (msgs/sec) | 331785.0033 | 398328 | 1.20x |
| Consume (MB/sec) | 81.0022 | 97.25 | 1.20x |

## Resource cost for the same stream

Broker-container CPU and memory sampled once a second for the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 272.3 / 272.3 | 141.2 / 141.2 |
| Produce memory MiB (avg / peak) | 532 / 532 | 11 / 11 |
| Consume CPU % (avg / peak) | 121.2 / 185.8 | 24.0 / 24.0 |
| Consume memory MiB (avg / peak) | 630 / 759 | 68 / 68 |
| Log directory bytes after produce | 410949125 | 136468127 |
| Bytes on disk per record | 821.9 | 272.9 |
| Msgs/sec per CPU% (produce) | 1518 | 2187 |

Raw tool output and per-second samples: `bench/results/raw/`.
