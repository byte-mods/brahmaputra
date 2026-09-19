# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 500000 records of 256 B, acks=1, batch.size=65536,
linger.ms=10, compression=none. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `false` on both producers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 278706.800446 | 285639 | 1.02x |
| Produce (MB/sec) | 68.04 | 69.74 | 1.02x |
| Consume (msgs/sec) | 319488.8179 | 384730 | 1.20x |
| Consume (MB/sec) | 78.0002 | 93.93 | 1.20x |

## Resource cost for the same stream

Broker-container CPU and memory sampled once a second for the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 242.8 / 353.4 | 111.9 / 111.9 |
| Produce memory MiB (avg / peak) | 538 / 678 | 24 / 24 |
| Consume CPU % (avg / peak) | 207.2 / 207.2 | 25.1 / 25.1 |
| Consume memory MiB (avg / peak) | 529 / 529 | 65 / 65 |
| Log directory bytes after produce | 410949573 | 136470642 |
| Bytes on disk per record | 821.9 | 272.9 |
| Msgs/sec per CPU% (produce) | 1148 | 2553 |

Raw tool output and per-second samples: `bench/results/raw/`.
