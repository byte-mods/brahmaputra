# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 50000 records of 256 B, acks=all, batch.size=65536,
linger.ms=10, compression=none. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `true` on both producers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 96525.096525 | 47770 | 0.49x |
| Produce (MB/sec) | 23.57 | 11.66 | 0.49x |
| Consume (msgs/sec) | 54406.9641 | 42977 | 0.79x |
| Consume (MB/sec) | 13.2830 | 10.49 | 0.79x |

## Resource cost for the same stream

Broker-container CPU and memory sampled once a second for the
duration of each phase; disk is the on-disk size of the log
directory after producing 50000 records of 256 B (12 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 255.4 / 255.4 | 114.6 / 114.6 |
| Produce memory MiB (avg / peak) | 488 / 488 | 24 / 24 |
| Consume CPU % (avg / peak) | 184.8 / 184.8 | 5.1 / 5.1 |
| Consume memory MiB (avg / peak) | 443 / 443 | 13 / 13 |
| Log directory bytes after produce | 291254636 | 18510219 |
| Bytes on disk per record | 5825.1 | 370.2 |
| Msgs/sec per CPU% (produce) | 378 | 417 |

Raw tool output and per-second samples: `bench/results/raw/`.
