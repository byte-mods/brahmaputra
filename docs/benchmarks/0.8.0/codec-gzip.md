# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 500000 records of 256 B, acks=1, batch.size=65536,
linger.ms=10, compression=gzip. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `false` on both producers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 352609.308886 | 252862 | 0.72x |
| Produce (MB/sec) | 86.09 | 61.73 | 0.72x |
| Consume (msgs/sec) | 314070.3518 | 348579 | 1.11x |
| Consume (MB/sec) | 76.6773 | 85.10 | 1.11x |

## Resource cost for the same stream

Broker-container CPU and memory sampled once a second for the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 126.4 / 190.4 | 134.8 / 134.8 |
| Produce memory MiB (avg / peak) | 389 / 417 | 25 / 25 |
| Consume CPU % (avg / peak) | 200.3 / 200.3 | n/a / n/a |
| Consume memory MiB (avg / peak) | 463 / 463 | n/a / n/a |
| Log directory bytes after produce | 274460275 | 1134243 |
| Bytes on disk per record | 548.9 | 2.3 |
| Msgs/sec per CPU% (produce) | 2790 | 1876 |

Raw tool output and per-second samples: `bench/results/raw/`.
