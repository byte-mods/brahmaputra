# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 500000 records of 256 B, acks=1, batch.size=65536,
linger.ms=10, compression=zstd. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `false` on both producers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 451671.183379 | 316877 | 0.70x |
| Produce (MB/sec) | 110.27 | 77.36 | 0.70x |
| Consume (msgs/sec) | 359195.4023 | 362152 | 1.01x |
| Consume (MB/sec) | 87.6942 | 88.42 | 1.01x |

## Resource cost for the same stream

Broker-container CPU and memory sampled once a second for the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 153.0 / 199.1 | 126.5 / 126.5 |
| Produce memory MiB (avg / peak) | 421 / 429 | 30 / 30 |
| Consume CPU % (avg / peak) | 194.3 / 194.3 | n/a / n/a |
| Consume memory MiB (avg / peak) | 487 / 487 | n/a / n/a |
| Log directory bytes after produce | 273705128 | 303852 |
| Bytes on disk per record | 547.4 | 0.6 |
| Msgs/sec per CPU% (produce) | 2952 | 2505 |

Raw tool output and per-second samples: `bench/results/raw/`.
