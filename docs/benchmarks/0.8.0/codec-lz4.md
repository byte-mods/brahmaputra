# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 500000 records of 256 B, acks=1, batch.size=65536,
linger.ms=10, compression=lz4. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `false` on both producers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 465116.279070 | 320682 | 0.69x |
| Produce (MB/sec) | 113.55 | 78.29 | 0.69x |
| Consume (msgs/sec) | 369276.2186 | 330836 | 0.90x |
| Consume (MB/sec) | 90.1553 | 80.77 | 0.90x |

## Resource cost for the same stream

Broker-container CPU and memory sampled once a second for the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 243.8 / 243.8 | 103.1 / 103.1 |
| Produce memory MiB (avg / peak) | 483 / 483 | 6 / 6 |
| Consume CPU % (avg / peak) | 204.0 / 204.0 | 41.1 / 41.1 |
| Consume memory MiB (avg / peak) | 466 / 466 | 7 / 7 |
| Log directory bytes after produce | 275337879 | 834494 |
| Bytes on disk per record | 550.7 | 1.7 |
| Msgs/sec per CPU% (produce) | 1908 | 3110 |

Raw tool output and per-second samples: `bench/results/raw/`.
