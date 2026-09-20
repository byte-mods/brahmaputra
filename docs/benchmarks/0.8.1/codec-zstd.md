# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 500000 records of 256 B, acks=1, batch.size=65536,
linger.ms=10, compression=zstd. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `false` on both producers.

Initial consumer-group rebalance delay is 0 ms on both brokers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 473933.649289 | 876253 | 1.85x |
| Produce (MB/sec) | 115.71 | 213.93 | 1.85x |
| Consume (msgs/sec) | 403225.8065 | 1268518 | 3.15x |
| Consume (MB/sec) | 98.4438 | 309.70 | 3.15x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 164.2 / 629.2 | 76.7 / 190.2 |
| Produce memory MiB (avg / peak) | 437.3 / 639.8 | 15.0 / 28.8 |
| Consume CPU % (avg / peak) | 149.6 / 499.4 | 36.2 / 109.2 |
| Consume memory MiB (avg / peak) | 472.5 / 765.7 | 57.4 / 282.4 |
| Log directory bytes after produce | 273705334 | 216780 |
| Bytes on disk per record | 547.4 | 0.4 |
| Msgs/sec per CPU% (produce) | 2886 | 11424 |

Raw tool output, cgroup samples and CPU-time summaries: `bench/results/raw/`.

Wall-clock phase rates, where reported, use monotonic elapsed time.

CPU time comes from cumulative cgroup v2 counters; memory is working set
(memory.current minus inactive_file), sampled every 0.05 seconds.
Sampling brackets the complete client phase, including startup/exit dispatch
and the probe itself. Multi-node metrics use the common observation window.
CPU percentages are core-equivalents (100% = one busy core); peaks are
observed/interpolated sample peaks. CPU time compares total work, while
average CPU describes utilization during each system's own run.

| Phase artifact | Sampling window (s) | CPU time (core-seconds) | Average cores |
|---|---:|---:|---:|
| brahma-consume-stats.txt | 1.340 | 0.485 | 0.362 |
| brahma-produce-stats.txt | 1.400 | 1.074 | 0.767 |
| kafka-consume-stats.txt | 3.500 | 5.236 | 1.496 |
| kafka-produce-stats.txt | 3.530 | 5.796 | 1.642 |
