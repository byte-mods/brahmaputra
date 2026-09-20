# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 50000 records of 256 B, acks=all, batch.size=65536,
linger.ms=10, compression=none. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `true` on both producers.

Initial consumer-group rebalance delay is 0 ms on both brokers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 38022.813688 | 446699 | 11.75x |
| Produce (MB/sec) | 9.28 | 109.06 | 11.75x |
| Consume (msgs/sec) | 4644.6818 | 390042 | 83.98x |
| Consume (MB/sec) | 1.1340 | 95.23 | 83.98x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 50000 records of 256 B (12 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 160.1 / 579.9 | 30.9 / 246.7 |
| Produce memory MiB (avg / peak) | 399.8 / 487.4 | 9.5 / 26.5 |
| Consume CPU % (avg / peak) | 57.9 / 562.1 | 13.6 / 78.3 |
| Consume memory MiB (avg / peak) | 445.4 / 515.2 | 11.8 / 35.9 |
| Log directory bytes after produce | 291255383 | 18414167 |
| Bytes on disk per record | 5825.1 | 368.3 |
| Msgs/sec per CPU% (produce) | 237 | 14456 |

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
| brahma-consume-stats.txt | 0.990 | 0.135 | 0.136 |
| brahma-produce-stats.txt | 1.090 | 0.337 | 0.309 |
| kafka-consume-stats.txt | 14.760 | 8.551 | 0.579 |
| kafka-produce-stats.txt | 5.640 | 9.032 | 1.601 |
