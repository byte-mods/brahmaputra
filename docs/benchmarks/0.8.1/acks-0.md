# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 500000 records of 256 B, acks=0, batch.size=65536,
linger.ms=10, compression=none. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `false` on both producers.

Initial consumer-group rebalance delay is 0 ms on both brokers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 451671.183379 | 803559 | 1.78x |
| Produce (MB/sec) | 110.27 | 196.18 | 1.78x |
| Consume (msgs/sec) | 367376.9287 | 1988244 | 5.41x |
| Consume (MB/sec) | 89.6916 | 485.41 | 5.41x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 173.9 / 711.5 | 78.0 / 188.7 |
| Produce memory MiB (avg / peak) | 452.1 / 672.7 | 16.7 / 28.7 |
| Consume CPU % (avg / peak) | 126.5 / 420.4 | 28.1 / 121.4 |
| Consume memory MiB (avg / peak) | 539.8 / 766.9 | 35.7 / 90.9 |
| Log directory bytes after produce | 410949321 | 136420377 |
| Bytes on disk per record | 821.9 | 272.8 |
| Msgs/sec per CPU% (produce) | 2597 | 10302 |

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
| brahma-consume-stats.txt | 1.130 | 0.317 | 0.281 |
| brahma-produce-stats.txt | 1.500 | 1.170 | 0.780 |
| kafka-consume-stats.txt | 4.120 | 5.214 | 1.265 |
| kafka-produce-stats.txt | 3.590 | 6.244 | 1.739 |
