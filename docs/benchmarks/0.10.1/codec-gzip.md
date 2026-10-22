# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 500000 records of 256 B, acks=1, batch.size=65536,
linger.ms=10, compression=gzip. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `false` on both producers.

Initial consumer-group rebalance delay is 0 ms on both brokers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 162866.449511 | 188804 | 1.16x |
| Produce (MB/sec) | 39.76 | 46.09 | 1.16x |
| Consume (msgs/sec) | 168236.8775 | 748089 | 4.45x |
| Consume (MB/sec) | 41.0735 | 182.64 | 4.45x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 194.9 / 643.6 | 114.2 / 194.3 |
| Produce memory MiB (avg / peak) | 437.1 / 650.3 | 20.7 / 28.5 |
| Consume CPU % (avg / peak) | 177.4 / 426.4 | 44.9 / 121.3 |
| Consume memory MiB (avg / peak) | 465.5 / 757.5 | 69.5 / 306.4 |
| Log directory bytes after produce | 274498986 | 1050170 |
| Bytes on disk per record | 549.0 | 2.1 |
| Msgs/sec per CPU% (produce) | 836 | 1653 |

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
| brahma-consume-stats.txt | 2.130 | 0.957 | 0.449 |
| brahma-produce-stats.txt | 3.940 | 4.500 | 1.142 |
| kafka-consume-stats.txt | 7.280 | 12.913 | 1.774 |
| kafka-produce-stats.txt | 7.640 | 14.891 | 1.949 |
