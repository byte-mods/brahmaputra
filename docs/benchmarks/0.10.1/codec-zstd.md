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
| Produce (msgs/sec) | 176056.338028 | 284348 | 1.62x |
| Produce (MB/sec) | 42.98 | 69.42 | 1.62x |
| Consume (msgs/sec) | 163025.7581 | 821137 | 5.04x |
| Consume (MB/sec) | 39.8012 | 200.47 | 5.04x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 202.3 / 679.3 | 107.5 / 211.8 |
| Produce memory MiB (avg / peak) | 432.9 / 684.0 | 20.1 / 31.9 |
| Consume CPU % (avg / peak) | 191.8 / 555.0 | 44.1 / 117.1 |
| Consume memory MiB (avg / peak) | 466.5 / 727.5 | 64.6 / 304.6 |
| Log directory bytes after produce | 273717713 | 229084 |
| Bytes on disk per record | 547.4 | 0.5 |
| Msgs/sec per CPU% (produce) | 870 | 2645 |

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
| brahma-consume-stats.txt | 2.020 | 0.891 | 0.441 |
| brahma-produce-stats.txt | 3.070 | 3.301 | 1.075 |
| kafka-consume-stats.txt | 7.210 | 13.832 | 1.918 |
| kafka-produce-stats.txt | 7.400 | 14.971 | 2.023 |
