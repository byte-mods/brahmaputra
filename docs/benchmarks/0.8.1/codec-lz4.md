# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 500000 records of 256 B, acks=1, batch.size=65536,
linger.ms=10, compression=lz4. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `false` on both producers.

Initial consumer-group rebalance delay is 0 ms on both brokers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 600961.538462 | 833140 | 1.39x |
| Produce (MB/sec) | 146.72 | 203.40 | 1.39x |
| Consume (msgs/sec) | 416666.6667 | 1440684 | 3.46x |
| Consume (MB/sec) | 101.7253 | 351.73 | 3.46x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 157.0 / 519.5 | 64.1 / 185.0 |
| Produce memory MiB (avg / peak) | 433.0 / 692.2 | 13.3 / 27.8 |
| Consume CPU % (avg / peak) | 146.5 / 457.8 | 32.4 / 107.5 |
| Consume memory MiB (avg / peak) | 458.4 / 666.0 | 54.4 / 283.8 |
| Log directory bytes after produce | 275335986 | 750909 |
| Bytes on disk per record | 550.7 | 1.5 |
| Msgs/sec per CPU% (produce) | 3828 | 12998 |

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
| brahma-consume-stats.txt | 1.290 | 0.418 | 0.324 |
| brahma-produce-stats.txt | 1.760 | 1.129 | 0.641 |
| kafka-consume-stats.txt | 3.500 | 5.128 | 1.465 |
| kafka-produce-stats.txt | 3.220 | 5.057 | 1.570 |
