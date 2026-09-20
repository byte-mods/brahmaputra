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
| Produce (msgs/sec) | 400641.025641 | 510529 | 1.27x |
| Produce (MB/sec) | 97.81 | 124.64 | 1.27x |
| Consume (msgs/sec) | 393700.7874 | 1429504 | 3.63x |
| Consume (MB/sec) | 96.1184 | 349.00 | 3.63x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 162.4 / 576.3 | 89.2 / 177.8 |
| Produce memory MiB (avg / peak) | 442.8 / 647.0 | 17.5 / 27.8 |
| Consume CPU % (avg / peak) | 141.3 / 405.0 | 32.3 / 107.7 |
| Consume memory MiB (avg / peak) | 463.3 / 710.1 | 47.1 / 284.1 |
| Log directory bytes after produce | 274455881 | 1051742 |
| Bytes on disk per record | 548.9 | 2.1 |
| Msgs/sec per CPU% (produce) | 2467 | 5723 |

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
| brahma-consume-stats.txt | 1.300 | 0.420 | 0.323 |
| brahma-produce-stats.txt | 1.810 | 1.614 | 0.892 |
| kafka-consume-stats.txt | 3.760 | 5.313 | 1.413 |
| kafka-produce-stats.txt | 3.730 | 6.056 | 1.624 |
