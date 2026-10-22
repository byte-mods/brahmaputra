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
| Produce (msgs/sec) | 221533.008418 | 319818 | 1.44x |
| Produce (MB/sec) | 54.09 | 78.08 | 1.44x |
| Consume (msgs/sec) | 182548.3753 | 767533 | 4.20x |
| Consume (MB/sec) | 44.5675 | 187.39 | 4.20x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 188.6 / 681.1 | 104.1 / 216.3 |
| Produce memory MiB (avg / peak) | 424.3 / 630.8 | 18.0 / 28.6 |
| Consume CPU % (avg / peak) | 184.6 / 446.8 | 49.6 / 127.3 |
| Consume memory MiB (avg / peak) | 453.9 / 693.3 | 66.5 / 279.8 |
| Log directory bytes after produce | 275341362 | 763747 |
| Bytes on disk per record | 550.7 | 1.5 |
| Msgs/sec per CPU% (produce) | 1175 | 3072 |

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
| brahma-consume-stats.txt | 1.970 | 0.977 | 0.496 |
| brahma-produce-stats.txt | 2.850 | 2.967 | 1.041 |
| kafka-consume-stats.txt | 7.030 | 12.980 | 1.846 |
| kafka-produce-stats.txt | 7.100 | 13.388 | 1.886 |
