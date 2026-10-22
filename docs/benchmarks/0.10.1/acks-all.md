# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 500000 records of 256 B, acks=all, batch.size=65536,
linger.ms=10, compression=none. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `false` on both producers.

Initial consumer-group rebalance delay is 0 ms on both brokers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 162179.695102 | 284478 | 1.75x |
| Produce (MB/sec) | 39.59 | 69.45 | 1.75x |
| Consume (msgs/sec) | 178062.6781 | 1136103 | 6.38x |
| Consume (MB/sec) | 43.4723 | 277.37 | 6.38x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 216.1 / 691.6 | 119.8 / 254.3 |
| Produce memory MiB (avg / peak) | 461.2 / 768.2 | 21.9 / 31.8 |
| Consume CPU % (avg / peak) | 172.7 / 604.7 | 42.4 / 136.5 |
| Consume memory MiB (avg / peak) | 497.3 / 638.5 | 44.2 / 91.4 |
| Log directory bytes after produce | 410951787 | 136421412 |
| Bytes on disk per record | 821.9 | 272.8 |
| Msgs/sec per CPU% (produce) | 750 | 2375 |

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
| brahma-consume-stats.txt | 1.710 | 0.725 | 0.424 |
| brahma-produce-stats.txt | 3.130 | 3.748 | 1.198 |
| kafka-consume-stats.txt | 7.600 | 13.123 | 1.727 |
| kafka-produce-stats.txt | 7.850 | 16.966 | 2.161 |
