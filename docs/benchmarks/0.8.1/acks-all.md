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
| Produce (msgs/sec) | 416666.666667 | 587426 | 1.41x |
| Produce (MB/sec) | 101.73 | 143.41 | 1.41x |
| Consume (msgs/sec) | 397140.5878 | 1747319 | 4.40x |
| Consume (MB/sec) | 96.9582 | 426.59 | 4.40x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 182.9 / 591.2 | 94.4 / 204.8 |
| Produce memory MiB (avg / peak) | 461.3 / 676.3 | 19.5 / 32.4 |
| Consume CPU % (avg / peak) | 130.3 / 447.0 | 29.8 / 119.5 |
| Consume memory MiB (avg / peak) | 539.7 / 758.3 | 39.3 / 95.0 |
| Log directory bytes after produce | 410949450 | 136424742 |
| Bytes on disk per record | 821.9 | 272.8 |
| Msgs/sec per CPU% (produce) | 2278 | 6223 |

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
| brahma-consume-stats.txt | 1.190 | 0.354 | 0.298 |
| brahma-produce-stats.txt | 1.820 | 1.718 | 0.944 |
| kafka-consume-stats.txt | 3.860 | 5.028 | 1.303 |
| kafka-produce-stats.txt | 3.600 | 6.586 | 1.829 |
