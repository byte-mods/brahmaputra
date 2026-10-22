# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 500000 records of 256 B, acks=1, batch.size=65536,
linger.ms=10, compression=none. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `false` on both producers.

Initial consumer-group rebalance delay is 0 ms on both brokers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 163398.692810 | 289614 | 1.77x |
| Produce (MB/sec) | 39.89 | 70.71 | 1.77x |
| Consume (msgs/sec) | 173550.8504 | 1068152 | 6.15x |
| Consume (MB/sec) | 42.3708 | 260.78 | 6.15x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 210.5 / 710.8 | 120.1 / 239.6 |
| Produce memory MiB (avg / peak) | 455.6 / 670.9 | 21.7 / 33.5 |
| Consume CPU % (avg / peak) | 165.9 / 453.1 | 42.5 / 137.9 |
| Consume memory MiB (avg / peak) | 503.4 / 716.1 | 45.9 / 98.7 |
| Log directory bytes after produce | 410951424 | 136422342 |
| Bytes on disk per record | 821.9 | 272.8 |
| Msgs/sec per CPU% (produce) | 776 | 2411 |

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
| brahma-consume-stats.txt | 1.800 | 0.764 | 0.425 |
| brahma-produce-stats.txt | 3.050 | 3.662 | 1.201 |
| kafka-consume-stats.txt | 7.620 | 12.643 | 1.659 |
| kafka-produce-stats.txt | 7.800 | 16.421 | 2.105 |
