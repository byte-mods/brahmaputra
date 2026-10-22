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
| Produce (msgs/sec) | 188679.245283 | 331279 | 1.76x |
| Produce (MB/sec) | 46.06 | 80.88 | 1.76x |
| Consume (msgs/sec) | 171821.3058 | 1132048 | 6.59x |
| Consume (MB/sec) | 41.9486 | 276.38 | 6.59x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 202.3 / 646.5 | 114.8 / 224.6 |
| Produce memory MiB (avg / peak) | 437.3 / 641.8 | 22.5 / 34.2 |
| Consume CPU % (avg / peak) | 173.7 / 454.7 | 38.9 / 128.7 |
| Consume memory MiB (avg / peak) | 516.4 / 749.1 | 41.7 / 85.0 |
| Log directory bytes after produce | 410951393 | 136430137 |
| Bytes on disk per record | 821.9 | 272.9 |
| Msgs/sec per CPU% (produce) | 933 | 2886 |

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
| brahma-consume-stats.txt | 1.800 | 0.700 | 0.389 |
| brahma-produce-stats.txt | 2.790 | 3.202 | 1.148 |
| kafka-consume-stats.txt | 7.550 | 13.111 | 1.737 |
| kafka-produce-stats.txt | 7.420 | 15.012 | 2.023 |
