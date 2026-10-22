# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 500000 records of 256 B, acks=1, batch.size=65536,
linger.ms=10, compression=snappy. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `false` on both producers.

Initial consumer-group rebalance delay is 0 ms on both brokers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 184842.883549 | 321073 | 1.74x |
| Produce (MB/sec) | 45.13 | 78.39 | 1.74x |
| Consume (msgs/sec) | 181818.1818 | 853540 | 4.69x |
| Consume (MB/sec) | 44.3892 | 208.38 | 4.69x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 199.5 / 586.7 | 105.4 / 216.6 |
| Produce memory MiB (avg / peak) | 434.4 / 695.8 | 18.4 / 28.0 |
| Consume CPU % (avg / peak) | 177.4 / 513.0 | 45.3 / 117.7 |
| Consume memory MiB (avg / peak) | 488.5 / 685.5 | 66.8 / 285.5 |
| Log directory bytes after produce | 281106128 | 6610268 |
| Bytes on disk per record | 562.2 | 13.2 |
| Msgs/sec per CPU% (produce) | 927 | 3046 |

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
| brahma-consume-stats.txt | 1.870 | 0.847 | 0.453 |
| brahma-produce-stats.txt | 2.930 | 3.088 | 1.054 |
| kafka-consume-stats.txt | 7.430 | 13.184 | 1.774 |
| kafka-produce-stats.txt | 7.470 | 14.905 | 1.995 |
