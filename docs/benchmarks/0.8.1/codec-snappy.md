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
| Produce (msgs/sec) | 365497.076023 | 786275 | 2.15x |
| Produce (MB/sec) | 89.23 | 191.96 | 2.15x |
| Consume (msgs/sec) | 262881.1777 | 1335014 | 5.08x |
| Consume (MB/sec) | 64.1800 | 325.93 | 5.08x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 179.3 / 641.0 | 79.9 / 192.3 |
| Produce memory MiB (avg / peak) | 443.3 / 717.4 | 15.8 / 27.7 |
| Consume CPU % (avg / peak) | 129.7 / 599.2 | 34.0 / 109.5 |
| Consume memory MiB (avg / peak) | 521.2 / 707.0 | 59.2 / 288.8 |
| Log directory bytes after produce | 281102188 | 6599117 |
| Bytes on disk per record | 562.2 | 13.2 |
| Msgs/sec per CPU% (produce) | 2038 | 9841 |

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
| brahma-consume-stats.txt | 1.400 | 0.476 | 0.340 |
| brahma-produce-stats.txt | 1.510 | 1.207 | 0.799 |
| kafka-consume-stats.txt | 4.670 | 6.055 | 1.297 |
| kafka-produce-stats.txt | 4.140 | 7.422 | 1.793 |
