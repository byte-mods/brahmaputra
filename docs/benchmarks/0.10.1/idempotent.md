# Brahmaputra vs Kafka — head to head

Both systems in Docker on one host: 4 CPUs, 4g memory, replication
factor 1, 6 partitions, 50000 records of 256 B, acks=all, batch.size=65536,
linger.ms=10, compression=none. Kafka image `apache/kafka:4.3.1`.

Each system is driven by its own client (Kafka: kafka-*-perf-test;
Brahmaputra: brahmaputra-cli), so these are system+client numbers.

Both producers send identical repeated `x` payloads. These are
highly compressible; codec results do not represent high-entropy data.

Idempotence is `true` on both producers.

Initial consumer-group rebalance delay is 0 ms on both brokers.

| Workload | Kafka | Brahmaputra | Brahmaputra / Kafka |
|---|---|---|---|
| Produce (msgs/sec) | 41152.263374 | 193519 | 4.70x |
| Produce (MB/sec) | 10.05 | 47.25 | 4.70x |
| Consume (msgs/sec) | 21862.7022 | 603748 | 27.62x |
| Consume (MB/sec) | 5.3376 | 147.40 | 27.62x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 50000 records of 256 B (12 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 157.1 / 669.5 | 43.6 / 258.7 |
| Produce memory MiB (avg / peak) | 386.2 / 490.3 | 12.7 / 31.2 |
| Consume CPU % (avg / peak) | 160.1 / 534.3 | 18.2 / 109.7 |
| Consume memory MiB (avg / peak) | 427.9 / 514.5 | 12.5 / 26.3 |
| Log directory bytes after produce | 291256768 | 18415453 |
| Bytes on disk per record | 5825.1 | 368.3 |
| Msgs/sec per CPU% (produce) | 262 | 4439 |

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
| brahma-consume-stats.txt | 1.490 | 0.271 | 0.182 |
| brahma-produce-stats.txt | 1.530 | 0.667 | 0.436 |
| kafka-consume-stats.txt | 6.870 | 10.996 | 1.601 |
| kafka-produce-stats.txt | 6.020 | 9.455 | 1.571 |
