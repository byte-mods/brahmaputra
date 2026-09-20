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
| Produce (msgs/sec) | 405515.004055 | 853078 | 2.10x |
| Produce (MB/sec) | 99.00 | 208.27 | 2.10x |
| Consume (msgs/sec) | 307881.7734 | 1799533 | 5.84x |
| Consume (MB/sec) | 75.1664 | 439.34 | 5.84x |

## Resource cost for the same stream

Broker-container cumulative CPU and 50 ms memory samples cover the
duration of each phase; disk is the on-disk size of the log
directory after producing 500000 records of 256 B (122 MiB of payload).

| Metric | Kafka | Brahmaputra |
|---|---|---|
| Produce CPU % (avg / peak of one core-equivalent) | 181.2 / 676.2 | 87.9 / 215.4 |
| Produce memory MiB (avg / peak) | 456.4 / 673.8 | 17.7 / 30.2 |
| Consume CPU % (avg / peak) | 144.1 / 449.4 | 30.7 / 123.0 |
| Consume memory MiB (avg / peak) | 547.8 / 768.9 | 41.0 / 103.2 |
| Log directory bytes after produce | 410949299 | 136418672 |
| Bytes on disk per record | 821.9 | 272.8 |
| Msgs/sec per CPU% (produce) | 2238 | 9705 |

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
| brahma-consume-stats.txt | 1.140 | 0.349 | 0.307 |
| brahma-produce-stats.txt | 1.400 | 1.231 | 0.879 |
| kafka-consume-stats.txt | 4.070 | 5.865 | 1.441 |
| kafka-produce-stats.txt | 3.640 | 6.597 | 1.812 |
