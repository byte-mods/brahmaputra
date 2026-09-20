# Kafka vs Brahmaputra (TCP) vs Brahmaputra (QUIC)

2000 records of 1048576 B (1 MiB each), 6 partitions, RF=1, acks=1,
batch.size=2097152, linger.ms=5, compression=none. Each broker gets 4 CPUs
and 4g, and each system is driven by its own client from inside its
own container. Kafka image `apache/kafka:4.3.1`.

Producer idempotence is disabled on both systems.

Initial consumer-group rebalance delay is 0 ms on both brokers.

| Metric | Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|---|
| Produce msgs/sec | 108.195834 | 137 | 144 |
| Produce MB/sec | 108.20 | 137.00 | 144.00 |
| Consume msgs/sec | 521.9207 | 425 | 209 |
| Consume MB/sec | 521.9207 | 425.00 | 209.00 |
| Produce CPU % avg / peak | 88.2 / 701.6 | 38.9 / 329.5 | 204.4 / 407.3 |
| Produce memory MiB avg / peak | 1334.9 / 1717.2 | 262.1 / 323.3 | 307.0 / 434.0 |
| Consume CPU % avg / peak | 144.0 / 321.1 | 30.7 / 137.2 | 219.0 / 319.9 |
| Consume memory MiB avg / peak | 2254.0 / 3910.8 | 497.0 / 2150.8 | 782.9 / 1401.9 |
| Log bytes on disk | n/a | 2097227627 | 2097226882 |
| Disk bytes per record | n/a | 1048614 | 1048613 |
| Produce msgs/sec per CPU% | 1.2 | 3.5 | 0.7 |

Raw output, cgroup samples and CPU-time summaries: `bench/results/three-way/`.

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
| kafka-consume-stats.txt | 6.290 | 9.060 | 1.440 |
| kafka-produce-stats.txt | 20.490 | 18.064 | 0.882 |
| quic-consume-stats.txt | 10.220 | 22.382 | 2.190 |
| quic-produce-stats.txt | 14.870 | 30.393 | 2.044 |
| tcp-consume-stats.txt | 5.470 | 1.679 | 0.307 |
| tcp-produce-stats.txt | 15.240 | 5.931 | 0.389 |
