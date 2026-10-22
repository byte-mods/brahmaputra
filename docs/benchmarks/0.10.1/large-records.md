# Kafka vs Brahmaputra (TCP) vs Brahmaputra (QUIC)

2000 records of 1048576 B (1 MiB each), 6 partitions, RF=1, acks=1,
batch.size=2097152, linger.ms=5, compression=none. Each broker gets 4 CPUs
and 4g, and each system is driven by its own client from inside its
own container. Kafka image `apache/kafka:4.3.1`.

Producer idempotence is disabled on both systems.

Initial consumer-group rebalance delay is 0 ms on both brokers.

| Metric | Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|---|
| Produce msgs/sec | 180.277628 | 242 | 92 |
| Produce MB/sec | 180.28 | 242.00 | 92.00 |
| Consume msgs/sec | 320.0512 | 898 | 85 |
| Consume MB/sec | 320.0512 | 898.00 | 85.00 |
| Produce CPU % avg / peak | 254.2 / 558.8 | 135.9 / 472.8 | 301.7 / 501.3 |
| Produce memory MiB avg / peak | 1008.1 / 1763.5 | 266.7 / 390.9 | 315.9 / 496.7 |
| Consume CPU % avg / peak | 200.1 / 476.6 | 88.1 / 157.8 | 272.4 / 364.1 |
| Consume memory MiB avg / peak | 2219.3 / 3935.1 | 1190.3 / 2242.4 | 811.9 / 1369.7 |
| Log bytes on disk | n/a | 2097228816 | 2097227597 |
| Disk bytes per record | n/a | 1048614 | 1048614 |
| Produce msgs/sec per CPU% | 0.7 | 1.8 | 0.3 |

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
| kafka-consume-stats.txt | 11.000 | 22.014 | 2.001 |
| kafka-produce-stats.txt | 15.160 | 38.534 | 2.542 |
| quic-consume-stats.txt | 24.730 | 67.368 | 2.724 |
| quic-produce-stats.txt | 23.110 | 69.718 | 3.017 |
| tcp-consume-stats.txt | 3.620 | 3.190 | 0.881 |
| tcp-produce-stats.txt | 9.560 | 12.996 | 1.359 |
