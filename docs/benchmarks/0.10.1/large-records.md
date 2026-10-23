# Kafka vs Brahmaputra (TCP) vs Brahmaputra (QUIC)

2000 records of 1048576 B (1 MiB each), 6 partitions, RF=1, acks=1,
batch.size=2097152, linger.ms=5, compression=none. Each broker gets 4 CPUs
and 4g, and each system is driven by its own client from inside its
own container. Kafka image `apache/kafka:4.3.1`.

Producer idempotence is disabled on both systems.

Initial consumer-group rebalance delay is 0 ms on both brokers.

Kafka is also measured over its SSL listener, and Brahmaputra over TLS on
TCP (tcp-tls), the transport that matches it: kernel TCP with userspace
TLS. Encrypted QUIC and tcp-tls are each compared with Kafka over TLS;
plaintext Kafka is the comparator for plaintext TCP.

| Metric | Kafka | Brahmaputra TCP | Brahmaputra QUIC | Kafka TLS | Brahmaputra TLS |
|---|---|---|---|---|---|
| Produce msgs/sec | 186.532363 | 284 | 92 | 161.004669 | 215 |
| Produce MB/sec | 186.53 | 284.00 | 92.00 | 161.00 | 215.00 |
| Consume msgs/sec | 323.4153 | 879 | 87 | 158.0778 | 442 |
| Consume MB/sec | 323.4153 | 879.00 | 87.00 | 158.0778 | 442.00 |
| Produce CPU % avg / peak | 256.9 / 530.2 | 191.4 / 2084.4 | 300.9 / 554.8 | 253.7 / 771.5 | 154.8 / 539.2 |
| Produce memory MiB avg / peak | 1008.2 / 1758.1 | 269.2 / 368.0 | 334.9 / 534.2 | 3452.9 / 3990.1 | 233.2 / 360.0 |
| Consume CPU % avg / peak | 193.5 / 416.9 | 86.5 / 160.7 | 267.3 / 365.7 | 201.2 / 493.4 | 144.8 / 262.3 |
| Consume memory MiB avg / peak | 2219.4 / 3943.1 | 1188.7 / 2252.1 | 854.4 / 1417.7 | 3313.9 / 3683.5 | 848.4 / 1453.3 |
| Log bytes on disk | n/a | 2097229606 | 2097227880 | (shared broker) | 2097229859 |
| Disk bytes per record | n/a | 1048615 | 1048614 | (shared broker) | 1048615 |
| Produce msgs/sec per CPU% | 0.7 | 1.5 | 0.3 | 0.6 | 1.4 |

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
| kafka-consume-stats.txt | 11.000 | 21.281 | 1.935 |
| kafka-produce-stats.txt | 14.800 | 38.023 | 2.569 |
| kafka-tls-consume-stats.txt | 17.630 | 35.466 | 2.012 |
| kafka-tls-produce-stats.txt | 16.770 | 42.545 | 2.537 |
| quic-consume-stats.txt | 24.250 | 64.813 | 2.673 |
| quic-produce-stats.txt | 23.250 | 69.959 | 3.009 |
| tcp-consume-stats.txt | 3.740 | 3.235 | 0.865 |
| tcp-produce-stats.txt | 8.440 | 16.156 | 1.914 |
| tls-consume-stats.txt | 5.850 | 8.468 | 1.448 |
| tls-produce-stats.txt | 10.720 | 16.599 | 1.548 |
