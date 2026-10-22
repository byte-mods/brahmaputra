# Replicated head-to-head: Kafka vs Brahmaputra at RF=3, acks=all

Three brokers per system on one host, 4 CPUs and 4g per container,
6 partitions, 256 B records, 500000 records per client at 1/2/4 concurrent
clients, batch.size=65536, linger.ms=5, compression=none. Kafka image `apache/kafka:4.3.1`.

Producer idempotence is disabled on both systems.

Initial consumer-group rebalance delay is 0 ms on both brokers.

Offered rate per producer: unlimited records/sec (unlimited means saturation).

Both clusters share one host, disk and NIC. Contention can affect
the systems differently; these observations are not deployment
capacity estimates or evidence of a universal throughput ratio.

Load generators run inside the broker containers, spread round-robin
across all three, so sampled CPU and memory cover broker plus client
for both systems and no one node carries all the client work.

### produce at RF=3, acks=all, min.insync.replicas=2

| Reading | Kafka | Brahmaputra |
|---|---|---|
| Clients at peak | 4 | 4 |
| msgs/sec (wall clock) | 119197 | 384098 |
| msgs/sec (client-measured) | 161113 | 497186 |
| CPU % avg, whole cluster | 781.9 | 795.2 |
| Memory MiB avg, whole cluster | 2662.2 | 820.8 |
| Ratio | 1.00x | 3.22x |

### consume at RF=3, acks=all, min.insync.replicas=2

| Reading | Kafka | Brahmaputra |
|---|---|---|
| Clients at peak | 4 | 4 |
| msgs/sec (wall clock) | 260010 | 1207001 |
| msgs/sec (client-measured) | 825274 | 3753357 |
| CPU % avg, whole cluster | 529.8 | 300.9 |
| Memory MiB avg, whole cluster | 2894.8 | 1069.1 |
| Ratio | 1.00x | 4.64x |

### produce: what replication costs each system

| System | RF=1 acks=1 | RF=3 acks=all | Kept | Cost |
|---|---|---|---|---|
| kafka | 188929 | 119197 | 63% | 1.59x |
| brahmaputra | 535475 | 384098 | 72% | 1.39x |

### consume: what replication costs each system

| System | RF=1 acks=1 | RF=3 acks=all | Kept | Cost |
|---|---|---|---|---|
| kafka | 266170 | 260010 | 98% | 1.02x |
| brahmaputra | 1160766 | 1207001 | 104% | 0.96x |

### Acknowledgement latency, produce (milliseconds)

| System | Config | Clients | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|
| kafka | rf3 | 1 | 2021.00 | 2404.00 | 2431.00 | 2462.00 |
| kafka | rf3 | 2 | 2437.00 | 3123.50 | 3161.50 | 3622.00 |
| kafka | rf3 | 4 | 3937.00 | 6580.00 | 6677.50 | 8946.00 |
| kafka | rf1 | 1 | 3.00 | 58.00 | 68.00 | 671.00 |
| kafka | rf1 | 2 | 3.00 | 51.00 | 59.50 | 777.00 |
| kafka | rf1 | 4 | 17.00 | 460.50 | 535.50 | 952.00 |
| brahmaputra | rf3 | 1 | 8.73 | 30.10 | 44.22 | 90.72 |
| brahmaputra | rf3 | 2 | 8.72 | 56.09 | 75.33 | 142.72 |
| brahmaputra | rf3 | 4 | 10.82 | 59.63 | 110.66 | 197.78 |
| brahmaputra | rf1 | 1 | 6.06 | 32.24 | 42.73 | 55.13 |
| brahmaputra | rf1 | 2 | 6.67 | 22.33 | 30.20 | 46.02 |
| brahmaputra | rf1 | 4 | 7.56 | 31.59 | 40.43 | 65.26 |

Kafka reports whole milliseconds, Brahmaputra two decimals; each
is parsed from its own client summary rather than recomputed. Both
measure the same span: record admitted to record acknowledged.

### Every level

`NA` means no valid resource sample completed during the workload.

| System | Config | Phase | Clients | Records | Seconds | msgs/sec | Client msgs/sec | CPU avg % | CPU peak % | Mem avg MiB | Mem peak MiB | p50 ms | p99 ms | p99.9 ms | max ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| kafka | rf3 | produce | 1 | 500000 | 10.32 | 48445 | 76057 | 543.5 | 1468.7 | 1266.0 | 1655.2 | 2021.00 | 2404.00 | 2431.00 | 2462.00 |
| kafka | rf3 | consume | 1 | 500000 | 6.79 | 73638 | 189681 | 275.3 | 1050.0 | 1405.3 | 1698.7 | 0 | 0 | 0 | 0 |
| kafka | rf3 | produce | 2 | 1000000 | 11.26 | 88810 | 139820 | 672.3 | 1780.8 | 1751.0 | 2305.4 | 2437.00 | 3123.50 | 3161.50 | 3622.00 |
| kafka | rf3 | consume | 2 | 1000000 | 6.11 | 163747 | 521408 | 318.3 | 889.4 | 1790.8 | 2296.4 | 0 | 0 | 0 | 0 |
| kafka | rf3 | produce | 4 | 2000000 | 16.78 | 119197 | 161113 | 781.9 | 2066.3 | 2662.2 | 3706.7 | 3937.00 | 6580.00 | 6677.50 | 8946.00 |
| kafka | rf3 | consume | 4 | 2000000 | 7.69 | 260010 | 825274 | 529.8 | 1498.5 | 2894.8 | 3859.0 | 0 | 0 | 0 | 0 |
| kafka | rf1 | produce | 1 | 500000 | 6.49 | 77018 | 183824 | 279.1 | 930.4 | 2632.2 | 2970.5 | 3.00 | 58.00 | 68.00 | 671.00 |
| kafka | rf1 | consume | 1 | 500000 | 6.29 | 79479 | 244260 | 180.0 | 578.3 | 2754.7 | 3078.4 | 0 | 0 | 0 | 0 |
| kafka | rf1 | produce | 2 | 1000000 | 7.08 | 141183 | 306045 | 448.5 | 1235.3 | 2924.0 | 3509.0 | 3.00 | 51.00 | 59.50 | 777.00 |
| kafka | rf1 | consume | 2 | 1000000 | 6.68 | 149589 | 437707 | 309.9 | 850.3 | 3116.9 | 3703.4 | 0 | 0 | 0 | 0 |
| kafka | rf1 | produce | 4 | 2000000 | 10.59 | 188929 | 387600 | 719.1 | 1916.6 | 3428.9 | 4045.4 | 17.00 | 460.50 | 535.50 | 952.00 |
| kafka | rf1 | consume | 4 | 2000000 | 7.51 | 266170 | 865322 | 558.9 | 1421.3 | 3659.6 | 4631.8 | 0 | 0 | 0 | 0 |
| brahmaputra | rf3 | produce | 1 | 500000 | 3.64 | 137438 | 194978 | 377.2 | 688.8 | 140.4 | 223.1 | 8.73 | 30.10 | 44.22 | 90.72 |
| brahmaputra | rf3 | consume | 1 | 500000 | 1.46 | 343171 | 858415 | 188.2 | 384.1 | 216.3 | 272.1 | 0 | 0 | 0 | 0 |
| brahmaputra | rf3 | produce | 2 | 1000000 | 3.92 | 255232 | 338029 | 591.1 | 1085.8 | 380.6 | 525.6 | 8.72 | 56.09 | 75.33 | 142.72 |
| brahmaputra | rf3 | consume | 2 | 1000000 | 1.41 | 707714 | 1682037 | 248.0 | 486.3 | 513.6 | 626.4 | 0 | 0 | 0 | 0 |
| brahmaputra | rf3 | produce | 4 | 2000000 | 5.21 | 384098 | 497186 | 795.2 | 1689.0 | 820.8 | 1098.1 | 10.82 | 59.63 | 110.66 | 197.78 |
| brahmaputra | rf3 | consume | 4 | 2000000 | 1.66 | 1207001 | 3753357 | 300.9 | 773.1 | 1069.1 | 1301.7 | 0 | 0 | 0 | 0 |
| brahmaputra | rf1 | produce | 1 | 500000 | 2.79 | 179533 | 256868 | 332.6 | 570.6 | 1055.7 | 1071.0 | 6.06 | 32.24 | 42.73 | 55.13 |
| brahmaputra | rf1 | consume | 1 | 500000 | 1.53 | 327439 | 853275 | 245.6 | 396.1 | 1086.5 | 1159.9 | 0 | 0 | 0 | 0 |
| brahmaputra | rf1 | produce | 2 | 1000000 | 3.09 | 323520 | 468885 | 482.4 | 794.3 | 1123.7 | 1150.5 | 6.67 | 22.33 | 30.20 | 46.02 |
| brahmaputra | rf1 | consume | 2 | 1000000 | 1.63 | 611995 | 1767168 | 294.8 | 517.6 | 1180.5 | 1317.2 | 0 | 0 | 0 | 0 |
| brahmaputra | rf1 | produce | 4 | 2000000 | 3.73 | 535475 | 795466 | 631.6 | 1284.1 | 1257.0 | 1312.7 | 7.56 | 31.59 | 40.43 | 65.26 |
| brahmaputra | rf1 | consume | 4 | 2000000 | 1.72 | 1160766 | 3489827 | 348.7 | 841.4 | 1340.1 | 1652.1 | 0 | 0 | 0 | 0 |

### Replication actually happened

```
kafka rep-1792712873-rf3-1-0: 6/6 partitions at ISR=3
kafka rep-1792712873-rf3-1-0: 6/6 partitions at ISR=3
kafka rep-1792712873-rf3-2-0: 6/6 partitions at ISR=3
kafka rep-1792712873-rf3-2-1: 6/6 partitions at ISR=3
kafka rep-1792712873-rf3-2-0: 6/6 partitions at ISR=3
kafka rep-1792712873-rf3-2-1: 6/6 partitions at ISR=3
kafka rep-1792712873-rf3-4-0: 6/6 partitions at ISR=3
kafka rep-1792712873-rf3-4-1: 6/6 partitions at ISR=3
kafka rep-1792712873-rf3-4-2: 6/6 partitions at ISR=3
kafka rep-1792712873-rf3-4-3: 6/6 partitions at ISR=3
kafka rep-1792712873-rf3-4-0: 6/6 partitions at ISR=3
kafka rep-1792712873-rf3-4-1: 6/6 partitions at ISR=3
kafka rep-1792712873-rf3-4-2: 6/6 partitions at ISR=3
kafka rep-1792712873-rf3-4-3: 6/6 partitions at ISR=3
kafka rep-1792712873-rf1-1-0: 6/6 partitions at ISR=1
kafka rep-1792712873-rf1-1-0: 6/6 partitions at ISR=1
kafka rep-1792712873-rf1-2-0: 6/6 partitions at ISR=1
kafka rep-1792712873-rf1-2-1: 6/6 partitions at ISR=1
kafka rep-1792712873-rf1-2-0: 6/6 partitions at ISR=1
kafka rep-1792712873-rf1-2-1: 6/6 partitions at ISR=1
kafka rep-1792712873-rf1-4-0: 6/6 partitions at ISR=1
kafka rep-1792712873-rf1-4-1: 6/6 partitions at ISR=1
kafka rep-1792712873-rf1-4-2: 6/6 partitions at ISR=1
kafka rep-1792712873-rf1-4-3: 6/6 partitions at ISR=1
kafka rep-1792712873-rf1-4-0: 6/6 partitions at ISR=1
kafka rep-1792712873-rf1-4-1: 6/6 partitions at ISR=1
kafka rep-1792712873-rf1-4-2: 6/6 partitions at ISR=1
kafka rep-1792712873-rf1-4-3: 6/6 partitions at ISR=1
brahmaputra rep-1792712873-rf3-1-0: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1792712873-rf3-2-0: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1792712873-rf3-2-1: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1792712873-rf3-4-0: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1792712873-rf3-4-1: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1792712873-rf3-4-2: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1792712873-rf3-4-3: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1792712873-rf1-1-0: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1792712873-rf1-2-0: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1792712873-rf1-2-1: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1792712873-rf1-4-0: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1792712873-rf1-4-1: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1792712873-rf1-4-2: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1792712873-rf1-4-3: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
```

Raw client output, cgroup samples and CPU-time summaries: `bench/results/replicated/`.

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
| brahmaputra-rf1-consume-1.stats | 2.330 | 5.722 | 2.456 |
| brahmaputra-rf1-consume-2.stats | 2.490 | 7.339 | 2.948 |
| brahmaputra-rf1-consume-4.stats | 3.080 | 10.740 | 3.487 |
| brahmaputra-rf1-produce-1.stats | 4.190 | 13.935 | 3.326 |
| brahmaputra-rf1-produce-2.stats | 3.920 | 18.912 | 4.824 |
| brahmaputra-rf1-produce-4.stats | 5.110 | 32.273 | 6.316 |
| brahmaputra-rf3-consume-1.stats | 2.810 | 5.289 | 1.882 |
| brahmaputra-rf3-consume-2.stats | 2.270 | 5.630 | 2.480 |
| brahmaputra-rf3-consume-4.stats | 3.070 | 9.239 | 3.009 |
| brahmaputra-rf3-produce-1.stats | 4.940 | 18.635 | 3.772 |
| brahmaputra-rf3-produce-2.stats | 4.800 | 28.374 | 5.911 |
| brahmaputra-rf3-produce-4.stats | 6.110 | 48.589 | 7.952 |
| kafka-rf1-consume-1.stats | 7.540 | 13.572 | 1.800 |
| kafka-rf1-consume-2.stats | 7.950 | 24.638 | 3.099 |
| kafka-rf1-consume-4.stats | 8.260 | 46.162 | 5.589 |
| kafka-rf1-produce-1.stats | 7.830 | 21.851 | 2.791 |
| kafka-rf1-produce-2.stats | 8.510 | 38.171 | 4.485 |
| kafka-rf1-produce-4.stats | 11.980 | 86.145 | 7.191 |
| kafka-rf3-consume-1.stats | 7.680 | 21.146 | 2.753 |
| kafka-rf3-consume-2.stats | 7.500 | 23.869 | 3.183 |
| kafka-rf3-consume-4.stats | 9.060 | 48.000 | 5.298 |
| kafka-rf3-produce-1.stats | 11.190 | 60.818 | 5.435 |
| kafka-rf3-produce-2.stats | 12.490 | 83.970 | 6.723 |
| kafka-rf3-produce-4.stats | 18.110 | 141.595 | 7.819 |
