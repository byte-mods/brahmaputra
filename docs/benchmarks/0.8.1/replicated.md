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
| msgs/sec (wall clock) | 235516 | 757576 |
| msgs/sec (client-measured) | 324641 | 1118540 |
| CPU % avg, whole cluster | 800.6 | 404.0 |
| Memory MiB avg, whole cluster | 2794.9 | 693.2 |
| Ratio | 1.00x | 3.22x |

### consume at RF=3, acks=all, min.insync.replicas=2

| Reading | Kafka | Brahmaputra |
|---|---|---|
| Clients at peak | 4 | 4 |
| msgs/sec (wall clock) | 439657 | 1403509 |
| msgs/sec (client-measured) | 1676449 | 4684019 |
| CPU % avg, whole cluster | 449.5 | 113.9 |
| Memory MiB avg, whole cluster | 3131.3 | 1064.6 |
| Ratio | 1.00x | 3.19x |

### produce: what replication costs each system

| System | RF=1 acks=1 | RF=3 acks=all | Kept | Cost |
|---|---|---|---|---|
| kafka | 308024 | 235516 | 76% | 1.31x |
| brahmaputra | 804829 | 757576 | 94% | 1.06x |

### consume: what replication costs each system

| System | RF=1 acks=1 | RF=3 acks=all | Kept | Cost |
|---|---|---|---|---|
| kafka | 323887 | 439657 | 136% | 0.74x |
| brahmaputra | 1237624 | 1403509 | 113% | 0.88x |

### Acknowledgement latency, produce (milliseconds)

| System | Config | Clients | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|
| kafka | rf3 | 1 | 1030.00 | 1261.00 | 1271.00 | 1274.00 |
| kafka | rf3 | 2 | 1123.50 | 1509.50 | 1550.50 | 1770.00 |
| kafka | rf3 | 4 | 2364.25 | 3349.25 | 3380.50 | 4327.00 |
| kafka | rf1 | 1 | 1.00 | 44.00 | 46.00 | 236.00 |
| kafka | rf1 | 2 | 2.00 | 38.00 | 50.50 | 335.00 |
| kafka | rf1 | 4 | 24.50 | 186.75 | 228.25 | 721.00 |
| brahmaputra | rf3 | 1 | 3.51 | 17.64 | 216.58 | 218.29 |
| brahmaputra | rf3 | 2 | 3.00 | 55.12 | 951.97 | 1018.49 |
| brahmaputra | rf3 | 4 | 5.47 | 34.93 | 90.46 | 117.69 |
| brahmaputra | rf1 | 1 | 3.90 | 10.32 | 16.48 | 31.25 |
| brahmaputra | rf1 | 2 | 3.23 | 12.84 | 26.20 | 42.35 |
| brahmaputra | rf1 | 4 | 4.11 | 16.35 | 28.32 | 44.97 |

Kafka reports whole milliseconds, Brahmaputra two decimals; each
is parsed from its own client summary rather than recomputed. Both
measure the same span: record admitted to record acknowledged.

### Every level

`NA` means no valid resource sample completed during the workload.

| System | Config | Phase | Clients | Records | Seconds | msgs/sec | Client msgs/sec | CPU avg % | CPU peak % | Mem avg MiB | Mem peak MiB | p50 ms | p99 ms | p99.9 ms | max ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| kafka | rf3 | produce | 1 | 500000 | 5.26 | 95003 | 150693 | 497.2 | 1387.3 | 1287.8 | 1680.1 | 1030.00 | 1261.00 | 1271.00 | 1274.00 |
| kafka | rf3 | consume | 1 | 500000 | 5.22 | 95785 | 185254 | 165.0 | 754.8 | 1433.3 | 1739.6 | 0 | 0 | 0 | 0 |
| kafka | rf3 | produce | 2 | 1000000 | 6.14 | 162893 | 250171 | 679.8 | 1825.7 | 1808.2 | 2321.7 | 1123.50 | 1509.50 | 1550.50 | 1770.00 |
| kafka | rf3 | consume | 2 | 1000000 | 3.95 | 252908 | 889742 | 269.9 | 903.6 | 1835.1 | 2324.7 | 0 | 0 | 0 | 0 |
| kafka | rf3 | produce | 4 | 2000000 | 8.49 | 235516 | 324641 | 800.6 | 1975.3 | 2794.9 | 3991.7 | 2364.25 | 3349.25 | 3380.50 | 4327.00 |
| kafka | rf3 | consume | 4 | 2000000 | 4.55 | 439657 | 1676449 | 449.5 | 1252.5 | 3131.3 | 4126.4 | 0 | 0 | 0 | 0 |
| kafka | rf1 | produce | 1 | 500000 | 2.96 | 168634 | 459559 | 218.0 | 791.0 | 2812.6 | 3118.3 | 1.00 | 44.00 | 46.00 | 236.00 |
| kafka | rf1 | consume | 1 | 500000 | 3.18 | 157085 | 643501 | 129.8 | 426.4 | 2953.5 | 3223.3 | 0 | 0 | 0 | 0 |
| kafka | rf1 | produce | 2 | 1000000 | 3.86 | 258799 | 596779 | 432.2 | 1364.4 | 3205.7 | 3854.9 | 2.00 | 38.00 | 50.50 | 335.00 |
| kafka | rf1 | consume | 2 | 1000000 | 3.73 | 268168 | 1121226 | 248.0 | 818.7 | 3616.8 | 4135.2 | 0 | 0 | 0 | 0 |
| kafka | rf1 | produce | 4 | 2000000 | 6.49 | 308024 | 710071 | 645.4 | 1965.5 | 4187.1 | 5041.9 | 24.50 | 186.75 | 228.25 | 721.00 |
| kafka | rf1 | consume | 4 | 2000000 | 6.17 | 323887 | 1044002 | 339.9 | 1235.7 | 4799.5 | 6007.9 | 0 | 0 | 0 | 0 |
| brahmaputra | rf3 | produce | 1 | 500000 | 1.96 | 255624 | 357123 | 165.3 | 488.6 | 132.0 | 211.1 | 3.51 | 17.64 | 216.58 | 218.29 |
| brahmaputra | rf3 | consume | 1 | 500000 | 1.81 | 275634 | 398396 | 40.0 | 151.6 | 206.6 | 271.3 | 0 | 0 | 0 | 0 |
| brahmaputra | rf3 | produce | 2 | 1000000 | 2.88 | 347222 | 476007 | 195.3 | 817.8 | 339.3 | 486.9 | 3.00 | 55.12 | 951.97 | 1018.49 |
| brahmaputra | rf3 | consume | 2 | 1000000 | 1.13 | 888099 | 1960290 | 74.7 | 268.1 | 499.3 | 605.4 | 0 | 0 | 0 | 0 |
| brahmaputra | rf3 | produce | 4 | 2000000 | 2.64 | 757576 | 1118540 | 404.0 | 1325.5 | 693.2 | 1055.0 | 5.47 | 34.93 | 90.46 | 117.69 |
| brahmaputra | rf3 | consume | 4 | 2000000 | 1.43 | 1403509 | 4684019 | 113.9 | 406.6 | 1064.6 | 1218.4 | 0 | 0 | 0 | 0 |
| brahmaputra | rf1 | produce | 1 | 500000 | 1.87 | 267666 | 443690 | 125.0 | 309.4 | 1038.7 | 1052.7 | 3.90 | 10.32 | 16.48 | 31.25 |
| brahmaputra | rf1 | consume | 1 | 500000 | 1.24 | 402901 | 893383 | 58.2 | 168.3 | 1072.3 | 1155.6 | 0 | 0 | 0 | 0 |
| brahmaputra | rf1 | produce | 2 | 1000000 | 1.66 | 602047 | 1004677 | 211.1 | 516.7 | 1102.9 | 1132.0 | 3.23 | 12.84 | 26.20 | 42.35 |
| brahmaputra | rf1 | consume | 2 | 1000000 | 2.00 | 498753 | 1711896 | 53.1 | 282.1 | 1126.0 | 1330.3 | 0 | 0 | 0 | 0 |
| brahmaputra | rf1 | produce | 4 | 2000000 | 2.48 | 804829 | 1670204 | 335.1 | 927.8 | 1228.5 | 1289.9 | 4.11 | 16.35 | 28.32 | 44.97 |
| brahmaputra | rf1 | consume | 4 | 2000000 | 1.62 | 1237624 | 2258428 | 106.0 | 494.7 | 1312.0 | 1613.8 | 0 | 0 | 0 | 0 |

### Replication actually happened

```
kafka rep-1789928227-rf3-1-0: 6/6 partitions at ISR=3
kafka rep-1789928227-rf3-1-0: 6/6 partitions at ISR=3
kafka rep-1789928227-rf3-2-0: 6/6 partitions at ISR=3
kafka rep-1789928227-rf3-2-1: 6/6 partitions at ISR=3
kafka rep-1789928227-rf3-2-0: 6/6 partitions at ISR=3
kafka rep-1789928227-rf3-2-1: 6/6 partitions at ISR=3
kafka rep-1789928227-rf3-4-0: 6/6 partitions at ISR=3
kafka rep-1789928227-rf3-4-1: 6/6 partitions at ISR=3
kafka rep-1789928227-rf3-4-2: 6/6 partitions at ISR=3
kafka rep-1789928227-rf3-4-3: 6/6 partitions at ISR=3
kafka rep-1789928227-rf3-4-0: 6/6 partitions at ISR=3
kafka rep-1789928227-rf3-4-1: 6/6 partitions at ISR=3
kafka rep-1789928227-rf3-4-2: 6/6 partitions at ISR=3
kafka rep-1789928227-rf3-4-3: 6/6 partitions at ISR=3
kafka rep-1789928227-rf1-1-0: 6/6 partitions at ISR=1
kafka rep-1789928227-rf1-1-0: 6/6 partitions at ISR=1
kafka rep-1789928227-rf1-2-0: 6/6 partitions at ISR=1
kafka rep-1789928227-rf1-2-1: 6/6 partitions at ISR=1
kafka rep-1789928227-rf1-2-0: 6/6 partitions at ISR=1
kafka rep-1789928227-rf1-2-1: 6/6 partitions at ISR=1
kafka rep-1789928227-rf1-4-0: 6/6 partitions at ISR=1
kafka rep-1789928227-rf1-4-1: 6/6 partitions at ISR=1
kafka rep-1789928227-rf1-4-2: 6/6 partitions at ISR=1
kafka rep-1789928227-rf1-4-3: 6/6 partitions at ISR=1
kafka rep-1789928227-rf1-4-0: 6/6 partitions at ISR=1
kafka rep-1789928227-rf1-4-1: 6/6 partitions at ISR=1
kafka rep-1789928227-rf1-4-2: 6/6 partitions at ISR=1
kafka rep-1789928227-rf1-4-3: 6/6 partitions at ISR=1
brahmaputra rep-1789928227-rf3-1-0: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789928227-rf3-2-0: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789928227-rf3-2-1: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789928227-rf3-4-0: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789928227-rf3-4-1: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789928227-rf3-4-2: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789928227-rf3-4-3: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789928227-rf1-1-0: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789928227-rf1-2-0: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789928227-rf1-2-1: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789928227-rf1-4-0: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789928227-rf1-4-1: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789928227-rf1-4-2: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789928227-rf1-4-3: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
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
| brahmaputra-rf1-consume-1.stats | 2.210 | 1.286 | 0.582 |
| brahmaputra-rf1-consume-2.stats | 3.180 | 1.688 | 0.531 |
| brahmaputra-rf1-consume-4.stats | 2.340 | 2.481 | 1.060 |
| brahmaputra-rf1-produce-1.stats | 2.860 | 3.574 | 1.250 |
| brahmaputra-rf1-produce-2.stats | 2.390 | 5.045 | 2.111 |
| brahmaputra-rf1-produce-4.stats | 3.180 | 10.656 | 3.351 |
| brahmaputra-rf3-consume-1.stats | 2.400 | 0.960 | 0.400 |
| brahmaputra-rf3-consume-2.stats | 1.830 | 1.366 | 0.747 |
| brahmaputra-rf3-consume-4.stats | 2.180 | 2.482 | 1.139 |
| brahmaputra-rf3-produce-1.stats | 2.760 | 4.562 | 1.653 |
| brahmaputra-rf3-produce-2.stats | 3.660 | 7.148 | 1.953 |
| brahmaputra-rf3-produce-4.stats | 4.720 | 19.071 | 4.040 |
| kafka-rf1-consume-1.stats | 3.730 | 4.843 | 1.298 |
| kafka-rf1-consume-2.stats | 4.300 | 10.663 | 2.480 |
| kafka-rf1-consume-4.stats | 7.970 | 27.093 | 3.399 |
| kafka-rf1-produce-1.stats | 3.540 | 7.717 | 2.180 |
| kafka-rf1-produce-2.stats | 4.420 | 19.104 | 4.322 |
| kafka-rf1-produce-4.stats | 7.440 | 48.016 | 6.454 |
| kafka-rf3-consume-1.stats | 5.850 | 9.653 | 1.650 |
| kafka-rf3-consume-2.stats | 4.700 | 12.686 | 2.699 |
| kafka-rf3-consume-4.stats | 5.200 | 23.375 | 4.495 |
| kafka-rf3-produce-1.stats | 5.930 | 29.483 | 4.972 |
| kafka-rf3-produce-2.stats | 6.740 | 45.818 | 6.798 |
| kafka-rf3-produce-4.stats | 9.100 | 72.856 | 8.006 |
