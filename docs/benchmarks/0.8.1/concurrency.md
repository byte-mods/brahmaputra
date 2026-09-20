# Resource-matched benchmark

Each system is driven at 1/2/4/8 concurrent clients, 1000000 records of 256 B
per client, 6 partitions per topic, RF=1, acks=1, batch.size=65536,
linger.ms=5, compression=none. Every container gets 4 CPUs and 4g,
and clients run inside the broker container on both sides, so the
sampled CPU and memory cover broker plus client for everyone.
Kafka image `apache/kafka:4.3.1`.

Producer idempotence is disabled on both systems.

Initial consumer-group rebalance delay is 0 ms on both brokers.

Kafka client heap: `-Xmx128m -Xms64m`; producer buffer: 32 MiB on both systems.

A single client can leave a fast broker idle, so raw single-client
throughput understates a system that was never saturated. The
matched reading picks, for each system, the concurrency level whose
average CPU is closest to Kafka best level, which is what makes
these descriptive nearest points, not equal-CPU measurements.
The fixed container limits are identical; actual usage can differ.

### produce

| Reading | Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|---|
| Clients | 1 | 2 | 1 |
| msgs/sec at nearest sampled CPU | 236016 | 661594 | 299312 |
| msgs/sec, client-measured | 419639 | 898832 | 349056 |
| CPU % avg | 231.9 | 184.5 | 184.0 |
| Memory MiB avg | 438.4 | 145.1 | 33.6 |
| Ratio vs Kafka at those points | 1.00x | 2.80x | 1.27x |
| Peak msgs/sec (any level) | 236016 | 661594 | 299312 |
| Peak ratio vs Kafka peak | 1.00x | 2.80x | 1.27x |

### consume

| Reading | Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|---|
| Clients | 2 | 4 | 2 |
| msgs/sec at nearest sampled CPU | 450755 | 2762431 | 736648 |
| msgs/sec, client-measured | 1149548 | 6536044 | 1010552 |
| CPU % avg | 237.7 | 120.8 | 210.5 |
| Memory MiB avg | 1113.8 | 569.5 | 144.5 |
| Ratio vs Kafka at those points | 1.00x | 6.13x | 1.63x |
| Peak msgs/sec (any level) | 450755 | 2762431 | 1252152 |
| Peak ratio vs Kafka peak | 1.00x | 6.13x | 2.78x |

### Disk after the full run

| Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|
| n/a B | 3934215670 B | 3934456865 B |

### Every level

| System | Phase | Clients | Records | Seconds | msgs/sec | Client msgs/sec | CPU avg % | CPU peak % | Mem avg MiB | Mem peak MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| kafka | produce | 1 | 1000000 | 4.24 | 236016 | 419639 | 231.9 | 780.1 | 438.4 | 552.4 |
| kafka | consume | 1 | 1000000 | 42.35 | 23613 | 24911 | 33.4 | 407.2 | 502.9 | 779.7 |
| kafka | produce | 2 | 2000000 | 10.66 | 187635 | 352557 | 257.7 | 2870.9 | 801.6 | 1145.4 |
| kafka | consume | 2 | 2000000 | 4.44 | 450755 | 1149548 | 237.7 | 689.3 | 1113.8 | 1547.5 |
| kafka | produce | 4 | 4000000 | 19.90 | 201025 | 325234 | 260.6 | 812.5 | 1745.7 | 2288.8 |
| kafka | consume | 4 | 4000000 | 12.76 | 313480 | 1367907 | 165.6 | 800.2 | 1851.0 | 2837.5 |
| kafka | produce | 8 | 8000000 | 71.83 | 111371 | 218411 | 134.9 | 865.8 | 2745.6 | 3895.0 |
| kafka | consume | 8 | 8000000 | 29.47 | 271499 | 479099 | 205.4 | 872.9 | 3361.1 | 3992.4 |
| tcp | produce | 1 | 1000000 | 2.31 | 432339 | 567304 | 104.4 | 217.4 | 21.2 | 33.7 |
| tcp | consume | 1 | 1000000 | 1.82 | 549753 | 857894 | 26.7 | 126.5 | 64.8 | 159.7 |
| tcp | produce | 2 | 2000000 | 3.02 | 661594 | 898832 | 184.5 | 396.9 | 145.1 | 170.1 |
| tcp | consume | 2 | 2000000 | 11.41 | 175331 | 189018 | 11.2 | 151.0 | 159.4 | 376.0 |
| tcp | produce | 4 | 4000000 | 28.04 | 142669 | 147492 | 47.0 | 698.9 | 426.9 | 446.8 |
| tcp | consume | 4 | 4000000 | 1.45 | 2762431 | 6536044 | 120.8 | 460.3 | 569.5 | 913.3 |
| tcp | produce | 8 | 8000000 | 33.09 | 241779 | 250147 | 94.5 | 1145.5 | 959.8 | 998.6 |
| tcp | consume | 8 | 8000000 | 9.24 | 865988 | 956622 | 60.0 | 880.8 | 1008.3 | 2013.0 |
| quic | produce | 1 | 1000000 | 3.34 | 299312 | 349056 | 184.0 | 328.9 | 33.6 | 53.8 |
| quic | consume | 1 | 1000000 | 2.40 | 416493 | 518173 | 123.3 | 261.9 | 62.4 | 103.3 |
| quic | produce | 2 | 2000000 | 14.27 | 140154 | 145394 | 107.5 | 554.7 | 110.1 | 121.8 |
| quic | consume | 2 | 2000000 | 2.71 | 736648 | 1010552 | 210.5 | 501.6 | 144.5 | 208.3 |
| quic | produce | 4 | 4000000 | 23.82 | 167926 | 180431 | 124.5 | 807.2 | 238.3 | 274.9 |
| quic | consume | 4 | 4000000 | 4.24 | 944510 | 1290374 | 291.0 | 810.9 | 339.0 | 450.9 |
| quic | produce | 8 | 8000000 | 53.05 | 150787 | 185535 | 108.5 | 966.6 | 449.8 | 504.4 |
| quic | consume | 8 | 8000000 | 6.39 | 1252152 | 1655586 | 328.8 | 810.5 | 642.8 | 839.9 |

Raw client output, cgroup samples and CPU-time summaries: `bench/results/matched/`.

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
| kafka-consume-1.stats | 43.020 | 14.377 | 0.334 |
| kafka-consume-2.stats | 5.090 | 12.098 | 2.377 |
| kafka-consume-4.stats | 13.490 | 22.334 | 1.656 |
| kafka-consume-8.stats | 30.250 | 62.147 | 2.054 |
| kafka-produce-1.stats | 4.880 | 11.315 | 2.319 |
| kafka-produce-2.stats | 11.310 | 29.150 | 2.577 |
| kafka-produce-4.stats | 20.660 | 53.841 | 2.606 |
| kafka-produce-8.stats | 72.550 | 97.893 | 1.349 |
| quic-consume-1.stats | 3.050 | 3.761 | 1.233 |
| quic-consume-2.stats | 3.360 | 7.073 | 2.105 |
| quic-consume-4.stats | 4.770 | 13.881 | 2.910 |
| quic-consume-8.stats | 7.050 | 23.180 | 3.288 |
| quic-produce-1.stats | 3.980 | 7.323 | 1.840 |
| quic-produce-2.stats | 14.810 | 15.924 | 1.075 |
| quic-produce-4.stats | 24.290 | 30.252 | 1.245 |
| quic-produce-8.stats | 53.660 | 58.223 | 1.085 |
| tcp-consume-1.stats | 2.700 | 0.722 | 0.267 |
| tcp-consume-2.stats | 12.070 | 1.353 | 0.112 |
| tcp-consume-4.stats | 2.220 | 2.681 | 1.208 |
| tcp-consume-8.stats | 10.040 | 6.025 | 0.600 |
| tcp-produce-1.stats | 3.160 | 3.299 | 1.044 |
| tcp-produce-2.stats | 3.790 | 6.991 | 1.845 |
| tcp-produce-4.stats | 31.360 | 14.747 | 0.470 |
| tcp-produce-8.stats | 33.810 | 31.953 | 0.945 |
