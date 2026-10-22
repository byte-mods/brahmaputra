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
| Clients | 8 | 8 | 8 |
| msgs/sec at nearest sampled CPU | 157931 | 420234 | 255918 |
| msgs/sec, client-measured | 218891 | 448080 | 268152 |
| CPU % avg | 386.3 | 328.2 | 368.9 |
| Memory MiB avg | 2782.7 | 963.6 | 514.7 |
| Ratio vs Kafka at those points | 1.00x | 2.66x | 1.62x |
| Peak msgs/sec (any level) | 157931 | 470921 | 255918 |
| Peak ratio vs Kafka peak | 1.00x | 2.98x | 1.62x |

### consume

| Reading | Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|---|
| Clients | 4 | 8 | 8 |
| msgs/sec at nearest sampled CPU | 294442 | 2402402 | 608967 |
| msgs/sec, client-measured | 641960 | 3953377 | 691709 |
| CPU % avg | 337.6 | 230.5 | 351.6 |
| Memory MiB avg | 1924.4 | 1351.4 | 701.7 |
| Ratio vs Kafka at those points | 1.00x | 8.16x | 2.07x |
| Peak msgs/sec (any level) | 294442 | 2402402 | 608967 |
| Peak ratio vs Kafka peak | 1.00x | 8.16x | 2.07x |

### Disk after the full run

| Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|
| n/a B | 3934555538 B | 3934957790 B |

### Every level

| System | Phase | Clients | Records | Seconds | msgs/sec | Client msgs/sec | CPU avg % | CPU peak % | Mem avg MiB | Mem peak MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| kafka | produce | 1 | 1000000 | 9.15 | 109230 | 181818 | 264.2 | 772.9 | 446.8 | 579.5 |
| kafka | consume | 1 | 1000000 | 7.40 | 135080 | 301386 | 163.6 | 424.9 | 511.8 | 766.2 |
| kafka | produce | 2 | 2000000 | 14.65 | 136547 | 197099 | 315.8 | 876.1 | 739.0 | 1008.3 |
| kafka | consume | 2 | 2000000 | 8.12 | 246275 | 574042 | 276.1 | 736.8 | 966.7 | 1436.8 |
| kafka | produce | 4 | 4000000 | 26.39 | 151590 | 214000 | 372.7 | 865.0 | 1611.2 | 2195.6 |
| kafka | consume | 4 | 4000000 | 13.59 | 294442 | 641960 | 337.6 | 845.6 | 1924.4 | 2786.9 |
| kafka | produce | 8 | 8000000 | 50.66 | 157931 | 218891 | 386.3 | 933.9 | 2782.7 | 3828.6 |
| kafka | consume | 8 | 8000000 | 33.18 | 241109 | 426387 | 372.3 | 763.3 | 3083.7 | 3952.0 |
| tcp | produce | 1 | 1000000 | 4.32 | 231428 | 279757 | 142.6 | 236.3 | 26.2 | 38.1 |
| tcp | consume | 1 | 1000000 | 1.54 | 650618 | 1234220 | 47.3 | 134.2 | 80.0 | 161.3 |
| tcp | produce | 2 | 2000000 | 4.51 | 443754 | 533367 | 265.7 | 461.1 | 160.1 | 181.1 |
| tcp | consume | 2 | 2000000 | 1.78 | 1122965 | 2585259 | 79.2 | 261.3 | 249.8 | 396.6 |
| tcp | produce | 4 | 4000000 | 8.49 | 470921 | 535139 | 326.5 | 785.4 | 430.6 | 462.9 |
| tcp | consume | 4 | 4000000 | 1.80 | 2224694 | 4364534 | 156.9 | 523.5 | 605.6 | 878.4 |
| tcp | produce | 8 | 8000000 | 19.04 | 420234 | 448080 | 328.2 | 968.2 | 963.6 | 1015.0 |
| tcp | consume | 8 | 8000000 | 3.33 | 2402402 | 3953377 | 230.5 | 817.9 | 1351.4 | 1884.9 |
| quic | produce | 1 | 1000000 | 7.33 | 136333 | 153265 | 214.1 | 347.9 | 38.0 | 55.8 |
| quic | consume | 1 | 1000000 | 4.61 | 217108 | 254011 | 163.7 | 350.7 | 69.0 | 96.5 |
| quic | produce | 2 | 2000000 | 9.28 | 215517 | 238150 | 324.7 | 611.8 | 118.6 | 148.5 |
| quic | consume | 2 | 2000000 | 5.33 | 375375 | 459153 | 276.5 | 615.0 | 168.9 | 215.1 |
| quic | produce | 4 | 4000000 | 16.27 | 245776 | 262197 | 360.4 | 813.2 | 263.5 | 317.8 |
| quic | consume | 4 | 4000000 | 8.16 | 490376 | 565212 | 319.2 | 812.5 | 376.1 | 459.0 |
| quic | produce | 8 | 8000000 | 31.26 | 255918 | 268152 | 368.9 | 866.9 | 514.7 | 570.7 |
| quic | consume | 8 | 8000000 | 13.14 | 608967 | 691709 | 351.6 | 816.3 | 701.7 | 852.4 |

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
| kafka-consume-1.stats | 8.500 | 13.905 | 1.636 |
| kafka-consume-2.stats | 9.040 | 24.957 | 2.761 |
| kafka-consume-4.stats | 14.430 | 48.720 | 3.376 |
| kafka-consume-8.stats | 34.070 | 126.848 | 3.723 |
| kafka-produce-1.stats | 10.060 | 26.581 | 2.642 |
| kafka-produce-2.stats | 16.500 | 52.104 | 3.158 |
| kafka-produce-4.stats | 27.230 | 101.486 | 3.727 |
| kafka-produce-8.stats | 51.470 | 198.835 | 3.863 |
| quic-consume-1.stats | 5.540 | 9.071 | 1.637 |
| quic-consume-2.stats | 6.100 | 16.864 | 2.765 |
| quic-consume-4.stats | 9.110 | 29.082 | 3.192 |
| quic-consume-8.stats | 14.020 | 49.288 | 3.516 |
| quic-produce-1.stats | 8.190 | 17.532 | 2.141 |
| quic-produce-2.stats | 10.050 | 32.636 | 3.247 |
| quic-produce-4.stats | 17.130 | 61.743 | 3.604 |
| quic-produce-8.stats | 32.230 | 118.902 | 3.689 |
| tcp-consume-1.stats | 2.460 | 1.163 | 0.473 |
| tcp-consume-2.stats | 2.620 | 2.075 | 0.792 |
| tcp-consume-4.stats | 2.650 | 4.157 | 1.569 |
| tcp-consume-8.stats | 4.230 | 9.749 | 2.305 |
| tcp-produce-1.stats | 5.190 | 7.399 | 1.426 |
| tcp-produce-2.stats | 5.450 | 14.482 | 2.657 |
| tcp-produce-4.stats | 9.380 | 30.625 | 3.265 |
| tcp-produce-8.stats | 19.890 | 65.274 | 3.282 |
