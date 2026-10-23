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
| msgs/sec at nearest sampled CPU | 154258 | 434428 | 263063 |
| msgs/sec, client-measured | 209603 | 478497 | 275871 |
| CPU % avg | 385.8 | 360.1 | 376.8 |
| Memory MiB avg | 3005.4 | 962.8 | 512.5 |
| Ratio vs Kafka at those points | 1.00x | 2.82x | 1.71x |
| Peak msgs/sec (any level) | 154258 | 434428 | 263063 |
| Peak ratio vs Kafka peak | 1.00x | 2.82x | 1.71x |
| Kafka over TLS, peak msgs/sec | 133427 | | |
| Brahmaputra over TLS (tcp-tls), peak msgs/sec | | 457300 | |
| Peak ratio vs Kafka over TLS | | 3.43x (tcp-tls) | 1.97x (quic) |

### consume

| Reading | Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|---|
| Clients | 4 | 8 | 8 |
| msgs/sec at nearest sampled CPU | 290065 | 2577320 | 681489 |
| msgs/sec, client-measured | 622746 | 4135994 | 803324 |
| CPU % avg | 338.6 | 229.4 | 339.3 |
| Memory MiB avg | 2320.1 | 1363.2 | 706.2 |
| Ratio vs Kafka at those points | 1.00x | 8.89x | 2.35x |
| Peak msgs/sec (any level) | 290065 | 2577320 | 681489 |
| Peak ratio vs Kafka peak | 1.00x | 8.89x | 2.35x |
| Kafka over TLS, peak msgs/sec | 198501 | | |
| Brahmaputra over TLS (tcp-tls), peak msgs/sec | | 1550989 | |
| Peak ratio vs Kafka over TLS | | 7.81x (tcp-tls) | 3.43x (quic) |

### Disk after the full run

| Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|
| n/a B | 3934479493 B | 3935053731 B |

### Every level

| System | Phase | Clients | Records | Seconds | msgs/sec | Client msgs/sec | CPU avg % | CPU peak % | Mem avg MiB | Mem peak MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| kafka | produce | 1 | 1000000 | 8.65 | 115647 | 201248 | 255.8 | 847.6 | 468.8 | 605.9 |
| kafka | consume | 1 | 1000000 | 7.22 | 138408 | 322165 | 176.1 | 490.4 | 527.3 | 778.6 |
| kafka | produce | 2 | 2000000 | 14.50 | 137893 | 201290 | 337.9 | 829.2 | 737.9 | 1036.5 |
| kafka | consume | 2 | 2000000 | 7.40 | 270343 | 666228 | 266.4 | 731.9 | 916.5 | 1373.0 |
| kafka | produce | 4 | 4000000 | 28.18 | 141945 | 187893 | 372.4 | 941.7 | 1682.8 | 2563.0 |
| kafka | consume | 4 | 4000000 | 13.79 | 290065 | 622746 | 338.6 | 846.5 | 2320.1 | 3129.8 |
| kafka | produce | 8 | 8000000 | 51.86 | 154258 | 209603 | 385.8 | 909.6 | 3005.4 | 3636.4 |
| kafka | consume | 8 | 8000000 | 35.41 | 225925 | 383870 | 361.2 | 873.9 | 3061.3 | 3968.3 |
| kafka-tls | produce | 1 | 1000000 | 24.63 | 40603 | 47920 | 153.2 | 686.5 | 1739.2 | 1980.7 |
| kafka-tls | consume | 1 | 1000000 | 9.73 | 102722 | 182715 | 212.5 | 604.0 | 1912.0 | 2045.8 |
| kafka-tls | produce | 2 | 2000000 | 18.09 | 110558 | 151334 | 310.0 | 828.1 | 2214.2 | 2503.4 |
| kafka-tls | consume | 2 | 2000000 | 10.32 | 193705 | 391429 | 300.5 | 652.8 | 2247.3 | 2481.4 |
| kafka-tls | produce | 4 | 4000000 | 29.98 | 133427 | 179026 | 375.0 | 1206.2 | 2633.1 | 2962.0 |
| kafka-tls | consume | 4 | 4000000 | 20.15 | 198501 | 354464 | 355.0 | 911.2 | 2568.2 | 2996.3 |
| kafka-tls | produce | 8 | 8000000 | 66.36 | 120547 | 157146 | 388.7 | 1164.5 | 3228.2 | 3843.1 |
| kafka-tls | consume | 8 | 8000000 | 47.03 | 170086 | 269654 | 381.4 | 901.0 | 2967.2 | 3881.2 |
| tcp | produce | 1 | 1000000 | 4.14 | 241663 | 304289 | 141.3 | 242.6 | 26.1 | 37.9 |
| tcp | consume | 1 | 1000000 | 1.63 | 612370 | 1254138 | 48.0 | 130.7 | 74.1 | 150.4 |
| tcp | produce | 2 | 2000000 | 4.63 | 431499 | 546592 | 255.7 | 445.4 | 154.0 | 177.2 |
| tcp | consume | 2 | 2000000 | 1.72 | 1166181 | 2346262 | 89.5 | 252.7 | 250.0 | 409.5 |
| tcp | produce | 4 | 4000000 | 9.77 | 409333 | 456270 | 300.4 | 815.8 | 427.1 | 457.1 |
| tcp | consume | 4 | 4000000 | 1.98 | 2023268 | 4023813 | 159.7 | 529.2 | 601.8 | 892.5 |
| tcp | produce | 8 | 8000000 | 18.41 | 434428 | 478497 | 360.1 | 1022.1 | 962.8 | 1016.6 |
| tcp | consume | 8 | 8000000 | 3.10 | 2577320 | 4135994 | 229.4 | 794.8 | 1363.2 | 1845.9 |
| quic | produce | 1 | 1000000 | 7.32 | 136705 | 151256 | 212.5 | 354.5 | 38.1 | 55.8 |
| quic | consume | 1 | 1000000 | 4.28 | 233645 | 289667 | 175.7 | 351.8 | 71.4 | 102.6 |
| quic | produce | 2 | 2000000 | 9.39 | 212970 | 235951 | 321.1 | 601.1 | 120.7 | 149.8 |
| quic | consume | 2 | 2000000 | 4.81 | 416060 | 519170 | 273.4 | 590.3 | 175.5 | 223.3 |
| quic | produce | 4 | 4000000 | 16.49 | 242630 | 266535 | 347.0 | 827.2 | 264.2 | 318.1 |
| quic | consume | 4 | 4000000 | 7.16 | 558737 | 649403 | 310.5 | 817.8 | 378.3 | 479.0 |
| quic | produce | 8 | 8000000 | 30.41 | 263063 | 275871 | 376.8 | 950.4 | 512.5 | 573.9 |
| quic | consume | 8 | 8000000 | 11.74 | 681489 | 803324 | 339.3 | 811.8 | 706.2 | 892.3 |
| tls | produce | 1 | 1000000 | 4.25 | 235128 | 287772 | 145.1 | 263.2 | 25.8 | 37.0 |
| tls | consume | 1 | 1000000 | 1.83 | 547046 | 978865 | 65.6 | 173.1 | 42.7 | 75.3 |
| tls | produce | 2 | 2000000 | 4.62 | 433369 | 536118 | 271.7 | 475.9 | 83.7 | 105.7 |
| tls | consume | 2 | 2000000 | 5.25 | 380590 | 1212428 | 56.3 | 215.4 | 99.6 | 130.9 |
| tls | produce | 4 | 4000000 | 8.75 | 457300 | 514464 | 334.1 | 763.7 | 186.5 | 217.1 |
| tls | consume | 4 | 4000000 | 2.76 | 1448750 | 2236648 | 208.0 | 668.0 | 232.7 | 371.8 |
| tls | produce | 8 | 8000000 | 19.66 | 406918 | 435163 | 365.2 | 1130.5 | 389.6 | 441.4 |
| tls | consume | 8 | 8000000 | 5.16 | 1550989 | 2106434 | 279.9 | 805.1 | 499.8 | 677.8 |

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
| kafka-consume-1.stats | 8.120 | 14.296 | 1.761 |
| kafka-consume-2.stats | 8.280 | 22.058 | 2.664 |
| kafka-consume-4.stats | 14.660 | 49.633 | 3.386 |
| kafka-consume-8.stats | 36.290 | 131.071 | 3.612 |
| kafka-produce-1.stats | 9.510 | 24.327 | 2.558 |
| kafka-produce-2.stats | 15.420 | 52.097 | 3.379 |
| kafka-produce-4.stats | 29.220 | 108.801 | 3.724 |
| kafka-produce-8.stats | 52.770 | 203.587 | 3.858 |
| kafka-tls-consume-1.stats | 10.640 | 22.613 | 2.125 |
| kafka-tls-consume-2.stats | 11.160 | 33.535 | 3.005 |
| kafka-tls-consume-4.stats | 21.030 | 74.663 | 3.550 |
| kafka-tls-consume-8.stats | 47.880 | 182.598 | 3.814 |
| kafka-tls-produce-1.stats | 25.520 | 39.091 | 1.532 |
| kafka-tls-produce-2.stats | 18.940 | 58.712 | 3.100 |
| kafka-tls-produce-4.stats | 30.820 | 115.560 | 3.750 |
| kafka-tls-produce-8.stats | 67.250 | 261.390 | 3.887 |
| quic-consume-1.stats | 5.160 | 9.068 | 1.757 |
| quic-consume-2.stats | 5.670 | 15.501 | 2.734 |
| quic-consume-4.stats | 8.110 | 25.184 | 3.105 |
| quic-consume-8.stats | 12.660 | 42.957 | 3.393 |
| quic-produce-1.stats | 8.160 | 17.337 | 2.125 |
| quic-produce-2.stats | 10.210 | 32.783 | 3.211 |
| quic-produce-4.stats | 17.380 | 60.312 | 3.470 |
| quic-produce-8.stats | 31.350 | 118.128 | 3.768 |
| tcp-consume-1.stats | 2.490 | 1.195 | 0.480 |
| tcp-consume-2.stats | 2.510 | 2.245 | 0.895 |
| tcp-consume-4.stats | 2.780 | 4.439 | 1.597 |
| tcp-consume-8.stats | 4.060 | 9.315 | 2.294 |
| tcp-produce-1.stats | 4.920 | 6.953 | 1.413 |
| tcp-produce-2.stats | 5.560 | 14.219 | 2.557 |
| tcp-produce-4.stats | 10.640 | 31.959 | 3.004 |
| tcp-produce-8.stats | 19.240 | 69.287 | 3.601 |
| tls-consume-1.stats | 2.710 | 1.778 | 0.656 |
| tls-consume-2.stats | 6.170 | 3.471 | 0.563 |
| tls-consume-4.stats | 3.770 | 7.841 | 2.080 |
| tls-consume-8.stats | 6.180 | 17.296 | 2.799 |
| tls-produce-1.stats | 5.160 | 7.489 | 1.451 |
| tls-produce-2.stats | 5.490 | 14.917 | 2.717 |
| tls-produce-4.stats | 9.590 | 32.039 | 3.341 |
| tls-produce-8.stats | 20.590 | 75.186 | 3.652 |
