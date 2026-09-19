# Resource-matched benchmark

Each system is driven at 1/2/4/8 concurrent clients, 1000000 records of 256 B
per client, 6 partitions per topic, RF=1, acks=1, batch.size=65536,
linger.ms=5, compression=none. Every container gets 4 CPUs and 4g,
and clients run inside the broker container on both sides, so the
sampled CPU and memory cover broker plus client for everyone.
Kafka image `apache/kafka:4.3.1`.

Producer idempotence is disabled on both systems.

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
| Clients | 2 | 4 | 8 |
| msgs/sec at nearest sampled CPU | 317209 | 438885 | 203593 |
| msgs/sec, client-measured | 451655 | 468179 | 218321 |
| CPU % avg | 350.5 | 217.1 | 255.8 |
| Memory MiB avg | 875 | 421 | 580 |
| Ratio vs Kafka at those points | 1.00x | 1.38x | 0.64x |
| Peak msgs/sec (any level) | 317209 | 570125 | 203593 |
| Peak ratio vs Kafka peak | 1.00x | 1.80x | 0.64x |

### consume

| Reading | Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|---|
| Clients | 4 | 2 | 8 |
| msgs/sec at nearest sampled CPU | 510725 | 1060445 | 780183 |
| msgs/sec, client-measured | 1052718 | 1340203 | 923195 |
| CPU % avg | 380.5 | 95.4 | 343.3 |
| Memory MiB avg | 2361 | 342 | 887 |
| Ratio vs Kafka at those points | 1.00x | 2.08x | 1.53x |
| Peak msgs/sec (any level) | 510725 | 2086594 | 780183 |
| Peak ratio vs Kafka peak | 1.00x | 4.09x | 1.53x |

### Disk after the full run

| Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|
| n/a B | 3935123419 B | 3935842209 B |

### Every level

| System | Phase | Clients | Records | Seconds | msgs/sec | Client msgs/sec | CPU avg % | CPU peak % | Mem avg MiB | Mem peak MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| kafka | produce | 1 | 1000000 | 3.92 | 255167 | 432152 | 141.5 | 171.5 | 381 | 386 |
| kafka | consume | 1 | 1000000 | 3.49 | 286862 | 724638 | 211.7 | 211.7 | 517 | 517 |
| kafka | produce | 2 | 2000000 | 6.30 | 317209 | 451655 | 350.5 | 393.3 | 875 | 1146 |
| kafka | consume | 2 | 2000000 | 3.98 | 502260 | 1362825 | 163.7 | 312.5 | 890 | 900 |
| kafka | produce | 4 | 4000000 | 14.62 | 273542 | 367729 | 392.8 | 402.2 | 1949 | 2615 |
| kafka | consume | 4 | 4000000 | 7.83 | 510725 | 1052718 | 380.5 | 410.3 | 2361 | 3073 |
| kafka | produce | 8 | 8000000 | 44.98 | 177877 | 220111 | 288.4 | 414.6 | 3174 | 4047 |
| kafka | consume | 8 | 8000000 | 22.31 | 358568 | 573516 | 296.8 | 405.1 | 3114 | 3909 |
| tcp | produce | 1 | 1000000 | 3.64 | 274574 | 309456 | 132.3 | 161.7 | 19 | 23 |
| tcp | consume | 1 | 1000000 | 1.88 | 531350 | 657782 | 2.2 | 2.2 | 38 | 38 |
| tcp | produce | 2 | 2000000 | 3.51 | 570125 | 656917 | 185.9 | 185.9 | 153 | 153 |
| tcp | consume | 2 | 2000000 | 1.89 | 1060445 | 1340203 | 95.4 | 95.4 | 342 | 342 |
| tcp | produce | 4 | 4000000 | 9.11 | 438885 | 468179 | 217.1 | 409.0 | 421 | 455 |
| tcp | consume | 4 | 4000000 | 2.10 | 1900238 | 2661918 | 11.5 | 11.5 | 380 | 380 |
| tcp | produce | 8 | 8000000 | 28.54 | 280338 | 291867 | 171.3 | 406.8 | 954 | 1001 |
| tcp | consume | 8 | 8000000 | 3.83 | 2086594 | 3216713 | 15.6 | 27.9 | 1271 | 1686 |
| quic | produce | 1 | 1000000 | 5.50 | 181818 | 195124 | 205.1 | 257.7 | 49 | 61 |
| quic | consume | 1 | 1000000 | 3.22 | 310559 | 358543 | 3.6 | 3.6 | 50 | 50 |
| quic | produce | 2 | 2000000 | 12.64 | 158190 | 165063 | 144.5 | 404.7 | 136 | 150 |
| quic | consume | 2 | 2000000 | 4.34 | 460617 | 534224 | 208.2 | 409.6 | 174 | 233 |
| quic | produce | 4 | 4000000 | 36.16 | 110604 | 129630 | 155.5 | 419.4 | 264 | 299 |
| quic | consume | 4 | 4000000 | 5.92 | 676133 | 803559 | 319.3 | 403.9 | 436 | 489 |
| quic | produce | 8 | 8000000 | 39.29 | 203593 | 218321 | 255.8 | 419.6 | 580 | 645 |
| quic | consume | 8 | 8000000 | 10.25 | 780183 | 923195 | 343.3 | 402.1 | 887 | 1019 |

Raw client output and per-second samples: `bench/results/matched/`.
