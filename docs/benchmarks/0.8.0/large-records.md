# Kafka vs Brahmaputra (TCP) vs Brahmaputra (QUIC)

2000 records of 1048576 B (1 MiB each), 6 partitions, RF=1, acks=1,
batch.size=2097152, linger.ms=5, compression=none. Each broker gets 4 CPUs
and 4g, and each system is driven by its own client from inside its
own container. Kafka image `apache/kafka:4.3.1`.

Producer idempotence is disabled on both systems.

| Metric | Kafka | Brahmaputra TCP | Brahmaputra QUIC |
|---|---|---|---|
| Produce msgs/sec | 96.292730 | 280 | 85 |
| Produce MB/sec | 96.29 | 280.00 | 85.00 |
| Consume msgs/sec | 294.8548 | 339 | 113 |
| Consume MB/sec | 294.8548 | 339.00 | 113.00 |
| Produce CPU % avg / peak | 97.1 / 308.8 | 84.1 / 194.0 | 184.1 / 343.1 |
| Produce memory MiB avg / peak | 1114 / 1726 | 208 / 289 | 290 / 385 |
| Consume CPU % avg / peak | 97.1 / 191.2 | 37.7 / 75.3 | 171.5 / 276.9 |
| Consume memory MiB avg / peak | 2070 / 3824 | 705 / 1290 | 630 / 1266 |
| Log bytes on disk | n/a | 2097231751 | 2097227151 |
| Disk bytes per record | n/a | 1048616 | 1048614 |
| Produce msgs/sec per CPU% | 1.0 | 3.3 | 0.5 |

Raw output and per-second samples: `bench/results/three-way/`.
