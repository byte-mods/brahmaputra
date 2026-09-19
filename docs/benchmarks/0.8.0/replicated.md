# Replicated head-to-head: Kafka vs Brahmaputra at RF=3, acks=all

Three brokers per system on one host, 4 CPUs and 4g per container,
6 partitions, 256 B records, 500000 records per client at 1/2/4 concurrent
clients, batch.size=65536, linger.ms=5, compression=none. Kafka image `apache/kafka:4.3.1`.

Producer idempotence is disabled on both systems.

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
| Clients at peak | 2 | 2 |
| msgs/sec (wall clock) | 194062 | 371747 |
| msgs/sec (client-measured) | 293212 | 460699 |
| CPU % avg, whole cluster | 689.5 | 541.2 |
| Memory MiB avg, whole cluster | 1945 | 362 |
| Ratio | 1.00x | 1.92x |

### consume at RF=3, acks=all, min.insync.replicas=2

| Reading | Kafka | Brahmaputra |
|---|---|---|
| Clients at peak | 4 | 4 |
| msgs/sec (wall clock) | 505561 | 1046025 |
| msgs/sec (client-measured) | 1635754 | 1495821 |
| CPU % avg, whole cluster | 778.4 | 147.7 |
| Memory MiB avg, whole cluster | 3291 | 1005 |
| Ratio | 1.00x | 2.07x |

### produce: what replication costs each system

| System | RF=1 acks=1 | RF=3 acks=all | Kept | Cost |
|---|---|---|---|---|
| kafka | 431220 | 194062 | 45% | 2.22x |
| brahmaputra | 751880 | 371747 | 49% | 2.02x |

### consume: what replication costs each system

| System | RF=1 acks=1 | RF=3 acks=all | Kept | Cost |
|---|---|---|---|---|
| kafka | 503651 | 505561 | 100% | 1.00x |
| brahmaputra | 1054852 | 1046025 | 99% | 1.01x |

### Acknowledgement latency, produce (milliseconds)

| System | Config | Clients | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|
| kafka | rf3 | 1 | 900.00 | 1151.00 | 1174.00 | 1178.00 |
| kafka | rf3 | 2 | 1233.00 | 1670.50 | 1694.50 | 1723.00 |
| kafka | rf3 | 4 | 2373.00 | 6175.25 | 6314.00 | 7275.00 |
| kafka | rf1 | 1 | 1.00 | 28.00 | 36.00 | 251.00 |
| kafka | rf1 | 2 | 2.00 | 43.00 | 56.50 | 332.00 |
| kafka | rf1 | 4 | 5.25 | 130.00 | 181.25 | 638.00 |
| brahmaputra | rf3 | 1 | 3.53 | 9.02 | 810.80 | 928.29 |
| brahmaputra | rf3 | 2 | 4.33 | 24.29 | 832.36 | 960.16 |
| brahmaputra | rf3 | 4 | 5.10 | 466.63 | 7082.25 | 9323.40 |
| brahmaputra | rf1 | 1 | 2.69 | 13.12 | 855.33 | 975.43 |
| brahmaputra | rf1 | 2 | 3.12 | 19.50 | 834.20 | 951.20 |
| brahmaputra | rf1 | 4 | 4.10 | 11.33 | 884.70 | 1007.12 |

Kafka reports whole milliseconds, Brahmaputra two decimals; each
is parsed from its own client summary rather than recomputed. Both
measure the same span: record admitted to record acknowledged.

### Every level

`NA` means no valid resource sample completed during the workload.

| System | Config | Phase | Clients | Records | Seconds | msgs/sec | Client msgs/sec | CPU avg % | CPU peak % | Mem avg MiB | Mem peak MiB | p50 ms | p99 ms | p99.9 ms | max ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| kafka | rf3 | produce | 1 | 500000 | 4.72 | 105932 | 162813 | 627.0 | 627.0 | 1309 | 1309 | 900.00 | 1151.00 | 1174.00 | 1178.00 |
| kafka | rf3 | consume | 1 | 500000 | 3.47 | 144134 | 423729 | 231.7 | 231.7 | 1592 | 1592 | 0 | 0 | 0 | 0 |
| kafka | rf3 | produce | 2 | 1000000 | 5.15 | 194062 | 293212 | 689.5 | 965.9 | 1945 | 2374 | 1233.00 | 1670.50 | 1694.50 | 1723.00 |
| kafka | rf3 | consume | 2 | 1000000 | 3.01 | 332447 | 1288007 | 467.6 | 467.6 | 2183 | 2183 | 0 | 0 | 0 | 0 |
| kafka | rf3 | produce | 4 | 2000000 | 10.99 | 182050 | 229433 | 489.8 | 951.4 | 3090 | 3840 | 2373.00 | 6175.25 | 6314.00 | 7275.00 |
| kafka | rf3 | consume | 4 | 2000000 | 3.96 | 505561 | 1635754 | 778.4 | 778.4 | 3291 | 3291 | 0 | 0 | 0 | 0 |
| kafka | rf1 | produce | 1 | 500000 | 2.79 | 179019 | 429185 | 386.8 | 386.8 | 3110 | 3110 | 1.00 | 28.00 | 36.00 | 251.00 |
| kafka | rf1 | consume | 1 | 500000 | 3.63 | 137703 | 331345 | 72.0 | 72.0 | 3061 | 3061 | 0 | 0 | 0 | 0 |
| kafka | rf1 | produce | 2 | 1000000 | 3.43 | 291290 | 656794 | 547.6 | 547.6 | 3274 | 3274 | 2.00 | 43.00 | 56.50 | 332.00 |
| kafka | rf1 | consume | 2 | 1000000 | 3.11 | 321958 | 1166737 | 403.1 | 403.1 | 3554 | 3554 | 0 | 0 | 0 | 0 |
| kafka | rf1 | produce | 4 | 2000000 | 4.64 | 431220 | 995944 | 427.7 | 839.3 | 3863 | 4000 | 5.25 | 130.00 | 181.25 | 638.00 |
| kafka | rf1 | consume | 4 | 2000000 | 3.97 | 503651 | 1798762 | 807.9 | 807.9 | 4948 | 4948 | 0 | 0 | 0 | 0 |
| brahmaputra | rf3 | produce | 1 | 500000 | 2.18 | 229148 | 269718 | 311.3 | 311.3 | 127 | 127 | 3.53 | 9.02 | 810.80 | 928.29 |
| brahmaputra | rf3 | consume | 1 | 500000 | 1.72 | 290529 | 361194 | 41.6 | 41.6 | 176 | 176 | 0 | 0 | 0 | 0 |
| brahmaputra | rf3 | produce | 2 | 1000000 | 2.69 | 371747 | 460699 | 541.2 | 541.2 | 362 | 362 | 4.33 | 24.29 | 832.36 | 960.16 |
| brahmaputra | rf3 | consume | 2 | 1000000 | 1.88 | 532765 | 704259 | 55.8 | 55.8 | 450 | 450 | 0 | 0 | 0 | 0 |
| brahmaputra | rf3 | produce | 4 | 2000000 | 12.62 | 158529 | 313667 | 176.3 | 692.2 | 908 | 1020 | 5.10 | 466.63 | 7082.25 | 9323.40 |
| brahmaputra | rf3 | consume | 4 | 2000000 | 1.91 | 1046025 | 1495821 | 147.7 | 147.7 | 1005 | 1005 | 0 | 0 | 0 | 0 |
| brahmaputra | rf1 | produce | 1 | 500000 | 2.08 | 240500 | 286352 | 173.5 | 173.5 | 1010 | 1010 | 2.69 | 13.12 | 855.33 | 975.43 |
| brahmaputra | rf1 | consume | 1 | 500000 | 1.73 | 288684 | 360022 | NA | NA | NA | NA | 0 | 0 | 0 | 0 |
| brahmaputra | rf1 | produce | 2 | 1000000 | 2.22 | 450653 | 541982 | 295.4 | 295.4 | 1079 | 1079 | 3.12 | 19.50 | 834.20 | 951.20 |
| brahmaputra | rf1 | consume | 2 | 1000000 | 1.76 | 568182 | 746986 | NA | NA | NA | NA | 0 | 0 | 0 | 0 |
| brahmaputra | rf1 | produce | 4 | 2000000 | 2.66 | 751880 | 951909 | 629.1 | 629.1 | 1257 | 1257 | 4.10 | 11.33 | 884.70 | 1007.12 |
| brahmaputra | rf1 | consume | 4 | 2000000 | 1.90 | 1054852 | 1428274 | 64.9 | 64.9 | 1460 | 1460 | 0 | 0 | 0 | 0 |

### Replication actually happened

```
kafka rep-1789841603-rf3-1-0: 6/6 partitions at ISR=3
kafka rep-1789841603-rf3-1-0: 6/6 partitions at ISR=3
kafka rep-1789841603-rf3-2-0: 6/6 partitions at ISR=3
kafka rep-1789841603-rf3-2-1: 6/6 partitions at ISR=3
kafka rep-1789841603-rf3-2-0: 6/6 partitions at ISR=3
kafka rep-1789841603-rf3-2-1: 6/6 partitions at ISR=3
kafka rep-1789841603-rf3-4-0: 6/6 partitions at ISR=3
kafka rep-1789841603-rf3-4-1: 6/6 partitions at ISR=3
kafka rep-1789841603-rf3-4-2: 6/6 partitions at ISR=3
kafka rep-1789841603-rf3-4-3: 6/6 partitions at ISR=3
kafka rep-1789841603-rf3-4-0: 6/6 partitions at ISR=3
kafka rep-1789841603-rf3-4-1: 6/6 partitions at ISR=3
kafka rep-1789841603-rf3-4-2: 6/6 partitions at ISR=3
kafka rep-1789841603-rf3-4-3: 6/6 partitions at ISR=3
kafka rep-1789841603-rf1-1-0: 6/6 partitions at ISR=1
kafka rep-1789841603-rf1-1-0: 6/6 partitions at ISR=1
kafka rep-1789841603-rf1-2-0: 6/6 partitions at ISR=1
kafka rep-1789841603-rf1-2-1: 6/6 partitions at ISR=1
kafka rep-1789841603-rf1-2-0: 6/6 partitions at ISR=1
kafka rep-1789841603-rf1-2-1: 6/6 partitions at ISR=1
kafka rep-1789841603-rf1-4-0: 6/6 partitions at ISR=1
kafka rep-1789841603-rf1-4-1: 6/6 partitions at ISR=1
kafka rep-1789841603-rf1-4-2: 6/6 partitions at ISR=1
kafka rep-1789841603-rf1-4-3: 6/6 partitions at ISR=1
kafka rep-1789841603-rf1-4-0: 6/6 partitions at ISR=1
kafka rep-1789841603-rf1-4-1: 6/6 partitions at ISR=1
kafka rep-1789841603-rf1-4-2: 6/6 partitions at ISR=1
kafka rep-1789841603-rf1-4-3: 6/6 partitions at ISR=1
brahmaputra rep-1789841603-rf3-1-0: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789841603-rf3-2-0: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789841603-rf3-2-1: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789841603-rf3-4-0: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789841603-rf3-4-1: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789841603-rf3-4-2: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789841603-rf3-4-3: 3/3 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789841603-rf1-1-0: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789841603-rf1-2-0: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789841603-rf1-2-1: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789841603-rf1-4-0: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789841603-rf1-4-1: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789841603-rf1-4-2: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
brahmaputra rep-1789841603-rf1-4-3: 3/1 nodes hold logs, 6 partitions, offsets sum 500000 (expected 500000)
```

Raw client output and per-second samples: `bench/results/replicated/`.
