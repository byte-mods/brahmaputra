# Replicated head-to-head: Kafka vs Brahmaputra at RF=3, acks=all

Three brokers per system on one host, 4 CPUs and 4g per container,
6 partitions, 256 B records, 50000 records per client at 1 concurrent
clients, batch.size=65536, linger.ms=5, compression=none. Kafka image `apache/kafka:4.3.1`.

Producer idempotence is disabled on both systems.

Offered rate per producer: 10000 records/sec (unlimited means saturation).

Both clusters share one host, disk and NIC. Contention can affect
the systems differently; these observations are not deployment
capacity estimates or evidence of a universal throughput ratio.

Load generators run inside the broker containers, spread round-robin
across all three, so sampled CPU and memory cover broker plus client
for both systems and no one node carries all the client work.

### produce at RF=3, acks=all, min.insync.replicas=2

| Reading | Kafka | Brahmaputra |
|---|---|---|
| Clients at peak | 1 | 1 |
| msgs/sec (wall clock) | 7562 | 9327 |
| msgs/sec (client-measured) | 9978 | 9984 |
| CPU % avg, whole cluster | 232.7 | 90.7 |
| Memory MiB avg, whole cluster | 1165 | 70 |
| Ratio | 1.00x | 1.23x |

### consume at RF=3, acks=all, min.insync.replicas=2

| Reading | Kafka | Brahmaputra |
|---|---|---|
| Clients at peak | 1 | 1 |
| msgs/sec (wall clock) | 16795 | 32595 |
| msgs/sec (client-measured) | 56561 | 43520 |
| CPU % avg, whole cluster | 285.6 | 19.4 |
| Memory MiB avg, whole cluster | 1182 | 59 |
| Ratio | 1.00x | 1.94x |

### produce: what replication costs each system

| System | RF=1 acks=1 | RF=3 acks=all | Kept | Cost |
|---|---|---|---|---|
| kafka | 7405 | 7562 | 102% | 0.98x |
| brahmaputra | 9383 | 9327 | 99% | 1.01x |

### consume: what replication costs each system

| System | RF=1 acks=1 | RF=3 acks=all | Kept | Cost |
|---|---|---|---|---|
| kafka | 19810 | 16795 | 85% | 1.18x |
| brahmaputra | 33201 | 32595 | 98% | 1.02x |

### Acknowledgement latency, produce (milliseconds)

| System | Config | Clients | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|
| kafka | rf3 | 1 | 7.00 | 72.00 | 81.00 | 267.00 |
| kafka | rf1 | 1 | 5.00 | 26.00 | 30.00 | 262.00 |
| brahmaputra | rf3 | 1 | 4.22 | 26.44 | 57.04 | 64.75 |
| brahmaputra | rf1 | 1 | 3.77 | 7.77 | 12.47 | 19.62 |

Kafka reports whole milliseconds, Brahmaputra two decimals; each
is parsed from its own client summary rather than recomputed. Both
measure the same span: record admitted to record acknowledged.

### Every level

`NA` means no valid resource sample completed during the workload.

| System | Config | Phase | Clients | Records | Seconds | msgs/sec | Client msgs/sec | CPU avg % | CPU peak % | Mem avg MiB | Mem peak MiB | p50 ms | p99 ms | p99.9 ms | max ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| kafka | rf3 | produce | 1 | 50000 | 6.61 | 7562 | 9978 | 232.7 | 291.8 | 1165 | 1239 | 7.00 | 72.00 | 81.00 | 267.00 |
| kafka | rf3 | consume | 1 | 50000 | 2.98 | 16795 | 56561 | 285.6 | 285.6 | 1182 | 1182 | 0 | 0 | 0 | 0 |
| kafka | rf1 | produce | 1 | 50000 | 6.75 | 7405 | 9974 | 131.8 | 192.0 | 1240 | 1243 | 5.00 | 26.00 | 30.00 | 262.00 |
| kafka | rf1 | consume | 1 | 50000 | 2.52 | 19810 | 123457 | 197.3 | 197.3 | 1305 | 1305 | 0 | 0 | 0 | 0 |
| brahmaputra | rf3 | produce | 1 | 50000 | 5.36 | 9327 | 9984 | 90.7 | 91.5 | 70 | 73 | 4.22 | 26.44 | 57.04 | 64.75 |
| brahmaputra | rf3 | consume | 1 | 50000 | 1.53 | 32595 | 43520 | 19.4 | 19.4 | 59 | 59 | 0 | 0 | 0 | 0 |
| brahmaputra | rf1 | produce | 1 | 50000 | 5.33 | 9383 | 9989 | 47.8 | 49.9 | 72 | 80 | 3.77 | 7.77 | 12.47 | 19.62 |
| brahmaputra | rf1 | consume | 1 | 50000 | 1.51 | 33201 | 44744 | 18.3 | 18.3 | 76 | 76 | 0 | 0 | 0 | 0 |

### Replication actually happened

```
kafka rep-1789842820-rf3-1-0: 6/6 partitions at ISR=3
kafka rep-1789842820-rf3-1-0: 6/6 partitions at ISR=3
kafka rep-1789842820-rf1-1-0: 6/6 partitions at ISR=1
kafka rep-1789842820-rf1-1-0: 6/6 partitions at ISR=1
brahmaputra rep-1789842820-rf3-1-0: 3/3 nodes hold logs, 6 partitions, offsets sum 50000 (expected 50000)
brahmaputra rep-1789842820-rf1-1-0: 3/1 nodes hold logs, 6 partitions, offsets sum 50000 (expected 50000)
```

Raw client output and per-second samples: `bench/results/replicated/`.
