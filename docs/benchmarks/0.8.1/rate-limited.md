# Replicated head-to-head: Kafka vs Brahmaputra at RF=3, acks=all

Three brokers per system on one host, 4 CPUs and 4g per container,
6 partitions, 256 B records, 50000 records per client at 1 concurrent
clients, batch.size=65536, linger.ms=5, compression=none. Kafka image `apache/kafka:4.3.1`.

Producer idempotence is disabled on both systems.

Initial consumer-group rebalance delay is 0 ms on both brokers.

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
| msgs/sec (wall clock) | 6876 | 9126 |
| msgs/sec (client-measured) | 9962 | 9996 |
| CPU % avg, whole cluster | 212.4 | 76.9 |
| Memory MiB avg, whole cluster | 1110.4 | 68.3 |
| Ratio | 1.00x | 1.33x |

### consume at RF=3, acks=all, min.insync.replicas=2

| Reading | Kafka | Brahmaputra |
|---|---|---|
| Clients at peak | 1 | 1 |
| msgs/sec (wall clock) | 13182 | 85616 |
| msgs/sec (client-measured) | 43554 | 829543 |
| CPU % avg, whole cluster | 184.7 | 27.2 |
| Memory MiB avg, whole cluster | 1176.1 | 63.6 |
| Ratio | 1.00x | 6.49x |

### produce: what replication costs each system

| System | RF=1 acks=1 | RF=3 acks=all | Kept | Cost |
|---|---|---|---|---|
| kafka | 7263 | 6876 | 95% | 1.06x |
| brahmaputra | 8954 | 9126 | 102% | 0.98x |

### consume: what replication costs each system

| System | RF=1 acks=1 | RF=3 acks=all | Kept | Cost |
|---|---|---|---|---|
| kafka | 17024 | 13182 | 77% | 1.29x |
| brahmaputra | 75301 | 85616 | 114% | 0.88x |

### Acknowledgement latency, produce (milliseconds)

| System | Config | Clients | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|
| kafka | rf3 | 1 | 9.00 | 216.00 | 229.00 | 559.00 |
| kafka | rf1 | 1 | 5.00 | 49.00 | 51.00 | 326.00 |
| brahmaputra | rf3 | 1 | 4.08 | 10.98 | 28.24 | 31.57 |
| brahmaputra | rf1 | 1 | 3.13 | 7.73 | 21.76 | 30.44 |

Kafka reports whole milliseconds, Brahmaputra two decimals; each
is parsed from its own client summary rather than recomputed. Both
measure the same span: record admitted to record acknowledged.

### Every level

`NA` means no valid resource sample completed during the workload.

| System | Config | Phase | Clients | Records | Seconds | msgs/sec | Client msgs/sec | CPU avg % | CPU peak % | Mem avg MiB | Mem peak MiB | p50 ms | p99 ms | p99.9 ms | max ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| kafka | rf3 | produce | 1 | 50000 | 7.27 | 6876 | 9962 | 212.4 | 785.3 | 1110.4 | 1268.9 | 9.00 | 216.00 | 229.00 | 559.00 |
| kafka | rf3 | consume | 1 | 50000 | 3.79 | 13182 | 43554 | 184.7 | 772.4 | 1176.1 | 1273.8 | 0 | 0 | 0 | 0 |
| kafka | rf1 | produce | 1 | 50000 | 6.88 | 7263 | 9976 | 108.7 | 603.9 | 1227.0 | 1267.2 | 5.00 | 49.00 | 51.00 | 326.00 |
| kafka | rf1 | consume | 1 | 50000 | 2.94 | 17024 | 122850 | 154.7 | 559.0 | 1232.4 | 1354.3 | 0 | 0 | 0 | 0 |
| brahmaputra | rf3 | produce | 1 | 50000 | 5.48 | 9126 | 9996 | 76.9 | 110.8 | 68.3 | 78.0 | 4.08 | 10.98 | 28.24 | 31.57 |
| brahmaputra | rf3 | consume | 1 | 50000 | 0.58 | 85616 | 829543 | 27.2 | 112.8 | 63.6 | 103.7 | 0 | 0 | 0 | 0 |
| brahmaputra | rf1 | produce | 1 | 50000 | 5.58 | 8954 | 9988 | 50.9 | 81.3 | 78.9 | 85.4 | 3.13 | 7.73 | 21.76 | 30.44 |
| brahmaputra | rf1 | consume | 1 | 50000 | 0.66 | 75301 | 767758 | 30.2 | 127.5 | 74.7 | 96.4 | 0 | 0 | 0 | 0 |

### Replication actually happened

```
kafka rep-1789929737-rf3-1-0: 6/6 partitions at ISR=3
kafka rep-1789929737-rf3-1-0: 6/6 partitions at ISR=3
kafka rep-1789929737-rf1-1-0: 6/6 partitions at ISR=1
kafka rep-1789929737-rf1-1-0: 6/6 partitions at ISR=1
brahmaputra rep-1789929737-rf3-1-0: 3/3 nodes hold logs, 6 partitions, offsets sum 50000 (expected 50000)
brahmaputra rep-1789929737-rf1-1-0: 3/1 nodes hold logs, 6 partitions, offsets sum 50000 (expected 50000)
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
| brahmaputra-rf1-consume-1.stats | 1.300 | 0.393 | 0.302 |
| brahmaputra-rf1-produce-1.stats | 6.340 | 3.225 | 0.509 |
| brahmaputra-rf3-consume-1.stats | 1.330 | 0.361 | 0.272 |
| brahmaputra-rf3-produce-1.stats | 6.120 | 4.709 | 0.769 |
| kafka-rf1-consume-1.stats | 3.540 | 5.478 | 1.547 |
| kafka-rf1-produce-1.stats | 7.450 | 8.096 | 1.087 |
| kafka-rf3-consume-1.stats | 4.370 | 8.070 | 1.847 |
| kafka-rf3-produce-1.stats | 7.960 | 16.904 | 2.124 |
