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
| msgs/sec (wall clock) | 5704 | 8420 |
| msgs/sec (client-measured) | 9942 | 9986 |
| CPU % avg, whole cluster | 312.3 | 283.6 |
| Memory MiB avg, whole cluster | 1086.3 | 68.1 |
| Ratio | 1.00x | 1.48x |

### consume at RF=3, acks=all, min.insync.replicas=2

| Reading | Kafka | Brahmaputra |
|---|---|---|
| Clients at peak | 1 | 1 |
| msgs/sec (wall clock) | 7948 | 54885 |
| msgs/sec (client-measured) | 22789 | 272251 |
| CPU % avg, whole cluster | 249.8 | 153.7 |
| Memory MiB avg, whole cluster | 1174.0 | 61.3 |
| Ratio | 1.00x | 6.91x |

### produce: what replication costs each system

| System | RF=1 acks=1 | RF=3 acks=all | Kept | Cost |
|---|---|---|---|---|
| kafka | 5710 | 5704 | 100% | 1.00x |
| brahmaputra | 8412 | 8420 | 100% | 1.00x |

### consume: what replication costs each system

| System | RF=1 acks=1 | RF=3 acks=all | Kept | Cost |
|---|---|---|---|---|
| kafka | 9397 | 7948 | 85% | 1.18x |
| brahmaputra | 49213 | 54885 | 112% | 0.90x |

### Acknowledgement latency, produce (milliseconds)

| System | Config | Clients | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|
| kafka | rf3 | 1 | 14.00 | 251.00 | 264.00 | 748.00 |
| kafka | rf1 | 1 | 6.00 | 43.00 | 46.00 | 626.00 |
| brahmaputra | rf3 | 1 | 9.78 | 47.11 | 68.61 | 81.10 |
| brahmaputra | rf1 | 1 | 5.84 | 28.89 | 35.12 | 47.43 |

Kafka reports whole milliseconds, Brahmaputra two decimals; each
is parsed from its own client summary rather than recomputed. Both
measure the same span: record admitted to record acknowledged.

### Every level

`NA` means no valid resource sample completed during the workload.

| System | Config | Phase | Clients | Records | Seconds | msgs/sec | Client msgs/sec | CPU avg % | CPU peak % | Mem avg MiB | Mem peak MiB | p50 ms | p99 ms | p99.9 ms | max ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| kafka | rf3 | produce | 1 | 50000 | 8.77 | 5704 | 9942 | 312.3 | 1069.7 | 1086.3 | 1267.4 | 14.00 | 251.00 | 264.00 | 748.00 |
| kafka | rf3 | consume | 1 | 50000 | 6.29 | 7948 | 22789 | 249.8 | 908.2 | 1174.0 | 1274.5 | 0 | 0 | 0 | 0 |
| kafka | rf1 | produce | 1 | 50000 | 8.76 | 5710 | 9934 | 207.3 | 707.3 | 1220.4 | 1295.1 | 6.00 | 43.00 | 46.00 | 626.00 |
| kafka | rf1 | consume | 1 | 50000 | 5.32 | 9397 | 42052 | 222.3 | 676.1 | 1242.6 | 1360.5 | 0 | 0 | 0 | 0 |
| brahmaputra | rf3 | produce | 1 | 50000 | 5.94 | 8420 | 9986 | 283.6 | 433.3 | 68.1 | 76.6 | 9.78 | 47.11 | 68.61 | 81.10 |
| brahmaputra | rf3 | consume | 1 | 50000 | 0.91 | 54885 | 272251 | 153.7 | 362.6 | 61.3 | 84.3 | 0 | 0 | 0 | 0 |
| brahmaputra | rf1 | produce | 1 | 50000 | 5.94 | 8412 | 9989 | 200.2 | 462.4 | 80.9 | 90.6 | 5.84 | 28.89 | 35.12 | 47.43 |
| brahmaputra | rf1 | consume | 1 | 50000 | 1.02 | 49213 | 449600 | 150.3 | 524.7 | 77.1 | 102.6 | 0 | 0 | 0 | 0 |

### Replication actually happened

```
kafka rep-1792714509-rf3-1-0: 6/6 partitions at ISR=3
kafka rep-1792714509-rf3-1-0: 6/6 partitions at ISR=3
kafka rep-1792714509-rf1-1-0: 6/6 partitions at ISR=1
kafka rep-1792714509-rf1-1-0: 6/6 partitions at ISR=1
brahmaputra rep-1792714509-rf3-1-0: 3/3 nodes hold logs, 6 partitions, offsets sum 50000 (expected 50000)
brahmaputra rep-1792714509-rf1-1-0: 3/1 nodes hold logs, 6 partitions, offsets sum 50000 (expected 50000)
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
| brahmaputra-rf1-consume-1.stats | 2.500 | 3.757 | 1.503 |
| brahmaputra-rf1-produce-1.stats | 7.270 | 14.558 | 2.002 |
| brahmaputra-rf3-consume-1.stats | 2.240 | 3.443 | 1.537 |
| brahmaputra-rf3-produce-1.stats | 6.780 | 19.227 | 2.836 |
| kafka-rf1-consume-1.stats | 6.580 | 14.629 | 2.223 |
| kafka-rf1-produce-1.stats | 9.880 | 20.477 | 2.073 |
| kafka-rf3-consume-1.stats | 7.110 | 17.763 | 2.498 |
| kafka-rf3-produce-1.stats | 10.190 | 31.826 | 3.123 |
