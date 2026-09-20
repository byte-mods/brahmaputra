# 0.8.1 Kafka comparison

Run `bash scripts/bench-release.sh` from the project root. The harness executes
12 scenario groups sequentially and checks each producer and consumer's reported
record count. [Scenario exit codes](scenarios.csv) accompany the reports.

| Scenario | Workload and report |
| --- | --- |
| Replication | [RF=1 and RF=3, 1/2/4 clients](replicated.md); 500,000 records per client; [all measurements](replicated-levels.csv) |
| Concurrency | [TCP, QUIC and Kafka at 1/2/4/8 clients](concurrency.md); 1,000,000 records per client; [all measurements](concurrency-levels.csv) |
| Large records | [2,000 records of 1 MiB](large-records.md), TCP/QUIC/Kafka |
| Codecs | 500,000 identical 256-B records: [none](codec-none.md), [LZ4](codec-lz4.md), [gzip](codec-gzip.md), [Snappy](codec-snappy.md), [Zstd](codec-zstd.md) |
| Acknowledgments | 500,000 records, RF=1: [acks=0](acks-0.md), [acks=all](acks-all.md); acks=1 is covered by codec-none |
| Idempotence | [50,000 records, idempotence enabled on both producers](idempotent.md) |
| Offered rate | [10,000 records/sec per producer, RF=1 and RF=3](rate-limited.md); 50,000 records; [all measurements](rate-limited-levels.csv) |

The brokers share one development host, with 4 CPUs and 4 GiB per broker.
Kafka is `apache/kafka:4.3.1`. Clients run inside their broker containers, so
resource readings include both. Both consumers use groups, and producer
idempotence is explicitly matched. Limits are equal; measured CPU use is not.
Results are short single passes without confidence intervals.

Read the [release review](../../release-0.8.1-review.md#benchmark-method) for
measurement boundaries and limitations. Initial consumer-group delay is zero
on both systems. [Every paired comparison](comparison.csv) retains losing rows.

CPU uses cumulative cgroup counters; working-set memory is sampled every 50 ms.
[Resource measurements](resources.csv) include sampling windows, sample counts,
CPU core-seconds, utilization and memory for every phase. Multi-node summaries
use the common observation window. Startup/exit dispatch and probe overhead
are included. Sample peaks are observed/interpolated, not instantaneous peaks.
Do not interpret utilization as total CPU work or nearest-CPU rows as controlled
equal-CPU runs. Codec payloads are highly compressible repeated bytes.

Reports retain their harness's default raw-output paths. This run used
`bench/results/performance-candidate-matrix-v7/<scenario>/`; raw logs remain local. Correctness, security,
transactions, disk failures and failover have separate validation; their Kafka
performance is outside this matrix. Brahmaputra uses its native protocol.

[Source hashes](source-manifest.json) and [Docker environment/image IDs](environment.json) identify the measured build.
