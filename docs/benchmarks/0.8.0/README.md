# 0.8.0 Kafka comparison

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

Read the [release review](../../release-0.8.0-review.md#benchmark-method) for host,
heap and payload settings, percentile interpretation and limitations. Codec
payloads are highly compressible repeated bytes. Wall-clock rates include
startup costs; client-measured rates have different boundaries. Missing resource
samples are `NA`. Do not interpret nearest-CPU rows as controlled equal-CPU runs.

Reports retain each harness's default raw-output path in their footer. This run
overrode it with `bench/results/release-benchmarks-final/<scenario>/`; raw logs
remain local. The large-record report corrects an unavailable Kafka disk sample
from a derived zero to `n/a`; the report generators now preserve missing values.

Failure survival, transactions, TLS/security and disk failures are checked in
the [correctness matrix](../../release-validation-matrix.md); their comparative
Kafka performance is outside this benchmark matrix. Brahmaputra uses its native
protocol and clients, not Kafka wire compatibility.
