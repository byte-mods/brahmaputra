# 0.8.0 verification record

The release review and [coverage matrix](../../release-validation-matrix.md)
describe scope and limitations. Exit-code CSVs are exported from the actual
local runs; throughput measurements are recorded separately.

| Gate | Result |
| --- | --- |
| Rust workspace, Windows | 419 passed |
| Rust workspace, Linux | 419 passed |
| Formatting and Clippy with warnings denied | Passed |
| Dashboard JavaScript | 9 passed |
| Windows shell live suites | 14 suites passed |
| Windows M2 metadata/failover | 31 checks passed |
| Controller outage and conditional re-registration | Passed |
| Extended Linux replication recovery | 97 checks passed |
| Five-minute kill soak | 1,260,000 acknowledged records, 630 successful batches, 0 failed batches, 6 broker kills; contiguous-offset audit passed |

Rust tests ran with `--test-threads=1`; each scenario retained its own internal
concurrency. This avoids unrelated subsecond lease tests competing for the
shared development host. These are local release checks, not GitHub Actions
results or long-term production certification.

The final extended run kept a follower offline for 600 seconds while production
continued and built at least 32 MiB of incompressible backlog. Persisted lag
decreased from 1,613 to zero. All replicas agreed on the final committed prefix:

```text
high watermark: 1622
SHA-256: 6a8cadccdcfc6a30e85e8a4a73641e281478f33f32943c2bde1a247ebbb48393
bytes: 33721360
batches: 595
```

Below-minimum-ISR production failed without advancing the log or high watermark.
After one follower returned, production resumed at the unused offset. Other
checks included dual failures, idempotence, stale-epoch fencing and divergent-log
repair. Message-count comparisons in performance tests do not replace these
explicit survival and replica-identity assertions.

Both full Rust suites, all live suites, controller-outage verification and the
smoke soak were rerun after the final cancellation and coordinator-recovery
fixes. Windows and Linux correctness runs overlapped; benchmarks ran afterward,
without concurrent validation. The final benchmark matrix uses the rebuilt
broker. Earlier benchmark failures are retained in `earlier-stages.csv` and
`isr-fix-stages.csv`; they prompted the recovery fixes and are not successful
release measurements.

All 12 benchmark groups subsequently passed. The wrapper's unsupported
`server --version` check stopped the initial benchmark invocation after a
successful replication case. Removing that check allowed the remaining cases
to finish using the same release image. `stages.csv` records the final combined
outcome; [scenario exit codes](../../benchmarks/0.8.0/scenarios.csv) and the
individual reports document the completed comparison.

The embedded dashboard script passed behavior tests, and its desktop layout
was inspected in headless Edge with illustrative sample data. The live M6 suite
checked authentication, RBAC, metrics APIs and the shipping analytics controls.
