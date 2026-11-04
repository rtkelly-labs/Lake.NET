# Benchmarks

**Status:** target state. Covers how performance is measured, compared over time and gated. Today there are
two wall-clock tools on the DuckLake branch, `tools/DuckLakeBench` and `tools/ParquetBackendBench` (best of
three runs after a warm-up), plus DuckLake's `PerformanceTests`. Iceberg has none. The capability baseline
lists "no benchmark suite, no resource envelope" as a gap ([capability-baseline.md](../../capability-baseline.md)).

## Three kinds of benchmark

| Kind | Measures | Tool | Examples |
|---|---|---|---|
| Micro | Time and allocations for one hot function | BenchmarkDotNet with `[MemoryDiagnoser]`, in `benchmarks/Lake.Benchmarks.Micro` | Avro manifest decode, metadata JSON parse, Roaring64 and Puffin codecs, Parquet encode/decode for each backend, expression binding and evaluation, Murmur3, Delta log-action parsing |
| Scenario | End-to-end time, peak memory, requests made, catalog round trips | A scenario runner in `benchmarks/Lake.Benchmarks.Scenarios`. It replaces the two ad-hoc tools and keeps their scenarios | Write N rows, pruned and full scans, upserts under each delete mechanism, commit latency for each catalog, compaction, expiry/vacuum. Each one for every format, over local storage and moto |
| Comparative | The same workload against the reference implementations | Scenario runner plus the `interop/` oracles | Against PyIceberg, DuckDB (DuckLake) and delta-rs. For information only, never a gate |

Scenario results include counts (storage requests, bytes read, catalog commands) from the telemetry the
library already emits ([telemetry.md](../../telemetry.md)). Unlike wall-clock time, these counts are
deterministic, so they can be gated exactly.

## Resource envelope

A committed table (`docs/verification/resource-envelope.md`) gives, for each scenario and data size, the time,
peak memory and request counts we expect, measured on a stated reference machine. It answers the capability
baseline's open item and is what users size against. It is refreshed whenever the nightly trend moves
noticeably.

## Gating, given noisy runners

GitHub-hosted runners are too noisy to gate on small wall-clock changes. So:

| Metric | PR gate | Nightly |
|---|---|---|
| Allocations (micro) | Fail above +10% compared with `main` (deterministic enough) | Trend |
| Request and command counts (scenario) | Fail on any increase not explained in the PR | Trend |
| Wall-clock time (micro and scenario) | Comment only | Compared against `main` on the same runner with BenchmarkDotNet's statistical test; open an issue on a significant regression above 15% |
| Comparative | — | Published |

Results are stored as JSON artifacts. A trend history (the `gh-pages` branch, or a dashboard artifact) keeps
each run's numbers next to the commit that produced them.

## Benchmarking the test tooling itself

How long CI takes is part of the developer experience, so it is tracked too:

- Record each suite's duration and the slowest tests on every run (from TRX). Set a budget for the PR tier
  (for example, L0 for every format under 2 minutes) and flag tests that grow.
- Build an emulator image once per run and reuse it, instead of rebuilding it in every job.

## Done when

- The micro and scenario projects exist and cover every format, replacing the ad-hoc tools.
- Allocation and request-count gates run on every PR, and wall-clock comparisons run nightly.
- The resource envelope is committed and linked from the capability baseline.
