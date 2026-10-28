# Chaos engineering

**Status:** target state. Covers how faults, races, crashes and slow dependencies are injected, and the
invariant every format has to hold under them. Iceberg already has in-process fault injection
(`FaultInjectingFileIO`, `FaultInjectionTests`, `ThrottlingTests`, `SoakTests`): see paths 5 and 6 of the
[roadmap](../../roadmap.md#verification-roadmap). DuckLake has conflict-rule and concurrency tests, but
no faults injected into its catalog database.

## The invariant

After any fault, at any point, for any format:

1. **The table is either in its before state or its after state.** No partial commit is visible.
2. **The format's validator passes** in deep mode.
3. **Nothing that might have been committed is deleted.** If the outcome is unknown, every file stays.
4. **Once the fault clears, retrying or resolving the commit converges.** No rows are lost or duplicated
   compared with a model of the writes.

Every chaos test asserts all four. Most of the work is the fault model; the assertions are the same every time.

## Fault model

| Layer | Faults | How it's injected |
|---|---|---|
| Storage (`IFileIO`) | Failure before or after the write lands, 503 throttling, latency, a conditional-create race, a truncated read, a list missing a fresh object | `FaultInjectingFileIO`, moved to `Lake.TestSupport` and used by every format |
| Iceberg catalog | Timeout after the swap was applied, 5xx, 419, connection reset, stale requirements | Scripted REST server (exists), Glue through a proxy |
| DuckLake catalog database | Connection drop mid-transaction, commit acknowledgement lost, serialization failure, lock timeout, PostgreSQL restart or failover | An EF Core `DbCommandInterceptor` / `DbTransactionInterceptor` in-process, plus Toxiproxy in front of PostgreSQL |
| Delta log | A lost race on `_delta_log/N.json`, a checkpoint written without its commit, a missing `_last_checkpoint` | `FaultInjectingFileIO` |
| Network | Latency, bandwidth limits, timeouts, resets, half-open connections | **Toxiproxy** container in `emulation/` between the client and moto, the REST servers and PostgreSQL |
| Process | `kill -9` at a random point during a commit or maintenance run | Writer runs as a child process (`tools/ChaosWriter`), killed on a seeded schedule, then recovery and validation |
| Clock | Skew between writers (timestamp time travel, expiry and vacuum retention) | A `TimeProvider` passed through the library, so tests can control it |

## How it runs

- **Deterministic tier (every PR).** In-process faults at every injection point of every operation, in the
  style of `FaultInjectionTests`, for every format. Seeded, fast, no Docker.
- **Chaos tier (nightly).** Network and process faults with Toxiproxy and `ChaosWriter`. The soak runs
  several writers per format while faults are injected at random, with the seed recorded, for a configured
  time (`LAKE_CHAOS_SECONDS`).
- **Game days (manual, against AWS).** The chaos tier pointed at real S3, Glue and a hosted PostgreSQL, run
  before each release that changes a commit path.

When a chaos failure is reproduced, it becomes a deterministic test in the PR tier.

## Done when

- Every format has deterministic fault coverage of every commit path, and maintenance operations too.
- Toxiproxy and `ChaosWriter` run nightly for every format, with the invariant asserted.
- Every chaos finding has a deterministic regression test.
