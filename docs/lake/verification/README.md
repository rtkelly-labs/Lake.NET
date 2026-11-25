# Verification: target state

**Status:** target state. Covers how Lake.NET shows that every format is correct, keeps it correct, and stays
fast. Much of this already exists for Iceberg; its current reference is
[regression-suite.md](../../regression-suite.md) and the 13 verification paths in
[roadmap.md](../../roadmap.md#verification-roadmap). Read the [Lake.NET overview](../README.md) first.

## The goal

Every supported format reaches the same verification bar. No format ships on weaker evidence just because it
arrived later. The test machinery is shared, so adding a format means writing its adapters rather than
rebuilding the harness.

## Pillars

| Pillar | Question it answers | Page |
|---|---|---|
| Spec alignment | Does every spec section we claim have a test, and do we notice when the spec changes? | [spec-alignment.md](spec-alignment.md) |
| BDD | Is the behaviour a user relies on written down as executable scenarios that read the same for every format? | [bdd.md](bdd.md) |
| Regression | Once something works, with our tables and with other engines' tables, does it keep working? | [regression.md](regression.md) |
| Integration and coverage | Is every supported combination of format, catalog, storage, backend and engine actually exercised, and how much code do the suites reach? | [integration-coverage.md](integration-coverage.md) |
| Chaos | Under faults, races, crashes and slow dependencies, is every table still in its before or after state, and still valid? | [chaos.md](chaos.md) |
| Benchmarks | How fast and how memory-hungry is it, and did this change make it worse? | [benchmarks.md](benchmarks.md) |

## Where each format stands (2026-10-28)

✅ built and running · 🟡 partial · ⏳ not started. DuckLake is measured on its branch (`ccr-87fadcf9-bxi9wr`).

| Pillar | Iceberg | DuckLake | Delta |
|---|---|---|---|
| Spec traceability + drift | ✅ 73/88 table + Puffin sections, 32/32 REST operations, upstream drift job | 🟡 sections pinned (`docs/spec/ducklake-sections.json`); no drift job | ⏳ |
| Spec vectors / reference fixtures | ✅ spec appendices, PyIceberg + Spark fixtures | ✅ spec script (`schema-1.0.sql`, `schema-1.1.sql`), DuckDB-written lakes | ⏳ DAT |
| BDD (Reqnroll) | ✅ write modes × deletes × versions; REST catalog | ✅ 12 features | ⏳ |
| Format validator (fsck) | ✅ `TableValidator` | ✅ `DuckLakeValidator` | ⏳ |
| Model-based property tests | ✅ | ✅ | ⏳ |
| Cross-engine oracles | ✅ PyIceberg, Spark, Trino, DuckDB, 6 REST servers | ✅ DuckDB 1.5 and 2.0, SQLite and PostgreSQL | ⏳ delta-rs, Spark |
| Corruption contract + fuzzing | ✅ | ✅ | ⏳ |
| Mutation testing | 🟡 Puffin, Core, `DeleteFiles` baselines | 🟡 catalog text decoders | ⏳ |
| Fault injection / chaos | 🟡 in-process faults, throttling, unknown outcomes, soak | 🟡 conflict rules, concurrency scenarios; no injected catalog-database faults | ⏳ |
| Code coverage | ⏳ not measured | ⏳ | ⏳ |
| Pathway coverage | ⏳ implicit only | ⏳ | ⏳ |
| Benchmarks | ⏳ none | 🟡 wall-clock tool (`tools/DuckLakeBench`), `PerformanceTests` | ⏳ |
| Scheduled runs | ⏳ heavy jobs are manual dispatch only (fuzz, mutation, upkeep, AWS) | ⏳ | ⏳ |

## Principles

1. **One harness, many formats.** Shared test infrastructure goes in `Lake.TestSupport` (fault-injecting
   storage, oracle runners, the pathway attribute, benchmark scenarios), and each format plugs in adapters.
   The rules from [Principles](../README.md#principles) apply to test code too: shared harness code never
   references a format.
2. **Same binaries, every target.** Tests choose emulation or AWS from the environment. When a target is
   missing, the test skips and gives a reason; with `*_REQUIRE_TARGET=1` it fails instead.
3. **The reference implementation is the oracle.** Each format is cross-checked against its owner's
   implementation, in both directions.
4. **Every run checks validity.** A format's validator runs after every scenario, model step, fault and soak
   run.
5. **Never weaken a test to suit the environment.** Fix the emulator with a shim and add a fidelity test
   (AGENTS.md).
6. **Every random test is reproducible.** Seeds are printed on failure and can be passed back in.
7. **Numbers only go up.** Spec-link coverage, pathway coverage, code coverage and mutation scores each
   have a recorded baseline that can't drop without a stated reason.

## When each suite runs

| Cadence | Runs | Gate |
|---|---|---|
| Every PR | L0 for every format, BDD (no Docker), spec-trace check, pathway check, code coverage with diff, the allocation benchmarks | Blocks merge |
| Every PR (Docker) | Cross-engine and regression suites in emulation | Blocks merge |
| Nightly | All engines and REST servers, long soak, chaos suite with network faults, full benchmark run compared with `main` | Opens an issue on failure |
| Weekly | Coverage-guided fuzzing, mutation testing (ratchet), upkeep against `:latest` images, spec-drift check for every format | Opens an issue on failure |
| Release | AWS run (L3), package smoke test, API compatibility check | Blocks release |

Today nothing runs on a schedule. Adding `schedule:` triggers to the workflows that are currently dispatched
by hand is the cheapest single improvement here.
