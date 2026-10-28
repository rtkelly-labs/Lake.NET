# BDD

**Status:** target state. Covers how behaviour is written as Gherkin scenarios (Reqnroll) that read the same
for every format. Today Iceberg has `Iceberg.Net.Behaviour.Tests` and `Iceberg.Net.RestCatalogBehaviour.Tests`,
and the DuckLake branch has `Iceberg.Net.DuckLake.Behaviour.Tests` (12 features). [write-modes.md](../../write-modes.md)
describes the existing scenarios.

## Why

Scenarios are where behaviour is decided. AGENTS.md already says "behaviour changes start as a scenario".
Across three formats they also show the reader where the formats behave the same and where they deliberately
differ.

## Layout

```
tests/
  Lake.Behaviour.Shared/        step library: tables, rows, assertions, oracles, faults (format-neutral)
  Lake.Behaviour.Tests/
    Features/Common/            one scenario, run for every format that supports it
    Features/Iceberg/           branches, tags, equality deletes, REST catalog, views
    Features/DuckLake/          inlining, catalog objects, macros
    Features/Delta/             table features, column mapping, checkpoints
```

## Rules

- **Common scenarios name no format.** A scenario in `Common/` says `Given an empty table with key "id"` and
  never mentions Iceberg. A `Scenario Outline` with an `Examples` table by `format`, or a
  `@format:iceberg @format:ducklake @format:delta` tag that the harness expands, runs it once per format. If a
  format doesn't support the behaviour, its row says so and the scenario expects the documented refusal
  rather than silently skipping.
- **Steps call adapters, not formats.** Each format implements `IFormatDriver` (create, write in each mode,
  read, time travel, validate, open with the oracle) in its own test project. Shared steps only see the
  driver.
- **Validity is checked after every scenario.** The format's validator runs in deep mode in an `AfterScenario`
  hook, as it does for Iceberg today.
- **Tags drive everything else:**

  | Tag | Meaning |
  |---|---|
  | `@spec:<spec>:<section>` | Traceability ([spec-alignment.md](spec-alignment.md)) |
  | `@engine:spark` / `@engine:duckdb` / … | Needs that oracle; skipped with a reason when it isn't available |
  | `@pathway:<id>` | Counts towards [pathway coverage](integration-coverage.md) |
  | `@chaos` | Runs with injected faults ([chaos.md](chaos.md)) |
  | `@slow` | Excluded from the PR run, included nightly |

- **The cross-engine steps read the same for every format:** `Then <engine> reads the same rows`, using
  PyIceberg/Spark/Trino for Iceberg, DuckDB for DuckLake, and delta-rs/Spark for Delta.
- **Living documentation.** CI publishes the scenarios with their pass/fail state as an artifact (Reqnroll's
  LivingDoc or an HTML report), so the behaviour can be read without opening the code.

## The common feature set

These are the scenarios every format must have, adapted from the Iceberg features that already exist:

| Feature | Scenarios |
|---|---|
| Write modes | Insert, upsert by key, group replacement, delete by key, predicate and file, each under every delete mechanism the format has |
| Update and merge | UPDATE, MERGE with each clause, cardinality violations |
| Schema evolution | Add, rename, widen and drop columns; read old data after each change |
| Partitioning | Write and prune; partition evolution where the format supports it |
| Time travel | By snapshot or version, and by timestamp |
| Concurrency | Two writers racing; conflict detected or retried; no lost rows |
| Maintenance | Compaction, expiry or vacuum, orphan cleanup; data read before maintenance equals data read after |
| Cross-engine | Each of the above read by the format's reference engine |

## Done when

- Every format runs the common feature set.
- Format-specific features cover each format's `supported` spec sections that have user-visible behaviour.
- The living-documentation artifact is published on every CI run.
