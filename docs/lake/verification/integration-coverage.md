# Integration and coverage

**Status:** target state. Two kinds of coverage, both measured and neither measured today:

- **Pathway coverage:** which supported combinations of format, catalog, storage, backend and engine are
  exercised end to end.
- **Code coverage:** which lines and branches the suites reach.

Read the [verification overview](README.md) first.

## Pathway coverage

A pathway is one way a user can use the library: a combination of these dimensions.

| Dimension | Values |
|---|---|
| Format and version | Iceberg v1 (read), v2, v3; DuckLake 1.0, 1.1; Delta reader/writer protocol versions |
| Catalog | Iceberg: in-memory, Glue, JDBC, REST (each server). DuckLake: SQLite, PostgreSQL. Delta: none (storage only) |
| Storage | Local, S3 (moto), S3 (AWS) |
| Parquet backend | ParquetSharp, Parquet.Net |
| Operation | Create, append, upsert, delete (per mechanism), update and merge, schema change, maintenance, time travel |
| Reader | This library, plus each reference engine |

Testing every combination isn't the goal. The goal is that **every combination we claim to support is
tested, and the claim is checked by a machine.**

1. **Declare.** `docs/verification/pathways.json` lists the supported pathways as rules ("every operation ×
   every catalog for Iceberg v2/v3 on S3 (moto)"), together with explicit exclusions and the reason for each
   ("Nessie: no v3").
2. **Tag.** A test claims the pathways it exercises with `[Pathway(Format.Iceberg, Catalog.Glue, Storage.S3,
   Op.Upsert, DeleteMode.Position)]`, or with `@pathway:` in Gherkin. Theory data and scenario outlines expand
   into one claim per row.
3. **Check.** `tools/pathway_trace.py`, built in the style of `spec_trace.py`, combines the declarations with
   the claims from tests that actually ran. It reads the test results (TRX), so a skipped test doesn't count.
   It writes a matrix artifact and fails CI when a declared pathway has no passing test in the suites that
   should cover it.
4. **Report** the gaps as a list, not just a percentage, so it's clear which test to write next.

Existing parameterised suites (`CatalogConformanceTests`, `FaultInjectionTests`, the write-mode features,
both backends) already cover most pathways. Tagging them is mostly mechanical, and it shows the real gaps.

## Code coverage

- **Collect** with `coverlet.collector` (`--collect:"XPlat Code Coverage"`) in every test project, in every CI
  suite.
- **Merge** the suites' reports with ReportGenerator into one report per package. Publish it as an artifact,
  plus a summary on the PR.
- **Diff coverage on PRs.** Report which changed lines aren't covered. Fail when new library code is below a
  threshold (start at 80% of changed lines), so that tests come with code instead of being chased afterwards.
- **Ratchet each package.** Record the baseline per package in `docs/verification/coverage-baseline.json`.
  CI fails if a package drops by more than 0.5 points without a baseline update in the same PR.
- **Coverage only shows where tests might be missing, not that the code is tested.**
  [Mutation testing](../../roadmap.md#mutation-testing) measures whether the tests actually catch bugs.
  Extend it to every package's decision-heavy code (commit protocols, delete applicability, log replay,
  expression evaluation) and ratchet its scores the same way, weekly.

## Done when

- `pathways.json` exists for every format, and CI fails on any declared pathway without a passing test.
- Code coverage is collected and merged for every suite, with diff coverage on PRs and a baseline per package.
- Mutation baselines exist for every format's commit, delete and decoder code.
