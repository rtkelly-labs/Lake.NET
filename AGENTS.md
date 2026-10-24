# iceberg-dotnet — agent notes

Self-contained C# port of Apache Iceberg. Keep it that way: no references to code outside this folder.

| Task | Start here |
|---|---|
| What the spec requires and what we cover | [docs/spec-coverage.md](docs/spec-coverage.md); test ↔ section links in [docs/spec-traceability.md](docs/spec-traceability.md) (generated) |
| What is planned, and how correctness is verified | [docs/roadmap.md](docs/roadmap.md) (delivery milestones + the 13 verification paths) |
| How commits, retries and metadata state work | [docs/state-management.md](docs/state-management.md), [docs/architecture.md](docs/architecture.md) |
| Adding or changing a catalog | [docs/catalogs.md](docs/catalogs.md) — every catalog must pass `CatalogConformanceTests` |
| Running tests (spec / emulation / AWS) | [docs/regression-suite.md](docs/regression-suite.md) |
| Emulator gaps and shims | [docs/emulation.md](docs/emulation.md) |
| Insert / upsert / group replacement / delete strategies | [docs/write-modes.md](docs/write-modes.md) — behaviour changes start as a scenario in `tests/Iceberg.Net.Behaviour.Tests/Features` |
| UPDATE / MERGE | [docs/update-merge.md](docs/update-merge.md) |
| Scans, pruning, projection, time travel | [docs/read-path.md](docs/read-path.md) — `ScanRequest` / `TableScan.ReadRowsAsync` is the one read entry point |
| Branches, tags, rollback, expiry retention, metadata tables, change feed | [docs/snapshot-management.md](docs/snapshot-management.md) |
| Typed queries / LINQ / model mapping | [docs/linq.md](docs/linq.md) — translation changes need a case in `QueryTranslationTests`' predicate matrix (oracle: LINQ to Objects) |
| EF Core provider (`DbContext`, `SaveChanges`, concurrency) | [docs/efcore.md](docs/efcore.md) |
| Following Iceberg v4 | [docs/iceberg-v4.md](docs/iceberg-v4.md) |
| Deletes, deletion vectors, Puffin, statistics | [docs/puffin.md](docs/puffin.md) |
| Adopting files from other Parquet writers (Parquet.SourceGenerator) | [docs/parquet-sourcegenerator.md](docs/parquet-sourcegenerator.md) |

## Commands

| What | Command |
|---|---|
| Build | `dotnet build` |
| L0 spec tests | `dotnet test tests/Iceberg.Net.Spec.Tests` |
| Typed layer (LINQ) | `dotnet test tests/Iceberg.Net.Linq.Tests` |
| EF Core provider | `dotnet test tests/Iceberg.Net.EntityFrameworkCore.Tests` |
| BDD specification (Reqnroll) | `ICEBERG_PYTHON=<python with interop deps> dotnet test tests/Iceberg.Net.Behaviour.Tests` |
| Start emulators | `./emulation/up.sh` (moto + REST fixture); add `--spark --trino --polaris` for the other engines/catalogs |
| Regression (emulation) | `ICEBERG_PYTHON=<python with interop deps> dotnet test tests/Iceberg.Net.Regression.Tests` |
| Regenerate PyIceberg fixtures | `uv run --project interop python -I interop/pyiceberg_oracle.py fixtures tests/Iceberg.Net.Spec.Tests/fixtures/pyiceberg` |
| Regenerate Puffin/DataSketches/Roaring fixtures | `uv run --project interop python -I interop/pyiceberg_oracle.py puffin-fixtures tests/Iceberg.Net.Spec.Tests/fixtures/puffin` |
| Regenerate Spark fixtures | see `interop/spark_oracle.py` docstring (`docker run apache/spark … spark_oracle.py fixtures`) |
| Spec traceability matrix / upstream drift | `python3 tools/spec_trace.py matrix` (commit the result) / `python3 tools/spec_trace.py drift` |
| Shadow-compare a table with PyIceberg | `dotnet run --project tools/ShadowCompare -- --metadata <metadata.json>` |
| Mutation testing (slow; nothing else may build meanwhile) | `cd mutation && dotnet stryker --config-file stryker-puffin.json` |
| Python lint/format (repo rules) | from repo root: `uv run ruff check iceberg-dotnet && uv run ruff format iceberg-dotnet` |

## Rules

- **The spec wins.** Tag tests with the section they verify (`[Spec("Row Lineage")]`, `@spec:row-lineage`) and
  regenerate the matrix; CI fails on a stale matrix or a link to a section that does not exist. Prefer the spec's
  own test vectors.
- **Resolve by field ID, never by name or position**, when reading manifests or data files.
- **Metadata files are never overwritten** (`IFileIO.WriteAsync(overwrite: false)`), and after a
  `CommitStateUnknownException` nothing written for that commit may be deleted.
- New behaviour that other engines can observe needs a cross-engine test (PyIceberg oracle, REST fixture).
- If an emulator lacks an AWS guarantee, add a shim in `emulation/moto_server.py` **and** an
  `EmulatorFidelityTests` case that runs in both modes. Never weaken a test to suit the emulator.
- **Decoders of untrusted bytes** (metadata, manifests, Puffin) follow the `Corruption` contract: bounds-check every
  length before it sizes an allocation, wrap the public entry point in `Corruption.Guard`, and add a sample to
  `CorruptionTests`.
- `tests/**/fixtures/` are committed outputs of other engines: regenerate them, never hand-edit them.
- Warnings are errors (`Directory.Build.props`).
