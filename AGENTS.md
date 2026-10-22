# iceberg-dotnet — agent notes

Self-contained C# port of Apache Iceberg. Keep it that way: no references to code outside this folder.

| Task | Start here |
|---|---|
| What the spec requires and what we cover | [docs/spec-coverage.md](docs/spec-coverage.md) |
| How commits, retries and metadata state work | [docs/state-management.md](docs/state-management.md), [docs/architecture.md](docs/architecture.md) |
| Adding or changing a catalog | [docs/catalogs.md](docs/catalogs.md) — every catalog must pass `CatalogConformanceTests` |
| Running tests (spec / emulation / AWS) | [docs/regression-suite.md](docs/regression-suite.md) |
| Emulator gaps and shims | [docs/emulation.md](docs/emulation.md) |
| Insert / upsert / group replacement / delete strategies | [docs/write-modes.md](docs/write-modes.md) — behaviour changes start as a scenario in `tests/Iceberg.Net.Behaviour.Tests/Features` |
| Deletes, deletion vectors, Puffin, statistics | [docs/puffin.md](docs/puffin.md) |
| Adopting files from other Parquet writers (Parquet.SourceGenerator) | [docs/parquet-sourcegenerator.md](docs/parquet-sourcegenerator.md) |

## Commands

| What | Command |
|---|---|
| Build | `dotnet build` |
| L0 spec tests | `dotnet test tests/Iceberg.Net.Spec.Tests` |
| BDD specification (Reqnroll) | `ICEBERG_PYTHON=<python with interop deps> dotnet test tests/Iceberg.Net.Behaviour.Tests` |
| Start emulators | `./emulation/up.sh` |
| Regression (emulation) | `ICEBERG_PYTHON=<python with interop deps> dotnet test tests/Iceberg.Net.Regression.Tests` |
| Regenerate PyIceberg fixtures | `uv run --project interop python -I interop/pyiceberg_oracle.py fixtures tests/Iceberg.Net.Spec.Tests/fixtures/pyiceberg` |
| Regenerate Puffin/DataSketches/Roaring fixtures | `uv run --project interop python -I interop/pyiceberg_oracle.py puffin-fixtures tests/Iceberg.Net.Spec.Tests/fixtures/puffin` |
| Python lint/format (repo rules) | from repo root: `uv run ruff check iceberg-dotnet && uv run ruff format iceberg-dotnet` |

## Rules

- **The spec wins.** Cite the spec section in tests that pin spec behaviour; prefer the spec's own test vectors.
- **Resolve by field ID, never by name or position**, when reading manifests or data files.
- **Metadata files are never overwritten** (`IFileIO.WriteAsync(overwrite: false)`), and after a
  `CommitStateUnknownException` nothing written for that commit may be deleted.
- New behaviour that other engines can observe needs a cross-engine test (PyIceberg oracle, REST fixture).
- If an emulator lacks an AWS guarantee, add a shim in `emulation/moto_server.py` **and** an
  `EmulatorFidelityTests` case that runs in both modes. Never weaken a test to suit the emulator.
- Warnings are errors (`Directory.Build.props`).
