# DuckLake testing and constraints

[DuckLake overview and guides](../ducklake.md)

## The same constraints

| Iceberg.NET rule (AGENTS.md) | For DuckLake |
|---|---|
| The spec wins; tests are tagged with the section they verify | `[Spec("ducklake: INSERT")]` links to sections of the DuckLake spec pinned in `docs/spec/ducklake-sections.json`; `spec_trace.py matrix --check` fails on unknown links, `drift --ducklake` reports upstream changes. All 49 implementable sections (1.1) have tests. Where DuckDB and the spec text disagree, the difference is listed in [Compatibility](compatibility.md#where-the-reference-implementation-and-the-spec-differ) rather than silently followed |
| Resolve by field ID, never by name or position | Data files by `column_id` = `field_id`; delete files by their field IDs; inlined tables by the column IDs of the schema version they were created for |
| Metadata files are never overwritten; nothing written for an unknown commit is deleted | Data and delete files are written once (`overwrite: false`) with UUIDv7 names; files are deleted only when the commit is known not to have happened (conflict, error before COMMIT, abandoned transaction); a failure *during* COMMIT throws `DuckLakeCommitStateUnknownException` and keeps them |
| New behaviour other engines can observe needs a cross-engine test | `CrossEngineTests` (each case on SQLite and PostgreSQL, 1.0 and 1.1) and the BDD `CrossEngine.feature`: DuckDB reads every type, delete, update, schema change and partition written here and agrees with time travel at every snapshot; this library reads DuckDB's inlined rows, inlined deletions, flushed and merged partial files, and partitioned tables; the two engines write to one lake in turn and conflict correctly |
| Decoders of untrusted input follow the `Corruption` contract | Catalog rows are untrusted: `changes_made`, column trees, statistics, timestamps and inlined nested text fail with format errors (tested in `CatalogTests`/`EncodingTests`), nesting is bounded, and `tools/fuzz.sh` has `ducklake-changes` and `ducklake-inlined` targets (in the weekly `fuzz` workflow; about 500,000 runs each without a finding so far) plus an always-on mutation test |
| Fixtures are other engines' outputs, never hand-edited | No committed fixtures: the oracle generates DuckDB's lakes at test time |
| A BDD specification for user-visible behaviour | `Iceberg.Net.DuckLake.Behaviour.Tests`: Gherkin features for tables, deletes, schema evolution, partitioning, concurrency, format versions and DuckDB interop; every scenario ends with a deep validation |
| Property tests against a model | `ModelBasedTests`: random operation sequences against an in-memory model, shrinking to a minimal failing sequence with a replay seed |
| Warnings are errors | Unchanged (`Directory.Build.props`) |

## Tests

| Suite | Needs | Command |
|---|---|---|
| `Iceberg.Net.DuckLake.Tests`: `EncodingTests`, `TypeTests`, `TemporalPropertyTests`, `AddFilesTests`, `ClrTests`, `DeletionVectorTests`, `EncryptionTests`, `ConflictRuleTests`, `CatalogTests`, `FormatVersionTests`, `ValidatorTests`, `ModelBasedTests`, and the inlining, maintenance, catalog object and performance tests | nothing (PostgreSQL variants skip without a server) | `dotnet test tests/Iceberg.Net.DuckLake.Tests --filter "FullyQualifiedName!~CrossEngine"` |
| `CrossEngineTests` | a Python with the interop `duckdb` group (1.0); DuckDB 2.0 (1.1) | `ICEBERG_DUCKDB_PYTHON=<python> dotnet test tests/Iceberg.Net.DuckLake.Tests` |
| `Iceberg.Net.DuckLake.Behaviour.Tests` (BDD) | as above for the DuckDB scenarios (`CrossEngine.feature` and the 1.1 ones in `FormatVersions.feature`) | `ICEBERG_DUCKDB_PYTHON=<python> dotnet test tests/Iceberg.Net.DuckLake.Behaviour.Tests` |

Every catalog-facing test class runs per database and format version (`SqliteCatalogTests10`, `PostgresCatalogTests11`,
…); `TestLake` (in `Iceberg.Net.DuckLake.TestSupport`) gives each test its own SQLite file or PostgreSQL database,
dropped afterwards, and validates the lake when the test ends. Environment:

| Variable | Meaning |
|---|---|
| `ICEBERG_DUCKLAKE_POSTGRES` | Npgsql connection string of a server the tests may create databases on (`emulation/up.sh` starts one on port 5434) |
| `ICEBERG_DUCKLAKE_POSTGRES_REQUIRED=1` | Fail instead of skipping when it is unset or unreachable (CI) |
| `ICEBERG_DUCKDB_PYTHON` / `ICEBERG_DUCKDB_REQUIRED=1` | The 1.0 oracle: a Python with the interop `duckdb` group |
| `ICEBERG_DUCKLAKE11_PYTHON` / `ICEBERG_DUCKLAKE11_REQUIRED=1` | The 1.1 oracle: a Python with `duckdb==2.0.0` (downloads the extension) |
| `ICEBERG_PBT_RUNS` / `ICEBERG_PBT_SEED` | Model-based test: number of cases, and a replay seed printed by a failure |

In CI (both databases, both oracles): 592 of the 594 tests (the two format-1.0 runs of the deletion-vector cross-engine test skip by design) and all 111 BDD scenarios pass. Locally without
the 1.1 oracle: 544 and 95 pass, the 50 and 16 others skip (DuckDB 2.0 cases, and the encrypted-lake test because DuckDB cannot download httpfs here). The projects are in the `cross-engine` CI suite, which starts the emulation stack's
PostgreSQL, installs both oracles and requires all three. The oracle is `interop/ducklake_oracle.py`: it attaches the
test's catalog (SQLite or PostgreSQL) in DuckDB and runs SQL against it.

Shared pieces changed for this: `SpecAttribute` accepts the `ducklake:` prefix; `tools/spec_trace.py` pins and
checks the DuckLake spec (`snapshot --ducklake`, `drift --ducklake`, a DuckLake line in the matrix);
`tests/Iceberg.Net.Fuzz` and `tools/fuzz.sh` gained two targets; the interop `duckdb` dependency group gained the
extension wheels (including `postgres_scanner`) and `pytz` (DuckDB's client needs it for `timestamptz` values); the emulation stack gained a PostgreSQL.
