# DuckLake testing and constraints

[DuckLake overview and guides](../ducklake.md)

## The same constraints

| Iceberg.NET rule (AGENTS.md) | For DuckLake |
|---|---|
| The spec wins; tests are tagged with the section they verify | `[Spec("ducklake: INSERT")]` links to sections of the DuckLake spec pinned in `docs/spec/ducklake-sections.json`; `spec_trace.py matrix --check` fails on unknown links, `drift --ducklake` reports upstream changes. All 49 implementable sections (1.1) have tests. Where DuckDB and the spec text disagree, the difference is listed in [Compatibility](compatibility.md#where-the-reference-implementation-and-the-spec-differ) rather than silently followed |
| Resolve by field ID, never by name or position | Data files by `column_id` = `field_id`; delete files by their field IDs; inlined tables by the column IDs of the schema version they were created for |
| Metadata files are never overwritten; nothing written for an unknown commit is deleted | Data and delete files are written once (`overwrite: false`) with UUIDv7 names; files are deleted only when the commit is known not to have happened (conflict, error before COMMIT, abandoned transaction, attempts exhausted). Transient database failures (SQLite `BUSY`/`LOCKED`, PostgreSQL `40001`/`40P01`) are retried with the same files. A failure *during* COMMIT is resolved by looking for the snapshot's idempotency token (ID, commit time to the microsecond, `changes_made`, author, message): found, the commit succeeded and only its acknowledgement was lost; certainly absent after a transient failure, it is retried; otherwise `DuckLakeCommitStateUnknownException`, files kept. `CatalogFaultTests` checks all of this under injected faults (see [Catalog database faults](#catalog-database-faults)) |
| New behaviour other engines can observe needs a cross-engine test | `CrossEngineTests` (each case on SQLite and PostgreSQL, 1.0 and 1.1) and the BDD `CrossEngine.feature`: DuckDB reads every type, delete, update, schema change and partition written here and agrees with time travel at every snapshot; this library reads DuckDB's inlined rows, inlined deletions, flushed and merged partial files, and partitioned tables; the two engines write to one lake in turn and conflict correctly |
| Decoders of untrusted input follow the `Corruption` contract | Catalog rows are untrusted: `changes_made`, column trees, statistics, timestamps, inlined nested text, paths, inlined-table names and encryption keys fail with format errors (tested in `CatalogTests`/`EncodingTests`, and see [Security](#security)), nesting is bounded, and `tools/fuzz.sh` has `ducklake-changes` and `ducklake-inlined` targets (in the weekly `fuzz` workflow; about 500,000 runs each without a finding so far) plus an always-on mutation test |
| Fixtures are other engines' outputs, never hand-edited | No committed fixtures: the oracle generates DuckDB's lakes at test time |
| A BDD specification for user-visible behaviour | `Iceberg.Net.DuckLake.Behaviour.Tests`: Gherkin features for tables, deletes, schema evolution, partitioning, concurrency, format versions and DuckDB interop; every scenario ends with a deep validation |
| Property tests against a model | `ModelBasedTests`: random operation sequences against an in-memory model, shrinking to a minimal failing sequence with a replay seed |
| Warnings are errors | Unchanged (`Directory.Build.props`) |

## Tests

| Suite | Needs | Command |
|---|---|---|
| `Iceberg.Net.DuckLake.Tests`: `EncodingTests`, `TypeTests`, `TemporalPropertyTests`, `AddFilesTests`, `ClrTests`, `DeletionVectorTests`, `EncryptionTests`, `ConflictRuleTests`, `CatalogTests`, `FormatVersionTests`, `ValidatorTests`, `ModelBasedTests`, `PathConfinementTests`, `SqlSafetyTests`, `DuckDbAttachTests`, `CatalogFaultTests`, `StorageTests`, and the inlining, maintenance, catalog object and performance tests | nothing (PostgreSQL variants skip without a server; S3 variants without the emulator) | `dotnet test tests/Iceberg.Net.DuckLake.Tests --filter "FullyQualifiedName!~CrossEngine"` |
| `CrossEngineTests` | a Python with the interop `duckdb` group (1.0); DuckDB 2.0 (1.1) | `ICEBERG_DUCKDB_PYTHON=<python> dotnet test tests/Iceberg.Net.DuckLake.Tests` |
| `Iceberg.Net.DuckLake.Behaviour.Tests` (BDD) | as above for the DuckDB scenarios (`CrossEngine.feature` and the 1.1 ones in `FormatVersions.feature`) | `ICEBERG_DUCKDB_PYTHON=<python> dotnet test tests/Iceberg.Net.DuckLake.Behaviour.Tests` |

Every catalog-facing test class runs per database and format version (`SqliteCatalogTests10`, `PostgresCatalogTests11`,
…); `TestLake` (in `Iceberg.Net.DuckLake.TestSupport`) gives each test its own SQLite file or PostgreSQL database,
dropped afterwards, and validates the lake when the test ends. It also has a storage dimension (`StorageBackend`):
`Local`, a temporary directory, or `S3`, a fresh prefix of the `iceberg-regression` bucket on the moto emulator through
`S3FileIO` (path-style, test credentials). `StorageTests` runs a representative subset on both — writes, deletes,
updates, time travel, inlining and flush, encryption, merging, rewriting, expiry and cleanup, orphan cleanup — on SQLite
(1.0 and 1.1) and PostgreSQL catalogs; each ends with a deep validation, which reads every live file back from S3.
Object stores cannot backdate an object, so orphan cleanup is tested through `AgedListingIO`, a decorator that ages the
listing (it can also return stray locations, to show they are never deleted). Environment:

| Variable | Meaning |
|---|---|
| `ICEBERG_DUCKLAKE_POSTGRES` | Npgsql connection string of a server the tests may create databases on (`emulation/up.sh` starts one on port 5434) |
| `ICEBERG_DUCKLAKE_POSTGRES_REQUIRED=1` | Fail instead of skipping when it is unset or unreachable (CI) |
| `ICEBERG_DUCKDB_PYTHON` / `ICEBERG_DUCKDB_REQUIRED=1` | The 1.0 oracle: a Python with the interop `duckdb` group |
| `ICEBERG_DUCKLAKE11_PYTHON` / `ICEBERG_DUCKLAKE11_REQUIRED=1` | The 1.1 oracle: a Python with `duckdb==2.0.0` (downloads the extension) |
| `ICEBERG_PBT_RUNS` / `ICEBERG_PBT_SEED` | Model-based test: number of cases, and a replay seed printed by a failure |
| `ICEBERG_S3_ENDPOINT` / `ICEBERG_DUCKLAKE_S3_REQUIRED=1` | The S3 emulator for `StorageBackend.S3` lakes (default `http://localhost:5000`, moto from `emulation/up.sh`, bucket `iceberg-regression`), and failing instead of skipping when it is unreachable |

In CI (both databases, both oracles), before the hardening below: 592 of the 594 tests (the two format-1.0 runs of the deletion-vector cross-engine test skip by design) and all 111 BDD scenarios passed. With the hardening (paths, SQL, encryption, catalog faults, the full conflict table, S3), the non-oracle suite has 1,584 tests; locally with moto but without PostgreSQL or the oracles, 1,311 pass and 239 skip, and 34 PostgreSQL tests written as plain `[Fact]`/`[Theory]` report their `SkipException` as a failure (they pass or skip properly in CI). The projects are in the `cross-engine` CI suite, which starts the emulation stack's
PostgreSQL, installs both oracles and requires all three. The oracle is `interop/ducklake_oracle.py`: it attaches the
test's catalog (SQLite or PostgreSQL) in DuckDB and runs SQL against it.

Shared pieces changed for this: `SpecAttribute` accepts the `ducklake:` prefix; `tools/spec_trace.py` pins and
checks the DuckLake spec (`snapshot --ducklake`, `drift --ducklake`, a DuckLake line in the matrix);
`tests/Iceberg.Net.Fuzz` and `tools/fuzz.sh` gained two targets; the interop `duckdb` dependency group gained the
extension wheels (including `postgres_scanner`) and `pytz` (DuckDB's client needs it for `timestamptz` values); the emulation stack gained a PostgreSQL.

## Catalog database faults

`CatalogFaults` (TestSupport) is an EF Core `IDbCommandInterceptor` and `IDbTransactionInterceptor`, attached through the
store's options hook (`DuckLakeStore.Sqlite(…, configure)`, or `TestLake.ConfigureStore`). Armed, it throws before or
after the Nth catalog command, before `COMMIT` (nothing committed), or after a successful `COMMIT` (a lost
acknowledgement), with a generic `DbException`, SQLite `SQLITE_BUSY` or PostgreSQL `40001`, once or repeatedly.
Statements the library sends on EF's connection itself (multi-row inserts, inlined tables) are not counted, so faults
land around the EF commands that read and update the catalog in the same transaction.

`CatalogFaultTests` runs insert, delete, update, inlined-data flush and snapshot expiry under ten faults each, per
database and format version, and checks:

| Fault | Outcome |
|---|---|
| Generic error before or after a command | That `DbException`; nothing landed; the files the commit wrote are deleted |
| Generic error before `COMMIT` | `DuckLakeCommitStateUnknownException`; nothing landed; the files are kept |
| Lost acknowledgement (error after `COMMIT`) | Success: the snapshot is found by its idempotency token, and applied exactly once |
| `BUSY` or `40001` before or after a command, or at `COMMIT` | Success after a retry, exactly once |
| `BUSY` on every attempt | `DuckLakeConflictException` after `MaxCommitAttempts` (the last failure inner); files deleted |

Every case also asserts that one snapshot landed or none did, and ends with a deep validation. Not covered yet:
PostgreSQL through a network proxy (Toxiproxy) and a primary/standby promotion, process death mid-commit, and
cancellation at `COMMIT`.

## Security

Catalog rows and catalog writers are trusted to describe the lake, not to reach outside it. The guarantees, each with
a test that failed before it was made:

| Concern | Guarantee | Tests |
|---|---|---|
| Paths in catalog rows | A path flagged relative (data and delete files, schemas, tables) may not be absolute, carry a scheme, or have a `.`/`..` segment, also percent-encoded (decoded until stable): a format error, never a read elsewhere | `PathConfinementTests` |
| Names that become directories | Schema and table names `.`, `..`, or containing `/`, `\` or NUL are refused at create and rename | `PathConfinementTests` |
| Deletion | `CleanupOldFilesAsync` deletes only scheduled files under `data_path` (rows naming anything else stay scheduled); files registered in place outside it only with `DuckLakeOptions.CleanupOutsideDataPath`. Orphan cleanup ignores listed locations outside `data_path`. A sentinel file outside survives both | `PathConfinementTests`, `StorageTests`, `AddFilesTests` |
| SQL from catalog rows | An inlined-table name must be `ducklake_inlined_data_{table_id}_{schema_version}` of its own row before anything selects from, deletes from or drops it (a row naming `ducklake_snapshot` is a format error, not a `DROP`); identifiers are quoted everywhere | `SqlSafetyTests` |
| DuckDB's ATTACH string | libpq values with spaces, quotes, backslashes or `=` are single-quoted and escaped, so a password like `a b';c` round-trips | `DuckDbAttachTests` |
| Names end to end | Schema, table, column, tag and comment text with `"`, `'`, `;` and Unicode round-trips (files and inlined rows) | `SqlSafetyTests` |
| Encryption | In an encrypted lake a Parquet file without a key is a format error, not a plaintext read; keys that are not Base64 or not 16/24/32 bytes, wrong or swapped keys, flipped bytes and truncations are format errors; no flipped byte reads back different rows; key material never appears in exception text or span tags | `EncryptionTests` |

Known limits: files of one write share a key and Parquet's AAD prefix is not used, so swapping two files of the same
write between their rows is not detected; the catalog database itself is trusted to hold the keys (as in DuckDB).
