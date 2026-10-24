# Spark, Athena and DuckDB helpers

Phase G pieces for working *alongside* other engines: generate the SQL they need, submit it, and use them as oracles.

| Package | What it does | Verified by |
|---|---|---|
| `Iceberg.Net.Spark` | Spark SQL for Iceberg's stored procedures; a PySpark driver that runs a list of statements; EMR Serverless and AWS Glue job runners | exact-SQL unit tests; the Spark 4.0.1 + Iceberg 1.12 oracle image runs a generated script through the driver; runners against overridden SDK clients |
| `Iceberg.Net.Athena` | Athena SQL for Iceberg DDL/DML (CREATE TABLE, OPTIMIZE, VACUUM, table properties, time travel) and a query runner | exact-SQL unit tests; runner against an overridden SDK client; real Athena only in the AWS-mode `AthenaTests` |
| `interop/duckdb_oracle.py` | DuckDB's `iceberg` extension as a reader of our tables | `DuckDbCrossEngineTests` (skips when the extension cannot load) |

Tests: `dotnet test tests/Iceberg.Net.Ecosystem.Tests` (with `emulation/up.sh --spark` for the Spark oracle and
`ICEBERG_DUCKDB_PYTHON` for DuckDB, see below).

## Spark procedures

```csharp
var procs = new IcebergProcedures("glue");                    // the Spark catalog name
var events = new SparkTable("glue", "analytics", "events");   // or SparkTable.From("glue", tableIdentifier)

procs.RewriteDataFiles(events, new RewriteDataFilesOptions { Where = "event_date >= '2026-10-01'", MinInputFiles = 2 }).ToSql();
// CALL `glue`.system.`rewrite_data_files`(table => '`analytics`.`events`', options => map('min-input-files', '2'),
//      where => 'event_date >= \'2026-10-01\'')
procs.ExpireSnapshots(events, new ExpireSnapshotsOptions { OlderThan = DateTimeOffset.UtcNow.AddDays(-7), RetainLast = 10 });
```

Covered: `rewrite_data_files` (bin-pack, sort, z-order; typed common options plus any `options` entry),
`rewrite_position_delete_files`, `expire_snapshots`, `remove_orphan_files`, `rewrite_manifests`,
`cherrypick_snapshot`, `rollback_to_snapshot`, `rollback_to_timestamp`, `set_current_snapshot` (ID or ref),
`fast_forward`, `publish_changes`, `create_changelog_view`, `add_files`, `register_table`, `ancestors_of`,
`compute_table_stats`; `Call(name, (arg, value)…)` for anything else.

Quoting rules (`SparkSql`):
- Identifiers are **always** back-quoted, a back-quote doubled, so reserved words, hyphens and dots are safe. The
  procedure's `table` argument is the namespace-qualified, back-quoted name **as a string literal**
  (`'`ns`.`t-1`'`), because Spark parses it as a multi-part identifier; it never carries the catalog (the procedure
  runs on its catalog, and a table of another catalog is refused).
- String literals escape `\` and `'` with a backslash (Spark's default parser unescapes them).
- Timestamps are written as `TIMESTAMP '… Z'`, so the session time zone cannot shift `older_than`.
- Maps are `map('k', 'v', …)` in key order; empty option maps and null arguments are left out, so Iceberg's defaults
  apply.
- A `where` / `sort_order` text is SQL the caller writes (it is passed as a literal and parsed by Iceberg).

Things the oracle run taught: `remove_orphan_files` refuses an `older_than` under 24 hours; Java's changelog scan
refuses snapshots with delete files, so `create_changelog_view` works only over append/overwrite history.

### Running a script on EMR Serverless or Glue

```csharp
var stager = new SparkScriptStager(s3FileIO, "s3://ops-bucket/iceberg-jobs/");
var script = new SparkSqlScript()
    .Add(procs.RewriteDataFiles(events))
    .Add(procs.RewritePositionDeleteFiles(events))
    .Add(procs.ExpireSnapshots(events, new() { RetainLast = 20 }));

ISparkSqlJobRunner runner = new EmrServerlessSqlJobRunner(new AmazonEMRServerlessClient(), stager, new EmrServerlessJobOptions
{
    ApplicationId = "00f…", ExecutionRoleArn = "arn:aws:iam::…:role/emr-iceberg", StagingPrefix = "s3://ops-bucket/iceberg-jobs/",
    Spark = SparkCatalogConfig.Glue("glue", "s3://lake/warehouse/"),
});
var run = await runner.RunAsync(script);   // submit, poll, throw SparkJobFailedException unless it succeeded
```

- The **driver** (`Scripts/iceberg_sql_driver.py`, embedded) reads a JSON array of statements, so nothing splits SQL
  on `;`. It is staged once under a content-hash name; each run writes a new `statements/…json`. It prints a
  `@@RESULTS@@` JSON line with every statement's rows (first 100).
- **EMR Serverless**: `StartJobRun` with the driver as `entryPoint`, `--statements <uri>` as arguments, and the catalog
  as `--conf` in `sparkSubmitParameters` (EMR 7.x images ship Iceberg); optional log URI, timeout, tags.
- **Glue**: `GlueSqlJobRunner.EnsureJobAsync` creates (or repoints) a `glueetl` job, Glue 5.0, whose script is the
  driver, with `--datalake-formats iceberg` and the catalog in Glue's single `--conf` argument; each `SubmitAsync` is a
  `StartJobRun` with `--statements`.
- `SparkCatalogConfig`: `Glue`, `Rest` (Polaris, S3 Tables / Glue REST endpoints) and `Hadoop` (tests), always with
  the Iceberg SQL extensions that `CALL` needs.

**Spark Connect**: not implemented (experimental). Spark Connect speaks gRPC with Spark's protobuf plan messages;
there is no maintained .NET client, and submitting `CALL` statements through it would need one. The SQL this package
generates works through any Spark SQL endpoint (Connect, Thrift server, Databricks SQL) the caller already has.

## Athena

```csharp
var t = new AthenaTable("analytics", "events");
AthenaSql.CreateTable(t, schema, "s3://lake/analytics/events", spec, new AthenaIcebergProperties { VacuumMaxSnapshotAgeSeconds = 259200 });
AthenaSql.Optimize(t, "event_date >= DATE '2026-10-01'");   // OPTIMIZE "analytics"."events" REWRITE DATA USING BIN_PACK WHERE …
AthenaSql.Vacuum(t);                                         // VACUUM "analytics"."events"
AthenaSql.SetTableProperties(t, new AthenaIcebergProperties { VacuumMinSnapshotsToKeep = 3 });
AthenaSql.Select(t, ["id"], AthenaSql.AsOf(instant));       // … FOR TIMESTAMP AS OF TIMESTAMP '… UTC'
AthenaSql.Select(t, timeTravel: AthenaSql.AsOfVersion(snapshotId));
AthenaSql.MetadataTable(t, "snapshots");                    // "analytics"."events$snapshots"

var athena = new AthenaQueryRunner(new AmazonAthenaClient(), new AthenaQueryOptions { WorkGroup = "primary", Database = "analytics" });
var result = await athena.RunAsync(AthenaSql.Vacuum(t));
```

- **Two quoting dialects**: Athena runs DDL (CREATE/ALTER TABLE) through its Hive parser (back-quotes) and queries,
  `OPTIMIZE` and `VACUUM` through Trino (double quotes, a double quote doubled; string literals with `''`).
- Hive DDL string literals have no escape Athena handles reliably, so locations, comments and property values
  containing quotes, backslashes or control characters are **refused** rather than escaped.
- `CreateTable` maps Iceberg types to Athena DDL (`bigint`, `decimal(p,s)`, `array<…>`, `map<…>`, `struct<…>`, …)
  and partition fields to `year/month/day/hour(col)`, `bucket(n, col)`, `truncate(w, col)`. Types Athena cannot
  declare (`timestamptz`, `uuid`, `time`, `fixed`, nanosecond timestamps, v3-only types) are refused: create such
  tables with this library through Glue instead. Athena creates format v2, assigns its own field IDs, and makes
  every column optional.
- Typed properties: `vacuum_max_snapshot_age_seconds`, `vacuum_min_snapshots_to_keep`,
  `vacuum_max_metadata_files_to_keep`, `optimize_rewrite_data_file_threshold`,
  `optimize_rewrite_delete_file_threshold`, `format`, `write_compression`, `write_target_data_file_size_bytes`.
- The runner polls `GetQueryExecution` with doubling backoff, reads every `GetQueryResults` page and drops the header
  row of SELECT results; failures raise `AthenaQueryFailedException` with Athena's reason.

The regression suite's `AthenaTests` (AWS mode only) still use their own small client; they skip without an AWS
target and are where Athena-written fixtures will come from.

## DuckDB oracle

DuckDB's `iceberg` extension is not bundled with the `duckdb` wheel or DuckDB.NET; DuckDB downloads it from
`extensions.duckdb.org` on first use. `interop/duckdb_oracle.py` tries, in order: `LOAD` (already installed),
`INSTALL` (CI), and the `duckdb-extension-iceberg` / `duckdb-extension-avro` wheels (community repackaging of DuckDB's
signed builds; DuckDB verifies the signature on `LOAD`, so they cannot smuggle in a modified binary). The extension
needs `avro` for manifests.

```sh
uv sync --project interop --group duckdb                        # duckdb 1.5.5
# offline (no extensions.duckdb.org), additionally:
uv pip install --python interop/.venv/bin/python duckdb-extensions==1.5.5 duckdb-extension-iceberg==1.5.5 duckdb-extension-avro==1.5.5
ICEBERG_DUCKDB_PYTHON=interop/.venv/bin/python dotnet test tests/Iceberg.Net.Ecosystem.Tests --filter DuckDb
```

`DuckDbCrossEngineTests` writes tables with inserts, an upsert and a delete under copy-on-write, v2 position
deletes, v2 equality deletes and v3 deletion vectors, and checks DuckDB 1.5.5 reads the same rows; all four agree.
Without a usable Python or extension the tests skip with the reason; CI sets `ICEBERG_DUCKDB_REQUIRED=1`, so there a
load failure fails. DuckDB.NET was not used: it would need the same extension download, and the Python oracle keeps
all oracles in one environment.
