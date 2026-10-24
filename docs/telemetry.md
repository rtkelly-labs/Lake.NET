# Telemetry (traces and metrics)

Scans, commits and catalog calls emit OpenTelemetry-compatible **traces** and **metrics** through the BCL's
`System.Diagnostics.ActivitySource` and `System.Diagnostics.Metrics.Meter`, both named **`Iceberg.Net`**. The
library takes no OpenTelemetry dependency: on .NET 8 these types are in the box, and the netstandard2.0 build of Core
references `System.Diagnostics.DiagnosticSource` 8.0. Nothing is recorded, and almost nothing allocated, until a
listener subscribes.

All of it lives in one helper, [`IcebergTelemetry`](../src/Iceberg.Net.Core/Diagnostics/IcebergTelemetry.cs);
instrumented code opens a scope with `using var telemetry = IcebergTelemetry.Start…(…)` and reports results on it in
one line.

## Wiring it into OpenTelemetry

```csharp
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Iceberg.Net.Diagnostics;

builder.Services.AddOpenTelemetry()
    .WithTracing(t => t
        .AddSource(IcebergTelemetry.Name)          // "Iceberg.Net"
        .AddHttpClientInstrumentation()            // REST catalog HTTP calls appear as children of catalog spans
        .AddAWSInstrumentation()                   // Glue/S3 calls, ditto (OpenTelemetry.Instrumentation.AWS)
        .AddOtlpExporter())
    .WithMetrics(m => m
        .AddMeter(IcebergTelemetry.Name)
        .AddOtlpExporter());
```

Without the hosting extensions: `Sdk.CreateTracerProviderBuilder().AddSource("Iceberg.Net")…Build()` and
`Sdk.CreateMeterProviderBuilder().AddMeter("Iceberg.Net")…Build()`. Any other `ActivityListener` /
`MeterListener` works too (that is how [`TelemetryTests`](../tests/Iceberg.Net.Ecosystem.Tests/TelemetryTests.cs)
observe them), as does `dotnet-counters monitor --counters Iceberg.Net`.

## Spans

| Span | Kind | Where | Tags |
|---|---|---|---|
| `iceberg.scan` | internal | every row read (`TableScan.ReadRowsAsync`, `ReadBatchesAsync`, `ReadAsync`, `CountAsync` when it scans, the LINQ provider); open while rows are enumerated | `iceberg.table`, `iceberg.snapshot.id`, `iceberg.scan.files.read`, `iceberg.scan.bytes`, `iceberg.scan.rows.returned`, `iceberg.scan.rows.deleted` |
| `iceberg.scan.plan` | internal | every scan plan (child of `iceberg.scan` when part of a read; also `PlanAsync`/`PlanFilesAsync` and the planning inside mutations) | `iceberg.table`, `iceberg.snapshot.id`, `iceberg.scan.manifests.read`, `iceberg.scan.manifests.skipped`, `iceberg.scan.files.planned`, `iceberg.scan.files.pruned.partition`, `iceberg.scan.files.pruned.metrics` |
| `iceberg.commit` | internal | one logical snapshot commit: a `SnapshotProducer.CommitAsync`, or a whole insert / upsert / delete / UPDATE / MERGE with its re-plans | `iceberg.table`, `iceberg.operation` (`append`, `overwrite`, `delete`, `replace`), `iceberg.snapshot.id`, `iceberg.commit.attempts`, `iceberg.commit.retries`, `iceberg.outcome`, `error.type`; an `iceberg.commit.retry` event per lost race (`iceberg.commit.attempt`, `iceberg.commit.backoff_ms`, `error.type`) |
| `iceberg.catalog.{operation}` | client | `load_table`, `create_table`, `commit_table`, `commit_transaction`, `drop_table` on the metastore catalogs (Glue, JDBC, in-memory) and the REST catalog | `iceberg.catalog.name`, `iceberg.catalog.type` (e.g. `GlueCatalog`, `RestCatalog`), `iceberg.catalog.operation`, `iceberg.table`, `iceberg.outcome` |

A commit's catalog calls are its children: a commit that lost one race shows two `iceberg.catalog.commit_table`
spans (the first with error status), the `load_table` of the refresh between them, and `iceberg.commit.attempts = 2`.

**Outcomes** (`iceberg.outcome`): `success`; `noop` (nothing to commit); `conflict` (a `CommitFailedException` that
retries did not resolve); `unknown` (`CommitStateUnknownException`: the catalog may have applied it); `cancelled`;
`error`. Failed spans have status `Error`, `error.type` set to the exception type and an `exception` event. Catalog
spans classify only success vs `error`: a lost race at the catalog level is expected, and the enclosing commit span
says whether it was resolved.

**Nesting of commits.** Row-level mutations re-plan on conflict around a `SnapshotProducer` with no retries of its own.
A commit scope opened while another is open for the same table on the same async flow reports into the outer one,
so a mutation is one `iceberg.commit` span however its retries are layered.

**Not traced yet**: maintenance (`ExpireSnapshotsAsync`, orphan cleanup) and ref operations commit through
`Table.CommitAsync` and so appear as `iceberg.catalog.commit_table` spans, not `iceberg.commit`; `IFileIO` reads and
writes (use the HTTP/AWS instrumentation above); namespace and view calls.

## Metrics

| Instrument | Type | Unit | Tags | Meaning |
|---|---|---|---|---|
| `iceberg.scan.manifests` | counter | `{manifest}` | `iceberg.table`, `iceberg.prune.reason` (`none` = read, `partition_summary`) | data manifests planning considered |
| `iceberg.scan.files.planned` | counter | `{file}` | `iceberg.table` | data files left after planning |
| `iceberg.scan.files.pruned` | counter | `{file}` | `iceberg.table`, `iceberg.prune.reason` (`partition`, `metrics`) | data files planning skipped |
| `iceberg.scan.bytes` | counter | `By` | `iceberg.table` | sizes of the data files scans opened (files are fetched whole today, so this is close to bytes downloaded; delete files are not counted) |
| `iceberg.scan.rows` | counter | `{row}` | `iceberg.table`, `iceberg.row.kind` (`returned`, `deleted`) | rows returned, and decoded rows dropped by position deletes, DVs or equality deletes |
| `iceberg.scan.planning.duration` | histogram | `s` | `iceberg.table` | planning time |
| `iceberg.commit.duration` | histogram | `s` | `iceberg.table`, `iceberg.operation`, `iceberg.outcome` | commit time including retries and backoff |
| `iceberg.commit.attempts` | histogram | `{attempt}` | as above | attempts per commit |
| `iceberg.commit.retries` | counter | `{retry}` | `iceberg.table`, `iceberg.operation` | retries after conflicts |
| `iceberg.catalog.duration` | histogram | `s` | `iceberg.catalog.name`, `iceberg.catalog.type`, `iceberg.catalog.operation`, `iceberg.outcome` | catalog call time |

`iceberg.table` is `namespace.table`; with very many tables, drop it with an OpenTelemetry view
(`AddView("iceberg.*", new MetricStreamConfiguration { TagKeys = [...] })`). Filters and row values never appear in tags.

`ScanMetrics` (on `ScanRequest.Metrics`) remains the per-call API for the same numbers plus row-group counts; the
meter is the process-wide view.

## Adding instrumentation

Keep call sites to one line, add the tag or instrument to `IcebergTelemetry`, document it in the tables above, and
cover it in `TelemetryTests` with a listener (filter on your test's table: tests in the process run in parallel).
