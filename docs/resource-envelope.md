# Resource and platform contract before 0.0.1

The supported shape is a batch-input writer and a row-group/block reader. Streaming results do not imply constant
memory for every stage or for an arbitrary file layout. The release has no agreed latency/throughput SLA or maximum
supported table size. The probes below are reproducible evidence, not capacity guarantees.

## Allocation boundaries

| Path | Current bound / caller responsibility |
|---|---|
| Table writes and typed inserts | Input rows are materialized and grouped by partition. Sorting/materialization and full file output also use memory. Chunk large inputs into appropriately sized batches; one call is one atomic snapshot |
| Parquet output | High-level writes produce a file per partition. Default row-group size can span the whole file; `ParquetFiles.WriteAsync` accepts explicit `ParquetWriteOptions.RowGroupRows`. Output is buffered before the FileIO write |
| Parquet scans | Decoded rows are bounded by each source row group; source files can have very large groups. A projection reads needed chunks when the manifest supplies the file size. Small/location-only/full-column paths can fetch the whole file |
| Avro scans | One decoded block, sequential ranged chunks and read-ahead when size is known; no block-statistics pruning; location-only fallback reads the full file |
| Deletes/planning | Manifest/task metadata and applicable equality-delete keys are retained in memory. Large delete sets can dominate memory even for streamed output |
| Queries | Residual operators run in .NET. Sorting/grouping and some async operators buffer rows. Strict filtering does not prohibit all client computation |
| Maintenance/change feed | Compaction gathers rows per planned group; change feed gathers changes per snapshot. These are not arbitrary-size streaming transforms |

`write.target-file-size-bytes` and a streaming enumerable must not be described as hard bounds enforced by the
high-level writer. For strict per-file/group control, write files with the lower-level APIs and commit the resulting
files explicitly. Partition skew and wide/nested rows must be included in a producer's own memory budget.

## Reproducible diagnostic probe

```bash
dotnet run --project tools/ResourceProbe -- 10000 "$PWD/temp"
dotnet run --project tools/ResourceProbe -- 100000 "$PWD/temp"
```

The `iceberg.scan.bytes` telemetry counter reports sizes of opened files, not actual transferred range bytes.
The probe's `scanReadBytes` counts the buffer-read APIs exercised by this workload, including metadata.

The probe generates deterministic data (long IDs and 269-character payloads), inserts it, projects only IDs, checks
the returned row count, and reports elapsed time, managed allocations, file sizes, buffer-read bytes and batch size.
Working-set fields are null when the runtime does not expose the metric. Managed allocation totals exclude native
Parquet allocations; working set includes runtime overhead and is not isolated per operation. This is a diagnostic
probe, not a statistically controlled benchmark. CI retains a 10,000-row probe report without a timing threshold.

Observed on macOS 15.7.7 / Arm64 / .NET 10.0.12 during this work:

| Input rows | Output file bytes | Projected scan bytes (including metadata) | Max decoded batch rows | Approximate process working set after workload |
|---|---|---|---|---|
| 10,000 | 2,013,640 | 98,728 | 10,000 | 73 MiB |
| 100,000 | 20,064,598 | 368,007 | 100,000 | 249 MiB |

The projection avoided most data bytes, but the default row group still held every row. The process working set and
latency vary with runtime, CPU, input entropy, concurrent work and native allocator behavior. These samples prove
neither a universal memory bound nor a supported upper row count.

## Runtime and platform evidence

| Surface | Build/runtime contract | Verification |
|---|---|---|
| Core and Avro | `net10.0` and `netstandard2.0` | Spec tests explicitly load the netstandard assemblies on .NET 10; netstandard package consumer builds. This is not a .NET Framework runtime test |
| Remaining libraries and server | `net10.0` | Require .NET 10 and the ParquetSharp native runtime wherever data I/O is used |
| Linux | Ubuntu CI, default hosted architecture | Full unit, cross-engine, regression, package and published-server smoke checks |
| macOS | Local Arm64 evidence; hosted macOS smoke matrix | Native types/nested reads/commits, typed writes/queries and EF Core tests in CI; hosted matrix requires a green run before claiming that platform |
| Windows | Hosted Windows smoke matrix | Same native/typed/EF checks; not the Linux Docker cross-engine suite. Requires a green run before claiming support |
| Other architectures/platforms | Not qualified | Do not infer support from a NuGet TFM or the existence of a native asset alone |

`global.json` pins the development SDK. CI's matrix is the qualification boundary; adding a supported runtime/OS
requires a matching build and representative native runtime test, not just a documentation change.
