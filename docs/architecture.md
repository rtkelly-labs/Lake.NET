# Architecture

## Layers

```mermaid
flowchart TD
  App["C# producer / consumer"] --> Data["Iceberg.Net.Data<br/>TableWriter · SnapshotProducer · TableScan · Maintenance"]
  Data --> Catalog["Iceberg.Net.Catalog<br/>ICatalog · MetastoreCatalog · Glue · REST · InMemory"]
  Data --> Avro["Iceberg.Net.Avro<br/>container codec · ManifestCodec"]
  Data --> Parquet["ParquetSharp<br/>(Arrow C++ parquet)"]
  Catalog --> Core["Iceberg.Net.Core<br/>types · transforms · TableMetadata · updates/requirements · JSON"]
  Avro --> Core
  Catalog --> IO["Iceberg.Net.IO<br/>IFileIO · Local · S3"]
  Data --> IO
```

`Core` has no dependencies at all (BCL only) and is where the spec lives. Everything that talks to the
outside world (AWS SDK, HTTP, Parquet) sits above it, so the spec layer can be tested in microseconds.

## The one idea: metadata is a value, changes are data

Iceberg table state is a single immutable JSON document (`TableMetadata`). Every change is expressed as a
list of `MetadataUpdate`s guarded by a list of `UpdateRequirement`s — exactly the payload of the REST
catalog's `updateTable` call:

```
requirements: [assert-table-uuid, assert-ref-snapshot-id(main = 42)]
updates:      [add-snapshot(43), set-snapshot-ref(main → 43)]
```

That gives one code path for every catalog type:

| Catalog kind | Who validates requirements and applies updates | Atomic step |
|---|---|---|
| REST (Polaris, Unity, S3 Tables, Nessie, Lakekeeper, fixture…) | the server | the HTTP request |
| Metastore (Glue, Hive, JDBC, in-memory) | the client, via `TableMetadataBuilder` | compare-and-swap of the `metadata_location` pointer |

`TableMetadataBuilder` mirrors Java's `TableMetadata.Builder` so a commit produces the same metadata whether
the server or this library applies it. That equivalence is what `CatalogConformanceTests` checks by running
identical scenarios against Glue (client-applied) and the Java REST fixture (server-applied).

## Metastore commit protocol

```mermaid
sequenceDiagram
  participant W as Writer
  participant S as S3
  participant G as Glue
  W->>G: GetTable → metadata_location = v5, VersionId = 17
  W->>S: GET v5.metadata.json
  Note over W: validate requirements<br/>apply updates (builder)
  W->>S: PUT v6.metadata.json (If-None-Match: *)
  W->>G: UpdateTable(metadata_location = v6, VersionId = 17)
  alt someone else committed first
    G-->>W: ConcurrentModificationException
    W->>S: DELETE v6 (we definitely lost)
    Note over W: CommitFailedException → refresh, re-apply, retry
  else timeout / 5xx / reset
    Note over W: CommitStateUnknownException<br/>keep every file, surface to caller
  end
```

Three rules come from this, and each is pinned by a test:

1. **Never overwrite a metadata file** — `IFileIO.WriteAsync(overwrite:false)` uses S3 conditional writes
   (`EmulatorFidelityTests.S3_conditional_create_rejects_existing_objects`).
2. **Lost race ⇒ clean up; unknown outcome ⇒ touch nothing** (`CommitProtocolTests`).
3. **The swap must be a real CAS** — Glue `VersionId` (`EmulatorFidelityTests.Glue_update_table_enforces_version_id`).

## Snapshot production and retries

`SnapshotProducer` (append, overwrite, delete-files) writes immutable artefacts first and commits last:

1. New data files (Parquet) — written once.
2. One manifest for added files, with **null snapshot ID and sequence numbers** so it inherits them from
   whichever manifest list finally references it. This makes it reusable across retries.
3. Rewritten manifests (when deleting) and the manifest list — regenerated per attempt, because they depend
   on the parent snapshot.
4. Commit `add-snapshot` + `set-snapshot-ref(main)` with `assert-ref-snapshot-id(main = parent)`.

On `CommitFailedException` it refreshes, rebuilds 3–4 on the new parent and retries with exponential backoff
(`commit.retry.*` table properties, Java defaults). After success it deletes artefacts from failed attempts
that the winning snapshot does not reference. Appends never conflict semantically, so they always succeed
after retry; overwrites currently re-evaluate their predicate against the new parent (snapshot isolation).
Serializable validation for overwrites/deletes (Java's `validateNoConflictingData`) is roadmap item M2.

## Reading

`TableScan.PlanFilesAsync` walks manifest list → manifests → live entries, resolving every column by field ID,
inheriting sequence numbers and row IDs as the spec requires. `ParquetFiles.ReadAsync` projects each file
onto the current schema by field ID; columns added later read as `initial-default` or null. There is no
expression engine yet, so filters are `Func<DataFile,bool>` over partition tuples and metrics.

## Values

Rows are `object?[]` aligned to the schema's top-level columns, holding Iceberg's *physical* values: `int`
days for `date`, `long` micros for `time`/`timestamp(tz)`, `long` nanos for `timestamp_ns`,
`IcebergDecimal` (BigInteger + scale) for decimals, `Guid` for uuid, `byte[]` for binary/fixed. This is
deliberately low-level; an Arrow `RecordBatch` surface (Apache.Arrow) is the intended public API once nested
types land (roadmap M3). Reasons the CLR types are not used directly are in
[libraries.md](libraries.md#clr-type-pitfalls).
