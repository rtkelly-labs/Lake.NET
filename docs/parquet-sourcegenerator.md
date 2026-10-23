# Parquet.SourceGenerator as an Iceberg write path

[rtkelly-labs/Parquet.SourceGenerator](https://github.com/rtkelly-labs/Parquet.SourceGenerator) emits
zero-reflection, AOT-safe Parquet serializers for C# models at compile time, on top of Parquet.Net 6. For Iceberg
that is attractive: C# producers already have typed models, and a generated writer is faster and allocates less
than both reflection serializers and the row-of-objects path in `Iceberg.Net.Data`.

This page records **what was measured** (not assumed), what already works, and which gaps belong in the generator,
in Parquet.Net upstream, or in this library.

## How they fit together

```mermaid
flowchart LR
  M["C# model<br/>[ParquetSerializable]"] -->|"generated WriteParquetAsync"| F["Parquet file<br/>(no field IDs)"]
  F -->|"ParquetImport.ToDataFileAsync<br/>name mapping · type checks · metrics"| D["DataFile"]
  D -->|"SnapshotProducer.Append"| T[("Iceberg table")]
  T --> A["Athena / PyIceberg / Spark"]
```

The seam is **files, not assemblies**: iceberg-dotnet does not reference the generator. `ParquetImport` adopts any
Parquet file (generator, pyarrow, Spark) the way Java/PyIceberg `add_files` does — resolve columns through the
table's `schema.name-mapping.default`, verify every column has the Parquet representation Iceberg requires, compute
metrics and the partition tuple from the data — and rejects the file with a list of reasons otherwise.

Proven by `SourceGeneratorInteropTests` (published package 0.0.3): a generated file with `long`, `string?`,
`DateTime` (µs), `DateOnly`, `Guid`, `decimal(9,2)` and `int?` columns is adopted, reads back value-for-value in
.NET, **and PyIceberg reads it through the name mapping**.

## Measured behaviour

Inspected with pyarrow (physical/logical types, field IDs). "main" is the repository at `216f6fc`, built from source.

| C# member | 0.0.3 writes | main writes | Iceberg requires | Verdict |
|---|---|---|---|---|
| `long`, `int`, `bool`, `float`, `double` | INT64/INT32/… | same | same | ✅ |
| `string` | BYTE_ARRAY STRING | same | same | ✅ |
| `Guid` | FIXED(16) UUID, big-endian | same | FIXED(16) UUID, big-endian | ✅ |
| `decimal` + `[ParquetDecimal(9,2)]` | INT32 DECIMAL(9,2) | same | INT32 for P≤9, INT64 ≤18, FIXED otherwise | ✅ (P ≤ 28: `System.Decimal` limit) |
| `decimal` without attribute | DECIMAL(38,18) (from emitter source) | same | the table's declared (P,S) | ⚠️ must match the table; import checks |
| `DateTime` + `[ParquetTimestamp(Microseconds)]` | INT64 TIMESTAMP(µs, **UTC-adjusted**) | same | `timestamptz`: UTC-adjusted; `timestamp`: **not** | ⚠️ only `timestamptz` is expressible |
| `DateTime` (no attribute) | **INT96** | **INT96** | INT64 TIMESTAMP(µs) — writers must not emit INT96 | ❌ |
| `DateOnly` | INT32 DATE — but **0.0.3 cannot read it back** (`ArgumentException`) | **INT96** (round-trips in .NET; pyarrow reads a *datetime*) | INT32 DATE | ❌ on main; regression vs 0.0.3 |
| `TimeOnly` | — | INT64 TIME(µs, UTC-adjusted) | INT64 TIME(µs) (`isAdjustedToUTC=false`) | ✅ readable; ⚠️ annotation differs from Java's writer |
| `TimeSpan` | INT32 TIME(**ms**) | same | no Iceberg duration type; `time` is µs | ❌ |
| `uint`, `ulong`, `ushort` | unsigned INT annotations | same | no unsigned types | ❌ |
| field IDs | none | none | required for ID projection, or a name mapping | ⚠️ works via name mapping |
| nested `struct`/`List<T>` | — | supported (Parquet.Net trees) | field IDs on **group** nodes too | ❌ blocked upstream (below) |
| `timestamp_ns` (v3) | — | not offered (Parquet.Net has no nanos `DateTimeFormat`) | INT64 TIMESTAMP(ns) | ❌ blocked upstream |
| Column statistics | row-group min/max/nulls (Parquet.Net) | same | per-file metrics by field ID | ✅ computed by `ParquetImport` |

Every ❌/⚠️ row except the upstream ones is a characterization test in `SourceGeneratorInteropTests`
(`Gap_*`): the test asserts today's behaviour is rejected with a precise message, so closing a gap makes the test
fail and move into the compatible model.

## Where each gap should be fixed

| Gap | Fix in | Proposal | Effort |
|---|---|---|---|
| No field IDs | **generator** | `[ParquetColumn(FieldId = n)]` → `DataField.FieldId` (Parquet.Net 6 backend; 4.x has no field IDs). **Patch ready** (below) | S |
| `timestamp` vs `timestamptz` | **generator** | `[ParquetTimestamp(Microseconds, AdjustedToUtc = false)]`. Measured: Parquet.Net 6.1 **ignores** `isAdjustedToUTC` (and `unit`) for `DateTimeFormat.DateAndTimeMicros`, which it writes as the legacy `TIMESTAMP_MICROS` converted type (always UTC); `DateTimeFormat.Timestamp` + `unit` writes the logical type and honours the flag. **Patch ready** | S |
| `DateOnly` → INT96 on main | **generator** (bug) | emit `DateTimeFormat.Date` → INT32 DATE; measured on 6.1: writes and reads `DateTime` values, pyarrow reads `date32`. **Patch ready** | S |
| `DateTime` default INT96 | **generator** | default to `DateAndTimeMicros`; INT96 is deprecated in Parquet and rejected by Iceberg | S (behaviour change → semver note) |
| `TimeSpan` as TIME(ms) | **generator** | an opt-in `Iceberg` profile that rejects `TimeSpan` (diagnostic) or maps it to `long` micros explicitly | S |
| Unsigned types | **generator** | same profile: diagnostic for unsigned members | XS |
| Iceberg schema from the model | **generator** | emit `public static string IcebergSchemaJson` (field IDs, required/optional, types) so `CreateTableAsync` can be driven by the model — no hand-written `Schema` | M |
| Per-file Iceberg metrics without a re-read | **generator** | expose written column stats (value/null/NaN counts, min/max) per file from the generated writer; `ParquetImport` currently re-reads the file to compute them | M |
| NDV sketches at write time (Puffin) | **generator** + this library | an optional per-column hook fed with Iceberg single-value bytes during write → `ThetaSketch.Update`; produces Puffin statistics with no second scan | M |
| Field IDs on nested groups | **Parquet.Net upstream** | lift `FieldId` to `Field` and emit/read it for struct/list/map — drafted issue + verified 19-line patch: [upstream/parquet-net-group-field-ids.md](upstream/parquet-net-group-field-ids.md) | S (upstream PR) |
| `timestamp_ns` | **generator** (not upstream) | Measured: Parquet.Net 6.1 (and 4.25) has `DateTimeFormat.Timestamp` with `DateTimeTimeUnit.Nanos`, which writes `TIMESTAMP(NANOS)` and round-trips `DateTime` at its 100 ns precision (pyarrow: `timestamp[ns]`). The generator needs a `Nanoseconds` unit mapped to it, plus its Arrow bridge (which assumes µs) | S |
| Decimal precision > 28 | generator + upstream | `System.Decimal` limit; would need a `BigInteger`-backed member type (Parquet.Net's plain encoder can't encode `BigInteger` today — generator rejects it with PARQ006) | M |

A suggested shape for the generator side is a single opt-in profile, so the default behaviour of the generator does
not change for non-Iceberg users:

```csharp
[ParquetSerializable(Profile = ParquetProfile.Iceberg)]   // diagnostics for INT96, TimeSpan, unsigned, missing IDs
public partial record Event
{
    [ParquetColumn("id", Id = 1)] public long Id { get; init; }
    [ParquetColumn("event_ts", Id = 2)]
    [ParquetTimestamp(ParquetTimestampUnit.Microseconds, AdjustedToUtc = true)] public DateTime EventTs { get; init; }
    [ParquetColumn("day", Id = 3)] public DateOnly Day { get; init; }          // INT32 DATE under the profile
}
// generated: Event.IcebergSchemaJson, writer returns per-column stats (+ optional theta sketch feed)
```

## The patch

`docs/upstream/parquet-sourcegenerator-iceberg-columns.patch` (against `main` at `216f6fc`, not yet proposed
upstream) closes the first three rows:

- `ParquetColumnAttribute.FieldId` (default -1 = none) → `DataField.FieldId` on the Parquet.Net 6 backend. The
  legacy backend leaves it out, because Parquet.Net 4.x has no field IDs.
- `ParquetTimestampAttribute.AdjustedToUtc` (default true). False emits
  `DateTimeFormat.Timestamp, isAdjustedToUTC: false, unit: Micros`. True keeps the exact output of earlier releases.
- `DateOnly` → `DateTimeFormat.Date` (INT32 DATE).
- `PublicAPI.Unshipped.txt` lines, a `docs/api/LEDGER.md` entry, and `IcebergColumnTests` (field IDs only where
  declared, the UTC flag, INT32 DATE, round-trip with nulls).

Verified three ways:

- The generator's suite: 1779 passed. The 15 failures are tests reading benchmark datasets that are Git LFS
  pointers in this checkout; they fail identically without the patch.
- pyarrow reads `field_id` 1–4, `timestamp[us]`, `timestamp[us, tz=UTC]` and `date32`.
- An end-to-end check: a generated file whose column names differ from the table's is adopted by
  `ParquetImport` through **field IDs alone (no name mapping)**, and its `timestamp`, `timestamptz` and `date`
  values match. Once a release carries it, the `Gap_no_field_ids_requires_a_name_mapping` and
  `Gap_timestamps_are_always_utc_adjusted` tests here flip to compatible.

## What iceberg-dotnet did on its side

- `NameMapping` (spec "Column Projection"): build from schema, aliases for renames, JSON round-trip; the reader
  resolves ID-less files through it.
- `ParquetImport`: the type checker above, required/null checks, single-partition check, metrics.
- Decimal (INT32/INT64/FIXED read; ≤18 write) and UUID columns in `ParquetFiles` — the generator writes both
  correctly, which exposed that our reader could not read *required* UUID columns (fixed).

## Why not switch iceberg-dotnet to Parquet.Net + the generator entirely

The library's own writer (ParquetSharp) must handle *dynamic* schemas (any table, any evolution) and nested types
with group field IDs; a compile-time generator is for *static* models. They are complementary: the generator is the
fast typed path for producers with a known model; ParquetSharp stays the general engine. Revisit if Parquet.Net
gains group field IDs.
