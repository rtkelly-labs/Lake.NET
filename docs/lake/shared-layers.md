# Shared layers

**Status:** target state. Covers what Iceberg, DuckLake and Delta share, where each shared piece lives, how it
gets extracted (phase 3), and the plan for typed access across formats. Package names are in
[packages.md](packages.md).

## What the formats share

| Concern | Iceberg | DuckLake | Delta | Shared home |
|---|---|---|---|---|
| Data files | Parquet, `field_id` on every node | Parquet, `field_id` = `column_id` | Parquet, `field_id` under column mapping mode `id` | `Lake.Data` |
| Column resolution | Field ID | Column ID | Field ID (`id` mode), physical name (`name` mode), logical name (no mapping) | `Lake.Data` projection by ID, plus a name path for Delta |
| File statistics | Manifest metrics (binary bounds) | `ducklake_file_column_stats` (text) | `add.stats` JSON | `Lake.Core` evaluators over typed statistics. `MetricsEvaluator.MightMatch` already accepts any source |
| Row-level deletes | Position deletes, equality deletes, v3 DVs in Puffin | Position delete files, 1.1 Puffin DVs, inlined deletes | DVs (`RoaringBitmapArray`, in `.bin` files or inline as Z85) | Roaring64 in `Lake.Core`, the container in `Lake.Puffin` |
| Commit atomicity | Catalog compare-and-swap of a metadata pointer | One SQL transaction | Put-if-absent of `_delta_log/N.json` in storage | `Lake.IO` conditional create (Iceberg and Delta); DuckLake uses its database |
| Row lineage | v3 `_row_id`, `_last_updated_sequence_number` | Row IDs kept through UPDATE | Row tracking (`baseRowId`, `defaultRowCommitVersion`) | Each format's own, for now |
| Variant / geo | v3 types | — | Table features | The Parquet variant encoding, once built, in `Lake.Data` |
| Telemetry | `IcebergTelemetry` | `DuckLakeTelemetry` | — | Helpers in `Lake.Core`; each format has its own source name ([D3](decisions.md#d3)) |

**Assumption:** the Delta column comes from the Delta protocol and hasn't been checked in this repository.
Iceberg's v3 deletion-vector blob was designed to match Delta's DV byte layout, which is why Roaring64 belongs in
`Lake.Core` and not `Lake.Puffin`. A test against Delta's own DV test files must confirm this before anything
depends on it ([delta.md](delta.md)).

The type model is not shared yet: see [D1](decisions.md#d1).

## Phase 3: extraction

This is the graduation step `docs/ducklake.md` sets for DuckLake ("move the format-neutral Parquet layer out of
`Iceberg.Net.Data`"), extended to the rest of the shared code. Each step is its own pull request, and every
suite has to pass unchanged on both Parquet backends.

1. **`Lake.Data`.** Move the Parquet data plane out of `Lake.Iceberg.Data`. `PositionDeleteFiles` stays in
   Iceberg because it hard-codes Iceberg's reserved field IDs; DuckLake already has its own reader.
2. **`Lake.Core`.** Move expressions and evaluators, physical values, `Corruption` (there are copies in both Core
   and Puffin today; keep one) and telemetry helpers, plus the type model as [D1](decisions.md#d1) decides.
3. **`Lake.DuckLake`** references only the shared layers, so it no longer pulls in Avro, the Iceberg catalogs or
   the Iceberg metadata model.
4. **A dependency-direction test** reads every `src/**/*.csproj` and fails if a shared layer references a format,
   or if one format references another.
5. **Optionally, merge the Iceberg packages** into one ([D4](decisions.md#d4)).

## Typed access

Today there are two typed layers:

- `Lake.Iceberg.Linq` and `Lake.Iceberg.EntityFrameworkCore` translate queries into Iceberg scans and writes.
- DuckLake has its own: `DuckLakeClr`, and `ReadAsync<T>` with a compiled record binder.

The target is one model-mapping layer over a shared scan and row contract, with each format providing its own
scan and write. It is only worth building once two formats are stable, and only for what they genuinely share:
projection, filters and row mapping. Format-specific behaviour, such as Iceberg branches or DuckLake inlining,
stays in each format's own API. Until then, both typed layers stay where they are.
