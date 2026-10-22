# Upstream proposal: Parquet field IDs on group fields (Parquet.Net)

**Target:** [aloneguid/parquet-dotnet](https://github.com/aloneguid/parquet-dotnet) · **Against:** `master` @ `6cdd7de` (2026-08-25), package 6.1.0
**Status here:** drafted and prototyped locally; **not filed**. The text under [Issue](#issue) is ready to paste.
**Why we care:** Iceberg resolves every column by field ID, including struct, list and map columns. Without IDs on
group nodes, nothing built on Parquet.Net (including [Parquet.SourceGenerator](../parquet-sourcegenerator.md)) can
write nested Iceberg columns. This is the one gap in that table that is blocked upstream rather than in the generator.

## Is it supported today? Partly, and the rest is small

| Piece | State in 6.1.0 / master |
|---|---|
| Thrift `SchemaElement.field_id` (format field 9) | ✅ modelled for **every** element and serialised (`Meta/Parquet.cs`) |
| `DataField.FieldId` → written | ✅ since #632 (`SchemaEncoder.Encode(DataField)`, line ~454) |
| `StructField` / `ListField` / `MapField` field ID | ❌ no property; their `Encode` overloads never set `FieldId` |
| Field ID populated **on read** | ❌ for all fields, leaves included: the decoder never copies `SchemaElement.FieldId` to the `Field` (it is only reachable through the low-level `Field.SchemaElement`) |

So the format support, Thrift model and writer plumbing all exist. Only the high-level schema API is missing, plus
three assignments in the encoder and one in the decoder.

## Minimal change

19 lines across 3 files (plus a test). It was prototyped on `master` and verified:

- **Parquet.Net's own suite:** 698 pass, meaning the baseline's 695 plus 3 new tests, with no regressions. The one
  failure is identical with and without the patch: `SimpleMapReadsWithPyArrow` shells out to a Python without pyarrow,
  so it is environmental.
- **pyarrow** reads `PARQUET:field_id` on every leaf *and* group of a file written with the patch.
- **PyIceberg's `pyarrow_to_schema`** derives the exact Iceberg schema (`1: id`, `2: location struct<3,4>`,
  `5: tags list<6>`, `7: attrs map<8,9>`) straight from the file, with no name mapping needed.

1. **Lift `FieldId` from `DataField` to `Field`.** Same name, type, default (-1) and semantics, so existing code
   `new DataField<long>("id") { FieldId = 1 }` compiles unchanged.
2. **Emit it for groups.** Set `FieldId` on the struct element, the outer list group and the outer map group. Not on
   the synthetic `list` / `key_value` repeated groups; this matches parquet-mr, Arrow C++ and Iceberg, where element,
   key and value IDs live on the child fields.
3. **Populate it on read** in `SchemaEncoder.Decode`, for leaves and groups.

```diff
--- a/src/Parquet/Schema/Field.cs
+++ b/src/Parquet/Schema/Field.cs
@@ public abstract class Field {
+    /// <summary>
+    /// Optional Parquet <c>field_id</c> (-1 when absent). Table formats such as Apache Iceberg and Delta column mapping
+    /// resolve columns by ID, and need it on group fields (struct, list, map) as well as on leaves.
+    /// The writer emits it when set; the reader populates it from the file.
+    /// See https://arrow.apache.org/docs/cpp/parquet.html#parquet-field-id
+    /// </summary>
+    public int FieldId { get; set; } = -1;

--- a/src/Parquet/Schema/DataField.cs
+++ b/src/Parquet/Schema/DataField.cs
-    private int _fieldId = -1;
-    public int FieldId { get => _fieldId; set => _fieldId = value; }

--- a/src/Parquet/Encodings/SchemaEncoder.cs
+++ b/src/Parquet/Encodings/SchemaEncoder.cs
@@ public static Field? Decode(List<SchemaElement> schema, ParquetOptions options, ref int index, out int ownedChildCount) {
         if(TryBuildDataField(se, options, out DataField? df)) {
+            if(se.FieldId != null)
+                df!.FieldId = se.FieldId.Value;
             f = df;
             index++;
             return f;
         }
         ...
+        if(f != null && se.FieldId != null)
+            f.FieldId = se.FieldId.Value;
+
         return f;
@@ private static void Encode(ListField listField, ...)
             RepetitionType = listField.IsNullable ? FieldRepetitionType.OPTIONAL : FieldRepetitionType.REQUIRED,
-            NumChildren = 1  //field container below
+            NumChildren = 1,  //field container below
+            FieldId = listField.FieldId == -1 ? null : listField.FieldId
@@ private static void Encode(MapField mapField, ...)
-            RepetitionType = FieldRepetitionType.OPTIONAL
+            RepetitionType = FieldRepetitionType.OPTIONAL,
+            FieldId = mapField.FieldId == -1 ? null : mapField.FieldId
@@ private static void Encode(StructField structField, ...)
-            RepetitionType = structField.IsNullable ? FieldRepetitionType.OPTIONAL : FieldRepetitionType.REQUIRED
+            RepetitionType = structField.IsNullable ? FieldRepetitionType.OPTIONAL : FieldRepetitionType.REQUIRED,
             // no logical or converted type annotations for structs
+            FieldId = structField.FieldId == -1 ? null : structField.FieldId
```

Test added to `src/Parquet.Test/Schema/FieldIdTest.cs`:

```csharp
[Fact]
public async Task Field_ids_on_groups_and_leaves_round_trip() {
    var schema = new ParquetSchema(
        new DataField<long>("id") { FieldId = 1 },
        new StructField("location",
            new DataField<double>("lat") { FieldId = 3 },
            new DataField<double>("lon") { FieldId = 4 }) { FieldId = 2 },
        new ListField("tags", new DataField<string>("element") { FieldId = 6 }) { FieldId = 5 },
        new MapField("attrs",
            new DataField<string>("key", nullable: false) { FieldId = 8 },
            new DataField<string>("value") { FieldId = 9 }) { FieldId = 7 });

    using var ms = new MemoryStream();
    await using(ParquetWriter writer = await ParquetWriter.CreateAsync(schema, ms)) { }

    ms.Position = 0;
    await using ParquetReader reader = await ParquetReader.CreateAsync(ms);
    Field[] f = reader.Schema.Fields.ToArray();
    Assert.Equal(1, f[0].FieldId);
    Assert.Equal(2, f[1].FieldId);
    Assert.Equal(new[] { 3, 4 }, ((StructField)f[1]).Fields.Select(x => x.FieldId));
    Assert.Equal(5, f[2].FieldId);  Assert.Equal(6, ((ListField)f[2]).Item.FieldId);
    Assert.Equal(7, f[3].FieldId);  Assert.Equal(8, ((MapField)f[3]).Key.FieldId);  Assert.Equal(9, ((MapField)f[3]).Value.FieldId);
}

[Fact]
public async Task Fields_without_ids_read_back_as_minus_one() { /* struct + leaf written without IDs → -1 on read */ }
```

## Compatibility

| Concern | Assessment |
|---|---|
| Source compatibility | Unchanged: `DataField.FieldId` still resolves (now inherited) |
| Binary compatibility | Moving a member to a base class is allowed by .NET's breaking-change rules: member references resolve through the hierarchy, so assemblies compiled against 6.1.0 keep working. Worth one consumer check against a compiled 6.1 dependant (e.g. Parquet.SourceGenerator 0.0.3) before release |
| Behaviour on write | Only changes when a caller sets `FieldId` on a group; default -1 writes nothing, so files are byte-identical |
| Behaviour on read | **New:** `FieldId` is now populated from files that carry IDs (previously always -1, even for leaves). Code that relied on -1 after reading such files would see the real ID. This is arguably a fix, but it belongs in the release notes |
| `Equals` / `GetHashCode` | Unaffected: schema equality never compared `FieldId`, before or after |
| `Clone()` | `DataField.Clone` is `MemberwiseClone`, so the base property is copied |
| Serializer (`ParquetSerializer`) | Untouched; a `[ParquetFieldId]`-style attribute could follow separately |

## Issue

> **Title:** Support `field_id` on struct, list and map fields (and populate it when reading)
>
> **Problem.** `DataField.FieldId` (#632) lets writers set Parquet `field_id` on leaf columns, which is what Apache
> Iceberg, Delta Lake column mapping and Arrow's `PARQUET:field_id` use to resolve columns independently of names.
> Those formats also need `field_id` on **group** nodes: a struct column, the outer group of a list, and the outer
> group of a map. `StructField`, `ListField` and `MapField` have no way to carry it, so nested columns written by
> Parquet.Net cannot be resolved by ID: an ID-based reader (Iceberg Java, PyIceberg) finds no ID on the group and has
> to fall back to name mapping, which defeats the purpose of writing IDs. Separately, `FieldId` is never populated
> when *reading* a file, for leaves included; it is only reachable through `Field.SchemaElement.FieldId`.
>
> **Proposal.** Move `FieldId` from `DataField` up to `Field` (same name, default -1, so source-compatible). Emit it in
> `SchemaEncoder` for the struct element, the outer list group and the outer map group (not the synthetic
> `list`/`key_value` groups, matching parquet-mr and Arrow C++). Populate it in `SchemaEncoder.Decode` for every field.
>
> **Size.** About 19 lines across `Field.cs`, `DataField.cs` and `SchemaEncoder.cs`, plus a round-trip test. I have a
> working patch with tests: the full suite passes with no regressions, and pyarrow reads the IDs on every node. I'm
> happy to open a PR if the direction is welcome.
>
> **Out of scope.** A `ParquetSerializer` attribute for IDs; nanosecond timestamps.

## Related, separate follow-ups

| Gap | Why it's separate |
|---|---|
| Nanosecond timestamps (`TIMESTAMP(NANOS)`) | `DateTimeFormat` tops out at micros. Iceberg v3 `timestamp_ns` needs either a new format or a logical-type override on `DataField<long>`. A different design discussion |
| Reading IDs through `ParquetSerializer` / reflection | Consumers like Parquet.SourceGenerator only need the schema API; serializer support can follow |

## How we'd consume it

Once released, Parquet.SourceGenerator can emit `FieldId` for nested members. iceberg-dotnet's `ParquetImport` already
resolves by ID when a file has IDs (falling back to the name mapping otherwise), so the only change on our side would
be lifting its current "nested columns not supported" check.
