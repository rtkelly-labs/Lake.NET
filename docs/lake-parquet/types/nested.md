# Arrays, lists, structs and maps

[Back to the type matrix](../types.md) · Status: write ⚠, read ⚠

The core API works on leaf columns. A list, struct or map is declared with `ListField`, `StructField` or `MapField`.
Each leaf is written and read as a flat run of values, with repetition levels to mark where each row's list starts.
There is no mapping from `List<T>`, arrays of objects or classes to nested columns in the package; that was the
reflection serializer's job, which the package leaves out (decision 6). Parquet.SourceGenerator generates it.

## Writing and reading a leaf

- A leaf under an optional parent is written from `T?[]` (a null is a missing parent or element) or, when nothing is
  missing, from `T[]` (`lake(0062)`; upstream threw "Definition levels are not ready yet").
- A list's leaf takes repetition levels: 0 starts a row, 1 continues its list.
- Reading into `T?[]`, `string?[]` or `byte[]?[]` with a repetition-level buffer gives one entry per value slot.
  A null is a null element, an empty list or a missing parent alike, anything below the column's maximum definition
  level (`lake(0062)`; upstream sized the buffer by rows and threw for lists).
- A leaf's buffer is the column chunk's value count (`num_values`), not the row count.

`DataField<int[]>` and `DataField<List<int>>` declare a legacy repeated column, with no LIST group around it.

## The offsets layer: NestedLayout

`Parquet.Data.NestedLayout` (decision 11, `lake(0079)`) turns a leaf's levels into Arrow's layout:

- one offsets array per repeated ancestor: a list's elements, a map's key-value pairs;
- one validity mask per optional ancestor: a null list, map or struct, or a null element;
- the count of non-null leaf values, which belong in order to the last layer's slots whose masks are all true.

`layout.ToLevels(...)` is the inverse, for `WriteAllPartsAsync`. A `List<List<int?>>` then assembles by walking
offset ranges:

```csharp
NestedLayout l = NestedLayout.FromLevels(schema, leaf, definitionLevels, repetitionLevels, entries);
for(int row = 0; row < l.SlotCounts[0]; row++)            // rows
    for(int i = l.Offsets[0][row]; i < l.Offsets[0][row + 1]; i++)   // outer lists' elements
        for(int j = l.Offsets[1][i]; j < l.Offsets[1][i + 1]; j++)   // inner lists' elements
            ... l.Validity[2].IsValid[j] ? packed[next++] : null ...
```

It works at any depth. It is proven against Arrow itself: for each leaf of a pyarrow file with lists of lists, a
map of lists and a list of structs holding a list, the offsets and validity match exactly the ones pyarrow builds.

Leaves written to one row group under a shared list, map or struct must agree. The writer refuses a leaf whose
levels describe different elements or nulls from a sibling's (`lake(0082)`), naming both columns.

**For Parquet.SourceGenerator:** consumers of Lake.Parquet can assemble and shred `List<List<T>>`, maps and nested
records through `NestedLayout` instead of emitting level arithmetic. That lifts its PARQ013 depth limit and its map
restriction. That change is the generator's.

## VARIANT

A VARIANT column is a group of a `metadata` and a `value` BYTE_ARRAY, plus shredded leaves. Each reads as a leaf;
there is no VARIANT decoder in the package.

## Tests

`Types/NestedTypedApiTest`, `Types/NestedLayoutTest`, `Types/NestedSiblingsTest`, upstream's `ListTest`, `StructureTest` and `RepeatableFieldsTest`, the conformance
harness's three-level lists, structs and maps against parquet-cpp, and the native AOT suite (`api`: nested typed
columns, NestedLayout on the pyarrow file).
