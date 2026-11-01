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

## VARIANT

A VARIANT column is a group of a `metadata` and a `value` BYTE_ARRAY, plus shredded leaves. Each reads as a leaf;
there is no VARIANT decoder in the package.

## Tests

`Types/NestedTypedApiTest`, upstream's `ListTest`, `StructureTest` and `RepeatableFieldsTest`, the conformance
harness's three-level lists, structs and maps against parquet-cpp, and the native AOT suite (`api`: nested typed
columns).
