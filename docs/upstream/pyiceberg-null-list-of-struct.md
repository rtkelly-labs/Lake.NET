# PyIceberg: a null `list<struct>` reads as an empty list (proposed ticket)

**Status:** proposed ticket, not filed (see [README.md](README.md)). Found by `ShadowCompareTests.Fingerprints_agree_with_pyiceberg_for_nested_types`
on PyIceberg 0.12.0 / pyarrow; Spark + Iceberg Java read the same files correctly
(`SparkCrossEngineTests.Spark_reads_the_same_nested_values`), and so does pyarrow reading the Parquet file directly.

## Summary

Scanning a table whose optional `list<struct<...>>` column holds a null returns `[]` for that row instead of
`None`. Lists of primitives are not affected.

## Cause

`ArrowProjectionVisitor.list` in `pyiceberg/io/pyarrow.py` rebuilds struct-element lists to work around
apache/arrow#38809:

```python
if isinstance(value_array, pa.StructArray):
    # This can be removed once this has been fixed:
    # https://github.com/apache/arrow/issues/38809
    list_array = pa.LargeListArray.from_arrays(list_array.offsets, value_array)
```

`from_arrays(offsets, values)` without a `mask` gives every slot a valid list, and a null list's offsets span zero
values, so it becomes empty. Passing the validity through keeps it:

```python
list_array = pa.LargeListArray.from_arrays(list_array.offsets, value_array, mask=list_array.is_null())
```

## Reproduction

1. Write a Parquet data file with schema `id long, events list<struct<at long>>` where row 0's `events` is null
   (pyarrow: `pa.array([None, [{"at": 1}]], pa.list_(pa.struct([("at", pa.int64())])))`, field IDs set).
2. Add it to an Iceberg table (`add_files`) and `table.scan().to_arrow()`.
3. Expected `events[0] is None`; actual `[]`.

## Our workaround

None needed in our code: our files are correct. The PyIceberg cross-check avoids null `list<struct>` values and
Spark checks them instead.
