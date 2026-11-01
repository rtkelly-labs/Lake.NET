# Guid

[Back to the type matrix](../types.md) · Status: write ✓, read ✓

| Declare with | Physical type | Annotation |
|---|---|---|
| `DataField<Guid>` | FIXED_LEN_BYTE_ARRAY(16) | UUID |

The 16 bytes are in RFC 4122 order (big-endian), as the format requires and as Spark, pyarrow and DuckDB write them.
This is not the order of `Guid.ToByteArray()`, which mixes endianness. The library converts both ways, so
`00112233-4455-6677-8899-aabbccddeeff` is the same UUID in every reader.

A UUID-annotated column reads as `Guid`. An unannotated FIXED_LEN_BYTE_ARRAY(16) reads as `byte[]`.

Tests: the conformance harness's UUID kind against parquet-cpp, and the native AOT suite.
