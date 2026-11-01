# Half

[Back to the type matrix](../types.md) · Status: write ✓, read ✓

| Declare with | Physical type | Annotation |
|---|---|---|
| `DataField<Half>` | FIXED_LEN_BYTE_ARRAY(2), the IEEE 754 binary16 bits, little-endian | FLOAT16 |

Bit for bit, NaN payloads and -0 included (`lake(0059)`). Statistics follow the floating-point rules: NaN is never a
bound but is counted, and a zero minimum is written as -0 and a zero maximum as +0. The two bytes stay readable through
the physical type (`byte[]`). Upstream could not open a file with a FLOAT16 column at all (fixed in `lake(0052)`).

Tests: `Types/NativeTypesTest`, `Reader/Float16Test`, the conformance harness's `float16` kind against parquet-cpp in
both directions, and the native AOT suite.
