# Strings and binary

[Back to the type matrix](../types.md) · Status: write ✓, read ✓, with the exceptions below

## Mapping

| .NET | Declare with | Physical type | Annotation |
|---|---|---|---|
| `string` | `DataField<string>` | BYTE_ARRAY, UTF-8 | STRING, UTF8 |
| `byte[]` | `DataField<byte[]>` | BYTE_ARRAY | none |
| `byte[]` of one length | a field with `FixedLength`, or `RawDataField` | FIXED_LEN_BYTE_ARRAY(n) | none |

A field of `string` or `byte[]` has `ClrType` `ReadOnlyMemory<char>` or `ReadOnlyMemory<byte>`; the `string` and
`byte[]` overloads of `WriteAsync` and `ReadAsync` convert. Reading as `ReadOnlyMemory<char>` avoids a string per value.

## Reading other writers' columns

| Annotation | Reads as | Why |
|---|---|---|
| STRING, ENUM, UTF8 | `string` | |
| none | `byte[]`, or `string` with `TreatByteArrayAsString` | Some older writers left strings unannotated |
| JSON | `byte[]`, the UTF-8 text | ⚠ No JSON type is chosen for you. Decode with `System.Text.Json` |
| BSON | `byte[]` | ⚠ Decode with a BSON library |
| GEOMETRY, GEOGRAPHY | `byte[]`, well-known binary | ⚠ Bounding-box statistics are read (`lake(0048)`'s geometry checks). Decode with a geometry library |

The annotation is still on the field (`DataField.SchemaElement.LogicalType`), so a caller can tell JSON from plain
bytes.

## Tests

Upstream's string and binary tests, the conformance harness's string kinds (empty, ASCII, BMP private use,
supplementary characters), binary and fixed-length kinds against parquet-cpp, and the native AOT suite (strings,
UTF-8 bytes, JSON and geometry files).
