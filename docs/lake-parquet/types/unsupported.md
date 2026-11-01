# Other .NET types

[Back to the type matrix](../types.md) · Status: ✗

Each throws `NotSupportedException` when the field is declared.

## char

Parquet has no character type. Store a `char` as a `string` (BYTE_ARRAY, STRING) if it is text, or as `ushort` (its
UTF-16 code unit) if it is a code. A `char` alone cannot hold a supplementary character.

## nint and nuint

Their size depends on the process (32 or 64 bits), so a file would mean different things on different machines.
Store `long` or `ulong`.

## Memory&lt;T&gt;, Span&lt;T&gt;, object

`ReadOnlyMemory<char>` and `ReadOnlyMemory<byte>` are the field types behind `string` and `byte[]` (see
[strings and binary](strings-and-binary.md)). Other memory types and `object` are not column types. Use the element
type.
