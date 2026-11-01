# Enums

[Back to the type matrix](../types.md) · Status: ✗

`DataField<MyEnum>` can be declared, but writing it throws `InvalidOperationException: type MyEnum is not supported`;
the core API has no encoder for enums.

## Why

An enum has two natural representations, and neither is obviously right:

- its underlying integer (INT32 with an INTEGER annotation). Compact, but the file means nothing without the enum,
  and reordering the enum changes the data;
- its name (BYTE_ARRAY with the ENUM or STRING annotation). Self-describing, and what Spark and parquet-mr write for
  Avro and Thrift enums.

Under decision 9, a type with two representations is asked for, not guessed.

## What to use

Convert at the edge: write `(int)value` to a `DataField<int>`, or `value.ToString()` to a `DataField<string>`.
Columns that other writers annotated ENUM read as `string`.
