using System.Numerics;
using Iceberg.Net.Types;

namespace Iceberg.Net.TestSupport;

/// <summary>A schema with every flat primitive type and deterministic rows exercising nulls, NaN, signs and edge widths.</summary>
public static class SampleData
{
    public static readonly Schema AllTypes = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "flag", PrimitiveType.Boolean),
        NestedField.Optional(3, "i", PrimitiveType.Int),
        NestedField.Optional(4, "f", PrimitiveType.Float),
        NestedField.Optional(5, "d", PrimitiveType.Double),
        NestedField.Optional(6, "dec", new DecimalType(9, 2)),
        NestedField.Optional(7, "wide", new DecimalType(38, 6)),
        NestedField.Optional(8, "day", PrimitiveType.Date),
        NestedField.Optional(9, "at", PrimitiveType.Time),
        NestedField.Optional(10, "ts", PrimitiveType.Timestamp),
        NestedField.Optional(11, "tstz", PrimitiveType.TimestampTz),
        NestedField.Optional(12, "s", PrimitiveType.String),
        NestedField.Optional(13, "u", PrimitiveType.Uuid),
        NestedField.Optional(14, "b", PrimitiveType.Binary),
        NestedField.Optional(15, "fx", new FixedType(3)),
    ], identifierFieldIds: [1]);

    public static List<object?[]> AllTypeRows(int n) => Enumerable.Range(0, n).Select(i => new object?[]
    {
        (long)i, i % 3 == 0 ? null : i % 2 == 0, i % 5 == 0 ? null : i - 7, i % 4 == 0 ? float.NaN : i / 3f, i % 7 == 0 ? null : Math.E * i,
        new IcebergDecimal(i * 101 - 300, 2),
        i % 3 == 1 ? null : new IcebergDecimal(BigInteger.Pow(10, 37) * (i % 2 == 0 ? 1 : -1) + i, 6),
        18_000 + i, (long)i * 1_000_001, 1_700_000_000_000_000L + i, i % 6 == 0 ? null : -86_400_000_000L * i,
        i % 8 == 0 ? null : $"värde-{i}", i % 5 == 1 ? null : new Guid($"{i:x8}-1111-4222-8333-{i:x12}"),
        i % 4 == 2 ? null : Enumerable.Range(0, i % 6).Select(b => (byte)(b + i)).ToArray(),
        i % 9 == 0 ? null : new byte[] { (byte)i, 0, 0xff },
    }).ToList();
}
