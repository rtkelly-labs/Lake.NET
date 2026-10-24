using Iceberg.Net.Expressions;
using Iceberg.Net.Manifests;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// The typed column-statistics model (<see cref="ColumnStats"/>, <see cref="ContentStats"/>): lossless conversion from
/// and to the v1–v3 metrics maps for every primitive type, using the spec's serialized bytes as the vectors, and proof
/// that pruning decodes bounds exactly as it did on the raw maps.
/// </summary>
[Spec("Field-level Metrics and Statistics")]
public sealed class ColumnStatsTests
{
    private static byte[] Hex(string hex) => Convert.FromHexString(hex.Replace(" ", ""));

    /// <summary>Type, typed lower and upper bound, and their serialized bytes per spec Appendix D.</summary>
    public static TheoryData<string, IcebergType, object, object, string, string> Vectors() => new()
    {
        { "boolean", PrimitiveType.Boolean, false, true, "00", "01" },
        { "int", PrimitiveType.Int, -1, 1, "ffffffff", "01000000" },
        { "long", PrimitiveType.Long, long.MinValue, 34L, "0000000000000080", "2200000000000000" },
        { "float", PrimitiveType.Float, -0.0f, 1.0f, "00000080", "0000803f" },
        { "double", PrimitiveType.Double, -0.0, 1.5, "0000000000000080", "000000000000f83f" },
        { "decimal(9,2)", new DecimalType(9, 2), new IcebergDecimal(-1, 2), new IcebergDecimal(1234, 2), "ff", "04d2" },
        { "decimal(38,0)", new DecimalType(38, 0), new IcebergDecimal(-128, 0), new IcebergDecimal(32768, 0), "80", "008000" },
        { "date", PrimitiveType.Date, IcebergTime.DaysFromIsoDate("1969-12-31"), IcebergTime.DaysFromIsoDate("2017-11-16"), "ffffffff", "4e440000" },
        { "time", PrimitiveType.Time, 0L, 81068123456L, "0000000000000000", "406509e012000000" },
        { "timestamp", PrimitiveType.Timestamp, -1L, 1510871468123456L, "ffffffffffffffff", "40a5282d215e0500" },
        { "timestamptz", PrimitiveType.TimestampTz, 0L, 1510871468123456L, "0000000000000000", "40a5282d215e0500" },
        { "timestamp_ns", PrimitiveType.TimestampNs, 1L, 1510871468123456789L, "0100000000000000", "1585c56698b1f714" },
        { "timestamptz_ns", PrimitiveType.TimestampTzNs, 0L, 1510871468123456789L, "0000000000000000", "1585c56698b1f714" },
        { "string", PrimitiveType.String, "iceberg", "\U0001F600", "69636562657267", "f09f9880" },
        { "uuid", PrimitiveType.Uuid, Guid.Parse("00000000-0000-0000-0000-000000000001"), Guid.Parse("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
            "00000000000000000000000000000001", "f79c3e09677c4bbda4793f349cb785e7" },
        { "fixed[3]", new FixedType(3), new byte[] { 0, 0, 1 }, new byte[] { 0xff, 0xfe, 0xfd }, "000001", "fffefd" },
        { "binary", PrimitiveType.Binary, Array.Empty<byte>(), new byte[] { 0, 1, 2, 0xff }, "", "000102ff" },
        // v3: geo bounds are points x:y[:z[:m]] (spec "Bound serialization"), x:y:NaN:m when only z is unset.
        { "geometry xy", new GeometryType(), new GeoPoint(1, 2), new GeoPoint(3, 4),
            "000000000000f03f 0000000000000040", "0000000000000840 0000000000001040" },
        { "geometry xyz/xym", new GeometryType(), new GeoPoint(1, 2, 3), new GeoPoint(1, 2, null, 4),
            "000000000000f03f 0000000000000040 0000000000000840", "000000000000f03f 0000000000000040 000000000000f87f 0000000000001040" },
        { "geography xyzm", new GeographyType(), new GeoPoint(170, -10, 0, 1), new GeoPoint(-170, 10, 5, 2),
            "0000000000406540 00000000000024c0 0000000000000000 000000000000f03f", "00000000004065c0 0000000000002440 0000000000001440 0000000000000040" },
        // variant bounds are kept serialized (metadata + object; variant values are not implemented here).
        { "variant", PrimitiveType.Variant, new VariantBound(Hex("01 01 00 01 24 02 01 00 01 0c 05")), new VariantBound(Hex("01 01 00 01 24 02 01 00 01 0c 09")),
            "01 01 00 01 24 02 01 00 01 0c 05", "01 01 00 01 24 02 01 00 01 0c 09" },
    };

    private static DataFile FileWith(int id, byte[]? lower, byte[]? upper) => new()
    {
        FilePath = "f", RecordCount = 10, FileSizeInBytes = 100,
        ColumnSizes = new Dictionary<int, long> { [id] = 42 },
        ValueCounts = new Dictionary<int, long> { [id] = 10 },
        NullValueCounts = new Dictionary<int, long> { [id] = 1 },
        NanValueCounts = new Dictionary<int, long> { [id] = 2 },
        LowerBounds = lower is null ? null : new Dictionary<int, byte[]> { [id] = lower },
        UpperBounds = upper is null ? null : new Dictionary<int, byte[]> { [id] = upper },
    };

    private static void AssertSameMaps(DataFile expected, DataFile actual)
    {
        static void Same<T>(IReadOnlyDictionary<int, T>? a, IReadOnlyDictionary<int, T>? b, Func<T, T, bool> eq)
        {
            Assert.Equal(a?.Count ?? 0, b?.Count ?? 0);
            foreach (var (k, v) in a ?? new Dictionary<int, T>()) Assert.True(b!.TryGetValue(k, out var w) && eq(v, w), $"field {k}");
        }
        Same(expected.ColumnSizes, actual.ColumnSizes, (x, y) => x == y);
        Same(expected.ValueCounts, actual.ValueCounts, (x, y) => x == y);
        Same(expected.NullValueCounts, actual.NullValueCounts, (x, y) => x == y);
        Same(expected.NanValueCounts, actual.NanValueCounts, (x, y) => x == y);
        Same(expected.LowerBounds, actual.LowerBounds, (x, y) => x.AsSpan().SequenceEqual(y));
        Same(expected.UpperBounds, actual.UpperBounds, (x, y) => x.AsSpan().SequenceEqual(y));
    }

    [Spec("Binary single-value serialization")]
    [Spec("Bound serialization")]
    [Theory]
    [MemberData(nameof(Vectors))]
    public void Spec_bytes_decode_to_typed_bounds_and_back(string name, IcebergType type, object lower, object upper, string lowerHex, string upperHex)
    {
        var file = FileWith(7, Hex(lowerHex), Hex(upperHex));
        var stats = ContentStats.FromDataFile(file, id => id == 7 ? type : null);
        var s = Assert.Single(stats.Fields.Values);
        Assert.Equal(new ColumnStats
        {
            FieldId = 7, Type = type, LowerBound = lower, UpperBound = upper,
            ValueCount = 10, NullValueCount = 1, NanValueCount = 2, ColumnSize = 42,
        }, s);
        Assert.Null(s.TightBounds); // v1-v3 cannot say
        Assert.Null(s.RawLowerBound); // canonical bytes: nothing kept aside
        Assert.Null(s.RawUpperBound);
        Assert.Equal(Hex(lowerHex), BoundSerialization.ToBytes(type, lower));
        Assert.Equal(Hex(upperHex), BoundSerialization.ToBytes(type, upper));
        AssertSameMaps(file, stats.ApplyTo(file with { LowerBounds = null, ColumnSizes = null }));
        Assert.False(string.IsNullOrEmpty(name));
    }

    [Fact]
    public void Unknown_columns_carry_counts_but_no_bounds()
    {
        var file = FileWith(3, null, null) with { NanValueCounts = null };
        var s = ContentStats.FromDataFile(file, _ => PrimitiveType.Unknown)[3]!;
        Assert.Equal((10L, 1L, (object?)null), (s.ValueCount!.Value, s.NullValueCount!.Value, s.LowerBound));
        AssertSameMaps(file, ContentStats.FromDataFile(file, _ => PrimitiveType.Unknown).ApplyTo(file));
    }

    /// <summary>Bytes the typed value cannot reproduce are kept verbatim, so the conversion is still lossless.</summary>
    [Theory]
    [InlineData("long from a promoted int column", "long", "05000000", 5L)]
    [InlineData("double from a promoted float column", "double", "0000803f", 1.0)]
    [InlineData("decimal with a redundant sign byte", "decimal", "0004d2", null)]
    [InlineData("true as 0x02", "boolean", "02", true)]
    [InlineData("string truncated inside a UTF-8 sequence", "string", "61f09f98", null)]
    [InlineData("int of the wrong width", "int", "0102", null)]
    [InlineData("geo point of the wrong width", "geometry", "0000000000000000", null)]
    public void Non_canonical_bounds_round_trip_byte_for_byte(string why, string typeName, string hex, object? decoded)
    {
        IcebergType type = typeName switch
        {
            "long" => PrimitiveType.Long, "double" => PrimitiveType.Double, "decimal" => new DecimalType(9, 2),
            "boolean" => PrimitiveType.Boolean, "string" => PrimitiveType.String, "int" => PrimitiveType.Int, _ => new GeometryType(),
        };
        var file = FileWith(1, Hex(hex), Hex(hex));
        var stats = ContentStats.FromDataFile(file, _ => type);
        var s = stats[1]!;
        Assert.Equal(Hex(hex), s.RawLowerBound);
        if (decoded is not null) Assert.Equal(decoded, s.LowerBound);
        if (typeName == "decimal") Assert.Equal(new IcebergDecimal(1234, 2), s.LowerBound);
        AssertSameMaps(file, stats.ApplyTo(file));
        Assert.False(string.IsNullOrEmpty(why));
    }

    [Fact]
    public void Fields_missing_from_the_schema_keep_their_bytes()
    {
        var file = FileWith(99, [1, 2, 3], [4, 5, 6]);
        var stats = ContentStats.FromDataFile(file, new Schema(NestedField.Optional(1, "a", PrimitiveType.Int)));
        Assert.Null(stats[99]!.Type);
        Assert.Null(stats[99]!.LowerBound);
        AssertSameMaps(file, stats.ApplyTo(file));
    }

    [Fact]
    public void Metrics_for_many_fields_round_trip_and_absent_maps_stay_absent()
    {
        var schema = new Schema(0,
        [
            NestedField.Require(1, "id", PrimitiveType.Long),
            NestedField.Optional(2, "x", PrimitiveType.Double),
            NestedField.Optional(3, "s", PrimitiveType.String),
        ]);
        var file = new DataFile
        {
            FilePath = "f", RecordCount = 3, FileSizeInBytes = 1,
            ValueCounts = new Dictionary<int, long> { [1] = 3, [2] = 3, [3] = 3 },
            NullValueCounts = new Dictionary<int, long> { [2] = 1 },
            LowerBounds = new Dictionary<int, byte[]> { [1] = Conversions.ToBytes(PrimitiveType.Long, 1L), [3] = "a"u8.ToArray() },
            UpperBounds = new Dictionary<int, byte[]> { [1] = Conversions.ToBytes(PrimitiveType.Long, 9L) },
        };
        var stats = ContentStats.FromDataFile(file, schema);
        Assert.Equal([1, 2, 3], stats.Fields.Keys);
        var back = stats.ApplyTo(file with { ValueCounts = null });
        AssertSameMaps(file, back);
        Assert.Null(back.ColumnSizes);
        Assert.Null(back.NanValueCounts);
    }

    // ---- Pruning decodes bounds the same way through the typed model ----

    private static readonly Schema Wide = new(0,
    [
        NestedField.Optional(1, "b", PrimitiveType.Boolean),
        NestedField.Optional(2, "i", PrimitiveType.Int),
        NestedField.Optional(3, "l", PrimitiveType.Long),
        NestedField.Optional(4, "f", PrimitiveType.Float),
        NestedField.Optional(5, "d", PrimitiveType.Double),
        NestedField.Optional(6, "dec", new DecimalType(9, 2)),
        NestedField.Optional(7, "dt", PrimitiveType.Date),
        NestedField.Optional(8, "ts", PrimitiveType.TimestampNs),
        NestedField.Optional(9, "s", PrimitiveType.String),
        NestedField.Optional(10, "u", PrimitiveType.Uuid),
        NestedField.Optional(11, "bin", PrimitiveType.Binary),
    ]);

    private static IEnumerable<(DataFile File, Expression Filter)> PruningCases()
    {
        var values = new (string Col, object Lo, object Hi, object[] Probes)[]
        {
            ("b", false, false, [true, false]),
            ("i", -5, 5, [-6, -5, 0, 5, 6]),
            ("l", 10L, 20L, [9L, 10L, 15L, 20L, 21L]),
            ("f", -0.0f, 2.5f, [-1f, 0f, 2.5f, 3f]),
            ("d", 1.0, 1.0, [0.5, 1.0, 1.5]),
            ("dec", new IcebergDecimal(100, 2), new IcebergDecimal(250, 2), [new IcebergDecimal(99, 2), new IcebergDecimal(100, 2), new IcebergDecimal(251, 2)]),
            ("dt", 100, 200, [99, 100, 200, 201]),
            ("ts", 1000L, 2000L, [999L, 1500L, 2001L]),
            ("s", "apple", "pear", ["aardvark", "apple", "banana", "pear", "zebra"]),
            ("u", Guid.Parse("10000000-0000-0000-0000-000000000000"), Guid.Parse("80000000-0000-0000-0000-000000000000"),
                [Guid.Parse("00000000-0000-0000-0000-000000000001"), Guid.Parse("7fffffff-0000-0000-0000-000000000000"), Guid.Parse("f0000000-0000-0000-0000-000000000000")]),
            ("bin", new byte[] { 1 }, new byte[] { 3 }, [new byte[] { 0 }, new byte[] { 2 }, new byte[] { 4 }]),
        };
        foreach (var (col, lo, hi, probes) in values)
        {
            var field = Wide.FindField(col)!;
            foreach (var nulls in new long?[] { 0, 2, 4, null })
            {
                var file = new DataFile
                {
                    FilePath = "f", RecordCount = 4, FileSizeInBytes = 1,
                    ValueCounts = new Dictionary<int, long> { [field.Id] = 4 },
                    NullValueCounts = nulls is { } n ? new Dictionary<int, long> { [field.Id] = n } : null,
                    LowerBounds = nulls == 4 ? null : new Dictionary<int, byte[]> { [field.Id] = Conversions.ToBytes(field.Type, lo) },
                    UpperBounds = nulls == 4 ? null : new Dictionary<int, byte[]> { [field.Id] = Conversions.ToBytes(field.Type, hi) },
                };
                yield return (file, Expr.IsNull(col));
                yield return (file, Expr.NotNull(col));
                foreach (var p in probes)
                {
                    yield return (file, Expr.Equal(col, p));
                    yield return (file, Expr.NotEqual(col, p));
                    yield return (file, Expr.LessThan(col, p));
                    yield return (file, Expr.LessThanOrEqual(col, p));
                    yield return (file, Expr.GreaterThan(col, p));
                    yield return (file, Expr.GreaterThanOrEqual(col, p));
                    yield return (file, Expr.In(col, p, lo));
                    if (p is string s) yield return (file, Expr.StartsWith(col, s[..2]));
                }
            }
        }
    }

    /// <summary>
    /// Mutation-style check of the move: for every type and predicate, the evaluator over the raw maps, over the typed
    /// <see cref="ContentStats"/>, and an independent oracle (decode with <see cref="Conversions.FromBytes"/>, the
    /// pre-model path) agree on what may match.
    /// </summary>
    [Spec("Scan Planning")]
    [Fact]
    public void Pruning_on_typed_stats_agrees_with_pruning_on_the_maps()
    {
        var checkedCases = 0;
        var pruned = 0;
        foreach (var (file, filter) in PruningCases())
        {
            var evaluator = new MetricsEvaluator(Binder.Bind(Wide, filter));
            var onMaps = evaluator.MightMatch(file);
            var typed = ContentStats.FromDataFile(file, Wide);
            Assert.Equal(onMaps, evaluator.MightMatch(file.RecordCount, typed));
            // Per-field callback form (the row-group path).
            Assert.Equal(onMaps, evaluator.MightMatch(file.RecordCount, (id, type) => ColumnStats.FromMetricsMaps(file, id, type)));
            checkedCases++;
            if (!onMaps) pruned++;
        }
        Assert.True(checkedCases > 300 && pruned > 50, $"{checkedCases} cases, {pruned} pruned");
    }

    /// <summary>The typed model decodes each bound exactly as <see cref="Conversions.FromBytes"/> does (the old path).</summary>
    [Spec("Binary single-value serialization")]
    [Fact]
    public void Typed_bounds_equal_the_single_value_decoding()
    {
        foreach (var (file, _) in PruningCases())
        {
            foreach (var (id, bytes) in file.LowerBounds ?? new Dictionary<int, byte[]>())
            {
                var type = Wide.FindType(id)!;
                var s = ColumnStats.FromMetricsMaps(file, id, type);
                var old = Conversions.FromBytes(type, bytes);
                if (old is byte[] ob) Assert.Equal(ob, (byte[])s.LowerBound!);
                else Assert.Equal(old, s.LowerBound);
                Assert.Equal(Conversions.FromBytes(type, file.UpperBounds![id]) is byte[] ub ? ub : null, s.UpperBound as byte[]);
            }
        }
    }

    [Fact]
    public void Geo_and_variant_bounds_are_never_used_to_prune()
    {
        var schema = new Schema(NestedField.Optional(1, "g", new GeometryType()), NestedField.Optional(2, "s", PrimitiveType.String));
        var file = FileWith(1, BoundSerialization.ToBytes(new GeometryType(), new GeoPoint(0, 0)), BoundSerialization.ToBytes(new GeometryType(), new GeoPoint(1, 1)));
        var stats = ContentStats.FromDataFile(file, schema);
        Assert.IsType<GeoPoint>(stats[1]!.LowerBound);
        Assert.True(new MetricsEvaluator(Binder.Bind(schema, Expr.IsNull("s"))).MightMatch(file.RecordCount, stats));
    }
}
