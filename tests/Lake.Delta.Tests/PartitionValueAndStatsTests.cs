using System.Text.Json.Nodes;

namespace Lake.Delta.Tests;

/// <summary>Partition value serialization, the schema parser, and <c>add.stats</c>.</summary>
public sealed class PartitionValueAndStatsTests
{
    private static DeltaType T(string name) => DeltaSchema.ParseType(JsonValue.Create(name), "t");

    public static TheoryData<string?, string, object?> Values() => new()
    {
        { null, "string", null },
        { "", "long", null }, // "" is null for every type
        { "", "string", null },
        { "a b/%", "string", "a b/%" },
        { "-42", "integer", -42 },
        { "7", "byte", (sbyte)7 },
        { "9000000000", "long", 9000000000L },
        { "1.5", "double", 1.5 },
        { "NaN", "double", double.NaN },
        { "true", "boolean", true },
        { "false", "boolean", false },
        { "12.000000000000000000", "decimal(38,18)", 12m },
        { "1970-01-01", "date", new DateOnly(1970, 1, 1) },
        { "2024-02-29 13:45:01", "timestamp", new DateTimeOffset(2024, 2, 29, 13, 45, 1, TimeSpan.Zero) },
        { "2024-02-29 13:45:01.123456", "timestamp", new DateTimeOffset(2024, 2, 29, 13, 45, 1, TimeSpan.Zero).AddTicks(1234560) },
        { "1970-01-01T00:00:00.123456Z", "timestamp", DateTimeOffset.UnixEpoch.AddTicks(1234560) },
        { "2024-02-29 13:45:01", "timestamp_ntz", new DateTime(2024, 2, 29, 13, 45, 1, DateTimeKind.Unspecified) },
    };

    [Theory]
    [MemberData(nameof(Values))]
    public void Partition_values_parse_per_type(string? text, string type, object? expected)
    {
        var value = PartitionValues.Parse(text, T(type), "c");
        Assert.Equal(expected, value);
        if (value is DateTime dt) Assert.Equal(DateTimeKind.Unspecified, dt.Kind);
    }

    [Fact]
    public void A_binary_partition_value_is_the_strings_bytes()
    {
        Assert.Equal(new byte[] { 1, 2, 3 }, PartitionValues.Parse("\u0001\u0002\u0003", DeltaPrimitiveType.Binary, "c"));
    }

    [Theory]
    [InlineData("x", "long")]
    [InlineData("yes", "boolean")]
    [InlineData("1970-13-01", "date")]
    [InlineData("300", "byte")]
    [InlineData("noon", "timestamp")]
    public void Malformed_partition_values_are_format_errors(string text, string type) =>
        Assert.Throws<InvalidDataException>(() => PartitionValues.Parse(text, T(type), "c"));

    [Fact]
    public void The_schema_parser_reads_every_type_and_column_mapping_metadata()
    {
        var schema = DeltaSchema.Parse("""
            {"type":"struct","fields":[
              {"name":"a","type":"decimal(38,18)","nullable":false,"metadata":{"delta.columnMapping.id":7,"delta.columnMapping.physicalName":"col-a"}},
              {"name":"b","type":{"type":"array","elementType":{"type":"map","keyType":"string","valueType":"timestamp_ntz","valueContainsNull":false},"containsNull":true},"nullable":true,"metadata":{}},
              {"name":"c","type":{"type":"struct","fields":[{"name":"d","type":"void","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}
            ]}
            """);
        Assert.Equal(new DeltaDecimalType(38, 18), schema.Fields[0].Type);
        Assert.False(schema.Fields[0].Nullable);
        Assert.Equal(7, schema.Fields[0].ColumnMappingId);
        Assert.Equal("col-a", schema.Fields[0].PhysicalName(ColumnMappingMode.Name));
        Assert.Equal("a", schema.Fields[0].PhysicalName(ColumnMappingMode.None));
        var map = Assert.IsType<DeltaMapType>(Assert.IsType<DeltaArrayType>(schema.Fields[1].Type).ElementType);
        Assert.Equal(DeltaPrimitiveType.TimestampNtz, map.ValueType);
        Assert.False(map.ValueContainsNull);
        Assert.Equal(DeltaPrimitiveType.Void, Assert.IsType<DeltaStructType>(schema.Fields[2].Type).Fields[0].Type);
    }

    [Theory]
    [InlineData("""{"type":"struct","fields":[{"name":"a","type":"varchar","nullable":true,"metadata":{}}]}""")]
    [InlineData("""{"type":"struct","fields":[{"name":"a","type":"decimal(39,0)","nullable":true,"metadata":{}}]}""")]
    [InlineData("""{"type":"struct","fields":[{"name":"a","type":"long"},{"name":"A","type":"long"}]}""")]
    [InlineData("""{"type":"array","elementType":"long","containsNull":true}""")]
    [InlineData("""{"type":"struct","fields":[{"name":"a"}]}""")]
    [InlineData("""{"type":"struct","fields":""")]
    public void Malformed_schemas_are_format_errors(string schema) => Assert.Throws<InvalidDataException>(() => DeltaSchema.Parse(schema));

    [Fact]
    public void A_variant_column_is_refused_as_the_variantType_feature()
    {
        var e = Assert.Throws<DeltaUnsupportedFeatureException>(() => DeltaSchema.Parse("""{"type":"struct","fields":[{"name":"v","type":"variant","nullable":true,"metadata":{}}]}"""));
        Assert.Equal(["variantType"], e.Features);
    }

    [Fact]
    public async Task Stats_expose_record_counts_bounds_and_null_counts()
    {
        var file = (await DeltaTable.OpenLocal(Fixtures.Dat("all_primitive_types")).GetSnapshotAsync()).Files.Single();
        var stats = file.Stats!;
        Assert.Equal(5, stats.NumRecords);
        Assert.True(stats.TightBounds); // absent means true
        Assert.Equal(0, stats.Min("int64")!.GetValue<long>());
        Assert.Equal(4, stats.Max("int64")!.GetValue<long>());
        Assert.Equal("1970-01-05", stats.Max("date32")!.GetValue<string>());
        Assert.Equal(0, stats.Nulls("binary"));
        Assert.Null(stats.Min("binary")); // no bounds recorded for binary

        var nested = (await DeltaTable.OpenLocal(Fixtures.Dat("nested_types")).GetSnapshotAsync()).Files.Single().Stats!;
        Assert.Equal(4.0, nested.Max("struct", "float64")!.GetValue<double>());
        Assert.Equal(0, nested.Nulls("struct", "bool"));
    }

    [Fact]
    public async Task Stats_after_a_DV_have_wide_bounds_and_physical_names_under_column_mapping()
    {
        var dv = (await DeltaTable.OpenLocal(Fixtures.Dat("deletion_vectors")).GetSnapshotAsync()).Files.Single();
        Assert.False(dv.Stats!.TightBounds);
        Assert.Equal(5, dv.Stats.NumRecords); // physical rows: the DV is not applied to numRecords

        var mapped = await DeltaTable.OpenLocal(Fixtures.Dat("column_mapping")).GetSnapshotAsync();
        var physical = mapped.Schema.Fields[0].PhysicalName(mapped.ColumnMapping);
        Assert.All(mapped.Files, f => Assert.NotNull(f.Stats!.Min(physical)));
        Assert.All(mapped.Files, f => Assert.Null(f.Stats!.Min("letter")));
    }

    [Fact]
    public async Task Without_json_stats_in_the_checkpoint_a_file_has_no_stats()
    {
        // stats_as_struct: writeStatsAsJson=false, so the checkpoint's add rows carry no stats string.
        var table = DeltaTable.OpenLocal(Fixtures.Dat("stats_as_struct"));
        var atCheckpoint = await table.GetSnapshotAsync(2);
        Assert.Null(Assert.Single(atCheckpoint.Files).Stats);
        Assert.Equal(5, Assert.Single((await table.GetSnapshotAsync()).Files).Stats!.NumRecords); // committed after it, as JSON
    }
}
