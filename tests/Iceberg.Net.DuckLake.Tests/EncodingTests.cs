using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>Text encodings: statistics (spec "Type Encoding for Statistics"), changes_made, and DuckDB's inlined nested values.</summary>
public sealed class EncodingTests
{
    private static DuckLakePrimitive T(string name) => new(name);

    // The spec's own examples, one per type.
    [Theory]
    [Spec("ducklake: Type Encoding for Statistics")]
    [InlineData("boolean", "1")]
    [InlineData("int8", "42")]
    [InlineData("int16", "1000")]
    [InlineData("int32", "100000")]
    [InlineData("int64", "100000000")]
    [InlineData("float32", "3.14")]
    [InlineData("float64", "3.14159265358979")]
    [InlineData("decimal(7,2)", "12345.67")]
    [InlineData("date", "2024-01-15")]
    [InlineData("time", "12:30:00.123456")]
    [InlineData("timestamp", "2024-01-15 12:30:00.123456")]
    [InlineData("timestamptz", "2024-01-15 12:30:00.123456+00")]
    [InlineData("timestamp_ns", "2024-01-15 12:30:00.123456789")]
    [InlineData("varchar", "hello")]
    [InlineData("json", "{\"key\": \"value\"}")]
    [InlineData("uuid", "550e8400-e29b-41d4-a716-446655440000")]
    public void Spec_examples_parse_and_format_back(string type, string text)
    {
        Assert.True(DuckLakeValues.TryParse(T(type), text, out var value));
        Assert.Equal(text, DuckLakeValues.Format(T(type), value!));
    }

    [Theory]
    [Spec("ducklake: Type Encoding for Statistics")]
    [InlineData("float64", "inf", double.PositiveInfinity)]
    [InlineData("float64", "-inf", double.NegativeInfinity)]
    [InlineData("float32", "inf", float.PositiveInfinity)]
    public void Infinities_use_the_special_values(string type, string text, object value)
    {
        Assert.Equal(text, DuckLakeValues.Format(T(type), value));
        Assert.True(DuckLakeValues.TryParse(T(type), text, out var parsed));
        Assert.Equal(value, parsed);
    }

    [Fact]
    [Spec("ducklake: Type Encoding for Statistics")]
    public void NaN_is_never_a_bound_and_unknown_text_never_prunes()
    {
        Assert.Null(DuckLakeValues.Format(DuckLakeType.Float64, double.NaN));
        Assert.Null(DuckLakeValues.Format(DuckLakeType.Blob, new byte[] { 1 }));
        // Special values and other writers' spellings this reader does not order are "no bound", not an error.
        Assert.False(DuckLakeValues.TryParse(DuckLakeType.Date, "infinity", out _));
        Assert.False(DuckLakeValues.TryParse(DuckLakeType.Int32, "not a number", out _));
        Assert.False(DuckLakeValues.TryParse(DuckLakeType.Int8, "300", out _));
        // A bound with more digits than the type keeps must not be truncated (that could move an upper bound down).
        Assert.False(DuckLakeValues.TryParse(DuckLakeType.Timestamp, "2024-01-01 00:00:00.1234567", out _));
        Assert.False(DuckLakeValues.TryParse(DuckLakeType.Decimal(5, 1), "1.25", out _));
        Assert.True(DuckLakeValues.TryParse(DuckLakeType.Decimal(5, 1), "1.20", out var d));
        Assert.Equal(new IcebergDecimal(12, 1), d);
    }

    [Theory]
    [Spec("ducklake: Type Encoding for Statistics")]
    [InlineData("2024-01-15 12:30:00+00", 1705321800000000L)]
    [InlineData("2024-01-15 13:30:00+01", 1705321800000000L)]
    [InlineData("2024-01-15 07:00:00-05:30", 1705321800000000L)]
    [InlineData("2024-01-15T12:30:00Z", 1705321800000000L)]
    public void Timestamptz_offsets_are_applied(string text, long micros)
    {
        Assert.True(DuckLakeValues.TryParse(DuckLakeType.TimestampTz, text, out var v));
        Assert.Equal(micros, v);
    }

    [Fact]
    [Spec("ducklake: Type Encoding for Statistics")]
    public void Blob_bounds_read_hex_and_DuckDB_escapes()
    {
        Assert.True(DuckLakeValues.TryParse(DuckLakeType.Blob, "68656C6C6F", out var hex));
        Assert.Equal("hello"u8.ToArray(), hex);
        Assert.True(DuckLakeValues.TryParse(DuckLakeType.Blob, "\\x00\\xFFa", out var escaped));
        Assert.Equal(new byte[] { 0, 0xFF, (byte)'a' }, escaped);
    }

    [Fact]
    [Spec("ducklake: ducklake_snapshot_changes")]
    public void Changes_split_at_commas_outside_quoted_names()
    {
        var changes = DuckLakeChange.Parse("created_table:\"main\".\"a, \"\"b\"\"\",inserted_into_table:2,inline_flush:3");
        Assert.Equal(3, changes.Count);
        Assert.Equal(["main", "a, \"b\""], DuckLakeChange.SplitName(changes[0].Target));
        Assert.Equal(2, changes[1].TargetId);
        Assert.Equal("\"this \"\"table\"\" contains quotes\"", DuckLakeChange.QuoteName("this \"table\" contains quotes"));
        Assert.Empty(DuckLakeChange.Parse(null));
    }

    [Theory]
    [Spec("ducklake: ducklake_snapshot_changes")]
    [InlineData("created_table:\"unterminated")]
    [InlineData("no_kind_separator")]
    [InlineData(":2")]
    public void Malformed_changes_are_format_errors(string text) =>
        Assert.True(Corruption.IsFormatError(Record.Exception(() => DuckLakeChange.Parse(text))!));

    [Fact]
    [Spec("ducklake: Type Encoding for Data Inlining")]
    [Spec("ducklake: SQLite Catalog")]
    public void Inlined_nested_values_parse_against_their_type()
    {
        // Exactly what DuckDB 1.5 writes into a SQLite catalog's inlined data table.
        var list = DuckLakeType.List(DuckLakeType.Varchar);
        Assert.Equal(new List<object?> { "a, b", "it's", null, "", "NULL", "[x]", " sp ", "q\"d", "{y}", "a\\b" },
            DuckLakeNestedText.Parse(list, "['a, b', 'it\\'s', NULL, '', 'NULL', '[x]', ' sp ', 'q\"d', '{y}', a\\b]"));

        var st = DuckLakeType.Struct(("a", DuckLakeType.Int32), ("b", list), ("c", DuckLakeType.Struct(("x", DuckLakeType.Date))));
        var parsed = (object?[])DuckLakeNestedText.Parse(st, "{'a': 1, 'b': ['x, y', NULL], 'c': {'x': CAST('2024-01-02' AS DATE)}}")!;
        Assert.Equal(1, parsed[0]);
        Assert.Equal(new List<object?> { "x, y", null }, parsed[1]);
        Assert.Equal(IcebergTime.DaysFromDate(new DateOnly(2024, 1, 2)), ((object?[])parsed[2]!)[0]);

        var map = (Dictionary<object, object?>)DuckLakeNestedText.Parse(DuckLakeType.Map(DuckLakeType.Varchar, DuckLakeType.Varchar), "{k 1='v, 1', 'k=2'=NULL, ''=e}")!;
        Assert.Equal("v, 1", map["k 1"]);
        Assert.Null(map["k=2"]);
        Assert.Equal("e", map[""]);

        Assert.Equal(new List<object?> { new List<object?> { 1, 2 }, new List<object?>(), null },
            DuckLakeNestedText.Parse(DuckLakeType.List(DuckLakeType.List(DuckLakeType.Int32)), "[[1, 2], [], NULL]"));
        var floats = (List<object?>)DuckLakeNestedText.Parse(DuckLakeType.List(DuckLakeType.Float64), "[1.5, nan, inf]")!;
        Assert.True(double.IsNaN((double)floats[1]!));
        Assert.Equal(new object?[] { null, new List<object?>(), null },
            DuckLakeNestedText.Parse(st, "{'a': NULL, 'b': [], 'c': NULL}"));
    }

    [Theory]
    [Spec("ducklake: Type Encoding for Data Inlining")]
    [InlineData("[1, 2")]
    [InlineData("[1, x]")]
    [InlineData("{'a': 1}")]
    [InlineData("['unterminated]")]
    [InlineData("[1] trailing")]
    public void Malformed_inlined_values_are_format_errors(string text) =>
        Assert.Throws<FormatException>(() => DuckLakeNestedText.Parse(DuckLakeType.List(DuckLakeType.Int32), text));

    [Fact]
    public void Deeply_nested_inlined_values_are_bounded()
    {
        DuckLakeType type = DuckLakeType.Int32;
        for (var i = 0; i < 70; i++) type = DuckLakeType.List(type);
        Assert.Throws<FormatException>(() => DuckLakeNestedText.Parse(type, new string('[', 70) + new string(']', 70)));
    }

    [Theory]
    [Spec("ducklake: Primitive Types")]
    [InlineData("uint8")]
    [InlineData("int128")]
    [InlineData("interval")]
    [InlineData("timetz")]
    [InlineData("timestamp_s")]
    [InlineData("variant")]
    [InlineData("geometry")]
    public void Unsupported_types_fail_loudly(string type) =>
        Assert.Throws<NotSupportedException>(() => new DuckLakePrimitive(type));

    [Fact]
    [Spec("ducklake: Nested Types")]
    [Spec("ducklake: ducklake_column")]
    public void Nested_columns_get_preorder_ids_and_map_to_iceberg_fields()
    {
        long next = 1;
        var tags = DuckLakeColumn.Assign("tags", DuckLakeType.List(DuckLakeType.Int32), true, ref next);
        var st = DuckLakeColumn.Assign("st", DuckLakeType.Struct(("a", DuckLakeType.Int32), ("m", DuckLakeType.Map(DuckLakeType.Varchar, DuckLakeType.Int64))), true, ref next);
        // As DuckDB numbers them: parent first, then children depth-first.
        Assert.Equal([1L, 2L], tags.SelfAndDescendants().Select(c => c.Id));
        Assert.Equal([3L, 4L, 5L, 6L, 7L], st.SelfAndDescendants().Select(c => c.Id));
        Assert.Equal(["st", "a", "m", "key", "value"], st.SelfAndDescendants().Select(c => c.Name));
        var schema = DuckLakeTypes.ToIceberg([tags, st]);
        Assert.Equal(new ListType(2, PrimitiveType.Int, false), schema.Columns[0].Type);
        Assert.Equal(new MapType(6, PrimitiveType.String, 7, PrimitiveType.Long, false), ((StructType)schema.Columns[1].Type).Fields[1].Type);
    }

    [Fact]
    [Spec("ducklake: ducklake_column")]
    public void Malformed_column_trees_are_format_errors()
    {
        ColumnRow Row(long id, string type, long? parent = null) => new(id, id, $"c{id}", type, true, parent, null);
        Assert.Throws<InvalidDataException>(() => DuckLakeTypes.Tree([Row(1, "list")]));                  // list without element
        Assert.Throws<InvalidDataException>(() => DuckLakeTypes.Tree([Row(1, "int32"), Row(2, "int32", 1)])); // primitive with a child
        Assert.Throws<InvalidDataException>(() => DuckLakeTypes.Tree([Row(1, "int32"), Row(2, "int32", 9)])); // orphan
        Assert.Throws<InvalidDataException>(() => DuckLakeTypes.Tree([Row(1, "int32"), Row(1, "int32")]));    // duplicate ID
        Assert.Throws<NotSupportedException>(() => DuckLakeTypes.Tree([Row(1, "hugeint_of_the_future")]));
        Assert.Throws<InvalidDataException>(() => DuckLakeTypes.ToIceberg([new DuckLakeColumn(1L << 40, "x", DuckLakeType.Int32, true, [])]));
    }
}
