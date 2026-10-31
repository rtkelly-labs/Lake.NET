using System.Globalization;
using System.Text.Json.Nodes;

namespace Lake.Delta.Tests;

/// <summary>
/// The Delta Acceptance Tests (delta-incubator/dat v0.0.3, fixtures/dat): every reader case, every version it gives
/// expected output for. Each must either read to exactly DAT's expected rows (order-independent, typed) with DAT's
/// version, protocol versions and properties, or be refused with the reason listed in <see cref="Refused"/>. A case in
/// neither list fails <see cref="Every_DAT_case_is_read_or_refused_for_a_stated_reason"/>, so a refreshed fixture set
/// cannot be skipped silently.
/// </summary>
public sealed class DatAcceptanceTests
{
    /// <summary>Cases this reader refuses, with the features named in the refusal. None in DAT v0.0.3.</summary>
    private static readonly Dictionary<string, string[]> Refused = [];

    private static IEnumerable<string> Cases() =>
        Directory.EnumerateDirectories(Fixtures.DatRoot).Select(Path.GetFileName).Order(StringComparer.Ordinal)!;

    public static TheoryData<string, string> ExpectedVersions()
    {
        var data = new TheoryData<string, string>();
        foreach (var name in Cases().Where(c => !Refused.ContainsKey(c)))
            foreach (var dir in Directory.EnumerateDirectories(Path.Combine(Fixtures.DatRoot, name, "expected")).Order(StringComparer.Ordinal))
                data.Add(name, Path.GetFileName(dir));
        return data;
    }

    [Fact]
    public void Every_DAT_case_is_read_or_refused_for_a_stated_reason()
    {
        var cases = Cases().ToList();
        Assert.Equal(19, cases.Count); // DAT v0.0.3's reader_tests/generated
        Assert.Empty(Refused.Keys.Except(cases)); // a stale refusal hides a renamed case
        Assert.All(cases, c => Assert.True(File.Exists(Path.Combine(Fixtures.DatRoot, c, "test_case_info.json")), c));
    }

    [Theory]
    [MemberData(nameof(ExpectedVersions))]
    public async Task DAT_case_reads_as_expected(string name, string expectedDir)
    {
        var dir = Path.Combine(Fixtures.DatRoot, name, "expected", expectedDir);
        var expectedMeta = JsonNode.Parse(File.ReadAllText(Path.Combine(dir, "table_version_metadata.json")))!.AsObject();
        long? requested = expectedDir == "latest" ? null : long.Parse(expectedDir[1..], CultureInfo.InvariantCulture);

        var table = DeltaTable.OpenLocal(Fixtures.Dat(name));
        var snapshot = await table.GetSnapshotAsync(requested);

        Assert.Equal(expectedMeta["version"]!.GetValue<long>(), snapshot.Version);
        Assert.Equal(expectedMeta["min_reader_version"]!.GetValue<int>(), snapshot.Protocol.MinReaderVersion);
        Assert.Equal(expectedMeta["min_writer_version"]!.GetValue<int>(), snapshot.Protocol.MinWriterVersion);
        var expectedProperties = expectedMeta["properties"]!.AsObject().ToDictionary(kv => kv.Key, kv => kv.Value!.GetValue<string>());
        Assert.Equal(expectedProperties.OrderBy(kv => kv.Key), snapshot.Metadata.Configuration.OrderBy(kv => kv.Key));

        var expectedFiles = Directory.EnumerateFiles(Path.Combine(dir, "table_content"), "*.parquet").Where(f => !Path.GetFileName(f).StartsWith('.'));
        var expected = Canon.Rows(await Fixtures.ReadParquetAsync(expectedFiles, snapshot.Schema));
        Assert.NotEmpty(expected); // every DAT version has rows; an empty side would make the comparison vacuous
        var actual = Canon.Rows(await Fixtures.ReadAllAsync(snapshot));
        Assert.True(expected.SequenceEqual(actual), $"{name}/{expectedDir}\nexpected:\n{Canon.Describe(expected)}\nactual:\n{Canon.Describe(actual)}");
    }

    [Fact]
    public async Task Refused_DAT_cases_name_their_reason()
    {
        foreach (var (name, features) in Refused)
        {
            var e = await Assert.ThrowsAsync<DeltaUnsupportedFeatureException>(() => DeltaTable.OpenLocal(Fixtures.Dat(name)).GetSnapshotAsync());
            Assert.Equal(features, e.Features);
        }
    }

    // ---------- spot checks against literal values, so the shared value decoder cannot hide a symmetric bug ----------

    [Fact]
    public async Task All_primitive_types_have_their_clr_types_and_values()
    {
        var table = DeltaTable.OpenLocal(Fixtures.Dat("all_primitive_types"));
        var snapshot = await table.GetSnapshotAsync();
        var rows = (await Fixtures.ReadAllAsync(snapshot)).OrderBy(r => (string)r[0]!).ToList();
        Assert.Equal(5, rows.Count);
        var names = snapshot.Schema.Fields.Select(f => f.Name).ToList();
        object? At(int row, string column) => rows[row][names.IndexOf(column)];
        Assert.Equal("1", At(1, "utf8"));
        Assert.Equal(1L, At(1, "int64"));
        Assert.Equal(1, At(1, "int32"));
        Assert.Equal((short)1, At(1, "int16"));
        Assert.Equal((sbyte)1, At(1, "int8"));
        Assert.Equal(1f, At(1, "float32"));
        Assert.Equal(1d, At(1, "float64"));
        Assert.Equal(false, At(1, "bool"));
        Assert.Equal(new byte[] { 0 }, At(1, "binary"));
        Assert.Equal(11.000m, At(1, "decimal"));
        Assert.Equal(new DateOnly(1970, 1, 2), At(1, "date32"));
        Assert.Equal(new DateTimeOffset(1970, 1, 1, 1, 0, 0, TimeSpan.Zero), At(1, "timestamp"));
    }

    [Fact]
    public async Task Nested_types_read_as_structs_lists_and_maps()
    {
        var rows = (await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(Fixtures.Dat("nested_types")))).OrderBy(r => (int)r[0]!).ToList();
        var last = rows[^1]; // pk 4
        Assert.Equal(new object?[] { 4.0, true }, (object?[])last[1]!);
        Assert.Equal(new object?[] { (short)0, (short)1, (short)2, (short)3, (short)4 }, ((List<object?>)last[2]!).ToArray());
        var map = (Dictionary<object, object?>)last[3]!;
        Assert.Equal(4, map.Count);
        Assert.Equal(3, map["3"]);
        Assert.Empty((Dictionary<object, object?>)rows[0][3]!);
    }

    [Fact]
    public async Task Partition_values_are_parsed_per_type_with_nulls_and_escapes()
    {
        var rows = await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(Fixtures.Dat("multi_partitioned")));
        // letter (string), date (date), data (binary) are partition columns; the path letter=%252F%252520%2525f is decoded once.
        Assert.Contains(rows, r => (string?)r[0] == "/%20%f" && (DateOnly?)r[1] == new DateOnly(1970, 1, 1) && ((byte[])r[2]!).SequenceEqual("hello"u8.ToArray()));
        Assert.Contains(rows, r => ((byte[])r[2]!).SequenceEqual(new byte[] { 0xF0, 0x9F, 0x98, 0x88 }));

        var v1 = await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(Fixtures.Dat("multi_partitioned")), 1);
        Assert.Contains(v1, r => r[0] is null && r[1] is null && r[2] is null && (long?)r[3] == 5);

        var typed = await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(Fixtures.Dat("multi_partitioned_2")));
        Assert.Contains(typed, r => (bool?)r[0] == false
            && (DateTimeOffset?)r[1] == new DateTimeOffset(1970, 1, 2, 8, 45, 0, TimeSpan.Zero)
            && (decimal?)r[2] == 12m && (long?)r[3] == 3);
    }

    [Fact]
    public async Task Timestamp_ntz_reads_as_an_unzoned_DateTime()
    {
        var rows = await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(Fixtures.Dat("timestamp_ntz")));
        var value = Assert.IsType<DateTime>(rows[0][3]);
        Assert.Equal(DateTimeKind.Unspecified, value.Kind);
        Assert.Equal(new DateTime(2024, 4, 2, 20, 58, 52, 292, DateTimeKind.Unspecified).AddTicks(9570), value);
    }

    [Fact]
    public async Task Column_mapping_name_mode_resolves_physical_names_after_a_rename()
    {
        var snapshot = await DeltaTable.OpenLocal(Fixtures.Dat("column_mapping")).GetSnapshotAsync();
        Assert.Equal(ColumnMappingMode.Name, snapshot.ColumnMapping);
        Assert.Equal(["letter", "new_int", "date"], snapshot.Schema.Fields.Select(f => f.Name));
        var rows = await Fixtures.ReadAllAsync(snapshot);
        Assert.Equal(10, rows.Count); // two files: before and after the rename of int to new_int
        Assert.DoesNotContain(rows, r => r[1] is null);
    }

    [Fact]
    public async Task A_reader_writer_feature_listed_only_as_a_writer_feature_is_still_honoured()
    {
        // delta-spark 3.1 wrote (3, 7) with readerFeatures [] and writerFeatures [columnMapping, icebergCompatV1].
        var snapshot = await DeltaTable.OpenLocal(Fixtures.Dat("iceberg_compat_v1")).GetSnapshotAsync();
        Assert.Empty(snapshot.Protocol.ReaderFeatures!);
        Assert.Equal(ColumnMappingMode.Name, snapshot.ColumnMapping);
    }

    [Fact]
    public async Task A_table_whose_early_log_was_cleaned_up_reads_from_its_checkpoint()
    {
        var table = DeltaTable.OpenLocal(Fixtures.Dat("no_replay"));
        Assert.Equal(5, (await table.GetSnapshotAsync()).Version);
        await Assert.ThrowsAsync<DeltaVersionNotFoundException>(() => table.GetSnapshotAsync(1));
    }
}
