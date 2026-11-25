using System.Text.Json.Nodes;

namespace Lake.Delta.Tests;

/// <summary>
/// The reader protocol gate (refuse every unsupported reader feature, by name, before reading), column mapping
/// <c>id</c> mode, and type widening.
/// </summary>
public sealed class ProtocolGateTests
{
    private static void SetProtocol(string table, long version, string protocolJson) =>
        Fixtures.EditCommit(table, version, line => line["protocol"] is null ? line : JsonNode.Parse(protocolJson)!.AsObject());

    [Fact]
    public async Task Unsupported_reader_features_are_refused_together_by_name()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        SetProtocol(table, 0, """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors","variantType","catalogManaged"],"writerFeatures":["deletionVectors","variantType","catalogManaged"]}}""");
        var e = await Assert.ThrowsAsync<DeltaUnsupportedFeatureException>(() => DeltaTable.OpenLocal(table).GetSnapshotAsync());
        Assert.Equal(["catalogManaged", "variantType"], e.Features);
        Assert.Contains("catalogManaged, variantType", e.Message);
    }

    [Fact]
    public async Task An_unknown_reader_feature_is_refused()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        SetProtocol(table, 0, """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["someFutureFeature"],"writerFeatures":["someFutureFeature"]}}""");
        var e = await Assert.ThrowsAsync<DeltaUnsupportedFeatureException>(() => Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table)));
        Assert.Equal(["someFutureFeature"], e.Features);
    }

    [Fact]
    public async Task A_reader_version_above_3_is_refused()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        SetProtocol(table, 0, """{"protocol":{"minReaderVersion":4,"minWriterVersion":7}}""");
        var e = await Assert.ThrowsAsync<DeltaUnsupportedFeatureException>(() => DeltaTable.OpenLocal(table).GetSnapshotAsync());
        Assert.Equal(["minReaderVersion=4"], e.Features);
    }

    [Fact]
    public async Task Writer_only_features_do_not_stop_a_reader()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        SetProtocol(table, 0, """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["vacuumProtocolCheck"],"writerFeatures":["vacuumProtocolCheck","appendOnly","invariants","checkConstraints","identityColumns","rowTracking","domainMetadata","inCommitTimestamp","clustering","icebergCompatV2"]}}""");
        Assert.Equal(5, (await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table))).Count);
    }

    [Fact]
    public async Task Every_supported_reader_feature_is_accepted()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        var features = string.Join(",", DeltaFeatures.SupportedReaderFeatures.Select(f => "\"" + f + "\""));
        SetProtocol(table, 0, """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[@@],"writerFeatures":[@@]}}""".Replace("@@", features, StringComparison.Ordinal));
        Assert.Equal(5, (await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table))).Count);
    }

    [Fact]
    public async Task Column_mapping_id_mode_resolves_by_Parquet_field_id_not_by_name()
    {
        // DAT column_mapping's files carry field IDs 1-3; switch the table to id mode and scramble every physical name.
        var table = Fixtures.CopyToTemp(Fixtures.Dat("column_mapping"));
        var expected = Canon.Rows(await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table)));
        foreach (var version in new[] { 0L, 2L })
            Fixtures.EditCommit(table, version, line =>
            {
                if (line["metaData"] is not JsonObject md) return line;
                md["configuration"]!["delta.columnMapping.mode"] = "id";
                var schema = JsonNode.Parse(md["schemaString"]!.GetValue<string>())!;
                foreach (var field in schema["fields"]!.AsArray())
                    field!["metadata"]!["delta.columnMapping.physicalName"] = "not-in-any-file";
                md["schemaString"] = schema.ToJsonString();
                return line;
            });
        var snapshot = await DeltaTable.OpenLocal(table).GetSnapshotAsync();
        Assert.Equal(ColumnMappingMode.Id, snapshot.ColumnMapping);
        Assert.Equal(expected, Canon.Rows(await Fixtures.ReadAllAsync(snapshot)));
    }

    [Fact]
    public async Task Column_mapping_id_mode_refuses_a_file_without_field_ids()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        SetProtocol(table, 0, """{"protocol":{"minReaderVersion":2,"minWriterVersion":5}}""");
        Fixtures.EditCommit(table, 0, line =>
        {
            if (line["metaData"] is not JsonObject md) return line;
            md["configuration"]!["delta.columnMapping.mode"] = "id";
            var schema = JsonNode.Parse(md["schemaString"]!.GetValue<string>())!;
            var id = 0;
            foreach (var field in schema["fields"]!.AsArray())
            {
                field!["metadata"]!["delta.columnMapping.id"] = ++id;
                field["metadata"]!["delta.columnMapping.physicalName"] = field["name"]!.GetValue<string>();
            }
            md["schemaString"] = schema.ToJsonString();
            return line;
        });
        var e = await Assert.ThrowsAsync<InvalidDataException>(() => Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table)));
        Assert.Contains("no Parquet field IDs", e.Message);
    }

    [Fact]
    public async Task Column_mapping_is_ignored_unless_the_protocol_supports_it()
    {
        // Reader version 1: delta.columnMapping.mode is not honoured (PROTOCOL.md "Reader Requirements for Column Mapping").
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        Fixtures.EditCommit(table, 0, line =>
        {
            if (line["metaData"] is JsonObject md) md["configuration"]!["delta.columnMapping.mode"] = "name";
            return line;
        });
        var snapshot = await DeltaTable.OpenLocal(table).GetSnapshotAsync();
        Assert.Equal(ColumnMappingMode.None, snapshot.ColumnMapping);
        Assert.Equal(5, (await Fixtures.ReadAllAsync(snapshot)).Count);
    }

    private static string Widen(string table, Dictionary<string, (string To, string From)> changes)
    {
        SetProtocol(table, 0, """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["typeWidening"],"writerFeatures":["typeWidening"]}}""");
        Fixtures.EditCommit(table, 0, line =>
        {
            if (line["metaData"] is not JsonObject md) return line;
            md["configuration"]!["delta.enableTypeWidening"] = "true";
            var schema = JsonNode.Parse(md["schemaString"]!.GetValue<string>())!;
            foreach (var field in schema["fields"]!.AsArray())
            {
                if (!changes.TryGetValue(field!["name"]!.GetValue<string>(), out var change)) continue;
                field["type"] = change.To;
                field["metadata"]!["delta.typeChanges"] = new JsonArray(new JsonObject { ["fromType"] = change.From, ["toType"] = change.To });
            }
            md["schemaString"] = schema.ToJsonString();
            return line;
        });
        return table;
    }

    [Fact]
    public async Task Type_widening_converts_old_files_to_the_wider_type()
    {
        var table = Widen(Fixtures.CopyToTemp(Fixtures.Dat("all_primitive_types")), new()
        {
            ["int8"] = ("short", "byte"),
            ["int16"] = ("integer", "short"),
            ["int32"] = ("long", "integer"),
            ["int64"] = ("decimal(22,2)", "long"),
            ["float32"] = ("double", "float"),
            ["decimal"] = ("decimal(10,5)", "decimal(5,3)"),
            ["date32"] = ("timestamp_ntz", "date"),
        });
        var snapshot = await DeltaTable.OpenLocal(table).GetSnapshotAsync();
        var names = snapshot.Schema.Fields.Select(f => f.Name).ToList();
        var row = (await Fixtures.ReadAllAsync(snapshot)).Single(r => (string)r[0]! == "2");
        Assert.Equal((short)2, row[names.IndexOf("int8")]);
        Assert.Equal(2, row[names.IndexOf("int16")]);
        Assert.Equal(2L, row[names.IndexOf("int32")]);
        Assert.Equal(2m, row[names.IndexOf("int64")]);
        Assert.Equal(2d, row[names.IndexOf("float32")]);
        Assert.Equal(12.000m, row[names.IndexOf("decimal")]);
        Assert.Equal(new DateTime(1970, 1, 3, 0, 0, 0, DateTimeKind.Unspecified), row[names.IndexOf("date32")]);
    }

    [Fact]
    public async Task A_type_change_outside_the_protocols_widenings_is_refused()
    {
        var table = Widen(Fixtures.CopyToTemp(Fixtures.Dat("all_primitive_types")), new() { ["int64"] = ("integer", "long") });
        var e = await Assert.ThrowsAsync<DeltaUnsupportedFeatureException>(() => Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table)));
        Assert.Contains("long -> integer", e.Message);
    }

    [Fact]
    public async Task A_file_type_that_is_not_a_widening_is_a_format_error()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("all_primitive_types"));
        Fixtures.EditCommit(table, 0, line =>
        {
            if (line["metaData"] is JsonObject md) md["schemaString"] = md["schemaString"]!.GetValue<string>().Replace("\"utf8\",\"type\":\"string\"", "\"utf8\",\"type\":\"long\"", StringComparison.Ordinal);
            return line;
        });
        var e = await Assert.ThrowsAsync<InvalidDataException>(() => Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table)));
        Assert.Contains("utf8", e.Message);
    }

    [Theory]
    [InlineData("byte", "long", true)]
    [InlineData("integer", "double", true)]
    [InlineData("long", "double", false)]
    [InlineData("float", "double", true)]
    [InlineData("date", "timestamp_ntz", true)]
    [InlineData("date", "timestamp", false)]
    [InlineData("decimal(5,2)", "decimal(7,4)", true)]
    [InlineData("decimal(5,2)", "decimal(6,4)", false)]
    [InlineData("integer", "decimal(10,0)", true)]
    [InlineData("integer", "decimal(11,2)", false)]
    [InlineData("long", "decimal(20,0)", true)]
    [InlineData("long", "integer", false)]
    public void Widenings_follow_the_protocol_list(string from, string to, bool allowed)
    {
        static DeltaType T(string name) => DeltaSchema.ParseType(JsonValue.Create(name), "t");
        Assert.Equal(allowed, DeltaFeatures.IsSupportedWidening(T(from), T(to)));
    }
}
