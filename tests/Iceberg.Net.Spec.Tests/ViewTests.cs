using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>View metadata (view spec v1) and the view lifecycle on the in-memory catalog.</summary>
public sealed class ViewTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-views", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Out = new(0, [NestedField.Optional(1, "day", PrimitiveType.Date), NestedField.Optional(2, "plays", PrimitiveType.Long)]);

    /// <summary>The view spec's own example metadata.</summary>
    private const string SpecExample = """
        {
          "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
          "format-version" : 1,
          "location" : "s3://bucket/warehouse/default.db/event_agg",
          "current-version-id" : 1,
          "properties" : { "comment" : "Daily event counts" },
          "versions" : [ {
            "version-id" : 1,
            "timestamp-ms" : 1573518431292,
            "schema-id" : 1,
            "default-catalog" : "prod",
            "default-namespace" : [ "default" ],
            "summary" : { "engine-name" : "Spark", "engine-version" : "3.3.2" },
            "representations" : [ { "type" : "sql", "sql" : "SELECT\n    COUNT(1), CAST(event_ts AS DATE)\nFROM events\nGROUP BY 2", "dialect" : "spark" } ]
          } ],
          "schemas": [ {
            "schema-id": 1, "type" : "struct",
            "fields" : [ { "id" : 1, "name" : "event_count", "required" : false, "type" : "int", "doc" : "Count of events" },
                         { "id" : 2, "name" : "event_date", "required" : false, "type" : "date" } ]
          } ],
          "version-log" : [ { "timestamp-ms" : 1573518431292, "version-id" : 1 } ]
        }
        """;

    [Fact]
    [Spec("Iceberg Table Spec")]
    public void Spec_example_round_trips()
    {
        var m = ViewJson.Read(SpecExample);
        Assert.Equal("prod", m.CurrentVersion.DefaultCatalog);
        Assert.Equal(["default"], m.CurrentVersion.DefaultNamespace);
        Assert.StartsWith("SELECT", m.CurrentVersion.For("spark")!.Sql);
        Assert.Equal("event_count", m.CurrentSchema.Columns[0].Name);
        var again = ViewJson.Read(ViewJson.Write(m));
        Assert.Equal(m.ViewUuid, again.ViewUuid);
        Assert.Equal(m.CurrentVersion.Representations, again.CurrentVersion.Representations);
        Assert.Throws<InvalidDataException>(() => ViewJson.Read(SpecExample.Replace("\"version-log\"", "\"x-version-log\"")));
    }

    [Fact]
    public async Task View_lifecycle_versions_and_history()
    {
        using var catalog = new InMemoryCatalog(_warehouse);
        await catalog.CreateNamespaceAsync(Namespace.Of("v"));
        var id = TableIdentifier.Of("v", "daily");
        var created = await catalog.CreateViewAsync(id, new ViewDefinition(Out, "SELECT day, count(*) AS plays FROM plays GROUP BY 1", "trino", ["v"]),
            new Dictionary<string, string> { ["version.history.num-entries"] = "2" });
        Assert.Equal(1, created.Metadata.CurrentVersionId);
        await Assert.ThrowsAsync<AlreadyExistsException>(() => catalog.CreateViewAsync(id, new ViewDefinition(Out, "SELECT 1", "trino", ["v"])));

        await catalog.ReplaceViewAsync(id, new ViewDefinition(Out, "SELECT day, sum(n) AS plays FROM plays GROUP BY 1", "trino", ["v"]));
        var wider = new Schema(0, [.. Out.Columns, NestedField.Optional(3, "region", PrimitiveType.String)]);
        var v3 = await catalog.ReplaceViewAsync(id, new ViewDefinition(wider, [new("SELECT day, sum(n), region FROM plays GROUP BY 1, 3", "trino"), new("SELECT day, sum(n), region FROM plays GROUP BY day, region", "spark")], ["v"]));

        var loaded = await catalog.LoadViewAsync(id);
        Assert.Equal(3, loaded.Metadata.CurrentVersionId);
        Assert.Equal([2, 3], loaded.Metadata.Versions.Select(v => v.VersionId)); // history trimmed to 2 versions
        Assert.Equal(3, loaded.Metadata.VersionLog.Count);
        Assert.Equal("region", loaded.Metadata.CurrentSchema.Columns[^1].Name);
        Assert.Contains("GROUP BY day, region", loaded.Sql("spark"));
        Assert.Throws<KeyNotFoundException>(() => loaded.Sql("duckdb"));

        await catalog.RenameViewAsync(id, TableIdentifier.Of("v", "daily2"));
        Assert.False(await catalog.ViewExistsAsync(id));
        Assert.Equal([TableIdentifier.Of("v", "daily2")], await catalog.ListViewsAsync(Namespace.Of("v")));
        await catalog.DropViewAsync(TableIdentifier.Of("v", "daily2"));
        await Assert.ThrowsAsync<NoSuchTableException>(() => catalog.LoadViewAsync(TableIdentifier.Of("v", "daily2")));
    }
}
