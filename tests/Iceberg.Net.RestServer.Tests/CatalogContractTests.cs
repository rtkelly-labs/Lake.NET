using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// The catalog contract (the same behaviours as the regression suite's <c>CatalogConformanceTests</c>, which also run
/// against this server as the "dotnet-rest" kind on moto), here network-free: the server in-process over a local
/// warehouse, under both stores, driven by the library's <see cref="RestCatalog"/>.
/// </summary>
public sealed class CatalogContractTests
{
    public static TheoryData<string> Stores => new() { "memory", "sqlite" };

    public static readonly Schema Schema = new(
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Optional(3, "amount", PrimitiveType.Double),
        NestedField.Optional(4, "event_ts", PrimitiveType.TimestampTz),
        NestedField.Optional(5, "region", PrimitiveType.String),
        NestedField.Optional(6, "qty", PrimitiveType.Int));

    public static PartitionSpec Spec => PartitionSpec.BuilderFor(Schema).Day("event_ts").Bucket("id", 4).Identity("region").Build();

    public static List<object?[]> Rows(int start, int count)
    {
        var baseTs = IcebergTime.MicrosFromTimestamp(new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        return Enumerable.Range(start, count).Select(i => new object?[]
        {
            (long)i, i % 7 == 0 ? null : $"name-{i}", i * 1.5, baseTs + i * 5L * 3_600_000_000L, i % 2 == 0 ? "eu" : "us", i % 10,
        }).ToList();
    }

    private static readonly Namespace Ns = Namespace.Of("db");
    private static TableIdentifier Id(string name) => new(Ns, name);

    private static async Task<(ServerHost Host, RestCatalog Catalog)> StartAsync(string store)
    {
        var host = ServerHost.Start(store);
        var catalog = await host.ConnectAsync();
        await catalog.CreateNamespaceAsync(Ns);
        return (host, catalog);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task Namespace_lifecycle_with_nesting_and_properties(string store)
    {
        var (host, catalog) = await StartAsync(store);
        await using var _ = host;
        using var __ = catalog;

        var nested = Namespace.Of("db", "team a", "raw");
        await catalog.CreateNamespaceAsync(Namespace.Of("db", "team a"));
        await catalog.CreateNamespaceAsync(nested, new Dictionary<string, string> { ["owner"] = "data-eng" });
        Assert.True(await catalog.NamespaceExistsAsync(nested));
        Assert.Contains(Ns, await catalog.ListNamespacesAsync());
        Assert.Equal([Namespace.Of("db", "team a")], await catalog.ListNamespacesAsync(Ns));
        Assert.Equal([nested], await catalog.ListNamespacesAsync(Namespace.Of("db", "team a")));
        await Assert.ThrowsAsync<AlreadyExistsException>(() => catalog.CreateNamespaceAsync(nested));
        Assert.False(await catalog.NamespaceExistsAsync(Namespace.Of("missing")));

        var t = await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(nested, "events"), Schema));
        Assert.Equal([new TableIdentifier(nested, "events")], await catalog.ListTablesAsync(nested));
        // A namespace with a table, or with a child namespace, cannot be dropped.
        await Assert.ThrowsAnyAsync<Exception>(() => catalog.DropNamespaceAsync(nested));
        await Assert.ThrowsAnyAsync<Exception>(() => catalog.DropNamespaceAsync(Namespace.Of("db", "team a")));
        await catalog.DropTableAsync(t.Identifier);
        await catalog.DropNamespaceAsync(nested);
        Assert.False(await catalog.NamespaceExistsAsync(nested));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task Create_append_read_round_trip(string store)
    {
        var (host, catalog) = await StartAsync(store);
        await using var _ = host;
        using var __ = catalog;
        var table = await catalog.CreateTableAsync(new CreateTableRequest(Id("events"), Schema, Spec));
        Assert.Equal(1000, table.Spec.Fields[0].FieldId);
        Assert.Null(table.Metadata.CurrentSnapshot);
        Assert.StartsWith(host.Warehouse, table.Location);

        var snap = await SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, Rows(0, 40))).CommitAsync();
        var loaded = await catalog.LoadTableAsync(Id("events"));
        Assert.Equal(snap.SnapshotId, loaded.Metadata.CurrentSnapshotId);
        Assert.Equal(40, (await TableScan.ReadAllAsync(loaded)).Count);
        Assert.Contains(Id("events"), await catalog.ListTablesAsync(Ns));
        Assert.True(File.Exists(loaded.MetadataLocation!));
        Assert.StartsWith("00001-", Path.GetFileName(loaded.MetadataLocation!));
        await Assert.ThrowsAsync<AlreadyExistsException>(() => catalog.CreateTableAsync(new CreateTableRequest(Id("events"), Schema)));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task Concurrent_commits_serialise_through_the_catalog(string store)
    {
        var (host, catalog) = await StartAsync(store);
        await using var _ = host;
        using var __ = catalog;
        var created = await catalog.CreateTableAsync(new CreateTableRequest(Id("t"), Schema, Spec));

        // Several writers start from the same base; each retries its lost races, so every one lands exactly once.
        const int writers = 4;
        var tables = await Task.WhenAll(Enumerable.Range(0, writers).Select(_ => catalog.LoadTableAsync(created.Identifier)));
        var files = await Task.WhenAll(tables.Select((t, i) => TableWriter.WriteAsync(t, Rows(i * 10, 10))));
        await Task.WhenAll(tables.Select((t, i) => SnapshotProducer.Append(t).AddFiles(files[i]).CommitAsync()));

        var final = await catalog.LoadTableAsync(created.Identifier);
        Assert.Equal(writers, final.Metadata.LastSequenceNumber);
        Assert.Equal(Enumerable.Range(0, writers * 10).Select(i => (long)i), (await TableScan.ReadAllAsync(final)).Select(r => (long)r[0]!).Order());
        Assert.Equal(writers, final.Metadata.Snapshots.Count);
        Assert.Equal(writers, final.Metadata.Ancestors(final.Metadata.CurrentSnapshotId).Count());
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task Stale_requirements_are_rejected(string store)
    {
        var (host, catalog) = await StartAsync(store);
        await using var _ = host;
        using var __ = catalog;
        var a = await catalog.CreateTableAsync(new CreateTableRequest(Id("t"), Schema));
        var b = await catalog.LoadTableAsync(a.Identifier);
        await a.UpdateSchemaAsync(new Schema(0, [.. Schema.Columns, NestedField.Optional(7, "x", PrimitiveType.String)]));
        await Assert.ThrowsAsync<CommitFailedException>(() =>
            b.UpdateSchemaAsync(new Schema(0, [.. Schema.Columns, NestedField.Optional(7, "y", PrimitiveType.Long)])));
        await Assert.ThrowsAsync<CommitFailedException>(() =>
            b.CommitAsync([new UpdateRequirement.AssertTableUuid(Guid.NewGuid())], [new MetadataUpdate.SetProperties(new Dictionary<string, string> { ["k"] = "v" })]));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task Overwrite_schema_evolution_and_time_travel(string store)
    {
        var (host, catalog) = await StartAsync(store);
        await using var _ = host;
        using var __ = catalog;
        var t = await catalog.CreateTableAsync(new CreateTableRequest(Id("t"), Schema, Spec));
        var first = await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, Rows(0, 20))).CommitAsync();
        await SnapshotProducer.Overwrite(t, f => (string?)f.Partition[2] == "us").CommitAsync();
        Assert.Equal(10, (await TableScan.ReadAllAsync(t)).Count);

        await t.UpdateSchemaAsync(new Schema(0, [.. Schema.Columns, NestedField.Optional(7, "note", PrimitiveType.String)]));
        var more = Rows(100, 4).Select(r => r.Append("late").ToArray()).ToList();
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, more)).CommitAsync();

        var loaded = await catalog.LoadTableAsync(t.Identifier);
        var rows = await TableScan.ReadAllAsync(loaded);
        Assert.Equal(14, rows.Count);
        Assert.Equal(4, rows.Count(r => (string?)r[6] == "late"));
        Assert.Equal(20, (await TableScan.ReadAllAsync(loaded, first.SnapshotId)).Count);
        // create, append, overwrite, add-schema, append: five metadata files, four in the log.
        Assert.Equal(4, loaded.Metadata.MetadataLog.Count);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task Rename_register_and_drop_with_purge(string store)
    {
        var (host, catalog) = await StartAsync(store);
        await using var _ = host;
        using var __ = catalog;
        var t = await catalog.CreateTableAsync(new CreateTableRequest(Id("a"), Schema));
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, Rows(0, 3))).CommitAsync();

        await catalog.RenameTableAsync(Id("a"), Id("b"));
        Assert.False(await catalog.TableExistsAsync(Id("a")));
        var b = await catalog.LoadTableAsync(Id("b"));
        Assert.Equal(t.Metadata.TableUuid, b.Metadata.TableUuid);
        await Assert.ThrowsAsync<NoSuchTableException>(() => catalog.RenameTableAsync(Id("a"), Id("c")));

        await catalog.DropTableAsync(Id("b"), purge: false);
        Assert.False(await catalog.TableExistsAsync(Id("b")));
        Assert.True(File.Exists(b.MetadataLocation!)); // without purge the files stay
        var registered = await catalog.RegisterTableAsync(Id("c"), b.MetadataLocation!);
        Assert.Equal(t.Metadata.TableUuid, registered.Metadata.TableUuid);
        Assert.Equal(3, (await TableScan.ReadAllAsync(registered)).Count);
        await Assert.ThrowsAsync<AlreadyExistsException>(() => catalog.RegisterTableAsync(Id("c"), b.MetadataLocation!));

        var dataFiles = Directory.GetFiles(registered.Location, "*.parquet", SearchOption.AllDirectories);
        Assert.NotEmpty(dataFiles);
        await catalog.DropTableAsync(Id("c"), purge: true);
        await Assert.ThrowsAsync<NoSuchTableException>(() => catalog.LoadTableAsync(Id("c")));
        Assert.All(dataFiles, f => Assert.False(File.Exists(f)));
        Assert.False(File.Exists(b.MetadataLocation!));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task Format_v3_tables(string store)
    {
        var (host, catalog) = await StartAsync(store);
        await using var _ = host;
        using var __ = catalog;
        var t = await catalog.CreateTableAsync(new CreateTableRequest(Id("t"), Schema, Spec, FormatVersion: 3));
        Assert.Equal(3, t.Metadata.FormatVersion);
        Assert.False(t.Metadata.Properties.ContainsKey("format-version"));
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, Rows(0, 12))).CommitAsync();
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, Rows(12, 8))).CommitAsync();
        var loaded = await catalog.LoadTableAsync(t.Identifier);
        Assert.Equal(20, loaded.Metadata.NextRowId);
        Assert.Equal(12, loaded.Metadata.CurrentSnapshot!.FirstRowId);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task Multi_table_transactions_apply_all_or_nothing(string store)
    {
        var (host, catalog) = await StartAsync(store);
        await using var _ = host;
        using var __ = catalog;
        var a = await catalog.CreateTableAsync(new CreateTableRequest(Id("a"), Schema, Spec));
        var b = await catalog.CreateTableAsync(new CreateTableRequest(Id("b"), Schema, Spec));
        async Task<TableCommit> Stage(Table t, int from) =>
            await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, Rows(from, 3))).StageAsync();

        await catalog.CommitTransactionAsync([await Stage(a, 0), await Stage(b, 100)]);
        Assert.Equal(3, (await TableScan.ReadAllAsync(await catalog.LoadTableAsync(a.Identifier))).Count);
        Assert.Equal(3, (await TableScan.ReadAllAsync(await catalog.LoadTableAsync(b.Identifier))).Count);

        // a moves underneath a staged transaction: neither table may change, and no metadata file is left behind.
        var staleA = await Stage(await catalog.LoadTableAsync(a.Identifier), 10);
        var stagedB = await Stage(await catalog.LoadTableAsync(b.Identifier), 110);
        await SnapshotProducer.Append(await catalog.LoadTableAsync(a.Identifier)).AddFiles(await TableWriter.WriteAsync(a, Rows(20, 1))).CommitAsync();
        var bMetadataFiles = Directory.GetFiles(Path.Combine(b.Location, "metadata"), "*.metadata.json").Length;
        await Assert.ThrowsAsync<CommitFailedException>(() => catalog.CommitTransactionAsync([staleA, stagedB]));
        Assert.Equal(4, (await TableScan.ReadAllAsync(await catalog.LoadTableAsync(a.Identifier))).Count);
        Assert.Equal(3, (await TableScan.ReadAllAsync(await catalog.LoadTableAsync(b.Identifier))).Count);
        Assert.Equal(bMetadataFiles, Directory.GetFiles(Path.Combine(b.Location, "metadata"), "*.metadata.json").Length);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task View_lifecycle(string store)
    {
        var (host, catalog) = await StartAsync(store);
        await using var _ = host;
        using var __ = catalog;
        var output = new Schema(0, [NestedField.Optional(1, "region", PrimitiveType.String), NestedField.Optional(2, "n", PrimitiveType.Long)]);
        var id = Id("by_region");
        var created = await catalog.CreateViewAsync(id, new ViewDefinition(output, "SELECT region, count(*) AS n FROM events GROUP BY 1", "trino", Ns.Levels.ToList()));
        Assert.Equal(1, created.Metadata.CurrentVersionId);
        Assert.True(await catalog.ViewExistsAsync(id));
        Assert.Contains(id, await catalog.ListViewsAsync(Ns));
        Assert.DoesNotContain(id, await catalog.ListTablesAsync(Ns));
        // Tables and views share names.
        await Assert.ThrowsAsync<AlreadyExistsException>(() => catalog.CreateTableAsync(new CreateTableRequest(id, Schema)));

        var replaced = await catalog.ReplaceViewAsync(id, new ViewDefinition(output, "SELECT region, sum(qty) AS n FROM events GROUP BY 1", "trino", Ns.Levels.ToList()));
        Assert.Equal(2, replaced.Metadata.CurrentVersionId);
        Assert.Contains("sum(qty)", (await catalog.LoadViewAsync(id)).Sql("trino"));
        Assert.Equal(created.Metadata.ViewUuid, replaced.Metadata.ViewUuid);
        Assert.Single(replaced.Metadata.Schemas); // the identical schema is reused

        var wider = new Schema(0, [.. output.Columns, NestedField.Optional(3, "avg", PrimitiveType.Double)]);
        var third = await catalog.ReplaceViewAsync(id, new ViewDefinition(wider, "SELECT region, count(*) AS n, avg(amount) AS avg FROM events GROUP BY 1", "trino", Ns.Levels.ToList()));
        Assert.Equal(3, third.Metadata.CurrentVersionId);
        Assert.Equal(3, third.Metadata.CurrentSchema.Columns.Count);

        var renamed = Id("by_region2");
        await catalog.RenameViewAsync(id, renamed);
        Assert.False(await catalog.ViewExistsAsync(id));
        await Assert.ThrowsAsync<NoSuchTableException>(() => catalog.LoadViewAsync(id));
        await catalog.DropViewAsync(renamed);
        Assert.False(await catalog.ViewExistsAsync(renamed));
    }
}
