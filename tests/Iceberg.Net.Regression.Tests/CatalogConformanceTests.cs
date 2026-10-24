using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// One behavioural contract, run against every catalog implementation. Adding a catalog (Nessie,
/// Polaris, S3 Tables, ...) means adding it to <see cref="Catalogs"/>; the contract must not fork.
/// </summary>
[Trait("Category", "Regression")]
public sealed class CatalogConformanceTests
{
    /// <summary>
    /// "rest" is the Iceberg project's Java test server; "polaris" is Apache Polaris, implemented independently;
    /// "jdbc" is our JdbcCatalog over SQLite, in the tables PyIceberg's SqlCatalog and Java's JdbcCatalog share;
    /// "dotnet-rest" is this repository's REST catalog server (src/Iceberg.Net.RestServer), hosted by the suite.
    /// </summary>
    public static TheoryData<string> Catalogs => new() { "glue", "rest", "polaris", "gravitino", "nessie", "lakekeeper", "jdbc", "dotnet-rest" };

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

    private sealed class Scope(ICatalog catalog, Namespace ns) : IAsyncDisposable
    {
        public ICatalog Catalog { get; } = catalog;
        public Namespace Ns { get; } = ns;
        public TableIdentifier Id(string name) => new(Ns, name);

        public async ValueTask DisposeAsync()
        {
            try
            {
                foreach (var t in await Catalog.ListTablesAsync(Ns)) await Catalog.DropTableAsync(t, purge: true);
                await Catalog.DropNamespaceAsync(Ns);
            }
            finally
            {
                Catalog.Dispose();
            }
        }
    }

    private static async Task<Scope> ScopeAsync(string kind, string purpose)
    {
        var catalog = await TestEnvironment.Current.CatalogAsync(kind);
        var ns = Namespace.Of(TestEnvironment.Current.NewNamespace($"{kind}_{purpose}"));
        await catalog.CreateNamespaceAsync(ns);
        return new Scope(catalog, ns);
    }

    [Spec("rest: createNamespace")]
    [Spec("rest: listNamespaces")]
    [Spec("rest: loadNamespaceMetadata")]
    [Spec("rest: dropNamespace")]
    [Spec("rest: getConfig")]
    [Spec("rest: namespaceExists")]
    [SkippableTheory]
    [MemberData(nameof(Catalogs))]
    public async Task Namespace_lifecycle(string kind)
    {
        Require.Catalog(kind);
        await using var s = await ScopeAsync(kind, "ns");
        Assert.True(await s.Catalog.NamespaceExistsAsync(s.Ns));
        Assert.Contains(s.Ns, await s.Catalog.ListNamespacesAsync());
        await Assert.ThrowsAsync<AlreadyExistsException>(() => s.Catalog.CreateNamespaceAsync(s.Ns));
        Assert.False(await s.Catalog.NamespaceExistsAsync(Namespace.Of(s.Ns.Levels[0] + "_missing")));
    }

    [Spec("rest: createTable")]
    [Spec("rest: loadTable")]
    [Spec("rest: updateTable")]
    [SkippableTheory]
    [MemberData(nameof(Catalogs))]
    public async Task Create_append_read_round_trip(string kind)
    {
        Require.Catalog(kind);
        await using var s = await ScopeAsync(kind, "rt");
        var table = await s.Catalog.CreateTableAsync(new CreateTableRequest(s.Id("events"), Schema, Spec));
        Assert.Equal(1000, table.Spec.Fields[0].FieldId);
        Assert.Null(table.Metadata.CurrentSnapshot);

        var snap = await SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, Rows(0, 40))).CommitAsync();
        var loaded = await s.Catalog.LoadTableAsync(s.Id("events"));
        Assert.Equal(snap.SnapshotId, loaded.Metadata.CurrentSnapshotId);
        Assert.Equal(40, (await TableScan.ReadAllAsync(loaded)).Count);
        Assert.Contains(s.Id("events"), await s.Catalog.ListTablesAsync(s.Ns));
        Assert.True(await loaded.IO.ExistsAsync(loaded.MetadataLocation!));
    }

    [Spec("Metastore Tables")]
    [Spec("rest: updateTable")]
    [SkippableTheory]
    [MemberData(nameof(Catalogs))]
    public async Task Concurrent_commits_serialise_through_the_catalog(string kind)
    {
        Require.Catalog(kind);
        await using var s = await ScopeAsync(kind, "cc");
        var created = await s.Catalog.CreateTableAsync(new CreateTableRequest(s.Id("t"), Schema, Spec));

        // Several writers start from the same base; every one must land exactly once.
        const int writers = 4;
        var tables = await Task.WhenAll(Enumerable.Range(0, writers).Select(_ => s.Catalog.LoadTableAsync(created.Identifier)));
        var files = await Task.WhenAll(tables.Select((t, i) => TableWriter.WriteAsync(t, Rows(i * 10, 10))));
        await Task.WhenAll(tables.Select((t, i) => SnapshotProducer.Append(t).AddFiles(files[i]).CommitAsync()));

        var final = await s.Catalog.LoadTableAsync(created.Identifier);
        Assert.Equal(writers, final.Metadata.LastSequenceNumber);
        Assert.Equal(Enumerable.Range(0, writers * 10).Select(i => (long)i), (await TableScan.ReadAllAsync(final)).Select(r => (long)r[0]!).Order());
        if (!TestEnvironment.KeepsSnapshotHistory(kind)) return;
        Assert.Equal(writers, final.Metadata.Snapshots.Count);
        // Snapshot chain is linear: each parent is the previous snapshot.
        Assert.Equal(writers, final.Metadata.Ancestors(final.Metadata.CurrentSnapshotId).Count());
    }

    [Spec("Optimistic Concurrency")]
    [SkippableTheory]
    [MemberData(nameof(Catalogs))]
    public async Task Stale_requirements_are_rejected(string kind)
    {
        Require.Catalog(kind);
        await using var s = await ScopeAsync(kind, "req");
        var a = await s.Catalog.CreateTableAsync(new CreateTableRequest(s.Id("t"), Schema));
        var b = await s.Catalog.LoadTableAsync(a.Identifier);
        await a.UpdateSchemaAsync(new Schema(0, [.. Schema.Columns, NestedField.Optional(7, "x", PrimitiveType.String)]));
        await Assert.ThrowsAsync<CommitFailedException>(() =>
            b.UpdateSchemaAsync(new Schema(0, [.. Schema.Columns, NestedField.Optional(7, "y", PrimitiveType.Long)])));
        await Assert.ThrowsAsync<CommitFailedException>(() =>
            b.CommitAsync([new UpdateRequirement.AssertTableUuid(Guid.NewGuid())], [new MetadataUpdate.SetProperties(new Dictionary<string, string> { ["k"] = "v" })]));
    }

    [Spec("Point in Time Reads (Time Travel)")]
    [Spec("Schema Evolution")]
    [SkippableTheory]
    [MemberData(nameof(Catalogs))]
    public async Task Overwrite_schema_evolution_and_time_travel(string kind)
    {
        Require.Catalog(kind);
        await using var s = await ScopeAsync(kind, "evo");
        var t = await s.Catalog.CreateTableAsync(new CreateTableRequest(s.Id("t"), Schema, Spec));
        var first = await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, Rows(0, 20))).CommitAsync();
        await SnapshotProducer.Overwrite(t, f => (string?)f.Partition[2] == "us").CommitAsync();
        Assert.Equal(10, (await TableScan.ReadAllAsync(t)).Count);

        await t.UpdateSchemaAsync(new Schema(0, [.. Schema.Columns, NestedField.Optional(7, "note", PrimitiveType.String)]));
        var more = Rows(100, 4).Select(r => r.Append("late").ToArray()).ToList();
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, more)).CommitAsync();

        var loaded = await s.Catalog.LoadTableAsync(t.Identifier);
        var rows = await TableScan.ReadAllAsync(loaded);
        Assert.Equal(14, rows.Count);
        Assert.Equal(4, rows.Count(r => (string?)r[6] == "late"));
        if (!TestEnvironment.KeepsSnapshotHistory(kind)) return;
        Assert.Equal(20, (await TableScan.ReadAllAsync(loaded, first.SnapshotId)).Count);
        // create, append, overwrite, add-schema, append: five metadata files, four in the log.
        Assert.True(loaded.Metadata.MetadataLog.Count >= 4, $"metadata-log has {loaded.Metadata.MetadataLog.Count} entries");
    }

    [Spec("rest: renameTable")]
    [Spec("rest: registerTable")]
    [Spec("rest: dropTable")]
    [Spec("rest: tableExists")]
    [Spec("rest: listTables")]
    [SkippableTheory]
    [MemberData(nameof(Catalogs))]
    public async Task Rename_register_and_drop(string kind)
    {
        Require.Catalog(kind);
        await using var s = await ScopeAsync(kind, "ren");
        var t = await s.Catalog.CreateTableAsync(new CreateTableRequest(s.Id("a"), Schema));
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, Rows(0, 3))).CommitAsync();

        await s.Catalog.RenameTableAsync(s.Id("a"), s.Id("b"));
        Assert.False(await s.Catalog.TableExistsAsync(s.Id("a")));
        var b = await s.Catalog.LoadTableAsync(s.Id("b"));
        Assert.Equal(t.Metadata.TableUuid, b.Metadata.TableUuid);

        // Register adopts existing metadata (how tables move between catalogs): unregister b, keeping its files, then
        // register its metadata as c. (Registering while b is still live would give two tables one location, which
        // Polaris rightly refuses.)
        await s.Catalog.DropTableAsync(s.Id("b"), purge: false);
        Assert.False(await s.Catalog.TableExistsAsync(s.Id("b")));
        var registered = await s.Catalog.RegisterTableAsync(s.Id("c"), b.MetadataLocation!);
        Assert.Equal(t.Metadata.TableUuid, registered.Metadata.TableUuid);
        Assert.Equal(3, (await TableScan.ReadAllAsync(registered)).Count);

        await s.Catalog.DropTableAsync(s.Id("c"));
        await Assert.ThrowsAsync<NoSuchTableException>(() => s.Catalog.LoadTableAsync(s.Id("c")));
    }

    [Spec("Version 3")]
    [SkippableTheory]
    [MemberData(nameof(Catalogs))]
    public async Task Format_v3_tables(string kind)
    {
        Require.Catalog(kind);
        // Nessie 0.109 has no v3: create returns a v2 table and upgrade-format-version to 3 fails with a 500.
        Skip.If(kind == "nessie", "Nessie 0.109 does not support format v3 tables");
        await using var s = await ScopeAsync(kind, "v3");
        var t = await s.Catalog.CreateTableAsync(new CreateTableRequest(s.Id("t"), Schema, Spec, FormatVersion: 3));
        Assert.Equal(3, t.Metadata.FormatVersion);
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, Rows(0, 12))).CommitAsync();
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, Rows(12, 8))).CommitAsync();
        var loaded = await s.Catalog.LoadTableAsync(t.Identifier);
        Assert.Equal(20, loaded.Metadata.NextRowId);
        Assert.Equal(12, loaded.Metadata.CurrentSnapshot!.FirstRowId);
    }

    [Spec("rest: commitTransaction")]
    [SkippableTheory]
    [MemberData(nameof(Catalogs))]
    public async Task Multi_table_transactions_apply_all_or_nothing(string kind)
    {
        Require.Catalog(kind);
        await using var s = await ScopeAsync(kind, "tx");
        var a = await s.Catalog.CreateTableAsync(new CreateTableRequest(s.Id("a"), Schema, Spec));
        var b = await s.Catalog.CreateTableAsync(new CreateTableRequest(s.Id("b"), Schema, Spec));
        async Task<TableCommit> Stage(Table t, int from) =>
            await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, Rows(from, 3))).StageAsync();

        if (kind is "glue" or "gravitino")
        {
            // Glue has no multi-table atomic update; Gravitino 1.3's REST service does not advertise one (checked from /v1/config).
            await Assert.ThrowsAsync<NotSupportedException>(async () => await s.Catalog.CommitTransactionAsync([await Stage(a, 0), await Stage(b, 0)]));
            return;
        }
        await s.Catalog.CommitTransactionAsync([await Stage(a, 0), await Stage(b, 100)]);
        Assert.Equal(3, (await TableScan.ReadAllAsync(await s.Catalog.LoadTableAsync(a.Identifier))).Count);
        Assert.Equal(3, (await TableScan.ReadAllAsync(await s.Catalog.LoadTableAsync(b.Identifier))).Count);

        // a moves underneath a staged transaction: neither table may change.
        var staleA = await Stage(await s.Catalog.LoadTableAsync(a.Identifier), 10);
        var stagedB = await Stage(await s.Catalog.LoadTableAsync(b.Identifier), 110);
        await SnapshotProducer.Append(await s.Catalog.LoadTableAsync(a.Identifier)).AddFiles(await TableWriter.WriteAsync(a, Rows(20, 1))).CommitAsync();
        await Assert.ThrowsAsync<CommitFailedException>(() => s.Catalog.CommitTransactionAsync([staleA, stagedB]));
        Assert.Equal(4, (await TableScan.ReadAllAsync(await s.Catalog.LoadTableAsync(a.Identifier))).Count);
        Assert.Equal(3, (await TableScan.ReadAllAsync(await s.Catalog.LoadTableAsync(b.Identifier))).Count);
    }
}
