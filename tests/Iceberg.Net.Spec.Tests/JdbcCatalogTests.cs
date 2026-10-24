using Microsoft.Data.Sqlite;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.IO;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>The JDBC catalog on SQLite: Java's V0 layout, the V1 migration, and views sharing the table namespace.</summary>
public sealed class JdbcCatalogTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-jdbc", Guid.NewGuid().ToString("N"));
    private string Db => Path.Combine(_dir, "catalog.db");

    public JdbcCatalogTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
    }

    private async Task<JdbcCatalog> Open(bool views)
    {
        var catalog = new JdbcCatalog(() => new SqliteConnection($"Data Source={Db}"), Path.Combine(_dir, "wh"), new LocalFileIO(), "c", views);
        await catalog.InitializeAsync();
        return catalog;
    }

    private static readonly Schema Schema = new(0, [NestedField.Require(1, "id", PrimitiveType.Long)]);
    private static readonly ViewDefinition Def = new(Schema, "SELECT id FROM t", "spark", ["ns"]);

    [Fact]
    [Spec("Optimistic Concurrency")]
    public async Task V0_layout_supports_tables_and_refuses_views()
    {
        var catalog = await Open(views: false);
        await catalog.CreateNamespaceAsync(Namespace.Of("ns"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("ns", "t"), Schema, PartitionSpec.Unpartitioned));
        await TableMutations.InsertAsync(t, [[1L]]);
        var stale = await catalog.LoadTableAsync(t.Identifier);
        await TableMutations.InsertAsync(t, [[2L]]);
        Assert.Equal(2, (await TableScan.ReadAllAsync(await catalog.LoadTableAsync(t.Identifier))).Count);
        // A writer holding stale metadata rebases rather than overwriting.
        await TableMutations.InsertAsync(stale, [[3L]]);
        Assert.Equal(3, (await TableScan.ReadAllAsync(await catalog.LoadTableAsync(t.Identifier))).Count);
        await Assert.ThrowsAsync<NotSupportedException>(() => catalog.CreateViewAsync(TableIdentifier.Of("ns", "v"), Def));
    }

    [Fact]
    public async Task Upgrading_to_v1_keeps_existing_tables_and_separates_views()
    {
        var v0 = await Open(views: false);
        await v0.CreateNamespaceAsync(Namespace.Of("ns"));
        await v0.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("ns", "t"), Schema, PartitionSpec.Unpartitioned));

        var catalog = await Open(views: true); // ALTER TABLE adds iceberg_type; the old row is NULL, i.e. a table
        await Open(views: true); // idempotent
        Assert.Equal([TableIdentifier.Of("ns", "t")], await catalog.ListTablesAsync(Namespace.Of("ns")));

        var id = TableIdentifier.Of("ns", "v");
        var view = await catalog.CreateViewAsync(id, Def);
        Assert.Equal([id], await catalog.ListViewsAsync(Namespace.Of("ns")));
        Assert.Equal([TableIdentifier.Of("ns", "t")], await catalog.ListTablesAsync(Namespace.Of("ns")));
        Assert.False(await catalog.TableExistsAsync(id));
        await Assert.ThrowsAsync<NoSuchTableException>(() => catalog.LoadTableAsync(id));
        await Assert.ThrowsAnyAsync<Exception>(() => catalog.CreateTableAsync(new CreateTableRequest(id, Schema, PartitionSpec.Unpartitioned)));
        await Assert.ThrowsAsync<AlreadyExistsException>(() => catalog.CreateViewAsync(TableIdentifier.Of("ns", "t"), Def));
        await Assert.ThrowsAsync<NoSuchTableException>(() => catalog.DropViewAsync(TableIdentifier.Of("ns", "t"))); // not a view

        var replaced = await catalog.ReplaceViewAsync(id, new ViewDefinition(Schema, "SELECT id FROM t WHERE id > 0", "spark", ["ns"]));
        Assert.Equal(view.Metadata.ViewUuid, replaced.Metadata.ViewUuid);
        Assert.Contains("id > 0", (await catalog.LoadViewAsync(id)).Sql("spark"));
        Assert.EndsWith(".metadata.json", replaced.Metadata.MetadataFileLocation);
        Assert.StartsWith("00001-", Path.GetFileName(replaced.Metadata.MetadataFileLocation));

        await catalog.RenameViewAsync(id, TableIdentifier.Of("ns", "v2"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => catalog.DropNamespaceAsync(Namespace.Of("ns")));
        await catalog.DropViewAsync(TableIdentifier.Of("ns", "v2"));
        Assert.Empty(await catalog.ListViewsAsync(Namespace.Of("ns")));
        Assert.True(await catalog.TableExistsAsync(TableIdentifier.Of("ns", "t")));
    }

    [Fact]
    public async Task Concurrent_view_replacements_do_not_lose_updates()
    {
        var catalog = await Open(views: true);
        await catalog.CreateNamespaceAsync(Namespace.Of("ns"));
        var id = TableIdentifier.Of("ns", "v");
        await catalog.CreateViewAsync(id, Def);
        var results = await Task.WhenAll(Enumerable.Range(0, 6).Select(async i =>
        {
            try { await catalog.ReplaceViewAsync(id, new ViewDefinition(Schema, $"SELECT {i}", "spark", ["ns"])); return true; }
            catch (CommitFailedException) { return false; }
        }));
        var current = await catalog.LoadViewAsync(id);
        Assert.Equal(1 + results.Count(ok => ok), current.Metadata.Versions.Count); // every acknowledged replace is in history
    }

    [Fact]
    [Spec("rest: commitTransaction")]
    public async Task Staged_merges_commit_across_tables_in_one_database_transaction()
    {
        var catalog = await Open(views: false);
        Assert.True(catalog.SupportsMultiTableTransactions);
        await catalog.CreateNamespaceAsync(Namespace.Of("ns"));
        var a = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("ns", "a"), Schema, PartitionSpec.Unpartitioned));
        var b = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("ns", "b"), Schema, PartitionSpec.Unpartitioned));
        await TableMutations.InsertAsync(a, [[1L], [2L]]);
        await TableMutations.InsertAsync(b, [[1L]]);
        var stagedA = await TableMutations.StageMergeAsync(await catalog.LoadTableAsync(a.Identifier), [[1L], [3L]], ["id"], m => m.WhenMatchedDelete().WhenNotMatchedInsert());
        var stagedB = await TableMutations.StageMergeAsync(await catalog.LoadTableAsync(b.Identifier), [[5L]], ["id"], m => m.WhenNotMatchedInsert());
        // a moves after staging: the transaction's row for a is stale, so neither row moves until both are re-planned.
        await TableMutations.InsertAsync(a, [[4L]]);

        await StagedCommit.CommitAllAsync([stagedA, stagedB]);

        Assert.Equal((2, 2), (stagedA.Attempts, stagedB.Attempts));
        Assert.Equal([2L, 3L, 4L], (await TableScan.ReadAllAsync(await catalog.LoadTableAsync(a.Identifier))).Select(r => (long)r[0]!).Order());
        Assert.Equal([1L, 5L], (await TableScan.ReadAllAsync(await catalog.LoadTableAsync(b.Identifier))).Select(r => (long)r[0]!).Order());
    }
}
