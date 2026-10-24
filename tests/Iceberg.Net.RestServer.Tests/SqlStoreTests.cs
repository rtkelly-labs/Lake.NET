using Microsoft.Data.Sqlite;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.IO;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// The SQL store keeps Java's JdbcCatalog tables, so the same database opens in this library's <see cref="JdbcCatalog"/>
/// (and in Java's JdbcCatalog and PyIceberg's SqlCatalog, which share that layout).
/// </summary>
public sealed class SqlStoreTests
{
    [Fact]
    public async Task Tables_written_through_the_server_open_in_the_jdbc_catalog_and_back()
    {
        await using var host = ServerHost.Start("sqlite");
        using var rest = await host.ConnectAsync();
        await rest.CreateNamespaceAsync(Namespace.Of("db"), new Dictionary<string, string> { ["owner"] = "eng" });
        var t = await rest.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "events"), CatalogContractTests.Schema));
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, CatalogContractTests.Rows(0, 7))).CommitAsync();

        using var jdbc = new JdbcCatalog(() => new SqliteConnection($"Data Source={host.Database};Default Timeout=30"), host.Warehouse, new LocalFileIO(), "rest", views: true);
        await jdbc.InitializeAsync();
        Assert.True(await jdbc.NamespaceExistsAsync(Namespace.Of("db")));
        var viaJdbc = await jdbc.LoadTableAsync(t.Identifier);
        Assert.Equal(7, (await TableScan.ReadAllAsync(viaJdbc)).Count);

        // A commit through the JDBC catalog is what the server serves next.
        await SnapshotProducer.Append(viaJdbc).AddFiles(await TableWriter.WriteAsync(viaJdbc, CatalogContractTests.Rows(7, 3))).CommitAsync();
        Assert.Equal(10, (await TableScan.ReadAllAsync(await rest.LoadTableAsync(t.Identifier))).Count);

        // And a table the JDBC catalog creates is listed by the server.
        await jdbc.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "from_jdbc"), CatalogContractTests.Schema));
        Assert.Contains(TableIdentifier.Of("db", "from_jdbc"), await rest.ListTablesAsync(Namespace.Of("db")));
    }

    [Fact]
    public async Task A_v0_database_is_migrated_and_catalog_names_are_isolated()
    {
        var dir = Path.Combine(Path.GetTempPath(), "iceberg-restserver-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var db = Path.Combine(dir, "v0.db");
        try
        {
            // A V0 database, as PyIceberg's SqlCatalog creates it (no iceberg_type column).
            using (var v0 = new JdbcCatalog(() => new SqliteConnection($"Data Source={db}"), dir, new LocalFileIO(), "other"))
            {
                await v0.InitializeAsync();
                await v0.CreateNamespaceAsync(Namespace.Of("theirs"));
                await v0.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("theirs", "t"), CatalogContractTests.Schema));
            }
            var store = SqlCatalogStore.Sqlite(db, "rest");
            await store.InitializeAsync();
            await store.CreateNamespaceAsync(Namespace.Of("mine"), new Dictionary<string, string>());
            Assert.Equal([Namespace.Of("mine")], await store.ListNamespacesAsync(null));
            Assert.Null(await store.LoadNamespaceAsync(Namespace.Of("theirs")));
            Assert.NotNull(await SqlCatalogStore.Sqlite(db, "other").GetAsync(TableIdentifier.Of("theirs", "t"), CatalogEntryKind.Table));
            await Assert.ThrowsAsync<ArgumentException>(() => store.CreateNamespaceAsync(Namespace.Of("a.b"), new Dictionary<string, string>()));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            Directory.Delete(dir, recursive: true);
        }
    }
}
