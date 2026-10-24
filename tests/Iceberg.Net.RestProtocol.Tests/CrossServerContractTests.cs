using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;
using Iceberg.Net.RestServer.Tests;
using Iceberg.Net.Types;

namespace Iceberg.Net.RestProtocol.Tests;

/// <summary>
/// The REST error contract a client relies on, through <see cref="Catalog.RestCatalog"/>, against every REST server the
/// suite can reach: this repository's (in-process, always), and in emulation Apache's Java reference server ("rest"),
/// Polaris, Nessie, Lakekeeper and Gravitino. A server that answers differently fails here, not in a caller's retry loop.
/// </summary>
[Trait("Category", "Regression")]
public sealed class CrossServerContractTests
{
    public static TheoryData<string> Servers => new() { "dotnet-rest", "rest", "polaris", "nessie", "lakekeeper", "gravitino" };

    private static readonly Schema Schema = new(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String)]);

    private sealed class Target(ICatalog catalog, ServerHost? host) : IAsyncDisposable
    {
        public ICatalog Catalog { get; } = catalog;

        public async ValueTask DisposeAsync()
        {
            Catalog.Dispose();
            if (host is not null) await host.DisposeAsync();
        }
    }

    private static async Task<Target> ConnectAsync(string kind)
    {
        if (kind == "dotnet-rest")
        {
            var host = ServerHost.Start();
            return new Target(await host.ConnectAsync(), host);
        }
        Require.Catalog(kind);
        return new Target(await TestEnvironment.Current.CatalogAsync(kind), null);
    }

    /// <summary>
    /// Known server difference: Nessie's namespaces are implicit (a path prefix in its commit tree), so listing the tables
    /// of one that was never created is an empty list, not a 404. Every other server answers NoSuchNamespace.
    /// </summary>
    private static bool ListingAMissingNamespaceFails(string kind) => kind != "nessie";

    private static Namespace NewNamespace() => Namespace.Of($"contract_{Guid.NewGuid():N}"[..20]);

    private static async Task CleanUpAsync(ICatalog catalog, Namespace ns)
    {
        try
        {
            foreach (var t in await catalog.ListTablesAsync(ns)) await catalog.DropTableAsync(t);
            await catalog.DropNamespaceAsync(ns);
        }
        catch (Exception)
        {
            // best effort: the assertion that failed is the one to report
        }
    }

    [Spec("rest: loadNamespaceMetadata")]
    [Spec("rest: loadTable")]
    [Spec("rest: listTables")]
    [SkippableTheory]
    [MemberData(nameof(Servers))]
    public async Task Missing_namespaces_and_tables_are_told_apart(string kind)
    {
        await using var target = await ConnectAsync(kind);
        var ns = NewNamespace();
        await target.Catalog.CreateNamespaceAsync(ns);
        try
        {
            await Assert.ThrowsAsync<NoSuchTableException>(() => target.Catalog.LoadTableAsync(new TableIdentifier(ns, "missing")));
            var absent = Namespace.Of($"{ns.Levels[0]}_absent");
            if (ListingAMissingNamespaceFails(kind)) await Assert.ThrowsAsync<NoSuchNamespaceException>(() => target.Catalog.ListTablesAsync(absent));
            else Assert.Empty(await target.Catalog.ListTablesAsync(absent));
            Assert.False(await target.Catalog.TableExistsAsync(new TableIdentifier(ns, "missing")));
        }
        finally
        {
            await CleanUpAsync(target.Catalog, ns);
        }
    }

    [Spec("rest: createNamespace")]
    [Spec("rest: createTable")]
    [Spec("rest: renameTable")]
    [SkippableTheory]
    [MemberData(nameof(Servers))]
    public async Task Taken_names_are_already_exists(string kind)
    {
        await using var target = await ConnectAsync(kind);
        var ns = NewNamespace();
        await target.Catalog.CreateNamespaceAsync(ns);
        try
        {
            await Assert.ThrowsAsync<AlreadyExistsException>(() => target.Catalog.CreateNamespaceAsync(ns));
            await target.Catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(ns, "a"), Schema));
            await target.Catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(ns, "b"), Schema));
            await Assert.ThrowsAsync<AlreadyExistsException>(() => target.Catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(ns, "a"), Schema)));
            await Assert.ThrowsAsync<AlreadyExistsException>(() => target.Catalog.RenameTableAsync(new TableIdentifier(ns, "a"), new TableIdentifier(ns, "b")));
        }
        finally
        {
            await CleanUpAsync(target.Catalog, ns);
        }
    }

    [Spec("rest: updateTable")]
    [SkippableTheory]
    [MemberData(nameof(Servers))]
    public async Task A_stale_commit_is_commit_failed_and_changes_nothing(string kind)
    {
        await using var target = await ConnectAsync(kind);
        var ns = NewNamespace();
        await target.Catalog.CreateNamespaceAsync(ns);
        try
        {
            var id = new TableIdentifier(ns, "t");
            await target.Catalog.CreateTableAsync(new CreateTableRequest(id, Schema));
            await Assert.ThrowsAsync<CommitFailedException>(() => target.Catalog.CommitTableAsync(new TableCommit(id,
                [new UpdateRequirement.AssertTableUuid(Guid.NewGuid())],
                [new MetadataUpdate.SetProperties(new Dictionary<string, string> { ["contract"] = "stale" })])));
            Assert.False((await target.Catalog.LoadTableAsync(id)).Metadata.Properties.ContainsKey("contract"));
        }
        finally
        {
            await CleanUpAsync(target.Catalog, ns);
        }
    }

    [Spec("rest: dropNamespace")]
    [SkippableTheory]
    [MemberData(nameof(Servers))]
    public async Task A_namespace_holding_a_table_is_not_dropped(string kind)
    {
        await using var target = await ConnectAsync(kind);
        var ns = NewNamespace();
        await target.Catalog.CreateNamespaceAsync(ns);
        try
        {
            await target.Catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(ns, "t"), Schema));
            await Assert.ThrowsAnyAsync<Exception>(() => target.Catalog.DropNamespaceAsync(ns));
            Assert.True(await target.Catalog.NamespaceExistsAsync(ns));
        }
        finally
        {
            await CleanUpAsync(target.Catalog, ns);
        }
    }
}
