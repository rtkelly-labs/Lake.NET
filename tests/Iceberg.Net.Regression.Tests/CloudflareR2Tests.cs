using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.IO;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// The Cloudflare target (docs/lake/reference/cloudflare.md), from the environment. Unset, the tests skip with the reason;
/// with <c>LAKE_CLOUDFLARE_REQUIRED=1</c> (the <c>cloudflare</c> workflow) a missing setting fails instead.
/// </summary>
public static class Cloudflare
{
    private static string? Env(string name) => Environment.GetEnvironmentVariable(name) is { Length: > 0 } v ? v : null;

    private static bool Required => Env("LAKE_CLOUDFLARE_REQUIRED") == "1";

    public static string? Account => Env("LAKE_R2_ACCOUNT_ID");
    public static string? Bucket => Env("LAKE_R2_BUCKET");

    public static S3FileIOOptions R2Options => S3FileIOOptions.ForR2(Account!, Env("LAKE_R2_ACCESS_KEY_ID")!, Env("LAKE_R2_SECRET_ACCESS_KEY")!, Env("LAKE_R2_JURISDICTION"));

    public static RestCatalogOptions BasinOptions => RestCatalogOptions.ForBasinCatalog(new Uri(Env("LAKE_BASIN_CATALOG_URI")!), Env("LAKE_BASIN_WAREHOUSE")!, Env("LAKE_BASIN_TOKEN")!);

    private static void Need(params string[] names)
    {
        var missing = names.Where(n => Env(n) is null).ToList();
        if (missing.Count == 0) return;
        var reason = $"Cloudflare target not configured: set {string.Join(", ", missing)}";
        if (Required) throw new InvalidOperationException("LAKE_CLOUDFLARE_REQUIRED=1 but: " + reason);
        Skip.If(true, reason);
    }

    public static void RequireR2() => Need("LAKE_R2_ACCOUNT_ID", "LAKE_R2_ACCESS_KEY_ID", "LAKE_R2_SECRET_ACCESS_KEY", "LAKE_R2_BUCKET");

    public static void RequireBasin() => Need("LAKE_BASIN_CATALOG_URI", "LAKE_BASIN_WAREHOUSE", "LAKE_BASIN_TOKEN");
}

/// <summary>The FileIO contract against R2's S3 API: conditional create, ranges, listings, NFC-normalised keys.</summary>
[Spec("File System Operations")]
[Trait("Category", "Cloudflare")]
public sealed class R2FileIOConformanceTests : FileIOConformanceTests
{
    private readonly string _root = $"s3://{Cloudflare.Bucket}/lake-net-conformance/{TestEnvironment.Current.RunId}/{Guid.NewGuid():N}/";

    protected override string Root => _root;

    protected override void RequireStore() => Cloudflare.RequireR2();

    protected override IFileIO Create() => new S3FileIO(Cloudflare.R2Options);
}

/// <summary>
/// Lake.NET against Basin Catalog (formerly R2 Data Catalog) as an Iceberg REST catalog, with the credentials it vends:
/// the round trip, schema evolution, time travel, a lost race, and drop with purge.
/// </summary>
[Spec("Metastore Tables")]
[Trait("Category", "Cloudflare")]
public sealed class BasinCatalogTests : IAsyncLifetime
{
    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Optional(3, "at", PrimitiveType.TimestampTz),
    ]);

    private readonly Namespace _ns = Namespace.Of($"lake_net_{TestEnvironment.Current.RunId}_{Guid.NewGuid().ToString("N")[..6]}".ToLowerInvariant());
    private RestCatalog? _catalog;

    private static List<object?[]> Rows(int start, int count) =>
        Enumerable.Range(start, count).Select(i => new object?[] { (long)i, i % 4 == 0 ? null : $"n{i}", 1_700_000_000_000_000L + i }).ToList();

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_catalog is null) return;
        foreach (var t in await _catalog.ListTablesAsync(_ns))
            await _catalog.DropTableAsync(t, purge: true);
        await _catalog.DropNamespaceAsync(_ns);
        _catalog.Dispose();
    }

    private async Task<RestCatalog> CatalogAsync()
    {
        Cloudflare.RequireBasin();
        _catalog = await RestCatalog.ConnectAsync(Cloudflare.BasinOptions, name: "basin");
        await _catalog.CreateNamespaceAsync(_ns);
        return _catalog;
    }

    [SkippableFact]
    public async Task Tables_written_through_Basin_Catalog_read_back_evolve_and_time_travel()
    {
        var catalog = await CatalogAsync();
        var table = await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(_ns, "events"), Schema));
        await SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, Rows(0, 100))).CommitAsync();
        var first = table.Metadata.CurrentSnapshotId!.Value;

        table = await catalog.LoadTableAsync(table.Identifier);
        await table.UpdateSchemaAsync(u => u.AddColumn("region", PrimitiveType.String));
        table = await catalog.LoadTableAsync(table.Identifier);
        await TableMutations.InsertAsync(table, Rows(100, 50).Select(r => (object?[])[.. r, "eu"]).ToList());
        table = await catalog.LoadTableAsync(table.Identifier);

        Assert.Equal(150, (await TableScan.ReadAllAsync(table)).Count);
        Assert.Equal(50, await TableScan.CountAsync(table, new ScanRequest { Filter = Expr.Equal("region", "eu") }));
        Assert.Equal(100, await TableScan.CountAsync(table, new ScanRequest { Snapshot = SnapshotSelector.Id(first) }));
        Assert.StartsWith($"s3://{Cloudflare.Bucket}/", table.Location, StringComparison.Ordinal);
        await TableValidator.AssertValidAsync(table, new ValidationOptions { Deep = true, AllSnapshots = true });
    }

    [SkippableFact]
    public async Task A_lost_race_fails_or_replans_and_loses_no_rows()
    {
        var catalog = await CatalogAsync();
        var table = await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(_ns, "race"), Schema));
        var a = await catalog.LoadTableAsync(table.Identifier);
        var b = await catalog.LoadTableAsync(table.Identifier);
        await Task.WhenAll(
            SnapshotProducer.Append(a).AddFiles(await TableWriter.WriteAsync(a, Rows(0, 10))).CommitAsync(),
            SnapshotProducer.Append(b).AddFiles(await TableWriter.WriteAsync(b, Rows(10, 10))).CommitAsync());
        Assert.Equal(20, (await TableScan.ReadAllAsync(await catalog.LoadTableAsync(table.Identifier))).Count);
    }

    [SkippableFact]
    public async Task Format_version_3_is_created_or_refused_cleanly()
    {
        // Whether Basin Catalog accepts v3 is recorded here rather than assumed (docs/lake/reference/cloudflare.md).
        var catalog = await CatalogAsync();
        try
        {
            var table = await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(_ns, "v3"), Schema, FormatVersion: 3));
            await SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, Rows(0, 10))).CommitAsync();
            table = await catalog.LoadTableAsync(table.Identifier);
            Assert.Equal(3, table.Metadata.FormatVersion);
            Assert.Equal(10, (await TableScan.ReadAllAsync(table)).Count);
        }
        catch (HttpRequestException e) when ((int?)e.StatusCode is 400 or 406 or 422)
        {
            Assert.False(await catalog.TableExistsAsync(new TableIdentifier(_ns, "v3")));
        }
    }
}
