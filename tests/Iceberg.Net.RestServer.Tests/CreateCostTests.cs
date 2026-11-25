using Iceberg.Net.Catalog;
using Iceberg.Net.IO;
using Iceberg.Net.TestSupport;
using Microsoft.Extensions.DependencyInjection;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// A create checks that its directories don't overlap any other table's, which needs every table's directories. Those
/// come from immutable metadata files, so after the first check they are cached by metadata location: the storage reads
/// of one more create must not grow with the number of tables (gap review S6).
/// </summary>
public sealed class CreateCostTests
{
    [Fact]
    public async Task Creating_one_more_table_reads_no_other_tables_metadata_again()
    {
        var metered = new MeteredFileIO(new LocalFileIO());
        await using var host = ServerHost.Start(services: s => s.AddSingleton<IFileIO>(metered));
        using var catalog = await host.ConnectAsync();
        await catalog.CreateNamespaceAsync(Namespace.Of("many"));
        for (var i = 0; i < 30; i++)
            await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("many", $"t{i}"), ScanPlanningTests.Schema));

        metered.Reset();
        await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("many", "one_more"), ScanPlanningTests.Schema));
        var reads = metered.Counts.GetValueOrDefault("read");
        Assert.True(reads <= 2, $"one create read {reads} metadata files with 30 tables in the catalog");
    }
}
