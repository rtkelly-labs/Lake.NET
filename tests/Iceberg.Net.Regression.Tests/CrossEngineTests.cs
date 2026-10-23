using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// Interop in both directions with PyIceberg (interop/pyiceberg_oracle.py), through the same catalog
/// and storage. The REST variant also exercises the Java reference server in the middle, so
/// "C# writes, Java commits, Python reads" is covered end to end.
/// </summary>
[Spec("Scan Planning")]
[Spec("Partition Transforms")]
[Trait("Category", "Regression")]
public sealed class CrossEngineTests
{
    public static TheoryData<string> Catalogs => CatalogConformanceTests.Catalogs;

    [SkippableTheory]
    [MemberData(nameof(Catalogs))]
    public async Task Dotnet_writes_pyiceberg_reads(string kind)
    {
        Require.Catalog(kind);
        Require.Oracle();
        var env = TestEnvironment.Current;
        using var catalog = await env.CatalogAsync(kind);
        var ns = Namespace.Of(env.NewNamespace($"{kind}_x2py"));
        await catalog.CreateNamespaceAsync(ns);
        var id = new TableIdentifier(ns, "events");
        try
        {
            var table = await catalog.CreateTableAsync(new CreateTableRequest(id, CatalogConformanceTests.Schema, CatalogConformanceTests.Spec));
            await SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, CatalogConformanceTests.Rows(0, 25))).CommitAsync();
            await SnapshotProducer.Overwrite(table, f => (string?)f.Partition[2] == "us")
                .AddFiles(await TableWriter.WriteAsync(table, CatalogConformanceTests.Rows(1000, 1).Select(r => { r[4] = "us"; return r; }).ToList()))
                .CommitAsync();

            var py = await env.OracleAsync("read", kind, $"{ns}.events");
            var expectedIds = Enumerable.Range(0, 25).Where(i => i % 2 == 0).Select(i => (long)i).Append(1000).ToList();
            Assert.Equal(expectedIds, py["ids"]!.AsArray().Select(i => i!.GetValue<long>()));
            Assert.Equal(TestEnvironment.KeepsSnapshotHistory(kind) ? 2 : 1, py["snapshot_count"]!.GetValue<int>());
            Assert.Equal("overwrite", py["summary"]!["operation"]!.GetValue<string>());
            Assert.Equal(["event_ts_day:day", "id_bucket:bucket[4]", "region:identity"], py["spec"]!.AsArray().Select(s => s!.GetValue<string>()));

            // Column values, not just keys: PyIceberg must decode our Parquet + metrics identically.
            var row = py["rows"]!.AsArray().First(r => r!["id"]!.GetValue<long>() == 2)!;
            var mine = CatalogConformanceTests.Rows(2, 1)[0];
            Assert.Equal((string)mine[1]!, row["name"]!.GetValue<string>());
            Assert.Equal((double)mine[2]!, row["amount"]!.GetValue<double>());
            Assert.Equal((long)mine[3]!, row["event_ts"]!.GetValue<long>());
        }
        finally
        {
            await catalog.DropTableAsync(id, purge: true);
            await catalog.DropNamespaceAsync(ns);
        }
    }

    [SkippableTheory]
    [MemberData(nameof(Catalogs))]
    public async Task Pyiceberg_writes_dotnet_reads_and_appends(string kind)
    {
        Require.Catalog(kind);
        Require.Oracle();
        var env = TestEnvironment.Current;
        var ns = env.NewNamespace($"{kind}_py2x");
        JsonNode py = await env.OracleAsync("write", kind, $"{ns}.events", "30");
        Assert.Equal(30, py["row_count"]!.GetValue<int>());

        using var catalog = await env.CatalogAsync(kind);
        var id = new TableIdentifier(Namespace.Of(ns), "events");
        try
        {
            var table = await catalog.LoadTableAsync(id);
            var rows = await TableScan.ReadAllAsync(table);
            Assert.Equal(py["ids"]!.AsArray().Select(i => i!.GetValue<long>()), rows.Select(r => (long)r[0]!).Order());

            // Our transforms must put rows into PyIceberg's (Rust) partitions.
            foreach (var task in await TableScan.PlanFilesAsync(table))
                foreach (var r in await ParquetFiles.ReadAsync(table.IO, task.File.FilePath, table.Schema))
                    Assert.Equal(task.File.Partition, table.Spec.Partition(table.Schema, fid => r[fid - 1]));

            // Then append on top of PyIceberg's snapshot and let PyIceberg read the result.
            await SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, CatalogConformanceTests.Rows(30, 5))).CommitAsync();
            var after = await env.OracleAsync("read", kind, $"{ns}.events");
            Assert.Equal(35, after["row_count"]!.GetValue<int>());
            Assert.Equal(TestEnvironment.KeepsSnapshotHistory(kind) ? 2 : 1, after["snapshot_count"]!.GetValue<int>());
        }
        finally
        {
            await catalog.DropTableAsync(id, purge: true);
            await catalog.DropNamespaceAsync(Namespace.Of(ns));
        }
    }
}
