using System.Text.Json;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Partitioning;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Tables written by earlier builds of this library, committed under <c>fixtures/lake-net/&lt;label&gt;/</c>, must stay
/// readable, valid and writable (docs/lake/goal.md, G3; gap review E3). Every snapshot of every table is read and compared
/// with the rows recorded when it was written, the table is validated deeply, and a new append must commit on top.
/// A release adds its own label, so no later version can silently lose the ability to read what an earlier one wrote.
/// Regenerate: <c>LAKE_WRITE_GOLDEN=&lt;label&gt; dotnet test --filter OwnGoldenFixtureTests.Write</c>, which writes
/// <c>tests/Iceberg.Net.Spec.Tests/fixtures/lake-net/&lt;label&gt;/</c> in the source tree.
/// </summary>
public sealed class OwnGoldenFixtureTests : IDisposable
{
    /// <summary>The location recorded in the fixtures' metadata; mapped to a temporary copy when they are read.</summary>
    private const string Root = "file:///tmp/lake-net-golden/warehouse";
    private static readonly string Fixtures = Path.Combine(AppContext.BaseDirectory, "fixtures", "lake-net");

    private readonly string _copy = Path.Combine(Path.GetTempPath(), "lake-net-golden", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_copy)) Directory.Delete(_copy, recursive: true);
    }

    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Optional(3, "day", PrimitiveType.Date),
        NestedField.Optional(4, "amount", new DecimalType(18, 2)),
        NestedField.Optional(5, "tags", new ListType(6, PrimitiveType.String, ElementRequired: false)),
    ], identifierFieldIds: [1]);

    private static List<object?[]> Rows(int start, int count, string suffix = "") => Enumerable.Range(start, count).Select(i => new object?[]
    {
        (long)i, i % 7 == 0 ? null : $"n{i}{suffix}", 19_700 + i % 4, new IcebergDecimal(i * 125, 2),
        i % 5 == 0 ? null : (object)new List<object?> { $"t{i % 3}", i % 2 == 0 ? null : "x" },
    }).ToList();

    public static TheoryData<string> Labels()
    {
        var data = new TheoryData<string>();
        if (Directory.Exists(Fixtures))
            foreach (var dir in Directory.GetDirectories(Fixtures).Order(StringComparer.Ordinal)) data.Add(Path.GetFileName(dir));
        return data;
    }

    [Fact]
    public void At_least_one_label_is_committed() => Assert.NotEmpty(Labels());

    [Spec("Scan Planning")]
    [Spec("Position Delete Files")]
    [Spec("Equality Delete Files")]
    [Spec("Deletion Vectors")]
    [Spec("Schema Evolution")]
    [Spec("Partition Evolution")]
    [Theory]
    [MemberData(nameof(Labels))]
    public async Task Every_snapshot_of_every_table_reads_as_written_validates_and_takes_a_new_commit(string label)
    {
        var dir = Path.Combine(Fixtures, label);
        var expected = JsonNode.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "expected.json")))!;
        var copy = Path.Combine(_copy, label);
        PrefixMappingFileIO.CopyTree(Path.Combine(dir, "warehouse"), copy);
        using var catalog = new InMemoryCatalog(Root, new PrefixMappingFileIO(Root, copy));
        await catalog.CreateNamespaceAsync(Namespace.Of("golden"));

        var tables = expected["tables"]!.AsObject();
        Assert.NotEmpty(tables);
        foreach (var (name, node) in tables)
        {
            var table = await catalog.RegisterTableAsync(new TableIdentifier(Namespace.Of("golden"), name), node!["metadata_location"]!.GetValue<string>());
            Assert.Equal(node["format_version"]!.GetValue<int>(), table.Metadata.FormatVersion);
            foreach (var snapshot in node["snapshots"]!.AsArray())
            {
                var id = snapshot!["id"]!.GetValue<long>();
                var want = snapshot["rows"]!.AsArray().Select(r => r!.GetValue<string>()).ToList();
                var got = await CanonicalAsync(table, SnapshotSelector.Id(id));
                Assert.True(want.SequenceEqual(got), $"{label}/{name} snapshot {id}: {got.Count} rows differ from the {want.Count} recorded");
            }
            foreach (var (refName, refNode) in node["refs"]!.AsObject())
                Assert.Equal(refNode!.GetValue<long>(), table.Metadata.Refs[refName].SnapshotId);
            await TableValidator.AssertValidAsync(table, new ValidationOptions { Deep = true, AllSnapshots = true });

            // Today's writer on yesterday's table.
            var before = (await TableScan.ReadAllAsync(table)).Count;
            await TableMutations.InsertAsync(table, [[9_000_000L + before, "new", 19_800, new IcebergDecimal(1, 2), null]]);
            table = await catalog.LoadTableAsync(table.Identifier);
            Assert.Equal(before + 1, (await TableScan.ReadAllAsync(table)).Count);
            await TableValidator.AssertValidAsync(table, new ValidationOptions { Deep = true });
        }
    }

    private static async Task<List<string>> CanonicalAsync(Table table, SnapshotSelector snapshot)
    {
        var rows = new List<object?[]>();
        await foreach (var row in TableScan.ReadRowsAsync(table, new ScanRequest { Snapshot = snapshot })) rows.Add(row);
        return rows.Select(r => NestedTypesTests.Canonical([r])).Order(StringComparer.Ordinal).ToList();
    }

    /// <summary>Writes a new label when <c>LAKE_WRITE_GOLDEN</c> is set; otherwise does nothing.</summary>
    [Fact]
    public async Task Write()
    {
        if (Environment.GetEnvironmentVariable("LAKE_WRITE_GOLDEN") is not { Length: > 0 } label) return;
        var source = FindSourceFixtures();
        var outDir = Path.Combine(source, label);
        if (Directory.Exists(outDir)) throw new InvalidOperationException($"{outDir} exists: a label is written once");
        var warehouse = Path.Combine(outDir, "warehouse");
        using var catalog = new InMemoryCatalog(Root, new PrefixMappingFileIO(Root, warehouse));
        await catalog.CreateNamespaceAsync(Namespace.Of("golden"));
        var tables = new JsonObject();

        foreach (var version in new[] { 2, 3 })
        {
            // Appends into a bucketed table, then key upserts and deletes under every delete mechanism the version has.
            foreach (var strategy in version == 2
                ? new[] { DeleteStrategy.CopyOnWrite, DeleteStrategy.PositionDeletes, DeleteStrategy.EqualityDeletes }
                : new[] { DeleteStrategy.CopyOnWrite, DeleteStrategy.PositionDeletes })
            {
                var name = $"v{version}_{strategy.ToString().ToLowerInvariant()}";
                var spec = PartitionSpec.BuilderFor(Schema).Bucket("id", 4).Build(); // equality deletes need the partition source in the key
                var table = await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(Namespace.Of("golden"), name), Schema, spec, FormatVersion: version));
                await TableMutations.InsertAsync(table, Rows(0, 40));
                await TableMutations.UpsertAsync(table, Rows(30, 20, "-u"), ["id"], strategy);
                await TableMutations.DeleteByKeysAsync(table, ["id"], Enumerable.Range(5, 6).Select(i => new object?[] { (long)i }), strategy);
                tables[name] = await RecordAsync(catalog, table.Identifier);
            }

            // Schema and partition evolution, then writes under the new schema and spec.
            {
                var name = $"v{version}_evolved";
                var table = await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(Namespace.Of("golden"), name), Schema,
                    PartitionSpec.BuilderFor(Schema).Identity("day").Build(), FormatVersion: version));
                await TableMutations.InsertAsync(table, Rows(0, 20));
                await table.UpdateSchemaAsync(u => u.AddColumn("region", PrimitiveType.String).RenameColumn("name", "title").DeleteColumn("tags"));
                table = await catalog.LoadTableAsync(table.Identifier);
                await table.UpdateSpecAsync(u => u.AddField("id", Transform.Bucket(4)).RemoveField("day"));
                table = await catalog.LoadTableAsync(table.Identifier);
                await TableMutations.InsertAsync(table, Enumerable.Range(20, 20).Select(i => new object?[] { (long)i, $"t{i}", 19_701, new IcebergDecimal(i, 2), i % 2 == 0 ? "eu" : null }).ToList());
                tables[name] = await RecordAsync(catalog, table.Identifier);
            }

            // A branch and a tag.
            {
                var name = $"v{version}_refs";
                var table = await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(Namespace.Of("golden"), name), Schema, FormatVersion: version));
                await TableMutations.InsertAsync(table, Rows(0, 10));
                table = await catalog.LoadTableAsync(table.Identifier);
                var first = table.Metadata.CurrentSnapshotId!.Value;
                await table.ManageSnapshots().CreateTag("v1", first).CreateBranch("audit", first).CommitAsync();
                table = await catalog.LoadTableAsync(table.Identifier);
                await TableMutations.InsertAsync(table, Rows(10, 10));
                tables[name] = await RecordAsync(catalog, table.Identifier);
            }
        }

        var doc = new JsonObject
        {
            ["writer"] = $"Lake.NET (Iceberg.Net.Data {typeof(TableWriter).Assembly.GetName().Version})",
            ["written"] = DateTime.UtcNow.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture),
            ["root"] = Root,
            ["tables"] = tables,
        };
        await File.WriteAllTextAsync(Path.Combine(outDir, "expected.json"), doc.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) + "\n");
    }

    private static async Task<JsonObject> RecordAsync(InMemoryCatalog catalog, TableIdentifier id)
    {
        var table = await catalog.LoadTableAsync(id);
        var snapshots = new JsonArray();
        foreach (var s in table.Metadata.Snapshots)
            snapshots.Add(new JsonObject
            {
                ["id"] = s.SnapshotId,
                ["operation"] = s.Summary.GetValueOrDefault("operation"),
                ["rows"] = new JsonArray((await CanonicalAsync(table, SnapshotSelector.Id(s.SnapshotId))).Select(r => (JsonNode)r).ToArray()),
            });
        var refs = new JsonObject();
        foreach (var (name, r) in table.Metadata.Refs) refs[name] = r.SnapshotId;
        return new JsonObject
        {
            ["metadata_location"] = table.MetadataLocation,
            ["format_version"] = table.Metadata.FormatVersion,
            ["snapshots"] = snapshots,
            ["refs"] = refs,
        };
    }

    private static string FindSourceFixtures()
    {
        for (var d = new DirectoryInfo(AppContext.BaseDirectory); d is not null; d = d.Parent)
            if (File.Exists(Path.Combine(d.FullName, "Iceberg.Net.slnx")))
                return Path.Combine(d.FullName, "tests", "Iceberg.Net.Spec.Tests", "fixtures", "lake-net");
        throw new InvalidOperationException("Iceberg.Net.slnx not found");
    }
}
