using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.IO;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Crash consistency: every write an operation performs is made to fail, before or after it lands. Whatever
/// happens, the table must read exactly the state before or exactly the state after the operation (atomicity),
/// and must pass a deep fsck (nothing it references is missing, metadata stays consistent).
/// </summary>
[Spec("Commit Conflict Resolution and Retry")]
[Spec("File System Operations")]
public sealed class FaultInjectionTests
{
    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Require(3, "event_date", PrimitiveType.Date),
    ], identifierFieldIds: [1, 3]);

    private static object?[] Row(long id, string name, int day) => [id, name, 19_700 + day];

    public sealed record Operation(string Name, int FormatVersion, Func<Table, Task> Run)
    {
        public override string ToString() => $"{Name} v{FormatVersion}";
    }

    public static TheoryData<Operation> Operations()
    {
        var data = new TheoryData<Operation>();
        foreach (var v in new[] { 2, 3 })
        {
            data.Add(new Operation("insert", v, t => TableMutations.InsertAsync(t, [Row(100, "n", 0)])));
            foreach (var s in new[] { DeleteStrategy.CopyOnWrite, DeleteStrategy.PositionDeletes, DeleteStrategy.EqualityDeletes })
            {
                data.Add(new Operation($"upsert/{s}", v, t => TableMutations.UpsertAsync(t, [Row(1, "u", 1), Row(50, "u", 2)], strategy: s)));
                data.Add(new Operation($"replace-groups/{s}", v, t => TableMutations.ReplaceGroupsAsync(t, [Row(60, "g", 0)], ["event_date"], s)));
            }
            data.Add(new Operation("delete-where/position", v, t => TableMutations.DeleteWhereAsync(t, r => (long)r[0]! % 4 == 0, DeleteStrategy.PositionDeletes)));
            data.Add(new Operation("drop-partition", v, t => SnapshotProducer.DeleteFiles(t, f => (int?)f.Partition[0] == 19_702).CommitAsync()));
            data.Add(new Operation("schema-change", v, t => t.UpdateSchemaAsync(new Schema(0, [.. Schema.Columns, NestedField.Optional(4, "extra", PrimitiveType.Int)], [1, 3]))));
            data.Add(new Operation("ndv-statistics", v, t => PuffinStatistics.ComputeNdvAsync(t)));
        }
        data.Add(new Operation("convert-to-dvs", 2, async t =>
        {
            if (t.Metadata.FormatVersion == 2) await t.CommitAsync([], [new MetadataUpdate.UpgradeFormatVersion(3)]);
            await SnapshotProducer.ConvertPositionDeletesToDeletionVectors(t).CommitAsync();
        }));
        return data;
    }

    private static string Canon(IEnumerable<object?[]> rows) =>
        string.Join(" ", rows.Select(r => string.Join(",", r.Take(3).Select(v => v?.ToString() ?? "null"))).Order(StringComparer.Ordinal));

    /// <summary>A fresh table with 12 rows over 3 partitions and one existing position delete.</summary>
    private static async Task<(InMemoryCatalog Catalog, FaultInjectingFileIO IO, string Dir)> Fixture(int formatVersion)
    {
        var dir = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-faults", Guid.NewGuid().ToString("N"));
        var io = new FaultInjectingFileIO(new LocalFileIO());
        var catalog = new InMemoryCatalog(dir, io);
        await catalog.CreateNamespaceAsync(Namespace.Of("f"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("f", "t"), Schema,
            PartitionSpec.BuilderFor(Schema).Identity("event_date").Build(), FormatVersion: formatVersion));
        await TableMutations.InsertAsync(t, Enumerable.Range(0, 12).Select(i => Row(i, $"v{i}", i % 3)).ToList());
        await TableMutations.DeleteByKeysAsync(t, ["id", "event_date"], [[2L, 19_702]], DeleteStrategy.PositionDeletes);
        return (catalog, io, dir);
    }

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task Every_write_fault_leaves_the_table_before_or_after(Operation op)
    {
        // Clean run: learn the post-state and how many writes the operation performs.
        string before, after;
        int writes;
        {
            var (catalog, io, dir) = await Fixture(op.FormatVersion);
            var t = await catalog.LoadTableAsync(TableIdentifier.Of("f", "t"));
            before = Canon(await TableScan.ReadAllAsync(t));
            io.Reset();
            await op.Run(t);
            writes = io.Writes;
            after = Canon(await TableScan.ReadAllAsync(await catalog.LoadTableAsync(t.Identifier)));
            catalog.Dispose();
            Directory.Delete(dir, true);
        }
        Assert.True(writes > 0);

        var outcomes = new List<string>();
        foreach (var kind in new[] { FaultKind.FailBeforeWrite, FaultKind.FailAfterWrite })
        {
            for (var k = 1; k <= writes; k++)
            {
                var (catalog, io, dir) = await Fixture(op.FormatVersion);
                try
                {
                    var t = await catalog.LoadTableAsync(TableIdentifier.Of("f", "t"));
                    io.Reset(k, kind);
                    Exception? error = null;
                    try { await op.Run(t); }
                    catch (Exception e) when (e is IOException or CommitStateUnknownException) { error = e; }

                    var reloaded = await catalog.LoadTableAsync(t.Identifier);
                    var state = Canon(await TableScan.ReadAllAsync(reloaded));
                    Assert.True(state == before || state == after,
                        $"{op} with {kind} at write {k}/{writes} left a third state:\n  before: {before}\n  after:  {after}\n  actual: {state}\n  error: {error?.Message}");
                    var report = await TableValidator.ValidateAsync(reloaded, new ValidationOptions { Deep = true, AllSnapshots = true });
                    Assert.True(report.IsValid, $"{op} with {kind} at write {k}/{writes}: {report}");
                    outcomes.Add(state == before ? "rolled-back" : "committed");
                }
                finally
                {
                    catalog.Dispose();
                    Directory.Delete(dir, true);
                }
            }
        }
        // Faults before the final pointer swap must roll back; at least one fault point must exist that does.
        Assert.Contains("rolled-back", outcomes);
    }

    [Fact]
    public async Task Ambiguous_catalog_swap_never_deletes_the_new_metadata()
    {
        // The catalog applied the swap but the caller saw an error: the table must be the committed state.
        var (catalog, io, dir) = await Fixture(3);
        try
        {
            var t = await catalog.LoadTableAsync(TableIdentifier.Of("f", "t"));
            io.Reset();
            io.PathFilter = p => p.EndsWith(".metadata.json", StringComparison.Ordinal);
            io.Reset(1, FaultKind.FailAfterWrite);
            await Assert.ThrowsAnyAsync<Exception>(() => TableMutations.InsertAsync(t, [Row(500, "x", 0)]));
            var reloaded = await catalog.LoadTableAsync(t.Identifier);
            await TableValidator.AssertValidAsync(reloaded, new ValidationOptions { Deep = true });
        }
        finally
        {
            catalog.Dispose();
            Directory.Delete(dir, true);
        }
    }
}
