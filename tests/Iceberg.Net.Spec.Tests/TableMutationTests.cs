using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Write modes (insert, upsert, group replacement, deletes) × delete strategies × format versions.</summary>
public sealed class TableMutationTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-tests", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;
    private int _tables;

    // id is the row key; event_date the partition (and the "group" for replacement).
    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Require(3, "event_date", PrimitiveType.Date),
        NestedField.Optional(4, "score", PrimitiveType.Double),
    ], identifierFieldIds: [1, 3]);

    public TableMutationTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("db")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    public static TheoryData<int, DeleteStrategy> Matrix()
    {
        var data = new TheoryData<int, DeleteStrategy>();
        foreach (var v in new[] { 2, 3 })
            foreach (var s in new[] { DeleteStrategy.CopyOnWrite, DeleteStrategy.PositionDeletes, DeleteStrategy.EqualityDeletes })
                data.Add(v, s);
        return data;
    }

    private static object?[] Row(long id, string? name, int day, double? score = null) => [id, name, 19_700 + day, score];

    /// <summary>ids 0..29 over 3 days (id % 3).</summary>
    private async Task<Table> Seeded(int formatVersion, bool partitioned = true)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", $"t{_tables++}"), Schema,
            partitioned ? PartitionSpec.BuilderFor(Schema).Identity("event_date").Build() : null, FormatVersion: formatVersion));
        await TableMutations.InsertAsync(t, Enumerable.Range(0, 30).Select(i => Row(i, $"v1-{i}", i % 3)).ToList());
        return t;
    }

    private async Task<Dictionary<long, object?[]>> Rows(Table t) =>
        (await TableScan.ReadAllAsync(await _catalog.LoadTableAsync(t.Identifier))).ToDictionary(r => (long)r[0]!);

    private static async Task<List<ManifestEntry>> LiveDeleteFiles(Table t)
    {
        var idx = await TableScan.DeletesAsync(t);
        return [.. idx.All.Concat(idx.EqualityDeletes).Select(e => new ManifestEntry(ManifestEntryStatus.Existing, null, null, null, e.File))];
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Insert_only_appends(int formatVersion)
    {
        var t = await Seeded(formatVersion);
        var r = await TableMutations.InsertAsync(t, [Row(100, "new", 0)]);
        Assert.Equal(Operations.Append, r.Snapshot.Operation);
        Assert.Equal(31, (await Rows(t)).Count);
        Assert.Empty(await LiveDeleteFiles(t));
    }

    [Theory]
    [MemberData(nameof(Matrix))]
    public async Task Upsert_replaces_matching_keys_and_inserts_new_ones(int formatVersion, DeleteStrategy strategy)
    {
        var t = await Seeded(formatVersion);
        // ids 27..29 exist (days 0,1,2); 30..32 are new. Same day as the existing rows, so the key (id, event_date) matches.
        var incoming = Enumerable.Range(27, 6).Select(i => Row(i, $"v2-{i}", i % 3)).ToList();
        var r = await TableMutations.UpsertAsync(t, incoming, strategy: strategy);

        Assert.Equal(Operations.Overwrite, r.Snapshot.Operation);
        var rows = await Rows(t);
        Assert.Equal(33, rows.Count);
        Assert.All(Enumerable.Range(27, 6), i => Assert.Equal($"v2-{i}", rows[i][1]));
        Assert.Equal("v1-26", rows[26][1]);
        Assert.Equal(strategy == DeleteStrategy.EqualityDeletes ? null : 3L, r.RowsDeleted);

        // Idempotent: the same upsert again changes values to themselves, never duplicates.
        await TableMutations.UpsertAsync(t, incoming, strategy: strategy);
        Assert.Equal(33, (await Rows(t)).Count);

        var deleteFiles = await LiveDeleteFiles(t);
        switch (strategy)
        {
            case DeleteStrategy.CopyOnWrite: Assert.Empty(deleteFiles); break;
            case DeleteStrategy.PositionDeletes:
                Assert.All(deleteFiles, e => Assert.Equal(formatVersion == 3 ? "PUFFIN" : "PARQUET", e.DataFile.FileFormat));
                if (formatVersion == 3) Assert.Equal(deleteFiles.Count, deleteFiles.Select(e => e.DataFile.ReferencedDataFile).Distinct().Count());
                break;
            case DeleteStrategy.EqualityDeletes:
                Assert.All(deleteFiles, e => Assert.Equal(FileContent.EqualityDeletes, e.DataFile.Content));
                Assert.All(deleteFiles, e => Assert.Equal([1, 3], e.DataFile.EqualityIds));
                break;
        }
    }

    [Theory]
    [MemberData(nameof(Matrix))]
    public async Task Group_replacement_swaps_whole_groups_only(int formatVersion, DeleteStrategy strategy)
    {
        var t = await Seeded(formatVersion);
        // Replace day 1 (ids 1,4,...,28: 10 rows) with two rows; days 0 and 2 untouched.
        var r = await TableMutations.ReplaceGroupsAsync(t, [Row(500, "a", 1), Row(501, "b", 1)], ["event_date"], strategy);
        var rows = await Rows(t);
        Assert.Equal(22, rows.Count);
        Assert.Equal([500L, 501L], rows.Values.Where(x => (int)x[2]! == 19_701).Select(x => (long)x[0]!).Order());
        Assert.Equal(10, rows.Values.Count(x => (int)x[2]! == 19_700));
        if (strategy == DeleteStrategy.CopyOnWrite) Assert.Equal(1, r.DataFilesRewritten);
    }

    [Theory]
    [MemberData(nameof(Matrix))]
    public async Task Delete_by_keys(int formatVersion, DeleteStrategy strategy)
    {
        var t = await Seeded(formatVersion);
        await TableMutations.DeleteByKeysAsync(t, ["id", "event_date"], [[3L, 19_700], [4L, 19_701], [999L, 19_700]], strategy);
        var rows = await Rows(t);
        Assert.Equal(28, rows.Count);
        Assert.DoesNotContain(3L, rows.Keys);
        Assert.DoesNotContain(4L, rows.Keys);
    }

    [Theory]
    [InlineData(2, DeleteStrategy.CopyOnWrite)]
    [InlineData(2, DeleteStrategy.PositionDeletes)]
    [InlineData(3, DeleteStrategy.CopyOnWrite)]
    [InlineData(3, DeleteStrategy.PositionDeletes)]
    public async Task Delete_where_predicate(int formatVersion, DeleteStrategy strategy)
    {
        var t = await Seeded(formatVersion);
        var r = await TableMutations.DeleteWhereAsync(t, row => (long)row[0]! % 10 == 0, strategy);
        Assert.Equal(3, r.RowsDeleted);
        // Copy-on-write replaces files (an overwrite, as Spark's CoW DELETE commits); merge-on-read only adds deletes.
        Assert.Equal(strategy == DeleteStrategy.CopyOnWrite ? Operations.Overwrite : Operations.Delete, r.Snapshot.Operation);
        Assert.Equal(27, (await Rows(t)).Count);
    }

    [Fact]
    public async Task Equality_deletes_cannot_express_predicates()
    {
        var t = await Seeded(2);
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.DeleteWhereAsync(t, _ => true, DeleteStrategy.EqualityDeletes));
    }

    [Fact]
    public async Task Equality_deletes_require_partition_sources_in_the_key()
    {
        var t = await Seeded(2);
        await Assert.ThrowsAsync<ValidationException>(() => TableMutations.UpsertAsync(t, [Row(1, "x", 0)], ["id"], DeleteStrategy.EqualityDeletes));
        var unpartitioned = await Seeded(2, partitioned: false);
        await TableMutations.UpsertAsync(unpartitioned, [Row(1, "x", 2)], ["id"], DeleteStrategy.EqualityDeletes); // global delete
        var rows = await Rows(unpartitioned);
        Assert.Equal("x", rows[1][1]);
        Assert.Equal(30, rows.Count);
    }

    [Fact]
    public async Task Upsert_input_must_have_unique_non_null_keys()
    {
        var t = await Seeded(2);
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.UpsertAsync(t, [Row(1, "a", 0), Row(1, "b", 0)]));
        var noIdentifier = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "noid"), new Schema(Schema.Columns.ToArray())));
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.UpsertAsync(noIdentifier, [Row(1, "a", 0)]));
    }

    [Theory]
    [InlineData("copy-on-write", DeleteStrategy.CopyOnWrite)]
    [InlineData("merge-on-read", DeleteStrategy.PositionDeletes)]
    public async Task Default_strategy_follows_table_properties(string mode, DeleteStrategy expected)
    {
        var t = await Seeded(3);
        await t.SetPropertiesAsync(new Dictionary<string, string> { [TableMutations.MergeModeProperty] = mode });
        var r = await TableMutations.UpsertAsync(t, [Row(1, "x", 1)]);
        Assert.Equal(expected, r.Strategy);
    }

    [Theory]
    [MemberData(nameof(Matrix))]
    public async Task Concurrent_upserts_of_the_same_new_key_leave_one_row(int formatVersion, DeleteStrategy strategy)
    {
        var a = await Seeded(formatVersion);
        var b = await _catalog.LoadTableAsync(a.Identifier); // stale
        await TableMutations.UpsertAsync(a, [Row(777, "from-a", 0)], strategy: strategy);
        var r = await TableMutations.UpsertAsync(b, [Row(777, "from-b", 0)], strategy: strategy);

        Assert.Equal(2, r.Attempts); // lost the race, re-planned against a's commit
        var rows = await Rows(a);
        Assert.Equal(31, rows.Count);
        Assert.Equal("from-b", rows[777][1]);
    }

    [Fact]
    public async Task Copy_on_write_removes_deletion_vectors_of_rewritten_files()
    {
        var t = await Seeded(3);
        await TableMutations.DeleteByKeysAsync(t, ["id", "event_date"], [[0L, 19_700]], DeleteStrategy.PositionDeletes);
        Assert.Single(await LiveDeleteFiles(t));
        await TableMutations.DeleteByKeysAsync(t, ["id", "event_date"], [[3L, 19_700]], DeleteStrategy.CopyOnWrite);
        Assert.Empty(await LiveDeleteFiles(t)); // the rewritten file's DV went with it
        Assert.Equal(28, (await Rows(t)).Count);
    }
}
