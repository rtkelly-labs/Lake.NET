using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Key handling in upsert, group replacement and MERGE that the stryker-writes mutation run (see docs/roadmap.md,
/// "Mutation testing") showed no test pinned down: invalid upsert keys, null group keys, and null key parts in MERGE.
/// </summary>
[Spec("Row-level Deletes")]
public sealed class KeyedWriteEdgeTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-keyededge", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    /// <summary>id, an optional group or key part g, and a value.</summary>
    private static readonly Schema Rows3 = new(0,
        [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "g", PrimitiveType.Long), NestedField.Optional(3, "v", PrimitiveType.String)]);

    public KeyedWriteEdgeTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("k")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private Task<Table> Create() =>
        _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("k", $"t{Guid.NewGuid():N}"), Rows3, FormatVersion: 2));

    private static async Task<List<(long Id, long? G, string? V)>> ReadAsync(Table t) =>
        (await TableScan.ReadAllAsync(t)).Select(r => ((long)r[0]!, (long?)r[1], (string?)r[2])).OrderBy(r => r.Item1).ThenBy(r => r.Item3, StringComparer.Ordinal).ToList();

    [Theory]
    [InlineData(DeleteStrategy.CopyOnWrite)]
    [InlineData(DeleteStrategy.PositionDeletes)]
    public async Task Upsert_rejects_a_null_or_duplicate_key_and_changes_nothing(DeleteStrategy strategy)
    {
        var t = await Create();
        await TableMutations.InsertAsync(t, [[1L, 1L, "a"]]);
        var before = t.MetadataLocation;
        var nullKey = await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.UpsertAsync(t, [[1L, null, "b"]], ["id", "g"], strategy));
        Assert.Contains("Null upsert key", nullKey.Message, StringComparison.Ordinal);
        var duplicate = await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.UpsertAsync(t, [[1L, 1L, "b"], [1L, 1L, "c"]], ["id", "g"], strategy));
        Assert.Contains("Duplicate upsert key", duplicate.Message, StringComparison.Ordinal);
        Assert.Equal(before, t.MetadataLocation);
        Assert.Equal([(1L, (long?)1L, (string?)"a")], await ReadAsync(t));
    }

    [Theory]
    [InlineData(DeleteStrategy.CopyOnWrite)]
    [InlineData(DeleteStrategy.PositionDeletes)]
    [InlineData(DeleteStrategy.EqualityDeletes)]
    public async Task Group_replacement_replaces_the_null_group_alongside_others(DeleteStrategy strategy)
    {
        var t = await Create();
        // One file holds only the null group, so its column metrics say "all null": the pruning filter has to keep it.
        await TableMutations.InsertAsync(t, [[1L, null, "old-null"], [2L, null, "old-null"]]);
        await TableMutations.InsertAsync(t, [[3L, 7L, "old-7"]]);
        await TableMutations.InsertAsync(t, [[4L, 8L, "kept-8"]]);
        await TableMutations.ReplaceGroupsAsync(t, [[10L, null, "new-null"], [11L, 7L, "new-7"]], ["g"], strategy);
        Assert.Equal([(4L, (long?)8L, (string?)"kept-8"), (10L, null, "new-null"), (11L, 7L, "new-7")], await ReadAsync(t));
    }

    [Spec("Equality Delete Files")]
    [Fact]
    public async Task Merge_never_matches_a_key_with_a_null_part()
    {
        var t = await Create();
        await TableMutations.InsertAsync(t, [[1L, null, "target"], [2L, 5L, "target"]]);
        // Two source rows share the key (1, null): not a cardinality violation, because a key with a null never matches.
        var result = await TableMutations.MergeAsync(t, [[1L, null, "s1"], [1L, null, "s2"], [2L, 5L, "s3"]], ["id", "g"],
            m => m.WhenMatchedUpdate((target, source) => source).WhenNotMatchedInsert());
        Assert.Equal(1, result.RowsUpdated);
        Assert.Equal(2, result.RowsInserted);
        Assert.Equal([(1L, null, "s1"), (1L, null, "s2"), (1L, null, "target"), (2L, (long?)5L, (string?)"s3")], await ReadAsync(t));
    }
}
