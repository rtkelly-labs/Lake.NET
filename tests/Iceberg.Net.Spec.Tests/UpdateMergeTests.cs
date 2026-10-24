using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>UPDATE and MERGE (TableMutations.UpdateAsync / MergeAsync) × delete strategies × format versions.</summary>
[Spec("Row-level Deletes")]
public sealed class UpdateMergeTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-tests", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;
    private int _tables;

    // id is the key, region the partition; name is nullable so it can serve as a key with nulls.
    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Require(3, "region", PrimitiveType.String),
        NestedField.Optional(4, "score", PrimitiveType.Int),
    ], identifierFieldIds: [1]);

    public UpdateMergeTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("db")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    public static TheoryData<int, DeleteStrategy> Removal()
    {
        var data = new TheoryData<int, DeleteStrategy>();
        foreach (var v in new[] { 2, 3 })
            foreach (var s in new[] { DeleteStrategy.CopyOnWrite, DeleteStrategy.PositionDeletes })
                data.Add(v, s);
        return data;
    }

    public static TheoryData<int, DeleteStrategy> AllStrategies()
    {
        var data = Removal();
        data.Add(2, DeleteStrategy.EqualityDeletes);
        data.Add(3, DeleteStrategy.EqualityDeletes);
        return data;
    }

    private static object?[] Row(long id, string? name, string region, int? score = null) => [id, name, region, score];

    /// <summary>ids 1..10; odd ids in "eu", even in "us"; score = id; written as two inserts so each partition has two files.</summary>
    private async Task<Table> Seeded(int formatVersion, bool partitioned = true, IReadOnlyDictionary<string, string>? properties = null)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", $"t{_tables++}"), Schema,
            partitioned ? PartitionSpec.BuilderFor(Schema).Identity("region").Build() : null, FormatVersion: formatVersion, Properties: properties));
        await TableMutations.InsertAsync(t, Enumerable.Range(1, 5).Select(i => Row(i, $"n{i}", i % 2 == 1 ? "eu" : "us", i)).ToList());
        await TableMutations.InsertAsync(t, Enumerable.Range(6, 5).Select(i => Row(i, $"n{i}", i % 2 == 1 ? "eu" : "us", i)).ToList());
        return t;
    }

    /// <summary>Rows by id, after asserting the table passes deep validation over every snapshot.</summary>
    private async Task<Dictionary<long, object?[]>> Rows(Table t)
    {
        var fresh = await _catalog.LoadTableAsync(t.Identifier);
        var report = await TableValidator.ValidateAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
        Assert.True(report.IsValid, report.ToString());
        var rows = await TableScan.ReadAllAsync(fresh);
        Assert.Equal(rows.Count, rows.Select(r => (long)r[0]!).Distinct().Count());
        return rows.ToDictionary(r => (long)r[0]!);
    }

    private static string Describe(object?[] r) => $"{r[0]}|{r[1] ?? "null"}|{r[2]}|{r[3]?.ToString() ?? "null"}";

    private async Task<List<DataFile>> DeleteFiles(Table t)
    {
        var idx = await TableScan.DeletesAsync(await _catalog.LoadTableAsync(t.Identifier));
        return [.. idx.All.Concat(idx.EqualityDeletes).Select(e => e.File)];
    }

    private async Task AssertDeleteFiles(Table t, int formatVersion, DeleteStrategy strategy)
    {
        var files = await DeleteFiles(t);
        switch (strategy)
        {
            case DeleteStrategy.CopyOnWrite: Assert.Empty(files); break;
            case DeleteStrategy.PositionDeletes:
                Assert.NotEmpty(files);
                Assert.All(files, f => Assert.Equal((FileContent.PositionDeletes, formatVersion >= 3 ? "PUFFIN" : "PARQUET"), (f.Content, f.FileFormat)));
                if (formatVersion >= 3) Assert.Equal(files.Count, files.Select(f => f.ReferencedDataFile).Distinct().Count()); // one DV per data file
                break;
            case DeleteStrategy.EqualityDeletes:
                Assert.NotEmpty(files);
                Assert.All(files, f => Assert.Equal(FileContent.EqualityDeletes, f.Content));
                break;
        }
    }

    // ------------------------------------------------------------------ UPDATE

    [Theory]
    [MemberData(nameof(Removal))]
    public async Task Update_sets_columns_of_matching_rows_only(int formatVersion, DeleteStrategy strategy)
    {
        var t = await Seeded(formatVersion);
        var r = await TableMutations.UpdateAsync(t, Expr.In("id", 2L, 5L, 9L), u => u.Set("name", "changed"), strategy);

        Assert.Equal(3, r.RowsUpdated);
        Assert.Equal((0L, 0L), (r.RowsDeleted, r.RowsInserted));
        Assert.Equal(strategy, r.Strategy);
        Assert.Equal(1, r.Attempts);
        Assert.Equal(Operations.Overwrite, r.Snapshot!.Operation);
        var rows = await Rows(t);
        Assert.Equal(10, rows.Count);
        foreach (var (id, row) in rows)
            Assert.Equal(id is 2 or 5 or 9 ? Row(id, "changed", id % 2 == 1 ? "eu" : "us", (int)id) : Row(id, $"n{id}", id % 2 == 1 ? "eu" : "us", (int)id), row);
        await AssertDeleteFiles(t, formatVersion, strategy);
    }

    [Theory]
    [MemberData(nameof(Removal))]
    public async Task Update_applies_every_assignment_to_the_row_as_it_was(int formatVersion, DeleteStrategy strategy)
    {
        var t = await Seeded(formatVersion);
        await TableMutations.UpdateAsync(t, Expr.LessThanOrEqual("id", 3L), u => u
            .Set("score", row => (int)row[3]! * 10)
            .Set("name", row => $"{row[1]}:{row[3]}"), strategy); // sees the old score, not score * 10
        var rows = await Rows(t);
        Assert.Equal(Row(1, "n1:1", "eu", 10), rows[1]);
        Assert.Equal(Row(3, "n3:3", "eu", 30), rows[3]);
        Assert.Equal(Row(4, "n4", "us", 4), rows[4]);
    }

    [Theory]
    [MemberData(nameof(Removal))]
    public async Task Updating_the_partition_column_moves_the_row(int formatVersion, DeleteStrategy strategy)
    {
        var t = await Seeded(formatVersion);
        await TableMutations.UpdateAsync(t, Expr.Equal("id", 3L), u => u.Set("region", "apac"), strategy);
        t = await _catalog.LoadTableAsync(t.Identifier);
        var rows = await Rows(t);
        Assert.Equal("apac", rows[3][2]);
        Assert.Equal([3L], (await TableScan.ReadAsync(t, Expr.Equal("region", "apac"))).Select(r => (long)r[0]!));
        Assert.DoesNotContain(3L, (await TableScan.ReadAsync(t, Expr.Equal("region", "eu"))).Select(r => (long)r[0]!));
        Assert.Contains(await TableScan.PlanFilesAsync(t), task => (string?)task.File.Partition[0] == "apac");
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Copy_on_write_update_rewrites_only_the_files_the_filter_reaches(int formatVersion)
    {
        var t = await Seeded(formatVersion);
        var usBefore = (await TableScan.PlanFilesAsync(t, Expr.Equal("region", "us"))).Select(x => x.File.FilePath).ToHashSet();
        await TableMutations.UpdateAsync(t, Expr.And(Expr.Equal("region", "eu"), Expr.Equal("id", 1L)), u => u.Set("score", 0), DeleteStrategy.CopyOnWrite);
        t = await _catalog.LoadTableAsync(t.Identifier);
        var all = (await TableScan.PlanFilesAsync(t)).Select(x => x.File.FilePath).ToHashSet();
        Assert.Subset(all, usBefore); // untouched partition: same files
        Assert.Equal(0, (await Rows(t))[1][3]);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Update_with_equality_deletes_is_rejected(int formatVersion)
    {
        var t = await Seeded(formatVersion);
        var e = await Assert.ThrowsAsync<ValidationException>(() => TableMutations.UpdateAsync(t, Expr.Equal("id", 1L), u => u.Set("name", "x"), DeleteStrategy.EqualityDeletes));
        Assert.Contains("equality deletes", e.Message);
        // Default resolved to merge-on-read never yields equality deletes either; but an explicit request is refused.
        Assert.Equal(2, (await _catalog.LoadTableAsync(t.Identifier)).Metadata.Snapshots.Count);
    }

    [Theory]
    [InlineData(null, null, DeleteStrategy.CopyOnWrite)]
    [InlineData("merge-on-read", null, DeleteStrategy.PositionDeletes)]
    [InlineData("copy-on-write", "merge-on-read", DeleteStrategy.CopyOnWrite)]
    [InlineData(null, "merge-on-read", DeleteStrategy.CopyOnWrite)] // as Java: write.update.mode alone decides UPDATE
    public async Task Default_update_strategy_follows_write_update_mode(string? updateMode, string? mergeMode, DeleteStrategy expected)
    {
        var props = new Dictionary<string, string>();
        if (updateMode is not null) props[TableMutations.UpdateModeProperty] = updateMode;
        if (mergeMode is not null) props[TableMutations.MergeModeProperty] = mergeMode;
        var t = await Seeded(3, properties: props);
        var r = await TableMutations.UpdateAsync(t, Expr.Equal("id", 1L), u => u.Set("name", "x"));
        Assert.Equal(expected, r.Strategy);
        await AssertDeleteFiles(t, 3, expected);
    }

    [Theory]
    [MemberData(nameof(Removal))]
    public async Task Update_matching_nothing_commits_nothing(int formatVersion, DeleteStrategy strategy)
    {
        var t = await Seeded(formatVersion);
        var before = t.Metadata.CurrentSnapshotId;
        var r = await TableMutations.UpdateAsync(t, Expr.Equal("id", 999L), u => u.Set("name", "x"), strategy);
        Assert.Null(r.Snapshot);
        Assert.Equal(0, r.RowsUpdated);
        Assert.Equal(before, (await _catalog.LoadTableAsync(t.Identifier)).Metadata.CurrentSnapshotId);

        var empty = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", $"empty{_tables++}"), Schema, FormatVersion: formatVersion));
        Assert.Null((await TableMutations.UpdateAsync(empty, Expression.True, u => u.Set("name", "x"), strategy)).Snapshot);
    }

    [Fact]
    public async Task Update_skips_rows_already_deleted()
    {
        var t = await Seeded(3);
        await TableMutations.DeleteWhereAsync(t, Expr.Equal("id", 2L), DeleteStrategy.PositionDeletes);
        var r = await TableMutations.UpdateAsync(t, Expr.Equal("region", "us"), u => u.Set("name", "us!"), DeleteStrategy.PositionDeletes);
        Assert.Equal(4, r.RowsUpdated); // 4, 6, 8, 10: id 2 is already gone and is not resurrected
        var rows = await Rows(t);
        Assert.False(rows.ContainsKey(2));
        Assert.Equal(9, rows.Count);
    }

    [Fact]
    public async Task Update_assignments_are_checked_against_the_schema()
    {
        var t = await Seeded(2);
        var snapshots = t.Metadata.Snapshots.Count;
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.UpdateAsync(t, Expr.Equal("id", 1L), u => u.Set("nope", 1)));
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.UpdateAsync(t, Expr.Equal("id", 1L), u => u.Set("score", "not a number")));
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.UpdateAsync(t, Expr.Equal("id", 1L), u => u.Set("score", long.MaxValue)));
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.UpdateAsync(t, Expr.Equal("id", 1L), u => u.Set("region", (object?)null)));
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.UpdateAsync(t, Expr.Equal("id", 1L), u => u.Set("name", "a").Set("name", "b")));
        Assert.Equal(snapshots, (await _catalog.LoadTableAsync(t.Identifier)).Metadata.Snapshots.Count);

        // Values convert like filter literals: a long that fits an int column, and null into an optional column.
        await TableMutations.UpdateAsync(t, Expr.Equal("id", 1L), u => u.Set("score", 42L).Set("name", (object?)null));
        Assert.Equal(Row(1, null, "eu", 42), (await Rows(t))[1]);
    }

    [Spec("Row Lineage")]
    [Spec("Row lineage assignment")]
    [Theory]
    [InlineData(DeleteStrategy.CopyOnWrite)]
    [InlineData(DeleteStrategy.PositionDeletes)]
    public async Task Updated_rows_keep_their_row_id_and_take_the_new_sequence_number(DeleteStrategy strategy)
    {
        var t = await Seeded(3);
        var before = (await TableScan.ReadWithLineageAsync(t)).ToDictionary(r => (long)r.Row[0]!);
        var r = await TableMutations.UpdateAsync(t, Expr.In("id", 1L, 4L), u => u.Set("region", "apac"), strategy);
        t = await _catalog.LoadTableAsync(t.Identifier);
        var after = (await TableScan.ReadWithLineageAsync(t)).ToDictionary(r => (long)r.Row[0]!);

        Assert.Equal(before.Keys.Order(), after.Keys.Order());
        foreach (var (id, row) in after)
        {
            Assert.Equal(before[id].RowId, row.RowId); // identity survives the update (and the partition move)
            Assert.Equal(id is 1 or 4 ? r.Snapshot!.SequenceNumber : before[id].LastUpdatedSequenceNumber, row.LastUpdatedSequenceNumber);
        }
        Assert.Equal(after.Count, after.Values.Select(x => x.RowId).Distinct().Count());
        await Rows(t);
    }

    // ------------------------------------------------------------------ MERGE

    [Theory]
    [MemberData(nameof(AllStrategies))]
    public async Task Merge_updates_matched_rows_and_inserts_the_rest(int formatVersion, DeleteStrategy strategy)
    {
        var t = await Seeded(formatVersion);
        // Equality deletes need the partition column in the key, so match on (id, region).
        var r = await TableMutations.MergeAsync(t, [Row(2, "two", "us", 20), Row(3, "three", "eu", 30), Row(11, "eleven", "eu", 11)], ["id", "region"],
            m => m.WhenMatchedUpdate((target, source) => source).WhenNotMatchedInsert(), strategy);

        Assert.Equal((2L, 0L, 1L), (r.RowsUpdated, r.RowsDeleted, r.RowsInserted));
        Assert.Equal(strategy, r.Strategy);
        var rows = await Rows(t);
        Assert.Equal(11, rows.Count);
        Assert.Equal(Row(2, "two", "us", 20), rows[2]);
        Assert.Equal(Row(3, "three", "eu", 30), rows[3]);
        Assert.Equal(Row(11, "eleven", "eu", 11), rows[11]);
        Assert.Equal(Row(4, "n4", "us", 4), rows[4]);
        await AssertDeleteFiles(t, formatVersion, strategy);
    }

    [Theory]
    [MemberData(nameof(Removal))]
    public async Task Merge_applies_every_clause_kind(int formatVersion, DeleteStrategy strategy)
    {
        var t = await Seeded(formatVersion);
        object?[][] source =
        [
            Row(1, "s1", "eu", 100), // matched, score > 50: update name from source
            Row(2, "s2", "us", 0),   // matched, score 0: delete
            Row(3, "s3", "eu", 5),   // matched, neither condition: second update clause (unconditional) sets score
            Row(20, "s20", "us", 1), // not matched, score > 0: insert
            Row(21, "s21", "eu", -1), // not matched, fails the insert condition: dropped
        ];
        var r = await TableMutations.MergeAsync(t, source, ["id"], m => m
            .WhenMatchedUpdate(u => u.Set("name", "first"), (tg, s) => (int)s[3]! > 50)
            .WhenMatchedDelete((tg, s) => (int)s[3]! == 0)
            .WhenMatchedUpdate(u => u.Set("score", row => (int)row[3]! + 1000))
            .WhenNotMatchedInsert(s => (int)s[3]! > 0)
            .WhenNotMatchedBySourceDelete(Expr.GreaterThan("id", 8L)), strategy); // 9, 10 are not in the source

        Assert.Equal((2L, 3L, 1L), (r.RowsUpdated, r.RowsDeleted, r.RowsInserted));
        var rows = await Rows(t);
        Assert.Equal(new[] { 1L, 3, 4, 5, 6, 7, 8, 20 }, rows.Keys.Order());
        Assert.Equal(Row(1, "first", "eu", 1), rows[1]);
        Assert.Equal(Row(3, "n3", "eu", 1003), rows[3]);
        Assert.Equal(Row(20, "s20", "us", 1), rows[20]);
        await AssertDeleteFiles(t, formatVersion, strategy);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task The_first_matched_clause_whose_condition_holds_wins(bool deleteFirst)
    {
        var t = await Seeded(2);
        await TableMutations.MergeAsync(t, [Row(1, "x", "eu")], ["id"], m =>
        {
            if (deleteFirst) m.WhenMatchedDelete().WhenMatchedUpdate(u => u.Set("name", "updated"));
            else m.WhenMatchedUpdate(u => u.Set("name", "updated")).WhenMatchedDelete();
        }, DeleteStrategy.PositionDeletes);
        var rows = await Rows(t);
        if (deleteFirst) Assert.False(rows.ContainsKey(1));
        else Assert.Equal("updated", rows[1][1]);
    }

    [Fact]
    public async Task The_first_not_matched_clause_whose_condition_holds_wins()
    {
        var t = await Seeded(2);
        var r = await TableMutations.MergeAsync(t, [Row(30, "a", "eu", 1), Row(31, "b", "eu", 2)], ["id"], m => m
            .WhenNotMatchedInsert(s => (int)s[3]! == 1, s => Row((long)s[0]!, "first", "eu", 1))
            .WhenNotMatchedInsert(null, s => Row((long)s[0]!, "second", "us", 2)));
        Assert.Equal(2, r.RowsInserted);
        var rows = await Rows(t);
        Assert.Equal(Row(30, "first", "eu", 1), rows[30]);
        Assert.Equal(Row(31, "second", "us", 2), rows[31]);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task A_duplicated_source_key_is_a_cardinality_violation(int formatVersion)
    {
        var t = await Seeded(formatVersion);
        var files = Directory.GetFiles(_warehouse, "*", SearchOption.AllDirectories).Length;
        var e = await Assert.ThrowsAsync<ValidationException>(() => TableMutations.MergeAsync(t, [Row(1, "a", "eu"), Row(1, "b", "eu")], ["id"],
            m => m.WhenMatchedUpdate((tg, s) => s).WhenNotMatchedInsert()));
        Assert.Contains("cardinality", e.Message);
        Assert.Equal(files, Directory.GetFiles(_warehouse, "*", SearchOption.AllDirectories).Length); // nothing written
        // Also when the duplicated key is not in the target: the source alone decides.
        await Assert.ThrowsAsync<ValidationException>(() => TableMutations.MergeAsync(t, [Row(50, "a", "eu"), Row(50, "b", "eu")], ["id"], m => m.WhenNotMatchedInsert()));
        Assert.Equal(10, (await Rows(t)).Count);
    }

    [Fact]
    public async Task One_source_row_may_match_many_target_rows()
    {
        var t = await Seeded(2);
        var r = await TableMutations.MergeAsync(t, [Row(0, "eu-wide", "eu")], ["region"], m => m.WhenMatchedUpdate(u => u.Set("name", "eu-wide")));
        Assert.Equal(5, r.RowsUpdated);
        Assert.All((await Rows(t)).Values, row => Assert.Equal((string)row[2]! == "eu" ? "eu-wide" : $"n{row[0]}", row[1]));
    }

    [Theory]
    [MemberData(nameof(Removal))]
    public async Task Null_keys_never_match(int formatVersion, DeleteStrategy strategy)
    {
        var t = await Seeded(formatVersion);
        await TableMutations.UpdateAsync(t, Expr.Equal("id", 1L), u => u.Set("name", (object?)null), strategy);
        // Key "name": the target row with a null name (id 1) is not matched by the source row with a null name.
        var r = await TableMutations.MergeAsync(t, [Row(100, null, "eu"), Row(102, "n2", "us")], ["name"], m => m
            .WhenMatchedDelete()
            .WhenNotMatchedInsert(), strategy);
        Assert.Equal((0L, 1L, 1L), (r.RowsUpdated, r.RowsDeleted, r.RowsInserted));
        var rows = await Rows(t);
        Assert.True(rows.ContainsKey(1));   // null target key: not matched, so not deleted
        Assert.False(rows.ContainsKey(2));  // matched "n2"
        Assert.True(rows.ContainsKey(100)); // null source key: not matched, so inserted
        Assert.False(rows.ContainsKey(102));

        // Two source rows with a null key are not a cardinality violation: neither matches anything.
        var again = await TableMutations.MergeAsync(t, [Row(200, null, "eu"), Row(201, null, "us")], ["name"], m => m.WhenNotMatchedInsert(), strategy);
        Assert.Equal(2, again.RowsInserted);

        // Not matched by source includes target rows with a null key.
        var cleanup = await TableMutations.MergeAsync(t, [], ["name"], m => m.WhenNotMatchedBySourceDelete(Expr.IsNull("name")), strategy);
        Assert.Equal(4, cleanup.RowsDeleted); // ids 1, 100, 200, 201
        Assert.DoesNotContain((await Rows(t)).Values, row => row[1] is null);
    }

    [Theory]
    [MemberData(nameof(Removal))]
    public async Task Not_matched_by_source_without_a_filter_makes_the_target_equal_the_source(int formatVersion, DeleteStrategy strategy)
    {
        var t = await Seeded(formatVersion);
        var r = await TableMutations.MergeAsync(t, [Row(2, "two", "us", 2), Row(12, "twelve", "us", 12)], ["id"], m => m
            .WhenMatchedUpdate((tg, s) => s)
            .WhenNotMatchedInsert()
            .WhenNotMatchedBySourceDelete(), strategy);
        Assert.Equal((1L, 9L, 1L), (r.RowsUpdated, r.RowsDeleted, r.RowsInserted));
        Assert.Equal(["2|two|us|2", "12|twelve|us|12"], (await Rows(t)).Values.OrderBy(x => (long)x[0]!).Select(Describe));
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Merge_that_changes_nothing_commits_nothing(int formatVersion)
    {
        var t = await Seeded(formatVersion);
        var before = t.Metadata.CurrentSnapshotId;
        var r = await TableMutations.MergeAsync(t, [Row(1, "x", "eu"), Row(99, "y", "eu")], ["id"], m => m
            .WhenMatchedUpdate(u => u.Set("name", "never"), (tg, s) => false)
            .WhenNotMatchedInsert(s => false)
            .WhenNotMatchedBySourceDelete(Expr.Equal("id", 500L)));
        Assert.Null(r.Snapshot);
        Assert.Equal((0L, 0L, 0L), (r.RowsUpdated, r.RowsDeleted, r.RowsInserted));
        Assert.Equal(before, (await _catalog.LoadTableAsync(t.Identifier)).Metadata.CurrentSnapshotId);
        Assert.Null((await TableMutations.MergeAsync(t, [], ["id"], m => m.WhenMatchedDelete().WhenNotMatchedInsert())).Snapshot);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Merge_into_an_empty_table_inserts(int formatVersion)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", $"empty{_tables++}"), Schema, FormatVersion: formatVersion));
        var r = await TableMutations.MergeAsync(t, [Row(1, "a", "eu"), Row(2, "b", "us")], ["id"], m => m.WhenMatchedDelete().WhenNotMatchedInsert());
        Assert.Equal(2, r.RowsInserted);
        Assert.Equal(Operations.Append, r.Snapshot!.Operation);
        Assert.Equal(2, (await Rows(t)).Count);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Merge_with_equality_deletes_needs_unconditional_matched_clauses(int formatVersion)
    {
        var t = await Seeded(formatVersion);
        object?[][] source = [Row(1, "a", "eu")];
        await Assert.ThrowsAsync<ValidationException>(() => TableMutations.MergeAsync(t, source, ["id", "region"],
            m => m.WhenMatchedUpdate(u => u.Set("name", "x"), (tg, s) => true), DeleteStrategy.EqualityDeletes));
        await Assert.ThrowsAsync<ValidationException>(() => TableMutations.MergeAsync(t, source, ["id", "region"],
            m => m.WhenMatchedDelete().WhenNotMatchedBySourceDelete(), DeleteStrategy.EqualityDeletes));
        // The partition column must be part of the key, as for upserts.
        await Assert.ThrowsAsync<ValidationException>(() => TableMutations.MergeAsync(t, source, ["id"],
            m => m.WhenMatchedDelete(), DeleteStrategy.EqualityDeletes));

        var r = await TableMutations.MergeAsync(t, [Row(1, "a", "eu"), Row(4, "d", "us"), Row(40, "new", "us")], ["id", "region"],
            m => m.WhenMatchedDelete().WhenNotMatchedInsert(), DeleteStrategy.EqualityDeletes);
        Assert.Equal((0L, 2L, 1L), (r.RowsUpdated, r.RowsDeleted, r.RowsInserted));
        var rows = await Rows(t);
        Assert.False(rows.ContainsKey(1));
        Assert.False(rows.ContainsKey(4));
        Assert.Equal(Row(40, "new", "us"), rows[40]);
        Assert.All(await DeleteFiles(t), f => Assert.Equal(FileContent.EqualityDeletes, f.Content));
    }

    [Fact]
    public async Task Merge_default_strategy_follows_write_merge_mode()
    {
        var t = await Seeded(3, properties: new Dictionary<string, string> { [TableMutations.MergeModeProperty] = "merge-on-read" });
        var r = await TableMutations.MergeAsync(t, [Row(1, "a", "eu")], ["id"], m => m.WhenMatchedUpdate((tg, s) => s));
        Assert.Equal(DeleteStrategy.PositionDeletes, r.Strategy);
        await AssertDeleteFiles(t, 3, DeleteStrategy.PositionDeletes);
    }

    [Fact]
    public async Task Merge_rows_are_checked_against_the_schema()
    {
        var t = await Seeded(2);
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.MergeAsync(t, [[1L, "short"]], ["id"], m => m.WhenNotMatchedInsert()));
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.MergeAsync(t, [Row(1, "a", "eu")], ["nope"], m => m.WhenNotMatchedInsert()));
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.MergeAsync(t, [Row(1, "a", "eu")], ["id"],
            m => m.WhenMatchedUpdate((tg, s) => Row(1, "a", "eu", 1)[..2])));
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.MergeAsync(t, [Row(77, "a", "eu")], ["id"],
            m => m.WhenNotMatchedInsert(row: s => [77L, "a", null, null]))); // region is required
        Assert.Equal(10, (await Rows(t)).Count);
        // An int key in the source matches a long key column.
        var r = await TableMutations.MergeAsync(t, [[1, "int-key", "eu", 1]], ["id"], m => m.WhenMatchedUpdate((tg, s) => [1L, s[1], s[2], s[3]]));
        Assert.Equal(1, r.RowsUpdated);
    }

    [Spec("Row Lineage")]
    [Spec("Row lineage assignment")]
    [Theory]
    [InlineData(DeleteStrategy.CopyOnWrite)]
    [InlineData(DeleteStrategy.PositionDeletes)]
    [InlineData(DeleteStrategy.EqualityDeletes)]
    public async Task Merged_rows_keep_their_row_id(DeleteStrategy strategy)
    {
        var t = await Seeded(3);
        var before = (await TableScan.ReadWithLineageAsync(t)).ToDictionary(r => (long)r.Row[0]!);
        var r = await TableMutations.MergeAsync(t, [Row(2, "two", "us"), Row(3, "three", "eu"), Row(50, "new", "eu")], ["id", "region"],
            m => m.WhenMatchedUpdate(u => u.Set("name", (tg) => $"{tg[1]}!")).WhenNotMatchedInsert(), strategy);
        t = await _catalog.LoadTableAsync(t.Identifier);
        var after = (await TableScan.ReadWithLineageAsync(t)).ToDictionary(x => (long)x.Row[0]!);
        var seq = r.Snapshot!.SequenceNumber;
        foreach (var id in new long[] { 2, 3 })
        {
            Assert.Equal($"n{id}!", after[id].Row[1]);
            Assert.Equal(before[id].RowId, after[id].RowId);
            Assert.Equal(seq, after[id].LastUpdatedSequenceNumber);
        }
        Assert.Equal(before[5].RowId, after[5].RowId);
        Assert.DoesNotContain(after[50].RowId, before.Values.Select(x => x.RowId)); // a new row gets a new identity
        Assert.Equal(seq, after[50].LastUpdatedSequenceNumber);
        Assert.Equal(after.Count, after.Values.Select(x => x.RowId).Distinct().Count());
        await Rows(t);
    }

    // ------------------------------------------------------------------ nested struct fields

    // address: struct<city optional, geo: struct<lat required, lon optional> optional, zip required>; tags: list<struct<k>>.
    private static readonly Schema NestedSchema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "address", new StructType([
            NestedField.Optional(3, "city", PrimitiveType.String),
            NestedField.Optional(4, "geo", new StructType([NestedField.Require(5, "lat", PrimitiveType.Double), NestedField.Optional(6, "lon", PrimitiveType.Double)])),
            NestedField.Optional(7, "zip", PrimitiveType.Int),
        ])),
        NestedField.Optional(8, "tags", new ListType(9, new StructType([NestedField.Optional(10, "k", PrimitiveType.String)]), false)),
        NestedField.Optional(11, "owner", new StructType([NestedField.Require(12, "name", PrimitiveType.String), NestedField.Optional(13, "age", PrimitiveType.Int)])),
    ], identifierFieldIds: [1]);

    private static object?[] Address(string? city, double? lat, int? zip) => [city, lat is null ? null : new object?[] { lat, null }, zip];

    /// <summary>id 1..4; id 3 has no address, id 4 an address without geo; tags and owner null.</summary>
    private async Task<Table> SeededNested(int formatVersion)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", $"t{_tables++}"), NestedSchema, FormatVersion: formatVersion));
        await TableMutations.InsertAsync(t, [
            [1L, Address("Leeds", 53.8, 1), null, null],
            [2L, Address("York", 53.9, 2), null, null],
            [3L, null, null, null],
            [4L, Address("Hull", null, 4), null, null],
        ]);
        return t;
    }

    private async Task<Dictionary<long, object?[]>> NestedRows(Table t)
    {
        var fresh = await _catalog.LoadTableAsync(t.Identifier);
        var report = await TableValidator.ValidateAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
        Assert.True(report.IsValid, report.ToString());
        return (await TableScan.ReadAllAsync(fresh)).ToDictionary(r => (long)r[0]!);
    }

    [Theory]
    [MemberData(nameof(Removal))]
    public async Task Update_sets_a_nested_struct_field_by_path(int formatVersion, DeleteStrategy strategy)
    {
        var t = await SeededNested(formatVersion);
        var r = await TableMutations.UpdateAsync(t, Expr.In("id", 1L, 4L), u => u
            .Set("address.city", row => ((string?)((object?[])row[1]!)[0])!.ToUpperInvariant())
            .Set("address.geo.lon", -1.5) // two levels down; id 4 has no geo, so it is created and needs its required lat
            .Set("address.geo.lat", 1.0)
            .Set("address.zip", 99L), strategy); // long → int, like a filter literal

        Assert.Equal(2, r.RowsUpdated);
        var rows = await NestedRows(t);
        Assert.Equal(new object?[] { "LEEDS", new object?[] { 1.0, -1.5 }, 99 }, rows[1][1]);
        Assert.Equal(new object?[] { "HULL", new object?[] { 1.0, -1.5 }, 99 }, rows[4][1]); // null geo materialised
        Assert.Equal(new object?[] { "York", new object?[] { 53.9, null }, 2 }, rows[2][1]); // untouched
        Assert.Null(rows[3][1]);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task A_nested_assignment_sees_and_keeps_the_row_as_it_was(int formatVersion)
    {
        var t = await SeededNested(formatVersion);
        // Two fields of one struct, the second computed from the first's old value; the whole-column read sees the old struct.
        await TableMutations.UpdateAsync(t, Expr.Equal("id", 2L), u => u
            .Set("address.zip", row => ((string)((object?[])row[1]!)[0]!).Length)
            .Set("address.city", "Scarborough")
            .Set("owner", row => new object?[] { ((object?[])row[1]!)[0], null }));
        var rows = await NestedRows(t);
        Assert.Equal(new object?[] { "Scarborough", new object?[] { 53.9, null }, 4 }, rows[2][1]);
        Assert.Equal(new object?[] { "York", null }, rows[2][3]);
    }

    [Fact]
    public async Task A_null_enclosing_struct_becomes_a_struct_of_nulls()
    {
        var t = await SeededNested(3);
        await TableMutations.UpdateAsync(t, Expr.Equal("id", 3L), u => u.Set("address.zip", 7));
        Assert.Equal(new object?[] { null, null, 7 }, (await NestedRows(t))[3][1]);

        // owner is null and owner.name is required: setting only owner.age cannot build a valid struct.
        var snapshots = (await _catalog.LoadTableAsync(t.Identifier)).Metadata.Snapshots.Count;
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.UpdateAsync(t, Expr.Equal("id", 1L), u => u.Set("owner.age", 40)));
        await TableMutations.UpdateAsync(t, Expr.Equal("id", 1L), u => u.Set("owner.age", 40).Set("owner.name", "Ann"));
        Assert.Equal(snapshots + 1, (await _catalog.LoadTableAsync(t.Identifier)).Metadata.Snapshots.Count);
        Assert.Equal(new object?[] { "Ann", 40 }, (await NestedRows(t))[1][3]);
    }

    [Fact]
    public async Task Nested_assignments_are_checked()
    {
        var t = await SeededNested(2);
        Task Update(Action<RowUpdate> set) => TableMutations.UpdateAsync(t, Expr.Equal("id", 1L), set);
        await Assert.ThrowsAsync<ArgumentException>(() => Update(u => u.Set("address.nope", 1)));
        await Assert.ThrowsAsync<ArgumentException>(() => Update(u => u.Set("tags.element.k", "x"))); // inside a list
        await Assert.ThrowsAsync<ArgumentException>(() => Update(u => u.Set("address", (object?)null).Set("address.city", "x"))); // struct and its field
        await Assert.ThrowsAsync<ArgumentException>(() => Update(u => u.Set("address.geo.lat", (object?)null))); // required
        await Assert.ThrowsAsync<ArgumentException>(() => Update(u => u.Set("address.zip", "seven")));
        Assert.Single((await _catalog.LoadTableAsync(t.Identifier)).Metadata.Snapshots);
    }

    [Theory]
    [MemberData(nameof(Removal))]
    public async Task Merge_sets_a_nested_field_from_the_source(int formatVersion, DeleteStrategy strategy)
    {
        var t = await SeededNested(formatVersion);
        object?[] Src(long id, string city) => [id, Address(city, null, null), null, null];
        var r = await TableMutations.MergeAsync(t, [Src(2, "Ripon"), Src(5, "Selby")], ["id"], m => m
            .WhenMatchedUpdate(u => u.Set("address.city", row => "was " + ((object?[])row[1]!)[0]))
            .WhenNotMatchedInsert(), strategy);
        Assert.Equal((1L, 1L), (r.RowsUpdated, r.RowsInserted));
        var rows = await NestedRows(t);
        Assert.Equal(new object?[] { "was York", new object?[] { 53.9, null }, 2 }, rows[2][1]);
        Assert.Equal(Address("Selby", null, null), rows[5][1]);
    }
}
