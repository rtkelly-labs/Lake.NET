using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Appends, overwrites and row deltas committed to a branch (Java's <c>toBranch</c>) and write-audit-publish.</summary>
[Spec("Snapshot References")]
[Spec("Sequence Numbers")]
public sealed class BranchWriteTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-branchwrite", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public BranchWriteTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("b")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static List<object?[]> Rows(int from, int n, string v = "a") => SnapshotManagementTests.Rows(from, n, v);

    private async Task<Table> NewTable(int formatVersion)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("b", $"t{Guid.NewGuid():N}"), SnapshotManagementTests.Schema,
            PartitionSpec.BuilderFor(SnapshotManagementTests.Schema).Identity("p").Build(), FormatVersion: formatVersion));
        await TableMutations.InsertAsync(t, Rows(0, 4));
        return t;
    }

    private static async Task<Dictionary<long, string?>> Read(Table t, string? branch = null)
    {
        var rows = new Dictionary<long, string?>();
        await foreach (var r in TableScan.ReadRowsAsync(t, new ScanRequest { Snapshot = branch is null ? SnapshotSelector.Current : SnapshotSelector.Ref(branch) }))
            rows.Add((long)r[0]!, (string?)r[1]);
        return rows;
    }

    private async Task Valid(Table t) =>
        await TableValidator.AssertValidAsync(await _catalog.LoadTableAsync(t.Identifier), new ValidationOptions { AllSnapshots = true, Deep = true });

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Appends_to_a_branch_leave_main_untouched_and_fork_from_main(int formatVersion)
    {
        var t = await NewTable(formatVersion);
        var main = t.Metadata.CurrentSnapshotId!.Value;
        var mainMetadataRef = t.Metadata.Refs[SnapshotRef.Main];

        var first = await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, Rows(10, 2))).ToBranch("audit").CommitAsync();
        Assert.Equal(main, first.ParentSnapshotId);                              // a new branch starts at main's head
        Assert.Equal(mainMetadataRef, t.Metadata.Refs[SnapshotRef.Main]);
        Assert.Equal(main, t.Metadata.CurrentSnapshotId);
        Assert.Equal(new SnapshotRef(first.SnapshotId, SnapshotRefType.Branch), t.Metadata.Refs["audit"]);

        await t.ManageSnapshots().SetMinSnapshotsToKeep("audit", 4).CommitAsync();
        await TableMutations.InsertAsync(t, Rows(20, 1));                        // main moves on independently
        var second = await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, Rows(30, 1))).ToBranch("audit").CommitAsync();
        Assert.Equal(first.SnapshotId, second.ParentSnapshotId);                 // the branch's own parent
        Assert.True(second.SequenceNumber > t.Metadata.SnapshotById(t.Metadata.CurrentSnapshotId!.Value)!.SequenceNumber); // one global sequence
        Assert.Equal(new SnapshotRef(second.SnapshotId, SnapshotRefType.Branch, 4), t.Metadata.Refs["audit"]); // retention kept
        Assert.Equal("5", second.Summary["total-data-files"]);                   // totals follow the branch (main has 3 files)

        Assert.Equal([0L, 1, 2, 3, 20], (await Read(t)).Keys.Order());
        Assert.Equal([0L, 1, 2, 3, 10, 11, 30], (await Read(t, "audit")).Keys.Order());
        await Valid(t);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Overwrites_and_row_deltas_on_a_branch_see_only_the_branch(int formatVersion)
    {
        var t = await NewTable(formatVersion);
        await t.ManageSnapshots().CreateBranch("dev").CommitAsync();
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, Rows(10, 2))).ToBranch("dev").CommitAsync();

        // Copy-on-write overwrite of partition p=0 on dev: main's files of p=0 are untouched.
        var devHead = t.Metadata.Refs["dev"].SnapshotId;
        var p0 = (await TableScan.PlanFilesAsync(t, devHead)).Where(x => (int)x.File.Partition[0]! == 0).Select(x => x.File.FilePath).ToHashSet();
        await SnapshotProducer.Overwrite(t, f => p0.Contains(f.FilePath)).AddFiles(await TableWriter.WriteAsync(t, [[0L, "dev", 0]])).ToBranch("dev").CommitAsync();
        Assert.Equal(new Dictionary<long, string?> { [0] = "dev", [1] = "a", [3] = "a", [11] = "a" }, await Read(t, "dev"));
        Assert.Equal(4, (await Read(t)).Count);

        // Position deletes / DVs against a file only the branch has.
        var file11 = (await TableScan.PlanFilesAsync(t, t.Metadata.Refs["dev"].SnapshotId)).Single(x => x.File.Partition[0] is 1 && x.File.RecordCount == 1).File.FilePath;
        var delete = await SnapshotProducer.DeleteRows(t, new Dictionary<string, IEnumerable<long>> { [file11] = [0] }).ToBranch("dev").CommitAsync();
        Assert.Equal(Operations.Delete, delete.Operation);
        Assert.Equal([0L, 1, 3], (await Read(t, "dev")).Keys.Order());
        // ... and a delete against a file main does not reference is refused on main.
        await Assert.ThrowsAsync<ValidationException>(() => SnapshotProducer.DeleteRows(t, new Dictionary<string, IEnumerable<long>> { [file11] = [0] }).CommitAsync());
        Assert.Equal([0L, 1, 2, 3], (await Read(t)).Keys.Order());
        await Valid(t);
    }

    [Fact]
    public async Task A_concurrent_commit_to_main_does_not_conflict_with_a_branch_overwrite()
    {
        var t = await NewTable(3);
        await t.ManageSnapshots().CreateBranch("dev").CommitAsync();
        var other = await _catalog.LoadTableAsync(t.Identifier);
        var all = (await TableScan.PlanFilesAsync(t)).Select(x => x.File.FilePath).ToHashSet();
        var overwrite = SnapshotProducer.Overwrite(t, f => all.Contains(f.FilePath)).AddFiles(await TableWriter.WriteAsync(t, [[100L, "x", 0]])).ToBranch("dev");

        await TableMutations.DeleteWhereAsync(other, Expr.Equal("id", 1L), DeleteStrategy.PositionDeletes); // main only
        await overwrite.CommitAsync(); // the branch did not move: only the sequence number and row IDs are re-assigned

        Assert.Equal([100L], (await Read(t, "dev")).Keys.Order());
        Assert.Equal([0L, 2, 3], (await Read(t)).Keys.Order());
        await Valid(t);

        // A concurrent change on the branch itself is validated like one on main.
        var stale = await _catalog.LoadTableAsync(t.Identifier);
        var devFiles = (await TableScan.PlanFilesAsync(stale, stale.Metadata.Refs["dev"].SnapshotId)).Select(x => x.File.FilePath).ToHashSet();
        var conflicting = SnapshotProducer.Overwrite(stale, f => devFiles.Contains(f.FilePath)).AddFiles(await TableWriter.WriteAsync(stale, [[200L, "y", 0]])).ToBranch("dev");
        var head = (await TableScan.PlanFilesAsync(t, t.Metadata.Refs["dev"].SnapshotId)).Single().File.FilePath;
        await SnapshotProducer.DeleteRows(t, new Dictionary<string, IEnumerable<long>> { [head] = [0] }).ToBranch("dev").CommitAsync();
        await Assert.ThrowsAsync<ValidationException>(() => conflicting.CommitAsync());
        await Valid(t);
    }

    [Fact]
    public async Task Tags_cannot_be_written_to()
    {
        var t = await NewTable(2);
        await t.ManageSnapshots().CreateTag("v1", t.Metadata.CurrentSnapshotId!.Value).CommitAsync();
        Assert.Throws<ArgumentException>(() => SnapshotProducer.Append(t).ToBranch("v1"));
    }

    /// <summary>Write-audit-publish: stage on an audit branch, audit it there, then publish by fast-forwarding main.</summary>
    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Write_audit_publish(int formatVersion)
    {
        var t = await NewTable(formatVersion);
        var before = await Read(t);

        // Write: an upsert-shaped row delta (delete id 1, add the new version) staged on "audit".
        var file1 = (await TableScan.PlanFilesAsync(t, Expr.And(Expr.Equal("id", 1L), Expr.Equal("p", 1)))).Single();
        var rows1 = await DataFiles.ReadAsync(t.IO, file1.File, t.Schema);
        var pos = rows1.FindIndex(r => (long)r[0]! == 1);
        await SnapshotProducer.RowDelta(t).DeletePositions(file1.File.FilePath, [pos])
            .AddFiles(await TableWriter.WriteAsync(t, [[1L, "audited", 1], [50L, "new", 0]]))
            .Set("iceberg-net.command-id", "wap-1")
            .ToBranch("audit").CommitAsync();
        Assert.Equal(before, await Read(t)); // readers of main see nothing yet

        // Audit: the staged state is a valid table and holds what we expect.
        var staged = await _catalog.LoadTableAsync(t.Identifier);
        var report = await TableValidator.ValidateAsync(staged, new ValidationOptions { AllSnapshots = true, Deep = true });
        Assert.True(report.IsValid, report.ToString());
        var audit = await Read(staged, "audit");
        Assert.Equal("audited", audit[1]);
        Assert.Equal(before.Count + 1, audit.Count);

        // Publish: main fast-forwards to the audited snapshot (no new snapshot), then the branch is dropped.
        var auditHead = staged.Metadata.Refs["audit"].SnapshotId;
        await staged.ManageSnapshots().FastForwardBranch(SnapshotRef.Main, "audit").RemoveBranch("audit").CommitAsync();
        Assert.Equal(auditHead, staged.Metadata.CurrentSnapshotId);
        Assert.Equal("wap-1", staged.Metadata.CurrentSnapshot!.Summary["iceberg-net.command-id"]);
        Assert.Equal(audit, await Read(staged));
        Assert.Equal([SnapshotRef.Main], staged.Metadata.Refs.Keys);
        await Valid(staged);
    }
}
