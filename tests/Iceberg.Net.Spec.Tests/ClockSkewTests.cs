using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;
using Microsoft.Extensions.Time.Testing;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Clock skew between the maintenance process, the writers and the object store. Ages are measured with the
/// maintenance process's <see cref="TimeProvider"/>; a skewed clock may delay a delete but must never make maintenance
/// delete a file a running writer is about to commit, or expire a snapshot a skewed writer just made.
/// </summary>
[Spec("Snapshot Retention Policy")]
[Spec("File System Operations")]
public sealed class ClockSkewTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-skew", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public ClockSkewTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("k")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Require(2, "p", PrimitiveType.Int),
    ], identifierFieldIds: [1]);

    private static List<object?[]> Rows(int from, int n) => Enumerable.Range(from, n).Select(i => new object?[] { (long)i, i % 2 }).ToList();

    private static string Local(string location) => location.StartsWith('/') ? location : new Uri(location).LocalPath;

    private async Task<Table> NewTable(string name)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("k", name), Schema, PartitionSpec.BuilderFor(Schema).Identity("p").Build()));
        await TableMutations.InsertAsync(t, Rows(0, 4));
        return t;
    }

    /// <summary>A writer's data file, written now and not yet committed: what orphan cleanup must not take for an orphan.</summary>
    private static async Task<string> InFlightFile(Table t)
    {
        var path = Path.Combine(Local(t.Location), "data", $"in-flight-{Guid.NewGuid():N}.parquet");
        await File.WriteAllBytesAsync(path, [1, 2, 3]);
        return path;
    }

    public static TheoryData<double> SkewsWithinTheMinimumAge() => new() { 0, 1, 12, 23.9 };

    [Theory]
    [MemberData(nameof(SkewsWithinTheMinimumAge))]
    public async Task A_concurrent_uncommitted_file_survives_a_maintenance_clock_ahead_by_up_to_the_minimum_age(double hoursAhead)
    {
        var t = await NewTable($"ahead{(int)(hoursAhead * 10)}");
        var inFlight = await InFlightFile(t);
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow.AddHours(hoursAhead));

        var result = await Maintenance.RemoveOrphanFilesAsync(t, new OrphanFilesOptions { OlderThan = Maintenance.MinOrphanAge, TimeProvider = clock });

        Assert.Empty(result.Orphans);
        Assert.True(File.Exists(inFlight));
    }

    [Fact]
    public async Task Beyond_the_minimum_age_the_same_file_is_an_orphan_and_committed_files_never_are()
    {
        var t = await NewTable("beyond");
        var inFlight = await InFlightFile(t);
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow.Add(Maintenance.MinOrphanAge).AddMinutes(5));

        var result = await Maintenance.RemoveOrphanFilesAsync(t, new OrphanFilesOptions { OlderThan = Maintenance.MinOrphanAge, TimeProvider = clock });

        Assert.Equal([inFlight], result.Orphans.Select(Local));
        Assert.False(File.Exists(inFlight));
        await TableValidator.AssertValidAsync(await _catalog.LoadTableAsync(t.Identifier), new ValidationOptions { Deep = true, AllSnapshots = true });
    }

    [Fact]
    public async Task A_file_the_store_stamped_in_the_future_is_never_old_enough()
    {
        var t = await NewTable("store-ahead");
        var inFlight = await InFlightFile(t);
        File.SetLastWriteTimeUtc(inFlight, DateTime.UtcNow.AddDays(2)); // the store's clock runs ahead of ours
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow.AddDays(3));

        var result = await Maintenance.RemoveOrphanFilesAsync(t, new OrphanFilesOptions { TimeProvider = clock }); // default age: 3 days

        Assert.Empty(result.Orphans);
        Assert.True(File.Exists(inFlight));
    }

    [Fact]
    public async Task The_default_age_is_measured_with_the_given_clock()
    {
        var t = await NewTable("default-age");
        var inFlight = await InFlightFile(t);
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow.AddDays(2));
        Assert.Empty((await Maintenance.RemoveOrphanFilesAsync(t, new OrphanFilesOptions { TimeProvider = clock, DryRun = true })).Orphans);
        clock.Advance(TimeSpan.FromDays(1.1));
        Assert.Equal([inFlight], (await Maintenance.RemoveOrphanFilesAsync(t, new OrphanFilesOptions { TimeProvider = clock, DryRun = true })).Orphans.Select(Local));
    }

    [Fact]
    public async Task Expiry_keeps_snapshots_stamped_after_its_now()
    {
        var now = DateTimeOffset.UtcNow;
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("k", "future"), Schema, PartitionSpec.BuilderFor(Schema).Identity("p").Build()));
        async Task<Snapshot> Append(int from, DateTimeOffset at, string? branch = null)
        {
            var producer = SnapshotProducer.Append(t).WithClock(new FakeTimeProvider(at)).AddFiles(await TableWriter.WriteAsync(t, Rows(from, 2)));
            return await (branch is null ? producer : producer.ToBranch(branch)).CommitAsync();
        }
        var old1 = await Append(0, now.AddDays(-10));
        var old2 = await Append(2, now.AddDays(-9));
        var future = await Append(4, now.AddHours(2)); // a writer whose clock runs two hours ahead of the maintenance process
        await t.ManageSnapshots().CreateBranch("skewed", future.SnapshotId, new RefRetention(MaxRefAgeMs: 60_000)).CommitAsync();
        await Append(6, now.AddHours(3), "skewed");

        var result = await Maintenance.ExpireAsync(t, new ExpireOptions { TimeProvider = new FakeTimeProvider(now) });

        Assert.Equal(new[] { old1.SnapshotId, old2.SnapshotId }.Order(), result.Expired.Order());
        Assert.Empty(result.RemovedRefs); // the branch's head is "younger" than now - max-ref-age
        var kept = (await _catalog.LoadTableAsync(t.Identifier)).Metadata;
        Assert.Contains(kept.Snapshots, s => s.SnapshotId == future.SnapshotId);
        Assert.Equal(2, kept.Snapshots.Count);
        Assert.True(kept.Refs.ContainsKey("skewed"));
        await TableValidator.AssertValidAsync(await _catalog.LoadTableAsync(t.Identifier), new ValidationOptions { Deep = true, AllSnapshots = true });
    }

    [Fact]
    public async Task Expiry_ages_are_measured_with_the_given_clock()
    {
        var t = await NewTable("clocked");
        await TableMutations.InsertAsync(t, Rows(10, 2));
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        Assert.Empty((await Maintenance.ExpireAsync(t, new ExpireOptions { TimeProvider = clock })).Expired); // 5 days by default
        clock.Advance(TimeSpan.FromDays(6));
        Assert.Single((await Maintenance.ExpireAsync(t, new ExpireOptions { TimeProvider = clock })).Expired);
    }
}
