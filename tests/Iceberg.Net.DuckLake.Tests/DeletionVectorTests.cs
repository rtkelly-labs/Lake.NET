using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.Puffin;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// Format 1.1's experimental Puffin delete files (spec <c>ducklake_delete_file</c>): Iceberg <c>deletion-vector-v1</c>
/// blobs, bare or in a Puffin container of cumulative vectors keyed by <c>ducklake-snapshot-id</c>. Read always;
/// written under <c>write_deletion_vectors</c>.
/// </summary>
public abstract class DeletionVectorTests : IAsyncLifetime
{
    private readonly TestLake _lake;

    protected DeletionVectorTests(CatalogBackend backend) => _lake = new TestLake(backend, DuckLakeCatalog.FormatVersion11);

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        try { await _lake.ValidateAsync(); }
        finally { _lake.Dispose(); }
    }

    private async Task<DuckLakeCatalog> LakeAsync(bool vectors = true)
    {
        var catalog = await _lake.CreateAsync(new DuckLakeOptions { WriteDeletionVectors = vectors });
        await using var tx = catalog.BeginTransaction();
        await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64)]);
        await tx.InsertAsync("main", "t", Enumerable.Range(0, 10).Select(i => new object?[] { (long)i }));
        await tx.CommitAsync();
        return catalog;
    }

    private static async Task DeleteAsync(DuckLakeCatalog catalog, params long[] ids)
    {
        await using var tx = catalog.BeginTransaction();
        await tx.DeleteAsync("main", "t", Expr.In("id", ids.Cast<object>().ToArray()));
        await tx.CommitAsync();
    }

    private static async Task<long[]> IdsAsync(DuckLakeCatalog catalog, long? snapshot = null) =>
        (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t", snapshot))).Select(r => (long)r[0]!).Order().ToArray();

    [Fact]
    [Spec("ducklake: ducklake_delete_file")]
    public async Task Deletes_are_written_as_bare_deletion_vectors_and_read_at_every_snapshot()
    {
        var catalog = await LakeAsync();
        await DeleteAsync(catalog, 1, 2);
        var first = (await catalog.CurrentSnapshotAsync()).SnapshotId;
        await DeleteAsync(catalog, 5);
        Assert.Equal(new long[] { 0, 3, 4, 6, 7, 8, 9 }, await IdsAsync(catalog));
        Assert.Equal(new long[] { 0, 3, 4, 5, 6, 7, 8, 9 }, await IdsAsync(catalog, first));

        var rows = await _lake.SqlAsync("SELECT format, delete_count, footer_size, row_group_count, path FROM ducklake_delete_file ORDER BY delete_file_id");
        Assert.Equal(["puffin 2 0 ", "puffin 3 0 "], rows.Select(r => $"{r[0]} {r[1]} {r[2]} {r[3]}"));
        var latest = DeletionVector.Decode(await File.ReadAllBytesAsync(_lake.Files().Single(f => f.EndsWith((string)rows[1][4]!, StringComparison.Ordinal))));
        Assert.Equal([1L, 2, 5], latest.Positions());
    }

    [Fact]
    public async Task The_lakes_option_decides_whether_vectors_are_written()
    {
        var catalog = await LakeAsync(vectors: false);
        await _lake.ExecAsync("INSERT INTO ducklake_metadata (\"key\", \"value\", \"scope\", scope_id) VALUES ('write_deletion_vectors', 'true', NULL, NULL)");
        await DeleteAsync(catalog, 1);
        await _lake.ExecAsync("UPDATE ducklake_metadata SET \"value\" = 'false' WHERE \"key\" = 'write_deletion_vectors'");
        await DeleteAsync(catalog, 2);
        Assert.Equal(["puffin", "parquet"], (await _lake.SqlAsync("SELECT format FROM ducklake_delete_file ORDER BY delete_file_id")).Select(r => (string)r[0]!));
        Assert.Equal(new long[] { 0, 3, 4, 5, 6, 7, 8, 9 }, await IdsAsync(catalog));
    }

    [Fact]
    public async Task A_Puffin_container_of_cumulative_vectors_reads_the_latest_vector_not_after_the_snapshot()
    {
        var catalog = await LakeAsync();
        await DeleteAsync(catalog, 1);
        var s = (await catalog.CurrentSnapshotAsync()).SnapshotId;
        // Replace the bare vector with a container: {1} at s, {1, 4} at s + 5 (a later snapshot this reader has not reached).
        var path = _lake.Files().Single(f => f.EndsWith(".puffin", StringComparison.Ordinal));
        PuffinBlob Vector(long snapshot, params long[] positions)
        {
            var bitmap = new RoaringPositionBitmap();
            bitmap.AddRange(positions);
            return new PuffinBlob(DuckLakeDeleteFiles.DeletionVectorBlob, [], DeletionVector.Encode(bitmap),
                Properties: new Dictionary<string, string> { [DuckLakeDeleteFiles.SnapshotProperty] = snapshot.ToString(System.Globalization.CultureInfo.InvariantCulture) });
        }
        var written = PuffinFile.Write([Vector(s + 5, 1, 4), Vector(s, 1)]);
        await File.WriteAllBytesAsync(path, written.Bytes);
        await _lake.ExecAsync($"UPDATE ducklake_delete_file SET file_size_bytes = {written.Bytes.Length}, footer_size = {written.FooterSize}");

        Assert.Equal(new long[] { 0, 2, 3, 4, 5, 6, 7, 8, 9 }, await IdsAsync(catalog));
        Assert.Equal([1L], await DuckLakeDeleteFiles.ReadPositionsAsync(_lake.IO, path, s));
        Assert.Equal([1L, 4], (await DuckLakeDeleteFiles.ReadPositionsAsync(_lake.IO, path, s + 5)).Order());
        Assert.Equal([(1L, (long?)s), (4L, s + 5)], (await DuckLakeDeleteFiles.ReadEntriesAsync(_lake.IO, path)).OrderBy(e => e.Position));
        // A delete over it writes the union of what is in effect and the new positions.
        await DeleteAsync(catalog, 7);
        Assert.Equal(new long[] { 0, 2, 3, 4, 5, 6, 8, 9 }, await IdsAsync(catalog));
    }

    [Fact]
    public async Task Format_10_lakes_never_write_vectors()
    {
        var lake = new TestLake(_lake.Backend, DuckLakeCatalog.FormatVersion10);
        try
        {
            var catalog = await lake.CreateAsync(new DuckLakeOptions { WriteDeletionVectors = true });
            await using (var tx = catalog.BeginTransaction())
            {
                await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64)]);
                await tx.InsertAsync("main", "t", [[1L], [2L]]);
                await tx.CommitAsync();
            }
            await DeleteAsync(catalog, 1);
            Assert.Equal(["parquet"], (await lake.SqlAsync("SELECT format FROM ducklake_delete_file")).Select(r => (string)r[0]!));
            await lake.ValidateAsync();
        }
        finally { lake.Dispose(); }
    }
}

public sealed class SqliteDeletionVectorTests() : DeletionVectorTests(CatalogBackend.Sqlite);
public sealed class PostgresDeletionVectorTests() : DeletionVectorTests(CatalogBackend.Postgres);
