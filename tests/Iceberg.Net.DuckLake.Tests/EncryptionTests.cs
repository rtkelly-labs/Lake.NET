using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// Encrypted lakes (<c>ducklake_metadata.encrypted</c>, <c>encryption_key</c> of data and delete files): every Parquet
/// file this library writes is encrypted with Parquet modular encryption under a fresh 256-bit key per write, stored
/// Base64 as DuckDB stores it, and read back with it. On every database and version.
/// </summary>
public abstract class EncryptionTests : IAsyncLifetime
{
    private readonly TestLake _lake;

    protected EncryptionTests(CatalogBackend backend, string version) => _lake = new TestLake(backend, version);

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        try { await _lake.ValidateAsync(); }
        finally { _lake.Dispose(); }
    }

    private async Task<DuckLakeCatalog> LakeAsync(int inlining = 0)
    {
        var catalog = await _lake.CreateAsync(new DuckLakeOptions { Encrypted = true, DataInliningRowLimit = inlining });
        await using var tx = catalog.BeginTransaction();
        await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64), new("name", DuckLakeType.Varchar)]);
        await tx.CommitAsync();
        return catalog;
    }

    private static async Task CommitAsync(DuckLakeCatalog catalog, Func<DuckLakeTransaction, Task> work)
    {
        await using var tx = catalog.BeginTransaction();
        await work(tx);
        await tx.CommitAsync();
    }

    private static async Task<List<string>> RowsAsync(DuckLakeCatalog catalog) =>
        (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"))).Select(r => $"{r[0]}:{r[1]}").Order(StringComparer.Ordinal).ToList();

    [Fact]
    [Spec("ducklake: ducklake_metadata")]
    [Spec("ducklake: ducklake_data_file")]
    [Spec("ducklake: ducklake_delete_file")]
    public async Task Every_file_is_encrypted_under_its_own_writes_key_and_reads_back()
    {
        var catalog = await LakeAsync();
        Assert.True(catalog.Encrypted);
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", Enumerable.Range(0, 10).Select(i => new object?[] { (long)i, $"n{i}" })));
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[100L, "second"]]));
        await CommitAsync(catalog, tx => tx.DeleteAsync("main", "t", Expr.In("id", 1L, 2L)));
        await CommitAsync(catalog, tx => tx.UpdateAsync("main", "t", Expr.Equal("id", 3L), r => [r[0], "updated"]));
        Assert.Equal(["0:n0", "100:second", "3:updated", "4:n4", "5:n5", "6:n6", "7:n7", "8:n8", "9:n9"], await RowsAsync(catalog));

        // Encrypted footers ("PARE"), each file with a 256-bit Base64 key, a fresh key per write.
        foreach (var file in _lake.Files())
        {
            var bytes = await File.ReadAllBytesAsync(file);
            Assert.Equal("PARE", System.Text.Encoding.ASCII.GetString(bytes[^4..]));
            Assert.DoesNotContain("second"u8.ToArray(), Enumerable.Range(0, bytes.Length - 6).Select(i => bytes[i..(i + 6)]), new BytesComparer());
        }
        var keys = (await _lake.SqlAsync("SELECT encryption_key FROM ducklake_data_file UNION ALL SELECT encryption_key FROM ducklake_delete_file"))
            .Select(r => (string)r[0]!).ToList();
        Assert.All(keys, k => Assert.Equal(32, Convert.FromBase64String(k).Length));
        Assert.Equal(keys.Count, keys.Distinct().Count());
        Assert.Equal("true", (await _lake.SqlAsync("SELECT value FROM ducklake_metadata WHERE \"key\" = 'encrypted'"))[0][0]);

        // A reader without the key cannot read the data; reopening the lake picks up its mode.
        await Assert.ThrowsAnyAsync<Exception>(() => Data.ParquetFiles.ReadAsync(_lake.IO, _lake.Files().First(f => !f.EndsWith("-delete.parquet", StringComparison.Ordinal)),
            new Types.Schema(0, [Types.NestedField.Optional(1, "id", Types.PrimitiveType.Long)])));
        var reopened = await _lake.OpenAsync();
        Assert.True(reopened.Encrypted);
        Assert.Equal(await RowsAsync(catalog), await RowsAsync(reopened));
    }

    [Fact]
    public async Task Maintenance_and_flushes_write_encrypted_files_too()
    {
        var catalog = await LakeAsync(inlining: 5);
        for (var i = 0; i < 3; i++) await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", Enumerable.Range(i * 10, 10).Select(x => new object?[] { (long)x, "big" })));
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[1000L, "inlined"]]));
        await catalog.FlushInlinedDataAsync();
        await catalog.MergeAdjacentFilesAsync();
        await CommitAsync(catalog, tx => tx.DeleteAsync("main", "t", Expr.LessThan("id", 25L)));
        await catalog.RewriteDataFilesAsync(deleteThreshold: 0.1);
        Assert.Equal(["1000:inlined", "25:big", "26:big", "27:big", "28:big", "29:big"], await RowsAsync(catalog));
        Assert.Empty(await _lake.SqlAsync("SELECT data_file_id FROM ducklake_data_file WHERE encryption_key IS NULL"));
        Assert.Empty(await _lake.SqlAsync("SELECT delete_file_id FROM ducklake_delete_file WHERE encryption_key IS NULL"));
    }

    [Fact]
    public async Task Existing_files_cannot_be_added_to_an_encrypted_lake()
    {
        var catalog = await LakeAsync();
        await using var tx = catalog.BeginTransaction();
        await Assert.ThrowsAsync<NotSupportedException>(() => tx.AddDataFilesAsync("main", "t", ["/nowhere.parquet"]));
    }

    private sealed class BytesComparer : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[]? x, byte[]? y) => x!.AsSpan().SequenceEqual(y);
        public int GetHashCode(byte[] obj) => obj.Length;
    }
}

public sealed class SqliteEncryptionTests10() : EncryptionTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion10);
public sealed class SqliteEncryptionTests11() : EncryptionTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion11);
public sealed class PostgresEncryptionTests10() : EncryptionTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion10);
public sealed class PostgresEncryptionTests11() : EncryptionTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion11);
