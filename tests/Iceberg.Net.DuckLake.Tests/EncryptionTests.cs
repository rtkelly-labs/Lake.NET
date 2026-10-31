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

    [SkippableFact]
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

    [SkippableFact]
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

    [SkippableFact]
    public async Task Existing_files_cannot_be_added_to_an_encrypted_lake()
    {
        var catalog = await LakeAsync();
        await using var tx = catalog.BeginTransaction();
        await Assert.ThrowsAsync<NotSupportedException>(() => tx.AddDataFilesAsync("main", "t", ["/nowhere.parquet"]));
    }

    // ---- hostile and damaged input (gap S12): each a format error, never a plaintext read, never a key in the text ----

    /// <summary>An encrypted lake with one table of ten rows in one data file; its key (Base64) and the file's path.</summary>
    private async Task<(DuckLakeCatalog Catalog, string Key, string File)> OneFileAsync()
    {
        var catalog = await LakeAsync();
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", Enumerable.Range(0, 10).Select(i => new object?[] { (long)i, $"secret{i}" })));
        var key = (string)(await _lake.SqlAsync("SELECT encryption_key FROM ducklake_data_file"))[0][0]!;
        _lake.Corrupted = true; // every case below damages the lake on purpose
        return (catalog, key, _lake.Files().Single());
    }

    /// <summary>The read fails with a format error whose text (inner exceptions too) and spans carry no key material.</summary>
    private async Task AssertFormatErrorAsync(DuckLakeCatalog catalog, params string[] keys)
    {
        using var spans = new SpanRecorder(_lake);
        var e = await Assert.ThrowsAsync<InvalidDataException>(async () => await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t")));
        var forms = keys.SelectMany(k => new[] { k, Convert.ToHexString(Convert.FromBase64String(k)), Convert.ToHexString(Convert.FromBase64String(k)).ToLowerInvariant() })
            .Where(f => f.Length >= 8).ToList();
        for (Exception? x = e; x is not null; x = x.InnerException)
            foreach (var f in forms) Assert.DoesNotContain(f, x.ToString(), StringComparison.Ordinal);
        Assert.NotEmpty(spans.Spans);
        foreach (var span in spans.Spans)
        {
            var text = string.Join("\n", span.TagObjects.Select(t => $"{t.Key}={t.Value}").Append(span.StatusDescription ?? ""));
            foreach (var f in forms) Assert.DoesNotContain(f, text, StringComparison.Ordinal);
        }
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_data_file")]
    public async Task A_file_without_a_key_in_an_encrypted_lake_is_refused_not_read_as_plaintext()
    {
        var catalog = await LakeAsync();
        // A plaintext file gets in (written while the lake claimed to be unencrypted); the lake is encrypted again.
        await _lake.ExecAsync("UPDATE ducklake_metadata SET value = 'false' WHERE \"key\" = 'encrypted'");
        var plain = await _lake.OpenAsync();
        Assert.False(plain.Encrypted);
        await CommitAsync(plain, tx => tx.InsertAsync("main", "t", [[666L, "planted"]]));
        await _lake.ExecAsync("UPDATE ducklake_metadata SET value = 'true' WHERE \"key\" = 'encrypted'");
        _lake.Corrupted = true;
        var reopened = await _lake.OpenAsync();
        Assert.True(reopened.Encrypted);
        var e = await Assert.ThrowsAsync<InvalidDataException>(async () => await reopened.ReadAsync(await reopened.LoadTableAsync("main", "t")));
        Assert.Contains("encryption_key", e.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task A_wrong_key_is_a_format_error()
    {
        var (catalog, key, _) = await OneFileAsync();
        var wrong = Convert.ToBase64String(System.Security.Cryptography.RandomNumberGenerator.GetBytes(32));
        await _lake.ExecAsync($"UPDATE ducklake_data_file SET encryption_key = '{wrong}'");
        await AssertFormatErrorAsync(catalog, key, wrong);
    }

    [SkippableFact]
    public async Task Keys_swapped_between_files_are_a_format_error()
    {
        var (catalog, key, _) = await OneFileAsync();
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[100L, "other write"]]));
        var keys = (await _lake.SqlAsync("SELECT data_file_id, encryption_key FROM ducklake_data_file ORDER BY data_file_id")).Select(r => (Id: r[0], Key: (string)r[1]!)).ToList();
        await _lake.ExecAsync($"UPDATE ducklake_data_file SET encryption_key = '{keys[1].Key}' WHERE data_file_id = {keys[0].Id}");
        await _lake.ExecAsync($"UPDATE ducklake_data_file SET encryption_key = '{keys[0].Key}' WHERE data_file_id = {keys[1].Id}");
        await AssertFormatErrorAsync(catalog, keys[0].Key, keys[1].Key);
    }

    [SkippableTheory]
    [InlineData(0)]
    [InlineData(7)]
    [InlineData(31)]
    [InlineData(33)]
    [InlineData(64)]
    public async Task A_key_of_the_wrong_length_is_a_format_error(int length)
    {
        var (catalog, key, _) = await OneFileAsync();
        var wrong = Convert.ToBase64String(System.Security.Cryptography.RandomNumberGenerator.GetBytes(length));
        await _lake.ExecAsync($"UPDATE ducklake_data_file SET encryption_key = '{wrong}'");
        await AssertFormatErrorAsync(catalog, length == 0 ? [key] : [key, wrong]);
    }

    [SkippableFact]
    public async Task A_key_that_is_not_Base64_is_a_format_error()
    {
        var (catalog, key, _) = await OneFileAsync();
        await _lake.ExecAsync("UPDATE ducklake_data_file SET encryption_key = 'not base64!'");
        await AssertFormatErrorAsync(catalog, key);
    }

    public static TheoryData<string> Damage => ["flip-footer", "flip-first-page", "truncate-half", "truncate-tail", "truncate-magic"];

    [SkippableTheory]
    [MemberData(nameof(Damage))]
    public async Task A_damaged_or_truncated_encrypted_file_is_a_format_error(string damage)
    {
        var (catalog, key, file) = await OneFileAsync();
        var bytes = await File.ReadAllBytesAsync(file);
        var footerLength = BitConverter.ToInt32(bytes, bytes.Length - 8);
        bytes = damage switch
        {
            "flip-footer" => Flip(bytes, bytes.Length - 8 - footerLength / 2),
            "flip-first-page" => Flip(bytes, 40),
            "truncate-half" => bytes[..(bytes.Length / 2)],
            "truncate-tail" => bytes[..^1],
            _ => bytes[..^4],
        };
        File.Delete(file);
        await File.WriteAllBytesAsync(file, bytes);
        if (damage.StartsWith("truncate", StringComparison.Ordinal))
            await _lake.ExecAsync($"UPDATE ducklake_data_file SET file_size_bytes = {bytes.Length}");
        await AssertFormatErrorAsync(catalog, key);

        static byte[] Flip(byte[] b, int at)
        {
            b[at] ^= 0x5A;
            return b;
        }
    }

    [SkippableFact]
    public async Task No_flipped_byte_reads_back_different_rows()
    {
        // AES-GCM authenticates every module the reader decrypts: a flipped byte is a format error, or (in bytes this read
        // never touches, such as page indexes) changes nothing. Never different rows.
        var (catalog, _, file) = await OneFileAsync();
        var table = await catalog.LoadTableAsync("main", "t");
        var expected = (await catalog.ReadAsync(table)).Select(r => string.Join("|", r)).ToList();
        var original = await File.ReadAllBytesAsync(file);
        var stride = Math.Max(1, original.Length / 40);
        var errors = 0;
        for (var at = 0; at < original.Length; at += stride)
        {
            var bytes = (byte[])original.Clone();
            bytes[at] ^= 0x01;
            File.Delete(file);
            await File.WriteAllBytesAsync(file, bytes);
            try
            {
                Assert.Equal(expected, (await catalog.ReadAsync(table)).Select(r => string.Join("|", r)).ToList());
            }
            catch (InvalidDataException)
            {
                errors++;
            }
        }
        Assert.True(errors > 0);
        File.Delete(file);
        await File.WriteAllBytesAsync(file, original);
        _lake.Corrupted = false;
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_delete_file")]
    public async Task A_delete_file_with_a_wrong_key_is_a_format_error_not_ignored()
    {
        var (catalog, key, _) = await OneFileAsync();
        await CommitAsync(catalog, tx => tx.DeleteAsync("main", "t", Expr.Equal("id", 3L)));
        var deleteKey = (string)(await _lake.SqlAsync("SELECT encryption_key FROM ducklake_delete_file"))[0][0]!;
        await _lake.ExecAsync($"UPDATE ducklake_delete_file SET encryption_key = '{key}'"); // the data file's key
        await AssertFormatErrorAsync(catalog, key, deleteKey);
        await _lake.ExecAsync("UPDATE ducklake_delete_file SET encryption_key = NULL");
        await AssertFormatErrorAsync(catalog, key, deleteKey);
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
