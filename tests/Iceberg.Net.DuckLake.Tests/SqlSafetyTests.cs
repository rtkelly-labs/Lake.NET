using System.Text;
using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;
using Npgsql;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// SQL built from catalog rows and user names (gap S11): inlined-table names read from the catalog must be the name
/// their row gives before anything selects from, deletes from or drops them; DuckDB's PostgreSQL ATTACH string quotes its
/// values; names with quotes, semicolons and Unicode round-trip everywhere a name goes.
/// </summary>
public abstract class SqlSafetyTests : IAsyncLifetime
{
    private readonly TestLake _lake;

    protected SqlSafetyTests(CatalogBackend backend, string version) => _lake = new TestLake(backend, version);

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        try { await _lake.ValidateAsync(); }
        finally { _lake.Dispose(); }
    }

    private static async Task CommitAsync(DuckLakeCatalog catalog, Func<DuckLakeTransaction, Task> work)
    {
        await using var tx = catalog.BeginTransaction();
        await work(tx);
        await tx.CommitAsync();
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_inlined_data_tables")]
    public async Task An_inlined_table_row_naming_a_catalog_table_is_never_dropped_or_deleted_from()
    {
        var catalog = await _lake.CreateAsync(new DuckLakeOptions { DataInliningRowLimit = 10 });
        await CommitAsync(catalog, tx => tx.CreateTableAsync("main", "gone", [new("id", DuckLakeType.Int64)]));
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "gone", [[1L]]));
        await CommitAsync(catalog, tx => tx.DropTableAsync("main", "gone"));
        _lake.Corrupted = true;
        await _lake.ExecAsync("UPDATE ducklake_inlined_data_tables SET table_name = 'ducklake_snapshot'");
        var snapshots = (await _lake.SqlAsync("SELECT count(*) FROM ducklake_snapshot"))[0][0];

        // Expiry would drop the dropped table's inlined table, and delete expired rows from every inlined table.
        await Assert.ThrowsAsync<InvalidDataException>(() => catalog.ExpireSnapshotsAsync(DateTimeOffset.UtcNow.AddMinutes(1)));
        Assert.Equal(snapshots, (await _lake.SqlAsync("SELECT count(*) FROM ducklake_snapshot"))[0][0]);
        Assert.Equal(2L, await _lake.TableCountAsync("ducklake_snapshot")); // ducklake_snapshot and ducklake_snapshot_changes
    }

    [SkippableFact]
    [Spec("ducklake: Read Inlined Data")]
    public async Task Reads_and_flushes_refuse_an_inlined_table_row_naming_another_table()
    {
        var catalog = await _lake.CreateAsync(new DuckLakeOptions { DataInliningRowLimit = 10 });
        await CommitAsync(catalog, tx => tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64)]));
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[1L]]));
        _lake.Corrupted = true;
        await _lake.ExecAsync("UPDATE ducklake_inlined_data_tables SET table_name = 'ducklake_metadata'");
        await Assert.ThrowsAsync<InvalidDataException>(async () => await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t")));
        await Assert.ThrowsAsync<InvalidDataException>(() => catalog.FlushInlinedDataAsync());
        Assert.NotEmpty(await _lake.SqlAsync("SELECT * FROM ducklake_metadata"));
    }

    public static TheoryData<int> Inlining => [0, 10];

    [SkippableTheory]
    [MemberData(nameof(Inlining))]
    [Spec("ducklake: ducklake_snapshot_changes")]
    [Spec("ducklake: ducklake_tag")]
    [Spec("ducklake: ducklake_column_tag")]
    public async Task Names_with_quotes_semicolons_and_unicode_round_trip(int inlining)
    {
        const string schema = "s\"q'l;ü 表";
        const string table = "t\"a'b; DROP TABLE ducklake_snapshot;--€";
        const string column = "c\"o'l;λ";
        const string quote = "\"";
        const string tagKey = "k\"'; ✓";
        const string tagValue = "v\"'); DELETE FROM ducklake_snapshot;-- ✓";
        const string comment = "it's \"quoted\"; ok ✓";
        var catalog = await _lake.CreateAsync(new DuckLakeOptions { DataInliningRowLimit = inlining });
        await CommitAsync(catalog, tx => tx.CreateSchemaAsync(schema));
        await CommitAsync(catalog, tx => tx.CreateTableAsync(schema, table, [new(column, DuckLakeType.Int64), new(quote, DuckLakeType.Varchar), new("'", DuckLakeType.Varchar)]));
        await CommitAsync(catalog, async tx =>
        {
            await tx.SetTagAsync(schema, table, tagKey, tagValue);
            await tx.SetTagAsync(schema, table, "comment", comment);
            await tx.SetColumnTagAsync(schema, table, column, tagKey, tagValue);
            await tx.SetColumnTagAsync(schema, table, quote, "comment", comment);
        });
        await CommitAsync(catalog, tx => tx.InsertAsync(schema, table, [[1L, "a'\"", "x;y"], [2L, "b", "ü"], [3L, null, "--"]]));
        await CommitAsync(catalog, tx => tx.DeleteAsync(schema, table, Expr.Equal(column, 2L)));
        await CommitAsync(catalog, tx => tx.UpdateAsync(schema, table, Expr.Equal(column, 3L), r => [r[0], "updated'", r[2]]));
        if (inlining > 0) await catalog.FlushInlinedDataAsync(schema, table);

        var reopened = await _lake.OpenAsync();
        Assert.Contains(schema, await reopened.ListSchemasAsync());
        Assert.Equal([table], await reopened.ListTablesAsync(schema));
        var loaded = await reopened.LoadTableAsync(schema, table);
        Assert.Equal([column, quote, "'"], loaded.Columns.Select(c => c.Name));
        Assert.Equal(tagValue, loaded.Tags[tagKey]);
        Assert.Equal(comment, loaded.Comment);
        Assert.Equal(tagValue, loaded.ColumnTags[loaded.Column(column).Id][tagKey]);
        Assert.Equal(comment, loaded.ColumnComment(quote));
        Assert.Equal(["1|a'\"|x;y", "3|updated'|--"],
            (await reopened.ReadAsync(loaded)).Select(r => string.Join("|", r)).Order(StringComparer.Ordinal));
        Assert.Equal(["1"], (await reopened.ReadAsync(loaded, Expr.Equal(quote, "a'\""))).Select(r => r[0]!.ToString()));

        // changes_made names them in the spec's quoted form, and parses back to the same names.
        var created = (await reopened.ListSnapshotsAsync()).SelectMany(s => s.Changes).Where(c => c.Kind is DuckLakeChange.CreatedSchema or DuckLakeChange.CreatedTable).ToList();
        Assert.Contains(created, c => c.Kind == DuckLakeChange.CreatedSchema && DuckLakeChange.SplitName(c.Target).SequenceEqual([schema]));
        Assert.Contains(created, c => c.Kind == DuckLakeChange.CreatedTable && DuckLakeChange.SplitName(c.Target).SequenceEqual([schema, table]));
        Assert.NotEmpty(await _lake.SqlAsync("SELECT * FROM ducklake_snapshot"));
    }
}

public sealed class SqliteSqlSafetyTests10() : SqlSafetyTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion10);
public sealed class SqliteSqlSafetyTests11() : SqlSafetyTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion11);
public sealed class PostgresSqlSafetyTests11() : SqlSafetyTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion11);

/// <summary>The PostgreSQL ATTACH string DuckDB is given, parsed back as libpq parses it.</summary>
public sealed class DuckDbAttachTests
{
    [Theory]
    [InlineData("postgres")]
    [InlineData("pa ss")]
    [InlineData("it's")]
    [InlineData("a;b")]
    [InlineData("x=y")]
    [InlineData("back\\slash")]
    [InlineData("a b';c\\d= 'e ü")]
    [InlineData("' host=evil password='")]
    public void A_password_with_quotes_semicolons_and_spaces_round_trips(string password)
    {
        var cs = new NpgsqlConnectionStringBuilder { Host = "db.example", Port = 5433, Database = "my lake", Username = "o'brien", Password = password }.ConnectionString;
        var attach = DuckLakeStore.PostgreSql(cs).DuckDbAttach();
        Assert.StartsWith("ducklake:postgres:", attach, StringComparison.Ordinal);
        var parsed = ParseLibpq(attach["ducklake:postgres:".Length..]);
        Assert.Equal(new Dictionary<string, string>
        {
            ["dbname"] = "my lake", ["host"] = "db.example", ["port"] = "5433", ["user"] = "o'brien", ["password"] = password,
        }, parsed);
    }

    [Fact]
    public void Plain_values_are_written_bare_as_before() =>
        Assert.Equal("ducklake:postgres:dbname=lake host=localhost port=5434 user=postgres password=postgres",
            DuckLakeStore.PostgreSql("Host=localhost;Port=5434;Database=lake;Username=postgres;Password=postgres").DuckDbAttach());

    /// <summary>libpq's keyword/value parser ("Keyword/Value Connection Strings"): single quotes, backslash escapes.</summary>
    private static Dictionary<string, string> ParseLibpq(string s)
    {
        var result = new Dictionary<string, string>();
        var i = 0;
        while (true)
        {
            while (i < s.Length && char.IsWhiteSpace(s[i])) i++;
            if (i >= s.Length) return result;
            var start = i;
            while (i < s.Length && s[i] != '=' && !char.IsWhiteSpace(s[i])) i++;
            var keyword = s[start..i];
            while (i < s.Length && char.IsWhiteSpace(s[i])) i++;
            if (i >= s.Length || s[i] != '=') throw new FormatException($"missing '=' after '{keyword}' in: {s}");
            i++;
            while (i < s.Length && char.IsWhiteSpace(s[i])) i++;
            var value = new StringBuilder();
            if (i < s.Length && s[i] == '\'')
            {
                i++;
                while (true)
                {
                    if (i >= s.Length) throw new FormatException($"unterminated quoted value in: {s}");
                    if (s[i] == '\\') { value.Append(s[i + 1]); i += 2; continue; }
                    if (s[i] == '\'') { i++; break; }
                    value.Append(s[i++]);
                }
            }
            else
            {
                while (i < s.Length && !char.IsWhiteSpace(s[i]))
                {
                    if (s[i] == '\\') i++;
                    value.Append(s[i++]);
                }
            }
            if (!result.TryAdd(keyword, value.ToString())) throw new FormatException($"'{keyword}' given twice in: {s}");
        }
    }
}
