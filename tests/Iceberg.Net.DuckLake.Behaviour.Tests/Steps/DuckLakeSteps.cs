using System.Globalization;
using System.Text.RegularExpressions;
using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;
using Reqnroll;
using Xunit;
using DataTable = Reqnroll.Table;

namespace Iceberg.Net.DuckLake.Behaviour.Tests.Steps;

/// <summary>Per-scenario state (Reqnroll context injection gives every scenario a fresh instance).</summary>
public sealed class DuckLakeWorld : IDisposable
{
    public TestLake? Lake { get; private set; }
    public DuckLakeCatalog? Catalog { get; set; }
    public DuckLakeSnapshot? LastSnapshot { get; set; }
    public Exception? Error { get; set; }
    public long? Affected { get; set; }
    /// <summary>Transactions started but not yet committed, by writer name ("A", "B").</summary>
    public Dictionary<string, DuckLakeTransaction> Writers { get; } = [];

    public TestLake L => Lake ?? throw new InvalidOperationException("No lake yet: start with 'Given a DuckLake on ...'");
    public DuckLakeCatalog C => Catalog ?? throw new InvalidOperationException("No catalog yet");
    public DuckLakeOracle Oracle => DuckLakeOracle.For(L.Version);

    public void Use(CatalogBackend backend, string version) => Lake = new TestLake(backend, version);

    public void Dispose()
    {
        foreach (var w in Writers.Values) w.DisposeAsync().AsTask().GetAwaiter().GetResult();
        Lake?.Dispose();
    }
}

[Binding]
public sealed class Hooks(DuckLakeWorld world)
{
    /// <summary>Whatever a scenario did, failed writes included, the lake must still pass a deep validation.</summary>
    [AfterScenario]
    public async Task LakeIsStillValid()
    {
        if (world.Lake is null || world.Catalog is null) return;
        await world.Lake.ValidateAsync();
    }
}

[Binding]
public sealed class DuckLakeSteps(DuckLakeWorld world)
{
    // ---- setup -----------------------------------------------------------------------------------------------------

    [Given(@"a DuckLake on (SQLite|PostgreSQL) in format (1\.0|1\.1)")]
    public async Task GivenALake(string catalog, string version)
    {
        world.Use(catalog == "SQLite" ? CatalogBackend.Sqlite : CatalogBackend.Postgres, version);
        world.Catalog = await world.L.CreateAsync();
    }

    [Given(@"a DuckLake that DuckDB created on (SQLite|PostgreSQL) in format (1\.0|1\.1)")]
    public void GivenADuckDbLake(string catalog, string version)
    {
        world.Use(catalog == "SQLite" ? CatalogBackend.Sqlite : CatalogBackend.Postgres, version);
        world.Oracle.Require();
    }

    [Given(@"(?:a|the) table ""(.*)"" with columns:")]
    public async Task GivenATable(string name, DataTable columns)
    {
        var (schema, table) = Split(name);
        await Commit(tx => tx.CreateTableAsync(schema, table, columns.Rows.Select(r =>
            new DuckLakeColumnDefinition(r["name"], ParseType(r["type"]), !r.TryGetValue("nullable", out var n) || n != "no")).ToList()));
    }

    [Given(@"the schema ""(.*)""")]
    [When(@"I create the schema ""(.*)""")]
    public Task CreateSchema(string name) => Try(() => Commit(tx => tx.CreateSchemaAsync(name)));

    [Given(@"""(.*)"" is partitioned by:")]
    public Task Partition(string name, DataTable keys)
    {
        var (schema, table) = Split(name);
        return Commit(tx => tx.SetPartitioningAsync(schema, table, keys.Rows.Select(r => (r["column"], r["transform"])).ToList()));
    }

    // ---- writes ----------------------------------------------------------------------------------------------------

    [Given(@"I inserted into ""(.*)"":")]
    [When(@"I insert into ""(.*)"":")]
    public async Task Insert(string name, DataTable rows)
    {
        var (schema, table) = Split(name);
        var t = await world.C.LoadTableAsync(schema, table);
        await Try(() => Commit(tx => tx.InsertAsync(schema, table, ParseRows(t, rows))));
    }

    [When(@"I delete from ""(.*)"" where (.*)")]
    public async Task Delete(string name, string condition)
    {
        var (schema, table) = Split(name);
        var t = await world.C.LoadTableAsync(schema, table);
        await Try(() => Commit(async tx => world.Affected = await tx.DeleteAsync(schema, table, ParseFilter(t, condition))));
    }

    [When(@"I update ""(.*)"" setting ""(.*)"" to ""(.*)"" where (.*)")]
    public async Task Update(string name, string column, string value, string condition)
    {
        var (schema, table) = Split(name);
        var t = await world.C.LoadTableAsync(schema, table);
        var index = t.Columns.ToList().FindIndex(c => c.Name == column);
        var newValue = ParseValue(t.Columns[index].Type, value);
        await Try(() => Commit(async tx => world.Affected = await tx.UpdateAsync(schema, table, ParseFilter(t, condition), r =>
        {
            r[index] = newValue;
            return r;
        })));
    }

    [When(@"I add the column ""(.*)"" of type ""(.*)"" to ""(.*)""")]
    public Task AddColumn(string column, string type, string name)
    {
        var (schema, table) = Split(name);
        return Try(() => Commit(tx => tx.AddColumnAsync(schema, table, new DuckLakeColumnDefinition(column, ParseType(type)))));
    }

    [When(@"I drop the column ""(.*)"" from ""(.*)""")]
    public Task DropColumn(string column, string name)
    {
        var (schema, table) = Split(name);
        return Try(() => Commit(tx => tx.DropColumnAsync(schema, table, column)));
    }

    [When(@"I rename the column ""(.*)"" of ""(.*)"" to ""(.*)""")]
    public Task RenameColumn(string column, string name, string newName)
    {
        var (schema, table) = Split(name);
        return Try(() => Commit(tx => tx.RenameColumnAsync(schema, table, column, newName)));
    }

    [When(@"I change the type of ""(.*)"" in ""(.*)"" to ""(.*)""")]
    public Task AlterType(string column, string name, string type)
    {
        var (schema, table) = Split(name);
        return Try(() => Commit(tx => tx.AlterColumnTypeAsync(schema, table, column, (DuckLakePrimitive)ParseType(type))));
    }

    [When(@"I drop the table ""(.*)""")]
    public Task DropTable(string name)
    {
        var (schema, table) = Split(name);
        return Try(() => Commit(tx => tx.DropTableAsync(schema, table)));
    }

    [When(@"I drop the schema ""(.*)""")]
    public Task DropSchema(string name) => Try(() => Commit(tx => tx.DropSchemaAsync(name)));

    // ---- concurrent writers ----------------------------------------------------------------------------------------

    [When(@"writer (\w+) stages an insert into ""(.*)"":")]
    public async Task StageInsert(string writer, string name, DataTable rows)
    {
        var (schema, table) = Split(name);
        var t = await world.C.LoadTableAsync(schema, table);
        await Writer(writer).InsertAsync(schema, table, ParseRows(t, rows));
    }

    [When(@"writer (\w+) stages a delete from ""(.*)"" where (.*)")]
    public async Task StageDelete(string writer, string name, string condition)
    {
        var (schema, table) = Split(name);
        var t = await world.C.LoadTableAsync(schema, table);
        await Writer(writer).DeleteAsync(schema, table, ParseFilter(t, condition));
    }

    [When(@"writer (\w+) stages the table ""(.*)""")]
    public Task StageTable(string writer, string name)
    {
        var (schema, table) = Split(name);
        return Writer(writer).CreateTableAsync(schema, table, [new("id", DuckLakeType.Int64)]);
    }

    [When(@"writer (\w+) commits")]
    public async Task CommitWriter(string writer)
    {
        world.Error = null;
        try
        {
            world.LastSnapshot = await world.Writers[writer].CommitAsync();
        }
        catch (Exception e) when (e is DuckLakeConflictException or InvalidOperationException)
        {
            world.Error = e;
        }
        finally
        {
            world.Writers.Remove(writer);
        }
    }

    private DuckLakeTransaction Writer(string name) =>
        world.Writers.TryGetValue(name, out var tx) ? tx : world.Writers[name] = world.C.BeginTransaction(author: name);

    // ---- the other engine ------------------------------------------------------------------------------------------

    [When(@"DuckDB runs:")]
    [Given(@"DuckDB ran:")]
    public async Task DuckDbRuns(string sql)
    {
        world.Oracle.Require();
        var statements = sql.Split(";\n", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(s => s.TrimEnd(';')).ToArray();
        await world.Oracle.RunAsync(world.L, statements);
        world.Catalog = await world.L.OpenAsync();
    }

    [Then(@"DuckDB reads the same rows from ""(.*)""")]
    public async Task DuckDbReadsTheSame(string name)
    {
        world.Oracle.Require();
        var t = await LoadAsync(name);
        LakeJson.AssertSameRows(await world.Oracle.RowsAsync(world.L, $"SELECT * FROM lake.{Quoted(name)}"), LakeJson.Rows(t.Columns, await world.C.ReadAsync(t)));
    }

    [Then(@"DuckDB reads the same rows from ""(.*)"" at every snapshot")]
    public async Task DuckDbReadsTheSameEverywhere(string name)
    {
        world.Oracle.Require();
        var (schema, table) = Split(name);
        foreach (var s in await world.C.ListSnapshotsAsync())
        {
            DuckLakeTable t;
            try { t = await world.C.LoadTableAsync(schema, table, s.SnapshotId); }
            catch (DuckLakeNotFoundException) { continue; }
            LakeJson.AssertSameRows(await world.Oracle.RowsAsync(world.L, $"SELECT * FROM lake.{Quoted(name)} AT (VERSION => {s.SnapshotId})"),
                LakeJson.Rows(t.Columns, await world.C.ReadAsync(t)));
        }
    }

    [Then(@"DuckDB answers ""(.*)"" with ""(.*)""")]
    public async Task DuckDbAnswers(string sql, string expected)
    {
        world.Oracle.Require();
        Assert.Equal([$"[{expected}]"], await world.Oracle.RowsAsync(world.L, sql));
    }

    // ---- outcomes --------------------------------------------------------------------------------------------------

    [Then(@"""(.*)"" contains exactly:")]
    public async Task ContainsExactly(string name, DataTable expected) => await AtSnapshot(name, null, expected);

    [Then(@"""(.*)"" at snapshot (\d+) contains exactly:")]
    public Task AtSnapshotContains(string name, long snapshot, DataTable expected) => AtSnapshot(name, snapshot, expected);

    [Then(@"""(.*)"" filtered by (.*) reads (\d+) files? and contains exactly:")]
    public async Task FilteredContains(string name, string condition, int files, DataTable expected)
    {
        var t = await LoadAsync(name);
        var filter = ParseFilter(t, condition);
        Assert.Equal(files, (await world.C.PlanScanAsync(t, filter)).Files.Count);
        LakeJson.AssertSameRows(Expected(t, expected), LakeJson.Rows(t.Columns, await world.C.ReadAsync(t, filter)));
    }

    private async Task AtSnapshot(string name, long? snapshot, DataTable expected)
    {
        var (schema, table) = Split(name);
        var t = await world.C.LoadTableAsync(schema, table, snapshot);
        Assert.Equal(expected.Header, t.Columns.Select(c => c.Name));
        LakeJson.AssertSameRows(Expected(t, expected), LakeJson.Rows(t.Columns, await world.C.ReadAsync(t)));
    }

    [Then(@"""(.*)"" has (\d+) live data files?")]
    public async Task LiveFiles(string name, int count) => Assert.Equal(count, (await world.C.PlanScanAsync(await LoadAsync(name))).Files.Count);

    [Then(@"the last snapshot records the changes ""(.*)""")]
    public async Task Changes(string changes) => Assert.Equal(changes, string.Join(",", (await world.C.CurrentSnapshotAsync()).Changes));

    [Then(@"(\d+) rows? (?:was|were) affected")]
    public void RowsAffected(long n) => Assert.Equal(n, world.Affected);

    [Then(@"the write fails with ""(.*)""")]
    [Then(@"the commit fails with ""(.*)""")]
    public void Fails(string message)
    {
        Assert.NotNull(world.Error);
        Assert.Contains(message, world.Error!.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Then(@"the commit succeeds")]
    public void Succeeds() => Assert.Null(world.Error);

    [Then(@"the schemas are ""(.*)""")]
    public async Task Schemas(string names) => Assert.Equal(names.Split(", "), await world.C.ListSchemasAsync());

    [Then(@"the partition values of ""(.*)"" are:")]
    public async Task PartitionValues(string name, DataTable expected)
    {
        var t = await LoadAsync(name);
        var rows = await world.L.SqlAsync($"SELECT v.data_file_id, v.partition_key_index, v.partition_value FROM ducklake_file_partition_value v " +
                                          $"WHERE v.table_id = {t.TableId} ORDER BY v.data_file_id, v.partition_key_index");
        var byFile = rows.GroupBy(r => (long)r[0]!).Select(g => string.Join("/", g.Select(r => r[2] as string ?? "null"))).Order().ToList();
        Assert.Equal(expected.Rows.Select(r => r["values"]).Order(), byFile);
    }

    // ---- helpers ---------------------------------------------------------------------------------------------------

    private async Task Commit(Func<DuckLakeTransaction, Task> stage)
    {
        await using var tx = world.C.BeginTransaction();
        await stage(tx);
        world.LastSnapshot = await tx.CommitAsync();
    }

    private async Task Try(Func<Task> action)
    {
        world.Error = null;
        try
        {
            await action();
        }
        catch (Exception e) when (e is InvalidOperationException or NotSupportedException or ArgumentException or DuckLakeConflictException or KeyNotFoundException)
        {
            world.Error = e;
        }
    }

    private Task<DuckLakeTable> LoadAsync(string name)
    {
        var (schema, table) = Split(name);
        return world.C.LoadTableAsync(schema, table);
    }

    /// <summary><c>t</c> is <c>main.t</c>; otherwise everything before the last dot is the schema path.</summary>
    private static (string Schema, string Table) Split(string name) =>
        name.LastIndexOf('.') is var i and > 0 ? (name[..i], name[(i + 1)..]) : ("main", name);

    private static string Quoted(string name)
    {
        var (schema, table) = Split(name);
        return string.Join('.', schema.Split('.').Append(table).Select(p => $"\"{p}\""));
    }

    /// <summary>
    /// A DuckLake type as written in a feature: a primitive (<c>int64</c>, <c>decimal(10,2)</c>), <c>list(T)</c>,
    /// <c>map(K, V)</c> or <c>struct(a T, b U)</c>.
    /// </summary>
    internal static DuckLakeType ParseType(string text)
    {
        text = text.Trim();
        var open = text.IndexOf('(');
        if (open < 0 || text.StartsWith("decimal", StringComparison.Ordinal)) return new DuckLakePrimitive(text);
        var kind = text[..open];
        var args = SplitTop(text[(open + 1)..^1]);
        return kind switch
        {
            "list" => DuckLakeType.List(ParseType(args[0])),
            "map" => DuckLakeType.Map(ParseType(args[0]), ParseType(args[1])),
            "struct" => DuckLakeType.Struct(args.Select(a => a.Trim().Split(' ', 2)).Select(p => (p[0], ParseType(p[1]))).ToArray()),
            _ => throw new ArgumentException($"Unknown type {text}"),
        };
    }

    private static List<string> SplitTop(string s)
    {
        var parts = new List<string>();
        var depth = 0;
        var start = 0;
        for (var i = 0; i < s.Length; i++)
        {
            if (s[i] == '(') depth++;
            else if (s[i] == ')') depth--;
            else if (s[i] == ',' && depth == 0)
            {
                parts.Add(s[start..i].Trim());
                start = i + 1;
            }
        }
        parts.Add(s[start..].Trim());
        return parts;
    }

    /// <summary>A cell as a value of the column: <c>null</c>, text in the statistics encoding, or a nested value in DuckDB's text form.</summary>
    internal static object? ParseValue(DuckLakeType type, string cell)
    {
        if (cell == "null") return null;
        if (type is not DuckLakePrimitive p) return DuckLakeNestedText.Parse(type, cell);
        if (p.Name is "varchar" or "json") return cell;
        return DuckLakeValues.TryParse(p, cell, out var v) ? v : throw new ArgumentException($"'{cell}' is not a {p}");
    }

    private static List<object?[]> ParseRows(DuckLakeTable t, DataTable rows) =>
        rows.Rows.Select(r => t.Columns.Select(c => r.TryGetValue(c.Name, out var cell) ? ParseValue(c.Type, cell) : null).ToArray()).ToList();

    private static List<string> Expected(DuckLakeTable t, DataTable rows) => LakeJson.Rows(t.Columns, ParseRows(t, rows));

    /// <summary><c>"col" op value</c>, joined by <c>and</c>: <c>"id" &gt;= 10 and "region" = eu</c>; also <c>"col" is null</c>.</summary>
    private static Expression ParseFilter(DuckLakeTable t, string condition)
    {
        var result = Expression.True;
        foreach (var part in Regex.Split(condition, @"\s+and\s+"))
        {
            var m = Regex.Match(part.Trim(), "^\"(?<col>[^\"]+)\"\\s*(?<op>>=|<=|!=|=|<|>|is not null|is null)\\s*(?<value>.*)$");
            if (!m.Success) throw new ArgumentException($"Cannot read the condition '{part}'");
            var column = t.Column(m.Groups["col"].Value);
            var value = m.Groups["value"].Value.Trim().Trim('"');
            object Literal() => ParseValue(column.Type, value) ?? throw new ArgumentException("Compare with null using 'is null'");
            Expression e = m.Groups["op"].Value switch
            {
                "=" => Expr.Equal(column.Name, Literal()),
                "!=" => Expr.NotEqual(column.Name, Literal()),
                "<" => Expr.LessThan(column.Name, Literal()),
                "<=" => Expr.LessThanOrEqual(column.Name, Literal()),
                ">" => Expr.GreaterThan(column.Name, Literal()),
                ">=" => Expr.GreaterThanOrEqual(column.Name, Literal()),
                "is null" => Expr.IsNull(column.Name),
                _ => Expr.NotNull(column.Name),
            };
            result = Expr.And(result, e);
        }
        return result;
    }
}
