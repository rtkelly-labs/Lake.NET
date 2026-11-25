using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using Iceberg.Net.IO;

namespace Lake.Delta.Tests;

/// <summary>Committed fixtures (read in place) and scratch copies of them for tests that change a table.</summary>
internal static class Fixtures
{
    public static string RepositoryRoot { get; } = FindRoot();

    private static string FindRoot()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
            if (File.Exists(Path.Combine(dir.FullName, "Iceberg.Net.slnx"))) return dir.FullName;
        throw new InvalidOperationException("Iceberg.Net.slnx not found above " + AppContext.BaseDirectory);
    }

    public static string DatRoot => Path.Combine(RepositoryRoot, "tests", "Lake.Delta.Tests", "fixtures", "dat");

    /// <summary>The Delta table of a DAT case.</summary>
    public static string Dat(string name) => Path.Combine(DatRoot, name, "delta");

    /// <summary>delta-rs's table-with-dv-small (Spark/Delta-written), shared with Iceberg.Net.Spec.Tests.</summary>
    public static string DeltaRsDvSmall => Path.Combine(RepositoryRoot, "tests", "Iceberg.Net.Spec.Tests", "fixtures", "delta-rs", "table-with-dv-small");

    /// <summary>A private copy of a fixture table to change.</summary>
    public static string CopyToTemp(string table)
    {
        var target = Path.Combine(Path.GetTempPath(), "lake-delta-tests", Guid.NewGuid().ToString("N"));
        foreach (var file in Directory.EnumerateFiles(table, "*", SearchOption.AllDirectories))
        {
            var to = Path.Combine(target, Path.GetRelativePath(table, file));
            Directory.CreateDirectory(Path.GetDirectoryName(to)!);
            File.Copy(file, to);
        }
        return target;
    }

    public static string EmptyTable()
    {
        var target = Path.Combine(Path.GetTempPath(), "lake-delta-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path.Combine(target, "_delta_log"));
        return target;
    }

    public static string CommitPath(string table, long version) =>
        Path.Combine(table, "_delta_log", version.ToString("D20", CultureInfo.InvariantCulture) + ".json");

    public static void WriteCommit(string table, long version, params string[] lines) =>
        File.WriteAllText(CommitPath(table, version), string.Join("\n", lines) + "\n");

    /// <summary>Rewrite each JSON line of a commit (return null to drop the line).</summary>
    public static void EditCommit(string table, long version, Func<JsonObject, JsonObject?> edit)
    {
        var path = CommitPath(table, version);
        var lines = File.ReadAllLines(path).Where(l => l.Length > 0)
            .Select(l => edit(JsonNode.Parse(l)!.AsObject())).Where(o => o is not null).Select(o => o!.ToJsonString());
        File.WriteAllText(path, string.Join("\n", lines) + "\n");
    }

    public static async Task<List<object?[]>> ReadAllAsync(DeltaTable table, long? version = null)
    {
        var rows = new List<object?[]>();
        await foreach (var row in table.ReadAsync(version)) rows.Add(row);
        return rows;
    }

    public static async Task<List<object?[]>> ReadAllAsync(DeltaSnapshot snapshot)
    {
        var rows = new List<object?[]>();
        await foreach (var row in snapshot.ReadAsync()) rows.Add(row);
        return rows;
    }

    /// <summary>
    /// The rows of standalone Parquet files (DAT's expected output, written by Spark with display names) read into
    /// <paramref name="schema"/> by name.
    /// </summary>
    public static async Task<List<object?[]>> ReadParquetAsync(IEnumerable<string> files, DeltaStructType schema)
    {
        var rows = new List<object?[]>();
        var io = new LocalFileIO();
        var none = new bool[schema.Fields.Count];
        foreach (var file in files)
            await foreach (var row in DataFileReader.ReadAsync(io, file, schema, ColumnMappingMode.None, new object?[schema.Fields.Count], none, null))
                rows.Add(row);
        return rows;
    }
}

/// <summary>
/// A canonical text form of a row, typed (<c>long:5</c> differs from <c>int:5</c>), for order-independent comparison.
/// Decimals compare by value (12.000 = 12), maps by sorted entries.
/// </summary>
internal static class Canon
{
    public static string Row(object?[] row) => "(" + string.Join(", ", row.Select(Value)) + ")";

    public static List<string> Rows(IEnumerable<object?[]> rows) => rows.Select(Row).Order(StringComparer.Ordinal).ToList();

    public static string Value(object? v) => v switch
    {
        null => "null",
        string s => "string:" + System.Text.Json.JsonSerializer.Serialize(s),
        byte[] b => "binary:0x" + Convert.ToHexString(b),
        bool b => "bool:" + (b ? "true" : "false"),
        sbyte n => "byte:" + n.ToString(CultureInfo.InvariantCulture),
        short n => "short:" + n.ToString(CultureInfo.InvariantCulture),
        int n => "int:" + n.ToString(CultureInfo.InvariantCulture),
        long n => "long:" + n.ToString(CultureInfo.InvariantCulture),
        float f => "float:" + f.ToString("R", CultureInfo.InvariantCulture),
        double d => "double:" + d.ToString("R", CultureInfo.InvariantCulture),
        decimal m => "decimal:" + (m / 1.0000000000000000000000000000m).ToString(CultureInfo.InvariantCulture),
        DateOnly d => "date:" + d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        DateTimeOffset t => "timestamp:" + t.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture),
        DateTime t => "timestamp_ntz:" + t.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + (t.Kind == DateTimeKind.Unspecified ? "" : "!" + t.Kind),
        object?[] s => "{" + string.Join(", ", s.Select(Value)) + "}",
        Dictionary<object, object?> m => "map{" + string.Join(", ", m.Select(kv => Value(kv.Key) + "=" + Value(kv.Value)).Order(StringComparer.Ordinal)) + "}",
        List<object?> l => "[" + string.Join(", ", l.Select(Value)) + "]",
        _ => throw new InvalidOperationException("Unexpected value type " + v.GetType()),
    };

    public static string Describe(IReadOnlyList<string> rows) => new StringBuilder().AppendJoin('\n', rows).ToString();
}
