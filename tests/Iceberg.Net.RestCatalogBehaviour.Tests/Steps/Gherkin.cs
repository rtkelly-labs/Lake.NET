using System.Globalization;
using System.Text.RegularExpressions;
using Iceberg.Net.Types;
using Reqnroll;
using Xunit;

namespace Iceberg.Net.RestCatalogBehaviour.Tests.Steps;

/// <summary>Reading Gherkin arguments: quoted lists, key/value tables and row tables.</summary>
public static partial class Gherkin
{
    [GeneratedRegex("\"([^\"]*)\"")]
    private static partial Regex Quoted();

    /// <summary><c>"a", "b" and "c"</c> → [a, b, c]; <c>none</c> → [].</summary>
    public static List<string> List(string text) =>
        text.Trim() == "none" ? [] : Quoted().Matches(text).Select(m => m.Groups[1].Value).ToList();

    public static Dictionary<string, string> Properties(DataTable table) =>
        table.Rows.ToDictionary(r => r["key"], r => r["value"]);

    /// <summary>A row table as rows of <paramref name="schema"/>: columns by name, absent ones null, <c>&lt;null&gt;</c> null.</summary>
    public static List<object?[]> Rows(DataTable table, Schema schema)
    {
        foreach (var header in table.Header)
            Assert.True(schema.FindField(header) is not null, $"The table has no column '{header}'");
        return table.Rows.Select(r => schema.Columns.Select(c => table.Header.Contains(c.Name) ? Parse(r[c.Name], c.Type) : null).ToArray()).ToList();
    }

    private static object? Parse(string text, IcebergType type) =>
        text == "<null>" ? null
        : type.Equals(PrimitiveType.Long) ? long.Parse(text, CultureInfo.InvariantCulture)
        : type.Equals(PrimitiveType.Int) ? int.Parse(text, CultureInfo.InvariantCulture)
        : text;

    /// <summary>Rows rendered for comparison: the named columns only, in table order, sorted.</summary>
    public static List<string> Render(IEnumerable<object?[]> rows, Schema schema, IReadOnlyList<string> columns)
    {
        var positions = columns.Select(c => schema.Columns.ToList().FindIndex(f => f.Name == c)).ToList();
        Assert.DoesNotContain(-1, positions);
        return rows.Select(r => string.Join(" | ", positions.Select(p => r[p] is { } v ? Convert.ToString(v, CultureInfo.InvariantCulture) : "<null>")))
            .Order(StringComparer.Ordinal).ToList();
    }

    public static List<string> Render(DataTable table) =>
        table.Rows.Select(r => string.Join(" | ", table.Header.Select(h => r[h]))).Order(StringComparer.Ordinal).ToList();
}
