using System.Collections;
using System.Globalization;
using System.Text;
using Iceberg.Net.Catalog;

namespace Iceberg.Net.Spark;

/// <summary>
/// Spark SQL quoting. Identifiers are always back-quoted (a back-quote inside is doubled), so reserved words, dots and
/// any other character are safe; string literals are single-quoted with backslash and quote escaped (Spark's default
/// <c>spark.sql.parser.escapedStringLiterals=false</c> unescapes them).
/// </summary>
public static class SparkSql
{
    /// <summary><c>`name`</c>.</summary>
    public static string Identifier(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return "`" + name.Replace("`", "``", StringComparison.Ordinal) + "`";
    }

    /// <summary><c>`a`.`b`.`c`</c>.</summary>
    public static string Identifier(IEnumerable<string> parts) => string.Join(".", parts.Select(Identifier));

    /// <summary>A string literal: <c>'it\'s'</c>.</summary>
    public static string String(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        var sb = new StringBuilder(value.Length + 2).Append('\'');
        foreach (var c in value)
        {
            switch (c)
            {
                case '\\': sb.Append(@"\\"); break;
                case '\'': sb.Append(@"\'"); break;
                case '\n': sb.Append(@"\n"); break;
                case '\r': sb.Append(@"\r"); break;
                case '\t': sb.Append(@"\t"); break;
                case '\0': sb.Append(@"\0"); break;
                default: sb.Append(c); break;
            }
        }
        return sb.Append('\'').ToString();
    }

    /// <summary>
    /// A timestamp literal pinned to UTC (<c>TIMESTAMP '2026-10-01 12:00:00.000000Z'</c>), so the session time zone does
    /// not shift it.
    /// </summary>
    public static string Timestamp(DateTimeOffset instant) =>
        "TIMESTAMP '" + instant.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "Z'";

    /// <summary><c>map('k1', 'v1', 'k2', 'v2')</c>, keys in ordinal order so the text is deterministic.</summary>
    public static string Map(IEnumerable<KeyValuePair<string, string>> entries) =>
        "map(" + string.Join(", ", entries.OrderBy(e => e.Key, StringComparer.Ordinal).Select(e => String(e.Key) + ", " + String(e.Value))) + ")";

    /// <summary>
    /// Any supported value as a literal: string, bool, integral and floating types, decimal, <see cref="DateTimeOffset"/>
    /// (a UTC timestamp), string maps, and sequences (<c>array(…)</c>). A <see cref="SparkSqlFragment"/> is inserted verbatim.
    /// </summary>
    public static string Literal(object? value) => value switch
    {
        null => "NULL",
        SparkSqlFragment f => f.Sql,
        string s => String(s),
        bool b => b ? "true" : "false",
        int or short or byte or sbyte => Convert.ToString(value, CultureInfo.InvariantCulture)!,
        long l => l.ToString(CultureInfo.InvariantCulture) + "L",
        double d when double.IsFinite(d) => d.ToString("R", CultureInfo.InvariantCulture) + "D",
        float f when float.IsFinite(f) => f.ToString("R", CultureInfo.InvariantCulture) + "F",
        decimal m => m.ToString(CultureInfo.InvariantCulture) + "BD",
        DateTimeOffset t => Timestamp(t),
        DateTime t => Timestamp(t.Kind == DateTimeKind.Local ? new DateTimeOffset(t) : new DateTimeOffset(DateTime.SpecifyKind(t, DateTimeKind.Utc))),
        IEnumerable<KeyValuePair<string, string>> map => Map(map),
        IEnumerable seq and not string => "array(" + string.Join(", ", seq.Cast<object?>().Select(Literal)) + ")",
        _ => throw new ArgumentException($"No Spark SQL literal for {value.GetType().Name} '{value}'", nameof(value)),
    };
}

/// <summary>SQL text inserted verbatim into a generated statement (an expression the caller has already made safe).</summary>
public sealed record SparkSqlFragment(string Sql)
{
    public override string ToString() => Sql;
}

/// <summary>A table as Spark names it: catalog, namespace levels, table.</summary>
public sealed record SparkTable(string Catalog, IReadOnlyList<string> Namespace, string Name)
{
    public SparkTable(string catalog, string @namespace, string name) : this(catalog, @namespace.Split('.'), name) { }

    /// <summary>The table <paramref name="id"/> in the Spark catalog named <paramref name="catalog"/>.</summary>
    public static SparkTable From(string catalog, TableIdentifier id) => new(catalog, id.Namespace.Levels, id.Name);

    /// <summary><c>`catalog`.`ns`.`table`</c>, for DDL/DML.</summary>
    public string QualifiedName => SparkSql.Identifier([Catalog, .. Namespace, Name]);

    /// <summary>
    /// The <c>table</c> argument of a procedure called on <see cref="Catalog"/>: the namespace-qualified identifier,
    /// back-quoted, as a string literal (Spark parses it as a multi-part identifier).
    /// </summary>
    public string ProcedureArgument => SparkSql.String(SparkSql.Identifier([.. Namespace, Name]));

    public override string ToString() => QualifiedName;
}
