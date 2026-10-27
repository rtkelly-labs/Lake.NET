using System.Globalization;
using System.Text;

namespace Iceberg.Net.DuckLake;

/// <summary>One entry of <c>ducklake_snapshot_changes.changes_made</c>, e.g. <c>inserted_into_table:2</c> or <c>created_table:"main"."t"</c>.</summary>
public sealed record DuckLakeChange(string Kind, string Target)
{
    public const string CreatedSchema = "created_schema";
    public const string CreatedTable = "created_table";
    public const string CreatedView = "created_view";
    public const string InsertedIntoTable = "inserted_into_table";
    public const string DeletedFromTable = "deleted_from_table";
    public const string CompactedTable = "compacted_table";
    public const string DroppedSchema = "dropped_schema";
    public const string DroppedTable = "dropped_table";
    public const string DroppedView = "dropped_view";
    public const string AlteredTable = "altered_table";
    public const string AlteredView = "altered_view";
    public const string CreatedScalarMacro = "created_scalar_macro";
    public const string CreatedTableMacro = "created_table_macro";
    public const string DroppedScalarMacro = "dropped_scalar_macro";
    public const string DroppedTableMacro = "dropped_table_macro";
    /// <summary>Files of the table were merged into partial files (spec: conflicts are per data file).</summary>
    public const string MergeAdjacent = "merge_adjacent";
    /// <summary>Files of the table were rewritten without their deleted rows (spec: conflicts are per data file).</summary>
    public const string RewriteDelete = "rewrite_delete";
    // Data inlining: inserts and deletes for conflict purposes.
    public const string InlinedInsert = "inlined_insert";
    public const string InlinedDelete = "inlined_delete";
    /// <summary>Inlined rows moved to Parquet: for a concurrent delete, a compaction.</summary>
    public const string InlineFlush = "inline_flush";

    public long? TargetId => long.TryParse(Target, NumberStyles.Integer, CultureInfo.InvariantCulture, out var id) ? id : null;

    public override string ToString() => $"{Kind}:{Target}";

    public static DuckLakeChange Of(string kind, long id) => new(kind, id.ToString(CultureInfo.InvariantCulture));

    /// <summary>A name in the quoted format the spec prescribes: <c>this "table"</c> → <c>"this ""table"""</c>.</summary>
    public static string QuoteName(string name) => "\"" + name.Replace("\"", "\"\"") + "\"";

    /// <summary>
    /// Split <c>changes_made</c> at commas outside quoted names. Untrusted catalog text: an unterminated quote or an
    /// entry without a kind is a format error.
    /// </summary>
    public static IReadOnlyList<DuckLakeChange> Parse(string? text) => Corruption.Guard("ducklake_snapshot_changes.changes_made", () =>
    {
        var result = new List<DuckLakeChange>();
        if (string.IsNullOrEmpty(text)) return (IReadOnlyList<DuckLakeChange>)result;
        var current = new StringBuilder();
        var quoted = false;
        foreach (var c in text)
        {
            if (c == '"') quoted = !quoted; // a doubled quote toggles twice: still inside
            if (c == ',' && !quoted)
            {
                result.Add(Entry(current.ToString()));
                current.Clear();
            }
            else current.Append(c);
        }
        if (quoted) throw new FormatException($"Unterminated quoted name in '{text}'");
        result.Add(Entry(current.ToString()));
        return result;
    });

    private static DuckLakeChange Entry(string text)
    {
        var colon = text.IndexOf(':');
        if (colon <= 0) throw new FormatException($"Change '{text}' has no kind");
        return new DuckLakeChange(text[..colon].Trim(), text[(colon + 1)..].Trim());
    }

    /// <summary>Split a quoted qualified name (<c>"main"."t"</c>) into its parts.</summary>
    public static IReadOnlyList<string> SplitName(string target)
    {
        var parts = new List<string>();
        var i = 0;
        while (i < target.Length)
        {
            if (target[i] == '.') { i++; continue; }
            if (target[i] != '"') throw new FormatException($"Expected a quoted name in '{target}'");
            var sb = new StringBuilder();
            i++;
            while (true)
            {
                if (i >= target.Length) throw new FormatException($"Unterminated quoted name in '{target}'");
                if (target[i] == '"')
                {
                    if (i + 1 < target.Length && target[i + 1] == '"') { sb.Append('"'); i += 2; continue; }
                    i++;
                    break;
                }
                sb.Append(target[i++]);
            }
            parts.Add(sb.ToString());
        }
        return parts;
    }
}

/// <summary>
/// What a transaction does, in the terms of the spec's conflict rules ("Conflict Resolution", "Logical Conflicts"):
/// on a lost race for the next <c>snapshot_id</c>, the committer reads the <c>changes_made</c> of every snapshot
/// committed since its base and retries only if none of them conflicts with these.
/// </summary>
internal sealed class ChangeSet
{
    public HashSet<string> CreatedSchemas { get; } = new(StringComparer.Ordinal);
    public Dictionary<long, string> DroppedSchemas { get; } = [];
    /// <summary>(schema name, table or view name) of each created table; schema ID when the schema already existed.</summary>
    public List<(string Schema, long? SchemaId, string Name)> CreatedTables { get; } = [];
    public HashSet<long> DroppedTables { get; } = [];
    public HashSet<long> AlteredTables { get; } = [];
    public HashSet<long> InsertedTables { get; } = [];
    public HashSet<long> DeletedTables { get; } = [];
    public HashSet<long> MergedTables { get; } = [];
    public HashSet<long> RewrittenTables { get; } = [];

    public bool IsEmpty => CreatedSchemas.Count + DroppedSchemas.Count + CreatedTables.Count + DroppedTables.Count
        + AlteredTables.Count + InsertedTables.Count + DeletedTables.Count + MergedTables.Count + RewrittenTables.Count == 0;

    private IEnumerable<long> Touched =>
        DroppedTables.Concat(AlteredTables).Concat(InsertedTables).Concat(DeletedTables).Concat(MergedTables).Concat(RewrittenTables);

    /// <summary>
    /// The first conflict between this transaction and a concurrent snapshot's changes, or null.
    /// Delete-versus-delete on the same table is not decided here: it conflicts only when both touch the same data
    /// file, which the committer checks against the catalog. So are compactions (<c>merge_adjacent</c>,
    /// <c>rewrite_delete</c>) against deletes and other compactions: "Compactions on different files in the same table
    /// can commit concurrently".
    /// </summary>
    public string? ConflictWith(long snapshotId, IReadOnlyList<DuckLakeChange> theirs)
    {
        foreach (var c in theirs)
        {
            var id = c.TargetId;
            string? reason = c.Kind switch
            {
                DuckLakeChange.CreatedSchema when CreatedSchemas.Contains(Single(c.Target)) => "both created schema " + c.Target,
                DuckLakeChange.DroppedSchema when id is { } s && DroppedSchemas.ContainsKey(s) => $"both dropped schema {s}",
                DuckLakeChange.DroppedSchema when id is { } s && CreatedTables.Any(t => t.SchemaId == s) => $"schema {s} was dropped while creating a table in it",
                DuckLakeChange.CreatedTable or DuckLakeChange.CreatedView when Qualified(c.Target) is var (schema, name) && schema is not null
                    && (CreatedTables.Any(t => t.Schema == schema && t.Name == name) || DroppedSchemas.ContainsValue(schema))
                    => $"concurrent {c}",
                DuckLakeChange.DroppedTable when id is { } t && Touched.Contains(t) => $"table {t} was dropped",
                DuckLakeChange.AlteredTable when id is { } t && (AlteredTables.Contains(t) || InsertedTables.Contains(t) || DeletedTables.Contains(t)) => $"table {t} was altered",
                DuckLakeChange.InsertedIntoTable or DuckLakeChange.InlinedInsert when id is { } t && DeletedTables.Contains(t) => $"table {t} was inserted into while deleting from it",
                DuckLakeChange.DeletedFromTable or DuckLakeChange.InlinedDelete when id is { } t && InsertedTables.Contains(t) => $"table {t} was deleted from while inserting into it",
                DuckLakeChange.CompactedTable or DuckLakeChange.InlineFlush when id is { } t && DeletedTables.Contains(t) => $"table {t} was compacted while deleting from it",
                DuckLakeChange.CreatedSchema or DuckLakeChange.DroppedSchema or DuckLakeChange.CreatedTable or DuckLakeChange.CreatedView
                    or DuckLakeChange.DroppedTable or DuckLakeChange.AlteredTable or DuckLakeChange.InsertedIntoTable or DuckLakeChange.InlinedInsert
                    or DuckLakeChange.DeletedFromTable or DuckLakeChange.InlinedDelete or DuckLakeChange.CompactedTable or DuckLakeChange.InlineFlush
                    or DuckLakeChange.DroppedView or DuckLakeChange.AlteredView or DuckLakeChange.MergeAdjacent or DuckLakeChange.RewriteDelete
                    or DuckLakeChange.CreatedScalarMacro or DuckLakeChange.CreatedTableMacro or DuckLakeChange.DroppedScalarMacro
                    or DuckLakeChange.DroppedTableMacro => null,
                // A kind this implementation does not know: conflict if it names a table we touch.
                _ when id is { } t && Touched.Contains(t) => $"unrecognised change {c} on a table this transaction modifies",
                _ => null,
            };
            if (reason is not null) return $"snapshot {snapshotId}: {reason}";
        }
        return null;
    }

    /// <summary>A schema path (<c>"sales"."europe"</c>, 1.1) as this library names schemas: <c>sales.europe</c>.</summary>
    private static string Single(string target) => string.Join('.', DuckLakeChange.SplitName(target));

    /// <summary>A table or view under a schema path: every name but the last is the schema's.</summary>
    private static (string? Schema, string? Name) Qualified(string target) =>
        DuckLakeChange.SplitName(target) is { Count: >= 2 } parts ? (string.Join('.', parts.Take(parts.Count - 1)), parts[^1]) : (null, null);

    /// <summary>A schema path quoted as the spec writes it: each name quoted, joined by <c>.</c>.</summary>
    internal static string QuotePath(string schemaPath) => string.Join('.', schemaPath.Split('.').Select(DuckLakeChange.QuoteName));

    public IEnumerable<DuckLakeChange> Entries()
    {
        foreach (var s in CreatedSchemas) yield return new(DuckLakeChange.CreatedSchema, QuotePath(s));
        foreach (var (schema, _, name) in CreatedTables)
            yield return new(DuckLakeChange.CreatedTable, QuotePath(schema) + "." + DuckLakeChange.QuoteName(name));
        foreach (var t in InsertedTables) yield return DuckLakeChange.Of(DuckLakeChange.InsertedIntoTable, t);
        foreach (var t in DeletedTables) yield return DuckLakeChange.Of(DuckLakeChange.DeletedFromTable, t);
        foreach (var t in AlteredTables) yield return DuckLakeChange.Of(DuckLakeChange.AlteredTable, t);
        foreach (var t in MergedTables) yield return DuckLakeChange.Of(DuckLakeChange.MergeAdjacent, t);
        foreach (var t in RewrittenTables) yield return DuckLakeChange.Of(DuckLakeChange.RewriteDelete, t);
        foreach (var t in DroppedTables) yield return DuckLakeChange.Of(DuckLakeChange.DroppedTable, t);
        foreach (var s in DroppedSchemas.Keys) yield return DuckLakeChange.Of(DuckLakeChange.DroppedSchema, s);
    }
}
