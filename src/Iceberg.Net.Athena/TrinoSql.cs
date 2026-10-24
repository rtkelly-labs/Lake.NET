using System.Globalization;
using System.Text;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Athena;

/// <summary>
/// The Trino dialect of the statements whose Athena form Trino does not parse. Athena's engine is Trino, and its
/// query SQL (<see cref="AthenaSql.Select"/>, <see cref="AthenaSql.AsOf"/>, <see cref="AthenaSql.AsOfVersion"/>,
/// <see cref="AthenaSql.MetadataTable"/>, identifiers and literals) runs on Trino unchanged; but Athena's DDL is Hive's
/// (<c>CREATE TABLE … TBLPROPERTIES</c>), and <c>OPTIMIZE</c> / <c>VACUUM</c> are Athena's own. Trino creates tables
/// with <c>WITH (…)</c> properties and maintains them with <c>ALTER TABLE … EXECUTE</c> procedures.
/// Name tables with <see cref="AthenaTable"/> and set its <see cref="AthenaTable.Catalog"/> to the Trino catalog.
/// </summary>
public static class TrinoSql
{
    /// <summary>The Trino type for an Iceberg type (types Trino cannot create are refused).</summary>
    public static string Type(IcebergType type) => type switch
    {
        DecimalType d => $"decimal({d.Precision}, {d.Scale})",
        StructType s => "row(" + string.Join(", ", s.Fields.Select(f => Column(f.Name) + " " + Type(f.Type))) + ")",
        ListType l => "array(" + Type(l.ElementType) + ")",
        MapType m => "map(" + Type(m.KeyType) + ", " + Type(m.ValueType) + ")",
        _ => type.TypeId switch
        {
            TypeId.Boolean => "boolean",
            TypeId.Int => "integer",
            TypeId.Long => "bigint",
            TypeId.Float => "real",
            TypeId.Double => "double",
            TypeId.Date => "date",
            TypeId.Time => "time(6)",
            TypeId.Timestamp => "timestamp(6)",
            TypeId.TimestampTz => "timestamp(6) with time zone",
            TypeId.String => "varchar",
            TypeId.Uuid => "uuid",
            TypeId.Binary => "varbinary",
            _ => throw new NotSupportedException($"Trino cannot create an Iceberg {type} column; create the table with this library instead"),
        },
    };

    /// <summary>A column or field name as Trino stores it: Trino folds names to lower case, so other names are refused.</summary>
    private static string Column(string name) =>
        name.Any(char.IsUpper) ? throw new ArgumentException($"Trino stores column names in lower case and would rename {name}", nameof(name)) : AthenaSql.Identifier(name);

    /// <summary>The Trino <c>partitioning</c> entry for a partition field, e.g. <c>bucket("id", 16)</c>.</summary>
    public static string PartitionExpression(PartitionField field, Schema schema)
    {
        var source = schema.Columns.FirstOrDefault(c => c.Id == field.SourceId)
            ?? throw new NotSupportedException($"Trino partitions only by top-level columns; field {field.Name} uses column {field.SourceId}");
        var column = Column(source.Name);
        return field.Transform switch
        {
            IdentityTransform => column,
            BucketTransform b => $"bucket({column}, {b.NumBuckets})",
            TruncateTransform t => $"truncate({column}, {t.Width})",
            TimeTransform { Unit: TimeUnit.Year } => $"year({column})",
            TimeTransform { Unit: TimeUnit.Month } => $"month({column})",
            TimeTransform { Unit: TimeUnit.Day } => $"day({column})",
            TimeTransform { Unit: TimeUnit.Hour } => $"hour({column})",
            var other => throw new NotSupportedException($"Trino has no partition transform for {other}"),
        };
    }

    /// <summary>
    /// <c>CREATE TABLE "c"."db"."t" (…) WITH (format_version = …, partitioning = ARRAY[…], location = …)</c>. Trino
    /// assigns its own field IDs and makes every column optional (it cannot declare NOT NULL on Iceberg columns).
    /// </summary>
    public static string CreateTable(AthenaTable table, Schema schema, PartitionSpec? spec = null, string? location = null, int? formatVersion = null,
        bool ifNotExists = false)
    {
        var sb = new StringBuilder("CREATE TABLE ");
        if (ifNotExists) sb.Append("IF NOT EXISTS ");
        sb.Append(table.Dml).Append(" (\n");
        sb.Append(string.Join(",\n", schema.Columns.Select(c =>
            "  " + Column(c.Name) + " " + Type(c.Type) + (c.Doc is null ? "" : " COMMENT " + AthenaSql.String(c.Doc)))));
        sb.Append(')');
        var with = new List<string>();
        if (formatVersion is { } v) with.Add("format_version = " + v.ToString(CultureInfo.InvariantCulture));
        if (spec is not null && !spec.IsUnpartitioned)
            with.Add("partitioning = ARRAY[" + string.Join(", ", spec.Fields.Where(f => f.Transform is not VoidTransform)
                .Select(f => AthenaSql.String(PartitionExpression(f, schema)))) + "]");
        if (location is not null) with.Add("location = " + AthenaSql.String(location));
        if (with.Count > 0) sb.Append("\nWITH (").Append(string.Join(", ", with)).Append(')');
        return sb.ToString();
    }

    /// <summary>
    /// <c>ALTER TABLE … EXECUTE optimize[(file_size_threshold => '…B')] [WHERE …]</c>, Athena's <c>OPTIMIZE</c>:
    /// rewrites files smaller than the threshold (Trino's default 100MB). Trino accepts a WHERE only on identity
    /// partition columns; <paramref name="where"/> is inserted as given.
    /// </summary>
    public static string Optimize(AthenaTable table, long? fileSizeThresholdBytes = null, string? where = null) =>
        $"ALTER TABLE {table.Dml} EXECUTE optimize" +
        (fileSizeThresholdBytes is { } b ? $"(file_size_threshold => '{b.ToString(CultureInfo.InvariantCulture)}B')" : "") +
        (string.IsNullOrWhiteSpace(where) ? "" : " WHERE " + where);

    /// <summary>
    /// <c>ALTER TABLE … EXECUTE expire_snapshots(retention_threshold => '…s')</c>: the snapshot half of Athena's
    /// <c>VACUUM</c>. Trino refuses a retention under <c>iceberg.expire_snapshots_min_retention</c> (default 7 days).
    /// </summary>
    public static string ExpireSnapshots(AthenaTable table, TimeSpan retention) =>
        $"ALTER TABLE {table.Dml} EXECUTE expire_snapshots(retention_threshold => {Duration(retention)})";

    /// <summary>
    /// <c>ALTER TABLE … EXECUTE remove_orphan_files(retention_threshold => '…s')</c>: the file half of Athena's
    /// <c>VACUUM</c>. Trino refuses a retention under <c>iceberg.remove_orphan_files_min_retention</c> (default 7 days).
    /// </summary>
    public static string RemoveOrphanFiles(AthenaTable table, TimeSpan retention) =>
        $"ALTER TABLE {table.Dml} EXECUTE remove_orphan_files(retention_threshold => {Duration(retention)})";

    /// <summary>Athena's <c>VACUUM</c> in Trino: expire snapshots, then remove orphan files.</summary>
    public static IReadOnlyList<string> Vacuum(AthenaTable table, TimeSpan retention) => [ExpireSnapshots(table, retention), RemoveOrphanFiles(table, retention)];

    /// <summary>A Trino duration literal in whole milliseconds, e.g. <c>'604800000ms'</c>.</summary>
    public static string Duration(TimeSpan value)
    {
        if (value < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(value), value, "A retention cannot be negative");
        return "'" + ((long)value.TotalMilliseconds).ToString(CultureInfo.InvariantCulture) + "ms'";
    }
}
