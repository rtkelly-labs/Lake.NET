using System.Globalization;
using System.Text;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Athena;

/// <summary>
/// A table as Athena names it. Athena runs DDL (CREATE / ALTER TABLE) through its Hive parser, which quotes with
/// back-quotes, and queries plus <c>OPTIMIZE</c> / <c>VACUUM</c> through Trino, which quotes with double quotes; the two
/// forms are <see cref="Ddl"/> and <see cref="Dml"/>.
/// </summary>
public sealed record AthenaTable(string Database, string Name, string? Catalog = null)
{
    /// <summary><c>`db`.`table`</c> (the Hive DDL parser takes no catalog prefix; set it on the query context instead).</summary>
    public string Ddl => AthenaSql.DdlIdentifier(Database) + "." + AthenaSql.DdlIdentifier(Name);

    /// <summary><c>"catalog"."db"."table"</c>, or <c>"db"."table"</c> without a catalog.</summary>
    public string Dml => (Catalog is null ? "" : AthenaSql.Identifier(Catalog) + ".") + AthenaSql.Identifier(Database) + "." + AthenaSql.Identifier(Name);

    public override string ToString() => Dml;
}

/// <summary>
/// Iceberg table properties Athena understands (<c>OPTIMIZE</c>, <c>VACUUM</c> and its writers read them). Null leaves
/// Athena's default.
/// </summary>
public sealed record AthenaIcebergProperties
{
    /// <summary>VACUUM expires snapshots older than this (default 432000 = 5 days).</summary>
    public long? VacuumMaxSnapshotAgeSeconds { get; init; }
    /// <summary>VACUUM keeps at least this many snapshots (default 1).</summary>
    public int? VacuumMinSnapshotsToKeep { get; init; }
    /// <summary>Metadata files kept (default 100).</summary>
    public int? VacuumMaxMetadataFilesToKeep { get; init; }
    /// <summary>OPTIMIZE rewrites a partition's data files only when at least this many need it (default 5).</summary>
    public int? OptimizeRewriteDataFileThreshold { get; init; }
    /// <summary>OPTIMIZE rewrites a data file once this many delete files apply to it (default 2).</summary>
    public int? OptimizeRewriteDeleteFileThreshold { get; init; }
    /// <summary><c>parquet</c> (default), <c>orc</c> or <c>avro</c>.</summary>
    public string? Format { get; init; }
    /// <summary>e.g. <c>zstd</c>, <c>snappy</c>, <c>gzip</c>.</summary>
    public string? WriteCompression { get; init; }
    public long? WriteTargetDataFileSizeBytes { get; init; }
    /// <summary>Anything else (Iceberg table properties pass through).</summary>
    public IReadOnlyDictionary<string, string>? Other { get; init; }

    public SortedDictionary<string, string> ToDictionary()
    {
        var d = new SortedDictionary<string, string>(StringComparer.Ordinal);
        void Set(string key, object? value)
        {
            if (value is not null) d[key] = Convert.ToString(value, CultureInfo.InvariantCulture)!;
        }
        Set("vacuum_max_snapshot_age_seconds", VacuumMaxSnapshotAgeSeconds);
        Set("vacuum_min_snapshots_to_keep", VacuumMinSnapshotsToKeep);
        Set("vacuum_max_metadata_files_to_keep", VacuumMaxMetadataFilesToKeep);
        Set("optimize_rewrite_data_file_threshold", OptimizeRewriteDataFileThreshold);
        Set("optimize_rewrite_delete_file_threshold", OptimizeRewriteDeleteFileThreshold);
        Set("format", Format);
        Set("write_compression", WriteCompression);
        Set("write_target_data_file_size_bytes", WriteTargetDataFileSizeBytes);
        if (Other is not null) foreach (var (k, v) in Other) d[k] = v;
        return d;
    }
}

/// <summary>
/// Athena (engine v3) SQL for Iceberg tables. Query identifiers are double-quoted (a double quote inside is doubled) and
/// string literals single-quoted with quotes doubled, as Trino parses them. Hive DDL strings (locations, table
/// properties) have no reliable escape in Athena, so values with a quote or backslash are refused.
/// </summary>
public static class AthenaSql
{
    /// <summary><c>"name"</c>, for queries and OPTIMIZE / VACUUM.</summary>
    public static string Identifier(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        return "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    }

    /// <summary><c>`name`</c>, for DDL. Glue names cannot hold a back-quote, so one is refused.</summary>
    public static string DdlIdentifier(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        if (name.Contains('`')) throw new ArgumentException($"Athena DDL identifiers cannot contain a back-quote: {name}", nameof(name));
        return "`" + name + "`";
    }

    /// <summary>A query string literal: <c>'it''s'</c>.</summary>
    public static string String(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    /// <summary>A DDL string literal (LOCATION, TBLPROPERTIES); refuses quotes, backslashes and control characters.</summary>
    public static string DdlString(string value)
    {
        if (value.Any(c => c is '\'' or '\\' or '"' || char.IsControl(c)))
            throw new ArgumentException($"Athena DDL strings cannot safely carry quotes, backslashes or control characters: {value}", nameof(value));
        return "'" + value + "'";
    }

    /// <summary><c>TIMESTAMP '2026-10-01 12:00:00.000000 UTC'</c>.</summary>
    public static string Timestamp(DateTimeOffset instant) =>
        "TIMESTAMP '" + instant.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + " UTC'";

    /// <summary>The Athena DDL type for an Iceberg type (Athena's Iceberg type support; anything else is refused).</summary>
    public static string DdlType(IcebergType type) => type switch
    {
        DecimalType d => $"decimal({d.Precision},{d.Scale})",
        StructType s => "struct<" + string.Join(",", s.Fields.Select(f => DdlIdentifierInType(f.Name) + ":" + DdlType(f.Type))) + ">",
        ListType l => "array<" + DdlType(l.ElementType) + ">",
        MapType m => "map<" + DdlType(m.KeyType) + "," + DdlType(m.ValueType) + ">",
        _ => type.TypeId switch
        {
            TypeId.Boolean => "boolean",
            TypeId.Int => "int",
            TypeId.Long => "bigint",
            TypeId.Float => "float",
            TypeId.Double => "double",
            TypeId.Date => "date",
            TypeId.Timestamp => "timestamp",
            TypeId.String => "string",
            TypeId.Binary => "binary",
            _ => throw new NotSupportedException(
                $"Athena cannot declare an Iceberg {type} column in CREATE TABLE; create the table with this library (it reads such columns) instead"),
        },
    };

    private static string DdlIdentifierInType(string name) =>
        name.All(c => char.IsAsciiLetterOrDigit(c) || c == '_') ? name : throw new ArgumentException($"Athena struct field names must be [A-Za-z0-9_]: {name}");

    /// <summary>The Athena PARTITIONED BY expression for a partition field.</summary>
    public static string PartitionExpression(PartitionField field, Schema schema)
    {
        var source = schema.Columns.FirstOrDefault(c => c.Id == field.SourceId)
            ?? throw new NotSupportedException($"Athena partitions only by top-level columns; field {field.Name} uses column {field.SourceId}");
        var column = DdlIdentifier(source.Name);
        return field.Transform switch
        {
            IdentityTransform => column,
            BucketTransform b => $"bucket({b.NumBuckets}, {column})",
            TruncateTransform t => $"truncate({t.Width}, {column})",
            TimeTransform { Unit: TimeUnit.Year } => $"year({column})",
            TimeTransform { Unit: TimeUnit.Month } => $"month({column})",
            TimeTransform { Unit: TimeUnit.Day } => $"day({column})",
            TimeTransform { Unit: TimeUnit.Hour } => $"hour({column})",
            var other => throw new NotSupportedException($"Athena has no partition transform for {other}"),
        };
    }

    /// <summary>
    /// <c>CREATE TABLE … TBLPROPERTIES ('table_type'='ICEBERG', …)</c> from an Iceberg schema and partition spec.
    /// Athena assigns its own field IDs and creates a format v2 table; required columns are not expressible in Athena
    /// DDL and become optional.
    /// </summary>
    public static string CreateTable(AthenaTable table, Schema schema, string location, PartitionSpec? spec = null, AthenaIcebergProperties? properties = null,
        bool ifNotExists = false)
    {
        var sb = new StringBuilder("CREATE TABLE ");
        if (ifNotExists) sb.Append("IF NOT EXISTS ");
        sb.Append(table.Ddl).Append(" (\n");
        sb.Append(string.Join(",\n", schema.Columns.Select(c =>
            "  " + DdlIdentifier(c.Name) + " " + DdlType(c.Type) + (c.Doc is null ? "" : " COMMENT " + DdlString(c.Doc)))));
        sb.Append(')');
        if (spec is not null && !spec.IsUnpartitioned)
            sb.Append("\nPARTITIONED BY (").Append(string.Join(", ", spec.Fields.Where(f => f.Transform is not VoidTransform).Select(f => PartitionExpression(f, schema)))).Append(')');
        sb.Append("\nLOCATION ").Append(DdlString(location));
        var props = new SortedDictionary<string, string>(properties?.ToDictionary() ?? new SortedDictionary<string, string>(), StringComparer.Ordinal);
        props.Remove("table_type");
        sb.Append("\nTBLPROPERTIES (").Append(string.Join(", ", new[] { "'table_type'='ICEBERG'" }.Concat(props.Select(p => $"{DdlString(p.Key)}={DdlString(p.Value)}")))).Append(')');
        return sb.ToString();
    }

    /// <summary><c>ALTER TABLE `db`.`t` SET TBLPROPERTIES ('k'='v', …)</c>.</summary>
    public static string SetTableProperties(AthenaTable table, IReadOnlyDictionary<string, string> properties)
    {
        if (properties.Count == 0) throw new ArgumentException("No properties to set", nameof(properties));
        return $"ALTER TABLE {table.Ddl} SET TBLPROPERTIES (" +
            string.Join(", ", properties.OrderBy(p => p.Key, StringComparer.Ordinal).Select(p => $"{DdlString(p.Key)}={DdlString(p.Value)}")) + ")";
    }

    public static string SetTableProperties(AthenaTable table, AthenaIcebergProperties properties) => SetTableProperties(table, properties.ToDictionary());

    /// <summary><c>ALTER TABLE `db`.`t` UNSET TBLPROPERTIES ('k', …)</c>.</summary>
    public static string UnsetTableProperties(AthenaTable table, IEnumerable<string> keys) =>
        $"ALTER TABLE {table.Ddl} UNSET TBLPROPERTIES (" + string.Join(", ", keys.Select(DdlString)) + ")";

    /// <summary>
    /// <c>OPTIMIZE "db"."t" REWRITE DATA USING BIN_PACK [WHERE …]</c>: compaction, also folding delete files into data.
    /// <paramref name="where"/> is an Athena predicate (on partition columns, for Athena to prune), inserted as given.
    /// </summary>
    public static string Optimize(AthenaTable table, string? where = null) =>
        $"OPTIMIZE {table.Dml} REWRITE DATA USING BIN_PACK" + (string.IsNullOrWhiteSpace(where) ? "" : " WHERE " + where);

    /// <summary><c>VACUUM "db"."t"</c>: expire snapshots per the <c>vacuum_*</c> properties, then remove orphan files.</summary>
    public static string Vacuum(AthenaTable table) => $"VACUUM {table.Dml}";

    /// <summary><c>FOR TIMESTAMP AS OF TIMESTAMP '… UTC'</c>.</summary>
    public static string AsOf(DateTimeOffset instant) => "FOR TIMESTAMP AS OF " + Timestamp(instant);

    /// <summary><c>FOR VERSION AS OF 123</c> (a snapshot ID).</summary>
    public static string AsOfVersion(long snapshotId) => "FOR VERSION AS OF " + snapshotId.ToString(CultureInfo.InvariantCulture);

    /// <summary><c>SELECT cols FROM "db"."t" [FOR … AS OF …] [WHERE …]</c>; <paramref name="where"/> is inserted as given.</summary>
    public static string Select(AthenaTable table, IEnumerable<string>? columns = null, string? timeTravel = null, string? where = null) =>
        "SELECT " + (columns is null ? "*" : string.Join(", ", columns.Select(Identifier))) + " FROM " + table.Dml +
        (timeTravel is null ? "" : " " + timeTravel) + (string.IsNullOrWhiteSpace(where) ? "" : " WHERE " + where);

    /// <summary>The <c>$snapshots</c>, <c>$history</c>, <c>$files</c>, <c>$manifests</c>, <c>$partitions</c> or <c>$refs</c> metadata table.</summary>
    public static string MetadataTable(AthenaTable table, string name)
    {
        if (name is not ("snapshots" or "history" or "files" or "manifests" or "partitions" or "refs"))
            throw new ArgumentException($"Unknown Athena Iceberg metadata table ${name}", nameof(name));
        return (table.Catalog is null ? "" : Identifier(table.Catalog) + ".") + Identifier(table.Database) + "." + Identifier(table.Name + "$" + name);
    }
}
