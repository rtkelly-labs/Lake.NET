using System.Data.Common;
using System.Globalization;
using Iceberg.Net.Types;

namespace Iceberg.Net.DuckLake;

/// <summary>
/// How a catalog database stores the types DuckLake's tables declare. The spec only requires "transactions and primary
/// key constraints"; databases differ in how they hold <c>BOOLEAN</c>, <c>UUID</c> and <c>TIMESTAMPTZ</c>. Each dialect
/// matches the schema DuckDB's <c>ducklake</c> extension creates in that database, so either side can create the catalog
/// and the other attach to it.
/// </summary>
public sealed class DuckLakeDialect
{
    /// <summary>SQLite: booleans as <c>BIGINT</c> 0/1, UUIDs and timestamps as <c>VARCHAR</c> text (as DuckDB writes them).</summary>
    public static readonly DuckLakeDialect Sqlite = new()
    {
        Name = "sqlite",
        BooleanType = "BIGINT",
        UuidType = "VARCHAR",
        TimestampTzType = "VARCHAR",
        TableExistsSql = "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = @p0",
    };

    /// <summary>PostgreSQL: native <c>BOOLEAN</c>, <c>UUID</c> and <c>TIMESTAMP WITH TIME ZONE</c>, tables in the connection's current schema.</summary>
    public static readonly DuckLakeDialect PostgreSql = new()
    {
        Name = "postgresql",
        BooleanType = "BOOLEAN",
        UuidType = "UUID",
        TimestampTzType = "TIMESTAMP WITH TIME ZONE",
        TableExistsSql = "SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = @p0",
    };

    public required string Name { get; init; }
    public required string BooleanType { get; init; }
    public required string UuidType { get; init; }
    public required string TimestampTzType { get; init; }
    /// <summary>Counts tables named <c>@p0</c> (for the tables DuckDB creates on demand, which no catalog table lists).</summary>
    public required string TableExistsSql { get; init; }

    public override string ToString() => Name;

    /// <summary>A TIMESTAMPTZ as DuckDB writes it into text columns: <c>2026-10-27 10:41:25.208586+00</c>.</summary>
    public string FormatTimestampTz(DateTimeOffset t) =>
        t.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "+00";

    public static DateTimeOffset ParseTimestampTz(string text) =>
        DuckLakeValues.TryParse(DuckLakeType.TimestampTz, text, out var micros)
            ? new DateTimeOffset(IcebergTime.Epoch.AddTicks((long)micros! * 10))
            : throw new InvalidDataException($"'{text}' is not a timestamp with time zone");

    /// <summary>
    /// The catalog tables of a format version (spec "Tables", full schema creation script): 28 in 1.0; 1.1 adds
    /// <c>ducklake_view_column_tag</c>, <c>parent_schema_id</c>, <c>row_group_count</c> and the exactness flags of bounds.
    /// </summary>
    internal IReadOnlyList<string> Ddl(string version)
    {
        var v11 = version != DuckLakeCatalog.FormatVersion10;
        var tables = Ddl10();
        if (!v11) return tables;
        string Add(string table, string columns) => tables.Single(t => t.StartsWith($"CREATE TABLE {table}(", StringComparison.Ordinal))[..^1] + ", " + columns + ")";
        return
        [
            .. tables.Select(t => t.StartsWith("CREATE TABLE ducklake_schema(", StringComparison.Ordinal) ? Add("ducklake_schema", "parent_schema_id BIGINT")
                : t.StartsWith("CREATE TABLE ducklake_data_file(", StringComparison.Ordinal) ? Add("ducklake_data_file", "row_group_count BIGINT")
                : t.StartsWith("CREATE TABLE ducklake_delete_file(", StringComparison.Ordinal) ? Add("ducklake_delete_file", "row_group_count BIGINT")
                : t.StartsWith("CREATE TABLE ducklake_file_column_stats(", StringComparison.Ordinal) ? Add("ducklake_file_column_stats", $"min_is_exact {BooleanType}, max_is_exact {BooleanType}")
                : t.StartsWith("CREATE TABLE ducklake_table_column_stats(", StringComparison.Ordinal) ? Add("ducklake_table_column_stats", $"min_is_exact {BooleanType}, max_is_exact {BooleanType}")
                : t),
            "CREATE TABLE ducklake_view_column_tag(view_id BIGINT, column_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, \"key\" VARCHAR, \"value\" VARCHAR)",
        ];
    }

    private List<string> Ddl10() =>
    [
        $"CREATE TABLE ducklake_metadata(\"key\" VARCHAR NOT NULL, \"value\" VARCHAR NOT NULL, \"scope\" VARCHAR, scope_id BIGINT)",
        $"CREATE TABLE ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time {TimestampTzType}, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT)",
        $"CREATE TABLE ducklake_snapshot_changes(snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR)",
        $"CREATE TABLE ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid {UuidType}, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative {BooleanType})",
        $"CREATE TABLE ducklake_table(table_id BIGINT, table_uuid {UuidType}, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative {BooleanType})",
        $"CREATE TABLE ducklake_view(view_id BIGINT, view_uuid {UuidType}, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, \"sql\" VARCHAR, column_aliases VARCHAR)",
        $"CREATE TABLE ducklake_tag(object_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, \"key\" VARCHAR, \"value\" VARCHAR)",
        $"CREATE TABLE ducklake_column_tag(table_id BIGINT, column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, \"key\" VARCHAR, \"value\" VARCHAR)",
        $"CREATE TABLE ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative {BooleanType}, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, partial_max BIGINT)",
        $"CREATE TABLE ducklake_file_column_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan {BooleanType}, extra_stats VARCHAR)",
        $"CREATE TABLE ducklake_file_variant_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, variant_path VARCHAR, shredded_type VARCHAR, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan {BooleanType}, extra_stats VARCHAR)",
        $"CREATE TABLE ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative {BooleanType}, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, partial_max BIGINT)",
        $"CREATE TABLE ducklake_column(column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed {BooleanType}, parent_column BIGINT, default_value_type VARCHAR, default_value_dialect VARCHAR)",
        $"CREATE TABLE ducklake_table_stats(table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT)",
        $"CREATE TABLE ducklake_table_column_stats(table_id BIGINT, column_id BIGINT, contains_null {BooleanType}, contains_nan {BooleanType}, min_value VARCHAR, max_value VARCHAR, extra_stats VARCHAR)",
        $"CREATE TABLE ducklake_partition_info(partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT)",
        $"CREATE TABLE ducklake_partition_column(partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, \"transform\" VARCHAR)",
        $"CREATE TABLE ducklake_file_partition_value(data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR)",
        $"CREATE TABLE ducklake_files_scheduled_for_deletion(data_file_id BIGINT, path VARCHAR, path_is_relative {BooleanType}, schedule_start {TimestampTzType})",
        $"CREATE TABLE ducklake_inlined_data_tables(table_id BIGINT, table_name VARCHAR, schema_version BIGINT)",
        $"CREATE TABLE ducklake_column_mapping(mapping_id BIGINT, table_id BIGINT, \"type\" VARCHAR)",
        $"CREATE TABLE ducklake_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, is_partition {BooleanType})",
        $"CREATE TABLE ducklake_schema_versions(begin_snapshot BIGINT, schema_version BIGINT, table_id BIGINT)",
        $"CREATE TABLE ducklake_macro(schema_id BIGINT, macro_id BIGINT, macro_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT)",
        $"CREATE TABLE ducklake_macro_impl(macro_id BIGINT, impl_id BIGINT, dialect VARCHAR, \"sql\" VARCHAR, \"type\" VARCHAR)",
        $"CREATE TABLE ducklake_macro_parameters(macro_id BIGINT, impl_id BIGINT, column_id BIGINT, parameter_name VARCHAR, parameter_type VARCHAR, default_value VARCHAR, default_value_type VARCHAR)",
        $"CREATE TABLE ducklake_sort_info(sort_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT)",
        $"CREATE TABLE ducklake_sort_expression(sort_id BIGINT, table_id BIGINT, sort_key_index BIGINT, expression VARCHAR, dialect VARCHAR, sort_direction VARCHAR, null_order VARCHAR)",
    ];
}

/// <summary>Small ADO.NET helpers: positional <c>@p0</c>… parameters (accepted by SQLite, PostgreSQL and SQL Server providers).</summary>
internal static class Db
{
    /// <summary>The visibility predicate every versioned catalog row is read with (spec "Queries").</summary>
    public const string Live = "@s >= begin_snapshot AND (end_snapshot IS NULL OR @s < end_snapshot)";

    public static DbCommand Command(DbConnection connection, DbTransaction? tx, string sql, IReadOnlyList<object?> args, long? snapshot = null)
    {
        var cmd = connection.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = sql;
        for (var i = 0; i < args.Count; i++) Add(cmd, $"@p{i}", args[i]);
        if (snapshot is { } s) Add(cmd, "@s", s);
        return cmd;
    }

    private static void Add(DbCommand cmd, string name, object? value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value ?? DBNull.Value;
        cmd.Parameters.Add(p);
    }

    public static async Task<int> ExecAsync(DbConnection c, DbTransaction? tx, string sql, CancellationToken ct, params object?[] args)
    {
        await using var cmd = Command(c, tx, sql, args);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    public static async Task<List<T>> QueryAsync<T>(DbConnection c, DbTransaction? tx, string sql, Func<DbDataReader, T> map, CancellationToken ct,
        long? snapshot = null, params object?[] args)
    {
        await using var cmd = Command(c, tx, sql, args, snapshot);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var result = new List<T>();
        while (await reader.ReadAsync(ct)) result.Add(Corruption.Guard("DuckLake catalog row", () => map(reader)));
        return result;
    }

    public static long? Long(DbDataReader r, int i) => r.IsDBNull(i) ? null : Convert.ToInt64(r.GetValue(i), CultureInfo.InvariantCulture);
    public static long ReqLong(DbDataReader r, int i) => Long(r, i) ?? throw new InvalidDataException($"{r.GetName(i)} is NULL");
    public static string? Str(DbDataReader r, int i) => r.IsDBNull(i) ? null : Convert.ToString(r.GetValue(i), CultureInfo.InvariantCulture);
    public static string ReqStr(DbDataReader r, int i) => Str(r, i) ?? throw new InvalidDataException($"{r.GetName(i)} is NULL");

    public static bool? Bool(DbDataReader r, int i) => r.IsDBNull(i) ? null : r.GetValue(i) switch
    {
        bool b => b,
        string s => s is "1" or "true" or "t" or "TRUE",
        var v => Convert.ToInt64(v, CultureInfo.InvariantCulture) != 0,
    };

    public static Guid? Uuid(DbDataReader r, int i) => r.IsDBNull(i) ? null : r.GetValue(i) switch
    {
        Guid g => g,
        byte[] b when b.Length == 16 => new Guid(b),
        var v => Guid.Parse(Convert.ToString(v, CultureInfo.InvariantCulture)!),
    };

    public static DateTimeOffset Timestamp(DbDataReader r, int i) => r.GetValue(i) switch
    {
        DateTimeOffset d => d,
        DateTime d => new DateTimeOffset(DateTime.SpecifyKind(d, DateTimeKind.Utc)),
        var v when DuckLakeValues.TryParse(DuckLakeType.TimestampTz, Convert.ToString(v, CultureInfo.InvariantCulture)!, out var micros) =>
            new DateTimeOffset(IcebergTime.Epoch.AddTicks((long)micros! * 10)),
        var v => throw new InvalidDataException($"{r.GetName(i)}: '{v}' is not a timestamp"),
    };

    /// <summary>A SQL identifier, quoted: inlined-data table names come from the catalog, so they are never spliced raw.</summary>
    public static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"") + "\"";
}
