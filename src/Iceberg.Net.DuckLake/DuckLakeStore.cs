using System.Data.Common;
using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Npgsql;

namespace Iceberg.Net.DuckLake;

/// <summary>
/// The catalog database a DuckLake lives in, reached through EF Core. Only the databases DuckDB's <c>ducklake</c>
/// extension can also attach are offered: a lake DuckDB cannot open has lost the format's main reader. The schema is
/// not EF's: it is the spec's script in the column types DuckDB uses for that database (<see cref="DuckLakeDialect"/>),
/// created by <see cref="DuckLakeCatalog.CreateAsync"/>; EF only queries and writes it, with no migrations.
/// </summary>
public sealed class DuckLakeStore
{
    private readonly Action<DbContextOptionsBuilder> _configure;

    private DuckLakeStore(DuckLakeDialect dialect, Action<DbContextOptionsBuilder> configure, string connectionString)
    {
        Dialect = dialect;
        _configure = configure;
        ConnectionString = connectionString;
    }

    public DuckLakeDialect Dialect { get; }
    public string ConnectionString { get; }

    /// <summary>
    /// A SQLite catalog file (DuckDB: <c>ATTACH 'ducklake:sqlite:&lt;file&gt;'</c>). One writer at a time; others wait on the
    /// file lock. <paramref name="configure"/> adds EF Core options such as logging or interceptors.
    /// </summary>
    public static DuckLakeStore Sqlite(string connectionString, Action<DbContextOptionsBuilder>? configure = null) =>
        new(DuckLakeDialect.Sqlite, o => { o.UseSqlite(connectionString); configure?.Invoke(o); }, connectionString);

    /// <summary>
    /// A PostgreSQL catalog (DuckDB: <c>ATTACH 'ducklake:postgres:dbname=… host=…'</c>), the multi-user choice.
    /// <paramref name="configure"/> adds EF Core options such as logging or interceptors.
    /// </summary>
    public static DuckLakeStore PostgreSql(string connectionString, Action<DbContextOptionsBuilder>? configure = null) =>
        new(DuckLakeDialect.PostgreSql, o => { o.UseNpgsql(connectionString); configure?.Invoke(o); }, connectionString);

    internal DuckLakeDbContext Context(string formatVersion, DuckLakeTelemetry.Scope? scope = null)
    {
        var builder = new DbContextOptionsBuilder<DuckLakeDbContext>();
        _configure(builder);
        builder.ReplaceService<IModelCacheKeyFactory, DuckLakeModelCacheKeyFactory>();
        builder.AddInterceptors(DuckLakeCommandCounter.Instance);
        return new DuckLakeDbContext(builder.Options, Dialect, formatVersion) { Scope = scope };
    }

    /// <summary>
    /// The <c>ATTACH</c> string DuckDB uses for this catalog (handy for handing a lake written here to DuckDB).
    /// </summary>
    public string DuckDbAttach()
    {
        if (Dialect == DuckLakeDialect.Sqlite)
            return "ducklake:sqlite:" + new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder(ConnectionString).DataSource;
        var b = new NpgsqlConnectionStringBuilder(ConnectionString);
        var parts = new List<string> { $"dbname={b.Database}", $"host={b.Host}", $"port={b.Port.ToString(CultureInfo.InvariantCulture)}" };
        if (b.Username is { } user) parts.Add($"user={user}");
        if (b.Password is { } password) parts.Add($"password={password}");
        return "ducklake:postgres:" + string.Join(' ', parts);
    }
}

/// <summary>One EF model per (database, format version): 1.0 catalogs lack the columns 1.1 added.</summary>
internal sealed class DuckLakeModelCacheKeyFactory : IModelCacheKeyFactory
{
    public object Create(DbContext context, bool designTime) =>
        context is DuckLakeDbContext d ? (d.GetType(), d.Dialect.Name, d.FormatVersion, designTime) : (object)(context.GetType(), designTime);
}

// ---- entities: the fixed catalog tables this library reads and writes ----------------------------------------------
// Keys are the EF model's only (most DuckLake tables have no primary key, and EF cannot insert into a keyless entity);
// they are never created in the database.

internal interface IVersioned
{
    long BeginSnapshot { get; }
    long? EndSnapshot { get; }
}

internal sealed class SnapshotEntity
{
    public long SnapshotId { get; set; }
    public DateTimeOffset SnapshotTime { get; set; }
    public long SchemaVersion { get; set; }
    public long NextCatalogId { get; set; }
    public long NextFileId { get; set; }
}

internal sealed class SnapshotChangesEntity
{
    public long SnapshotId { get; set; }
    public string? ChangesMade { get; set; }
    public string? Author { get; set; }
    public string? CommitMessage { get; set; }
    public string? CommitExtraInfo { get; set; }
}

internal sealed class SchemaEntity : IVersioned
{
    public long SchemaId { get; set; }
    public Guid? SchemaUuid { get; set; }
    public long BeginSnapshot { get; set; }
    public long? EndSnapshot { get; set; }
    public string? SchemaName { get; set; }
    public string? Path { get; set; }
    public bool? PathIsRelative { get; set; }
    /// <summary>1.1: the parent of a nested schema.</summary>
    public long? ParentSchemaId { get; set; }
}

internal sealed class TableEntity : IVersioned
{
    public long TableId { get; set; }
    public Guid? TableUuid { get; set; }
    public long BeginSnapshot { get; set; }
    public long? EndSnapshot { get; set; }
    public long SchemaId { get; set; }
    public string? TableName { get; set; }
    public string? Path { get; set; }
    public bool? PathIsRelative { get; set; }
}

internal sealed class ColumnEntity : IVersioned
{
    public long ColumnId { get; set; }
    public long BeginSnapshot { get; set; }
    public long? EndSnapshot { get; set; }
    public long TableId { get; set; }
    public long? ColumnOrder { get; set; }
    public string? ColumnName { get; set; }
    public string? ColumnType { get; set; }
    public string? InitialDefault { get; set; }
    public string? DefaultValue { get; set; }
    public bool? NullsAllowed { get; set; }
    public long? ParentColumn { get; set; }
    public string? DefaultValueType { get; set; }
    public string? DefaultValueDialect { get; set; }
}

internal sealed class DataFileEntity : IVersioned
{
    public long DataFileId { get; set; }
    public long TableId { get; set; }
    public long BeginSnapshot { get; set; }
    public long? EndSnapshot { get; set; }
    public long? FileOrder { get; set; }
    public string? Path { get; set; }
    public bool? PathIsRelative { get; set; }
    public string? FileFormat { get; set; }
    public long RecordCount { get; set; }
    public long FileSizeBytes { get; set; }
    public long? FooterSize { get; set; }
    public long? RowIdStart { get; set; }
    public long? PartitionId { get; set; }
    public string? EncryptionKey { get; set; }
    public long? MappingId { get; set; }
    public long? PartialMax { get; set; }
    /// <summary>1.1.</summary>
    public long? RowGroupCount { get; set; }
}

internal sealed class DeleteFileEntity : IVersioned
{
    public long DeleteFileId { get; set; }
    public long TableId { get; set; }
    public long BeginSnapshot { get; set; }
    public long? EndSnapshot { get; set; }
    public long DataFileId { get; set; }
    public string? Path { get; set; }
    public bool? PathIsRelative { get; set; }
    public string? Format { get; set; }
    public long? DeleteCount { get; set; }
    public long? FileSizeBytes { get; set; }
    public long? FooterSize { get; set; }
    public string? EncryptionKey { get; set; }
    public long? PartialMax { get; set; }
    /// <summary>1.1.</summary>
    public long? RowGroupCount { get; set; }
}

internal sealed class FileColumnStatsEntity
{
    public long DataFileId { get; set; }
    public long TableId { get; set; }
    public long ColumnId { get; set; }
    public long? ColumnSizeBytes { get; set; }
    public long? ValueCount { get; set; }
    public long? NullCount { get; set; }
    public string? MinValue { get; set; }
    public string? MaxValue { get; set; }
    public bool? ContainsNan { get; set; }
    public string? ExtraStats { get; set; }
    /// <summary>1.1.</summary>
    public bool? MinIsExact { get; set; }
    /// <summary>1.1.</summary>
    public bool? MaxIsExact { get; set; }
}

internal sealed class TableStatsEntity
{
    public long TableId { get; set; }
    public long? RecordCount { get; set; }
    public long? NextRowId { get; set; }
    public long? FileSizeBytes { get; set; }
}

internal sealed class TableColumnStatsEntity
{
    public long TableId { get; set; }
    public long ColumnId { get; set; }
    public bool? ContainsNull { get; set; }
    public bool? ContainsNan { get; set; }
    public string? MinValue { get; set; }
    public string? MaxValue { get; set; }
    public string? ExtraStats { get; set; }
    /// <summary>1.1.</summary>
    public bool? MinIsExact { get; set; }
    /// <summary>1.1.</summary>
    public bool? MaxIsExact { get; set; }
}

internal sealed class PartitionInfoEntity : IVersioned
{
    public long PartitionId { get; set; }
    public long TableId { get; set; }
    public long BeginSnapshot { get; set; }
    public long? EndSnapshot { get; set; }
}

internal sealed class PartitionColumnEntity
{
    public long PartitionId { get; set; }
    public long TableId { get; set; }
    public long PartitionKeyIndex { get; set; }
    public long ColumnId { get; set; }
    public string? Transform { get; set; }
}

internal sealed class FilePartitionValueEntity
{
    public long DataFileId { get; set; }
    public long TableId { get; set; }
    public long PartitionKeyIndex { get; set; }
    public string? PartitionValue { get; set; }
}

internal sealed class SchemaVersionEntity
{
    public long BeginSnapshot { get; set; }
    public long SchemaVersion { get; set; }
    public long TableId { get; set; }
}

internal sealed class InlinedDataTableEntity
{
    public long TableId { get; set; }
    public string TableName { get; set; } = "";
    public long SchemaVersion { get; set; }
}

internal sealed class FileScheduledForDeletionEntity
{
    public long DataFileId { get; set; }
    public string Path { get; set; } = "";
    public bool? PathIsRelative { get; set; }
    public DateTimeOffset ScheduleStart { get; set; }
}

internal sealed class TagEntity : IVersioned
{
    public long ObjectId { get; set; }
    public long BeginSnapshot { get; set; }
    public long? EndSnapshot { get; set; }
    public string Key { get; set; } = "";
    public string? Value { get; set; }
}

internal sealed class ColumnTagEntity : IVersioned
{
    public long TableId { get; set; }
    public long ColumnId { get; set; }
    public long BeginSnapshot { get; set; }
    public long? EndSnapshot { get; set; }
    public string Key { get; set; } = "";
    public string? Value { get; set; }
}

internal sealed class ViewEntity : IVersioned
{
    public long ViewId { get; set; }
    public Guid? ViewUuid { get; set; }
    public long BeginSnapshot { get; set; }
    public long? EndSnapshot { get; set; }
    public long SchemaId { get; set; }
    public string? ViewName { get; set; }
    public string? Dialect { get; set; }
    public string? Sql { get; set; }
    public string? ColumnAliases { get; set; }
}

/// <summary>1.1.</summary>
internal sealed class ViewColumnTagEntity : IVersioned
{
    public long ViewId { get; set; }
    public string ColumnName { get; set; } = "";
    public long BeginSnapshot { get; set; }
    public long? EndSnapshot { get; set; }
    public string Key { get; set; } = "";
    public string? Value { get; set; }
}

internal sealed class MacroEntity : IVersioned
{
    public long SchemaId { get; set; }
    public long MacroId { get; set; }
    public string? MacroName { get; set; }
    public long BeginSnapshot { get; set; }
    public long? EndSnapshot { get; set; }
}

internal sealed class MacroImplEntity
{
    public long MacroId { get; set; }
    public long ImplId { get; set; }
    public string? Dialect { get; set; }
    public string? Sql { get; set; }
    public string? Type { get; set; }
}

internal sealed class MacroParameterEntity
{
    public long MacroId { get; set; }
    public long ImplId { get; set; }
    public long ColumnId { get; set; }
    public string? ParameterName { get; set; }
    public string? ParameterType { get; set; }
    public string? DefaultValue { get; set; }
    public string? DefaultValueType { get; set; }
}

internal sealed class SortInfoEntity : IVersioned
{
    public long SortId { get; set; }
    public long TableId { get; set; }
    public long BeginSnapshot { get; set; }
    public long? EndSnapshot { get; set; }
}

internal sealed class SortExpressionEntity
{
    public long SortId { get; set; }
    public long TableId { get; set; }
    public long SortKeyIndex { get; set; }
    public string? Expression { get; set; }
    public string? Dialect { get; set; }
    public string? SortDirection { get; set; }
    public string? NullOrder { get; set; }
}

internal sealed class ColumnMappingEntity
{
    public long MappingId { get; set; }
    public long TableId { get; set; }
    public string? Type { get; set; }
}

internal sealed class NameMappingEntity
{
    public long MappingId { get; set; }
    public long ColumnId { get; set; }
    public string? SourceName { get; set; }
    public long TargetFieldId { get; set; }
    public long? ParentColumn { get; set; }
    public bool? IsPartition { get; set; }
}

internal sealed class MetadataEntity
{
    public string Key { get; set; } = "";
    public string Value { get; set; } = "";
    public string? Scope { get; set; }
    public long? ScopeId { get; set; }
}

internal sealed class DuckLakeDbContext(DbContextOptions<DuckLakeDbContext> options, DuckLakeDialect dialect, string formatVersion) : DbContext(options)
{
    public DuckLakeDialect Dialect { get; } = dialect;
    public string FormatVersion { get; } = formatVersion;
    public bool Is11 => FormatVersion != DuckLakeCatalog.FormatVersion10;
    /// <summary>The operation this context works for; its commands are counted there.</summary>
    public DuckLakeTelemetry.Scope? Scope { get; init; }

    public DbSet<SnapshotEntity> Snapshots => Set<SnapshotEntity>();
    public DbSet<SnapshotChangesEntity> SnapshotChanges => Set<SnapshotChangesEntity>();
    public DbSet<SchemaEntity> Schemas => Set<SchemaEntity>();
    public DbSet<TableEntity> Tables => Set<TableEntity>();
    public DbSet<ColumnEntity> Columns => Set<ColumnEntity>();
    public DbSet<DataFileEntity> DataFiles => Set<DataFileEntity>();
    public DbSet<DeleteFileEntity> DeleteFiles => Set<DeleteFileEntity>();
    public DbSet<FileColumnStatsEntity> FileColumnStats => Set<FileColumnStatsEntity>();
    public DbSet<TableStatsEntity> TableStats => Set<TableStatsEntity>();
    public DbSet<TableColumnStatsEntity> TableColumnStats => Set<TableColumnStatsEntity>();
    public DbSet<PartitionInfoEntity> PartitionInfos => Set<PartitionInfoEntity>();
    public DbSet<PartitionColumnEntity> PartitionColumns => Set<PartitionColumnEntity>();
    public DbSet<FilePartitionValueEntity> FilePartitionValues => Set<FilePartitionValueEntity>();
    public DbSet<SchemaVersionEntity> SchemaVersions => Set<SchemaVersionEntity>();
    public DbSet<InlinedDataTableEntity> InlinedDataTables => Set<InlinedDataTableEntity>();
    public DbSet<MetadataEntity> Metadata => Set<MetadataEntity>();
    public DbSet<FileScheduledForDeletionEntity> FilesScheduledForDeletion => Set<FileScheduledForDeletionEntity>();
    public DbSet<TagEntity> Tags => Set<TagEntity>();
    public DbSet<ColumnTagEntity> ColumnTags => Set<ColumnTagEntity>();
    public DbSet<ViewEntity> Views => Set<ViewEntity>();
    /// <summary>1.1 catalogs only.</summary>
    public DbSet<ViewColumnTagEntity> ViewColumnTags => Is11 ? Set<ViewColumnTagEntity>() : throw new NotSupportedException("ducklake_view_column_tag needs format 1.1");
    public DbSet<MacroEntity> Macros => Set<MacroEntity>();
    public DbSet<MacroImplEntity> MacroImpls => Set<MacroImplEntity>();
    public DbSet<MacroParameterEntity> MacroParameters => Set<MacroParameterEntity>();
    public DbSet<SortInfoEntity> SortInfos => Set<SortInfoEntity>();
    public DbSet<SortExpressionEntity> SortExpressions => Set<SortExpressionEntity>();
    public DbSet<ColumnMappingEntity> ColumnMappings => Set<ColumnMappingEntity>();
    public DbSet<NameMappingEntity> NameMappings => Set<NameMappingEntity>();

    /// <summary>
    /// The connection and the transaction EF is using, for a statement EF cannot model (inlined tables, DDL). Take it
    /// once per statement: it counts one catalog command, as EF's interceptor counts EF's.
    /// </summary>
    public (DbConnection Connection, DbTransaction? Transaction) Raw()
    {
        if (Scope is { } scope) Interlocked.Increment(ref scope.CatalogCommands);
        return (Database.GetDbConnection(), Database.CurrentTransaction is { } t ? Microsoft.EntityFrameworkCore.Storage.DbContextTransactionExtensions.GetDbTransaction(t) : null);
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) =>
        optionsBuilder.UseQueryTrackingBehavior(QueryTrackingBehavior.NoTracking);

    protected override void OnModelCreating(ModelBuilder m)
    {
        var sqlite = Dialect == DuckLakeDialect.Sqlite;
        // DuckDB's SQLite catalogs keep UUIDs and timestamps as text; PostgreSQL has the native types.
        var uuid = new ValueConverter<Guid?, string?>(g => g == null ? null : g.Value.ToString("D"), s => s == null ? null : Guid.Parse(s));
        var time = new ValueConverter<DateTimeOffset, string>(t => DuckLakeDialect.Sqlite.FormatTimestampTz(t), s => DuckLakeDialect.ParseTimestampTz(s));
        var utc = new ValueConverter<DateTimeOffset, DateTimeOffset>(t => t.ToUniversalTime(), t => t);

        void Table<T>(string name, Action<EntityTypeBuilder<T>> configure) where T : class
        {
            var e = m.Entity<T>();
            e.ToTable(name);
            foreach (var p in typeof(T).GetProperties())
                e.Property(p.Name).HasColumnName(Snake(p.Name));
            configure(e);
        }

        Table<SnapshotEntity>("ducklake_snapshot", e =>
        {
            e.HasKey(x => x.SnapshotId);
            e.Property(x => x.SnapshotId).ValueGeneratedNever();
            if (sqlite) e.Property(x => x.SnapshotTime).HasConversion(time);
            else e.Property(x => x.SnapshotTime).HasConversion(utc);
        });
        Table<SnapshotChangesEntity>("ducklake_snapshot_changes", e => { e.HasKey(x => x.SnapshotId); e.Property(x => x.SnapshotId).ValueGeneratedNever(); });
        Table<SchemaEntity>("ducklake_schema", e =>
        {
            e.HasKey(x => x.SchemaId);
            e.Property(x => x.SchemaId).ValueGeneratedNever();
            if (sqlite) e.Property(x => x.SchemaUuid).HasConversion(uuid);
            if (!Is11) e.Ignore(x => x.ParentSchemaId);
        });
        Table<TableEntity>("ducklake_table", e =>
        {
            e.HasKey(x => new { x.TableId, x.BeginSnapshot });
            if (sqlite) e.Property(x => x.TableUuid).HasConversion(uuid);
        });
        Table<ColumnEntity>("ducklake_column", e => e.HasKey(x => new { x.TableId, x.ColumnId, x.BeginSnapshot }));
        Table<DataFileEntity>("ducklake_data_file", e =>
        {
            e.HasKey(x => x.DataFileId);
            e.Property(x => x.DataFileId).ValueGeneratedNever();
            if (!Is11) e.Ignore(x => x.RowGroupCount);
        });
        Table<DeleteFileEntity>("ducklake_delete_file", e =>
        {
            e.HasKey(x => x.DeleteFileId);
            e.Property(x => x.DeleteFileId).ValueGeneratedNever();
            if (!Is11) e.Ignore(x => x.RowGroupCount);
        });
        Table<FileColumnStatsEntity>("ducklake_file_column_stats", e =>
        {
            e.HasKey(x => new { x.DataFileId, x.ColumnId });
            if (!Is11) { e.Ignore(x => x.MinIsExact); e.Ignore(x => x.MaxIsExact); }
        });
        Table<TableStatsEntity>("ducklake_table_stats", e => { e.HasKey(x => x.TableId); e.Property(x => x.TableId).ValueGeneratedNever(); });
        Table<TableColumnStatsEntity>("ducklake_table_column_stats", e =>
        {
            e.HasKey(x => new { x.TableId, x.ColumnId });
            if (!Is11) { e.Ignore(x => x.MinIsExact); e.Ignore(x => x.MaxIsExact); }
        });
        Table<PartitionInfoEntity>("ducklake_partition_info", e => { e.HasKey(x => x.PartitionId); e.Property(x => x.PartitionId).ValueGeneratedNever(); });
        Table<PartitionColumnEntity>("ducklake_partition_column", e => e.HasKey(x => new { x.PartitionId, x.PartitionKeyIndex }));
        Table<FilePartitionValueEntity>("ducklake_file_partition_value", e => e.HasKey(x => new { x.DataFileId, x.PartitionKeyIndex }));
        Table<SchemaVersionEntity>("ducklake_schema_versions", e => e.HasKey(x => new { x.TableId, x.BeginSnapshot }));
        Table<InlinedDataTableEntity>("ducklake_inlined_data_tables", e => e.HasKey(x => new { x.TableId, x.SchemaVersion }));
        Table<MetadataEntity>("ducklake_metadata", e => e.HasKey(x => new { x.Key, x.Value }));
        Table<FileScheduledForDeletionEntity>("ducklake_files_scheduled_for_deletion", e =>
        {
            e.HasKey(x => new { x.DataFileId, x.Path });
            if (sqlite) e.Property(x => x.ScheduleStart).HasConversion(time);
            else e.Property(x => x.ScheduleStart).HasConversion(utc);
        });
        Table<TagEntity>("ducklake_tag", e => e.HasKey(x => new { x.ObjectId, x.Key, x.BeginSnapshot }));
        Table<ColumnTagEntity>("ducklake_column_tag", e => e.HasKey(x => new { x.TableId, x.ColumnId, x.Key, x.BeginSnapshot }));
        Table<ViewEntity>("ducklake_view", e =>
        {
            e.HasKey(x => new { x.ViewId, x.BeginSnapshot });
            if (sqlite) e.Property(x => x.ViewUuid).HasConversion(uuid);
        });
        if (Is11) Table<ViewColumnTagEntity>("ducklake_view_column_tag", e => e.HasKey(x => new { x.ViewId, x.ColumnName, x.Key, x.BeginSnapshot }));
        else m.Ignore<ViewColumnTagEntity>();
        Table<MacroEntity>("ducklake_macro", e => e.HasKey(x => new { x.MacroId, x.BeginSnapshot }));
        Table<MacroImplEntity>("ducklake_macro_impl", e => e.HasKey(x => new { x.MacroId, x.ImplId }));
        Table<MacroParameterEntity>("ducklake_macro_parameters", e => e.HasKey(x => new { x.MacroId, x.ImplId, x.ColumnId }));
        Table<SortInfoEntity>("ducklake_sort_info", e => { e.HasKey(x => x.SortId); e.Property(x => x.SortId).ValueGeneratedNever(); });
        Table<SortExpressionEntity>("ducklake_sort_expression", e => e.HasKey(x => new { x.SortId, x.SortKeyIndex }));
        Table<ColumnMappingEntity>("ducklake_column_mapping", e => { e.HasKey(x => x.MappingId); e.Property(x => x.MappingId).ValueGeneratedNever(); });
        Table<NameMappingEntity>("ducklake_name_mapping", e => e.HasKey(x => new { x.MappingId, x.ColumnId }));
    }

    internal static string Snake(string pascal)
    {
        var sb = new System.Text.StringBuilder();
        for (var i = 0; i < pascal.Length; i++)
        {
            if (char.IsUpper(pascal[i]) && i > 0) sb.Append('_');
            sb.Append(char.ToLowerInvariant(pascal[i]));
        }
        return sb.ToString();
    }
}

internal static class VersionedQueries
{
    /// <summary>The rows valid in snapshot <paramref name="s"/> (spec "Queries": <c>s &gt;= begin_snapshot AND (s &lt; end_snapshot OR end_snapshot IS NULL)</c>).</summary>
    public static IQueryable<T> LiveAt<T>(this IQueryable<T> rows, long s) where T : class, IVersioned =>
        rows.Where(x => x.BeginSnapshot <= s && (x.EndSnapshot == null || s < x.EndSnapshot));

    /// <summary>The rows not yet ended (the latest state, for commits).</summary>
    public static IQueryable<T> Current<T>(this IQueryable<T> rows) where T : class, IVersioned =>
        rows.Where(x => x.EndSnapshot == null);
}

/// <summary>
/// Multi-row <c>INSERT</c>s of catalog rows, from the EF model's own mapping (table and column names, value converters,
/// the columns a format version lacks). EF Core's SQLite provider sends one statement per row and its PostgreSQL
/// provider one batch per <c>SaveChanges</c>: a commit of many files would otherwise cost a command per row.
/// </summary>
internal static class BulkInsert
{
    /// <summary>Parameters per statement: under SQLite's 32,766 and PostgreSQL's 65,535.</summary>
    private const int MaxParameters = 30_000;

    public static async Task InsertAsync<T>(DuckLakeDbContext db, IReadOnlyCollection<T> rows, CancellationToken ct) where T : class
    {
        if (rows.Count == 0) return;
        var entity = db.Model.FindEntityType(typeof(T)) ?? throw new InvalidOperationException($"{typeof(T).Name} is not in the model");
        var table = entity.GetTableName()!;
        var store = Microsoft.EntityFrameworkCore.Metadata.StoreObjectIdentifier.Table(table, entity.GetSchema());
        var columns = entity.GetProperties().Select(p => (
            Name: p.GetColumnName(store) ?? throw new InvalidOperationException($"{p.Name} has no column"),
            Get: p.PropertyInfo ?? throw new InvalidOperationException($"{p.Name} has no CLR property"),
            Convert: p.GetValueConverter() ?? p.GetTypeMapping().Converter)).ToList();
        var head = $"INSERT INTO {Db.Quote(table)} ({string.Join(", ", columns.Select(c => Db.Quote(c.Name)))}) VALUES ";
        var perStatement = Math.Max(1, Math.Min(1000, MaxParameters / columns.Count));
        await db.Database.OpenConnectionAsync(ct);
        foreach (var chunk in rows.Chunk(perStatement))
        {
            var args = new object?[chunk.Length * columns.Count];
            var sql = new System.Text.StringBuilder(head);
            for (var r = 0; r < chunk.Length; r++)
            {
                sql.Append(r == 0 ? "(" : ", (");
                for (var c = 0; c < columns.Count; c++)
                {
                    var i = r * columns.Count + c;
                    var value = columns[c].Get.GetValue(chunk[r]);
                    args[i] = value is null || columns[c].Convert is not { } convert ? value : convert.ConvertToProvider(value);
                    sql.Append(c == 0 ? "@p" : ", @p").Append(i.ToString(CultureInfo.InvariantCulture));
                }
                sql.Append(')');
            }
            var (connection, tx) = db.Raw();
            await Db.ExecAsync(connection, tx, sql.ToString(), ct, args);
        }
    }
}
