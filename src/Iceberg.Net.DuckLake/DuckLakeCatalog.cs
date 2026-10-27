using System.Data.Common;
using System.Runtime.CompilerServices;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.IO;
using Iceberg.Net.Manifests;
using Iceberg.Net.Types;

namespace Iceberg.Net.DuckLake;

public sealed record DuckLakeOptions
{
    public DuckLakeDialect Dialect { get; init; } = DuckLakeDialect.Sqlite;
    /// <summary>How many times a commit that lost the race for the next snapshot ID is retried (when nothing conflicts).</summary>
    public int MaxCommitAttempts { get; init; } = 10;
    /// <summary>Written to <c>ducklake_metadata.created_by</c> when this library creates a catalog.</summary>
    public string CreatedBy { get; init; } = "Iceberg.NET";
    public ParquetWriteOptions Parquet { get; init; } = ParquetWriteOptions.Default;
}

/// <summary>A row of <c>ducklake_snapshot</c>, with its <c>ducklake_snapshot_changes</c>.</summary>
public sealed record DuckLakeSnapshot(long SnapshotId, DateTimeOffset Time, long SchemaVersion, long NextCatalogId, long NextFileId)
{
    public IReadOnlyList<DuckLakeChange> Changes { get; init; } = [];
    public string? Author { get; init; }
    public string? CommitMessage { get; init; }
    public string? CommitExtraInfo { get; init; }
}

/// <summary>A partition key of a table (<c>ducklake_partition_column</c>).</summary>
public sealed record DuckLakePartitionKey(long ColumnId, string Transform);

/// <summary>A table as of one snapshot: its columns (as DuckLake columns and as the Iceberg schema files are read with), location and partitioning.</summary>
public sealed class DuckLakeTable
{
    public required long TableId { get; init; }
    public required Guid TableUuid { get; init; }
    public required long SchemaId { get; init; }
    public required string SchemaName { get; init; }
    public required string Name { get; init; }
    /// <summary>The snapshot this view of the table was loaded at.</summary>
    public required long SnapshotId { get; init; }
    public required IReadOnlyList<DuckLakeColumn> Columns { get; init; }
    public required Schema Schema { get; init; }
    /// <summary>Directory the table's relative file paths resolve against (ends in <c>/</c>).</summary>
    public required string Location { get; init; }
    public long? PartitionId { get; init; }
    public IReadOnlyList<DuckLakePartitionKey> PartitionKeys { get; init; } = [];

    public DuckLakeColumn Column(string name) =>
        Columns.FirstOrDefault(c => c.Name == name) ?? throw new KeyNotFoundException($"Table {SchemaName}.{Name} has no column '{name}'");

    public override string ToString() => $"{SchemaName}.{Name}@{SnapshotId}";
}

/// <summary>A data file a scan reads, with the delete file that applies to it in the scanned snapshot.</summary>
public sealed record DuckLakeFileTask(long DataFileId, string Location, long RecordCount, long FileSizeBytes, string? DeleteFileLocation, long RowIdStart);

/// <summary>Which files a scan reads and how many the file statistics ruled out.</summary>
public sealed record DuckLakeScanPlan(IReadOnlyList<DuckLakeFileTask> Files, int PrunedFiles, IReadOnlyList<string> InlinedTables)
{
    /// <summary>Positions deleted through DuckDB's deletion inlining (<c>ducklake_inlined_delete_{table_id}</c>), by data file.</summary>
    internal IReadOnlyDictionary<long, HashSet<long>> InlinedDeletions { get; init; } = new Dictionary<long, HashSet<long>>();
}

/// <summary>A row with its DuckLake row ID (spec "Row Lineage": <c>row_id_start</c> + position, or the inlined <c>row_id</c>).</summary>
public sealed record DuckLakeRow(long RowId, object?[] Values);

public sealed class DuckLakeNotFoundException(string message) : KeyNotFoundException(message);

/// <summary>
/// A DuckLake 1.0 catalog: metadata in SQL tables of any ADO.NET database (spec "Building Blocks"), data in Parquet
/// files through an <see cref="IFileIO"/>. Reads take a snapshot (latest by default) and see exactly the catalog rows
/// valid in it; writes go through <see cref="DuckLakeTransaction"/>, one snapshot per commit.
/// </summary>
public sealed class DuckLakeCatalog
{
    /// <summary>The format version this library reads and writes (<c>ducklake_metadata.version</c>).</summary>
    public const string FormatVersion = "1.0";

    private readonly Func<DbConnection> _connect;

    private DuckLakeCatalog(Func<DbConnection> connect, IFileIO io, DuckLakeOptions options, string dataPath)
    {
        _connect = connect;
        IO = io;
        Options = options;
        DataPath = dataPath;
    }

    public IFileIO IO { get; }
    public DuckLakeOptions Options { get; }
    /// <summary><c>ducklake_metadata.data_path</c>: the root relative paths resolve against (ends in <c>/</c>).</summary>
    public string DataPath { get; }

    /// <summary>
    /// Create the catalog tables in an empty database and commit snapshot 0, which creates the <c>main</c> schema (as
    /// DuckDB does, so a catalog made here is indistinguishable to it from one it made).
    /// </summary>
    public static async Task<DuckLakeCatalog> CreateAsync(Func<DbConnection> connect, IFileIO io, string dataPath, DuckLakeOptions? options = null,
        CancellationToken ct = default)
    {
        options ??= new DuckLakeOptions();
        if (!dataPath.EndsWith('/')) dataPath += "/";
        await using var c = await OpenConnectionAsync(connect, ct);
        await using var tx = await c.BeginTransactionAsync(ct);
        foreach (var ddl in options.Dialect.Ddl()) await Db.ExecAsync(c, tx, ddl, ct);
        const string meta = "INSERT INTO ducklake_metadata(\"key\", \"value\", \"scope\", scope_id) VALUES (@p0, @p1, NULL, NULL)";
        await Db.ExecAsync(c, tx, meta, ct, "version", FormatVersion);
        await Db.ExecAsync(c, tx, meta, ct, "created_by", options.CreatedBy);
        await Db.ExecAsync(c, tx, meta, ct, "data_path", dataPath);
        await Db.ExecAsync(c, tx, meta, ct, "encrypted", "false");
        await Db.ExecAsync(c, tx, "INSERT INTO ducklake_snapshot(snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) VALUES (0, @p0, 0, 1, 0)",
            ct, options.Dialect.TimestampTz(DateTimeOffset.UtcNow));
        await Db.ExecAsync(c, tx, "INSERT INTO ducklake_snapshot_changes(snapshot_id, changes_made, author, commit_message, commit_extra_info) VALUES (0, @p0, NULL, NULL, NULL)",
            ct, $"{DuckLakeChange.CreatedSchema}:{DuckLakeChange.QuoteName("main")}");
        await Db.ExecAsync(c, tx, "INSERT INTO ducklake_schema(schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative) VALUES (0, @p0, 0, NULL, 'main', 'main/', @p1)",
            ct, options.Dialect.Uuid(Guid.NewGuid()), true);
        await tx.CommitAsync(ct);
        return new DuckLakeCatalog(connect, io, options, dataPath);
    }

    /// <summary>Attach to an existing catalog. <paramref name="dataPathOverride"/> replaces the stored <c>data_path</c> (e.g. a mounted copy).</summary>
    public static async Task<DuckLakeCatalog> OpenAsync(Func<DbConnection> connect, IFileIO io, DuckLakeOptions? options = null,
        string? dataPathOverride = null, CancellationToken ct = default)
    {
        options ??= new DuckLakeOptions();
        await using var c = await OpenConnectionAsync(connect, ct);
        var meta = await Db.QueryAsync(c, null, "SELECT \"key\", \"value\" FROM ducklake_metadata WHERE \"scope\" IS NULL",
            r => (Db.ReqStr(r, 0), Db.ReqStr(r, 1)), ct);
        var settings = meta.GroupBy(m => m.Item1).ToDictionary(g => g.Key, g => g.Last().Item2);
        var version = settings.GetValueOrDefault("version") ?? throw new InvalidDataException("ducklake_metadata has no version");
        if (version != FormatVersion)
            throw new NotSupportedException($"DuckLake format version {version}; Iceberg.Net.DuckLake reads and writes {FormatVersion} (migrate the catalog with DuckDB first)");
        if (settings.GetValueOrDefault("encrypted") == "true")
            throw new NotSupportedException("Encrypted DuckLake catalogs are not supported");
        var dataPath = dataPathOverride ?? settings.GetValueOrDefault("data_path") ?? throw new InvalidDataException("ducklake_metadata has no data_path");
        if (!dataPath.EndsWith('/')) dataPath += "/";
        return new DuckLakeCatalog(connect, io, options, dataPath);
    }

    private static async Task<DbConnection> OpenConnectionAsync(Func<DbConnection> connect, CancellationToken ct)
    {
        var c = connect();
        if (c.State != System.Data.ConnectionState.Open) await c.OpenAsync(ct);
        return c;
    }

    internal Task<DbConnection> ConnectAsync(CancellationToken ct) => OpenConnectionAsync(_connect, ct);

    // ---- snapshots -------------------------------------------------------------------------------------------------

    private const string SnapshotColumns =
        "s.snapshot_id, s.snapshot_time, s.schema_version, s.next_catalog_id, s.next_file_id, c.changes_made, c.author, c.commit_message, c.commit_extra_info";

    private static DuckLakeSnapshot ReadSnapshot(DbDataReader r) =>
        new(Db.ReqLong(r, 0), Db.Timestamp(r, 1), Db.ReqLong(r, 2), Db.ReqLong(r, 3), Db.ReqLong(r, 4))
        {
            Changes = DuckLakeChange.Parse(Db.Str(r, 5)),
            Author = Db.Str(r, 6),
            CommitMessage = Db.Str(r, 7),
            CommitExtraInfo = Db.Str(r, 8),
        };

    internal static async Task<DuckLakeSnapshot> LatestSnapshotAsync(DbConnection c, DbTransaction? tx, CancellationToken ct) =>
        (await Db.QueryAsync(c, tx,
            $"SELECT {SnapshotColumns} FROM ducklake_snapshot s LEFT JOIN ducklake_snapshot_changes c ON c.snapshot_id = s.snapshot_id " +
            "WHERE s.snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot)", ReadSnapshot, ct)).SingleOrDefault()
        ?? throw new InvalidDataException("ducklake_snapshot is empty");

    public async Task<DuckLakeSnapshot> CurrentSnapshotAsync(CancellationToken ct = default)
    {
        await using var c = await ConnectAsync(ct);
        return await LatestSnapshotAsync(c, null, ct);
    }

    public async Task<IReadOnlyList<DuckLakeSnapshot>> ListSnapshotsAsync(CancellationToken ct = default)
    {
        await using var c = await ConnectAsync(ct);
        return await Db.QueryAsync(c, null,
            $"SELECT {SnapshotColumns} FROM ducklake_snapshot s LEFT JOIN ducklake_snapshot_changes c ON c.snapshot_id = s.snapshot_id ORDER BY s.snapshot_id",
            ReadSnapshot, ct);
    }

    /// <summary>The latest snapshot committed at or before <paramref name="at"/> (time travel by timestamp).</summary>
    public async Task<DuckLakeSnapshot> SnapshotAsOfAsync(DateTimeOffset at, CancellationToken ct = default) =>
        (await ListSnapshotsAsync(ct)).LastOrDefault(s => s.Time <= at)
        ?? throw new DuckLakeNotFoundException($"No snapshot at or before {at:O}");

    private async Task<long> ResolveSnapshotAsync(DbConnection c, long? snapshotId, CancellationToken ct)
    {
        if (snapshotId is null) return (await LatestSnapshotAsync(c, null, ct)).SnapshotId;
        var found = await Db.QueryAsync(c, null, "SELECT snapshot_id FROM ducklake_snapshot WHERE snapshot_id = @p0", r => Db.ReqLong(r, 0), ct, null, snapshotId);
        return found.Count == 1 ? found[0] : throw new DuckLakeNotFoundException($"Snapshot {snapshotId} does not exist (expired?)");
    }

    // ---- schemas and tables ----------------------------------------------------------------------------------------

    internal sealed record SchemaRow(long SchemaId, string Name, string? Path, bool Relative);
    internal sealed record TableRow(long TableId, Guid Uuid, long SchemaId, string Name, string? Path, bool Relative);

    internal static Task<List<SchemaRow>> SchemasAsync(DbConnection c, DbTransaction? tx, long snapshot, CancellationToken ct) =>
        Db.QueryAsync(c, tx, $"SELECT schema_id, schema_name, path, path_is_relative FROM ducklake_schema WHERE {Db.Live} ORDER BY schema_id",
            r => new SchemaRow(Db.ReqLong(r, 0), Db.ReqStr(r, 1), Db.Str(r, 2), Db.Bool(r, 3) ?? true), ct, snapshot);

    internal static Task<List<TableRow>> TablesAsync(DbConnection c, DbTransaction? tx, long snapshot, CancellationToken ct) =>
        Db.QueryAsync(c, tx, $"SELECT table_id, table_uuid, schema_id, table_name, path, path_is_relative FROM ducklake_table WHERE {Db.Live} ORDER BY table_id",
            r => new TableRow(Db.ReqLong(r, 0), Db.Uuid(r, 1) ?? Guid.Empty, Db.ReqLong(r, 2), Db.ReqStr(r, 3), Db.Str(r, 4), Db.Bool(r, 5) ?? true), ct, snapshot);

    public async Task<IReadOnlyList<string>> ListSchemasAsync(long? snapshotId = null, CancellationToken ct = default)
    {
        await using var c = await ConnectAsync(ct);
        var s = await ResolveSnapshotAsync(c, snapshotId, ct);
        return (await SchemasAsync(c, null, s, ct)).Select(x => x.Name).ToList();
    }

    public async Task<IReadOnlyList<string>> ListTablesAsync(string schema, long? snapshotId = null, CancellationToken ct = default)
    {
        await using var c = await ConnectAsync(ct);
        var s = await ResolveSnapshotAsync(c, snapshotId, ct);
        var sc = Find(await SchemasAsync(c, null, s, ct), x => x.Name, schema, "schema");
        return (await TablesAsync(c, null, s, ct)).Where(t => t.SchemaId == sc.SchemaId).Select(t => t.Name).ToList();
    }

    /// <summary>Names match exactly, else case-insensitively when that is unambiguous (DuckDB identifiers are case-insensitive).</summary>
    internal static T Find<T>(IEnumerable<T> items, Func<T, string> name, string wanted, string what)
    {
        var list = items.ToList();
        var exact = list.Where(x => name(x) == wanted).ToList();
        if (exact.Count == 1) return exact[0];
        var loose = list.Where(x => string.Equals(name(x), wanted, StringComparison.OrdinalIgnoreCase)).ToList();
        return loose.Count == 1 ? loose[0] : throw new DuckLakeNotFoundException($"No {what} named '{wanted}'");
    }

    public async Task<DuckLakeTable> LoadTableAsync(string schema, string table, long? snapshotId = null, CancellationToken ct = default)
    {
        await using var c = await ConnectAsync(ct);
        var s = await ResolveSnapshotAsync(c, snapshotId, ct);
        return await LoadTableAsync(c, null, schema, table, s, ct);
    }

    internal async Task<DuckLakeTable> LoadTableAsync(DbConnection c, DbTransaction? tx, string schema, string table, long snapshot, CancellationToken ct)
    {
        var sc = Find(await SchemasAsync(c, tx, snapshot, ct), x => x.Name, schema, "schema");
        var t = Find((await TablesAsync(c, tx, snapshot, ct)).Where(x => x.SchemaId == sc.SchemaId), x => x.Name, table, $"table in schema {sc.Name}");
        var columns = await ColumnsAsync(c, tx, t.TableId, snapshot, ct);
        var partitions = await Db.QueryAsync(c, tx, $"SELECT partition_id FROM ducklake_partition_info WHERE table_id = @p0 AND {Db.Live}",
            r => Db.ReqLong(r, 0), ct, snapshot, t.TableId);
        if (partitions.Count > 1) throw new InvalidDataException($"Table {t.TableId} has {partitions.Count} live partition specs in snapshot {snapshot}");
        IReadOnlyList<DuckLakePartitionKey> keys = [];
        if (partitions.Count == 1)
            keys = await Db.QueryAsync(c, tx, "SELECT column_id, \"transform\" FROM ducklake_partition_column WHERE partition_id = @p0 AND table_id = @p1 ORDER BY partition_key_index",
                r => new DuckLakePartitionKey(Db.ReqLong(r, 0), Db.ReqStr(r, 1)), ct, null, partitions[0], t.TableId);
        var schemaLocation = Resolve(DataPath, sc.Path, sc.Relative);
        return new DuckLakeTable
        {
            TableId = t.TableId,
            TableUuid = t.Uuid,
            SchemaId = sc.SchemaId,
            SchemaName = sc.Name,
            Name = t.Name,
            SnapshotId = snapshot,
            Columns = columns,
            Schema = DuckLakeTypes.ToIceberg(columns),
            Location = Resolve(schemaLocation, t.Path, t.Relative),
            PartitionId = partitions.Count == 1 ? partitions[0] : null,
            PartitionKeys = keys,
        };
    }

    internal static async Task<IReadOnlyList<DuckLakeColumn>> ColumnsAsync(DbConnection c, DbTransaction? tx, long tableId, long snapshot, CancellationToken ct) =>
        DuckLakeTypes.Tree(await Db.QueryAsync(c, tx,
            $"SELECT column_id, column_order, column_name, column_type, nulls_allowed, parent_column, initial_default FROM ducklake_column WHERE table_id = @p0 AND {Db.Live}",
            r => new ColumnRow(Db.ReqLong(r, 0), Db.Long(r, 1) ?? Db.ReqLong(r, 0), Db.ReqStr(r, 2), Db.ReqStr(r, 3), Db.Bool(r, 4) ?? true, Db.Long(r, 5), Db.Str(r, 6)),
            ct, snapshot, tableId));

    /// <summary>
    /// A path under its parent (spec "Note on Paths": <c>path_is_relative</c>). A null path means the parent itself, as in
    /// catalogs written before schemas and tables had paths.
    /// </summary>
    internal static string Resolve(string parent, string? path, bool relative)
    {
        if (string.IsNullOrEmpty(path)) return parent;
        return relative ? parent + path : path;
    }

    // ---- scans -----------------------------------------------------------------------------------------------------

    internal sealed record DataFileRow(long Id, string Path, bool Relative, string Format, long RecordCount, long FileSize, long RowIdStart,
        string? EncryptionKey, long? MappingId, long? PartialMax, long BeginSnapshot);

    internal sealed record DeleteFileRow(long Id, long DataFileId, string Path, bool Relative, string Format, long DeleteCount, string? EncryptionKey, long? PartialMax);

    internal static Task<List<DataFileRow>> DataFilesAsync(DbConnection c, DbTransaction? tx, long tableId, long snapshot, CancellationToken ct) =>
        Db.QueryAsync(c, tx,
            "SELECT data_file_id, path, path_is_relative, file_format, record_count, file_size_bytes, row_id_start, encryption_key, mapping_id, partial_max, begin_snapshot " +
            $"FROM ducklake_data_file WHERE table_id = @p0 AND {Db.Live} ORDER BY file_order, data_file_id",
            r => new DataFileRow(Db.ReqLong(r, 0), Db.ReqStr(r, 1), Db.Bool(r, 2) ?? true, Db.Str(r, 3) ?? "parquet", Db.ReqLong(r, 4),
                Db.ReqLong(r, 5), Db.Long(r, 6) ?? 0, Db.Str(r, 7), Db.Long(r, 8), Db.Long(r, 9), Db.ReqLong(r, 10)),
            ct, snapshot, tableId);

    internal static async Task<Dictionary<long, DeleteFileRow>> DeleteFilesAsync(DbConnection c, DbTransaction? tx, long tableId, long snapshot, CancellationToken ct)
    {
        var rows = await Db.QueryAsync(c, tx,
            "SELECT delete_file_id, data_file_id, path, path_is_relative, format, delete_count, encryption_key, partial_max " +
            $"FROM ducklake_delete_file WHERE table_id = @p0 AND {Db.Live}",
            r => new DeleteFileRow(Db.ReqLong(r, 0), Db.ReqLong(r, 1), Db.ReqStr(r, 2), Db.Bool(r, 3) ?? true, Db.Str(r, 4) ?? "parquet",
                Db.Long(r, 5) ?? 0, Db.Str(r, 6), Db.Long(r, 7)),
            ct, snapshot, tableId);
        var result = new Dictionary<long, DeleteFileRow>();
        foreach (var d in rows)
        {
            // Spec "Queries", SELECT: "There can be at most one delete file per file in a single snapshot."
            if (!result.TryAdd(d.DataFileId, d))
                throw new InvalidDataException($"Data file {d.DataFileId} has more than one delete file in snapshot {snapshot}");
        }
        return result;
    }

    private sealed record StatsRow(long DataFileId, long ColumnId, long? ValueCount, long? NullCount, string? Min, string? Max, bool? ContainsNan);

    /// <summary>Plan a scan: the live data files of the table's snapshot, minus those whose file statistics rule out <paramref name="filter"/>.</summary>
    public async Task<DuckLakeScanPlan> PlanScanAsync(DuckLakeTable table, Expression? filter = null, CancellationToken ct = default)
    {
        await using var c = await ConnectAsync(ct);
        return await PlanScanAsync(c, null, table, filter, ct);
    }

    internal async Task<DuckLakeScanPlan> PlanScanAsync(DbConnection c, DbTransaction? tx, DuckLakeTable table, Expression? filter, CancellationToken ct)
    {
        var files = await DataFilesAsync(c, tx, table.TableId, table.SnapshotId, ct);
        var deletes = await DeleteFilesAsync(c, tx, table.TableId, table.SnapshotId, ct);
        var bound = filter is null or AlwaysTrue ? null : Binder.Bind(table.Schema, filter);
        Dictionary<long, Dictionary<long, StatsRow>>? stats = null;
        if (bound is not null)
            stats = (await Db.QueryAsync(c, tx,
                    "SELECT data_file_id, column_id, value_count, null_count, min_value, max_value, contains_nan FROM ducklake_file_column_stats WHERE table_id = @p0",
                    r => new StatsRow(Db.ReqLong(r, 0), Db.ReqLong(r, 1), Db.Long(r, 2), Db.Long(r, 3), Db.Str(r, 4), Db.Str(r, 5), Db.Bool(r, 6)), ct, null, table.TableId))
                .GroupBy(s => s.DataFileId).ToDictionary(g => g.Key, g => g.GroupBy(s => s.ColumnId).ToDictionary(x => x.Key, x => x.First()));
        var types = table.Columns.SelectMany(col => col.SelfAndDescendants()).Where(col => col.Type is DuckLakePrimitive)
            .ToDictionary(col => col.Id, col => (DuckLakePrimitive)col.Type);
        var evaluator = bound is null ? null : new MetricsEvaluator(bound);
        var tasks = new List<DuckLakeFileTask>();
        var pruned = 0;
        foreach (var f in files)
        {
            if (f.EncryptionKey is not null) throw new NotSupportedException($"Data file {f.Id} is encrypted");
            if (!string.Equals(f.Format, "parquet", StringComparison.OrdinalIgnoreCase)) throw new NotSupportedException($"Data file {f.Id} has format {f.Format}");
            if (f.MappingId is not null) throw new NotSupportedException($"Data file {f.Id} needs name mapping {f.MappingId} (files added without field IDs)");
            if (evaluator is not null && !evaluator.MightMatch(f.RecordCount, (fieldId, type) => Stats(stats!, f.Id, fieldId, type, types)))
            {
                pruned++;
                continue;
            }
            string? deleteLocation = null;
            if (deletes.TryGetValue(f.Id, out var d))
            {
                if (d.EncryptionKey is not null) throw new NotSupportedException($"Delete file {d.Id} is encrypted");
                if (!string.Equals(d.Format, "parquet", StringComparison.OrdinalIgnoreCase))
                    throw new NotSupportedException($"Delete file {d.Id} has format {d.Format} (deletion vectors are not read yet)");
                deleteLocation = Resolve(table.Location, d.Path, d.Relative);
            }
            tasks.Add(new DuckLakeFileTask(f.Id, Resolve(table.Location, f.Path, f.Relative), f.RecordCount, f.FileSize, deleteLocation, f.RowIdStart));
        }
        var inlined = await Db.QueryAsync(c, tx, "SELECT table_name FROM ducklake_inlined_data_tables WHERE table_id = @p0 ORDER BY schema_version",
            r => Db.ReqStr(r, 0), ct, null, table.TableId);
        return new DuckLakeScanPlan(tasks, pruned, inlined) { InlinedDeletions = await InlinedDeletionsAsync(c, tx, table.TableId, table.SnapshotId, ct) };
    }

    /// <summary>The per-table deletion inlining table DuckDB creates on first use; absent until then.</summary>
    internal static string InlinedDeleteTable(long tableId) => $"ducklake_inlined_delete_{tableId}";

    internal async Task<bool> TableExistsAsync(DbConnection c, DbTransaction? tx, string name, CancellationToken ct) =>
        (await Db.QueryAsync(c, tx, Options.Dialect.TableExistsSql, r => Db.ReqLong(r, 0), ct, null, name))[0] > 0;

    /// <summary>Rows of a data file (<c>file_id</c>, position <c>row_id</c>) deleted in or before <paramref name="snapshot"/>.</summary>
    private async Task<Dictionary<long, HashSet<long>>> InlinedDeletionsAsync(DbConnection c, DbTransaction? tx, long tableId, long snapshot, CancellationToken ct)
    {
        var result = new Dictionary<long, HashSet<long>>();
        var name = InlinedDeleteTable(tableId);
        if (!await TableExistsAsync(c, tx, name, ct)) return result;
        foreach (var (file, pos) in await Db.QueryAsync(c, tx, $"SELECT file_id, row_id FROM {Db.Quote(name)} WHERE begin_snapshot <= @s",
                     r => (Db.ReqLong(r, 0), Db.ReqLong(r, 1)), ct, snapshot))
            (result.TryGetValue(file, out var set) ? set : result[file] = []).Add(pos);
        return result;
    }

    /// <summary>
    /// Positions of a data file deleted as of <paramref name="snapshot"/>: its delete file's (a partial delete file's only
    /// up to the snapshot) plus inlined deletions.
    /// </summary>
    internal async Task<HashSet<long>?> DeletedPositionsAsync(DuckLakeScanPlan plan, DuckLakeFileTask file, long snapshot, CancellationToken ct)
    {
        var deleted = file.DeleteFileLocation is { } location ? await DuckLakeDeleteFiles.ReadPositionsAsync(IO, location, snapshot, ct) : null;
        if (plan.InlinedDeletions.TryGetValue(file.DataFileId, out var inlined)) (deleted ??= []).UnionWith(inlined);
        return deleted;
    }

    /// <summary>
    /// A file's DuckLake statistics as Iceberg typed statistics. DuckDB's <c>value_count</c> excludes nulls, so the
    /// Iceberg count (which includes them) is <c>value_count + null_count</c>; under the spec's reading (nulls
    /// included) that overstates, which only ever stops pruning, never prunes wrongly.
    /// </summary>
    private static ColumnStats? Stats(Dictionary<long, Dictionary<long, StatsRow>> stats, long fileId, int fieldId, IcebergType type,
        Dictionary<long, DuckLakePrimitive> types)
    {
        if (!stats.TryGetValue(fileId, out var byColumn) || !byColumn.TryGetValue(fieldId, out var s) || !types.TryGetValue(fieldId, out var dl)) return null;
        object? lower = null, upper = null;
        if (s.Min is not null && s.Max is not null && DuckLakeValues.TryParse(dl, s.Min, out var lo) && DuckLakeValues.TryParse(dl, s.Max, out var hi)
            && !ValueComparer.IsNaN(lo) && !ValueComparer.IsNaN(hi) && SameKind(lo!, type))
        {
            lower = lo;
            upper = hi;
        }
        return new ColumnStats
        {
            FieldId = fieldId,
            Type = type,
            LowerBound = lower,
            UpperBound = upper,
            ValueCount = s.ValueCount is { } v && s.NullCount is { } n ? v + n : null,
            NullValueCount = s.NullCount,
            NanValueCount = s.ContainsNan == false ? 0 : null,
        };
    }

    private static bool SameKind(object value, IcebergType type) => (value, type.TypeId) switch
    {
        (int, TypeId.Int or TypeId.Date) or (long, TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs) => true,
        (float, TypeId.Float) or (double, TypeId.Double) or (bool, TypeId.Boolean) or (string, TypeId.String) or (Guid, TypeId.Uuid) => true,
        (IcebergDecimal, TypeId.Decimal) => true,
        _ => false,
    };

    /// <summary>Every visible row of the table's snapshot (matching <paramref name="filter"/>, when given), in table column order.</summary>
    public async Task<List<object?[]>> ReadAsync(DuckLakeTable table, Expression? filter = null, CancellationToken ct = default)
    {
        var rows = new List<object?[]>();
        await foreach (var r in ScanAsync(table, filter, ct)) rows.Add(r.Values);
        return rows;
    }

    /// <summary>As <see cref="ReadAsync"/>, with row IDs.</summary>
    public async IAsyncEnumerable<DuckLakeRow> ReadRowsAsync(DuckLakeTable table, Expression? filter = null, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var r in ScanAsync(table, filter, ct)) yield return new DuckLakeRow(r.RowId, r.Values);
    }

    /// <summary>Field ID of the row ID column DuckDB embeds in files whose rows do not have consecutive row IDs (merged files).</summary>
    public const int RowIdFieldId = 2147483540;
    /// <summary>Field ID of the per-row snapshot column of partial data and delete files (spec <c>partial_max</c>).</summary>
    public const int SnapshotIdFieldId = 2147483539;

    private static readonly NestedField[] InternalColumns =
    [
        NestedField.Optional(RowIdFieldId, "_ducklake_internal_row_id", PrimitiveType.Long),
        NestedField.Optional(SnapshotIdFieldId, "_ducklake_internal_snapshot_id", PrimitiveType.Long),
    ];

    internal readonly record struct ScannedRow(long RowId, object?[] Values, long? DataFileId, long Position, string? InlinedTable);

    internal async IAsyncEnumerable<ScannedRow> ScanAsync(DuckLakeTable table, Expression? filter, [EnumeratorCancellation] CancellationToken ct)
    {
        DuckLakeScanPlan plan;
        await using (var c = await ConnectAsync(ct)) plan = await PlanScanAsync(c, null, table, filter, ct);
        var bound = filter is null or AlwaysTrue ? null : Binder.Bind(table.Schema, filter);
        var residual = bound is null ? null : RowEvaluator.Compile(bound);
        var width = table.Columns.Count;
        var projection = new Schema(0, table.Schema.Columns.Concat(InternalColumns));
        foreach (var file in plan.Files)
        {
            var deleted = await DeletedPositionsAsync(plan, file, table.SnapshotId, ct);
            var options = new DataFileReadOptions { FileSize = file.FileSizeBytes, Filter = bound };
            await foreach (var batch in ParquetFiles.ReadBatchesAsync(IO, file.Location, projection, null, options, ct))
            {
                for (var i = 0; i < batch.Rows.Count; i++)
                {
                    var pos = batch.FirstPosition + i;
                    if (deleted is not null && deleted.Contains(pos)) continue;
                    var full = batch.Rows[i];
                    // Partial files (merged by DuckDB) carry each row's snapshot: rows committed after the scanned one are not visible.
                    if (full[width + 1] is long rowSnapshot && rowSnapshot > table.SnapshotId) continue;
                    var row = full[..width];
                    if (residual is not null && !residual(row)) continue;
                    yield return new ScannedRow(full[width] is long rowId ? rowId : file.RowIdStart + pos, row, file.DataFileId, pos, null);
                }
            }
        }
        foreach (var name in plan.InlinedTables)
        {
            List<(long RowId, object?[] Values)> rows;
            await using (var c = await ConnectAsync(ct)) rows = await DuckLakeInlinedData.ReadAsync(c, table, name, ct);
            foreach (var (rowId, values) in rows)
            {
                if (residual is not null && !residual(values)) continue;
                yield return new ScannedRow(rowId, values, null, rowId, name);
            }
        }
    }

    // ---- writes ----------------------------------------------------------------------------------------------------

    /// <summary>Start a transaction; nothing is visible to readers until <see cref="DuckLakeTransaction.CommitAsync"/>.</summary>
    public DuckLakeTransaction BeginTransaction(string? author = null, string? commitMessage = null) => new(this, author, commitMessage);
}
