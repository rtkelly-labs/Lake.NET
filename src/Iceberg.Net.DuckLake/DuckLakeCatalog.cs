using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.IO;
using Iceberg.Net.Manifests;
using Iceberg.Net.Types;
using Microsoft.EntityFrameworkCore;

namespace Iceberg.Net.DuckLake;

public sealed record DuckLakeOptions
{
    /// <summary>How many times a commit that lost the race for the next snapshot ID is retried (when nothing conflicts).</summary>
    public int MaxCommitAttempts { get; init; } = 10;
    /// <summary>The wait before the first retry; later ones grow by half each time (with jitter), up to <see cref="MaxRetryWait"/>.</summary>
    public TimeSpan RetryWait { get; init; } = TimeSpan.FromMilliseconds(10);
    public TimeSpan MaxRetryWait { get; init; } = TimeSpan.FromSeconds(1);
    /// <summary>Written to <c>ducklake_metadata.created_by</c> when this library creates a catalog.</summary>
    public string CreatedBy { get; init; } = "Iceberg.NET";
    /// <summary>
    /// Parquet layout of written files. Row groups of 122,880 rows by default (DuckDB's size): readers skip row groups
    /// by their statistics, and a scan holds at most a few row groups per file in memory.
    /// </summary>
    public ParquetWriteOptions Parquet { get; init; } = ParquetWriteOptions.Default with { RowGroupRows = DefaultRowGroupRows };
    /// <summary>
    /// How many data files a scan reads ahead concurrently (decoding is CPU-bound; object storage reads are
    /// latency-bound). Rows still come out in file order. 1 reads one file at a time.
    /// </summary>
    public int MaxParallelFileReads { get; init; } = 4;
    /// <summary>
    /// Data inlining (DuckDB's <c>data_inlining_row_limit</c>): an insert of at most this many rows is written into the
    /// catalog database instead of a Parquet file, and a delete of at most this many rows from one data file is recorded
    /// there instead of in a delete file. 0 (the default) turns it off; DuckDB's default is 10. A limit persisted in the
    /// lake (<c>ducklake_metadata</c>, per table, schema or catalog) takes precedence, as in DuckDB.
    /// </summary>
    public int DataInliningRowLimit { get; init; }

    public const int DefaultRowGroupRows = 122_880;
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
    /// <summary>The schema's path: its name, or for a nested schema (1.1) its names from the top, joined by <c>.</c>.</summary>
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
    /// <summary>The table's tags (<c>ducklake_tag</c>); SQL comments are the <c>comment</c> key.</summary>
    public IReadOnlyDictionary<string, string?> Tags { get; init; } = new Dictionary<string, string?>();
    /// <summary>Tags of the table's columns, by column ID (<c>ducklake_column_tag</c>).</summary>
    public IReadOnlyDictionary<long, IReadOnlyDictionary<string, string?>> ColumnTags { get; init; } = new Dictionary<long, IReadOnlyDictionary<string, string?>>();
    /// <summary>The sort order new data is written in (<c>ducklake_sort_info</c>), empty when none is set.</summary>
    public IReadOnlyList<DuckLakeSortKey> SortKeys { get; init; } = [];

    public string? Comment => Tags.GetValueOrDefault("comment");

    public string? ColumnComment(string column) =>
        ColumnTags.TryGetValue(Column(column).Id, out var tags) ? tags.GetValueOrDefault("comment") : null;

    public DuckLakeColumn Column(string name) =>
        Columns.FirstOrDefault(c => c.Name == name) ?? throw new KeyNotFoundException($"Table {SchemaName}.{Name} has no column '{name}'");

    public override string ToString() => $"{SchemaName}.{Name}@{SnapshotId}";
}

/// <summary>A data file a scan reads, with the delete file that applies to it in the scanned snapshot.</summary>
public sealed record DuckLakeFileTask(long DataFileId, string Location, long RecordCount, long FileSizeBytes, string? DeleteFileLocation, long? RowIdStart)
{
    /// <summary>Parquet row groups in the file, when the catalog records it (1.1).</summary>
    public long? RowGroupCount { get; init; }
    /// <summary>The file's name mapping, for files registered without (or with other) field IDs.</summary>
    internal DuckLakeNameMap? Mapping { get; init; }
}

/// <summary>Which files a scan reads and how many the file statistics ruled out.</summary>
public sealed record DuckLakeScanPlan(IReadOnlyList<DuckLakeFileTask> Files, int PrunedFiles, IReadOnlyList<string> InlinedTables)
{
    /// <summary>Positions deleted through DuckDB's deletion inlining (<c>ducklake_inlined_delete_{table_id}</c>), by data file.</summary>
    internal IReadOnlyDictionary<long, HashSet<long>> InlinedDeletions { get; init; } = new Dictionary<long, HashSet<long>>();
}

/// <summary>A row with its DuckLake row ID (spec "Row Lineage": <c>row_id_start</c> + position, or the stored row ID).</summary>
public sealed record DuckLakeRow(long RowId, object?[] Values);

public sealed class DuckLakeNotFoundException(string message) : KeyNotFoundException(message);

/// <summary>
/// A DuckLake catalog: metadata in SQL tables of a catalog database (<see cref="DuckLakeStore"/>, spec "Building
/// Blocks"), data in Parquet files through an <see cref="IFileIO"/>. Reads take a snapshot (latest by default) and see
/// exactly the catalog rows valid in it; writes go through <see cref="DuckLakeTransaction"/>, one snapshot per commit.
/// Format versions 1.0 and 1.1 are both read and written; a new catalog is 1.1.
/// </summary>
public sealed partial class DuckLakeCatalog
{
    public const string FormatVersion10 = "1.0";
    public const string FormatVersion11 = "1.1";
    /// <summary>The format version new catalogs are created with.</summary>
    public const string FormatVersion = FormatVersion11;
    public static readonly IReadOnlyList<string> SupportedVersions = [FormatVersion10, FormatVersion11];

    private DuckLakeCatalog(DuckLakeStore store, IFileIO io, DuckLakeOptions options, string dataPath, string version)
    {
        Store = store;
        IO = io;
        Options = options;
        DataPath = dataPath;
        Version = version;
    }

    public DuckLakeStore Store { get; }
    public IFileIO IO { get; }
    public DuckLakeOptions Options { get; }
    /// <summary><c>ducklake_metadata.data_path</c>: the root relative paths resolve against (ends in <c>/</c>).</summary>
    public string DataPath { get; }
    /// <summary><c>ducklake_metadata.version</c>.</summary>
    public string Version { get; }
    public bool Is11 => Version != FormatVersion10;

    /// <summary>
    /// Create the catalog tables of <paramref name="version"/> in an empty database and commit snapshot 0, which creates
    /// the <c>main</c> schema (as DuckDB does, so a catalog made here is indistinguishable to it from one it made).
    /// </summary>
    public static async Task<DuckLakeCatalog> CreateAsync(DuckLakeStore store, IFileIO io, string dataPath, DuckLakeOptions? options = null,
        string version = FormatVersion, CancellationToken ct = default)
    {
        if (!SupportedVersions.Contains(version)) throw new NotSupportedException($"DuckLake format version {version}");
        options ??= new DuckLakeOptions();
        if (!dataPath.EndsWith('/')) dataPath += "/";
        await using var db = store.Context(version);
        await db.Database.OpenConnectionAsync(ct);
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        var (c, t) = db.Raw();
        foreach (var ddl in store.Dialect.Ddl(version)) await Db.ExecAsync(c, t, ddl, ct);
        db.Metadata.AddRange(
            new MetadataEntity { Key = "version", Value = version },
            new MetadataEntity { Key = "created_by", Value = options.CreatedBy },
            new MetadataEntity { Key = "data_path", Value = dataPath },
            new MetadataEntity { Key = "encrypted", Value = "false" });
        db.Snapshots.Add(new SnapshotEntity { SnapshotId = 0, SnapshotTime = DateTimeOffset.UtcNow, SchemaVersion = 0, NextCatalogId = 1, NextFileId = 0 });
        db.SnapshotChanges.Add(new SnapshotChangesEntity { SnapshotId = 0, ChangesMade = $"{DuckLakeChange.CreatedSchema}:{DuckLakeChange.QuoteName("main")}" });
        db.Schemas.Add(new SchemaEntity { SchemaId = 0, SchemaUuid = Guid.NewGuid(), BeginSnapshot = 0, SchemaName = "main", Path = "main/", PathIsRelative = true });
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
        return new DuckLakeCatalog(store, io, options, dataPath, version);
    }

    /// <summary>Attach to an existing catalog. <paramref name="dataPathOverride"/> replaces the stored <c>data_path</c> (e.g. a mounted copy).</summary>
    public static async Task<DuckLakeCatalog> OpenAsync(DuckLakeStore store, IFileIO io, DuckLakeOptions? options = null,
        string? dataPathOverride = null, CancellationToken ct = default)
    {
        options ??= new DuckLakeOptions();
        await using var db = store.Context(FormatVersion10);
        var meta = await db.Metadata.Where(m => m.Scope == null).ToListAsync(ct);
        var settings = meta.GroupBy(m => m.Key).ToDictionary(g => g.Key, g => g.Last().Value);
        var version = settings.GetValueOrDefault("version") ?? throw new InvalidDataException("ducklake_metadata has no version");
        if (!SupportedVersions.Contains(version))
            throw new NotSupportedException($"DuckLake format version {version}; Iceberg.Net.DuckLake reads and writes {string.Join(" and ", SupportedVersions)}");
        if (settings.GetValueOrDefault("encrypted") == "true")
            throw new NotSupportedException("Encrypted DuckLake catalogs are not supported");
        var dataPath = dataPathOverride ?? settings.GetValueOrDefault("data_path") ?? throw new InvalidDataException("ducklake_metadata has no data_path");
        if (!dataPath.EndsWith('/')) dataPath += "/";
        return new DuckLakeCatalog(store, io, options, dataPath, version);
    }

    internal DuckLakeDbContext Context(DuckLakeTelemetry.Scope? scope = null) => Store.Context(Version, scope);

    /// <summary>
    /// Catalog reads under the <see cref="Corruption"/> contract: a row the provider cannot materialise (a NULL in a
    /// required column, text that is not a UUID or timestamp) is a format error naming the table.
    /// </summary>
    internal static async Task<T> Guarded<T>(string what, Func<Task<T>> read)
    {
        try
        {
            return await read();
        }
        catch (Exception e) when (e is InvalidCastException or FormatException or OverflowException or NullReferenceException
                                      || (e is InvalidOperationException && e.InnerException is InvalidCastException or FormatException or NullReferenceException))
        {
            throw new InvalidDataException($"Corrupt {what}: {e.GetType().Name}: {e.Message}", e);
        }
    }

    // ---- snapshots -------------------------------------------------------------------------------------------------

    private static async Task<List<DuckLakeSnapshot>> SnapshotsAsync(DuckLakeDbContext db, Func<IQueryable<SnapshotEntity>, IQueryable<SnapshotEntity>> filter,
        CancellationToken ct)
    {
        var snapshots = await Guarded("ducklake_snapshot", () => filter(db.Snapshots).OrderBy(s => s.SnapshotId).ToListAsync(ct));
        var ids = snapshots.Select(s => s.SnapshotId).ToList();
        var changes = (await Guarded("ducklake_snapshot_changes", () => db.SnapshotChanges.Where(c => ids.Contains(c.SnapshotId)).ToListAsync(ct)))
            .ToDictionary(c => c.SnapshotId);
        return Corruption.Guard("ducklake_snapshot_changes", () => snapshots.Select(s =>
        {
            changes.TryGetValue(s.SnapshotId, out var c);
            return new DuckLakeSnapshot(s.SnapshotId, s.SnapshotTime, s.SchemaVersion, s.NextCatalogId, s.NextFileId)
            {
                Changes = DuckLakeChange.Parse(c?.ChangesMade),
                Author = c?.Author,
                CommitMessage = c?.CommitMessage,
                CommitExtraInfo = c?.CommitExtraInfo,
            };
        }).ToList());
    }

    internal static async Task<DuckLakeSnapshot> LatestSnapshotAsync(DuckLakeDbContext db, CancellationToken ct)
    {
        var max = await db.Snapshots.MaxAsync(s => (long?)s.SnapshotId, ct) ?? throw new InvalidDataException("ducklake_snapshot is empty");
        return (await SnapshotsAsync(db, q => q.Where(s => s.SnapshotId == max), ct)).Single();
    }

    public async Task<DuckLakeSnapshot> CurrentSnapshotAsync(CancellationToken ct = default)
    {
        await using var db = Context();
        return await LatestSnapshotAsync(db, ct);
    }

    public async Task<IReadOnlyList<DuckLakeSnapshot>> ListSnapshotsAsync(CancellationToken ct = default)
    {
        await using var db = Context();
        return await SnapshotsAsync(db, q => q, ct);
    }

    /// <summary>The latest snapshot committed at or before <paramref name="at"/> (time travel by timestamp).</summary>
    public async Task<DuckLakeSnapshot> SnapshotAsOfAsync(DateTimeOffset at, CancellationToken ct = default) =>
        (await ListSnapshotsAsync(ct)).LastOrDefault(s => s.Time <= at)
        ?? throw new DuckLakeNotFoundException($"No snapshot at or before {at:O}");

    private static async Task<long> ResolveSnapshotAsync(DuckLakeDbContext db, long? snapshotId, CancellationToken ct)
    {
        if (snapshotId is null) return (await LatestSnapshotAsync(db, ct)).SnapshotId;
        return await db.Snapshots.AnyAsync(s => s.SnapshotId == snapshotId, ct)
            ? snapshotId.Value
            : throw new DuckLakeNotFoundException($"Snapshot {snapshotId} does not exist (expired?)");
    }

    // ---- schemas and tables ----------------------------------------------------------------------------------------

    /// <summary>A schema with its full path (nested schemas, 1.1) and the directory its tables' relative paths resolve against.</summary>
    internal sealed record SchemaRow(long SchemaId, string Name, IReadOnlyList<string> Parts, string Location, long? ParentId);

    internal sealed record TableRow(long TableId, Guid Uuid, long SchemaId, string Name, string? Path, bool Relative);

    /// <summary>
    /// The schemas valid in <paramref name="snapshot"/>, named by their path. A nested schema's relative path is relative
    /// to the catalog's <c>data_path</c>, not its parent's (spec <c>ducklake_schema</c>); every parent must be visible too.
    /// </summary>
    internal async Task<List<SchemaRow>> SchemasAsync(DuckLakeDbContext db, long snapshot, CancellationToken ct)
    {
        var rows = await Guarded("ducklake_schema", () => db.Schemas.LiveAt(snapshot).OrderBy(s => s.SchemaId).ToListAsync(ct));
        var byId = rows.ToDictionary(r => r.SchemaId);
        var result = new List<SchemaRow>();
        foreach (var r in rows)
        {
            var parts = new List<string>();
            SchemaEntity? at = r;
            for (var depth = 0; at is not null; depth++)
            {
                if (depth > 64) throw new InvalidDataException($"Schema {r.SchemaId}: parent chain is deeper than 64 or cyclic");
                parts.Insert(0, at.SchemaName ?? throw new InvalidDataException($"Schema {at.SchemaId} has no name"));
                if (at.ParentSchemaId is not { } parent) break;
                at = byId.GetValueOrDefault(parent); // a parent that is not visible hides the schema too
            }
            if (at is null) continue;
            result.Add(new SchemaRow(r.SchemaId, string.Join('.', parts), parts, Resolve(DataPath, r.Path, r.PathIsRelative ?? true), r.ParentSchemaId));
        }
        return result;
    }

    internal static async Task<List<TableRow>> TablesAsync(DuckLakeDbContext db, long snapshot, CancellationToken ct) =>
        (await Guarded("ducklake_table", () => db.Tables.LiveAt(snapshot).OrderBy(t => t.TableId).ToListAsync(ct)))
        .Select(t => new TableRow(t.TableId, t.TableUuid ?? Guid.Empty, t.SchemaId,
            t.TableName ?? throw new InvalidDataException($"Table {t.TableId} has no name"), t.Path, t.PathIsRelative ?? true)).ToList();

    public async Task<IReadOnlyList<string>> ListSchemasAsync(long? snapshotId = null, CancellationToken ct = default)
    {
        await using var db = Context();
        var s = await ResolveSnapshotAsync(db, snapshotId, ct);
        return (await SchemasAsync(db, s, ct)).Select(x => x.Name).ToList();
    }

    public async Task<IReadOnlyList<string>> ListTablesAsync(string schema, long? snapshotId = null, CancellationToken ct = default)
    {
        await using var db = Context();
        var s = await ResolveSnapshotAsync(db, snapshotId, ct);
        var sc = Find(await SchemasAsync(db, s, ct), x => x.Name, schema, "schema");
        return (await TablesAsync(db, s, ct)).Where(t => t.SchemaId == sc.SchemaId).Select(t => t.Name).ToList();
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
        await using var db = Context();
        var s = await ResolveSnapshotAsync(db, snapshotId, ct);
        return await LoadTableAsync(db, schema, table, s, ct);
    }

    internal async Task<DuckLakeTable> LoadTableAsync(DuckLakeDbContext db, string schema, string table, long snapshot, CancellationToken ct)
    {
        var sc = Find(await SchemasAsync(db, snapshot, ct), x => x.Name, schema, "schema");
        var t = Find((await TablesAsync(db, snapshot, ct)).Where(x => x.SchemaId == sc.SchemaId), x => x.Name, table, $"table in schema {sc.Name}");
        var columns = await ColumnsAsync(db, t.TableId, snapshot, ct);
        var partitions = await db.PartitionInfos.LiveAt(snapshot).Where(p => p.TableId == t.TableId).Select(p => p.PartitionId).ToListAsync(ct);
        if (partitions.Count > 1) throw new InvalidDataException($"Table {t.TableId} has {partitions.Count} live partition specs in snapshot {snapshot}");
        IReadOnlyList<DuckLakePartitionKey> keys = [];
        if (partitions.Count == 1)
        {
            var pid = partitions[0];
            keys = (await Guarded("ducklake_partition_column", () => db.PartitionColumns.Where(p => p.PartitionId == pid && p.TableId == t.TableId)
                    .OrderBy(p => p.PartitionKeyIndex).ToListAsync(ct)))
                .Select(p => new DuckLakePartitionKey(p.ColumnId, p.Transform ?? throw new InvalidDataException($"Partition {pid} key {p.PartitionKeyIndex} has no transform")))
                .ToList();
        }
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
            Location = Resolve(sc.Location, t.Path, t.Relative),
            PartitionId = partitions.Count == 1 ? partitions[0] : null,
            PartitionKeys = keys,
            Tags = await TagsAsync(db, t.TableId, snapshot, ct),
            ColumnTags = await ColumnTagsAsync(db, t.TableId, snapshot, ct),
            SortKeys = await SortKeysAsync(db, t.TableId, snapshot, ct),
        };
    }

    internal static async Task<IReadOnlyList<DuckLakeColumn>> ColumnsAsync(DuckLakeDbContext db, long tableId, long snapshot, CancellationToken ct)
    {
        var rows = await Guarded("ducklake_column", () => db.Columns.LiveAt(snapshot).Where(c => c.TableId == tableId).ToListAsync(ct));
        return DuckLakeTypes.Tree(rows.Select(r => new ColumnRow(r.ColumnId, r.ColumnOrder ?? r.ColumnId,
            r.ColumnName ?? throw new InvalidDataException($"Column {r.ColumnId} has no name"),
            r.ColumnType ?? throw new InvalidDataException($"Column {r.ColumnId} has no type"),
            r.NullsAllowed ?? true, r.ParentColumn, r.InitialDefault)).ToList());
    }

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

    /// <summary>Plan a scan: the live data files of the table's snapshot, minus those whose file statistics rule out <paramref name="filter"/>.</summary>
    public async Task<DuckLakeScanPlan> PlanScanAsync(DuckLakeTable table, Expression? filter = null, CancellationToken ct = default)
    {
        using var scope = DuckLakeTelemetry.Start("ducklake.scan.plan", this, table.ToString());
        scope.Snapshot(table.SnapshotId);
        try
        {
            await using var db = Context(scope);
            var plan = await PlanScanAsync(db, table, filter, ct);
            scope.Succeeded();
            return plan;
        }
        catch (Exception e)
        {
            scope.Failed(e);
            throw;
        }
    }

    internal static async Task<Dictionary<long, DeleteFileEntity>> DeleteFilesAsync(DuckLakeDbContext db, long tableId, long snapshot, CancellationToken ct)
    {
        var rows = await Guarded("ducklake_delete_file", () => db.DeleteFiles.LiveAt(snapshot).Where(d => d.TableId == tableId).ToListAsync(ct));
        var result = new Dictionary<long, DeleteFileEntity>();
        foreach (var d in rows)
        {
            // Spec "Queries", SELECT: "There can be at most one delete file per file in a single snapshot."
            if (!result.TryAdd(d.DataFileId, d))
                throw new InvalidDataException($"Data file {d.DataFileId} has more than one delete file in snapshot {snapshot}");
        }
        return result;
    }

    internal async Task<DuckLakeScanPlan> PlanScanAsync(DuckLakeDbContext db, DuckLakeTable table, Expression? filter, CancellationToken ct)
    {
        var files = await Guarded("ducklake_data_file", () => db.DataFiles.LiveAt(table.SnapshotId).Where(f => f.TableId == table.TableId)
            .OrderBy(f => f.FileOrder).ThenBy(f => f.DataFileId).ToListAsync(ct));
        var deletes = await DeleteFilesAsync(db, table.TableId, table.SnapshotId, ct);
        var bound = filter is null or AlwaysTrue ? null : Binder.Bind(table.Schema, filter);
        if (bound is not null)
        {
            // timetz and interval values have no order this reader shares with their Parquet form: filter them after reading.
            var ids = FieldIds(bound);
            if (table.Columns.SelectMany(c => c.SelfAndDescendants()).FirstOrDefault(c => ids.Contains((int)c.Id) && DuckLakeTypes.HasFileForm(c.Type)) is { } unfiltered)
                throw new NotSupportedException($"Column {unfiltered.Name} ({unfiltered.Type}) cannot be filtered on; filter the rows read instead");
        }
        Dictionary<long, Dictionary<long, FileColumnStatsEntity>>? stats = null;
        if (bound is not null)
        {
            // Only the statistics of the columns the filter names, of the live files: by table, not by an IN list of
            // every file (whose size would grow with the table).
            var columnIds = FieldIds(bound).Select(id => (long)id).ToList();
            var snapshot = table.SnapshotId;
            stats = (await Guarded("ducklake_file_column_stats", () => db.FileColumnStats
                    .Where(s => s.TableId == table.TableId && columnIds.Contains(s.ColumnId))
                    .Join(db.DataFiles.LiveAt(snapshot), s => s.DataFileId, f => f.DataFileId, (s, f) => s)
                    .ToListAsync(ct)))
                .GroupBy(s => s.DataFileId).ToDictionary(g => g.Key, g => g.GroupBy(s => s.ColumnId).ToDictionary(x => x.Key, x => x.First()));
        }
        var types = table.Columns.SelectMany(col => col.SelfAndDescendants()).Where(col => col.Type is DuckLakePrimitive)
            .ToDictionary(col => col.Id, col => (DuckLakePrimitive)col.Type);
        var evaluator = bound is null ? null : new MetricsEvaluator(bound);
        var tasks = new List<DuckLakeFileTask>();
        var mapped = new List<(int Index, long MappingId)>();
        var pruned = 0;
        foreach (var f in files)
        {
            if (f.EncryptionKey is not null) throw new NotSupportedException($"Data file {f.DataFileId} is encrypted");
            if (!string.Equals(f.FileFormat ?? "parquet", "parquet", StringComparison.OrdinalIgnoreCase))
                throw new NotSupportedException($"Data file {f.DataFileId} has format {f.FileFormat}");
            if (evaluator is not null && !evaluator.MightMatch(f.RecordCount, (fieldId, type) => Stats(stats!, f.DataFileId, fieldId, type, types)))
            {
                pruned++;
                continue;
            }
            var location = Resolve(table.Location, f.Path ?? throw new InvalidDataException($"Data file {f.DataFileId} has no path"), f.PathIsRelative ?? true);
            string? deleteLocation = null;
            if (deletes.TryGetValue(f.DataFileId, out var d))
            {
                if (d.EncryptionKey is not null) throw new NotSupportedException($"Delete file {d.DeleteFileId} is encrypted");
                if (!string.Equals(d.Format ?? "parquet", "parquet", StringComparison.OrdinalIgnoreCase))
                    throw new NotSupportedException($"Delete file {d.DeleteFileId} has format {d.Format} (deletion vectors are not read yet)");
                deleteLocation = Resolve(table.Location, d.Path ?? throw new InvalidDataException($"Delete file {d.DeleteFileId} has no path"), d.PathIsRelative ?? true);
            }
            tasks.Add(new DuckLakeFileTask(f.DataFileId, location, f.RecordCount, f.FileSizeBytes, deleteLocation, f.RowIdStart) { RowGroupCount = f.RowGroupCount });
            if (f.MappingId is { } m) mapped.Add((tasks.Count - 1, m));
        }
        if (mapped.Count > 0)
        {
            var maps = await DuckLakeNameMap.LoadAsync(db, mapped.Select(x => x.MappingId).Distinct().ToList(), ct);
            foreach (var (index, id) in mapped) tasks[index] = tasks[index] with { Mapping = maps[id] };
        }
        var inlined = await db.InlinedDataTables.Where(i => i.TableId == table.TableId).OrderBy(i => i.SchemaVersion).Select(i => i.TableName).ToListAsync(ct);
        db.Scope?.Planned(tasks.Count, pruned);
        return new DuckLakeScanPlan(tasks, pruned, inlined) { InlinedDeletions = await InlinedDeletionsAsync(db, table.TableId, table.SnapshotId, ct) };
    }

    /// <summary>The per-table deletion inlining table DuckDB creates on first use; absent until then.</summary>
    internal static string InlinedDeleteTable(long tableId) => $"ducklake_inlined_delete_{tableId}";

    internal static async Task<bool> TableExistsAsync(DuckLakeDbContext db, string name, CancellationToken ct)
    {
        await db.Database.OpenConnectionAsync(ct);
        var (c, t) = db.Raw();
        return (await Db.QueryAsync(c, t, db.Dialect.TableExistsSql, r => Db.ReqLong(r, 0), ct, null, name))[0] > 0;
    }

    /// <summary>Rows of a data file (<c>file_id</c>, position <c>row_id</c>) deleted in or before <paramref name="snapshot"/>.</summary>
    internal static async Task<Dictionary<long, HashSet<long>>> InlinedDeletionsAsync(DuckLakeDbContext db, long tableId, long snapshot, CancellationToken ct)
    {
        var result = new Dictionary<long, HashSet<long>>();
        var name = InlinedDeleteTable(tableId);
        if (!await TableExistsAsync(db, name, ct)) return result;
        var (c, t) = db.Raw();
        foreach (var (file, pos) in await Db.QueryAsync(c, t, $"SELECT file_id, row_id FROM {Db.Quote(name)} WHERE begin_snapshot <= @s",
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

    private static HashSet<int> FieldIds(Expression e)
    {
        var ids = new HashSet<int>();
        Visit(e);
        return ids;

        void Visit(Expression x)
        {
            switch (x)
            {
                case BoundPredicate p: ids.Add(p.FieldId); break;
                case And a: Visit(a.Left); Visit(a.Right); break;
                case Or o: Visit(o.Left); Visit(o.Right); break;
                case Not n: Visit(n.Child); break;
            }
        }
    }

    /// <summary>
    /// A file's DuckLake statistics as Iceberg typed statistics. <c>value_count</c> counts non-null values (as DuckDB
    /// writes it, and as the 1.1 spec says), so the Iceberg count (which includes nulls) is <c>value_count + null_count</c>;
    /// a writer that counted nulls in would only ever stop pruning, never prune wrongly.
    /// </summary>
    private static ColumnStats? Stats(Dictionary<long, Dictionary<long, FileColumnStatsEntity>> stats, long fileId, int fieldId, IcebergType type,
        Dictionary<long, DuckLakePrimitive> types)
    {
        if (!stats.TryGetValue(fileId, out var byColumn) || !byColumn.TryGetValue(fieldId, out var s) || !types.TryGetValue(fieldId, out var dl)) return null;
        object? lower = null, upper = null;
        if (s.MinValue is not null && s.MaxValue is not null && DuckLakeValues.TryParse(dl, s.MinValue, out var lo) && DuckLakeValues.TryParse(dl, s.MaxValue, out var hi)
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

    /// <summary>
    /// The rows as <typeparamref name="T"/>, bound by column name (see <see cref="DuckLakeClr.Binder{T}"/>): a record
    /// such as <c>record Order(long Id, string? Name, DateOnly Day)</c> for a table <c>(id, name, day)</c>.
    /// </summary>
    public async IAsyncEnumerable<T> ReadAsync<T>(DuckLakeTable table, Expression? filter = null, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var bind = DuckLakeClr.Binder<T>(table.Columns);
        await foreach (var r in ScanAsync(table, filter, ct)) yield return bind(r.Values);
    }

    /// <summary>As <see cref="ReadAsync"/>, with values as natural CLR types (see <see cref="DuckLakeClr"/>).</summary>
    public async Task<List<object?[]>> ReadClrAsync(DuckLakeTable table, Expression? filter = null, CancellationToken ct = default)
    {
        var rows = new List<object?[]>();
        await foreach (var r in ScanAsync(table, filter, ct)) rows.Add(DuckLakeClr.ToClr(table.Columns, r.Values));
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

    private const int NullColumnFieldId = 2147483000;

    /// <summary>
    /// The schema files are read with: the table's columns plus DuckDB's internal row-ID and snapshot columns. Columns of
    /// the 1.1 <c>null</c> type are never in files (always null): they are read under an ID no file has.
    /// </summary>
    internal static Schema FileProjection(DuckLakeTable table) =>
        new(0, table.Schema.Columns.Select((f, i) => f.Type.TypeId == TypeId.Unknown ? f with { Id = NullColumnFieldId - i, Type = PrimitiveType.Int } : f)
            .Concat(InternalColumns));

    /// <summary>
    /// A data file's batches in <paramref name="projection"/>: by field ID, or for a file registered under a name
    /// mapping, by the mapped names, with its Hive partition columns filled in from its path.
    /// </summary>
    internal IAsyncEnumerable<DataFileBatch> ReadDataFileAsync(DuckLakeTable table, string location, DuckLakeNameMap? mapping, Schema projection,
        DataFileReadOptions options, CancellationToken ct)
    {
        if (mapping is null) return ParquetFiles.ReadBatchesAsync(IO, location, projection, null, options, ct);
        var (mappedProjection, mappedOptions) = mapping.Projection(projection, options);
        var partition = mapping.PartitionValues(table.Columns, location);
        var batches = ParquetFiles.ReadBatchesAsync(IO, location, mappedProjection, null, mappedOptions, ct);
        return partition.Count == 0 ? batches : batches.Select(b =>
        {
            foreach (var row in b.Rows)
                foreach (var (index, value) in partition) row[index] = value;
            return b;
        });
    }

    /// <summary>A data file being read ahead: its deleted positions and its row groups, decoded on the thread pool into a bounded queue.</summary>
    private sealed record FileRead(DuckLakeFileTask File, Task<HashSet<long>?> Deleted, ChannelReader<DataFileBatch> Batches, Task Producer);

    private FileRead StartRead(DuckLakeScanPlan plan, DuckLakeFileTask file, DuckLakeTable table, Schema projection, Expression? bound, CancellationToken ct)
    {
        var queue = Channel.CreateBounded<DataFileBatch>(new BoundedChannelOptions(2) { SingleReader = true, SingleWriter = true });
        var deleted = DeletedPositionsAsync(plan, file, table.SnapshotId, ct);
        var producer = Task.Run(async () =>
        {
            try
            {
                var options = new DataFileReadOptions { FileSize = file.FileSizeBytes, Filter = bound };
                await foreach (var batch in ReadDataFileAsync(table, file.Location, file.Mapping, projection, options, ct))
                    await queue.Writer.WriteAsync(batch, ct);
            }
            finally
            {
                queue.Writer.TryComplete();
            }
        }, ct);
        return new FileRead(file, deleted, queue.Reader, producer);
    }

    internal async IAsyncEnumerable<ScannedRow> ScanAsync(DuckLakeTable table, Expression? filter, [EnumeratorCancellation] CancellationToken ct)
    {
        // Disposed when the rows are read or the caller stops early (the iterator's finally runs on DisposeAsync).
        using var scope = DuckLakeTelemetry.Start("ducklake.scan", this, table.ToString());
        scope.Snapshot(table.SnapshotId);
        long returned = 0;
        var failed = false;
        await using var rows = ScanRowsAsync(table, filter, scope, ct).GetAsyncEnumerator(ct);
        try
        {
            while (true)
            {
                bool more;
                try
                {
                    more = await rows.MoveNextAsync();
                }
                catch (Exception e)
                {
                    failed = true;
                    scope.Failed(e);
                    throw;
                }
                if (!more) break;
                returned++;
                yield return rows.Current;
            }
        }
        finally
        {
            // A caller that stops early has not failed.
            if (!failed) scope.Succeeded();
            scope.Rows(returned);
        }
    }

    private async IAsyncEnumerable<ScannedRow> ScanRowsAsync(DuckLakeTable table, Expression? filter, DuckLakeTelemetry.Scope scope,
        [EnumeratorCancellation] CancellationToken ct)
    {
        DuckLakeScanPlan plan;
        await using (var db = Context(scope)) plan = await PlanScanAsync(db, table, filter, ct);
        var bound = filter is null or AlwaysTrue ? null : Binder.Bind(table.Schema, filter);
        var residual = bound is null ? null : RowEvaluator.Compile(bound);
        var width = table.Columns.Count;
        var projection = FileProjection(table);
        var fromFile = DuckLakeValues.FileRowConverter(table.Columns, toFile: false);
        // Up to MaxParallelFileReads files are read ahead, each into a queue of two batches (row groups): decoding
        // overlaps, memory stays bounded, and rows come out in file order.
        var parallelism = Math.Max(1, Options.MaxParallelFileReads);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var inFlight = new Queue<FileRead>();
        var next = 0;
        try
        {
            while (true)
            {
                while (inFlight.Count < parallelism && next < plan.Files.Count) inFlight.Enqueue(StartRead(plan, plan.Files[next++], table, projection, bound, stop.Token));
                if (inFlight.Count == 0) break;
                var read = inFlight.Dequeue();
                var file = read.File;
                var deleted = await read.Deleted;
                await foreach (var batch in read.Batches.ReadAllAsync(stop.Token))
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
                        // Stored row IDs take precedence; without a row_id_start (1.1) they are required.
                        var rowId = full[width] is long stored ? stored
                            : file.RowIdStart is { } start ? start + pos
                            : throw new InvalidDataException($"Data file {file.DataFileId} has no row_id_start and no stored row IDs");
                        yield return new ScannedRow(rowId, fromFile is null ? row : fromFile(row), file.DataFileId, pos, null);
                    }
                }
                await read.Producer; // rethrows a read failure (the queue only ends early)
            }
        }
        finally
        {
            // The caller stopped (or a read failed): stop the reads still in flight and wait for them to let go of their files.
            await stop.CancelAsync();
            foreach (var read in inFlight)
            {
                try { await read.Producer; } catch { /* stopped on purpose */ }
                try { await read.Deleted; } catch { /* stopped on purpose */ }
            }
        }
        foreach (var name in plan.InlinedTables)
        {
            List<(long RowId, object?[] Values)> rows;
            await using (var db = Context(scope)) rows = await DuckLakeInlinedData.ReadAsync(db, table, name, ct);
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
