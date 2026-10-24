namespace Iceberg.Net.Spark;

/// <summary>
/// A <c>CALL catalog.system.procedure(name => value, …)</c> statement. Arguments are named (Iceberg's recommended
/// form), in the order given, and arguments whose value is null are left out so the procedure's default applies.
/// </summary>
public sealed record SparkProcedureCall(string Catalog, string Procedure, IReadOnlyList<KeyValuePair<string, object?>> Arguments)
{
    /// <summary>Argument names are inserted as written, so only Spark's plain identifier form is accepted.</summary>
    public IReadOnlyList<KeyValuePair<string, object?>> Arguments { get; } = Arguments.All(a => IsArgumentName(a.Key))
        ? Arguments
        : throw new ArgumentException($"Procedure argument names must be letters, digits and underscores: {string.Join(", ", Arguments.Select(a => a.Key).Where(k => !IsArgumentName(k)))}", nameof(Arguments));

    private static bool IsArgumentName(string name) =>
        name.Length > 0 && (char.IsAsciiLetter(name[0]) || name[0] == '_') && name.All(c => char.IsAsciiLetterOrDigit(c) || c == '_');

    public string ToSql() =>
        $"CALL {SparkSql.Identifier(Catalog)}.system.{SparkSql.Identifier(Procedure)}(" +
        string.Join(", ", Arguments.Where(a => a.Value is not null && a.Value is not IEmpty { IsEmpty: true })
            .Select(a => $"{a.Key} => {SparkSql.Literal(a.Value)}")) + ")";

    public override string ToString() => ToSql();
}

/// <summary>A collection argument that is omitted when empty (Iceberg's defaults differ from an empty map).</summary>
internal interface IEmpty
{
    bool IsEmpty { get; }
}

/// <summary>String options (<c>map(…)</c>) that are left out of the call when empty.</summary>
public sealed class SparkOptions : Dictionary<string, string>, IEmpty
{
    public SparkOptions() : base(StringComparer.Ordinal) { }
    public SparkOptions(IEnumerable<KeyValuePair<string, string>> entries) : this() { foreach (var (k, v) in entries) this[k] = v; }
    public bool IsEmpty => Count == 0;
}

public enum RewriteStrategy { BinPack, Sort }

/// <summary>Arguments of <c>rewrite_data_files</c>; null leaves Iceberg's default.</summary>
public sealed record RewriteDataFilesOptions
{
    public RewriteStrategy? Strategy { get; init; }
    /// <summary>Spark sort order text (<c>id DESC NULLS LAST, name</c>) or <see cref="ZOrder"/>; implies the sort strategy.</summary>
    public string? SortOrder { get; init; }
    /// <summary>A Spark SQL predicate selecting the rows (and so files) to rewrite, e.g. <c>event_date &gt;= '2026-01-01'</c>.</summary>
    public string? Where { get; init; }
    public long? TargetFileSizeBytes { get; init; }
    public int? MinInputFiles { get; init; }
    public bool? RewriteAll { get; init; }
    public bool? PartialProgressEnabled { get; init; }
    public int? PartialProgressMaxCommits { get; init; }
    public int? MaxConcurrentFileGroupRewrites { get; init; }
    public int? DeleteFileThreshold { get; init; }
    public bool? RemoveDanglingDeletes { get; init; }
    public bool? UseStartingSequenceNumber { get; init; }
    /// <summary>Any other <c>options</c> entry (they win over the typed ones).</summary>
    public SparkOptions Options { get; init; } = [];

    /// <summary>The z-order sort order text: <c>zorder(c1,c2)</c>.</summary>
    public static string ZOrder(params string[] columns) => "zorder(" + string.Join(",", columns) + ")";

    internal SparkOptions AllOptions()
    {
        var o = new SparkOptions();
        Set(o, "target-file-size-bytes", TargetFileSizeBytes);
        Set(o, "min-input-files", MinInputFiles);
        Set(o, "rewrite-all", RewriteAll);
        Set(o, "partial-progress.enabled", PartialProgressEnabled);
        Set(o, "partial-progress.max-commits", PartialProgressMaxCommits);
        Set(o, "max-concurrent-file-group-rewrites", MaxConcurrentFileGroupRewrites);
        Set(o, "delete-file-threshold", DeleteFileThreshold);
        Set(o, "remove-dangling-deletes", RemoveDanglingDeletes);
        Set(o, "use-starting-sequence-number", UseStartingSequenceNumber);
        foreach (var (k, v) in Options) o[k] = v;
        return o;
    }

    internal static void Set(SparkOptions o, string key, object? value)
    {
        if (value is not null) o[key] = value is bool b ? (b ? "true" : "false") : Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)!;
    }
}

/// <summary>Arguments of <c>expire_snapshots</c>.</summary>
public sealed record ExpireSnapshotsOptions
{
    public DateTimeOffset? OlderThan { get; init; }
    public int? RetainLast { get; init; }
    public IReadOnlyList<long>? SnapshotIds { get; init; }
    public int? MaxConcurrentDeletes { get; init; }
    public bool? StreamResults { get; init; }
    /// <summary>Also remove unused schemas and partition specs (Iceberg 1.10+).</summary>
    public bool? CleanExpiredMetadata { get; init; }
}

/// <summary>Arguments of <c>remove_orphan_files</c>.</summary>
public sealed record RemoveOrphanFilesOptions
{
    public DateTimeOffset? OlderThan { get; init; }
    public string? Location { get; init; }
    public bool? DryRun { get; init; }
    public int? MaxConcurrentDeletes { get; init; }
    public string? FileListView { get; init; }
    /// <summary>Schemes treated as equal, e.g. <c>s3a,s3n</c> → <c>s3</c>.</summary>
    public SparkOptions EqualSchemes { get; init; } = [];
    public SparkOptions EqualAuthorities { get; init; } = [];
    /// <summary><c>ERROR</c> (default), <c>IGNORE</c> or <c>DELETE</c>.</summary>
    public string? PrefixMismatchMode { get; init; }
}

/// <summary>Arguments of <c>create_changelog_view</c>.</summary>
public sealed record ChangelogViewOptions
{
    public string? ViewName { get; init; }
    public long? StartSnapshotId { get; init; }
    public long? EndSnapshotId { get; init; }
    /// <summary>Other read options, e.g. <c>start-timestamp</c>.</summary>
    public SparkOptions Options { get; init; } = [];
    public bool? NetChanges { get; init; }
    public bool? ComputeUpdates { get; init; }
    public IReadOnlyList<string>? IdentifierColumns { get; init; }
}

/// <summary>
/// Spark SQL for Iceberg's stored procedures (Iceberg 1.12 names and arguments) on one Spark catalog. Each method
/// returns a <see cref="SparkProcedureCall"/>; <see cref="SparkProcedureCall.ToSql"/> is the statement, ready for
/// <c>spark.sql</c>, a <see cref="SparkSqlScript"/> or a job runner.
/// </summary>
public sealed class IcebergProcedures(string catalog)
{
    public string Catalog { get; } = catalog;

    /// <summary>Any procedure; <paramref name="arguments"/> are named, and null values are omitted.</summary>
    public SparkProcedureCall Call(string procedure, params (string Name, object? Value)[] arguments) =>
        new(Catalog, procedure, arguments.Select(a => new KeyValuePair<string, object?>(a.Name, a.Value)).ToList());

    private SparkProcedureCall OnTable(string procedure, SparkTable table, params (string Name, object? Value)[] arguments)
    {
        if (table.Catalog != Catalog)
            throw new ArgumentException($"Table {table} is in catalog '{table.Catalog}', but these procedures run on '{Catalog}'", nameof(table));
        return Call(procedure, [("table", new SparkSqlFragment(table.ProcedureArgument)), .. arguments]);
    }

    public SparkProcedureCall RewriteDataFiles(SparkTable table, RewriteDataFilesOptions? options = null)
    {
        options ??= new RewriteDataFilesOptions();
        var strategy = options.Strategy ?? (options.SortOrder is not null ? RewriteStrategy.Sort : null);
        if (strategy == RewriteStrategy.BinPack && options.SortOrder is not null)
            throw new ArgumentException("A sort order needs the sort strategy", nameof(options));
        return OnTable("rewrite_data_files", table,
            ("strategy", strategy switch { RewriteStrategy.BinPack => "binpack", RewriteStrategy.Sort => "sort", _ => null }),
            ("sort_order", options.SortOrder),
            ("options", options.AllOptions()),
            ("where", options.Where));
    }

    /// <param name="options">e.g. <c>rewrite-all</c>, <c>min-input-files</c>, <c>target-file-size-bytes</c>.</param>
    public SparkProcedureCall RewritePositionDeleteFiles(SparkTable table, SparkOptions? options = null, string? where = null) =>
        OnTable("rewrite_position_delete_files", table, ("options", options ?? []), ("where", where));

    public SparkProcedureCall ExpireSnapshots(SparkTable table, ExpireSnapshotsOptions? options = null)
    {
        options ??= new ExpireSnapshotsOptions();
        return OnTable("expire_snapshots", table,
            ("older_than", options.OlderThan),
            ("retain_last", options.RetainLast),
            ("max_concurrent_deletes", options.MaxConcurrentDeletes),
            ("stream_results", options.StreamResults),
            ("snapshot_ids", options.SnapshotIds),
            ("clean_expired_metadata", options.CleanExpiredMetadata));
    }

    public SparkProcedureCall RemoveOrphanFiles(SparkTable table, RemoveOrphanFilesOptions? options = null)
    {
        options ??= new RemoveOrphanFilesOptions();
        return OnTable("remove_orphan_files", table,
            ("older_than", options.OlderThan),
            ("location", options.Location),
            ("dry_run", options.DryRun),
            ("max_concurrent_deletes", options.MaxConcurrentDeletes),
            ("file_list_view", options.FileListView),
            ("equal_schemes", options.EqualSchemes),
            ("equal_authorities", options.EqualAuthorities),
            ("prefix_mismatch_mode", options.PrefixMismatchMode));
    }

    public SparkProcedureCall RewriteManifests(SparkTable table, bool? useCaching = null, int? specId = null) =>
        OnTable("rewrite_manifests", table, ("use_caching", useCaching), ("spec_id", specId));

    public SparkProcedureCall CherrypickSnapshot(SparkTable table, long snapshotId) =>
        OnTable("cherrypick_snapshot", table, ("snapshot_id", snapshotId));

    public SparkProcedureCall RollbackToSnapshot(SparkTable table, long snapshotId) =>
        OnTable("rollback_to_snapshot", table, ("snapshot_id", snapshotId));

    public SparkProcedureCall RollbackToTimestamp(SparkTable table, DateTimeOffset timestamp) =>
        OnTable("rollback_to_timestamp", table, ("timestamp", timestamp));

    /// <summary>Set the current snapshot by ID, or to the snapshot a branch or tag points at.</summary>
    public SparkProcedureCall SetCurrentSnapshot(SparkTable table, long? snapshotId = null, string? @ref = null) =>
        (snapshotId is null) == (@ref is null)
            ? throw new ArgumentException("Give exactly one of snapshotId and ref")
            : OnTable("set_current_snapshot", table, ("snapshot_id", snapshotId), ("ref", @ref));

    public SparkProcedureCall FastForward(SparkTable table, string branch, string to) =>
        OnTable("fast_forward", table, ("branch", branch), ("to", to));

    /// <summary>Write-audit-publish: cherry-pick the snapshot staged with <c>wap.id</c> = <paramref name="wapId"/>.</summary>
    public SparkProcedureCall PublishChanges(SparkTable table, string wapId) =>
        OnTable("publish_changes", table, ("wap_id", wapId));

    public SparkProcedureCall CreateChangelogView(SparkTable table, ChangelogViewOptions? options = null)
    {
        options ??= new ChangelogViewOptions();
        var read = new SparkOptions(options.Options);
        RewriteDataFilesOptions.Set(read, "start-snapshot-id", options.StartSnapshotId);
        RewriteDataFilesOptions.Set(read, "end-snapshot-id", options.EndSnapshotId);
        return OnTable("create_changelog_view", table,
            ("changelog_view", options.ViewName),
            ("options", read),
            ("net_changes", options.NetChanges),
            ("compute_updates", options.ComputeUpdates),
            ("identifier_columns", options.IdentifierColumns));
    }

    /// <summary>Adopt existing files: <paramref name="sourceTable"/> is a table name, or <c>`parquet`.`s3://path`</c>.</summary>
    public SparkProcedureCall AddFiles(SparkTable table, string sourceTable, IReadOnlyDictionary<string, string>? partitionFilter = null, bool? checkDuplicateFiles = null) =>
        OnTable("add_files", table, ("source_table", sourceTable),
            ("partition_filter", partitionFilter is null ? null : new SparkOptions(partitionFilter)),
            ("check_duplicate_files", checkDuplicateFiles));

    public SparkProcedureCall RegisterTable(SparkTable table, string metadataFile) =>
        OnTable("register_table", table, ("metadata_file", metadataFile));

    public SparkProcedureCall AncestorsOf(SparkTable table, long? snapshotId = null) =>
        OnTable("ancestors_of", table, ("snapshot_id", snapshotId));

    /// <summary>Compute NDV statistics into a Puffin file (Iceberg 1.8+).</summary>
    public SparkProcedureCall ComputeTableStats(SparkTable table, long? snapshotId = null, IReadOnlyList<string>? columns = null) =>
        OnTable("compute_table_stats", table, ("snapshot_id", snapshotId), ("columns", columns));
}
