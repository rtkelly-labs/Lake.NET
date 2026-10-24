using System.Globalization;
using Iceberg.Net.Avro;
using Iceberg.Net.Catalog;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Puffin;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

public enum IssueSeverity { Warning, Error }

/// <summary>One broken invariant. <see cref="Code"/> is stable so tests and dashboards can key on it.</summary>
public sealed record ValidationIssue(IssueSeverity Severity, string Code, string Message, string? Path = null)
{
    public override string ToString() => $"{Severity} {Code}: {Message}{(Path is null ? "" : $" [{Path}]")}";
}

public sealed record ValidationReport(IReadOnlyList<ValidationIssue> Issues, int SnapshotsChecked, int FilesChecked, bool Deep)
{
    public IEnumerable<ValidationIssue> Errors => Issues.Where(i => i.Severity == IssueSeverity.Error);
    public bool IsValid => !Errors.Any();
    public override string ToString() => IsValid && Issues.Count == 0
        ? $"OK ({SnapshotsChecked} snapshots, {FilesChecked} files{(Deep ? ", deep" : "")})"
        : string.Join('\n', Issues);
}

public sealed record ValidationOptions
{
    /// <summary>Validate every snapshot reachable from a ref, not just the current one.</summary>
    public bool AllSnapshots { get; init; }
    /// <summary>Read data and delete files: row counts, column metrics, partition values, delete positions.</summary>
    public bool Deep { get; init; }
    /// <summary>Check that referenced files exist with their recorded sizes (one HEAD per file).</summary>
    public bool CheckFiles { get; init; } = true;
}

/// <summary>
/// A read-only "fsck" for Iceberg tables: checks the invariants the spec and the writers' contracts guarantee,
/// independently of the code that wrote the table. Run after every test that writes, and against production
/// tables (it never writes). Each check names the rule it enforces.
/// </summary>
public static class TableValidator
{
    public static async Task<ValidationReport> ValidateAsync(Table table, ValidationOptions? options = null, CancellationToken ct = default)
    {
        options ??= new ValidationOptions();
        var m = table.Metadata;
        var issues = new List<ValidationIssue>();
        void Error(string code, string message, string? path = null) => issues.Add(new ValidationIssue(IssueSeverity.Error, code, message, path));
        void Warn(string code, string message, string? path = null) => issues.Add(new ValidationIssue(IssueSeverity.Warning, code, message, path));

        CheckMetadata(m, Error);

        var snapshots = options.AllSnapshots
            ? m.Refs.Values.SelectMany(r => m.Ancestors(r.SnapshotId)).DistinctBy(s => s.SnapshotId).ToList()
            : m.CurrentSnapshot is { } c ? [c] : [];
        var files = 0;
        foreach (var snapshot in snapshots)
            files += await CheckSnapshotAsync(table, snapshot, options, Error, Warn, ct);

        return new ValidationReport(issues, snapshots.Count, files, options.Deep);
    }

    /// <summary>Throws with every issue listed when the table is invalid (warnings included in the message).</summary>
    public static async Task AssertValidAsync(Table table, ValidationOptions? options = null, CancellationToken ct = default)
    {
        var report = await ValidateAsync(table, options, ct);
        if (!report.IsValid) throw new InvalidOperationException($"Table {table.Identifier} failed validation:\n{report}");
    }

    private static void CheckMetadata(TableMetadata m, Action<string, string, string?> error)
    {
        if (m.CurrentSnapshotId is { } cur && (!m.Refs.TryGetValue(SnapshotRef.Main, out var main) || main.SnapshotId != cur))
            error("META-MAIN", "current-snapshot-id does not match the main branch", null);
        foreach (var (name, r) in m.Refs)
            if (m.SnapshotById(r.SnapshotId) is null) error("META-REF", $"ref {name} points at missing snapshot {r.SnapshotId}", null);

        var maxFieldId = m.Schemas.Max(s => s.HighestFieldId);
        if (maxFieldId > m.LastColumnId) error("META-LAST-COLUMN", $"last-column-id {m.LastColumnId} < highest field id {maxFieldId}", null);
        var maxPartitionId = m.PartitionSpecs.SelectMany(s => s.Fields).Select(f => f.FieldId).DefaultIfEmpty(999).Max();
        if (maxPartitionId > m.LastPartitionId) error("META-LAST-PARTITION", $"last-partition-id {m.LastPartitionId} < highest partition field id {maxPartitionId}", null);

        if (m.FormatVersion >= 2)
        {
            foreach (var s in m.Snapshots)
            {
                if (s.SequenceNumber > m.LastSequenceNumber)
                    error("META-SEQ-MAX", $"snapshot {s.SnapshotId} sequence number {s.SequenceNumber} > last-sequence-number {m.LastSequenceNumber}", null);
                if (s.ParentSnapshotId is { } p && m.SnapshotById(p) is { } parent && s.SequenceNumber <= parent.SequenceNumber)
                    error("META-SEQ-ORDER", $"snapshot {s.SnapshotId} sequence number {s.SequenceNumber} is not greater than its parent's ({parent.SequenceNumber})", null);
            }
        }

        // v4: a snapshot's timestamp is strictly greater than its parent's. Earlier versions do not require it (and
        // tables written by clocks that skew, or by fast writers within one millisecond, break it), so it is not
        // reported for them, not even as a warning. SnapshotProducer always writes it.
        if (m.FormatVersion >= 4)
            foreach (var s in m.Snapshots)
                if (s.ParentSnapshotId is { } p && m.SnapshotById(p) is { } parent && s.TimestampMs <= parent.TimestampMs)
                    error("META-TIMESTAMP-ORDER", $"snapshot {s.SnapshotId} timestamp-ms {s.TimestampMs} is not greater than its parent's ({parent.TimestampMs})", null);

        if (m.FormatVersion >= 3)
        {
            var ranges = m.Snapshots.Where(s => s.FirstRowId is not null).Select(s => (Start: s.FirstRowId!.Value, End: s.FirstRowId!.Value + (s.AddedRows ?? 0), s.SnapshotId)).ToList();
            foreach (var r in ranges)
                if (r.End > (m.NextRowId ?? 0)) error("META-ROW-ID", $"snapshot {r.SnapshotId} assigned row IDs up to {r.End} but next-row-id is {m.NextRowId}", null);
            foreach (var (a, b) in Overlaps(ranges.Select(r => (r.Start, r.End, r.SnapshotId.ToString(CultureInfo.InvariantCulture)))))
                error("META-ROW-ID-OVERLAP", $"snapshots {a} and {b} assigned overlapping row IDs", null);
        }
    }

    private static IEnumerable<(string, string)> Overlaps(IEnumerable<(long Start, long End, string Name)> ranges)
    {
        var sorted = ranges.Where(r => r.End > r.Start).OrderBy(r => r.Start).ToList();
        for (var i = 1; i < sorted.Count; i++)
            if (sorted[i].Start < sorted[i - 1].End) yield return (sorted[i - 1].Name, sorted[i].Name);
    }

    private static async Task<int> CheckSnapshotAsync(Table table, Snapshot snapshot, ValidationOptions options,
        Action<string, string, string?> error, Action<string, string, string?> warn, CancellationToken ct)
    {
        var m = table.Metadata;
        var io = table.IO;
        var at = $"snapshot {snapshot.SnapshotId}";
        if (snapshot.ManifestList is null) { error("SNAP-LIST", $"{at} has no manifest list", null); return 0; }

        IReadOnlyList<ManifestFile> manifests;
        try { manifests = await TableScan.ManifestsAsync(table, snapshot, ct); }
        catch (Exception e) when (e is not OperationCanceledException) { error("SNAP-LIST-READ", $"{at}: manifest list unreadable: {e.Message}", snapshot.ManifestList); return 0; }

        var liveData = new List<(DataFile File, long Seq)>();
        var liveDeletes = new List<(DataFile File, long Seq)>();
        foreach (var mf in manifests)
        {
            if (options.CheckFiles && await LengthOrNull(io, mf.ManifestPath, ct) is { } len && len != mf.ManifestLength)
                error("MANIFEST-LENGTH", $"manifest_length {mf.ManifestLength} but file is {len} bytes", mf.ManifestPath);
            if (mf.SequenceNumber > snapshot.SequenceNumber)
                error("MANIFEST-SEQ", $"manifest sequence number {mf.SequenceNumber} > {at} sequence number {snapshot.SequenceNumber}", mf.ManifestPath);

            List<ManifestEntry> entries;
            try
            {
                entries = ManifestCodec.ReadManifest(await io.ReadAllAsync(mf.ManifestPath, ct), m.SpecById(mf.PartitionSpecId), m.CurrentSchema, mf).Entries.ToList();
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                error("MANIFEST-READ", $"manifest unreadable: {e.Message}", mf.ManifestPath);
                continue;
            }

            Count(entries, ManifestEntryStatus.Added, mf.AddedFilesCount, mf.AddedRowsCount, "added");
            Count(entries, ManifestEntryStatus.Existing, mf.ExistingFilesCount, mf.ExistingRowsCount, "existing");
            Count(entries, ManifestEntryStatus.Deleted, mf.DeletedFilesCount, mf.DeletedRowsCount, "deleted");
            void Count(List<ManifestEntry> es, ManifestEntryStatus status, int files, long rows, string what)
            {
                var actualFiles = es.Count(e => e.Status == status);
                var actualRows = es.Where(e => e.Status == status).Sum(e => e.DataFile.RecordCount);
                if (actualFiles != files || actualRows != rows)
                    error("MANIFEST-COUNTS", $"{what} counts {files} files / {rows} rows in the manifest list, {actualFiles} / {actualRows} in the manifest", mf.ManifestPath);
            }

            var spec = m.SpecById(mf.PartitionSpecId);
            var partitionType = spec.PartitionType(m.CurrentSchema);
            foreach (var e in entries)
            {
                var f = e.DataFile;
                var expectedContent = mf.Content == ManifestContent.Data ? f.Content == FileContent.Data : f.Content != FileContent.Data;
                if (!expectedContent) error("MANIFEST-CONTENT", $"{f.Content} file in a {mf.Content} manifest", f.FilePath);
                if (e.SequenceNumber is { } seq && seq > snapshot.SequenceNumber)
                    error("ENTRY-SEQ", $"entry sequence number {seq} > {at} sequence number", f.FilePath);
                if (e.Status == ManifestEntryStatus.Added && e.SnapshotId != mf.AddedSnapshotId)
                    error("ENTRY-SNAPSHOT", $"ADDED entry has snapshot {e.SnapshotId}, manifest was added by {mf.AddedSnapshotId}", f.FilePath);
                if (f.Partition.Count != spec.Fields.Count)
                    error("ENTRY-PARTITION-ARITY", $"partition tuple has {f.Partition.Count} values, spec {spec.SpecId} has {spec.Fields.Count} fields", f.FilePath);
                if (e.Status == ManifestEntryStatus.Deleted) continue;
                (mf.Content == ManifestContent.Data ? liveData : liveDeletes).Add((f, e.SequenceNumber ?? 0));
                CheckPartitionSummary(mf, partitionType, f, error);
            }
        }

        // Live files: unique, present, right size.
        foreach (var dup in liveData.GroupBy(d => d.File.FilePath).Where(g => g.Count() > 1))
            error("DATA-DUPLICATE", $"data file is live {dup.Count()} times", dup.Key);
        if (options.CheckFiles)
        {
            foreach (var path in liveData.Select(d => (d.File.FilePath, d.File.FileSizeInBytes)).Concat(liveDeletes.Select(d => (d.File.FilePath, d.File.FileSizeInBytes))).Distinct())
            {
                var len = await LengthOrNull(io, path.FilePath, ct);
                if (len is null) error("FILE-MISSING", "referenced file does not exist", path.FilePath);
                else if (len != path.FileSizeInBytes) error("FILE-SIZE", $"file_size_in_bytes {path.FileSizeInBytes} but file is {len} bytes", path.FilePath);
            }
        }

        // Deletes.
        var livePaths = liveData.Select(d => d.File.FilePath).ToHashSet(StringComparer.Ordinal);
        var dvs = liveDeletes.Where(d => d.File.FileFormat == "PUFFIN").ToList();
        foreach (var dup in dvs.GroupBy(d => d.File.ReferencedDataFile).Where(g => g.Count() > 1))
            error("DV-MULTIPLE", $"{dup.Count()} deletion vectors for one data file (spec allows one)", dup.Key);
        foreach (var dv in dvs)
        {
            if (dv.File.ReferencedDataFile is not { } target) { error("DV-REFERENCE", "deletion vector without referenced_data_file", dv.File.FilePath); continue; }
            if (!livePaths.Contains(target)) warn("DV-DANGLING", $"deletion vector for a data file that is not live: {target}", dv.File.FilePath);
            if (options.Deep) await CheckDeletionVectorAsync(io, dv.File, error, ct);
        }
        if (m.FormatVersion >= 3 && liveDeletes.Any(d => d.File.Content == FileContent.PositionDeletes && d.File.FileFormat != "PUFFIN"))
            warn("V3-POSITION-FILES", "v3 table still has v2 position delete files (convert them to deletion vectors)", null);

        // v3 row lineage on files.
        if (m.FormatVersion >= 3)
        {
            foreach (var (a, b) in Overlaps(liveData.Where(d => d.File.FirstRowId is not null)
                         .Select(d => (d.File.FirstRowId!.Value, d.File.FirstRowId!.Value + d.File.RecordCount, d.File.FilePath))))
                error("FILE-ROW-ID-OVERLAP", $"data files {a} and {b} have overlapping row ID ranges", null);
        }

        // Snapshot summary totals.
        CheckTotal(snapshot, "total-data-files", liveData.Count, error);
        CheckTotal(snapshot, "total-records", liveData.Sum(d => d.File.RecordCount), error);
        CheckTotal(snapshot, "total-delete-files", liveDeletes.Count, error);
        CheckTotal(snapshot, "total-position-deletes", liveDeletes.Where(d => d.File.Content == FileContent.PositionDeletes).Sum(d => d.File.RecordCount), error);
        CheckTotal(snapshot, "total-equality-deletes", liveDeletes.Where(d => d.File.Content == FileContent.EqualityDeletes).Sum(d => d.File.RecordCount), error);

        if (options.Deep)
        {
            var mapping = NameMapping.ForTable(m.Properties);
            foreach (var (file, _) in liveData.Where(d => d.File.FileFormat is "PARQUET" or "AVRO"))
                await CheckDataFileAsync(table, file, mapping, error, ct);
        }
        return liveData.Count + liveDeletes.Count;
    }

    private static void CheckTotal(Snapshot s, string key, long actual, Action<string, string, string?> error)
    {
        if (s.Summary.TryGetValue(key, out var v) && long.TryParse(v, CultureInfo.InvariantCulture, out var claimed) && claimed != actual)
            error("SUMMARY-TOTAL", $"snapshot {s.SnapshotId} summary {key}={claimed}, actual {actual}", null);
    }

    private static void CheckPartitionSummary(ManifestFile mf, StructType partitionType, DataFile f, Action<string, string, string?> error)
    {
        if (mf.Partitions is null) return;
        for (var i = 0; i < Math.Min(mf.Partitions.Count, f.Partition.Count); i++)
        {
            var summary = mf.Partitions[i];
            var value = f.Partition[i];
            if (value is null)
            {
                if (!summary.ContainsNull) error("SUMMARY-NULL", $"partition field {partitionType.Fields[i].Name} is null but the manifest summary says contains_null=false", f.FilePath);
                continue;
            }
            var type = partitionType.Fields[i].Type;
            if (type.TypeId is TypeId.Binary or TypeId.Uuid or TypeId.Fixed || summary.LowerBound is null || summary.UpperBound is null) continue;
            if (Compare(type, value, Conversions.FromBytes(type, summary.LowerBound)) < 0 || Compare(type, value, Conversions.FromBytes(type, summary.UpperBound)) > 0)
                error("SUMMARY-BOUNDS", $"partition value {value} of {partitionType.Fields[i].Name} is outside the manifest's partition summary", f.FilePath);
        }
    }

    private static int Compare(IcebergType type, object a, object b) => (a, b) switch
    {
        (string x, string y) => string.CompareOrdinal(x, y),
        (int x, long y) => ((long)x).CompareTo(y),
        _ => ((IComparable)a).CompareTo(b),
    };

    private static async Task CheckDeletionVectorAsync(IO.IFileIO io, DataFile dv, Action<string, string, string?> error, CancellationToken ct)
    {
        try
        {
            var puffin = PuffinFile.Read(await io.ReadAllAsync(dv.FilePath, ct));
            var blob = puffin.Blobs.FirstOrDefault(b => b.Offset == dv.ContentOffset && b.Length == dv.ContentSizeInBytes);
            if (blob is null) { error("DV-BLOB", $"no blob at offset {dv.ContentOffset} length {dv.ContentSizeInBytes}", dv.FilePath); return; }
            var positions = DeletionVector.Decode(puffin.Payload(blob));
            if (positions.Cardinality != dv.RecordCount)
                error("DV-CARDINALITY", $"manifest record_count {dv.RecordCount} but the vector holds {positions.Cardinality} positions", dv.FilePath);
            if (blob.Properties?.GetValueOrDefault(DeletionVector.ReferencedDataFileProperty) != dv.ReferencedDataFile)
                error("DV-BLOB-REFERENCE", "blob referenced-data-file property differs from the manifest entry", dv.FilePath);
        }
        catch (Exception e) when (e is FormatException or NotSupportedException)
        {
            error("DV-CORRUPT", e.Message, dv.FilePath);
        }
    }

    private static async Task CheckDataFileAsync(Table table, DataFile file, NameMapping? mapping, Action<string, string, string?> error, CancellationToken ct)
    {
        var m = table.Metadata;
        var schema = m.CurrentSchema;
        List<object?[]> rows;
        try { rows = await DataFiles.ReadAsync(table.IO, file, schema, mapping, ct); }
        catch (Exception e) when (e is not OperationCanceledException) { error("DATA-READ", e.Message, file.FilePath); return; }

        if (rows.Count != file.RecordCount) error("DATA-RECORD-COUNT", $"record_count {file.RecordCount} but the file has {rows.Count} rows", file.FilePath);

        var spec = m.SpecById(file.SpecId);
        if (spec.Fields.All(f => f.AllSourceIds.All(id => schema.StructPath(id) is not null)))
        {
            var accessors = spec.Fields.SelectMany(f => f.AllSourceIds).Distinct().ToDictionary(id => id, schema.Accessor);
            foreach (var row in rows)
            {
                var computed = spec.Partition(schema, id => accessors[id](row));
                if (!computed.SequenceEqual(file.Partition, PartitionValueComparer.Instance))
                {
                    error("DATA-PARTITION", $"a row's partition [{string.Join(", ", computed)}] differs from the file's [{string.Join(", ", file.Partition)}]", file.FilePath);
                    break;
                }
            }
        }

        for (var c = 0; c < schema.Columns.Count; c++)
        {
            var field = schema.Columns[c];
            var values = rows.Select(r => r[c]).ToList();
            if (file.NullValueCounts?.TryGetValue(field.Id, out var nulls) == true && nulls != values.Count(v => v is null))
                error("METRIC-NULLS", $"{field.Name}: null_value_count {nulls}, actual {values.Count(v => v is null)}", file.FilePath);
            if (file.ValueCounts?.TryGetValue(field.Id, out var count) == true && count != values.Count)
                error("METRIC-VALUES", $"{field.Name}: value_count {count}, actual {values.Count}", file.FilePath);
            var present = values.Where(v => v is not null && !(v is double d && double.IsNaN(d)) && !(v is float f && float.IsNaN(f))).ToList();
            if (present.Count == 0 || field.Type.TypeId is TypeId.Binary or TypeId.Uuid or TypeId.Fixed) continue;
            // Bounds may be truncated, so they only need to contain the data: lower ≤ min and upper ≥ max.
            if (file.LowerBounds?.TryGetValue(field.Id, out var lower) == true && present.Any(v => Compare(field.Type, v!, Conversions.FromBytes(field.Type, lower)) < 0))
                error("METRIC-LOWER", $"{field.Name}: lower bound is above a value in the file (pruning would skip live rows)", file.FilePath);
            if (file.UpperBounds?.TryGetValue(field.Id, out var upper) == true && present.Any(v => Compare(field.Type, v!, Conversions.FromBytes(field.Type, upper)) > 0))
                error("METRIC-UPPER", $"{field.Name}: upper bound is below a value in the file (pruning would skip live rows)", file.FilePath);
        }
    }

    private static async Task<long?> LengthOrNull(IO.IFileIO io, string location, CancellationToken ct)
    {
        try { return await io.LengthAsync(location, ct); }
        catch (Exception e) when (e is FileNotFoundException or DirectoryNotFoundException || (e is Amazon.S3.AmazonS3Exception s3 && s3.StatusCode == System.Net.HttpStatusCode.NotFound))
        {
            return null;
        }
    }

    private sealed class PartitionValueComparer : IEqualityComparer<object?>
    {
        public static readonly PartitionValueComparer Instance = new();
        public new bool Equals(object? a, object? b) => (a, b) switch
        {
            (null, null) => true,
            (byte[] x, byte[] y) => x.AsSpan().SequenceEqual(y),
            (int x, long y) => x == y,
            (long x, int y) => x == y,
            _ => object.Equals(a, b),
        };
        public int GetHashCode(object? o) => o?.GetHashCode() ?? 0;
    }
}
