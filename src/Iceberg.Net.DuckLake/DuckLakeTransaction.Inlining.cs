using Iceberg.Net.Data;
using Iceberg.Net.Types;
using Microsoft.EntityFrameworkCore;

namespace Iceberg.Net.DuckLake;

/// <summary>One table's part of <see cref="DuckLakeCatalog.FlushInlinedDataAsync"/>.</summary>
public sealed record DuckLakeFlush(string Schema, string Table, long RowsFlushed);

public sealed partial class DuckLakeTransaction
{
    private readonly Dictionary<PendingTable, int> _inliningLimits = [];

    /// <summary>
    /// The data inlining row limit for a table: persisted in <c>ducklake_metadata</c> (table, then schema, then catalog
    /// scope; DuckDB's <c>set_option('data_inlining_row_limit', …)</c>), else <see cref="DuckLakeOptions.DataInliningRowLimit"/>.
    /// </summary>
    private async Task<int> InliningLimitAsync(PendingTable t, CancellationToken ct)
    {
        if (_inliningLimits.TryGetValue(t, out var cached)) return cached;
        var text = await SettingAsync(t, "data_inlining_row_limit", ct);
        var limit = text is not null && int.TryParse(text, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var persisted)
            ? persisted : _catalog.Options.DataInliningRowLimit;
        return _inliningLimits[t] = Math.Max(0, limit);
    }

    /// <summary>A <c>ducklake_metadata</c> option for a table: table scope, then schema, then catalog (as DuckDB resolves them).</summary>
    private async Task<string?> SettingAsync(PendingTable t, string key, CancellationToken ct)
    {
        await using var db = _catalog.Context(_stage);
        var rows = await db.Metadata.Where(m => m.Key == key).ToListAsync(ct);
        string? Scoped(string? scope, long? id) => rows.LastOrDefault(r => r.Scope == scope && (scope is null || r.ScopeId == id))?.Value;
        return (t.TableId is { } tableId ? Scoped("table", tableId) : null)
               ?? (t.Schema.SchemaId is { } schemaId ? Scoped("schema", schemaId) : null)
               ?? Scoped(null, null);
    }

    /// <summary>Whether deletes from a table are written as deletion vectors (<c>write_deletion_vectors</c>, format 1.1 only).</summary>
    private async Task<bool> WritesDeletionVectorsAsync(PendingTable t, CancellationToken ct) =>
        _catalog.Is11 && (await SettingAsync(t, "write_deletion_vectors", ct) is { } text
            ? text.Equals("true", StringComparison.OrdinalIgnoreCase)
            : _catalog.Options.WriteDeletionVectors);

    /// <summary>
    /// Inlined rows moved to Parquet (DuckDB's <c>ducklake_flush_inlined_data</c>). Each inlined data table becomes one
    /// partial data file in its own schema version's columns, holding every row version with its row ID and snapshot;
    /// rows deleted while inlined go into a partial delete file. Inlined deletions of live data files are merged with
    /// the file's delete file into a partial delete file. Every snapshot reads exactly what it did before.
    /// </summary>
    private sealed record FlushOp(PendingTable Table, IReadOnlyList<FlushedRows> Files, IReadOnlyList<FlushedDeletions> Deletions, IReadOnlyList<string> InlinedTables,
        long RowsFlushed) : Op;

    private sealed record FlushedRows(WrittenFile File, long Begin, long PartialMax, long RowIdStart, WrittenDelete? Deletes, long DeletesBegin, long DeletesPartialMax);

    private sealed record FlushedDeletions(long DataFileId, long? OldDeleteFileId, string? OldDeleteLocation, WrittenDelete File, long Begin, long PartialMax);

    internal async Task<DuckLakeFlush?> FlushInlinedDataAsync(string schema, string table, CancellationToken ct)
    {
        PendingTable t;
        DuckLakeTable loaded;
        var inlined = new List<(string Name, IReadOnlyList<DuckLakeColumn> Columns, List<(long RowId, long Begin, long? End, object?[] Values)> Rows)>();
        Dictionary<long, List<(long Position, long Snapshot)>> deletions = [];
        DuckLakeScanPlan plan;
        Dictionary<long, DeleteFileEntity> deleteRows;
        await using (var db = await BeginAsync(ct))
        {
            (t, loaded) = await CompactableAsync(db, schema, table, ct);
            var tableId = loaded.TableId;
            foreach (var registered in await db.InlinedDataTables.Where(i => i.TableId == tableId).OrderBy(i => i.SchemaVersion).ToListAsync(ct))
            {
                var (columns, rows) = await DuckLakeInlinedData.ReadAllAsync(db, tableId, registered.TableName, registered.SchemaVersion, ct);
                if (rows.Count > 0) inlined.Add((registered.TableName, columns, rows));
            }
            plan = await _catalog.PlanScanAsync(db, loaded, null, ct);
            deleteRows = await DuckLakeCatalog.DeleteFilesAsync(db, tableId, loaded.SnapshotId, ct);
            if (await DuckLakeCatalog.TableExistsAsync(db, DuckLakeCatalog.InlinedDeleteTable(tableId), ct))
            {
                var live = plan.Files.Select(f => f.DataFileId).ToHashSet();
                var (c, tx) = db.Raw();
                foreach (var (file, pos, begin) in await Db.QueryAsync(c, tx, $"SELECT file_id, row_id, begin_snapshot FROM {Db.Quote(DuckLakeCatalog.InlinedDeleteTable(tableId))}",
                             r => (Db.ReqLong(r, 0), Db.ReqLong(r, 1), Db.ReqLong(r, 2)), ct))
                    if (live.Contains(file)) (deletions.TryGetValue(file, out var l) ? l : deletions[file] = []).Add((pos, begin));
            }
        }
        if (inlined.Count == 0 && deletions.Count == 0) return null;

        var files = new List<FlushedRows>();
        foreach (var (_, columns, rows) in inlined)
        {
            rows.Sort((a, b) => a.RowId.CompareTo(b.RowId));
            var file = await WriteCompactedAsync(t, columns, "", rows.Select(r => (r.Values, r.RowId, r.Begin)).ToList(), keepSnapshots: true, [], ct);
            // Rows deleted while inlined: a partial delete file, each position with the snapshot that deleted it.
            var ended = rows.Select((r, pos) => (r.End, Position: (long)pos)).Where(x => x.End is not null).Select(x => (x.Position, Snapshot: x.End!.Value)).ToList();
            WrittenDelete? deletes = null;
            if (ended.Count > 0) deletes = await WritePartialDeletesAsync(t, file.RelativePath, ended, ct);
            files.Add(new FlushedRows(file, rows.Min(r => r.Begin), rows.Max(r => r.Begin), rows.Min(r => r.RowId), deletes,
                ended.Count > 0 ? ended.Min(e => e.Snapshot) : 0, ended.Count > 0 ? ended.Max(e => e.Snapshot) : 0));
        }
        var merged = new List<FlushedDeletions>();
        foreach (var (fileId, inline) in deletions)
        {
            var task = plan.Files.Single(f => f.DataFileId == fileId);
            // The merged partial delete file begins at its earliest deletion and replaces the current delete file; an
            // inlined deletion older than that file would make two delete files live at once, so such a file stays inlined.
            if (deleteRows.TryGetValue(fileId, out var current) && inline.Any(d => d.Snapshot < current.BeginSnapshot)) continue;
            // The file's current deletions keep the snapshot that made them (per row for a partial delete file).
            var entries = new List<(long Position, long Snapshot)>(inline);
            long? oldId = null;
            if (deleteRows.TryGetValue(fileId, out var old))
            {
                oldId = old.DeleteFileId;
                foreach (var (pos, snapshot) in await DuckLakeDeleteFiles.ReadEntriesAsync(_catalog.IO, task.DeleteFileLocation!, ct, task.DeleteKey))
                    entries.Add((pos, snapshot ?? old.BeginSnapshot));
            }
            var relative = task.Location.StartsWith(t.Location, StringComparison.Ordinal) ? task.Location[t.Location.Length..] : task.Location;
            var written = await WritePartialDeletesAsync(t, relative, entries, ct);
            merged.Add(new FlushedDeletions(fileId, oldId, task.DeleteFileLocation, written, entries.Min(e => e.Snapshot), entries.Max(e => e.Snapshot)));
        }
        if (files.Count == 0 && merged.Count == 0) return null;
        var flushed = inlined.Sum(i => (long)i.Rows.Count);
        _ops.Add(new FlushOp(t, files, merged, inlined.Select(i => i.Name).ToList(), flushed));
        t.Compacted = true;
        return new DuckLakeFlush(t.Schema.Name, t.Name, flushed);
    }

    /// <summary>A partial delete file for the data file at <paramref name="dataFileRelative"/> (positions with the snapshot of each deletion).</summary>
    private async Task<WrittenDelete> WritePartialDeletesAsync(PendingTable t, string dataFileRelative, IReadOnlyCollection<(long Position, long Snapshot)> entries,
        CancellationToken ct)
    {
        var relative = $"ducklake-{Guid.CreateVersion7()}-delete.parquet";
        var location = t.Location + relative;
        _written.Add(location);
        var (key, keyText) = _catalog.NewKey();
        var (size, footer) = await DuckLakeDeleteFiles.WritePartialAsync(_catalog.IO, location, DuckLakeCatalog.Resolve(t.Location, dataFileRelative, true), entries, ct, key);
        return new WrittenDelete(relative, entries.Count, size, footer) { Key = keyText };
    }
}
