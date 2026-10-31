using Microsoft.EntityFrameworkCore;

namespace Iceberg.Net.DuckLake;

/// <summary>A broken invariant of a DuckLake catalog, with where it was found.</summary>
public sealed record DuckLakeProblem(string Where, string What)
{
    public override string ToString() => $"{Where}: {What}";
}

public sealed record DuckLakeValidationOptions
{
    /// <summary>Also read every live data and delete file (sizes and row counts), not just check they exist.</summary>
    public bool Deep { get; init; }
}

/// <summary>
/// A consistency check of a whole DuckLake (the counterpart of Iceberg.Net's <c>TableValidator</c>): the invariants the
/// spec's tables imply, across every snapshot. Finds what a buggy writer (this one or another) leaves behind; reports,
/// never repairs.
/// </summary>
public static class DuckLakeValidator
{
    public static async Task<IReadOnlyList<DuckLakeProblem>> ValidateAsync(DuckLakeCatalog catalog, DuckLakeValidationOptions? options = null,
        CancellationToken ct = default)
    {
        options ??= new DuckLakeValidationOptions();
        var problems = new List<DuckLakeProblem>();
        void Problem(string where, string what) => problems.Add(new DuckLakeProblem(where, what));
        await using var db = catalog.Context();

        // ducklake_snapshot: counters never going back, changes that parse. Expiry leaves gaps (any snapshot but the
        // latest can go), but every snapshot ID that ever existed has a changes row (the spec keeps them on expiry;
        // DuckDB 1.5.5 does not, so a missing one is not a problem).
        var snapshots = await db.Snapshots.OrderBy(s => s.SnapshotId).ToListAsync(ct);
        if (snapshots.Count == 0) Problem("ducklake_snapshot", "there are no snapshots");
        for (var i = 1; i < snapshots.Count; i++)
        {
            var (prev, s) = (snapshots[i - 1], snapshots[i]);
            if (s.SchemaVersion < prev.SchemaVersion) Problem($"snapshot {s.SnapshotId}", "schema_version went back");
            if (s.NextCatalogId < prev.NextCatalogId) Problem($"snapshot {s.SnapshotId}", "next_catalog_id went back");
            if (s.NextFileId < prev.NextFileId) Problem($"snapshot {s.SnapshotId}", "next_file_id went back");
            if (s.SnapshotTime < prev.SnapshotTime) Problem($"snapshot {s.SnapshotId}", "snapshot_time is before its predecessor's");
        }
        foreach (var c in await db.SnapshotChanges.ToListAsync(ct))
        {
            try { DuckLakeChange.Parse(c.ChangesMade); }
            catch (InvalidDataException e) { Problem($"snapshot {c.SnapshotId}", e.Message); }
        }
        if (snapshots.Count == 0) return problems;
        var latest = snapshots[^1];

        // IDs below the counters of the latest snapshot.
        var maxSchema = await db.Schemas.MaxAsync(x => (long?)x.SchemaId, ct) ?? -1;
        var maxTable = await db.Tables.MaxAsync(x => (long?)x.TableId, ct) ?? -1;
        var maxPartition = await db.PartitionInfos.MaxAsync(x => (long?)x.PartitionId, ct) ?? -1;
        if (Math.Max(maxSchema, Math.Max(maxTable, maxPartition)) >= latest.NextCatalogId)
            Problem("ducklake_snapshot", $"next_catalog_id {latest.NextCatalogId} is not above every schema, table and partition ID");
        var maxData = await db.DataFiles.MaxAsync(x => (long?)x.DataFileId, ct) ?? -1;
        var maxDelete = await db.DeleteFiles.MaxAsync(x => (long?)x.DeleteFileId, ct) ?? -1;
        if (Math.Max(maxData, maxDelete) >= latest.NextFileId)
            Problem("ducklake_snapshot", $"next_file_id {latest.NextFileId} is not above every data and delete file ID");

        // Versioned rows: begin before end, neither after the latest snapshot, and (expiry's rule) visible in at least
        // one retained snapshot.
        var ids = snapshots.Select(s => s.SnapshotId).ToHashSet();
        var retained = snapshots.Select(s => s.SnapshotId).ToArray();
        void Range(string where, long begin, long? end)
        {
            if (begin > latest.SnapshotId) Problem(where, $"begin_snapshot {begin} is after the latest snapshot {latest.SnapshotId}");
            if (end is { } e && e <= begin) Problem(where, $"end_snapshot {e} is not after begin_snapshot {begin}");
            if (end is { } e2 && e2 > latest.SnapshotId) Problem(where, $"end_snapshot {e2} is after the latest snapshot {latest.SnapshotId}");
            if (end is { } e3 && e3 > begin && !retained.Any(s => s >= begin && s < e3))
                Problem(where, $"is visible in no retained snapshot ({begin} to {e3}) but was not expired");
        }
        var dataFiles = await db.DataFiles.ToListAsync(ct);
        var deleteFiles = await db.DeleteFiles.ToListAsync(ct);
        foreach (var f in dataFiles) Range($"data file {f.DataFileId}", f.BeginSnapshot, f.EndSnapshot);
        foreach (var d in deleteFiles) Range($"delete file {d.DeleteFileId}", d.BeginSnapshot, d.EndSnapshot);
        foreach (var t in await db.Tables.ToListAsync(ct)) Range($"table {t.TableId}", t.BeginSnapshot, t.EndSnapshot);
        foreach (var c in await db.Columns.ToListAsync(ct)) Range($"table {c.TableId} column {c.ColumnId}", c.BeginSnapshot, c.EndSnapshot);

        // A delete file belongs to a data file of its table that is live whenever the delete file is.
        var dataById = dataFiles.ToDictionary(f => f.DataFileId);
        foreach (var d in deleteFiles)
        {
            if (!dataById.TryGetValue(d.DataFileId, out var f)) { Problem($"delete file {d.DeleteFileId}", $"data file {d.DataFileId} does not exist"); continue; }
            if (f.TableId != d.TableId) Problem($"delete file {d.DeleteFileId}", $"belongs to table {d.TableId}, its data file to table {f.TableId}");
            if (d.BeginSnapshot < f.BeginSnapshot || (f.EndSnapshot is { } fe && (d.EndSnapshot is not { } de || de > fe)))
                Problem($"delete file {d.DeleteFileId}", $"is live outside the life of data file {f.DataFileId}");
            if (d.DeleteCount is { } n && n > f.RecordCount) Problem($"delete file {d.DeleteFileId}", $"deletes {n} of {f.RecordCount} rows");
        }

        // Nothing scheduled for deletion is still referenced.
        var scheduled = (await db.FilesScheduledForDeletion.ToListAsync(ct)).Select(f => (f.DataFileId, f.Path)).ToList();
        foreach (var (id, path) in scheduled)
            if (dataById.ContainsKey(id) || deleteFiles.Any(d => d.DeleteFileId == id))
                Problem($"scheduled file {id}", $"{path} is scheduled for deletion but still in the catalog");

        // Per snapshot: at most one delete file per data file; live tables have a valid column tree; row IDs do not overlap.
        foreach (var s in ids)
        {
            foreach (var g in deleteFiles.Where(d => d.BeginSnapshot <= s && (d.EndSnapshot == null || s < d.EndSnapshot)).GroupBy(d => d.DataFileId).Where(g => g.Count() > 1))
                Problem($"snapshot {s}", $"data file {g.Key} has {g.Count()} delete files");
        }
        foreach (var table in (await DuckLakeCatalog.TablesAsync(db, latest.SnapshotId, ct)))
        {
            try { await DuckLakeCatalog.ColumnsAsync(db, table.TableId, latest.SnapshotId, ct); }
            catch (Exception e) when (Corruption.IsFormatError(e)) { Problem($"table {table.TableId}", e.Message); }
            var ranges = dataFiles.Where(f => f.TableId == table.TableId && f.RowIdStart is not null && f.PartialMax is null)
                .Select(f => (f.DataFileId, Start: f.RowIdStart!.Value, End: f.RowIdStart!.Value + f.RecordCount)).OrderBy(r => r.Start).ToList();
            for (var i = 1; i < ranges.Count; i++)
                if (ranges[i].Start < ranges[i - 1].End)
                    Problem($"table {table.TableId}", $"row IDs of data files {ranges[i - 1].DataFileId} and {ranges[i].DataFileId} overlap");
            var stats = await db.TableStats.FirstOrDefaultAsync(x => x.TableId == table.TableId, ct);
            if (ranges.Count > 0 && (stats?.NextRowId ?? 0) < ranges[^1].End)
                Problem($"table {table.TableId}", $"next_row_id {stats?.NextRowId} is not above the last file's row IDs ({ranges[^1].End})");
        }

        // Live files exist in storage, with the recorded size (and, deep, the recorded rows).
        foreach (var t in await DuckLakeCatalog.TablesAsync(db, latest.SnapshotId, ct))
        {
            DuckLakeTable table;
            try
            {
                var schemaName = (await catalog.SchemasAsync(db, latest.SnapshotId, ct)).Single(x => x.SchemaId == t.SchemaId).Name;
                table = await catalog.LoadTableAsync(db, schemaName, t.Name, latest.SnapshotId, ct);
            }
            catch (Exception e) when (Corruption.IsFormatError(e) || e is InvalidOperationException) { Problem($"table {t.TableId}", e.Message); continue; }
            DuckLakeScanPlan plan;
            try { plan = await catalog.PlanScanAsync(db, table, null, ct); }
            catch (NotSupportedException) { continue; } // files this library does not read are not checked
            catch (Exception e) when (Corruption.IsFormatError(e))
            {
                if (!problems.Any(p => e.Message.Contains(p.Where, StringComparison.Ordinal))) Problem($"table {t.TableId}", e.Message);
                continue;
            }
            foreach (var f in plan.Files)
            {
                if (!await catalog.IO.ExistsAsync(f.Location, ct)) { Problem($"data file {f.DataFileId}", $"{f.Location} does not exist"); continue; }
                var size = await catalog.IO.LengthAsync(f.Location, ct);
                if (size != f.FileSizeBytes) Problem($"data file {f.DataFileId}", $"is {size} bytes, recorded as {f.FileSizeBytes}");
                if (f.DeleteFileLocation is { } dl && !await catalog.IO.ExistsAsync(dl, ct)) Problem($"data file {f.DataFileId}", $"delete file {dl} does not exist");
                if (!options.Deep) continue;
                var rows = 0L;
                await foreach (var batch in catalog.ReadDataFileAsync(table, f.Location, f.Mapping, DuckLakeCatalog.FileProjection(table), new Data.DataFileReadOptions { DecryptionKey = f.Key }, ct))
                    rows += batch.Rows.Count;
                if (rows != f.RecordCount) Problem($"data file {f.DataFileId}", $"holds {rows} rows, recorded as {f.RecordCount}");
                if (f.DeleteFileLocation is { } d)
                {
                    var positions = await DuckLakeDeleteFiles.ReadPositionsAsync(catalog.IO, d, ct: ct, key: f.DeleteKey);
                    if (positions.Any(p => p < 0 || p >= f.RecordCount)) Problem($"data file {f.DataFileId}", "its delete file has positions outside the file");
                }
            }
        }
        return problems;
    }

    /// <summary>Throw when <see cref="ValidateAsync"/> finds anything (for tests and checks in pipelines).</summary>
    public static async Task AssertValidAsync(DuckLakeCatalog catalog, DuckLakeValidationOptions? options = null, CancellationToken ct = default)
    {
        var problems = await ValidateAsync(catalog, options, ct);
        if (problems.Count > 0) throw new InvalidDataException("DuckLake is inconsistent:\n" + string.Join("\n", problems));
    }
}
