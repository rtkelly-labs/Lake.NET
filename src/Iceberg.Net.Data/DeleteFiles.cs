using Iceberg.Net.Avro;
using Iceberg.Net.Catalog;
using Iceberg.Net.IO;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Puffin;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>v2 position delete files: Parquet with the reserved <c>file_path</c>/<c>pos</c> columns ("Position Delete Files").</summary>
public static class PositionDeleteFiles
{
    public const int FilePathFieldId = 2147483546;
    public const int PosFieldId = 2147483545;

    public static readonly Schema Schema = new(0,
    [
        NestedField.Require(FilePathFieldId, "file_path", PrimitiveType.String),
        NestedField.Require(PosFieldId, "pos", PrimitiveType.Long),
    ]);

    /// <summary>Write one position delete file, rows sorted by (file_path, pos) as the spec requires.</summary>
    public static async Task<DataFile> WriteAsync(IFileIO io, string location, int specId, IReadOnlyList<object?> partition,
        IEnumerable<(string FilePath, long Pos)> deletes, CancellationToken ct = default)
    {
        var rows = deletes.Distinct().OrderBy(d => d.FilePath, StringComparer.Ordinal).ThenBy(d => d.Pos)
            .Select(d => new object?[] { d.FilePath, d.Pos }).ToList();
        var file = await ParquetFiles.WriteAsync(io, location, Schema, specId, partition, rows, ct);
        return file with { Content = FileContent.PositionDeletes };
    }

    public static async Task<IReadOnlyDictionary<string, RoaringPositionBitmap>> ReadAsync(IFileIO io, string location, CancellationToken ct = default) =>
        await ReadAsync(io, location, "PARQUET", ct);

    public static Task<IReadOnlyDictionary<string, RoaringPositionBitmap>> ReadAsync(IFileIO io, string location, string format, CancellationToken ct = default) =>
        ReadAsync(io, location, format, null, ct);

    /// <summary>A manifest's position delete file: its <c>file_size_in_bytes</c> lets it be read in ranges (Parquet) or streamed (Avro).</summary>
    public static Task<IReadOnlyDictionary<string, RoaringPositionBitmap>> ReadAsync(IFileIO io, DataFile file, CancellationToken ct = default) =>
        ReadAsync(io, file.FilePath, file.FileFormat, file.FileSizeInBytes > 0 ? file.FileSizeInBytes : null, ct);

    private static async Task<IReadOnlyDictionary<string, RoaringPositionBitmap>> ReadAsync(IFileIO io, string location, string format, long? size, CancellationToken ct)
    {
        var result = new Dictionary<string, RoaringPositionBitmap>(StringComparer.Ordinal);
        var rows = new List<object?[]>();
        await foreach (var batch in DataFiles.ReadBatchesAsync(io, location, format, Schema, null, new DataFileReadOptions { FileSize = size }, ct)) rows.AddRange(batch.Rows);
        foreach (var row in rows)
        {
            var path = (string)row[0]!;
            if (!result.TryGetValue(path, out var bm)) result[path] = bm = new RoaringPositionBitmap();
            bm.Add((long)row[1]!);
        }
        return result;
    }
}

/// <summary>
/// Equality delete files: Parquet holding the key values of deleted rows, columns carrying the table's field IDs
/// and recorded in the manifest's <c>equality_ids</c>. Cheap to write (no scan of existing data) but every later
/// read pays to apply them; Athena/Trino/Spark read them, PyIceberg 0.12 refuses tables that have them, and v4
/// forbids writing new ones.
/// </summary>
public static class EqualityDeleteFiles
{
    public static async Task<DataFile> WriteAsync(IFileIO io, string location, int specId, IReadOnlyList<object?> partition,
        IReadOnlyList<NestedField> keyFields, IEnumerable<object?[]> keys, CancellationToken ct = default)
    {
        var schema = new Schema(0, keyFields);
        var distinct = keys.GroupBy(k => EqualityKey.Encode(keyFields, i => k[i]), StringComparer.Ordinal).Select(g => g.First()).ToList();
        var file = await ParquetFiles.WriteAsync(io, location, schema, specId, partition, distinct, ct);
        return file with { Content = FileContent.EqualityDeletes, EqualityIds = keyFields.Select(f => f.Id).ToList() };
    }
}

/// <summary>A live delete file in a snapshot, with the data sequence number that decides what it applies to.</summary>
public sealed record DeleteEntry(DataFile File, long DataSequenceNumber, ManifestFile Manifest)
{
    public bool IsDeletionVector => File.FileFormat == "PUFFIN";
}

/// <summary>
/// Equality-key encoding: a row matches an equality delete when the values of the delete's equality columns
/// are equal, with null matching null (spec "Equality Delete Files"). Values are compared through their
/// single-value binary serialization so promoted widths (int read as long) and byte arrays compare correctly.
/// </summary>
public static class EqualityKey
{
    public static string Encode(IReadOnlyList<NestedField> fields, Func<int, object?> valueOf)
    {
        var parts = new string[fields.Count];
        for (var i = 0; i < fields.Count; i++)
        {
            var v = valueOf(i);
            parts[i] = v is null ? "\u0000" : Convert.ToBase64String(Conversions.ToBytes(fields[i].Type, Normalise(fields[i].Type, v)));
        }
        return string.Join('|', parts);
    }

    private static object Normalise(IcebergType type, object v) => (type.TypeId, v) switch
    {
        (TypeId.Long or TypeId.Timestamp or TypeId.TimestampTz or TypeId.Time, int i) => (long)i,
        (TypeId.Double, float f) => (double)f,
        _ => v,
    };
}

/// <summary>
/// The delete files of a snapshot, indexed for planning. Applicability rules (spec "Scan Planning"):
/// a position delete or DV applies to a data file when its data sequence number is ≥ the data file's;
/// when a data file has a DV, position delete files for it are ignored (v3: the DV already folds them in).
/// An equality delete applies when its data sequence number is strictly greater than the data file's (so a
/// delete and an insert of the same key in one commit keep the new row) and it is in the data file's
/// partition, or is unpartitioned (global).
/// </summary>
public sealed class DeleteIndex
{
    private readonly Dictionary<string, DeleteEntry> _dvs = new(StringComparer.Ordinal);
    private readonly List<DeleteEntry> _positionFiles = [];
    private readonly List<DeleteEntry> _equalityFiles = [];
    private readonly Dictionary<string, Task<HashSet<string>>> _equalityCache = new(StringComparer.Ordinal);
    private TableMetadata _metadata = null!;
    private readonly Dictionary<string, Task<IReadOnlyDictionary<string, RoaringPositionBitmap>>> _positionCache = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Task<PuffinFile.Contents>> _puffinCache = new(StringComparer.Ordinal);
    private readonly IFileIO _io;

    private DeleteIndex(IFileIO io) => _io = io;

    /// <summary>Position deletes and deletion vectors (equality deletes are in <see cref="EqualityDeletes"/>).</summary>
    public IReadOnlyList<DeleteEntry> All => [.. _dvs.Values, .. _positionFiles];
    public IReadOnlyList<DeleteEntry> EqualityDeletes => _equalityFiles;
    public bool IsEmpty => _dvs.Count == 0 && _positionFiles.Count == 0 && _equalityFiles.Count == 0;

    public static async Task<DeleteIndex> LoadAsync(Table table, IReadOnlyList<ManifestFile> manifests, CancellationToken ct = default)
    {
        var index = new DeleteIndex(table.IO) { _metadata = table.Metadata };
        var m = table.Metadata;
        foreach (var manifest in manifests.Where(mf => mf.Content == ManifestContent.Deletes))
        {
            var contents = (await ManifestStore.ReadAsync(table.IO, manifest.ManifestPath, m.SpecById(manifest.PartitionSpecId), m.CurrentSchema, manifest, ct: ct));
            foreach (var e in contents.Entries.Where(e => e.Status != ManifestEntryStatus.Deleted))
            {
                var entry = new DeleteEntry(e.DataFile, e.SequenceNumber ?? 0, manifest);
                switch (e.DataFile.Content)
                {
                    case FileContent.EqualityDeletes:
                        if (e.DataFile.EqualityIds is not { Count: > 0 }) throw new FormatException($"Equality delete file {e.DataFile.FilePath} has no equality_ids");
                        index._equalityFiles.Add(entry);
                        break;
                    case FileContent.PositionDeletes when entry.IsDeletionVector:
                        var target = e.DataFile.ReferencedDataFile ?? throw new FormatException("Deletion vector without referenced_data_file");
                        if (!index._dvs.TryAdd(target, entry))
                            throw new InvalidOperationException($"Table has more than one deletion vector for {target}, which the spec forbids");
                        break;
                    case FileContent.PositionDeletes:
                        index._positionFiles.Add(entry);
                        break;
                }
            }
        }
        return index;
    }

    /// <summary>
    /// An index for deletes someone else already matched to data files (server-side scan planning): it holds no files
    /// itself; <see cref="RowFilterAsync(DataFile, IReadOnlyList{DataFile}, Schema, CancellationToken)"/> applies exactly
    /// the deletes given for each file, caching what it reads across files.
    /// </summary>
    public static DeleteIndex ForResolvedDeletes(Table table) => new(table.IO) { _metadata = table.Metadata };

    /// <summary>
    /// Like <see cref="RowFilterAsync(DataFile, long, Schema, CancellationToken)"/>, but with the applicable delete files
    /// given (sequence numbers and partition scoping were already applied by whoever chose them). Position deletes and
    /// deletion vectors still only remove rows recorded against <paramref name="dataFile"/>'s path.
    /// </summary>
    public async Task<Func<long, object?[], bool>?> RowFilterAsync(DataFile dataFile, IReadOnlyList<DataFile> applicable, Schema projection, CancellationToken ct = default)
    {
        RoaringPositionBitmap? positions = null;
        foreach (var d in applicable.Where(d => d.Content == FileContent.PositionDeletes))
        {
            if (d.ReferencedDataFile is { } r && r != dataFile.FilePath) continue;
            RoaringPositionBitmap? bm;
            if (d.FileFormat == "PUFFIN") bm = await ReadDeletionVectorAsync(d, ct);
            else (await PositionDeletesAsync(d)).TryGetValue(dataFile.FilePath, out bm);
            if (bm is not null) (positions ??= new RoaringPositionBitmap()).Or(bm);
        }
        var equality = await EqualityFiltersAsync(applicable.Where(d => d.Content == FileContent.EqualityDeletes), projection);
        if (positions is null && equality.Count == 0) return null;
        return (pos, row) => (positions?.Contains(pos) ?? false)
            || equality.Any(eq => eq.Keys.Contains(EqualityKey.Encode(eq.Fields, i => row[eq.Columns[i]])));
    }

    private async Task<List<(int[] Columns, IReadOnlyList<NestedField> Fields, HashSet<string> Keys)>> EqualityFiltersAsync(IEnumerable<DataFile> files, Schema projection)
    {
        var equality = new List<(int[] Columns, IReadOnlyList<NestedField> Fields, HashSet<string> Keys)>();
        foreach (var group in files.GroupBy(f => string.Join(',', (f.EqualityIds ?? throw new FormatException($"Equality delete file {f.FilePath} has no equality_ids")).Order())))
        {
            var ids = group.First().EqualityIds!.Order().ToList();
            var columns = ids.Select(id => projection.Columns.ToList().FindIndex(c => c.Id == id)).ToArray();
            if (columns.Any(c => c < 0)) throw new NotSupportedException($"Equality deletes on columns [{string.Join(',', ids)}] must be top-level columns of the projection");
            var fields = columns.Select(c => projection.Columns[c]).ToList();
            var keys = new HashSet<string>(StringComparer.Ordinal);
            foreach (var f in group) keys.UnionWith(await EqualityKeysAsync(f, fields));
            equality.Add((columns, fields, keys));
        }
        return equality;
    }

    /// <summary>
    /// The delete files that apply to <paramref name="dataFile"/> (spec "Scan Planning"), chosen from metadata alone,
    /// as a server-side planner reports them: its deletion vector when it has one (position delete files are then
    /// ignored, as in <see cref="DeletedPositionsAsync"/>), else the position delete files with a data sequence number
    /// ≥ its own that are global or in its partition and whose <c>file_path</c> bounds (or <c>referenced_data_file</c>)
    /// admit its path; plus the equality delete files that apply. Position delete files may still hold no row for it.
    /// </summary>
    public IReadOnlyList<DataFile> DeleteFilesFor(DataFile dataFile, long dataSequenceNumber)
    {
        var result = new List<DataFile>();
        if (_dvs.TryGetValue(dataFile.FilePath, out var dv))
        {
            if (dv.DataSequenceNumber >= dataSequenceNumber) result.Add(dv.File);
        }
        else
        {
            foreach (var pd in _positionFiles)
                if (pd.DataSequenceNumber >= dataSequenceNumber && InScope(pd.File, dataFile) && MayReference(pd.File, dataFile.FilePath))
                    result.Add(pd.File);
        }
        foreach (var eq in _equalityFiles)
            if (AppliesTo(eq, dataFile, dataSequenceNumber)) result.Add(eq.File);
        return result;
    }

    private bool InScope(DataFile delete, DataFile data) =>
        _metadata.SpecById(delete.SpecId).IsUnpartitioned || (delete.SpecId == data.SpecId && delete.Partition.SequenceEqual(data.Partition));

    private static bool MayReference(DataFile delete, string path)
    {
        if (delete.ReferencedDataFile is { } referenced) return referenced == path;
        var bytes = System.Text.Encoding.UTF8.GetBytes(path);
        if (delete.LowerBounds?.GetValueOrDefault(PositionDeleteFiles.FilePathFieldId) is { } lower && bytes.AsSpan().SequenceCompareTo(lower) < 0) return false;
        if (delete.UpperBounds?.GetValueOrDefault(PositionDeleteFiles.FilePathFieldId) is { } upper && bytes.AsSpan().SequenceCompareTo(upper) > 0) return false;
        return true;
    }

    public DeleteEntry? DeletionVectorFor(string dataFile) => _dvs.GetValueOrDefault(dataFile);

    /// <summary>
    /// <paramref name="projection"/> plus the equality-delete columns it lacks, appended so the projection's positions
    /// are kept: read data files with it and pass it (and the rows read) to <c>RowFilterAsync</c>. A delete still
    /// applies after its column is renamed, promoted or dropped, or when a time-travel read's schema predates a later
    /// one: each missing column is taken from the newest table schema that has it (the widest type it was promoted
    /// to, which reads older files too), so delete keys and data values are decoded by field ID with one type.
    /// </summary>
    public Schema WithEqualityColumns(Schema projection)
    {
        if (_equalityFiles.Count == 0) return projection;
        var present = projection.Columns.Select(c => c.Id).ToHashSet();
        var extra = new List<NestedField>();
        foreach (var id in _equalityFiles.SelectMany(e => e.File.EqualityIds ?? []).Distinct().Order())
        {
            if (present.Contains(id)) continue;
            // Optional: rows of files written without the column read as null, which is what the delete compares.
            var field = _metadata.Schemas.OrderByDescending(s => s.SchemaId).Select(s => s.Columns.FirstOrDefault(c => c.Id == id)).FirstOrDefault(f => f is not null)
                ?? throw new NotSupportedException($"Equality deletes on field {id}: it is not a top-level column of any table schema");
            // A dropped column's name may since have been reused: names only matter to files without field IDs.
            var name = projection.FindField(field.Name) is null && extra.All(e => e.Name != field.Name) ? field.Name : $"{field.Name}#{id}";
            extra.Add(field with { Name = name, Required = false });
            present.Add(id);
        }
        return extra.Count == 0 ? projection : new Schema(projection.SchemaId, [.. projection.Columns, .. extra], projection.IdentifierFieldIds);
    }

    /// <summary>Positions deleted from <paramref name="dataFile"/>, or null when nothing applies.</summary>
    public async Task<RoaringPositionBitmap?> DeletedPositionsAsync(DataFile dataFile, long dataSequenceNumber, CancellationToken ct = default)
    {
        if (_dvs.TryGetValue(dataFile.FilePath, out var dv))
            return dv.DataSequenceNumber >= dataSequenceNumber ? await ReadDeletionVectorAsync(dv.File, ct) : null;

        RoaringPositionBitmap? result = null;
        foreach (var pd in _positionFiles)
        {
            if (pd.DataSequenceNumber < dataSequenceNumber) continue;
            if (pd.File.ReferencedDataFile is { } r && r != dataFile.FilePath) continue;
            var byFile = await PositionDeletesAsync(pd.File);
            if (!byFile.TryGetValue(dataFile.FilePath, out var bm)) continue;
            (result ??= new RoaringPositionBitmap()).Or(bm);
        }
        return result;
    }

    /// <summary>
    /// A predicate over (position, projected row) that is true for rows deleted by any applicable delete
    /// file, or null when nothing applies to <paramref name="dataFile"/>. Equality columns must be top-level
    /// columns of <paramref name="projection"/>.
    /// </summary>
    public async Task<Func<long, object?[], bool>?> RowFilterAsync(DataFile dataFile, long dataSequenceNumber, Schema projection, CancellationToken ct = default)
    {
        var positions = await DeletedPositionsAsync(dataFile, dataSequenceNumber, ct);
        var equality = await EqualityFiltersAsync(_equalityFiles.Where(e => AppliesTo(e, dataFile, dataSequenceNumber)).Select(e => e.File), projection);
        if (positions is null && equality.Count == 0) return null;
        return (pos, row) => (positions?.Contains(pos) ?? false)
            || equality.Any(eq => eq.Keys.Contains(EqualityKey.Encode(eq.Fields, i => row[eq.Columns[i]])));
    }

    private bool AppliesTo(DeleteEntry delete, DataFile data, long dataSequenceNumber)
    {
        if (delete.DataSequenceNumber <= dataSequenceNumber) return false;
        var deleteSpec = _metadata.SpecById(delete.File.SpecId);
        if (deleteSpec.IsUnpartitioned) return true;
        return delete.File.SpecId == data.SpecId && delete.File.Partition.SequenceEqual(data.Partition);
    }

    private Task<HashSet<string>> EqualityKeysAsync(DataFile file, IReadOnlyList<NestedField> fields)
    {
        lock (_equalityCache)
        {
            if (!_equalityCache.TryGetValue(file.FilePath, out var task))
                _equalityCache[file.FilePath] = task = LoadEqualityKeysAsync(file, fields);
            return task;
        }
    }

    private async Task<HashSet<string>> LoadEqualityKeysAsync(DataFile file, IReadOnlyList<NestedField> fields)
    {
        var rows = await DataFiles.ReadAsync(_io, file, new Schema(0, fields));
        return rows.Select(r => EqualityKey.Encode(fields, i => r[i])).ToHashSet(StringComparer.Ordinal);
    }

    /// <summary>All positions a v2 position delete file records, grouped by data file.</summary>
    public Task<IReadOnlyDictionary<string, RoaringPositionBitmap>> PositionDeletesAsync(string location, string format = "PARQUET")
    {
        lock (_positionCache)
        {
            if (!_positionCache.TryGetValue(location, out var task))
                _positionCache[location] = task = PositionDeleteFiles.ReadAsync(_io, location, format);
            return task;
        }
    }

    /// <summary>The same for a manifest's delete file, read with its recorded size (ranged Parquet, streamed Avro).</summary>
    public Task<IReadOnlyDictionary<string, RoaringPositionBitmap>> PositionDeletesAsync(DataFile deleteFile)
    {
        lock (_positionCache)
        {
            if (!_positionCache.TryGetValue(deleteFile.FilePath, out var task))
                _positionCache[deleteFile.FilePath] = task = PositionDeleteFiles.ReadAsync(_io, deleteFile);
            return task;
        }
    }

    public async Task<RoaringPositionBitmap> ReadDeletionVectorAsync(DataFile dv, CancellationToken ct = default)
    {
        Task<PuffinFile.Contents> puffin;
        lock (_puffinCache)
        {
            if (!_puffinCache.TryGetValue(dv.FilePath, out puffin!))
                _puffinCache[dv.FilePath] = puffin = LoadPuffinAsync(dv.FilePath, ct);
        }
        var contents = await puffin;
        var blob = contents.Blobs.FirstOrDefault(b => b.Offset == dv.ContentOffset && b.Length == dv.ContentSizeInBytes)
            ?? throw new FormatException($"No blob at offset {dv.ContentOffset} in {dv.FilePath}");
        return DeletionVector.Decode(contents.Payload(blob));
    }

    private async Task<PuffinFile.Contents> LoadPuffinAsync(string location, CancellationToken ct) => PuffinFile.Read(await _io.ReadAllAsync(location, ct));
}
