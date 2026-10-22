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

    public static async Task<IReadOnlyDictionary<string, RoaringPositionBitmap>> ReadAsync(IFileIO io, string location, CancellationToken ct = default)
    {
        var result = new Dictionary<string, RoaringPositionBitmap>(StringComparer.Ordinal);
        foreach (var row in await ParquetFiles.ReadAsync(io, location, Schema, ct))
        {
            var path = (string)row[0]!;
            if (!result.TryGetValue(path, out var bm)) result[path] = bm = new RoaringPositionBitmap();
            bm.Add((long)row[1]!);
        }
        return result;
    }
}

/// <summary>A live delete file in a snapshot, with the data sequence number that decides what it applies to.</summary>
public sealed record DeleteEntry(DataFile File, long DataSequenceNumber, ManifestFile Manifest)
{
    public bool IsDeletionVector => File.FileFormat == "PUFFIN";
}

/// <summary>
/// The delete files of a snapshot, indexed for planning. Applicability rules (spec "Scan Planning"):
/// a position delete or DV applies to a data file when its data sequence number is ≥ the data file's;
/// when a data file has a DV, position delete files for it are ignored (v3: the DV already folds them in).
/// Equality deletes are not supported yet and make planning fail rather than return deleted rows.
/// </summary>
public sealed class DeleteIndex
{
    private readonly Dictionary<string, DeleteEntry> _dvs = new(StringComparer.Ordinal);
    private readonly List<DeleteEntry> _positionFiles = [];
    private readonly Dictionary<string, Task<IReadOnlyDictionary<string, RoaringPositionBitmap>>> _positionCache = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Task<PuffinFile.Contents>> _puffinCache = new(StringComparer.Ordinal);
    private readonly IFileIO _io;

    private DeleteIndex(IFileIO io) => _io = io;

    public IReadOnlyList<DeleteEntry> All => [.. _dvs.Values, .. _positionFiles];
    public bool IsEmpty => _dvs.Count == 0 && _positionFiles.Count == 0;

    public static async Task<DeleteIndex> LoadAsync(Table table, IReadOnlyList<ManifestFile> manifests, CancellationToken ct = default)
    {
        var index = new DeleteIndex(table.IO);
        var m = table.Metadata;
        foreach (var manifest in manifests.Where(mf => mf.Content == ManifestContent.Deletes))
        {
            var contents = ManifestCodec.ReadManifest(await table.IO.ReadAllAsync(manifest.ManifestPath, ct), m.SpecById(manifest.PartitionSpecId), m.CurrentSchema, manifest);
            foreach (var e in contents.Entries.Where(e => e.Status != ManifestEntryStatus.Deleted))
            {
                var entry = new DeleteEntry(e.DataFile, e.SequenceNumber ?? 0, manifest);
                switch (e.DataFile.Content)
                {
                    case FileContent.EqualityDeletes:
                        throw new NotSupportedException("Equality delete files are not supported yet");
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

    public DeleteEntry? DeletionVectorFor(string dataFile) => _dvs.GetValueOrDefault(dataFile);

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
            var byFile = await PositionDeletesAsync(pd.File.FilePath);
            if (!byFile.TryGetValue(dataFile.FilePath, out var bm)) continue;
            (result ??= new RoaringPositionBitmap()).Or(bm);
        }
        return result;
    }

    /// <summary>All positions a v2 position delete file records, grouped by data file.</summary>
    public Task<IReadOnlyDictionary<string, RoaringPositionBitmap>> PositionDeletesAsync(string location)
    {
        lock (_positionCache)
        {
            if (!_positionCache.TryGetValue(location, out var task))
                _positionCache[location] = task = PositionDeleteFiles.ReadAsync(_io, location);
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
