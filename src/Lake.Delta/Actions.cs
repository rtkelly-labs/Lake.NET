using System.Text.Json.Nodes;

namespace Lake.Delta;

/// <summary>The <c>protocol</c> action.</summary>
public sealed record DeltaProtocol(int MinReaderVersion, int MinWriterVersion, IReadOnlyList<string>? ReaderFeatures, IReadOnlyList<string>? WriterFeatures);

/// <summary>The <c>metaData</c> action. <see cref="Schema"/> is parsed from <see cref="SchemaString"/> on first use.</summary>
public sealed record DeltaMetadata(
    string Id,
    string? Name,
    string? Description,
    string FormatProvider,
    string SchemaString,
    IReadOnlyList<string> PartitionColumns,
    IReadOnlyDictionary<string, string> Configuration,
    long? CreatedTime)
{
    private DeltaStructType? _schema;

    public DeltaStructType Schema => _schema ??= DeltaSchema.Parse(SchemaString);
}

/// <summary>A deletion vector descriptor (PROTOCOL.md "Deletion Vector Descriptor Schema").</summary>
public sealed record DeletionVectorDescriptor(string StorageType, string PathOrInlineDv, int? Offset, int SizeInBytes, long Cardinality)
{
    /// <summary><c>&lt;storageType&gt;&lt;pathOrInlineDv&gt;[@&lt;offset&gt;]</c>, half of a logical file's key.</summary>
    public string UniqueId => Offset is { } o ? $"{StorageType}{PathOrInlineDv}@{o}" : StorageType + PathOrInlineDv;
}

/// <summary>The <c>add</c> action: one logical file (data file plus optional deletion vector) of the snapshot.</summary>
public sealed record DeltaAddFile(
    string Path,
    IReadOnlyDictionary<string, string?> PartitionValues,
    long Size,
    long ModificationTime,
    bool DataChange,
    string? StatsJson,
    IReadOnlyDictionary<string, string?>? Tags,
    DeletionVectorDescriptor? DeletionVector,
    long? BaseRowId,
    long? DefaultRowCommitVersion,
    string? ClusteringProvider)
{
    private DeltaFileStats? _stats;

    /// <summary>The parsed <c>stats</c>, or null when the writer recorded none. Corrupt stats throw <see cref="InvalidDataException"/>.</summary>
    public DeltaFileStats? Stats => StatsJson is null ? null : _stats ??= DeltaFileStats.Parse(StatsJson);
}

/// <summary>The <c>remove</c> action: a tombstone, kept for VACUUM.</summary>
public sealed record DeltaRemoveFile(
    string Path,
    long? DeletionTimestamp,
    bool DataChange,
    bool? ExtendedFileMetadata,
    IReadOnlyDictionary<string, string?>? PartitionValues,
    long? Size,
    DeletionVectorDescriptor? DeletionVector,
    long? BaseRowId,
    long? DefaultRowCommitVersion);

/// <summary>The <c>txn</c> (SetTransaction) action.</summary>
public sealed record DeltaTransaction(string AppId, long Version, long? LastUpdated);

/// <summary>The <c>domainMetadata</c> action.</summary>
public sealed record DeltaDomainMetadata(string Domain, string Configuration, bool Removed);

/// <summary>The <c>commitInfo</c> action: free-form, with the two fields readers use pulled out.</summary>
public sealed record DeltaCommitInfo(JsonObject Raw)
{
    public long? Timestamp => Json.Long(Raw, "timestamp", "commitInfo");
    public long? InCommitTimestamp => Json.Long(Raw, "inCommitTimestamp", "commitInfo");
}

/// <summary>V2 checkpoint actions (never in commits).</summary>
internal sealed record SidecarAction(string Path, long? SizeInBytes);

internal sealed record CheckpointMetadataAction(long Version);

/// <summary>Turns one log JSON object (a commit line, or a checkpoint row rendered as JSON) into an action.</summary>
internal static class ActionParser
{
    /// <summary>
    /// The action in <paramref name="line"/>, or null when it holds none we know (unknown actions are ignored, as the
    /// protocol requires). A line with more than one known action is malformed.
    /// </summary>
    public static object? Parse(JsonObject line, string where)
    {
        object? found = null;
        foreach (var (key, value) in line)
        {
            if (value is null) continue; // checkpoint rows render absent actions as nothing; tolerate explicit nulls too
            object? action = key switch
            {
                "add" => Add(Body(value, key, where), where + " add"),
                "remove" => Remove(Body(value, key, where), where + " remove"),
                "metaData" => Metadata(Body(value, key, where), where + " metaData"),
                "protocol" => Protocol(Body(value, key, where), where + " protocol"),
                "txn" => Txn(Body(value, key, where), where + " txn"),
                "domainMetadata" => Domain(Body(value, key, where), where + " domainMetadata"),
                "commitInfo" => new DeltaCommitInfo(Body(value, key, where)),
                "sidecar" => new SidecarAction(Json.ReqStr(Body(value, key, where), "path", where + " sidecar"), Json.Long(Body(value, key, where), "sizeInBytes", where + " sidecar")),
                "checkpointMetadata" => new CheckpointMetadataAction(Json.ReqLong(Body(value, key, where), "version", where + " checkpointMetadata")),
                _ => null, // cdc and future actions: not part of a snapshot read
            };
            if (action is null) continue;
            if (found is not null) throw new InvalidDataException($"{where}: one line holds more than one action ({found.GetType().Name} and {key})");
            found = action;
        }
        return found;
    }

    private static JsonObject Body(JsonNode value, string key, string where) =>
        value as JsonObject ?? throw new InvalidDataException($"{where}: '{key}' is not an object");

    private static DeltaAddFile Add(JsonObject o, string w)
    {
        var size = Json.ReqLong(o, "size", w);
        if (size < 0) throw new InvalidDataException($"{w}.size is negative");
        return new DeltaAddFile(
            Json.ReqStr(o, "path", w),
            Json.StrMap(o, "partitionValues", w) ?? throw new InvalidDataException($"{w}.partitionValues is required"),
            size,
            Json.Long(o, "modificationTime", w) ?? 0,
            Json.Bool(o, "dataChange", w) ?? true,
            Json.Str(o, "stats", w),
            Json.StrMap(o, "tags", w),
            Dv(Json.Obj(o, "deletionVector", w), w + ".deletionVector"),
            Json.Long(o, "baseRowId", w),
            Json.Long(o, "defaultRowCommitVersion", w),
            Json.Str(o, "clusteringProvider", w));
    }

    private static DeltaRemoveFile Remove(JsonObject o, string w) => new(
        Json.ReqStr(o, "path", w),
        Json.Long(o, "deletionTimestamp", w),
        Json.Bool(o, "dataChange", w) ?? true,
        Json.Bool(o, "extendedFileMetadata", w),
        Json.StrMap(o, "partitionValues", w),
        Json.Long(o, "size", w),
        Dv(Json.Obj(o, "deletionVector", w), w + ".deletionVector"),
        Json.Long(o, "baseRowId", w),
        Json.Long(o, "defaultRowCommitVersion", w));

    private static DeletionVectorDescriptor? Dv(JsonObject? o, string w)
    {
        if (o is null) return null;
        var storage = Json.ReqStr(o, "storageType", w);
        if (storage is not ("u" or "i" or "p")) throw new InvalidDataException($"{w}.storageType '{storage}' is not u, i or p");
        var size = Json.Int(o, "sizeInBytes", w) ?? throw new InvalidDataException($"{w}.sizeInBytes is required");
        var cardinality = Json.ReqLong(o, "cardinality", w);
        var offset = Json.Int(o, "offset", w);
        if (size < 0 || cardinality < 0 || offset < 0) throw new InvalidDataException($"{w} has a negative size, cardinality or offset");
        return new DeletionVectorDescriptor(storage, Json.ReqStr(o, "pathOrInlineDv", w), offset, size, cardinality);
    }

    private static DeltaMetadata Metadata(JsonObject o, string w)
    {
        var format = Json.Obj(o, "format", w);
        var configuration = Json.StrMap(o, "configuration", w) ?? [];
        return new DeltaMetadata(
            Json.ReqStr(o, "id", w),
            Json.Str(o, "name", w),
            Json.Str(o, "description", w),
            format is null ? "parquet" : Json.Str(format, "provider", w + ".format") ?? "parquet",
            Json.ReqStr(o, "schemaString", w),
            Json.StrList(o, "partitionColumns", w) ?? [],
            configuration.Where(kv => kv.Value is not null).ToDictionary(kv => kv.Key, kv => kv.Value!, StringComparer.Ordinal),
            Json.Long(o, "createdTime", w));
    }

    private static DeltaProtocol Protocol(JsonObject o, string w)
    {
        var reader = Json.Int(o, "minReaderVersion", w) ?? throw new InvalidDataException($"{w}.minReaderVersion is required");
        var writer = Json.Int(o, "minWriterVersion", w) ?? throw new InvalidDataException($"{w}.minWriterVersion is required");
        if (reader < 1 || writer < 1) throw new InvalidDataException($"{w}: protocol versions start at 1");
        return new DeltaProtocol(reader, writer, Json.StrList(o, "readerFeatures", w), Json.StrList(o, "writerFeatures", w));
    }

    private static DeltaTransaction Txn(JsonObject o, string w) =>
        new(Json.ReqStr(o, "appId", w), Json.ReqLong(o, "version", w), Json.Long(o, "lastUpdated", w));

    private static DeltaDomainMetadata Domain(JsonObject o, string w) =>
        new(Json.ReqStr(o, "domain", w), Json.Str(o, "configuration", w) ?? "", Json.Bool(o, "removed", w) ?? false);
}
