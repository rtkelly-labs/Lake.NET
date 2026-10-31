using Iceberg.Net.IO;

namespace Lake.Delta;

/// <summary>
/// The reconciled state of a Delta table at one version: one protocol, one metadata, live files, tombstones, the latest
/// <c>txn</c> per application, live domain metadata, and the version's own <c>commitInfo</c> (when its JSON still exists).
/// A snapshot is only built for a protocol <see cref="DeltaFeatures.EnsureReadable"/> accepts.
/// </summary>
public sealed class DeltaSnapshot
{
    private readonly IFileIO _io;

    internal DeltaSnapshot(IFileIO io, string root, long version, ReplayState state, DeltaCommitInfo? commitInfo)
    {
        _io = io;
        TableRoot = root;
        Version = version;
        Protocol = state.Protocol ?? throw new InvalidDataException($"Version {version} of {root} has no protocol action");
        Metadata = state.Metadata ?? throw new InvalidDataException($"Version {version} of {root} has no metaData action");
        DeltaFeatures.EnsureReadable(Protocol);
        if (!string.Equals(Metadata.FormatProvider, "parquet", StringComparison.OrdinalIgnoreCase))
            throw new DeltaUnsupportedFeatureException($"Data files in format '{Metadata.FormatProvider}' are not supported (only parquet)", ["format=" + Metadata.FormatProvider]);
        ColumnMapping = DeltaFeatures.ColumnMapping(Protocol, Metadata);
        Files = state.Adds.Values.ToList();
        Tombstones = state.Removes.Values.ToList();
        Transactions = new Dictionary<string, DeltaTransaction>(state.Transactions, StringComparer.Ordinal);
        DomainMetadata = new Dictionary<string, DeltaDomainMetadata>(state.Domains, StringComparer.Ordinal);
        CommitInfo = commitInfo;
    }

    public string TableRoot { get; }
    public long Version { get; }
    public DeltaProtocol Protocol { get; }
    public DeltaMetadata Metadata { get; }
    public DeltaStructType Schema => Metadata.Schema;
    public ColumnMappingMode ColumnMapping { get; }

    /// <summary>Live logical files (<c>add</c> actions).</summary>
    public IReadOnlyList<DeltaAddFile> Files { get; }

    /// <summary>Tombstones (<c>remove</c> actions), all kept: their expiry is VACUUM's business, not a reader's.</summary>
    public IReadOnlyList<DeltaRemoveFile> Tombstones { get; }

    public IReadOnlyDictionary<string, DeltaTransaction> Transactions { get; }
    public IReadOnlyDictionary<string, DeltaDomainMetadata> DomainMetadata { get; }
    public DeltaCommitInfo? CommitInfo { get; }

    /// <summary>The table's rows: one <c>object?[]</c> per row, aligned with <see cref="Schema"/>'s top-level fields.</summary>
    public async IAsyncEnumerable<object?[]> ReadAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var schema = Schema;
        if (DeltaFeatures.IsSupportedBy(Protocol, "typeWidening")) DeltaFeatures.EnsureTypeChangesSupported(schema);
        var isPartition = new bool[schema.Fields.Count];
        foreach (var column in Metadata.PartitionColumns)
        {
            var index = schema.IndexOf(column);
            if (index < 0) throw new InvalidDataException($"Partition column '{column}' is not in the schema");
            isPartition[index] = true;
        }
        foreach (var file in Files)
        {
            var partitionValues = new object?[schema.Fields.Count];
            for (var f = 0; f < schema.Fields.Count; f++)
            {
                if (!isPartition[f]) continue;
                var field = schema.Fields[f];
                // Keyed by physical name under column mapping; a missing key reads as null.
                file.PartitionValues.TryGetValue(field.PhysicalName(ColumnMapping), out var raw);
                partitionValues[f] = PartitionValues.Parse(raw, field.Type, field.Name);
            }
            var deleted = file.DeletionVector is null ? null : await DeletionVectors.LoadAsync(_io, TableRoot, file.DeletionVector, ct).ConfigureAwait(false);
            var location = DeltaPaths.Absolute(TableRoot, file.Path);
            await foreach (var row in DataFileReader.ReadAsync(_io, location, schema, ColumnMapping, partitionValues, isPartition, deleted, ct).ConfigureAwait(false))
                yield return row;
        }
    }
}
