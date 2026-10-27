namespace Iceberg.Net.Data;

// A Parquet file as the data layer sees it, whichever library reads or writes it (ParquetSharp, or Parquet.Net through
// Iceberg.Net.Data.ParquetNet): the schema tree with physical types and annotations, column chunk sizes and statistics,
// and each leaf column of a row group as its levels plus its defined values.

/// <summary>Parquet physical types (names as parquet-cpp's, which messages show).</summary>
internal enum PqPhysical { Boolean, Int32, Int64, Int96, Float, Double, ByteArray, FixedLenByteArray }

internal enum PqRepetition { Required, Optional, Repeated }

internal enum PqTimeUnit { Millis, Micros, Nanos }

internal enum PqLogicalKind { None, String, Enum, Json, Bson, Uuid, Date, Time, Timestamp, Decimal, Int, Interval, List, Map, Float16, Other }

/// <summary>A logical annotation (or the legacy converted type it stands for).</summary>
internal sealed record PqLogical(PqLogicalKind Kind)
{
    public static readonly PqLogical None = new(PqLogicalKind.None);
    public static readonly PqLogical String = new(PqLogicalKind.String);
    public static readonly PqLogical Date = new(PqLogicalKind.Date);
    public static readonly PqLogical Uuid = new(PqLogicalKind.Uuid);
    public static readonly PqLogical List = new(PqLogicalKind.List);
    public static readonly PqLogical Map = new(PqLogicalKind.Map);
    public static readonly PqLogical Interval = new(PqLogicalKind.Interval);

    public int BitWidth { get; init; }
    public bool Signed { get; init; } = true;
    public PqTimeUnit Unit { get; init; }
    public bool UtcAdjusted { get; init; }
    public int Precision { get; init; }
    public int Scale { get; init; }

    public static PqLogical Int(int bits, bool signed) => new(PqLogicalKind.Int) { BitWidth = bits, Signed = signed };
    public static PqLogical Time(bool utc, PqTimeUnit unit) => new(PqLogicalKind.Time) { UtcAdjusted = utc, Unit = unit };
    public static PqLogical Timestamp(bool utc, PqTimeUnit unit) => new(PqLogicalKind.Timestamp) { UtcAdjusted = utc, Unit = unit };
    public static PqLogical Decimal(int precision, int scale) => new(PqLogicalKind.Decimal) { Precision = precision, Scale = scale };

    public override string ToString() => Kind switch
    {
        PqLogicalKind.Int => $"Int({BitWidth}, {(Signed ? "signed" : "unsigned")})",
        PqLogicalKind.Time => $"Time({Unit}, utc={UtcAdjusted})",
        PqLogicalKind.Timestamp => $"Timestamp({Unit}, utc={UtcAdjusted})",
        PqLogicalKind.Decimal => $"Decimal({Precision}, {Scale})",
        _ => Kind.ToString(),
    };
}

/// <summary>A schema node: a group (no <see cref="Physical"/>) or a primitive.</summary>
internal sealed class PqNode
{
    public required string Name { get; init; }
    public required PqRepetition Repetition { get; init; }
    public int FieldId { get; init; } = -1;
    public PqLogical Logical { get; init; } = PqLogical.None;
    public PqPhysical? Physical { get; init; }
    /// <summary>Length of FIXED_LEN_BYTE_ARRAY values; 0 otherwise.</summary>
    public int TypeLength { get; init; }
    public IReadOnlyList<PqNode> Children { get; init; } = [];

    public bool IsGroup => Physical is null;
    public PqPhysical PhysicalType => Physical ?? throw new InvalidOperationException($"{Name} is a group");

    /// <summary>How min/max statistics of this primitive are ordered (parquet-format "Sort Order").</summary>
    public PqSortOrder SortOrder => (PhysicalType, Logical.Kind) switch
    {
        (_, PqLogicalKind.Decimal) => PqSortOrder.Signed,
        (_, PqLogicalKind.Int) => Logical.Signed ? PqSortOrder.Signed : PqSortOrder.Unsigned,
        (_, PqLogicalKind.Interval) => PqSortOrder.Unknown,
        (PqPhysical.Int96, _) => PqSortOrder.Unknown,
        (PqPhysical.ByteArray or PqPhysical.FixedLenByteArray, PqLogicalKind.Float16) => PqSortOrder.Signed,
        (PqPhysical.ByteArray or PqPhysical.FixedLenByteArray, _) => PqSortOrder.Unsigned,
        _ => PqSortOrder.Signed,
    };

    /// <summary>The type as messages show it: <c>Int64</c>, <c>ByteArray (String)</c>.</summary>
    public string Describe() => Logical.Kind == PqLogicalKind.None ? PhysicalType.ToString() : $"{PhysicalType} ({Logical})";

    /// <summary>The primitive leaves below (or at) this node, in file order.</summary>
    public IEnumerable<PqNode> Leaves() => IsGroup ? Children.SelectMany(c => c.Leaves()) : [this];

    public int LeafCount => IsGroup ? Children.Sum(c => c.LeafCount) : 1;
}

internal enum PqSortOrder { Signed, Unsigned, Unknown }

/// <summary>
/// A column chunk's statistics. <see cref="Min"/>/<see cref="Max"/> are physical values (<c>bool</c>, <c>int</c>,
/// <c>long</c>, <c>float</c>, <c>double</c>, <c>byte[]</c>), both null when the chunk has none usable;
/// <see cref="NullCount"/> as written (some writers omit it, which reads as 0).
/// </summary>
internal sealed record PqStatistics(long NullCount, object? Min, object? Max)
{
    public bool HasMinMax => Min is not null && Max is not null;
}

/// <summary>
/// A column chunk: its size, value count and statistics, and where it starts in the file when the backend reads it
/// page by page (null when the library reads a chunk in one request itself, as parquet-cpp does).
/// </summary>
internal sealed record PqColumnChunk(long TotalCompressedSize, long NumValues, PqStatistics? Statistics)
{
    public long? Offset { get; init; }
}

/// <summary>
/// One leaf column of a row group: <see cref="Levels"/> level entries, definition and repetition levels (null when the
/// column has none), and the defined values only, as a typed array: <c>bool[]</c>, <c>int[]</c>, <c>long[]</c>,
/// <c>float[]</c>, <c>double[]</c>, <c>byte[][]</c> (byte arrays), or <c>string[]</c> (strings, when asked for).
/// </summary>
internal sealed record PqLeafData(int Levels, short[]? Defs, short[]? Reps, Array Values);

internal sealed record PqWriteSettings(bool Statistics, bool Dictionary, byte[]? EncryptionKey);

internal interface IPqReader : IDisposable
{
    /// <summary>The root group.</summary>
    PqNode Schema { get; }
    int RowGroups { get; }
    long NumRows { get; }
    long RowGroupRows(int rowGroup);
    PqColumnChunk Chunk(int rowGroup, int leaf);
    /// <summary>A leaf's levels and values; byte arrays as <c>string[]</c> when <paramref name="strings"/>.</summary>
    ValueTask<PqLeafData> ReadLeafAsync(int rowGroup, int leaf, bool strings, CancellationToken ct);
}

internal interface IPqWriter : IDisposable
{
    /// <summary>One row group: every leaf, in file order.</summary>
    ValueTask WriteRowGroupAsync(IReadOnlyList<PqLeafData> leaves, CancellationToken ct);
    /// <summary>Closes the file and returns its bytes.</summary>
    ValueTask<byte[]> FinishAsync(CancellationToken ct);
}

/// <summary>A Parquet library behind the data layer.</summary>
internal interface IParquetBackend
{
    string Name { get; }
    /// <summary>Opens a file on a seekable stream (with the AES footer key of an encrypted file).</summary>
    ValueTask<IPqReader> OpenAsync(Stream stream, byte[]? decryptionKey, CancellationToken ct);
    /// <summary>A writer of files with this schema (a root group).</summary>
    IPqWriter Create(PqNode schema, PqWriteSettings settings);
}
