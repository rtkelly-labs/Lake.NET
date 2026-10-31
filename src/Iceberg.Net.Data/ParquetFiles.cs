using System.Runtime.CompilerServices;
using ParquetSharp;
using Iceberg.Net.Expressions;
using Iceberg.Net.Manifests;
using Iceberg.Net.IO;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>Parquet writer layout: rows per row group (default: one group per file) and whether footer statistics are written.</summary>
public sealed record ParquetWriteOptions
{
    public static ParquetWriteOptions Default { get; } = new();
    /// <summary>Rows per row group; null or ≤ 0 for a single row group.</summary>
    public int? RowGroupRows { get; init; }
    /// <summary>Write column-chunk min/max/null-count statistics (as other writers may not).</summary>
    public bool Statistics { get; init; } = true;
    /// <summary>
    /// Compute the Iceberg column metrics of the returned <see cref="DataFile"/> (value, null and NaN counts, bounds).
    /// A caller that keeps its own statistics (DuckLake) turns this off and saves a pass over every value.
    /// </summary>
    public bool Metrics { get; init; } = true;
    /// <summary>
    /// Parquet annotations by field ID, for columns stored narrower or differently than their Iceberg type (formats
    /// layered on these files, such as DuckLake's unsigned integers). Readers decode them from the file's own
    /// annotations, so no option is needed to read.
    /// </summary>
    public IReadOnlyDictionary<int, ParquetAnnotation>? Annotations { get; init; }
    /// <summary>
    /// Parquet modular encryption: an AES key (16, 24 or 32 bytes) that encrypts every column and the footer
    /// (AES-GCM, encrypted-footer mode). Readers need the same key (<see cref="DataFileReadOptions.DecryptionKey"/>).
    /// </summary>
    public byte[]? EncryptionKey { get; init; }
    /// <summary>Dictionary-encode columns where it pays (Parquet's default); off writes every page plain.</summary>
    public bool Dictionary { get; init; } = true;

    internal static ReaderProperties ReaderProperties(byte[]? key)
    {
        var props = ParquetSharp.ReaderProperties.GetDefaultReaderProperties();
        if (key is null) return props;
        using var builder = new FileDecryptionPropertiesBuilder();
        props.FileDecryptionProperties = builder.FooterKey(key).Build();
        return props;
    }
}

/// <summary>
/// How a column is stored when it is not the Iceberg default for its type. The Iceberg type a column must have is given
/// per member; the value passed to the writer is that type's value and the writer converts it.
/// </summary>
public enum ParquetAnnotation
{
    /// <summary><c>int</c> as INT32 annotated INT(8, signed).</summary>
    Int8,
    /// <summary><c>int</c> as INT32 annotated INT(16, signed).</summary>
    Int16,
    /// <summary><c>int</c> (0–255) as INT32 annotated INT(8, unsigned).</summary>
    UInt8,
    /// <summary><c>int</c> (0–65535) as INT32 annotated INT(16, unsigned).</summary>
    UInt16,
    /// <summary><c>long</c> (0–4294967295) as INT32 annotated INT(32, unsigned).</summary>
    UInt32,
    /// <summary><c>decimal(20, 0)</c> (0–18446744073709551615) as INT64 annotated INT(64, unsigned).</summary>
    UInt64,
    /// <summary><c>timestamp</c>/<c>timestamptz</c> as INT64 milliseconds (sub-millisecond digits are dropped).</summary>
    TimestampMillis,
    /// <summary><c>time</c> annotated as adjusted to UTC.</summary>
    TimeUtc,
    /// <summary><c>fixed[12]</c> annotated INTERVAL: little-endian unsigned months, days and milliseconds.</summary>
    Interval,
}

/// <summary>A written Parquet file: its Iceberg <see cref="DataFile"/> and the layout facts some catalogs record.</summary>
public sealed record ParquetWriteResult(DataFile File, long FooterSize, int RowGroups);

/// <summary>
/// Parquet data files with Iceberg field IDs. Rows are <c>object?[]</c> aligned to the schema's top-level
/// columns, holding physical Iceberg values (int days for date, long micros for time/timestamp, long nanos
/// for timestamp_ns, string, byte[], bool, int, long, float, double).
/// <see cref="IcebergDecimal"/> for decimal and <see cref="Guid"/> for uuid.
/// Files are read and written through ParquetSharp by default, or through Parquet.Net
/// (<c>Iceberg.Net.Data.ParquetNet</c>, or <c>ICEBERG_PARQUET_BACKEND=parquet-net</c>); both produce the same files.
/// </summary>
public static class ParquetFiles
{
    private static IParquetBackend? _backend;
    private static readonly AsyncLocal<IParquetBackend?> Scoped = new();

    /// <summary>
    /// The Parquet library in use: one chosen for the current async flow (<see cref="Use"/>), else the process-wide one:
    /// ParquetSharp unless <c>ICEBERG_PARQUET_BACKEND</c> names <c>parquet-net</c> (which needs the
    /// <c>Iceberg.Net.Data.ParquetNet</c> assembly) or the Parquet.Net backend was installed.
    /// </summary>
    internal static IParquetBackend Backend
    {
        get => Scoped.Value ?? (_backend ??= FromEnvironment());
        set => _backend = value;
    }

    /// <summary>Uses <paramref name="backend"/> in the current async flow until the result is disposed.</summary>
    internal static IDisposable Use(IParquetBackend backend)
    {
        var previous = Scoped.Value;
        Scoped.Value = backend;
        return new Restore(previous);
    }

    private sealed class Restore(IParquetBackend? previous) : IDisposable
    {
        public void Dispose() => Scoped.Value = previous;
    }

    /// <summary>The name of the Parquet library in use (<c>ParquetSharp</c> or <c>Parquet.Net</c>).</summary>
    public static string BackendName => Backend.Name;

    private static IParquetBackend FromEnvironment()
    {
        var name = Environment.GetEnvironmentVariable("ICEBERG_PARQUET_BACKEND");
        if (string.IsNullOrEmpty(name) || name.Equals("parquetsharp", StringComparison.OrdinalIgnoreCase)) return ParquetSharpBackend.Instance;
        if (!name.Equals("parquet-net", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"ICEBERG_PARQUET_BACKEND={name}: expected parquetsharp or parquet-net");
        var type = Type.GetType("Iceberg.Net.Data.ParquetNet.ParquetNetBackend, Iceberg.Net.Data.ParquetNet")
            ?? throw new InvalidOperationException("ICEBERG_PARQUET_BACKEND=parquet-net needs the Iceberg.Net.Data.ParquetNet assembly");
        return (IParquetBackend)Activator.CreateInstance(type, nonPublic: true)!;
    }

    internal static PqNode ToPq(NestedField f, IReadOnlyDictionary<int, ParquetAnnotation>? annotations = null)
    {
        if (!f.Type.IsPrimitive) return NestedColumns.ToPq(f, annotations);
        var rep = f.Required ? PqRepetition.Required : PqRepetition.Optional;
        if (annotations is not null && annotations.TryGetValue(f.Id, out var annotation)) return Annotated(f, rep, annotation);
        PqNode Leaf(PqPhysical physical, PqLogical? logical = null, int length = 0) =>
            new() { Name = f.Name, Repetition = rep, FieldId = f.Id, Physical = physical, Logical = logical ?? PqLogical.None, TypeLength = length };
        return f.Type.TypeId switch
        {
            TypeId.Boolean => Leaf(PqPhysical.Boolean),
            TypeId.Int => Leaf(PqPhysical.Int32),
            TypeId.Long => Leaf(PqPhysical.Int64),
            TypeId.Float => Leaf(PqPhysical.Float),
            TypeId.Double => Leaf(PqPhysical.Double),
            TypeId.Date => Leaf(PqPhysical.Int32, PqLogical.Date),
            TypeId.Time => Leaf(PqPhysical.Int64, PqLogical.Time(false, PqTimeUnit.Micros)),
            TypeId.Timestamp => Leaf(PqPhysical.Int64, PqLogical.Timestamp(false, PqTimeUnit.Micros)),
            TypeId.TimestampTz => Leaf(PqPhysical.Int64, PqLogical.Timestamp(true, PqTimeUnit.Micros)),
            TypeId.TimestampNs => Leaf(PqPhysical.Int64, PqLogical.Timestamp(false, PqTimeUnit.Nanos)),
            TypeId.TimestampTzNs => Leaf(PqPhysical.Int64, PqLogical.Timestamp(true, PqTimeUnit.Nanos)),
            TypeId.String => Leaf(PqPhysical.ByteArray, PqLogical.String),
            TypeId.Binary => Leaf(PqPhysical.ByteArray),
            TypeId.Uuid => Leaf(PqPhysical.FixedLenByteArray, PqLogical.Uuid, 16),
            // Iceberg (Appendix A): decimals with precision ≤ 9 as INT32, ≤ 18 as INT64, larger as the smallest fixed.
            TypeId.Decimal when f.Type is DecimalType { Precision: <= 9 } d => Leaf(PqPhysical.Int32, PqLogical.Decimal(d.Precision, d.Scale)),
            TypeId.Decimal when f.Type is DecimalType { Precision: <= 18 } d => Leaf(PqPhysical.Int64, PqLogical.Decimal(d.Precision, d.Scale)),
            TypeId.Decimal when f.Type is DecimalType d => Leaf(PqPhysical.FixedLenByteArray, PqLogical.Decimal(d.Precision, d.Scale), DecimalBytes(d.Precision)),
            TypeId.Fixed when f.Type is FixedType x => Leaf(PqPhysical.FixedLenByteArray, length: x.Length),
            _ => throw new NotSupportedException($"Column {f.Name}: {f.Type} is not supported by the Parquet writer"),
        };
    }

    private static PqNode Annotated(NestedField f, PqRepetition rep, ParquetAnnotation annotation)
    {
        var (expected, logical, physical, length) = annotation switch
        {
            ParquetAnnotation.Int8 => (f.Type.TypeId == TypeId.Int, PqLogical.Int(8, true), PqPhysical.Int32, 0),
            ParquetAnnotation.Int16 => (f.Type.TypeId == TypeId.Int, PqLogical.Int(16, true), PqPhysical.Int32, 0),
            ParquetAnnotation.UInt8 => (f.Type.TypeId == TypeId.Int, PqLogical.Int(8, false), PqPhysical.Int32, 0),
            ParquetAnnotation.UInt16 => (f.Type.TypeId == TypeId.Int, PqLogical.Int(16, false), PqPhysical.Int32, 0),
            ParquetAnnotation.UInt32 => (f.Type.TypeId == TypeId.Long, PqLogical.Int(32, false), PqPhysical.Int32, 0),
            ParquetAnnotation.UInt64 => (f.Type is DecimalType { Precision: 20, Scale: 0 }, PqLogical.Int(64, false), PqPhysical.Int64, 0),
            ParquetAnnotation.TimestampMillis => (f.Type.TypeId is TypeId.Timestamp or TypeId.TimestampTz,
                PqLogical.Timestamp(f.Type.TypeId == TypeId.TimestampTz, PqTimeUnit.Millis), PqPhysical.Int64, 0),
            ParquetAnnotation.TimeUtc => (f.Type.TypeId == TypeId.Time, PqLogical.Time(true, PqTimeUnit.Micros), PqPhysical.Int64, 0),
            ParquetAnnotation.Interval => (f.Type is FixedType { Length: 12 }, PqLogical.Interval, PqPhysical.FixedLenByteArray, 12),
            _ => throw new ArgumentOutOfRangeException(nameof(annotation), annotation, null),
        };
        if (!expected) throw new ArgumentException($"Column {f.Name}: annotation {annotation} does not apply to {f.Type}");
        return new PqNode { Name = f.Name, Repetition = rep, FieldId = f.Id, Physical = physical, Logical = logical, TypeLength = length };
    }

    /// <summary>A value of an annotated column as its stored physical value (unsigned bits, milliseconds).</summary>
    internal static object Encode(ParquetAnnotation annotation, object v) => annotation switch
    {
        ParquetAnnotation.UInt8 => (int)checked((byte)Convert.ToInt32(v, System.Globalization.CultureInfo.InvariantCulture)),
        ParquetAnnotation.UInt16 => (int)checked((ushort)Convert.ToInt32(v, System.Globalization.CultureInfo.InvariantCulture)),
        ParquetAnnotation.UInt32 => unchecked((int)checked((uint)Convert.ToInt64(v, System.Globalization.CultureInfo.InvariantCulture))),
        ParquetAnnotation.UInt64 => v is IcebergDecimal { Scale: 0 } d ? unchecked((long)checked((ulong)d.Unscaled))
            : throw new ArgumentException($"Expected decimal(20, 0), got {v}"),
        ParquetAnnotation.TimestampMillis => IcebergTime.FloorDiv(Convert.ToInt64(v, System.Globalization.CultureInfo.InvariantCulture), 1000),
        _ => v,
    };

    /// <summary>How a stored value of a column is decoded, from the column's own annotation.</summary>
    internal enum Decoding { None, UInt32, UInt64, Millis }

    internal static Decoding DecodingOf(PqLogical logical) => logical switch
    {
        { Kind: PqLogicalKind.Int, Signed: false, BitWidth: 32 } => Decoding.UInt32,
        { Kind: PqLogicalKind.Int, Signed: false, BitWidth: 64 } => Decoding.UInt64,
        { Kind: PqLogicalKind.Timestamp, Unit: PqTimeUnit.Millis } => Decoding.Millis,
        _ => Decoding.None,
    };

    /// <summary>A raw stored value decoded per <paramref name="decoding"/>: unsigned 32-bit as long, unsigned 64-bit as ulong, millis as micros.</summary>
    internal static object Decode(Decoding decoding, object raw) => (decoding, raw) switch
    {
        (Decoding.UInt32, int i) => (long)unchecked((uint)i),
        (Decoding.UInt64, long l) => unchecked((ulong)l),
        (Decoding.Millis, long l) => checked(l * 1000),
        _ => raw,
    };

    /// <summary>Smallest byte width whose two's-complement range holds every unscaled value of the precision.</summary>
    public static int DecimalBytes(int precision)
    {
        var max = System.Numerics.BigInteger.Pow(10, precision) - 1;
        var bytes = 1;
        while (System.Numerics.BigInteger.Pow(2, 8 * bytes - 1) <= max) bytes++;
        return bytes;
    }

    internal static byte[] FixedDecimal(System.Numerics.BigInteger unscaled, int width)
    {
        var raw = unscaled.ToByteArray(isUnsigned: false, isBigEndian: true);
        if (raw.Length > width) throw new ArgumentException($"Decimal {unscaled} does not fit in {width} bytes");
        var result = new byte[width];
        if (unscaled.Sign < 0) Array.Fill(result, (byte)0xFF); // sign extension
        raw.CopyTo(result, width - raw.Length);
        return result;
    }

    /// <summary>Write rows to a Parquet file at <paramref name="location"/> and return the Iceberg <see cref="DataFile"/> with metrics.</summary>
    public static Task<DataFile> WriteAsync(
        IFileIO io,
        string location,
        Schema schema,
        int specId,
        IReadOnlyList<object?> partition,
        IReadOnlyList<object?[]> rows,
        CancellationToken ct = default) =>
        WriteAsync(io, location, schema, specId, partition, rows, ParquetWriteOptions.Default, ct);

    /// <summary>As <see cref="WriteAsync(IFileIO, string, Schema, int, IReadOnlyList{object?}, IReadOnlyList{object?[]}, CancellationToken)"/>, with row-group layout and statistics options.</summary>
    public static async Task<DataFile> WriteAsync(
        IFileIO io,
        string location,
        Schema schema,
        int specId,
        IReadOnlyList<object?> partition,
        IReadOnlyList<object?[]> rows,
        ParquetWriteOptions options,
        CancellationToken ct = default) =>
        (await WriteFileAsync(io, location, schema, specId, partition, rows, options, ct)).File;

    /// <summary>As <see cref="WriteAsync(IFileIO, string, Schema, int, IReadOnlyList{object?}, IReadOnlyList{object?[]}, ParquetWriteOptions, CancellationToken)"/>, also returning the footer size and row-group count.</summary>
    public static async Task<ParquetWriteResult> WriteFileAsync(
        IFileIO io,
        string location,
        Schema schema,
        int specId,
        IReadOnlyList<object?> partition,
        IReadOnlyList<object?[]> rows,
        ParquetWriteOptions options,
        CancellationToken ct = default)
    {
        var columns = schema.Columns;
        var root = new PqNode { Name = "table", Repetition = PqRepetition.Required, Children = columns.Select(c => ToPq(c, options.Annotations)).ToList() };
        var backend = Backend;
        var groupRows = options.RowGroupRows is > 0 and var g ? g : int.MaxValue;
        var leafIds = new List<int>(); // field ID of each Parquet leaf column, in file order
        byte[] bytes;
        using (var writer = backend.Create(root, new PqWriteSettings(options.Statistics, options.Dictionary, options.EncryptionKey)))
        {
            // One row group per RowGroupRows rows (at least one, so an empty file still has a schema'd group).
            for (var start = 0; start == 0 || start < rows.Count; start += groupRows)
            {
                ct.ThrowIfCancellationRequested();
                var count = Math.Min(groupRows, rows.Count - start);
                var first = start == 0;
                var leaves = new List<PqLeafData>();
                for (var c = 0; c < columns.Count; c++)
                {
                    var values = new object?[count];
                    for (var r = 0; r < count; r++) values[r] = rows[start + r][c];
                    if (columns[c].Type.IsPrimitive)
                    {
                        leaves.Add(FlatLeaf(columns[c], values, options.Annotations));
                        if (first) leafIds.Add(columns[c].Id);
                        continue;
                    }
                    var plan = NestedColumns.Plan(columns[c], leaves.Count, options.Annotations);
                    NestedColumns.Shred(plan, values);
                    foreach (var leaf in plan.Leaves)
                    {
                        leaves.Add(NestedColumns.ToLeafData(leaf));
                        if (first) leafIds.Add(leaf.FieldId);
                    }
                }
                await writer.WriteRowGroupAsync(leaves, ct);
                if (rows.Count == 0) break;
            }
            bytes = await writer.FinishAsync(ct);
        }

        var chunkSizes = new long[leafIds.Count];
        int rowGroups;
        using (var check = await backend.OpenAsync(new MemoryStream(bytes), options.EncryptionKey, ct))
        {
            rowGroups = check.RowGroups;
            for (var rg = 0; rg < rowGroups; rg++)
                for (var i = 0; i < leafIds.Count; i++)
                    chunkSizes[i] += check.Chunk(rg, i).TotalCompressedSize;
        }
        await io.WriteAsync(location, bytes, overwrite: false, ct);

        var metrics = options.Metrics ? Metrics.Compute(schema, rows) : null;
        var file = new DataFile
        {
            FilePath = location,
            FileFormat = "PARQUET",
            SpecId = specId,
            Partition = partition,
            RecordCount = rows.Count,
            FileSizeInBytes = bytes.Length,
            ColumnSizes = leafIds.Select((id, i) => (Id: id, chunkSizes[i])).Where(x => !RowLineage.IsReserved(x.Id)).ToDictionary(x => x.Id, x => x.Item2),
            ValueCounts = metrics?.ValueCounts,
            NullValueCounts = metrics?.NullCounts,
            NanValueCounts = metrics is { NanCounts.Count: > 0 } m ? m.NanCounts : null,
            LowerBounds = metrics?.Lower,
            UpperBounds = metrics?.Upper,
            SplitOffsets = [4],
        };
        // The footer length is the 4-byte little-endian integer before the closing "PAR1" (or "PARE").
        var footer = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        return new ParquetWriteResult(file, footer, rowGroups);
    }

    /// <summary>A top-level primitive column's values as its leaf: definition levels when optional, the defined values typed.</summary>
    private static PqLeafData FlatLeaf(NestedField f, object?[] values, IReadOnlyDictionary<int, ParquetAnnotation>? annotations)
    {
        short[]? defs = null;
        var present = values.Length;
        if (!f.Required)
        {
            defs = new short[values.Length];
            present = 0;
            for (var i = 0; i < values.Length; i++)
                if (values[i] is not null) { defs[i] = 1; present++; }
        }
        else if (values.Any(v => v is null)) throw new ArgumentException($"Required column {f.Name} contains nulls");

        T[] Typed<T>(Func<object, T> convert)
        {
            var result = new T[present];
            var j = 0;
            foreach (var v in values)
                if (v is not null) result[j++] = convert(v);
            return result;
        }
        static T To<T>(object v) => v is T t ? t : (T)Convert.ChangeType(v, typeof(T), System.Globalization.CultureInfo.InvariantCulture);

        Array typed;
        if (annotations is not null && annotations.TryGetValue(f.Id, out var annotation))
        {
            typed = annotation switch
            {
                ParquetAnnotation.UInt64 or ParquetAnnotation.TimestampMillis => Typed(v => (long)Encode(annotation, v)),
                ParquetAnnotation.TimeUtc => Typed(To<long>),
                ParquetAnnotation.Interval => Typed(v => FixedBytes(f, v, 12)),
                _ => Typed(v => (int)Encode(annotation, v)),
            };
            return new PqLeafData(values.Length, defs, null, typed);
        }
        typed = f.Type.TypeId switch
        {
            TypeId.Boolean => Typed(To<bool>),
            TypeId.Int or TypeId.Date => Typed(To<int>),
            TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs => Typed(To<long>),
            TypeId.Float => Typed(To<float>),
            TypeId.Double => Typed(To<double>),
            TypeId.String => Typed(v => (string)v),
            TypeId.Binary => Typed(v => (byte[])v),
            TypeId.Uuid => Typed(v => ((Guid)v).ToByteArray(bigEndian: true)),
            TypeId.Decimal when f.Type is DecimalType d => d.Precision switch
            {
                <= 9 => Typed(v => (int)Unscaled(f, d, v)),
                <= 18 => Typed(v => (long)Unscaled(f, d, v)),
                _ => Typed(v => FixedDecimal(Unscaled(f, d, v), DecimalBytes(d.Precision))),
            },
            TypeId.Fixed when f.Type is FixedType x => Typed(v => FixedBytes(f, v, x.Length)),
            _ => throw new NotSupportedException(f.Type.ToString()),
        };
        return new PqLeafData(values.Length, defs, null, typed);
    }

    private static System.Numerics.BigInteger Unscaled(NestedField f, DecimalType d, object v) => v switch
    {
        IcebergDecimal x when x.Scale == d.Scale => x.Unscaled,
        IcebergDecimal x => throw new ArgumentException($"Column {f.Name}: value scale {x.Scale} != {d.Scale}"),
        decimal m => IcebergDecimal.FromDecimal(m, d.Scale).Unscaled,
        _ => throw new ArgumentException($"Column {f.Name}: expected a decimal, got {v.GetType().Name}"),
    };

    private static byte[] FixedBytes(NestedField f, object v, int length) =>
        v is byte[] b && b.Length == length ? b : throw new ArgumentException($"Column {f.Name}: fixed[{length}] needs {length}-byte values");

    /// <summary>
    /// Read a data file projected onto <paramref name="projection"/>. Columns are matched by Parquet
    /// field_id; files without field IDs (e.g. migrated Hive data) fall back to name matching, which is
    /// what the table's <c>schema.name-mapping.default</c> would formalise.
    /// Columns absent from the file (added by schema evolution) read as their initial-default or null.
    /// </summary>
    public static async Task<List<object?[]>> ReadAsync(IFileIO io, string location, Schema projection, CancellationToken ct = default) =>
        await ReadAsync(io, location, projection, null, ct);

    /// <param name="mapping">Resolves columns of files written without field IDs; defaults to current column names.</param>
    public static async Task<List<object?[]>> ReadAsync(IFileIO io, string location, Schema projection, NameMapping? mapping, CancellationToken ct = default)
    {
        var rows = new List<object?[]>();
        await foreach (var batch in ReadBatchesAsync(io, location, projection, mapping, null, ct)) rows.AddRange(batch.Rows);
        return rows;
    }

    /// <summary>Files up to this size are fetched whole: one request costs about as much as several small ones.</summary>
    internal const long WholeFileBelow = 1 << 20;

    /// <summary>What a Parquet reader reads first to find the footer (parquet-cpp's default footer read size).</summary>
    internal const long FooterPrefetch = 64 * 1024;

    /// <summary>
    /// Stream a data file one row group at a time, decoding only the leaf columns of <paramref name="projection"/>'s
    /// top-level columns. With <see cref="DataFileReadOptions.Filter"/>, row groups whose footer statistics rule the
    /// filter out (inclusive metrics evaluation, by field ID) are skipped; batch positions stay absolute file positions.
    /// </summary>
    public static async IAsyncEnumerable<DataFileBatch> ReadBatchesAsync(IFileIO io, string location, Schema projection, NameMapping? mapping,
        DataFileReadOptions? options, [EnumeratorCancellation] CancellationToken ct = default)
    {
        // With the size known (from the manifest), ranged reads: the footer first, then only the column chunks of the
        // projected columns in row groups that are read. A small file, or one whose every column is read, is fetched
        // in one request. Without it, one whole-object read beats a length lookup plus ranges.
        Stream source;
        RangeReadStream? ranged = null;
        if (options?.FileSize is { } length)
        {
            source = ranged = new RangeReadStream(io, location, length, ct);
            await ranged.PrefetchAsync(length <= WholeFileBelow ? 0 : length - FooterPrefetch, length, ct);
        }
        else source = new MemoryStream(await io.ReadAllAsync(location, ct));
        using var reader = await Backend.OpenAsync(source, options?.DecryptionKey, ct);
        var leafCount = reader.Schema.LeafCount;
        var layout = FileLayout.Of(reader.Schema, projection, mapping, options?.MatchByName ?? false);
        var evaluator = options?.Filter is { } filter && filter is not AlwaysTrue ? new MetricsEvaluator(filter) : null;
        if (ranged is not null && evaluator is null && layout.LeafCount == leafCount) await ranged.PrefetchAsync(0, ranged.Length, ct);
        var metrics = options?.Metrics;
        var groups = reader.RowGroups;
        long position = 0;
        for (var rg = 0; rg < groups; rg++)
        {
            ct.ThrowIfCancellationRequested();
            var n = checked((int)reader.RowGroupRows(rg));
            if (evaluator is not null && !evaluator.MightMatch(n, (id, type) => layout.StatisticsFor(reader, rg, id, type, n)))
            {
                metrics?.RowGroupSkipped(n);
                position += n;
                continue;
            }
            if (ranged is not null) await PrefetchChunksAsync(ranged, reader, rg, layout, ct);
            var rows = new object?[n][];
            for (var r = 0; r < n; r++) rows[r] = new object?[projection.Columns.Count];
            for (var c = 0; c < projection.Columns.Count; c++)
            {
                var f = projection.Columns[c];
                switch (layout.Sources[c])
                {
                    case ColumnSource.Missing m:
                        for (var r = 0; r < n; r++) rows[r][c] = m.Default;
                        break;
                    case ColumnSource.Leaf l:
                    {
                        var data = await reader.ReadLeafAsync(rg, l.Column, f.Type.TypeId switch
                        {
                            TypeId.String => PqValues.Strings,
                            TypeId.Uuid => PqValues.Uuids,
                            _ => PqValues.Physical,
                        }, ct);
                        var values = ReadColumn(data, layout.Leaves[l.Column], f.Type, n);
                        for (var r = 0; r < n; r++) rows[r][c] = values[r];
                        break;
                    }
                    case ColumnSource.Nested nested:
                        await NestedColumns.LoadAsync(reader, rg, nested.Tree, ct);
                        for (var r = 0; r < n; r++) rows[r][c] = NestedColumns.Project(NestedColumns.Assemble(nested.Tree), nested.Tree, f.Type, FromPhysical);
                        break;
                }
            }
            metrics?.RowGroupRead(n);
            yield return new DataFileBatch(position, rows);
            position += n;
        }
    }

    /// <summary>
    /// Fetches the column chunks a row group's read decodes, adjacent ones in one request, for a library that reads a
    /// chunk page by page (Parquet.Net) rather than in one request of its own (parquet-cpp, which reports no offsets).
    /// </summary>
    private static async Task PrefetchChunksAsync(RangeReadStream stream, IPqReader reader, int rowGroup, FileLayout layout, CancellationToken ct)
    {
        var ranges = new List<(long Start, long End)>();
        foreach (var leaf in layout.LeafColumns)
        {
            var chunk = reader.Chunk(rowGroup, leaf);
            if (chunk.Offset is not { } start) return;
            ranges.Add((start, start + chunk.TotalCompressedSize));
        }
        ranges.Sort();
        for (var i = 0; i < ranges.Count;)
        {
            var (start, end) = ranges[i++];
            while (i < ranges.Count && ranges[i].Start <= end + stream.MinimumFetch) end = Math.Max(end, ranges[i++].End);
            await stream.PrefetchAsync(start, Math.Min(end, stream.Length) - start, ct);
        }
    }

    /// <summary>Where each projected column comes from in a file, and which leaf holds each field ID's statistics.</summary>
    private abstract record ColumnSource
    {
        public sealed record Missing(object? Default) : ColumnSource;
        public sealed record Leaf(int Column) : ColumnSource;
        public sealed record Nested(NestedColumns.PNode Tree) : ColumnSource;
    }

    /// <summary>The file's leaves in order, with their dotted paths and levels.</summary>
    internal sealed record LeafInfo(PqNode Node, string Path, int MaxDef, int MaxRep);

    internal static List<LeafInfo> LeavesOf(PqNode root)
    {
        var result = new List<LeafInfo>();
        void Walk(PqNode n, string prefix, int def, int rep)
        {
            var d = def + (n.Repetition == PqRepetition.Required ? 0 : 1);
            var r = rep + (n.Repetition == PqRepetition.Repeated ? 1 : 0);
            var path = prefix.Length == 0 ? n.Name : prefix + "." + n.Name;
            if (!n.IsGroup) result.Add(new LeafInfo(n, path, d, r));
            else foreach (var c in n.Children) Walk(c, path, d, r);
        }
        foreach (var c in root.Children) Walk(c, "", 0, 0);
        return result;
    }

    private sealed class FileLayout
    {
        public required ColumnSource[] Sources { get; init; }
        /// <summary>Field ID → leaf column index, for leaves outside lists and maps (one value per row).</summary>
        public required Dictionary<int, int> StatsLeaves { get; init; }
        public required List<LeafInfo> Leaves { get; init; }
        /// <summary>The distinct leaf columns of the file the projection decodes.</summary>
        public IReadOnlyList<int> LeafColumns => Sources.SelectMany(s => s switch
        {
            ColumnSource.Leaf l => [l.Column],
            ColumnSource.Nested n => n.Tree.Leaves.Select(x => x.Column),
            _ => [],
        }).Distinct().ToList();

        public int LeafCount => LeafColumns.Count;

        public static FileLayout Of(PqNode root, Schema projection, NameMapping? mapping, bool matchByName = false)
        {
            var leaves = LeavesOf(root);
            var byFieldId = new Dictionary<int, int>();
            var byName = new Dictionary<string, int>();
            var statsLeaves = new Dictionary<int, int>();
            for (var i = 0; i < leaves.Count; i++)
            {
                var leaf = leaves[i];
                if (leaf.Node.FieldId >= 0 && !matchByName)
                {
                    byFieldId[leaf.Node.FieldId] = i;
                    if (leaf.MaxRep == 0) statsLeaves[leaf.Node.FieldId] = i;
                }
                byName[leaf.Path] = i;
            }
            var hasIds = byFieldId.Count > 0;
            // Top-level fields (nested columns span several leaves), with the index of their first leaf column.
            var topLevel = new List<(PqNode Node, int FirstColumn)>();
            var column = 0;
            foreach (var child in root.Children)
            {
                topLevel.Add((child, column));
                column += child.LeafCount;
            }

            IReadOnlyList<string> NamesOf(NestedField f) => mapping?.NamesFor(f.Id) is { Count: > 0 } ns ? ns : [f.Name];
            var sources = new ColumnSource[projection.Columns.Count];
            for (var c = 0; c < sources.Length; c++)
            {
                var f = projection.Columns[c];
                if (!f.Type.IsPrimitive)
                {
                    var top = hasIds
                        ? topLevel.FindIndex(t => t.Node.FieldId == f.Id)
                        : topLevel.FindIndex(t => NamesOf(f).Contains(t.Node.Name));
                    if (top < 0)
                    {
                        sources[c] = new ColumnSource.Missing(Defaults.ToValue(f.Type, f.InitialDefault));
                        continue;
                    }
                    var first = topLevel[top].FirstColumn;
                    sources[c] = new ColumnSource.Nested(NestedColumns.FromFile(topLevel[top].Node, ref first, ignoreIds: matchByName));
                    continue;
                }
                var colIndex = -1;
                var found = byFieldId.TryGetValue(f.Id, out colIndex)
                    || (!hasIds && NamesOf(f).Any(n => byName.TryGetValue(n, out colIndex)));
                if (!found)
                {
                    sources[c] = new ColumnSource.Missing(Defaults.ToValue(f.Type, f.InitialDefault));
                    continue;
                }
                sources[c] = new ColumnSource.Leaf(colIndex);
                if (!hasIds && leaves[colIndex].MaxRep == 0) statsLeaves[f.Id] = colIndex;
            }
            return new FileLayout { Sources = sources, StatsLeaves = statsLeaves, Leaves = leaves };
        }

        /// <summary>A row group's statistics for one field, or null when the file has none usable for it.</summary>
        public ColumnStats? StatisticsFor(IPqReader reader, int rowGroup, int fieldId, IcebergType type, long rows) =>
            StatsLeaves.TryGetValue(fieldId, out var leaf) ? RowGroupStatistics.For(reader.Chunk(rowGroup, leaf), Leaves[leaf].Node, fieldId, type, rows) : null;
    }

    /// <summary>A flat leaf's values as Iceberg values of <paramref name="type"/>, one per row (null where undefined).</summary>
    private static object?[] ReadColumn(PqLeafData data, LeafInfo leaf, IcebergType type, int n)
    {
        var node = leaf.Node;
        var decoding = DecodingOf(node.Logical);
        var physical = node.PhysicalType;
        var result = new object?[n];
        var defs = data.Defs;
        var max = leaf.MaxDef;
        void Fill<T>(T[] values, Func<T, object> convert)
        {
            var v = 0;
            for (var r = 0; r < n; r++)
                if (defs is null || defs[r] == max) result[r] = convert(values[v++]);
        }
        var scale = type is DecimalType d ? d.Scale : 0;
        switch (data.Values)
        {
            case bool[] b when type.TypeId == TypeId.Boolean: Fill(b, x => x); break;
            case int[] i when decoding == Decoding.UInt32 && type.TypeId == TypeId.Long: Fill(i, x => (long)unchecked((uint)x)); break;
            case int[] i when type.TypeId is TypeId.Int or TypeId.Date: Fill(i, x => x); break;
            case int[] i when type.TypeId == TypeId.Long: Fill(i, x => (long)x); break; // int -> long promotion: an old file may still store INT32
            case int[] i when type is DecimalType: Fill(i, x => new IcebergDecimal(x, scale)); break;
            case long[] l when decoding == Decoding.UInt64 && type is DecimalType: Fill(l, x => new IcebergDecimal(unchecked((ulong)x), scale)); break;
            case long[] l when decoding == Decoding.Millis && type.TypeId is TypeId.Timestamp or TypeId.TimestampTz: Fill(l, x => checked(x * 1000)); break;
            case long[] l when type.TypeId is TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs: Fill(l, x => x); break;
            case long[] l when type is DecimalType: Fill(l, x => new IcebergDecimal(x, scale)); break;
            case float[] fl when type.TypeId == TypeId.Float: Fill(fl, x => x); break;
            case float[] fl when type.TypeId == TypeId.Double: Fill(fl, x => (double)x); break;
            case double[] db when type.TypeId == TypeId.Double: Fill(db, x => x); break;
            case string[] s when type.TypeId == TypeId.String: Fill(s, x => x); break;
            case byte[][] s when type.TypeId == TypeId.String: Fill(s, x => System.Text.Encoding.UTF8.GetString(x)); break;
            case byte[][] bytes when type.TypeId is TypeId.Binary or TypeId.Fixed: Fill(bytes, x => x); break;
            case Guid[] uuids when type.TypeId == TypeId.Uuid: Fill(uuids, x => x); break;
            case byte[][] bytes when type.TypeId == TypeId.Uuid: Fill(bytes, x => new Guid(x, bigEndian: true)); break;
            // FIXED (or BINARY) decimals, any precision: big-endian two's-complement unscaled value.
            case byte[][] bytes when type is DecimalType:
                Fill(bytes, x => new IcebergDecimal(new System.Numerics.BigInteger(x, isUnsigned: false, isBigEndian: true), scale));
                break;
            default: throw new NotSupportedException($"Reading {type} from a {physical} column is not supported");
        }
        return result;
    }

    /// <summary>A raw physical value of a nested leaf as an Iceberg value of <paramref name="type"/>.</summary>
    private static object FromPhysical(IcebergType type, PqPhysical physical, int typeLength, object raw) => type switch
    {
        PrimitiveType { TypeId: TypeId.String } => raw as string ?? System.Text.Encoding.UTF8.GetString((byte[])raw),
        PrimitiveType { TypeId: TypeId.Uuid } => raw as Guid? ?? new Guid((byte[])raw, bigEndian: true),
        DecimalType d => new IcebergDecimal(raw switch
        {
            int i => i,
            long l => l,
            ulong u => u,
            byte[] b => new System.Numerics.BigInteger(b, isUnsigned: false, isBigEndian: true),
            _ => throw new InvalidDataException($"Decimal stored as {physical}"),
        }, d.Scale),
        PrimitiveType { TypeId: TypeId.Long } when raw is int i => (long)i,
        PrimitiveType { TypeId: TypeId.Double } when raw is float x => (double)x,
        _ => raw,
    };
}

/// <summary>
/// Parquet row-group footer statistics as typed Iceberg <see cref="ColumnStats"/>, converted exactly as the reader converts
/// values (promoted int → long and float → double, unscaled decimals), so pruning agrees with reading. Statistics are
/// optional and partly untrustworthy across writers, so anything doubtful becomes "unknown" (never prunes):
/// <list type="bullet">
/// <item>no statistics, or no min/max: no bounds;</item>
/// <item>a sort order other than the one Iceberg compares with (signed numerics, unsigned bytes): no bounds;</item>
/// <item>a null count of 0 is trusted only alongside min/max (some writers omit null_count, which reads as 0);</item>
/// <item>string bounds that are not valid UTF-8 (a truncated max can split a character): that bound dropped. A
/// truncated min is a prefix and a truncated max is rounded up by the writer, so both stay valid bounds;</item>
/// <item>uuid, fixed-width/binary decimals and INT96: no bounds (orderings writers disagree on).</item>
/// </list>
/// NaN counts are not in Parquet statistics, so they are unknown; NaN bounds are ignored by the evaluator.
/// </summary>
internal static class RowGroupStatistics
{
    private static readonly System.Text.UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static ColumnStats? For(PqColumnChunk chunk, PqNode leaf, int fieldId, IcebergType type, long rows)
    {
        if (chunk.Statistics is not { } stats) return null;
        var hasMinMax = stats.HasMinMax;
        long? nulls = hasMinMax || stats.NullCount > 0 ? stats.NullCount : null;
        object? lower = null, upper = null;
        if (hasMinMax && Comparable(leaf, type))
        {
            try
            {
                lower = Convert(stats.Min!, leaf, type);
                upper = Convert(stats.Max!, leaf, type);
            }
            catch (Exception e) when (e is ArgumentException or InvalidCastException or OverflowException or NotSupportedException)
            {
                lower = upper = null;
            }
        }
        // Footer min/max may be truncated or loose (writers differ), so tightness is unknown.
        return new ColumnStats { FieldId = fieldId, Type = type, ValueCount = rows, NullValueCount = nulls, LowerBound = lower, UpperBound = upper };
    }

    /// <summary>Whether the file's ordering for this column is the one Iceberg compares values with.</summary>
    private static bool Comparable(PqNode leaf, IcebergType type) => (leaf.PhysicalType, type.TypeId) switch
    {
        (PqPhysical.Boolean, TypeId.Boolean) => true,
        (PqPhysical.Int32, TypeId.Int or TypeId.Date or TypeId.Long or TypeId.Decimal) => leaf.SortOrder == PqSortOrder.Signed,
        (PqPhysical.Int64, TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs or TypeId.Decimal) =>
            leaf.SortOrder == PqSortOrder.Signed,
        (PqPhysical.Float, TypeId.Float or TypeId.Double) or (PqPhysical.Double, TypeId.Double) => leaf.SortOrder == PqSortOrder.Signed,
        (PqPhysical.ByteArray, TypeId.String or TypeId.Binary) => leaf.SortOrder == PqSortOrder.Unsigned,
        (PqPhysical.FixedLenByteArray, TypeId.Fixed) => leaf.SortOrder == PqSortOrder.Unsigned,
        _ => false,
    };

    private static object? Convert(object value, PqNode leaf, IcebergType type)
    {
        switch (value)
        {
            case bool b: return b;
            case int i:
                return type switch
                {
                    DecimalType d => new IcebergDecimal(i, d.Scale),
                    PrimitiveType { TypeId: TypeId.Long } => (long)i,
                    _ => i,
                };
            case long l:
                if (ParquetFiles.DecodingOf(leaf.Logical) == ParquetFiles.Decoding.Millis) return checked(l * 1000);
                return type is DecimalType dl ? new IcebergDecimal(l, dl.Scale) : l;
            case float f: return type.TypeId == TypeId.Double ? (double)f : f;
            case double d: return d;
            case byte[] bytes when leaf.PhysicalType == PqPhysical.ByteArray:
                if (type.TypeId != TypeId.String) return bytes;
                try
                {
                    return StrictUtf8.GetString(bytes);
                }
                catch (System.Text.DecoderFallbackException)
                {
                    return null; // e.g. a max truncated inside a multi-byte character: no bound
                }
            case byte[] bytes: return bytes.Length == leaf.TypeLength ? bytes : null;
            default: return null;
        }
    }
}

/// <summary>Column metrics computed from the written values (independent of Parquet statistics).</summary>
internal sealed record Metrics(
    Dictionary<int, long> ValueCounts,
    Dictionary<int, long> NullCounts,
    Dictionary<int, long> NanCounts,
    Dictionary<int, byte[]> Lower,
    Dictionary<int, byte[]> Upper)
{
    /// <summary>Default metrics mode is truncate(16); rather than computing truncated upper bounds we omit bounds for longer strings.</summary>
    private const int StringBoundLimit = 16;

    public static Metrics Compute(Schema schema, IReadOnlyList<object?[]> rows)
    {
        var m = new Metrics([], [], [], [], []);
        // Every primitive reachable through structs (a top-level column or a field inside struct columns); leaves
        // inside lists and maps get none, as in Java (one row holds many of their values).
        foreach (var (f, path) in Leaves(schema.Columns, []))
        {
            if (RowLineage.IsReserved(f.Id)) continue; // metadata columns carry no column metrics
            var values = rows.Select(r => Schema.Navigate(r, path)).ToList();
            m.ValueCounts[f.Id] = values.Count;
            m.NullCounts[f.Id] = values.Count(v => v is null);
            var present = values.Where(v => v is not null).Select(v => v!).ToList();
            if (f.Type.TypeId is TypeId.Float or TypeId.Double)
            {
                m.NanCounts[f.Id] = present.Count(v => v is double d ? double.IsNaN(d) : float.IsNaN((float)v));
                present = present.Where(v => v is double d ? !double.IsNaN(d) : !float.IsNaN((float)v)).ToList();
            }
            // Bounds are optional. Omit them where CLR ordering is not Iceberg's (uuid is compared as unsigned
            // big-endian bytes; Guid.CompareTo is not) and for binary, rather than risk wrong pruning.
            if (present.Count == 0 || f.Type.TypeId is TypeId.Binary or TypeId.Uuid or TypeId.Fixed) continue;
            if (f.Type.TypeId == TypeId.String && present.Any(v => ((string)v).EnumerateRunes().Count() > StringBoundLimit)) continue;
            // Strings compare by code point (UTF-16 order differs for supplementary characters).
            var comparer = Comparer<object>.Create((a, b) => a is string sa ? Expressions.ValueComparer.CompareCodePoints(sa, (string)b) : ((IComparable)a).CompareTo(b));
            m.Lower[f.Id] = Conversions.ToBytes(f.Type, present.Min(comparer)!);
            m.Upper[f.Id] = Conversions.ToBytes(f.Type, present.Max(comparer)!);
        }
        return m;
    }

    private static IEnumerable<(NestedField Field, int[] Path)> Leaves(IReadOnlyList<NestedField> fields, int[] prefix)
    {
        for (var i = 0; i < fields.Count; i++)
        {
            int[] path = [.. prefix, i];
            if (fields[i].Type.IsPrimitive) yield return (fields[i], path);
            else if (fields[i].Type is StructType s) foreach (var leaf in Leaves(s.Fields, path)) yield return leaf;
        }
    }
}

/// <summary>Converts JSON single-value defaults (spec Appendix D, JSON form) to physical values.</summary>
internal static class Defaults
{
    public static object? ToValue(IcebergType type, object? jsonDefault) =>
        jsonDefault is System.Text.Json.Nodes.JsonNode node ? Json.SingleValueJson.FromJson(type, node) : null;
}
