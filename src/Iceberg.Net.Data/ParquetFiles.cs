using System.Runtime.CompilerServices;
using ParquetSharp;
using ParquetSharp.IO;
using ParquetSharp.Schema;
using Iceberg.Net.Expressions;
using Iceberg.Net.Manifests;
using Iceberg.Net.IO;
using Iceberg.Net.Types;
using TimeUnit = ParquetSharp.TimeUnit;

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
/// Scope: flat schemas; nested and fixed columns are rejected, decimals are written with precision ≤ 18.
/// </summary>
public static class ParquetFiles
{
    internal static Node ToNode(NestedField f, IReadOnlyDictionary<int, ParquetAnnotation>? annotations = null)
    {
        if (!f.Type.IsPrimitive) return NestedColumns.ToNode(f, annotations);
        var rep = f.Required ? Repetition.Required : Repetition.Optional;
        if (annotations is not null && annotations.TryGetValue(f.Id, out var annotation)) return Annotated(f, rep, annotation);
        return f.Type.TypeId switch
        {
            TypeId.Boolean => new PrimitiveNode(f.Name, rep, LogicalType.None(), PhysicalType.Boolean, fieldId: f.Id),
            TypeId.Int => new PrimitiveNode(f.Name, rep, LogicalType.None(), PhysicalType.Int32, fieldId: f.Id),
            TypeId.Long => new PrimitiveNode(f.Name, rep, LogicalType.None(), PhysicalType.Int64, fieldId: f.Id),
            TypeId.Float => new PrimitiveNode(f.Name, rep, LogicalType.None(), PhysicalType.Float, fieldId: f.Id),
            TypeId.Double => new PrimitiveNode(f.Name, rep, LogicalType.None(), PhysicalType.Double, fieldId: f.Id),
            TypeId.Date => new PrimitiveNode(f.Name, rep, LogicalType.Date(), PhysicalType.Int32, fieldId: f.Id),
            TypeId.Time => new PrimitiveNode(f.Name, rep, LogicalType.Time(false, TimeUnit.Micros), PhysicalType.Int64, fieldId: f.Id),
            TypeId.Timestamp => new PrimitiveNode(f.Name, rep, LogicalType.Timestamp(false, TimeUnit.Micros), PhysicalType.Int64, fieldId: f.Id),
            TypeId.TimestampTz => new PrimitiveNode(f.Name, rep, LogicalType.Timestamp(true, TimeUnit.Micros), PhysicalType.Int64, fieldId: f.Id),
            TypeId.TimestampNs => new PrimitiveNode(f.Name, rep, LogicalType.Timestamp(false, TimeUnit.Nanos), PhysicalType.Int64, fieldId: f.Id),
            TypeId.TimestampTzNs => new PrimitiveNode(f.Name, rep, LogicalType.Timestamp(true, TimeUnit.Nanos), PhysicalType.Int64, fieldId: f.Id),
            TypeId.String => new PrimitiveNode(f.Name, rep, LogicalType.String(), PhysicalType.ByteArray, fieldId: f.Id),
            TypeId.Binary => new PrimitiveNode(f.Name, rep, LogicalType.None(), PhysicalType.ByteArray, fieldId: f.Id),
            TypeId.Uuid => new PrimitiveNode(f.Name, rep, LogicalType.Uuid(), PhysicalType.FixedLenByteArray, 16, f.Id),
            // Iceberg (Appendix A): decimals with precision ≤ 9 as INT32, ≤ 18 as INT64, larger as the smallest fixed.
            TypeId.Decimal when f.Type is DecimalType { Precision: <= 9 } d => new PrimitiveNode(f.Name, rep, LogicalType.Decimal(d.Precision, d.Scale), PhysicalType.Int32, fieldId: f.Id),
            TypeId.Decimal when f.Type is DecimalType { Precision: <= 18 } d => new PrimitiveNode(f.Name, rep, LogicalType.Decimal(d.Precision, d.Scale), PhysicalType.Int64, fieldId: f.Id),
            TypeId.Decimal when f.Type is DecimalType d => new PrimitiveNode(f.Name, rep, LogicalType.Decimal(d.Precision, d.Scale), PhysicalType.FixedLenByteArray, DecimalBytes(d.Precision), f.Id),
            TypeId.Fixed when f.Type is FixedType x => new PrimitiveNode(f.Name, rep, LogicalType.None(), PhysicalType.FixedLenByteArray, x.Length, f.Id),
            _ => throw new NotSupportedException($"Column {f.Name}: {f.Type} is not supported by the phase-0 Parquet writer"),
        };
    }

    private static PrimitiveNode Annotated(NestedField f, Repetition rep, ParquetAnnotation annotation)
    {
        var (expected, logical, physical, length) = annotation switch
        {
            ParquetAnnotation.Int8 => (f.Type.TypeId == TypeId.Int, LogicalType.Int(8, true), PhysicalType.Int32, -1),
            ParquetAnnotation.Int16 => (f.Type.TypeId == TypeId.Int, LogicalType.Int(16, true), PhysicalType.Int32, -1),
            ParquetAnnotation.UInt8 => (f.Type.TypeId == TypeId.Int, LogicalType.Int(8, false), PhysicalType.Int32, -1),
            ParquetAnnotation.UInt16 => (f.Type.TypeId == TypeId.Int, LogicalType.Int(16, false), PhysicalType.Int32, -1),
            ParquetAnnotation.UInt32 => (f.Type.TypeId == TypeId.Long, LogicalType.Int(32, false), PhysicalType.Int32, -1),
            ParquetAnnotation.UInt64 => (f.Type is DecimalType { Precision: 20, Scale: 0 }, LogicalType.Int(64, false), PhysicalType.Int64, -1),
            ParquetAnnotation.TimestampMillis => (f.Type.TypeId is TypeId.Timestamp or TypeId.TimestampTz,
                LogicalType.Timestamp(f.Type.TypeId == TypeId.TimestampTz, TimeUnit.Millis), PhysicalType.Int64, -1),
            ParquetAnnotation.TimeUtc => (f.Type.TypeId == TypeId.Time, LogicalType.Time(true, TimeUnit.Micros), PhysicalType.Int64, -1),
            ParquetAnnotation.Interval => (f.Type is FixedType { Length: 12 }, LogicalType.Interval(), PhysicalType.FixedLenByteArray, 12),
            _ => throw new ArgumentOutOfRangeException(nameof(annotation), annotation, null),
        };
        using (logical)
        {
            if (!expected) throw new ArgumentException($"Column {f.Name}: annotation {annotation} does not apply to {f.Type}");
            return new PrimitiveNode(f.Name, rep, logical, physical, length, f.Id);
        }
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

    internal static Decoding DecodingOf(LogicalType logical) => logical switch
    {
        IntLogicalType { IsSigned: false, BitWidth: 32 } => Decoding.UInt32,
        IntLogicalType { IsSigned: false, BitWidth: 64 } => Decoding.UInt64,
        TimestampLogicalType { TimeUnit: TimeUnit.Millis } => Decoding.Millis,
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
        using var root = new GroupNode("table", Repetition.Required, columns.Select(c => ToNode(c, options.Annotations)).ToArray());
        using var builder = new WriterPropertiesBuilder().Compression(Compression.Zstd);
        if (!options.Statistics) builder.DisableStatistics();
        if (!options.Dictionary) builder.DisableDictionary();
        FileEncryptionProperties? encryption = null;
        if (options.EncryptionKey is { } key)
        {
            using var encryptionBuilder = new FileEncryptionPropertiesBuilder(key);
            encryption = encryptionBuilder.Build();
            builder.Encryption(encryption);
        }
        using var encryptionScope = encryption;
        using var props = builder.Build();
        var groupRows = options.RowGroupRows is > 0 and var g ? g : int.MaxValue;
        var leafIds = new List<int>(); // field ID of each Parquet leaf column, in file order
        byte[] bytes;
        using (var ms = new MemoryStream())
        {
            using (var output = new ManagedOutputStream(ms, leaveOpen: true))
            using (var writer = new ParquetFileWriter(output, root, props))
            {
                // One row group per RowGroupRows rows (at least one, so an empty file still has a schema'd group).
                for (var start = 0; start == 0 || start < rows.Count; start += groupRows)
                {
                    var slice = rows.Skip(start).Take(groupRows).ToList();
                    var first = start == 0;
                    using var rg = writer.AppendRowGroup();
                    var leafCount = 0;
                    for (var c = 0; c < columns.Count; c++)
                    {
                        if (columns[c].Type.IsPrimitive)
                        {
                            using var col = rg.NextColumn();
                            WriteColumn(col, columns[c], slice.Select(r => r[c]).ToArray(), options.Annotations);
                            if (first) leafIds.Add(columns[c].Id);
                            leafCount++;
                            continue;
                        }
                        var plan = NestedColumns.Plan(columns[c], leafCount, options.Annotations);
                        NestedColumns.Shred(plan, slice.Select(r => r[c]));
                        foreach (var leaf in plan.Leaves)
                        {
                            using var col = rg.NextColumn();
                            NestedColumns.WriteLeaf(col, leaf);
                            if (first) leafIds.Add(leaf.FieldId);
                            leafCount++;
                        }
                    }
                    if (rows.Count == 0) break;
                }
                writer.Close();
            }
            bytes = ms.ToArray();
        }

        long[] chunkSizes;
        int rowGroups;
        using (var input = new ManagedRandomAccessFile(new MemoryStream(bytes)))
        using (var readerProps = ParquetWriteOptions.ReaderProperties(options.EncryptionKey))
        using (var check = new ParquetFileReader(input, readerProps))
        {
            chunkSizes = new long[leafIds.Count];
            using var meta = check.FileMetaData;
            rowGroups = meta.NumRowGroups;
            for (var rg = 0; rg < meta.NumRowGroups; rg++)
            {
                using var group = check.RowGroup(rg);
                for (var i = 0; i < leafIds.Count; i++)
                {
                    using var chunk = group.MetaData.GetColumnChunkMetaData(i);
                    chunkSizes[i] += chunk.TotalCompressedSize;
                }
            }
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
        // The footer length is the 4-byte little-endian integer before the closing "PAR1".
        var footer = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
        return new ParquetWriteResult(file, footer, rowGroups);
    }

    private static void WriteColumn(ColumnWriter col, NestedField f, object?[] values, IReadOnlyDictionary<int, ParquetAnnotation>? annotations)
    {
        if (annotations is not null && annotations.TryGetValue(f.Id, out var annotation) && annotation is not (ParquetAnnotation.TimeUtc or ParquetAnnotation.Interval))
        {
            var stored = values.Select(v => v is null ? null : Encode(annotation, v)).ToArray();
            if (annotation is ParquetAnnotation.UInt64 or ParquetAnnotation.TimestampMillis) Write<long>(col, f, stored);
            else Write<int>(col, f, stored);
            return;
        }
        switch (f.Type.TypeId)
        {
            case TypeId.Boolean: Write<bool>(col, f, values); break;
            case TypeId.Int or TypeId.Date: Write<int>(col, f, values); break;
            case TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs: Write<long>(col, f, values); break;
            case TypeId.Float: Write<float>(col, f, values); break;
            case TypeId.Double: Write<double>(col, f, values); break;
            case TypeId.String:
                using (var w = col.LogicalWriter<string?>()) w.WriteBatch(values.Select(v => (string?)v).ToArray());
                break;
            case TypeId.Binary:
                using (var w = col.LogicalWriter<byte[]?>()) w.WriteBatch(values.Select(v => (byte[]?)v).ToArray());
                break;
            case TypeId.Uuid when f.Required:
                using (var w = col.LogicalWriter<Guid>()) w.WriteBatch(values.Select(v => v is Guid g ? g : throw new ArgumentException($"Required column {f.Name} contains nulls")).ToArray());
                break;
            case TypeId.Uuid:
                using (var w = col.LogicalWriter<Guid?>()) w.WriteBatch(values.Select(v => (Guid?)v).ToArray());
                break;
            case TypeId.Decimal:
                var scale = ((DecimalType)f.Type).Scale;
                var unscaled = values.Select(v => v is IcebergDecimal d
                    ? d.Scale == scale ? (object)d.Unscaled : throw new ArgumentException($"Column {f.Name}: value scale {d.Scale} != {scale}")
                    : null).ToArray();
                var precision = ((DecimalType)f.Type).Precision;
                if (precision <= 9) Write<int>(col, f, unscaled.Select(u => u is null ? null : (object)(int)(System.Numerics.BigInteger)u).ToArray());
                else if (precision <= 18) Write<long>(col, f, unscaled.Select(u => u is null ? null : (object)(long)(System.Numerics.BigInteger)u).ToArray());
                else WriteBytes(col, f, unscaled.Select(u => u is null ? null : FixedDecimal((System.Numerics.BigInteger)u, DecimalBytes(precision))).ToArray());
                break;
            case TypeId.Fixed:
                var length = ((FixedType)f.Type).Length;
                WriteBytes(col, f, values.Select(v => v is null ? null : v is byte[] b && b.Length == length ? b : throw new ArgumentException($"Column {f.Name}: fixed[{length}] needs {length}-byte values")).ToArray());
                break;
            default: throw new NotSupportedException(f.Type.ToString());
        }
    }

    /// <summary>
    /// FIXED_LEN_BYTE_ARRAY through the physical writer (ParquetSharp's logical writers do not take raw bytes for it):
    /// values are packed into one pinned buffer and passed as pointers.
    /// </summary>
    private static void WriteBytes(ColumnWriter col, NestedField f, byte[]?[] values)
    {
        if (f.Required && values.Any(v => v is null)) throw new ArgumentException($"Required column {f.Name} contains nulls");
        var writer = (ColumnWriter<FixedLenByteArray>)col;
        var present = values.Where(v => v is not null).ToList();
        var width = present.Count == 0 ? 0 : present[0]!.Length;
        var buffer = new byte[Math.Max(1, present.Count * width)];
        for (var i = 0; i < present.Count; i++) present[i]!.CopyTo(buffer, i * width);
        var handle = System.Runtime.InteropServices.GCHandle.Alloc(buffer, System.Runtime.InteropServices.GCHandleType.Pinned);
        try
        {
            var start = handle.AddrOfPinnedObject();
            var pointers = Enumerable.Range(0, present.Count).Select(i => new FixedLenByteArray(start + i * width)).ToArray();
            if (f.Required) writer.WriteBatch(pointers);
            else writer.WriteBatch(values.Length, values.Select(v => (short)(v is null ? 0 : 1)).ToArray(), ReadOnlySpan<short>.Empty, pointers);
        }
        finally
        {
            handle.Free();
        }
    }

    /// <summary>Raw FIXED_LEN_BYTE_ARRAY values (null where undefined), copied out of the reader's buffers.</summary>
    private static byte[]?[] ReadFixed(ColumnReader col, int n)
    {
        var reader = (ColumnReader<FixedLenByteArray>)col;
        var width = col.ColumnDescriptor.TypeLength;
        var optional = col.ColumnDescriptor.MaxDefinitionLevel > 0;
        var result = new byte[]?[n];
        var defs = new short[Math.Max(1, n)];
        var values = new FixedLenByteArray[Math.Max(1, n)];
        var row = 0;
        while (row < n)
        {
            var levels = reader.ReadBatch(n - row, defs, Span<short>.Empty, values, out var valuesRead);
            if (levels == 0) break;
            var v = 0;
            for (var i = 0; i < levels; i++, row++)
            {
                if (optional && defs[i] == 0) continue;
                var bytes = new byte[width];
                System.Runtime.InteropServices.Marshal.Copy(values[v++].Pointer, bytes, 0, width);
                result[row] = bytes;
            }
        }
        return result;
    }

    private static void Write<T>(ColumnWriter col, NestedField f, object?[] values) where T : struct
    {
        if (f.Required)
        {
            if (values.Any(v => v is null)) throw new ArgumentException($"Required column {f.Name} contains nulls");
            using var w = col.LogicalWriterOverride<T>();
            w.WriteBatch(values.Select(v => (T)Convert.ChangeType(v!, typeof(T), System.Globalization.CultureInfo.InvariantCulture)).ToArray());
        }
        else
        {
            using var w = col.LogicalWriterOverride<T?>();
            w.WriteBatch(values.Select(v => v is null ? (T?)null : (T)Convert.ChangeType(v, typeof(T), System.Globalization.CultureInfo.InvariantCulture)).ToArray());
        }
    }

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
        using var input = new ManagedRandomAccessFile(source);
        using var readerProps = ParquetWriteOptions.ReaderProperties(options?.DecryptionKey);
        using var reader = new ParquetFileReader(input, readerProps);
        using var meta = reader.FileMetaData;
        var layout = FileLayout.Of(meta, projection, mapping, options?.MatchByName ?? false);
        var evaluator = options?.Filter is { } filter && filter is not AlwaysTrue ? new MetricsEvaluator(filter) : null;
        if (ranged is not null && evaluator is null && layout.LeafCount == meta.NumColumns) await ranged.PrefetchAsync(0, ranged.Length, ct);
        var metrics = options?.Metrics;
        var groups = meta.NumRowGroups;
        long position = 0;
        for (var rg = 0; rg < groups; rg++)
        {
            ct.ThrowIfCancellationRequested();
            using var group = reader.RowGroup(rg);
            var groupMeta = group.MetaData;
            var n = checked((int)groupMeta.NumRows);
            if (evaluator is not null && !evaluator.MightMatch(n, (id, type) => layout.StatisticsFor(groupMeta, id, type, n)))
            {
                metrics?.RowGroupSkipped(n);
                position += n;
                continue;
            }
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
                        using var col = group.Column(l.Column);
                        var values = ReadColumn(col, f.Type, n);
                        for (var r = 0; r < n; r++) rows[r][c] = values[r];
                        break;
                    }
                    case ColumnSource.Nested nested:
                        NestedColumns.Load(group, nested.Tree);
                        for (var r = 0; r < n; r++) rows[r][c] = NestedColumns.Project(NestedColumns.Assemble(nested.Tree), nested.Tree, f.Type, FromPhysical);
                        break;
                }
            }
            metrics?.RowGroupRead(n);
            yield return new DataFileBatch(position, rows);
            position += n;
        }
    }

    /// <summary>Where each projected column comes from in a file, and which leaf holds each field ID's statistics.</summary>
    private abstract record ColumnSource
    {
        public sealed record Missing(object? Default) : ColumnSource;
        public sealed record Leaf(int Column) : ColumnSource;
        public sealed record Nested(NestedColumns.PNode Tree) : ColumnSource;
    }

    private sealed class FileLayout
    {
        public required ColumnSource[] Sources { get; init; }
        /// <summary>Field ID → leaf column index, for leaves outside lists and maps (one value per row).</summary>
        public required Dictionary<int, int> StatsLeaves { get; init; }
        public required SchemaDescriptor Descriptor { get; init; }
        /// <summary>How many distinct leaf columns of the file the projection decodes.</summary>
        public int LeafCount => Sources.SelectMany(s => s switch
        {
            ColumnSource.Leaf l => [l.Column],
            ColumnSource.Nested n => n.Tree.Leaves.Select(x => x.Column),
            _ => [],
        }).Distinct().Count();

        public static FileLayout Of(FileMetaData meta, Schema projection, NameMapping? mapping, bool matchByName = false)
        {
            var schema = meta.Schema;
            var byFieldId = new Dictionary<int, int>();
            var byName = new Dictionary<string, int>();
            var statsLeaves = new Dictionary<int, int>();
            for (var i = 0; i < meta.NumColumns; i++)
            {
                var descr = schema.Column(i);
                using var node = descr.SchemaNode;
                if (node.FieldId >= 0 && !matchByName)
                {
                    byFieldId[node.FieldId] = i;
                    if (descr.MaxRepetitionLevel == 0) statsLeaves[node.FieldId] = i;
                }
                byName[descr.Path.ToDotString()] = i;
            }
            var hasIds = byFieldId.Count > 0;
            // Top-level fields (nested columns span several leaves), with the index of their first leaf column.
            var topLevel = new List<(Node Node, int FirstColumn)>();
            var column = 0;
            foreach (var child in schema.GroupNode.Fields)
            {
                topLevel.Add((child, column));
                column += CountLeaves(child);
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
                if (!hasIds && schema.Column(colIndex).MaxRepetitionLevel == 0) statsLeaves[f.Id] = colIndex;
            }
            return new FileLayout { Sources = sources, StatsLeaves = statsLeaves, Descriptor = schema };
        }

        /// <summary>A row group's statistics for one field, or null when the file has none usable for it.</summary>
        public ColumnStats? StatisticsFor(RowGroupMetaData group, int fieldId, IcebergType type, long rows) =>
            StatsLeaves.TryGetValue(fieldId, out var leaf) ? RowGroupStatistics.For(group, Descriptor.Column(leaf), leaf, fieldId, type, rows) : null;
    }

    private static object?[] ReadColumn(ColumnReader col, IcebergType type, int n)
    {
        Decoding decoding;
        using (var logical = col.ColumnDescriptor.LogicalType) decoding = DecodingOf(logical);
        return (decoding, type.TypeId) switch
        {
            (Decoding.UInt32, TypeId.Long) =>
                col.LogicalReaderOverride<int?>().ReadAll(n).Select(v => v is null ? null : (object?)(long)unchecked((uint)v.Value)).ToArray(),
            (Decoding.UInt64, TypeId.Decimal) => col.LogicalReaderOverride<long?>().ReadAll(n)
                .Select(v => v is null ? null : (object?)new IcebergDecimal(unchecked((ulong)v.Value), ((DecimalType)type).Scale)).ToArray(),
            (Decoding.Millis, TypeId.Timestamp or TypeId.TimestampTz) =>
                col.LogicalReaderOverride<long?>().ReadAll(n).Select(v => v is null ? null : (object?)checked(v.Value * 1000)).ToArray(),
            _ => ReadPlain(col, type, n),
        };
    }

    private static object?[] ReadPlain(ColumnReader col, IcebergType type, int n) => type.TypeId switch
    {
        TypeId.Boolean => col.LogicalReaderOverride<bool?>().ReadAll(n).Select(v => (object?)v).ToArray(),
        TypeId.Int or TypeId.Date => col.LogicalReaderOverride<int?>().ReadAll(n).Select(v => (object?)v).ToArray(),
        // int -> long promotion: an old file may still store the column as INT32.
        TypeId.Long when col.ColumnDescriptor.PhysicalType == PhysicalType.Int32 =>
            col.LogicalReaderOverride<int?>().ReadAll(n).Select(v => v is null ? null : (object?)(long)v).ToArray(),
        TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs =>
            col.LogicalReaderOverride<long?>().ReadAll(n).Select(v => (object?)v).ToArray(),
        TypeId.Float => col.LogicalReaderOverride<float?>().ReadAll(n).Select(v => (object?)v).ToArray(),
        TypeId.Double when col.ColumnDescriptor.PhysicalType == PhysicalType.Float =>
            col.LogicalReaderOverride<float?>().ReadAll(n).Select(v => v is null ? null : (object?)(double)v).ToArray(),
        TypeId.Double => col.LogicalReaderOverride<double?>().ReadAll(n).Select(v => (object?)v).ToArray(),
        TypeId.String => col.LogicalReader<string?>().ReadAll(n).Select(v => (object?)v).ToArray(),
        TypeId.Binary => col.LogicalReader<byte[]?>().ReadAll(n).Select(v => (object?)v).ToArray(),
        TypeId.Uuid => ReadStruct<Guid>(col, n),
        TypeId.Decimal when col.ColumnDescriptor.PhysicalType == PhysicalType.Int32 =>
            col.LogicalReaderOverride<int?>().ReadAll(n).Select(v => v is null ? null : (object?)new IcebergDecimal(v.Value, ((DecimalType)type).Scale)).ToArray(),
        TypeId.Decimal when col.ColumnDescriptor.PhysicalType == PhysicalType.Int64 =>
            col.LogicalReaderOverride<long?>().ReadAll(n).Select(v => v is null ? null : (object?)new IcebergDecimal(v.Value, ((DecimalType)type).Scale)).ToArray(),
        // FIXED (or BINARY) decimals, any precision: big-endian two's-complement unscaled value.
        TypeId.Decimal when col.ColumnDescriptor.PhysicalType == PhysicalType.FixedLenByteArray => ReadFixed(col, n)
            .Select(v => v is null ? null : (object?)new IcebergDecimal(new System.Numerics.BigInteger(v, isUnsigned: false, isBigEndian: true), ((DecimalType)type).Scale)).ToArray(),
        TypeId.Decimal => col.LogicalReaderOverride<byte[]?>().ReadAll(n) // BINARY-backed decimals
            .Select(v => v is null ? null : (object?)new IcebergDecimal(new System.Numerics.BigInteger(v, isUnsigned: false, isBigEndian: true), ((DecimalType)type).Scale)).ToArray(),
        TypeId.Fixed => ReadFixed(col, n).Select(v => (object?)v).ToArray(),
        _ => throw new NotSupportedException($"Reading {type} is not supported yet"),
    };

    private static int CountLeaves(Node n) => n is GroupNode g ? g.Fields.Sum(CountLeaves) : 1;

    /// <summary>A raw physical value of a nested leaf as an Iceberg value of <paramref name="type"/>.</summary>
    private static object FromPhysical(IcebergType type, PhysicalType physical, int typeLength, object raw) => type switch
    {
        PrimitiveType { TypeId: TypeId.String } => System.Text.Encoding.UTF8.GetString((byte[])raw),
        PrimitiveType { TypeId: TypeId.Uuid } => new Guid((byte[])raw, bigEndian: true),
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

    /// <summary>ParquetSharp's non-override logical readers require <c>T</c> for required columns and <c>T?</c> for optional ones.</summary>
    private static object?[] ReadStruct<T>(ColumnReader col, int n) where T : struct =>
        col.ColumnDescriptor.MaxDefinitionLevel == 0
            ? col.LogicalReader<T>().ReadAll(n).Select(v => (object?)v).ToArray()
            : col.LogicalReader<T?>().ReadAll(n).Select(v => (object?)v).ToArray();
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

    public static ColumnStats? For(RowGroupMetaData group, ColumnDescriptor descr, int leaf, int fieldId, IcebergType type, long rows)
    {
        using var chunk = group.GetColumnChunkMetaData(leaf);
        if (!chunk.IsStatsSet) return null;
        using var stats = chunk.Statistics;
        if (stats is null) return null;
        var hasMinMax = stats.HasMinMax;
        long? nulls = hasMinMax || stats.NullCount > 0 ? stats.NullCount : null;
        object? lower = null, upper = null;
        if (hasMinMax && Comparable(descr, type))
        {
            try
            {
                lower = Convert(stats, min: true, descr, type);
                upper = Convert(stats, min: false, descr, type);
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
    private static bool Comparable(ColumnDescriptor descr, IcebergType type) => (descr.PhysicalType, type.TypeId) switch
    {
        (PhysicalType.Boolean, TypeId.Boolean) => true,
        (PhysicalType.Int32, TypeId.Int or TypeId.Date or TypeId.Long or TypeId.Decimal) => descr.SortOrder == SortOrder.Signed,
        (PhysicalType.Int64, TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs or TypeId.Decimal) =>
            descr.SortOrder == SortOrder.Signed,
        (PhysicalType.Float, TypeId.Float or TypeId.Double) or (PhysicalType.Double, TypeId.Double) => descr.SortOrder == SortOrder.Signed,
        (PhysicalType.ByteArray, TypeId.String or TypeId.Binary) => descr.SortOrder == SortOrder.Unsigned,
        (PhysicalType.FixedLenByteArray, TypeId.Fixed) => descr.SortOrder == SortOrder.Unsigned,
        _ => false,
    };

    private static object? Convert(Statistics stats, bool min, ColumnDescriptor descr, IcebergType type)
    {
        switch (stats)
        {
            case Statistics<bool> b: return min ? b.Min : b.Max;
            case Statistics<int> i:
            {
                var v = min ? i.Min : i.Max;
                return type switch
                {
                    DecimalType d => new IcebergDecimal(v, d.Scale),
                    PrimitiveType { TypeId: TypeId.Long } => (long)v,
                    _ => v,
                };
            }
            case Statistics<long> l:
            {
                var v = min ? l.Min : l.Max;
                using (var logical = descr.LogicalType)
                    if (ParquetFiles.DecodingOf(logical) == ParquetFiles.Decoding.Millis) return checked(v * 1000);
                return type is DecimalType d ? new IcebergDecimal(v, d.Scale) : v;
            }
            case Statistics<float> f:
            {
                var v = min ? f.Min : f.Max;
                return type.TypeId == TypeId.Double ? (double)v : v;
            }
            case Statistics<double> d: return min ? d.Min : d.Max;
            case Statistics<ByteArray> ba:
            {
                var v = min ? ba.Min : ba.Max;
                var bytes = new byte[v.Length];
                if (v.Length > 0) System.Runtime.InteropServices.Marshal.Copy(v.Pointer, bytes, 0, v.Length);
                if (type.TypeId != TypeId.String) return bytes;
                try
                {
                    return StrictUtf8.GetString(bytes);
                }
                catch (System.Text.DecoderFallbackException)
                {
                    return null; // e.g. a max truncated inside a multi-byte character: no bound
                }
            }
            case Statistics<FixedLenByteArray> fl:
            {
                var v = min ? fl.Min : fl.Max;
                var bytes = new byte[descr.TypeLength];
                System.Runtime.InteropServices.Marshal.Copy(v.Pointer, bytes, 0, bytes.Length);
                return bytes;
            }
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
