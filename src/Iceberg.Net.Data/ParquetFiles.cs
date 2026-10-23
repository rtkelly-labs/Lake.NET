using ParquetSharp;
using ParquetSharp.IO;
using ParquetSharp.Schema;
using Iceberg.Net.Manifests;
using Iceberg.Net.IO;
using Iceberg.Net.Types;
using TimeUnit = ParquetSharp.TimeUnit;

namespace Iceberg.Net.Data;

/// <summary>
/// Parquet data files with Iceberg field IDs. Rows are <c>object?[]</c> aligned to the schema's top-level
/// columns, holding physical Iceberg values (int days for date, long micros for time/timestamp, long nanos
/// for timestamp_ns, string, byte[], bool, int, long, float, double).
/// <see cref="IcebergDecimal"/> for decimal and <see cref="Guid"/> for uuid.
/// Scope: flat schemas; nested and fixed columns are rejected, decimals are written with precision ≤ 18.
/// </summary>
public static class ParquetFiles
{
    private static Node ToNode(NestedField f)
    {
        var rep = f.Required ? Repetition.Required : Repetition.Optional;
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

    /// <summary>Smallest byte width whose two's-complement range holds every unscaled value of the precision.</summary>
    public static int DecimalBytes(int precision)
    {
        var max = System.Numerics.BigInteger.Pow(10, precision) - 1;
        var bytes = 1;
        while (System.Numerics.BigInteger.Pow(2, 8 * bytes - 1) <= max) bytes++;
        return bytes;
    }

    private static byte[] FixedDecimal(System.Numerics.BigInteger unscaled, int width)
    {
        var raw = unscaled.ToByteArray(isUnsigned: false, isBigEndian: true);
        if (raw.Length > width) throw new ArgumentException($"Decimal {unscaled} does not fit in {width} bytes");
        var result = new byte[width];
        if (unscaled.Sign < 0) Array.Fill(result, (byte)0xFF); // sign extension
        raw.CopyTo(result, width - raw.Length);
        return result;
    }

    /// <summary>Write rows to a Parquet file at <paramref name="location"/> and return the Iceberg <see cref="DataFile"/> with metrics.</summary>
    public static async Task<DataFile> WriteAsync(
        IFileIO io,
        string location,
        Schema schema,
        int specId,
        IReadOnlyList<object?> partition,
        IReadOnlyList<object?[]> rows,
        CancellationToken ct = default)
    {
        var columns = schema.Columns;
        using var root = new GroupNode("table", Repetition.Required, columns.Select(ToNode).ToArray());
        using var props = new WriterPropertiesBuilder().Compression(Compression.Zstd).Build();
        byte[] bytes;
        using (var ms = new MemoryStream())
        {
            using (var output = new ManagedOutputStream(ms, leaveOpen: true))
            using (var writer = new ParquetFileWriter(output, root, props))
            {
                using (var rg = writer.AppendRowGroup())
                {
                    for (var c = 0; c < columns.Count; c++)
                    {
                        using var col = rg.NextColumn();
                        WriteColumn(col, columns[c], rows.Select(r => r[c]).ToArray());
                    }
                }
                writer.Close();
            }
            bytes = ms.ToArray();
        }

        long[] chunkSizes;
        using (var input = new ManagedRandomAccessFile(new MemoryStream(bytes)))
        using (var check = new ParquetFileReader(input))
        using (var group = check.RowGroup(0))
        {
            chunkSizes = Enumerable.Range(0, columns.Count).Select(i => group.MetaData.GetColumnChunkMetaData(i).TotalCompressedSize).ToArray();
        }
        await io.WriteAsync(location, bytes, overwrite: false, ct);

        var metrics = Metrics.Compute(schema, rows);
        return new DataFile
        {
            FilePath = location,
            FileFormat = "PARQUET",
            SpecId = specId,
            Partition = partition,
            RecordCount = rows.Count,
            FileSizeInBytes = bytes.Length,
            ColumnSizes = columns.Select((f, i) => (f.Id, chunkSizes[i])).Where(x => !RowLineage.IsReserved(x.Id)).ToDictionary(x => x.Id, x => x.Item2),
            ValueCounts = metrics.ValueCounts,
            NullValueCounts = metrics.NullCounts,
            NanValueCounts = metrics.NanCounts.Count > 0 ? metrics.NanCounts : null,
            LowerBounds = metrics.Lower,
            UpperBounds = metrics.Upper,
            SplitOffsets = [4],
        };
    }

    private static void WriteColumn(ColumnWriter col, NestedField f, object?[] values)
    {
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
        var bytes = await io.ReadAllAsync(location, ct);
        using var input = new ManagedRandomAccessFile(new MemoryStream(bytes));
        using var reader = new ParquetFileReader(input);
        using var meta = reader.FileMetaData;
        var numRows = checked((int)meta.NumRows);

        var byFieldId = new Dictionary<int, int>();
        var byName = new Dictionary<string, int>();
        for (var i = 0; i < meta.NumColumns; i++)
        {
            var descr = meta.Schema.Column(i);
            using var node = descr.SchemaNode;
            if (node.FieldId >= 0) byFieldId[node.FieldId] = i;
            byName[descr.Path.ToDotString()] = i;
        }

        var result = Enumerable.Range(0, numRows).Select(_ => new object?[projection.Columns.Count]).ToList();
        for (var c = 0; c < projection.Columns.Count; c++)
        {
            var f = projection.Columns[c];
            if (!f.Type.IsPrimitive) throw new NotSupportedException($"Nested column {f.Name} is not supported by the phase-0 reader");
            var colIndex = -1;
            var found = byFieldId.TryGetValue(f.Id, out colIndex)
                || (byFieldId.Count == 0 && (mapping?.NamesFor(f.Id) is { Count: > 0 } names ? names : [f.Name]).Any(n => byName.TryGetValue(n, out colIndex)));
            if (!found)
            {
                var def = Defaults.ToValue(f.Type, f.InitialDefault);
                foreach (var row in result) row[c] = def;
                continue;
            }

            var offset = 0;
            for (var rg = 0; rg < reader.FileMetaData.NumRowGroups; rg++)
            {
                using var group = reader.RowGroup(rg);
                var rgRows = checked((int)group.MetaData.NumRows);
                using var col = group.Column(colIndex);
                var values = ReadColumn(col, f.Type, rgRows);
                for (var r = 0; r < rgRows; r++) result[offset + r][c] = values[r];
                offset += rgRows;
            }
        }
        return result;
    }

    private static object?[] ReadColumn(ColumnReader col, IcebergType type, int n) => type.TypeId switch
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

    /// <summary>ParquetSharp's non-override logical readers require <c>T</c> for required columns and <c>T?</c> for optional ones.</summary>
    private static object?[] ReadStruct<T>(ColumnReader col, int n) where T : struct =>
        col.ColumnDescriptor.MaxDefinitionLevel == 0
            ? col.LogicalReader<T>().ReadAll(n).Select(v => (object?)v).ToArray()
            : col.LogicalReader<T?>().ReadAll(n).Select(v => (object?)v).ToArray();
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
        for (var c = 0; c < schema.Columns.Count; c++)
        {
            var f = schema.Columns[c];
            if (RowLineage.IsReserved(f.Id)) continue; // metadata columns carry no column metrics
            var values = rows.Select(r => r[c]).ToList();
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
            var comparer = Comparer<object>.Create((a, b) => a is string sa ? string.CompareOrdinal(sa, (string)b) : ((IComparable)a).CompareTo(b));
            m.Lower[f.Id] = Conversions.ToBytes(f.Type, present.Min(comparer)!);
            m.Upper[f.Id] = Conversions.ToBytes(f.Type, present.Max(comparer)!);
        }
        return m;
    }
}

/// <summary>Converts JSON single-value defaults (spec Appendix D, JSON form) to physical values.</summary>
internal static class Defaults
{
    public static object? ToValue(IcebergType type, object? jsonDefault) =>
        jsonDefault is System.Text.Json.Nodes.JsonNode node ? Json.SingleValueJson.FromJson(type, node) : null;
}
