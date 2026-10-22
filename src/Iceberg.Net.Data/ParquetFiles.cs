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
/// Phase-0 scope: flat schemas of those primitives; nested, decimal and uuid columns are rejected.
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
            _ => throw new NotSupportedException($"Column {f.Name}: {f.Type} is not supported by the phase-0 Parquet writer"),
        };
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
            ColumnSizes = columns.Select((f, i) => (f.Id, chunkSizes[i])).ToDictionary(x => x.Id, x => x.Item2),
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
            default: throw new NotSupportedException(f.Type.ToString());
        }
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
    public static async Task<List<object?[]>> ReadAsync(IFileIO io, string location, Schema projection, CancellationToken ct = default)
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
            var found = byFieldId.TryGetValue(f.Id, out var colIndex) || (byFieldId.Count == 0 && byName.TryGetValue(f.Name, out colIndex));
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
        _ => throw new NotSupportedException($"Reading {type} is not supported yet"),
    };
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
            var values = rows.Select(r => r[c]).ToList();
            m.ValueCounts[f.Id] = values.Count;
            m.NullCounts[f.Id] = values.Count(v => v is null);
            var present = values.Where(v => v is not null).Select(v => v!).ToList();
            if (f.Type.TypeId is TypeId.Float or TypeId.Double)
            {
                m.NanCounts[f.Id] = present.Count(v => v is double d ? double.IsNaN(d) : float.IsNaN((float)v));
                present = present.Where(v => v is double d ? !double.IsNaN(d) : !float.IsNaN((float)v)).ToList();
            }
            if (present.Count == 0 || f.Type.TypeId == TypeId.Binary) continue;
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
    public static object? ToValue(IcebergType type, object? jsonDefault)
    {
        if (jsonDefault is not System.Text.Json.Nodes.JsonValue v) return null;
        return type.TypeId switch
        {
            TypeId.Boolean => v.GetValue<bool>(),
            TypeId.Int => v.GetValue<int>(),
            TypeId.Long => v.GetValue<long>(),
            TypeId.Float => v.GetValue<float>(),
            TypeId.Double => v.GetValue<double>(),
            TypeId.String => v.GetValue<string>(),
            TypeId.Date => IcebergTime.DaysFromDate(DateOnly.Parse(v.GetValue<string>(), System.Globalization.CultureInfo.InvariantCulture)),
            _ => throw new NotSupportedException($"initial-default for {type} is not supported yet"),
        };
    }
}
