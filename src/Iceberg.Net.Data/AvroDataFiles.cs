using System.Numerics;
using System.Runtime.CompilerServices;
using Iceberg.Net.Avro;
using Iceberg.Net.Json;
using Iceberg.Net.IO;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>One decoded slice of a data file (a Parquet row group, an Avro block): rows from absolute position <see cref="FirstPosition"/>.</summary>
public sealed record DataFileBatch(long FirstPosition, IReadOnlyList<object?[]> Rows);

/// <summary>How a data file is streamed.</summary>
public sealed record DataFileReadOptions
{
    /// <summary>
    /// A bound row filter (any schema: only field IDs, types and literals are used). Parquet row groups whose footer
    /// statistics rule it out are skipped; rows of groups that are read are not filtered.
    /// </summary>
    public Expressions.Expression? Filter { get; init; }
    /// <summary>Receives row-group read/skip counts.</summary>
    public ScanMetrics? Metrics { get; init; }
}

/// <summary>
/// Data (and delete) files by their <c>file_format</c>: Parquet, or Avro as other writers produce it
/// (<c>write.format.default=avro</c> in Spark/Java).
/// </summary>
public static class DataFiles
{
    public static Task<List<object?[]>> ReadAsync(IFileIO io, Manifests.DataFile file, Schema projection, NameMapping? mapping = null, CancellationToken ct = default) =>
        ReadAsync(io, file.FilePath, file.FileFormat, projection, mapping, ct);

    public static async Task<List<object?[]>> ReadAsync(IFileIO io, string location, string format, Schema projection, NameMapping? mapping = null, CancellationToken ct = default)
    {
        var rows = new List<object?[]>();
        await foreach (var batch in ReadBatchesAsync(io, location, format, projection, mapping, null, ct)) rows.AddRange(batch.Rows);
        return rows;
    }

    /// <summary>
    /// Stream a data file projected onto <paramref name="projection"/> (only its columns are decoded), one Parquet row
    /// group or Avro block at a time, so memory is bounded by a group rather than the file's rows.
    /// </summary>
    public static IAsyncEnumerable<DataFileBatch> ReadBatchesAsync(IFileIO io, Manifests.DataFile file, Schema projection, NameMapping? mapping = null,
        DataFileReadOptions? options = null, CancellationToken ct = default) =>
        ReadBatchesAsync(io, file.FilePath, file.FileFormat, projection, mapping, options, ct);

    public static IAsyncEnumerable<DataFileBatch> ReadBatchesAsync(IFileIO io, string location, string format, Schema projection, NameMapping? mapping = null,
        DataFileReadOptions? options = null, CancellationToken ct = default) =>
        format.ToUpperInvariant() switch
        {
            "PARQUET" => ParquetFiles.ReadBatchesAsync(io, location, projection, mapping, options, ct),
            "AVRO" => AvroDataFiles.ReadBatchesAsync(io, location, projection, mapping, options, ct),
            _ => throw new NotSupportedException($"{format} data files are not supported (Parquet and Avro are)"),
        };
}

/// <summary>
/// Avro data files (Iceberg spec "Avro"): an Avro object container whose record schema carries <c>field-id</c> on
/// every field, <c>element-id</c> on arrays and <c>key-id</c>/<c>value-id</c> on maps. Read projected by field ID
/// (by name through the mapping when a file has none); logical types as the spec maps them: <c>date</c>,
/// <c>time-micros</c>, <c>timestamp-micros</c>/<c>-nanos</c>, <c>decimal</c> on bytes or fixed, <c>uuid</c> on fixed(16)
/// or string. Maps with string keys are Avro maps; others are arrays of key/value records (logical type <c>map</c>).
/// </summary>
public static class AvroDataFiles
{
    public static async Task<List<object?[]>> ReadAsync(IFileIO io, string location, Schema projection, NameMapping? mapping = null, CancellationToken ct = default)
    {
        var rows = new List<object?[]>();
        await foreach (var batch in ReadBatchesAsync(io, location, projection, mapping, null, ct)) rows.AddRange(batch.Rows);
        return rows;
    }

    private static readonly byte[] Magic = "Obj\u0001"u8.ToArray();

    /// <summary>
    /// Stream an Avro data file one container block at a time. Top-level fields outside the projection are skipped
    /// without being materialised (Avro is row-oriented, so their bytes are still scanned).
    /// </summary>
    public static async IAsyncEnumerable<DataFileBatch> ReadBatchesAsync(IFileIO io, string location, Schema projection, NameMapping? mapping = null,
        DataFileReadOptions? options = null, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var bytes = await io.ReadAllAsync(location, ct);
        var header = Corruption.Guard("Avro data file", () => ReadHeader(bytes, location));
        var record = header.Record;
        var hasIds = record.Fields.Any(f => f.FieldId is not null);
        var columns = projection.Columns.Select(f =>
        {
            var i = hasIds
                ? record.Fields.ToList().FindIndex(a => a.FieldId == f.Id)
                : (mapping?.NamesFor(f.Id) is { Count: > 0 } names ? names : [f.Name]).Select(record.IndexOf).FirstOrDefault(x => x >= 0, -1);
            return (Field: f, Index: i);
        }).ToList();
        // Which output column each file field feeds (-1: skip it).
        var target = Enumerable.Repeat(-1, record.Fields.Count).ToArray();
        for (var c = 0; c < columns.Count; c++)
            if (columns[c].Index >= 0) target[columns[c].Index] = c;
        var defaults = columns.Select(c => c.Index < 0 ? Defaults.ToValue(c.Field.Type, c.Field.InitialDefault) : null).ToArray();

        var position = header.DataStart;
        long rowPosition = 0;
        while (position < bytes.Length)
        {
            ct.ThrowIfCancellationRequested();
            var (rows, next) = Corruption.Guard("Avro data file", () => ReadBlock(bytes, position, header, columns, target, defaults));
            position = next;
            options?.Metrics?.RowGroupRead(rows.Count);
            yield return new DataFileBatch(rowPosition, rows);
            rowPosition += rows.Count;
        }
    }

    private sealed record Header(AvroRecord Record, string Codec, byte[] Sync, int DataStart);

    private static Header ReadHeader(byte[] bytes, string location)
    {
        using var s = new MemoryStream(bytes);
        var magic = new byte[4];
        s.ReadExactly(magic);
        if (!magic.AsSpan().SequenceEqual(Magic)) throw new FormatException("Not an Avro object container file");
        var rawMeta = (Dictionary<string, object?>)AvroBinary.Read(s, new AvroMap(AvroSchema.Bytes))!;
        var meta = rawMeta.ToDictionary(kv => kv.Key, kv => System.Text.Encoding.UTF8.GetString((byte[])kv.Value!));
        var sync = new byte[16];
        s.ReadExactly(sync);
        var schema = AvroSchemaJson.Parse(meta.TryGetValue("avro.schema", out var json) ? json : throw new InvalidDataException("Avro file has no avro.schema"));
        var record = schema as AvroRecord ?? throw new InvalidDataException($"Avro data file {location} does not hold records");
        var codec = meta.GetValueOrDefault("avro.codec", "null");
        if (codec is not ("null" or "deflate"))
            throw new NotSupportedException($"Avro codec {codec} (only null and deflate are implemented; Iceberg defaults to deflate)");
        return new Header(record, codec, sync, checked((int)s.Position));
    }

    private static (List<object?[]> Rows, int Next) ReadBlock(byte[] bytes, int start, Header header, List<(NestedField Field, int Index)> columns,
        int[] target, object?[] defaults)
    {
        using var s = new MemoryStream(bytes);
        s.Position = start;
        var count = AvroBinary.ReadLong(s);
        var size = AvroBinary.ReadLong(s);
        if (count < 0 || size < 0 || size > s.Length - s.Position) throw new InvalidDataException($"Avro block of {size} bytes / {count} records out of range");
        var data = new byte[size];
        s.ReadExactly(data);
        var marker = new byte[16];
        s.ReadExactly(marker);
        if (!marker.AsSpan().SequenceEqual(header.Sync)) throw new FormatException("Avro sync marker mismatch");
        var block = header.Codec == "deflate" ? Inflate(data) : data;
        // A record of an Iceberg schema takes at least one byte: a larger count is corrupt (and must not size a list).
        if (count > block.Length && header.Record.Fields.Count > 0) throw new InvalidDataException($"Avro block count {count} exceeds its {block.Length} bytes");
        using var bs = new MemoryStream(block);
        var rows = new List<object?[]>((int)Math.Min(count, 1 << 16));
        var fields = header.Record.Fields;
        for (var r = 0; r < count; r++)
        {
            var row = (object?[])defaults.Clone();
            for (var i = 0; i < fields.Count; i++)
            {
                if (target[i] < 0)
                {
                    Skip(bs, fields[i].Schema);
                    continue;
                }
                var c = target[i];
                row[c] = Convert(columns[c].Field.Type, fields[i].Schema, AvroBinary.Read(bs, fields[i].Schema));
            }
            rows.Add(row);
        }
        return (rows, checked((int)s.Position));
    }

    private static byte[] Inflate(byte[] data)
    {
        using var input = new MemoryStream(data);
        using var d = new System.IO.Compression.DeflateStream(input, System.IO.Compression.CompressionMode.Decompress);
        using var ms = new MemoryStream();
        d.CopyTo(ms);
        return ms.ToArray();
    }

    /// <summary>Advance past one value of <paramref name="schema"/> without materialising it.</summary>
    private static void Skip(Stream s, AvroSchema schema)
    {
        switch (schema)
        {
            case AvroPrimitive p:
                switch (p.Kind)
                {
                    case AvroKind.Null: return;
                    case AvroKind.Boolean: SkipBytes(s, 1); return;
                    case AvroKind.Int or AvroKind.Long: AvroBinary.ReadLong(s); return;
                    case AvroKind.Float: SkipBytes(s, 4); return;
                    case AvroKind.Double: SkipBytes(s, 8); return;
                    case AvroKind.Bytes or AvroKind.String: SkipBytes(s, AvroBinary.ReadLong(s)); return;
                    default: throw new NotSupportedException(p.Kind.ToString());
                }
            case AvroRecord r:
                foreach (var f in r.Fields) Skip(s, f.Schema);
                return;
            case AvroFixed f:
                SkipBytes(s, f.Size);
                return;
            case AvroUnion u:
            {
                var branch = AvroBinary.ReadLong(s);
                if (branch < 0 || branch >= u.Branches.Count) throw new InvalidDataException($"Avro union branch {branch} out of range");
                Skip(s, u.Branches[(int)branch]);
                return;
            }
            case AvroArray or AvroMap:
                while (true)
                {
                    var count = AvroBinary.ReadLong(s);
                    if (count == 0) return;
                    if (count < 0)
                    {
                        SkipBytes(s, AvroBinary.ReadLong(s)); // a negative count is followed by the block's byte size
                        continue;
                    }
                    if (count > s.Length - s.Position) throw new InvalidDataException($"Avro block count {count} exceeds the remaining {s.Length - s.Position} bytes");
                    for (var i = 0; i < count; i++)
                    {
                        if (schema is AvroMap m)
                        {
                            SkipBytes(s, AvroBinary.ReadLong(s));
                            Skip(s, m.Values);
                        }
                        else Skip(s, ((AvroArray)schema).Items);
                    }
                }
            default:
                throw new NotSupportedException(schema.GetType().Name);
        }
    }

    private static void SkipBytes(Stream s, long n)
    {
        if (n < 0 || n > s.Length - s.Position) throw new InvalidDataException($"Avro length {n} out of range");
        s.Position += n;
    }

    /// <summary>Write rows as an Avro data file (deflate) and return its <see cref="Manifests.DataFile"/> with the same metrics as Parquet.</summary>
    public static async Task<Manifests.DataFile> WriteAsync(IFileIO io, string location, Schema schema, int specId, IReadOnlyList<object?> partition,
        IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        var record = SchemaFor(schema);
        var records = rows.Select(r => (object?)schema.Columns.Select((c, i) => ToAvro(c.Type, r[i], c.Required, c.Name)).ToArray());
        var bytes = AvroContainer.Write(record, new Dictionary<string, string> { ["iceberg.schema"] = Json.TypeJson.SchemaToJson(schema).ToJsonString() }, records);
        await io.WriteAsync(location, bytes, overwrite: false, ct);
        var metrics = Metrics.Compute(schema, rows);
        return new Manifests.DataFile
        {
            FilePath = location,
            FileFormat = "AVRO",
            SpecId = specId,
            Partition = partition,
            RecordCount = rows.Count,
            FileSizeInBytes = bytes.Length,
            ValueCounts = metrics.ValueCounts,
            NullValueCounts = metrics.NullCounts,
            NanValueCounts = metrics.NanCounts.Count > 0 ? metrics.NanCounts : null,
            LowerBounds = metrics.Lower,
            UpperBounds = metrics.Upper,
        };
    }

    /// <summary>The Avro schema Java writes for an Iceberg schema: records and fixed types named by field ID (unique).</summary>
    public static AvroRecord SchemaFor(Schema schema) => new("table", schema.Columns.Select(Field).ToList());

    private static AvroField Field(NestedField f)
    {
        var s = Type(f.Type, f.Id);
        return f.Required ? new AvroField(f.Name, s, f.Id) : new AvroField(f.Name, AvroSchema.Optional(s), f.Id, DefaultNull: true);
    }

    private static AvroSchema Option(AvroSchema s, bool required) => required ? s : AvroSchema.Optional(s);

    private static AvroSchema Type(IcebergType t, int id) => t switch
    {
        StructType st => new AvroRecord($"r{id}", st.Fields.Select(Field).ToList()),
        ListType lt => new AvroArray(Option(Type(lt.ElementType, lt.ElementId), lt.ElementRequired)).WithProp("element-id", lt.ElementId),
        MapType { KeyType.TypeId: TypeId.String } mt => new AvroMap(Option(Type(mt.ValueType, mt.ValueId), mt.ValueRequired))
            .WithProp("key-id", mt.KeyId).WithProp("value-id", mt.ValueId),
        MapType mt => new AvroArray(new AvroRecord($"k{mt.KeyId}_v{mt.ValueId}",
            [new AvroField("key", Type(mt.KeyType, mt.KeyId), mt.KeyId), mt.ValueRequired ? new AvroField("value", Type(mt.ValueType, mt.ValueId), mt.ValueId)
                : new AvroField("value", AvroSchema.Optional(Type(mt.ValueType, mt.ValueId)), mt.ValueId, DefaultNull: true)])).WithLogical("map"),
        PrimitiveType { TypeId: TypeId.Uuid } => new AvroFixed($"uuid_fixed_{id}", 16).WithLogical("uuid"),
        FixedType f => new AvroFixed($"fixed_{id}", f.Length),
        DecimalType d => new AvroFixed($"decimal_{id}", ManifestCodec.DecimalRequiredBytes(d.Precision)).WithLogical("decimal", ("precision", d.Precision), ("scale", d.Scale)),
        _ => ManifestCodec.ToAvro(t),
    };

    private static object? ToAvro(IcebergType type, object? v, bool required, string name)
    {
        if (v is null) return required ? throw new ArgumentException($"Required field {name} is null") : null;
        switch (type)
        {
            case StructType st:
                var fields = (object?[])v;
                return st.Fields.Select((f, i) => ToAvro(f.Type, fields[i], f.Required, f.Name)).ToArray();
            case ListType lt:
                return ((System.Collections.IEnumerable)v).Cast<object?>().Select(e => ToAvro(lt.ElementType, e, lt.ElementRequired, "element")).ToList();
            case MapType { KeyType.TypeId: TypeId.String } mt:
                return ((System.Collections.IDictionary)v).Keys.Cast<object>().ToDictionary(k => (string)k, k => ToAvro(mt.ValueType, ((System.Collections.IDictionary)v)[k], mt.ValueRequired, "value"));
            case MapType mt:
            {
                var d = (System.Collections.IDictionary)v;
                return d.Keys.Cast<object>().Select(k => (object?)new[] { ToAvro(mt.KeyType, k, true, "key"), ToAvro(mt.ValueType, d[k], mt.ValueRequired, "value") }).ToList();
            }
            case DecimalType dt:
            {
                var unscaled = v is IcebergDecimal m ? m.Scale == dt.Scale ? m.Unscaled : throw new ArgumentException($"{name}: decimal scale {m.Scale} != {dt.Scale}") : throw new ArgumentException($"{name}: expected a decimal");
                return ParquetFiles.FixedDecimal(unscaled, ManifestCodec.DecimalRequiredBytes(dt.Precision));
            }
            case PrimitiveType { TypeId: TypeId.Uuid }:
                return ((Guid)v).ToByteArray(bigEndian: true);
            default:
                return v;
        }
    }

    private static object? Convert(IcebergType type, AvroSchema schema, object? raw)
    {
        if (raw is null) return null;
        var s = schema.NonNull;
        switch (type)
        {
            case StructType st:
            {
                var rec = (AvroRecord)s;
                var values = (object?[])raw;
                return st.Fields.Select(f =>
                {
                    var i = rec.Fields.ToList().FindIndex(a => a.FieldId == f.Id);
                    if (i < 0) i = rec.IndexOf(f.Name);
                    return i < 0 ? Defaults.ToValue(f.Type, f.InitialDefault) : Convert(f.Type, rec.Fields[i].Schema, values[i]);
                }).ToArray();
            }
            case ListType lt:
                return ((List<object?>)raw).Select(e => Convert(lt.ElementType, ((AvroArray)s).Items, e)).ToList();
            case MapType mt when s is AvroMap am:
            {
                var map = new Dictionary<object, object?>();
                foreach (var (k, v) in (Dictionary<string, object?>)raw) map[k] = Convert(mt.ValueType, am.Values, v);
                return map;
            }
            case MapType mt:
            {
                var entry = (AvroRecord)((AvroArray)s).Items.NonNull;
                var map = new Dictionary<object, object?>();
                foreach (var kv in (List<object?>)raw)
                {
                    var pair = (object?[])kv!;
                    map[Convert(mt.KeyType, entry.Fields[0].Schema, pair[0])!] = Convert(mt.ValueType, entry.Fields[1].Schema, pair[1]);
                }
                return map;
            }
            case DecimalType d:
                return new IcebergDecimal(new BigInteger((byte[])raw, isUnsigned: false, isBigEndian: true), d.Scale);
            case PrimitiveType { TypeId: TypeId.Uuid }:
                return raw is string text ? Guid.Parse(text) : new Guid((byte[])raw, bigEndian: true);
            case PrimitiveType { TypeId: TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs } when raw is int i:
                return (long)i; // int -> long promotion
            case PrimitiveType { TypeId: TypeId.Double } when raw is float f:
                return (double)f;
            default:
                return raw; // boolean, int/date, long/time/timestamps, float, double, string, binary, fixed
        }
    }
}
