using System.Numerics;
using Iceberg.Net.Avro;
using Iceberg.Net.Json;
using Iceberg.Net.IO;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>
/// Data (and delete) files by their <c>file_format</c>: Parquet, or Avro as other writers produce it
/// (<c>write.format.default=avro</c> in Spark/Java).
/// </summary>
public static class DataFiles
{
    public static Task<List<object?[]>> ReadAsync(IFileIO io, Manifests.DataFile file, Schema projection, NameMapping? mapping = null, CancellationToken ct = default) =>
        ReadAsync(io, file.FilePath, file.FileFormat, projection, mapping, ct);

    public static Task<List<object?[]>> ReadAsync(IFileIO io, string location, string format, Schema projection, NameMapping? mapping = null, CancellationToken ct = default) =>
        format.ToUpperInvariant() switch
        {
            "PARQUET" => ParquetFiles.ReadAsync(io, location, projection, mapping, ct),
            "AVRO" => AvroDataFiles.ReadAsync(io, location, projection, mapping, ct),
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
        var contents = AvroContainer.Read(await io.ReadAllAsync(location, ct));
        var record = contents.Schema as AvroRecord ?? throw new InvalidDataException($"Avro data file {location} does not hold records");
        var hasIds = record.Fields.Any(f => f.FieldId is not null);
        var columns = projection.Columns.Select(f =>
        {
            var i = hasIds
                ? record.Fields.ToList().FindIndex(a => a.FieldId == f.Id)
                : (mapping?.NamesFor(f.Id) is { Count: > 0 } names ? names : [f.Name]).Select(record.IndexOf).FirstOrDefault(x => x >= 0, -1);
            return (Field: f, Index: i);
        }).ToList();
        return Corruption.Guard("Avro data file", () => contents.Records.Select(r =>
        {
            var values = (object?[])r!;
            return columns.Select(c => c.Index < 0
                ? Defaults.ToValue(c.Field.Type, c.Field.InitialDefault)
                : Convert(c.Field.Type, record.Fields[c.Index].Schema, values[c.Index])).ToArray();
        }).ToList());
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
