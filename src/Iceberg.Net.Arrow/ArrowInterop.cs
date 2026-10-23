using System.Numerics;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Iceberg.Net.Types;
using ArrowSchema = Apache.Arrow.Schema;
using IcebergSchema = Iceberg.Net.Types.Schema;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace Iceberg.Net.Arrow;

/// <summary>
/// Iceberg ↔ Apache Arrow, the way PyIceberg and Java's Arrow readers map them:
/// field IDs travel as <c>PARQUET:field_id</c> metadata; dates are <c>date32</c>, times <c>time64[us]</c>, timestamps
/// <c>timestamp[us]</c> (<c>UTC</c> for <c>timestamptz</c>), nanosecond variants <c>timestamp[ns]</c>, decimals
/// <c>decimal128(P,S)</c>, UUIDs and <c>fixed(L)</c> <c>fixed_size_binary</c>; structs are <c>struct</c>, lists
/// <c>list&lt;element&gt;</c>, maps <c>map&lt;key, value&gt;</c>, with field IDs on every child. Arrays are built from raw
/// buffers, so values round-trip exactly (38-digit decimals included). Nested values use the row shapes of
/// Iceberg.Net.Data: <c>object?[]</c> structs, <c>List&lt;object?&gt;</c> lists, <c>Dictionary&lt;object, object?&gt;</c> maps.
/// </summary>
public static class ArrowInterop
{
    public const string FieldIdKey = "PARQUET:field_id";

    public static ArrowSchema ToArrow(IcebergSchema schema) => new(schema.Columns.Select(ToField), null);

    private static Field ToField(NestedField f) => new(f.Name, ToArrow(f.Type), !f.Required, FieldMetadata(f));

    /// <summary>The field ID, and for UUIDs Arrow's canonical <c>arrow.uuid</c> extension type (as PyIceberg produces).</summary>
    private static IEnumerable<KeyValuePair<string, string>> FieldMetadata(NestedField f)
    {
        yield return KeyValuePair.Create(FieldIdKey, f.Id.ToString(System.Globalization.CultureInfo.InvariantCulture));
        if (f.Type.TypeId == TypeId.Uuid)
        {
            yield return KeyValuePair.Create("ARROW:extension:name", "arrow.uuid");
            yield return KeyValuePair.Create("ARROW:extension:metadata", "");
        }
    }

    public static IArrowType ToArrow(IcebergType type) => type switch
    {
        PrimitiveType { TypeId: TypeId.Boolean } => BooleanType.Default,
        PrimitiveType { TypeId: TypeId.Int } => Int32Type.Default,
        PrimitiveType { TypeId: TypeId.Long } => Int64Type.Default,
        PrimitiveType { TypeId: TypeId.Float } => FloatType.Default,
        PrimitiveType { TypeId: TypeId.Double } => DoubleType.Default,
        DecimalType d => new Decimal128Type(d.Precision, d.Scale),
        PrimitiveType { TypeId: TypeId.Date } => Date32Type.Default,
        PrimitiveType { TypeId: TypeId.Time } => new Time64Type(TimeUnit.Microsecond),
        PrimitiveType { TypeId: TypeId.Timestamp } => new TimestampType(TimeUnit.Microsecond, (string?)null),
        PrimitiveType { TypeId: TypeId.TimestampTz } => new TimestampType(TimeUnit.Microsecond, "UTC"),
        PrimitiveType { TypeId: TypeId.TimestampNs } => new TimestampType(TimeUnit.Nanosecond, (string?)null),
        PrimitiveType { TypeId: TypeId.TimestampTzNs } => new TimestampType(TimeUnit.Nanosecond, "UTC"),
        PrimitiveType { TypeId: TypeId.String } => StringType.Default,
        PrimitiveType { TypeId: TypeId.Binary } => BinaryType.Default,
        PrimitiveType { TypeId: TypeId.Uuid } => new FixedSizeBinaryType(16),
        FixedType f => new FixedSizeBinaryType(f.Length),
        Types.StructType s => new Apache.Arrow.Types.StructType(s.Fields.Select(ToField).ToList()),
        Types.ListType l => new Apache.Arrow.Types.ListType(ToField(l.ElementField)),
        Types.MapType m => new Apache.Arrow.Types.MapType(ToField(m.KeyField), ToField(m.ValueField)),
        _ => throw new NotSupportedException($"No Arrow mapping for {type}"),
    };

    /// <summary>Rows (aligned to <paramref name="schema"/>'s columns, physical values) as one record batch.</summary>
    public static RecordBatch ToRecordBatch(IcebergSchema schema, IReadOnlyList<object?[]> rows)
    {
        var arrays = schema.Columns.Select((f, c) => Build(f, rows.Select(r => r[c]).ToArray())).ToList();
        return new RecordBatch(ToArrow(schema), arrays, rows.Count);
    }

    /// <summary>
    /// A record batch as rows aligned to <paramref name="schema"/>: columns are matched by field ID metadata when present,
    /// else by name; columns missing from the batch are null.
    /// </summary>
    public static List<object?[]> ToRows(IcebergSchema schema, RecordBatch batch)
    {
        var byId = new Dictionary<int, int>();
        var byName = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < batch.Schema.FieldsList.Count; i++)
        {
            var field = batch.Schema.FieldsList[i];
            if (field.Metadata is { } md && md.TryGetValue(FieldIdKey, out var id) && int.TryParse(id, out var fid)) byId[fid] = i;
            byName[field.Name] = i;
        }
        var columns = schema.Columns.Select(f => byId.TryGetValue(f.Id, out var i) || byName.TryGetValue(f.Name, out i) ? Read(f, batch.Column(i)) : null).ToList();
        var rows = new List<object?[]>(batch.Length);
        for (var r = 0; r < batch.Length; r++) rows.Add(columns.Select(col => col?[r]).ToArray());
        return rows;
    }

    private static IArrowArray Build(NestedField f, object?[] values)
    {
        var type = ToArrow(f.Type);
        switch (f.Type.TypeId)
        {
            case TypeId.Boolean:
            {
                var b = new BooleanArray.Builder();
                foreach (var v in values) if (v is bool x) b.Append(x); else b.AppendNull();
                return b.Build();
            }
            case TypeId.String:
            {
                var b = new StringArray.Builder();
                foreach (var v in values) if (v is string x) b.Append(x); else b.AppendNull();
                return b.Build();
            }
            case TypeId.Binary:
            {
                var b = new BinaryArray.Builder();
                foreach (var v in values) if (v is byte[] x) b.Append(x.AsSpan()); else b.AppendNull();
                return b.Build();
            }
            case TypeId.Int or TypeId.Date: return Fixed(type, values, v => (int)v);
            case TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs: return Fixed(type, values, v => (long)v);
            case TypeId.Float: return Fixed(type, values, v => (float)v);
            case TypeId.Double: return Fixed(type, values, v => (double)v);
            case TypeId.Decimal:
            {
                var scale = ((DecimalType)f.Type).Scale;
                return Bytes(type, 16, values, v => Int128Le(((IcebergDecimal)v).Scale == scale ? ((IcebergDecimal)v).Unscaled : throw new ArgumentException($"Column {f.Name}: decimal scale mismatch")));
            }
            case TypeId.Uuid: return Bytes(type, 16, values, v => ((Guid)v).ToByteArray(bigEndian: true));
            case TypeId.Fixed: return Bytes(type, ((FixedType)f.Type).Length, values, v => (byte[])v);
            case TypeId.Struct:
            {
                var st = (Types.StructType)f.Type;
                var children = st.Fields.Select((child, i) => Build(child, values.Select(v => v is null ? null : ((object?[])v)[i]).ToArray())).ToList();
                var (bitmap, nulls) = Validity(values);
                return new StructArray(type, values.Length, children, bitmap, nulls);
            }
            case TypeId.List:
            {
                var lt = (Types.ListType)f.Type;
                var (offsets, flat) = Flatten(values, v => ((System.Collections.IEnumerable)v).Cast<object?>());
                var (bitmap, nulls) = Validity(values);
                return new ListArray(type, values.Length, offsets, Build(lt.ElementField, flat), bitmap, nulls);
            }
            case TypeId.Map:
            {
                var mt = (Types.MapType)f.Type;
                var (offsets, flat) = Flatten(values, v => Entries(v).Select(e => (object?)e));
                var keys = Build(mt.KeyField, flat.Select(e => ((KeyValuePair<object?, object?>)e!).Key).ToArray());
                var vals = Build(mt.ValueField, flat.Select(e => ((KeyValuePair<object?, object?>)e!).Value).ToArray());
                var entries = new StructArray(((Apache.Arrow.Types.MapType)type).KeyValueType, flat.Length, [keys, vals], ArrowBuffer.Empty, 0);
                var (bitmap, nulls) = Validity(values);
                return new MapArray(type, values.Length, offsets, entries, bitmap, nulls);
            }
            default: throw new NotSupportedException(f.Type.ToString());
        }
    }

    private static (ArrowBuffer Bitmap, int Nulls) Validity(object?[] values)
    {
        var validity = new ArrowBuffer.BitmapBuilder(values.Length);
        foreach (var v in values) validity.Append(v is not null);
        return (validity.Build(), values.Count(v => v is null));
    }

    /// <summary>Concatenate each value's items; offsets[i]..offsets[i+1] are row i's (a null row has none).</summary>
    private static (ArrowBuffer Offsets, object?[] Items) Flatten(object?[] values, Func<object, IEnumerable<object?>> items)
    {
        var offsets = new ArrowBuffer.Builder<int>(values.Length + 1);
        var flat = new List<object?>();
        offsets.Append(0);
        foreach (var v in values)
        {
            if (v is not null) flat.AddRange(items(v));
            offsets.Append(flat.Count);
        }
        return (offsets.Build(), flat.ToArray());
    }

    private static IEnumerable<KeyValuePair<object?, object?>> Entries(object map) => map switch
    {
        System.Collections.IDictionary d => d.Keys.Cast<object>().Select(k => new KeyValuePair<object?, object?>(k, d[k])),
        IEnumerable<KeyValuePair<object, object?>> pairs => pairs.Select(p => new KeyValuePair<object?, object?>(p.Key, p.Value)),
        _ => throw new ArgumentException($"Map values must be dictionaries, got {map.GetType().Name}"),
    };

    private static IArrowArray Fixed<T>(IArrowType type, object?[] values, Func<object, T> convert) where T : struct
    {
        var data = new ArrowBuffer.Builder<T>(values.Length);
        var validity = new ArrowBuffer.BitmapBuilder(values.Length);
        var nulls = 0;
        foreach (var v in values)
        {
            data.Append(v is null ? default : convert(v));
            validity.Append(v is not null);
            if (v is null) nulls++;
        }
        return ArrowArrayFactory.BuildArray(new ArrayData(type, values.Length, nulls, 0, [validity.Build(), data.Build()]));
    }

    private static IArrowArray Bytes(IArrowType type, int width, object?[] values, Func<object, byte[]> convert)
    {
        var data = new ArrowBuffer.Builder<byte>(values.Length * width);
        var validity = new ArrowBuffer.BitmapBuilder(values.Length);
        var nulls = 0;
        foreach (var v in values)
        {
            var bytes = v is null ? new byte[width] : convert(v);
            if (bytes.Length != width) throw new ArgumentException($"Expected {width}-byte values, got {bytes.Length}");
            data.Append(bytes.AsSpan());
            validity.Append(v is not null);
            if (v is null) nulls++;
        }
        return ArrowArrayFactory.BuildArray(new ArrayData(type, values.Length, nulls, 0, [validity.Build(), data.Build()]));
    }

    /// <summary>Arrow decimal128: 16-byte little-endian two's complement.</summary>
    private static byte[] Int128Le(BigInteger unscaled)
    {
        var raw = unscaled.ToByteArray(isUnsigned: false, isBigEndian: false);
        if (raw.Length > 16) throw new ArgumentException($"Decimal {unscaled} does not fit in 128 bits");
        var result = new byte[16];
        if (unscaled.Sign < 0) System.Array.Fill(result, (byte)0xFF);
        raw.CopyTo(result, 0);
        return result;
    }

    private static object?[] Read(NestedField f, IArrowArray array) => Enumerable.Range(0, array.Length).Select(i => ReadValue(f, array, i)).ToArray();

    private static object? ReadValue(NestedField f, IArrowArray array, int i)
    {
        if (array.IsNull(i)) return null;
        return (f.Type.TypeId, array) switch
            {
                (TypeId.Struct, StructArray a) => ReadStruct((Types.StructType)f.Type, a, i),
                (TypeId.List, ListArray a) => Enumerable.Range(a.ValueOffsets[i], a.ValueOffsets[i + 1] - a.ValueOffsets[i])
                    .Select(k => ReadValue(((Types.ListType)f.Type).ElementField, a.Values, k)).ToList(),
                (TypeId.Map, MapArray a) => ReadMap((Types.MapType)f.Type, a, i),
                (TypeId.Boolean, BooleanArray a) => a.GetValue(i),
                (TypeId.String, StringArray a) => a.GetString(i),
                (TypeId.Binary, BinaryArray a) => a.GetBytes(i).ToArray(),
                (TypeId.Int or TypeId.Date, _) => Value<int>(array, i),
                (TypeId.Long, Int32Array a) => (long)a.GetValue(i)!.Value, // promoted column
                (TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs, _) => Value<long>(array, i),
                (TypeId.Float, _) => Value<float>(array, i),
                (TypeId.Double, FloatArray a) => (double)a.GetValue(i)!.Value,
                (TypeId.Double, _) => Value<double>(array, i),
                (TypeId.Decimal, _) => new IcebergDecimal(new BigInteger(Slice(array, i, 16), isUnsigned: false, isBigEndian: false), ((DecimalType)f.Type).Scale),
                (TypeId.Uuid, _) => new Guid(Slice(array, i, 16), bigEndian: true),
                (TypeId.Fixed, _) => Slice(array, i, ((FixedType)f.Type).Length).ToArray(),
                _ => throw new NotSupportedException($"Reading {f.Type} from {array.GetType().Name}"),
            };
    }

    /// <summary>Struct children are matched by field ID metadata when present, else by name.</summary>
    private static object?[] ReadStruct(Types.StructType type, StructArray a, int i)
    {
        var fields = ((Apache.Arrow.Types.StructType)a.Data.DataType).Fields;
        return type.Fields.Select(f =>
        {
            var j = fields.ToList().FindIndex(x => x.Metadata is { } md && md.TryGetValue(FieldIdKey, out var id) && id == f.Id.ToString(System.Globalization.CultureInfo.InvariantCulture));
            if (j < 0) j = fields.ToList().FindIndex(x => x.Name == f.Name);
            return j < 0 ? null : ReadValue(f, a.Fields[j], i);
        }).ToArray();
    }

    private static Dictionary<object, object?> ReadMap(Types.MapType type, MapArray a, int i)
    {
        var map = new Dictionary<object, object?>();
        for (var k = a.ValueOffsets[i]; k < a.ValueOffsets[i + 1]; k++)
            map[ReadValue(type.KeyField, a.Keys, k)!] = ReadValue(type.ValueField, a.Values, k);
        return map;
    }

    private static T Value<T>(IArrowArray array, int i) where T : struct =>
        MemoryMarshal.Cast<byte, T>(array.Data.Buffers[1].Span)[array.Offset + i];

    private static ReadOnlySpan<byte> Slice(IArrowArray array, int i, int width) =>
        array.Data.Buffers[1].Span.Slice((array.Offset + i) * width, width);
}
