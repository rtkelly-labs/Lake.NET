using System.Buffers.Binary;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;

namespace Iceberg.Net.Avro;

/// <summary>
/// Avro binary encoding and the object container file format. Generic values: records are
/// <c>object?[]</c> (positional), arrays <c>IList&lt;object?&gt;</c>, maps <c>IDictionary&lt;string, object?&gt;</c>,
/// bytes/fixed <c>byte[]</c>.
/// </summary>
public static class AvroBinary
{
    public static void Write(Stream s, AvroSchema schema, object? value)
    {
        switch (schema)
        {
            case AvroPrimitive p:
                WritePrimitive(s, p.Kind, value);
                break;
            case AvroRecord r:
                var fields = (object?[])value!;
                for (var i = 0; i < r.Fields.Count; i++) Write(s, r.Fields[i].Schema, fields[i]);
                break;
            case AvroArray a:
                var items = (System.Collections.IList)value!;
                if (items.Count > 0)
                {
                    WriteLong(s, items.Count);
                    foreach (var item in items) Write(s, a.Items, item);
                }
                WriteLong(s, 0);
                break;
            case AvroMap m:
                var map = (IDictionary<string, object?>)value!;
                if (map.Count > 0)
                {
                    WriteLong(s, map.Count);
                    foreach (var (k, v) in map)
                    {
                        WriteString(s, k);
                        Write(s, m.Values, v);
                    }
                }
                WriteLong(s, 0);
                break;
            case AvroFixed f:
                var bytes = (byte[])value!;
                if (bytes.Length != f.Size) throw new ArgumentException($"fixed {f.Name} needs {f.Size} bytes, got {bytes.Length}");
                s.Write(bytes);
                break;
            case AvroUnion u:
                var branch = ChooseBranch(u, value);
                WriteLong(s, branch);
                Write(s, u.Branches[branch], value);
                break;
            default:
                throw new NotSupportedException(schema.GetType().Name);
        }
    }

    private static int ChooseBranch(AvroUnion u, object? value)
    {
        for (var i = 0; i < u.Branches.Count; i++)
        {
            var matches = (u.Branches[i], value) switch
            {
                (AvroPrimitive { Kind: AvroKind.Null }, null) => true,
                (AvroPrimitive { Kind: AvroKind.Null }, _) => false,
                (_, null) => false,
                (AvroPrimitive { Kind: AvroKind.Boolean }, bool) => true,
                (AvroPrimitive { Kind: AvroKind.Int }, int) => true,
                (AvroPrimitive { Kind: AvroKind.Long }, long or int) => true,
                (AvroPrimitive { Kind: AvroKind.Float }, float) => true,
                (AvroPrimitive { Kind: AvroKind.Double }, double or float) => true,
                (AvroPrimitive { Kind: AvroKind.String }, string) => true,
                (AvroPrimitive { Kind: AvroKind.Bytes }, byte[]) => true,
                (AvroFixed f, byte[] b) => b.Length == f.Size,
                (AvroRecord, object?[]) => true,
                (AvroArray, System.Collections.IList) => true,
                (AvroMap, IDictionary<string, object?>) => true,
                _ => false,
            };
            if (matches) return i;
        }
        throw new ArgumentException($"No union branch for {value?.GetType().Name ?? "null"}");
    }

    private static void WritePrimitive(Stream s, AvroKind kind, object? value)
    {
        Span<byte> buf = stackalloc byte[8];
        switch (kind)
        {
            case AvroKind.Null: break;
            case AvroKind.Boolean: s.WriteByte((bool)value! ? (byte)1 : (byte)0); break;
            case AvroKind.Int: WriteLong(s, Convert.ToInt32(value)); break;
            case AvroKind.Long: WriteLong(s, Convert.ToInt64(value)); break;
            case AvroKind.Float:
                BinaryPrimitives.WriteSingleLittleEndian(buf, Convert.ToSingle(value));
                s.Write(buf[..4]);
                break;
            case AvroKind.Double:
                BinaryPrimitives.WriteDoubleLittleEndian(buf, Convert.ToDouble(value));
                s.Write(buf);
                break;
            case AvroKind.Bytes:
                var b = (byte[])value!;
                WriteLong(s, b.Length);
                s.Write(b);
                break;
            case AvroKind.String: WriteString(s, (string)value!); break;
        }
    }

    public static void WriteString(Stream s, string value)
    {
        var b = Encoding.UTF8.GetBytes(value);
        WriteLong(s, b.Length);
        s.Write(b);
    }

    /// <summary>Zig-zag varint.</summary>
    public static void WriteLong(Stream s, long value)
    {
        var n = (ulong)((value << 1) ^ (value >> 63));
        while ((n & ~0x7FUL) != 0)
        {
            s.WriteByte((byte)((n & 0x7F) | 0x80));
            n >>= 7;
        }
        s.WriteByte((byte)n);
    }

    public static object? Read(Stream s, AvroSchema schema)
    {
        switch (schema)
        {
            case AvroPrimitive p:
                return ReadPrimitive(s, p.Kind);
            case AvroRecord r:
                var values = new object?[r.Fields.Count];
                for (var i = 0; i < values.Length; i++) values[i] = Read(s, r.Fields[i].Schema);
                return values;
            case AvroArray a:
            {
                var list = new List<object?>();
                for (var count = ReadBlockCount(s); count != 0; count = ReadBlockCount(s))
                    for (var i = 0; i < count; i++) list.Add(Read(s, a.Items));
                return list;
            }
            case AvroMap m:
            {
                var map = new Dictionary<string, object?>();
                for (var count = ReadBlockCount(s); count != 0; count = ReadBlockCount(s))
                    for (var i = 0; i < count; i++) map[ReadString(s)] = Read(s, m.Values);
                return map;
            }
            case AvroFixed f:
            {
                var b = new byte[f.Size];
                s.ReadExactly(b);
                return b;
            }
            case AvroUnion u:
            {
                var branch = ReadLong(s);
                if (branch < 0 || branch >= u.Branches.Count) throw new InvalidDataException($"Avro union branch {branch} out of range");
                return Read(s, u.Branches[(int)branch]);
            }
            default:
                throw new NotSupportedException(schema.GetType().Name);
        }
    }

    /// <summary>Negative block counts are followed by the block's byte size (allows skipping).</summary>
    private static long ReadBlockCount(Stream s)
    {
        var count = ReadLong(s);
        if (count < 0)
        {
            ReadLong(s);
            count = count == long.MinValue ? throw new InvalidDataException("Avro block count out of range") : -count;
        }
        // Every item of an Iceberg schema takes at least one byte, so a count beyond the remaining bytes is corrupt
        // (and would otherwise spin through billions of empty reads).
        if (s.CanSeek && count > s.Length - s.Position) throw new InvalidDataException($"Avro block count {count} exceeds the remaining {s.Length - s.Position} bytes");
        return count;
    }

    private static object? ReadPrimitive(Stream s, AvroKind kind)
    {
        Span<byte> buf = stackalloc byte[8];
        switch (kind)
        {
            case AvroKind.Null: return null;
            case AvroKind.Boolean: return s.ReadByte() != 0;
            case AvroKind.Int:
            {
                var v = ReadLong(s);
                return v is < int.MinValue or > int.MaxValue ? throw new InvalidDataException($"Avro int {v} out of range") : (int)v;
            }
            case AvroKind.Long: return ReadLong(s);
            case AvroKind.Float:
                s.ReadExactly(buf[..4]);
                return BinaryPrimitives.ReadSingleLittleEndian(buf);
            case AvroKind.Double:
                s.ReadExactly(buf);
                return BinaryPrimitives.ReadDoubleLittleEndian(buf);
            case AvroKind.Bytes:
                var b = new byte[ReadLength(s)];
                s.ReadExactly(b);
                return b;
            case AvroKind.String: return ReadString(s);
            default: throw new NotSupportedException(kind.ToString());
        }
    }

    public static string ReadString(Stream s)
    {
        var b = new byte[ReadLength(s)];
        s.ReadExactly(b);
        return Encoding.UTF8.GetString(b);
    }

    /// <summary>A bytes/string length, checked against what is left before it sizes an allocation.</summary>
    private static int ReadLength(Stream s)
    {
        var length = ReadLong(s);
        if (length < 0 || length > int.MaxValue || (s.CanSeek && length > s.Length - s.Position))
            throw new InvalidDataException($"Avro length {length} out of range");
        return (int)length;
    }

    public static long ReadLong(Stream s)
    {
        ulong result = 0;
        var shift = 0;
        while (true)
        {
            var b = s.ReadByte();
            if (b < 0) throw new EndOfStreamException();
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
            if (shift > 63) throw new InvalidDataException("Avro varint longer than 10 bytes");
        }
        return (long)(result >> 1) ^ -(long)(result & 1);
    }
}

public enum AvroCodecKind { Null, Deflate }

/// <summary>Avro object container files (<c>Obj\x01</c> magic, metadata map, sync-delimited blocks).</summary>
public static class AvroContainer
{
    private static readonly byte[] Magic = "Obj\u0001"u8.ToArray();
    private static readonly AvroMap MetaSchema = new(AvroSchema.Bytes);

    public static byte[] Write(AvroSchema schema, IReadOnlyDictionary<string, string> metadata, IEnumerable<object?> records, AvroCodecKind codec = AvroCodecKind.Deflate, int blockSize = 1000)
    {
        using var output = new MemoryStream();
        output.Write(Magic);
        var meta = new Dictionary<string, object?>
        {
            ["avro.schema"] = Encoding.UTF8.GetBytes(AvroSchemaJson.ToJson(schema).ToJsonString()),
            ["avro.codec"] = Encoding.UTF8.GetBytes(codec == AvroCodecKind.Deflate ? "deflate" : "null"),
        };
        foreach (var (k, v) in metadata) meta[k] = Encoding.UTF8.GetBytes(v);
        AvroBinary.Write(output, MetaSchema, meta);
        var sync = RandomNumberGenerator.GetBytes(16);
        output.Write(sync);

        foreach (var block in records.Chunk(blockSize))
        {
            using var raw = new MemoryStream();
            foreach (var r in block) AvroBinary.Write(raw, schema, r);
            var data = codec == AvroCodecKind.Deflate ? Deflate(raw.ToArray()) : raw.ToArray();
            AvroBinary.WriteLong(output, block.Length);
            AvroBinary.WriteLong(output, data.Length);
            output.Write(data);
            output.Write(sync);
        }
        return output.ToArray();
    }

    public sealed record Contents(AvroSchema Schema, IReadOnlyDictionary<string, string> Metadata, IReadOnlyList<object?> Records);

    public static Contents Read(byte[] bytes) => Corruption.Guard("Avro file", () => ReadUnguarded(bytes));

    private static Contents ReadUnguarded(byte[] bytes)
    {
        using var s = new MemoryStream(bytes);
        var magic = new byte[4];
        s.ReadExactly(magic);
        if (!magic.AsSpan().SequenceEqual(Magic)) throw new FormatException("Not an Avro object container file");
        var rawMeta = (Dictionary<string, object?>)AvroBinary.Read(s, MetaSchema)!;
        var meta = rawMeta.ToDictionary(kv => kv.Key, kv => Encoding.UTF8.GetString((byte[])kv.Value!));
        var sync = new byte[16];
        s.ReadExactly(sync);
        var schema = AvroSchemaJson.Parse(meta.TryGetValue("avro.schema", out var json) ? json : throw new InvalidDataException("Avro file has no avro.schema"));
        var codec = meta.GetValueOrDefault("avro.codec", "null");

        var records = new List<object?>();
        var marker = new byte[16];
        while (s.Position < s.Length)
        {
            var count = AvroBinary.ReadLong(s);
            var size = AvroBinary.ReadLong(s);
            if (count < 0 || size < 0 || size > s.Length - s.Position) throw new InvalidDataException($"Avro block of {size} bytes / {count} records out of range");
            var data = new byte[size];
            s.ReadExactly(data);
            s.ReadExactly(marker);
            if (!marker.AsSpan().SequenceEqual(sync)) throw new FormatException("Avro sync marker mismatch");
            var block = codec switch
            {
                "null" => data,
                "deflate" => Inflate(data),
                _ => throw new NotSupportedException($"Avro codec {codec} (only null and deflate are implemented; Iceberg defaults to deflate)"),
            };
            using var bs = new MemoryStream(block);
            for (var i = 0; i < count; i++) records.Add(AvroBinary.Read(bs, schema));
        }
        return new Contents(schema, meta, records);
    }

    /// <summary>Avro's deflate codec is raw RFC 1951 (no zlib header).</summary>
    private static byte[] Deflate(byte[] data)
    {
        using var ms = new MemoryStream();
        using (var d = new DeflateStream(ms, CompressionLevel.Optimal, leaveOpen: true)) d.Write(data);
        return ms.ToArray();
    }

    private static byte[] Inflate(byte[] data)
    {
        using var input = new MemoryStream(data);
        using var d = new DeflateStream(input, CompressionMode.Decompress);
        using var ms = new MemoryStream();
        d.CopyTo(ms);
        return ms.ToArray();
    }
}
