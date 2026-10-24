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
    /// <summary>
    /// The largest Avro block, compressed or inflated, that a reader accepts. Writers flush blocks of tens of kilobytes to
    /// a few megabytes; a claim beyond this is corrupt or hostile and must not size an allocation.
    /// </summary>
    public const int MaxBlockBytes = 128 * 1024 * 1024;

    /// <summary>Records allowed in one block of a schema that encodes in zero bytes (Iceberg never writes one).</summary>
    public const int MaxZeroWidthRecords = 1 << 20;

    /// <summary>
    /// Records allowed in any one block. Writers flush blocks by size (Java's sync interval is tens of kilobytes; this
    /// library's writer flushes at <see cref="WriterFlushBytes"/> or 1,000 records), so millions of tiny records in one
    /// block is hostile: each decoded record is an allocation.
    /// </summary>
    public const int MaxRecordsPerBlock = 1 << 22;

    /// <summary>Encoded bytes after which the container writer starts a new block, well under <see cref="MaxBlockBytes"/>.</summary>
    public const int WriterFlushBytes = 16 * 1024 * 1024;

    /// <summary>Inflate a <c>deflate</c> block, refusing output beyond <see cref="MaxBlockBytes"/>.</summary>
    public static byte[] Inflate(byte[] data)
    {
        using var input = new MemoryStream(data);
        using var d = new DeflateStream(input, CompressionMode.Decompress);
        return Corruption.ReadAtMost(d, MaxBlockBytes, "Avro deflate block");
    }

    /// <summary>
    /// Refuse a block that claims more records than its bytes can hold. Every record of a schema with a non-empty
    /// encoding takes at least one byte; a zero-width schema (<c>null</c>, records of nothing but those) is capped at
    /// <see cref="MaxZeroWidthRecords"/>, so a count cannot spin through billions of empty reads.
    /// </summary>
    public static void CheckRecordCount(long count, int blockBytes, AvroSchema schema)
    {
        if (count > MaxRecordsPerBlock) throw new InvalidDataException($"Avro block count {count} exceeds the limit of {MaxRecordsPerBlock} records");
        var limit = MinEncodedBytes(schema) > 0 ? blockBytes : MaxZeroWidthRecords;
        if (count > limit) throw new InvalidDataException($"Avro block count {count} exceeds what its {blockBytes} bytes can hold");
    }

    /// <summary>
    /// The fewest bytes one value of <paramref name="schema"/> can encode to. Named types are shared by reference, so each
    /// schema node is measured once: a chain of records that each use the previous one twice stays linear.
    /// </summary>
    public static long MinEncodedBytes(AvroSchema schema) => MinEncodedBytes(schema, new Dictionary<AvroSchema, long>(ByReference.Instance));

    /// <summary>Schema nodes by identity (records compare by value, which would revisit shared named types).</summary>
    private sealed class ByReference : IEqualityComparer<AvroSchema>
    {
        public static readonly ByReference Instance = new();
        public bool Equals(AvroSchema? x, AvroSchema? y) => ReferenceEquals(x, y);
        public int GetHashCode(AvroSchema obj) => System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj);
    }

    private static long MinEncodedBytes(AvroSchema schema, Dictionary<AvroSchema, long> seen)
    {
        if (seen.TryGetValue(schema, out var known)) return known;
        var width = schema switch
        {
            AvroPrimitive { Kind: AvroKind.Null } => 0,
            AvroRecord r => r.Fields.Aggregate(0L, (sum, f) => sum + MinEncodedBytes(f.Schema, seen)),
            AvroFixed f => f.Size,
            _ => 1L, // every other primitive, and arrays, maps and unions (a count or branch varint)
        };
        seen[schema] = width;
        return width;
    }

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
                Compat.WriteSingle(buf, Convert.ToSingle(value));
                s.Write(buf[..4]);
                break;
            case AvroKind.Double:
                Compat.WriteDouble(buf, Convert.ToDouble(value));
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
                if (f.Size < 0 || f.Size > MaxBlockBytes || (s.CanSeek && f.Size > s.Length - s.Position))
                    throw new InvalidDataException($"Avro fixed of {f.Size} bytes exceeds the remaining {(s.CanSeek ? s.Length - s.Position : 0)}");
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
                return Compat.ReadSingle(buf);
            case AvroKind.Double:
                s.ReadExactly(buf);
                return Compat.ReadDouble(buf);
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
        var sync = Compat.RandomBytes(16);
        output.Write(sync);

        // A block ends at blockSize records or AvroBinary.WriterFlushBytes encoded bytes, so every block stays within what
        // the readers accept (AvroBinary.MaxBlockBytes, MaxRecordsPerBlock); a single record over the limit is refused.
        using var raw = new MemoryStream();
        var count = 0;
        void Flush()
        {
            if (count == 0) return;
            var data = codec == AvroCodecKind.Deflate ? Deflate(raw.ToArray()) : raw.ToArray();
            AvroBinary.WriteLong(output, count);
            AvroBinary.WriteLong(output, data.Length);
            output.Write(data);
            output.Write(sync);
            raw.SetLength(0);
            count = 0;
        }
        foreach (var r in records)
        {
            var start = raw.Length;
            AvroBinary.Write(raw, schema, r);
            if (raw.Length - start > AvroBinary.MaxBlockBytes)
                throw new InvalidOperationException($"An Avro record of {raw.Length - start} bytes exceeds the {AvroBinary.MaxBlockBytes}-byte block limit readers accept");
            count++;
            if (count >= blockSize || raw.Length >= AvroBinary.WriterFlushBytes) Flush();
        }
        Flush();
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
            if (count < 0 || size < 0 || size > s.Length - s.Position || size > AvroBinary.MaxBlockBytes)
                throw new InvalidDataException($"Avro block of {size} bytes / {count} records out of range");
            var data = new byte[size];
            s.ReadExactly(data);
            s.ReadExactly(marker);
            if (!marker.AsSpan().SequenceEqual(sync)) throw new FormatException("Avro sync marker mismatch");
            var block = codec switch
            {
                "null" => data,
                "deflate" => AvroBinary.Inflate(data),
                _ => throw new NotSupportedException($"Avro codec {codec} (only null and deflate are implemented; Iceberg defaults to deflate)"),
            };
            AvroBinary.CheckRecordCount(count, block.Length, schema);
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

}
