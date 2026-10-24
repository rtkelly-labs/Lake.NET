using System.Buffers.Binary;
using System.Numerics;
using System.Text;

namespace Iceberg.Net.Types;

/// <summary>
/// Single-value binary serialisation (spec Appendix D), used for column lower/upper bounds in manifests
/// and partition field summaries in manifest lists.
/// </summary>
public static class Conversions
{
    public static byte[] ToBytes(IcebergType type, object value)
    {
        switch (type.TypeId, value)
        {
            case (TypeId.Boolean, bool b): return [(byte)(b ? 1 : 0)];
            case (TypeId.Int or TypeId.Date, int i):
            {
                var buf = new byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(buf, i);
                return buf;
            }
            case (TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs, long l):
            {
                var buf = new byte[8];
                BinaryPrimitives.WriteInt64LittleEndian(buf, l);
                return buf;
            }
            case (TypeId.Float, float f):
            {
                var buf = new byte[4];
                Compat.WriteSingle(buf, f);
                return buf;
            }
            case (TypeId.Double, double d):
            {
                var buf = new byte[8];
                Compat.WriteDouble(buf, d);
                return buf;
            }
            case (TypeId.String, string s): return Encoding.UTF8.GetBytes(s);
            case (TypeId.Uuid, Guid g): return g.ToByteArray(bigEndian: true);
            case (TypeId.Fixed or TypeId.Binary, byte[] b): return b;
            case (TypeId.Decimal, IcebergDecimal d): return d.Unscaled.ToByteArray(isUnsigned: false, isBigEndian: true);
            default: throw new ArgumentException($"Cannot serialise {value.GetType().Name} as {type}");
        }
    }

    /// <summary>
    /// Decode a single-value serialisation. The bytes come from manifests, so a length that does not fit the type is
    /// an <see cref="InvalidDataException"/>, never an index or range exception.
    /// </summary>
    public static object FromBytes(IcebergType type, ReadOnlySpan<byte> bytes) => type switch
    {
        { TypeId: TypeId.Boolean } => Sized(type, bytes, 1)[0] != 0,
        { TypeId: TypeId.Int or TypeId.Date } => BinaryPrimitives.ReadInt32LittleEndian(Sized(type, bytes, 4)),
        // A v3 date->timestamp promotion can leave 4-byte bounds on a long column.
        { TypeId: TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs } =>
            bytes.Length == 4 ? (long)BinaryPrimitives.ReadInt32LittleEndian(bytes) : BinaryPrimitives.ReadInt64LittleEndian(Sized(type, bytes, 8)),
        { TypeId: TypeId.Float } => Compat.ReadSingle(Sized(type, bytes, 4)),
        { TypeId: TypeId.Double } => bytes.Length == 4 ? (double)Compat.ReadSingle(bytes) : Compat.ReadDouble(Sized(type, bytes, 8)),
        { TypeId: TypeId.String } => Encoding.UTF8.GetString(bytes.ToArray()),
        { TypeId: TypeId.Uuid } => Compat.GuidFromBigEndian(Sized(type, bytes, 16)),
        { TypeId: TypeId.Fixed or TypeId.Binary } => bytes.ToArray(),
        DecimalType d => new IcebergDecimal(Compat.SignedBigEndian(bytes), d.Scale),
        _ => throw new NotSupportedException($"Cannot deserialise bound for {type}"),
    };

    private static ReadOnlySpan<byte> Sized(IcebergType type, ReadOnlySpan<byte> bytes, int length) =>
        bytes.Length == length ? bytes : throw new InvalidDataException($"A {type} value takes {length} bytes, not {bytes.Length}");
}
