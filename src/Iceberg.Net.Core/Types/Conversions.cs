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
                BinaryPrimitives.WriteSingleLittleEndian(buf, f);
                return buf;
            }
            case (TypeId.Double, double d):
            {
                var buf = new byte[8];
                BinaryPrimitives.WriteDoubleLittleEndian(buf, d);
                return buf;
            }
            case (TypeId.String, string s): return Encoding.UTF8.GetBytes(s);
            case (TypeId.Uuid, Guid g): return g.ToByteArray(bigEndian: true);
            case (TypeId.Fixed or TypeId.Binary, byte[] b): return b;
            case (TypeId.Decimal, IcebergDecimal d): return d.Unscaled.ToByteArray(isUnsigned: false, isBigEndian: true);
            default: throw new ArgumentException($"Cannot serialise {value.GetType().Name} as {type}");
        }
    }

    public static object FromBytes(IcebergType type, ReadOnlySpan<byte> bytes) => type switch
    {
        { TypeId: TypeId.Boolean } => bytes[0] != 0,
        { TypeId: TypeId.Int or TypeId.Date } => BinaryPrimitives.ReadInt32LittleEndian(bytes),
        // A v3 date->timestamp promotion can leave 4-byte bounds on a long column.
        { TypeId: TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs } =>
            bytes.Length == 4 ? (long)BinaryPrimitives.ReadInt32LittleEndian(bytes) : BinaryPrimitives.ReadInt64LittleEndian(bytes),
        { TypeId: TypeId.Float } => BinaryPrimitives.ReadSingleLittleEndian(bytes),
        { TypeId: TypeId.Double } => bytes.Length == 4 ? (double)BinaryPrimitives.ReadSingleLittleEndian(bytes) : BinaryPrimitives.ReadDoubleLittleEndian(bytes),
        { TypeId: TypeId.String } => Encoding.UTF8.GetString(bytes),
        { TypeId: TypeId.Uuid } => new Guid(bytes, bigEndian: true),
        { TypeId: TypeId.Fixed or TypeId.Binary } => bytes.ToArray(),
        DecimalType d => new IcebergDecimal(new BigInteger(bytes, isUnsigned: false, isBigEndian: true), d.Scale),
        _ => throw new NotSupportedException($"Cannot deserialise bound for {type}"),
    };
}
