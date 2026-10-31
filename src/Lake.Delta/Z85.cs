using System.Buffers.Binary;

namespace Lake.Delta;

/// <summary>
/// Z85 (ZeroMQ RFC 32), the Base85 variant Delta uses for deletion vector UUIDs and inline deletion vectors: every 4
/// bytes are one big-endian 32-bit value written as 5 characters, most significant first. Delta pads the input to a
/// multiple of 4 bytes and records the unpadded size (<c>sizeInBytes</c>) beside the text.
/// </summary>
public static class Z85
{
    private const string Alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
    private static readonly sbyte[] Index = BuildIndex();

    private static sbyte[] BuildIndex()
    {
        var index = new sbyte[128];
        Array.Fill(index, (sbyte)-1);
        for (var i = 0; i < Alphabet.Length; i++) index[Alphabet[i]] = (sbyte)i;
        return index;
    }

    /// <summary>Decode Z85 text whose length is a multiple of 5.</summary>
    public static byte[] Decode(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        if (text.Length % 5 != 0) throw new InvalidDataException($"Z85 text length {text.Length} is not a multiple of 5");
        var result = new byte[text.Length / 5 * 4];
        for (int c = 0, b = 0; c < text.Length; c += 5, b += 4)
        {
            ulong value = 0;
            for (var k = 0; k < 5; k++)
            {
                var ch = text[c + k];
                var digit = ch < 128 ? Index[ch] : -1;
                if (digit < 0) throw new InvalidDataException($"Z85 text has an invalid character at {c + k}");
                value = value * 85 + (ulong)digit;
            }
            if (value > uint.MaxValue) throw new InvalidDataException($"Z85 group at {c} exceeds 32 bits");
            BinaryPrimitives.WriteUInt32BigEndian(result.AsSpan(b), (uint)value);
        }
        return result;
    }

    /// <summary>Decode Z85 text that carries <paramref name="size"/> bytes padded to a multiple of 4.</summary>
    public static byte[] Decode(string text, int size)
    {
        if (size < 0) throw new InvalidDataException($"Negative Z85 payload size {size}");
        var padded = Decode(text);
        if (padded.Length < size || padded.Length - size >= 4)
            throw new InvalidDataException($"Z85 text decodes to {padded.Length} bytes, which is not {size} bytes padded to a multiple of 4");
        return padded.AsSpan(0, size).ToArray();
    }

    /// <summary>Encode bytes, zero-padding them to a multiple of 4 (Delta's convention).</summary>
    public static string Encode(ReadOnlySpan<byte> data)
    {
        var padded = new byte[(data.Length + 3) / 4 * 4];
        data.CopyTo(padded);
        var chars = new char[padded.Length / 4 * 5];
        for (int b = 0, c = 0; b < padded.Length; b += 4, c += 5)
        {
            var value = BinaryPrimitives.ReadUInt32BigEndian(padded.AsSpan(b));
            for (var k = 4; k >= 0; k--)
            {
                chars[c + k] = Alphabet[(int)(value % 85)];
                value /= 85;
            }
        }
        return new string(chars);
    }

    /// <summary>The 20-character Z85 form of a UUID (its 16 bytes in canonical order) as canonical UUID text.</summary>
    public static string DecodeUuid(string text)
    {
        if (text.Length != 20) throw new InvalidDataException($"A Z85 UUID is 20 characters, not {text.Length}");
        var hex = Convert.ToHexString(Decode(text)).ToLowerInvariant();
        return $"{hex[..8]}-{hex[8..12]}-{hex[12..16]}-{hex[16..20]}-{hex[20..]}";
    }
}
