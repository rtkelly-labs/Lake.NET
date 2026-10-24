using System.Text.Json;

namespace Iceberg.Net;

/// <summary>
/// The decoder contract for untrusted bytes (metadata JSON, manifests, Puffin): bad input fails with a format error —
/// <see cref="FormatException"/>, <see cref="InvalidDataException"/>, <see cref="EndOfStreamException"/>,
/// <see cref="NotSupportedException"/> or <see cref="JsonException"/> — never a programming-error exception.
/// Lengths and counts are bounds-checked before they drive an allocation; <see cref="Guard{T}"/> at each public entry
/// point turns any remaining low-level failure (a cast, a missing key, a slice past the end) into
/// <see cref="InvalidDataException"/> naming what was being decoded, with the original as the inner exception.
/// </summary>
public static class Corruption
{
    public static bool IsFormatError(Exception e) =>
        e is FormatException or InvalidDataException or EndOfStreamException or NotSupportedException or JsonException;

    /// <summary>
    /// Read a decompressing stream to the end, refusing output beyond <paramref name="maxBytes"/>: a decompression bomb,
    /// or a block larger than any writer produces, is a format error rather than an allocation of whatever it claims.
    /// </summary>
    public static byte[] ReadAtMost(Stream source, int maxBytes, string what)
    {
        using var ms = new MemoryStream();
        var buffer = new byte[81920];
        int n;
        while ((n = source.Read(buffer, 0, buffer.Length)) > 0)
        {
            if (ms.Length + n > maxBytes) throw new InvalidDataException($"{what} decompresses to more than {maxBytes} bytes");
            ms.Write(buffer, 0, n);
        }
        return ms.ToArray();
    }

    public static T Guard<T>(string what, Func<T> decode)
    {
        try
        {
            return decode();
        }
        catch (Exception e) when (!IsFormatError(e) && e is not OperationCanceledException and not OutOfMemoryException)
        {
            throw new InvalidDataException($"Corrupt {what}: {e.GetType().Name}: {e.Message}", e);
        }
    }
}
