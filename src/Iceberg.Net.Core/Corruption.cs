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
