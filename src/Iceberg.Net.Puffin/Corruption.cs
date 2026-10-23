using System.Text.Json;

namespace Iceberg.Net.Puffin;

/// <summary>
/// Same decoder contract as <c>Iceberg.Net.Corruption</c> (this assembly deliberately has no dependency on Core):
/// corrupt input surfaces as a format error, never a programming-error exception.
/// </summary>
internal static class Corruption
{
    private static bool IsFormatError(Exception e) =>
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

    public delegate T SpanDecoder<T>(ReadOnlySpan<byte> data);

    public static T Guard<T>(string what, ReadOnlySpan<byte> data, SpanDecoder<T> decode)
    {
        try
        {
            return decode(data);
        }
        catch (Exception e) when (!IsFormatError(e) && e is not OperationCanceledException and not OutOfMemoryException)
        {
            throw new InvalidDataException($"Corrupt {what}: {e.GetType().Name}: {e.Message}", e);
        }
    }
}
