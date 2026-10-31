using System.Text.Json;

namespace Lake.Delta;

/// <summary>
/// The table needs something this reader does not implement: a reader feature (listed in
/// <see cref="Features"/>), a reader version above 3, or a type change it cannot convert. Raised before any data is read.
/// </summary>
public sealed class DeltaUnsupportedFeatureException(string message, IReadOnlyList<string> features) : NotSupportedException(message)
{
    public IReadOnlyList<string> Features { get; } = features;
}

/// <summary>The requested version (or timestamp) cannot be reconstructed from the log that is still there.</summary>
public sealed class DeltaVersionNotFoundException(string message) : KeyNotFoundException(message);

/// <summary>
/// A Delta log, checkpoint, deletion vector or data file that does not decode (malformed JSON, a missing required field,
/// an impossible length) fails with <see cref="InvalidDataException"/>, as every decoder of untrusted bytes in this
/// repository does (the <c>Corruption</c> contract in AGENTS.md). This turns any other exception thrown while decoding
/// into one, so a corrupt table never surfaces as a programming error.
/// </summary>
internal static class Corruption
{
    private static bool PassesThrough(Exception e) =>
        e is InvalidDataException or DeltaUnsupportedFeatureException or DeltaVersionNotFoundException
            or OperationCanceledException or OutOfMemoryException or FileNotFoundException or DirectoryNotFoundException
            || (e is IOException && e is not EndOfStreamException);

    public static InvalidDataException Wrap(string what, Exception e) => e switch
    {
        JsonException j => new InvalidDataException($"Corrupt {what}: malformed JSON: {j.Message}", j),
        _ => new InvalidDataException($"Corrupt {what}: {e.GetType().Name}: {e.Message}", e),
    };

    public static T Guard<T>(string what, Func<T> decode)
    {
        try
        {
            return decode();
        }
        catch (Exception e) when (!PassesThrough(e))
        {
            throw Wrap(what, e);
        }
    }

    public static async Task<T> GuardAsync<T>(string what, Func<Task<T>> decode)
    {
        try
        {
            return await decode().ConfigureAwait(false);
        }
        catch (Exception e) when (!PassesThrough(e))
        {
            throw Wrap(what, e);
        }
    }
}
