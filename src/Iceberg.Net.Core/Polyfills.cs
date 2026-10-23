#if !NET6_0_OR_GREATER
// netstandard2.0 lacks these instance-style members; as extensions under their BCL namespaces they keep call sites
// identical to .NET 8 (where these files compile to nothing).
namespace System.Collections.Generic
{
    internal static class KeyValuePair
    {
        public static KeyValuePair<TKey, TValue> Create<TKey, TValue>(TKey key, TValue value) => new(key, value);
    }

    internal static class CollectionPolyfills
    {
        public static void Deconstruct<TKey, TValue>(this KeyValuePair<TKey, TValue> kv, out TKey key, out TValue value)
        {
            key = kv.Key;
            value = kv.Value;
        }

        public static TValue? GetValueOrDefault<TKey, TValue>(this IReadOnlyDictionary<TKey, TValue> d, TKey key) =>
            d.TryGetValue(key, out var v) ? v : default;

        public static TValue GetValueOrDefault<TKey, TValue>(this IReadOnlyDictionary<TKey, TValue> d, TKey key, TValue fallback) =>
            d.TryGetValue(key, out var v) ? v : fallback;

        public static bool TryAdd<TKey, TValue>(this Dictionary<TKey, TValue> d, TKey key, TValue value) where TKey : notnull
        {
            if (d.ContainsKey(key)) return false;
            d.Add(key, value);
            return true;
        }
    }
}

namespace System.Linq
{
    internal static class LinqPolyfills
    {
        public static HashSet<T> ToHashSet<T>(this IEnumerable<T> source) => new(source);

        public static IEnumerable<T> TakeLast<T>(this IEnumerable<T> source, int count)
        {
            var all = source.ToList();
            return all.Skip(Math.Max(0, all.Count - count));
        }

        public static IEnumerable<T[]> Chunk<T>(this IEnumerable<T> source, int size)
        {
            var chunk = new List<T>(size);
            foreach (var item in source)
            {
                chunk.Add(item);
                if (chunk.Count == size) { yield return chunk.ToArray(); chunk.Clear(); }
            }
            if (chunk.Count > 0) yield return chunk.ToArray();
        }

        public static IEnumerable<(TFirst First, TSecond Second)> Zip<TFirst, TSecond>(this IEnumerable<TFirst> first, IEnumerable<TSecond> second) =>
            first.Zip(second, (a, b) => (a, b));
    }
}

namespace System.IO
{
    internal static class StreamPolyfills
    {
        public static void Write(this Stream s, ReadOnlySpan<byte> bytes) => s.Write(bytes.ToArray(), 0, bytes.Length);

        public static void ReadExactly(this Stream s, Span<byte> buffer)
        {
            var tmp = new byte[buffer.Length];
            var read = 0;
            while (read < tmp.Length)
            {
                var n = s.Read(tmp, read, tmp.Length - read);
                if (n == 0) throw new EndOfStreamException();
                read += n;
            }
            tmp.CopyTo(buffer);
        }
    }
}

namespace System
{
    internal static class SystemPolyfills
    {
        /// <summary>RFC 4122 byte order (big-endian fields), as .NET 8's <c>ToByteArray(bigEndian: true)</c>.</summary>
        public static byte[] ToByteArray(this Guid g, bool bigEndian)
        {
            var b = g.ToByteArray();
            if (!bigEndian) return b;
            Array.Reverse(b, 0, 4);
            Array.Reverse(b, 4, 2);
            Array.Reverse(b, 6, 2);
            return b;
        }

        public static byte[] ToByteArray(this Numerics.BigInteger v, bool isUnsigned, bool isBigEndian)
        {
            if (isUnsigned) throw new NotSupportedException("Unsigned BigInteger bytes are not used by this library");
            var b = v.ToByteArray(); // minimal two's complement, little-endian
            if (isBigEndian) Array.Reverse(b);
            return b;
        }

        public static bool EndsWith(this string s, char c) => s.Length > 0 && s[s.Length - 1] == c;
        public static bool StartsWith(this string s, char c) => s.Length > 0 && s[0] == c;

        /// <summary>The string's Unicode code points (surrogate pairs kept together), each as a string.</summary>
        public static IEnumerable<string> EnumerateRunes(this string s)
        {
            for (var i = 0; i < s.Length; i++)
            {
                if (char.IsHighSurrogate(s[i]) && i + 1 < s.Length && char.IsLowSurrogate(s[i + 1]))
                {
                    yield return s.Substring(i, 2);
                    i++;
                }
                else yield return s[i].ToString();
            }
        }
    }
}
#endif
