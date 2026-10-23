using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>
/// Order-independent content hash of a table snapshot, for shadow comparison with another engine without moving
/// rows: per column the value count, null count and the sum (mod 2^64) of a hash of each value's canonical text, plus
/// the same over whole rows (so values swapped between rows are caught). Sums, not XOR, so duplicates count.
/// The canonical text per type is shared with interop/pyiceberg_oracle.py <c>fingerprint</c>; keep them in step.
/// Reads one data file at a time, so memory is bounded by the largest file, not the table.
/// </summary>
public sealed record TableFingerprint(long Rows, ulong RowHash, IReadOnlyDictionary<int, ColumnFingerprint> Columns)
{
    public static async Task<TableFingerprint> ComputeAsync(Table table, long? snapshotId = null, CancellationToken ct = default)
    {
        var schema = table.Schema;
        var fields = schema.Columns.ToList();
        var deletes = await TableScan.DeletesAsync(table, snapshotId, ct);
        var mapping = NameMapping.ForTable(table.Metadata.Properties);
        var counts = new long[fields.Count];
        var nulls = new long[fields.Count];
        var hashes = new ulong[fields.Count];
        long rows = 0;
        ulong rowHash = 0;
        var parts = new string[fields.Count];
        foreach (var task in await TableScan.PlanFilesAsync(table, snapshotId, null, ct))
        {
            var fileRows = await ParquetFiles.ReadAsync(table.IO, task.File.FilePath, schema, mapping, ct);
            var isDeleted = deletes.IsEmpty ? null : await deletes.RowFilterAsync(task.File, task.DataSequenceNumber, schema, ct);
            for (var pos = 0; pos < fileRows.Count; pos++)
            {
                var row = fileRows[pos];
                if (isDeleted?.Invoke(pos, row) == true) continue;
                for (var c = 0; c < fields.Count; c++)
                {
                    counts[c]++;
                    if (row[c] is null)
                    {
                        nulls[c]++;
                        parts[c] = "\0";
                        continue;
                    }
                    parts[c] = Canonical(fields[c].Type, row[c]!);
                    hashes[c] += Hash(parts[c]);
                }
                rowHash += Hash(string.Join('\u001f', parts));
                rows++;
            }
        }
        return new TableFingerprint(rows, rowHash,
            fields.Select((f, c) => (f.Id, new ColumnFingerprint(f.Name, counts[c], nulls[c], hashes[c]))).ToDictionary(x => x.Id, x => x.Item2));
    }

    /// <summary>Canonical text: integers for integral and temporal types (days/micros/nanos), IEEE-754 big-endian bit
    /// patterns for floats ("NaN" for any NaN), unscaled integers for decimals, hex for binary, RFC 4122 text for UUIDs.</summary>
    /// <remarks>Nested values: structs <c>(a,b)</c> in field order, lists <c>[a,b]</c>, maps <c>{k=v,…}</c> sorted by
    /// canonical key (ordinal); a nested null is <c>\0</c>.</remarks>
    public static string Canonical(IcebergType type, object value) => type switch
    {
        StructType st => "(" + string.Join(",", st.Fields.Select((f, i) => Nested(f.Type, ((object?[])value)[i]))) + ")",
        ListType lt => "[" + string.Join(",", ((System.Collections.IEnumerable)value).Cast<object?>().Select(e => Nested(lt.ElementType, e))) + "]",
        MapType mt => "{" + string.Join(",", ((System.Collections.IDictionary)value).Keys.Cast<object>()
            .Select(k => (K: Canonical(mt.KeyType, k), V: Nested(mt.ValueType, ((System.Collections.IDictionary)value)[k])))
            .OrderBy(x => x.K, StringComparer.Ordinal).Select(x => $"{x.K}={x.V}")) + "}",
        _ => CanonicalPrimitive(type, value),
    };

    private static string Nested(IcebergType type, object? value) => value is null ? "\u0000" : Canonical(type, value);

    private static string CanonicalPrimitive(IcebergType type, object value) => (((PrimitiveType)type).TypeId, value) switch
    {
        (TypeId.Boolean, bool b) => b ? "true" : "false",
        // Hex of the bit pattern as an integer == hex of its big-endian bytes (Python: struct.pack(">f", v).hex()).
        (TypeId.Float, float f) => float.IsNaN(f) ? "NaN" : BitConverter.SingleToUInt32Bits(f).ToString("x8", CultureInfo.InvariantCulture),
        (TypeId.Double, double d) => double.IsNaN(d) ? "NaN" : BitConverter.DoubleToUInt64Bits(d).ToString("x16", CultureInfo.InvariantCulture),
        (TypeId.Decimal, IcebergDecimal m) => Rescale(m, ((DecimalType)type).Scale).ToString(CultureInfo.InvariantCulture),
        (TypeId.Decimal, decimal m) => IcebergDecimal.FromDecimal(m, ((DecimalType)type).Scale).Unscaled.ToString(CultureInfo.InvariantCulture),
        (TypeId.Uuid, Guid g) => g.ToString("D"),
        (TypeId.Binary or TypeId.Fixed, byte[] b) => Convert.ToHexString(b).ToLowerInvariant(),
        (_, string s) => s,
        (_, IFormattable n) => n.ToString(null, CultureInfo.InvariantCulture),
        _ => throw new NotSupportedException($"No canonical form for {value.GetType().Name} in a {type} column"),
    };

    private static BigInteger Rescale(IcebergDecimal m, int scale) =>
        m.Scale == scale ? m.Unscaled
        : m.Scale < scale ? m.Unscaled * BigInteger.Pow(10, scale - m.Scale)
        : throw new InvalidDataException($"Decimal {m} has more scale than its column ({scale})");

    private static ulong Hash(string text) => BinaryPrimitives.ReadUInt64LittleEndian(SHA256.HashData(Encoding.UTF8.GetBytes(text)));

    /// <summary>Parse the JSON printed by <c>pyiceberg_oracle.py fingerprint</c>.</summary>
    public static TableFingerprint FromJson(string json)
    {
        var n = JsonNode.Parse(json)!;
        return new TableFingerprint(
            n["rows"]!.GetValue<long>(),
            Convert.ToUInt64(n["row_hash"]!.GetValue<string>(), 16),
            n["columns"]!.AsObject().ToDictionary(kv => int.Parse(kv.Key, CultureInfo.InvariantCulture), kv => new ColumnFingerprint(
                kv.Value!["name"]!.GetValue<string>(), kv.Value["count"]!.GetValue<long>(), kv.Value["nulls"]!.GetValue<long>(),
                Convert.ToUInt64(kv.Value["hash"]!.GetValue<string>(), 16))));
    }

    /// <summary>Human-readable differences; empty when the fingerprints agree.</summary>
    public IReadOnlyList<string> DifferencesFrom(TableFingerprint other)
    {
        var diffs = new List<string>();
        if (Rows != other.Rows) diffs.Add($"row count {Rows} vs {other.Rows}");
        foreach (var (id, mine) in Columns)
        {
            if (!other.Columns.TryGetValue(id, out var theirs)) { diffs.Add($"column {id} ({mine.Name}) missing from the reference"); continue; }
            if (mine.Nulls != theirs.Nulls) diffs.Add($"{mine.Name}: {mine.Nulls} nulls vs {theirs.Nulls}");
            if (mine.Hash != theirs.Hash) diffs.Add($"{mine.Name}: value hash {mine.Hash:x16} vs {theirs.Hash:x16}");
        }
        if (RowHash != other.RowHash && diffs.Count == 0) diffs.Add($"row hash {RowHash:x16} vs {other.RowHash:x16} (same column contents, different rows)");
        return diffs;
    }
}

public sealed record ColumnFingerprint(string Name, long Count, long Nulls, ulong Hash);
