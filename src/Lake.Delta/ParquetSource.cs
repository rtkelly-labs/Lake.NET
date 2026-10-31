using System.Globalization;
using System.Text.Json.Nodes;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Iceberg.Net.IO;
using ArrowField = Apache.Arrow.Field;
using ArrowSchema = Apache.Arrow.Schema;

namespace Lake.Delta;

/// <summary>
/// Parquet through ParquetSharp's Arrow reader: it reassembles structs, lists and maps, so data files and checkpoints
/// (whose actions are nested structs) share one decoder. A file is read whole into memory first.
/// </summary>
internal static class ParquetSource
{
    public const string FieldIdKey = "PARQUET:field_id";

    /// <summary>Every record batch of the file at <paramref name="location"/>, in file order.</summary>
    public static async IAsyncEnumerable<(ArrowSchema Schema, RecordBatch Batch)> ReadAsync(
        IFileIO io, string location, string what, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var bytes = await io.ReadAllAsync(location, ct).ConfigureAwait(false);
        using var stream = new MemoryStream(bytes, writable: false);
        ParquetSharp.Arrow.FileReader reader;
        Apache.Arrow.Ipc.IArrowArrayStream batches;
        try
        {
            reader = new ParquetSharp.Arrow.FileReader(stream, leaveOpen: true);
            batches = reader.GetRecordBatchReader();
        }
        catch (Exception e) when (e is not OperationCanceledException and not OutOfMemoryException)
        {
            throw Corruption.Wrap($"{what} {location}", e);
        }
        using (reader)
        using (batches)
        {
            var schema = reader.Schema;
            while (true)
            {
                RecordBatch? batch;
                try
                {
                    batch = await batches.ReadNextRecordBatchAsync(ct).ConfigureAwait(false);
                }
                catch (Exception e) when (e is not OperationCanceledException and not OutOfMemoryException)
                {
                    throw Corruption.Wrap($"{what} {location}", e);
                }
                if (batch is null) yield break;
                using (batch) yield return (schema, batch);
            }
        }
    }

    public static int? FieldId(ArrowField f) =>
        f.Metadata is { } md && md.TryGetValue(FieldIdKey, out var s) && int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var id) ? id : null;

    // ---------- checkpoint rows as log JSON ----------

    /// <summary>A checkpoint row as the JSON object a commit line would hold (absent and null fields omitted).</summary>
    public static JsonObject RowAsJson(RecordBatch batch, ArrowSchema schema, int row)
    {
        var line = new JsonObject();
        for (var c = 0; c < schema.FieldsList.Count; c++)
        {
            var column = batch.Column(c);
            if (column.IsNull(row)) continue;
            var node = ToJson(column, row, schema.FieldsList[c].Name);
            if (node is not null) line[schema.FieldsList[c].Name] = node;
        }
        return line;
    }

    private static JsonNode? ToJson(IArrowArray a, int i, string name)
    {
        if (a.IsNull(i)) return null;
        switch (a)
        {
            case StructArray s:
            {
                var type = (StructType)s.Data.DataType;
                var o = new JsonObject();
                JsonObject? statsParsed = null;
                for (var k = 0; k < type.Fields.Count; k++)
                {
                    var child = ToJson(s.Fields[k], i + s.Offset, type.Fields[k].Name);
                    if (child is null) continue;
                    if (type.Fields[k].Name == "stats_parsed") statsParsed = child as JsonObject;
                    else if (type.Fields[k].Name != "partitionValues_parsed") o[type.Fields[k].Name] = child;
                }
                // writeStatsAsStruct without writeStatsAsJson: render the struct back to the JSON string readers expect.
                if (statsParsed is not null && !o.ContainsKey("stats")) o["stats"] = statsParsed.ToJsonString();
                return o;
            }
            case MapArray m:
            {
                var o = new JsonObject();
                var start = m.ValueOffsets[i];
                var end = m.ValueOffsets[i + 1];
                for (var j = start; j < end; j++)
                {
                    var key = ToJson(m.Keys, j, name + ".key") ?? throw new InvalidDataException($"{name}: null map key");
                    o[key.GetValueKind() == System.Text.Json.JsonValueKind.String ? key.GetValue<string>() : key.ToJsonString()] = ToJson(m.Values, j, name + ".value");
                }
                return o;
            }
            case ListArray l:
            {
                var arr = new JsonArray();
                var start = l.ValueOffsets[i];
                var end = l.ValueOffsets[i + 1];
                for (var j = start; j < end; j++) arr.Add(ToJson(l.Values, j, name + ".element"));
                return arr;
            }
            default:
                return Primitive(a, i) switch
                {
                    null => null,
                    string s => JsonValue.Create(s),
                    bool b => JsonValue.Create(b),
                    sbyte v => JsonValue.Create((long)v),
                    short v => JsonValue.Create((long)v),
                    int v => JsonValue.Create((long)v),
                    long v => JsonValue.Create(v),
                    float v => JsonValue.Create((double)v),
                    double v => JsonValue.Create(v),
                    decimal v => JsonValue.Create(v),
                    DateOnly d => JsonValue.Create(d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)),
                    DateTimeOffset t => JsonValue.Create(t.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture)),
                    byte[] bytes => JsonValue.Create(Convert.ToBase64String(bytes)),
                    var other => throw new InvalidDataException($"{name}: unexpected value {other.GetType().Name}"),
                };
        }
    }

    // ---------- primitive values ----------

    /// <summary>The value at <paramref name="i"/> of a non-nested array, as its natural CLR type (null when null).</summary>
    public static object? Primitive(IArrowArray a, int i)
    {
        if (a.IsNull(i)) return null;
        return a switch
        {
            BooleanArray x => x.GetValue(i)!.Value,
            Int8Array x => x.GetValue(i)!.Value,
            Int16Array x => x.GetValue(i)!.Value,
            Int32Array x => x.GetValue(i)!.Value,
            Int64Array x => x.GetValue(i)!.Value,
            UInt8Array x => (short)x.GetValue(i)!.Value,
            UInt16Array x => (int)x.GetValue(i)!.Value,
            UInt32Array x => (long)x.GetValue(i)!.Value,
            FloatArray x => x.GetValue(i)!.Value,
            DoubleArray x => x.GetValue(i)!.Value,
            StringArray x => x.GetString(i),
            LargeStringArray x => x.GetString(i),
            BinaryArray x => x.GetBytes(i).ToArray(),
            LargeBinaryArray x => x.GetBytes(i).ToArray(),
            Decimal128Array x => Decimal(() => x.GetValue(i)!.Value),
            Decimal256Array x => Decimal(() => x.GetValue(i)!.Value),
            FixedSizeBinaryArray x => x.GetBytes(i).ToArray(),
            Date32Array x => x.GetDateOnly(i)!.Value,
            Date64Array x => x.GetDateOnly(i)!.Value,
            TimestampArray x => x.GetTimestamp(i)!.Value,
            NullArray => null,
            _ => throw new InvalidDataException($"Unsupported Parquet column type {a.Data.DataType.Name}"),
        };
    }

    private static decimal Decimal(Func<decimal> read)
    {
        try
        {
            return read();
        }
        catch (OverflowException e)
        {
            throw new DeltaUnsupportedFeatureException($"A decimal value exceeds System.Decimal's range (28-29 significant digits): {e.Message}", ["decimal(38) values beyond System.Decimal"]);
        }
    }
}
