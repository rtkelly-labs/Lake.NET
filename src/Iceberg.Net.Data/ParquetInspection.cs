using Iceberg.Net.IO;
using Iceberg.Net.Manifests;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>The shape of a Parquet field, as an engine registering an existing file needs to map it by name.</summary>
public enum ParquetFieldKind { Primitive, Struct, List, Map }

/// <summary>
/// One field of an existing Parquet file: its name, field ID (if the writer set one) and children. A list has one child
/// (the element, named as the file's repeated group: <c>list</c> for three-level lists, the element itself for two-level
/// ones); a map has two (<c>key</c>, <c>value</c>).
/// </summary>
public sealed record ParquetField(string Name, int? FieldId, ParquetFieldKind Kind, IReadOnlyList<ParquetField> Children)
{
    /// <summary>The Parquet type, for messages: <c>INT64</c>, <c>BYTE_ARRAY (String)</c>, …; null for groups.</summary>
    public string? PhysicalType { get; init; }
}

/// <summary>
/// What an existing Parquet file holds, read from its footer only: row count, sizes, top-level fields, and (for an
/// Iceberg projection) per field ID whether the file's column can be read as that type, plus its statistics.
/// </summary>
public sealed record ParquetFileFacts(long RecordCount, long FileSize, long FooterSize, int RowGroups, IReadOnlyList<ParquetField> Fields)
{
    /// <summary>Statistics of each primitive projection field found in the file (by field ID): sizes, counts and footer bounds.</summary>
    public IReadOnlyDictionary<int, ColumnStats> Statistics { get; init; } = new Dictionary<int, ColumnStats>();
    /// <summary>Projection fields found in the file whose Parquet type cannot be read as their Iceberg type, with the file's type.</summary>
    public IReadOnlyDictionary<int, string> Incompatible { get; init; } = new Dictionary<int, string>();
    /// <summary>Projection fields (top-level or nested) the file has no field for.</summary>
    public IReadOnlyList<int> Missing { get; init; } = [];
    /// <summary>Top-level file fields no projection field matched.</summary>
    public IReadOnlyList<string> Extra { get; init; } = [];
}

public static class ParquetInspection
{
    /// <summary>
    /// Read an existing file's footer. With <paramref name="projection"/>, its fields are matched to the file's by name
    /// (struct fields by name, list elements and map keys and values by position), ignoring field IDs, as a name mapping
    /// reads them (<see cref="DataFileReadOptions.MatchByName"/>).
    /// </summary>
    public static async Task<ParquetFileFacts> InspectAsync(IFileIO io, string location, Schema? projection = null, CancellationToken ct = default)
    {
        var length = await io.LengthAsync(location, ct);
        if (length < 12) throw new InvalidDataException($"{location} is not a Parquet file ({length} bytes)");
        var stream = new RangeReadStream(io, location, length, ct);
        await stream.PrefetchAsync(Math.Max(0, length - ParquetFiles.FooterPrefetch), length, ct);
        var tail = new byte[8];
        stream.Position = length - 8;
        stream.ReadExactly(tail);
        if (tail[4] != 'P' || tail[5] != 'A' || tail[6] != 'R' || tail[7] != '1') throw new InvalidDataException($"{location} is not a Parquet file (no PAR1 trailer)");
        var footer = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(tail);
        stream.Position = 0;
        using var reader = await ParquetFiles.Backend.OpenAsync(stream, null, ct);
        var schema = reader.Schema;
        var column = 0;
        var tops = new List<(PqNode Node, ParquetField Field, int FirstColumn)>();
        foreach (var node in schema.Children)
        {
            var first = column;
            tops.Add((node, Describe(node, ref column), first));
        }
        var facts = new ParquetFileFacts(reader.NumRows, length, footer, reader.RowGroups, tops.Select(t => t.Field).ToList());
        if (projection is null) return facts;

        var stats = new Dictionary<int, ColumnStats>();
        var incompatible = new Dictionary<int, string>();
        var missing = new List<int>();
        void Match(NestedField f, PqNode? node, ref int leaf)
        {
            if (node is null)
            {
                Missing(f.Type, f.Id);
                return;
            }
            switch (f.Type)
            {
                case StructType st when node.IsGroup && KindOf(node) == ParquetFieldKind.Struct:
                {
                    var start = leaf;
                    var offsets = new Dictionary<string, (PqNode Node, int Leaf)>(StringComparer.Ordinal);
                    foreach (var c in node.Children)
                    {
                        offsets.TryAdd(c.Name, (c, start));
                        start += c.LeafCount;
                    }
                    foreach (var child in st.Fields)
                    {
                        if (offsets.TryGetValue(child.Name, out var at))
                        {
                            var l = at.Leaf;
                            Match(child, at.Node, ref l);
                        }
                        else Missing(child.Type, child.Id);
                    }
                    leaf = start;
                    return;
                }
                case ListType lt when node.IsGroup && KindOf(node) == ParquetFieldKind.List:
                {
                    var element = ElementOf(node);
                    Match(new NestedField(lt.ElementId, "element", lt.ElementType, lt.ElementRequired), element, ref leaf);
                    return;
                }
                case MapType mt when node.IsGroup && KindOf(node) == ParquetFieldKind.Map:
                {
                    var kv = node.Children[0];
                    Match(new NestedField(mt.KeyId, "key", mt.KeyType, true), kv.Children[0], ref leaf);
                    Match(new NestedField(mt.ValueId, "value", mt.ValueType, mt.ValueRequired), kv.Children[1], ref leaf);
                    return;
                }
                case PrimitiveType or DecimalType or FixedType when !node.IsGroup:
                {
                    if (!Readable(node, f.Type)) incompatible[f.Id] = node.Describe();
                    else stats[f.Id] = Statistics(reader, node, leaf, f);
                    leaf++;
                    return;
                }
                default:
                    incompatible[f.Id] = node.IsGroup ? KindOf(node).ToString().ToLowerInvariant() : node.PhysicalType.ToString();
                    leaf += node.LeafCount;
                    return;
            }
        }
        void Missing(IcebergType type, int id)
        {
            missing.Add(id);
            switch (type)
            {
                case StructType s: foreach (var c in s.Fields) Missing(c.Type, c.Id); break;
                case ListType l: Missing(l.ElementType, l.ElementId); break;
                case MapType m: Missing(m.KeyType, m.KeyId); Missing(m.ValueType, m.ValueId); break;
            }
        }

        var byName = new Dictionary<string, (PqNode Node, int FirstColumn)>(StringComparer.Ordinal);
        foreach (var (node, _, first) in tops) byName.TryAdd(node.Name, (node, first));
        var matched = new HashSet<string>(StringComparer.Ordinal);
        foreach (var f in projection.Columns)
        {
            if (byName.TryGetValue(f.Name, out var top))
            {
                matched.Add(f.Name);
                var leaf = top.FirstColumn;
                Match(f, top.Node, ref leaf);
            }
            else Match(f, null, ref column);
        }
        return facts with
        {
            Statistics = stats, Incompatible = incompatible, Missing = missing,
            Extra = tops.Select(t => t.Node.Name).Where(n => !matched.Contains(n)).ToList(),
        };
    }

    private static ParquetField Describe(PqNode node, ref int column)
    {
        if (!node.IsGroup)
        {
            column++;
            return new ParquetField(node.Name, node.FieldId >= 0 ? node.FieldId : null, ParquetFieldKind.Primitive, []) { PhysicalType = node.Describe() };
        }
        var kind = KindOf(node);
        var children = new List<ParquetField>();
        switch (kind)
        {
            case ParquetFieldKind.List:
            {
                var repeated = node.Children[0];
                var element = ElementOf(node);
                var described = Describe(element, ref column);
                // A three-level list's element is named after its repeated group ("list"), as DuckDB maps it.
                children.Add(ReferenceEquals(repeated, element) ? described : described with { Name = repeated.Name });
                break;
            }
            case ParquetFieldKind.Map:
                foreach (var c in node.Children[0].Children) children.Add(Describe(c, ref column));
                break;
            default:
                foreach (var c in node.Children) children.Add(Describe(c, ref column));
                break;
        }
        return new ParquetField(node.Name, node.FieldId >= 0 ? node.FieldId : null, kind, children);
    }

    private static ParquetFieldKind KindOf(PqNode g)
    {
        var logical = g.Logical.Kind;
        if (logical == PqLogicalKind.Map || g.Children.Count == 1 && g.Children[0] is { IsGroup: true, Repetition: PqRepetition.Repeated, Children.Count: 2 } && logical != PqLogicalKind.List)
            return ParquetFieldKind.Map;
        if (logical == PqLogicalKind.List || g.Children.Count == 1 && g.Children[0].Repetition == PqRepetition.Repeated) return ParquetFieldKind.List;
        return ParquetFieldKind.Struct;
    }

    /// <summary>A list's element: the single child of its repeated group (three-level), or the repeated field itself.</summary>
    private static PqNode ElementOf(PqNode list)
    {
        var repeated = list.Children[0];
        return repeated is { IsGroup: true, Children.Count: 1 } && repeated.Name is not "array" && !repeated.Name.EndsWith("_tuple", StringComparison.Ordinal)
            ? repeated.Children[0] : repeated;
    }

    /// <summary>Whether the reader decodes this column as <paramref name="type"/> (the conversions it implements, no others).</summary>
    internal static bool Readable(PqNode d, IcebergType type)
    {
        var logical = d.Logical;
        var kind = logical.Kind;
        var decoding = ParquetFiles.DecodingOf(logical);
        return (d.PhysicalType, type) switch
        {
            (PqPhysical.Boolean, PrimitiveType { TypeId: TypeId.Boolean }) => true,
            (PqPhysical.Int32, PrimitiveType { TypeId: TypeId.Int }) => kind == PqLogicalKind.None || kind == PqLogicalKind.Int && (logical.BitWidth < 32 || logical.Signed),
            (PqPhysical.Int32, PrimitiveType { TypeId: TypeId.Long }) => kind is PqLogicalKind.None or PqLogicalKind.Int,
            (PqPhysical.Int64, PrimitiveType { TypeId: TypeId.Long }) => kind == PqLogicalKind.None || kind == PqLogicalKind.Int && logical.Signed,
            (PqPhysical.Int32, PrimitiveType { TypeId: TypeId.Date }) => kind == PqLogicalKind.Date,
            (PqPhysical.Int64, PrimitiveType { TypeId: TypeId.Time }) => kind == PqLogicalKind.Time && logical.Unit == PqTimeUnit.Micros,
            (PqPhysical.Int64, PrimitiveType { TypeId: TypeId.Timestamp or TypeId.TimestampTz }) =>
                kind == PqLogicalKind.Timestamp && logical.Unit is PqTimeUnit.Micros or PqTimeUnit.Millis,
            (PqPhysical.Int64, PrimitiveType { TypeId: TypeId.TimestampNs or TypeId.TimestampTzNs }) => kind == PqLogicalKind.Timestamp && logical.Unit == PqTimeUnit.Nanos,
            (PqPhysical.Float, PrimitiveType { TypeId: TypeId.Float or TypeId.Double }) => true,
            (PqPhysical.Double, PrimitiveType { TypeId: TypeId.Double }) => true,
            (PqPhysical.ByteArray, PrimitiveType { TypeId: TypeId.String }) => kind is PqLogicalKind.None or PqLogicalKind.String or PqLogicalKind.Json or PqLogicalKind.Enum,
            (PqPhysical.ByteArray, PrimitiveType { TypeId: TypeId.Binary }) => kind is PqLogicalKind.None or PqLogicalKind.Bson,
            (PqPhysical.FixedLenByteArray, PrimitiveType { TypeId: TypeId.Uuid }) => d.TypeLength == 16,
            (PqPhysical.FixedLenByteArray, FixedType f) => d.TypeLength == f.Length,
            (PqPhysical.Int64, DecimalType { Scale: 0 }) when decoding == ParquetFiles.Decoding.UInt64 => true,
            (PqPhysical.Int32 or PqPhysical.Int64 or PqPhysical.FixedLenByteArray or PqPhysical.ByteArray, DecimalType t) =>
                kind == PqLogicalKind.Decimal && logical.Scale == t.Scale && logical.Precision <= t.Precision,
            _ => false,
        };
    }

    private static ColumnStats Statistics(IPqReader reader, PqNode node, int leaf, NestedField f)
    {
        long size = 0, values = 0;
        long? nulls = 0;
        object? lower = null, upper = null;
        var bounded = true;
        for (var rg = 0; rg < reader.RowGroups; rg++)
        {
            var chunk = reader.Chunk(rg, leaf);
            size += chunk.TotalCompressedSize;
            values += chunk.NumValues;
            var s = RowGroupStatistics.For(chunk, node, f.Id, f.Type, chunk.NumValues);
            nulls = s?.NullValueCount is { } n && nulls is { } total ? total + n : null;
            if (s?.LowerBound is null || s.UpperBound is null)
            {
                // Any group without bounds leaves the file without bounds (a group of nulls only has none to give).
                if (s?.NullValueCount != chunk.NumValues) bounded = false;
                continue;
            }
            if (lower is null || Expressions.ValueComparer.Compare(s.LowerBound, lower) < 0) lower = s.LowerBound;
            if (upper is null || Expressions.ValueComparer.Compare(s.UpperBound, upper) > 0) upper = s.UpperBound;
        }
        return new ColumnStats
        {
            FieldId = f.Id, Type = f.Type, ColumnSize = size, ValueCount = values, NullValueCount = nulls,
            LowerBound = bounded ? lower : null, UpperBound = bounded ? upper : null,
        };
    }
}
