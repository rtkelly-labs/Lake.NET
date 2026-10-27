using ParquetSharp;
using ParquetSharp.IO;
using ParquetSharp.Schema;
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
        using var input = new ManagedRandomAccessFile(stream);
        using var reader = new ParquetFileReader(input);
        using var meta = reader.FileMetaData;
        var schema = meta.Schema;
        var column = 0;
        var tops = new List<(Node Node, ParquetField Field, int FirstColumn)>();
        foreach (var node in schema.GroupNode.Fields)
        {
            var first = column;
            tops.Add((node, Describe(node, ref column), first));
        }
        var facts = new ParquetFileFacts(meta.NumRows, length, footer, meta.NumRowGroups, tops.Select(t => t.Field).ToList());
        if (projection is null) return facts;

        var stats = new Dictionary<int, ColumnStats>();
        var incompatible = new Dictionary<int, string>();
        var missing = new List<int>();
        void Match(NestedField f, Node? node, ref int leaf)
        {
            if (node is null)
            {
                Missing(f.Type, f.Id);
                return;
            }
            switch (f.Type)
            {
                case StructType st when node is GroupNode g && KindOf(g) == ParquetFieldKind.Struct:
                {
                    var start = leaf;
                    var offsets = new Dictionary<string, (Node Node, int Leaf)>(StringComparer.Ordinal);
                    foreach (var c in g.Fields)
                    {
                        offsets.TryAdd(c.Name, (c, start));
                        start += CountLeaves(c);
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
                case ListType lt when node is GroupNode g && KindOf(g) == ParquetFieldKind.List:
                {
                    var element = ElementOf(g);
                    Match(new NestedField(lt.ElementId, "element", lt.ElementType, lt.ElementRequired), element, ref leaf);
                    return;
                }
                case MapType mt when node is GroupNode g && KindOf(g) == ParquetFieldKind.Map:
                {
                    var kv = (GroupNode)g.Fields[0];
                    Match(new NestedField(mt.KeyId, "key", mt.KeyType, true), kv.Fields[0], ref leaf);
                    Match(new NestedField(mt.ValueId, "value", mt.ValueType, mt.ValueRequired), kv.Fields[1], ref leaf);
                    return;
                }
                case PrimitiveType or DecimalType or FixedType when node is PrimitiveNode:
                {
                    var descr = schema.Column(leaf);
                    if (!Readable(descr, f.Type)) incompatible[f.Id] = Describe(descr);
                    else stats[f.Id] = Statistics(meta, reader, descr, leaf, f);
                    leaf++;
                    return;
                }
                default:
                    incompatible[f.Id] = node is PrimitiveNode p ? p.PhysicalType.ToString() : KindOf((GroupNode)node).ToString().ToLowerInvariant();
                    leaf += CountLeaves(node);
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

        var byName = new Dictionary<string, (Node Node, int FirstColumn)>(StringComparer.Ordinal);
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

    private static ParquetField Describe(Node node, ref int column)
    {
        if (node is PrimitiveNode p)
        {
            column++;
            return new ParquetField(p.Name, p.FieldId >= 0 ? p.FieldId : null, ParquetFieldKind.Primitive, []) { PhysicalType = DescribeNode(p) };
        }
        var g = (GroupNode)node;
        var kind = KindOf(g);
        var children = new List<ParquetField>();
        switch (kind)
        {
            case ParquetFieldKind.List:
            {
                var repeated = g.Fields[0];
                var element = ElementOf(g);
                var described = Describe(element, ref column);
                // A three-level list's element is named after its repeated group ("list"), as DuckDB maps it.
                children.Add(ReferenceEquals(repeated, element) ? described : described with { Name = repeated.Name });
                break;
            }
            case ParquetFieldKind.Map:
                foreach (var c in ((GroupNode)g.Fields[0]).Fields) children.Add(Describe(c, ref column));
                break;
            default:
                foreach (var c in g.Fields) children.Add(Describe(c, ref column));
                break;
        }
        return new ParquetField(g.Name, g.FieldId >= 0 ? g.FieldId : null, kind, children);
    }

    private static ParquetFieldKind KindOf(GroupNode g)
    {
        using var logical = g.LogicalType;
        if (logical is MapLogicalType || g.Fields.Length == 1 && g.Fields[0] is GroupNode { Repetition: Repetition.Repeated, Fields.Length: 2 } && logical is not ListLogicalType)
            return ParquetFieldKind.Map;
        if (logical is ListLogicalType || g.Fields.Length == 1 && g.Fields[0].Repetition == Repetition.Repeated) return ParquetFieldKind.List;
        return ParquetFieldKind.Struct;
    }

    /// <summary>A list's element: the single child of its repeated group (three-level), or the repeated field itself.</summary>
    private static Node ElementOf(GroupNode list)
    {
        var repeated = list.Fields[0];
        return repeated is GroupNode { Fields.Length: 1 } rg && rg.Name is not "array" && !rg.Name.EndsWith("_tuple", StringComparison.Ordinal) ? rg.Fields[0] : repeated;
    }

    private static int CountLeaves(Node n) => n is GroupNode g ? g.Fields.Sum(CountLeaves) : 1;

    private static string DescribeNode(PrimitiveNode p)
    {
        using var logical = p.LogicalType;
        return logical.Type == LogicalTypeEnum.None ? p.PhysicalType.ToString() : $"{p.PhysicalType} ({logical})";
    }

    private static string Describe(ColumnDescriptor d)
    {
        using var logical = d.LogicalType;
        return logical.Type == LogicalTypeEnum.None ? d.PhysicalType.ToString() : $"{d.PhysicalType} ({logical})";
    }

    /// <summary>Whether the reader decodes this column as <paramref name="type"/> (the conversions it implements, no others).</summary>
    internal static bool Readable(ColumnDescriptor d, IcebergType type)
    {
        using var logical = d.LogicalType;
        var decoding = ParquetFiles.DecodingOf(logical);
        return (d.PhysicalType, type) switch
        {
            (PhysicalType.Boolean, PrimitiveType { TypeId: TypeId.Boolean }) => true,
            (PhysicalType.Int32, PrimitiveType { TypeId: TypeId.Int }) => logical is NoneLogicalType or IntLogicalType { BitWidth: < 32 } or IntLogicalType { IsSigned: true },
            (PhysicalType.Int32, PrimitiveType { TypeId: TypeId.Long }) => logical is NoneLogicalType or IntLogicalType,
            (PhysicalType.Int64, PrimitiveType { TypeId: TypeId.Long }) => logical is NoneLogicalType or IntLogicalType { IsSigned: true },
            (PhysicalType.Int32, PrimitiveType { TypeId: TypeId.Date }) => logical is DateLogicalType,
            (PhysicalType.Int64, PrimitiveType { TypeId: TypeId.Time }) => logical is TimeLogicalType { TimeUnit: TimeUnit.Micros },
            (PhysicalType.Int64, PrimitiveType { TypeId: TypeId.Timestamp or TypeId.TimestampTz }) =>
                logical is TimestampLogicalType { TimeUnit: TimeUnit.Micros or TimeUnit.Millis },
            (PhysicalType.Int64, PrimitiveType { TypeId: TypeId.TimestampNs or TypeId.TimestampTzNs }) => logical is TimestampLogicalType { TimeUnit: TimeUnit.Nanos },
            (PhysicalType.Float, PrimitiveType { TypeId: TypeId.Float or TypeId.Double }) => true,
            (PhysicalType.Double, PrimitiveType { TypeId: TypeId.Double }) => true,
            (PhysicalType.ByteArray, PrimitiveType { TypeId: TypeId.String }) => logical is NoneLogicalType or StringLogicalType or JsonLogicalType or EnumLogicalType,
            (PhysicalType.ByteArray, PrimitiveType { TypeId: TypeId.Binary }) => logical is NoneLogicalType or BsonLogicalType,
            (PhysicalType.FixedLenByteArray, PrimitiveType { TypeId: TypeId.Uuid }) => d.TypeLength == 16,
            (PhysicalType.FixedLenByteArray, FixedType f) => d.TypeLength == f.Length,
            (PhysicalType.Int64, DecimalType { Scale: 0 }) when decoding == ParquetFiles.Decoding.UInt64 => true,
            (PhysicalType.Int32 or PhysicalType.Int64 or PhysicalType.FixedLenByteArray or PhysicalType.ByteArray, DecimalType t) =>
                logical is DecimalLogicalType dl && dl.Scale == t.Scale && dl.Precision <= t.Precision,
            _ => false,
        };
    }

    private static ColumnStats Statistics(FileMetaData meta, ParquetFileReader reader, ColumnDescriptor descr, int leaf, NestedField f)
    {
        long size = 0, values = 0;
        long? nulls = 0;
        object? lower = null, upper = null;
        var bounded = true;
        for (var rg = 0; rg < meta.NumRowGroups; rg++)
        {
            using var group = reader.RowGroup(rg);
            using var chunk = group.MetaData.GetColumnChunkMetaData(leaf);
            size += chunk.TotalCompressedSize;
            values += chunk.NumValues;
            var s = RowGroupStatistics.For(group.MetaData, descr, leaf, f.Id, f.Type, chunk.NumValues);
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
