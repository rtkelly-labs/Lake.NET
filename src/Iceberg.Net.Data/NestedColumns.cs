using System.Collections;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>
/// Struct, list and map columns in Parquet, shredded into definition/repetition levels and reassembled (Dremel) over
/// the physical column data of either Parquet backend (<see cref="IPqReader"/>, <see cref="IPqWriter"/>), in the layout Iceberg's spec and Java/Spark/PyIceberg use: structs are groups,
/// lists are <c>&lt;group (LIST)&gt; { repeated group list { element } }</c>, maps
/// <c>&lt;group (MAP)&gt; { repeated group key_value { required key; value } }</c>, with field IDs on every element,
/// key and value. Values are <c>object?[]</c> for structs (aligned to the struct's fields), <c>List&lt;object?&gt;</c>
/// for lists and <c>Dictionary&lt;object, object?&gt;</c> for maps; any <see cref="IEnumerable"/> and
/// <see cref="IDictionary"/> are accepted on write.
/// </summary>
internal static class NestedColumns
{
    /// <summary>One node of a nested column, with its levels: <see cref="Def"/> is the definition level when the node is present.</summary>
    internal abstract class PNode
    {
        public required bool Optional { get; init; }
        public required int Def { get; init; }
        public int FieldId { get; init; } = -1;
        public string Name { get; init; } = "";
        public List<Leaf> Leaves { get; } = [];
    }

    internal sealed class Leaf : PNode
    {
        public required int Column { get; init; }
        public required PqPhysical Physical { get; init; }
        public int TypeLength { get; init; }
        public IcebergType? Type { get; init; } // write side only
        public ParquetAnnotation? Annotation { get; init; } // write side only
        /// <summary>Highest repetition level (lists and maps around the leaf); write side only.</summary>
        public int MaxRep { get; init; }
        public ParquetFiles.Decoding Decoding { get; init; } // read side only
        /// <summary>A byte array column annotated as text: read as strings.</summary>
        public bool Strings { get; init; } // read side only
        // Column data for one row group: levels plus the defined values in order.
        public List<short> Defs { get; } = [];
        public List<short> Reps { get; } = [];
        public List<object> Values { get; } = [];
        public int Position, ValuePosition;
        public short PeekDef => Defs[Position];
        public bool HasMore => Position < Defs.Count;
        public short PeekRep => Reps[Position];
    }

    internal sealed class Struct : PNode
    {
        public required List<PNode> Children { get; init; }
    }

    /// <summary>A list, or a map when <see cref="Value"/> is set (then <see cref="Element"/> is the key).</summary>
    internal sealed class Repeated : PNode
    {
        public required int ElementDef { get; init; } // definition level with at least one element
        public required int Rep { get; init; }
        public required PNode Element { get; init; }
        public PNode? Value { get; init; }
    }

    // ---------- schema ----------

    public static PqNode ToPq(NestedField f, IReadOnlyDictionary<int, ParquetAnnotation>? annotations) => ToPq(f.Name, f.Type, f.Required, f.Id, annotations);

    private static PqNode ToPq(string name, IcebergType type, bool required, int id, IReadOnlyDictionary<int, ParquetAnnotation>? annotations)
    {
        var rep = required ? PqRepetition.Required : PqRepetition.Optional;
        switch (type)
        {
            case StructType s:
                return new PqNode { Name = name, Repetition = rep, FieldId = id, Children = s.Fields.Select(f => ToPq(f, annotations)).ToList() };
            case ListType l:
            {
                var element = ToPq("element", l.ElementType, l.ElementRequired, l.ElementId, annotations);
                var list = new PqNode { Name = "list", Repetition = PqRepetition.Repeated, Children = [element] };
                return new PqNode { Name = name, Repetition = rep, FieldId = id, Logical = PqLogical.List, Children = [list] };
            }
            case MapType m:
            {
                var key = ToPq("key", m.KeyType, true, m.KeyId, annotations);
                var value = ToPq("value", m.ValueType, m.ValueRequired, m.ValueId, annotations);
                var kv = new PqNode { Name = "key_value", Repetition = PqRepetition.Repeated, Children = [key, value] };
                return new PqNode { Name = name, Repetition = rep, FieldId = id, Logical = PqLogical.Map, Children = [kv] };
            }
            default:
                return ParquetFiles.ToPq(new NestedField(id, name, type, required), annotations);
        }
    }

    /// <summary>The level tree for writing <paramref name="f"/>, with leaves numbered from <paramref name="firstColumn"/>.</summary>
    public static PNode Plan(NestedField f, int firstColumn, IReadOnlyDictionary<int, ParquetAnnotation>? annotations = null)
    {
        var column = firstColumn;
        return Build(f.Type, !f.Required, 0, 0, f.Id, f.Name, ref column, annotations);
    }

    private static PNode Build(IcebergType type, bool optional, int parentDef, int parentRep, int id, string name, ref int column,
        IReadOnlyDictionary<int, ParquetAnnotation>? annotations)
    {
        var def = parentDef + (optional ? 1 : 0);
        PNode node;
        switch (type)
        {
            case StructType s:
            {
                var children = new List<PNode>();
                foreach (var c in s.Fields) children.Add(Build(c.Type, !c.Required, def, parentRep, c.Id, c.Name, ref column, annotations));
                node = new Struct { Optional = optional, Def = def, FieldId = id, Name = name, Children = children };
                break;
            }
            case ListType l:
            {
                var element = Build(l.ElementType, !l.ElementRequired, def + 1, parentRep + 1, l.ElementId, "element", ref column, annotations);
                node = new Repeated { Optional = optional, Def = def, FieldId = id, Name = name, ElementDef = def + 1, Rep = parentRep + 1, Element = element };
                break;
            }
            case MapType m:
            {
                var key = Build(m.KeyType, false, def + 1, parentRep + 1, m.KeyId, "key", ref column, annotations);
                var value = Build(m.ValueType, !m.ValueRequired, def + 1, parentRep + 1, m.ValueId, "value", ref column, annotations);
                node = new Repeated { Optional = optional, Def = def, FieldId = id, Name = name, ElementDef = def + 1, Rep = parentRep + 1, Element = key, Value = value };
                break;
            }
            default:
            {
                var p = ParquetFiles.ToPq(new NestedField(id, name, type, !optional), annotations);
                ParquetAnnotation? annotation = annotations is not null && annotations.TryGetValue(id, out var a) ? a : null;
                var leaf = new Leaf { Optional = optional, Def = def, FieldId = id, Name = name, Column = column++, Physical = p.PhysicalType, TypeLength = p.TypeLength, Type = type,
                    Annotation = annotation, MaxRep = parentRep };
                leaf.Leaves.Add(leaf);
                return leaf;
            }
        }
        CollectLeaves(node);
        return node;
    }

    private static void CollectLeaves(PNode node)
    {
        switch (node)
        {
            case Struct s: foreach (var c in s.Children) node.Leaves.AddRange(c.Leaves); break;
            case Repeated r:
                node.Leaves.AddRange(r.Element.Leaves);
                if (r.Value is not null) node.Leaves.AddRange(r.Value.Leaves);
                break;
        }
    }

    /// <summary>
    /// The level tree of a nested column read from a file. Three-level lists and maps as written by Iceberg engines are
    /// read, as are two-level lists whose repeated field is the element itself (older Parquet writers).
    /// </summary>
    public static PNode FromFile(PqNode n, ref int column, int parentDef = 0, int parentRep = 0, bool ignoreIds = false)
    {
        var optional = n.Repetition == PqRepetition.Optional;
        var def = parentDef + (optional ? 1 : 0);
        if (!n.IsGroup)
        {
            if (n.Repetition == PqRepetition.Repeated) throw new NotSupportedException($"Repeated primitive {n.Name} outside a LIST group");
            return LeafOf(n, optional, def, ref column, ignoreIds);
        }
        PNode node;
        var logical = n.Logical.Kind;
        if (logical is PqLogicalKind.List or PqLogicalKind.Map || n.Children.Count == 1 && n.Children[0].Repetition == PqRepetition.Repeated)
        {
            var repeated = n.Children.Count == 1 ? n.Children[0] : throw new NotSupportedException($"LIST/MAP group {n.Name} has {n.Children.Count} children");
            if (repeated.Repetition != PqRepetition.Repeated) throw new NotSupportedException($"LIST/MAP group {n.Name} without a repeated child");
            var elementDef = def + 1;
            var rep = parentRep + 1;
            if (logical == PqLogicalKind.Map || repeated is { IsGroup: true, Children.Count: 2 } && logical != PqLogicalKind.List)
            {
                var key = FromFile(repeated.Children[0], ref column, elementDef, rep, ignoreIds);
                var value = FromFile(repeated.Children[1], ref column, elementDef, rep, ignoreIds);
                node = new Repeated { Optional = optional, Def = def, FieldId = ignoreIds ? -1 : n.FieldId, Name = n.Name, ElementDef = elementDef, Rep = rep, Element = key, Value = value };
            }
            else if (repeated is { IsGroup: true, Children.Count: 1 } && repeated.Name is not "array" && !repeated.Name.EndsWith("_tuple", StringComparison.Ordinal))
            {
                var element = FromFile(repeated.Children[0], ref column, elementDef, rep, ignoreIds);
                node = new Repeated { Optional = optional, Def = def, FieldId = ignoreIds ? -1 : n.FieldId, Name = n.Name, ElementDef = elementDef, Rep = rep, Element = element };
            }
            else
            {
                // Two-level list: the repeated field is the (required) element.
                var element = FromFileAsRequired(repeated, ref column, elementDef, rep, ignoreIds);
                node = new Repeated { Optional = optional, Def = def, FieldId = ignoreIds ? -1 : n.FieldId, Name = n.Name, ElementDef = elementDef, Rep = rep, Element = element };
            }
        }
        else
        {
            var children = new List<PNode>();
            foreach (var c in n.Children) children.Add(FromFile(c, ref column, def, parentRep, ignoreIds));
            node = new Struct { Optional = optional, Def = def, FieldId = ignoreIds ? -1 : n.FieldId, Name = n.Name, Children = children };
        }
        CollectLeaves(node);
        return node;
    }

    private static Leaf LeafOf(PqNode p, bool optional, int def, ref int column, bool ignoreIds)
    {
        var leaf = new Leaf
        {
            Optional = optional, Def = def, FieldId = ignoreIds ? -1 : p.FieldId, Name = p.Name, Column = column++, Physical = p.PhysicalType,
            TypeLength = p.TypeLength, Decoding = ParquetFiles.DecodingOf(p.Logical),
            Strings = p.PhysicalType == PqPhysical.ByteArray && p.Logical.Kind is PqLogicalKind.String or PqLogicalKind.Enum or PqLogicalKind.Json,
        };
        leaf.Leaves.Add(leaf);
        return leaf;
    }

    private static PNode FromFileAsRequired(PqNode repeated, ref int column, int def, int rep, bool ignoreIds)
    {
        if (!repeated.IsGroup) return LeafOf(repeated, false, def, ref column, ignoreIds);
        var children = new List<PNode>();
        foreach (var c in repeated.Children) children.Add(FromFile(c, ref column, def, rep, ignoreIds));
        var node = new Struct { Optional = false, Def = def, FieldId = ignoreIds ? -1 : repeated.FieldId, Name = repeated.Name, Children = children };
        CollectLeaves(node);
        return node;
    }

    // ---------- write: shredding ----------

    public static void Shred(PNode node, IEnumerable<object?> values)
    {
        foreach (var v in values) Shred(node, v, 0, 0);
    }

    private static void Shred(PNode n, object? v, int parentDef, int rep)
    {
        if (v is null)
        {
            if (!n.Optional) throw new ArgumentException($"Required field {n.Name} is null");
            foreach (var leaf in n.Leaves) { leaf.Defs.Add((short)parentDef); leaf.Reps.Add((short)rep); }
            return;
        }
        switch (n)
        {
            case Leaf leaf:
                leaf.Defs.Add((short)leaf.Def);
                leaf.Reps.Add((short)rep);
                leaf.Values.Add(ToPhysical(leaf, v));
                break;
            case Struct s:
            {
                var fields = v as object?[] ?? throw new ArgumentException($"Struct {n.Name} expects object?[] values, got {v.GetType().Name}");
                if (fields.Length != s.Children.Count) throw new ArgumentException($"Struct {n.Name} has {s.Children.Count} fields, value has {fields.Length}");
                for (var i = 0; i < fields.Length; i++) Shred(s.Children[i], fields[i], s.Def, rep);
                break;
            }
            case Repeated r when r.Value is null:
            {
                if (v is string or byte[] || v is not IEnumerable items) throw new ArgumentException($"List {n.Name} expects a collection, got {v.GetType().Name}");
                var first = true;
                foreach (var item in items)
                {
                    Shred(r.Element, item, r.ElementDef, first ? rep : r.Rep);
                    first = false;
                }
                if (first) foreach (var leaf in n.Leaves) { leaf.Defs.Add((short)r.Def); leaf.Reps.Add((short)rep); } // empty list
                break;
            }
            case Repeated r:
            {
                var first = true;
                foreach (var (key, value) in Entries(n.Name, v))
                {
                    if (key is null) throw new ArgumentException($"Map {n.Name} has a null key");
                    Shred(r.Element, key, r.ElementDef, first ? rep : r.Rep);
                    Shred(r.Value!, value, r.ElementDef, first ? rep : r.Rep);
                    first = false;
                }
                if (first) foreach (var leaf in n.Leaves) { leaf.Defs.Add((short)r.Def); leaf.Reps.Add((short)rep); } // empty map
                break;
            }
        }
    }

    private static IEnumerable<(object? Key, object? Value)> Entries(string name, object v)
    {
        switch (v)
        {
            case IDictionary d:
                foreach (DictionaryEntry e in d) yield return (e.Key, e.Value);
                break;
            case IEnumerable<KeyValuePair<object, object?>> pairs:
                foreach (var (k, val) in pairs) yield return (k, val);
                break;
            default: throw new ArgumentException($"Map {name} expects a dictionary, got {v.GetType().Name}");
        }
    }

    private static object ToPhysical(Leaf leaf, object v)
    {
        if (leaf.Annotation is { } annotation and not (ParquetAnnotation.TimeUtc or ParquetAnnotation.Interval)) return ParquetFiles.Encode(annotation, v);
        var type = leaf.Type!;
        return type switch
        {
            PrimitiveType { TypeId: TypeId.String } => System.Text.Encoding.UTF8.GetBytes((string)v),
            PrimitiveType { TypeId: TypeId.Uuid } => ((Guid)v).ToByteArray(bigEndian: true),
            DecimalType d => leaf.Physical switch
            {
                PqPhysical.Int32 => (object)(int)Unscaled(d, v),
                PqPhysical.Int64 => (long)Unscaled(d, v),
                _ => ParquetFiles.FixedDecimal(Unscaled(d, v), leaf.TypeLength),
            },
            PrimitiveType { TypeId: TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs } => Convert.ToInt64(v, System.Globalization.CultureInfo.InvariantCulture),
            PrimitiveType { TypeId: TypeId.Int or TypeId.Date } => Convert.ToInt32(v, System.Globalization.CultureInfo.InvariantCulture),
            PrimitiveType { TypeId: TypeId.Double } => Convert.ToDouble(v, System.Globalization.CultureInfo.InvariantCulture),
            PrimitiveType { TypeId: TypeId.Float } => Convert.ToSingle(v, System.Globalization.CultureInfo.InvariantCulture),
            _ => v, // bool, binary, fixed
        };
    }

    private static System.Numerics.BigInteger Unscaled(DecimalType d, object v) => v switch
    {
        IcebergDecimal x when x.Scale == d.Scale => x.Unscaled,
        IcebergDecimal x => throw new ArgumentException($"Decimal scale {x.Scale} does not match {d}"),
        decimal m => IcebergDecimal.FromDecimal(m, d.Scale).Unscaled,
        _ => throw new ArgumentException($"Expected a decimal for {d}, got {v.GetType().Name}"),
    };

    /// <summary>One leaf's levels and values, as a backend writes them to its column chunk.</summary>
    public static PqLeafData ToLeafData(Leaf leaf)
    {
        var defs = leaf.Def > 0 ? leaf.Defs.ToArray() : null;
        var reps = leaf.MaxRep > 0 ? leaf.Reps.ToArray() : null;
        Array values = leaf.Physical switch
        {
            PqPhysical.Boolean => leaf.Values.Cast<bool>().ToArray(),
            PqPhysical.Int32 => leaf.Values.Cast<int>().ToArray(),
            PqPhysical.Int64 => leaf.Values.Cast<long>().ToArray(),
            PqPhysical.Float => leaf.Values.Cast<float>().ToArray(),
            PqPhysical.Double => leaf.Values.Cast<double>().ToArray(),
            PqPhysical.ByteArray or PqPhysical.FixedLenByteArray => leaf.Values.Cast<byte[]>().ToArray(),
            _ => throw new NotSupportedException(leaf.Physical.ToString()),
        };
        if (leaf.Physical == PqPhysical.FixedLenByteArray && ((byte[][])values).Any(b => b.Length != leaf.TypeLength))
            throw new ArgumentException($"Field {leaf.Name} expects {leaf.TypeLength}-byte values");
        return new PqLeafData(leaf.Defs.Count, defs, reps, values);
    }

    // ---------- read: assembly ----------

    /// <summary>Load every leaf of <paramref name="node"/> from one row group.</summary>
    public static async ValueTask LoadAsync(IPqReader reader, int rowGroup, PNode node, CancellationToken ct)
    {
        foreach (var leaf in node.Leaves)
        {
            leaf.Defs.Clear(); leaf.Reps.Clear(); leaf.Values.Clear();
            leaf.Position = leaf.ValuePosition = 0;
            var data = await reader.ReadLeafAsync(rowGroup, leaf.Column, leaf.Strings, ct);
            for (var i = 0; i < data.Levels; i++)
            {
                leaf.Defs.Add(data.Defs?[i] ?? 0);
                leaf.Reps.Add(data.Reps?[i] ?? 0);
            }
            var decoding = leaf.Decoding;
            switch (data.Values)
            {
                case int[] ints when decoding != ParquetFiles.Decoding.None: foreach (var v in ints) leaf.Values.Add(ParquetFiles.Decode(decoding, v)); break;
                case long[] longs when decoding != ParquetFiles.Decoding.None: foreach (var v in longs) leaf.Values.Add(ParquetFiles.Decode(decoding, v)); break;
                default: foreach (var v in data.Values) leaf.Values.Add(v!); break;
            }
        }
    }

    /// <summary>Reassemble the next record's value of <paramref name="n"/> in file shape (raw physical leaves).</summary>
    public static object? Assemble(PNode n)
    {
        var first = n.Leaves[0];
        if (n.Optional && first.PeekDef < n.Def)
        {
            Skip(n);
            return null;
        }
        switch (n)
        {
            case Leaf leaf:
                leaf.Position++;
                return leaf.Values[leaf.ValuePosition++];
            case Struct s:
                return s.Children.Select(Assemble).ToArray();
            case Repeated r:
            {
                var items = new List<object?>();
                if (first.PeekDef < r.ElementDef)
                {
                    Skip(n); // empty
                    return items;
                }
                do
                {
                    items.Add(r.Value is null ? Assemble(r.Element) : new KeyValuePair<object?, object?>(Assemble(r.Element), Assemble(r.Value)));
                }
                while (first.HasMore && first.PeekRep == r.Rep);
                return items;
            }
            default: throw new InvalidOperationException();
        }
    }

    /// <summary>A null or empty node has exactly one level entry, with no value, in every leaf below it.</summary>
    private static void Skip(PNode n)
    {
        foreach (var leaf in n.Leaves) leaf.Position++;
    }

    /// <summary>
    /// File-shape value → the projected Iceberg type: struct fields matched by field ID (by name if the file has none),
    /// missing fields read as their initial default, primitives converted (with int→long, float→double promotion).
    /// </summary>
    public static object? Project(object? v, PNode file, IcebergType target, Func<IcebergType, PqPhysical, int, object, object> primitive)
    {
        if (v is null) return null;
        switch (target)
        {
            case StructType st:
            {
                var s = (Struct)file;
                var fields = (object?[])v;
                return st.Fields.Select(tf =>
                {
                    var i = s.Children.FindIndex(c => c.FieldId >= 0 ? c.FieldId == tf.Id : c.Name == tf.Name);
                    return i < 0 ? Defaults.ToValue(tf.Type, tf.InitialDefault) : Project(fields[i], s.Children[i], tf.Type, primitive);
                }).ToArray();
            }
            case ListType lt:
                return ((List<object?>)v).Select(e => Project(e, ((Repeated)file).Element, lt.ElementType, primitive)).ToList();
            case MapType mt:
            {
                var r = (Repeated)file;
                var map = new Dictionary<object, object?>();
                foreach (var kv in ((List<object?>)v).Cast<KeyValuePair<object?, object?>>())
                    map[Project(kv.Key, r.Element, mt.KeyType, primitive)!] = Project(kv.Value, r.Value!, mt.ValueType, primitive);
                return map;
            }
            default:
                var leaf = (Leaf)file;
                return primitive(target, leaf.Physical, leaf.TypeLength, v);
        }
    }
}
