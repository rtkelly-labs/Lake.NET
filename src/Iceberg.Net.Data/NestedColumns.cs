using System.Collections;
using System.Runtime.InteropServices;
using ParquetSharp;
using ParquetSharp.Schema;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>
/// Struct, list and map columns in Parquet, shredded into definition/repetition levels and reassembled (Dremel) over
/// ParquetSharp's physical column API, in the layout Iceberg's spec and Java/Spark/PyIceberg use: structs are groups,
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
        public required PhysicalType Physical { get; init; }
        public int TypeLength { get; init; }
        public IcebergType? Type { get; init; } // write side only
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

    public static Node ToNode(NestedField f) => ToNode(f.Name, f.Type, f.Required, f.Id);

    private static Node ToNode(string name, IcebergType type, bool required, int id)
    {
        var rep = required ? Repetition.Required : Repetition.Optional;
        switch (type)
        {
            case StructType s:
                return new GroupNode(name, rep, s.Fields.Select(ToNode).ToArray(), null, id);
            case ListType l:
            {
                using var element = ToNode("element", l.ElementType, l.ElementRequired, l.ElementId);
                using var list = new GroupNode("list", Repetition.Repeated, [element]);
                using var logical = LogicalType.List();
                return new GroupNode(name, rep, [list], logical, id);
            }
            case MapType m:
            {
                using var key = ToNode("key", m.KeyType, true, m.KeyId);
                using var value = ToNode("value", m.ValueType, m.ValueRequired, m.ValueId);
                using var kv = new GroupNode("key_value", Repetition.Repeated, [key, value]);
                using var logical = LogicalType.Map();
                return new GroupNode(name, rep, [kv], logical, id);
            }
            default:
                return ParquetFiles.ToNode(new NestedField(id, name, type, required));
        }
    }

    /// <summary>The level tree for writing <paramref name="f"/>, with leaves numbered from <paramref name="firstColumn"/>.</summary>
    public static PNode Plan(NestedField f, int firstColumn)
    {
        var column = firstColumn;
        return Build(f.Type, !f.Required, 0, 0, f.Id, f.Name, ref column);
    }

    private static PNode Build(IcebergType type, bool optional, int parentDef, int parentRep, int id, string name, ref int column)
    {
        var def = parentDef + (optional ? 1 : 0);
        PNode node;
        switch (type)
        {
            case StructType s:
            {
                var children = new List<PNode>();
                foreach (var c in s.Fields) children.Add(Build(c.Type, !c.Required, def, parentRep, c.Id, c.Name, ref column));
                node = new Struct { Optional = optional, Def = def, FieldId = id, Name = name, Children = children };
                break;
            }
            case ListType l:
            {
                var element = Build(l.ElementType, !l.ElementRequired, def + 1, parentRep + 1, l.ElementId, "element", ref column);
                node = new Repeated { Optional = optional, Def = def, FieldId = id, Name = name, ElementDef = def + 1, Rep = parentRep + 1, Element = element };
                break;
            }
            case MapType m:
            {
                var key = Build(m.KeyType, false, def + 1, parentRep + 1, m.KeyId, "key", ref column);
                var value = Build(m.ValueType, !m.ValueRequired, def + 1, parentRep + 1, m.ValueId, "value", ref column);
                node = new Repeated { Optional = optional, Def = def, FieldId = id, Name = name, ElementDef = def + 1, Rep = parentRep + 1, Element = key, Value = value };
                break;
            }
            default:
            {
                using var p = (PrimitiveNode)ParquetFiles.ToNode(new NestedField(id, name, type, !optional));
                var leaf = new Leaf { Optional = optional, Def = def, FieldId = id, Name = name, Column = column++, Physical = p.PhysicalType, TypeLength = p.TypeLength, Type = type };
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
    public static PNode FromFile(Node n, ref int column, int parentDef = 0, int parentRep = 0)
    {
        var optional = n.Repetition == Repetition.Optional;
        var def = parentDef + (optional ? 1 : 0);
        if (n is PrimitiveNode p)
        {
            if (n.Repetition == Repetition.Repeated) throw new NotSupportedException($"Repeated primitive {n.Name} outside a LIST group");
            var leaf = new Leaf { Optional = optional, Def = def, FieldId = n.FieldId, Name = n.Name, Column = column++, Physical = p.PhysicalType, TypeLength = p.TypeLength };
            leaf.Leaves.Add(leaf);
            return leaf;
        }
        var g = (GroupNode)n;
        using var logical = g.LogicalType;
        PNode node;
        if (logical is ListLogicalType || logical is MapLogicalType || g.Fields.Length == 1 && g.Fields[0].Repetition == Repetition.Repeated)
        {
            var repeated = g.Fields.Single();
            if (repeated.Repetition != Repetition.Repeated) throw new NotSupportedException($"LIST/MAP group {n.Name} without a repeated child");
            var elementDef = def + 1;
            var rep = parentRep + 1;
            if (logical is MapLogicalType || repeated is GroupNode { Fields.Length: 2 } && logical is not ListLogicalType)
            {
                var kv = (GroupNode)repeated;
                var key = FromFile(kv.Fields[0], ref column, elementDef, rep);
                var value = FromFile(kv.Fields[1], ref column, elementDef, rep);
                node = new Repeated { Optional = optional, Def = def, FieldId = n.FieldId, Name = n.Name, ElementDef = elementDef, Rep = rep, Element = key, Value = value };
            }
            else if (repeated is GroupNode { Fields.Length: 1 } rg && rg.Name is not "array" && !rg.Name.EndsWith("_tuple", StringComparison.Ordinal))
            {
                var element = FromFile(rg.Fields[0], ref column, elementDef, rep);
                node = new Repeated { Optional = optional, Def = def, FieldId = n.FieldId, Name = n.Name, ElementDef = elementDef, Rep = rep, Element = element };
            }
            else
            {
                // Two-level list: the repeated field is the (required) element.
                var element = FromFileAsRequired(repeated, ref column, elementDef, rep);
                node = new Repeated { Optional = optional, Def = def, FieldId = n.FieldId, Name = n.Name, ElementDef = elementDef, Rep = rep, Element = element };
            }
        }
        else
        {
            var children = new List<PNode>();
            foreach (var c in g.Fields) children.Add(FromFile(c, ref column, def, parentRep));
            node = new Struct { Optional = optional, Def = def, FieldId = n.FieldId, Name = n.Name, Children = children };
        }
        CollectLeaves(node);
        return node;
    }

    private static PNode FromFileAsRequired(Node repeated, ref int column, int def, int rep)
    {
        if (repeated is PrimitiveNode p)
        {
            var leaf = new Leaf { Optional = false, Def = def, FieldId = p.FieldId, Name = p.Name, Column = column++, Physical = p.PhysicalType, TypeLength = p.TypeLength };
            leaf.Leaves.Add(leaf);
            return leaf;
        }
        var g = (GroupNode)repeated;
        var children = new List<PNode>();
        foreach (var c in g.Fields) children.Add(FromFile(c, ref column, def, rep));
        var node = new Struct { Optional = false, Def = def, FieldId = g.FieldId, Name = g.Name, Children = children };
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
        var type = leaf.Type!;
        return type switch
        {
            PrimitiveType { TypeId: TypeId.String } => System.Text.Encoding.UTF8.GetBytes((string)v),
            PrimitiveType { TypeId: TypeId.Uuid } => ((Guid)v).ToByteArray(bigEndian: true),
            DecimalType d => leaf.Physical switch
            {
                PhysicalType.Int32 => (object)(int)Unscaled(d, v),
                PhysicalType.Int64 => (long)Unscaled(d, v),
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

    /// <summary>Write one leaf's levels and values to its column chunk.</summary>
    public static void WriteLeaf(ColumnWriter col, Leaf leaf)
    {
        var defs = leaf.Defs.ToArray();
        var reps = leaf.Reps.ToArray();
        switch (leaf.Physical)
        {
            case PhysicalType.Boolean: ((ColumnWriter<bool>)col).WriteBatch(defs.Length, defs, reps, leaf.Values.Cast<bool>().ToArray()); break;
            case PhysicalType.Int32: ((ColumnWriter<int>)col).WriteBatch(defs.Length, defs, reps, leaf.Values.Cast<int>().ToArray()); break;
            case PhysicalType.Int64: ((ColumnWriter<long>)col).WriteBatch(defs.Length, defs, reps, leaf.Values.Cast<long>().ToArray()); break;
            case PhysicalType.Float: ((ColumnWriter<float>)col).WriteBatch(defs.Length, defs, reps, leaf.Values.Cast<float>().ToArray()); break;
            case PhysicalType.Double: ((ColumnWriter<double>)col).WriteBatch(defs.Length, defs, reps, leaf.Values.Cast<double>().ToArray()); break;
            case PhysicalType.ByteArray or PhysicalType.FixedLenByteArray:
            {
                var bytes = leaf.Values.Cast<byte[]>().ToList();
                if (leaf.Physical == PhysicalType.FixedLenByteArray && bytes.Any(b => b.Length != leaf.TypeLength))
                    throw new ArgumentException($"Field {leaf.Name} expects {leaf.TypeLength}-byte values");
                var buffer = new byte[Math.Max(1, bytes.Sum(b => b.Length))];
                var offsets = new int[bytes.Count];
                for (int i = 0, at = 0; i < bytes.Count; at += bytes[i].Length, i++) { offsets[i] = at; bytes[i].CopyTo(buffer, at); }
                var handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                try
                {
                    var start = handle.AddrOfPinnedObject();
                    if (leaf.Physical == PhysicalType.ByteArray)
                        ((ColumnWriter<ByteArray>)col).WriteBatch(defs.Length, defs, reps, bytes.Select((b, i) => new ByteArray(start + offsets[i], b.Length)).ToArray());
                    else
                        ((ColumnWriter<FixedLenByteArray>)col).WriteBatch(defs.Length, defs, reps, bytes.Select((_, i) => new FixedLenByteArray(start + offsets[i])).ToArray());
                }
                finally
                {
                    handle.Free();
                }
                break;
            }
            default: throw new NotSupportedException(leaf.Physical.ToString());
        }
    }

    // ---------- read: assembly ----------

    /// <summary>Load every leaf of <paramref name="node"/> from one row group.</summary>
    public static void Load(RowGroupReader group, PNode node)
    {
        foreach (var leaf in node.Leaves)
        {
            leaf.Defs.Clear(); leaf.Reps.Clear(); leaf.Values.Clear();
            leaf.Position = leaf.ValuePosition = 0;
            using var col = group.Column(leaf.Column);
            switch (leaf.Physical)
            {
                case PhysicalType.Boolean: ReadAll<bool>(col, leaf, v => v); break;
                case PhysicalType.Int32: ReadAll<int>(col, leaf, v => v); break;
                case PhysicalType.Int64: ReadAll<long>(col, leaf, v => v); break;
                case PhysicalType.Float: ReadAll<float>(col, leaf, v => v); break;
                case PhysicalType.Double: ReadAll<double>(col, leaf, v => v); break;
                case PhysicalType.ByteArray:
                    ReadAll<ByteArray>(col, leaf, v =>
                    {
                        var b = new byte[v.Length];
                        if (v.Length > 0) Marshal.Copy(v.Pointer, b, 0, v.Length);
                        return b;
                    });
                    break;
                case PhysicalType.FixedLenByteArray:
                    ReadAll<FixedLenByteArray>(col, leaf, v =>
                    {
                        var b = new byte[leaf.TypeLength];
                        Marshal.Copy(v.Pointer, b, 0, b.Length);
                        return b;
                    });
                    break;
                default: throw new NotSupportedException(leaf.Physical.ToString());
            }
        }
    }

    private static void ReadAll<T>(ColumnReader col, Leaf leaf, Func<T, object> copy) where T : unmanaged
    {
        var reader = (ColumnReader<T>)col;
        const int batch = 4096;
        var defs = new short[batch];
        var reps = new short[batch];
        var values = new T[batch];
        var maxDef = col.ColumnDescriptor.MaxDefinitionLevel;
        var maxRep = col.ColumnDescriptor.MaxRepetitionLevel;
        while (reader.HasNext)
        {
            var levels = reader.ReadBatch(batch, defs, reps, values, out var valuesRead);
            if (levels == 0) break;
            for (var i = 0; i < levels; i++)
            {
                leaf.Defs.Add(maxDef == 0 ? (short)0 : defs[i]);
                leaf.Reps.Add(maxRep == 0 ? (short)0 : reps[i]);
            }
            for (var i = 0; i < valuesRead; i++) leaf.Values.Add(copy(values[i])); // copy now: buffers are reused by the next batch
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
    public static object? Project(object? v, PNode file, IcebergType target, Func<IcebergType, PhysicalType, int, object, object> primitive)
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
