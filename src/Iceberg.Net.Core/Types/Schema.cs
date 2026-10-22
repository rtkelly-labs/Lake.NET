namespace Iceberg.Net.Types;

/// <summary>
/// A table schema: a struct of fields plus a schema ID and optional identifier (row key) field IDs.
/// </summary>
public sealed class Schema
{
    private readonly Dictionary<int, NestedField> _byId;
    private readonly Dictionary<string, int> _byName;
    private readonly Dictionary<int, string> _nameById;

    public Schema(int schemaId, IEnumerable<NestedField> columns, IEnumerable<int>? identifierFieldIds = null)
    {
        SchemaId = schemaId;
        Struct = new StructType(columns);
        IdentifierFieldIds = (identifierFieldIds ?? []).ToList();
        _byId = TypeUtil.IndexById(Struct);
        _byName = TypeUtil.IndexByName(Struct);
        _nameById = _byName.ToDictionary(kv => kv.Value, kv => kv.Key);
        foreach (var id in IdentifierFieldIds)
        {
            if (!_byId.TryGetValue(id, out var f)) throw new ArgumentException($"Identifier field {id} not in schema");
            if (!f.Required || !f.Type.IsPrimitive || f.Type.TypeId is TypeId.Float or TypeId.Double)
                throw new ArgumentException($"Identifier field {f.Name} must be a required, non-floating primitive");
        }
    }

    public Schema(params NestedField[] columns) : this(0, columns) { }

    public int SchemaId { get; }
    public StructType Struct { get; }
    public IReadOnlyList<NestedField> Columns => Struct.Fields;
    public IReadOnlyList<int> IdentifierFieldIds { get; }

    public int HighestFieldId => _byId.Count == 0 ? 0 : _byId.Keys.Max();

    public NestedField? FindField(int id) => _byId.GetValueOrDefault(id);

    /// <summary>Find a field by its full dotted name, e.g. <c>location.lat</c> or <c>tags.element</c>.</summary>
    public NestedField? FindField(string name) => _byName.TryGetValue(name, out var id) ? _byId[id] : null;

    public string? FindColumnName(int id) => _nameById.GetValueOrDefault(id);

    public IcebergType? FindType(int id) => FindField(id)?.Type;

    public Schema WithId(int schemaId) => new(schemaId, Columns, IdentifierFieldIds);

    /// <summary>Structural equality ignoring the schema ID, used to reuse an existing schema on evolution.</summary>
    public bool SameSchema(Schema other) => Struct.Equals(other.Struct) && IdentifierFieldIds.SequenceEqual(other.IdentifierFieldIds);

    public int MinFormatVersion => _byId.Values.Select(f => f.Type.MinFormatVersion).DefaultIfEmpty(1).Max();

    public override string ToString() => $"table {{\n  {string.Join("\n  ", Columns)}\n}}";
}

public static class TypeUtil
{
    public static Dictionary<int, NestedField> IndexById(StructType root)
    {
        var index = new Dictionary<int, NestedField>();
        void Visit(NestedField f)
        {
            if (!index.TryAdd(f.Id, f)) throw new ArgumentException($"Duplicate field id {f.Id}");
            VisitType(f.Type);
        }
        void VisitType(IcebergType t)
        {
            switch (t)
            {
                case StructType s: foreach (var f in s.Fields) Visit(f); break;
                case ListType l: Visit(l.ElementField); break;
                case MapType m: Visit(m.KeyField); Visit(m.ValueField); break;
            }
        }
        VisitType(root);
        return index;
    }

    /// <summary>Full dotted names, using the spec's <c>element</c>/<c>key</c>/<c>value</c> path segments.</summary>
    public static Dictionary<string, int> IndexByName(StructType root)
    {
        var index = new Dictionary<string, int>();
        void VisitType(IcebergType t, string prefix)
        {
            switch (t)
            {
                case StructType s:
                    foreach (var f in s.Fields)
                    {
                        var name = prefix + f.Name;
                        index[name] = f.Id;
                        VisitType(f.Type, name + ".");
                    }
                    break;
                case ListType l:
                    index[prefix + "element"] = l.ElementId;
                    VisitType(l.ElementType, prefix + "element.");
                    break;
                case MapType m:
                    index[prefix + "key"] = m.KeyId;
                    VisitType(m.KeyType, prefix + "key.");
                    index[prefix + "value"] = m.ValueId;
                    VisitType(m.ValueType, prefix + "value.");
                    break;
            }
        }
        VisitType(root, "");
        return index;
    }

    /// <summary>
    /// Allowed type promotions ("Schema Evolution" in the spec). <paramref name="formatVersion"/> gates the
    /// v3 promotions (date to timestamp/timestamp_ns, and anything to/from <c>unknown</c>).
    /// </summary>
    public static bool IsPromotionAllowed(IcebergType from, IcebergType to, int formatVersion = 2)
    {
        if (from.Equals(to)) return true;
        return (from, to) switch
        {
            ({ TypeId: TypeId.Int }, { TypeId: TypeId.Long }) => true,
            ({ TypeId: TypeId.Float }, { TypeId: TypeId.Double }) => true,
            (DecimalType a, DecimalType b) => a.Scale == b.Scale && b.Precision >= a.Precision,
            ({ TypeId: TypeId.Date }, { TypeId: TypeId.Timestamp or TypeId.TimestampNs }) => formatVersion >= 3,
            ({ TypeId: TypeId.Unknown }, _) => formatVersion >= 3,
            _ => false,
        };
    }

    /// <summary>
    /// Reassign every field ID in <paramref name="schema"/> using <paramref name="nextId"/>, depth-first
    /// with siblings numbered before children (matches Java/PyIceberg fresh-ID assignment on create).
    /// </summary>
    public static Schema AssignFreshIds(Schema schema, Func<int> nextId, int schemaId = 0)
    {
        var mapping = new Dictionary<int, int>();
        StructType ReassignStruct(StructType s)
        {
            var ids = s.Fields.Select(f => mapping[f.Id] = nextId()).ToList();
            return new StructType(s.Fields.Select((f, i) => f with { Id = ids[i], Type = Reassign(f.Type) }));
        }
        IcebergType Reassign(IcebergType t) => t switch
        {
            StructType s => ReassignStruct(s),
            ListType l => ReassignList(l),
            MapType m => ReassignMap(m),
            _ => t,
        };
        ListType ReassignList(ListType l)
        {
            var id = mapping[l.ElementId] = nextId();
            return l with { ElementId = id, ElementType = Reassign(l.ElementType) };
        }
        MapType ReassignMap(MapType m)
        {
            var k = mapping[m.KeyId] = nextId();
            var v = mapping[m.ValueId] = nextId();
            return m with { KeyId = k, ValueId = v, KeyType = Reassign(m.KeyType), ValueType = Reassign(m.ValueType) };
        }
        var root = ReassignStruct(schema.Struct);
        return new Schema(schemaId, root.Fields, schema.IdentifierFieldIds.Select(id => mapping[id]));
    }
}
