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

    /// <summary>
    /// Positions from a row (aligned to <see cref="Columns"/>) down to field <paramref name="id"/> through enclosing
    /// structs — <c>[2, 0]</c> for the first field of the third column's struct — or null when the field is inside a
    /// list or map (no single value per row) or absent.
    /// </summary>
    public IReadOnlyList<int>? StructPath(int id)
    {
        List<int>? Find(IReadOnlyList<NestedField> fields)
        {
            for (var i = 0; i < fields.Count; i++)
            {
                if (fields[i].Id == id) return [i];
                if (fields[i].Type is StructType s && Find(s.Fields) is { } rest) return [i, .. rest];
            }
            return null;
        }
        return Find(Columns);
    }

    /// <summary>Whether a field can never be null in a row: it and every struct enclosing it are required.</summary>
    public bool AlwaysPresent(int id)
    {
        var path = StructPath(id);
        if (path is null) return false;
        IReadOnlyList<NestedField> fields = Columns;
        foreach (var p in path)
        {
            if (!fields[p].Required) return false;
            fields = fields[p].Type is StructType s ? s.Fields : [];
        }
        return true;
    }

    /// <summary>Reads field <paramref name="id"/> from a row through its enclosing structs (null when one of them is null).</summary>
    public Func<object?[], object?> Accessor(int id)
    {
        var path = StructPath(id) ?? throw new NotSupportedException($"Field {id} is not reachable through structs (it is inside a list or map, or absent)");
        if (path.Count == 1) return row => row[path[0]];
        return row => Navigate(row, path);
    }

    public static object? Navigate(IReadOnlyList<object?> row, IReadOnlyList<int> path)
    {
        var v = row[path[0]];
        for (var i = 1; i < path.Count; i++)
        {
            if (v is not object?[] fields) return null;
            v = fields[path[i]];
        }
        return v;
    }

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
            if (f.Type is FileType)
                foreach (var sub in FileType.SubFields(f.Id)) Visit(sub); // implicit, but their IDs are taken
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
        void Add(string name, int id, IcebergType t)
        {
            index[name] = id;
            if (t is FileType)
                foreach (var sub in FileType.SubFields(id)) index[name + "." + sub.Name] = sub.Id;
            VisitType(t, name + ".");
        }
        void VisitType(IcebergType t, string prefix)
        {
            switch (t)
            {
                case StructType s:
                    foreach (var f in s.Fields) Add(prefix + f.Name, f.Id, f.Type);
                    break;
                case ListType l:
                    Add(prefix + "element", l.ElementId, l.ElementType);
                    break;
                case MapType m:
                    Add(prefix + "key", m.KeyId, m.KeyType);
                    Add(prefix + "value", m.ValueId, m.ValueType);
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
        // A file field takes its own ID and the next six (its implicit sub-fields).
        int Next(IcebergType t)
        {
            var id = nextId();
            if (t is FileType)
                for (var i = 1; i < FileType.ReservedIds; i++)
                    if (nextId() != id + i) throw new ArgumentException("A file field needs consecutive field IDs");
            return id;
        }
        StructType ReassignStruct(StructType s)
        {
            var ids = s.Fields.Select(f => mapping[f.Id] = Next(f.Type)).ToList();
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
            var id = mapping[l.ElementId] = Next(l.ElementType);
            return l with { ElementId = id, ElementType = Reassign(l.ElementType) };
        }
        MapType ReassignMap(MapType m)
        {
            var k = mapping[m.KeyId] = Next(m.KeyType);
            var v = mapping[m.ValueId] = Next(m.ValueType);
            return m with { KeyId = k, ValueId = v, KeyType = Reassign(m.KeyType), ValueType = Reassign(m.ValueType) };
        }
        var root = ReassignStruct(schema.Struct);
        return new Schema(schemaId, root.Fields, schema.IdentifierFieldIds.Select(id => mapping[id]));
    }
}
