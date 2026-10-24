using Iceberg.Net.Metadata;

namespace Iceberg.Net.Types;

/// <summary>
/// Schema evolution (spec "Schema Evolution"), as Java's <c>UpdateSchema</c>: add, delete, rename, widen, reorder,
/// document and re-key columns. Nested fields are named by dotted path (<c>location.lat</c>; list elements and map
/// keys/values are <c>element</c>, <c>key</c> and <c>value</c>, e.g. <c>events.element.kind</c>), except where a
/// top-level column has that exact name. Changes are applied in order to a copy of the table's current schema; new
/// field IDs continue from the table's <c>last-column-id</c> and are never reused. Changes that could break existing
/// readers or data (making a column required, adding a required column without a default) need
/// <see cref="AllowIncompatibleChanges"/>.
/// </summary>
public sealed class SchemaUpdate
{
    private readonly TableMetadata _base;
    private readonly List<NestedField> _columns;
    private List<int> _identifiers;
    private int _lastColumnId;
    private bool _allowIncompatible;

    public SchemaUpdate(TableMetadata metadata)
    {
        _base = metadata;
        _columns = [.. metadata.CurrentSchema.Columns];
        _identifiers = [.. metadata.CurrentSchema.IdentifierFieldIds];
        _lastColumnId = metadata.LastColumnId;
    }

    public SchemaUpdate AllowIncompatibleChanges() { _allowIncompatible = true; return this; }

    private int IndexOf(string name) =>
        _columns.FindIndex(c => c.Name == name) is var i and >= 0 ? i : throw new ArgumentException($"Column '{name}' does not exist");

    public SchemaUpdate AddColumn(string name, IcebergType type, string? doc = null)
    {
        EnsureFree(name);
        _columns.Add(new NestedField(NextId(type), name, FreshIds(type), false, doc));
        return this;
    }

    /// <summary>Add an optional field to the struct at <paramref name="parent"/> (a dotted path).</summary>
    public SchemaUpdate AddColumn(string parent, string name, IcebergType type, string? doc = null)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Column name is empty");
        UpdateNested(parent, f =>
        {
            if (f.Type is not StructType s) throw new ArgumentException($"'{parent}' is not a struct");
            if (s.Fields.Any(x => x.Name == name)) throw new ArgumentException($"Field '{parent}.{name}' already exists");
            var id = NextId(type);
            return f with { Type = new StructType([.. s.Fields, new NestedField(id, name, FreshIds(type), false, doc)]) };
        });
        return this;
    }

    /// <summary>
    /// A required column: existing rows need a value, so v3 tables need <paramref name="initialDefault"/> (the value
    /// readers fill in for files written before the column existed); otherwise only with incompatible changes allowed.
    /// </summary>
    public SchemaUpdate AddRequiredColumn(string name, IcebergType type, object? initialDefault = null, string? doc = null)
    {
        EnsureFree(name);
        if (initialDefault is null && !_allowIncompatible)
            throw new InvalidOperationException($"Adding required column '{name}' without an initial default breaks reading existing data");
        if (initialDefault is not null && _base.FormatVersion < 3)
            throw new InvalidOperationException("Column default values need format v3");
        // Stored in metadata as the JSON single-value form (spec Appendix D).
        var json = initialDefault is null ? null : Json.SingleValueJson.ToJson(type, type is PrimitiveType ? Expressions.Literals.Convert(type, initialDefault, name) : initialDefault);
        _columns.Add(new NestedField(NextId(type), name, FreshIds(type), true, doc, json, json?.DeepClone()));
        return this;
    }

    public SchemaUpdate DeleteColumn(string name)
    {
        if (!IsTopLevel(name))
        {
            UpdateNested(name, f =>
            {
                var ids = AllIds(f).ToHashSet();
                if (_identifiers.Any(ids.Contains)) throw new InvalidOperationException($"Cannot delete identifier field '{name}'");
                if (_base.DefaultSpec.Fields.Any(pf => pf.AllSourceIds.Any(ids.Contains))) throw new InvalidOperationException($"Cannot delete '{name}': the current partition spec uses it");
                if (_base.DefaultSortOrder.Fields.Any(sf => ids.Contains(sf.SourceId))) throw new InvalidOperationException($"Cannot delete '{name}': the default sort order uses it");
                return null;
            });
            return this;
        }
        var field = _columns[IndexOf(name)];
        if (_identifiers.Contains(field.Id)) throw new InvalidOperationException($"Cannot delete identifier column '{name}'; change the identifier fields first");
        if (_base.DefaultSpec.Fields.Any(f => f.AllSourceIds.Contains(field.Id)))
            throw new InvalidOperationException($"Cannot delete '{name}': the current partition spec uses it");
        if (_base.DefaultSortOrder.Fields.Any(f => f.SourceId == field.Id))
            throw new InvalidOperationException($"Cannot delete '{name}': the default sort order uses it");
        _columns.RemoveAt(IndexOf(name));
        return this;
    }

    /// <summary>Rename: data files are read by field ID, so existing data is untouched.</summary>
    public SchemaUpdate RenameColumn(string name, string newName)
    {
        if (!IsTopLevel(name)) { UpdateNested(name, f => f with { Name = newName }); return this; }
        var i = IndexOf(name);
        EnsureFree(newName);
        _columns[i] = _columns[i] with { Name = newName };
        return this;
    }

    /// <summary>Widen a column's type (int→long, float→double, decimal precision, v3 date→timestamp).</summary>
    public SchemaUpdate UpdateColumn(string name, PrimitiveType newType)
    {
        if (!IsTopLevel(name))
        {
            UpdateNested(name, f => TypeUtil.IsPromotionAllowed(f.Type, newType, _base.FormatVersion)
                ? f with { Type = newType }
                : throw new InvalidOperationException($"Cannot change '{name}' from {f.Type} to {newType}"));
            return this;
        }
        var i = IndexOf(name);
        if (!TypeUtil.IsPromotionAllowed(_columns[i].Type, newType, _base.FormatVersion))
            throw new InvalidOperationException($"Cannot change '{name}' from {_columns[i].Type} to {newType}");
        _columns[i] = _columns[i] with { Type = newType };
        return this;
    }

    public SchemaUpdate UpdateDoc(string name, string? doc)
    {
        if (!IsTopLevel(name)) { UpdateNested(name, f => f with { Doc = doc }); return this; }
        var i = IndexOf(name);
        _columns[i] = _columns[i] with { Doc = doc };
        return this;
    }

    public SchemaUpdate MakeOptional(string name)
    {
        if (!IsTopLevel(name))
        {
            UpdateNested(name, f => _identifiers.Contains(f.Id) ? throw new InvalidOperationException($"Identifier field '{name}' must stay required") : f with { Required = false });
            return this;
        }
        var i = IndexOf(name);
        if (_identifiers.Contains(_columns[i].Id)) throw new InvalidOperationException($"Identifier column '{name}' must stay required");
        _columns[i] = _columns[i] with { Required = false };
        return this;
    }

    /// <summary>Existing files may hold nulls, so this needs incompatible changes allowed.</summary>
    public SchemaUpdate RequireColumn(string name)
    {
        if (!_allowIncompatible) throw new InvalidOperationException($"Making '{name}' required can break existing data; allow incompatible changes to force it");
        var i = IndexOf(name);
        _columns[i] = _columns[i] with { Required = true };
        return this;
    }

    public SchemaUpdate SetIdentifierFields(params string[] names)
    {
        _identifiers = names.Select(n => _columns[IndexOf(n)].Id).ToList();
        return this;
    }

    public SchemaUpdate MoveFirst(string name) => Move(name, 0);
    public SchemaUpdate MoveBefore(string name, string other) => Move(name, () => IndexOf(other));
    public SchemaUpdate MoveAfter(string name, string other) => Move(name, () => IndexOf(other) + 1);

    private SchemaUpdate Move(string name, int index) => Move(name, () => index);

    private SchemaUpdate Move(string name, Func<int> target)
    {
        var field = _columns[IndexOf(name)];
        _columns.Remove(field);
        _columns.Insert(target(), field);
        return this;
    }

    /// <summary>The evolved schema (validated: identifier rules, format-version type requirements).</summary>
    public Schema Apply()
    {
        // The next free schema ID, as Java's client sends it (servers such as Nessie do not renumber an add-schema).
        var schema = new Schema(_base.Schemas.Max(s => s.SchemaId) + 1, _columns, _identifiers);
        if (schema.MinFormatVersion > _base.FormatVersion)
            throw new InvalidOperationException($"The schema needs format v{schema.MinFormatVersion}; the table is v{_base.FormatVersion}");
        return schema;
    }

    /// <summary>Metadata updates and the requirements that make them safe to apply (REST <c>commitTable</c> shape).</summary>
    public (IReadOnlyList<UpdateRequirement> Requirements, IReadOnlyList<MetadataUpdate> Updates) ToCommit() =>
        ([new UpdateRequirement.AssertCurrentSchemaId(_base.CurrentSchemaId), new UpdateRequirement.AssertLastAssignedFieldId(_base.LastColumnId)],
         [new MetadataUpdate.AddSchema(Apply()), new MetadataUpdate.SetCurrentSchema(MetadataUpdate.LastAdded)]);

    private bool IsTopLevel(string name) => !name.Contains('.') || _columns.Any(c => c.Name == name);

    /// <summary>Replace (or with null, remove) the nested field at a dotted path, rebuilding its ancestors.</summary>
    private void UpdateNested(string path, Func<NestedField, NestedField?> change)
    {
        string[] rest;
        var top = _columns.FindIndex(c => c.Name == path);
        if (top >= 0) rest = [];
        else
        {
            var parts = path.Split('.');
            top = IndexOf(parts[0]);
            rest = parts.Skip(1).ToArray();
        }
        var updated = Descend(_columns[top], rest, change, path);
        if (updated is null) throw new InvalidOperationException($"Use DeleteColumn on top-level column '{path}'");
        _columns[top] = updated;
    }

    private static NestedField? Descend(NestedField f, string[] rest, Func<NestedField, NestedField?> change, string path)
    {
        if (rest.Length == 0) return change(f);
        var (segment, tail) = (rest[0], rest.Skip(1).ToArray());
        switch (f.Type)
        {
            case StructType s:
            {
                var fields = s.Fields.ToList();
                var i = fields.FindIndex(x => x.Name == segment);
                if (i < 0) throw new ArgumentException($"Field '{path}' does not exist");
                var child = Descend(fields[i], tail, change, path);
                if (child is null) fields.RemoveAt(i); else fields[i] = child;
                if (fields.Count == 0) throw new InvalidOperationException($"Cannot delete the last field of struct '{f.Name}'");
                return f with { Type = new StructType(fields) };
            }
            case ListType l when segment == "element":
            {
                var e = Descend(l.ElementField, tail, change, path) ?? throw new InvalidOperationException("Cannot delete a list element");
                if (e.Name != "element") throw new InvalidOperationException("Cannot rename a list element");
                return f with { Type = new ListType(l.ElementId, e.Type, e.Required) };
            }
            case MapType m when segment == "value":
            {
                var v = Descend(m.ValueField, tail, change, path) ?? throw new InvalidOperationException("Cannot delete a map value");
                if (v.Name != "value") throw new InvalidOperationException("Cannot rename a map value");
                return f with { Type = new MapType(m.KeyId, m.KeyType, m.ValueId, v.Type, v.Required) };
            }
            case MapType m when segment == "key":
            {
                var k = Descend(m.KeyField, tail, change, path) ?? throw new InvalidOperationException("Cannot delete a map key");
                if (k.Name != "key" || !k.Required) throw new InvalidOperationException("Map keys keep their name and stay required");
                if (k.Type is PrimitiveType && !k.Type.Equals(m.KeyType)) throw new InvalidOperationException("Map key types cannot change");
                return f with { Type = new MapType(m.KeyId, k.Type, m.ValueId, m.ValueType, m.ValueRequired) };
            }
            default: throw new ArgumentException($"Field '{path}' does not exist");
        }
    }

    private static IEnumerable<int> AllIds(NestedField f) => f.Type switch
    {
        StructType s => s.Fields.SelectMany(AllIds).Prepend(f.Id),
        ListType l => AllIds(l.ElementField).Prepend(f.Id),
        MapType m => AllIds(m.KeyField).Concat(AllIds(m.ValueField)).Prepend(f.Id),
        _ => [f.Id],
    };

    private void EnsureFree(string name)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Column name is empty");
        if (_columns.Any(c => c.Name == name)) throw new ArgumentException($"Column '{name}' already exists");
    }

    /// <summary>Nested types carry field IDs of their own; give them fresh ones.</summary>
    private IcebergType FreshIds(IcebergType type) => type switch
    {
        StructType s => new StructType(s.Fields.Select(f => f with { Id = NextId(f.Type), Type = FreshIds(f.Type) }).ToList()),
        ListType l => new ListType(NextId(l.ElementType), FreshIds(l.ElementType), l.ElementRequired),
        MapType m => new MapType(NextId(m.KeyType), FreshIds(m.KeyType), NextId(m.ValueType), FreshIds(m.ValueType), m.ValueRequired),
        _ => type,
    };

    /// <summary>The next column ID; a <c>file</c> field also takes the six after it for its implicit sub-fields.</summary>
    private int NextId(IcebergType type)
    {
        var id = ++_lastColumnId;
        if (type is FileType) _lastColumnId += FileType.ReservedIds - 1;
        return id;
    }
}
