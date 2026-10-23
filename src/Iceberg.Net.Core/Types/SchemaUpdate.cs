using Iceberg.Net.Metadata;

namespace Iceberg.Net.Types;

/// <summary>
/// Schema evolution (spec "Schema Evolution"), as Java's <c>UpdateSchema</c>: add, delete, rename, widen, reorder,
/// document and re-key top-level columns. Changes are applied in order to a copy of the table's current schema; new
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
        _columns.Add(new NestedField(++_lastColumnId, name, FreshIds(type), false, doc));
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
        _columns.Add(new NestedField(++_lastColumnId, name, FreshIds(type), true, doc, json, json?.DeepClone()));
        return this;
    }

    public SchemaUpdate DeleteColumn(string name)
    {
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
        var i = IndexOf(name);
        EnsureFree(newName);
        _columns[i] = _columns[i] with { Name = newName };
        return this;
    }

    /// <summary>Widen a column's type (int→long, float→double, decimal precision, v3 date→timestamp).</summary>
    public SchemaUpdate UpdateColumn(string name, PrimitiveType newType)
    {
        var i = IndexOf(name);
        if (!TypeUtil.IsPromotionAllowed(_columns[i].Type, newType, _base.FormatVersion))
            throw new InvalidOperationException($"Cannot change '{name}' from {_columns[i].Type} to {newType}");
        _columns[i] = _columns[i] with { Type = newType };
        return this;
    }

    public SchemaUpdate UpdateDoc(string name, string? doc)
    {
        var i = IndexOf(name);
        _columns[i] = _columns[i] with { Doc = doc };
        return this;
    }

    public SchemaUpdate MakeOptional(string name)
    {
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
        var schema = new Schema(_base.CurrentSchemaId, _columns, _identifiers);
        if (schema.MinFormatVersion > _base.FormatVersion)
            throw new InvalidOperationException($"The schema needs format v{schema.MinFormatVersion}; the table is v{_base.FormatVersion}");
        return schema;
    }

    /// <summary>Metadata updates and the requirements that make them safe to apply (REST <c>commitTable</c> shape).</summary>
    public (IReadOnlyList<UpdateRequirement> Requirements, IReadOnlyList<MetadataUpdate> Updates) ToCommit() =>
        ([new UpdateRequirement.AssertCurrentSchemaId(_base.CurrentSchemaId), new UpdateRequirement.AssertLastAssignedFieldId(_base.LastColumnId)],
         [new MetadataUpdate.AddSchema(Apply()), new MetadataUpdate.SetCurrentSchema(MetadataUpdate.LastAdded)]);

    private void EnsureFree(string name)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Column name is empty");
        if (_columns.Any(c => c.Name == name)) throw new ArgumentException($"Column '{name}' already exists");
    }

    /// <summary>Nested types carry field IDs of their own; give them fresh ones.</summary>
    private IcebergType FreshIds(IcebergType type) => type switch
    {
        StructType s => new StructType(s.Fields.Select(f => f with { Id = ++_lastColumnId, Type = FreshIds(f.Type) }).ToList()),
        ListType l => new ListType(++_lastColumnId, FreshIds(l.ElementType), l.ElementRequired),
        MapType m => new MapType(++_lastColumnId, FreshIds(m.KeyType), ++_lastColumnId, FreshIds(m.ValueType), m.ValueRequired),
        _ => type,
    };
}
