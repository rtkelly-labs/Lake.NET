using Iceberg.Net.Metadata;
using Iceberg.Net.Transforms;

namespace Iceberg.Net.Partitioning;

/// <summary>
/// Partition evolution (spec "Partition Evolution"), as Java's <c>UpdatePartitionSpec</c>: start from the table's
/// default spec, add, remove and rename fields, and commit the result as a new default spec. A field added with a
/// source and transform some earlier spec already had keeps that field's ID (so metrics and manifests line up across
/// specs); other new fields get IDs above <c>last-partition-id</c>. On format v1, where field positions are fixed, a
/// removed field becomes a <c>void</c> transform instead.
/// </summary>
public sealed class PartitionSpecUpdate
{
    private readonly TableMetadata _base;
    private readonly List<PartitionField> _fields;
    private int _lastPartitionId;

    public PartitionSpecUpdate(TableMetadata metadata)
    {
        _base = metadata;
        _fields = [.. metadata.DefaultSpec.Fields];
        _lastPartitionId = metadata.LastPartitionId;
    }

    /// <summary>Partition by <paramref name="transform"/> of a column (a dotted name for a field inside structs).</summary>
    public PartitionSpecUpdate AddField(string sourceColumn, Transform transform, string? name = null)
    {
        var schema = _base.CurrentSchema;
        var probe = PartitionSpec.BuilderFor(schema).Add(sourceColumn, transform, name).Build().Fields[0]; // validates, names
        if (_fields.Any(f => f.SourceId == probe.SourceId && f.Transform.Equals(transform) && f.Transform is not VoidTransform))
            throw new ArgumentException($"The spec already partitions by {transform}({sourceColumn})");
        if (_fields.Any(f => f.Name == probe.Name)) throw new ArgumentException($"Partition field '{probe.Name}' already exists");
        var reused = _base.PartitionSpecs.SelectMany(s => s.Fields).FirstOrDefault(f => f.SourceId == probe.SourceId && f.Transform.Equals(transform));
        var id = reused is not null && _fields.All(f => f.FieldId != reused.FieldId) ? reused.FieldId : ++_lastPartitionId;
        var field = probe with { FieldId = id };
        var voided = _base.FormatVersion == 1 ? _fields.FindIndex(f => f.Transform is VoidTransform && f.SourceId == field.SourceId) : -1;
        if (voided >= 0) _fields[voided] = field; else _fields.Add(field);
        return this;
    }

    public PartitionSpecUpdate AddField(string sourceColumn) => AddField(sourceColumn, Transform.Identity);

    public PartitionSpecUpdate RemoveField(string name)
    {
        var i = _fields.FindIndex(f => f.Name == name);
        if (i < 0) throw new ArgumentException($"Partition field '{name}' does not exist");
        if (_base.FormatVersion == 1) _fields[i] = _fields[i] with { Transform = Transform.Void };
        else _fields.RemoveAt(i);
        return this;
    }

    public PartitionSpecUpdate RenameField(string name, string newName)
    {
        var i = _fields.FindIndex(f => f.Name == name);
        if (i < 0) throw new ArgumentException($"Partition field '{name}' does not exist");
        if (_fields.Any(f => f.Name == newName)) throw new ArgumentException($"Partition field '{newName}' already exists");
        _fields[i] = _fields[i] with { Name = newName };
        return this;
    }

    /// <summary>The evolved spec, numbered with the next free spec ID (an identical existing spec is reused on commit).</summary>
    public PartitionSpec Apply() => new(_base.PartitionSpecs.Max(s => s.SpecId) + 1, _fields);

    /// <summary>Requirements (the default spec and last partition ID have not moved) and updates (add + make default).</summary>
    public (IReadOnlyList<UpdateRequirement> Requirements, IReadOnlyList<MetadataUpdate> Updates) ToCommit() =>
        ([new UpdateRequirement.AssertDefaultSpecId(_base.DefaultSpecId), new UpdateRequirement.AssertLastAssignedPartitionId(_base.LastPartitionId)],
         [new MetadataUpdate.AddPartitionSpec(Apply()), new MetadataUpdate.SetDefaultPartitionSpec(MetadataUpdate.LastAdded)]);
}
