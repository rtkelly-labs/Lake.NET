using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Partitioning;

/// <summary>
/// A partition field. v3 allows multi-argument transforms via <see cref="SourceIds"/>; for single-source
/// fields <see cref="SourceId"/> is the only source and is what v1/v2 metadata writes.
/// </summary>
public sealed record PartitionField(int SourceId, int FieldId, string Name, Transform Transform, IReadOnlyList<int>? SourceIds = null)
{
    public IReadOnlyList<int> AllSourceIds => SourceIds ?? [SourceId];
}

public sealed class PartitionSpec
{
    /// <summary>Partition field IDs start at 1000 (spec: "last-partition-id" for an unpartitioned table is 999).</summary>
    public const int PartitionDataIdStart = 1000;

    public static readonly PartitionSpec Unpartitioned = new(0, []);

    public PartitionSpec(int specId, IEnumerable<PartitionField> fields)
    {
        SpecId = specId;
        Fields = fields.ToList();
    }

    public int SpecId { get; }
    public IReadOnlyList<PartitionField> Fields { get; }
    public bool IsUnpartitioned => Fields.All(f => f.Transform is VoidTransform);
    public int LastAssignedFieldId => Fields.Count == 0 ? PartitionDataIdStart - 1 : Fields.Max(f => f.FieldId);

    /// <summary>The struct type of partition tuples for <paramref name="schema"/>; written into manifests as <c>partition</c> (field 102).</summary>
    public StructType PartitionType(Schema schema) => new(Fields.Select(f =>
    {
        var source = schema.FindType(f.SourceId) ?? throw new InvalidOperationException($"Partition source {f.SourceId} not in schema {schema.SchemaId}");
        return NestedField.Optional(f.FieldId, f.Name, f.Transform.ResultType(source));
    }));

    /// <summary>Compute the partition tuple for a row, given a lookup from source field ID to physical value.</summary>
    public object?[] Partition(Schema schema, Func<int, object?> sourceValue) =>
        Fields.Select(f => f.Transform.Apply(schema.FindType(f.SourceId)!, sourceValue(f.SourceId))).ToArray();

    /// <summary>Hive-style relative path, e.g. <c>event_day=2024-01-02/id_bucket=3</c>.</summary>
    public string PartitionPath(Schema schema, object?[] partition)
    {
        var type = PartitionType(schema);
        return string.Join('/', Fields.Select((f, i) =>
            $"{Uri.EscapeDataString(f.Name)}={Uri.EscapeDataString(f.Transform.ToHumanString(type.Fields[i].Type, partition[i]))}"));
    }

    /// <summary>Specs are compatible (and can be reused instead of added) when fields match ignoring spec ID.</summary>
    public bool CompatibleWith(PartitionSpec other) => Fields.SequenceEqual(other.Fields);

    public static Builder BuilderFor(Schema schema) => new(schema);

    public override string ToString() => $"[{string.Join(", ", Fields.Select(f => $"{f.FieldId}: {f.Name}: {f.Transform}({f.SourceId})"))}]";

    public sealed class Builder(Schema schema)
    {
        private readonly List<PartitionField> _fields = [];
        private int _specId;
        private int _lastFieldId = PartitionDataIdStart - 1;

        public Builder WithSpecId(int id) { _specId = id; return this; }

        public Builder Add(string sourceColumn, Transform transform, string? name = null)
        {
            var source = schema.FindField(sourceColumn) ?? throw new ArgumentException($"Unknown column {sourceColumn}");
            if (!transform.CanTransform(source.Type)) throw new ArgumentException($"Cannot apply {transform} to {source.Type}");
            name ??= transform switch
            {
                IdentityTransform => sourceColumn,
                BucketTransform => $"{sourceColumn}_bucket",
                TruncateTransform => $"{sourceColumn}_trunc",
                TimeTransform t => $"{sourceColumn}_{t}",
                _ => $"{sourceColumn}_{transform}",
            };
            if (_fields.Any(f => f.Name == name)) throw new ArgumentException($"Duplicate partition name {name}");
            _fields.Add(new PartitionField(source.Id, ++_lastFieldId, name, transform));
            return this;
        }

        public Builder Identity(string col) => Add(col, Transform.Identity);
        public Builder Bucket(string col, int n) => Add(col, Transform.Bucket(n));
        public Builder Truncate(string col, int w) => Add(col, Transform.Truncate(w));
        public Builder Year(string col) => Add(col, Transform.Year);
        public Builder Month(string col) => Add(col, Transform.Month);
        public Builder Day(string col) => Add(col, Transform.Day);
        public Builder Hour(string col) => Add(col, Transform.Hour);

        public PartitionSpec Build() => new(_specId, _fields);
    }
}

public enum SortDirection { Asc, Desc }
public enum NullOrder { NullsFirst, NullsLast }

public sealed record SortField(Transform Transform, int SourceId, SortDirection Direction, NullOrder NullOrder);

public sealed class SortOrder(int orderId, IEnumerable<SortField> fields)
{
    public static readonly SortOrder Unsorted = new(0, []);

    public int OrderId { get; } = orderId;
    public IReadOnlyList<SortField> Fields { get; } = fields.ToList();
    public bool IsUnsorted => Fields.Count == 0;

    public bool SameOrder(SortOrder other) => Fields.SequenceEqual(other.Fields);
}
