using Iceberg.Net.Metadata;

namespace Iceberg.Net.Data;

/// <summary>Bindings and input arrays belong to the table identity/schema observed when a command was created.</summary>
internal sealed class WritePlanGuard(TableMetadata metadata)
{
    internal Guid TableUuid { get; } = metadata.TableUuid;
    internal int SchemaId { get; } = metadata.CurrentSchemaId;
    internal int SpecId { get; } = metadata.DefaultSpecId;

    internal void Validate(TableMetadata current, bool partitionSensitive = false)
    {
        ValidateIdentity(current);
        if (current.CurrentSchemaId != SchemaId)
            throw new ValidationException("Table schema changed while this write was planned; rebuild the command and its input bindings against the current schema");
        if (partitionSensitive && current.DefaultSpecId != SpecId)
            throw new ValidationException("Partition spec changed while a file-filter write was planned; rebuild the filter against the current table");
    }

    internal void ValidateIdentity(TableMetadata current)
    {
        if (current.TableUuid != TableUuid)
            throw new ValidationException("Table UUID changed while this write was planned; do not apply the command to a replacement table");
    }
}
