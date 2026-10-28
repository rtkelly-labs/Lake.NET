namespace Iceberg.Net.Linq;

/// <summary>Whether a column derived from a model member may hold nulls.</summary>
public enum IcebergNullability
{
    /// <summary>From the member's type: non-nullable value types and non-nullable reference types (NRT) are required.</summary>
    Inferred,
    Required,
    Optional,
}

/// <summary>
/// Binds a property to an Iceberg field. With an <see cref="Id"/> the property is bound by field ID (robust to renames,
/// and the only binding Iceberg guarantees); without one, by <see cref="Name"/> or the property name.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Parameter, AllowMultiple = false)]
public sealed class IcebergFieldAttribute : Attribute
{
    public IcebergFieldAttribute() { }

    public IcebergFieldAttribute(int id) => Id = id;

    /// <summary>The field ID, or 0 to bind by name.</summary>
    public int Id { get; }

    /// <summary>The column name, when it differs from the property name (ignored for binding when <see cref="Id"/> is set).</summary>
    public string? Name { get; set; }

    /// <summary>
    /// The Iceberg type in the spec's text form (<c>decimal(18, 2)</c>, <c>timestamptz</c>, <c>date</c>, <c>fixed[16]</c>), for
    /// <see cref="IcebergSchema.FromType{T}"/>. Defaults follow the member type (see docs/linq.md).
    /// </summary>
    public string? Type { get; set; }

    public IcebergNullability Nullability { get; set; } = IcebergNullability.Inferred;

    /// <summary>Column documentation, for <see cref="IcebergSchema.FromType{T}"/>.</summary>
    public string? Doc { get; set; }
}

/// <summary>Excludes a property from mapping.</summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public sealed class IcebergIgnoreAttribute : Attribute;

/// <summary>Marks a property as part of the table's identifier fields (the default upsert key).</summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public sealed class IcebergKeyAttribute : Attribute;

/// <summary>
/// Instructs the Iceberg source generator to emit compile-time, zero-reflection row mapping and schema definitions for this type.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = false)]
public sealed class IcebergMappableAttribute : Attribute;
