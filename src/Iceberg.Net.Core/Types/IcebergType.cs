namespace Iceberg.Net.Types;

/// <summary>
/// Type IDs from the Iceberg table spec, "Schemas and Data Types".
/// The minimum format version for each type is enforced by <see cref="IcebergType.MinFormatVersion"/>.
/// </summary>
public enum TypeId
{
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Decimal,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    TimestampNs,   // v3
    TimestampTzNs, // v3
    String,
    Uuid,
    Fixed,
    Binary,
    Variant,       // v3
    Geometry,      // v3
    Geography,     // v3
    Unknown,       // v3
    Struct,
    List,
    Map,
}

public abstract record IcebergType(TypeId TypeId)
{
    public bool IsPrimitive => this is PrimitiveType;
    public bool IsNested => !IsPrimitive;

    /// <summary>The lowest table format version that allows this type.</summary>
    public virtual int MinFormatVersion => 1;

    public StructType AsStruct() => this as StructType ?? throw new InvalidOperationException($"{this} is not a struct");
}

/// <summary>A primitive type. Parameterised primitives (decimal, fixed, geo) are subclasses.</summary>
public record PrimitiveType : IcebergType
{
    protected PrimitiveType(TypeId id) : base(id) { }

    public static readonly PrimitiveType Boolean = new(TypeId.Boolean);
    public static readonly PrimitiveType Int = new(TypeId.Int);
    public static readonly PrimitiveType Long = new(TypeId.Long);
    public static readonly PrimitiveType Float = new(TypeId.Float);
    public static readonly PrimitiveType Double = new(TypeId.Double);
    public static readonly PrimitiveType Date = new(TypeId.Date);
    public static readonly PrimitiveType Time = new(TypeId.Time);
    public static readonly PrimitiveType Timestamp = new(TypeId.Timestamp);
    public static readonly PrimitiveType TimestampTz = new(TypeId.TimestampTz);
    public static readonly PrimitiveType TimestampNs = new(TypeId.TimestampNs);
    public static readonly PrimitiveType TimestampTzNs = new(TypeId.TimestampTzNs);
    public static readonly PrimitiveType String = new(TypeId.String);
    public static readonly PrimitiveType Uuid = new(TypeId.Uuid);
    public static readonly PrimitiveType Binary = new(TypeId.Binary);
    public static readonly PrimitiveType Variant = new(TypeId.Variant);
    public static readonly PrimitiveType Unknown = new(TypeId.Unknown);

    public override int MinFormatVersion => TypeId switch
    {
        TypeId.TimestampNs or TypeId.TimestampTzNs or TypeId.Variant or TypeId.Unknown => 3,
        _ => 1,
    };

    public override string ToString() => TypeId switch
    {
        TypeId.Boolean => "boolean",
        TypeId.Int => "int",
        TypeId.Long => "long",
        TypeId.Float => "float",
        TypeId.Double => "double",
        TypeId.Date => "date",
        TypeId.Time => "time",
        TypeId.Timestamp => "timestamp",
        TypeId.TimestampTz => "timestamptz",
        TypeId.TimestampNs => "timestamp_ns",
        TypeId.TimestampTzNs => "timestamptz_ns",
        TypeId.String => "string",
        TypeId.Uuid => "uuid",
        TypeId.Binary => "binary",
        TypeId.Variant => "variant",
        TypeId.Unknown => "unknown",
        _ => throw new InvalidOperationException($"Unhandled primitive {TypeId}"),
    };
}

public sealed record DecimalType : PrimitiveType
{
    public DecimalType(int precision, int scale) : base(TypeId.Decimal)
    {
        if (precision is < 1 or > 38) throw new ArgumentOutOfRangeException(nameof(precision), "Decimal precision must be 1..38");
        if (scale < 0 || scale > precision) throw new ArgumentOutOfRangeException(nameof(scale), "Decimal scale must be 0..precision");
        Precision = precision;
        Scale = scale;
    }

    public int Precision { get; }
    public int Scale { get; }

    public override string ToString() => $"decimal({Precision}, {Scale})";
}

public sealed record FixedType : PrimitiveType
{
    public FixedType(int length) : base(TypeId.Fixed)
    {
        if (length < 1) throw new ArgumentOutOfRangeException(nameof(length));
        Length = length;
    }

    public int Length { get; }

    public override string ToString() => $"fixed[{Length}]";
}

/// <summary>v3 geometry type with an optional CRS (default OGC:CRS84).</summary>
public sealed record GeometryType(string? Crs = null) : PrimitiveType(TypeId.Geometry)
{
    public override int MinFormatVersion => 3;
    public override string ToString() => Crs is null ? "geometry" : $"geometry({Crs})";
}

/// <summary>v3 geography type with optional CRS and edge-interpolation algorithm (default spherical).</summary>
public sealed record GeographyType(string? Crs = null, string? Algorithm = null) : PrimitiveType(TypeId.Geography)
{
    public override int MinFormatVersion => 3;

    public override string ToString() => (Crs, Algorithm) switch
    {
        (null, null) => "geography",
        (_, null) => $"geography({Crs})",
        _ => $"geography({Crs ?? "OGC:CRS84"}, {Algorithm})",
    };
}

/// <summary>A struct field. Field IDs are the identity of a column across schema evolution.</summary>
public sealed record NestedField(
    int Id,
    string Name,
    IcebergType Type,
    bool Required,
    string? Doc = null,
    object? InitialDefault = null,
    object? WriteDefault = null)
{
    public static NestedField Optional(int id, string name, IcebergType type, string? doc = null) => new(id, name, type, false, doc);
    public static NestedField Require(int id, string name, IcebergType type, string? doc = null) => new(id, name, type, true, doc);

    public bool IsOptional => !Required;

    public override string ToString() => $"{Id}: {Name}: {(Required ? "required" : "optional")} {Type}";
}

public sealed record StructType : IcebergType
{
    public StructType(IEnumerable<NestedField> fields) : base(TypeId.Struct)
    {
        Fields = fields.ToList();
        var dupes = Fields.GroupBy(f => f.Name).Where(g => g.Count() > 1).Select(g => g.Key).ToList();
        if (dupes.Count > 0) throw new ArgumentException($"Duplicate struct field names: {string.Join(", ", dupes)}");
    }

    public StructType(params NestedField[] fields) : this((IEnumerable<NestedField>)fields) { }

    public IReadOnlyList<NestedField> Fields { get; }

    public NestedField? Field(int id) => Fields.FirstOrDefault(f => f.Id == id);
    public NestedField? Field(string name) => Fields.FirstOrDefault(f => f.Name == name);

    public bool Equals(StructType? other) => other is not null && Fields.SequenceEqual(other.Fields);
    public override int GetHashCode() => Fields.Aggregate(17, (h, f) => HashCode.Combine(h, f));

    public override string ToString() => $"struct<{string.Join(", ", Fields)}>";
}

public sealed record ListType(int ElementId, IcebergType ElementType, bool ElementRequired) : IcebergType(TypeId.List)
{
    public NestedField ElementField => new(ElementId, "element", ElementType, ElementRequired);
    public override string ToString() => $"list<{ElementType}>";
}

public sealed record MapType(int KeyId, IcebergType KeyType, int ValueId, IcebergType ValueType, bool ValueRequired) : IcebergType(TypeId.Map)
{
    public NestedField KeyField => new(KeyId, "key", KeyType, true);
    public NestedField ValueField => new(ValueId, "value", ValueType, ValueRequired);
    public override string ToString() => $"map<{KeyType}, {ValueType}>";
}
