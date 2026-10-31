namespace Lake.TestSupport;

/// <summary>The column types every format supports, by their physical value in a row.</summary>
public enum ColumnKind
{
    /// <summary>64-bit integer: <see cref="long"/>.</summary>
    Long,
    /// <summary>UTF-8 text: <see cref="string"/>.</summary>
    String,
    /// <summary>Days since 1970-01-01: <see cref="int"/>.</summary>
    Date,
    /// <summary>IEEE 754 double: <see cref="double"/>.</summary>
    Double,
}

/// <summary>A table's columns, the first being the key.</summary>
public sealed record TestSchema(IReadOnlyList<(string Name, ColumnKind Kind)> Columns)
{
    public string Key => Columns[0].Name;

    /// <summary>id (long, key), name (string), day (date), score (double): the shape the common scenarios use.</summary>
    public static TestSchema Standard { get; } = new([("id", ColumnKind.Long), ("name", ColumnKind.String), ("day", ColumnKind.Date), ("score", ColumnKind.Double)]);
}

/// <summary>What a format can do; a scenario needing a missing capability expects the documented refusal instead.</summary>
[Flags]
public enum Capabilities
{
    None = 0,
    Write = 1,
    Upsert = 2,
    DeleteByKey = 4,
    TimeTravel = 8,
    AddColumn = 16,
    ConcurrentWriters = 32,
}

/// <summary>
/// One format behind the common scenarios. A driver translates the neutral operations into the format's own API and
/// nothing else: no scenario logic lives in a driver, so every format is held to the same assertions.
/// </summary>
public interface IFormatDriver : IAsyncDisposable
{
    /// <summary>The format's name in test output: "iceberg-v2", "ducklake-1.1", "delta".</summary>
    string Name { get; }

    Capabilities Capabilities { get; }

    Task<ITestTable> CreateTableAsync(string name, TestSchema schema);
}

/// <summary>A table under test. Rows are physical values in schema column order.</summary>
public interface ITestTable
{
    Task InsertAsync(IReadOnlyList<object?[]> rows);

    /// <summary>Insert rows, replacing any existing row with the same key.</summary>
    Task UpsertAsync(IReadOnlyList<object?[]> rows);

    Task DeleteByKeyAsync(IReadOnlyList<long> keys);

    /// <summary>Append a nullable string column; later rows may carry it.</summary>
    Task AddColumnAsync(string name);

    /// <summary>The current version (snapshot id, DuckLake snapshot, Delta version), or null before the first commit.</summary>
    Task<long?> CurrentVersionAsync();

    /// <summary>Every row, at the current version or at <paramref name="version"/>.</summary>
    Task<List<object?[]>> ReadAsync(long? version = null);

    /// <summary>A second handle on the same table, as another writer would open it.</summary>
    Task<ITestTable> OpenAgainAsync();

    /// <summary>The format's own validator (fsck) in deep mode; throws with every problem when the table is invalid.</summary>
    Task ValidateAsync();
}
