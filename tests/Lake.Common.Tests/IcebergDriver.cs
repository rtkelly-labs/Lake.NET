using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Types;
using Lake.TestSupport;

namespace Lake.Common.Tests;

/// <summary>Iceberg behind the common scenarios: an in-memory catalog on the local filesystem, one per driver.</summary>
public sealed class IcebergDriver(int formatVersion, DeleteStrategy strategy) : IFormatDriver
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "lake-common", Guid.NewGuid().ToString("N"));
    private InMemoryCatalog? _catalog;

    public string Name => $"iceberg-v{formatVersion}-{strategy.ToString().ToLowerInvariant()}";

    public Capabilities Capabilities => Capabilities.Write | Capabilities.Upsert | Capabilities.DeleteByKey | Capabilities.TimeTravel
        | Capabilities.AddColumn | Capabilities.ConcurrentWriters;

    public async Task<ITestTable> CreateTableAsync(string name, TestSchema schema)
    {
        if (_catalog is null)
        {
            _catalog = new InMemoryCatalog(_warehouse);
            await _catalog.CreateNamespaceAsync(Namespace.Of("common"));
        }
        var id = 1;
        var fields = schema.Columns.Select(c => new NestedField(id++, c.Name, c.Kind switch
        {
            ColumnKind.Long => PrimitiveType.Long,
            ColumnKind.String => PrimitiveType.String,
            ColumnKind.Date => PrimitiveType.Date,
            ColumnKind.Double => PrimitiveType.Double,
            _ => throw new ArgumentOutOfRangeException(nameof(schema)),
        }, Required: c.Name == schema.Key)).ToList();
        var table = await _catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(Namespace.Of("common"), name),
            new Schema(0, fields, identifierFieldIds: [1]), FormatVersion: formatVersion));
        return new IcebergTestTable(_catalog, table.Identifier, schema.Key, strategy);
    }

    public ValueTask DisposeAsync()
    {
        _catalog?.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
        return ValueTask.CompletedTask;
    }

    private sealed class IcebergTestTable(InMemoryCatalog catalog, TableIdentifier id, string key, DeleteStrategy strategy) : ITestTable
    {
        private Task<Table> LoadAsync() => catalog.LoadTableAsync(id);

        public async Task InsertAsync(IReadOnlyList<object?[]> rows) => await TableMutations.InsertAsync(await LoadAsync(), rows);

        public async Task UpsertAsync(IReadOnlyList<object?[]> rows) => await TableMutations.UpsertAsync(await LoadAsync(), rows, [key], strategy);

        public async Task DeleteByKeyAsync(IReadOnlyList<long> keys) =>
            await TableMutations.DeleteByKeysAsync(await LoadAsync(), [key], keys.Select(k => new object?[] { k }), strategy);

        public async Task AddColumnAsync(string name) => await (await LoadAsync()).UpdateSchemaAsync(u => u.AddColumn(name, PrimitiveType.String));

        public async Task<long?> CurrentVersionAsync() => (await LoadAsync()).Metadata.CurrentSnapshotId;

        public async Task<List<object?[]>> ReadAsync(long? version = null)
        {
            var table = await LoadAsync();
            var rows = new List<object?[]>();
            var request = new ScanRequest { Snapshot = version is { } v ? SnapshotSelector.Id(v) : SnapshotSelector.Current };
            await foreach (var row in TableScan.ReadRowsAsync(table, request)) rows.Add(row);
            return rows;
        }

        public Task<ITestTable> OpenAgainAsync() => Task.FromResult<ITestTable>(new IcebergTestTable(catalog, id, key, strategy));

        public async Task ValidateAsync() =>
            await TableValidator.AssertValidAsync(await LoadAsync(), new ValidationOptions { Deep = true, AllSnapshots = true });
    }
}

public sealed class IcebergV2CopyOnWrite : CommonScenarios
{
    protected override IFormatDriver CreateDriver() => new IcebergDriver(2, DeleteStrategy.CopyOnWrite);
}

public sealed class IcebergV2PositionDeletes : CommonScenarios
{
    protected override IFormatDriver CreateDriver() => new IcebergDriver(2, DeleteStrategy.PositionDeletes);
}

public sealed class IcebergV2EqualityDeletes : CommonScenarios
{
    protected override IFormatDriver CreateDriver() => new IcebergDriver(2, DeleteStrategy.EqualityDeletes);
}

public sealed class IcebergV3DeletionVectors : CommonScenarios
{
    protected override IFormatDriver CreateDriver() => new IcebergDriver(3, DeleteStrategy.PositionDeletes);
}
