using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Linq;
using Iceberg.Net.Types;
using Reqnroll;

namespace Iceberg.Net.Behaviour.Tests.Steps;

[Binding]
public sealed class WriteDefaultSteps(IcebergWorld world)
{
    public sealed class IdOnly
    {
        [IcebergField(1)] public long Id { get; init; }
    }

    [Given(@"^a format v3 table with typed write defaults using (parquet|avro)$")]
    public async Task GivenDefaults(string format)
    {
        var schema = new Schema(0,
        [
            NestedField.Require(1, "id", PrimitiveType.Long),
            new NestedField(2, "name", PrimitiveType.String, false, WriteDefault: JsonValue.Create("anonymous")),
            new NestedField(3, "region", PrimitiveType.String, true, WriteDefault: JsonValue.Create("eu")),
        ], identifierFieldIds: [1]);
        world.Table = await world.Catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(world.Namespace, "defaults"), schema,
            Properties: new Dictionary<string, string> { ["write.format.default"] = format }, FormatVersion: 3));
    }

    [When(@"^I insert a typed row omitting the defaulted columns$")]
    public Task InsertTyped() => world.T.InsertAsync([new IdOnly { Id = 1 }]);
}
