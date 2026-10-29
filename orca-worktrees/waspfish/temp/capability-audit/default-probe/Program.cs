using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Linq;
using Iceberg.Net.Types;

var root = Path.Combine(Directory.GetCurrentDirectory(), "temp", "capability-audit", "default-probe", "warehouse");
using var catalog = new InMemoryCatalog(root);
await catalog.CreateNamespaceAsync(Namespace.Of("probe"));
foreach (var required in new[] { false, true })
{
    var schema = new Schema(0, new[] {
        NestedField.Require(1, "Id", PrimitiveType.Long),
        new NestedField(2, "Extra", PrimitiveType.Int, required, WriteDefault: JsonValue.Create(7)),
    });
    var table = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("probe", required ? "required" : "optional"), schema, FormatVersion: 3));
    try
    {
        await table.InsertAsync(new[] { new OnlyId { Id = 1 } });
        var rows = await TableScan.ReadAllAsync(table);
        Console.WriteLine($"required={required}: extra={rows[0][1] ?? "null"}; expected write default=7");
    }
    catch (Exception e)
    {
        Console.WriteLine($"required={required}: {e.GetType().Name}: {e.Message}");
    }
}
public sealed class OnlyId { public long Id { get; set; } }
