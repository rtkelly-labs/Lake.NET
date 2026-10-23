using Reqnroll;
using Iceberg.Net.Data;

namespace Iceberg.Net.Behaviour.Tests.Steps;

[Binding]
public sealed class Hooks(IcebergWorld world)
{
    /// <summary>Whatever a scenario did, including failed writes, the table must still pass a deep fsck.</summary>
    [AfterScenario]
    public async Task TableIsStillValid()
    {
        if (world.Table is null) return;
        var table = await world.Catalog.LoadTableAsync(world.Table.Identifier);
        await TableValidator.AssertValidAsync(table, new ValidationOptions { Deep = true, AllSnapshots = true });
    }
}
