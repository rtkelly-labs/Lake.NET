using Reqnroll;

namespace Iceberg.Net.RestCatalogBehaviour.Tests.Steps;

[Binding]
public sealed class Hooks(CatalogWorld world)
{
    /// <summary>Every scenario gets its own server; it is torn down here, after any refusal nobody checked fails the scenario.</summary>
    [AfterScenario]
    public async Task TearDownServer()
    {
        try
        {
            world.ThrowUnobserved();
        }
        finally
        {
            await world.DisposeAsync();
        }
    }
}
