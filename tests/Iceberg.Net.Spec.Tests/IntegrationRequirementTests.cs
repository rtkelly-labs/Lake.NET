using Iceberg.Net.TestSupport;

namespace Iceberg.Net.Spec.Tests;

public sealed class IntegrationRequirementTests
{
    [Theory]
    [InlineData("spark", "spark")]
    [InlineData("spark", "trino, SPARK ")]
    [InlineData("catalog:nessie", "catalog:nessie")]
    [InlineData("azurite", "*")]
    public void Required_missing_dependencies_fail_instead_of_skipping(string name, string required)
    {
        var error = Assert.Throws<InvalidOperationException>(() => Require.Dependency(name, "deliberately unavailable", required));
        Assert.Contains(name, error.Message);
        Assert.Contains("deliberately unavailable", error.Message);
    }

    [Fact]
    public void An_available_required_dependency_proceeds() => Require.Dependency("spark", null, "spark");
}
