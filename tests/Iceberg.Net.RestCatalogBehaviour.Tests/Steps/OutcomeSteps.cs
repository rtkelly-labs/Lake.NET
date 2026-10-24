using System.Net;
using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;
using Reqnroll;
using Xunit;

namespace Iceberg.Net.RestCatalogBehaviour.Tests.Steps;

/// <summary>How refusals look: through the client (exception types) and on the wire (status and error model).</summary>
[Binding]
public sealed class OutcomeSteps(CatalogWorld world)
{
    [Then(@"^the attempt fails because it already exists$")]
    public void AlreadyExists() => Assert.IsType<AlreadyExistsException>(world.TakeError());

    [Then(@"^the attempt fails because the table does not exist$")]
    public void NoSuchTable() => Assert.IsType<NoSuchTableException>(world.TakeError());

    /// <summary>The client has no view-specific exception: a missing view surfaces as <see cref="NoSuchTableException"/>.</summary>
    [Then(@"^the attempt fails because the view does not exist$")]
    public void NoSuchView() => Assert.IsType<NoSuchTableException>(world.TakeError());

    [Then(@"^the attempt fails because the namespace does not exist$")]
    public void NoSuchNamespace() => Assert.IsType<NoSuchNamespaceException>(world.TakeError());

    [Then(@"^the commit fails with CommitFailed$")]
    public void CommitFailed() => Assert.IsType<CommitFailedException>(world.TakeError());

    /// <summary>The spec's ErrorModel: <c>{"error": {"message", "type", "code"}}</c>, with the code equal to the status.</summary>
    [Then(@"^the request is refused with (\d+) ""([^""]*)""$")]
    public void Refused(int status, string type)
    {
        Assert.Equal((HttpStatusCode)status, world.Status);
        var error = world.Json?["error"];
        Assert.True(error is not null, $"No error model in: {world.Body}");
        Assert.Equal(type, error!["type"]!.GetValue<string>());
        Assert.Equal(status, error["code"]!.GetValue<int>());
        Assert.False(string.IsNullOrWhiteSpace(error["message"]!.GetValue<string>()));
    }
}
