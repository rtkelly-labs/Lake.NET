using System.Net;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Reqnroll;
using Xunit;
using Table = Iceberg.Net.Catalog.Table;

namespace Iceberg.Net.RestCatalogBehaviour.Tests.Steps;

[Binding]
public sealed class AuthSteps(CatalogWorld world, TableSteps tables)
{
    private StatusRecorder? _recorder;
    private string? _orders;
    private IReadOnlyList<TableIdentifier>? _listed;

    [Given(@"^the server requires authentication, with these principals and grants:$")]
    public void RequireAuth(DataTable principals)
    {
        world.Settings["Auth:SigningKey"] = "a-behaviour-test-signing-key-of-32-bytes-or-more";
        var n = new Dictionary<string, int>();
        foreach (var row in principals.Rows)
        {
            var principal = row["principal"];
            world.Secrets[principal] = $"{principal}-secret";
            world.Settings[$"Auth:Clients:{principal}"] = world.Secrets[principal];
            if (row["grant"] is { Length: > 0 } grant)
            {
                var i = n.GetValueOrDefault(principal);
                n[principal] = i + 1;
                world.Settings[$"Authorization:Grants:{principal}:{i}"] = grant;
            }
        }
    }

    // ---------- authentication ----------

    [When(@"^I list the namespaces without a token$")]
    public Task ListWithoutToken() => world.SendAsync(HttpMethod.Get, "/v1/namespaces", anonymous: true);

    [When(@"^I list the namespaces with a token the server never issued$")]
    public Task ListWithForgedToken() => world.SendAsync(HttpMethod.Get, "/v1/namespaces", bearer: "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJyb290In0.not-a-signature");

    private HttpStatusCode _tokenStatus;
    private JsonNode? _tokenBody;

    [When(@"^""([^""]*)"" asks for a token with the secret ""([^""]*)""$")]
    public async Task AskForToken(string principal, string secret)
    {
        using var http = world.Host.Http();
        using var form = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = principal,
            ["client_secret"] = secret,
            ["scope"] = "catalog",
        });
        using var response = await http.PostAsync("/v1/oauth/tokens", form);
        _tokenStatus = response.StatusCode;
        _tokenBody = JsonNode.Parse(await response.Content.ReadAsStringAsync());
    }

    /// <summary>The token endpoint answers in the OAuth2 error shape (<c>{"error": "invalid_client", …}</c>), not the ErrorModel.</summary>
    [Then(@"^the token request is refused with (\d+) ""([^""]*)""$")]
    public void TokenRefused(int status, string error)
    {
        Assert.Equal((HttpStatusCode)status, _tokenStatus);
        Assert.Equal(error, _tokenBody!["error"]!.GetValue<string>());
        Assert.Null(_tokenBody["access_token"]);
    }

    [When(@"^""([^""]*)"" connects with its client credentials$")]
    public async Task Connects(string principal) => _recorder = (await world.ClientAsync(principal)).Recorder;

    [Then(@"^""([^""]*)"" can load ""([^""]*)""$")]
    public async Task CanLoad(string principal, string name)
    {
        var table = await (await world.ClientAsync(principal)).Client.LoadTableAsync(CatalogWorld.Id(name));
        Assert.Equal(name, table.Identifier.ToString());
        Assert.All(_recorder!.Statuses, s => Assert.Equal(HttpStatusCode.OK, s));
    }

    // ---------- roles ----------

    [When(@"^""([^""]*)"" tries to (.*)$")]
    public async Task Tries(string principal, string operation)
    {
        var (client, recorder) = await world.ClientAsync(principal);
        _recorder = recorder;
        _orders = (await tables.LoadAsync("sales.orders")).MetadataLocation;
        await world.AttemptAsync(async () =>
        {
            switch (operation)
            {
                case "list the tables of \"sales\"":
                    _listed = await client.ListTablesAsync(Namespace.Of("sales"));
                    break;
                case "load \"sales.orders\"":
                    await client.LoadTableAsync(CatalogWorld.Id("sales.orders"));
                    break;
                case "load \"sales.invoices\"":
                    await client.LoadTableAsync(CatalogWorld.Id("sales.invoices"));
                    break;
                case "append a row to \"sales.orders\"":
                    // The rows are written locally; the commit is what the catalog decides on, so it goes through this principal.
                    var seen = await tables.LoadAsync("sales.orders");
                    var mine = new Table(client, seen.Identifier, seen.Metadata, seen.IO);
                    await TableMutations.InsertAsync(mine, [[42L, "appended", "eu"]]);
                    break;
                case "create the table \"sales.fresh\"":
                    await client.CreateTableAsync(new CreateTableRequest(CatalogWorld.Id("sales.fresh"), CatalogWorld.Schema));
                    break;
                case "drop the table \"sales.orders\"":
                    await client.DropTableAsync(CatalogWorld.Id("sales.orders"));
                    break;
                default:
                    throw new ArgumentException($"Unknown operation '{operation}'");
            }
        });
    }

    [Then(@"^the attempt is allowed$")]
    public void Allowed()
    {
        world.AssertSucceeded();
        Assert.All(_recorder!.Statuses, s => Assert.True((int)s < 400, $"Got {s}"));
        if (_listed is not null) Assert.Contains(CatalogWorld.Id("sales.orders"), _listed);
    }

    /// <summary>Forbidden: a 403 (not a 401: the principal is known), and nothing in the namespace changed.</summary>
    [Then(@"^the attempt is forbidden$")]
    public async Task Forbidden()
    {
        Assert.IsType<UnauthorizedAccessException>(world.TakeError());
        Assert.Equal(HttpStatusCode.Forbidden, _recorder!.Last);
        var observer = await world.CatalogAsync();
        Assert.Equal(_orders, (await observer.LoadTableAsync(CatalogWorld.Id("sales.orders"))).MetadataLocation);
        Assert.Equal([CatalogWorld.Id("sales.orders")], await observer.ListTablesAsync(Namespace.Of("sales")));
    }

    [Then(@"^""([^""]*)"" sees the top-level namespaces (.*)$")]
    public async Task Sees(string principal, string list) =>
        Assert.Equal(Gherkin.List(list), (await (await world.ClientAsync(principal)).Client.ListNamespacesAsync()).Select(n => n.ToString()));

    [Then(@"^""([^""]*)"" sees no top-level namespaces$")]
    public async Task SeesNone(string principal) => Assert.Empty(await (await world.ClientAsync(principal)).Client.ListNamespacesAsync());
}
