using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// Our SigV4 canonicalisation agrees with botocore's (an independent implementation) on the awkward cases REST
/// catalogs produce: %1F-separated namespaces, reserved and non-ASCII characters, repeated and empty query values.
/// </summary>
[Trait("Category", "Regression")]
[Spec("rest: getConfig")]
public sealed class SigV4CrossCheckTests
{
    [SkippableTheory]
    [InlineData("GET", "https://glue.eu-west-1.amazonaws.com/iceberg/v1/catalogs/123456789012/namespaces/a%1Fb/tables/t", "")]
    [InlineData("GET", "https://glue.eu-west-1.amazonaws.com/iceberg/v1/config?warehouse=123456789012%3As3tablescatalog%2Fbucket", "")]
    [InlineData("GET", "https://s3tables.eu-west-1.amazonaws.com/iceberg/v1/arn%3Aaws%3As3tables/namespaces?pageToken=a%2Bb%3D&parent=x%1Fy&empty=", "")]
    [InlineData("POST", "https://glue.eu-west-1.amazonaws.com/iceberg/v1/catalogs/123/namespaces/caf%C3%A9/tables", "{\"name\":\"t\"}")]
    [InlineData("DELETE", "https://glue.eu-west-1.amazonaws.com/iceberg/v1/catalogs/123/namespaces/n/tables/t?purgeRequested=true", "")]
    public async Task Canonical_requests_and_signatures_match_botocore(string method, string url, string body)
    {
        Require.LocalOracle();
        var when = new DateTime(2026, 10, 23, 8, 0, 0, DateTimeKind.Utc);
        var py = await TestEnvironment.Current.OracleAsync("sigv4", new JsonObject
        {
            ["access_key"] = "AKIDEXAMPLE", ["secret_key"] = "secret", ["token"] = "tok", ["service"] = "glue", ["region"] = "eu-west-1",
            ["method"] = method, ["url"] = url, ["body"] = body, ["timestamp"] = "20261023T080000Z",
            ["headers"] = new JsonObject { ["Content-Type"] = "application/json" },
        }.ToJsonString());

        // Same headers botocore signed: host, content-type, x-amz-date, x-amz-security-token.
        var request = new HttpRequestMessage(new HttpMethod(method), url) { Content = new ByteArrayContent(System.Text.Encoding.UTF8.GetBytes(body)) };
        request.Content.Headers.TryAddWithoutValidation("Content-Type", "application/json");
        request.Headers.TryAddWithoutValidation("X-Amz-Date", "20261023T080000Z");
        request.Headers.TryAddWithoutValidation("X-Amz-Security-Token", "tok");
        var (canonical, signed) = SigV4Handler.CanonicalRequest(request, SigV4Handler.PayloadHash(System.Text.Encoding.UTF8.GetBytes(body)));
        Assert.Equal(py["canonical"]!.GetValue<string>(), canonical);
        var auth = SigV4Handler.Authorization(canonical, signed, when, "eu-west-1", "glue", "AKIDEXAMPLE", "secret");
        Assert.EndsWith("Signature=" + py["signature"]!.GetValue<string>(), auth);
    }
}
