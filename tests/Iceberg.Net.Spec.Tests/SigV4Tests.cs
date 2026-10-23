using Iceberg.Net.Catalog;

namespace Iceberg.Net.Spec.Tests;

/// <summary>SigV4 signing matches AWS's published examples (also reproduced with botocore).</summary>
public sealed class SigV4Tests
{
    private const string AccessKey = "AKIDEXAMPLE";
    private const string SecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    private static readonly DateTime When = new(2015, 8, 30, 12, 36, 0, DateTimeKind.Utc);
    private static readonly string EmptyHash = SigV4Handler.PayloadHash([]);

    private static string Signature(HttpRequestMessage request, string service)
    {
        request.Headers.TryAddWithoutValidation("X-Amz-Date", "20150830T123600Z");
        var (canonical, signed) = SigV4Handler.CanonicalRequest(request, EmptyHash);
        var auth = SigV4Handler.Authorization(canonical, signed, When, "us-east-1", service, AccessKey, SecretKey);
        return auth[(auth.IndexOf("Signature=", StringComparison.Ordinal) + 10)..];
    }

    [Theory]
    [InlineData("https://example.amazonaws.com/", "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31")]           // get-vanilla
    [InlineData("https://example.amazonaws.com/?Param2=value2&Param1=value1", "b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500")] // query order
    public void Signature_v4_test_suite(string url, string expected) =>
        Assert.Equal(expected, Signature(new HttpRequestMessage(HttpMethod.Get, url), "service"));

    [Fact]
    public void Iam_list_users_example_from_the_aws_documentation()
    {
        var request = new HttpRequestMessage(HttpMethod.Get, "https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08")
        {
            Content = new ByteArrayContent([]),
        };
        request.Content.Headers.TryAddWithoutValidation("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
        Assert.Equal("5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7", Signature(request, "iam"));
    }

    [Fact]
    public async Task Handler_moves_an_oauth_token_aside_and_signs_body_and_session_token()
    {
        var handler = new SigV4Handler(_ => Task.FromResult(new Amazon.Runtime.ImmutableCredentials(AccessKey, SecretKey, "session")), "eu-west-1", "glue", () => When);
        var request = new HttpRequestMessage(HttpMethod.Post, "https://glue.eu-west-1.amazonaws.com/iceberg/v1/catalogs/123/namespaces") { Content = new StringContent("{}") };
        request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", "oauth");
        await handler.SignAsync(request);
        Assert.Equal("Bearer oauth", request.Headers.GetValues("Original-Authorization").Single());
        var auth = request.Headers.GetValues("Authorization").Single();
        Assert.StartsWith("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/eu-west-1/glue/aws4_request, SignedHeaders=", auth);
        Assert.Contains("x-amz-content-sha256", auth);
        Assert.Contains("x-amz-security-token", auth);
        Assert.Equal(SigV4Handler.PayloadHash("{}"u8.ToArray()), request.Headers.GetValues("X-Amz-Content-SHA256").Single());
    }
}
