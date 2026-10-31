using Iceberg.Net.Catalog;
using Iceberg.Net.IO;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// The Cloudflare presets (docs/lake/reference/cloudflare.md): R2's S3 endpoint, region and checksum settings, and Basin
/// Catalog's REST options. Live behaviour against R2 is in the regression suite's <c>CloudflareR2Tests</c>.
/// </summary>
public sealed class CloudflarePresetTests
{
    [Fact]
    public void R2_uses_the_account_endpoint_region_auto_and_checksums_only_when_required()
    {
        var o = S3FileIOOptions.ForR2("0123abcd", "key", "secret");
        Assert.Equal("https://0123abcd.r2.cloudflarestorage.com", o.Endpoint);
        Assert.Equal("auto", o.Region);
        Assert.True(o.SendsChecksumsOnlyWhenRequired);
        Assert.Equal("https://0123abcd.eu.r2.cloudflarestorage.com", S3FileIOOptions.ForR2("0123abcd", "k", "s", jurisdiction: "eu").Endpoint);
        Assert.Equal("token", S3FileIOOptions.ForR2("0123abcd", "k", "s", sessionToken: "token").SessionToken);
        using var io = new S3FileIO(o); // the region "auto" and the endpoint make a client without an AWS lookup
    }

    [Theory]
    [InlineData("")]
    [InlineData("acct.evil.com/x")]
    [InlineData("a/b")]
    public void R2_refuses_an_account_id_that_would_change_the_host(string account) =>
        Assert.Throws<ArgumentException>(() => S3FileIOOptions.ForR2(account, "k", "s"));

    [Theory]
    [InlineData(null, false)]                                          // AWS through the SDK's endpoint resolution
    [InlineData("https://s3.eu-west-1.amazonaws.com", false)]          // AWS, explicit
    [InlineData("https://acct.r2.cloudflarestorage.com", true)]        // R2
    [InlineData("http://localhost:5000", true)]                        // moto, MinIO
    [InlineData("https://minio.internal:9000", true)]
    public void Checksums_follow_the_endpoint_unless_set(string? endpoint, bool onlyWhenRequired)
    {
        Assert.Equal(onlyWhenRequired, new S3FileIOOptions { Endpoint = endpoint }.SendsChecksumsOnlyWhenRequired);
        Assert.False(new S3FileIOOptions { Endpoint = endpoint, ChecksumsOnlyWhenRequired = false }.SendsChecksumsOnlyWhenRequired);
        Assert.True(new S3FileIOOptions { Endpoint = endpoint, ChecksumsOnlyWhenRequired = true }.SendsChecksumsOnlyWhenRequired);
    }

    [Fact]
    public void Basin_Catalog_asks_for_vended_credentials_over_https_with_a_token()
    {
        var o = RestCatalogOptions.ForBasinCatalog(new Uri("https://catalog.cloudflarestorage.com/acct/bucket"), "acct_bucket", "cf-token");
        Assert.Equal("vended-credentials", o.AccessDelegation);
        Assert.Equal("acct_bucket", o.Warehouse);
        Assert.Equal("cf-token", o.Token);
        Assert.DoesNotContain("cf-token", o.ToString(), StringComparison.Ordinal);
        Assert.Throws<ArgumentException>(() => RestCatalogOptions.ForBasinCatalog(new Uri("http://catalog.example/acct"), "w", "t"));
        Assert.Throws<ArgumentException>(() => RestCatalogOptions.ForBasinCatalog(new Uri("https://catalog.example/acct"), "", "t"));
        Assert.Throws<ArgumentException>(() => RestCatalogOptions.ForBasinCatalog(new Uri("https://catalog.example/acct"), "w", ""));
    }
}
