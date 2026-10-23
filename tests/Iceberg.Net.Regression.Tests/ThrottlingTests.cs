using System.Net;
using Amazon.S3;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.IO;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// S3 throttling (<c>503 SlowDown</c>) is retried with back-off (<c>s3.retry.num-retries</c>), including the
/// ambiguous case where the request was applied but the response was lost — the table stays consistent either way.
/// </summary>
[Trait("Category", "Regression")]
[Spec("File System Operations")]
public sealed class ThrottlingTests
{
    /// <summary>Answers every <c>Every</c>-th request with 503 SlowDown, before forwarding it or (Ambiguous) after.</summary>
    private sealed class ThrottlingHandler(int every, bool ambiguous) : DelegatingHandler(new SocketsHttpHandler())
    {
        private int _count;
        public int Throttled;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            if (Interlocked.Increment(ref _count) % every != 0) return await base.SendAsync(request, ct);
            Interlocked.Increment(ref Throttled);
            if (ambiguous) (await base.SendAsync(request, ct)).Dispose(); // applied, but the caller never hears so
            return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)
            {
                Content = new StringContent("<?xml version=\"1.0\"?><Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>"),
                RequestMessage = request,
            };
        }
    }

    private static async Task<(Table Table, InMemoryCatalog Catalog)> Create(S3FileIO io, string name)
    {
        var catalog = new InMemoryCatalog(TestEnvironment.Current.Warehouse.TrimEnd('/') + "/throttle", io);
        var ns = Namespace.Of(TestEnvironment.Current.NewNamespace("throttle"));
        await catalog.CreateNamespaceAsync(ns);
        var t = await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(ns, name), CatalogConformanceTests.Schema, CatalogConformanceTests.Spec));
        return (t, catalog);
    }

    [SkippableTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Throttled_requests_are_retried_and_the_table_stays_consistent(bool ambiguous)
    {
        Require.Target();
        // Throttling back-off is seconds per retry, so the workload is small: create, append, upsert, read.
        var handler = new ThrottlingHandler(every: 5, ambiguous);
        using var io = new S3FileIO(TestEnvironment.Current.S3Options with { HttpHandler = handler, MaxRetries = 5 });
        var (t, catalog) = await Create(io, $"t_{(ambiguous ? "ambiguous" : "clean")}");
        using var _ = catalog;

        await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(0, 20));
        await TableMutations.UpsertAsync(t, CatalogConformanceTests.Rows(15, 10), ["id"], DeleteStrategy.PositionDeletes);

        Assert.True(handler.Throttled > 3, $"only {handler.Throttled} requests were throttled");
        Assert.Equal(Enumerable.Range(0, 25).Select(i => (long)i), (await TableScan.ReadAllAsync(t)).Select(r => (long)r[0]!).Order());
        var report = await TableValidator.ValidateAsync(t, new ValidationOptions { AllSnapshots = true, Deep = true });
        Assert.True(report.IsValid, report.ToString());
    }

    [SkippableFact]
    public async Task Without_retries_throttling_surfaces_as_an_error()
    {
        Require.Target();
        using var io = new S3FileIO(TestEnvironment.Current.S3Options with { HttpHandler = new ThrottlingHandler(every: 1, ambiguous: false), MaxRetries = 0 });
        var e = await Assert.ThrowsAsync<AmazonS3Exception>(() => io.ReadAllAsync(TestEnvironment.Current.Warehouse.TrimEnd('/') + "/missing.json"));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, e.StatusCode);
    }
}
