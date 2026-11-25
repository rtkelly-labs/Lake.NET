using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.IO;
using Iceberg.Net.RestServer;
using Iceberg.Net.Types;

/// <summary>
/// The REST server's request bodies, without HTTP: a body is parsed as the endpoint parses it and committed through
/// <see cref="CatalogService"/> against two tables in a fresh in-memory store on local files. The contract is the
/// server's: a bad request is a 4xx (by <see cref="ErrorModel.Classify"/>), never a 500 or an escaping exception. A
/// rejected commit moves no pointer and leaves no file behind; an accepted one writes metadata that reads back.
/// Anything else throws <see cref="FuzzFindingException"/>, which the harness does not treat as a format error.
/// </summary>
internal static class RestTargets
{
    private static readonly TableIdentifier First = new(Namespace.Of("fuzz"), "t");
    private static readonly TableIdentifier Second = new(Namespace.Of("fuzz"), "u");
    private static readonly Lazy<Tables> Base = new(() => Tables.CreateAsync().GetAwaiter().GetResult());

    /// <summary><c>POST /v1/namespaces/fuzz/tables/t</c>: a <c>CommitTableRequest</c>.</summary>
    public static void Commit(byte[] bytes) => RunAsync(bytes, transaction: false).GetAwaiter().GetResult();

    /// <summary><c>POST /v1/transactions/commit</c>: a <c>CommitTransactionRequest</c>.</summary>
    public static void Transaction(byte[] bytes) => RunAsync(bytes, transaction: true).GetAwaiter().GetResult();

    private static async Task RunAsync(byte[] bytes, bool transaction)
    {
        var tables = Base.Value;
        var store = new InMemoryCatalogStore();
        await store.CreateNamespaceAsync(First.Namespace, new Dictionary<string, string>());
        await store.SwapAsync([new PointerSwap(First, null, tables.FirstLocation), new PointerSwap(Second, null, tables.SecondLocation)], CatalogEntryKind.Table);
        var service = new CatalogService(store, tables.IO, tables.Options);
        var filesBefore = Files(tables.Root);

        IReadOnlyList<LoadedTable> committed;
        try
        {
            JsonNode body;
            try
            {
                body = JsonNode.Parse(bytes) ?? throw new BadRequestException("Request body must be a JSON object");
            }
            catch (System.Text.Json.JsonException e)
            {
                throw new BadRequestException($"Malformed JSON: {e.Message}");
            }
            committed = transaction
                ? await service.CommitAsync(Endpoints.ParseTransaction(body), default)
                : [await service.CommitTableAsync(Endpoints.ParseTableCommit(First, body), default)];
        }
        catch (Exception e) when (e is not FuzzFindingException)
        {
            var (status, type) = ErrorModel.Classify(e);
            if (status >= 500) throw new FuzzFindingException($"HTTP {status} {type}", e);
            Check(await store.GetAsync(First, CatalogEntryKind.Table) == tables.FirstLocation, "a rejected commit moved the first table's pointer");
            Check(await store.GetAsync(Second, CatalogEntryKind.Table) == tables.SecondLocation, "a rejected commit moved the second table's pointer");
            Check(Files(tables.Root) == filesBefore, "a rejected commit left a file behind");
            return;
        }

        foreach (var table in committed)
        {
            if (table.MetadataLocation is not { } location || location == tables.FirstLocation || location == tables.SecondLocation) continue;
            Check(await store.GetAsync(table.Id, CatalogEntryKind.Table) == location, "an accepted commit did not move the pointer");
            try
            {
                await service.ReadMetadataAsync(location, default);
            }
            catch (Exception e)
            {
                throw new FuzzFindingException("an accepted commit wrote metadata that does not read back", e);
            }
            await tables.IO.DeleteAsync(location);
        }
    }

    private static int Files(string root) => Directory.Exists(root) ? Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories).Count() : 0;

    private static void Check(bool condition, string message)
    {
        if (!condition) throw new FuzzFindingException(message);
    }

    /// <summary>Two tables written once per process; each input gets a fresh store that points at them.</summary>
    private sealed record Tables(string Root, RestServerOptions Options, IFileIO IO, string FirstLocation, string SecondLocation)
    {
        public static async Task<Tables> CreateAsync()
        {
            var root = Path.Combine(Path.GetTempPath(), $"iceberg-fuzz-rest-{Environment.ProcessId}");
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
            var options = new RestServerOptions { Warehouse = Path.Combine(root, "warehouse"), DefaultFormatVersion = 2 };
            var io = new LocalFileIO();
            var service = new CatalogService(new InMemoryCatalogStore(), io, options);
            await service.CreateNamespaceAsync(First.Namespace, new Dictionary<string, string>(), default);
            var schema = new Schema(
                NestedField.Require(1, "id", PrimitiveType.Long),
                NestedField.Optional(2, "data", PrimitiveType.String),
                NestedField.Optional(3, "ts", PrimitiveType.TimestampTz));
            async Task<string> CreateAsync(TableIdentifier id) =>
                (await service.CreateTableAsync(id.Namespace, new CreateTableSpec(id.Name, schema, null, null, null, new Dictionary<string, string>(), false), default))
                .MetadataLocation!;
            return new Tables(root, options, io, await CreateAsync(First), await CreateAsync(Second));
        }
    }
}

/// <summary>A broken contract in a REST target; never a format error, so the harness reports it.</summary>
internal sealed class FuzzFindingException(string message, Exception? inner = null) : Exception(message, inner);
