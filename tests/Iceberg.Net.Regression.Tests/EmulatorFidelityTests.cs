using Amazon.Glue;
using Amazon.Glue.Model;
using Iceberg.Net.IO;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// The storage/catalog guarantees Iceberg commits rely on. These run unchanged in both modes:
/// passing against AWS proves the behaviour is real; passing in emulation proves the emulator
/// (plus emulation/moto_server.py shims) reproduces it. A failure in only one mode is a fidelity gap.
/// </summary>
[Spec("File System Operations")]
[Spec("Metastore Tables")]
[Trait("Category", "Regression")]
public sealed class EmulatorFidelityTests
{
    private static string Location(string name) => $"{TestEnvironment.Current.Warehouse.TrimEnd('/')}/fidelity/{TestEnvironment.Current.RunId}/{name}";

    [SkippableFact]
    public async Task S3_conditional_create_rejects_existing_objects()
    {
        Require.Target();
        using var io = new S3FileIO(TestEnvironment.Current.S3Options);
        var loc = Location($"{Guid.NewGuid():N}.metadata.json");
        await io.WriteAsync(loc, "first"u8.ToArray());
        await Assert.ThrowsAsync<FileAlreadyExistsException>(() => io.WriteAsync(loc, "second"u8.ToArray()));
        Assert.Equal("first"u8.ToArray(), await io.ReadAllAsync(loc));
        await io.DeleteAsync(loc);
    }

    [SkippableFact]
    public async Task S3_ranged_reads_and_lengths()
    {
        Require.Target();
        using var io = new S3FileIO(TestEnvironment.Current.S3Options);
        var loc = Location($"{Guid.NewGuid():N}.bin");
        var bytes = Enumerable.Range(0, 4096).Select(i => (byte)i).ToArray();
        await io.WriteAsync(loc, bytes);
        Assert.Equal(4096, await io.LengthAsync(loc));
        Assert.Equal(bytes[100..164], await io.ReadRangeAsync(loc, 100, 64));
        Assert.True(await io.ExistsAsync(loc));
        await io.DeleteAsync(loc);
        Assert.False(await io.ExistsAsync(loc));
    }

    [SkippableFact]
    public async Task Glue_update_table_enforces_version_id()
    {
        Require.Target();
        var env = TestEnvironment.Current;
        var config = new AmazonGlueConfig { RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(env.Region) };
        if (env.GlueEndpoint is not null)
        {
            config.ServiceURL = env.GlueEndpoint;
            config.AuthenticationRegion = env.Region;
        }
        using var glue = new AmazonGlueClient(config);
        var db = env.NewNamespace("fidelity");
        await glue.CreateDatabaseAsync(new CreateDatabaseRequest { DatabaseInput = new DatabaseInput { Name = db } });
        try
        {
            TableInput Input(string v) => new() { Name = "t", Parameters = new Dictionary<string, string> { ["v"] = v } };
            await glue.CreateTableAsync(new CreateTableRequest { DatabaseName = db, TableInput = Input("0") });
            var stale = (await glue.GetTableAsync(new GetTableRequest { DatabaseName = db, Name = "t" })).Table.VersionId;

            await glue.UpdateTableAsync(new UpdateTableRequest { DatabaseName = db, TableInput = Input("1"), VersionId = stale });
            await Assert.ThrowsAsync<ConcurrentModificationException>(() =>
                glue.UpdateTableAsync(new UpdateTableRequest { DatabaseName = db, TableInput = Input("2"), VersionId = stale }));

            var final = (await glue.GetTableAsync(new GetTableRequest { DatabaseName = db, Name = "t" })).Table;
            Assert.Equal("1", final.Parameters["v"]);
        }
        finally
        {
            await glue.DeleteTableAsync(new DeleteTableRequest { DatabaseName = db, Name = "t" });
            await glue.DeleteDatabaseAsync(new DeleteDatabaseRequest { Name = db });
        }
    }
}
