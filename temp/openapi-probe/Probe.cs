using Iceberg.Net.RestServer.Tests;
public class Probe
{
    [Xunit.Fact]
    public async System.Threading.Tasks.Task Dump()
    {
        await using var host = ServerHost.Start();
        var json = await host.Http().GetStringAsync("openapi/v1.json");
        System.IO.File.WriteAllText(System.Environment.GetEnvironmentVariable("PROBE_OUT")!, json);
    }
}
