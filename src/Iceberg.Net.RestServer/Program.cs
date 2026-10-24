// Standalone Iceberg REST catalog server. Configuration: the "Iceberg" section (see RestServerOptions), e.g.
//   dotnet run --project src/Iceberg.Net.RestServer -- --urls http://localhost:8181 \
//     --Iceberg:Warehouse=/tmp/warehouse --Iceberg:SqlitePath=/tmp/catalog.db
// See docs/rest-server.md.
var app = Iceberg.Net.RestServer.RestServer.Build(args);
app.Run();

/// <summary>Entry point, public for WebApplicationFactory.</summary>
public partial class Program;
