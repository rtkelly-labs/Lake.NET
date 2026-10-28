#!/usr/bin/env bash
# Pack the libraries, then build and run a throwaway console app that consumes them from a local feed:
# create a table, append, read it back. Proves the packages (and their dependency graph) work outside the solution.
#   tools/package-smoke.sh [output-dir]
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p temp
out="${1:-$(mktemp -d "$PWD/temp/packages.XXXXXX")}"
work="$(mktemp -d "$PWD/temp/package-smoke.XXXXXX")"
trap 'rm -rf "$work"' EXIT
pkgver="${VERSION:-$(dotnet msbuild src/Iceberg.Net.Core/Iceberg.Net.Core.csproj -getProperty:Version --nologo)}"
dotnet pack Iceberg.Net.slnx -c Release -p:Version="$pkgver" -o "$out" --nologo -v quiet
# Restore each consumer into a fresh cache against exactly this pack's version.
# These are external consumers: do not inherit repository build or central-package settings.
printf '<Project />\n' > "$work/Directory.Build.props"
printf '<Project />\n' > "$work/Directory.Packages.props"
export NUGET_PACKAGES="$work/packages"
export ICEBERG_SMOKE_ROOT="$work"
mkdir -p "$work/app"
cat > "$work/app/app.csproj" <<XML
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup><OutputType>Exe</OutputType><TargetFramework>net10.0</TargetFramework><Nullable>enable</Nullable><ImplicitUsings>enable</ImplicitUsings></PropertyGroup>
  <ItemGroup><PackageReference Include="Iceberg.Net.Data" Version="[${pkgver}]" /></ItemGroup>
</Project>
XML
cat > "$work/app/Program.cs" <<'CS'
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;

var dir = Path.Combine(Environment.GetEnvironmentVariable("ICEBERG_SMOKE_ROOT") ?? Path.GetTempPath(), "iceberg-package-smoke", Guid.NewGuid().ToString("N"));
using var catalog = new InMemoryCatalog(dir);
await catalog.CreateNamespaceAsync(Namespace.Of("smoke"));
var schema = new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String)], identifierFieldIds: [1]);
var table = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("smoke", "t"), schema, FormatVersion: 3));
await TableMutations.InsertAsync(table, [[1L, "a"], [2L, "b"]]);
await TableMutations.UpsertAsync(table, [[2L, "c"]], strategy: DeleteStrategy.PositionDeletes);
var rows = await TableScan.ReadAsync(table, Expr.GreaterThan("id", 1L));
if (rows.Count != 1 || (string?)rows[0][1] != "c") throw new Exception($"unexpected rows: {rows.Count}");
Console.WriteLine($"package smoke test OK: {table.Metadata.Snapshots.Count} snapshots");
Directory.Delete(dir, true);
CS
cat > "$work/app/nuget.config" <<XML
<configuration><packageSources><add key="local" value="$out" /></packageSources></configuration>
XML
# The local feed first; public dependencies (ParquetSharp, AWSSDK) from the machine's default sources.
dotnet restore "$work/app/app.csproj" --source "$out" --source https://api.nuget.org/v3/index.json --nologo
dotnet run --project "$work/app" --no-restore

# Exactly the packages released: a project that is not ready must not reach the feed by accident.
expected="Iceberg.Net.Arrow Iceberg.Net.Athena Iceberg.Net.Avro Iceberg.Net.Catalog Iceberg.Net.Core Iceberg.Net.Data Iceberg.Net.EntityFrameworkCore Iceberg.Net.IO Iceberg.Net.Linq Iceberg.Net.Puffin Iceberg.Net.Spark"
actual="$(ls "$out"/*.nupkg | xargs -n1 basename | sed -E 's/\.[0-9]+\.[0-9]+\.[0-9]+.*//' | sort -u | tr '\n' ' ' | sed 's/ $//')"
[[ "$actual" == "$expected" ]] || { echo "packed: $actual" >&2; echo "expected: $expected" >&2; exit 1; }

# The typed layer: a model, its schema, typed writes and a LINQ query pushed into the scan.
mkdir -p "$work/typed"
cat > "$work/typed/typed.csproj" <<XML
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup><OutputType>Exe</OutputType><TargetFramework>net10.0</TargetFramework><Nullable>enable</Nullable><ImplicitUsings>enable</ImplicitUsings></PropertyGroup>
  <ItemGroup><PackageReference Include="Iceberg.Net.Linq" Version="[${pkgver}]" /></ItemGroup>
</Project>
XML
cat > "$work/typed/Program.cs" <<'CS'
using Iceberg.Net.Catalog;
using Iceberg.Net.Linq;

var dir = Path.Combine(Environment.GetEnvironmentVariable("ICEBERG_SMOKE_ROOT") ?? Path.GetTempPath(), "iceberg-package-smoke", Guid.NewGuid().ToString("N"));
using var catalog = new InMemoryCatalog(dir);
await catalog.CreateNamespaceAsync(Namespace.Of("smoke"));
var table = await catalog.CreateTableAsync<Order>(TableIdentifier.Of("smoke", "orders"));
await table.InsertAsync([new Order(1, "a", 10), new Order(2, "b", 20)], new ReflectionRowMapper<Order>());
var big = await table.AsQueryable<Order>().Where(o => o.Total > 15).Select(o => o.Customer).ToListAsync();
if (big.Count != 1 || big[0] != "b") throw new Exception($"unexpected LINQ result: {string.Join(",", big)}");
Console.WriteLine("typed package OK");
Directory.Delete(dir, true);

public sealed record Order(long Id, string Customer, int Total);
CS
cp "$work/app/nuget.config" "$work/typed/"
dotnet restore "$work/typed/typed.csproj" --source "$out" --source https://api.nuget.org/v3/index.json --nologo
dotnet run --project "$work/typed" --no-restore

# Core and Avro also ship netstandard2.0 (for .NET Framework 4.6.2+): a netstandard2.0 library must build against them.
for pkg in Iceberg.Net.Core Iceberg.Net.Avro; do
  # Listed first: under pipefail, grep -q closing the pipe early would fail unzip with SIGPIPE.
  listing="$(unzip -l "$out/$pkg.$pkgver.nupkg")"
  grep -q "lib/netstandard2.0/$pkg.dll" <<<"$listing" || { echo "$pkg has no netstandard2.0 build" >&2; exit 1; }
done
mkdir -p "$work/legacy"
cat > "$work/legacy/legacy.csproj" <<XML
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup><TargetFramework>netstandard2.0</TargetFramework><LangVersion>latest</LangVersion></PropertyGroup>
  <ItemGroup><PackageReference Include="Iceberg.Net.Avro" Version="[${pkgver}]" /></ItemGroup>
</Project>
XML
cat > "$work/legacy/Reader.cs" <<'CS'
using Iceberg.Net.Avro;
using Iceberg.Net.Json;

public static class Reader
{
    public static int DataFiles(string metadataJson, byte[] manifest)
    {
        var metadata = MetadataJson.Read(metadataJson);
        return ManifestCodec.ReadManifest(manifest, metadata.DefaultSpec, metadata.CurrentSchema).Entries.Count;
    }
}
CS
cp "$work/app/nuget.config" "$work/legacy/"
dotnet build "$work/legacy" --nologo -v quiet --source "$out" --source https://api.nuget.org/v3/index.json
echo "netstandard2.0 consumer builds"

# Consume the rest of the selected stack from packages, not project references.
mkdir -p "$work/full-stack"
cat > "$work/full-stack/app.csproj" <<XML
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup><OutputType>Exe</OutputType><TargetFramework>net10.0</TargetFramework><Nullable>enable</Nullable><ImplicitUsings>enable</ImplicitUsings></PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Iceberg.Net.EntityFrameworkCore" Version="[${pkgver}]" />
    <PackageReference Include="Iceberg.Net.Arrow" Version="[${pkgver}]" />
    <PackageReference Include="Iceberg.Net.Spark" Version="[${pkgver}]" />
    <PackageReference Include="Iceberg.Net.Athena" Version="[${pkgver}]" />
  </ItemGroup>
</Project>
XML
cat > "$work/full-stack/Program.cs" <<'CS'
using Microsoft.EntityFrameworkCore;
using Iceberg.Net.Catalog;
using Iceberg.Net.EntityFrameworkCore;
using Iceberg.Net.Arrow;
using Iceberg.Net.Spark;
using Iceberg.Net.Athena;

var root = Path.Combine(Environment.GetEnvironmentVariable("ICEBERG_SMOKE_ROOT") ?? Path.GetTempPath(), "iceberg-full-stack-smoke", Guid.NewGuid().ToString("N"));
using ICatalog catalog = args.Length > 0
    ? await RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = new Uri(args[0]) })
    : new InMemoryCatalog(root);
var options = new DbContextOptionsBuilder<Shop>().UseIceberg(catalog, "smoke").Options;
await using (var db = new Shop(options))
{
    await db.Database.EnsureCreatedAsync();
    db.Orders.Add(new Order { Id = 1, Name = "one" });
    await db.SaveChangesAsync();
    if (await db.Orders.CountAsync() != 1) throw new Exception("EF consumer did not persist its row");
}
var table = await catalog.LoadTableAsync(TableIdentifier.Of("smoke", "orders"));
var arrow = ArrowInterop.ToArrow(table.Schema);
if (arrow.FieldsList.Count != 2) throw new Exception("Arrow schema mismatch");
if (string.IsNullOrEmpty(AthenaSql.Select(new AthenaTable("smoke", "Orders")))) throw new Exception("Athena SQL missing");
if (string.IsNullOrEmpty(SparkSql.String("smoke"))) throw new Exception("Spark SQL missing");
Console.WriteLine("EF Core, Arrow, Spark and Athena packages OK");
if (Directory.Exists(root)) Directory.Delete(root, true);
public sealed class Order { public long Id { get; set; } public string Name { get; set; } = ""; }
public sealed class Shop(DbContextOptions<Shop> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    protected override void OnModelCreating(ModelBuilder model) => model.Entity<Order>().ToIcebergTable("orders");
}
CS
cp "$work/app/nuget.config" "$work/full-stack/"
dotnet restore "$work/full-stack/app.csproj" --source "$out" --source https://api.nuget.org/v3/index.json --nologo
dotnet run --project "$work/full-stack" --no-restore

# The same package consumer through the published REST executable, then verify restart persistence.
dotnet publish src/Iceberg.Net.RestServer -c Release --no-build -p:Version="$pkgver" -o "$work/server" --nologo -v quiet
python3 tools/server-smoke.py "$work/server" "$work" "$work/full-stack/app.csproj"
