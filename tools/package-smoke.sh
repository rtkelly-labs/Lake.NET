#!/usr/bin/env bash
# Pack the libraries, then build and run a throwaway console app that consumes them from a local feed:
# create a table, append, read it back. Proves the packages (and their dependency graph) work outside the solution.
#   tools/package-smoke.sh [output-dir]
set -euo pipefail
cd "$(dirname "$0")/.."
out="${1:-$(mktemp -d)}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
dotnet pack Iceberg.Net.slnx -c Release -o "$out" --nologo -v quiet
# The consumers restore into their own cache: every pack is version 0.1.0-preview, so the machine-wide cache could
# hand back an older build of the same version.
export NUGET_PACKAGES="$work/packages"
mkdir -p "$work/app"
cat > "$work/app/app.csproj" <<'XML'
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup><OutputType>Exe</OutputType><TargetFramework>net10.0</TargetFramework><Nullable>enable</Nullable><ImplicitUsings>enable</ImplicitUsings></PropertyGroup>
  <ItemGroup><PackageReference Include="Iceberg.Net.Data" Version="0.1.0-*" /></ItemGroup>
</Project>
XML
cat > "$work/app/Program.cs" <<'CS'
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;

var dir = Path.Combine(Path.GetTempPath(), "iceberg-package-smoke", Guid.NewGuid().ToString("N"));
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
dotnet run --project "$work/app" --nologo --source "$out" --source https://api.nuget.org/v3/index.json

# Exactly the packages released: a project that is not ready must not reach the feed by accident.
expected="Iceberg.Net.Avro Iceberg.Net.Catalog Iceberg.Net.Core Iceberg.Net.Data Iceberg.Net.IO Iceberg.Net.Linq Iceberg.Net.Puffin"
actual="$(ls "$out"/*.nupkg | xargs -n1 basename | sed -E 's/\.[0-9]+\.[0-9]+\.[0-9]+.*//' | sort -u | tr '\n' ' ' | sed 's/ $//')"
[[ "$actual" == "$expected" ]] || { echo "packed: $actual" >&2; echo "expected: $expected" >&2; exit 1; }

# The typed layer: a model, its schema, typed writes and a LINQ query pushed into the scan.
mkdir -p "$work/typed"
cat > "$work/typed/typed.csproj" <<'XML'
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup><OutputType>Exe</OutputType><TargetFramework>net10.0</TargetFramework><Nullable>enable</Nullable><ImplicitUsings>enable</ImplicitUsings></PropertyGroup>
  <ItemGroup><PackageReference Include="Iceberg.Net.Linq" Version="0.1.0-*" /></ItemGroup>
</Project>
XML
cat > "$work/typed/Program.cs" <<'CS'
using Iceberg.Net.Catalog;
using Iceberg.Net.Linq;

var dir = Path.Combine(Path.GetTempPath(), "iceberg-package-smoke", Guid.NewGuid().ToString("N"));
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
dotnet run --project "$work/typed" --nologo --source "$out" --source https://api.nuget.org/v3/index.json

# Core and Avro also ship netstandard2.0 (for .NET Framework 4.6.2+): a netstandard2.0 library must build against them.
for pkg in Iceberg.Net.Core Iceberg.Net.Avro; do
  # Listed first: under pipefail, grep -q closing the pipe early would fail unzip with SIGPIPE.
  listing="$(unzip -l "$out/$pkg".0.1.0-*[0-9a-z].nupkg)"
  grep -q "lib/netstandard2.0/$pkg.dll" <<<"$listing" || { echo "$pkg has no netstandard2.0 build" >&2; exit 1; }
done
mkdir -p "$work/legacy"
cat > "$work/legacy/legacy.csproj" <<'XML'
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup><TargetFramework>netstandard2.0</TargetFramework><LangVersion>latest</LangVersion></PropertyGroup>
  <ItemGroup><PackageReference Include="Iceberg.Net.Avro" Version="0.1.0-*" /></ItemGroup>
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
