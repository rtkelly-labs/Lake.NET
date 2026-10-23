using System.Diagnostics;
using System.Text;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.IO;
using Iceberg.Net.Json;

// Shadow comparison: read one table with iceberg-dotnet and with a reference engine (PyIceberg) and compare
// order-independent content fingerprints (Iceberg.Net.Data.TableFingerprint). Both read the same metadata file,
// so a concurrent commit cannot cause a false mismatch. Used by .github/workflows/iceberg-dotnet-aws.yml against
// production tables, and by ShadowCompareTests locally.
//
//   ShadowCompare --catalog glue|rest --table db.table [--report out.md] [--validate true|false]
//   ShadowCompare --metadata <metadata.json location> [--report out.md] [--validate true|false]
// --validate (default true) also runs the read-only table checker (TableValidator: metadata, manifests, file
// existence and sizes, deletion vectors; not the deep data re-read) and fails on any error it reports.
// Environment: AWS_REGION, ICEBERG_S3_ENDPOINT, ICEBERG_GLUE_ENDPOINT, ICEBERG_REST_URI (as the regression suite);
// ICEBERG_PYTHON for the reference (default: uv run in interop/). Exit 0 = identical, 1 = different, 2 = error.

var opts = new Dictionary<string, string>();
for (var i = 0; i + 1 < args.Length; i += 2)
{
    if (!args[i].StartsWith("--", StringComparison.Ordinal)) return Usage($"unexpected argument {args[i]}");
    opts[args[i][2..]] = args[i + 1];
}
if (args.Length % 2 != 0) return Usage("every option takes a value");

try
{
    var region = Environment.GetEnvironmentVariable("AWS_REGION") ?? "eu-west-1";
    var s3Endpoint = Environment.GetEnvironmentVariable("ICEBERG_S3_ENDPOINT");
    var s3 = new S3FileIOOptions { Endpoint = s3Endpoint, PathStyleAccess = s3Endpoint is not null, Region = region };
    var io = new ResolvingFileIO(() => new S3FileIO(s3));

    Table table;
    if (opts.TryGetValue("metadata", out var location))
    {
        var metadata = MetadataJson.Read(Encoding.UTF8.GetString(await io.ReadAllAsync(location)), location);
        table = new Table(new InMemoryCatalog(Path.GetTempPath()), TableIdentifier.Of("shadow", "t"), metadata, io);
    }
    else if (opts.TryGetValue("catalog", out var kind) && opts.TryGetValue("table", out var name))
    {
        var dot = name.LastIndexOf('.');
        if (dot <= 0) return Usage("--table must be namespace.table");
        ICatalog catalog = kind switch
        {
            "glue" => GlueCatalog.Create(io, new GlueCatalogOptions { Region = region, Endpoint = Environment.GetEnvironmentVariable("ICEBERG_GLUE_ENDPOINT"), Warehouse = "" }),
            "rest" => await RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = new Uri(Environment.GetEnvironmentVariable("ICEBERG_REST_URI") ?? "http://localhost:8181") }, _ => io),
            _ => throw new ArgumentException($"unknown catalog {kind}"),
        };
        table = await catalog.LoadTableAsync(new TableIdentifier(Namespace.Of(name[..dot].Split('.')), name[(dot + 1)..]));
    }
    else return Usage("give --metadata, or --catalog and --table");

    var sw = Stopwatch.StartNew();
    var ours = await TableFingerprint.ComputeAsync(table);
    var oursTime = sw.Elapsed;
    sw.Restart();
    var reference = await PyIcebergFingerprintAsync(table.MetadataLocation!);
    var refTime = sw.Elapsed;

    var diffs = ours.DifferencesFrom(reference);
    var validation = opts.GetValueOrDefault("validate", "true") == "true"
        ? await TableValidator.ValidateAsync(table, new ValidationOptions { AllSnapshots = false, Deep = false })
        : null;
    var report = new StringBuilder()
        .AppendLine($"### Shadow read: `{opts.GetValueOrDefault("table", table.MetadataLocation!)}`")
        .AppendLine()
        .AppendLine($"Snapshot {table.Metadata.CurrentSnapshotId?.ToString() ?? "(none)"}, {ours.Rows:N0} rows; iceberg-dotnet {oursTime.TotalSeconds:F1}s, PyIceberg {refTime.TotalSeconds:F1}s.")
        .AppendLine()
        .AppendLine(diffs.Count == 0 ? "✅ identical content (every column and every row hash agree)" : "❌ **content differs**:")
        .AppendJoin(Environment.NewLine, diffs.Select(d => $"- {d}"))
        .AppendLine()
        .AppendLine(validation is null ? "" : validation.IsValid
            ? $"✅ table checker: no errors ({validation.Issues.Count} warnings)"
            : $"❌ **table checker found {validation.Errors.Count()} errors**:")
        .AppendJoin(Environment.NewLine, validation?.Issues.Select(i => $"- {i}") ?? [])
        .AppendLine()
        .ToString();
    Console.Write(report);
    if (opts.TryGetValue("report", out var path)) await File.WriteAllTextAsync(path, report);
    return diffs.Count == 0 && validation?.IsValid != false ? 0 : 1;
}
catch (Exception e)
{
    Console.Error.WriteLine($"shadow comparison failed: {e}");
    return 2;
}

static int Usage(string problem)
{
    Console.Error.WriteLine($"{problem}\nusage: ShadowCompare (--metadata <location> | --catalog glue|rest --table ns.table) [--report out.md]");
    return 2;
}

static async Task<TableFingerprint> PyIcebergFingerprintAsync(string metadataLocation)
{
    var dir = new DirectoryInfo(AppContext.BaseDirectory);
    while (dir is not null && !Directory.Exists(Path.Combine(dir.FullName, "interop"))) dir = dir.Parent;
    var interop = Path.Combine(dir?.FullName ?? throw new DirectoryNotFoundException("interop/ not found"), "interop");
    var script = Path.Combine(interop, "pyiceberg_oracle.py");
    var python = Environment.GetEnvironmentVariable("ICEBERG_PYTHON");
    var psi = python is not null ? new ProcessStartInfo(python) : new ProcessStartInfo("uv");
    if (python is null) foreach (var a in new[] { "run", "--quiet", "--project", interop, "python" }) psi.ArgumentList.Add(a);
    foreach (var a in new[] { "-I", script, "fingerprint", metadataLocation }) psi.ArgumentList.Add(a);
    psi.RedirectStandardOutput = psi.RedirectStandardError = true;
    using var p = Process.Start(psi)!;
    var stdout = p.StandardOutput.ReadToEndAsync();
    var stderr = p.StandardError.ReadToEndAsync();
    await p.WaitForExitAsync();
    if (p.ExitCode != 0) throw new InvalidOperationException($"PyIceberg fingerprint failed: {await stderr}");
    return TableFingerprint.FromJson(await stdout);
}
