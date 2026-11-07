using System.Text;
using Iceberg.Net;
using Iceberg.Net.Avro;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;
using Iceberg.Net.Puffin;
using SharpFuzz;

// One decoder per run, chosen by FUZZ_TARGET (libfuzzer-dotnet passes a single --target_arg, the assembly).
// The contract under test is Corruption's: untrusted bytes fail with a format error or not at all. Anything else
// escaping (an IndexOutOfRange outside a Guard, a stack overflow, an allocation sized by a hostile length that runs
// out of memory, a hang) is a finding, and libFuzzer saves the input. With FUZZ_STRICT=1, a low-level exception
// that a Guard had to convert also counts, which points at a missing bounds check.
if (args is ["extract-blob", var puffinPath, var outPath])
{
    var puffin = PuffinFile.Read(File.ReadAllBytes(puffinPath)); // seed for the deletion-vector target
    File.WriteAllBytes(outPath, puffin.Payload(puffin.Blobs[0]).ToArray());
    return;
}

var target = Environment.GetEnvironmentVariable("FUZZ_TARGET") ?? (args.Length > 0 ? args[0] : null)
    ?? throw new ArgumentException($"Set FUZZ_TARGET to one of: {string.Join(", ", Targets.All.Keys)}");
var decode = Targets.All[target];
var strict = Environment.GetEnvironmentVariable("FUZZ_STRICT") == "1";

// Replay a corpus without libFuzzer (from an uninstrumented build, e.g. under a coverage collector): how many inputs
// decode, how many fail with a format error, and any other exception (a finding the fuzzer should also have saved).
if (args is ["replay", .. var dirs])
{
    int accepted = 0, rejected = 0, other = 0;
    foreach (var file in dirs.SelectMany(d => Directory.Exists(d) ? Directory.EnumerateFiles(d) : new[] { d }))
    {
        try
        {
            decode(File.ReadAllBytes(file));
            accepted++;
        }
        catch (Exception e) when (Corruption.IsFormatError(e))
        {
            rejected++;
        }
        catch (Exception e)
        {
            other++;
            Console.Error.WriteLine($"{file}: {e.GetType().Name}: {e.Message}");
        }
    }
    Console.WriteLine($"{{\"target\":\"{target}\",\"inputs\":{accepted + rejected + other},\"accepted\":{accepted},\"format_errors\":{rejected},\"other\":{other}}}");
    return;
}

Fuzzer.LibFuzzer.Run(span =>
{
    try
    {
        decode(span.ToArray());
    }
    catch (Exception e) when (Corruption.IsFormatError(e) && !(strict && e.InnerException is { } inner && !Corruption.IsFormatError(inner)))
    {
    }
});

internal static class Targets
{
    private static readonly Lazy<TableMetadata> Metadata = new(() =>
        MetadataJson.Read(File.ReadAllText(Environment.GetEnvironmentVariable("FUZZ_TABLE_METADATA")
            ?? throw new ArgumentException("The manifest target needs FUZZ_TABLE_METADATA (a metadata.json for its spec and schema)"))));

    public static readonly Dictionary<string, Action<byte[]>> All = new()
    {
        ["metadata"] = b => MetadataJson.Read(Encoding.UTF8.GetString(b)),
        ["view-metadata"] = b => ViewJson.Read(Encoding.UTF8.GetString(b)),
        ["manifest-list"] = b => ManifestCodec.ReadManifestList(b),
        ["manifest"] = b => ManifestCodec.ReadManifest(b, Metadata.Value.DefaultSpec, Metadata.Value.CurrentSchema),
        ["puffin"] = b =>
        {
            var p = PuffinFile.Read(b);
            foreach (var blob in p.Blobs) p.Payload(blob);
        },
        ["deletion-vector"] = b => DeletionVector.Decode(b),
        ["roaring"] = b => RoaringPositionBitmap.Deserialize(b),
        ["mumbling"] = b => MumblingBitmap.Deserialize(b),
        ["theta"] = b => ThetaSketch.Deserialize(b),
        ["rest-commit"] = RestTargets.Commit,
        ["rest-transaction"] = RestTargets.Transaction,
    };
}
