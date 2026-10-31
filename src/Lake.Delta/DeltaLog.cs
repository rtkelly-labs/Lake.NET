using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Iceberg.Net.IO;

namespace Lake.Delta;

/// <summary>One checkpoint found in <c>_delta_log</c>: a classic file, every part of a multi-part one, or a UUID-named V2 one.</summary>
internal sealed record CheckpointFiles(long Version, string Kind, IReadOnlyList<string> Locations);

/// <summary>What <c>_delta_log</c> holds: commits by version and complete checkpoints.</summary>
internal sealed record LogListing(SortedDictionary<long, StoredFile> Commits, List<CheckpointFiles> Checkpoints, string? LastCheckpoint);

/// <summary>Listing, checkpoint choice and replay (PROTOCOL.md "Checkpoints", "Action Reconciliation").</summary>
internal static partial class DeltaLog
{
    [GeneratedRegex(@"^(\d{20})\.json$")]
    private static partial Regex CommitName();

    [GeneratedRegex(@"^(\d{20})\.checkpoint\.parquet$")]
    private static partial Regex ClassicCheckpointName();

    [GeneratedRegex(@"^(\d{20})\.checkpoint\.(\d{10})\.(\d{10})\.parquet$")]
    private static partial Regex MultiPartCheckpointName();

    [GeneratedRegex(@"^(\d{20})\.checkpoint\.([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\.(json|parquet)$")]
    private static partial Regex UuidCheckpointName();

    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static string VersionName(long version) => version.ToString("D20", CultureInfo.InvariantCulture);

    public static async Task<LogListing> ListAsync(IFileIO io, string root, CancellationToken ct)
    {
        if (io is not ISupportsListing listing)
            throw new NotSupportedException("Reading a Delta table needs a FileIO that can list (ISupportsListing): the log is found by listing _delta_log");
        var commits = new SortedDictionary<long, StoredFile>();
        var classic = new List<CheckpointFiles>();
        var parts = new Dictionary<(long Version, int Total), SortedDictionary<int, string>>();
        string? lastCheckpoint = null;
        await foreach (var file in listing.ListAsync(DeltaPaths.LogDirectory(root) + "/", ct).ConfigureAwait(false))
        {
            var at = file.Location.LastIndexOf("_delta_log/", StringComparison.Ordinal);
            if (at < 0) continue;
            var name = file.Location[(at + "_delta_log/".Length)..];
            if (name.Contains('/')) continue; // _sidecars/, _staged_commits/
            Match m;
            if ((m = CommitName().Match(name)).Success) commits[ParseVersion(m.Groups[1].Value)] = file;
            else if ((m = ClassicCheckpointName().Match(name)).Success) classic.Add(new(ParseVersion(m.Groups[1].Value), "classic", [file.Location]));
            else if ((m = UuidCheckpointName().Match(name)).Success) classic.Add(new(ParseVersion(m.Groups[1].Value), "v2-" + m.Groups[3].Value, [file.Location]));
            else if ((m = MultiPartCheckpointName().Match(name)).Success)
            {
                var part = int.Parse(m.Groups[2].Value, CultureInfo.InvariantCulture);
                var total = int.Parse(m.Groups[3].Value, CultureInfo.InvariantCulture);
                if (part < 1 || part > total) continue; // not a valid part name: ignored like any unknown file
                var key = (ParseVersion(m.Groups[1].Value), total);
                if (!parts.TryGetValue(key, out var set)) parts[key] = set = [];
                set[part] = file.Location;
            }
            else if (name == "_last_checkpoint") lastCheckpoint = file.Location;
        }
        // Readers must ignore a multi-part checkpoint with a missing part (PROTOCOL.md "Checkpoints").
        foreach (var ((version, total), set) in parts)
            if (set.Count == total) classic.Add(new(version, "multi-part", set.Values.ToList()));
        return new LogListing(commits, classic, lastCheckpoint);
    }

    private static long ParseVersion(string digits) => long.Parse(digits, NumberStyles.None, CultureInfo.InvariantCulture);

    /// <summary>
    /// <c>_last_checkpoint</c> is a hint (PROTOCOL.md "Last Checkpoint File"): when it names a checkpoint newer than any the
    /// listing shows (a lagging listing), the files it implies are checked directly. A hint that does not parse is
    /// ignored, never trusted: the listing alone still finds every checkpoint.
    /// </summary>
    public static async Task AddLastCheckpointHintAsync(IFileIO io, string root, LogListing listing, CancellationToken ct)
    {
        if (listing.LastCheckpoint is null) return;
        long version;
        int? parts;
        try
        {
            var o = JsonNode.Parse(await io.ReadAllAsync(listing.LastCheckpoint, ct).ConfigureAwait(false), documentOptions: Json.DocumentOptions) as JsonObject;
            if (o is null) return;
            version = Json.ReqLong(o, "version", "_last_checkpoint");
            parts = Json.Int(o, "parts", "_last_checkpoint");
        }
        catch (Exception e) when (e is System.Text.Json.JsonException or InvalidDataException or FileNotFoundException)
        {
            return;
        }
        if (version < 0 || listing.Checkpoints.Any(c => c.Version >= version)) return;
        var log = DeltaPaths.LogDirectory(root);
        var names = parts is > 1
            ? Enumerable.Range(1, parts.Value).Select(p => $"{VersionName(version)}.checkpoint.{p:D10}.{parts:D10}.parquet").ToList()
            : [$"{VersionName(version)}.checkpoint.parquet"];
        var locations = names.Select(n => DeltaPaths.Join(log, n)).ToList();
        foreach (var location in locations)
            if (!await io.ExistsAsync(location, ct).ConfigureAwait(false)) return;
        listing.Checkpoints.Add(new CheckpointFiles(version, parts is > 1 ? "multi-part" : "classic", locations));
    }

    // ---------- reading actions ----------

    /// <summary>The actions of one newline-delimited JSON file (a commit or a JSON V2 checkpoint), in order.</summary>
    public static async Task<List<object>> ReadJsonActionsAsync(IFileIO io, string location, CancellationToken ct)
    {
        var bytes = await io.ReadAllAsync(location, ct).ConfigureAwait(false);
        return Corruption.Guard($"log file {location}", () =>
        {
            var text = StrictUtf8.GetString(bytes);
            var actions = new List<object>();
            var lineNumber = 0;
            foreach (var raw in text.Split('\n'))
            {
                lineNumber++;
                var line = raw.TrimEnd('\r');
                if (string.IsNullOrWhiteSpace(line)) continue;
                JsonNode? node;
                try
                {
                    node = JsonNode.Parse(line, documentOptions: Json.DocumentOptions);
                }
                catch (System.Text.Json.JsonException e)
                {
                    throw new InvalidDataException($"Corrupt log file {location}: line {lineNumber} is not valid JSON: {e.Message}", e);
                }
                if (node is not JsonObject o) throw new InvalidDataException($"Corrupt log file {location}: line {lineNumber} is not a JSON object");
                if (ActionParser.Parse(o, $"{location}:{lineNumber}") is { } action) actions.Add(action);
            }
            return actions;
        });
    }

    /// <summary>The actions of one Parquet checkpoint or sidecar file.</summary>
    public static async Task<List<object>> ReadParquetActionsAsync(IFileIO io, string location, CancellationToken ct)
    {
        var actions = new List<object>();
        await foreach (var (schema, batch) in ParquetSource.ReadAsync(io, location, "checkpoint", ct).ConfigureAwait(false))
        {
            Corruption.Guard($"checkpoint {location}", () =>
            {
                for (var r = 0; r < batch.Length; r++)
                    if (ActionParser.Parse(ParquetSource.RowAsJson(batch, schema, r), $"{location} row {r}") is { } action) actions.Add(action);
                return 0;
            });
        }
        return actions;
    }

    private static Task<List<object>> ReadCheckpointFileAsync(IFileIO io, string location, CancellationToken ct) =>
        location.EndsWith(".json", StringComparison.Ordinal) ? ReadJsonActionsAsync(io, location, ct) : ReadParquetActionsAsync(io, location, ct);

    /// <summary>Every action of a checkpoint, sidecars (V2) included.</summary>
    public static async Task<List<object>> ReadCheckpointAsync(IFileIO io, string root, CheckpointFiles checkpoint, CancellationToken ct)
    {
        var actions = new List<object>();
        foreach (var location in checkpoint.Locations) actions.AddRange(await ReadCheckpointFileAsync(io, location, ct).ConfigureAwait(false));
        var sidecars = actions.OfType<SidecarAction>().ToList();
        var metadata = actions.OfType<CheckpointMetadataAction>().ToList();
        if (metadata.Count > 1) throw new InvalidDataException($"Checkpoint {checkpoint.Locations[0]} has {metadata.Count} checkpointMetadata actions; a V2 checkpoint has exactly one");
        if (checkpoint.Kind.StartsWith("v2-", StringComparison.Ordinal) && metadata.Count == 0)
            throw new InvalidDataException($"UUID-named checkpoint {checkpoint.Locations[0]} has no checkpointMetadata action");
        if (metadata.Count == 1 && metadata[0].Version != checkpoint.Version)
            throw new InvalidDataException($"Checkpoint {checkpoint.Locations[0]} says it is version {metadata[0].Version}");
        if (sidecars.Count > 0)
        {
            if (actions.Any(a => a is DeltaAddFile or DeltaRemoveFile))
                throw new InvalidDataException($"Checkpoint {checkpoint.Locations[0]} has file actions both inline and in sidecars");
            var sidecarDirectory = DeltaPaths.Join(DeltaPaths.LogDirectory(root), "_sidecars");
            foreach (var sidecar in sidecars)
            {
                var location = StorageScheme.Of(sidecar.Path) is null ? DeltaPaths.Join(sidecarDirectory, Uri.UnescapeDataString(sidecar.Path)) : sidecar.Path;
                foreach (var action in await ReadParquetActionsAsync(io, location, ct).ConfigureAwait(false))
                {
                    if (action is DeltaAddFile or DeltaRemoveFile) actions.Add(action);
                    else throw new InvalidDataException($"Sidecar {location} holds a {action.GetType().Name}; sidecars hold only add and remove actions");
                }
            }
        }
        return actions;
    }
}

/// <summary>Action reconciliation (PROTOCOL.md "Action Reconciliation"), applied in log order.</summary>
internal sealed class ReplayState
{
    public DeltaProtocol? Protocol { get; private set; }
    public DeltaMetadata? Metadata { get; private set; }
    public Dictionary<string, DeltaTransaction> Transactions { get; } = new(StringComparer.Ordinal);
    public Dictionary<string, DeltaDomainMetadata> Domains { get; } = new(StringComparer.Ordinal);
    public Dictionary<(string Path, string? DvId), DeltaAddFile> Adds { get; } = [];
    public Dictionary<(string Path, string? DvId), DeltaRemoveFile> Removes { get; } = [];

    /// <summary>
    /// Apply one version's actions. Two protocol or metaData actions in one version are refused (which would win is
    /// undefined); two actions for one logical file in a version are also illegal, but that is the writer's obligation and
    /// the later one simply wins here, as in other readers.
    /// </summary>
    public void ApplyVersion(IEnumerable<object> actions, string where)
    {
        var protocols = 0;
        var metadatas = 0;
        foreach (var action in actions)
        {
            switch (action)
            {
                case DeltaProtocol p:
                    if (++protocols > 1) throw new InvalidDataException($"{where} has more than one protocol action");
                    Protocol = p;
                    break;
                case DeltaMetadata m:
                    if (++metadatas > 1) throw new InvalidDataException($"{where} has more than one metaData action");
                    Metadata = m;
                    break;
                case DeltaTransaction t:
                    Transactions[t.AppId] = t; // the latest seen wins; versions are not assumed monotonic
                    break;
                case DeltaDomainMetadata d:
                    if (d.Removed) Domains.Remove(d.Domain);
                    else Domains[d.Domain] = d;
                    break;
                case DeltaAddFile add:
                {
                    var key = DeltaPaths.FileKey(add.Path, add.DeletionVector);
                    Adds[key] = add;
                    Removes.Remove(key);
                    break;
                }
                case DeltaRemoveFile remove:
                {
                    var key = DeltaPaths.FileKey(remove.Path, remove.DeletionVector);
                    Adds.Remove(key);
                    Removes[key] = remove;
                    break;
                }
                // commitInfo, sidecar, checkpointMetadata: not reconciled
            }
        }
    }
}
