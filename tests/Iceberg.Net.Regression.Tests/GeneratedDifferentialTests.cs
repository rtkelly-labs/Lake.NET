using System.Diagnostics;
using System.Numerics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Arrow.Ipc;
using Iceberg.Net.Arrow;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;
using Xunit.Abstractions;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// Differential testing against PyIceberg on generated tables. ShadowCompareTests pins two hand-written schemas;
/// this campaign draws random schemas (primitives, decimals of every precision, fixed, nested struct/list/map) and
/// random rows from a seed, writes them with one implementation, and compares what each implementation reads, by
/// <see cref="TableFingerprint"/> (shared canonical form with the oracle's <c>fingerprint</c> command):
/// <list type="bullet">
///   <item>.NET writes (inserts in batches, optionally a delete by copy-on-write or position deletes), PyIceberg reads.
///   Half of these tables are partitioned (identity, bucket, truncate, year/month/day/hour over top-level columns), and
///   each partitioned case also runs one equality filter on a partition source through both scans: the full-table
///   fingerprint reads every file, so a wrong partition value only shows once a filter prunes by it.</item>
///   <item>PyIceberg writes (oracle <c>write-arrow</c>, from an Arrow IPC file of the same rows), both read.</item>
/// </list>
/// A disagreement is shrunk (drop columns, drop the delete, merge batches, drop rows) and the smallest failing table
/// is kept under ICEBERG_DIFFERENTIAL_OUT. Off unless ICEBERG_DIFFERENTIAL_SECONDS is set (the <c>differential</c>
/// workflow sets it); it then fails, rather than skips, when the oracle is missing.
/// Agreement is weaker evidence than disagreement: both sides can share a misreading of the spec.
/// </summary>
[Trait("Category", "Differential")]
[Spec("Scan Planning")]
[Spec("Nested Types")]
[Spec("Partition Transforms")]
public sealed class GeneratedDifferentialTests(ITestOutputHelper output) : IDisposable
{
    private static string? Env(string name) => Environment.GetEnvironmentVariable(name) is { Length: > 0 } v ? v : null;
    private static int? Seconds => int.TryParse(Env("ICEBERG_DIFFERENTIAL_SECONDS"), out var s) ? s : null;
    private static int BaseSeed => int.TryParse(Env("ICEBERG_DIFFERENTIAL_SEED"), out var s) ? s : 1;
    private static string OutDir => Env("ICEBERG_DIFFERENTIAL_OUT") ?? Path.Combine(Path.GetTempPath(), "iceberg-dotnet-differential-out");

    private readonly string _root = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-differential", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
    }

    [SkippableFact]
    public Task Generated_tables_written_by_dotnet_read_the_same_in_pyiceberg() => CampaignAsync("dotnet-writes", DotnetWritesAsync, maxFormatVersion: 3);

    // PyIceberg 0.12 cannot write format version 3 (apache/iceberg-python#1551), so this direction stays on v2.
    [SkippableFact]
    public Task Generated_tables_written_by_pyiceberg_read_the_same_in_dotnet() => CampaignAsync("pyiceberg-writes", PyIcebergWritesAsync, maxFormatVersion: 2);

    // ---------------------------------------------------------------- campaign

    private async Task CampaignAsync(string direction, Func<Case, string, Task<Outcome>> run, int maxFormatVersion)
    {
        Skip.If(Seconds is null, "Differential campaign off: set ICEBERG_DIFFERENTIAL_SECONDS (the differential workflow does)");
        if (TestEnvironment.Current.LocalOracleSkipReason is { } reason) throw new InvalidOperationException(reason);

        var budget = Stopwatch.StartNew();
        var outDir = Path.Combine(OutDir, direction);
        Directory.CreateDirectory(outDir);
        var cases = 0;
        var byCategory = new SortedDictionary<string, int>(StringComparer.Ordinal);
        var findings = new JsonArray();
        var signatures = new HashSet<string>(StringComparer.Ordinal);
        var oracleTime = TimeSpan.Zero;
        var rawSeen = new HashSet<string>(StringComparer.Ordinal);
        var repeats = 0;
        for (var seed = BaseSeed; budget.Elapsed < TimeSpan.FromSeconds(Seconds!.Value); seed++)
        {
            var c = Case.Generate(seed, maxFormatVersion, partitioned: direction == "dotnet-writes");
            var started = Stopwatch.StartNew();
            var outcome = await run(c, Path.Combine(_root, $"s{seed}"));
            oracleTime += started.Elapsed;
            cases++;
            byCategory[outcome.Category] = byCategory.GetValueOrDefault(outcome.Category) + 1;
            if (outcome.Category is "agree" || outcome.Category.StartsWith("known-", StringComparison.Ordinal)) continue;
            // Shrinking costs up to 40 oracle round trips: do it once per raw symptom, count repeats.
            if (!rawSeen.Add($"{outcome.Category}|{Signature(outcome.Detail)}"))
            {
                repeats++;
                continue;
            }

            var (shrunk, last, attempts) = await ShrinkAsync(c, outcome, run, Path.Combine(_root, $"s{seed}-shrink"));
            // One finding per distinct signature (category + first difference with values stripped + shrunk column types).
            var signature = $"{last.Category}|{Signature(last.Detail)}|{string.Join(",", shrunk.Schema.Columns.Skip(1).Select(f => f.Type.ToString()))}";
            var first = signatures.Add(signature);
            var kept = Path.Combine(outDir, $"seed-{seed}");
            if (first) await RerunAndKeepAsync(shrunk, run, kept);
            findings.Add(new JsonObject
            {
                ["seed"] = seed,
                ["category"] = last.Category,
                ["detail"] = Truncate(last.Detail, 2000),
                ["new_signature"] = first,
                ["shrink_attempts"] = attempts,
                ["format_version"] = shrunk.FormatVersion,
                ["schema"] = shrunk.Schema.ToString(),
                ["rows"] = shrunk.Rows.Count,
                ["batches"] = shrunk.Batches,
                ["delete"] = shrunk.Delete?.ToString(),
                ["partition"] = shrunk.PartitionText,
                ["filter"] = shrunk.Filter?.ToString(),
                ["original_schema"] = c.Schema.ToString(),
            });
            output.WriteLine($"seed {seed}: {last.Category}: {Truncate(last.Detail, 300)} (shrunk to {shrunk.Schema.Columns.Count} columns, {shrunk.Rows.Count} rows)");
        }

        var summary = new JsonObject
        {
            ["direction"] = direction,
            ["base_seed"] = BaseSeed,
            ["seconds"] = Math.Round(budget.Elapsed.TotalSeconds),
            ["cases"] = cases,
            ["seconds_per_case"] = cases == 0 ? null : Math.Round(oracleTime.TotalSeconds / cases, 2),
            ["outcomes"] = new JsonObject(byCategory.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))),
            ["distinct_disagreements"] = signatures.Count,
            ["repeated_symptoms_not_shrunk"] = repeats,
            ["findings"] = findings,
        };
        var text = summary.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
        await File.WriteAllTextAsync(Path.Combine(outDir, "summary.json"), text);
        output.WriteLine(text);
        Assert.True(signatures.Count == 0, $"{signatures.Count} distinct disagreement(s) with PyIceberg in {cases} generated tables; see {outDir}");
    }

    private static string Signature(string detail)
    {
        var sb = new StringBuilder();
        foreach (var ch in detail.Length > 200 ? detail[..200] : detail) sb.Append(char.IsAsciiHexDigit(ch) ? '#' : ch);
        return sb.ToString();
    }

    private static string Truncate(string s, int n) => s.Length <= n ? s : s[..n] + "…";

    private static async Task RerunAndKeepAsync(Case c, Func<Case, string, Task<Outcome>> run, string kept)
    {
        if (Directory.Exists(kept)) Directory.Delete(kept, recursive: true);
        try
        {
            await run(c, kept);
        }
        catch (Exception)
        {
            // The table files on disk are what matters; the outcome was already recorded.
        }
    }

    /// <summary>Greedy shrink: keep any smaller case that fails in the same category. Bounded by attempts.</summary>
    private static async Task<(Case Case, Outcome Outcome, int Attempts)> ShrinkAsync(Case c, Outcome outcome, Func<Case, string, Task<Outcome>> run, string dir)
    {
        var attempts = 0;
        const int MaxAttempts = 40;
        var progress = true;
        while (progress && attempts < MaxAttempts)
        {
            progress = false;
            foreach (var candidate in c.Smaller())
            {
                if (attempts++ >= MaxAttempts) break;
                var o = await run(candidate, Path.Combine(dir, attempts.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                if (o.Category != outcome.Category) continue;
                (c, outcome, progress) = (candidate, o, true);
                break;
            }
        }
        return (c, outcome, attempts);
    }

    // ---------------------------------------------------------------- directions

    /// <summary>Counted in the outcomes but not shrunk or reported: a limitation upstream that is already known.</summary>
    private const string KnownLimitation = "known-pyarrow-required-child-of-null-struct";

    private sealed record Outcome(string Category, string Detail)
    {
        public static readonly Outcome Agree = new("agree", "");
    }

    private static async Task<Outcome> DotnetWritesAsync(Case c, string dir)
    {
        Table t;
        try
        {
            t = await c.WriteWithDotnetAsync(dir);
        }
        catch (Exception e)
        {
            return new("dotnet-write-failed", e.GetType().Name + ": " + e.Message);
        }
        var outcome = await CompareAsync(t, c.ExpectedRows);
        return outcome != Outcome.Agree || c.Filter is null ? outcome : await CompareFilteredAsync(t, c);
    }

    /// <summary>One equality filter, three answers: the generated rows, .NET's scan, PyIceberg's scan. Both scans prune
    /// by the partition values and column metrics .NET wrote, so a wrong value drops rows from one or both.</summary>
    private static async Task<Outcome> CompareFilteredAsync(Table t, Case c)
    {
        var f = c.Filter!;
        var expected = c.ExpectedIds(f);
        List<long> ours;
        try
        {
            ours = (await TableScan.ReadAsync(t, Expr.Equal(f.Column, f.Value))).Select(r => (long)r[0]!).Order().ToList();
        }
        catch (Exception e)
        {
            return new("dotnet-filter-failed", e.GetType().Name + ": " + e.Message);
        }
        List<long> theirs;
        try
        {
            var py = await TestEnvironment.Current.OracleAsync("read-filter", t.MetadataLocation!, f.Json().ToJsonString());
            theirs = py["ids"]!.AsArray().Select(i => i!.GetValue<long>()).ToList();
        }
        catch (Exception e)
        {
            // pyarrow cannot filter a table that holds a uuid (an Arrow extension type) at any depth.
            if (e.Message.Contains("ArrowNotImplementedError: extension", StringComparison.Ordinal) && c.Schema.Columns.Any(col => HoldsUuid(col.Type)))
                return new("known-pyarrow-filter-over-uuid", "");
            return new("pyiceberg-filter-failed", LastLine(e.Message));
        }
        string Ids(List<long> ids) => $"[{string.Join(",", ids.Take(20))}{(ids.Count > 20 ? ",…" : "")}] ({ids.Count})";
        if (!ours.SequenceEqual(expected)) return new("dotnet-filter-rows", $"{f}: expected {Ids(expected)}, .NET read {Ids(ours)}");
        if (!theirs.SequenceEqual(expected))
            return TruncateOverflows(c, f)
                ? new("known-truncate-overflow", "")
                : new("pyiceberg-filter-rows", $"{f}: expected {Ids(expected)}, PyIceberg read {Ids(theirs)}");
        return Outcome.Agree;
    }

    private static bool HoldsUuid(IcebergType t) => t switch
    {
        StructType s => s.Fields.Any(f => HoldsUuid(f.Type)),
        ListType l => HoldsUuid(l.ElementType),
        MapType m => HoldsUuid(m.KeyType) || HoldsUuid(m.ValueType),
        _ => t.TypeId == TypeId.Uuid,
    };

    /// <summary>truncate[W] of an int or long within W of the type's minimum: v - (((v % W) + W) % W) overflows. Java's
    /// reference implementation and .NET wrap; PyIceberg's Python integers do not, so its projected literal never
    /// matches the written partition value.</summary>
    private static bool TruncateOverflows(Case c, FilterPick f) =>
        c.Partition?.Any(p => p.Column == f.Column && p.Transform is TruncateTransform t && f.Value switch
        {
            int i => i < int.MinValue + t.Width,
            long l => l < long.MinValue + t.Width,
            _ => false,
        }) == true;

    private static async Task<Outcome> PyIcebergWritesAsync(Case c, string dir)
    {
        Directory.CreateDirectory(dir);
        var ipc = Path.Combine(dir, "rows.arrow");
        try
        {
            await using var stream = File.Create(ipc);
            using var writer = new ArrowFileWriter(stream, ArrowInterop.ToArrow(c.Schema));
            await writer.WriteRecordBatchAsync(ArrowInterop.ToRecordBatch(c.Schema, c.Rows));
            await writer.WriteEndAsync();
        }
        catch (Exception e)
        {
            return new("dotnet-arrow-failed", e.GetType().Name + ": " + e.Message);
        }
        string location;
        try
        {
            var written = await TestEnvironment.Current.OracleAsync("write-arrow", Path.Combine(dir, "warehouse"), ipc, c.FormatVersion.ToString(System.Globalization.CultureInfo.InvariantCulture));
            location = written["metadata_location"]!.GetValue<string>();
        }
        catch (Exception e)
        {
            // pyarrow rejects a required child that holds nulls under a null parent struct (Arrow allows the child
            // values; pyarrow's non-nullable check does not look at the parent). Known, see compare-arrow in the oracle.
            if (e.Message.Contains("cannot be viewed as non-nullable", StringComparison.Ordinal)
                || e.Message.Contains("non-nullable but contains nulls", StringComparison.Ordinal))
                return new(KnownLimitation, "");
            return new("pyiceberg-write-failed", LastLine(e.Message));
        }
        Table t;
        try
        {
            var catalog = new InMemoryCatalog(Path.Combine(dir, "dotnet"));
            await catalog.CreateNamespaceAsync(Namespace.Of("gen"));
            t = await catalog.RegisterTableAsync(TableIdentifier.Of("gen", "t"), location);
        }
        catch (Exception e)
        {
            return new("dotnet-load-failed", e.GetType().Name + ": " + e.Message);
        }
        return await CompareAsync(t, c.Rows.Count);
    }

    private static async Task<Outcome> CompareAsync(Table t, int expectedRows)
    {
        TableFingerprint ours;
        try
        {
            ours = await TableFingerprint.ComputeAsync(t);
        }
        catch (Exception e)
        {
            return new("dotnet-read-failed", e.GetType().Name + ": " + e.Message);
        }
        TableFingerprint theirs;
        try
        {
            theirs = TableFingerprint.FromJson(await TestEnvironment.Current.OracleRawAsync("fingerprint", t.MetadataLocation!));
        }
        catch (Exception e)
        {
            return new("pyiceberg-read-failed", LastLine(e.Message));
        }
        if (ours.Rows != expectedRows) return new("dotnet-row-count", $"expected {expectedRows} rows, .NET read {ours.Rows}");
        var diffs = ours.DifferencesFrom(theirs);
        if (diffs.Count > 0) return new("mismatch", string.Join("; ", diffs));
        return ours.RowHash != theirs.RowHash ? new("mismatch", "row hash only") : Outcome.Agree;
    }

    private static string LastLine(string message)
    {
        var lines = message.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (lines.Length <= 2) return message;
        // Keep the first line (which command failed) and the exception line. PyIceberg's schema errors end in a table,
        // not the exception, so take the last "...Error: " line and the table rows it marks as mismatched.
        var exception = Array.FindLastIndex(lines, l => System.Text.RegularExpressions.Regex.IsMatch(l, @"^[A-Za-z_.]*(Error|Exception): "));
        var marked = lines.Where(l => l.Contains('\u274c', StringComparison.Ordinal)).Select(l => l.Trim('\u2502', ' '));
        return string.Join(" … ", new[] { lines[0], exception >= 0 ? lines[exception] : lines[^1] }.Concat(marked));
    }

    // ---------------------------------------------------------------- generated cases

    private sealed record DeleteSpec(int Modulus, int Remainder, DeleteStrategy Strategy)
    {
        public bool Deletes(long id) => id % Modulus == Remainder;
        public override string ToString() => $"id % {Modulus} == {Remainder} by {Strategy}";
    }

    private sealed record PartitionPick(string Column, Transform Transform)
    {
        public override string ToString() => $"{Transform}({Column})";
    }

    /// <summary>An equality filter on a top-level column. <see cref="Value"/> is the physical value as stored in the
    /// rows (int days, long micros), which the binder accepts; the oracle gets ISO strings for dates and timestamps.</summary>
    private sealed record FilterPick(string Column, IcebergType Type, object Value)
    {
        public JsonObject Json() => new() { ["op"] = "eq", ["term"] = Column, ["value"] = JsonLiteral() };

        private JsonNode JsonLiteral() => (Type.TypeId, Value) switch
        {
            (TypeId.Date, int days) => DateOnly.FromDayNumber(days + 719_162).ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture),
            (TypeId.Timestamp, long us) => Iso(us),
            (TypeId.TimestampTz, long us) => Iso(us) + "+00:00",
            (_, int i) => i,
            (_, long l) => l,
            (_, string text) => text,
            _ => throw new NotSupportedException($"{Type} filter value {Value}"),
        };

        private static string Iso(long micros) =>
            new DateTime(DateTime.UnixEpoch.Ticks + micros * 10, DateTimeKind.Utc).ToString("yyyy-MM-ddTHH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture);

        public override string ToString() => $"{Column} == {JsonLiteral().ToJsonString()}";
    }

    private sealed record Case(int Seed, int FormatVersion, Schema Schema, List<object?[]> Rows, int Batches, DeleteSpec? Delete,
        IReadOnlyList<PartitionPick>? Partition = null, FilterPick? Filter = null)
    {
        public int ExpectedRows => Delete is null ? Rows.Count : Rows.Count(r => !Delete.Deletes((long)r[0]!));

        public string? PartitionText => Partition is { Count: > 0 } p ? $"[{string.Join(", ", p)}]" : null;

        public List<long> ExpectedIds(FilterPick f)
        {
            var index = Schema.Columns.ToList().FindIndex(c => c.Name == f.Column);
            return Rows.Where(r => Equals(r[index], f.Value) && (Delete is null || !Delete.Deletes((long)r[0]!)))
                .Select(r => (long)r[0]!).Order().ToList();
        }

        public async Task<Table> WriteWithDotnetAsync(string dir)
        {
            var catalog = new InMemoryCatalog(dir);
            await catalog.CreateNamespaceAsync(Namespace.Of("gen"));
            PartitionSpec? spec = null;
            if (Partition is { Count: > 0 })
            {
                var builder = PartitionSpec.BuilderFor(Schema);
                foreach (var p in Partition) builder.Add(p.Column, p.Transform);
                spec = builder.Build();
            }
            var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("gen", "t"), Schema, Spec: spec, FormatVersion: FormatVersion));
            var size = Math.Max(1, (Rows.Count + Batches - 1) / Batches);
            foreach (var chunk in Rows.Chunk(size)) await TableMutations.InsertAsync(t, chunk);
            if (Delete is { } d) await TableMutations.DeleteWhereAsync(t, r => d.Deletes((long)r[0]!), d.Strategy);
            return t;
        }

        /// <summary>Candidates one step smaller, most aggressive first.</summary>
        public IEnumerable<Case> Smaller()
        {
            if (Rows.Count > 1) yield return this with { Rows = Rows.Take(Rows.Count / 2).ToList() };
            if (Rows.Count > 1) yield return this with { Rows = Rows.Skip(Rows.Count / 2).ToList() };
            for (var c = Schema.Columns.Count - 1; c >= 1; c--) yield return WithoutColumn(c);
            if (Delete is not null) yield return this with { Delete = null };
            if (Partition is { Count: > 0 }) yield return this with { Partition = null };
            if (Partition is { Count: > 1 })
                foreach (var p in Partition) yield return this with { Partition = [p] };
            if (Batches > 1) yield return this with { Batches = 1 };
            if (Rows.Count is > 1 and <= 8)
                for (var r = 0; r < Rows.Count; r++)
                {
                    var skip = r;
                    yield return this with { Rows = Rows.Where((_, i) => i != skip).ToList() };
                }
        }

        private Case WithoutColumn(int c) => this with
        {
            Schema = new Schema(0, Schema.Columns.Where((_, i) => i != c)),
            Rows = Rows.Select(r => r.Where((_, i) => i != c).ToArray()).ToList(),
            Partition = Partition?.Where(p => p.Column != Schema.Columns[c].Name).ToList(),
            Filter = Filter?.Column == Schema.Columns[c].Name ? null : Filter,
        };

        private static readonly Transform[] Transforms =
        [
            Transform.Identity, Transform.Bucket(1), Transform.Bucket(4), Transform.Bucket(7), Transform.Bucket(16),
            Transform.Truncate(1), Transform.Truncate(3), Transform.Truncate(10), Transform.Truncate(100),
            Transform.Year, Transform.Month, Transform.Day, Transform.Hour,
        ];

        /// <summary>Filter literals the oracle's expression format carries without loss.</summary>
        private static bool Filterable(IcebergType t) => t.TypeId is TypeId.Int or TypeId.Long or TypeId.String or TypeId.Date or TypeId.Timestamp or TypeId.TimestampTz;

        /// <summary>Half the cases get one or two partition fields over distinct top-level primitive columns (floats,
        /// doubles and nanosecond timestamps left out), plus an equality filter on a filterable partition source with
        /// a value drawn from the rows. A separate Random, so the schemas and rows of every seed stay as before.</summary>
        private static (List<PartitionPick>?, FilterPick?) PartitionAndFilter(int seed, Schema schema, List<object?[]> rows)
        {
            var r = new Random(seed ^ 0x2C1B3C6D);
            if (r.Next(2) == 0) return (null, null);
            var sources = schema.Columns
                .Where(c => c.Type.IsPrimitive && c.Type.TypeId is not (TypeId.Float or TypeId.Double or TypeId.TimestampNs or TypeId.TimestampTzNs))
                .OrderBy(_ => r.Next()).Take(1 + r.Next(2)).ToList();
            var picks = new List<PartitionPick>();
            foreach (var c in sources)
            {
                var valid = Transforms.Where(t => t.CanTransform(c.Type)).ToList();
                if (valid.Count > 0) picks.Add(new PartitionPick(c.Name, valid[r.Next(valid.Count)]));
            }
            if (picks.Count == 0) return (null, null);
            var columns = schema.Columns.ToList();
            var filterable = picks.Select(p => columns.FindIndex(c => c.Name == p.Column)).Where(i => Filterable(columns[i].Type)).ToList();
            if (filterable.Count == 0) return (picks, null);
            var index = filterable[r.Next(filterable.Count)];
            var values = rows.Select(row => row[index]).Where(v => v is not null).ToList();
            return (picks, values.Count == 0 ? null : new FilterPick(columns[index].Name, columns[index].Type, values[r.Next(values.Count)]!));
        }

        public static Case Generate(int seed, int maxFormatVersion, bool partitioned = false)
        {
            var g = new Gen(new Random(seed));
            var formatVersion = g.R.Next(2) == 0 ? 2 : maxFormatVersion;
            var columns = new List<NestedField> { NestedField.Require(g.NextId(), "id", PrimitiveType.Long) };
            var count = 1 + g.R.Next(8);
            for (var i = 0; i < count; i++)
            {
                var type = g.Type(depth: 0, formatVersion, topLevel: true);
                columns.Add(new NestedField(g.NextId(), $"c{i}", type, g.R.Next(3) == 0));
            }
            var schema = new Schema(0, columns);
            var rows = Enumerable.Range(0, g.R.Next(1, 60)).Select(i =>
            {
                var row = new object?[columns.Count];
                row[0] = (long)i;
                for (var c = 1; c < columns.Count; c++) row[c] = g.Value(columns[c].Type, columns[c].Required);
                return row;
            }).ToList();
            DeleteSpec? delete = g.R.Next(2) == 0 ? null
                : new DeleteSpec(2 + g.R.Next(5), 0, g.R.Next(2) == 0 ? DeleteStrategy.CopyOnWrite : DeleteStrategy.PositionDeletes);
            var batches = 1 + g.R.Next(3);
            var (partition, filter) = partitioned ? PartitionAndFilter(seed, schema, rows) : (null, null);
            return new Case(seed, formatVersion, schema, rows, batches, delete, partition, filter);
        }
    }

    /// <summary>Seeded schema and value generator. Values stay inside what both engines can represent (years 1..9999).</summary>
    private sealed class Gen(Random r)
    {
        public Random R { get; } = r;
        private int _id;

        public int NextId() => ++_id;

        private const int MinDay = -719_162; // 0001-01-01
        private const int MaxDay = 2_932_896; // 9999-12-31
        private const long MicrosPerDay = 86_400_000_000L;

        public IcebergType Type(int depth, int formatVersion, bool topLevel)
        {
            var roll = R.Next(depth >= 2 ? 16 : 20);
            return roll switch
            {
                0 => PrimitiveType.Boolean,
                1 => PrimitiveType.Int,
                2 => PrimitiveType.Long,
                3 => PrimitiveType.Float,
                4 => PrimitiveType.Double,
                5 or 6 => Decimal(),
                7 => PrimitiveType.Date,
                8 => PrimitiveType.Time,
                9 => PrimitiveType.Timestamp,
                10 => PrimitiveType.TimestampTz,
                11 => PrimitiveType.String,
                12 => PrimitiveType.Uuid,
                13 => PrimitiveType.Binary,
                14 => new FixedType(1 + R.Next(20)),
                // Nanosecond timestamps (v3) only at top level: the oracle canonicalises nested temporals through
                // Python datetimes, which hold microseconds.
                15 => formatVersion >= 3 && topLevel ? (R.Next(2) == 0 ? PrimitiveType.TimestampNs : PrimitiveType.TimestampTzNs) : PrimitiveType.String,
                16 or 17 => new StructType(Enumerable.Range(0, 1 + R.Next(4))
                    .Select(i => new NestedField(NextId(), $"f{i}", Type(depth + 1, formatVersion, false), R.Next(3) == 0)).ToList()),
                18 => new ListType(NextId(), Type(depth + 1, formatVersion, false), R.Next(3) == 0),
                _ => new MapType(NextId(), MapKey(), NextId(), Type(depth + 1, formatVersion, false), R.Next(3) == 0),
            };
        }

        private DecimalType Decimal()
        {
            var p = 1 + R.Next(38);
            return new DecimalType(p, R.Next(p + 1));
        }

        private PrimitiveType MapKey() => R.Next(4) switch
        {
            0 => PrimitiveType.String,
            1 => PrimitiveType.Int,
            2 => PrimitiveType.Long,
            _ => PrimitiveType.Date,
        };

        /// <summary>Lists and maps whose values contain a struct are never null: PyIceberg 0.12 reads them as empty
        /// (known: ShadowCompareTests.NestedRows for lists; apache/iceberg-python#4114 for maps, fixed by #4118 after
        /// 0.12), and the campaign should find new things.</summary>
        private static bool HoldsStructList(IcebergType t) => t switch
        {
            ListType l => l.ElementType is StructType || HoldsStructList(l.ElementType),
            MapType m => m.ValueType is StructType || HoldsStructList(m.ValueType),
            _ => false,
        };

        public object? Value(IcebergType type, bool required)
        {
            if (!required && !HoldsStructList(type) && R.Next(6) == 0) return null;
            return type switch
            {
                StructType s => s.Fields.Select(f => Value(f.Type, f.Required)).ToArray(),
                ListType l => Enumerable.Range(0, R.Next(5)).Select(_ => Value(l.ElementType, l.ElementRequired)).ToList(),
                MapType m => Map(m),
                DecimalType d => DecimalValue(d),
                FixedType f => Bytes(f.Length),
                PrimitiveType p => p.TypeId switch
                {
                    TypeId.Boolean => R.Next(2) == 0,
                    TypeId.Int => Pick(0, 1, -1, int.MinValue, int.MaxValue, R.Next(int.MinValue, int.MaxValue)),
                    TypeId.Long => Pick(0L, -1L, long.MinValue, long.MaxValue, R.NextInt64(long.MinValue, long.MaxValue)),
                    TypeId.Float => Pick(0f, -0f, float.NaN, float.PositiveInfinity, float.NegativeInfinity, float.Epsilon, float.MaxValue, float.MinValue, (float)(R.NextDouble() * 2e6 - 1e6)),
                    TypeId.Double => Pick(0d, -0d, double.NaN, double.PositiveInfinity, double.NegativeInfinity, double.Epsilon, double.MaxValue, double.MinValue, R.NextDouble() * 2e12 - 1e12),
                    TypeId.Date => Pick(0, MinDay, MaxDay, -1, R.Next(MinDay, MaxDay)),
                    TypeId.Time => Pick(0L, MicrosPerDay - 1, R.NextInt64(0, MicrosPerDay)),
                    TypeId.Timestamp or TypeId.TimestampTz => Pick(0L, -1L, MinDay * MicrosPerDay, (MaxDay + 1L) * MicrosPerDay - 1, R.NextInt64(MinDay * MicrosPerDay, MaxDay * MicrosPerDay)),
                    // Nanoseconds since the epoch in a long cover 1677..2262.
                    TypeId.TimestampNs or TypeId.TimestampTzNs => Pick(0L, -1L, long.MinValue + 1, long.MaxValue, R.NextInt64(long.MinValue + 1, long.MaxValue)),
                    TypeId.String => Text(),
                    TypeId.Uuid => new Guid(Bytes(16)),
                    TypeId.Binary => Bytes(R.Next(24)),
                    _ => throw new NotSupportedException(p.ToString()),
                },
                _ => throw new NotSupportedException(type.ToString()),
            };
        }

        private Dictionary<object, object?> Map(MapType m)
        {
            var map = new Dictionary<object, object?>();
            for (var i = R.Next(5); i > 0; i--) map.TryAdd(Value(m.KeyType, required: true)!, Value(m.ValueType, m.ValueRequired));
            return map;
        }

        private IcebergDecimal DecimalValue(DecimalType d)
        {
            var max = BigInteger.Pow(10, d.Precision) - 1;
            var unscaled = R.Next(4) switch
            {
                0 => max,
                1 => -max,
                2 => BigInteger.Zero,
                _ => new BigInteger(Bytes(17), isUnsigned: true) % (max + 1) * (R.Next(2) == 0 ? 1 : -1),
            };
            return new IcebergDecimal(unscaled, d.Scale);
        }

        private byte[] Bytes(int n)
        {
            var b = new byte[n];
            R.NextBytes(b);
            return b;
        }

        private static readonly string[] Alphabet = ["a", "Z", "0", " ", "é", "ß", "ü", "€", "中", "😀", "\t", "\u0001", "'", "\"", "\\"];

        private string Text() => string.Concat(Enumerable.Range(0, R.Next(12)).Select(_ => Alphabet[R.Next(Alphabet.Length)]));

        private T Pick<T>(params T[] options) => options[R.Next(options.Length)];
    }
}
