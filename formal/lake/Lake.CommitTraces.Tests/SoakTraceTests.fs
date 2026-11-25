/// The first data source: a soak run captured in process. Writers race on one table through the library's real
/// commit paths (append, insert, upsert, delete under every strategy) while the catalog injects unknown outcomes; every
/// logical commit in the recorded trace must parse.
module Lake.CommitTraces.Tests.SoakTraceTests

open System
open System.Collections.Concurrent
open System.Threading.Tasks
open Lake.CommitTraces
open Lake.CommitTraces.Tests.Harness
open Iceberg.Net.Catalog
open Iceberg.Net.Data
open Iceberg.Net.Diagnostics
open Iceberg.Net.Metadata
open Xunit
open Xunit.Abstractions

let private writers = 4

/// FORMAL_TRACES_SOAK_SECONDS overrides the default of 3.
let private seconds =
    match Int32.TryParse(Environment.GetEnvironmentVariable "FORMAL_TRACES_SOAK_SECONDS") with
    | true, s when s > 0 -> s
    | _ -> 3

let private row (k: int64) (v: string) : obj[] = [| box k; box v; box (int (k % 3L)) |]

let private writer (catalog: ICatalog) (id: TableIdentifier) (deadline: DateTime) (w: int) (outcomes: ConcurrentBag<string>) =
    task {
        let rnd = Random(w * 7919)
        let mine = ResizeArray<int64>()
        let mutable next = 0L
        let! loaded = catalog.LoadTableAsync id
        let mutable table = loaded
        while DateTime.UtcNow < deadline do
            let strategy = enum<DeleteStrategy> (rnd.Next(1, 4))
            let fresh () =
                let k = 1_000L + int64 w * 100_000L + next
                next <- next + 1L
                k
            try
                match rnd.Next 5 with
                | 0 ->
                    let keys = List.init (rnd.Next(1, 4)) (fun _ -> fresh ())
                    let! _ = TableMutations.InsertAsync(table, keys |> List.map (fun k -> row k "i"))
                    mine.AddRange keys
                | 1 when mine.Count > 0 ->
                    let k = mine[rnd.Next mine.Count]
                    let! _ = TableMutations.UpsertAsync(table, [ row k $"w%d{w}" ], strategy = strategy)
                    ()
                | 2 when mine.Count > 0 ->
                    let k = mine[rnd.Next mine.Count]
                    mine.Remove k |> ignore
                    let! _ = TableMutations.DeleteByKeysAsync(table, [ "id"; "bucket" ], [ [| box k; box (int (k % 3L)) |] ], strategy)
                    ()
                | 3 ->
                    let k = fresh ()
                    let! files = TableWriter.WriteAsync(table, [ row k "a" ])
                    let! _ = SnapshotProducer.Append(table).AddFiles(files).CommitAsync()
                    mine.Add k
                | _ ->
                    let! _ = TableMutations.UpsertAsync(table, [ row (int64 (rnd.Next 5)) $"hot-w%d{w}" ], strategy = strategy)
                    ()
                outcomes.Add "ok"
            with
            | :? CommitFailedException ->
                outcomes.Add "gave-up"
                let! reloaded = catalog.LoadTableAsync id
                table <- reloaded
            | :? CommitStateUnknownException ->
                outcomes.Add "unknown"
                let! reloaded = catalog.LoadTableAsync id
                table <- reloaded
    }

type SoakTraceTests(output: ITestOutputHelper) =

    [<Fact>]
    [<Trait("Category", "Soak")>]
    member _.``A soak run's commits all follow the protocol``() =
        task {
            let! catalog, table, ns, dir =
                createTable 42 0.15 [ "commit.retry.num-retries", "50"; "commit.retry.min-wait-ms", "1"; "commit.retry.max-wait-ms", "20" ]
            try
                use capture = new Capture(ns)
                // One of each fault first, so every branch is reached whatever the random ones do.
                for apply, responded in [ true, true; false, true; true, false; false, false ] do
                    catalog.Planned.Enqueue { Apply = apply; Responded = responded }
                let outcomes = ConcurrentBag<string>()
                let deadline = DateTime.UtcNow.AddSeconds(float seconds)
                do!
                    Task.WhenAll [ for w in 0 .. writers - 1 -> Task.Run(fun () -> writer catalog table.Identifier deadline w outcomes :> Task) ]

                let spans = capture.Spans
                let report, projection = Spans.check spans
                output.WriteLine
                    $"%d{report.Slices} commits, %d{report.Events} events, %d{projection.OrphanCatalogCommits} catalog commits outside a commit span"
                Assert.True(report.Passed, Spans.explain report)
                Assert.Empty(Spans.snapshots spans)
                // Not vacuous: the library tags each catalog commit inside a commit span with its attempt.
                Assert.Contains(spans, fun s -> s.Name = Spans.Names.commitTable && s.Tags.ContainsKey IcebergTelemetry.Tags.CommitAttempt)

                // Not vacuous: the run went through every branch the injected faults and the races can reach.
                let seen = (Spans.project spans).Events |> List.map _.Event |> Set.ofList
                output.WriteLine $"events seen: %A{seen}"
                output.WriteLine $"writer outcomes: %A{outcomes |> Seq.countBy id |> List.ofSeq}"
                for e in [ SwapApplied; SwapRejected; SwapUnknown; Found; Absent; Retry; Reload; Committed; LeftUnknown ] do
                    Assert.True(seen.Contains e, $"the soak never produced %A{e}")
                Assert.DoesNotContain(SwapFailed, seen) // commit_table classifies every failure
            finally
                catalog.Dispose()
                cleanUp dir
        }
