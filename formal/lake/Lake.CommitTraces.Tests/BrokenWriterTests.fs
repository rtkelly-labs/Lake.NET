/// End to end on real spans: Iceberg.Net's own commit path parses under every injected outcome, and hand-written
/// writers that break the protocol are caught, on the spans as recorded: commit_table spans classify their failures,
/// and the commit span says how each unknown outcome was resolved.
module Lake.CommitTraces.Tests.BrokenWriterTests

open System.Threading.Tasks
open Formal.Traces
open Lake.CommitTraces
open Lake.CommitTraces.Tests.Harness
open Iceberg.Net.Catalog
open Iceberg.Net.Data
open Iceberg.Net.Diagnostics
open Iceberg.Net.Metadata
open Xunit
open Xunit.Abstractions

let private retries = [ "commit.retry.num-retries", "3"; "commit.retry.min-wait-ms", "1"; "commit.retry.max-wait-ms", "5" ]

let private row (k: int64) : obj[] = [| box k; box "v"; box (int (k % 3L)) |]

/// Run <paramref name="write"/> against a table whose next swaps fail as planned; return the spans it recorded.
let private record (faults: Fault list) (write: Table -> Task) =
    task {
        let! catalog, table, ns, dir = createTable 1 0.0 retries
        try
            use capture = new Capture(ns)
            for f in faults do catalog.Planned.Enqueue f
            try
                do! write table
            with
            | :? CommitStateUnknownException
            | :? CommitFailedException -> ()
            return capture.Spans
        finally
            catalog.Dispose()
            cleanUp dir
    }

let private appendOne (table: Table) : Task =
    task {
        let! files = TableWriter.WriteAsync(table, [ row 1L ])
        let! _ = SnapshotProducer.Append(table).AddFiles(files).CommitAsync()
        ()
    }

let private props = readOnlyDict [ "k", "v" ]

/// Retries a commit whose outcome is unknown, as if it were a conflict, without looking.
let private blindRetry (table: Table) : Task =
    task {
        use scope = IcebergTelemetry.StartCommit(string table.Identifier, "set-properties")
        try
            let! _ = table.SetPropertiesAsync props
            ()
        with :? CommitStateUnknownException as unknown ->
            scope.Retrying(1, CommitFailedException("treated as a conflict", unknown))
            let! _ = table.RefreshAsync()
            let! _ = table.SetPropertiesAsync props
            ()
        scope.Committed(0L, "set-properties")
    }

/// Reloads after an unknown outcome, then commits again whatever it finds. With <paramref name="records"/> it also
/// records what it found, as the library does, before ignoring it.
let private doubleApply (records: bool) (table: Table) : Task =
    task {
        use scope = IcebergTelemetry.StartCommit(string table.Identifier, "set-properties")
        try
            let! _ = table.SetPropertiesAsync props
            ()
        with :? CommitStateUnknownException ->
            let! _ = table.RefreshAsync()
            if records then scope.Resolved(table.Metadata.Properties.ContainsKey "k")
            let! _ = table.SetPropertiesAsync props
            ()
        scope.Committed(0L, "set-properties")
    }

let private eventsOf spans =
    (Spans.project spans).Events |> List.map _.Event

type BrokenWriterTests(output: ITestOutputHelper) =

    let check label spans =
        let report, _ = Spans.check spans
        output.WriteLine $"%s{label}: %A{eventsOf spans}"
        if not report.Passed then output.WriteLine(Spans.explain report)
        report

    [<Theory>]
    [<InlineData(true, true)>]
    [<InlineData(false, true)>]
    [<InlineData(true, false)>]
    [<InlineData(false, false)>]
    member _.``The library's own commit parses whatever the catalog did``(apply: bool, responded: bool) =
        task {
            let! spans = record [ { Apply = apply; Responded = responded } ] appendOne
            Assert.True((check "spans" spans).Passed)
            Assert.Empty(Spans.snapshots spans)
            let expected =
                match apply, responded with
                | true, true -> [ Begin; SwapUnknown; Reload; Found; Committed ]
                | false, true -> [ Begin; SwapUnknown; Reload; Absent; Retry; Reload; SwapApplied; Committed ]
                | _, false -> [ Begin; SwapUnknown; LeftUnknown ]
            Assert.Equal<CommitEvent list>(expected, eventsOf spans)
        }

    [<Fact>]
    member _.``A writer that retries an unknown outcome blindly is caught``() =
        task {
            let! spans = record [ { Apply = false; Responded = true } ] blindRetry
            Assert.Equal<CommitEvent list>([ Begin; SwapUnknown; Retry; Reload; SwapApplied; Committed ], eventsOf spans)
            let report = check "spans" spans
            let _, e = Assert.Single report.Violations
            Assert.Equal("Uncertain", e.State)
            Assert.Equal(Lookahead.Event Retry, e.Got)
            Assert.Contains("must reload and look for the snapshot", Spans.explain report)
        }

    /// Recording what it found, the second swap is caught as a swap after the commit was found; not recording it, as a
    /// swap straight after the reload that followed an unknown outcome, which the protocol never allows either.
    [<Theory>]
    [<InlineData(true, "Done")>]
    [<InlineData(false, "Resolving")>]
    member _.``A writer that commits again after its commit was applied is caught``(records: bool, state: string) =
        task {
            let! spans = record [ { Apply = true; Responded = true } ] (doubleApply records)
            let report = check "spans" spans
            let _, e = Assert.Single report.Violations
            Assert.Equal(state, e.State)
            Assert.Equal(Lookahead.Event SwapApplied, e.Got)
        }
