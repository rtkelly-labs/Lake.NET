/// In-process trace capture: a fault-injecting catalog, an ActivityListener, and the two enrichments that stand in
/// for the proposed telemetry (classified catalog failures, resolution events) so tests can check against both
/// today's spans and the proposed ones.
module Lake.CommitTraces.Tests.Harness

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Runtime.ExceptionServices
open System.Threading.Tasks
open Lake.CommitTraces
open Iceberg.Net.Catalog
open Iceberg.Net.Diagnostics
open Iceberg.Net.Metadata
open Iceberg.Net.Partitioning
open Iceberg.Net.Types

/// What an injected swap failure does: apply the swap first or not, and whether the "server" responded (so the
/// writer may resolve the outcome by reloading).
type Fault = { Apply: bool; Responded: bool }

/// An in-memory catalog whose pointer swap fails with <see cref="CommitStateUnknownException"/>: the faults queued in
/// <see cref="Planned"/> first, then at random with probability <c>rate</c>.
type FaultyCatalog(warehouse: string, seed: int, rate: float) =
    inherit InMemoryCatalog(warehouse)

    let rnd = Random(seed)
    let roll () = lock rnd (fun () -> rnd.NextDouble())

    /// Faults to inject, in order, before random ones.
    member val Planned = ConcurrentQueue<Fault>()

    /// commit_table span ID to whether the swap applied, for every fault the writer could resolve (server responded).
    member val Applied = ConcurrentDictionary<string, bool>()

    /// Every fault injected.
    member val Injected = ConcurrentBag<Fault>()

    member private this.Swap(id, expected, location, metadata, ct) =
        base.SwapMetadataLocationAsync(id, expected, location, metadata, ct)

    override this.SwapMetadataLocationAsync(id, expected, location, metadata, ct) : Task =
        let fault =
            match this.Planned.TryDequeue() with
            | true, f -> Some f
            | _ when rate > 0.0 && roll () < rate -> Some { Apply = roll () < 0.5; Responded = roll () < 0.85 }
            | _ -> None
        match fault with
        | None -> this.Swap(id, expected, location, metadata, ct)
        | Some f ->
            task {
                if f.Apply then do! this.Swap(id, expected, location, metadata, ct)
                this.Injected.Add f
                match Activity.Current with
                | a when not (isNull a) && f.Responded && a.OperationName = Spans.Names.commitTable ->
                    this.Applied[a.SpanId.ToHexString()] <- f.Apply
                | _ -> ()
                return raise (CommitStateUnknownException("injected: HTTP 500 from the catalog", ServerResponded = f.Responded))
            }
            :> Task

/// Records every Iceberg.Net span whose table is in <paramref name="ns"/>, and how each failed catalog commit failed.
type Capture(ns: string) =
    let spans = ConcurrentQueue<Activity>()
    let failures = ConcurrentDictionary<string, string * string>()

    let mine (a: Activity) =
        match a.GetTagItem IcebergTelemetry.Tags.Table with
        | :? string as t -> t.StartsWith(ns + ".", StringComparison.Ordinal)
        | _ -> false

    let listener =
        new ActivityListener(
            ShouldListenTo = (fun s -> s.Name = IcebergTelemetry.Name),
            Sample = SampleActivity<ActivityContext>(fun _ -> ActivitySamplingResult.AllDataAndRecorded),
            ActivityStopped = (fun a -> if mine a then spans.Enqueue a)
        )

    // Stands in for the proposed `catch (Exception e) when (telemetry.FailedWith(e))` in CommitTableAsync: the last
    // exception thrown while a commit_table span is current is the one that leaves it.
    let firstChance =
        EventHandler<FirstChanceExceptionEventArgs>(fun _ args ->
            match Activity.Current with
            | a when not (isNull a) && a.OperationName = Spans.Names.commitTable && mine a ->
                failures[a.SpanId.ToHexString()] <- (IcebergTelemetry.OutcomeOf args.Exception, args.Exception.GetType().FullName)
            | _ -> ())

    do
        ActivitySource.AddActivityListener listener
        AppDomain.CurrentDomain.FirstChanceException.AddHandler firstChance

    /// The spans as today's telemetry records them.
    member _.Spans = spans |> Seq.map Spans.ofActivity |> List.ofSeq

    /// Failure classification per commit_table span ID.
    member _.Failures = failures :> IReadOnlyDictionary<_, _>

    interface IDisposable with
        member _.Dispose() =
            AppDomain.CurrentDomain.FirstChanceException.RemoveHandler firstChance
            listener.Dispose()

/// Tag failed commit_table spans with their outcome and error type, as the proposed FailedWith call would.
let classify (failures: IReadOnlyDictionary<string, string * string>) (spans: SpanRecord list) =
    spans
    |> List.map (fun s ->
        match failures.TryGetValue s.SpanId with
        | true, (outcome, errorType) when s.Name = Spans.Names.commitTable && Spans.catalogOutcome s <> IcebergTelemetry.Outcomes.Success ->
            { s with Tags = s.Tags.Add(IcebergTelemetry.Tags.Outcome, outcome).Add(IcebergTelemetry.Tags.ErrorType, errorType) }
        | _ -> s)

/// Add the proposed resolution event to each commit span: after a resolvable unknown outcome, at the end of the reload
/// that resolved it, found or absent. A writer that never resolved the outcome gets no event.
let resolve (applied: IReadOnlyDictionary<string, bool>) (spans: SpanRecord list) =
    let children = spans |> List.groupBy _.ParentSpanId |> Map.ofList
    spans
    |> List.map (fun c ->
        if c.Name <> Spans.Names.commit then c
        else
            let mine = children |> Map.tryFind c.SpanId |> Option.defaultValue [] |> List.sortBy _.Start
            let resolutions =
                [ for s in mine do
                      match applied.TryGetValue s.SpanId with
                      | true, wasApplied ->
                          match mine |> List.tryFind (fun r -> r.Name = Spans.Names.loadTable && r.Start >= s.End) with
                          // Only the reload that resolves (SnapshotProducer's AppliedAsync) emits it: one that follows a retry does not.
                          | Some reload when
                              Spans.reload reload = Reload
                              && not (c.Events |> List.exists (fun e -> e.Name = Spans.Names.retry && e.Time >= s.End && e.Time <= reload.Start))
                              ->
                              { Name = Spans.Names.resolution
                                Time = reload.End
                                Tags = Map [ Spans.Names.resolutionTag, (if wasApplied then "found" else "absent") ] }
                          | _ -> ()
                      | _ -> () ]
            { c with Events = c.Events @ resolutions })

/// The proposed telemetry: classified failures and resolution events.
let enrich (capture: Capture) (catalog: FaultyCatalog) =
    capture.Spans |> classify capture.Failures |> resolve catalog.Applied

let schema =
    Schema(
        0,
        [ NestedField.Require(1, "id", PrimitiveType.Long)
          NestedField.Optional(2, "name", PrimitiveType.String)
          NestedField.Require(3, "bucket", PrimitiveType.Int) ],
        identifierFieldIds = [ 1; 3 ]
    )

/// A fresh catalog in a temporary warehouse and a table in a namespace unique to this test.
let createTable (seed: int) (rate: float) (properties: (string * string) list) =
    task {
        let dir = Path.Combine(Path.GetTempPath(), "formal-traces", Guid.NewGuid().ToString "N")
        let ns = "traces_" + Guid.NewGuid().ToString "N"
        let catalog = new FaultyCatalog(dir, seed, rate)
        let id = TableIdentifier(Namespace.Of ns, "t")
        do! catalog.CreateNamespaceAsync(Namespace.Of ns)
        let! table =
            catalog.CreateTableAsync(
                CreateTableRequest(
                    id,
                    schema,
                    PartitionSpec.BuilderFor(schema).Identity("bucket").Build(),
                    Properties = readOnlyDict properties,
                    FormatVersion = 2
                )
            )
        return catalog, table, ns, dir
    }

let cleanUp (dir: string) =
    try
        if Directory.Exists dir then Directory.Delete(dir, true)
    with _ -> ()
