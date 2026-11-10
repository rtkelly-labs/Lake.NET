/// In-process trace capture: a fault-injecting catalog and an ActivityListener.
module Lake.CommitTraces.Tests.Harness

open System
open System.Collections.Concurrent
open System.Diagnostics
open System.IO
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
                return raise (CommitStateUnknownException("injected: HTTP 500 from the catalog", ServerResponded = f.Responded))
            }
            :> Task

/// Records every Iceberg.Net span whose table is in <paramref name="ns"/>.
type Capture(ns: string) =
    let spans = ConcurrentQueue<Activity>()

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

    do ActivitySource.AddActivityListener listener

    /// The spans as the telemetry records them.
    member _.Spans = spans |> Seq.map Spans.ofActivity |> List.ofSeq

    interface IDisposable with
        member _.Dispose() = listener.Dispose()

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
