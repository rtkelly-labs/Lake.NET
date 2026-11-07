/// Drives the real InMemoryCatalog and SnapshotProducer with the model's actions, and compares the two after every step.
///
/// Each writer is a real SnapshotProducer commit running on its own task. A catalog subclass holds every pointer swap
/// until the driver decides what happens to it, which is exactly what the model's actions say:
///   Begin w             start writer w's commit; it runs until its swap reaches the catalog
///   Apply(w, k)         the catalog performs the compare-and-swap of attempt k (InMemoryCatalog's own CAS)
///   Ack / Reject        the writer gets the result of that swap; a rejected writer refreshes, validates and retries
///   ServerError(w, k)   the writer gets CommitStateUnknownException with ServerResponded = true
///   Timeout(w, k)       the writer's call times out; the swap request stays pending and may still be applied
/// After each action the driver waits until the writer is blocked on its next swap or its commit has finished.
/// RestConformance holds the same commits at the HTTP layer instead, against the REST server.
module Lake.CommitProtocol.Tests.Conformance

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open Iceberg.Net.Catalog
open Iceberg.Net.Data
open Iceberg.Net.Manifests
open Iceberg.Net.Metadata
open Iceberg.Net.Partitioning
open Iceberg.Net.Types
open Lake.CommitProtocol

let internal writerKey = "formal.writer"

/// A swap request the catalog has received but not yet performed.
type private Request =
    { Expected: string
      NewLocation: string
      Metadata: TableMetadata }

/// InMemoryCatalog whose pointer swaps, when made by a writer, wait for the driver.
type private HookCatalog(warehouse: string) =
    inherit InMemoryCatalog(warehouse)

    /// The writer whose commit is running in this async flow (none during setup).
    static member val Current = AsyncLocal<Writer option>()

    /// Called on each writer swap with (writer, request); returns the task the writer's swap waits on.
    member val OnSwap: Writer -> Request -> Task = fun _ _ -> Task.CompletedTask with get, set

    /// The catalog's own compare-and-swap.
    member this.Swap(id: TableIdentifier, request: Request) =
        base.SwapMetadataLocationAsync(id, request.Expected, request.NewLocation, request.Metadata, CancellationToken.None)

    override this.SwapMetadataLocationAsync(id, expected, newLocation, metadata, ct) =
        match HookCatalog.Current.Value with
        | Some w ->
            this.OnSwap w { Expected = expected; NewLocation = newLocation; Metadata = metadata }
        | None -> base.SwapMetadataLocationAsync(id, expected, newLocation, metadata, ct)

/// What the real system looks like, in the model's terms (only what both can observe).
type Observed =
    { Pointer: File
      /// Metadata files that exist.
      MetadataFiles: Set<File>
      Writers: Map<Writer, string>
      /// Writer of each snapshot in the current history, oldest first (None: the seed).
      History: Writer option list
      LiveFiles: Set<File> }

/// The model's state, reduced to what Observed holds.
let expected (cfg: Config) (s: State) : Observed =
    let phase (p: Phase) =
        match p with
        | Waiting(k, _, _) -> sprintf "Waiting %d" k
        | other -> sprintf "%A" other
    { Pointer = s.Pointer
      MetadataFiles = s.Store |> Set.filter (fun f -> match f with Meta _ | Meta0 -> true | _ -> false)
      Writers = s.Writers |> Map.map (fun _ p -> phase p)
      History = Protocol.history s |> Set.toList |> List.sortBy (fun x -> x.SeqNo) |> List.map (fun x -> x.Writer)
      LiveFiles = Protocol.liveFiles (Protocol.history s) }

/// The model took a step the real system cannot take: it has no such request or reply, or the reply is not the one the
/// model expects. That too is a difference from the model.
exception NotPerformable of string

let notPerformable fmt = Printf.kprintf (NotPerformable >> raise) fmt

/// A real system the model's actions drive.
type IDriver =
    inherit IDisposable
    abstract Step: Action -> unit
    abstract Observe: unit -> Observed
    /// The model's state, reduced to what this system lets Observe see.
    abstract Expected: State -> Observed

let internal stepTimeout = TimeSpan.FromSeconds 30.

let internal wait (t: Task<'T>) = t.WaitAsync(stepTimeout).GetAwaiter().GetResult()

let internal waitUnit (t: Task) = t.WaitAsync(stepTimeout).GetAwaiter().GetResult()

let internal tcs () = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

/// Removes and returns d.[key]; a missing key is a step the real system cannot take.
let internal take (d: Dictionary<'K, 'V>) (key: 'K) (what: string) : 'V =
    match d.TryGetValue key with
    | true, v ->
        d.Remove key |> ignore
        v
    | _ -> notPerformable "The real system has no %s for %A" what key

let internal partitionName (p: Part) =
    match p with
    | P -> "P"
    | Q -> "Q"

let internal fileName (f: File) =
    match f with
    | Seed -> "seed.parquet"
    | Data w -> sprintf "%A.parquet" w
    | _ -> invalidArg "f" "not a data file"

let internal dataFile (root: string) (f: File) (part: Part) =
    DataFile(
        FilePath = Path.Combine(root, "data", fileName f),
        Partition = [| box (partitionName part) |],
        RecordCount = 1L,
        FileSizeInBytes = 1L
    )

let internal tableId = TableIdentifier.Of("formal", "t")

/// Creates the partitioned table and commits the seed file, with no wait between commit retries.
let internal setUp (catalog: ICatalog) (root: string) =
    waitUnit (catalog.CreateNamespaceAsync(Namespace.Of("formal")))
    let schema = Schema(NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "part", PrimitiveType.String))
    let spec = PartitionSpec.BuilderFor(schema).Identity("part").Build()
    let properties = readOnlyDict [ "commit.retry.min-wait-ms", "0"; "commit.retry.max-wait-ms", "0" ]
    let table = wait (catalog.CreateTableAsync(CreateTableRequest(tableId, schema, spec, Properties = properties)))
    wait (SnapshotProducer.Append(table).AddFiles([ dataFile root Seed P ]).CommitAsync()) |> ignore
    (wait (catalog.LoadTableAsync(tableId))).MetadataLocation

/// Writer w's commit: the producer for its operation, tagged with the writer.
let internal producer (cfg: Config) (root: string) (table: Table) (w: Writer) =
    let p =
        match cfg.Ops.[w] with
        | Append p -> SnapshotProducer.Append(table).AddFiles([ dataFile root (Data w) p ])
        | Overwrite p ->
            let name = partitionName p
            SnapshotProducer
                .Overwrite(table, (fun f -> string f.Partition.[0] = name))
                .AddFiles([ dataFile root (Data w) p ])
        | Delete p ->
            let name = partitionName p
            SnapshotProducer.DeleteFiles(table, (fun f -> string f.Partition.[0] = name))
    p.Set(writerKey, sprintf "%A" w).WithRetries(cfg.MaxAttempts - 1)

/// What a writer's commit looks like from outside: finished (and how), or blocked on attempt k.
let internal phaseOf (tasks: Dictionary<Writer, Task>) (blocked: seq<Writer * int>) (w: Writer) =
    match tasks.TryGetValue w with
    | false, _ -> "Idle"
    | true, t when t.IsCompletedSuccessfully -> "Succeeded"
    | true, t when t.IsFaulted ->
        match t.Exception.InnerException with
        | :? CommitStateUnknownException -> "Unknown"
        | :? CommitFailedException
        | :? ValidationException -> "Failed"
        | e -> sprintf "Crashed: %O" e
    | true, _ ->
        match blocked |> Seq.filter (fun (x, _) -> x = w) |> Seq.toList with
        | [ (_, k) ] -> sprintf "Waiting %d" k
        | other -> sprintf "Running (blocked on %A)" other

/// Reads the real system in the model's terms. locations maps each metadata file the model names to where it is.
let internal observe
    (cfg: Config)
    (catalog: ICatalog)
    (locations: Dictionary<File, string>)
    (phase: Writer -> string)
    : Observed =
    let table = wait (catalog.LoadTableAsync(tableId))
    let byLocation = locations |> Seq.map (fun kv -> kv.Value, kv.Key) |> dict
    let fileOf (location: string) =
        match byLocation.TryGetValue location with
        | true, f -> f
        | _ -> failwithf "The catalog points at %s, which no writer wrote" location
    let m = table.Metadata
    let history =
        m.Ancestors(m.CurrentSnapshotId)
        |> Seq.rev
        |> Seq.map (fun s ->
            match s.Summary.TryGetValue writerKey with
            | true, name -> cfg.Ops.Keys |> Seq.tryFind (fun w -> sprintf "%A" w = name)
            | _ -> None)
        |> Seq.toList
    let live =
        wait (TableScan.PlanFilesAsync(table, m.CurrentSnapshotId))
        |> Seq.map (fun t -> Path.GetFileName t.File.FilePath)
        |> Seq.map (fun name ->
            if name = fileName Seed then Seed
            else cfg.Ops.Keys |> Seq.find (fun w -> fileName (Data w) = name) |> Data)
        |> Set.ofSeq
    { Pointer = fileOf table.MetadataLocation
      MetadataFiles =
        locations
        |> Seq.filter (fun kv -> wait (catalog.FileIO.ExistsAsync kv.Value))
        |> Seq.map (fun kv -> kv.Key)
        |> Set.ofSeq
      Writers = cfg.Ops |> Map.map (fun w _ -> phase w)
      History = history
      LiveFiles = live }

type Driver(cfg: Config, root: string) =
    let catalog = new HookCatalog(root)
    let pending = Dictionary<Writer * int, Request>()
    let replies = Dictionary<Writer * int, exn option>()
    let blocked = Dictionary<Writer * int, TaskCompletionSource>()
    let attempts = Dictionary<Writer, int>()
    let arrived = Dictionary<Writer, TaskCompletionSource>()
    let tasks = Dictionary<Writer, Task>()
    let locations = Dictionary<File, string>()

    do
        catalog.OnSwap <-
            fun w request ->
                let k = attempts.GetValueOrDefault(w) + 1
                attempts.[w] <- k
                locations.[Meta(w, k)] <- request.NewLocation
                pending.[(w, k)] <- request
                let reply = tcs ()
                blocked.[(w, k)] <- reply
                arrived.[w].TrySetResult() |> ignore
                reply.Task
        locations.[Meta0] <- setUp catalog root

    /// Waits until writer w is blocked on a swap it had not made before, or its commit has finished.
    let settle (w: Writer) (signal: TaskCompletionSource) =
        waitUnit (Task.WhenAny(tasks.[w], signal.Task))

    let arm (w: Writer) =
        let signal = tcs ()
        arrived.[w] <- signal
        signal

    let respond (w: Writer) (k: int) (complete: TaskCompletionSource -> unit) =
        let reply = take blocked (w, k) "blocked swap"
        let signal = arm w
        complete reply
        settle w signal

    member _.Step(action: Action) =
        match action with
        | Begin w ->
            let signal = arm w
            let task =
                Task.Run(Func<Task>(fun () ->
                    HookCatalog.Current.Value <- Some w
                    let table = wait (catalog.LoadTableAsync(tableId))
                    (producer cfg root table w).CommitAsync() :> Task))
            tasks.[w] <- task
            settle w signal
        | Apply(w, k) ->
            let request = take pending (w, k) "pending swap"
            let result =
                try
                    waitUnit (catalog.Swap(tableId, request))
                    None
                with e ->
                    Some e
            // A writer that stopped waiting (it timed out) never hears the result.
            if blocked.ContainsKey((w, k)) then replies.[(w, k)] <- result
        | Ack(w, k) ->
            match take replies (w, k) "reply" with
            | None -> respond w k (fun r -> r.SetResult())
            | Some(e: exn) -> notPerformable "Ack(%A, %d), but the swap failed: %s" w k e.Message
        | Reject(w, k) ->
            match take replies (w, k) "reply" with
            | Some(e: exn) -> respond w k (fun r -> r.SetException(e))
            | None -> notPerformable "Reject(%A, %d), but the swap won" w k
        | ServerError(w, k) ->
            pending.Remove((w, k)) |> ignore
            replies.Remove((w, k)) |> ignore
            respond w k (fun r -> r.SetException(CommitStateUnknownException("injected server error", ServerResponded = true)))
        | Timeout(w, k) ->
            replies.Remove((w, k)) |> ignore
            respond w k (fun r -> r.SetException(TimeoutException "injected timeout"))
        // A resend with an idempotency key is invisible by design: the catalog joins it to the original request.
        // InMemoryCatalog has no keys; RestDriver checks the REST server's deduplication.
        | Resend _ -> ()

    member _.Observe() : Observed = observe cfg catalog locations (phaseOf tasks blocked.Keys)

    interface IDriver with
        member this.Step(action) = this.Step action
        member this.Observe() = this.Observe()
        member _.Expected(s) = expected cfg s

    interface IDisposable with
        member _.Dispose() =
            // Release any writer still waiting, so no task outlives the test.
            for reply in blocked.Values do
                reply.TrySetException(OperationCanceledException()) |> ignore
            (catalog :> IDisposable).Dispose()
