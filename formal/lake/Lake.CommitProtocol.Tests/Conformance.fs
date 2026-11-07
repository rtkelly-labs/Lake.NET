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

let private writerKey = "formal.writer"

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

let private tcs () = TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

let private partitionName (p: Part) =
    match p with
    | P -> "P"
    | Q -> "Q"

let private fileName (f: File) =
    match f with
    | Seed -> "seed.parquet"
    | Data w -> sprintf "%A.parquet" w
    | _ -> invalidArg "f" "not a data file"

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

type Driver(cfg: Config, root: string) =
    let catalog = new HookCatalog(root)
    let id = TableIdentifier.Of("formal", "t")
    let timeout = TimeSpan.FromSeconds 30.
    let pending = Dictionary<Writer * int, Request>()
    let replies = Dictionary<Writer * int, exn option>()
    let blocked = Dictionary<Writer * int, TaskCompletionSource>()
    let attempts = Dictionary<Writer, int>()
    let arrived = Dictionary<Writer, TaskCompletionSource>()
    let tasks = Dictionary<Writer, Task>()
    let locations = Dictionary<File, string>()

    let run (t: Task<'T>) = t.WaitAsync(timeout).GetAwaiter().GetResult()
    let runUnit (t: Task) = t.WaitAsync(timeout).GetAwaiter().GetResult()

    let dataFile (f: File) (part: Part) =
        DataFile(
            FilePath = Path.Combine(root, "data", fileName f),
            Partition = [| box (partitionName part) |],
            RecordCount = 1L,
            FileSizeInBytes = 1L
        )

    let load () = run (catalog.LoadTableAsync(id))

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
        runUnit (catalog.CreateNamespaceAsync(Namespace.Of("formal")))
        let schema = Schema(NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "part", PrimitiveType.String))
        let spec = PartitionSpec.BuilderFor(schema).Identity("part").Build()
        let properties = readOnlyDict [ "commit.retry.min-wait-ms", "0"; "commit.retry.max-wait-ms", "0" ]
        let table =
            run (catalog.CreateTableAsync(CreateTableRequest(id, schema, spec, Properties = properties)))
        run (SnapshotProducer.Append(table).AddFiles([ dataFile Seed P ]).CommitAsync()) |> ignore
        locations.[Meta0] <- (load ()).MetadataLocation

    /// Waits until writer w is blocked on a swap it had not made before, or its commit has finished.
    let settle (w: Writer) (signal: TaskCompletionSource) =
        runUnit (Task.WhenAny(tasks.[w], signal.Task))

    let arm (w: Writer) =
        let signal = tcs ()
        arrived.[w] <- signal
        signal

    let take (d: Dictionary<'K, 'V>) (key: 'K) (what: string) : 'V =
        match d.TryGetValue key with
        | true, v ->
            d.Remove key |> ignore
            v
        | _ -> failwithf "The real system has no %s for %A" what key

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
                    let table = load ()
                    let producer =
                        match cfg.Ops.[w] with
                        | Append p -> SnapshotProducer.Append(table).AddFiles([ dataFile (Data w) p ])
                        | Overwrite p ->
                            let name = partitionName p
                            SnapshotProducer
                                .Overwrite(table, (fun f -> string f.Partition.[0] = name))
                                .AddFiles([ dataFile (Data w) p ])
                        | Delete p ->
                            let name = partitionName p
                            SnapshotProducer.DeleteFiles(table, (fun f -> string f.Partition.[0] = name))
                    producer.Set(writerKey, sprintf "%A" w).WithRetries(cfg.MaxAttempts - 1).CommitAsync() :> Task))
            tasks.[w] <- task
            settle w signal
        | Apply(w, k) ->
            let request = take pending (w, k) "pending swap"
            let result =
                try
                    runUnit (catalog.Swap(id, request))
                    None
                with e ->
                    Some e
            // A writer that stopped waiting (it timed out) never hears the result.
            if blocked.ContainsKey((w, k)) then replies.[(w, k)] <- result
        | Ack(w, k) ->
            match take replies (w, k) "reply" with
            | None -> respond w k (fun r -> r.SetResult())
            | Some(e: exn) -> failwithf "Ack(%A, %d), but the swap failed: %s" w k e.Message
        | Reject(w, k) ->
            match take replies (w, k) "reply" with
            | Some(e: exn) -> respond w k (fun r -> r.SetException(e))
            | None -> failwithf "Reject(%A, %d), but the swap won" w k
        | ServerError(w, k) ->
            pending.Remove((w, k)) |> ignore
            replies.Remove((w, k)) |> ignore
            respond w k (fun r -> r.SetException(CommitStateUnknownException("injected server error", ServerResponded = true)))
        | Timeout(w, k) ->
            replies.Remove((w, k)) |> ignore
            respond w k (fun r -> r.SetException(TimeoutException "injected timeout"))
        // A resend with an idempotency key is invisible by design: the catalog joins it to the original request.
        // InMemoryCatalog has no keys; the REST server's deduplication is what this action stands for.
        | Resend _ -> ()

    member _.Observe() : Observed =
        let table = load ()
        let fileOf =
            let byLocation = locations |> Seq.map (fun kv -> kv.Value, kv.Key) |> dict
            fun (location: string) ->
                match byLocation.TryGetValue location with
                | true, f -> f
                | _ -> failwithf "The catalog points at %s, which no writer wrote" location
        let phase (w: Writer) =
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
                match blocked.Keys |> Seq.filter (fun (x, _) -> x = w) |> Seq.toList with
                | [ (_, k) ] -> sprintf "Waiting %d" k
                | other -> sprintf "Running (blocked on %A)" other
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
            run (TableScan.PlanFilesAsync(table, m.CurrentSnapshotId))
            |> Seq.map (fun t -> Path.GetFileName t.File.FilePath)
            |> Seq.map (fun name ->
                if name = fileName Seed then Seed
                else cfg.Ops.Keys |> Seq.find (fun w -> fileName (Data w) = name) |> Data)
            |> Set.ofSeq
        { Pointer = fileOf table.MetadataLocation
          MetadataFiles =
            locations
            |> Seq.filter (fun kv -> run (catalog.FileIO.ExistsAsync kv.Value))
            |> Seq.map (fun kv -> kv.Key)
            |> Set.ofSeq
          Writers = cfg.Ops |> Map.map (fun w _ -> phase w)
          History = history
          LiveFiles = live }

    interface IDisposable with
        member _.Dispose() =
            // Release any writer still waiting, so no task outlives the test.
            for reply in blocked.Values do
                reply.TrySetException(OperationCanceledException()) |> ignore
            (catalog :> IDisposable).Dispose()
