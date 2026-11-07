/// The commit protocol as an FsTla model, with the configurations CI checks.
module Lake.CommitProtocol.CommitModel

open FsTla

/// The writer an action belongs to (None: orphan cleanup).
let writerOf (a: Action) =
    match a with
    | Begin w -> Some w
    | Apply(w, _)
    | Ack(w, _)
    | Reject(w, _)
    | ServerError(w, _)
    | Timeout(w, _)
    | Resend(w, _) -> Some w
    | CleanOrphans -> None

let seedSnapshot =
    { SeqNo = 1
      Writer = None
      Attempt = 0
      Adds = Set.singleton Seed
      Removes = Set.empty
      Start = 0 }

let init (cfg: Config) : State =
    { Pointer = Meta0
      Metadata = Map.ofList [ Meta0, Set.singleton seedSnapshot ]
      Store = set [ Meta0; Seed ]
      InFlight = Map.empty
      Replies = Map.empty
      Writers = cfg.Ops |> Map.map (fun _ _ -> Idle)
      Faults = cfg.Faults
      Sent = Map.empty
      Published = Set.empty }

/// Every action of the writers in the configuration, for attempts 1..MaxAttempts, and orphan cleanup if it runs.
let actions (cfg: Config) : Action list =
    Values.all<Action> [ 1 .. cfg.MaxAttempts ]
    |> List.filter (fun a ->
        match writerOf a with
        | Some w -> cfg.Ops.ContainsKey w
        | None -> cfg.Cleaner)

let model =
    Model.define "CommitProtocol" <@ Protocol.step @> init actions
    |> Model.invariant "AtMostOnce" <@ Protocol.atMostOnce @>
    |> Model.invariant "NoLostCommit" <@ Protocol.noLostCommit @>
    |> Model.invariant "FailedNeverApplies" <@ Protocol.failedNeverApplies @>
    |> Model.invariant "ReachableFilesExist" <@ Protocol.reachableFilesExist @>
    |> Model.invariant "SerializableRemoval" <@ Protocol.serializableRemoval @>

/// Two appends to different partitions and an overwrite of the first one, one retry each, one injected fault.
let standard =
    { Ops = Map.ofList [ W1, Append P; W2, Append Q; W3, Overwrite P ]
      MaxAttempts = 2
      Faults = 1
      Cleaner = false
      Bugs = Set.empty }

/// Three removers of one partition: an append, a delete and an overwrite of P. This is where the concurrent-removal
/// check (failMissingDeletePaths) matters: a delete or overwrite retried after another removed its files.
let removers =
    { standard with Ops = Map.ofList [ W1, Append P; W2, Delete P; W3, Overwrite P ] }

/// The standard writers with orphan cleanup running between any two steps: cleanup racing in-flight commits.
let cleanup = { standard with Cleaner = true }

/// The configurations CI checks the correct protocol in.
let configs = [ "standard", standard; "removers", removers; "cleanup", cleanup ]

let withBug (bug: Bug) (cfg: Config) = { cfg with Bugs = Set.add bug cfg.Bugs }

/// Each seeded bug, the configuration it is checked in, and the invariants it may break; TLC reports whichever its
/// breadth-first search reaches first (the shortest trace). A blind retry after an unknown outcome either applies the
/// commit twice (an append) or, for an overwrite, fails validation against its own landed commit, so the writer is told
/// Failed although its snapshot is in the table.
let seeded =
    [ RetryUnknownBlindly, standard, [ "AtMostOnce"; "FailedNeverApplies" ]
      DeleteOnUnknown, standard, [ "ReachableFilesExist" ]
      SnapshotIsolation, standard, [ "SerializableRemoval" ]
      BlindSwap, standard, [ "NoLostCommit" ]
      // A keyless resend of a commit that landed loses its CAS: the writer deletes the current metadata file, or
      // retries and applies its commit a second time.
      NoIdempotencyKey, standard, [ "ReachableFilesExist"; "AtMostOnce" ]
      // A delete or overwrite retried after the other removed its files commits over that removal.
      NoMissingDeleteCheck, removers, [ "SerializableRemoval" ]
      // Cleanup deletes a commit's new files before its request is applied, and the commit then lands on them.
      OrphanCleanupNoAge, cleanup, [ "ReachableFilesExist" ] ]

/// A seeded bug's configuration, with the bug, and the invariants it may break.
let seededCase (bug: Bug) =
    let _, cfg, expected = seeded |> List.find (fun (b, _, _) -> b = bug)
    withBug bug cfg, expected
