/// The commit protocol as an FsTla model, with the configurations CI checks.
module Lake.CommitProtocol.CommitModel

open FsTla

let writerOf (a: Action) =
    match a with
    | Begin w -> w
    | Apply(w, _)
    | Ack(w, _)
    | Reject(w, _)
    | ServerError(w, _)
    | Timeout(w, _) -> w

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
      Published = Set.empty }

/// Every action of the writers in the configuration, for attempts 1..MaxAttempts.
let actions (cfg: Config) : Action list =
    Values.all<Action> [ 1 .. cfg.MaxAttempts ] |> List.filter (fun a -> cfg.Ops.ContainsKey(writerOf a))

let model =
    Model.define "CommitProtocol" <@ Protocol.step @> init actions
    |> Model.invariant "AtMostOnce" <@ Protocol.atMostOnce @>
    |> Model.invariant "NoLostCommit" <@ Protocol.noLostCommit @>
    |> Model.invariant "FailedNeverApplies" <@ Protocol.failedNeverApplies @>
    |> Model.invariant "ReachableFilesExist" <@ Protocol.reachableFilesExist @>
    |> Model.invariant "SerializableOverwrite" <@ Protocol.serializableOverwrite @>

/// Two appends to different partitions and an overwrite of the first one, one retry each, one injected fault.
let standard =
    { Ops = Map.ofList [ W1, Append P; W2, Append Q; W3, Overwrite P ]
      MaxAttempts = 2
      Faults = 1
      Bugs = Set.empty }

let withBug (bug: Bug) (cfg: Config) = { cfg with Bugs = Set.add bug cfg.Bugs }

/// The invariant each seeded bug must break.
let seeded =
    [ RetryUnknownBlindly, "AtMostOnce"
      DeleteOnUnknown, "ReachableFilesExist"
      SnapshotIsolation, "SerializableOverwrite"
      BlindSwap, "NoLostCommit" ]
