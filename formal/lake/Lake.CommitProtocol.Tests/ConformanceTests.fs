/// The model as an oracle for the real commit path: every run of model actions must leave the real system in the state
/// the model predicts, step by step. Each test runs against InMemoryCatalog ("memory") and against RestCatalog with the
/// REST server over its in-memory and SQLite stores ("rest-memory", "rest-sqlite").
module Lake.CommitProtocol.Tests.ConformanceTests

open System
open System.IO
open FsCheck
open FsCheck.FSharp
open FsTla
open Lake.CommitProtocol
open Lake.CommitProtocol.Tests.Conformance
open Lake.CommitProtocol.Tests.RestConformance
open Xunit

let private model = CommitModel.model
let private standard = CommitModel.standard

let private drivers = [ "memory"; "rest-memory"; "rest-sqlite" ]

let private start (driver: string) (cfg: Config) (root: string) : IDriver =
    match driver with
    | "memory" -> new Driver(cfg, root)
    | "rest-memory" -> new RestDriver(cfg, root, "memory")
    | "rest-sqlite" -> new RestDriver(cfg, root, "sqlite")
    | other -> invalidArg "driver" other

/// Runs the actions against a fresh catalog, comparing it with the model after each one.
let private conform (driverName: string) (cfg: Config) (steps: (Action * State) list) =
    let root = Path.Combine(Path.GetTempPath(), "formal-conformance", Guid.NewGuid().ToString("N"))
    try
        use driver = start driverName cfg root
        let check (i: int) (action: Action option) (state: State) =
            let want = driver.Expected state
            let got = driver.Observe()
            if got <> want then
                failwithf
                    "The real catalog differs from the model after step %d (%A).\nActions: %A\nModel: %A\nReal:  %A"
                    i action (List.map fst steps) want got
        check 0 None (CommitModel.init cfg)
        for i, (action, state) in List.indexed steps do
            try
                driver.Step action
            with NotPerformable why ->
                failwithf
                    "The real catalog differs from the model at step %d (%A): %s.\nActions: %A"
                    (i + 1) action why (List.map fst steps)
            check (i + 1) (Some action) state
    finally
        try Directory.Delete(root, true) with _ -> ()

let configsAndDrivers =
    let data = TheoryData<string, string>()
    for name, _ in CommitModel.configs do
        for d in drivers do
            data.Add(name, d)
    data

/// Each REST run hosts its own server, so it gets fewer runs than InMemoryCatalog.
[<Theory>]
[<MemberData(nameof configsAndDrivers)>]
let ``the real catalog follows the model along random runs`` (configName: string, driver: string) =
    let cfg = CommitModel.configs |> List.find (fst >> (=) configName) |> snd
    let property (choices: int list) = conform driver cfg (Model.walk model cfg choices)
    let runs = if driver = "memory" then 100 else 30
    Check.One(Config.QuickThrowOnFailure.WithMaxTest(runs).WithEndSize(40), property)

let private scenarioIn (driver: string) (cfg: Config) (actions: Action list) =
    conform driver cfg (List.zip actions (Model.replay model cfg actions |> List.tail))

[<Theory>]
[<InlineData("memory")>]
[<InlineData("rest-memory")>]
[<InlineData("rest-sqlite")>]
let ``after a server error the real writer finds its commit instead of retrying`` (driver: string) =
    scenarioIn driver standard (List.take 3 ModelTests.retryUnknownWitness)

[<Theory>]
[<InlineData("memory")>]
[<InlineData("rest-memory")>]
[<InlineData("rest-sqlite")>]
let ``a timed-out attempt is reported unknown and may still land`` (driver: string) =
    scenarioIn driver standard [ Begin W1; Timeout(W1, 1); Apply(W1, 1) ]

[<Theory>]
[<InlineData("memory")>]
[<InlineData("rest-memory")>]
[<InlineData("rest-sqlite")>]
let ``a lost swap is retried and a conflicting overwrite fails validation`` (driver: string) =
    scenarioIn
        driver
        standard
        [ Begin W1
          Begin W3
          Apply(W1, 1)
          Apply(W3, 1)
          Ack(W1, 1)
          Reject(W3, 1) ]

[<Theory>]
[<InlineData("memory")>]
[<InlineData("rest-memory")>]
[<InlineData("rest-sqlite")>]
let ``an overwrite retried after a concurrent delete of its files fails validation`` (driver: string) =
    scenarioIn driver CommitModel.removers ModelTests.concurrentRemoval

/// Resends with the request's Idempotency-Key, before the server has seen it and after: the server applies it once
/// and replays its response.
[<Theory>]
[<InlineData("rest-memory")>]
[<InlineData("rest-sqlite")>]
let ``a keyed resend is applied once`` (driver: string) =
    scenarioIn driver standard (List.take 3 ModelTests.resendWitness @ [ Ack(W1, 1) ])
    scenarioIn driver standard [ Begin W1; Resend(W1, 1); Apply(W1, 1); Ack(W1, 1) ]
    scenarioIn driver standard [ Begin W1; Begin W2; Apply(W2, 1); Resend(W1, 1); Apply(W1, 1); Reject(W1, 1); Apply(W1, 2); Ack(W1, 2) ]

/// The witness each seeded bug departs from the real system on.
let private witness (bug: Bug) =
    match bug with
    | RetryUnknownBlindly -> List.take 3 ModelTests.retryUnknownWitness
    | DeleteOnUnknown -> [ Begin W1; Apply(W1, 1); ServerError(W1, 1) ]
    | NoMissingDeleteCheck -> ModelTests.concurrentRemoval
    | NoIdempotencyKey -> ModelTests.resendWitness
    | _ -> [ Begin W1; Begin W3; Apply(W1, 1); Apply(W3, 1); Ack(W1, 1); Reject(W3, 1) ]

/// The check has teeth: the real catalog does not follow a model with a seeded bug. NoIdempotencyKey only against REST:
/// InMemoryCatalog has no keys, so a resend is not observable there.
[<Theory>]
[<InlineData("RetryUnknownBlindly", "memory")>]
[<InlineData("DeleteOnUnknown", "memory")>]
[<InlineData("SnapshotIsolation", "memory")>]
[<InlineData("NoMissingDeleteCheck", "memory")>]
[<InlineData("RetryUnknownBlindly", "rest-memory")>]
[<InlineData("DeleteOnUnknown", "rest-memory")>]
[<InlineData("SnapshotIsolation", "rest-memory")>]
[<InlineData("NoMissingDeleteCheck", "rest-memory")>]
[<InlineData("NoIdempotencyKey", "rest-memory")>]
[<InlineData("NoIdempotencyKey", "rest-sqlite")>]
let ``the real catalog departs from each buggy model`` (bugName: string, driver: string) =
    let bug, _, _ = CommitModel.seeded |> List.find (fun (b, _, _) -> sprintf "%A" b = bugName)
    let cfg, _ = CommitModel.seededCase bug
    let actions = witness bug
    let steps = List.zip actions (Model.replay model cfg actions |> List.tail)
    let error = Assert.ThrowsAny<exn>(fun () -> conform driver cfg steps)
    Assert.Contains("differs from the model", error.Message)
