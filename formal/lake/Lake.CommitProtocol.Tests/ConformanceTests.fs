/// The model as an oracle for the real InMemoryCatalog and SnapshotProducer: every run of model actions must leave the
/// real system in the state the model predicts, step by step.
module Lake.CommitProtocol.Tests.ConformanceTests

open System
open System.IO
open FsCheck
open FsCheck.FSharp
open FsTla
open Lake.CommitProtocol
open Lake.CommitProtocol.Tests.Conformance
open Xunit

let private model = CommitModel.model
let private standard = CommitModel.standard

/// Runs the actions against a fresh catalog, comparing it with the model after each one.
let private conform (cfg: Config) (steps: (Action * State) list) =
    let root = Path.Combine(Path.GetTempPath(), "formal-conformance", Guid.NewGuid().ToString("N"))
    try
        use driver = new Driver(cfg, root)
        let check (i: int) (action: Action option) (state: State) =
            let want = expected cfg state
            let got = driver.Observe()
            if got <> want then
                failwithf
                    "The real catalog differs from the model after step %d (%A).\nActions: %A\nModel: %A\nReal:  %A"
                    i action (List.map fst steps) want got
        check 0 None (CommitModel.init cfg)
        for i, (action, state) in List.indexed steps do
            driver.Step action
            check (i + 1) (Some action) state
    finally
        try Directory.Delete(root, true) with _ -> ()

[<Theory>]
[<InlineData("standard")>]
[<InlineData("removers")>]
let ``the real catalog follows the model along random runs`` (configName: string) =
    let cfg = CommitModel.configs |> List.find (fst >> (=) configName) |> snd
    let property (choices: int list) = conform cfg (Model.walk model cfg choices)
    Check.One(Config.QuickThrowOnFailure.WithMaxTest(100).WithEndSize(40), property)

let private scenarioIn (cfg: Config) (actions: Action list) =
    conform cfg (List.zip actions (Model.replay model cfg actions |> List.tail))

let private scenario = scenarioIn standard

[<Fact>]
let ``after a server error the real writer finds its commit instead of retrying`` () =
    scenario (List.take 3 ModelTests.retryUnknownWitness)

[<Fact>]
let ``a timed-out attempt is reported unknown and may still land`` () =
    scenario [ Begin W1; Timeout(W1, 1); Apply(W1, 1) ]

[<Fact>]
let ``a lost swap is retried and a conflicting overwrite fails validation`` () =
    scenario
        [ Begin W1
          Begin W3
          Apply(W1, 1)
          Apply(W3, 1)
          Ack(W1, 1)
          Reject(W3, 1) ]

[<Fact>]
let ``an overwrite retried after a concurrent delete of its files fails validation`` () =
    scenarioIn CommitModel.removers ModelTests.concurrentRemoval

/// The check has teeth: the real catalog does not follow a model with a seeded bug.
[<Theory>]
[<InlineData("RetryUnknownBlindly")>]
[<InlineData("DeleteOnUnknown")>]
[<InlineData("SnapshotIsolation")>]
[<InlineData("NoMissingDeleteCheck")>]
let ``the real catalog departs from each buggy model`` (bugName: string) =
    let bug, _, _ = CommitModel.seeded |> List.find (fun (b, _, _) -> sprintf "%A" b = bugName)
    let cfg, _ = CommitModel.seededCase bug
    let actions =
        match bug with
        | RetryUnknownBlindly -> List.take 3 ModelTests.retryUnknownWitness
        | DeleteOnUnknown -> [ Begin W1; Timeout(W1, 1) ]
        | NoMissingDeleteCheck -> ModelTests.concurrentRemoval
        | _ -> [ Begin W1; Begin W3; Apply(W1, 1); Apply(W3, 1); Ack(W1, 1); Reject(W3, 1) ]
    let steps = List.zip actions (Model.replay model cfg actions |> List.tail)
    let error = Assert.ThrowsAny<exn>(fun () -> conform cfg steps)
    Assert.Contains("differs from the model", error.Message)
