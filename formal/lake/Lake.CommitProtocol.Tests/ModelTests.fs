/// Checks of the commit-protocol model that need no TLC: the generated module, the value encoding, and random walks.
module Lake.CommitProtocol.Tests.ModelTests

open FsCheck
open FsCheck.FSharp
open FsTla
open Lake.CommitProtocol
open Lake.CommitProtocol.Tests.Support
open Xunit

let private model = CommitModel.model
let private standard = CommitModel.standard

/// Retry after a server error without looking for the commit: the first attempt landed, and so does the second.
let retryUnknownWitness = [ Begin W1; Apply(W1, 1); ServerError(W1, 1); Apply(W1, 2) ]

[<Fact>]
let ``the generated TLA+ module is the committed one`` () =
    let generated = Model.toTla model standard
    golden "lake/tla/CommitProtocol.tla" generated.Module
    golden "lake/tla/CommitProtocol.cfg" generated.Config

let private walkProperty (cfg: Config) (check: (Action * State) list -> unit) =
    let property (choices: int list) = check (Model.walk model cfg choices)
    Check.One(Config.QuickThrowOnFailure.WithMaxTest(500).WithEndSize(60), property)

[<Theory>]
[<InlineData("standard")>]
[<InlineData("removers")>]
let ``every invariant holds along random walks of the correct protocol`` (configName: string) =
    let cfg = CommitModel.configs |> List.find (fst >> (=) configName) |> snd
    walkProperty cfg (fun steps ->
        for i, (action, state) in List.indexed steps do
            match Model.violations model cfg state with
            | [] -> ()
            | broken -> failwithf "%A breaks %A after step %d (%A):\n%A" broken (List.map fst steps) (i + 1) action state)

[<Theory>]
[<InlineData("standard")>]
[<InlineData("removers")>]
let ``every state reached encodes to TLA+ and decodes back to itself`` (configName: string) =
    let cfg = CommitModel.configs |> List.find (fst >> (=) configName) |> snd
    walkProperty cfg (fun steps ->
        for _, state in steps do
            let text = Values.literalOf state
            let back = Values.decodeAs<State> (TlaParse.parse text)
            if back <> state then failwithf "Round trip changed the state.\nText: %s\nBefore: %A\nAfter: %A" text state back)

[<Fact>]
let ``retrying an unknown outcome blindly applies the commit twice`` () =
    let cfg = CommitModel.withBug RetryUnknownBlindly standard
    let states = Model.replay model cfg retryUnknownWitness
    Assert.Equal<string list>([ "AtMostOnce" ], Model.violations model cfg (List.last states))

/// The trace TLC reports for the same bug (its shortest): an overwrite retried blindly fails validation against its own
/// landed commit, so the writer is told Failed while its snapshot is current.
[<Fact>]
let ``retrying an unknown overwrite blindly reports failure for a commit that landed`` () =
    let cfg = CommitModel.withBug RetryUnknownBlindly standard
    let states = Model.replay model cfg [ Begin W3; Apply(W3, 1); ServerError(W3, 1) ]
    Assert.Equal(Failed, (List.last states).Writers.[W3])
    Assert.Equal<string list>([ "FailedNeverApplies" ], Model.violations model cfg (List.last states))

/// A resend of a commit that already landed, as an idempotency key prevents.
let resendWitness = [ Begin W1; Apply(W1, 1); Resend(W1, 1); Apply(W1, 1); Reject(W1, 1) ]

[<Fact>]
let ``a resend without an idempotency key deletes the current metadata file`` () =
    let cfg = CommitModel.withBug NoIdempotencyKey standard
    let states = Model.replay model cfg resendWitness
    Assert.Equal<string list>([ "ReachableFilesExist" ], Model.violations model cfg (List.last states))

[<Fact>]
let ``with an idempotency key the resend joins the original and the writer hears it won`` () =
    let states = Model.replay model standard (List.take 3 resendWitness)
    Assert.Equal(None, model.Step standard (Apply(W1, 1)) (List.last states))
    let acked = Model.replay model standard (List.take 3 resendWitness @ [ Ack(W1, 1) ]) |> List.last
    Assert.Equal(Succeeded, acked.Writers.[W1])

[<Fact>]
let ``the correct protocol finds its commit after a server error instead of retrying`` () =
    let states = Model.replay model standard (List.take 3 retryUnknownWitness)
    Assert.Equal(Succeeded, (List.last states).Writers.[W1])
    Assert.Equal(None, model.Step standard (Apply(W1, 2)) (List.last states))

[<Fact>]
let ``a timed-out attempt is never retried`` () =
    let states = Model.replay model standard [ Begin W1; Timeout(W1, 1) ]
    Assert.Equal(Unknown, (List.last states).Writers.[W1])
    // The request is still in flight and may land later; nothing written for it was deleted.
    let landed = Model.replay model standard [ Begin W1; Timeout(W1, 1); Apply(W1, 1) ] |> List.last
    Assert.Equal(Meta(W1, 1), landed.Pointer)
    Assert.Empty(Model.violations model standard landed)

/// A delete lands first; the overwrite of the same partition loses its swap, and its retry finds a file it removes gone.
let concurrentRemoval = [ Begin W2; Begin W3; Apply(W2, 1); Apply(W3, 1); Ack(W2, 1); Reject(W3, 1) ]

[<Fact>]
let ``an overwrite retried after a concurrent delete of its files fails, and without the check commits over it`` () =
    let removers = CommitModel.removers
    Assert.Equal(Failed, (Model.replay model removers concurrentRemoval |> List.last).Writers.[W3])
    let cfg, _ = CommitModel.seededCase NoMissingDeleteCheck
    let landed = Model.replay model cfg (concurrentRemoval @ [ Apply(W3, 2) ]) |> List.last
    Assert.Equal<string list>([ "SerializableRemoval" ], Model.violations model cfg landed)
