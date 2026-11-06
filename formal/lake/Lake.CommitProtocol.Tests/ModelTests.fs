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

[<Fact>]
let ``every invariant holds along random walks of the correct protocol`` () =
    walkProperty standard (fun steps ->
        for i, (action, state) in List.indexed steps do
            match Model.violations model standard state with
            | [] -> ()
            | broken -> failwithf "%A breaks %A after step %d (%A):\n%A" broken (List.map fst steps) (i + 1) action state)

[<Fact>]
let ``every state reached encodes to TLA+ and decodes back to itself`` () =
    walkProperty standard (fun steps ->
        for _, state in steps do
            let text = Values.literalOf state
            let back = Values.decodeAs<State> (TlaParse.parse text)
            if back <> state then failwithf "Round trip changed the state.\nText: %s\nBefore: %A\nAfter: %A" text state back)

[<Fact>]
let ``retrying an unknown outcome blindly applies the commit twice`` () =
    let cfg = CommitModel.withBug RetryUnknownBlindly standard
    let states = Model.replay model cfg retryUnknownWitness
    Assert.Equal<string list>([ "AtMostOnce" ], Model.violations model cfg (List.last states))

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
