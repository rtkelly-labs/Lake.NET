/// The translator on the toy model: what it emits, what it refuses, and TLC agreeing with the F# model.
module FsTla.Tests.TranslateTests

open System
open System.IO
open FsTla
open FsTla.Tests.Toy
open Xunit

let private init (_: Config) =
    { Counts = Map.ofList [ A, 0; B, 0 ]
      Last = None
      Seen = Set.empty }

let private model =
    Model.define "Toy" <@ bump @> init (fun _ -> Values.all<Bump> [ 1; 2 ])
    |> Model.invariant "WithinLimit" <@ withinLimit @>
    |> Model.invariant "LastWasSeen" <@ lastWasSeen @>

let private correct = { Limit = 3; Buggy = false }

[<Fact>]
let ``the generated module defines the step, the invariants and the next-state relation`` () =
    let generated = Model.toTla model correct
    for expected in [ "---- MODULE Toy ----"; "Inv_WithinLimit =="; "Inv_LastWasSeen =="; "Next ==" ] do
        Assert.Contains(expected, generated.Module)
    Assert.Contains("INVARIANT Inv_WithinLimit", generated.Config)

[<Fact>]
let ``a construct outside the subset is refused with the function it is in`` () =
    let broken = Model.define "Broken" <@ unsupported @> init (fun _ -> [])
    let error = Assert.ThrowsAny<exn>(fun () -> Model.toTla broken correct |> ignore)
    Assert.Contains("In FsTla.Tests.Toy.unsupported:", error.Message)
    Assert.Contains("models are pure expressions", error.Message)

[<Fact>]
let ``walks of the correct model keep every invariant`` () =
    for seed in 0..50 do
        let choices = List.init 12 (fun i -> (seed * 7 + i * 13) % 5)
        for _, state in Model.walk model correct choices do
            Assert.Empty(Model.violations model correct state)

let private tlcJar () =
    let jar = Environment.GetEnvironmentVariable "TLA2TOOLS_JAR"
    let present = not (String.IsNullOrEmpty jar) && File.Exists jar
    if not present && Environment.GetEnvironmentVariable "FORMAL_REQUIRE_TLC" = "1" then
        failwith "FORMAL_REQUIRE_TLC=1 but TLA2TOOLS_JAR does not name a jar"
    Skip.IfNot(present, "TLA2TOOLS_JAR is not set: TLC runs in CI")
    jar

let private timeout = TimeSpan.FromMinutes 2.

[<SkippableFact>]
let ``TLC agrees with the F# model on every reachable state`` () =
    let result = Model.check model correct (tlcJar ()) true timeout
    match result.Verdict with
    | Model.Holds _ -> ()
    | Model.Violated(name, _, steps) -> failwithf "TLC: %s violated by %A" name (List.map fst steps)
    let states = result.States |> Option.defaultWith (fun () -> failwith "TLC wrote no state dump")
    Assert.Empty(Model.differential model correct states true)

[<SkippableFact>]
let ``TLC finds the overshoot and its trace replays in F#`` () =
    let buggy = { correct with Buggy = true }
    match (Model.check model buggy (tlcJar ()) false timeout).Verdict with
    | Model.Holds _ -> failwith "TLC missed the overshoot"
    | Model.Violated(name, _, steps) ->
        Assert.Equal("WithinLimit", name)
        let replayed = Model.replay model buggy (List.map fst steps)
        Assert.Equal(snd (List.last steps), List.last replayed)
        Assert.Contains("WithinLimit", Model.violations model buggy (List.last replayed))
