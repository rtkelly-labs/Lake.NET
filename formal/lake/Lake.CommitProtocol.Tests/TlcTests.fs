/// TLC model-checks the generated module. Skipped without TLA2TOOLS_JAR, except in CI (FORMAL_REQUIRE_TLC=1).
module Lake.CommitProtocol.Tests.TlcTests

open System
open FsTla
open Lake.CommitProtocol
open Lake.CommitProtocol.Tests.Support
open Xunit
open Xunit.Abstractions

let private model = CommitModel.model
let private timeout = TimeSpan.FromMinutes 10.

type TlcTests(output: ITestOutputHelper) =

    let describe (steps: (Action * State) list) =
        steps |> List.mapi (fun i (a, _) -> sprintf "  %d. %A" (i + 1) a) |> String.concat "\n"

    [<SkippableTheory>]
    [<InlineData("standard")>]
    [<InlineData("removers")>]
    [<InlineData("cleanup")>]
    member _.``TLC finds no violation in the correct protocol, and agrees with the F# model on every state``(configName: string) =
        let jar = tlcJar ()
        let cfg = CommitModel.configs |> List.find (fst >> (=) configName) |> snd
        let result = Model.check model cfg jar true timeout
        output.WriteLine(sprintf "TLC (%s): %A in %O" configName result.Run.Stats result.Run.Elapsed)
        match result.Verdict with
        | Model.Holds _ -> ()
        | Model.Violated(name, _, steps) -> failwithf "TLC: %s is violated by\n%s" name (describe steps)
        let states = result.States |> Option.defaultWith (fun () -> failwith "TLC wrote no state dump")
        let problems = Model.differential model cfg states true
        Assert.True(problems.IsEmpty, String.Join("\n\n", problems))

    [<SkippableTheory>]
    [<InlineData("RetryUnknownBlindly")>]
    [<InlineData("DeleteOnUnknown")>]
    [<InlineData("SnapshotIsolation")>]
    [<InlineData("BlindSwap")>]
    [<InlineData("NoIdempotencyKey")>]
    [<InlineData("NoMissingDeleteCheck")>]
    [<InlineData("OrphanCleanupNoAge")>]
    member _.``TLC catches each seeded bug, with a trace that replays in F#``(bugName: string) =
        let jar = tlcJar ()
        let bug, _, _ = CommitModel.seeded |> List.find (fun (b, _, _) -> sprintf "%A" b = bugName)
        let cfg, expected = CommitModel.seededCase bug
        let result = Model.check model cfg jar false timeout
        match result.Verdict with
        | Model.Holds stats -> failwithf "TLC missed the seeded bug %A (%A)" bug stats
        | Model.Violated(name, _, steps) ->
            output.WriteLine(sprintf "%A breaks %s in %d steps (TLC %O):\n%s" bug name steps.Length result.Run.Elapsed (describe steps))
            Assert.Contains(name, expected)
            // The trace is a list of F# actions: replaying it natively reaches the same state and breaks the same invariant.
            let replayed = Model.replay model cfg (List.map fst steps)
            Assert.Equal(snd (List.last steps), List.last replayed)
            Assert.Contains(name, Model.violations model cfg (List.last replayed))
