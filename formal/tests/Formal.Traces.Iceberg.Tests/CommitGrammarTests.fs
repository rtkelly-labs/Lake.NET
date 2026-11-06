module Formal.Traces.Iceberg.Tests.CommitGrammarTests

open Formal.Traces
open Formal.Traces.Iceberg
open Xunit
open Xunit.Abstractions

let private violation (events: CommitEvent list) =
    match CommitGrammar.parse events with
    | Ok _ -> failwithf "%A must not parse" events
    | Error e -> e, CommitGrammar.explain e

let private accepts (events: CommitEvent list) =
    match CommitGrammar.parse events with
    | Ok _ -> ()
    | Error e -> failwith (CommitGrammar.explain e)

type CommitGrammarTests(output: ITestOutputHelper) =

    [<Fact>]
    member _.``The commit grammar is right-linear and LL(1), so it is a DFA``() =
        Assert.True(Grammar.isRightLinear CommitGrammar.grammar)
        Assert.True(Grammar.ll1 CommitGrammar.grammar |> Result.isOk)
        output.WriteLine(Grammar.render CommitEvent.show CommitGrammar.grammar)

    [<Fact>]
    member _.``Bounded search finds no ambiguous commit trace``() =
        let report = Ambiguity.search 8 CommitGrammar.grammar
        Assert.Empty(report.Ambiguous)
        Assert.True(report.Sentences > 100, $"only %d{report.Sentences} sentences up to length 8")
        output.WriteLine $"%d{report.Sentences} sentences of length <= %d{report.MaxLength}; no sentence has two derivations"

    [<Fact>]
    member _.``Every event but a blind retry is in the grammar``() =
        let used = CommitGrammar.grammar.Terminals |> Set.ofSeq
        Assert.Equal<Set<CommitEvent>>(set CommitEvent.all - set [ RetryAfterUnknown ], used)

    [<Fact>]
    member _.``The protocol's paths parse``() =
        accepts [ Begin; SwapApplied; Committed ]
        accepts [ Begin; NothingToCommit ]
        accepts [ Begin; SwapRejected; Retry; Reload; SwapApplied; Committed ]
        accepts [ Begin; SwapRejected; GaveUp ]
        accepts [ Begin; SwapUnknown; Reload; Found; Committed ]
        accepts [ Begin; SwapUnknown; Reload; Absent; Retry; Reload; SwapApplied; Committed ]
        accepts [ Begin; SwapUnknown; ReloadFailed; LeftUnknown ]
        accepts [ Begin; SwapUnknown; LeftUnknown ]
        // Today's telemetry: failures unclassified, no resolution event.
        accepts [ Begin; SwapFailed; Retry; Reload; SwapApplied; Committed ]
        accepts [ Begin; SwapFailed; Reload; Committed ]
        accepts [ Begin; SwapFailed; Reload; Retry; Reload; SwapApplied; Committed ]
        // A base file vanished while planning: reload, then back off and re-plan.
        accepts [ Begin; Reload; Retry; Reload; SwapApplied; Committed ]

    [<Fact>]
    member _.``A blind retry after an unknown outcome is a violation in Uncertain``() =
        let e, text = violation [ Begin; SwapUnknown; Retry; Reload; SwapApplied; Committed ]
        output.WriteLine text
        Assert.Equal(2, e.Position)
        Assert.Equal("Uncertain", e.State)
        Assert.Equal(Lookahead.Event Retry, e.Got)
        Assert.Contains("commit protocol: protocol violation at event 3", text)
        Assert.Contains("trace:    Begin, SwapUnknown, Retry  <-- here", text)
        Assert.Contains("must reload and look for the snapshot before anything else", text)
        Assert.Contains("expected: Reload | ReloadFailed | LeftUnknown", text)

    [<Fact>]
    member _.``A retry recorded as caused by an unknown outcome is always a violation``() =
        let e, text = violation [ Begin; SwapFailed; RetryAfterUnknown; Reload; SwapApplied; Committed ]
        output.WriteLine text
        Assert.Equal("Unclassified", e.State)
        Assert.Equal(Lookahead.Event RetryAfterUnknown, e.Got)

    [<Fact>]
    member _.``Retrying a commit that was found applied is a double apply``() =
        let e, text = violation [ Begin; SwapUnknown; Reload; Found; Retry; Reload; SwapApplied; Committed ]
        output.WriteLine text
        Assert.Equal("Done", e.State)
        Assert.Equal<Lookahead<CommitEvent> list>([ Lookahead.Event Committed ], e.Expected)
        Assert.Contains("the catalog applied this commit; nothing may follow but its end", text)

    [<Fact>]
    member _.``Swapping again after an applied swap is a double apply``() =
        let e, text = violation [ Begin; SwapApplied; SwapApplied; Committed ]
        output.WriteLine text
        Assert.Equal(2, e.Position)
        Assert.Equal("Done", e.State)

    [<Fact>]
    member _.``A trace cut off before the commit ends is a violation at its end``() =
        let e, _ = violation [ Begin; SwapRejected; Retry ]
        Assert.Equal(Lookahead.End, e.Got)
        Assert.Equal("Backoff", e.State)
