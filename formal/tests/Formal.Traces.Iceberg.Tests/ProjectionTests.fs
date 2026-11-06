/// The span-to-event mapping on hand-built span records, one row of the README's mapping table per case.
module Formal.Traces.Iceberg.Tests.ProjectionTests

open System
open Formal.Traces
open Formal.Traces.Iceberg
open Iceberg.Net.Diagnostics
open Xunit

let private t0 = DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc)
let private at (ms: int) = t0.AddMilliseconds(float ms)

let private span name id parent start finish status (tags: (string * string) list) =
    { TraceId = "t"
      SpanId = id
      ParentSpanId = parent
      Name = name
      Start = at start
      End = at finish
      Status = status
      Tags = Map tags
      Events = [] }

let private commit outcome finish (events: SpanEvent list) =
    { span Spans.Names.commit "c" "" 0 finish "Unset" [ IcebergTelemetry.Tags.Table, "db.t"; IcebergTelemetry.Tags.Outcome, outcome ] with
        Events = events }

let private retryAt ms errorType =
    { Name = Spans.Names.retry; Time = at ms; Tags = Map [ IcebergTelemetry.Tags.ErrorType, errorType ] }

let private events spans =
    (Spans.project spans).Events |> List.map _.Event

[<Fact>]
let ``A conflict then a success projects in time order``() =
    let spans =
        [ commit "success" 100 [ retryAt 20 "Iceberg.Net.Metadata.CommitFailedException" ]
          span Spans.Names.commitTable "s1" "c" 5 10 "Error" [ IcebergTelemetry.Tags.Outcome, "conflict" ]
          span Spans.Names.loadTable "l1" "c" 30 35 "Ok" []
          span Spans.Names.commitTable "s2" "c" 40 50 "Ok" [] ]
    Assert.Equal<CommitEvent list>([ Begin; SwapRejected; Retry; Reload; SwapApplied; Committed ], events spans)

[<Fact>]
let ``Today's unclassified catalog failure projects to SwapFailed``() =
    let spans =
        [ commit "success" 100 []
          span Spans.Names.commitTable "s1" "c" 5 10 "Error" [ IcebergTelemetry.Tags.Outcome, "error" ]
          span Spans.Names.loadTable "l1" "c" 30 35 "Ok" [] ]
    Assert.Equal<CommitEvent list>([ Begin; SwapFailed; Reload; Committed ], events spans)

[<Fact>]
let ``Resolution events and unknown outcomes project``() =
    let resolution =
        { Name = Spans.Names.resolution; Time = at 35; Tags = Map [ Spans.Names.resolutionTag, "absent" ] }
    let spans =
        [ commit "error" 100 [ resolution; retryAt 40 "Iceberg.Net.Metadata.CommitFailedException" ]
          span Spans.Names.commitTable "s1" "c" 5 10 "Error" [ IcebergTelemetry.Tags.Outcome, "unknown" ]
          span Spans.Names.loadTable "l1" "c" 30 35 "Ok" []
          span Spans.Names.loadTable "l2" "c" 50 55 "Error" [ IcebergTelemetry.Tags.Outcome, "error" ] ]
    Assert.Equal<CommitEvent list>([ Begin; SwapUnknown; Reload; Absent; Retry; ReloadFailed; Aborted ], events spans)
    Assert.True(Spans.check spans |> fst |> _.Passed)

[<Fact>]
let ``A retry caused by an unknown outcome projects to a blind retry``() =
    let spans = [ commit "success" 100 [ retryAt 20 "Iceberg.Net.Metadata.CommitStateUnknownException" ] ]
    Assert.Equal<CommitEvent list>([ Begin; RetryAfterUnknown; Committed ], events spans)

[<Theory>]
[<InlineData("success", "Committed")>]
[<InlineData("noop", "NothingToCommit")>]
[<InlineData("conflict", "GaveUp")>]
[<InlineData("unknown", "LeftUnknown")>]
[<InlineData("cancelled", "Aborted")>]
[<InlineData("error", "Aborted")>]
let ``The commit span's outcome is the last event``(outcome: string, expected: string) =
    Assert.Equal(expected, events [ commit outcome 10 [] ] |> List.last |> CommitEvent.show)

[<Fact>]
let ``Catalog commits outside a commit span are counted, not checked``() =
    let p =
        Spans.project [ span Spans.Names.commitTable "s" "" 0 1 "Ok" []; span Spans.Names.commitTable "s2" "x" 0 1 "Ok" [] ]
    Assert.Empty(p.Events)
    Assert.Equal(2, p.OrphanCatalogCommits)

[<Fact>]
let ``Interleaved commits are sliced apart``() =
    let a = commit "success" 100 []
    let b = { commit "success" 90 [] with SpanId = "c2"; Start = at 1 }
    let spans =
        [ a; b
          span Spans.Names.commitTable "s1" "c" 5 10 "Ok" []
          span Spans.Names.commitTable "s2" "c2" 6 9 "Ok" [] ]
    let report, _ = Spans.check spans
    Assert.Equal(2, report.Slices)
    Assert.True(report.Passed, Spans.explain report)
