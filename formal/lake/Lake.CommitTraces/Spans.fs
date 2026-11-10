namespace Lake.CommitTraces

open System
open System.Diagnostics
open Formal.Traces
open Iceberg.Net.Diagnostics

/// An event recorded on a span.
type SpanEvent =
    { Name: string
      Time: DateTime
      Tags: Map<string, string> }

/// A finished span, reduced to what the projection reads. Built from an in-process <see cref="Activity"/> here; an
/// OTLP reader would build the same record from exported spans.
type SpanRecord =
    { TraceId: string
      SpanId: string
      ParentSpanId: string
      Name: string
      Start: DateTime
      End: DateTime
      /// "Ok", "Error" or "Unset".
      Status: string
      Tags: Map<string, string>
      Events: SpanEvent list }

/// Which logical commit an event belongs to: the slice key.
type CommitKey =
    { TraceId: string
      SpanId: string
      Table: string }

    override k.ToString() = $"%s{k.Table} (commit span %s{k.SpanId})"

/// The projected events, plus what the projection saw and could not place.
type Projection =
    { Events: Keyed<CommitKey, CommitEvent> list
      /// Catalog commits outside any iceberg.commit span (ref and maintenance operations commit directly).
      OrphanCatalogCommits: int }

/// The projection from Iceberg.Net's spans onto <see cref="CommitEvent"/>: each <c>iceberg.commit</c> span is one
/// slice, and its events come from the span itself, its retry events and its direct catalog children, in time order.
[<RequireQualifiedAccess>]
module Spans =

    /// Span and event names this projection reads. The first four are emitted today.
    [<RequireQualifiedAccess>]
    module Names =
        let commit = "iceberg.commit"
        let commitTable = "iceberg.catalog.commit_table"
        let loadTable = "iceberg.catalog.load_table"
        let retry = "iceberg.commit.retry"

        /// Proposed: an event on the commit span when an unknown outcome is resolved, with
        /// <see cref="resolutionTag"/> set to <c>found</c> or <c>absent</c>.
        let resolution = "iceberg.commit.resolution"

        /// Proposed tag on <see cref="resolution"/>.
        let resolutionTag = "iceberg.commit.resolution"

    let private text (value: obj) =
        match value with
        | null -> ""
        | :? IFormattable as f -> f.ToString(null, Globalization.CultureInfo.InvariantCulture)
        | v -> string v

    let private tags (pairs: Collections.Generic.KeyValuePair<string, obj> seq) =
        pairs |> Seq.map (fun kv -> kv.Key, text kv.Value) |> Map.ofSeq

    /// Record a finished activity.
    let ofActivity (a: Activity) : SpanRecord =
        { TraceId = a.TraceId.ToHexString()
          SpanId = a.SpanId.ToHexString()
          ParentSpanId = (if a.ParentSpanId = Unchecked.defaultof<ActivitySpanId> then "" else a.ParentSpanId.ToHexString())
          Name = a.OperationName
          Start = a.StartTimeUtc
          End = a.StartTimeUtc + a.Duration
          Status = string a.Status
          Tags = tags a.TagObjects
          Events =
            [ for e in a.Events ->
                  { Name = e.Name
                    Time = e.Timestamp.UtcDateTime
                    Tags = tags e.Tags } ] }

    let private tag key (span: SpanRecord) = span.Tags |> Map.tryFind key

    /// A catalog span's outcome. Catalog spans tag every outcome; spans from releases that tagged only failures have
    /// status Ok and no tag on success.
    let catalogOutcome (span: SpanRecord) =
        match tag IcebergTelemetry.Tags.Outcome span with
        | Some outcome -> outcome
        | None when span.Status = "Ok" -> IcebergTelemetry.Outcomes.Success
        | None -> IcebergTelemetry.Outcomes.Error

    /// <c>iceberg.catalog.commit_table</c> by outcome. A failure the span does not classify ("error", as every failure
    /// was before the catalogs called FailedWith) projects to <see cref="SwapFailed"/>.
    let swap (span: SpanRecord) =
        match catalogOutcome span with
        | o when o = IcebergTelemetry.Outcomes.Success -> SwapApplied
        | o when o = IcebergTelemetry.Outcomes.Conflict -> SwapRejected
        | o when o = IcebergTelemetry.Outcomes.Unknown -> SwapUnknown
        | _ -> SwapFailed

    /// <c>iceberg.catalog.load_table</c> by outcome.
    let reload (span: SpanRecord) =
        if catalogOutcome span = IcebergTelemetry.Outcomes.Success then Reload else ReloadFailed

    /// An <c>iceberg.commit.retry</c> event: a retry whose cause was an unresolved unknown outcome is blind.
    let retry (e: SpanEvent) =
        match e.Tags |> Map.tryFind IcebergTelemetry.Tags.ErrorType with
        | Some t when t.EndsWith "CommitStateUnknownException" -> RetryAfterUnknown
        | _ -> Retry

    /// The end of an <c>iceberg.commit</c> span, from <c>iceberg.outcome</c>.
    let finish (span: SpanRecord) =
        match tag IcebergTelemetry.Tags.Outcome span with
        | Some o when o = IcebergTelemetry.Outcomes.Success -> Committed
        | Some o when o = IcebergTelemetry.Outcomes.NoOp -> NothingToCommit
        | Some o when o = IcebergTelemetry.Outcomes.Conflict -> GaveUp
        | Some o when o = IcebergTelemetry.Outcomes.Unknown -> LeftUnknown
        | _ -> Aborted

    /// Project finished spans onto commit events, one slice per iceberg.commit span, ordered by commit start.
    let project (spans: SpanRecord seq) : Projection =
        let spans = List.ofSeq spans
        let commits = spans |> List.filter (fun s -> s.Name = Names.commit) |> List.sortBy _.Start
        let commitIds = commits |> List.map _.SpanId |> Set.ofList
        let children =
            spans
            |> List.filter (fun s -> s.Name = Names.commitTable || s.Name = Names.loadTable)
            |> List.groupBy _.ParentSpanId
            |> Map.ofList
        let orphans =
            spans
            |> List.filter (fun s -> s.Name = Names.commitTable && not (commitIds.Contains s.ParentSpanId))
            |> List.length
        let events =
            [ for c in commits do
                  let key =
                      { TraceId = c.TraceId
                        SpanId = c.SpanId
                        Table = tag IcebergTelemetry.Tags.Table c |> Option.defaultValue "" }
                  // (time, rank): at equal times the start comes first and the end last.
                  let timed =
                      [ yield (c.Start, 0), Begin
                        for s in children |> Map.tryFind c.SpanId |> Option.defaultValue [] do
                            yield (s.Start, 1), (if s.Name = Names.commitTable then swap s else reload s)
                        for e in c.Events do
                            if e.Name = Names.retry then
                                yield (e.Time, 1), retry e
                            elif e.Name = Names.resolution then
                                match e.Tags |> Map.tryFind Names.resolutionTag with
                                | Some "found" -> yield (e.Time, 1), Found
                                | Some "absent" -> yield (e.Time, 1), Absent
                                | _ -> ()
                        yield (c.End, 2), finish c ]
                  for _, event in List.sortBy fst timed -> { Key = key; Event = event } ]
        { Events = events; OrphanCatalogCommits = orphans }

    /// Project and check a recorded run.
    let check (spans: SpanRecord seq) : TraceReport<CommitKey, CommitEvent> * Projection =
        let projection = project spans
        Slicing.check CommitGrammar.table projection.Events, projection

    /// Every violation in a report, as text.
    let explain (report: TraceReport<CommitKey, CommitEvent>) =
        [ for key, e in report.Violations -> $"%O{key}\n%s{CommitGrammar.explain e}" ] |> String.concat "\n\n"
