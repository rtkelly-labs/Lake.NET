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

    /// Span and event names this projection reads.
    [<RequireQualifiedAccess>]
    module Names =
        let commit = "iceberg.commit"
        let commitTable = "iceberg.catalog.commit_table"
        let loadTable = "iceberg.catalog.load_table"
        let retry = "iceberg.commit.retry"

        /// An event on the commit span when an unknown outcome is resolved, with <see cref="resolutionTag"/> set to
        /// <c>found</c> or <c>absent</c>.
        let resolution = "iceberg.commit.resolution"

        /// The tag on <see cref="resolution"/>.
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

    /// What the grammar cannot check, because it has no data: that each catalog commit is tagged with the attempt it
    /// belongs to (one more than the retries before it), that a found resolution names the snapshot the swap before it
    /// attempted, and that a committed snapshot is the one its last applied swap, or its found resolution, attempted.
    /// Checked only where the tags are present. Returns one message per mismatch.
    let snapshots (spans: SpanRecord seq) : string list =
        let spans = List.ofSeq spans
        let swaps =
            spans
            |> List.filter (fun s -> s.Name = Names.commitTable)
            |> List.groupBy _.ParentSpanId
            |> Map.ofList
        let snapshotOf (tags: Map<string, string>) = tags |> Map.tryFind IcebergTelemetry.Tags.SnapshotId
        [ for c in spans |> List.filter (fun s -> s.Name = Names.commit) do
              let mine = swaps |> Map.tryFind c.SpanId |> Option.defaultValue [] |> List.sortBy _.Start
              let retriesBefore (t: DateTime) = c.Events |> List.filter (fun e -> e.Name = Names.retry && e.Time <= t) |> List.length
              for s in mine do
                  match tag IcebergTelemetry.Tags.CommitAttempt s with
                  | Some attempt when attempt <> string (retriesBefore s.Start + 1) ->
                      yield $"commit span %s{c.SpanId}: catalog commit %s{s.SpanId} is tagged attempt %s{attempt} after %d{retriesBefore s.Start} retries"
                  | _ -> ()
              let found =
                  [ for e in c.Events do
                        if e.Name = Names.resolution && Map.tryFind Names.resolutionTag e.Tags = Some "found" then
                            match snapshotOf e.Tags with
                            | Some id -> yield e.Time, id, mine |> List.filter (fun s -> s.Start <= e.Time) |> List.tryLast
                            | None -> () ]
              for _, id, before in found do
                  match before |> Option.bind (fun s -> snapshotOf s.Tags) with
                  | Some attempted when attempted <> id ->
                      yield $"commit span %s{c.SpanId}: resolution found snapshot %s{id}, but the swap before it attempted %s{attempted}"
                  | _ -> ()
              // The snapshot that each applied swap and each found resolution settled on, in time order.
              let settled =
                  [ for s in mine do
                        match swap s, snapshotOf s.Tags with
                        | SwapApplied, Some id -> yield s.End, id
                        | _ -> ()
                    for t, id, _ in found -> t, id ]
              match finish c, snapshotOf c.Tags, settled |> List.sortBy fst |> List.tryLast with
              | Committed, Some committed, Some(_, last) when committed <> last ->
                  yield $"commit span %s{c.SpanId}: committed snapshot %s{committed}, but the last swap applied or found %s{last}"
              | _ -> () ]

    /// Project and check a recorded run.
    let check (spans: SpanRecord seq) : TraceReport<CommitKey, CommitEvent> * Projection =
        let projection = project spans
        Slicing.check CommitGrammar.table projection.Events, projection

    /// Every violation in a report, as text.
    let explain (report: TraceReport<CommitKey, CommitEvent>) =
        [ for key, e in report.Violations -> $"%O{key}\n%s{CommitGrammar.explain e}" ] |> String.concat "\n\n"
