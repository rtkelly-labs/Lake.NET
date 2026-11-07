/// The trace check against the model itself. The model's conformance driver runs real SnapshotProducer commits on
/// InMemoryCatalog step by step under random walks of the model; the spans those commits emit must parse, and read
/// back as model actions they must be exactly the visible actions the driver took. This validates the span-to-event
/// projection and the event-to-action mapping against the model, rather than against hand-written expectations.
module Lake.CommitTraces.Tests.ConformanceTraceTests

open System
open System.IO
open Lake.CommitProtocol
open Lake.CommitProtocol.Tests.Conformance
open Lake.CommitTraces
open Lake.CommitTraces.Tests.Harness
open FsTla
open Xunit
open Xunit.Abstractions

/// Writers whose commit has finished, by the driver's own reading of the real system.
let private finished (o: Observed) =
    o.Writers
    |> Map.filter (fun _ phase -> phase = "Succeeded" || phase = "Failed" || phase = "Unknown")
    |> Map.keys
    |> Set.ofSeq

/// The visible actions of one run, as the driver took them and as the spans tell them, per finished writer.
type private Run =
    { Taken: Map<Writer, Action list>
      Read: Map<Writer, Result<Action list, string>>
      Report: string option }

/// Runs the actions through the driver while capturing spans. Every driver instance uses table formal.t, so the
/// tests in this module must not run in parallel with one another (one class, see below).
let private run (cfg: Config) (actions: Action list) : Run =
    let root = Path.Combine(Path.GetTempPath(), "formal-traces-conformance", Guid.NewGuid().ToString "N")
    try
        use driver = new Driver(cfg, root)
        // Begin w settles only once w's commit has reached its first swap, so its commit span starts inside that step:
        // the step's time window attributes the span to the writer.
        let windows = ResizeArray<Writer * DateTime * DateTime>()
        let capture = new Capture("formal")
        let observed =
            // Disposed before the driver: the driver's Dispose cancels blocked writers, which would end their commit
            // spans as cancelled, slices for commits the walk never finished.
            using capture (fun _ ->
                for a in actions do
                    let t0 = DateTime.UtcNow
                    driver.Step a
                    match a with
                    | Action.Begin w -> windows.Add((w, t0, DateTime.UtcNow))
                    | _ -> ()
                driver.Observe())
        let spans = capture.Spans |> classify capture.Failures
        let report, projection = Spans.check spans
        let writerOf (start: DateTime) =
            windows |> Seq.tryFind (fun (_, t0, t1) -> t0 <= start && start <= t1) |> Option.map (fun (w, _, _) -> w)
        let commits =
            spans
            |> List.filter (fun s -> s.Name = Spans.Names.commit)
            |> List.map (fun s ->
                match writerOf s.Start with
                | Some w -> w, s.SpanId
                | None -> failwithf "commit span %s started outside every Begin step" s.SpanId)
        let done' = finished observed
        Assert.Equal<Set<Writer>>(done', commits |> List.map fst |> Set.ofList)
        { Taken = done' |> Seq.map (fun w -> w, Actions.visible w actions) |> Map.ofSeq
          Read =
            commits
            |> List.map (fun (w, id) ->
                let events = projection.Events |> List.filter (fun e -> e.Key.SpanId = id) |> List.map _.Event
                w, Actions.ofSlice w events)
            |> Map.ofList
          Report = if report.Passed then None else Some(Spans.explain report) }
    finally
        try
            Directory.Delete(root, true)
        with _ ->
            ()

/// One class, so xunit runs these one at a time: each driver's table is formal.t, and the span capture is process-wide.
type ConformanceTraceTests(output: ITestOutputHelper) =

    // Orphan cleanup is not driven by the conformance driver, so the cleanup configuration is left out.
    let configs = CommitModel.configs |> List.filter (fun (_, c) -> not c.Cleaner)

    [<Fact>]
    member _.``Along random walks of the model, the spans read back as the actions the driver took``() =
        let seen = Collections.Generic.HashSet<string>()
        for name, cfg in configs do
            for seed in 1..40 do
                let rnd = Random(seed)
                let choices = List.init 40 (fun _ -> rnd.Next 1000)
                let actions = Model.walk CommitModel.model cfg choices |> List.map fst
                let r = run cfg actions
                let where = $"%s{name}, seed %d{seed}: %A{actions}"
                match r.Report with
                | Some why -> failwith $"the spans of a model run do not parse (%s{where})\n%s{why}"
                | None -> ()
                for KeyValue(w, taken) in r.Taken do
                    match r.Read[w] with
                    | Ok read -> Assert.True((read = taken), $"writer %A{w}: spans say %A{read}, driver took %A{taken} (%s{where})")
                    | Error why -> failwith $"writer %A{w}: %s{why} (%s{where})"
                    for a in taken do
                        seen.Add((sprintf "%A" a).Split(' ')[0]) |> ignore
        output.WriteLine $"visible actions read back: %A{List.ofSeq seen}"
        // Not vacuous: every visible action kind went through the mapping.
        for kind in [ "Begin"; "Ack"; "Reject"; "ServerError"; "Timeout" ] do
            Assert.True(seen.Contains kind, $"no run read back a %s{kind}")
