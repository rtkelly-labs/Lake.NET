/// Drives RestCatalog against the in-process REST server (Iceberg.Net.RestServer) with the model's actions. The same
/// SnapshotProducer commits run as in Conformance, but each commit request is held at the HTTP layer:
///   Begin w             start writer w's commit; it runs until its commit request reaches the client's handler
///   Apply(w, k)         forward the request to the server, which checks its requirements and swaps (or answers 409)
///   Ack / Reject        hand the writer the server's 200 or 409
///   ServerError(w, k)   drop the request and answer 500: CommitStateUnknownException with ServerResponded = true
///   Timeout(w, k)       fail the call in flight; the request stays held and may still be forwarded
///   Resend(w, k)        send the held request again with its Idempotency-Key: the server must recognise it and replay
///                       the original response (a resend of a request not yet forwarded follows it to the server)
/// The client's own idempotent retries are off (IdempotentRetries = 0), so each injected fault maps to one action.
module Lake.CommitProtocol.Tests.RestConformance

open System
open System.Collections.Generic
open System.Net
open System.Net.Http
open System.Text
open System.Text.Json.Nodes
open System.Threading
open System.Threading.Tasks
open Iceberg.Net.Catalog
open Iceberg.Net.IO
open Lake.CommitProtocol
open Lake.CommitProtocol.Tests.Conformance
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Mvc.Testing
open Microsoft.Extensions.Configuration

/// A request or response read into memory, so it can be sent or handed out again after the original is disposed.
type private Message =
    { Status: HttpStatusCode
      Method: HttpMethod
      Uri: Uri
      Headers: (string * string list) list
      ContentHeaders: (string * string list) list
      Body: byte array }

let private headers (h: Headers.HttpHeaders) =
    h |> Seq.map (fun kv -> kv.Key, List.ofSeq kv.Value) |> List.ofSeq

let private contentOf (content: HttpContent) =
    match content with
    | null -> [], [||]
    | c -> headers c.Headers, c.ReadAsByteArrayAsync().GetAwaiter().GetResult()

let private readRequest (r: HttpRequestMessage) =
    let contentHeaders, body = contentOf r.Content
    { Status = HttpStatusCode.OK
      Method = r.Method
      Uri = r.RequestUri
      Headers = headers r.Headers
      ContentHeaders = contentHeaders
      Body = body }

let private readResponse (r: HttpResponseMessage) =
    use r = r
    let contentHeaders, body = contentOf r.Content
    { Status = r.StatusCode
      Method = HttpMethod.Get
      Uri = null
      Headers = headers r.Headers
      ContentHeaders = contentHeaders
      Body = body }

let private withContent (m: Message) =
    let content = new ByteArrayContent(m.Body)
    for k, v in m.ContentHeaders do
        content.Headers.TryAddWithoutValidation(k, v) |> ignore
    content

let private toRequest (m: Message) =
    let r = new HttpRequestMessage(m.Method, m.Uri, Content = withContent m)
    for k, v in m.Headers do
        r.Headers.TryAddWithoutValidation(k, v) |> ignore
    r

let private toResponse (m: Message) =
    let r = new HttpResponseMessage(m.Status, Content = withContent m)
    for k, v in m.Headers do
        r.Headers.TryAddWithoutValidation(k, v) |> ignore
    r

let private replayed (m: Message) =
    m.Headers |> List.exists (fun (k, v) -> k = "Idempotent-Replayed" && v = [ "true" ])

/// The client's message handler: a writer's commit request (a POST to the table) waits for the driver; everything else
/// goes straight to the server.
type private HoldHandler(server: HttpMessageHandler) =
    inherit HttpMessageHandler()
    let invoker = new HttpMessageInvoker(server, true)

    /// The writer whose commit is running in this async flow (none during setup and observation).
    static member val Current = AsyncLocal<Writer option>()

    /// Called with each writer's commit request; returns the response the writer's call waits on.
    member val OnCommit: Writer -> Message -> Task<HttpResponseMessage> =
        fun _ _ -> failwith "no driver" with get, set

    /// Sends a request to the server, bypassing the hold.
    member _.Forward(m: Message) =
        use request = toRequest m
        readResponse (wait (invoker.SendAsync(request, CancellationToken.None)))

    override this.SendAsync(request, ct) =
        match HoldHandler.Current.Value with
        | Some w when request.Method = HttpMethod.Post && request.RequestUri.AbsolutePath.EndsWith "/namespaces/formal/tables/t" ->
            this.OnCommit w (readRequest request)
        | _ -> invoker.SendAsync(request, ct)

    override _.Dispose(disposing) =
        if disposing then invoker.Dispose()
        base.Dispose(disposing)

let private serverError =
    { Status = HttpStatusCode.InternalServerError
      Method = HttpMethod.Get
      Uri = null
      Headers = []
      ContentHeaders = [ "Content-Type", [ "application/json" ] ]
      Body = Encoding.UTF8.GetBytes """{"error":{"message":"injected server error","type":"ServerErrorException","code":500}}""" }

/// The model's state as the REST client sees it. The server writes a metadata file only for a request that wins, and
/// the client never writes or deletes one, so the metadata files that exist are those of the snapshots in the history
/// (each won when it was applied), less any the model has deleted.
let expectedRest (cfg: Config) (s: State) : Observed =
    { expected cfg s with
        MetadataFiles = Set.intersect (Set.map Protocol.metadataOf (Protocol.history s)) s.Store }

/// store: the REST server's catalog store, "memory" or "sqlite".
type RestDriver(cfg: Config, root: string, store: string) =
    let factory =
        let settings =
            dict
                [ "Iceberg:Store", store
                  "Iceberg:Warehouse", IO.Path.Combine(root, "warehouse")
                  "Iceberg:SqlitePath", IO.Path.Combine(root, "catalog.db")
                  "Logging:LogLevel:Default", "Warning" ]
        (new WebApplicationFactory<Program>())
            .WithWebHostBuilder(fun b ->
                b
                    .UseEnvironment("Development")
                    .ConfigureAppConfiguration(fun _ c -> c.AddInMemoryCollection(settings) |> ignore)
                |> ignore)
    let handler = new HoldHandler(factory.Server.CreateHandler())
    let catalog =
        let options = RestCatalogOptions(Uri = Uri "http://localhost/", Handler = handler, IdempotentRetries = 0)
        wait (RestCatalog.ConnectAsync(options, (fun _ -> new LocalFileIO() :> IFileIO), "formal"))
    let pending = Dictionary<Writer * int, Message>()
    /// Keyed resends of a held request, sent after it when it is forwarded.
    let resends = Dictionary<Writer * int, int>()
    /// The server's response to each forwarded request.
    let responses = Dictionary<Writer * int, Message>()
    let replies = Dictionary<Writer * int, Message>()
    let blocked = Dictionary<Writer * int, TaskCompletionSource<HttpResponseMessage>>()
    let attempts = Dictionary<Writer, int>()
    let arrived = Dictionary<Writer, TaskCompletionSource>()
    let tasks = Dictionary<Writer, Task>()
    let locations = Dictionary<File, string>()

    do
        handler.OnCommit <-
            fun w request ->
                let k = attempts.GetValueOrDefault(w) + 1
                attempts.[w] <- k
                pending.[(w, k)] <- request
                let reply = TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously)
                blocked.[(w, k)] <- reply
                arrived.[w].TrySetResult() |> ignore
                reply.Task
        locations.[Meta0] <- setUp catalog root

    let settle (w: Writer) (signal: TaskCompletionSource) =
        waitUnit (Task.WhenAny(tasks.[w], signal.Task))

    let arm (w: Writer) =
        let signal = tcs ()
        arrived.[w] <- signal
        signal

    let respond (w: Writer) (k: int) (complete: TaskCompletionSource<HttpResponseMessage> -> unit) =
        let reply = take blocked (w, k) "blocked commit request"
        let signal = arm w
        complete reply
        settle w signal

    /// A resend must be recognised by its key: the server replays the original response and changes nothing.
    let resend (w: Writer) (k: int) (request: Message) =
        let original = responses.[(w, k)]
        let again = handler.Forward request
        if not (replayed again && again.Status = original.Status) then
            failwithf
                "The real catalog differs from the model: a resend of (%A, %d) was not deduplicated (%A, replayed: %b; the original was %A)"
                w k again.Status (replayed again) original.Status

    let held = Dictionary<Writer * int, Message>()

    member _.Step(action: Action) =
        match action with
        | Begin w ->
            let signal = arm w
            let task =
                Task.Run(Func<Task>(fun () ->
                    HoldHandler.Current.Value <- Some w
                    let table = wait (catalog.LoadTableAsync(tableId))
                    (producer cfg root table w).CommitAsync() :> Task))
            tasks.[w] <- task
            settle w signal
        | Apply(w, k) ->
            let request = take pending (w, k) "held commit request"
            held.[(w, k)] <- request
            let response = handler.Forward request
            responses.[(w, k)] <- response
            if response.Status = HttpStatusCode.OK then
                let body = JsonNode.Parse(response.Body)
                locations.[Meta(w, k)] <- body.["metadata-location"].GetValue<string>()
            for _ in 1 .. resends.GetValueOrDefault((w, k)) do
                resend w k request
            resends.Remove((w, k)) |> ignore
            // A writer that stopped waiting (it timed out) never hears the response.
            if blocked.ContainsKey((w, k)) then replies.[(w, k)] <- response
        | Ack(w, k) ->
            let reply = take replies (w, k) "response"
            if reply.Status <> HttpStatusCode.OK then
                notPerformable "Ack(%A, %d), but the server answered %A" w k reply.Status
            respond w k (fun r -> r.SetResult(toResponse reply))
        | Reject(w, k) ->
            let reply = take replies (w, k) "response"
            if reply.Status <> HttpStatusCode.Conflict then
                notPerformable "Reject(%A, %d), but the server answered %A" w k reply.Status
            respond w k (fun r -> r.SetResult(toResponse reply))
        | ServerError(w, k) ->
            pending.Remove((w, k)) |> ignore
            resends.Remove((w, k)) |> ignore
            replies.Remove((w, k)) |> ignore
            respond w k (fun r -> r.SetResult(toResponse serverError))
        | Timeout(w, k) ->
            replies.Remove((w, k)) |> ignore
            respond w k (fun r -> r.SetException(HttpRequestException "injected timeout"))
        | Resend(w, k) ->
            if pending.ContainsKey((w, k)) then resends.[(w, k)] <- resends.GetValueOrDefault((w, k)) + 1
            elif held.ContainsKey((w, k)) then resend w k held.[(w, k)]
            else notPerformable "Resend(%A, %d), but there is no such request" w k

    member _.Observe() : Observed = observe cfg catalog locations (phaseOf tasks blocked.Keys)

    interface IDriver with
        member this.Step(action) = this.Step action
        member this.Observe() = this.Observe()
        member _.Expected(s) = expectedRest cfg s

    interface IDisposable with
        member _.Dispose() =
            for reply in blocked.Values do
                reply.TrySetException(OperationCanceledException()) |> ignore
            for t in tasks.Values do
                try t.Wait(stepTimeout) |> ignore with _ -> ()
            (catalog :> IDisposable).Dispose()
            handler.Dispose()
            factory.Dispose()
