namespace Lake.CommitTraces

open Lake.CommitProtocol

/// From a commit slice, read as client-observable events, back to the model's actions.
///
/// Only the writer's own steps are visible. Apply happens inside the catalog, Resend is joined to its original request by
/// the idempotency key, and CleanOrphans is maintenance, not a commit, so none of them appears in a slice. The attempt
/// number is recovered by counting swaps, since each attempt makes exactly one (Spans.snapshots checks it against the tag).
[<RequireQualifiedAccess>]
module Actions =

    /// The model actions writer w took in this slice, in order.
    ///
    /// An unknown outcome is a ServerError when the writer then tried to resolve it (the server answered, so the request
    /// is finished), and a Timeout when it gave up at once (no answer: the request may still land). A slice with an
    /// unclassified catalog failure (SwapFailed) has no counterpart, so the result is an Error naming
    /// the position. The slice is assumed to parse; use CommitGrammar.parse first.
    let ofSlice (w: Writer) (events: CommitEvent list) : Result<Action list, string> =
        let rec go (attempt: int) (acc: Action list) (events: CommitEvent list) (i: int) =
            match events with
            | [] -> Ok(List.rev acc)
            | CommitEvent.Begin :: rest -> go attempt (Action.Begin w :: acc) rest (i + 1)
            | CommitEvent.SwapApplied :: rest -> go (attempt + 1) (Ack(w, attempt + 1) :: acc) rest (i + 1)
            | CommitEvent.SwapRejected :: rest -> go (attempt + 1) (Reject(w, attempt + 1) :: acc) rest (i + 1)
            | CommitEvent.SwapUnknown :: (CommitEvent.LeftUnknown :: _ as rest) ->
                go (attempt + 1) (Timeout(w, attempt + 1) :: acc) rest (i + 1)
            | CommitEvent.SwapUnknown :: rest -> go (attempt + 1) (ServerError(w, attempt + 1) :: acc) rest (i + 1)
            | CommitEvent.SwapFailed :: _ ->
                Error(sprintf "position %d: SwapFailed is an unclassified catalog failure, with no model action" i)
            | _ :: rest -> go attempt acc rest (i + 1)
        go 0 [] events 0

    /// The visible part of a run of model actions: writer w's own replies and its Begin, which is what ofSlice recovers.
    let visible (w: Writer) (actions: Action list) : Action list =
        actions
        |> List.filter (fun a ->
            match a with
            | Action.Begin v -> v = w
            | Ack(v, _)
            | Reject(v, _)
            | ServerError(v, _)
            | Timeout(v, _) -> v = w
            | Apply _
            | Resend _
            | CleanOrphans -> false)
