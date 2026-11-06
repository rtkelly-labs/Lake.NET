namespace Formal.Traces.Iceberg

/// The commit protocol's observable actions: the trace grammar's alphabet. Each case is one thing a writer does or
/// learns during one logical commit (an <c>iceberg.commit</c> span). The cases are payload-free on purpose: the grammar
/// is over the protocol's shape; identities (commit, table, snapshot) travel beside the event, as the slice key.
///
/// It refines the model's actions (Begin, Ack, Reject, ServerError, Timeout) to what a client's spans can show: one
/// model step is several events here. formal/traces/README.md tabulates the correspondence. It is derived from
/// docs/state-management.md (the failure taxonomy) and the spans IcebergTelemetry emits; Spans.fs maps the spans.
type CommitEvent =
    /// The writer starts a logical commit (plans against the state it has loaded).
    | Begin
    /// The pointer swap succeeded: the catalog applied this attempt.
    | SwapApplied
    /// The swap was refused (<c>CommitFailedException</c>): nothing was applied; refresh before trying again.
    | SwapRejected
    /// The catalog may or may not have applied it (<c>CommitStateUnknownException</c>).
    | SwapUnknown
    /// The swap failed and the trace does not say how. Today's catalog spans do not classify failures, so a
    /// production trace has only this; the grammar then allows what either a rejection or an unknown outcome allows.
    | SwapFailed
    /// The writer reloaded the table from the catalog.
    | Reload
    /// Reloading failed.
    | ReloadFailed
    /// The writer backs off and will try again, after a lost race.
    | Retry
    /// The writer backs off and will try again after an outcome it never resolved: a blind retry. No rule of the
    /// protocol produces it, so any trace containing it is rejected.
    | RetryAfterUnknown
    /// After an unknown outcome and a reload: the attempted snapshot is in the table (proposed telemetry).
    | Found
    /// After an unknown outcome and a reload: the attempted snapshot is not in the table (proposed telemetry).
    | Absent
    /// The commit ended applied.
    | Committed
    /// The commit ended with nothing to apply.
    | NothingToCommit
    /// The commit ended refused: retries did not resolve the conflict.
    | GaveUp
    /// The commit ended with its outcome unknown to the caller.
    | LeftUnknown
    /// The commit ended with another error, or was cancelled.
    | Aborted

[<RequireQualifiedAccess>]
module CommitEvent =
    /// Every case, for exhaustiveness checks.
    let all =
        Reflection.FSharpType.GetUnionCases typeof<CommitEvent>
        |> Array.map (fun c -> Reflection.FSharpValue.MakeUnion(c, [||]) :?> CommitEvent)
        |> List.ofArray

    let show (e: CommitEvent) = $"%A{e}"
