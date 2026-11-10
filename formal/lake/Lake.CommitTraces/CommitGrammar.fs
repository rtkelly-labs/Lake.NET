namespace Lake.CommitTraces

open Formal.Traces

/// The commit protocol of docs/state-management.md as a grammar over one logical commit's events. It is right-linear
/// (every alternative is terminals followed by at most one nonterminal), so the language is regular; and each rule's
/// alternatives begin with distinct events, so it is LL(1) and the parser is the protocol's state machine. Rules are
/// the states a writer can be in; their docs are what the violation message prints.
[<RequireQualifiedAccess>]
module CommitGrammar =

    let private t e = T e
    let private n s = N s

    /// Alternatives open to a writer holding a plan it may commit.
    let private acting =
        [ "reload", [ t Reload; n "Ready" ]
          "reload-failed", [ t ReloadFailed; n "Failed" ]
          "retry", [ t Retry; n "Backoff" ]
          "applied", [ t SwapApplied; n "Done" ]
          "rejected", [ t SwapRejected; n "Stale" ]
          "unknown", [ t SwapUnknown; n "Uncertain" ]
          "failed", [ t SwapFailed; n "Unclassified" ]
          "nothing", [ t NothingToCommit ]
          "gave-up", [ t GaveUp ]
          "aborted", [ t Aborted ] ]

    let rules =
        [ Grammar.rule "Commit" "one logical commit (an iceberg.commit span)" [ "begin", [ t Begin; n "Ready" ] ]
          Grammar.rule "Ready" "planning against the state last loaded; may swap, reload, or back off" acting
          Grammar.rule "Done" "the catalog applied this commit; nothing may follow but its end" [ "committed", [ t Committed ] ]
          Grammar.rule "Failed" "a reload failed; the commit can only end" [ "aborted", [ t Aborted ] ]
          Grammar.rule
              "Stale"
              "the catalog refused the swap, so nothing was applied; back off and reload, or give up"
              [ "retry", [ t Retry; n "Backoff" ]
                "reload", [ t Reload; n "Ready" ]
                "gave-up", [ t GaveUp ]
                "aborted", [ t Aborted ] ]
          Grammar.rule
              "Backoff"
              "backing off after a lost race; the next attempt must start from a reload"
              [ "reload", [ t Reload; n "Ready" ]; "reload-failed", [ t ReloadFailed; n "Failed" ]; "aborted", [ t Aborted ] ]
          Grammar.rule
              "Uncertain"
              "the catalog may have applied the commit; it must reload and look for the snapshot before anything else"
              [ "resolve", [ t Reload; n "Resolving" ]
                "resolve-failed", [ t ReloadFailed; n "Unresolved" ]
                "left-unknown", [ t LeftUnknown ] ]
          Grammar.rule
              "Unresolved"
              "the outcome could not be resolved; the commit must end unknown"
              [ "left-unknown", [ t LeftUnknown ]; "aborted", [ t Aborted ] ]
          Grammar.rule
              "Resolving"
              "reloaded after an unknown outcome; found means committed, absent means a plain conflict"
              [ "found", [ t Found; n "Done" ]
                "absent", [ t Absent; n "Stale" ]
                // A reload without a resolution event (a writer that does not record one): the result is only
                // visible in what follows.
                "committed", [ t Committed ]
                "retry", [ t Retry; n "Backoff" ]
                "gave-up", [ t GaveUp ] ]
          Grammar.rule
              "Unclassified"
              "the swap failed and the trace does not say how; allowed: what a rejection or an unknown outcome allows"
              [ "retry", [ t Retry; n "Backoff" ]
                "reload", [ t Reload; n "Reloaded" ]
                "reload-failed", [ t ReloadFailed; n "Unresolved" ]
                "gave-up", [ t GaveUp ]
                "left-unknown", [ t LeftUnknown ]
                "aborted", [ t Aborted ] ]
          Grammar.rule
              "Reloaded"
              "reloaded after an unclassified failure; as Ready, or as resolving an unknown outcome"
              ([ "found", [ t Found; n "Done" ]; "absent", [ t Absent; n "Stale" ]; "committed", [ t Committed ] ] @ acting) ]

    /// The grammar; construction checks every rule is defined, reachable and productive.
    let grammar = Grammar.create "commit protocol" "Commit" rules

    /// The LL(1) table; a conflict here is a bug in the grammar, reported in full.
    let table =
        match Grammar.ll1 grammar with
        | Ok table -> table
        | Error conflicts ->
            conflicts
            |> List.map (Grammar.explainConflict CommitEvent.show)
            |> String.concat "\n"
            |> invalidOp

    /// Check one commit's events.
    let parse (events: CommitEvent list) = Parser.parse table events

    /// A violation as text.
    let explain (e: ParseError<CommitEvent>) = Parser.explain CommitEvent.show e
