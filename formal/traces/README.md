# Trace grammars

The model checks the commit protocol as designed. These grammars check that recorded runs followed it. A run is a set
of OpenTelemetry spans, captured in process for now and exported over OTLP later. It is projected onto the protocol's
events, sliced into one sequence per logical commit, and each slice is parsed. A slice that does not parse is a
protocol violation. The parser reports it in protocol terms: the event, the state the writer was in, what was allowed
there, and what happened instead:

```
commit protocol: protocol violation at event 3
  trace:    Begin, SwapUnknown, Retry  <-- here
  state:    Uncertain - the catalog may have applied the commit; it must reload and look for the snapshot before anything else
  expected: Reload | ReloadFailed | LeftUnknown
  got:      Retry
```

| Project | What it is |
|---|---|
| `src/Formal.Traces` | A generic library with no Iceberg.Net reference. It covers grammars over any event type, FIRST/FOLLOW, LL(1) table construction with conflict explanations, a table-driven parser with protocol-level errors, a bounded ambiguity search, and parametric slicing. |
| `lake/Lake.CommitTraces` | Lake.NET's side: the event vocabulary (`CommitEvent`), the commit grammar (`CommitGrammar`) and the span projection (`Spans`). |
| `tests/Formal.Traces.Tests` | Library tests, including a differential property: on 1000 random grammars, the table, the parser and enumeration must agree. |
| `lake/Lake.CommitTraces.Tests` | Grammar properties, the projection, broken writers on real spans, and the in-process soak. |

They are in `formal/Formal.slnx`, so the formal workflow runs them with the model's tests. On their own: `dotnet test formal/tests/Formal.Traces.Tests` and `dotnet test formal/lake/Lake.CommitTraces.Tests`.
Neither needs Docker or Python. `FORMAL_TRACES_SOAK_SECONDS` lengthens the soak (the default is 3).

## The event vocabulary

The grammar's alphabet is the protocol's observable actions, as a client sees them. They refine the model's actions
(see the alignment below), so the model, the conformance tests and the trace check share one vocabulary at two levels
of detail. It was derived from the failure taxonomy in
[docs/state-management.md](../../docs/state-management.md) and from the spans `IcebergTelemetry` emits.

| Event | Meaning |
|---|---|
| `Begin` | A logical commit starts. |
| `SwapApplied` / `SwapRejected` / `SwapUnknown` | The catalog applied the swap, refused it (`CommitFailedException`), or may have applied it (`CommitStateUnknownException`). |
| `SwapFailed` | The swap failed with something other than a conflict or an unknown outcome (for example a stale-state `ArgumentException`), so the trace does not say whether the catalog applied it. |
| `Reload` / `ReloadFailed` | The writer reloaded the table, or failed to. |
| `Retry` | The writer backs off after a lost race. |
| `RetryAfterUnknown` | The writer backs off after an outcome it never resolved. No rule produces it, so it is always a violation. |
| `Found` / `Absent` | The writer resolved an unknown outcome: the attempted snapshot is in the reloaded table, or it is not. |
| `Committed` / `NothingToCommit` / `GaveUp` / `LeftUnknown` / `Aborted` | How the commit ended. |

Events carry no payload. Identities (commit, table, snapshot) travel beside an event as its slice key, so the
grammar describes the protocol's shape and nothing else.

### Alignment with the model's `Action`

`CommitEvent` is a client-observable refinement of the model's `Action` (`formal/lake/Lake.CommitProtocol`), and
`Actions.ofSlice` maps a parsed slice back to it. A model step is atomic, while a trace records the client doing that
step in several spans. So each visible model action corresponds to a short run of events, and the rest correspond to
nothing a client span can see:

| Model action | Trace events |
|---|---|
| `Begin w` | `Begin` (the writer is the slice key) |
| `Apply (w, k)` | none (it happens inside the catalog; a server-side trace could record it) |
| `Ack (w, k)` | `SwapApplied`, then `Committed` |
| `Reject (w, k)` | `SwapRejected`, then `Retry Reload`, or `GaveUp` once attempts run out |
| `ServerError (w, k)` | `SwapUnknown`, then a reload (`Found`, `Absent` or `ReloadFailed`); after `Absent`, as `Reject` |
| `Timeout (w, k)` | `SwapUnknown LeftUnknown` |
| `Resend (w, k)` | none yet: an `iceberg.catalog.resend` event on the `commit_table` span records it, but the projection does not read it |
| `CleanOrphans` | none (maintenance, not a commit) |

`ofSlice` recovers the attempt number `k` by counting swaps, one per attempt; `Spans.snapshots` checks that count
against the `iceberg.commit.attempt` tag on each swap. A slice with an unclassified `SwapFailed` maps to no action, so
`ofSlice` returns an error there.

`ConformanceTraceTests` checks the mapping against the model itself. The model's conformance driver runs real
`SnapshotProducer` commits on `InMemoryCatalog` along random walks of the model (the `standard` and `removers`
configurations; the driver does not drive orphan cleanup). The spans those commits emit must parse, and
`Actions.ofSlice` must return exactly the visible actions the driver took for every writer that finished. This also
confirms that the model's `Timeout` reaches the trace as `SwapUnknown LeftUnknown`: the metastore catalog wraps a
transport timeout as an unknown outcome the server did not answer. The REST driver is a follow-up: its in-process
server emits spans on the same `ActivitySource`, so server-side spans need separating from the client's first.

## The commit grammar

```
Commit       := Begin Ready
Ready        := Reload Ready | ReloadFailed Failed | Retry Backoff | SwapApplied Done | SwapRejected Stale
              | SwapUnknown Uncertain | SwapFailed Unclassified | NothingToCommit | GaveUp | Aborted
Done         := Committed
Failed       := Aborted
Stale        := Retry Backoff | Reload Ready | GaveUp | Aborted
Backoff      := Reload Ready | ReloadFailed Failed | Aborted
Uncertain    := Reload Resolving | ReloadFailed Unresolved | LeftUnknown
Unresolved   := LeftUnknown | Aborted
Resolving    := Found Done | Absent Stale | Committed | Retry Backoff | GaveUp
Unclassified := Retry Backoff | Reload Reloaded | ReloadFailed Unresolved | GaveUp | LeftUnknown | Aborted
Reloaded     := Found Done | Absent Stale | Committed | Ready's alternatives
```

Each nonterminal is a state a writer can be in, and its doc string is what a violation prints. The rules that matter:

- **After an unknown outcome**, the only moves are to reload (`Uncertain`) or to end unknown. A retry there is a
  blind retry.
- **After an applied swap**, only the end may follow (`Done`). A second swap, or a retry after `Found`, is a double
  apply.
- **After a back-off**, the next attempt starts from a reload (`Backoff`).

`Unclassified` and `Reloaded` exist so that a swap failure the catalog did not classify (`SwapFailed`) still parses.
They allow whatever either a rejection or an unknown outcome would allow. That is sound, but weaker: see
[What the telemetry records](#what-the-telemetry-records).

## Why a regular grammar, and why per-commit slices

A run interleaves many writers. Interleavings are not context-free in any useful sense: checking them as one string
would need a grammar for every interleaving. Parametric trace slicing (Chen and Roşu, TACAS 2009) removes that
problem. The trace is split by a parameter, here the `iceberg.commit` span, and each slice is checked independently.
Each slice is one writer's sequential protocol, so nothing in it needs a stack:

- `CommitScope` already flattens the one place commits nest. A `SnapshotProducer` attempt inside a mutation's re-plan
  loop reports into the outer scope, so a mutation is one span however its retries are layered.
- Every loop in the protocol (retry, reload, re-plan) is tail recursion. Nothing has to be matched later, as brackets
  would be.

So the grammar is **right-linear**: every alternative is terminals followed by at most one nonterminal. Its language is
regular and the parser is a DFA. A context-free grammar would add power the protocol does not use, and with it
ambiguity that is undecidable in general. The library still accepts any context-free grammar and refuses the ones
that are not LL(1) (the generic tests use the textbook expression grammar), so a future protocol that genuinely
nests, such as multi-table transactions, has room to grow.

## Ambiguity

Two independent checks:

1. **LL(1) table construction.** `Grammar.ll1` either builds a conflict-free table or reports every conflict, worded
   as Menhir words its explanations: the state, the lookahead, and the alternatives that both start with it. The
   approach is the one taken by Vermillion (Lasser et al., ITP 2019), which refuses any grammar whose table has a
   conflict. A successful build is therefore a statement about the whole grammar, not about the inputs seen so far.
   A conflict-free table means one leftmost derivation per sentence, so the grammar is unambiguous. That step is our
   own reasoning; Vermillion does not state it as a theorem. The commit grammar builds with no conflicts.
2. **Bounded enumeration.** `Ambiguity.search` counts derivations of every sentence up to a length bound (counts are
   capped at 2) and reports any sentence with two. The commit grammar has 2,379 sentences of length ≤ 8 and none has
   two derivations. Unlike a SAT encoding (Axelsson, Heljanko and Lange, ICALP 2008), this is plain enumeration. That
   is enough for the grammars here, and it is independent of the table code.

Our table builder is not verified as Vermillion's is. The substitute is translation validation in the spirit of
MenhirLib's validator (Jourdan, Pottier and Leroy, ESOP 2012): rather than trust the builder, check its output.
`DifferentialTests` generates 1000 random grammars. For each one that gets a table, it requires that the bounded search
finds no ambiguity and that the parser accepts exactly the sentences enumeration derives. The run must also produce
LL(1), conflicting and ambiguous grammars, so the property is not vacuous. Two cautions. MenhirLib's own unambiguity
result is about semantic values, not parse trees, so we borrow its pattern, not its theorem. CoStar (PLDI 2021)
detects ambiguity per input, so a clean run of it says nothing about unseen inputs; a table does.

## From spans to events

`Spans.project` reads finished spans as `SpanRecord`s. The in-process listener builds them from `Activity`; an OTLP
reader would build the same records. For each `iceberg.commit` span C:

| Source | Event |
|---|---|
| C starts | `Begin` |
| Direct child `iceberg.catalog.commit_table`, by `iceberg.outcome` | success (status Ok, no outcome tag) → `SwapApplied`; `conflict` → `SwapRejected`; `unknown` → `SwapUnknown`; anything else → `SwapFailed` |
| Direct child `iceberg.catalog.load_table` | success → `Reload`, otherwise `ReloadFailed` |
| Event `iceberg.commit.retry` on C | `error.type` ending in `CommitStateUnknownException` → `RetryAfterUnknown`, otherwise `Retry` |
| Event `iceberg.commit.resolution` on C | `found` → `Found`, `absent` → `Absent` |
| C ends, by `iceberg.outcome` | `success` → `Committed`, `noop` → `NothingToCommit`, `conflict` → `GaveUp`, `unknown` → `LeftUnknown`, `cancelled` / `error` → `Aborted` |

Events are ordered by time. At equal times the start comes first and the end last. The slice key is C's trace and span
ID, with the table name kept for messages. `commit_table` spans with no `iceberg.commit` parent are counted and
reported, not checked.

`Spans.snapshots` then checks the data the grammar cannot: each swap's `iceberg.commit.attempt` is the number of
retries before it plus one, a `found` resolution names the snapshot of the swap before it, and the committed snapshot
is the last one applied or found.

## What the telemetry records

`IcebergTelemetry` records what the checker needs; [docs/telemetry.md](../../docs/telemetry.md) lists the spans and
tags. Each item below was once a gap that forced a weaker check.

1. **Catalog commits classify failures.** `commit_table` spans tag `iceberg.outcome` as `conflict`, `unknown` and
   so on, with `error.type`, so a rejection and an unknown outcome project to different events. `SwapFailed` and
   `Unclassified` remain for anything else, such as a stale-state `ArgumentException`.
2. **The resolution of an unknown outcome is recorded.** After a reload, `SnapshotProducer` adds an
   `iceberg.commit.resolution` event (`found` or `absent`) to the commit span. Without it, a writer that finds its
   commit applied and commits again is indistinguishable from one that found it absent and retried. With (1) and (2),
   both broken writers in `BrokenWriterTests` are caught.
3. **Catalog commits say which attempt they are.** A `commit_table` inside a commit carries `iceberg.commit.attempt`
   and the attempted `iceberg.snapshot.id`, and a resolution names the snapshot it looked for, which is what
   `Spans.snapshots` checks.
4. **Ref, expiry and evolution commits have commit spans.** `SnapshotManagement` (branches, tags, rollback),
   `Maintenance.ExpireAsync`, and schema and spec evolution open an `iceberg.commit` span around their retry loops
   (operations `manage-snapshots`, `expire-snapshots`, `update-schema`, `update-spec`), so their swaps are checked by
   the same grammar. They add no snapshot, so `Spans.snapshots` has nothing to match for them. One-shot commits with
   no retry loop (`SetPropertiesAsync`, `ReplaceSortOrderAsync`, statistics, name mapping, the cherry-pick
   fast-forward) still commit directly and remain orphans: counted, not checked.
5. **Multi-table transactions** (`commit_transaction`) are out of scope for this grammar.
6. **Catalog spans tag success.** A successful catalog call sets `iceberg.outcome = success` as well as status Ok, so
   an exporter that drops status keeps it.
7. **Idempotent resends are recorded, not yet projected.** Each time `RestCatalog` resends a request with the same
   idempotency key, it adds an `iceberg.catalog.resend` event to the catalog span (named for the catalog, since any
   mutating call can be resent). The projection does not read it yet, so the model's `Resend` has no trace event.

## Data sources

- **In CI (built):** the soak in `SoakTraceTests`. Four writers race on one in-memory table through the library's real
  paths: append, insert, upsert, and delete under every strategy. Meanwhile the catalog injects unknown outcomes: one
  of each kind first, then 15% of swaps at random. An `ActivityListener` captures the spans. Every commit must parse,
  and `Spans.snapshots` must find nothing wrong. The run must also reach every branch the faults and races can
  reach (applied, rejected, unknown found, unknown absent, left unknown, retry), so it cannot pass vacuously.
  `BrokenWriterTests` drives the library's own commit through each fault deterministically.
- **Production (described, not built):** read OTLP-exported spans into `SpanRecord`s, slice by commit span, and
  report violations. This is the shape of PObserve's checker: a parser into typed events with a partition key, a
  sequencer that orders them, a demultiplexer, then one monitor per partition. Two things differ from the in-process case. Spans arrive out of
  order and late, so a slice is checked once its root span has ended. A sampled trace may lack children, so a
  production checker should only check traces sampled as a whole (parent-based sampling), and report an incomplete
  slice as unknown, not as a violation.

## Prior art

- **Trace validation against a specification.** etcd's raft repository checks recorded NDJSON events against its TLA+
  spec by mapping each event to a spec action. Cirstea, Kuppe, Loillier and Merz (2024, arXiv:2404.16075) describe
  that method in general. When a trace fails, it does so without a counterexample: the result is the longest prefix
  that could be matched. A grammar gives more here: the position, the state and the allowed events. It gives less
  elsewhere: it has no data, so it cannot check, for example, that a found snapshot is the one attempted; `Spans.snapshots` does
  that beside it. Nor
  does it tolerate gaps: that method only asks that some behaviour of the spec matches the trace, letting the checker
  choose unlogged steps, whereas a grammar needs every event. Hence the sampling rule for production above. The
  model handles data; the grammar handles shape.
- **Runtime verification.** Parametric trace slicing (Chen and Roşu, TACAS 2009), as above. PObserve, for the
  architecture of a production checker.
- **Grammar construction and validation.** Vermillion (Lasser, Casinghino, Fisher and Roux, ITP 2019) refuses a
  grammar with any LL(1) conflict. MenhirLib (Jourdan, Pottier and Leroy, ESOP 2012) gives the translation-validation
  pattern. Menhir's conflict explanations shape the wording of our conflict messages, and Pottier's error-reachability
  work (CC 2016) motivates naming the state in errors.
- **Not used:** FsLexYacc builds LALR tables with no LL(1) analysis. FParsec combinators have no table to analyse.
  Neither can say a grammar is unambiguous before it sees an input. Session types describe the same per-writer
  protocols as types, which suits code more than recorded traces.
