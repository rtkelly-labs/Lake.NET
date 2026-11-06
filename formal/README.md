# formal/

Lake.NET's commit protocol, written once in F# and checked in two ways:

- **TLC model-checks it.** FsTla translates the F# model to TLA+, and TLC explores every reachable state of a small
  configuration.
- **It is an oracle for the production code.** FsCheck walks the same model and drives the real `InMemoryCatalog` and
  `SnapshotProducer` step by step with the same actions, comparing the two after every step.

The model and the code under test share one type, `Action`, so a TLC counterexample is a list of F# values. You can
replay it natively, step it in a debugger, or turn it into a conformance scenario.

This is an incubator. It has its own solution (`Formal.slnx`) and its own CI workflow (`.github/workflows/formal.yml`).
It is not part of `Iceberg.Net.slnx`, and nothing in `src/` depends on it.

## Layout

| Path | What |
|---|---|
| `src/FsTla` | The generic library: F# value ↔ TLA+ value encoding, the quotation-to-TLA+ translator, a TLC runner and parser of its output, and `Model` (define, walk, replay, check, differential). It references nothing from Iceberg.Net. |
| `tests/FsTla.Tests` | FsTla's own tests: the encoding round-trips, refusals of untranslatable code, and a toy model checked by TLC. |
| `lake/Lake.CommitProtocol` | The commit protocol: `Types.fs` (the vocabulary, including `Action`), `Protocol.fs` (step and invariants, translated to TLA+), `CommitModel.fs` (the model and the configurations CI checks). |
| `lake/Lake.CommitProtocol.Tests` | Model tests, TLC tests, and conformance against the real catalog. This is the one project that references `src/` (by relative path). |
| `lake/tla` | The generated `CommitProtocol.tla` / `.cfg`. They are committed so a reviewer can read the TLA+, and a test fails if they drift from the F#. |
| `traces/` | Reserved for phase two (see [Traces](#traces-phase-two)). |

## How a model is written

A model is a `[<ReflectedDefinition>]` F# module made of plain, total functions:

- the step has the shape `Config -> Action -> State -> State option` (`None` means the action is not enabled);
- each invariant has the shape `Config -> State -> bool`.

`Model.define` and `Model.invariant` take quotations of those functions. FsTla reads their reflected definitions and
translates every function they call into a TLA+ operator.

**The translatable subset.** Records, unions, options, `Set`, `Map`, `int`, `bool` and `string`. Pattern matching,
`if`, `let`, and the `Set`, `Map` and `Option` module functions that have a TLA+ counterpart. Anything else, such as
mutation, loops, sequencing or arbitrary .NET calls, is refused with the function it appears in. For example:
`In FsTla.Tests.Toy.unsupported: sequencing (a; b) is not translated: models are pure expressions`.

**The encoding.**

- A union case is a record with a `tag`: `[tag |-> "Meta", writer |-> [tag |-> "W1"], attempt |-> 2]`. An unnamed
  field is `Item`, and `option`'s field is `Value`.
- A `Map` is a TLA+ function, written `(k1 :> v1 @@ k2 :> v2)`. The empty map is `<<>>`.
- The generated module has one variable, `state`. It defines
  `Next == \E a \in Actions : LET r == Step(Cfg, a, state) IN r.tag = "Some" /\ state' = r.Value`,
  where `Actions` is every value of the action type for the configuration.

**Why TLC can be trusted.** FsTla never decides anything itself: TLC is the checker. Three checks keep the
translation honest:

- **Differential.** TLC dumps every reachable state. Each one must be the F# model's step from some predecessor, and
  must have the same invariant verdicts in F#. If the F# model and TLC disagree, the translator has a bug.
- **Replay.** Each counterexample is replayed through the F# step and must reach the same final state and the same
  violated invariant.
- **Round-trip.** Every state reached on random walks is encoded as a TLA+ literal, parsed back, and must decode to
  itself.

## The commit protocol model

It follows `SnapshotProducer.CommitAsync` and `MetastoreCatalog`'s write-then-swap:

1. A writer writes a new metadata file on top of the current one.
2. It asks the catalog to swap the pointer from the location it read (an optimistic compare-and-swap).

The outcomes:

- **Lost swap (`CommitFailedException`).** The writer deletes that metadata file, refreshes, re-validates and retries
  up to `commit.retry.num-retries` times.
- **Validation conflict.** If re-validating an overwrite finds a conflict, the commit fails (`ValidationException`).
  That is serializable isolation.
- **Server error (`CommitStateUnknownException` with `ServerResponded`).** The writer reloads the table and looks for
  its own snapshot before deciding whether to retry.
- **Timeout.** The commit ends as unknown, and nothing written for it is deleted.

**Actions** (`Types.fs`): `Begin w`, `Apply(w, k)` (the catalog's CAS of attempt `k`), and the ways the reply reaches
the writer: `Ack`, `Reject`, `ServerError`, `Timeout`, and `Resend` (the client resends the request with the same
`Idempotency-Key`, as `RestCatalog` does when the server advertises a key lifetime). Splitting `Apply` from the reply
is what makes unknown outcomes expressible: a request can be applied and its reply lost, or time out and still be
applied later.

**Invariants**:

| Invariant | Meaning |
|---|---|
| `AtMostOnce` | No writer's commit is in the current history twice. |
| `NoLostCommit` | Every acknowledged commit is in the current history (an ack implies a commit, and concurrent commits never lose one). |
| `FailedNeverApplies` | A writer told its commit failed has nothing in the table and no request left that could still land. |
| `ReachableFilesExist` | The current metadata file and every live data file exist. Nothing written for a commit that may have landed is deleted. |
| `SerializableOverwrite` | No overwrite committed over a file added to its partition after it began. |

**Seeded bugs**: each must be caught by TLC, and its trace must replay in F#.

| Bug | Broken invariant(s) | Shortest trace TLC finds |
|---|---|---|
| `RetryUnknownBlindly`: retry after a server error without looking for the commit | `AtMostOnce` or `FailedNeverApplies` | `Begin W3; Apply(W3, 1); ServerError(W3, 1)`: the overwrite's blind retry fails validation against its own landed commit, so it reports Failed for a commit that is in the table. For an append it is a double apply instead (`ModelTests.retryUnknownWitness`). |
| `DeleteOnUnknown`: delete the metadata file after an unknown outcome | `ReachableFilesExist` | a timed-out swap that is applied after its file was deleted |
| `SnapshotIsolation`: skip retry-time validation | `SerializableOverwrite` | an overwrite retried over an append to its partition |
| `BlindSwap`: a catalog that swaps without comparing | `NoLostCommit` | two writers on the same base; the second swap erases the acknowledged first |
| `NoIdempotencyKey`: a resend is evaluated as a new request | `ReachableFilesExist` or `AtMostOnce` | `Begin W1; Apply(W1, 1); Resend(W1, 1); Apply(W1, 1); Reject(W1, 1)`: the resend of a commit that landed loses its CAS, and the writer deletes the current metadata file |

**The configuration CI checks** (`CommitModel.standard`):

- three writers: two appends to different partitions, and an overwrite of the first partition;
- two attempts each;
- one injected fault (a server error, a timeout or a resend).

**Abstractions**:

- **Data.** One data file per writer and a seed file, in two identity partitions.
- **Begin is atomic.** A writer writes its data file, reads the table and sends attempt 1 in one step. The read and
  the swap are separate steps, which is where the races are.
- **Retries are atomic too.** A retry's refresh, validation and new attempt are one step, and backoff is not modelled.
- **Unreachable rejections.** A catalog that rejects on its own requirement checks (as opposed to a lost CAS) cannot
  occur in this configuration.
- **Resend and the conformance driver.** `Resend` stands for the REST server's deduplication, which `InMemoryCatalog`
  does not have, so the conformance driver treats a keyed resend as the invisible no-op the key makes it.

### Conformance against the real catalog

`Conformance.fs` runs the real thing:

- An `InMemoryCatalog` subclass holds every writer's pointer swap until the driver plays the model's next action:
  - `Apply` runs the catalog's own CAS;
  - `Ack` / `Reject` hand the writer its result;
  - `ServerError` and `Timeout` hand it the exceptions the production code sees.
- Each writer is a real `SnapshotProducer` commit on its own task.

After every step the driver compares the following with the model:

- the catalog pointer;
- which metadata files exist;
- each writer's outcome (waiting on attempt k, succeeded, failed or unknown);
- the snapshot history;
- the live data files from a real scan plan.

FsCheck generates and shrinks the runs. A further test checks that the comparison has teeth: the real system departs
from each buggy model.

## Prior art, and what this adds

- **Jack Vanlightly's [table-formats-tlaplus](https://github.com/Vanlightly/table-formats-tlaplus)** has the closest
  model, of Iceberg in FizzBee. It has a writer state machine, a catalog that is a CAS, and an object store. A lost
  CAS deletes the writer's metadata files, reloads and re-runs the Java conflict validations against the starting
  snapshot. This model departs from it in four ways:
  - **Unknown outcomes.** There, the catalog only ever answers OK or `COMMIT_FAILED`. Here, the request and its reply
    are separate steps, and a reply can be lost (timeout), be an error (server error) or be resent. That is what
    `DeleteOnUnknown` and `RetryUnknownBlindly` need, and Vanlightly's cleanup-on-failure would be exactly the
    `DeleteOnUnknown` bug if the failure were ambiguous.
  - **Idempotency keys.** There are none there; here, `Resend` and `NoIdempotencyKey` cover them.
  - **Scope.** Here: partitions, and serializable overwrite checked as a history invariant. There: copy-on-write and
    merge-on-read deletes, compaction, row-level consistent reads and snapshot isolation, which this model leaves out.
  - **Tooling.** This model is checked exhaustively by TLC at a small size, rather than by simulation.
- **CockroachDB's [ParallelCommits](https://github.com/cockroachdb/cockroach/tree/HEAD/docs/tla-plus/ParallelCommits)**
  is the one public spec found that models an ambiguous commit outcome. Its committer reads durable state before
  concluding, and its `AckImpliesCommit` matches `NoLostCommit` here. The "look for your snapshot after a server
  error" rule is the same idea.
- **Elastic's [ReplicaEngine](https://github.com/elastic/elasticsearch-formal-models)** injects duplicate and retried
  requests on purpose and checks that the result is unchanged. `Resend` does the same for idempotency keys.
- **tlaplus/Examples' [KeyValueStore](https://github.com/tlaplus/Examples/tree/HEAD/specifications/KeyValueStore)**
  has first-committer-wins conflict checks and isolation judged over a recorded history.
  `SerializableOverwrite` is stated the same way, over the snapshot history rather than over the protocol's internals.
- **[Quint](https://github.com/informalsystems/quint)** and **PlusCal** are typed or programmer-friendly surfaces over
  TLA+. FsTla is in the same spirit, but its surface is an ordinary F# module that also runs, so the model is also
  executable .NET code.
- **The AWS practice of TLA+ plus conformance testing**, and **[cedar-spec](https://github.com/cedar-policy/cedar-spec)**'s
  differential testing of a verified Lean model against the Rust implementation, motivate the conformance half. Here
  the checked model and the oracle are the same F# functions, so nothing is maintained twice.
- **F\*** and the various F#-to-Z3 or F#-to-TLA+ experiments verify or translate F# code. This project does not prove
  F# programs correct. It uses F# as the authoring language for a TLA+ model and leaves the checking to TLC.

What is new here is the combination: one .NET model that TLC checks natively, whose counterexamples are F# values, and
which drives the production code as a test oracle.

## Running

```sh
dotnet build formal/Formal.slnx
dotnet test formal/Formal.slnx
```

- TLC tests skip unless `TLA2TOOLS_JAR` names a `tla2tools.jar` (they need Java 11+). With `FORMAL_REQUIRE_TLC=1` they
  fail instead of skipping, which is how CI runs them. CI installs Temurin 21 and a checksum-pinned TLC v1.7.4.
- After a deliberate model change, `FORMAL_UPDATE_GOLDEN=1 dotnet test formal/Formal.slnx` rewrites `lake/tla`.
  Review the TLA+ diff with the change.

## Traces (phase two)

`traces/` will hold trace grammars that read recorded runs and OpenTelemetry spans of real commits as sequences of
the same `Action` type, and check them against the model.

## Open

- **Apalache**, or another SMT backend, for inductive invariants and larger parameters than TLC's explicit-state
  search reaches.
- **More catalogs under conformance.** The REST client against the REST server (with real idempotency-key
  deduplication, which would make `Resend` observable), Glue, and JDBC.
- **More protocol.** Snapshot expiry and orphan cleanup racing in-flight commits, multi-table transactions, and
  branches.
- **Liveness.** For example, every writer eventually finishes when faults are bounded.
