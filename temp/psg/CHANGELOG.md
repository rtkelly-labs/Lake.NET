# Changelog

All notable changes to **Parquet.SourceGenerator** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

Changes since `0.0.3`, the last tagged release; this section becomes the next release entry when one is cut.

### Migrating from 0.0.x to 0.1

The first release after `0.0.3` breaks the generated API on purpose (pre-1.0, no shims). This table is
the migration list; each row's detail is in the entries below and in `docs/api/LEDGER.md`. Rows marked
**pending** are in flight and will be finalised when they land; they are placeholders, not promises.

| 0.0.x | 0.1 | Source |
|:--|:--|:--|
| `ReadParquetAsync(Stream)` / `(ReadOnlyMemory<byte>)` (returned a `List<T>`) | `<Model>Parquet.From(source).ToArrayAsync()` returns `T[]`; call `.ToList()` for a `List<T>` | #480, #489 |
| `ReadParquetArrayAsync`, `ReadParquetStreamAsync` | `From(source).ToArrayAsync()`, `From(source).AsAsyncEnumerable()` | #480, #489 |
| `ReadParquetParallelAsync`, `ReadParquetParallelArrayAsync` | `From(buffer).WithOptions(...).Parallel().ToArrayAsync()` (buffer source only) | #480, #489 |
| `ReadParquetBatchesAsync` / `Batches()` | `From(source).AsBatches()` | #508, #576 |
| `ToListAsync()` | `ToArrayAsync()` then `.ToList()` | #479 |
| `<Model>ParquetStreamSource`, `...MemorySource`, `...FilteredSource`, `...ParallelSource` | one `<Model>ParquetReader` returned by both `From` overloads | #478 |
| `<Model>ColumnarBatch` (write) and `ColumnBatch` (read, `x.AmountSpan`) | one `<Model>Batch` (`x.Amount.Span`; nullable columns are packed values plus definition levels) | #508, #576 |
| row-group writers and columnar helpers, `RowGroupMetadata` constructor | internal | #481, #459 |
| `Parquet.SourceGenerator.V5` (package id) | `Parquet.SourceGenerator.Legacy`; the classic generator keeps its flat reads | #494 |
| `ReadParquetBy<Key>Async`, `ReadParquet<Key>RangeAsync`, `ParquetPruneStatistics` | `<Model>Parquet.From(stream).Where(m => m.<Key>.MayContain(...))` and `MayContainBetween(...)`; `ParquetPruneStatistics` internalized | #584 |
| `ParquetColumnStatistics<T>` constructor and `FromRaw` | Hidden with `[EditorBrowsable(Never)]` | #586 |
| `<Model>ParquetExtensions.Schema` | Internalized; backend-specific `ParquetSchema` hidden from consumer surface | #585 |
| `WriteParquetBatchedAsync` | Folded into `WriteParquetAsync(stream, options)` (#512) | #512 |
| `ParquetGeneratorFeatureLevel` and `ParquetGeneratorOptionsAttribute` | Removed; use `<ParquetGeneratorFlatOnly>true</ParquetGeneratorFlatOnly>` in MSBuild if flat-only behavior is required | #587 |
| legacy backend parity (nested, Arrow, batches) and its migration table | **pending** (#604) | |
| baseline: `PublicAPI.Unshipped.txt` promoted to `Shipped.txt` | 101 public package signatures across all 13 public types shipped for 0.1 | #592 |

### Added
- **`[ParquetColumn(FieldId = n)]` writes a Parquet field ID** on the column's schema element (Parquet.Net 6
  backend; the Legacy package reports `PARQ023`, since Parquet.Net 4.x/5.x has no field IDs). Apache Iceberg and other
  readers that resolve columns by ID can then adopt generated files without a name mapping. Negative, the
  default, writes none, so existing models are unchanged. An ID declared by two columns of one schema, nested
  leaves included, is an error (`PARQ024`).
- **`[ParquetTimestamp(Microseconds, AdjustedToUtc = false)]` writes a local timestamp**:
  `TIMESTAMP(MICROS, isAdjustedToUTC=false)`, Iceberg's `timestamp` as opposed to `timestamptz`. It is emitted
  through `DateTimeFormat.Timestamp`, because Parquet.Net ignores `isAdjustedToUTC` for `DateAndTimeMicros`
  and writes that as the always-UTC `TIMESTAMP_MICROS` converted type. The default (`true`) keeps today's output. The Legacy package reports `PARQ023` for `false`:
  Parquet.Net before 4.25 has no `DateTimeFormat.Timestamp`.
- **`ParquetSerializerOptions.MaxAllocationBytes`, a byte budget for what a read allocates** (#361), default
  256 MiB per row group. `MaxAllocationValues` bounds a count, so a file of about 1 KB declaring ten
  million rows reserved roughly a gigabyte of pooled buffers before a page was read. The generated
  readers (modern and classic) now multiply a row group's declared rows by the model's per-row buffer
  size and throw `InvalidDataException` naming the option before renting; list columns are added to the row group's running total as their chunks are read. Raise it for files with genuinely larger row groups. In a parallel read it applies per worker.
- **Diagnostics for abstract, `ref struct` and file-local targets (PARQ020, PARQ021, PARQ022).** Each shape passed every
  declaration check and then failed to compile inside the generated file (`CS0144`, `CS0306`/`CS8345`, `CS9051`). Each
  now reports one diagnostic at the declaration and emits nothing for the type; other targets are unaffected (#402).
- **CI gate: every `src/` analyzer runs on the emitted code, and any finding fails** (`analysis/`,
  `scripts/GeneratedCodeAnalysis.cs`, `docs/50-GENERATED-CODE-ANALYSIS.md`, #553). The golden
  models' emitted source is compiled bare (no implicit usings) against both backends with
  NetAnalyzers, Meziantou, Roslynator, the metric gates and the trim/AOT analyzers, with
  generated-code classification switched off. The baseline is empty. The `generated-analysis` job
  names each rule and count on failure, and a seeded-violation positive control keeps the gate from
  passing without examining the emitted files.
- **`stack` workflow for GitHub native stacked pull requests.** Labelling the bottom pull request of
  a chain `stack:link` (or dispatching the workflow with explicit numbers) walks the open pull
  requests upward, requires the chain to be linear, and runs `gh stack link` over it. Merge a linked
  stack with `gh stack merge <top> --squash --yes`; `gh pr merge` refuses stacked pull requests.
- **Code metrics baselines and a complexity ratchet** (`metrics/*.metrics.txt`,
  `docs/21-CODE-METRICS.md`). Roslyn's maintainability index, cyclomatic complexity, class
  coupling, inheritance depth and line counts are now recorded per namespace, type and member for
  the hand-written code under `src/`, checked in as a deterministic ordinal text artifact, and
  gated in CI on **drift** rather than on absolute values — the `*.api.txt` pattern applied to
  quality. `CA1502` / `CA1505` / `CA1506` are enabled for `src/` with thresholds pinned in
  `CodeMetricsConfig.txt`. Refresh with `UPDATE_GOLDEN_FILES=true dotnet run scripts/CodeMetrics.cs`
  or `/update-golden`. The recorded evidence: `CodeEmitter` is 2,536 source lines at cyclomatic
  complexity 144, and `TargetParser.CollectMembers` carries a cyclomatic complexity of **105** in
  one 463-line method — fifteen times the recommended maximum, and the worst maintainability index
  in the repository at 14.
- **Generated-code metrics** (`test/Parquet.SourceGenerator.Tests/GoldenFiles/*.metrics.txt`,
  `docs/22-GENERATED-CODE-METRICS.md`). The same computation turned on the code the generator
  *emits*: one baseline beside every `*.api.txt`, covering both emitters, refreshed by the same
  `UPDATE_GOLDEN_FILES=true` / `/update-golden` command as the golden files themselves, and gated
  on drift. Each carries a size-per-capability ratio — emitted executable lines per emitted public
  member — so growth in emitted volume can be argued against growth in emitted API. The
  Maintainability Index is **reported but not gated** for emitted code: it correlates with emitted
  method length at r = −0.95 and has already bottomed out at 0 on the worst method, so it detects
  nothing that `SLOC` does not. Measured: flat models cost ~10 executable lines per emitted member,
  row-level lists 33, and `ListOrderParquetExtensions.WriteParquetRowGroupAsync` is a single
  1,148-line method at cyclomatic complexity **97** — within 8% of the worst hand-written method in
  the repository. Compiling each golden file in order to measure it also found that the emitted
  writer does not compile for a nullable value-type compound member (#255).
- **Duplication measurement with a drift gate** (`metrics/duplication.txt`,
  `docs/23-DUPLICATION.md`, layer 3 of #251). Token-level clones across the hand-written sources —
  identifier-blind, literal-sensitive, 16-token windows extended along diagonal alignments, spans
  of 40+ tokens reported per method pair — checked in with the `*.api.txt` grammar and gated on
  drift in CI. Emitted code is excluded by design (repetition in generated output is the design,
  not a defect). The detector was calibrated against the repo's demonstrated failure before
  adoption: it names the historical `ResolveSchemaField` copies and all three read paths that
  broke on #196. Current state, reported as the number the refactor case rests on: **93 clusters,
  12,034 duplicated tokens**, worst pair `EmitReadArrayAsync` ↔ `EmitReadAsync` at 180 tokens.
- **Metrics oracle — `Metrics.exe` cross-checks the bespoke computation**
  (`.github/workflows/metrics-oracle.yml`, `scripts/MetricsOracleCompare.cs`,
  `docs/24-METRICS-ORACLE.md`, #254). A nightly `windows-latest` job runs Microsoft's own
  (Windows-only) metrics tool over both generator projects and compares it, type by type,
  against the layer-1 baselines: MI within ±2, CC/CL/SLOC exact. Assembly totals are
  reported but not gated — enumeration scope differs between the tools, and a tolerance
  there would paper over real drift. Any disagreement fails the job and opens or updates a
  `metrics-oracle` issue, because a red schedule gets muted and an issue gets acted on.
  The check `CodeMetrics.cs` could never run on itself.
- **Deterministic call-graph artifact with cycle, fan-out and layering gates**
  (`scripts/CallGraph.cs`, `graph/*.callgraph.txt`, `graph/callgraph.allowlist.txt`,
  `docs/callgraph.md`, `docs/callgraph-generated.md`, `docs/25-CALL-GRAPH.md`, #252).
  This repo's defects have been graph defects — #252 makes the graph an artifact: static
  edges per method as an ordinal baseline gated on drift, no multi-node cycle without a
  catalogued reason (today's entire cycle inventory is the #176 compound parser and the
  definition-ladder emitter — tree recursion, `SELF`/`SCC` lines with the argument written
  down), a fan-out ratchet (28 today), and a layering rule that would have caught the
  `ResolveSchemaField` divergence as it happened: components must not call back into the
  emitter hub. Measured shape: 211 nodes / 356 edges / depth 6. The honesty clause is part
  of the artifact: delegates and virtual dispatch are invisible to it, and the unresolved
  call-site count rides in the baseline header.
- **Mutation testing for the behavioural suite** (`stryker-config.json`,
  `.github/workflows/mutation.yml`, `scripts/MutationSummary.cs`,
  `docs/26-MUTATION-TESTING.md`, layer 4 of #251). Stryker.NET runs nightly and answers the
  question coverage cannot: would a test notice if a line were *wrong*? The defining decision
  is what it leaves out — the golden-file, API-baseline, metrics-baseline and IL-shape suites
  are excluded, because they assert the *text/shape* of emitted output and would "kill" every
  mutation trivially, reporting a flattering ~100% that proves nothing about semantic coverage.
  Round-trip, property-based (#198), diagnostic and compile-check tests stay in. It reports
  through a single PR refreshed nightly (the #189 lesson: a red schedule gets muted, a
  reviewable PR gets acted on), and the score is recomputed with an explicit denominator.
  **No threshold is set** — the first night establishes the baseline; a floor picked before the
  number is known is vacuous or permanently red.

### Changed
- **Behavioral test coverage:** remove attribute plumbing and redundant emitter-text checks;
  compare every primitive, nullable and large-fixture value across both PyArrow format settings
  and the C# producer; exercise column-contract edits through a reused Roslyn driver. The Windows
  net472 package consumer and both cross-runtime handoffs now participate in required CI.
- **Removed the SonarAnalyzer.CSharp analyzer (source-available license).** Its license grants use only for a
  non-competitive purpose, which excludes AI tooling that ingests or interprets the analyzer's output,
  and this repository has AI agents read and fix diagnostics. The remaining analyzers stay
  (NetAnalyzers, Meziantou, Roslynator); the Sonar checks that had a counterpart are now enforced by
  those, and `docs/55-SONAR-REMOVAL.md` maps every rule. No shipped package ever contained it.
- **BREAKING: the emitted `{T}ColumnarBatch` is a `readonly struct` with get-only properties and a
  validating constructor (#550, part of #508 and #554).** It no longer exposes mutable public fields,
  so `RowCount` cannot drift from the column lanes. The constructor takes `rowCount` and one
  `ReadOnlyMemory<…>` per column in schema order (a definition-levels lane follows each nullable
  value column) and throws `ArgumentOutOfRangeException` or `ArgumentException` naming the column
  when a lane is shorter than `rowCount`. A `default` batch is an empty batch and writes no row
  group. The column layout is unchanged. Migration: replace
  `new FooColumnarBatch { RowCount = n, Id = ids, … }` with
  `new FooColumnarBatch(rowCount: n, id: ids, …)`, and build a modified batch by constructing a new
  one rather than assigning a field. Unifying it with the read-side `ColumnBatch` is still open in #508.
  The Arrow `RecordBatch` bridge no longer builds this struct internally.
- **Derived outputs are generated in CI, not checked in.** The golden files
  (`GoldenFiles/*.g.cs`, `*.api.txt`, `*.api.shape.txt`, `*.metrics.txt`), the `src/` metrics and
  duplication baselines (`metrics/`), the call-graph edge baselines (`graph/*.callgraph.txt`) and
  `docs/callgraph*.md` are removed. They were all a pure function of the code, and keeping them
  meant a refresh commit (or `/update-golden`) on most pull requests. A new `derived` CI job runs
  `scripts/DerivedOutputs.cs` for the pull request and compares it with its merge base's outputs,
  which each push to main publishes as a `derived-baseline-<sha>` artifact (regenerated in the job
  when missing). It uploads both trees as the `derived-outputs` artifact and posts the difference
  as one sticky PR comment (`scripts/DerivedReport/`): a deterministic summary of the whole PR —
  files by area, API catalogues, tests, CI and tooling touched, then what drifted in the derived
  outputs — linking to the full diff and the head's state as self-contained HTML pages rendered
  with Razor components and the official `HtmlRenderer`. The comment is updated on every run, and
  says so when the head failed or the base was unavailable instead of leaving a stale diff; a base
  that cannot be produced never fails the check. The required `build` check is now an aggregate of
  the `test` job (the former `build`) and `derived`, so both block a merge. Each full release attaches its derived outputs as
  `derived-outputs.tar.gz` and dispatches the docs site with the tag, so the API grid renders what
  that version shipped. The golden models now live in
  `GoldenCorpus`; `GoldenCodeGenRegressionTests` checks their invariants and publishes them to
  `artifacts/golden/`. The remaining gates are unchanged in intent: golden models must parse and
  compile, emitted code must compile against `GoldenModels/` (ERRORS=0), `CodeMetricsConfig.txt`
  must be valid, the call graph must satisfy its cycle, fan-out and layering rules, and
  `CA1502`/`CA1505`/`CA1506` still gate `src/`. **PARQAPI001 is retired**: the emitted consumer API
  is no longer a catalogue gated by a `docs/api/LEDGER.md` entry, and is reviewed from the PR
  comment instead; `PARQAPI002`, `RS0016` and the ledger rule for `src/api/seams.txt` and
  `PublicAPI.Unshipped.txt` are unchanged. The `/update-golden` workflow is removed. The nightly
  metrics oracle compares `Metrics.exe` against `CodeMetrics.cs --src-only` run on the same commit.
- **BREAKING: generated implementation plumbing is no longer public (#459, #481, part of #477).**
  The `{T}RowGroupMetadata` constructor (whose parameters were emitter slot indices such as
  `column_0, column_2`) is now `internal`; the struct and its properties stay public for pruning
  predicates. The `ParquetWriter`-taking row-group writers `WriteParquetRowGroupAsync(writer, chunk)`,
  `WriteParquetRowGroupAsync(writer, {T}ColumnarBatch)`, the positional
  `WriteParquetRowGroupColumnarAsync(writer, rowCount, …)`, the `AsColumnarText` /
  `AsColumnarBinary` helpers, and the classic backend's `WriteRowGroupAsync(writer, items)` are
  `internal` too. Use `items.WriteParquetAsync(stream)`, `asyncItems.WriteParquetAsync(stream)`,
  `items.WriteParquetBatchedAsync(stream)` or `batch.WriteParquetAsync(stream)` instead. Code in
  the model's own assembly can still call them. The Arrow `WriteParquetRowGroupAsync(writer,
  RecordBatch)` bridge stays public. Emitted public surface across the seven golden models drops
  from 384 to 362 members and from 559 to 462 parameters.
- **`CHANGELOG.md` is now the release authority (#248).** `scripts/ParseChangelog.cs` validates the
  changelog structure in CI and, in `--release` mode, is the only source of the release version and
  notes. `release.yml` lost its `version` input: a `prepare` job reads the first cut
  `## [x.y.z] - YYYY-MM-DD` section, refuses an already-taken `v<version>` tag, and hands the
  version to a `build` job (full verification battery) and a `publish` job (NuGet push plus a
  GitHub release whose body is the changelog section). A version that is not described in the
  changelog cannot be published.
- **`TargetParser.CollectMembers` decomposed (#263).** The generator's front door — every
  `[ParquetSerializable]` type in every consumer's compilation passes through it — measured
  cyclomatic complexity **105** across 463 lines with 11 parameters. It is now a `MemberScope` /
  `MemberSink` pipeline of small methods, one per attribute family, rule, and model shape:
  `CollectMember` itself is CC 15 and the largest fragment 21, `GetTargetModelCore` (CC 38, the
  other grandfathered method) is 16, and no method in the repository exceeds the pre-existing
  worst of 25. Both `[SuppressMessage]` grandfather clauses are deleted, the class-coupling
  method gate has eight points of headroom where it had zero, and the maintainability floor moved
  14 → 27. Behaviour-preserving by construction: every golden file and emitted-API baseline is
  byte-identical.

### Removed
- **BREAKING: one `<Model>Batch` for columnar read and write, and `Batches()` becomes `AsBatches()`
  (#508, #507; tracker #505).** The write-side `<Model>ColumnarBatch` and the read-side nested
  `<Model>ParquetExtensions.ColumnBatch` are replaced by one top-level `readonly struct
  <Model>Batch` with get-only properties and a validating constructor. `<Model>ParquetReader.AsBatches(ct)`
  returns `IAsyncEnumerable<<Model>Batch>`, and the columnar `<Model>Batch.WriteParquetAsync(stream, options, ct)`
  takes the same type, so `await foreach (var batch in reader.AsBatches()) await batch.WriteParquetAsync(output);`
  round-trips with no conversion. Layout (decided in #508, measured in #561): a nullable value
  column is a packed lane of its non-null values (`ReadOnlyMemory<T>`) plus a definition-level lane
  (`ReadOnlyMemory<int>`, one per row), the shape the writer consumes; string and byte-array
  columns stay inline-nullable `ReadOnlyMemory<ReadOnlyMemory<char>?>` / `ReadOnlyMemory<ReadOnlyMemory<byte>?>`.
  The read path decodes nullable value columns straight into that layout (`ReadRawAsync`), with no
  expansion pass.

  Ownership (#369, closed by this change; owner decision, stable for 0.1): a batch from `AsBatches()` is
  borrowed, valid until the next `MoveNextAsync` or disposal. It carries a lease the iterator expires
  before returning the pooled buffers, so a kept batch throws `ObjectDisposedException` from every lane
  property (and from the fill methods and `WriteParquetAsync`) instead of reading recycled memory. A
  batch you construct yourself never expires. Using a batch, or a lane taken from it, after the
  enumerator advances is invalid. A `ReadOnlyMemory<T>` lane already copied out of a live batch is not
  checked yet (lane-level checking: #580), the lanes of a borrowed batch are not guaranteed to be
  array-backed (do not rely on `MemoryMarshal.TryGetArray` or `Pin` outliving the batch), and batches
  are not thread-safe. To keep data, copy each lane with `.ToArray()` into the public constructor.
  There is no owned, callback or ref-counted variant in 0.1.

  | Before | After |
  |:---|:---|
  | `XParquet.From(s).Batches()` | `XParquet.From(s).AsBatches()` |
  | `XParquetExtensions.ColumnBatch` (read item type) | `XBatch` |
  | `XColumnarBatch` (write input type) | `XBatch` (same constructor and lanes) |
  | `batch.AmountSpan` (`ReadOnlySpan<T>`) | `batch.Amount.Span` (`ReadOnlyMemory<T>` lane) |
  | `batch.DiscountSpan` (`ReadOnlySpan<double?>` on a nullable column) | `batch.Discount` + `batch.DiscountDefinitionLevels` (packed + levels), or `batch.FillDiscountNullable(Span<double?>)` into a buffer you own |
  | `batch.RegionSpan` (`ReadOnlySpan<string>`) | `batch.Region.Span` (`ReadOnlyMemory<char>` per row, `?` for nullable columns; `.ToString()` to materialise) |
  | `batch.RowGroupIndex` | removed: count the batches |
  | a model's `RowCount` column next to the batch row count | unchanged: the row count is `BatchRowCount` when a column is named `RowCount` |

  `Fill<Column>Nullable` is an explicit, opt-in convenience (O(rows), caller-supplied buffer) and
  not a cached `T?` property: the cached form allocated row-count-sized arrays per batch and was 2x
  slower under Server GC in the #561 experiment. Rationale and the layout measurements:
  `docs/12-BUFFER-REUSE-AND-EXTRACTION-STRATEGIES.md` section 7.
- **BREAKING: the four public read state types collapse into one reader, and `ToListAsync` is
  gone (#478, #479).** `<Model>Parquet.From(Stream)` and `From(ReadOnlyMemory<byte>)` now both
  return a single `readonly struct <Model>ParquetReader`; `WithOptions`, `Where` and `Parallel`
  return the same type with updated private state (source kind, options, predicate, parallel flag),
  so composing a read still allocates nothing. `ToArrayAsync(ct)` and `AsAsyncEnumerable(ct)` are
  the stable terminals; `Batches(ct)` is unchanged in shape (its ownership model is #369). The
  internal `List<T>`-only read paths (`ReadListCoreAsync`, `ReadParallelListCoreAsync`) are no
  longer emitted, since nothing reaches them. Measured on the golden models: `OrderEvent` 65 → 48
  members and 48 → 34 parameters; `ListOrder` / `NestedOrder` / `PocoOrder` 35 → 19 and 31 → 18;
  `SortedShipment` 58 → 41 and 64 → 50. The legacy package is unchanged.

  Combinations the separate types made unrepresentable now throw `NotSupportedException` from the
  call that completes them (never silently degraded): `From(stream).Parallel()` (the message names
  `From(ReadOnlyMemory<byte>)` as the parallel source), `.Where(...).Parallel()` and
  `.Parallel().Where(...)`, a second `.Where(...)`, `.Parallel().AsAsyncEnumerable()`,
  `.Parallel().Batches()` and `.Where(...).Batches()`. `WithOptions(null)` and `Where(null)` still
  throw `ArgumentNullException`. Rationale: `docs/47-0.1-CONTRACT-AND-DESIGN-GOALS.md` §4.2.

  | Before | After |
  |:---|:---|
  | `XParquetStreamSource` (from `XParquet.From(stream)`) | `XParquetReader` |
  | `XParquetMemorySource` (from `XParquet.From(bytes)`) | `XParquetReader` |
  | `XParquetFilteredSource` (from `.Where(p)`) | `XParquetReader` |
  | `XParquetParallelSource` (from `.Parallel()`) | `XParquetReader` |
  | `….ToListAsync(ct)` | `….ToArrayAsync(ct)`; where a `List<T>` is genuinely needed, `(await ….ToArrayAsync(ct)).ToList()` or `new List<T>(await ….ToArrayAsync(ct))` |
  | `….Parallel().ToListAsync(ct)` | `….Parallel().ToArrayAsync(ct)` |

  Code that only chains calls needs no change other than replacing `ToListAsync`; code that named
  a state type in a variable, field or parameter declaration renames it to `XParquetReader`.
- **BREAKING: the flat `ReadParquet*Async` read methods are gone from the modern generator
  (#480).** `<Model>Parquet.From(...)` is now the only generated read surface of
  `Parquet.SourceGenerator`. Removed, in both the `Stream` and the `ReadOnlyMemory<byte>` overload:
  `ReadParquetAsync`, `ReadParquetArrayAsync`, `ReadParquetStreamAsync`, `ReadParquetBatchesAsync`,
  `ReadParquetParallelAsync` and `ReadParquetParallelArrayAsync`, plus the `predicate` parameter four
  of them carried. There is no `[Obsolete]` release first (`0.0.x` has no published consumers to
  warn). Measured on the golden models: **66 fewer emitted members and 222 fewer parameter slots**
  (e.g. `OrderEventParquetExtensions` 82 → 70 members, 104 → 64 parameters). The
  `Parquet.SourceGenerator.Legacy` package has no builder and **keeps** its flat `ReadParquetAsync`
  and `ReadParquetArrayAsync`. Decision record: `docs/48-FLAT-READ-REMOVAL-480.md`, superseding
  `docs/41`. Every call maps 1:1 onto a builder chain:

  | Before (`XParquetExtensions.…`) | After |
  |:---|:---|
  | `ReadParquetAsync(source)` | `XParquet.From(source).ToArrayAsync()` (then `.ToList()` where a `List<T>` is needed; `ToListAsync` went with #479) |
  | `ReadParquetArrayAsync(source)` | `XParquet.From(source).ToArrayAsync()` |
  | `ReadParquetStreamAsync(source)` | `XParquet.From(source).AsAsyncEnumerable()` |
  | `ReadParquetBatchesAsync(source)` | `XParquet.From(source).Batches()` |
  | `ReadParquetParallelAsync(bytes)` | `XParquet.From(bytes).Parallel().ToArrayAsync()` |
  | `ReadParquetParallelArrayAsync(bytes)` | `XParquet.From(bytes).Parallel().ToArrayAsync()` |
  | `ReadParquetParallelAsync(stream)` / `…ArrayAsync(stream)` | `XParquet.From(stream).ToArrayAsync()` — the stream overloads always read sequentially; buffer the file and use `From(bytes).Parallel()` for real parallelism |
  | `…, options, …` | `.WithOptions(options)` before the terminal (omit it for `null`; `WithOptions(null)` throws) |
  | `…, predicate: p` | `.Where(p)` before the terminal |
  | `…, cancellationToken: ct` | the terminal's argument, e.g. `.ToArrayAsync(ct)` |
- **Implementation types are no longer public API (#461, part of the 0.1 contract #477).**
  `NullableColumnExtractor` and `VectorizedColumnTransforms` in `Parquet.SourceGenerator.Attributes`
  are now `internal`: no generated code calls either, so they were shipped API with no consumer
  use case (24 `PublicAPI.Unshipped.txt` lines removed). Every type in the analyzer-only
  `Parquet.SourceGenerator` and `Parquet.SourceGenerator.Legacy` assemblies — `CodeEmitter`,
  `TargetParser`, `TargetClassModel`, `PropertyModel`, `EquatableArray<T>`, `DiagnosticInfo`,
  the `[Generator]` entry points and the rest — is now `internal`. Nothing can reference those
  assemblies, so their 171 catalogued signatures were governing a surface no consumer could reach;
  the generator's `PublicAPI.*.txt` files and its `PublicApiAnalyzers` reference are deleted, as
  are the binary-compat constructor overloads on `TargetClassModel` and `PropertyModel`. Breaking
  only for code that referenced the Attributes helpers directly.

### Fixed
- **`DateOnly` is written as INT32 `DATE` again**, not INT96. INT96 round-tripped in .NET, but other readers
  (pyarrow, Spark) saw a timestamp, and Apache Iceberg rejects INT96.
- **Arrow scope is stated plainly** (#590). The READMEs, `docs/14`, `docs/19` and `docs/47` say that only Arrow
  ingestion (`RecordBatch` to Parquet, when Apache.Arrow is referenced) ships; export to Arrow does not, and
  `docs/19` no longer lists the closed #178 as open work.
- **A base class moved into another project keeps contributing its columns.** The base-type walk stopped at any base
  with no declaring syntax, which is every base read from metadata, so a shared base moved into a contracts project
  silently dropped its inherited columns (same source, different schema, no diagnostic). A metadata base now
  contributes when it is Parquet-aware (`[ParquetSerializable]` or a member with a Parquet attribute); framework bases
  such as `Exception` still contribute nothing (#420).
- **Hostile row counts and times fail with `InvalidDataException` on every read path** (#371). A row group count
  above `int.MaxValue` reached a checked cast first on the streaming, batch and sorted-range reads and
  surfaced as `OverflowException`; a `TIME_MICROS` value outside one day surfaced as
  `ArgumentOutOfRangeException`, or, when the multiply wrapped, as a wrong time of day. The row count sum behind reads with no statistics to prune on had the same unchecked narrowing and is now checked too. All are
  range-checked first and rejected with `InvalidDataException`. Unvalidated enum widening is left
  as is (see the issue).
- **The blittable zero-copy fast path requires the single field to back the single serialized member.** A one-field
  struct whose property computed its value from a differently-meaning field (a unit-converting wrapper) took the
  `MemoryMarshal.Cast` path for `List<T>` and `T[]` and wrote the field's bytes, while `IEnumerable<T>` wrote the
  property value. Eligibility now needs the field to be the member itself or its auto-property backing field, with
  the same type; anything else uses the per-element loop (#389).
- **A user type shadowing a framework collection name with another arity no longer crashes the generator.** Compound
  classification matched `System.Collections.Generic.List` and `Dictionary` by display name alone, so a `Dictionary<T>`
  declared into that namespace classified as a map and the parser indexed past its type arguments (`CS8785`,
  losing every generated type). Arity is now part of the match and the member is rejected with PARQ006 (#417).
- **The columnar writer bounds a nullable column's packed lane by its definition levels** (#381). The batch
  constructor checks lane shapes in O(1) and never counted the packed lane, so a lane shorter than the
  rows marked present, or a level other than 0 or 1, reached Parquet.Net unchecked and was written as a
  corrupt column chunk with no error. The writer now counts the present rows in the levels it is about
  to write (it passes over them anyway), slices a longer lane to that count and rejects a shorter lane or
  a bad level with an `InvalidOperationException` naming the column.
- **A column chunk shorter than its row group is rejected instead of being read into stale pool data** (#382).
  The read buffers are rented for the footer's `num_rows` and Parquet.Net fills only the chunk's own
  `num_values`, so a file declaring more rows than a chunk holds left the buffer tail unwritten, and
  pooled value-type buffers are returned uncleared. The emitted reader now compares each unrepeated
  column chunk's `num_values` with the row count and throws `InvalidDataException` naming the column.
- **The sorted range reader enforces `MaxAllocationValues` across row groups** (#387). It capped each row group
  but not their sum, the one read path that did not. The emitted reader now accumulates the rows it scans
  and throws `InvalidDataException` when the total exceeds the cap, as the other paths do.
- **The nesting-depth limit is enforced during the schema walk, not after it** (#366). The emitted validation
  computed a schema's full depth recursively and only then compared it with `MaxNestingDepth`, so the
  walk was bounded by the stack, and a deep enough footer ended the process with a stack overflow (verified
  at 300,000 nested lists) before the limit could fire. The walk now carries the depth and throws
  `InvalidDataException` as soon as it passes the limit.
- **The eager buffer return now has a test that pins the Parquet.Net behaviour it depends on** (#399). The
  emitted writers return each column's pooled buffer as soon as its `WriteAsync` completes, which is only
  safe if Parquet.Net has finished with the memory by then. `WriteCompletionContractTests` asserts that
  (chunk bytes on the stream at completion, source overwritten afterwards leaves the file intact, and a
  round trip under concurrent pool poisoning), and `docs/12` records the assumption and the fallback.
- **A property or type named after a C# keyword no longer breaks the generated code** (#376). Roslyn drops
  the `@` from a member declared `@event`, so the model held the bare keyword and the emitters wrote
  `item.event`, `{ event = ... }` and `new class` into generated source (CS1001, CS1525). Every
  emitted use of a model name as a standalone identifier now goes through `EmittedText.Ident`, which
  adds `@` to reserved and contextual keywords, and the batch parameter names use it in place of a
  hand-kept keyword list. Names glued into a longer identifier (`{Name}DefinitionLevels`) stay raw.
- **A diagnostic in a cached model no longer pins its syntax tree.** `DiagnosticInfo` held a Roslyn `Location`, which
  references the `SyntaxTree`, so every model with a diagnostic retained its tree for the driver's lifetime. It now
  keeps path, span and line span as data, rebuilds the `Location` at report time, and holds its message arguments in
  an `EquatableArray` (#398).
- **A list shorter than the count captured at the start of a write throws instead of being read past its end** (#375). The write fast
- **A collection whose `Count` disagrees with its items now throws instead of writing the wrong rows** (#388).
  The enumerable fallback of the row-group writer (anything that is neither a `List<T>` nor an array)
  filled buffers rented for `Count` rows, but rented arrays are longer than asked for, so a collection
  yielding more items than it reported wrote them into the slack and lost them silently, and one
  yielding fewer wrote stale pool data. The loop is now bounded by `count` and checked afterwards, and
  throws `InvalidOperationException` either way.
- **A list that shrinks during a write throws instead of being read past its end** (#375). The write fast
  path walks the list's span with unchecked `Unsafe.Add`, sized by the count read at method entry. If
  the list lost elements in between, the walk passed the end of the span it had taken (into slots of
  removed items, or past the array when the list was cleared). The emitted code now detects a span
  shorter than the captured count once, before the loop, and throws `InvalidOperationException`.
- **A struct-in-list column whose sibling declares a different value count is rejected** (#365). The columns
  under one `List<Struct>` are read by one walk driven by the first column's entry count, which indexed
  every sibling's definition levels. Each column sizes and validates its own `num_values`, so a shorter
  sibling was read past what was written, including stale levels left in its pooled array. The emitted
  reader now compares each sibling's count with the anchor's and throws `InvalidDataException`.
- **A file that omits an optional column under a list now fails with `InvalidDataException`** (#367). Schema
  resolution reported the absent column as missing and returned the generator's own template field, and
  the next statement of a list or struct leaf read that field's chunk metadata, which surfaced as a
  `NullReferenceException`. Nested leaves are outside the missing-column envelope, so the emitted reader
  now rejects the absence by name.
- **The generator pipeline caches value-equatable models, not a syntax context.** The main generator cached
  the `GeneratorSyntaxContext` itself, which retains a `SemanticModel` (and the compilation behind it) per
  target and has no value equality, so nothing downstream could compare as unchanged. It now parses inside
  the syntax transform, as the legacy generator already did (#395).
- **A column name is escaped for wherever the emitter writes it** (#363, #364, #372). A
  `[ParquetColumn]` name reached the `ColumnEncodingHints` string literal, an Arrow `// column`
  comment and the zone-map XML doc comment unescaped, so a quote or newline in it broke the generated
  file or, in the first two, injected statements into it. Every such site now goes through one
  helper (`EmittedText.Literal`, `Comment`, `XmlDoc`). The columnar batch doc comment for a nullable
  text column also wrote an unescaped `<char>`, a CS1570 warning wherever documentation is generated.
- **The compiler diagnostics reference matches the code** (#428, #593). `docs/13` documents `PARQ012`
  to `PARQ014`, lists all four conditions of `PARQ005` and describes `PARQ009` as the private-nested-type
  rule it now is; the `PARQ001` - `PARQ099` range cited in `docs/01` and `docs/03` is gone. Both
  generator projects now carry `AnalyzerReleases.Shipped.md` and `.Unshipped.md`, so a diagnostic id
  added or removed without a release-tracking entry fails the build (RS2000 series).
- **`AsBatches()` now enforces the decompression limits** (#358). The columnar batch reader was the
  one read path that handed the caller's raw stream to `ParquetReader`, so `MaxDecompressedPageSize`
  and `MaxDecompressionExpansionRatio` were silently ignored for it. It now reads through the same
  guarded stream as every other path.
- **A nullable value column next to a property named `<Column>DefinitionLevels` no longer emits a
  duplicate batch member** (#384, part a). Every derived batch member name (row count, definition
  levels, fill method, private plumbing) is now claimed against the model's own property names and
  moved aside on a clash (`ValueDefinitionLevels` becomes `ValueDefinitionLevels_`). Part b (the
  parameter list) was already handled.
- **A second partial part carrying any attribute no longer takes the whole generator down.** The syntax
  provider returned one element per attributed declaration, so a partial type split across files
  produced two identical models, a second `AddSource` with the same hint name (`CS8785`, losing every
  generated serializer) and every diagnostic twice. Both generators now admit only the declaration
  that carries `[ParquetSerializable]`, one element per type (#368).
- **Emitted read and write methods are bounded by column shape, not column count.** The per-column dictionary guard, all-null bypass, list-leaf sizing, list lanes, struct reconstruction, compound extraction and pooled-buffer returns are now shared or per-column `private static` helpers called once per column, clearing CA1502 and CA1505 in the generated output with no public API change (#552, part of #554).
- **Emitted code braces multi-statement blocks under `if`** in the compound list readers, clearing S2681
  in the generated output (#551, part of #554).
- **Emitted code now calls `ConfigureAwait(false)` on every await** and passes its cancellation token to the
  source of a streaming write (`IAsyncEnumerable` `WriteParquetAsync`), clearing CA2007 and MA0004 in the
  generated output (#418, #423, #425).
- **Emitted code clears seven mechanical analyzer findings** (CA1510, S8969, CA1068, MA0011, S4581,
  S1172, S3626; #549, part of #554). Null guards use `ArgumentNullException.ThrowIfNull` (legacy
  output keeps the manual throw behind `#if` for net472), the internal `Read*CoreAsync` helpers take
  `CancellationToken` last, and unused parameters and redundant jumps are gone. No public API
  change.
- **The emitted `DecompressionGuardStream` now honours the `Stream` contract** (#547, part of
  #554). `Flush()` is a no-op instead of throwing, `Dispose(bool)` calls the base and documents that
  the caller owns the inner stream, and `Read(Span<byte>)` / `ReadAsync(Memory<byte>)` pass through
  like the array overloads (gated for net472). Clears CA2215, CA1844, CA1835 and S1186 in emitted code.
- **Emitted async iterators validate arguments at call time, and the modern parallel readers cancel with `CancelAsync`** (#548). `ReadEnumerableCoreAsync` and `ReadBatchesCoreAsync` (stream overloads) are now non-iterator wrappers that throw `ArgumentNullException` on the call, not on the first `MoveNextAsync`, then return a private iterator; the modern emitter's parallel readers `await linkedCts.CancelAsync().ConfigureAwait(false)` instead of a synchronous `Cancel()` (the net472 legacy emitter keeps `Cancel()`, which has no async form there). Clears S4456 (9) and S6966 (12) from the generated-code analysis. No public API change.
- **Arrow bridge validation checks structure, not just the schema.** Before any column is cast or
  sliced, `WriteParquetRowGroupAsync(writer, RecordBatch)` now also rejects: a field name that
  appears more than once (it previously resolved silently to the first occurrence); a schema whose
  declared type disagrees with the array it describes (previously an `InvalidCastException` from
  inside the bridge); an array whose type parameters (decimal precision/scale, time unit,
  fixed-size width) disagree with its schema field, which previously passed and was decoded with
  the schema's parameters, silently shifting every value; a required column whose validity bitmap
  marks nulls its `NullCount` does not declare (the null check trusted `NullCount`, so the undefined
  slots were written as values); and Utf8/Binary offsets that decrease or run past the value buffer
  (previously an `ArgumentOutOfRangeException` while slicing). Each is reported with the other
  validation errors in the one `InvalidDataException`. Backported from Arrow.SourceGenerator.
- **Arrow bridge no longer rounds wide decimals.** `WriteParquetRowGroupAsync(writer, RecordBatch)`
  read `Decimal128` values with Apache.Arrow's `Decimal128Array.GetValue`, which silently rounds a
  value with more significant digits than `System.Decimal` holds — so a 38-digit value in a
  `Decimal128(38, 18)` column (the default mapping) reached the Parquet file rounded, with no error.
  The bridge now decodes the unscaled 128-bit integer itself, strips trailing decimal zeros (so
  `10^19` at scale 18, stored as `10^37`, is still read exactly), accepts the value only when it is
  then exactly representable in `System.Decimal`, and otherwise throws `InvalidDataException` naming
  the column and row. Found while building Arrow.SourceGenerator.
- **Colliding nested targets are diagnosed (PARQ016) instead of failing with CS0101.** Generated
  types are named after the containing-type path with the dots removed, so `A.BC` and `AB.C` (or a
  nested `A.BC` and a top-level `ABC`) both emitted `ABCParquetExtensions`, `ABCRowGroupMetadata`
  and the rest into one namespace, and the build failed on a cascade of CS0101 errors inside
  generated files. Each colliding target now reports PARQ016, naming both types, and emits nothing.
  Found while building Arrow.SourceGenerator.
- **Annotations on an overridden base property are no longer lost.** `[ParquetColumn]`,
  `[ParquetDecimal]`, `[ParquetIgnore]` and the other member attributes are `Inherited = true`,
  but an `override` replaces the base declaration during member collection and Roslyn's
  `GetAttributes()` returns only attributes written on the override itself. An override that did
  not repeat the annotation silently fell back to the defaults: the property's own name as the
  column, `Decimal(38, 18)`, or a column that should have been ignored. Attribute lookup now walks
  the overridden property; an annotation on the override still wins. Found while building
  Arrow.SourceGenerator, which had the same gap.
- **Coexistence of the two row-group pruning mechanisms is now pinned by a behavioural
  test** (`PredicatePushdownAndSortedLookupCoexistOnOneModel`, completes #264's coverage).
  `SortedEvent` carries both the predicate zone-map path and three sort-key binary-search
  paths; the test asserts full-scan, pushdown, and lookup all agree on one file, and that
  co-location does not disable either mechanism. The #281 merge landed the combined golden
  and the convergence-constraint comments but not this test — the correctness floor the
  actual footer-read convergence will need.
- **The decompression guard fails closed on a page header it cannot parse, and reads the header the way Parquet.Net does.** The guard parsed three i32 fields in one fixed order and silently skipped validation for any other legal Thrift encoding (reordered fields, a `crc` field, long-form field ids, a nested struct ahead of the sizes), so a page in that encoding bypassed `MaxDecompressedPageSize` and `MaxDecompressionExpansionRatio`. It now reads the header by field id to the stop byte, refuses a field type Parquet.Net would decode differently, and throws `InvalidDataException` for an unparseable header, an unsupported page type, a missing size or a page extending past the end of the stream. Seeks inside the page it last validated are not treated as new headers (#359).
- **The decompression guard validates a page header reached by reading on from the previous page, not only one reached by a seek.** Parquet.Net 4.x (the legacy backend) seeks once to the start of a column chunk and then reads its pages one after another, so the limits bounded the first page of each chunk and nothing after it. The guard now validates the header at the end of the page it last validated when a read starts there, and shortens a read that would run across that point so the next read starts on it (#360).
- **The decompression guard no longer stops validating silently.** An I/O failure while it read a page header was swallowed and the read carried on with no limit applied, with nothing an operator could observe. It now reaches the caller as the I/O error it is; every other way the guard cannot validate a page already throws `InvalidDataException` (#463, with #359 and #360).
- **The legacy read path bounds file-declared row and value counts like the modern one** (#362). `ReadParquetArrayAsync` narrowed each row group's 64-bit `num_rows` to `int` unchecked and never applied `MaxAllocationValues`, so a count past `int.MaxValue` threw `OverflowException` from the array allocation, a count of exactly 2^32 truncated to zero and returned an empty array without an error, and a count larger than the column it described indexed past the end of the column (`IndexOutOfRangeException`). Each row group's count and the running total are now checked in 64 bits against `MaxAllocationValues` before anything is narrowed or allocated, a column's declared value count is checked before Parquet.Net allocates for it, and a column that supplies fewer values than its row group declares rows is refused; every case throws `InvalidDataException` with the modern path's messages.

---

### Carried over from the entry that was headed `[0.0.4] - 2026-09-11`

> `0.0.4` was never tagged or released: the tags and GitHub releases stop at `v0.0.3`. These changes are
> unreleased and ship with the next release, so the heading is folded into this section (#594). It used
> to be a version heading, which `scripts/ParseChangelog.cs --release` would have picked as the version
> to publish.
>
> **Superseded.** Statements in this carried-over block that the flat read methods remain, that they ship
> alongside the builder, or that use `ToListAsync` describe the unreleased 0.0.4 state, not 0.1: the flat
> reads and `ToListAsync` are removed in 0.1, and the migration table above is the guidance that applies.
This release introduces the generated read builder (`{T}Parquet.From(...)`) and keeps every
existing flat read method working as a forwarder. Both surfaces ship together while the compatibility
window remains open. `docs/19-PUBLIC-API-SURFACE.md` decision D3 retains the flat methods through
the `0.1.0` window; document 41 records the later removal gate. Callers can adopt the builder now;
the flat methods are not yet marked
`[Obsolete]` because the two surfaces are still being validated against each other.

#### Added
- **Generated read builder (`{T}Parquet.From(...)`)**: reads are now expressed as a chain rather
  than a cross-product of method names — `PersonParquet.From(stream).ToListAsync(ct)`,
  `PersonParquet.From(bytes).Parallel().ToArrayAsync(ct)`,
  `PersonParquet.From(bytes).Where(m => m.Id.Min >= 100).ToListAsync(ct)`. The builder is
  **type-state**: a combination that cannot work is not a member you can call. `Parallel()` is
  absent on a `Stream` source, because a single `ParquetReader` seeks within its stream and
  concurrent row-group reads corrupt each other; `Where()` and `Parallel()` are mutually absent
  until a parallel reader accepts a predicate. `Where` also closes a gap in the flat methods,
  where pushdown existed on the `Stream` overloads but not the buffer ones. Rationale and the
  full axis grid are in `docs/19-PUBLIC-API-SURFACE.md` (decision D2).
- **Row-group predicate pushdown from footer statistics**: a predicate over a generated per-column
  `[Min, Max]` metadata view skips row groups that cannot contain a match, so they are never
  decompressed. This is the mechanism the builder's `Where()` exposes.
- **Struct-of-arrays (SoA) columnar batch reading**: a caller that wants columns rather than
  objects can read straight into contiguous per-column buffers and skip materialising a row type
  at all.
- **API change contract (`PARQAPI001` / `PARQAPI002`)**: three API surfaces are now governed, and
  nothing enters one without a catalogue line *and* a `docs/api/LEDGER.md` entry recording its
  semver bucket. The emitted consumer API is gated at **build** time against the `*.api.txt`
  baselines added by #215 (`PARQAPI001`); internal seams — members widened past `private` for
  cross-component reuse — are catalogued in the new `src/api/seams.txt` and gated by `PARQAPI002`;
  the shipped package API keeps its existing `RS0016` gate unchanged. Body, performance and comment
  changes alter the golden `.g.cs` but not the `.api.txt`, and do not trip anything. A
  `**Unapproved-by-design:**` ledger entry suppresses the build error for spikes and is rejected by
  CI on `main`. Both gates are analyzers in `tools/Parquet.SourceGenerator.ApiGates`, are never
  packed, and short-circuit unless handed their catalogue as an `AdditionalFile`, so they cannot
  run in a consumer's compilation. See `docs/18-API-CHANGE-CONTRACT.md`.
- **Direct columnar handoff (write)**: flat `[ParquetSerializable]` models now also emit a
  `{Type}ColumnarBatch` struct plus `WriteParquetRowGroupAsync(batch)`,
  `WriteParquetRowGroupColumnarAsync(rowCount, ...)` and a stream-level `batch.WriteParquetAsync`.
  A caller whose data is already in contiguous column buffers skips the row-to-column transpose and
  its `ArrayPool` rentals entirely — buffers reach Parquet.Net verbatim. Nullable value columns take
  packed values plus explicit definition levels. Measured at ~7% of end-to-end write time on a
  16-column schema, identical in Workstation and Server GC, with allocation unchanged; see
  `docs/12-BUFFER-REUSE-AND-EXTRACTION-STRATEGIES.md` §6. Models with struct, list or map members
  are unaffected and keep the row-oriented API only.
- **Apache Arrow `RecordBatch` ingestion (experimental, #177)**: when — and only when — the consumer
  compilation references Apache.Arrow, the generator emits an extra `{Namespace}.{Type}.Arrow.g.cs`
  per Arrow-representable `[ParquetSerializable]` type, adding
  `WriteParquetRowGroupAsync(ParquetWriter, RecordBatch, ParquetSerializerOptions?, CancellationToken)`
  to the same partial class. Neither shipped package takes an Apache.Arrow dependency; the gate is a
  single `bool` off `CompilationProvider`, so toggling the reference re-runs only the Arrow-gated
  output. Columns are matched by name and validated strictly (every offending field reported at
  once); fixed-width Arrow buffers are handed to the writer with no copy and no `ArrayPool` rental;
  nullable columns derive definition levels from the Arrow validity bitmap and produce byte-identical
  files to the POCO write path. Supported Apache.Arrow floor: `23.0.0`.
- **Sorted row-group pruning (experiment, issue #151)**: mark a column `[ParquetSortKey]` and the
  generator emits `ReadParquetBy<Column>Async` (point lookup) and `ReadParquet<Column>RangeAsync`
  (inclusive slice) for it. Both certify the column as sorted from the footer `[Min, Max]`
  statistics and binary search that metadata, so only the row groups that can contain the key are
  decompressed; overlapping or missing statistics fall back to a full scan and the answer is
  identical either way. Pass a `ParquetPruneStatistics` to see how many row groups were skipped.
  The marker is opt-in: a model with no `[ParquetSortKey]` emits exactly what it did before, with
  none of the lookup API. Marking a member the rules cannot support — nullable, `string`, a
  compound member, or a type with no Parquet statistics order — is reported as **PARQ014** with the
  reason rather than silently emitting nothing.
  Note the cost shape: certifying sortedness reads every row group's statistics, so the metadata
  phase is O(N) in row groups. Only decompression is logarithmic — that is where the speedup
  comes from, and it is why the win grows with row-group payload size rather than with row-group
  count alone.
- **Span-keyed string deduplication** (`DeduplicateStrings = true`): string columns are read
  through Parquet.Net's raw `ReadOnlyMemory<char>` surface and interned against a pooled
  open-addressed table keyed on `ReadOnlySpan<char>`, so a repeated value costs no `string`
  allocation at all. Hash matches are always confirmed with a full ordinal comparison. On the
  Adult Census dataset (32,561 rows, 9 categorical columns) this cuts managed read allocation
  from 20.38 MB to 8.75 MB. The `ReadOnlySpan<byte>` variant the design originally called for is
  not reachable — Parquet.Net 6.1.0 exposes no UTF-8 byte surface for string columns; see
  `UPSTREAM_DEPENDENCY_LIMITATIONS.md`.
- **Compound models (nested POCOs, lists and maps)**: a recursive model and parser replace the
  flat-only target model, and the v6 backend emits struct schemas with definition-ladder shredding
  and matching reconstruction on read, plus row-level lists and arrays of primitives. Two new
  diagnostics guard the shapes that cannot work: **PARQ012** for a compound member that closes a
  type cycle, and **PARQ013** for nesting deeper than the emitter will expand.
- **Format-level dictionary encoding and column encoding hints**, exposed so a model can ask for
  the Parquet encoding a column should use.
- **Conformance and interoperability suites**: generated and fixture files are validated against
  pinned Apache tooling, with bidirectional PyArrow and DuckDB tests, a producer/consumer version
  and schema-evolution matrix, a semantic compatibility oracle, and property-based
  supported-schema and corrupted-file coverage. The envelope these test is written down in the
  compatibility docs.

#### Changed
- **Breaking (pre-1.0), #218 — every configuration option now has a single home.** The duplicated
  positional parameters are removed from the generated API; `ParquetSerializerOptions` is the only
  place either setting lives:
  - `maxDegreeOfParallelism` is gone from `ReadParquetParallelAsync` and
    `ReadParquetParallelArrayAsync` (both the `Stream` and the `ReadOnlyMemory<byte>` overload).
    Set `ParquetSerializerOptions.MaxDegreeOfParallelism` instead.
  - `rowGroupSize` is gone from `WriteParquetBatchedAsync` and from the `IAsyncEnumerable<T>`
    overload of `WriteParquetAsync`, on both the modern and the classic emitter. Set
    `ParquetSerializerOptions.RowGroupSize` instead.

  Migration is mechanical: `WriteParquetBatchedAsync(stream, rowGroupSize: 10_000)` becomes
  `WriteParquetBatchedAsync(stream, new ParquetSerializerOptions { RowGroupSize = 10_000 })`. Both
  parameters sat behind another optional parameter and so were already passed by name, and both
  previously took precedence over the options property — silently, since the signature said nothing
  about it. One behaviour change beyond the removal: a non-positive `maxDegreeOfParallelism` used to
  be discarded without error, so a caller who passed `0` got the options value or
  `Environment.ProcessorCount`; there is now no argument to discard. The rule this applies is
  recorded in `docs/19-PUBLIC-API-SURFACE.md`. `0.0.x` permits the break; the decision is recorded
  as `breaking-major` in `docs/api/LEDGER.md`.
- **Formatting tooling consolidated on CSharpier.** `dotnet format whitespace` is removed from CI:
  its Roslyn formatter disagrees with CSharpier on layout (case-body and pattern-arm indentation),
  and it policed nothing beyond `.cs` files anyway. `.editorconfig` now carries CSharpier's
  configuration (`max_line_length`, its non-configurable behaviors) and disables `IDE0055`, so no
  Roslyn-based formatter — build, IDE or CLI — competes with CSharpier over C# layout. `.gitattributes`
  pins `eol=lf` on checkout, replacing the whitespace formatter's line-ending role for non-C# files.
- Generated public API baselines are emitted as signature-only `*.api.txt` files beside the golden
  `.g.cs` files, so an API change is reviewable separately from a body change.

#### Performance
- All-null nullable chunks are detected and bypassed instead of being decoded value by value.
- Write-side extraction loops drop their bounds checks via `MemoryMarshal` and `Unsafe.Add`.
- Null bitmap construction for nullable column extraction is branchless.

#### Fixed
- The benchmark headline table is regenerated through a pull request rather than pushed directly
  to protected `main`, and is written without a UTF-8 BOM.
- CI builds pull requests that are not based on `main`.

#### Known gaps
- Nested collections, `DateTimeOffset` and positional records are unsupported. They are now
  rejected at compile time (`PARQ006`/`PARQ008`) rather than failing at runtime.
- `ReadParquetParallelAsync(Stream)` reads row groups sequentially — a single `ParquetReader`
  seeks within its stream, so concurrent row-group reads would corrupt each other. It silently
  ignores `ParquetSerializerOptions.MaxDegreeOfParallelism`; pass a `ReadOnlyMemory<byte>` for the
  genuine parallel path. On the new builder this combination is simply unwriteable — `Parallel()`
  does not exist on a `Stream` source — so the flat method is the only place the lie remains.
- Nested and generic target types are rejected (`PARQ009`/`PARQ010`) rather than supported.
- .NET Framework needs the `Parquet.SourceGenerator.Legacy` package. The classic backend has no
  `ArrayPool` story — `DataColumn` allocates its own arrays — so it does not inherit the main
  package's allocation characteristics, and it offers no streaming or parallel reader.
- IronCompress ships no `win-x86` native binary, so 32-bit .NET Framework applications fail at
  runtime on any compressed write.
- See [docs/07-KNOWN-LIMITATIONS.md](docs/07-KNOWN-LIMITATIONS.md) for the full audit.
- **The decompression guard checks the footer before Parquet.Net parses it** (#374). Parquet.Net sizes the lists and byte arrays it allocates for the footer from counts and lengths inside it before it reads an element, so a footer a few bytes long that claimed twenty million row groups cost 160 MB before any generated limit ran, and a stored footer length near `int.MaxValue` wrapped in Parquet.Net's 32-bit arithmetic. When Parquet.Net seeks to the footer length the guard now checks the stored length (it must be positive and fit in the stream, otherwise `InvalidDataException` rather than an `IOException` or `OverflowException`) and walks the footer once without building anything, refusing any list count or byte length larger than the bytes that remain, and a row group count above `MaxRowGroupCount`. A footer that mis-declares the compact type of some other field can still hide a count from the walk; closing that needs the bounds inside Parquet.Net's own reader (see `UPSTREAM_DEPENDENCY_LIMITATIONS.md`).

---

## [0.0.3] - 2026-09-05

> Backfilled retrospectively from git history (`v0.0.2..v0.0.3`, 55 commits). No changelog section
> was written when this version was published, so these notes were reconstructed after the fact
> from commit subjects and pull request descriptions rather than recorded at release time.

### Added
- **Parallel reader over `ReadOnlyMemory<byte>` (real concurrency)**: `ReadParquetParallelAsync`
  now decodes row groups concurrently — one `ParquetReader` over one stream per worker, groups
  claimed dynamically — with results in file order. Over an arbitrary `Stream` it stays
  sequential, because a stream cannot be shared between readers, and `maxDegreeOfParallelism` is
  not honoured there. Before this release the "parallel" reader did no parallel work.
- **Native `T[]` reader overloads and zero-copy `List<T>` deserialization**, with threshold-driven
  drop-down optimizations for small reads.
- **`TimeOnly` columns**, via a migration to Parquet.Net's `TimeDataField`.
- **Drop-in attribute interoperability**: `System.Text.Json` and Parquet.Net's own attributes are
  honoured, so an existing annotated model does not need a second set of attributes.
- **Diagnostic `PARQ011`**: a member type that Parquet.Net 6 supports but the 4.x/5.x
  `DataColumn` API cannot represent is reported by the classic backend, pointing at the package
  switch rather than at the model.
- **`InvalidDataException` with a descriptive message** when a required schema column is missing
  from the file being read.
- **Real-world benchmark datasets**, provenanced and stored via Git LFS, with reporting scripts.
- **Source-controlled golden generated code** plus an `/update-golden` workflow, so a change to
  emitted output is visible in review.
- **Fail-fast cancellation for parallel workers** through a linked `CancellationTokenSource`.

### Performance
- **SIMD hardware acceleration** for column transforms and timestamp conversions.
- **Zero-copy blittable struct array materialization** via `MemoryMarshal.Cast`, made memory-safe
  and covered by a full property test matrix.
- **Dictionary string deduplication with an L1 span cache** on the read path.
- **String and binary serialization no longer box**, with a universal zero-boxing IL bytecode
  assertion across all models to keep it that way.
- **Zero-copy nullable writes** through `ParquetRowGroupWriter.WriteAllPartsAsync`.
- **Eager progressive column buffer return** during row-group serialization.
- **Single-pass layout probe and lifted `ArrayPool` rentals** in the parallel worker.
- **`CollectionsMarshal.AsSpan`** for `List<T>` write extraction.
- **A dedicated sequential buffer reader**, removing recursive delegation from the emitted read
  path.

### Changed
- Parquet.Net bumped from 6.0.3 to 6.1.0; test SDK and runner dependencies bumped.
- `CodeEmitter` and `LegacyCodeEmitter` split into modular partial classes and then into
  composable emitter components, removing the shared-helper class entirely.
- Analyzer and style enforcement added: `PublicApiAnalyzers` guarding the public API surface,
  `Meziantou.Analyzer` for performance and correctness rules, `.editorconfig` code style and
  naming rules at warning level, and CSharpier for deterministic formatting.
- CI hardened: a code coverage gate with a sticky PR comment (raised to 85% line / 70% branch),
  an automated IL interrogation regression gate during `dotnet test`, all GitHub Actions pinned
  to 40-character commit SHAs, and tiered benchmarking for fast PR slices and deep profiling.
- Diagnostics tooling added for investigation rather than for consumers: an `ilspycmd`-based IL
  interrogation workflow and a `dotnet-dump` memory profiling and triage workflow.

### Fixed
- Release workflow hydrates Git LFS test datasets and verifies their provenance, so a release
  build no longer runs against LFS pointer files.
- The PR coverage sticky comment no longer fails on fork pull requests, where the token is
  read-only.
- `issue_comment` workflows authorize the commenting actor and sanitize their input.
- `gh pr` commands in CI specify `--repo`, so they work before the repository is checked out.
- Benchmark model column names align with the reflection serializer, the baseline is guarded in
  tests, and a 1.0x result is reported as parity rather than as a speedup.

---

## [0.0.2] - 2026-09-02

> Backfilled retrospectively from git history (`v0.0.1..v0.0.2`, 20 commits). No changelog section
> was written when this version was published, so these notes were reconstructed after the fact
> from commit subjects and pull request descriptions rather than recorded at release time.

### Added
- **`Parquet.SourceGenerator.Legacy`**: a second generator emitting against the Parquet.Net
  4.x/5.x `DataColumn` API, which is what restores .NET Framework 4.7.2 support. It accepts a
  narrower set of member types than the v6 backend. Introduced as `Parquet.SourceGenerator.V5`
  and renamed to `.Legacy` before release, because the API break is between v5 and v6 rather than
  between v4 and v5, so one backend covers both. The release workflow verifies the extra package.
- **`ReadParquetStreamAsync`**: an `IAsyncEnumerable<T>` streaming *reader*, complementing the
  streaming write path that shipped in `0.0.1`.
- **.NET 9 target framework support**, with package consumption tested against both .NET 8 and
  .NET 9.
- **Compiler diagnostics `PARQ006`–`PARQ010`**: unsupported member types, unassignable members,
  types with no parameterless constructor, and nested or generic target types. Shapes that
  previously emitted uncompilable code or failed at runtime — positional records, get-only
  members, unsupported types — now fail the build with a pointer to the declaration responsible.
- **`docs/BENCHMARKS.md`** as a standalone document, and a dedicated `PACKAGE_README.md` for
  NuGet packaging.

### Changed
- **Un-ordered columns default to declaration order** rather than to an arbitrary order, so a
  model without explicit column ordering produces a stable, predictable schema.
- **`ParquetSerializerOptions` reaches the reader.** All three readers assigned the options and
  then never used them; they are now threaded through to `ParquetReader.CreateAsync`. This is
  plumbing rather than a behaviour change today, since the options type carries only write-side
  settings, but a read-relevant option added later will not be silently dropped.
- **`rowGroupSize` no longer uses its default value as an "unset" sentinel.** Asking for exactly
  50,000 was indistinguishable from not asking, and whenever options carried a size it overrode
  the explicit method argument — the more specific value losing to the more general one. The
  parameter is now nullable, so "not supplied" is representable.
- Benchmarks standardized on a 100k item scale across all suites, with a concise headline table
  embedded in the READMEs and refreshed by CI.
- READMEs use plain Markdown rather than raw HTML so they render on NuGet, and the status badge
  and installation guide reflect the published packages.

### Fixed
- **The read path reallocated its result list once per row group.** `results` was pre-sized to the
  file's total row count and then had `Capacity` reassigned to the running total inside the
  row-group loop, so each row group allocated a *smaller* array and copied into it before the list
  grew back — O(groups × rows) of copying to arrive at the capacity it already had.
  Single-row-group files were unaffected, which is why it survived; the multi-row-group files
  `WriteParquetBatchedAsync` produces are the ones that paid.
- **The parallel reader could lose its pre-allocated destination array** while making stream reads
  thread-safe; the array is now preserved.
- Release workflow uses embedded PDB symbols and passes `--no-symbols` to `nuget push`, so
  publishing does not fail on the symbol package.
- Benchmarks use the `InProcess` execution toolchain, resolving reference-assembly build errors.

### Performance
- **`ReadParquetParallelAsync` materialises into a single pre-sized array** indexed by row-group
  offset, rather than concatenating per-group results. (The decode itself was still sequential at
  this release; genuine concurrency arrived in `0.0.3`.)

---

## [0.0.1] - 2026-08-05

Initial published release, alongside the `0.0.1-dev.1` and `0.0.1-dev.2` prereleases.

### Added
- **Roslyn incremental source generator**: compiles zero-reflection Parquet serializers and
  deserializers against Parquet.Net low-level primitives.
- **Native AOT support**, exercised on every CI run by publishing the AOT test project with
  `-r linux-x64` and executing the resulting native binary. `linux-x64` only, and note that
  Parquet.Net 6.0.3 emits its own trim (`IL2104`) and AOT-analysis (`IL3053`) warnings — so this
  covers the paths the test exercises rather than guaranteeing AOT safety in general.
- **`Guid` columns** written as native 16-byte values via pooled struct buffers rather than
  strings.
- **`IAsyncEnumerable<T>` streaming** directly into chunked row groups.
- **Microsecond `Int64` timestamps** via `[ParquetTimestamp(ParquetTimestampUnit.Microseconds)]`.
- **`ParquetSerializerOptions`** for `RowGroupSize`, `CompressionMethod` (`None`, `Snappy`, `Gzip`,
  `Lz4`, `Brotli`, `Zstd`) and `CompressionLevel` (`Optimal`, `Fastest`, `NoCompression`,
  `SmallestSize`; unset keeps Parquet.Net's default). `MaxDegreeOfParallelism` supplies the worker
  count for the buffer-based parallel read.
- **Compiler diagnostics `PARQ001`–`PARQ005`**: partial-type enforcement, duplicate column names,
  no serializable members, ignored non-public members, and invalid decimal precision/scale.
- **CI workflow** building, testing and packing the solution. Benchmarks run on demand.
- Generated logo and favicon set, with the design record.

### Fixed
The following landed after the options were documented but before the first publish, so no
released version ever carried them:

- `CompressionMethod` was accepted and discarded — no compression setting ever reached the writer.
- `[ParquetTimestamp(Microseconds)]` mapped to `DateTimeFormat.DateAndTime`, which Parquet.Net
  defines as *millisecond* precision, so microsecond columns were silently written coarser and the
  sub-millisecond component was lost. Now maps to `DateAndTimeMicros`.
- `ParquetTimestampUnit.Nanoseconds` and `ParquetSerializerOptions.UseMicrosecondTimestamps` are
  removed. Neither could work: Parquet.Net has no nanosecond format, and the schema is emitted at
  compile time so no runtime flag can change a column's encoding.
- Column names are escaped and hint names qualified by namespace in the emitted code.
- The NuGet packages were not usable as published; packaging is fixed and verified in CI, and the
  release workflow publishes the tagged version.
