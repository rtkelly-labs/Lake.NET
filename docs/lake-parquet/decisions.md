# Lake.Parquet decisions

The decisions the fork is built on. Each one is settled until a Lake.NET pull request changes this page; the fork's
code and its tooling follow it.

## The stack

1. **Upstream's history is kept exactly as upstream wrote it, and the fork is a stack on top.** `master` is upstream,
   then `lake(0001)` (packaging, tooling) and one commit per change, `lake(NNNN): title`, each with its record in the
   same commit. No merge commits above upstream. `git log upstream/master..master` is the whole of what the fork adds.
2. **The stack is immutable once published.** It moves onto a newer upstream only when the maintainer decides to
   (`rebaseline`), through a `staging/<name>` branch that CI builds and verifies, and only then replaces `master`.
   Nothing published is lost: every replaced head is tagged `lake/stack/<date>-<sha>`, and every published version of
   the stack and of each change branch is recorded in the append-only `lake/history/*` refs, which only fast-forward.
   Between rebaselines `master` only moves forward.
3. **Change branches are generated, never edited.** `lake/change/NNNN-name` is upstream plus one change and what it
   depends on: the diff an upstream pull request would contain.

## How changes are written

4. **Every change is written so that it can be rewritten deterministically.** At each rebaseline a change is replayed
   onto new upstream code, and sometimes has to be re-made rather than merged. So:
   - a change is the smallest edit that does its job, in upstream's style, touching nothing it does not need, so it
     replays cleanly or is easy to re-make;
   - its record says what it does and why precisely enough that someone could write it again from the record alone,
     and names its evidence (the test that fails without it, or the benchmark);
   - mechanical edits are done by tools from data, not by hand: the namespace rename (Roslyn, on a CI copy), files left
     out of the package (`Lake.Parquet.targets`), members left out (`eng/lake/strip-members.json`, each with a hash of
     its syntax, so an upstream edit to one fails the build instead of being dropped unseen);
   - generated sources are changed in their template and regenerated (`*.tt`), never edited alone;
   - when a replay conflicts, the change is re-made from its record onto the new code, and its tests and `verify`
     prove it again, rather than hunks being merged until it compiles.
5. **Every code change proves itself alone on upstream.** `lake-stack verify`: its change branch passes the suite, and
   the suite fails (or does not compile) without its library code; performance changes (`evidence: benchmark`) carry
   BenchmarkDotNet numbers against upstream instead. The conformance harness checks the library against parquet-cpp
   in both directions on every build.

## What the package is

6. **The package is as narrow as its consumers need.** Lake.NET and Parquet.SourceGenerator use the reader, the writer,
   the schema, the footer model and the serialization attributes. The reflection-based `ParquetSerializer` and what
   only it used are left out of the package at build time (upstream's files stay untouched); the plain build and its
   suite keep them, so nothing upstream tests goes untested. Code nothing in the package calls is left out the same
   way, member by member through the hashed manifest (`eng/lake/strip-members.json`), so that everything the package
   ships is code the native AOT suite can run: first, upstream's big-endian bit-packing kernels, which only the
   deprecated `BIT_PACKED` encoding would use and which the reader refuses. A hash that changes, or a caller that
   appears, fails the build until someone checks the member is still unneeded. Where a member is needed but one of its
   branches served only the serializer (a dispatch arm for `string` values, whose fields are `ReadOnlyMemory<char>`
   in the core API), the branch is left out the same way, by its condition and a hash of the branch. The coverage
   target follows: every line the package ships is run by the regression suite (`testing.md`, "Coverage"), and a line
   the public API cannot reach is either stripped or tested from the inside.
7. **Performance work is on the core path, measured, and changes no file layout by default.** Changes target the calls
   generated readers and writers make (page I/O, compression, encoding, levels, statistics), each with its numbers in
   its record. Anything that changes the files written (data pages within a chunk, a page index, bloom filters) is an
   option, off by default.
8. **Lake.Parquet supports native AOT and trimming, verified on every build.** The package is analyzer-clean with
   `IsAotCompatible` as a build error, and a native AOT regression suite, sharing the conformance harness's generator,
   runs it natively against the JIT build (`testing.md`, "Native AOT"). A change that breaks either does not merge.
9. **Native .NET types are supported where the format has a matching type, losslessly by default.** `System.Half`
   for FLOAT16, `TimeOnly` and `TimeSpan` for TIME, and their kin are read and written through the core API, each
   with tests against other writers (the conformance harness's parquet-cpp oracle, both directions) and in the native
   AOT suite. Where a type has more than one Parquet representation, or a conversion could lose precision (TIME in
   nanoseconds as 100 ns ticks) or change what existing code reads (upstream reads TIME as `int`/`long`), the mapping
   is opt-in, by an option or an explicit field, and the default stays upstream's. One exception, decided
   1 November 2026: a value the file cannot hold exactly is refused on write rather than changed, even where upstream
   changes it. A decimal with more fractional digits than its column's scale throws unless
   `ParquetOptions.RoundDecimalsToScale` asks for upstream's truncation. Wall-clock semantics for TIMESTAMP columns
   that are not UTC-adjusted (store the value as it is, read it back as `Unspecified`) are asked for with
   `ParquetOptions.UnadjustedTimestampsAsWallClock`; the default stays upstream's. Every type's mapping, and each exception to
   it, is in [types.md](types.md).
10. **The fork publishes its own package, `Lake.Parquet`**, renamed at build time, beside Parquet.Net rather than in
   place of it, with upstream's licence and attribution.

11. **Nested data: the library owns levels and offsets; mapping objects belongs to Parquet.SourceGenerator.** The
   package gets no reflection-based object mapper (decision 6). For lists, maps and structs at any depth it offers
   leaf columns with definition and repetition levels (`WriteAllPartsAsync`, `ReadRawAsync`, and the typed `ReadAsync`
   and `WriteAsync`), and an offsets layer between them: for a leaf, one offsets array per repeated ancestor and one
   validity mask per optional ancestor, with the packed values — Arrow's layout — and the reverse for writing. These
   are functions over spans, with no reflection or runtime generics, so native AOT is not affected. Generated code
   assembles `List<List<T>>`, `Dictionary<K, V>` and nested records by walking offset ranges, and Lake.NET's read path
   and Arrow bridge take the same layout. A write whose sibling leaves disagree on their shared ancestors' levels is
   refused, since nothing else would catch it. Proven against parquet-cpp in the conformance harness (lists of lists,
   maps of lists, lists of structs holding lists) and in the native AOT suite.

## Upstream

12. **Changes go upstream by hand only.** Upstream does not accept issues, pull requests or comments written by an LLM.
   A person proposes a change from its change branch, using its record ([upstreaming.md](upstreaming.md)). The aim is a
   stack that shrinks: a change leaves when upstream has it or an equivalent.

## Where things live

13. **Decisions live in Lake.NET, detail lives with the change.** This folder is the fork's design and process; the
    fork carries its code, its tooling, a short `README.md` and `AGENTS.md`, and the per-change records, which are
    written in the change's own pull request.
