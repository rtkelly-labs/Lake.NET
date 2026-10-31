# Lake.Parquet: Vision

> These pages hold the Lake.Parquet fork's decisions; the fork ([rtkelly-labs/parquet-dotnet](https://github.com/rtkelly-labs/parquet-dotnet)) carries the code
> and one change record per change. Paths in `code` are paths in the fork.

## Why the fork exists

Lake.NET and Parquet.SourceGenerator both read and write Parquet through Parquet.Net, and building them
keeps turning up things that are best fixed in the library itself.

**Lake.NET** is the reason the fork exists today. Apache Iceberg identifies every column, nested ones
included, by a Parquet field ID, and upstream Parquet.Net neither reads field IDs back nor writes them
on structs, lists and maps. Iceberg's value model is physical (`long` nanoseconds, unscaled decimals),
its `fixed[n]` type needs `FIXED_LEN_BYTE_ARRAY(n)`, DuckLake's encrypted lakes need Parquet modular
encryption, and pruning needs statistics in each type's own sort order. Without these Lake.NET's only
Parquet engine is ParquetSharp, a wrapper over native parquet-cpp, which rules out WASM, mobile and
trimmed or AOT apps. Lake.NET's `docs/parquet-net-gaps.md` lists the gaps. On its Parquet.Net backend
branch, built on these changes, its whole suite runs on either backend, and Parquet.Net reads within
about 10% of ParquetSharp and writes slightly faster and smaller files.

**Parquet.SourceGenerator** measures the decoder against other libraries (Polars decodes the same files
in about a third of the time) and keeps a list of what it would fix in the library: allocation
amplification from untrusted footer counts, a nullable read path with no fast route when a chunk has
no nulls, page checksums that are never verified, UTF-8 columns that can only be read as `string`, and,
from .NET 11, integer-backed decimals that decode with the error of a `double`. Those become changes
here as they are made and measured.

Upstream is the right home for all of it. But upstream decides its own priorities and does not
accept AI-assisted contributions, and the projects that need these fixes cannot wait for them. The
fork is where a fix can be made, measured and shipped, while it is offered upstream.

## What it is

- **Upstream, plus a short stack of recorded changes.** Upstream's history is kept exactly as upstream
  wrote it. On top of it, every line that differs from upstream belongs to one commit, which is one
  change in [`changes/`](https://github.com/rtkelly-labs/parquet-dotnet/tree/master/docs/changes), with what it does, why, how it was evaluated against upstream, and
  its upstream status.
- **Moved onto newer upstreams deliberately.** The maintainer rebaselines the stack onto upstream
  when there is a reason to (a release, a fix), and looks at what broke before publishing it; a change
  upstream has taken leaves the stack by itself at that point.
- **Compatible with upstream by default.** Same API, same behaviour, same file format. A change that
  alters behaviour says so in its record, so a difference is a decision, never a surprise.
- **Narrower than upstream in one place.** The package leaves out upstream's reflection-based
  `ParquetSerializer` (and the Dremel compilers and CLR schema inference behind it): Parquet.SourceGenerator
  maps classes to columns at compile time, and neither it nor Lake.NET calls the serializer. Its files stay
  in the tree, untouched, so rebaselines never conflict on them; the Lake build only does not compile them.
  Code that uses `ParquetSerializer` should stay on Parquet.Net.
- **Built to be retired.** A change is written as if it were going upstream: minimal, in upstream's
  style, without fork-only abstractions. When upstream has it, or solves the same problem another
  way, the change is removed and so is its record, whoever raised it upstream.
- **A separate package and namespace.** `Lake.Parquet` can sit beside `Parquet.Net` in one process,
  which is what makes side-by-side compatibility tests and benchmarks possible, and nobody gets it by
  accident.

## What it is not

- Not a rewrite, and not a competitor. It does not add features upstream would not take, and it does
  not reformat, restructure or modernise upstream's code.
- Not a place for consumer-specific behaviour. Anything that only Parquet.SourceGenerator or Lake.NET
  needs stays in those projects.
- Not permanent. A fork that only grows has failed; success is a stack with only `lake(0001)` left.

## How a change gets in

1. **Evidence first.** A failing test or a benchmark against upstream (Lake.NET's backend suite and
   benchmark, Parquet.SourceGenerator's cross-library suite, or a test here) shows the problem.
2. **The smallest fix**, in upstream's style, as one commit `lake(NNNN)` with its record, so that
   upstream plus that commit is exactly what an upstream PR would contain.
3. **Evaluated against upstream**: the same tests or benchmarks, before and after, in the record.
4. **Offered upstream** where it is generally useful, by a person (upstream's rules), or noted as
   proposed when someone else has raised the same thing. Others may well reach the same conclusion
   independently; the record tracks the outcome either way.
5. **Retired** when upstream releases it.

## Relationship to the other projects

| Project | Relationship |
|---|---|
| [aloneguid/parquet-dotnet](https://github.com/aloneguid/parquet-dotnet) | Upstream. Its history is kept unchanged; the fork's stack is rebased onto it. |
| [Lake.NET](https://github.com/rtkelly-labs/Lake.NET) | Apache Iceberg and DuckLake for .NET. The main consumer: its managed Parquet backend needs most of the changes here. Consumes Lake.Parquet (by release tag until it is on NuGet). |
| [Parquet.SourceGenerator](https://github.com/rtkelly-labs/Parquet.SourceGenerator) | Finds decoder problems and measures fixes (cross-library benchmarks, regression suite). May consume Lake.Parquet. |
| [rig](https://github.com/rtkelly13/rig) | Estate governance; its managed-fork rules (`docs/forks.md`) need to describe this rebased stack. |
