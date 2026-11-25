# Forks: when, and how changes go back upstream

**Status:** policy, checked 2026-10-31.

Lake.NET is self-contained (AGENTS.md): it never references code outside its repository. Where a dependency lacks
something a format needs, we first try to get it fixed upstream. A fork is a fallback, never a destination: success is a
fork with nothing left in it.

## Decision

```
Need a change in a dependency?
 ├─ Can Lake.NET work around it in its own code, cheaply and correctly?  → do that; note it in the dependency's row below
 ├─ Does upstream accept our kind of contribution, at a pace we can wait for?
 │    └─ yes → upstream-first: issue + PR from a fork branch; consume upstream's release      (DuckDB.NET)
 └─ no  → rebased-stack fork: upstream + one recorded commit per change, own package identity   (Parquet.Net)
```

## The two models

| | Upstream-first | Rebased stack |
|---|---|---|
| What the fork holds | Short-lived PR branches only | Upstream's history plus `lake(NNNN)` commits, each with a record (`docs/changes/`) |
| What Lake.NET consumes | Upstream's NuGet release (or a pinned upstream commit, built in CI, while waiting) | The fork's package (`Lake.Parquet`), or the fork as a git submodule |
| Rebase | None | Daily onto upstream `master`, tagged before force-push |
| Package identity | Upstream's | Its own (`Lake.Parquet`, namespace `Lake.Parquet`), so it can sit beside upstream in one process |
| Exit | Nothing to exit | Each change leaves when upstream ships it; the fork is retired when only packaging remains |
| Cost | A PR review cycle | Rebase conflicts, a second package, CI on every sync |

## Dependencies and their model

| Dependency | Model | Why | Where |
|---|---|---|---|
| **Parquet.Net** | Rebased stack | Upstream doesn't accept AI-assisted issues or PRs; 25 changes Lake.NET needs (field IDs on groups, physical values, typed statistics, modular encryption…) | `rtkelly-labs/parquet-dotnet`; Lake.NET consumes it as `external/parquet-dotnet` on the DuckLake branch |
| **DuckDB.NET** | Upstream-first | Upstream accepts AI-assisted contributions, is very active and already on DuckDB 2.0; we need no behavioural change today | No fork yet; create `rtkelly-labs/DuckDB.NET` only when the first PR is ready ([duckdb.md](duckdb.md)) |
| ParquetSharp | Use as is | Wrapper over Arrow C++; gaps are worked around in `Iceberg.Net.Data` | — |
| Apache.Arrow (C#) | Use as is | — | — |
| EF Core, Npgsql, Microsoft.Data.Sqlite | Use as is | DuckLake catalogs; providers are mature | — |
| AWS SDK | Use as is | Two SDK behaviours are configured around (retry quota, conditional PUT retries) and covered by `ThrottlingTests` | — |

## Rules for a rebased-stack fork

These apply to Parquet.Net today and to any future one. The fork's own `docs/changes/README.md` is authoritative.

1. **Evidence first.** A failing test or benchmark in the consumer (Lake.NET) or the fork shows the problem.
2. **One change, one commit, one record**, in upstream's style, so `upstream + that commit` is exactly an upstream PR.
3. **A Lake-owned contract suite** in the fork tests what Lake.NET calls, independent of the change records, so
   retiring a change when upstream takes it can't silently remove the behaviour's tests (gap review PQ3).
4. **The daily rebase is gated** on the fork's suite on the renamed build, on Linux, Windows and macOS, and on the
   contract suite; ideally on Lake.NET's Parquet.Net backend suites too, through `repository_dispatch`.
5. **Hostile input is the fork's job.** The managed decoder reads files other engines wrote: it carries the fuzz
   targets and the malformed-file corpus (`parquet-testing/bad_data`) that ParquetSharp, being native, can't run
   in-process (gap review PQ1).
6. **Upstream rules are respected.** Nothing is sent to an upstream that refuses machine-assisted contributions; a
   person proposes changes from the generated `lake/change/NNNN-*` branches.

## Rules for upstream-first

1. Open an issue upstream with a minimal reproduction from our suite before writing the change.
2. Work on a branch of `rtkelly-labs/<repo>`, rebased on upstream's default branch; open the PR from it.
3. Until the release ships, Lake.NET either works around the gap in its own code or builds the pinned upstream commit
   in a CI job. It never depends on the fork branch.
4. Delete the branch when the PR merges.
