# DuckDB, DuckDB.NET and the 2.0 transition

**Status:** reference and tracker, checked 2026-10-31. Update the tracker table whenever a row changes; it is the
one place that says which DuckDB each part of Lake.NET runs against.

## DuckDB's three roles in Lake.NET

1. **Oracle.** DuckDB is the reference implementation for DuckLake, and an independent reader for Iceberg (its
   `iceberg` extension) and, later, Delta (its `delta` extension, built on Delta Kernel). It is also an independent
   Parquet reader: its Parquet code is DuckDB's own, not Arrow C++ (which writes our files through ParquetSharp) or
   Parquet.Net.
2. **Format owner.** DuckLake's spec and its reference implementation (the `ducklake` extension) ship with DuckDB.
   DuckLake 1.0 is written by DuckDB 1.5; DuckLake 1.1 by DuckDB 2.0. A DuckLake release is effectively a DuckDB
   release, so the DuckDB version line *is* the DuckLake version line.
3. **Query engine for users.** Lake.NET is not a query engine (VISION.md). An application that wants SQL over a lake
   uses DuckDB, either through DuckDB's own extensions or, in-process from .NET, through DuckDB.NET.

## How we reach DuckDB

| Path | Used for | Cost | Status |
|---|---|---|---|
| Python `duckdb` + extensions (`interop/duckdb_oracle.py`, `interop/ducklake_oracle.py`) | Iceberg via the `iceberg` extension; DuckLake 1.0 / 1.1 both ways | Needs Python, `uv`, extension downloads from `extensions.duckdb.org` at test time | `cross-engine` CI job; skips locally without Python |
| **DuckDB.NET in-process** (`Iceberg.Net.DuckDbNet.Tests`) | DuckDB's Parquet reader over every file type we write: values, field IDs, statistics; DuckDB-written files imported | One NuGet package with native libraries for every RID; no Python, Docker or network | Unit suite, every PR, cannot skip |
| DuckDB.NET + extensions | Same oracles as the Python path, without Python | Extensions install from a pinned local directory (`extension_directory` / `custom_extension_repository` in the connection string) | Not built: the next step once DuckDB.NET ships 2.0 (below) |

Moving the oracles from Python to DuckDB.NET removes the most common reason cross-engine tests skip, lets them run in
the unit suite, and lets a test hold a DuckDB transaction open while Lake.NET commits (the concurrent-writer oracle
the gap review asks for in §4 D5). It needs DuckDB.NET on DuckDB 2.0 for DuckLake 1.1, and extensions installed from a
pinned local repository so CI does not depend on `extensions.duckdb.org`.

## DuckDB 2.0 tracker

| Item | State (2026-10-31) | Source |
|---|---|---|
| DuckDB 2.0.0 LTS | Released 2026-10-21; supported until at least October 2027. VARIANT fully integrated; the PEG parser is the default | duckdb.org announcement; endoflife.ai |
| DuckDB 2.0.x patch | `2.0.1.dev*` pre-releases on PyPI; `2.1.0.dev*` started | PyPI `duckdb` |
| Python `duckdb==2.0.0` | On PyPI | PyPI |
| DuckLake 1.1 | Written by DuckDB 2.0's `ducklake` extension; Lake.NET reads and writes it on the DuckLake branch, DuckDB 2.0 oracle in CI | DuckLake branch `docs/ducklake/` |
| DuckDB.NET on 2.0 | **Merged to `develop` 2026-10-26** ("Update to DuckDB v2.0.0": all 285 bound entry points exported by 2.0; native libraries now ship as `duckdb-shared-libs-<platform>.tar.gz`); **not on NuGet** | `Giorgi/DuckDB.NET` `develop` |
| DuckDB.NET on NuGet | `1.5.6.1` (built on DuckDB 1.5.6), 2026-10-16 | nuget.org `DuckDB.NET.Data.Full` |
| Lake.NET in-process DuckDB | DuckDB.NET 1.5.6.1 → DuckDB 1.5.6 | `Directory.Packages.props` |
| Lake.NET Python oracles | DuckDB 1.5.5 (Iceberg, DuckLake 1.0); DuckDB 2.0.0 (DuckLake 1.1, extension unpinned) | `interop/pyproject.toml`, DuckLake branch `ci.yml` |
| Known 1.5 → 2.0 differences that touched our tests | Lambda syntax (use a form both accept); view SQL stored unqualified by 2.0; encrypted-file filtered reads fail in DuckDB (pinned as a characterization test) | DuckLake branch commits `fb31b57`, `48f8cb0`, `7c03913` |

**Checklist when DuckDB.NET publishes 2.0:**

1. Bump `DuckDB.NET.Data.Full` in `Directory.Packages.props`; run `Iceberg.Net.DuckDbNet.Tests`.
2. Add a second package alias or test matrix entry so the in-process oracle runs on **both** 1.5 (DuckLake 1.0) and
   2.0 (DuckLake 1.1) for as long as DuckLake 1.0 is supported.
3. Port `ducklake_oracle.py` to DuckDB.NET with the `ducklake` extension installed from a pinned local extension
   directory; keep the Python oracle until the .NET one has run green for a release.
4. Record the versions in `interop/engines.json` (W5).

**Until then:** DuckDB.NET's `nightly-builds` branch and CI build packages against DuckDB `main`. We can build
DuckDB.NET `develop` from a pinned commit in a CI job (the same way the Parquet.Net fork is consumed as a submodule) if
we need 2.0 in-process before the release. We do not need to fork for this.

## Should we fork DuckDB.NET?

**Recommendation: upstream-first, no long-lived fork.** Fork only as a staging area for pull requests.

| Consideration | Parquet.Net | DuckDB.NET |
|---|---|---|
| Upstream accepts AI-assisted contributions | **No**: such issues and PRs are treated as spam | **Yes**: 192 of the 210 commits on `develop` since 2026-09-01 carry a `Co-Authored-By: Claude` trailer |
| Upstream activity | Active, own priorities | Very active (daily commits; 2.0 support within five days of DuckDB's release) |
| How much we need to change | 25 behavioural changes (field IDs, physical values, encryption…) | None so far; we use its public ADO.NET API |
| Fork model | Rebased stack, published as `Lake.Parquet` ([forks.md](forks.md)) | Upstream-first: PR branches on a fork, consume upstream's NuGet |

A long-lived fork costs a daily rebase, a separate package identity and a second copy of DuckDB's native binaries.
Nothing we need from DuckDB.NET justifies that today. If a change is rejected upstream and Lake.NET cannot work
without it, switch to the rebased-stack model in [forks.md](forks.md) with that one change.

### What Lake.NET would contribute to DuckDB.NET

Candidates, each to be raised upstream as an issue first, with a reproduction from our suites:

| Candidate | Why Lake.NET needs it |
|---|---|
| Reading VARIANT (DuckDB 2.0) into a .NET value | Iceberg v3 and Delta `variantType` oracles |
| TIMESTAMP_NS read without loss: it maps to `DateTime` today, whose 100 ns ticks drop the last two digits | Iceberg `timestamp_ns`; our tests work around it with `epoch_ns()` in SQL |
| Arrow C Stream *import* (DuckDB scanning an `Apache.Arrow` `IArrowArrayStream` produced in .NET). Export already exists (`ExecuteArrowStream`) | A `Lake.DuckDB` bridge: query any Lake.NET table with DuckDB SQL in-process, with no DuckDB extension (below) |

Already upstream and usable: `extension_directory`, `custom_extension_repository` and `autoinstall_known_extensions`
in the connection string, enough to install extensions from a pinned local directory in CI; .NET table and scalar
functions; Arrow export.

### A possible `Lake.DuckDB` bridge (phase 5, optional)

DuckDB.NET can register .NET table functions, and DuckDB can scan Arrow streams. Together they let an application run
DuckDB SQL over a table that Lake.NET reads (Iceberg with v3 features the `iceberg` extension lacks, DuckLake with a
catalog DuckDB can't attach, or Delta), with pruning and projection pushed into Lake.NET's scan. This keeps Lake.NET a
library, not a query engine: DuckDB does the querying. Prerequisites: the shared scan contract (phase 3) and Arrow
output for every format (`Lake.Arrow`).
