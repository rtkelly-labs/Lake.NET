# Lake.Parquet

Lake.Parquet is [rtkelly-labs/parquet-dotnet](https://github.com/rtkelly-labs/parquet-dotnet), a fork of
[aloneguid/parquet-dotnet](https://github.com/aloneguid/parquet-dotnet) that Lake.NET reads and writes Parquet
through (the `external/parquet-dotnet` submodule, and the Lake.Parquet package). Parquet.SourceGenerator can use it
too. This folder holds the fork's decisions; the fork holds the code, and one record per change in
[`docs/changes/`](https://github.com/rtkelly-labs/parquet-dotnet/tree/master/docs/changes), which goes with that change's pull request.

| Page | What it decides |
|---|---|
| [decisions.md](decisions.md) | The major decisions, why, and how changes are written so they can be rewritten deterministically |
| [vision.md](vision.md) | Why the fork exists, what it is and is not, how a change gets in |
| [changes.md](changes.md) | The stack's rules and the change record format |
| [maintaining.md](maintaining.md) | Operating the stack: refs, rebaselining onto upstream, staging, history, recovering |
| [testing.md](testing.md) | The test layers, the conformance harness, change verification, benchmarks and profiling |
| [upstreaming.md](upstreaming.md) | Handing changes to upstream, by hand |

What upstream Parquet.Net lacks, and how Lake.NET uses each backend, is [../parquet-net-gaps.md](../parquet-net-gaps.md).

A decision changes here, in a Lake.NET pull request, before the fork's code follows it. A change's own detail (what it
does, why, its evidence, its upstream status) is its record in the fork, in the same commit as its code.
