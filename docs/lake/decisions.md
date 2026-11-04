# Open decisions

**Status:** D8–D11 are decided; the rest are open (D12 is due before the rename). These are the decisions the target state depends on. Each has its options, a
recommendation and an outcome. When a decision is made, fill in **Outcome** with what was chosen and the
date. The other pages in this folder link here by ID.

## D1

**Shared type model.** DuckLake maps its types onto Iceberg's `PrimitiveType` (`DuckLakeTypes.ToIceberg`). Types
Iceberg lacks (`uint8`–`uint64`, `timestamp_s`/`_ms`, `timetz`, `interval`) are kept in a side model
(`DuckLakePrimitive`, `DuckLakeTimeTz`, `DuckLakeInterval`) and stored with wider physical values, for example
`uint64` as a decimal.

| | |
|---|---|
| Options | (a) Move Iceberg's type model into `Lake.Core` unchanged. (b) A neutral superset (unsigned ints, more timestamp units, interval) that every format maps onto. (c) Share only physical values and Parquet schemas; each format keeps its own logical types |
| Recommendation | Start with (c), which is what DuckLake already does and costs nothing. Move to (b) once Delta shows which types are really shared. Avoid (a): it would hold the other formats to Iceberg's limits |
| Outcome | Open |

## D2

**Row model:** should the `object?[]` row of physical values be shared by all formats, or should each format
have its own?

| | |
|---|---|
| Options | Share one physical row and add a converter for each format; or give each format its own row model |
| Recommendation | Share it. DuckLake already uses it unchanged |
| Outcome | Open |

## D3

**Telemetry source names** (ActivitySource and Meter).

| | |
|---|---|
| Options | Keep `"Iceberg.Net"`; use a single `"Lake"`; use one per format (`"Lake.Iceberg"`, `"Lake.DuckLake"`) |
| Recommendation | One per format, all starting with `Lake.`, so `AddSource("Lake.*")` picks up every format. This breaks anyone already listening to `"Iceberg.Net"`, so the changelog must say so |
| Outcome | Open |

## D4

**Iceberg packages after phase 3:** merge `Lake.Iceberg.Core`, `Avro` and `Data` into one `Lake.Iceberg`?

| | |
|---|---|
| Options | Merge them into one package; or keep the three |
| Recommendation | Merge, but keep a `netstandard2.0` metadata package if net472 consumers still need one (roadmap open question 1) |
| Outcome | Open |

## D5

**`ICEBERG_*` environment variables.** There are about 50, and only tests and emulation use them.

| | |
|---|---|
| Options | Rename them to `LAKE_*` in phase 1; rename them later; keep them |
| Recommendation | Keep them in phase 1 so the rename stays mechanical. Rename the format-neutral ones (`ICEBERG_TEST_TARGET`, `ICEBERG_PYTHON`, `ICEBERG_PARQUET_BACKEND`, …) in a follow-up. No released code reads them |
| Outcome | Open |

## D6

**Version line for the first `Lake.*` release.**

| | |
|---|---|
| Options | Start again at `0.0.1-alpha.1`; or continue at `0.0.1-alpha.2` |
| Recommendation | Start again at `0.0.1-alpha.1`, because these are new package IDs |
| Outcome | Open |

## D7

**Delta reference implementations for oracle tests.**

| | |
|---|---|
| Options | delta-rs (the Python `deltalake` package); Spark with Delta; the Delta Acceptance Tests (DAT) |
| Recommendation | All three, each for a different job: DAT as the spec vectors (as PyIceberg fixtures are for Iceberg), delta-rs as the fast oracle, and Spark in the `cross-engine` suite |
| Outcome | Open |

## D8

**ORC data files.** ORC is a columnar file format from the Hive world, an alternative to Parquet. Iceberg allows
ORC data files (and Iceberg Java can write them); DuckLake and Delta are Parquet-only. Today we refuse ORC
files explicitly ([libraries.md](../libraries.md)).

State of .NET support, checked on NuGet on 2026-10-28:

| Library | Version | Published | Downloads | State |
|---|---|---|---|---|
| `EngineeredWood.Orc` | 0.3.0 | 2026-08-04 (0.1.0 on 2026-07-20) | 432 | Managed reader and writer over Apache Arrow; "preliminary, APIs may change before 1.0.0"; built with Claude Code per its README; part of the experimental `clast-project/engineered-wood` set |
| `ApacheOrcDotNet` | 1.0.0 | 2020-11-06 | 3,827 | Unmaintained for six years |
| Apache Arrow C# / ParquetSharp | — | — | — | No ORC support (Arrow's ORC adapter is C++ only and not exposed by ParquetSharp) |

Field IDs are what make it hard: Iceberg resolves ORC columns by the `iceberg.id` type attribute, so a reader has
to expose ORC type attributes. Whether either library does is not yet checked.

| | |
|---|---|
| Options | (a) Keep refusing ORC. (b) Read-only ORC through `EngineeredWood.Orc` (Apache-2.0), behind an optional package. (c) A managed ORC reader and writer of our own. (d) A C# port of Apache ORC's Java implementation (Apache-2.0), the reference implementation Iceberg Java uses |
| Recommendation | (a) until there is a consumer; then (d) rather than (b), since Apache ORC Java is the established implementation and `EngineeredWood.Orc` is three releases old |
| Outcome | **Decided 2026-10-28: (d).** Port Apache ORC Java to C# as `Lake.Orc` in this repository, credited in `NOTICE`. Iceberg ORC data files then plug into `Lake.Data` as a second file format. Reads first, verified against Spark-written ORC fixtures with `iceberg.id` attributes; writes after. ORC stays refused until the port passes those fixtures |

## D9

**Licence.** The repository was MIT. Most of the ecosystem is Apache-2.0 (Iceberg, Arrow, Parquet, ORC, Spark,
Delta, delta-rs, ParquetSharp). DuckDB, DuckLake and Parquet.Net are MIT. Parts of this code follow Iceberg Java,
and the ORC port (D8) will be derived from Apache ORC Java.

| | |
|---|---|
| Options | Keep MIT; switch to Apache-2.0 |
| Recommendation | Apache-2.0: it gives an explicit patent grant, contribution terms and a NOTICE file for crediting ported Apache code. It matches the ecosystem, and it keeps the option of proposing the project to Apache open |
| Outcome | **Decided 2026-10-28: Apache-2.0** for this repository, with `NOTICE` packed into every package. The Parquet.Net fork stays MIT, like Parquet.Net, in its own repository ([D10](#d10)). `Iceberg.Net.*` `0.0.1-alpha.1` stays MIT as published |

## D10

**How the Parquet.Net fork is consumed.** The fork (`rtkelly-labs/parquet-dotnet`) keeps upstream's `Parquet`
namespace and is published as `Lake.Parquet`, a drop-in replacement for Parquet.Net. A consumer that also uses
Parquet.Net, directly or through Parquet.SourceGenerator, then has two packages defining the same types. With
different assembly names the build fails (CS0433). With the same assembly name (`Parquet.dll`) only one copy is
copied to the output, and fork-only APIs such as `FieldId` on groups fail at runtime if upstream's copy wins.
Parquet.Net types do not appear in Lake's public API: `ParquetNetBackend` exposes only `ParquetNet.UseAsDefault()`,
`Scope()` and Lake's own `IPqReader` / `IPqWriter`.

| | |
|---|---|
| Options | (a) Publish the fork as a drop-in `Lake.Parquet` with namespace `Parquet`. (b) Rewrite the namespace and assembly name of a separate package. (c) Two tiers: the fork carries behaviour, and the build in this repository rewrites it |
| Recommendation | (c) |
| Outcome | **Decided 2026-10-28: (c), two tiers.** |

**Tier 1: behaviour, in the fork.** `rtkelly-labs/parquet-dotnet` carries only behavioural changes over upstream:
field IDs on every node, statistics, encryption, raw level access and the other gaps in `docs/parquet-net-gaps.md`.
It keeps upstream's namespace, assembly name and layout, so each change stays a clean patch that a person can
propose upstream, and the weekly upstream merge stays cheap. It carries no Lake identity or renaming. It is MIT.

**Tier 2: rewrite, in this repository's build.** `external/parquet-dotnet` stays a git submodule pinned to a fork
commit. When this repository builds it, it gives the result a Lake-owned identity so it can sit alongside
Parquet.Net in the same application. The build changes the assembly name, and also the namespace, or makes the
types internal. The fork's source isn't touched. This build step lives in `external/Directory.Build.targets`
(which already sets the submodule's build settings) plus a rewrite step.

The mechanism is a follow-up decision, with these options:
- **IL-level:** merge the assembly into `Lake.Data.ParquetNet` and make its types internal (ILRepack-style), so
  no namespace rename is needed. This is robust to upstream source changes, but debugging and SourceLink
  are less faithful.
- **Source-level:** rewrite `namespace Parquet` and the `using` directives before compiling. This keeps PDBs
  readable, but every upstream merge has to pass through the rewrite.

Recommendation: IL-level, because nothing outside `Lake.Data.ParquetNet` needs to see the types. Either way, a
test loads the upstream `Parquet.Net` package and `Lake.Data.ParquetNet` in one process, writes a file through each,
and checks that each reads the other's file.

Consequences:
- **`Lake.Data.ParquetNet` no longer waits for a published `Lake.Parquet`.** It packs as soon as the tier-2 build
  exists.
- **Parquet.SourceGenerator's `lake/` packaging step becomes optional.** A public drop-in `Lake.Parquet` is published
  only if someone wants Parquet.Net with the fork's fixes. It can never sit alongside Parquet.Net in one application.
- **The `NOTICE` file and package licence metadata say that MIT code from Parquet.Net is included**, under its own
  licence, inside an Apache-2.0 package.

## D11

**Package versioning.** Today one version and one root `CHANGELOG.md` cover every package, and the release
packs the whole solution.

| | |
|---|---|
| Options | One shared version (today); a version per package, with its own changelog and public API files |
| Recommendation | A version per package |
| Outcome | **Decided 2026-10-28: a version per package.** Each package has its own `CHANGELOG.md`, `PublicAPI.Shipped.txt` / `PublicAPI.Unshipped.txt` (PublicApiAnalyzers, as in Parquet.SourceGenerator) and version. `ReleasePlan.cs` works out which packages need a release and how big a bump. Design: [releases.md](releases.md) |

## D12

**Version families.** With D11, every package has its own changelog and API files. The open question is whether
every package also has its own version line, or whether packages released together share one.

| | |
|---|---|
| Options | (a) Fully independent: about 15 version lines. (b) Families sharing a version: platform (`Lake.Core`, `Lake.IO`, `Lake.Data`, `Lake.Data.ParquetNet`, `Lake.Arrow`), `Lake.Puffin`, `Lake.Orc`, Iceberg (`Lake.Iceberg.*`), `Lake.DuckLake`, `Lake.Delta`: about 6 lines |
| Recommendation | (b). It removes most of the coordination, and each package still records exactly what changed in its own changelog and API files. Families can split later if one package needs its own pace |
| Outcome | Open; decide before the rename branch sets up release-please |
