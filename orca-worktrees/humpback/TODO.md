# Local Task Tracking

## Pathway 1: Performance & AOT Optimizations (PR #24 - Merged)
- [x] Analyze codebase tooling, architecture, and current performance characteristics
- [x] Evaluate reflection usage across Linq, EF Core, RestServer, and ShadowCompare
- [x] Investigate NativeAOT and trimming compatibility blockers
- [x] Implement streaming batch reads & zero-allocation hashing for `TableFingerprint`
- [x] Enable `<PublishAot>true</PublishAot>` on `tools/ShadowCompare`
- [x] Eliminate per-entry reflection on dictionary mapping in `ReflectionRowMapper`
- [x] Eliminate repeated `Activator.CreateInstance` in `TypeModel` list/map factories
- [x] Eliminate per-query generic method reflection and avoid runtime `Expression.Compile()` for constant evaluation in `IcebergQueryCompiler`
- [x] Rely on CI for full suite execution & benchmarks as requested
- [x] PR #24 merged to `main`

## Pathway 2: TypeModel Source Generator (`rtkelly13/typemodel-sourcegenerator` - PR #25)
- [x] Design incremental Roslyn source generator (`IcebergMappableGenerator`)
- [x] Implement `[IcebergMappable]` attribute and `IIcebergMappable<T>` interface
- [x] Implement zero-reflection compile-time schema derivation (`T.IcebergSchema`)
- [x] Implement compile-time zero-reflection row mapper (`ToRow`, `CreateReader`) with physical conversions
- [x] Implement mapper resolver (`IcebergRowMappers.For<T>`) favoring `IIcebergMappable<T>`
- [x] Wire analyzer into `Iceberg.Net.Linq.Tests` and add unit & end-to-end integration tests
- [x] Address code review findings:
  - [x] Resolve unannotated members before binding their field IDs against table schema
  - [x] Initialize mapped properties outside constructor parameters in reader
  - [x] Emit identifier fields for `[IcebergKey]` and enforce required non-null fields
  - [x] Preserve field type and nullability overrides (`[IcebergField(Type = ...)]`, `Nullability`)
  - [x] Report diagnostics on unsupported member types / structures rather than defaulting to string
  - [x] Fix duplicate field IDs when explicit and auto IDs are mixed
  - [x] Fix nullable scalar reading (`default(T)`)
  - [x] Fully qualify LINQ references and ensure unique source hint names
- [x] All 98 unit tests passing
- [x] Resolve all review conversations across PR #25
- [x] Set `<IsPackable>false</IsPackable>` on `Iceberg.Net.SourceGenerator.csproj`
- [x] Author comprehensive documentation in `docs/source-generation.md` covering differences from `main` and learnings from `Parquet.SourceGenerator`
- [x] Link `docs/source-generation.md` in `docs/README.md` and `AGENTS.md`
- [x] Commit and push changes to PR #25 (auto-merge enabled)
