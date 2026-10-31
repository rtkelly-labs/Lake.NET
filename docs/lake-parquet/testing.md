# Testing Lake.Parquet

> These pages hold the Lake.Parquet fork's decisions; the fork ([rtkelly-labs/parquet-dotnet](https://github.com/rtkelly-labs/parquet-dotnet)) carries the code
> and one change record per change. Paths in `code` are paths in the fork.

What tests the fork, what each layer catches, and how to run it. Every layer runs in CI (`lake.yml`); the
expensive ones also run on demand with more cases.

| Layer | What it proves | Where | CI |
|---|---|---|---|
| Upstream's suite and each change's tests | the library still does everything upstream tested, and every change does what its record says | `src/Parquet.Test` | every push and PR, net8.0 and net10.0: on the renamed `Lake.Parquet` build less the serializer's tests (`build`), and whole on the plain build (`suite`) |
| Conformance, forward | files the library writes read back exactly and agree with parquet-cpp, statistics included | `eng/lake/Lake.Parquet.Conformance` | every push and PR (200 seeds), deep on demand |
| Conformance, reverse | files parquet-cpp writes, with its own encodings and page layouts, read back exactly | the same project | the same |
| Change verification | each change, alone on upstream, passes the suite, and its tests fail without its code | `lake-stack verify` | every `staging/*` push, one job per change |
| Benchmarks | what performance changes claim, through the calls Parquet.SourceGenerator's generated code makes | `eng/lake/Lake.Parquet.Benchmarks` | on demand (shared runners are too noisy to gate on) |
| The stack tool | the process (rules, rebaseline, staging, branches) does what [maintaining.md](maintaining.md) says | `eng/lake/Lake.Stack.Tests` | every push and PR |

## The conformance harness

A generator and a set of oracles, so coverage grows with seeds rather than with hand-written cases.

**What a case is.** From a seed: 1 to 6 columns, each a required or optional primitive, a three-level list,
a struct or a map, of any of these kinds:

- integers of every width, signed and unsigned (`int8` … `uint64`), `bool`, `float`, `double` (with NaN,
  ±0, infinities and extremes);
- decimals on INT32, INT64 and fixed-length bytes;
- strings (empty, ASCII, BMP private use, supplementary characters), binary, `fixed[6]`, UUID;
- timestamps in millis, micros and nanos, UTC-adjusted or not, the legacy `TIMESTAMP_MILLIS`, dates as
  `DateTime` and `DateOnly`, and nanosecond timestamps as physical `long` values (`lake(0017)`);

then 1 to 3 row groups of 0 to 200 rows, field IDs on most fields, groups included, and the writer's options:
compression (none, Snappy, Gzip, Brotli, Zstd; LZ4 too in the reverse direction), statistics on or off, chunks
split into data pages of 1 to 60 values in half the cases,
encoding hints per column (dictionary, delta, byte stream split), and in a quarter of cases Parquet modular
encryption (AES_GCM_V1 or AES_GCM_CTR_V1, encrypted or plaintext footer, keys of 128, 192 or 256 bits, some
columns with their own key).

**Forward oracles** (the library writes):

- *round trip*: the library reads back exactly the values and the definition and repetition levels it wrote, the
  levels it declares match the shape, every field keeps its field ID, and every row group its row count;
- *parquet-cpp*: ParquetSharp reads the same physical values, levels and field IDs, decrypting with the same keys;
- *statistics*: null counts and min/max are exactly what the values give under the column's sort order (unsigned
  integers and byte strings unsigned, NaN left out, -0.0/+0.0 for zero bounds), the library's and parquet-cpp's
  readings agree, and parquet-cpp trusts them (the column orders are written); with statistics off, there are none.

**Reverse oracle** (parquet-cpp writes): the same case written by parquet-cpp with its own choices: a random
encoding per column (plain, dictionary, delta binary packed, byte stream split, delta length byte array, delta
byte array), data page v1 or v2, small or large pages, page checksums, page indexes, statistics on or off, and
encryption. The library must read back exactly the values, levels, field IDs and statistics.

**Running it.**

```bash
dotnet test eng/lake/Lake.Parquet.Conformance                                   # seeds 1-200, both directions
LAKE_CONFORMANCE_CASES=5000 dotnet test eng/lake/Lake.Parquet.Conformance       # more
LAKE_CONFORMANCE_FROM=100000 LAKE_CONFORMANCE_CASES=5000 dotnet test eng/lake/Lake.Parquet.Conformance   # fresh seeds
LAKE_CONFORMANCE_SEED=4711 dotnet test eng/lake/Lake.Parquet.Conformance        # one case, to reproduce
LAKE_CONFORMANCE_KEEP=/tmp/failing dotnet test eng/lake/Lake.Parquet.Conformance   # keep failing files
```

A failure prints the case (its shape, options and seed) and every disagreement, with the command that reproduces
it. In CI, `Actions → Lake.Parquet → Run workflow` takes `conformance_cases` for a deep run on fresh seeds.

**What it found** when it was first run: six bugs in upstream's code, now changes `lake(0026)` to `lake(0031)`
(int16/uint16 byte stream split, delta encoding of INT32-backed types, logical types under delta and byte
stream split, RLE booleans, empty column chunks, and delta strings with non-ASCII prefixes), and one in
parquet-cpp, which records the wrong cipher in a plaintext footer when pages use AES_GCM_CTR_V1, so the reverse
direction does not generate that combination.

**Extending it.** A new kind is an entry in `Kinds.All` (how to declare, generate and physically encode it) plus,
for the reverse direction, its parquet-cpp column in `Reverse.Describe`. A new shape is a `Shape` with its
levels. A new writer option goes in `Case.Generate` and `Case.WriteOptions` (or `Reverse.Write`). Anything that
fails is either a bug (make it a change, with a focused test in `src/Parquet.Test`) or a documented limitation
of the other side, kept out of generation with a comment saying why.

## Change verification

```bash
lake-stack verify              # every code change
lake-stack verify 0026 0028    # some
lake-stack verify --list       # the code changes, as a JSON array
```

For each code change it builds `lake/change/NNNN-name` (upstream, then the changes in its `depends_on`, then
it) in a temporary worktree, and requires:

1. the whole suite passes there, so the change works on upstream without the rest of the stack;
2. with the change's library files reverted (its files outside `src/Parquet.Test`), the suite fails or does not
   compile, so the change is what makes its tests pass.

Records with `evidence: benchmark` (performance changes, whose tests guard behaviour and pass either way) and
changes with no library code (tests only) are held to the first only. CI runs it on every `staging/*` branch, one
job per change, so `lake-stack rebaseline --push` only promotes a stack in which every change still proves
itself on the new upstream.

## Benchmarks

```bash
cd eng/lake/Lake.Parquet.Benchmarks
dotnet run -c Release -- --filter '*'                         # every benchmark, on the fork
dotnet run -c Release -- --filter '*Read*' --job short        # quicker, noisier
ParquetProject=/path/to/upstream/src/Parquet/Parquet.csproj dotnet run -c Release -- --filter '*'   # upstream, to compare
```

The library to build against is an environment variable, not `-p:`: BenchmarkDotNet rebuilds the project in a
child process, which sees the environment but not the command line's properties.

Million-row columns read and written the way generated readers and writers do (`ReadRawAsync` into reused
buffers, `ReadAsync` into `T?[]`, strings as chars or as their UTF-8 bytes), with allocations. A change with
`evidence: benchmark` quotes these numbers before and after in its record. The `*_Utf8` benchmarks need the fork
(`lake(0017)`): upstream rejects `ReadOnlyMemory<byte>` for string fields.

### In CI, and comparing two runs

`.github/workflows/lake-bench.yml` runs every benchmark against the upstream commit the stack is built on and
against the fork, one after the other on the same runner, for every `staging/*` branch and on demand (`Actions →
Lake.Parquet benchmarks → Run workflow`, with a `--filter` and the `short` or `medium` job). Its summary is a table of
each benchmark's time and allocation on both sides and the fork's ratio to upstream; the full BenchmarkDotNet reports
are its `benchmarks` artifact. Shared runners are noisy, so it is not part of `lake-ci`: compare runs of the workflow,
and use a quiet machine for the numbers a record quotes.

The same comparison locally, from two runs exported as JSON:

```bash
cd eng/lake/Lake.Parquet.Benchmarks
ParquetProject=/path/to/upstream/src/Parquet/Parquet.csproj dotnet run -c Release -- --filter '*' --exporters json --artifacts /tmp/bench/upstream
dotnet run -c Release -- --filter '*' --exporters json --artifacts /tmp/bench/fork
cd ../../.. && dotnet run eng/lake/CompareBenchmarks.cs -- /tmp/bench/upstream/results /tmp/bench/fork/results upstream fork
```

## Native AOT

Lake.Parquet is supported under native AOT and trimming, and that is enforced and proven, not assumed:

- **Enforced:** the Lake build sets `IsAotCompatible` (`eng/lake/Lake.Parquet.targets`), so any trimming or AOT finding
  in the package fails the build. With the serializer left out, there is none.
- **Proven natively:** `eng/lake/Lake.Parquet.AotTests` is published with `PublishAot`, every trimming and AOT warning
  an error, against the package as published, and runs:

| Mode | What it checks |
|---|---|
| `api` | the calls generated readers and writers make (typed fields of every type, nullable reads into `T?[]`, strings as chars and as UTF-8, decimals with their scale, timestamps with their kind, statistics) under every codec, encoding hint and page layout, and with encryption |
| `cases <from> <count>` | the conformance harness's generated cases, checked by its library-only oracles (values, levels, field IDs, row counts, statistics) |
| `write`/`read <from> <count> <dir>` | files one build wrote, read by the other: JIT to native and native to JIT |
| `hashes <from> <count>` | the files written natively are byte for byte the JIT build's (not encrypted ones, whose nonces are random, nor gzip, whose zlib native AOT links itself) |
| `corpus <dir>` | upstream's test files decoded to the same schema, values and levels as on the JIT |

CI's `aot` job (required by `lake-ci`) runs every mode, 3,000 fixed and 2,000 fresh seeds. Locally:

```bash
cd eng/lake/Lake.Parquet.AotTests
dotnet publish -c Release -r linux-x64 -o /tmp/aot          # needs clang and zlib1g-dev
/tmp/aot/Lake.Parquet.AotTests api
/tmp/aot/Lake.Parquet.AotTests cases 1 3000
dotnet run -c Release -- hashes 1 1000 > /tmp/jit.txt && /tmp/aot/Lake.Parquet.AotTests hashes 1 1000 | diff /tmp/jit.txt -
```

That builds the plain library (upstream's names); CI does the same against the stripped, renamed package, which is
what ships. A consumer generic over `T` that hands `typeof(T)` to a data field annotates its `T` with
`[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)]`, as the analyzer will say.

## Profiling

The benchmark project also runs one scenario in a loop for a profiler (`profile <scenario> [seconds]`; the
scenarios are `read-int64`, `read-double`, `read-timestamp`, `read-decimal`, `read-strings`, `read-strings-utf8`,
`write-int64`, `write-double`, `write-timestamp`, `write-decimal`, `write-strings`). It prints the time and
allocations per run and the collections and pause time it caused:

```bash
cd eng/lake/Lake.Parquet.Benchmarks && dotnet build -c Release
dotnet tool install -g dotnet-trace
dotnet-trace collect --profile dotnet-sampled-thread-time --format Speedscope -o write-decimal.nettrace \
    -- dotnet bin/Release/net10.0/Lake.Parquet.Benchmarks.dll profile write-decimal 10
```

Open `write-decimal.speedscope.json` at [speedscope.app](https://www.speedscope.app) for the flame graph
("Left Heavy" merges the runs). For allocations by type and stack, add `--profile gc-verbose`. On a small VM,
a large array's first use costs page faults that show up as `Memmove` in whatever touches it first: check the
allocations per run before reading those frames as copying.

## The suite on the renamed build

`lake.yml` renames a copy of the source to the `Lake.Parquet` namespace and runs `src/Parquet.Test` against it,
so the package that is published is the one that was tested. That build leaves the reflection-based serializer out (README, "What Lake.Parquet
leaves out"), and with it the tests that use it; the `suite` job runs every test on the plain build, so those
files' tests of other code (encryption, the test-file corpus, legacy lists) keep running. The conformance harness and change verification run
on the plain build: they test the library's behaviour, which the rename does not change.
