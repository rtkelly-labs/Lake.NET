# Parquet.SourceGenerator

A zero-reflection C# Roslyn source generator that emits Parquet serializers and deserializers at compile time, targeting [Parquet.Net](https://github.com/aloneguid/parquet-dotnet) low-level primitives.

## 📦 Installation

```bash
dotnet add package Parquet.SourceGenerator
dotnet add package Parquet.Net
```

`Parquet.SourceGenerator.Attributes` is automatically included as a dependency. `Parquet.Net` must be referenced directly by your project.

## 📖 Quick Start Example

```csharp compile-file
using System;
using Parquet.SourceGenerator;

[ParquetSerializable]
public partial record UserEvent
{
    [ParquetColumn("event_id", Order = 1)]
    public Guid Id { get; init; }

    [ParquetColumn("username", Order = 2)]
    public string Username { get; init; } = string.Empty;

    [ParquetColumn("timestamp", Order = 3)]
    [ParquetTimestamp(ParquetTimestampUnit.Microseconds)]
    public DateTime Timestamp { get; init; }

    [ParquetColumn("duration", Order = 4)]
    public TimeSpan Duration { get; init; }
}
```

<!-- readme-compile-members
static List<UserEvent> events = new();
static Stream stream = Stream.Null;
static List<UserEvent> GetEvents() => events;
static async IAsyncEnumerable<UserEvent> GetAsyncEventStream() { await Task.CompletedTask; yield break; }
-->

### Writing Parquet Files

```csharp compile
List<UserEvent> events = GetEvents();
using var stream = File.Create("events.parquet");

// Simple write
await events.WriteParquetAsync(stream);

// Chunked row-group streaming write
await events.WriteParquetAsync(
    stream,
    new ParquetSerializerOptions { RowGroupSize = 10_000 });

// Stream directly from IAsyncEnumerable<T>
IAsyncEnumerable<UserEvent> eventStream = GetAsyncEventStream();
await eventStream.WriteParquetAsync(
    stream,
    new ParquetSerializerOptions { RowGroupSize = 10_000 });
```

### Reading Parquet Files (Sequential & Parallel)

```csharp compile
using var stream = File.OpenRead("events.parquet");

// Sequential read
UserEvent[] events = await UserEventParquet.From(stream).ToArrayAsync();

// Need a List<T>? Convert the array: the reader has one materialised shape.
List<UserEvent> eventList = events.ToList();

// Read from in-memory byte buffer
ReadOnlyMemory<byte> buffer = File.ReadAllBytes("events.parquet");
UserEvent[] memEvents = await UserEventParquet.From(buffer).ToArrayAsync();

// Multi-core parallel read across row groups (buffer source only: one reader over one
// stream cannot decode row groups concurrently, so From(stream).Parallel() throws
// NotSupportedException)
UserEvent[] parallelEvents = await UserEventParquet
    .From(buffer)
    .WithOptions(new ParquetSerializerOptions { MaxDegreeOfParallelism = 4 })
    .Parallel()
    .ToArrayAsync();
```

### Custom Configuration (`ParquetSerializerOptions`)

```csharp compile
var options = new ParquetSerializerOptions
{
    RowGroupSize = 25_000,
    MaxDegreeOfParallelism = 8,
    CompressionMethod = ParquetCompressionMethod.Zstd
};

await events.WriteParquetAsync(stream, options: options);
```

Compression options: `None`, `Snappy` (default), `Gzip`, `Lz4`, `Brotli`, `Zstd`.

<!-- BENCHMARK_TABLE_START -->
## ⚡ Performance & Benchmarks

Zero-reflection C# source generation vs **`ParquetSerializer` v6** reflection baseline:

| Operation | Scale | Reflection Baseline | Source Generator | Speedup | Memory Reduction |
|:--- |:---:|:---:|:---:|:---:|:---:|
| **File Serialization (Write)** | 100,000 items | 6.66 ms (11.00 MB) | **2.57 ms** (**5.74 MB**) | ⚡ **2.6x faster** | 📉 **48% less memory** |
| **Streaming Batched Write** | 100,000 items | 6.66 ms (11.00 MB) | **3.39 ms** (**5.03 MB**) | ⚡ **2.0x faster** | 📉 **54% less memory** |
| **File Deserialization (Read)** | 100,000 items | 12.41 ms (12.30 MB) | **6.05 ms** (**8.99 MB**) | ⚡ **2.0x faster** | 📉 **27% less memory** |
| **Parallel Deserialization (Read)** | 100,000 items | 12.41 ms (12.30 MB) | **6.95 ms** (**9.86 MB**) | ⚡ **1.8x faster** | 📉 **20% less memory** |
| **Streaming Read (IAsyncEnumerable)** | 100,000 items | 12.41 ms (12.30 MB) | **5.18 ms** (**8.22 MB**) | ⚡ **2.4x faster** | 📉 **33% less memory** |
| **Guid Serialization** | 100,000 items | 15.82 ms (17.71 MB) | **10.39 ms** (**10.70 MB**) | ⚡ **1.5x faster** | 📉 **40% less memory** |

> 📌 **Note**: BenchmarkDotNet results captured on GitHub Actions. Detailed multi-scale reports (1K, 10K, 100K, 1M rows) are in [docs/BENCHMARKS.md](https://github.com/rtkelly13/Parquet.SourceGenerator/blob/main/docs/BENCHMARKS.md).


## 🌐 Real-World Provenanced Dataset Benchmarks

Fixed public datasets tracked under Git LFS with full cryptographic SHA-256 data provenance:
- **TPC-H SF 0.01 LineItem**: 60,175 rows, 16 columns (decimals, dates, strings, dictionary encoding)
- **Adult Census Income**: 32,561 rows, 15 columns (9 categorical dictionary columns)
- **Diamonds**: 53,940 rows, 10 columns (continuous float metrics & ordinal cuts)

| Operation | Scale | Reflection Baseline | Source Generator | Speedup | Memory Reduction |
|:--- |:---:|:---:|:---:|:---:|:---:|
| **TPC-H LineItem Deserialization** | 60,175 rows | 85.45 ms (55.11 MB) | **54.69 ms** (**38.59 MB**) | ⚡ **1.6x faster** | 📉 **30% less memory** |
| **TPC-H LineItem Parallel Deserialization** | 60,175 rows | 85.45 ms (55.11 MB) | **57.63 ms** (**39.08 MB**) | ⚡ **1.5x faster** | 📉 **29% less memory** |
| **TPC-H LineItem Streaming Deserialization** | 60,175 rows | 85.45 ms (55.11 MB) | **50.11 ms** (**38.13 MB**) | ⚡ **1.7x faster** | 📉 **31% less memory** |
| **Adult Census Deserialization (Dictionaries)** | 32,561 rows | 44.32 ms (29.20 MB) | **31.55 ms** (**20.38 MB**) | ⚡ **1.4x faster** | 📉 **30% less memory** |
| **Adult Census Parallel Deserialization** | 32,561 rows | 44.32 ms (29.20 MB) | **34.92 ms** (**23.04 MB**) | ⚡ **1.3x faster** | 📉 **21% less memory** |
| **Adult Census Streaming Deserialization** | 32,561 rows | 44.32 ms (29.20 MB) | **14.09 ms** (**20.13 MB**) | ⚡ **3.1x faster** | 📉 **31% less memory** |
| **Diamonds Deserialization** | 53,940 rows | 30.74 ms (19.73 MB) | **14.08 ms** (**12.66 MB**) | ⚡ **2.2x faster** | 📉 **36% less memory** |
| **Diamonds Parallel Deserialization** | 53,940 rows | 30.74 ms (19.73 MB) | **14.03 ms** (**15.79 MB**) | ⚡ **2.2x faster** | 📉 **20% less memory** |
| **Diamonds Streaming Deserialization** | 53,940 rows | 30.74 ms (19.73 MB) | **7.93 ms** (**12.25 MB**) | ⚡ **3.8x faster** | 📉 **38% less memory** |

### 🗜️ TPC-H LineItem Multi-Codec Serialization Throughput (60,175 rows)

| Codec | Compression Profile | Serialization Time | Allocated Memory |
|:--- |:---:|:---:|:---:|
| **Snappy** | Generator Built-in | **21.51 ms** | **10.96 MB** |
| **Zstandard (Fastest)** | Generator Built-in | **61.62 ms** | **22.40 MB** |
| **Zstandard (Optimal)** | Generator Built-in | **70.02 ms** | **23.06 MB** |
| **Uncompressed** | Generator Built-in | **40.31 ms** | **38.18 MB** |
<!-- BENCHMARK_TABLE_END -->

## 🛡️ Compiler Diagnostics

| Diagnostic ID | Severity | Description |
|:--- |:---:|:--- |
| **`PARQ001`** | **Error** | Target type decorated with `[ParquetSerializable]` must be declared as `partial`. |
| **`PARQ002`** | **Error** | Duplicate `[ParquetColumn]` column names detected on model. |
| **`PARQ003`** | **Warning** | Target type has no valid public serializable properties or fields. |
| **`PARQ004`** | **Warning** | Non-public property decorated with `[ParquetColumn]` will be ignored. |
| **`PARQ005`** | **Error** | Invalid `[ParquetDecimal]` precision or scale parameters. |

The remaining rules (`PARQ006` to `PARQ016`: unsupported types, assignability, constructors, nested-type accessibility, generics, cyclic or too-deep compound types, sort keys, feature level and name collisions) are catalogued in the [Compiler Diagnostics Reference](https://github.com/rtkelly13/Parquet.SourceGenerator/blob/main/docs/13-COMPILER-DIAGNOSTICS.md).

## 🚀 Native AOT & Cold-Start Performance

Under Native AOT, cold invocations (CLI tools, serverless Lambda functions, batch workers) run **7.2× faster** with **72% less memory**:

| Metric | CoreCLR (JIT) | Native AOT | Improvement |
| :--- | :---: | :---: | :---: |
| **Total Process Time** | **358 ms** | **50 ms** | ⚡ **7.2× faster** |
| **CPU User Time** | **0.30 s** | **0.02 s** | ⚡ **15× less CPU** |
| **Peak Working Set** | **57.0 MB** | **15.8 MB** | 📉 **72% less RAM** |

To enable Native AOT:
```xml
<PropertyGroup>
    <PublishAot>true</PublishAot>
</PropertyGroup>
```

Flat-only schemas can be enforced project-wide with `<ParquetGeneratorFlatOnly>true</ParquetGeneratorFlatOnly>` in MSBuild.

## Compatibility

The V5/classic generator is a declared core subset for Parquet.Net 4.x/5.x: flat read/write,
batched write, row-group write, and schema. The modern v6 generator additionally provides builder,
filtering, parallel, streaming, column-batch, and Arrow `RecordBatch` ingestion surfaces (Arrow export is not shipped). The compatibility
matrix documents the supported model and file envelope.

## 🔗 Links & Resources

* [Documentation Portal](https://docs.ryankelly.dev/parquet-sourcegenerator)
* [GitHub Repository](https://github.com/rtkelly13/Parquet.SourceGenerator)
* [Design Documentation](https://github.com/rtkelly13/Parquet.SourceGenerator/blob/main/docs/INDEX.md)
* [Contributing Guide](https://github.com/rtkelly13/Parquet.SourceGenerator/blob/main/CONTRIBUTING.md)
* [MIT License](https://github.com/rtkelly13/Parquet.SourceGenerator/blob/main/LICENSE)
