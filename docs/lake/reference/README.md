# Reference material

**Status:** reference. Facts that the plan and the code rely on, gathered in one place and dated, so a decision can
cite them and a reader can tell when they are stale. Each page says when it was last checked; re-check before relying
on anything older than a release cycle.

| Page | Read it for |
|---|---|
| [specs/](specs/README.md) | The table specifications (Iceberg, DuckLake, Delta): structures, versions, writer invariants, interop pitfalls, and where Lake.NET implements each part; a side-by-side comparison |
| [formats/](formats/README.md) | The file formats under them (Parquet, Avro, Puffin and deletion vectors, ORC): layouts, type mappings, statistics, encryption, and both Parquet backends |
| [formats-and-engines.md](formats-and-engines.md) | The specs we follow and where they are pinned; each format's reference engines; **which Parquet reader each engine uses**, which decides what interop proves |
| [dotnet-ecosystem.md](dotnet-ecosystem.md) | Every .NET library we use or should use, its role, and whether we take it, wrap it, fork it or avoid it |
| [duckdb.md](duckdb.md) | DuckDB's three roles in Lake.NET, the DuckDB 2.0 transition tracker, DuckDB.NET's state and what we would upstream to it |
| [forks.md](forks.md) | How a dependency is forked when it must be, the two fork models (rebased stack vs upstream-first), and which one each dependency uses |

The goal these serve is [../goal.md](../goal.md).
