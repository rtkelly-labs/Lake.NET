# File formats

**Status:** reference, checked 2026-10-31. The file formats that sit under the table formats in
[../specs/](../specs/README.md). The shared data plane (`Lake.Data`, `Lake.Puffin`, Roaring64 in `Lake.Core`) is built
on these, so a change here can affect every format.

| Format | Page | Used by | Lake.NET code |
|---|---|---|---|
| Apache Parquet | [parquet.md](parquet.md) | Data files of all three formats; DuckLake and Delta position deletes; Delta checkpoints | Two backends behind `IParquetBackend`: ParquetSharp (default) and Lake.Parquet, the Parquet.Net fork |
| Apache Avro | [avro.md](avro.md) | Iceberg manifests and manifest lists; Iceberg Avro data files | In-house codec (`Iceberg.Net.Avro`) |
| Puffin and deletion vectors | [puffin-and-deletion-vectors.md](puffin-and-deletion-vectors.md) | Iceberg v3 DVs and statistics; DuckLake 1.1 DVs; Delta DVs (Delta's own container) | `Iceberg.Net.Puffin`, Roaring64 in Core |
| Apache ORC | [orc.md](orc.md) | Iceberg only (optional) | Refused today; a port is planned (decision D8) |

## Which format uses what

| | Iceberg | DuckLake | Delta |
|---|---|---|---|
| Data files | Parquet (Avro and ORC allowed) | Parquet | Parquet |
| Column identity in the file | Parquet `field_id` / Avro `field-id` | Parquet `field_id` = column ID | Logical name, or physical name / `field_id` under column mapping |
| Metadata files | JSON (table metadata), Avro (manifest lists, manifests) | none (SQL rows) | JSON (log), Parquet (checkpoints) |
| Deletion bitmaps | Puffin `deletion-vector-v1` blob | Puffin (1.1) | Delta DV file or inline Z85 |
| Statistics files | Puffin (theta sketches), partition statistics (Parquet) | none | none |
