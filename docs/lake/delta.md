# Delta Lake (phase 4)

**Status:** target state. Nothing has been built yet. Covers the scope of `Lake.Delta` and how it will be
verified. The protocol facts here come from the Delta protocol specification and haven't been checked in
this repository yet; each one becomes a tagged test before code relies on it. What Delta shares with the other
formats is in [shared-layers.md](shared-layers.md#what-the-formats-share).

Delta starts after [phase 3](shared-layers.md#phase-3-extraction), so it is built on `Lake.Core` and
`Lake.Data` from the beginning and never depends on Iceberg.

## Scope

| Step | Covers |
|---|---|
| Read | Log replay: JSON commits, Parquet checkpoints (classic, multi-part, V2 with sidecars) and log compaction files. Protocol and table-feature checks, refusing unsupported features explicitly (as Iceberg refuses ORC). Column mapping (`none`, `name`, `id`). Partition values. Pruning on statistics. Deletion vectors. Time travel by version and timestamp |
| Write | Append, overwrite and delete, committed by put-if-absent through `Lake.IO`. Conflict detection and retry. Writing checkpoints and deletion vectors. Row tracking |
| Maintenance | Vacuum, checkpointing and log cleanup, compaction (optimize) |

## Verification

The same structure as the other formats ([VISION.md](../../VISION.md#rules-every-format-follows)):

- **Spec vectors:** the Delta Acceptance Tests (DAT) reader cases, committed under `tests/**/fixtures/` and
  regenerated rather than edited by hand ([D7](decisions.md#d7)).
- **Oracles:** delta-rs (the Python `deltalake` package) under `interop/` as the fast oracle, and Spark with
  Delta in the `cross-engine` suite, run in both directions.
- **Traceability:** a section map of the Delta protocol for `tools/spec_trace.py`, with tests tagged by section.
- **Corruption:** the log, checkpoint and deletion-vector decoders follow the `Corruption` contract, with
  samples in `CorruptionTests` and fuzz targets.
- **Deletion-vector bytes:** read Delta's own DV test files with the Roaring64 codec shared with Iceberg, to
  confirm that the two layouts really match.
- **Storage:** S3 put-if-absent on `_delta_log` covered by `EmulatorFidelityTests` in both emulation and AWS
  modes.
