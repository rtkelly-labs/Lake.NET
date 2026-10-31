# Lake.Delta test fixtures

Committed outputs of other engines. **Regenerate them, never hand-edit them.** Tests that need a changed table copy
one to a temporary directory first (`Fixtures.CopyToTemp`). The fixtures are read in place from the source tree, not
copied to the build output: DAT's partition directories have percent escapes in their names (`letter=%2F%2520%25f`),
which MSBuild would unescape on copy.

## `dat/`: Delta Acceptance Tests

| | |
|---|---|
| Source | [delta-incubator/dat](https://github.com/delta-incubator/dat), release `v0.0.3`, asset `deltalake-dat-v0.0.3.tar.gz` (sha256 `19c045bc6f4e8531d1985d0f7bb156d788e65078b435d613c8e4a9c753b4a982`), directory `out/reader_tests/generated/` |
| Written by | Apache Spark 3.5.1 with Delta Lake 3.1.0 (`engineInfo` in each commit) |
| Licence | Apache License 2.0 (the DAT repository's licence) |
| Size | 19 cases, 2.4 MB, copied whole (`.crc` side files included) |
| Latest release? | v0.0.3 was the newest asset reachable on 2026-10-31 (`v0.0.4` returns 404) |

Regenerate:

```sh
curl -sSL -o dat.tar.gz https://github.com/delta-incubator/dat/releases/download/v0.0.3/deltalake-dat-v0.0.3.tar.gz
mkdir dat-x && tar -xzf dat.tar.gz -C dat-x
rm -rf tests/Lake.Delta.Tests/fixtures/dat && cp -a dat-x/out/reader_tests/generated tests/Lake.Delta.Tests/fixtures/dat
```

Each case has `delta/` (the table), `expected/<v0|v1|…|latest>/table_content/*.parquet` (Spark's read of that
version) and `expected/*/table_version_metadata.json` (version, protocol versions, properties).
`DatAcceptanceTests` compares every expected version, order-independently and typed.

| Case | What it exercises | Result |
|---|---|---|
| `all_primitive_types` | every non-nested type | ✅ read |
| `basic_append` | two appends; time travel v0, v1 | ✅ read |
| `basic_partitioned` | string partitions; v0, v1 | ✅ read |
| `cdf` | change data feed on (writer 4); `cdc` actions ignored by a snapshot read | ✅ read |
| `check_constraints` | a CHECK constraint (writer feature) with `deletionVectors` | ✅ read |
| `column_mapping` | name mode across a column rename | ✅ read |
| `deletion_vectors` | a stored (`u`) DV deleting 4 of 5 rows | ✅ read |
| `generated_columns` | a generated column (writer 4) | ✅ read |
| `iceberg_compat_v1` | name mode on reader 3 with `columnMapping` only in `writerFeatures` | ✅ read (see below) |
| `multi_partitioned` | string/date/binary partitions, nulls, escapes, emoji; v0–v2 | ✅ read |
| `multi_partitioned_2` | boolean, timestamp, decimal(38,18) partitions | ✅ read |
| `nested_types` | struct, array, map | ✅ read |
| `no_replay` | log before the checkpoint cleaned up | ✅ read |
| `no_stats` | no stats, JSON stats off in the checkpoint | ✅ read |
| `partitioned_with_null` | a null partition value; v0, v1 | ✅ read |
| `stats_as_struct` | checkpoint with `writeStatsAsStruct` | ✅ read |
| `timestamp_ntz` | `timestampNtz` reader feature | ✅ read |
| `with_checkpoint` | classic checkpoint + later commits | ✅ read |
| `with_schema_change` | overwrite with a new schema; v1 | ✅ read |

**Refused:** none. A case the reader cannot read must be listed in `DatAcceptanceTests.Refused` with the features its
refusal names (`DeltaUnsupportedFeatureException.Features`); a case in neither list fails the suite.

`iceberg_compat_v1` note: PROTOCOL.md honours `delta.columnMapping.mode` at reader version 3 only when `columnMapping`
is in `readerFeatures`. delta-spark 3.1 lists it only in `writerFeatures`, and Spark reads the table with the mapping.
Lake.Delta treats a reader-writer feature in either list as in use (`DeltaFeatures.RequiredReaderFeatures`), which also
means it refuses an unsupported reader-writer feature listed only for writers.

## `checkpoint-shapes/`

DAT ships only classic checkpoints. `tools/delta_checkpoint_fixtures.py` cuts DAT `with_checkpoint`'s checkpoint into
a two-part multi-part checkpoint (`multi-part/part1.parquet`: the remove and add rows; `part2.parquet`: protocol and
metaData) and a V2 sidecar (`sidecar.parquet`: the add and remove columns of the file-action rows), with pyarrow.
Rows are unchanged. Regenerate:

```sh
uv run --project interop python -I tools/delta_checkpoint_fixtures.py
```

(ParquetSharp's own Arrow writer cannot be used for this: re-exporting a nested batch imported from its reader crashes
Apache.Arrow 18's C data release callback.)

## Shared with Iceberg.Net.Spec.Tests

`tests/Iceberg.Net.Spec.Tests/fixtures/delta-rs/table-with-dv-small` (see its README) is read in place by
`DeletionVectorTests`: values 1–8 after its DV removes 0 and 9.
