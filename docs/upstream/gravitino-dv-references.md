# Gravitino: scan planning sends deletion vectors to the wrong data files (proposed ticket)

**Status:** proposed ticket, not filed (see [README.md](README.md)). Found by `ServerScanPlanningLiveTests` against `apache/gravitino-iceberg-rest:1.3.1`
(iceberg-core 1.11.0 underneath). The same behaviour reproduces in Iceberg Java 1.10's `PlanTableScanResponse`
serializer (`JavaMisreferencedDvTests`, fixture from the 1.10 REST image) and is gone in Iceberg 1.12.

## Summary

`POST /v1/{prefix}/namespaces/{ns}/tables/{table}/plan` on a format v3 table whose deletion vectors share one Puffin
file (the normal case: one Puffin file per commit, one blob per data file) returns `delete-file-references` that point
each file scan task at another data file's DV, or give files with no deletes a DV. A client that trusts the
references reads deleted rows back.

## Cause (Iceberg ≤ 1.11)

The response serializer indexes delete files by path. DVs in the same Puffin file have the same path and differ only
in `content-offset`, so every reference resolves to one of them. Iceberg 1.12 derives the delete files from the tasks
and keeps them apart.

## Suggested fix

Upgrade the bundled Iceberg to 1.12, or key delete files by path and content offset when serializing plans.

## Reproduction

1. Create a format v3 table, append two data files in one partition, then delete one row from each in one commit
   (one Puffin file, two DVs).
2. `POST …/plan` with no filter.
3. Compare each task's `data-file.file-path` with the `referenced-data-file` of the delete files it references.

## Our workaround

`RestCatalog.PlanTableScanAsync` matches deletion vectors to tasks by `referenced-data-file`, which the spec requires
on every DV, so it reads these plans correctly.
