# Proposed upstream tickets

Bugs and gaps this project found in other projects, written up as tickets ready to file. **None has been filed**:
filing is a decision for the team. Each page has a title, the target repository, and a body you can paste as is.

| Proposed ticket | Target | Type | Page |
|---|---|---|---|
| Parquet field IDs on group fields (struct, list, map) | [aloneguid/parquet-dotnet](https://github.com/aloneguid/parquet-dotnet/issues/new) | Feature request with a verified minimal patch | [parquet-net-group-field-ids.md](parquet-net-group-field-ids.md) (body under "Issue") |
| A null `list<struct>` reads as an empty list | [apache/iceberg-python](https://github.com/apache/iceberg-python/issues/new) | Bug, one-line fix | [pyiceberg-null-list-of-struct.md](pyiceberg-null-list-of-struct.md) |
| Scan planning sends deletion vectors to the wrong data files | [apache/gravitino](https://github.com/apache/gravitino/issues/new) | Bug (fixed by Iceberg 1.12; ask for the upgrade) | [gravitino-dv-references.md](gravitino-dv-references.md) |
| Iceberg-ready columns: field IDs, local timestamps, INT32 dates | Parquet.SourceGenerator (our own) | Patch, `git am`-ready | [parquet-sourcegenerator-iceberg-columns.patch](parquet-sourcegenerator-iceberg-columns.patch) |

## Filing one

1. Search the target's issues for the title first; someone may have reported it since.
2. Use the page's title as the issue title. Paste the page from "Summary" (or the "Issue" section for Parquet.Net)
   to the end as the body, without the "Status" and "Our workaround" parts, which are about this repository.
3. Change the page's **Status** line to the issue link, and update the matching row here.

## What depends on them

- **Parquet.Net:** nested Iceberg columns through Parquet.SourceGenerator. ParquetSharp, which the library itself
  writes with, is not affected.
- **PyIceberg:** only our PyIceberg cross-check, which avoids null `list<struct>` values; Spark checks them instead.
- **Gravitino:** nothing in our code. `RestCatalog` matches deletion vectors by `referenced-data-file`, so it reads
  these plans correctly.
