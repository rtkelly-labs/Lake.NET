@spec:ducklake:ducklake_files_scheduled_for_deletion @spec:ducklake:ducklake_data_file
Feature: Maintenance
  Merging small files, rewriting files with many deleted rows and expiring snapshots change how a table is stored,
  never what any kept snapshot reads. Files no snapshot reads any more are scheduled for deletion, and deleted later.

  Scenario Outline: Small files merge into one partial file that every snapshot still reads through
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name | type    |
      | id   | int64   |
      | v    | varchar |
    And I inserted into "t":
      | id | v |
      | 1  | a |
      | 2  | b |
    And I inserted into "t":
      | id | v |
      | 3  | c |
    And I inserted into "t":
      | id | v |
      | 4  | d |
    And I remember every snapshot of "t"
    When I merge adjacent files
    Then 3 files were compacted into 1
    And "t" has 1 live data file
    And the last snapshot records the changes "merge_adjacent:1"
    And every remaining snapshot of "t" reads as it did, row IDs included
    And 3 files are scheduled for deletion
    When I clean up old files
    Then 3 files were deleted from storage
    And every remaining snapshot of "t" reads as it did, row IDs included
    And DuckDB reads the same rows from "t" at every snapshot

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  Scenario Outline: A file that lost most of its rows is rewritten without them
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name | type  |
      | id   | int64 |
    And I inserted into "t":
      | id |
      | 1  |
      | 2  |
      | 3  |
      | 4  |
    And I deleted from "t" where "id" < 4
    And I remember every snapshot of "t"
    When I rewrite files with at least 50% deleted rows
    Then 1 file was compacted into 1
    And the last snapshot records the changes "rewrite_delete:1"
    And "t" contains exactly:
      | id |
      | 4  |
    And every remaining snapshot of "t" reads as it did, row IDs included

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  Scenario Outline: Expiring snapshots removes what only they could see
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name | type  |
      | id   | int64 |
    And I inserted into "t":
      | id |
      | 1  |
    And I inserted into "t":
      | id |
      | 2  |
    And I deleted from "t" where "id" = 1
    And I remember every snapshot of "t"
    When I expire snapshots 2, 3
    Then the snapshots are "0, 1, 4"
    And every remaining snapshot of "t" reads as it did, row IDs included
    And 1 file is scheduled for deletion
    When I clean up old files
    Then 1 file was deleted from storage
    And "t" contains exactly:
      | id |
      | 2  |

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |
