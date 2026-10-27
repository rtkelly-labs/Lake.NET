@spec:ducklake:data-inlining @spec:ducklake:insertion-inlining @spec:ducklake:deletion-inlining
Feature: Data inlining
  Small inserts and deletes can be kept in the catalog database instead of in Parquet files, as DuckDB does by default,
  and flushed to Parquet later. Inlined or not, every snapshot reads the same rows.

  Scenario Outline: Small inserts and deletes stay in the catalog until they are flushed
    Given a DuckLake on <catalog> in format <version> that inlines inserts of up to 5 rows
    And a table "t" with columns:
      | name | type    |
      | id   | int64   |
      | v    | varchar |
    And I inserted into "t":
      | id | v      |
      | 1  | it's   |
      | 2  | b      |
    And I inserted into "t":
      | id | v |
      | 3  | c |
    When I delete from "t" where "id" = 2
    Then there are 0 data files in storage
    And "t" has 3 inlined rows
    And the last snapshot records the changes "inlined_delete:1"
    And "t" contains exactly:
      | id | v    |
      | 1  | it's |
      | 3  | c    |
    And DuckDB reads the same rows from "t" at every snapshot
    Given I remember every snapshot of "t"
    When I flush the inlined data
    Then 3 rows were affected
    And there is 1 data file in storage
    And "t" has 0 inlined rows
    And every remaining snapshot of "t" reads as it did, row IDs included
    And DuckDB reads the same rows from "t" at every snapshot

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  Scenario Outline: An insert over the limit is written to a file
    Given a DuckLake on <catalog> in format <version> that inlines inserts of up to 1 row
    And a table "t" with columns:
      | name | type  |
      | id   | int64 |
    When I insert into "t":
      | id |
      | 1  |
      | 2  |
    Then there is 1 data file in storage
    And "t" has 0 inlined rows
    And the last snapshot records the changes "inserted_into_table:1"

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | PostgreSQL | 1.1     |
