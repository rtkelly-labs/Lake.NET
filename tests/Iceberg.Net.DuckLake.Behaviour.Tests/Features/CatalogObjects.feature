@spec:ducklake:ducklake_tag @spec:ducklake:ducklake_view @spec:ducklake:ducklake_sort_info
Feature: Comments, views, renames and sort orders
  Metadata DuckDB keeps in the catalog besides tables: comments (tags), views, table renames and the order new data is
  written in. They are versioned like everything else, and DuckDB uses what this library records.

  Scenario Outline: A commented, renamed and sorted table with a view over it
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name | type    |
      | id   | int64   |
      | v    | varchar |
    And I inserted into "t":
      | id | v |
      | 1  | a |
      | 2  | b |
    When I comment on "t" with "events"
    And I comment on the column "v" of "t" with "a value"
    And I rename the table "t" to "events"
    And I sort "events" by "id" descending
    And I create the view "recent" as "SELECT id FROM {DUCKLAKE_CATALOG}.events WHERE (id > 1)"
    Then the comment of "events" is "events"
    And the comment of the column "v" of "events" is "a value"
    And the views of "main" are "recent"
    And "t" at snapshot 2 contains exactly:
      | id | v |
      | 1  | a |
      | 2  | b |
    And DuckDB answers "SELECT comment FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = 'events'" with ""events""
    And DuckDB answers "SELECT count(*) FROM lake.recent" with "1"

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |
