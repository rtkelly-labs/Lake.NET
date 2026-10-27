@spec:ducklake:delete @spec:ducklake:update @spec:ducklake:ducklake_delete_file
Feature: Deleting and updating rows
  Deletes are merge-on-read: a delete file lists the deleted positions of one data file, and a newer delete file
  replaces the older one. UPDATE is a delete and an insert in one snapshot.

  Scenario Outline: Delete by predicate, twice on the same file
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name   | type    |
      | id     | int64   |
      | region | varchar |
    And I inserted into "t":
      | id | region |
      | 1  | eu     |
      | 2  | us     |
      | 3  | eu     |
      | 4  | us     |
    When I delete from "t" where "region" = us
    Then 2 rows were affected
    When I delete from "t" where "id" = 1
    Then "t" contains exactly:
      | id | region |
      | 3  | eu     |
    And "t" at snapshot 2 contains exactly:
      | id | region |
      | 1  | eu     |
      | 2  | us     |
      | 3  | eu     |
      | 4  | us     |
    And the last snapshot records the changes "deleted_from_table:1"

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  Scenario Outline: A file that loses every row leaves the table
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
    When I delete from "t" where "id" = 2
    Then "t" has 1 live data file

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  Scenario Outline: Update rewrites the matching rows in one snapshot
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name   | type    |
      | id     | int64   |
      | status | varchar |
    And I inserted into "t":
      | id | status  |
      | 1  | open    |
      | 2  | open    |
    When I update "t" setting "status" to "closed" where "id" = 2
    Then 1 row was affected
    And "t" contains exactly:
      | id | status |
      | 1  | open   |
      | 2  | closed |
    And the last snapshot records the changes "inserted_into_table:1,deleted_from_table:1"

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |
