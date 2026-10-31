@spec:ducklake:ducklake_column @spec:ducklake:ducklake_schema_versions
Feature: Schema evolution
  Columns are versioned rows: adding, dropping, renaming or widening a column writes new versions with the same
  column_id, so files written before keep reading by field ID, with added columns as NULL.

  Scenario Outline: Add, rename and drop columns between writes
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name | type    |
      | id   | int32   |
      | old  | varchar |
      | gone | int32   |
    And I inserted into "t":
      | id | old | gone |
      | 1  | a   | 10   |
    When I rename the column "old" of "t" to "label"
    And I drop the column "gone" from "t"
    And I add the column "score" of type "float64" to "t"
    And I insert into "t":
      | id | label | score |
      | 2  | b     | 2.5   |
    Then "t" contains exactly:
      | id | label | score |
      | 1  | a     | null  |
      | 2  | b     | 2.5   |
    And "t" at snapshot 2 contains exactly:
      | id | old | gone |
      | 1  | a   | 10   |

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  Scenario Outline: Integer columns widen without rewriting files, and never narrow
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name | type  |
      | n    | int16 |
    And I inserted into "t":
      | n   |
      | 300 |
    When I change the type of "n" in "t" to "int64"
    And I insert into "t":
      | n             |
      | 5000000000000 |
    Then "t" contains exactly:
      | n             |
      | 300           |
      | 5000000000000 |
    When I change the type of "n" in "t" to "int32"
    Then the write fails with "cannot become"

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |
