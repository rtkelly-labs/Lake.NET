@spec:ducklake:conflict-resolution @spec:ducklake:data
Feature: Concurrent writers
  Two transactions that start from the same snapshot race for the next snapshot ID. The loser reads what the
  winner changed (changes_made) and retries without rewriting files, unless the spec's rules call it a conflict.

  Scenario Outline: Concurrent inserts both land
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name | type  |
      | id   | int64 |
    When writer A stages an insert into "t":
      | id |
      | 1  |
    And writer B stages an insert into "t":
      | id |
      | 2  |
    And writer A commits
    And writer B commits
    Then the commit succeeds
    And "t" contains exactly:
      | id |
      | 1  |
      | 2  |

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  Scenario Outline: An insert loses to a concurrent delete
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name | type  |
      | id   | int64 |
    And I inserted into "t":
      | id |
      | 1  |
      | 2  |
    When writer A stages an insert into "t":
      | id |
      | 3  |
    And writer B stages a delete from "t" where "id" = 1
    And writer B commits
    And writer A commits
    Then the commit fails with "inserting"
    And "t" contains exactly:
      | id |
      | 2  |

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  Scenario Outline: Deletes conflict only on the same data file
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name | type  |
      | id   | int64 |
    And I inserted into "t":
      | id |
      | 1  |
      | 2  |
    And I inserted into "t":
      | id |
      | 10 |
      | 11 |
    When writer A stages a delete from "t" where "id" = 1
    And writer B stages a delete from "t" where "id" = 2
    And writer C stages a delete from "t" where "id" = 10
    And writer A commits
    And writer B commits
    Then the commit fails with "concurrently"
    When writer C commits
    Then the commit succeeds
    And "t" contains exactly:
      | id |
      | 2  |
      | 11 |

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  @spec:ducklake:tables-views
  Scenario Outline: Two writers cannot create the same table
    Given a DuckLake on <catalog> in format <version>
    When writer A stages the table "same"
    And writer B stages the table "same"
    And writer A commits
    And writer B commits
    Then the commit fails with "created"

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |
