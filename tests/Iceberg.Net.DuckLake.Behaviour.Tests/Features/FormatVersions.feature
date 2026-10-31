@spec:ducklake:ducklake_schema @spec:ducklake:ducklake_column
Feature: Format versions 1.0 and 1.1
  Both versions are read and written; a new catalog is 1.1. Features 1.1 added are refused by a 1.0 catalog
  rather than written where 1.0 readers would misread them.

  Scenario Outline: Nested schemas
    Given a DuckLake on <catalog> in format 1.1
    And the schema "sales"
    And the schema "sales.europe"
    And a table "sales.europe.orders" with columns:
      | name | type  |
      | id   | int64 |
    When I insert into "sales.europe.orders":
      | id |
      | 1  |
    Then the schemas are "main, sales, sales.europe"
    And "sales.europe.orders" contains exactly:
      | id |
      | 1  |

    Examples:
      | catalog    |
      | SQLite     |
      | PostgreSQL |

  Scenario Outline: A 1.0 catalog refuses nested schemas
    Given a DuckLake on <catalog> in format 1.0
    And the schema "sales"
    When I create the schema "sales.europe"
    Then the write fails with "needs DuckLake format version 1.1"

    Examples:
      | catalog    |
      | SQLite     |
      | PostgreSQL |

  @spec:ducklake:primitive-types
  Scenario Outline: A null column is typed later
    Given a DuckLake on <catalog> in format 1.1
    And a table "signups" with columns:
      | name     | type  |
      | id       | int32 |
      | referrer | null  |
    And I inserted into "signups":
      | id | referrer |
      | 1  | null     |
    When I change the type of "referrer" in "signups" to "varchar"
    And I insert into "signups":
      | id | referrer   |
      | 2  | newsletter |
    Then "signups" contains exactly:
      | id | referrer   |
      | 1  | null       |
      | 2  | newsletter |

    Examples:
      | catalog    |
      | SQLite     |
      | PostgreSQL |
