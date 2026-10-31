@spec:ducklake:create-table @spec:ducklake:insert @spec:ducklake:select
Feature: Tables and their rows
  A DuckLake keeps table metadata in SQL tables of a catalog database and rows in Parquet files. Every write is one
  snapshot; a table reads as the files live in the snapshot, minus their deleted rows.

  Scenario Outline: Rows written in several snapshots read back together
    Given a DuckLake on <catalog> in format <version>
    And a table "orders" with columns:
      | name     | type                         | nullable |
      | id       | int64                        | no       |
      | customer | varchar                      | yes      |
      | amount   | decimal(10,2)                | yes      |
      | placed   | date                         | yes      |
      | items    | list(struct(sku varchar, qty int32)) | yes |
    When I insert into "orders":
      | id | customer | amount | placed     | items                       |
      | 1  | ada      | 12.50  | 2024-01-01 | [{'sku': a1, 'qty': 2}]     |
      | 2  | grace    | 7.00   | 2024-01-02 | []                          |
    And I insert into "orders":
      | id | customer | amount | placed     | items |
      | 3  | null     | null   | null       | null  |
    Then "orders" contains exactly:
      | id | customer | amount | placed     | items                   |
      | 1  | ada      | 12.50  | 2024-01-01 | [{'sku': a1, 'qty': 2}] |
      | 2  | grace    | 7.00   | 2024-01-02 | []                      |
      | 3  | null     | null   | null       | null                    |
    And "orders" has 2 live data files
    And the last snapshot records the changes "inserted_into_table:1"

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  Scenario Outline: A NOT NULL column refuses a null
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name | type  | nullable |
      | id   | int64 | no       |
    When I insert into "t":
      | id   |
      | null |
    Then the write fails with "NOT NULL"

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  @spec:ducklake:select-with-file-pruning
  Scenario Outline: File statistics skip files a filter cannot match
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name | type  |
      | id   | int64 |
    And I inserted into "t":
      | id |
      | 1  |
      | 5  |
    And I inserted into "t":
      | id |
      | 50 |
      | 90 |
    Then "t" filtered by "id" >= 40 reads 1 file and contains exactly:
      | id |
      | 50 |
      | 90 |
    And "t" filtered by "id" > 1 and "id" < 6 reads 1 file and contains exactly:
      | id |
      | 5  |

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  @spec:ducklake:drop-table @spec:ducklake:drop-schema @spec:ducklake:create-schema
  Scenario Outline: Dropping a table and its schema keeps them readable at older snapshots
    Given a DuckLake on <catalog> in format <version>
    And the schema "sales"
    And a table "sales.orders" with columns:
      | name | type  |
      | id   | int64 |
    And I inserted into "sales.orders":
      | id |
      | 1  |
    When I drop the schema "sales"
    Then the write fails with "not empty"
    When I drop the table "sales.orders"
    And I drop the schema "sales"
    Then the schemas are "main"
    And "sales.orders" at snapshot 3 contains exactly:
      | id |
      | 1  |

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |
