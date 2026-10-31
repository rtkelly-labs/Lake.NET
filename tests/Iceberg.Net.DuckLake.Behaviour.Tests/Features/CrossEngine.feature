@spec:ducklake:conflict-resolution @spec:ducklake:select
Feature: DuckDB and this library on one lake
  DuckDB's ducklake extension is the reference implementation: it reads what this library writes, and this
  library reads what it writes, including the rows DuckDB keeps in the catalog (inlining) and the files its
  maintenance merges. Each version runs against the DuckDB that writes it (1.5 for 1.0, 2.0 for 1.1).

  Scenario Outline: DuckDB reads what this library wrote, at every snapshot
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name | type              |
      | id   | int64             |
      | tags | list(varchar)     |
      | p    | struct(x int32, label varchar) |
    And I inserted into "t":
      | id | tags          | p                       |
      | 1  | [a, b]        | {'x': 1, 'label': one}  |
      | 2  | []            | null                    |
    When I delete from "t" where "id" = 1
    And I add the column "extra" of type "varchar" to "t"
    And I insert into "t":
      | id | tags | p    | extra |
      | 3  | null | null | e     |
    Then DuckDB reads the same rows from "t" at every snapshot
    And DuckDB answers "SELECT count(*) FROM lake.t" with "2"

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  @spec:ducklake:data-inlining @spec:ducklake:deletion-inlining
  Scenario Outline: This library reads and deletes what DuckDB wrote
    Given a DuckLake that DuckDB created on <catalog> in format <version>
    And DuckDB ran:
      """
      CREATE TABLE lake.t (id BIGINT, name VARCHAR);
      INSERT INTO lake.t SELECT i, 'n' || i FROM range(100) r(i);
      INSERT INTO lake.t VALUES (1000, 'inlined');
      DELETE FROM lake.t WHERE id < 3;
      UPDATE lake.t SET name = 'upd' WHERE id = 50
      """
    When I delete from "t" where "id" >= 90
    Then DuckDB reads the same rows from "t" at every snapshot
    And DuckDB answers "SELECT count(*) FROM lake.t" with "87"

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  Scenario Outline: A commit retries over DuckDB's insert and refuses over its delete
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name | type  |
      | id   | int64 |
    And I inserted into "t":
      | id |
      | 1  |
    When writer A stages an insert into "t":
      | id |
      | 2  |
    And DuckDB runs:
      """
      INSERT INTO lake.t VALUES (3)
      """
    And writer A commits
    Then the commit succeeds
    When writer B stages an insert into "t":
      | id |
      | 4  |
    And DuckDB runs:
      """
      DELETE FROM lake.t WHERE id = 1
      """
    And writer B commits
    Then the commit fails with "deleted from"
    And DuckDB reads the same rows from "t"

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |
