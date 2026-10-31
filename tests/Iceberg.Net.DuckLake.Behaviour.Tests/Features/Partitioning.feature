@spec:ducklake:ducklake_partition_info @spec:ducklake:ducklake_partition_column @spec:ducklake:ducklake_file_partition_value
Feature: Partitioned writes
  A partitioned table splits each insert into one file per partition key value and records the values per file;
  file statistics then prune by the partition columns.

  Scenario Outline: Identity and calendar transforms
    Given a DuckLake on <catalog> in format <version>
    And a table "events" with columns:
      | name   | type    |
      | id     | int64   |
      | region | varchar |
      | day    | date    |
    And "events" is partitioned by:
      | column | transform |
      | region | identity  |
      | day    | year      |
    When I insert into "events":
      | id | region | day        |
      | 1  | eu     | 2023-03-01 |
      | 2  | eu     | 2024-03-01 |
      | 3  | us     | 2024-07-01 |
      | 4  | null   | null       |
    Then "events" has 4 live data files
    And the partition values of "events" are:
      | values    |
      | eu/2023   |
      | eu/2024   |
      | us/2024   |
      | null/null |
    And "events" filtered by "region" = eu reads 2 files and contains exactly:
      | id | region | day        |
      | 1  | eu     | 2023-03-01 |
      | 2  | eu     | 2024-03-01 |

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |


  Scenario Outline: Iceberg's epoch transforms and buckets
    Given a DuckLake on <catalog> in format 1.1
    And a table "clicks" with columns:
      | name    | type        |
      | user_id | int32       |
      | ts      | timestamptz |
    And "clicks" is partitioned by:
      | column  | transform   |
      | ts      | epoch_month |
      | user_id | bucket(8)   |
    When I insert into "clicks":
      | user_id | ts                     |
      | 42      | 2024-03-15 10:00:00+00 |
    Then the partition values of "clicks" are:
      | values |
      | 650/<bucket> |

    Examples:
      | catalog    | bucket |
      | SQLite     | 6      |
      | PostgreSQL | 6      |
