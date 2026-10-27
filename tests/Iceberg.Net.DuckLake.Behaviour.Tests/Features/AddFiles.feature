@spec:ducklake:ducklake_column_mapping @spec:ducklake:ducklake_name_mapping
Feature: Adding existing Parquet files
  Parquet files written by other tools can be registered as a table's data files without rewriting them, as DuckDB's
  ducklake_add_data_files does: their columns are matched by name through a column mapping, their statistics come
  from their footers, and they stay where they are.

  Scenario Outline: Files DuckDB wrote are added here and read by both engines
    Given a DuckLake on <catalog> in format 1.0
    And a table "t" with columns:
      | name   | type    |
      | id     | int64   |
      | name   | varchar |
      | region | varchar |
    And DuckDB ran:
      """
      COPY (SELECT 'n' || i AS name, i AS id, 'eu' AS region FROM range(3) r(i)) TO '{root}/ext' (FORMAT parquet, PARTITION_BY (region));
      COPY (SELECT (i + 10)::INT AS id FROM range(2) r(i)) TO '{root}/ext/b.parquet'
      """
    When I add the files "ext/region=eu/data_0.parquet" to "t" with Hive partitioning
    Then 3 rows were affected
    When I add the files "ext/b.parquet" to "t"
    Then the write fails with "was not found in file"
    When I add the files "ext/b.parquet" to "t" allowing missing columns
    Then there are 0 data files in storage
    And "t" contains exactly:
      | id | name | region |
      | 0  | n0   | eu     |
      | 1  | n1   | eu     |
      | 2  | n2   | eu     |
      | 10 | null | null   |
      | 11 | null | null   |
    And "t" filtered by "id" >= 10 reads 1 file and contains exactly:
      | id | name | region |
      | 10 | null | null   |
      | 11 | null | null   |
    And DuckDB reads the same rows from "t"

    Examples:
      | catalog    |
      | SQLite     |
      | PostgreSQL |
