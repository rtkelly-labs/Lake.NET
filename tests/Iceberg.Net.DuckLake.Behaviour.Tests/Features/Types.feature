@spec:ducklake:data-types @spec:ducklake:primitive-types
Feature: Unsigned integers, coarse timestamps, timetz and interval
  Every DuckLake type DuckDB can store in Parquet and read back is stored as DuckDB stores it. Parquet keeps the instant
  of a timetz (at offset +00) and the milliseconds of an interval; inlined rows keep both exactly, as DuckDB's do.

  Scenario Outline: Values written to a data file read back as Parquet keeps them
    Given a DuckLake on <catalog> in format <version>
    And a table "t" with columns:
      | name | type         |
      | id   | int64        |
      | u8   | uint8        |
      | u64  | uint64       |
      | ms   | timestamp_ms |
      | tz   | timetz       |
      | iv   | interval     |
    When I insert into "t":
      | id | u8  | u64                  | ms                         | tz                    | iv                             |
      | 1  | 255 | 18446744073709551615 | 2024-01-02 03:04:05.678    | 01:02:03.456789+05:30 | 1 month 2 days 00:00:03.004005 |
      | 2  | 0   | 0                    | 1960-01-01 00:00:00.001    | 23:59:59-03           | 1 year 3 days                  |
    Then there is 1 data file in storage
    And "t" contains exactly:
      | id | u8  | u64                  | ms                      | tz                 | iv                          |
      | 1  | 255 | 18446744073709551615 | 2024-01-02 03:04:05.678 | 19:32:03.456789+00 | 1 month 2 days 00:00:03.004 |
      | 2  | 0   | 0                    | 1960-01-01 00:00:00.001 | 02:59:59+00        | 1 year 3 days               |
    And "t" filtered by "u64" > 9223372036854775807 reads 1 file and contains exactly:
      | id | u8  | u64                  | ms                      | tz                 | iv                          |
      | 1  | 255 | 18446744073709551615 | 2024-01-02 03:04:05.678 | 19:32:03.456789+00 | 1 month 2 days 00:00:03.004 |
    And "t" filtered by "ms" > 2030-01-01 00:00:00 reads 0 files and contains exactly:
      | id | u8 | u64 | ms | tz | iv |
    And DuckDB answers "SELECT u64::VARCHAR || ' ' || tz::VARCHAR || ' ' || iv::VARCHAR FROM lake.t WHERE id = 1" with ""18446744073709551615 19:32:03.456789+00 1 month 2 days 00:00:03.004""

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  Scenario Outline: Inlined values keep their offset and microseconds until they are flushed
    Given a DuckLake on <catalog> in format <version> that inlines inserts of up to 5 rows
    And a table "t" with columns:
      | name | type     |
      | id   | int64    |
      | tz   | timetz   |
      | iv   | interval |
    When I insert into "t":
      | id | tz                    | iv                                |
      | 1  | 01:02:03.456789+05:30 | 1 month 2 days 00:00:03.004005    |
      | 2  | 12:00:00-15:59:59     | 0.000001 seconds                  |
    Then there are 0 data files in storage
    And "t" contains exactly:
      | id | tz                    | iv                             |
      | 1  | 01:02:03.456789+05:30 | 1 month 2 days 00:00:03.004005 |
      | 2  | 12:00:00-15:59:59     | 00:00:00.000001                |
    And DuckDB answers "SELECT tz::VARCHAR || ' ' || iv::VARCHAR FROM lake.t WHERE id = 1" with ""01:02:03.456789+05:30 1 month 2 days 00:00:03.004005""
    When I flush the inlined data
    Then there is 1 data file in storage
    And "t" contains exactly:
      | id | tz                 | iv                          |
      | 1  | 19:32:03.456789+00 | 1 month 2 days 00:00:03.004 |
      | 2  | 03:59:59+00        | 00:00:00                    |

    Examples:
      | catalog    | version |
      | SQLite     | 1.0     |
      | SQLite     | 1.1     |
      | PostgreSQL | 1.0     |
      | PostgreSQL | 1.1     |

  Scenario Outline: Values a type cannot hold are refused
    Given a DuckLake on SQLite in format 1.0 that inlines inserts of up to <limit> rows
    And a table "t" with columns:
      | name | type   |
      | id   | int64  |
      | <column> | <type> |
    When I insert into "t":
      | id | <column> |
      | 1  | <value>  |
    Then the write fails with "<message>"

    Examples:
      | limit | column | type     | value                | message                     |
      | 5     | u8     | uint8    | 256                  | is not a uint8              |
      | 5     | u64    | uint64   | 18446744073709551616 | is not a uint64             |
      | 0     | iv     | interval | -1 day               | do not support negative     |
