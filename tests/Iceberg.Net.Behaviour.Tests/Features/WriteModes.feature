@spec:row-level-deletes @spec:identifier-field-ids
Feature: Write modes
  C# producers change Iceberg tables in three ways: pure insertion, upsert by key, and replacement of whole
  groups. Existing rows are removed with one of three delete mechanisms (copy-on-write, position deletes,
  equality deletes). Whatever the mechanism, every reader must see the same rows.

  Scenario Outline: Pure insertion never touches existing rows
    Given a format v<version> table keyed by "id"
    And the table contains:
      | id | name | region |
      | 1  | a    | eu     |
      | 2  | b    | us     |
    When I insert:
      | id | name | region |
      | 3  | c    | eu     |
    Then the table contains exactly:
      | id | name | region |
      | 1  | a    | eu     |
      | 2  | b    | us     |
      | 3  | c    | eu     |
    And the last snapshot operation is "append"
    And the table has no delete files
    And PyIceberg reads the same rows

    Examples:
      | version |
      | 2       |
      | 3       |

  Scenario Outline: Upsert by key replaces matching rows and inserts new ones
    Given a format v<version> table keyed by "id"
    And the table contains:
      | id | name | region |
      | 1  | a    | eu     |
      | 2  | b    | us     |
      | 3  | c    | eu     |
    When I upsert with <strategy>:
      | id | name   | region |
      | 2  | b-v2   | us     |
      | 4  | d      | us     |
    Then the table contains exactly:
      | id | name | region |
      | 1  | a    | eu     |
      | 2  | b-v2 | us     |
      | 3  | c    | eu     |
      | 4  | d    | us     |
    And the last snapshot operation is "overwrite"
    And the table's delete files are <delete files>
    And PyIceberg <pyiceberg>

    Examples:
      | version | strategy         | delete files              | pyiceberg            |
      | 2       | copy-on-write    | none                      | reads the same rows  |
      | 2       | position deletes | position delete files     | reads the same rows  |
      | 2       | equality deletes | equality delete files     | refuses the table    |
      | 3       | copy-on-write    | none                      | reads the same rows  |
      | 3       | position deletes | deletion vectors          | reads the same rows  |
      | 3       | equality deletes | equality delete files     | refuses the table    |

  Scenario Outline: Upserting the same rows twice is idempotent
    Given a format v<version> table keyed by "id"
    And the table contains:
      | id | name | region |
      | 1  | a    | eu     |
    When I upsert with <strategy>:
      | id | name | region |
      | 1  | a2   | eu     |
    And I upsert with <strategy>:
      | id | name | region |
      | 1  | a2   | eu     |
    Then the table contains exactly:
      | id | name | region |
      | 1  | a2   | eu     |

    Examples:
      | version | strategy         |
      | 2       | copy-on-write    |
      | 2       | position deletes |
      | 2       | equality deletes |
      | 3       | position deletes |

  Scenario: Upsert input must not repeat a key
    Given a format v2 table keyed by "id"
    When I upsert with copy-on-write:
      | id | name | region |
      | 1  | a    | eu     |
      | 1  | b    | eu     |
    Then the write fails with "Duplicate upsert key"

  Scenario Outline: Group replacement swaps whole groups and leaves the rest alone
    Given a format v<version> table keyed by "id" and partitioned by "region"
    And the table contains:
      | id | name | region |
      | 1  | a    | eu     |
      | 2  | b    | us     |
      | 3  | c    | eu     |
      | 4  | d    | apac   |
    When I replace the "region" groups with <strategy>:
      | id | name | region |
      | 10 | x    | eu     |
      | 11 | y    | us     |
    Then the table contains exactly:
      | id | name | region |
      | 4  | d    | apac   |
      | 10 | x    | eu     |
      | 11 | y    | us     |
    And PyIceberg <pyiceberg>

    Examples:
      | version | strategy         | pyiceberg           |
      | 2       | copy-on-write    | reads the same rows |
      | 2       | position deletes | reads the same rows |
      | 2       | equality deletes | refuses the table   |
      | 3       | copy-on-write    | reads the same rows |
      | 3       | position deletes | reads the same rows |
