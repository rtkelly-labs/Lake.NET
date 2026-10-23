@spec:commit-conflict-resolution-and-retry
Feature: Concurrent writers and format conversion
  Two writers working from the same table version must both land without losing or duplicating rows,
  and a v2 table's position deletes must survive the upgrade to v3 deletion vectors unchanged.

  Scenario Outline: Concurrent upserts of the same key keep a single row
    Given a format v<version> table keyed by "id"
    And the table contains:
      | id | name | region |
      | 1  | a    | eu     |
    And a second writer that has already loaded the table
    When I upsert with <strategy>:
      | id | name    | region |
      | 7  | from-me | eu     |
    And the second writer upserts with <strategy>:
      | id | name        | region |
      | 7  | from-second | eu     |
    Then the table contains exactly:
      | id | name        | region |
      | 1  | a           | eu     |
      | 7  | from-second | eu     |
    And the second writer needed 2 attempts

    Examples:
      | version | strategy         |
      | 2       | copy-on-write    |
      | 2       | position deletes |
      | 2       | equality deletes |
      | 3       | copy-on-write    |
      | 3       | position deletes |

  Scenario: A writer whose target file disappeared does not resurrect deletes
    Given a format v3 table keyed by "id"
    And the table contains:
      | id | name | region |
      | 1  | a    | eu     |
    And a second writer that has already loaded the table
    When I delete rows where region is "eu" with copy-on-write
    And the second writer upserts with position deletes:
      | id | name | region |
      | 1  | a2   | eu     |
    Then the table contains exactly:
      | id | name | region |
      | 1  | a2   | eu     |

  @spec:deletion-vectors @spec:version-3

  Scenario: Upgrading to v3 converts position deletes into deletion vectors
    Given a format v2 table keyed by "id"
    And the table contains:
      | id | name | region |
      | 1  | a    | eu     |
      | 2  | b    | us     |
      | 3  | c    | eu     |
    And I delete keys 1 and 3 with position deletes
    When I upgrade the table to format v3
    And I convert position deletes to deletion vectors
    Then the table contains exactly:
      | id | name | region |
      | 2  | b    | us     |
    And the table's delete files are deletion vectors
    And the last snapshot operation is "replace"
    And PyIceberg reads the same rows
