@spec:row-level-deletes @spec:delete-formats
Feature: Deleting rows
  Rows can be deleted by key, by predicate, or by dropping whole data files, using the mechanism that fits
  the workload: copy-on-write (cheap reads), position deletes / deletion vectors (cheap writes after a scan),
  or equality deletes (cheapest writes, keys only, no scan).

  Background:
    Given the default rows are:
      | id | name | region |
      | 1  | a    | eu     |
      | 2  | b    | us     |
      | 3  | c    | eu     |
      | 4  | d    | us     |

  Scenario Outline: Delete by key
    Given a format v<version> table keyed by "id" holding the default rows
    When I delete keys 2 and 3 with <strategy>
    Then the table contains exactly:
      | id | name | region |
      | 1  | a    | eu     |
      | 4  | d    | us     |
    And the last snapshot operation is "<operation>"

    Examples:
      | version | strategy         | operation |
      | 2       | copy-on-write    | overwrite |
      | 2       | position deletes | delete    |
      | 2       | equality deletes | delete    |
      | 3       | copy-on-write    | overwrite |
      | 3       | position deletes | delete    |
      | 3       | equality deletes | delete    |

  Scenario Outline: Delete by predicate
    Given a format v<version> table keyed by "id" holding the default rows
    When I delete rows where region is "us" with <strategy>
    Then the table contains exactly:
      | id | name | region |
      | 1  | a    | eu     |
      | 3  | c    | eu     |
    And PyIceberg reads the same rows

    Examples:
      | version | strategy         |
      | 2       | copy-on-write    |
      | 2       | position deletes |
      | 3       | copy-on-write    |
      | 3       | position deletes |

  @spec:equality-delete-files

  Scenario: Equality deletes cannot express a predicate
    Given a format v2 table keyed by "id" holding the default rows
    When I delete rows where region is "us" with equality deletes
    Then the write fails with "Equality deletes delete by key"

  @spec:equality-delete-files

  Scenario: Equality deletes need the partition columns in the key
    Given a format v2 table keyed by "id" and partitioned by "region"
    And the table contains the default rows
    When I delete keys 2 and 3 with equality deletes
    Then the write fails with "partition source column"

  Scenario: Dropping whole partitions removes their files without reading them
    Given a format v2 table keyed by "id" and partitioned by "region"
    And the table contains the default rows
    When I drop the data files where region is "us"
    Then the table contains exactly:
      | id | name | region |
      | 1  | a    | eu     |
      | 3  | c    | eu     |
    And the table has no delete files

  @spec:deletion-vectors

  Scenario: Later deletes on a v3 table merge into one deletion vector per data file
    Given a format v3 table keyed by "id" holding the default rows
    When I delete keys 2 and 3 with position deletes
    And I delete keys 4 and 4 with position deletes
    Then the table contains exactly:
      | id | name | region |
      | 1  | a    | eu     |
    And every data file has at most one deletion vector
    And PyIceberg reads the same rows
