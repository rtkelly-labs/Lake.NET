@spec:row-level-deletes
Feature: Updating and merging rows
  Producers correct rows in place (UPDATE … SET … WHERE) and reconcile a table with a feed (MERGE INTO … USING …
  ON key). Each is one snapshot: a reader sees all of the change or none of it. Changed rows are removed with
  copy-on-write or position deletes (deletion vectors on v3) and written again with their new values.

  Background:
    Given the default rows are:
      | id | name  | region |
      | 1  | alice | eu     |
      | 2  | bob   | us     |
      | 3  | carol | eu     |
      | 4  | dave  | us     |

  Scenario Outline: Correcting a value changes only the rows the filter selects
    Given a format v<version> table keyed by "id" holding the default rows
    When I set name to "robert" where id is 2 with <strategy>
    Then the change reports 1 updated, 0 deleted and 0 inserted
    And the table contains exactly:
      | id | name   | region |
      | 1  | alice  | eu     |
      | 2  | robert | us     |
      | 3  | carol  | eu     |
      | 4  | dave   | us     |
    And the table's delete files are <delete files>
    And PyIceberg reads the same rows

    Examples:
      | version | strategy         | delete files          |
      | 2       | copy-on-write    | none                  |
      | 2       | position deletes | position delete files |
      | 3       | copy-on-write    | none                  |
      | 3       | position deletes | deletion vectors      |

  Scenario Outline: A customer who moves region moves to that region's partition
    Given a format v<version> table keyed by "id" and partitioned by "region"
    And the table contains the default rows
    When I set region to "apac" where id is 3 with <strategy>
    Then the table contains exactly:
      | id | name  | region |
      | 1  | alice | eu     |
      | 2  | bob   | us     |
      | 3  | carol | apac   |
      | 4  | dave  | us     |
    And the row with id 3 is stored in the "apac" partition
    And PyIceberg reads the same rows

    Examples:
      | version | strategy         |
      | 2       | copy-on-write    |
      | 3       | position deletes |

  Scenario: An update that matches nothing commits nothing
    Given a format v3 table keyed by "id" holding the default rows
    When I set name to "nobody" where id is 99 with position deletes
    Then no snapshot was committed

  @spec:equality-delete-files
  Scenario: Equality deletes cannot express an update's filter
    Given a format v2 table keyed by "id" holding the default rows
    When I set name to "x" where id is 1 with equality deletes
    Then the write fails with "UPDATE cannot use equality deletes"

  @spec:row-lineage
  Scenario Outline: Updated and merged rows keep their identity
    Given a format v3 table keyed by "id" holding the default rows
    And I note every row's row id
    When I set name to "robert" where id is 2 with <strategy>
    And I merge the feed with <strategy>, updating matches and inserting new rows:
      | id | name  | region |
      | 3  | caz   | eu     |
      | 5  | erin  | eu     |
    Then rows 1, 2, 3 and 4 keep their row ids
    And row 5 has a new row id

    Examples:
      | strategy         |
      | copy-on-write    |
      | position deletes |

  Scenario Outline: Merging a feed updates known keys and inserts new ones
    Given a format v<version> table keyed by "id" holding the default rows
    When I merge the feed with <strategy>, updating matches and inserting new rows:
      | id | name  | region |
      | 2  | bobby | us     |
      | 5  | erin  | eu     |
    Then the change reports 1 updated, 0 deleted and 1 inserted
    And the table contains exactly:
      | id | name  | region |
      | 1  | alice | eu     |
      | 2  | bobby | us     |
      | 3  | carol | eu     |
      | 4  | dave  | us     |
      | 5  | erin  | eu     |
    And PyIceberg <pyiceberg>

    Examples:
      | version | strategy         | pyiceberg           |
      | 2       | copy-on-write    | reads the same rows |
      | 2       | position deletes | reads the same rows |
      | 2       | equality deletes | refuses the table   |
      | 3       | copy-on-write    | reads the same rows |
      | 3       | position deletes | reads the same rows |
      | 3       | equality deletes | refuses the table   |

  Scenario Outline: A full feed for a region also removes that region's rows it no longer lists
    Given a format v<version> table keyed by "id" holding the default rows
    When I merge the "eu" feed with <strategy>, updating matches, inserting new rows and deleting unlisted rows:
      | id | name  | region |
      | 3  | caz   | eu     |
      | 5  | erin  | eu     |
    Then the change reports 1 updated, 1 deleted and 1 inserted
    And the table contains exactly:
      | id | name  | region |
      | 2  | bob   | us     |
      | 3  | caz   | eu     |
      | 4  | dave  | us     |
      | 5  | erin  | eu     |
    And PyIceberg reads the same rows

    Examples:
      | version | strategy         |
      | 2       | copy-on-write    |
      | 3       | position deletes |

  Scenario: A feed that lists a key twice is rejected before anything is written
    Given a format v2 table keyed by "id" holding the default rows
    When I merge the feed with copy-on-write, updating matches and inserting new rows:
      | id | name  | region |
      | 2  | bobby | us     |
      | 2  | rob   | us     |
    Then the write fails with "cardinality violation"
    And the table still holds the default rows and nothing new was written
