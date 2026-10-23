@spec:row-level-deletes @spec:row-lineage
Feature: Other engines see what we write
  Whatever write mode and delete mechanism a C# producer uses, Spark with Iceberg Java (the reference
  implementation) must read exactly the same rows, and on format v3 derive the same row lineage; so must Trino,
  through the shared REST catalog. (Each Spark check starts a JVM, so this feature is deliberately small.)

  Scenario Outline: Upserts and deletes read identically in Spark
    Given a format v<version> table keyed by "id"
    And the table contains:
      | id | name  | region |
      | 1  | alice | eu     |
      | 2  | bob   | us     |
      | 3  | carol | eu     |
      | 4  | dave  | us     |
    When I upsert with <mechanism>:
      | id | name    | region |
      | 2  | bobby   | us     |
      | 5  | eve     | eu     |
    And I delete keys 1 and 3 with <mechanism>
    Then the table contains exactly:
      | id | name  | region |
      | 2  | bobby | us     |
      | 4  | dave  | us     |
      | 5  | eve   | eu     |
    And Spark reads the same rows

    Examples:
      | version | mechanism        |
      | 2       | position deletes |
      | 2       | equality deletes |
      | 3       | position deletes |
      | 3       | copy-on-write    |

  # Trino 478 reads v3 tables but not deletion vectors, so v3 here is copy-on-write.
  Scenario Outline: Trino reads our deletes through the REST catalog
    Given the tables live in the shared REST catalog
    And a format v<version> table keyed by "id"
    And the table contains:
      | id | name  | region |
      | 1  | alice | eu     |
      | 2  | bob   | us     |
      | 3  | carol | eu     |
    When I upsert with <mechanism>:
      | id | name  | region |
      | 2  | bobby | us     |
      | 4  | dan   | eu     |
    And I delete keys 1 and 3 with <mechanism>
    Then the table contains exactly:
      | id | name  | region |
      | 2  | bobby | us     |
      | 4  | dan   | eu     |
    And Trino reads the same rows

    Examples:
      | version | mechanism        |
      | 2       | equality deletes |
      | 2       | position deletes |
      | 3       | copy-on-write    |
