@spec:default-values
Feature: Typed write defaults
  Missing typed members receive the table's write default. An explicit null is a value, not a missing member.

  Scenario Outline: A typed insert fills omitted optional and required columns
    Given a format v3 table with typed write defaults using <format>
    When I insert a typed row omitting the defaulted columns
    Then the table contains exactly:
      | id | name      | region |
      | 1  | anonymous | eu     |
    And <reader> reads the same rows

    Examples:
      | format  | reader    |
      | parquet | PyIceberg |
      | avro    | Spark     |
