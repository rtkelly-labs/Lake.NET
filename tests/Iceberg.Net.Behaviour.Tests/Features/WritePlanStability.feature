@spec:commit-conflict-resolution-and-retry @spec:schema-evolution
Feature: Preserve write bindings through concurrent metadata changes
  A retry cannot reinterpret an old input batch or key against a new column layout.

  Scenario Outline: Reordered schema rejects the old plan and preserves the concurrent winner
    Given a format v<version> stability table
    When a schema reorder and competing upsert land during my <mode>
    Then the old stability plan is rejected without replay
    And PyIceberg reads only the concurrent stability winner

    Examples:
      | version | mode   |
      | 2       | upsert |
      | 3       | upsert |
      | 2       | staged |
      | 3       | staged |
      | 2       | update |
      | 3       | update |

  Scenario Outline: A metadata-only schema change fences a prepared append
    Given a format v<version> stability table
    When a prepared append races a schema-only reorder
    Then the prepared stability append is rejected
    And PyIceberg reads only the unchanged stability input

    Examples:
      | version |
      | 2       |
      | 3       |

  @spec:partition-evolution
  Scenario Outline: Prepared appends retain their original partition spec
    Given a format v<version> stability table
    When a prepared append races a partition spec change
    Then PyIceberg reads both stability rows

    Examples:
      | version |
      | 2       |
      | 3       |
