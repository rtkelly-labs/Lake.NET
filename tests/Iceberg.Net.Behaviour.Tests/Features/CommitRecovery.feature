@spec:commit-conflict-resolution-and-retry @spec:snapshot-retention-policy
Feature: Recover uncertain commits without replaying them
  Missing snapshot evidence does not prove that a commit failed.
  Response loss and maintenance must never turn one input batch into two inserts.

  Scenario Outline: Expiry after a lost response preserves uncertainty and the committed rows
    Given a recovery table in format v<version> using <mode>
    When an insert commits but its response is lost after another insert and snapshot expiry
    Then recovery reports an unknown outcome without replaying the input
    And uncertain staged files cannot be discarded
    And PyIceberg reads each recovery row exactly once

    Examples:
      | version | mode        |
      | 2       | single      |
      | 3       | single      |
      | 2       | transaction |
      | 3       | transaction |
