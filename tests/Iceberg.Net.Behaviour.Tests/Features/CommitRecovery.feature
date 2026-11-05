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

  Scenario Outline: Cancellation after submission preserves uncertainty and prevents producer replay
    Given a recovery table in format v<version> using single
    When the caller cancels after the insert is committed but before its response arrives
    Then the cancelled producer remains unknown and cannot be replayed
    And uncertain staged files cannot be discarded
    And PyIceberg reads the cancelled insert exactly once

    Examples:
      | version |
      | 2       |
      | 3       |
